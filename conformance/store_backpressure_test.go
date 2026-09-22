//go:build integration

package conformance_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sink "github.com/batchstream/sink-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Only faults and network delay are injected. Every successful operation is
// executed by the real backend, through separate Gateway/Engine/Worker processes.
type storePressureProxy struct {
	backend   backend
	delay     atomic.Int64
	capacity  atomic.Int64
	reject    atomic.Bool
	active    atomic.Int64
	requests  atomic.Int64
	overloads atomic.Int64
}

func newStorePressureProxy(t *testing.T, store backend) *storePressureProxy {
	t.Helper()
	target, err := url.Parse(store.endpoint)
	if err != nil {
		t.Fatal(err)
	}
	forward := httputil.NewSingleHostReverseProxy(target)
	pressure := &storePressureProxy{backend: store}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/_bulk") && !strings.HasSuffix(r.URL.Path, "/_mget") {
			forward.ServeHTTP(w, r)
			return
		}
		pressure.requests.Add(1)
		active := pressure.active.Add(1)
		defer pressure.active.Add(-1)
		maximum := pressure.capacity.Load()
		if pressure.reject.Load() || maximum > 0 && active > maximum {
			pressure.overloads.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":{"type":"es_rejected_execution_exception","reason":"qualification overload"},"status":429}`))
			return
		}
		if delay := time.Duration(pressure.delay.Load()); delay > 0 {
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-r.Context().Done():
				return
			case <-timer.C:
			}
		}
		forward.ServeHTTP(w, r)
	})
	server := httptest.NewServer(handler)
	pressure.backend.endpoint = server.URL
	t.Cleanup(server.Close)
	return pressure
}

func storeMetrics(t *testing.T, server *candidate) map[string]float64 {
	t.Helper()
	call := httpCall{endpoint: server.metrics, method: http.MethodGet}
	code, body := request(t, call)
	metrics, err := parseMetrics(body)
	if code != http.StatusOK || err != nil {
		t.Fatalf("Store metrics: HTTP %d: %v", code, err)
	}
	found := false
	for name := range metrics {
		if strings.HasPrefix(name, "sink_store_concurrency_limit{") {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("candidate does not export Store admission metrics")
	}
	return metrics
}

func waitStore(t *testing.T, server *candidate, ready func(map[string]float64) bool) map[string]float64 {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		metrics := storeMetrics(t, server)
		if ready(metrics) {
			return metrics
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("Store condition not reached: %v", storeMetrics(t, server))
	return nil
}

func windowChanges(metrics map[string]float64, role, reason string) float64 {
	return metrics[fmt.Sprintf(`sink_store_window_changes_total{reason=%q,role=%q,store="primary"}`, reason, role)]
}

func TestStoreBackpressureKeepsSharedAdmissionBounded(t *testing.T) {
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			index := indexFor(t, store, "-1")
			proxy := proxyBackend(t, store)
			opts := serverOptions{backend: proxy.backend, storeConcurrent: 1, coldStore: true, batchOps: 1, queued: 8}
			server := startCandidate(t, opts)
			seed := put(t, addressFor(t, index, "seed"), `{"counter":1}`, sink.WriteUpsert)
			applied(t, writeAsync(t.Context(), server.client, sink.CompletionWaitUntilApplied, seed), 1)
			baseline := storeMetrics(t, server)
			gate := proxy.hold("/_bulk", "held", 1)
			t.Cleanup(gate.open)
			held := put(t, addressFor(t, index, "held"), `{"counter":1}`, sink.WriteUpsert)
			busy := writeAsync(t.Context(), server.client, sink.CompletionWaitUntilApplied, held)
			gate.wait(t)
			var pending []<-chan writeOutcome
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			for i := range 8 {
				op := put(t, addressFor(t, index, fmt.Sprint("queued-", i)), `{"counter":1}`, sink.WriteUpsert)
				pending = append(pending, writeAsync(ctx, server.client, sink.CompletionWaitUntilApplied, op))
			}
			waitStore(t, server, func(m map[string]float64) bool {
				return metricForStore(m, `sink_batcher_queued_operations{method="Write"}`, "primary") == 8
			})
			for range 32 {
				op := put(t, addressFor(t, index, "excess"), `{"counter":1}`, sink.WriteUpsert)
				result := <-writeAsync(t.Context(), server.client, sink.CompletionWaitUntilApplied, op)
				if result.err != nil || len(result.results) != 1 || result.results[0].Status != sink.WriteFailed || result.results[0].Failure == nil || result.results[0].Failure.Code != sink.FailureResourceExhausted {
					t.Fatalf("full bounded queue: %+v", result)
				}
			}
			count := sink.CountRequest{Command: nativeSearch(index)}
			if _, err := server.client.Count(t.Context(), count); status.Code(err) != codes.ResourceExhausted {
				t.Fatalf("Native bypassed shared budget: %v", err)
			}
			metrics := storeMetrics(t, server)
			if memoryMetricTotal(metrics, "sink_store_executions_in_flight") != 1 || metrics["go_goroutines"] > baseline["go_goroutines"]+80 || metrics["go_memstats_heap_alloc_bytes"] > baseline["go_memstats_heap_alloc_bytes"]+64<<20 {
				t.Fatal("saturation escaped bounded execution/resources")
			}
			server.waitReady(t)
			cancel()
			for _, call := range pending {
				if result := <-call; status.Code(result.err) != codes.Canceled {
					t.Fatalf("queued cancellation: %+v", result)
				}
			}
			waitStore(t, server, func(m map[string]float64) bool {
				return metricForStore(m, `sink_batcher_queued_operations{method="Write"}`, "primary") == 0
			})
			for i := range 8 {
				if proxy.count("/_bulk", fmt.Sprint("queued-", i)) != 0 {
					t.Fatal("canceled work reached backend")
				}
			}
			gate.open()
			applied(t, busy, 1)
			server.waitIdle(t)
			refresh := httpCall{endpoint: store.endpoint, method: http.MethodPost, path: "/" + index + "/_refresh"}
			if code, body := request(t, refresh); code != http.StatusOK {
				t.Fatalf("refresh: %d %s", code, body)
			}
			response, err := server.client.Count(t.Context(), count)
			if err != nil || response.Count != 2 {
				t.Fatalf("recovered Native count: %+v %v", response, err)
			}
		})
	}
}

type storeLoad struct {
	stop     chan struct{}
	once     sync.Once
	workers  sync.WaitGroup
	applied  atomic.Int64
	failures chan error
}

func (load *storeLoad) close() { load.once.Do(func() { close(load.stop) }); load.workers.Wait() }

func startStoreLoad(t *testing.T, servers []*candidate, index string) *storeLoad {
	t.Helper()
	load := &storeLoad{stop: make(chan struct{}), failures: make(chan error, 1)}
	for replica, server := range servers {
		for writer := range 8 {
			operation := put(t, addressFor(t, index, fmt.Sprintf("replica-%d-writer-%d", replica, writer)), `{"counter":1}`, sink.WriteUpsert)
			request := sink.NewWriteRequest(operation).WithCompletionMode(sink.CompletionWaitUntilApplied)
			load.workers.Go(func() {
				for {
					select {
					case <-load.stop:
						return
					default:
					}
					ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
					results, err := server.client.Write(ctx, request)
					cancel()
					if err == nil && len(results) == 1 && results[0].Status == sink.WriteApplied {
						load.applied.Add(1)
						continue
					}
					if status.Code(err) == codes.ResourceExhausted || status.Code(err) == codes.DeadlineExceeded {
						continue
					}
					if err == nil && len(results) == 1 && results[0].Failure != nil && results[0].Failure.Retryable {
						continue
					}
					select {
					case load.failures <- fmt.Errorf("write returned unexpected result: %v %v", results, err):
					default:
					}
					return
				}
			})
		}
	}
	t.Cleanup(load.close)
	return load
}

func TestStoreBackpressureReplicasConvergeAndRecover(t *testing.T) {
	for _, store := range searchBackends(t) {
		for _, replicas := range []int{1, 4} {
			t.Run(fmt.Sprintf("%s/%d", store.driver, replicas), func(t *testing.T) {
				index := indexFor(t, store, "-1")
				proxy := newStorePressureProxy(t, store)
				proxy.delay.Store(int64(20 * time.Millisecond))
				var servers []*candidate
				for range replicas {
					opts := serverOptions{backend: proxy.backend, storeConcurrent: 8, coldStore: true, batchOps: 1}
					servers = append(servers, startCandidate(t, opts))
				}
				load := startStoreLoad(t, servers, index)
				for _, server := range servers {
					waitStore(t, server, func(m map[string]float64) bool { return memoryMetricTotal(m, "sink_store_concurrency_limit") >= 4 })
				}
				before := storeMetrics(t, servers[0])
				proxy.delay.Store(int64(180 * time.Millisecond))
				waitStore(t, servers[0], func(m map[string]float64) bool {
					return windowChanges(m, "engine", "latency") > windowChanges(before, "engine", "latency")
				})
				proxy.reject.Store(true)
				for _, server := range servers {
					waitStore(t, server, func(m map[string]float64) bool {
						return windowChanges(m, "engine", "overload") >= 2 && memoryMetricTotal(m, "sink_store_concurrency_limit") == 0
					})
				}
				failedRequests := proxy.requests.Load()
				time.Sleep(300 * time.Millisecond)
				if proxy.requests.Load()-failedRequests > int64(20*replicas) {
					t.Fatal("cooldown continued flooding backend")
				}
				proxy.delay.Store(int64(20 * time.Millisecond))
				proxy.capacity.Store(2)
				proxy.reject.Store(false)
				beforeApplied := load.applied.Load()
				waitStore(t, servers[0], func(m map[string]float64) bool { return load.applied.Load() >= beforeApplied+40 })
				proxy.capacity.Store(0)
				for _, server := range servers {
					waitStore(t, server, func(m map[string]float64) bool { return memoryMetricTotal(m, "sink_store_concurrency_limit") >= 4 })
				}
				load.close()
				select {
				case err := <-load.failures:
					t.Fatal(err)
				default:
				}
				for replica, server := range servers {
					server.waitIdle(t)
					for writer := range 8 {
						assertCounter(t, server.client, addressFor(t, index, fmt.Sprintf("replica-%d-writer-%d", replica, writer)), 1)
					}
				}
				t.Logf("replicas=%d applied=%d backend_requests=%d injected_overloads=%d", replicas, load.applied.Load(), proxy.requests.Load(), proxy.overloads.Load())
			})
		}
	}
}

func TestStoreBackpressureWorkerRetainsBacklogAndRecovers(t *testing.T) {
	broker := startBroker(t)
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			index := indexFor(t, store, "-1")
			proxy := newStorePressureProxy(t, store)
			proxy.reject.Store(true)
			topic := fmt.Sprintf("sink-backpressure-%d", time.Now().UnixNano())
			opts := serverOptions{backend: store, broker: broker.address, topic: topic, batchOps: 1, coldStore: true}
			publisher := startCandidate(t, opts)
			address := addressFor(t, index, "counter")
			seed := put(t, address, `{"counter":0}`, sink.WriteUpsert)
			applied(t, writeAsync(t.Context(), publisher.client, sink.CompletionWaitUntilApplied, seed), 1)
			operation := merge(t, address, `return function(current,incoming) current.counter=current.counter+1; return current end`)
			for range 32 {
				accepted(t, publisher.client, operation)
			}
			opts.backend = proxy.backend
			opts.worker = true
			opts.storeConcurrent = 1
			worker := startCandidate(t, opts)
			waitStore(t, worker, func(m map[string]float64) bool {
				return windowChanges(m, "worker", "overload") >= 3 && memoryMetricTotal(m, "sink_store_concurrency_limit") == 0
			})
			if committed := broker.committed(t, topic); committed > 0 {
				t.Fatalf("overload advanced offsets: %d", committed)
			}
			if requests := proxy.requests.Load(); requests > 20 {
				t.Fatalf("worker ignored cooldown: %d requests", requests)
			}
			proxy.reject.Store(false)
			broker.waitCommitted(t, topic, 32)
			broker.assertEnd(t, topic+".dlq", 0)
			assertCounter(t, publisher.client, address, 32)
			waitStore(t, worker, func(m map[string]float64) bool { return memoryMetricTotal(m, "sink_store_executions_in_flight") == 0 })
		})
	}
}

//go:build integration

package conformance_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	sink "github.com/batchstream/sink-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Calls use the ordinary SDK with the harness's single-attempt read policy.
// Neither an admission retry nor warmup traffic may hide a cold-start rejection.
func nativeReadAsync(ctx context.Context, client *sink.Client, method string, command sink.Command) <-chan error {
	done := make(chan error, 1)
	go func() {
		var err error
		if method == "Count" {
			request := sink.CountRequest{Command: command}
			response, failure := client.Count(ctx, request)
			err = failure
			if failure == nil && (response.Count != 0 || response.Estimated) {
				err = fmt.Errorf("empty exact Count changed: %+v", response)
			}
		} else {
			request := sink.QueryRequest{Command: command, PageSize: 1}
			response, failure := client.Query(ctx, request)
			err = failure
			if failure == nil && (len(response.Documents) != 0 || response.HasMore) {
				err = fmt.Errorf("empty Query changed: %+v", response)
			}
		}
		done <- err
	}()
	return done
}

func TestReadyStoreAdmitsColdQueryBurst(t *testing.T) {
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			index := indexFor(t, store, "-1")
			opts := serverOptions{backend: store, storeConcurrent: 16, coldStore: true, batchOps: 1, queued: 128, queuedTasks: 128}
			server := startCandidate(t, opts)
			metrics := storeMetrics(t, server)
			if window := memoryMetricTotal(metrics, "sink_store_concurrency_limit"); window < 4 || window > 16 {
				t.Fatalf("ready Engine advertised an uninitialized/single-slot Store: %g", window)
			}
			if memoryMetricTotal(metrics, "sink_store_admissions_total") != 0 {
				t.Fatal("cold-start fixture was silently warmed by business requests")
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			start := make(chan struct{})
			results := make(chan error, 64)
			command := nativeSearch(index)
			// This empty index has no dynamic field mappings; sorting on the
			// ordinary fixture's counter field would be a backend query error.
			command.Payload = []byte(`{"query":{"match_all":{}}}`)
			for i := range 64 {
				method := "Query"
				if i%2 == 0 {
					method = "Count"
				}
				go func() {
					<-start
					results <- <-nativeReadAsync(ctx, server.client, method, command)
				}()
			}
			close(start)
			for range 64 {
				select {
				case err := <-results:
					if err != nil {
						t.Fatalf("healthy cold burst returned an error: %v", err)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("healthy cold burst did not drain")
				}
			}
			server.waitIdle(t)
			metrics = storeMetrics(t, server)
			if metrics[`sink_store_admissions_total{outcome="rejected",role="engine",store="primary"}`] != 0 {
				t.Fatal("healthy cold burst rejected an admission")
			}
		})
	}
}

func TestQueryAndCountAdmissionQueueBoundedAndCancelable(t *testing.T) {
	for _, store := range searchBackends(t) {
		for _, method := range []string{"Query", "Count"} {
			t.Run(store.driver+"/"+method, func(t *testing.T) {
				index := indexFor(t, store, "-1")
				proxy := proxyBackend(t, store)
				opts := serverOptions{backend: proxy.backend, storeConcurrent: 1, batchOps: 1, queued: 3, queuedTasks: 3}
				server := startCandidate(t, opts)
				gate := proxy.hold("/"+index+"/_search", "match_all", 1)
				t.Cleanup(gate.open)
				held := nativeReadAsync(t.Context(), server.client, "Count", nativeSearch(index))
				gate.wait(t)
				var waiting []<-chan error
				var cancels []context.CancelFunc
				for i := range 3 {
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					cancels = append(cancels, cancel)
					command := nativeSearch(index)
					command.Payload = []byte(fmt.Sprintf(`{"query":{"term":{"queue_marker":"queued-%d"}}}`, i))
					waiting = append(waiting, nativeReadAsync(ctx, server.client, method, command))
					waitStore(t, server, func(metrics map[string]float64) bool {
						select {
						case err := <-waiting[i]:
							t.Fatalf("read was rejected instead of waiting for healthy capacity: %v", err)
						default:
						}
						return memoryMetricTotal(metrics, "sink_store_admission_queued_tasks") == float64(i+1)
					})
				}
				for _, result := range waiting {
					select {
					case err := <-result:
						t.Fatalf("bounded read failed before cancellation/release: %v", err)
					default:
					}
				}
				for range 16 {
					result := nativeReadAsync(t.Context(), server.client, method, nativeSearch(index))
					select {
					case err := <-result:
						if status.Code(err) != codes.ResourceExhausted {
							t.Fatalf("full read queue did not reject: %v", err)
						}
					case <-time.After(5 * time.Second):
						t.Fatal("full read queue created an unbounded waiter")
					}
				}
				server.waitReady(t)
				for i := range 2 {
					cancels[i]()
					if err := <-waiting[i]; status.Code(err) != codes.Canceled {
						t.Fatalf("queued read cancellation changed: %v", err)
					}
				}
				waitStore(t, server, func(metrics map[string]float64) bool {
					return memoryMetricTotal(metrics, "sink_store_admission_queued_tasks") == 1
				})
				deadline, stop := context.WithTimeout(t.Context(), 500*time.Millisecond)
				defer stop()
				command := nativeSearch(index)
				command.Payload = []byte(`{"query":{"term":{"queue_marker":"deadline"}}}`)
				expired := nativeReadAsync(deadline, server.client, method, command)
				waitStore(t, server, func(metrics map[string]float64) bool {
					return memoryMetricTotal(metrics, "sink_store_admission_queued_tasks") == 2
				})
				if err := <-expired; status.Code(err) != codes.DeadlineExceeded {
					t.Fatalf("caller deadline was replaced: %v", err)
				}
				waitStore(t, server, func(metrics map[string]float64) bool {
					return memoryMetricTotal(metrics, "sink_store_admission_queued_tasks") == 1
				})
				gate.open()
				for _, result := range []<-chan error{held, waiting[2]} {
					select {
					case err := <-result:
						if err != nil {
							t.Fatalf("capacity recovery failed: %v", err)
						}
					case <-time.After(5 * time.Second):
						t.Fatal("released capacity did not wake queued reads")
					}
				}
				server.waitIdle(t)
				proxy.mu.Lock()
				defer proxy.mu.Unlock()
				forwarded := 0
				for _, request := range proxy.requests {
					if request.Phase != "" {
						continue
					}
					if strings.Contains(request.Body, `"queued-0"`) || strings.Contains(request.Body, `"queued-1"`) || strings.Contains(request.Body, `"deadline"`) {
						t.Fatal("canceled or expired read reached the backend")
					}
					if strings.Contains(request.Body, `"queued-2"`) {
						forwarded++
					}
				}
				if forwarded != 1 {
					t.Fatalf("waiting read replayed or disappeared: %d backend calls", forwarded)
				}
			})
		}
	}
}

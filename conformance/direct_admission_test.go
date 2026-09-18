//go:build integration

package conformance_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/liran/sink-production-suite/internal/testuri"

	sink "github.com/liran/sink-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type countOutcome struct {
	response sink.CountResponse
	err      error
}

func countAsync(ctx context.Context, client *sink.Client, request sink.CountRequest) <-chan countOutcome {
	finished := make(chan countOutcome, 1)
	go func() {
		response, err := client.Count(ctx, request)
		outcome := countOutcome{response: response, err: err}
		finished <- outcome
	}()
	return finished
}

func counted(t *testing.T, finished <-chan countOutcome) {
	t.Helper()
	select {
	case result := <-finished:
		if result.err != nil || result.response.Count != 1 || result.response.Estimated {
			t.Fatalf("exact Count failed: %+v", result)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("admitted Count did not finish after capacity was released")
	}
}

func (c *candidate) waitDirectQueued(t *testing.T, store string, count int) map[string]float64 {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		metrics := c.metricSnapshot(t)
		bytes := metricForStore(metrics, "sink_execution_queued_bytes", store)
		// Prometheus gathers separate gauges independently. Wait for both
		// observations while the held backend keeps this queue stable.
		if metricForStore(metrics, "sink_execution_queued_requests", store) == float64(count) && ((count == 0 && bytes == 0) || (count > 0 && bytes > 0)) {
			return metrics
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("direct admission queue did not reach %d requests for %s", count, store)
	return nil
}

func TestDirectAdmissionQueuesBurstsAndIsolatesStores(t *testing.T) {
	if usesMemoryAdmission(t) {
		testMemoryDirectBursts(t)
		return
	}
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			index := indexFor(t, store, "100ms")
			proxy := proxyBackend(t, store)
			other := independentBackend(t, store)
			otherIndex := indexFor(t, other, "100ms")
			opts := serverOptions{backend: proxy.backend, secondary: &other, capacity: 1}
			server := startCandidate(t, opts)
			operation := put(t, addressFor(t, index, "record"), `{"counter":1}`, sink.WriteUpsert)
			applied(t, writeAsync(t.Context(), server.client, sink.CompletionWaitUntilVisible, operation), 1)
			otherAddress, err := sink.NewRecordAddress(testuri.Resource("secondary", []string{otherIndex}), sink.StringKey("record"))
			if err != nil {
				t.Fatal(err)
			}
			otherOperation := put(t, otherAddress, `{"counter":1}`, sink.WriteUpsert)
			applied(t, writeAsync(t.Context(), server.client, sink.CompletionWaitUntilVisible, otherOperation), 1)
			gate := proxy.hold("/"+index+"/_search", "match_all", 1)
			t.Cleanup(gate.open)
			req := sink.CountRequest{Command: nativeSearch(index)}
			busy := countAsync(t.Context(), server.client, req)
			gate.wait(t)
			before := server.metricSnapshot(t)
			var pending []<-chan countOutcome
			for range 8 {
				pending = append(pending, countAsync(t.Context(), server.client, req))
			}
			deadline := time.Now().Add(time.Second)
			for metricForStore(server.metricSnapshot(t), "sink_execution_queued_requests", "primary") != 8 {
				for _, call := range pending {
					select {
					case result := <-call:
						t.Fatalf("transient Count burst was rejected before capacity was released: %+v", result)
					default:
					}
				}
				if time.Now().After(deadline) {
					t.Fatal("Count burst did not enter the bounded admission queue")
				}
				time.Sleep(time.Millisecond)
			}
			queued := server.metricSnapshot(t)
			bytes := metricForStore(queued, "sink_execution_queued_bytes", "primary")
			if bytes <= 0 || bytes > 1<<20 || queued["sink_in_flight_requests"] != before["sink_in_flight_requests"] || queued["sink_in_flight_bytes"] != before["sink_in_flight_bytes"] {
				t.Fatalf("queued Count requests consumed execution slots or document reservations: before=%v queued=%v", before, queued)
			}
			healthy := sink.CountRequest{Command: nativeSearch(otherIndex)}
			healthy.Command.URI = strings.Replace(healthy.Command.URI, "sink://primary/", "sink://secondary/", 1)
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			counted(t, countAsync(ctx, server.client, healthy))
			cancel()
			gate.open()
			counted(t, busy)
			for _, call := range pending {
				counted(t, call)
			}
			server.waitIdle(t)
		})
	}
}

func TestDirectAdmissionQueueBoundsAndCancellation(t *testing.T) {
	if usesMemoryAdmission(t) {
		testMemoryAdmissionCancellation(t)
		return
	}
	for _, store := range searchBackends(t) {
		for _, bound := range []string{"requests", "bytes"} {
			t.Run(store.driver+"/"+bound, func(t *testing.T) {
				index := indexFor(t, store, "100ms")
				proxy := proxyBackend(t, store)
				queue := &admissionQueueOptions{requests: 4, bytes: 1 << 20, wait: 10 * time.Second}
				switch bound {
				case "requests":
					queue.requests = 1
				case "bytes":
					queue.bytes = 600
				}
				opts := serverOptions{backend: proxy.backend, capacity: 1, admissionQueue: queue}
				server := startCandidate(t, opts)
				operation := put(t, addressFor(t, index, "record"), `{"counter":1}`, sink.WriteUpsert)
				applied(t, writeAsync(t.Context(), server.client, sink.CompletionWaitUntilVisible, operation), 1)
				primary := sink.CountRequest{Command: nativeSearch(index)}
				var busy []<-chan countOutcome
				var gates []*requestGate
				for _, req := range []sink.CountRequest{primary} {
					// A newly installed gate observes only subsequent requests.
					gate := proxy.hold("/"+index+"/_search", "match_all", 1)
					t.Cleanup(gate.open)
					busy = append(busy, countAsync(t.Context(), server.client, req))
					gate.wait(t)
					gates = append(gates, gate)
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				pending := countAsync(ctx, server.client, primary)
				metrics := server.waitDirectQueued(t, "primary", 1)
				bytes := metricForStore(metrics, "sink_execution_queued_bytes", "primary")
				if bytes <= 0 || bytes > float64(queue.bytes) {
					t.Fatalf("queued input exceeded its byte budget: %v", metrics)
				}
				excess := primary
				call, stop := context.WithTimeout(t.Context(), time.Second)
				_, err := server.client.Count(call, excess)
				stop()
				if status.Code(err) != codes.ResourceExhausted {
					t.Fatalf("direct queue did not enforce %s bound: %v", bound, err)
				}
				var following []<-chan countOutcome
				cancel()
				select {
				case result := <-pending:
					if status.Code(result.err) != codes.Canceled {
						t.Fatalf("queued cancellation changed status: %+v", result)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("queued cancellation did not release the caller")
				}
				server.waitDirectQueued(t, "primary", 0)
				replacement := primary
				following = append(following, countAsync(t.Context(), server.client, replacement))
				server.waitDirectQueued(t, "primary", 1)
				for _, gate := range gates {
					gate.open()
				}
				for _, call := range busy {
					counted(t, call)
				}
				for _, call := range following {
					counted(t, call)
				}
				server.waitIdle(t)
			})
		}
	}
}

func TestReturnedPutsUseKnownDocumentReservations(t *testing.T) {
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			index := indexFor(t, store, "-1")
			proxy := proxyBackend(t, store)
			opts := serverOptions{backend: proxy.backend, executionBytes: 128 << 20, batchOps: 32, batchWait: 1000}
			server := startCandidate(t, opts)
			var calls []<-chan writeOutcome
			for i := range 32 {
				address := addressFor(t, index, fmt.Sprintf("returned-%d", i))
				operation := put(t, address, fmt.Sprintf(`{"counter":%d}`, i), sink.WriteUpsert).WithReturnedDocument()
				calls = append(calls, writeAsync(t.Context(), server.client, sink.CompletionWaitUntilApplied, operation))
				if i == 0 {
					server.waitQueued(t, "Write")
				}
			}
			for i, call := range calls {
				results := applied(t, call, 1)
				var document struct {
					Counter int `json:"counter"`
				}
				if err := results[0].Document.Decode(&document); err != nil || document.Counter != i {
					t.Fatalf("returning Put lost its own committed document: %+v %v", document, err)
				}
			}
			proxy.mu.Lock()
			writes := 0
			for _, request := range proxy.requests {
				if request.Phase == "" && strings.HasPrefix(request.Path, "/_bulk") {
					writes++
				}
			}
			proxy.mu.Unlock()
			if writes != 1 {
				t.Fatalf("known returning Put sizes fragmented a collected batch: backend writes=%d", writes)
			}
			server.waitIdle(t)
		})
	}
}

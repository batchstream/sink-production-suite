//go:build integration

package conformance_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	sink "github.com/batchstream/sink-go"
	"github.com/batchstream/sink-production-suite/internal/testuri"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func memoryMetricTotal(metrics map[string]float64, name string) float64 {
	total := 0.0
	for key, value := range metrics {
		if strings.HasPrefix(key, name+"{") || key == name {
			total += value
		}
	}
	return total
}

func testMemoryDirectBursts(t *testing.T) {
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			index := indexFor(t, store, "100ms")
			proxy := proxyBackend(t, store)
			other := independentBackend(t, store)
			otherIndex := indexFor(t, other, "100ms")
			opts := serverOptions{backend: proxy.backend, secondary: &other}
			server := startCandidate(t, opts)
			operation := put(t, addressFor(t, index, "record"), `{"counter":1}`, sink.WriteUpsert)
			applied(t, writeAsync(t.Context(), server.client, sink.CompletionWaitUntilVisible, operation), 1)
			otherAddress, err := sink.NewRecordAddress(testuri.Resource("secondary", []string{otherIndex}), sink.StringKey("record"))
			if err != nil {
				t.Fatal(err)
			}
			operation = put(t, otherAddress, `{"counter":1}`, sink.WriteUpsert)
			applied(t, writeAsync(t.Context(), server.client, sink.CompletionWaitUntilVisible, operation), 1)
			gate := proxy.hold("/"+index+"/_search", "match_all", 1)
			t.Cleanup(gate.open)
			req := sink.CountRequest{Command: nativeSearch(index)}
			busy := countAsync(t.Context(), server.client, req)
			gate.wait(t)
			var burst []<-chan countOutcome
			for range 8 {
				burst = append(burst, countAsync(t.Context(), server.client, req))
			}
			completed, rejected := 0, 0
			for _, call := range burst {
				result := <-call
				if status.Code(result.err) == codes.ResourceExhausted {
					rejected++
				} else if result.err == nil && result.response.Count == 1 && !result.response.Estimated {
					completed++
				} else {
					t.Fatalf("independent Count: %+v %v", result.response, result.err)
				}
			}
			if completed == 0 {
				t.Fatal("independent work failed despite the warmed Store window")
			}
			used := memoryMetricTotal(server.metricSnapshot(t), "sink_memory_used_bytes")
			if used <= 0 {
				t.Fatalf("process memory was not observed: %g", used)
			}
			healthy := sink.CountRequest{Command: nativeSearch(otherIndex)}
			healthy.Command.URI = strings.Replace(healthy.Command.URI, "sink://primary/", "sink://secondary/", 1)
			counted(t, countAsync(t.Context(), server.client, healthy))
			gate.open()
			counted(t, busy)
			for range rejected {
				counted(t, countAsync(t.Context(), server.client, req))
			}
			server.waitIdle(t)
		})
	}
}

func testMemoryAdmissionCancellation(t *testing.T) {
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			index := indexFor(t, store, "100ms")
			proxy := proxyBackend(t, store)
			opts := serverOptions{backend: proxy.backend}
			server := startCandidate(t, opts)
			operation := put(t, addressFor(t, index, "record"), `{"counter":1}`, sink.WriteUpsert)
			applied(t, writeAsync(t.Context(), server.client, sink.CompletionWaitUntilVisible, operation), 1)
			gate := proxy.hold("/"+index+"/_search", "match_all", 1)
			t.Cleanup(gate.open)
			held := sink.CountRequest{Command: nativeSearch(index)}
			held.Command.Payload = append(held.Command.Payload, []byte(strings.Repeat(" ", 23<<10))...)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			busy := countAsync(ctx, server.client, held)
			gate.wait(t)
			ordinary := sink.CountRequest{Command: nativeSearch(index)}
			counted(t, countAsync(t.Context(), server.client, ordinary))
			cancel()
			if result := <-busy; status.Code(result.err) != codes.Canceled {
				t.Fatalf("cancellation: %+v", result)
			}
			server.waitIdle(t)
			gate.open()
			counted(t, countAsync(t.Context(), server.client, ordinary))
			server.waitIdle(t)
		})
	}
}

func testMemoryStoreSaturation(t *testing.T, rounds int) {
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			index := indexFor(t, store, "100ms")
			proxy := proxyBackend(t, store)
			other := independentBackend(t, store)
			otherIndex := indexFor(t, other, "100ms")
			opts := serverOptions{backend: proxy.backend, secondary: &other, maxOps: 32, batchOps: 16, queued: 128}
			server := startCandidate(t, opts)
			healthy, err := sink.NewRecordAddress(testuri.Resource("secondary", []string{otherIndex}), sink.StringKey("healthy"))
			if err != nil {
				t.Fatal(err)
			}
			baseline := server.metricSnapshot(t)
			for round := range rounds {
				gate := proxy.hold("/"+index+"/_search", "match_all", 1)
				t.Cleanup(gate.open)
				held := sink.CountRequest{Command: nativeSearch(index)}
				held.Command.Payload = append(held.Command.Payload, []byte(strings.Repeat(" ", 350<<10))...)
				ctx, cancel := context.WithCancel(t.Context())
				t.Cleanup(cancel)
				busy := countAsync(ctx, server.client, held)
				gate.wait(t)
				payload := `{"value":"` + strings.Repeat("x", 32<<10) + `"}`
				var written []sink.Address
				var calls []<-chan writeOutcome
				for i := range 16 {
					address := addressFor(t, index, fmt.Sprintf("excess-%d-%d", round, i))
					written = append(written, address)
					operation := put(t, address, payload, sink.WriteUpsert)
					calls = append(calls, writeAsync(t.Context(), server.client, sink.CompletionWaitUntilApplied, operation))
				}
				// The slow Store may retain these writes when its window shrinks.
				// Independent Store progress must not depend on draining that backlog.
				for sample := range 8 {
					call, stop := context.WithTimeout(t.Context(), time.Second)
					operation := put(t, healthy, fmt.Sprintf(`{"counter":%d}`, sample), sink.WriteUpsert)
					applied(t, writeAsync(call, server.client, sink.CompletionWaitUntilApplied, operation), 1)
					stop()
				}
				cancel()
				if result := <-busy; status.Code(result.err) != codes.Canceled {
					t.Fatalf("held request cancellation: %+v", result)
				}
				gate.open()
				for _, call := range calls {
					applied(t, call, 1)
				}
				server.waitIdle(t)
				for _, address := range written {
					readRequest := sink.ReadRequest{
						Addresses: []sink.Address{address},
					}
					results, err := server.client.Read(t.Context(), readRequest)
					if err != nil || len(results) != 1 || results[0].Status != sink.ReadFound {
						t.Fatalf("independent write missing: %+v %v", results, err)
					}
				}
				metrics := server.waitIdle(t)
				if metrics["go_goroutines"] > baseline["go_goroutines"]+80 || metrics["go_memstats_heap_alloc_bytes"] > baseline["go_memstats_heap_alloc_bytes"]+64<<20 {
					t.Fatalf("resource growth after capacity saturation: baseline=%v current=%v", baseline, metrics)
				}
			}
		})
	}
}

func testMemoryPublisherStall(t *testing.T) {
	broker := startBroker(t)
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			index := indexFor(t, store, "-1")
			topic := fmt.Sprintf("sink-memory-publisher-%d", time.Now().UnixNano())
			opts := serverOptions{backend: store, broker: broker.address, topic: topic, batchOps: 1}
			server := startCandidate(t, opts)
			broker.docker(t, "pause")
			paused := true
			t.Cleanup(func() {
				if paused {
					broker.docker(t, "unpause")
				}
			})
			var pending []<-chan writeOutcome
			for _, key := range []string{"first", "second"} {
				operation := put(t, addressFor(t, index, key), `{"counter":3}`, sink.WriteUpsert)
				pending = append(pending, writeAsync(t.Context(), server.client, sink.CompletionReturnAfterAccepted, operation))
			}
			deadline := time.Now().Add(5 * time.Second)
			for server.metricSnapshot(t)["sink_in_flight_requests"] < 2 {
				if time.Now().After(deadline) {
					t.Fatal("publishers did not start")
				}
				time.Sleep(10 * time.Millisecond)
			}
			operation := put(t, addressFor(t, index, "healthy"), `{"counter":5}`, sink.WriteUpsert)
			applied(t, writeAsync(t.Context(), server.client, sink.CompletionWaitUntilApplied, operation), 1)
			for _, call := range pending {
				select {
				case result := <-call:
					t.Fatalf("publisher completed before broker acknowledgement: %+v", result)
				default:
				}
			}
			broker.docker(t, "unpause")
			paused = false
			for _, call := range pending {
				select {
				case result := <-call:
					if result.err != nil || len(result.results) != 1 || result.results[0].Status != sink.WriteAccepted {
						t.Fatalf("publisher recovery: %+v", result)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("publisher failed to recover")
				}
			}
			broker.assertEnd(t, topic, 2)
			opts.worker = true
			startCandidate(t, opts)
			broker.waitCommitted(t, topic, 2)
			for _, key := range []string{"first", "second"} {
				assertCounter(t, server.client, addressFor(t, index, key), 3)
			}
			server.waitIdle(t)
		})
	}
}

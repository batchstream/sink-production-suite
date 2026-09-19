//go:build integration

package conformance_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
	"github.com/liran/sink-production-suite/internal/testuri"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Negotiate the candidate's configuration capability so released servers still
// run their count/queue contract and newer servers run real capacity scenarios.
func usesMemoryAdmission(t *testing.T) bool {
	t.Helper()
	binary := os.Getenv("SINK_SERVER_BINARY")
	if binary == "" {
		t.Fatal("SINK_SERVER_BINARY is required")
	}
	config := "mode: gateway\nmemory: {burst_percent: 10}\nforwarding:\n  routes:\n    - store: primary\n      target: 127.0.0.1:8080\n      tls: {insecure: true}\n"
	filename := filepath.Join(t.TempDir(), "memory-capability.yaml")
	if err := os.WriteFile(filename, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(t.Context(), binary, "config", "check", "--config", filename)
	output, err := command.CombinedOutput()
	if err == nil {
		return true
	}
	if strings.Contains(string(output), "field memory not found") {
		return false
	}
	t.Fatalf("cannot determine memory configuration support: %s: %v", output, err)
	return false
}

func memoryMetricTotal(metrics map[string]float64, name string) float64 {
	total := 0.0
	for key, value := range metrics {
		if strings.HasPrefix(key, name+"{") || key == name {
			total += value
		}
	}
	return total
}

func memoryOccupancyMetric(name string) bool {
	for _, prefix := range []string{"sink_memory_used_bytes", "sink_memory_opaque_reserved_bytes", "sink_memory_waiting_bytes", "sink_memory_waiting_requests", "sink_memory_burst_borrowers"} {
		if strings.HasPrefix(name, prefix+"{") || name == prefix {
			return true
		}
	}
	return false
}

func testMemoryDirectBursts(t *testing.T) {
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			index := indexFor(t, store, "100ms")
			proxy := proxyBackend(t, store)
			other := independentBackend(t, store)
			otherIndex := indexFor(t, other, "100ms")
			opts := serverOptions{backend: proxy.backend, secondary: &other, capacity: 1, memoryBytes: 4 << 20}
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
			for _, call := range burst {
				counted(t, call)
			}
			used := memoryMetricTotal(server.metricSnapshot(t), "sink_memory_used_bytes")
			if used <= 0 || used >= 1<<20 {
				t.Fatalf("small Count charged a maximum response instead of owned bytes: %g", used)
			}
			healthy := sink.CountRequest{Command: nativeSearch(otherIndex)}
			healthy.Command.URI = strings.Replace(healthy.Command.URI, "sink://primary/", "sink://secondary/", 1)
			counted(t, countAsync(t.Context(), server.client, healthy))
			gate.open()
			counted(t, busy)
			server.waitIdle(t)
		})
	}
}

func testMemoryAdmissionCancellation(t *testing.T) {
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			index := indexFor(t, store, "100ms")
			proxy := proxyBackend(t, store)
			opts := serverOptions{backend: proxy.backend, capacity: 1, memoryBytes: 320 << 10}
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
			_, err := server.client.Count(t.Context(), ordinary)
			if status.Code(err) != codes.ResourceExhausted {
				t.Fatalf("occupied ordinary capacity accepted excess input: %v", err)
			}
			metrics := server.metricSnapshot(t)
			if memoryMetricTotal(metrics, "sink_memory_rejected_total") == 0 || memoryMetricTotal(metrics, "sink_memory_burst_borrowers") != 0 {
				t.Fatalf("admission did not reject without borrowing completion reserve: %v", metrics)
			}
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
			opts := serverOptions{backend: proxy.backend, secondary: &other, capacity: 2, memoryBytes: 1 << 20, maxOps: 8, batchOps: 1, queued: 8}
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
				var rejected []sink.Address
				var calls []<-chan writeOutcome
				for i := range 66 {
					address := addressFor(t, index, fmt.Sprintf("excess-%d-%d", round, i))
					rejected = append(rejected, address)
					operation := put(t, address, payload, sink.WriteUpsert)
					calls = append(calls, writeAsync(t.Context(), server.client, sink.CompletionWaitUntilApplied, operation))
				}
				for _, call := range calls {
					select {
					case result := <-call:
						failed := result.err == nil && len(result.results) == 1 && result.results[0].Failure != nil && result.results[0].Failure.Code == sink.FailureResourceExhausted
						if status.Code(result.err) != codes.ResourceExhausted && !failed {
							t.Fatalf("excess work escaped capacity admission: %+v", result)
						}
					case <-time.After(5 * time.Second):
						t.Fatal("excess work did not fail promptly")
					}
				}
				metrics := server.metricSnapshot(t)
				if memoryMetricTotal(metrics, "sink_memory_used_bytes") > 1<<20 || memoryMetricTotal(metrics, "sink_memory_rejected_total") == 0 {
					t.Fatalf("capacity bound or overload telemetry failed: %v", metrics)
				}
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
				server.waitIdle(t)
				gate.open()
				for _, address := range rejected {
					assertAbsent(t, server.client, address)
				}
				metrics = server.waitIdle(t)
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
			opts := serverOptions{backend: store, broker: broker.address, topic: topic, capacity: 1, batchOps: 1, memoryBytes: 4 << 20}
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
			for memoryMetricTotal(server.metricSnapshot(t), "sink_memory_used_bytes") == 0 {
				if time.Now().After(deadline) {
					t.Fatal("publishers did not retain their input")
				}
				time.Sleep(10 * time.Millisecond)
			}
			operation := put(t, addressFor(t, index, "healthy"), `{"counter":5}`, sink.WriteUpsert)
			applied(t, writeAsync(t.Context(), server.client, sink.CompletionWaitUntilApplied, operation), 1)
			for _, call := range pending {
				select {
				case result := <-call:
					t.Fatalf("publisher returned without broker acknowledgement or obeyed obsolete count cap: %+v", result)
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

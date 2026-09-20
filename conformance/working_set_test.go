//go:build integration

package conformance_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
)

func TestSynchronousMergesProcessCollectedWorkingSets(t *testing.T) {
	for _, store := range searchBackends(t) {
		for _, scenario := range []string{"snapshots", "outputs"} {
			t.Run(store.driver+"/"+scenario, func(t *testing.T) {
				index := indexFor(t, store, "-1")
				proxy := proxyBackend(t, store)
				opts := serverOptions{backend: proxy.backend, batchOps: 8, batchWait: 1000}
				server := startCandidate(t, opts)
				padding := strings.Repeat("x", 700)
				initialPadding := ""
				if scenario == "snapshots" {
					initialPadding = padding
				}
				var seed []sink.WriteOperation
				var addresses []sink.Address
				for record := range 8 {
					address := addressFor(t, index, fmt.Sprintf("record-%d", record))
					addresses = append(addresses, address)
					seed = append(seed, put(t, address, fmt.Sprintf(`{"counter":0,"padding":%q}`, initialPadding), sink.WriteUpsert))
				}
				applied(t, writeAsync(t.Context(), server.client, sink.CompletionWaitUntilApplied, seed...), 8)
				program := fmt.Sprintf(`return function(current, incoming) return {counter=current.counter+incoming.counter,padding=%q} end`, padding)
				var calls []<-chan writeOutcome
				for _, address := range addresses {
					operation := merge(t, address, program).WithReturnedDocument()
					calls = append(calls, writeAsync(t.Context(), server.client, sink.CompletionWaitUntilApplied, operation))
				}
				for _, call := range calls {
					results := applied(t, call, 1)
					var value struct {
						Counter int    `json:"counter"`
						Padding string `json:"padding"`
					}
					if err := results[0].Document.Decode(&value); err != nil || value.Counter != 1 || value.Padding != padding {
						t.Fatalf("batching lost a caller's committed document: %+v, %v", value, err)
					}
				}
				proxy.mu.Lock()
				reads, writes := 0, 0
				for _, request := range proxy.requests {
					if request.Phase != "" {
						continue
					}
					if strings.HasSuffix(request.Path, "/_mget") {
						reads++
					}
					if strings.HasPrefix(request.Path, "/_bulk") {
						writes++
					}
				}
				proxy.mu.Unlock()
				if writes != 2 || reads != 1 {
					t.Fatalf("collected batch was split into extra backend requests: reads=%d writes=%d", reads, writes)
				}
				// Require one seed batch and one coalesced eight-caller merge batch.
				// Eight independent executions would not exercise coalescing.
				deadline := time.Now().Add(2 * time.Second)
				for {
					call := httpCall{endpoint: server.metrics, method: http.MethodGet}
					_, body := request(t, call)
					metrics, err := parseMetrics(body)
					if err != nil {
						t.Fatal(err)
					}
					if metricForStore(metrics, `sink_batcher_operations_count{method="Write"}`, "primary") == 2 {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("batching oracle requires all eight merge callers in one batch")
					}
					time.Sleep(10 * time.Millisecond)
				}
				// Verify every independent caller persisted its result exactly once.
				for _, address := range addresses {
					assertCounter(t, server.client, address, 1)
				}
				server.waitIdle(t)
			})
		}
	}
}

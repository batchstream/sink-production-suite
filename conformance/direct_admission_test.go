//go:build integration

package conformance_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
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

func TestDirectBurstsProgressAcrossStores(t *testing.T) { testMemoryDirectBursts(t) }

func TestDirectCancellationReleasesBackend(t *testing.T) { testMemoryAdmissionCancellation(t) }

func TestReturnedPutsKeepCollectedBatch(t *testing.T) {
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			index := indexFor(t, store, "-1")
			proxy := proxyBackend(t, store)
			opts := serverOptions{backend: proxy.backend, batchOps: 32, batchWait: 1000}
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

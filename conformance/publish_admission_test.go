//go:build integration

package conformance_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
)

func TestPublishingSurvivesSynchronousSaturation(t *testing.T) {
	broker := startBroker(t)
	for _, store := range searchBackends(t) {
		for _, batchOps := range []int{1000, 1} {
			t.Run(fmt.Sprintf("%s/batch-ops=%d", store.driver, batchOps), func(t *testing.T) {
				index := indexFor(t, store, "-1")
				proxy := proxyBackend(t, store)
				topic := fmt.Sprintf("sink-publish-isolation-%d", time.Now().UnixNano())
				opts := serverOptions{backend: proxy.backend, broker: broker.address, topic: topic, batchOps: batchOps}
				server := startCandidate(t, opts)
				blocked := addressFor(t, index, "blocked")
				gate := proxy.hold("/_bulk", "blocked", 1)
				t.Cleanup(gate.open)
				synchronous := writeAsync(t.Context(), server.client, sink.CompletionWaitUntilApplied, put(t, blocked, `{"counter":1}`, sink.WriteUpsert))
				gate.wait(t)
				// No worker exists yet. Acceptance must come from real Kafka while
				// the same store's synchronous execution slot remains occupied.
				kept, removed := addressFor(t, index, "kept"), addressFor(t, index, "removed")
				for _, address := range []sink.Address{kept, removed} {
					ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
					operation := put(t, address, `{"counter":7}`, sink.WriteUpsert)
					writeRequest := sink.WriteRequest{
						CompletionMode: sink.CompletionReturnAfterAccepted,
						Operations:     []sink.WriteOperation{operation},
					}
					results, err := server.client.Write(ctx, writeRequest)
					cancel()
					if err != nil || len(results) != 1 || results[0].Status != sink.WriteAccepted || results[0].Failure != nil {
						t.Fatalf("synchronous saturation blocked Kafka publishing: %+v, %v", results, err)
					}
				}
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				deleteRequest := sink.DeleteRequest{
					CompletionMode: sink.CompletionReturnAfterAccepted,
					Addresses:      []sink.Address{removed},
				}
				deleted, err := server.client.Delete(ctx, deleteRequest)
				cancel()
				if err != nil || len(deleted) != 1 || deleted[0].Status != sink.DeleteAccepted || deleted[0].Failure != nil {
					t.Fatalf("synchronous saturation blocked Kafka delete: %+v, %v", deleted, err)
				}
				broker.assertEnd(t, topic, 3)
				select {
				case result := <-synchronous:
					t.Fatalf("storage gate released before publishing completed: %+v", result)
				default:
				}
				gate.open()
				applied(t, synchronous, 1)
				assertAbsent(t, server.client, kept)
				assertAbsent(t, server.client, removed)
				opts.worker, opts.backend = true, store
				startCandidate(t, opts)
				broker.waitCommitted(t, topic, 3)
				assertCounter(t, server.client, kept, 7)
				assertAbsent(t, server.client, removed)
				assertCounter(t, server.client, blocked, 1)
				broker.assertEnd(t, topic+".dlq", 0)
				server.waitIdle(t)
			})
		}
	}
}

func TestSynchronousWritesSurvivePublisherSaturation(t *testing.T) { testMemoryPublisherStall(t) }

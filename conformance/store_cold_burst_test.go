//go:build integration

package conformance_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	sink "github.com/batchstream/sink-go"
)

// No warmup, application retry, or injected success: every request crosses the
// candidate's public transport and reaches a disposable search backend.
func TestStoreColdStartQueuesMixedBurstWithoutRejection(t *testing.T) {
	for _, store := range searchBackends(t) {
		for _, replicas := range []int{1, 4} {
			t.Run(fmt.Sprintf("%s/%d", store.driver, replicas), func(t *testing.T) {
				index := indexFor(t, store, "100ms")
				// nativeSearch sorts on counter. Define its schema before the first
				// concurrent write, without sending warmup traffic through Sink.
				mapping := httpCall{endpoint: store.endpoint, method: http.MethodPut, path: "/" + index + "/_mapping", body: []byte(`{"properties":{"counter":{"type":"long"}}}`)}
				if code, body := request(t, mapping); code != http.StatusOK {
					t.Fatalf("create cold-burst mapping: HTTP %d: %s", code, body)
				}
				proxy := newStorePressureProxy(t, store)
				proxy.delay.Store(int64(20 * time.Millisecond))
				var servers []*candidate
				for range replicas {
					opts := serverOptions{backend: proxy.backend, coldStore: true, storeConcurrent: 8, batchOps: 8}
					servers = append(servers, startCandidate(t, opts))
				}
				ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
				defer cancel()
				start := make(chan struct{})
				results := make(chan error, replicas*64)
				for replica, server := range servers {
					for i := range 64 {
						address := addressFor(t, index, fmt.Sprintf("burst-%d-%d", replica, i))
						operation := put(t, address, fmt.Sprintf(`{"counter":%d}`, replica*64+i+1), sink.WriteUpsert)
						go func() {
							<-start
							command := nativeSearch(index)
							var err error
							switch i % 6 {
							case 0:
								result := <-writeAsync(ctx, server.client, sink.CompletionWaitUntilApplied, operation)
								err = result.err
								if err == nil && (len(result.results) != 1 || result.results[0].Status != sink.WriteApplied) {
									err = fmt.Errorf("cold write failed: %+v", result)
								}
							case 1:
								request := sink.CountRequest{Command: command}
								_, err = server.client.Count(ctx, request)
							case 2:
								request := sink.ExecuteRequest{Command: command}
								response, executeErr := server.client.Execute(ctx, request)
								err = executeErr
								if err == nil && !response.Success {
									err = fmt.Errorf("cold Execute failed: %+v", response)
								}
							case 3:
								request := sink.QueryRequest{Command: command, PageSize: 1}
								_, err = server.client.Query(ctx, request)
							case 4:
								request := sink.ScanRequest{Command: command, BatchSize: 1}
								_, err = server.client.Scan(ctx, request)
							case 5:
								request := sink.ReadRequest{Addresses: []sink.Address{address}}
								response, readErr := server.client.Read(ctx, request)
								err = readErr
								if err == nil && (len(response) != 1 || response[0].Status != sink.ReadNotFound) {
									err = fmt.Errorf("cold Read failed: %+v", response)
								}
							}
							results <- err
						}()
					}
				}
				close(start)
				for range replicas * 64 {
					if err := <-results; err != nil {
						t.Fatalf("healthy cold burst must queue instead of failing: %v", err)
					}
				}
				for replica, server := range servers {
					server.waitIdle(t)
					metrics := storeMetrics(t, server)
					for name, value := range metrics {
						if strings.HasPrefix(name, "sink_store_admissions_total{") && strings.Contains(name, `outcome="rejected"`) && value != 0 {
							t.Fatalf("cold burst was masked by a client retry: %s=%v", name, value)
						}
					}
					if memoryMetricTotal(metrics, "sink_store_buffered_bytes") != 0 || memoryMetricTotal(metrics, "sink_store_admission_queued_tasks") != 0 {
						t.Fatal("drained cold burst retained queue capacity")
					}
					for i := 0; i < 64; i += 6 {
						address := addressFor(t, index, fmt.Sprintf("burst-%d-%d", replica, i))
						assertCounter(t, server.client, address, replica*64+i+1)
					}
				}
			})
		}
	}
}

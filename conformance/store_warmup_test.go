//go:build integration

package conformance_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	sink "github.com/batchstream/sink-go"
	"github.com/batchstream/sink-production-suite/internal/testuri"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Existing fault schedules require spare execution slots before holding a call.
// Establish that capacity with real public Count traffic on a disposable index,
// never by altering controller state or disabling admission. Backpressure tests
// opt into coldStore to exercise untrained processes and startup staggering.
func warmStoreTraffic(t *testing.T, server *candidate, opts serverOptions) {
	t.Helper()
	index := indexFor(t, opts.backend, "-1")
	store := opts.store
	if store == "" {
		store = "primary"
	}
	command := sink.Command{URI: testuri.Resource(store, []string{index}), Method: "POST", Path: "/_search", ContentType: "application/json", Payload: []byte(`{"query":{"match_all":{}}}`)}
	request := sink.CountRequest{Command: command}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	stop := make(chan struct{})
	var workers sync.WaitGroup
	defer func() { close(stop); workers.Wait() }()
	failures := make(chan error, 1)
	// Match the required window; excess callers only load the RPC rejection path.
	target := min(4, defaultInt(opts.storeConcurrent, 64))
	for range target {
		workers.Go(func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-stop:
					return
				default:
				}
				response, err := server.client.Count(ctx, request)
				if err != nil && status.Code(err) != codes.ResourceExhausted && status.Code(err) != codes.DeadlineExceeded && status.Code(err) != codes.Canceled {
					select {
					case failures <- err:
					default:
					}
					return
				}
				if err == nil && response.Count != 0 {
					select {
					case failures <- fmt.Errorf("warmup index is not empty: %d", response.Count):
					default:
					}
					return
				}
				if err != nil {
					time.Sleep(20 * time.Millisecond)
				} else {
					time.Sleep(2 * time.Millisecond)
				}
			}
		})
	}
	for {
		select {
		case err := <-failures:
			t.Fatalf("steady-state fixture traffic: %v", err)
		default:
		}
		metrics := storeMetrics(t, server)
		if memoryMetricTotal(metrics, "sink_store_concurrency_limit") >= float64(target) {
			return
		}
		if ctx.Err() != nil {
			t.Fatalf("real Count traffic did not establish the steady-state fixture window: limit=%g increases=%g latency_cuts=%g overloads=%g",
				memoryMetricTotal(metrics, "sink_store_concurrency_limit"),
				windowChanges(metrics, "engine", "increase"), windowChanges(metrics, "engine", "latency"), windowChanges(metrics, "engine", "overload"))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

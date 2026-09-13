//go:build integration

package conformance_test

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/liran/sink-production-suite/internal/historycheck"
)

func TestReplaceConflictExhaustionIsRetryable(t *testing.T) {
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			index := indexFor(t, store, "-1")
			call := httpCall{endpoint: store.endpoint, method: http.MethodPut, path: "/" + index + "/_doc/replace", body: []byte(`{"counter":0}`)}
			code, body := request(t, call)
			if code != http.StatusCreated {
				t.Fatalf("seed replacement: %d %s", code, body)
			}
			proxy := proxyBackend(t, store)
			opts := serverOptions{backend: proxy.backend, batchOps: 1}
			server := startCandidate(t, opts)
			gates := make([]*requestGate, 3)
			for index := range gates {
				gate := proxy.hold("/_bulk", "replace", index+1)
				t.Cleanup(gate.open)
				gates[index] = gate
			}
			address := addressFor(t, index, "replace")
			entry := historycheck.Entry{Kind: historycheck.Replace, Input: 100}
			done := make(chan error, 1)
			go func() {
				result, err := runHistoryCall(t.Context(), server.client, address, entry)
				if err == nil && (!result.Conflict || result.Applied) {
					err = fmt.Errorf("exhausted Replace was not an unresolved conflict: %+v", result)
				}
				done <- err
			}()
			for attempt, gate := range gates {
				gate.wait(t)
				// A real competing writer invalidates every snapshot while the
				// addressed record continues to exist throughout the operation.
				call.body = []byte(fmt.Sprintf(`{"counter":%d}`, attempt+1))
				code, body = request(t, call)
				if code != http.StatusOK {
					t.Fatalf("competing mutation: %d %s", code, body)
				}
				gate.open()
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("retryable conflict contract: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Replace conflict exhaustion did not terminate")
			}
			if proxy.count("/_mget", "replace") != 3 || proxy.count("/_bulk", "replace") != 3 {
				t.Fatal("Replace did not exhaust exactly three conditional attempts")
			}
			assertCounter(t, server.client, address, 3)
		})
	}
}

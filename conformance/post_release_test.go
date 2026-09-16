//go:build integration

package conformance_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLuaBudgetFailuresPreserveStateAndSiblings(t *testing.T) {
	cases := []struct{ name, body string }{
		{"insert", `table.insert(current.values, 1, "x")`},
		{"remove", `table.remove(current.values, 1)`},
		{"sort", `table.sort(current.values)`},
		{"unpack", `table.unpack(current.values)`},
		{"move", `table.move(current.values, 1, 400, 1, {})`},
		{"concat", `table.concat(current.values)`},
		{"pack", `string.pack("` + strings.Repeat(" ", 400) + `")`},
		{"packsize", `string.packsize("` + strings.Repeat(" ", 400) + `")`},
		{"string-unpack", `string.unpack("` + strings.Repeat(" ", 400) + `", "")`},
		{"pattern", `for i=1,8 do string.find(current.text, "^a*") end`},
		{"unicode", `for i=1,8 do utf8.upper(current.text) end`},
		{"cumulative-helper", `for i=1,8 do sink.v1.array.keep_tail(current.short, 40) end`},
	}
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			index := indexFor(t, store, "100ms")
			proxy := proxyBackend(t, store)
			opts := serverOptions{backend: proxy.backend, luaInstructions: 200}
			server := startCandidate(t, opts)
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					address := addressFor(t, index, tc.name)
					raw := `{"counter":0,"values":[` + strings.Repeat(`"",`, 399) + `""],"short":[` + strings.Repeat(`"",`, 39) + `""],"text":"` + strings.Repeat("a", 32768) + `"}`
					seed := put(t, address, raw, sink.WriteCreate)
					applied(t, writeAsync(t.Context(), server.client, sink.CompletionWaitUntilApplied, seed), 1)
					before, err := server.client.Read(t.Context(), address)
					if err != nil || len(before) != 1 || before[0].Status != sink.ReadFound {
						t.Fatalf("seed read: %+v, %v", before, err)
					}
					source := `return function(current, incoming) current.counter=99; ` + tc.body + `; return current end`
					limited := merge(t, address, source)
					healthyAddress := addressFor(t, index, tc.name+"-healthy")
					healthy := put(t, healthyAddress, `{"counter":7}`, sink.WriteCreate)
					ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
					defer cancel()
					results, err := server.client.Write(ctx, sink.CompletionWaitUntilApplied, limited, healthy)
					if err != nil || len(results) != 2 {
						t.Fatalf("budget failure poisoned RPC: %+v, %v", results, err)
					}
					failed := results[0]
					if failed.OperationIndex != 0 || failed.Status != sink.WriteFailed || failed.Failure == nil || failed.Failure.Code != sink.FailureResourceExhausted {
						t.Fatalf("Lua native work escaped its instruction budget: %+v", failed)
					}
					if results[1].OperationIndex != 1 || results[1].Status != sink.WriteApplied || results[1].Failure != nil {
						t.Fatalf("Lua budget failure suppressed healthy sibling: %+v", results[1])
					}
					after, err := server.client.Read(ctx, address)
					if err != nil || len(after) != 1 || after[0].Status != sink.ReadFound || !bytes.Equal(before[0].Document.Payload(), after[0].Document.Payload()) || !bytes.Equal(before[0].Revision.Bytes(), after[0].Revision.Bytes()) {
						t.Fatalf("failed Lua merge changed persisted document or revision: %+v, %v", after, err)
					}
					assertCounter(t, server.client, healthyAddress, 7)
					following := merge(t, address, increment)
					applied(t, writeAsync(ctx, server.client, sink.CompletionWaitUntilApplied, following), 1)
					assertCounter(t, server.client, address, 1)
				})
			}
		})
	}
}

func TestManagedQueriesCannotMutateDocuments(t *testing.T) {
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			index := indexFor(t, store, "100ms")
			proxy := proxyBackend(t, store)
			opts := serverOptions{backend: proxy.backend}
			server := startCandidate(t, opts)
			address := addressFor(t, index, "_search")
			seed := put(t, address, `{"counter":0}`, sink.WriteCreate)
			applied(t, writeAsync(t.Context(), server.client, sink.CompletionWaitUntilApplied, seed), 1)
			call := httpCall{endpoint: store.endpoint, method: http.MethodGet, path: "/" + index + "/_doc/_search"}
			code, before := request(t, call)
			if code != http.StatusOK {
				t.Fatalf("seed: HTTP %d %s", code, before)
			}
			command := nativeSearch(index)
			command.Path = "/_doc/_search"
			for _, method := range []string{"Query", "Count", "Scan"} {
				t.Run(method, func(t *testing.T) {
					count, err := managedCall(t.Context(), server.client, command, method)
					proxy.mu.Lock()
					forwarded := 0
					for _, observed := range proxy.requests {
						if observed.Phase == "" && strings.Split(observed.Path, "?")[0] == call.path {
							forwarded++
						}
					}
					proxy.mu.Unlock()
					if forwarded != 0 {
						t.Errorf("managed query forwarded mutation endpoint to storage: %d requests", forwarded)
					}
					if status.Code(err) != codes.InvalidArgument || count != 0 {
						t.Errorf("managed query accepted mutation endpoint: %v", err)
					}
					code, after := request(t, call)
					if code != http.StatusOK || !bytes.Equal(before, after) {
						t.Fatalf("managed query mutated the search-named record: HTTP %d %s", code, after)
					}
				})
			}
		})
	}
}

func managedCall(ctx context.Context, client *sink.Client, command sink.Command, method string) (int, error) {
	switch method {
	case "Query":
		req := sink.QueryRequest{Command: command, PageSize: 1}
		result, err := client.Query(ctx, req)
		return len(result.Documents), err
	case "Count":
		req := sink.CountRequest{Command: command}
		result, err := client.Count(ctx, req)
		return int(result.Count), err
	case "Scan":
		req := sink.ScanRequest{Command: command, BatchSize: 1}
		result, err := client.Scan(ctx, req)
		return len(result.Documents), err
	default:
		return 0, fmt.Errorf("unknown managed method %q", method)
	}
}

func TestManagedQueryEndpointRecovery(t *testing.T) {
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			index := indexFor(t, store, "100ms")
			seed := httpCall{endpoint: store.endpoint, method: http.MethodPut, path: "/" + index + "/_doc/seed?refresh=true", body: []byte(`{"counter":1}`)}
			code, body := request(t, seed)
			if code != http.StatusCreated {
				t.Fatalf("seed recovery document: HTTP %d %s", code, body)
			}
			target, err := url.Parse(store.endpoint)
			if err != nil {
				t.Fatal(err)
			}
			for _, method := range []string{"Query", "Count", "Scan", "Execute"} {
				t.Run(method, func(t *testing.T) {
					var failed, recovered atomic.Int32
					var failedEndpoint, recoveredEndpoint atomic.Value
					forward := httputil.NewSingleHostReverseProxy(target)
					// Fail the first business request regardless of which endpoint health
					// checks leave next in the client's round-robin order.
					handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if r.URL.Path == "/"+index+"/_search" {
							if failed.CompareAndSwap(0, 1) {
								failedEndpoint.Store(r.Host)
								w.WriteHeader(http.StatusServiceUnavailable)
								return
							}
							recovered.Add(1)
							recoveredEndpoint.Store(r.Host)
						}
						forward.ServeHTTP(w, r)
					})
					first := httptest.NewServer(handler)
					t.Cleanup(first.Close)
					second := httptest.NewServer(handler)
					t.Cleanup(second.Close)
					opts := serverOptions{backend: store, endpoints: []string{first.URL, second.URL}}
					server := startCandidate(t, opts)
					command := nativeSearch(index)
					if method == "Execute" {
						req := sink.ExecuteRequest{Command: command}
						result, err := server.client.Execute(t.Context(), req)
						if err == nil || result.Success || result.StatusCode != http.StatusServiceUnavailable || failed.Load() != 1 || recovered.Load() != 0 {
							t.Fatalf("Execute replayed a native operation or lost its failure: %+v, %v, calls=%d/%d", result, err, failed.Load(), recovered.Load())
						}
						return
					}
					count, err := managedCall(t.Context(), server.client, command, method)
					if err != nil || count != 1 || failed.Load() != 1 || recovered.Load() != 1 || failedEndpoint.Load() == recoveredEndpoint.Load() {
						t.Fatalf("managed read failed to recover through healthy endpoint: %v, count=%d calls=%d/%d", err, count, failed.Load(), recovered.Load())
					}
				})
			}
		})
	}
}

func TestQueryLookaheadDoesNotConsumeDocumentBudget(t *testing.T) {
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			index := indexFor(t, store, "100ms")
			opts := serverOptions{backend: store, readBytes: 2048}
			server := startCandidate(t, opts)
			first := put(t, addressFor(t, index, "first"), `{"counter":0}`, sink.WriteCreate)
			second := put(t, addressFor(t, index, "second"), `{"counter":1,"padding":"`+strings.Repeat("x", 4096)+`"}`, sink.WriteCreate)
			applied(t, writeAsync(t.Context(), server.client, sink.CompletionWaitUntilVisible, first, second), 2)
			req := sink.QueryRequest{Command: nativeSearch(index), PageSize: 1}
			result, err := server.client.Query(t.Context(), req)
			if err != nil || len(result.Documents) != 1 || !result.HasMore || !bytes.Contains(result.Documents[0].Payload(), []byte(`"_id":"first"`)) {
				t.Fatalf("unreturned lookahead consumed the page budget: %+v, %v", result, err)
			}
		})
	}
}

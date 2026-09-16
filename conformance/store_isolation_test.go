//go:build integration

package conformance_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/liran/sink-production-suite/internal/testuri"

	sink "github.com/liran/sink-go"
)

func isolatedConfig(contents string, opts serverOptions, address, metrics string) string {
	if opts.role == "gateway" {
		return fmt.Sprintf("mode: gateway\ngrpc: {address: %q}\nprometheus: {enabled: true, address: %q}\ngateway:\n  routes_file: %q\n  reload_interval: 100ms\n  max_requests: 1024\n  max_requests_per_store: 128\nservice:\n  request:\n    timeout: %ds\n    max_operations: %d\n    max_read_bytes: %d\n", address, metrics, opts.routes, defaultInt(opts.requestTimeout, 2), defaultInt(opts.maxOps, 1000), defaultInt(opts.readBytes, 32<<20))
	}
	if opts.store != "" {
		contents = strings.Replace(contents, "storage:\n  name: primary", "storage:\n  name: "+opts.store, 1)
	}
	return contents
}

// Cross-Store scenarios use separate Engine processes and separate real database targets.
func startStoreTopology(t *testing.T, opts serverOptions) *candidate {
	t.Helper()
	primaryOpts := opts
	primaryOpts.secondary = nil
	primaryOpts.role, primaryOpts.store = "engine", "primary"
	primary := startCandidate(t, primaryOpts)
	secondaryOpts := opts
	secondaryOpts.secondary = nil
	secondaryOpts.backend = *opts.secondary
	secondaryOpts.role, secondaryOpts.store = "engine", "secondary"
	secondary := startCandidate(t, secondaryOpts)
	gatewayOpts := serverOptions{role: "gateway", routes: isolatedRoutes(t, primary, secondary), maxOps: opts.maxOps, readBytes: opts.readBytes, requestTimeout: defaultInt(opts.requestTimeout, 20)}
	gateway := startCandidate(t, gatewayOpts)
	// Queue and execution assertions deliberately observe the saturated Engine.
	gateway.metrics = primary.metrics
	return gateway
}

func independentBackend(t *testing.T, current backend) backend {
	t.Helper()
	for _, available := range searchBackends(t) {
		if available.driver != current.driver {
			return available
		}
	}
	t.Fatal("cross-Store scenarios require an independent database target")
	var empty backend
	return empty
}

func isolatedRoutes(t *testing.T, primary, secondary *candidate) string {
	t.Helper()
	file := filepath.Join(t.TempDir(), "routes.yaml")
	data := fmt.Sprintf("routes:\n  - store: primary\n    target: %s\n    tls: {insecure: true}\n  - store: secondary\n    target: %s\n    tls: {insecure: true}\n", primary.address, secondary.address)
	if err := os.WriteFile(file, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	return file
}
func TestStoreIsolatedGatewayPublicContract(t *testing.T) {
	backends := searchBackends(t)
	firstIndex := indexFor(t, backends[0], "100ms")
	secondIndex := indexFor(t, backends[1], "100ms")
	firstOpts := serverOptions{role: "engine", store: "primary", backend: backends[0]}
	secondOpts := serverOptions{role: "engine", store: "secondary", backend: backends[1]}
	first := startCandidate(t, firstOpts)
	second := startCandidate(t, secondOpts)
	gatewayOpts := serverOptions{role: "gateway", routes: isolatedRoutes(t, first, second)}
	gateway := startCandidate(t, gatewayOpts)
	firstAddress := addressFor(t, firstIndex, "first")
	secondAddress, err := sink.NewRecordAddress(testuri.Resource("secondary", []string{secondIndex}), sink.StringKey("second"))
	if err != nil {
		t.Fatal(err)
	}
	firstOp := put(t, firstAddress, `{"counter":1}`, sink.WriteUpsert)
	secondOp := put(t, secondAddress, `{"counter":2}`, sink.WriteUpsert)
	applied(t, writeAsync(t.Context(), gateway.client, sink.CompletionWaitUntilVisible, firstOp, secondOp), 2)
	records, err := gateway.client.Read(t.Context(), secondAddress, firstAddress, secondAddress)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 3 {
		t.Fatal(records)
	}
	for _, result := range records {
		if result.Status != sink.ReadFound {
			t.Fatal(records)
		}
	}
	// All native methods retain their public shape across the internal envelope.
	command := nativeSearch(firstIndex)
	query := sink.QueryRequest{Command: command, PageSize: 10}
	page, err := gateway.client.Query(t.Context(), query)
	if err != nil || len(page.Documents) != 1 {
		t.Fatalf("Query: %v %v", page, err)
	}
	countRequest := sink.CountRequest{Command: command}
	count, err := gateway.client.Count(t.Context(), countRequest)
	if err != nil || count.Count != 1 {
		t.Fatalf("Count: %v %v", count, err)
	}
	scanRequest := sink.ScanRequest{Command: command, BatchSize: 10}
	scan, err := gateway.client.Scan(t.Context(), scanRequest)
	if err != nil || len(scan.Documents) != 1 {
		t.Fatalf("Scan: %v %v", scan, err)
	}
	executeRequest := sink.ExecuteRequest{Command: command}
	executed, err := gateway.client.Execute(t.Context(), executeRequest)
	if err != nil || !executed.Success {
		t.Fatalf("Execute: %v %v", executed, err)
	}

	t.Run("cross-store-return-budget", func(t *testing.T) {
		constrainedOpts := serverOptions{role: "gateway", routes: gatewayOpts.routes, readBytes: 200}
		constrained := startCandidate(t, constrainedOpts)
		firstBudgetAddress := addressFor(t, firstIndex, "budget")
		secondBudgetAddress, err := sink.NewRecordAddress(testuri.Resource("secondary", []string{secondIndex}), sink.StringKey("budget"))
		if err != nil {
			t.Fatal(err)
		}
		firstReturned := put(t, firstBudgetAddress, `{"counter":3}`, sink.WriteUpsert).WithReturnedDocument()
		secondReturned := put(t, secondBudgetAddress, `{"counter":4}`, sink.WriteUpsert).WithReturnedDocument()
		outcome := <-writeAsync(t.Context(), constrained.client, sink.CompletionWaitUntilApplied, firstReturned, secondReturned)
		if len(outcome.results) != 2 || outcome.results[0].Status != sink.WriteApplied || outcome.results[1].Failure == nil || outcome.results[1].Failure.Code != sink.FailureResourceExhausted {
			t.Fatalf("cross-Store budget: %v", outcome)
		}
		lookup := httpCall{endpoint: backends[1].endpoint, method: http.MethodGet, path: "/" + secondIndex + "/_doc/budget"}
		code, body := request(t, lookup)
		if code != http.StatusNotFound {
			t.Fatalf("over-budget write committed: %d %s", code, body)
		}
	})
	// Killing one Engine leaves the other Store functional and never marks an unknown write safe to retry.
	first.crash(t)
	result := writeAsync(t.Context(), gateway.client, sink.CompletionWaitUntilApplied, firstOp, secondOp)
	outcome := <-result
	if len(outcome.results) != 2 {
		t.Fatalf("partial results missing: %v", outcome)
	}
	if outcome.results[0].Failure == nil || outcome.results[0].Failure.Retryable || outcome.results[1].Status != sink.WriteApplied {
		t.Fatalf("Store failure leaked or became replayable: %v", outcome)
	}
	deleted, err := gateway.client.Delete(t.Context(), sink.CompletionWaitUntilApplied, secondAddress)
	if err != nil || deleted[0].Status != sink.DeleteApplied {
		t.Fatalf("Delete: %v %v", deleted, err)
	}
}

func TestStoreIsolatedWorkerColdStartWithoutEngine(t *testing.T) {
	store := searchBackends(t)[0]
	index := indexFor(t, store, "100ms")
	broker := startBroker(t)
	topic := fmt.Sprintf("isolated-%d", time.Now().UnixNano())
	engineOpts := serverOptions{role: "engine", store: "primary", backend: store, broker: broker.address, topic: topic}
	engine := startCandidate(t, engineOpts)
	// Capability readiness is explicit; process readiness does not aggregate dependencies.
	deadline := time.Now().Add(20 * time.Second)
	for {
		endpoint := strings.TrimSuffix(engine.metrics, "/metrics") + "/readyz?service=sink.kafka.primary"
		response, err := http.Get(endpoint)
		ready := err == nil && response.StatusCode == http.StatusOK
		if response != nil {
			response.Body.Close()
		}
		if ready {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Engine producer did not become ready")
		}
		time.Sleep(20 * time.Millisecond)
	}
	operation := put(t, addressFor(t, index, "cold"), `{"counter":7}`, sink.WriteUpsert)
	result := <-writeAsync(t.Context(), engine.client, sink.CompletionReturnAfterAccepted, operation)
	if result.err != nil || result.results[0].Status != sink.WriteAccepted {
		t.Fatalf("not accepted: %v", result)
	}
	engine.crash(t)
	workerOpts := serverOptions{worker: true, store: "primary", backend: store, broker: broker.address, topic: topic}
	startCandidate(t, workerOpts)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, store.endpoint+"/"+index+"/_doc/cold", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := http.DefaultClient.Do(request)
		if err == nil {
			body, readErr := io.ReadAll(response.Body)
			response.Body.Close()
			if readErr == nil && response.StatusCode == 200 && strings.Contains(string(body), `"counter":7`) {
				break
			}
		}
		if ctx.Err() != nil {
			t.Fatal("Worker did not persist accepted mutation without Engine")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

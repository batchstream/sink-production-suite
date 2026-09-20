//go:build integration

package conformance_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/liran/sink-production-suite/internal/testuri"

	sink "github.com/liran/sink-go"
)

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
	gatewayOpts := serverOptions{role: "gateway", routes: isolatedRoutes(t, primary, secondary), maxOps: opts.maxOps, readBytes: opts.readBytes}
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
	return fmt.Sprintf("  routes:\n    - store: primary\n      target: %s\n      tls: {insecure: true}\n    - store: secondary\n      target: %s\n      tls: {insecure: true}\n", primary.engineAddress, secondary.engineAddress)
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
	readRequest := sink.ReadRequest{
		Addresses: []sink.Address{secondAddress, firstAddress, secondAddress},
	}
	records, err := gateway.client.Read(t.Context(), readRequest)
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

	t.Run("cross-store-result-streams", func(t *testing.T) {
		constrainedOpts := serverOptions{role: "gateway", routes: gatewayOpts.routes, readBytes: 1024}
		constrained := startCandidate(t, constrainedOpts)
		firstReturnedAddress := addressFor(t, firstIndex, "budget")
		secondReturnedAddress, err := sink.NewRecordAddress(testuri.Resource("secondary", []string{secondIndex}), sink.StringKey("budget"))
		if err != nil {
			t.Fatal(err)
		}
		firstDocument := fmt.Sprintf(`{"counter":3,"padding":%q}`, strings.Repeat("a", 600))
		secondDocument := fmt.Sprintf(`{"counter":4,"padding":%q}`, strings.Repeat("b", 600))
		firstReturned := put(t, firstReturnedAddress, firstDocument, sink.WriteUpsert).WithReturnedDocument()
		secondReturned := put(t, secondReturnedAddress, secondDocument, sink.WriteUpsert).WithReturnedDocument()
		results := applied(t, writeAsync(t.Context(), constrained.client, sink.CompletionWaitUntilApplied, firstReturned, secondReturned), 2)
		for i, expected := range []string{firstDocument, secondDocument} {
			if string(results[i].Document.Payload()) != expected {
				t.Fatalf("cross-Store stream lost returned document %d", i)
			}
		}
		assertCounter(t, constrained.client, firstReturnedAddress, 3)
		assertCounter(t, constrained.client, secondReturnedAddress, 4)
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
	deleteRequest := sink.DeleteRequest{
		CompletionMode: sink.CompletionWaitUntilApplied,
		Addresses:      []sink.Address{secondAddress},
	}
	deleted, err := gateway.client.Delete(t.Context(), deleteRequest)
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
		endpoint := engine.health + "/readyz?service=sink.kafka.primary"
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

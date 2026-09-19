//go:build integration

package conformance_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
	collector "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	logs "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/proto"
)

func TestProcessLoggingSurvivesCollectorOutage(t *testing.T) {
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			var ready atomic.Bool
			var mu sync.Mutex
			var records []*logs.LogRecord
			failed := make(chan struct{}, 1)
			collectorServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !ready.Load() {
					select {
					case failed <- struct{}{}:
					default:
					}
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				if r.URL.Path != "/v1/logs" || r.Header.Get("Content-Type") != "application/x-protobuf" {
					t.Errorf("incorrect collector request: %s %s", r.URL.Path, r.Header.Get("Content-Type"))
				}
				raw, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
				}
				request := &collector.ExportLogsServiceRequest{}
				if err := proto.Unmarshal(raw, request); err != nil {
					t.Error(err)
				}
				mu.Lock()
				for _, resource := range request.ResourceLogs {
					for _, scope := range resource.ScopeLogs {
						records = append(records, scope.LogRecords...)
					}
				}
				mu.Unlock()
				w.Header().Set("Content-Type", "application/x-protobuf")
			}))
			t.Cleanup(collectorServer.Close)
			logging := fmt.Sprintf(`logging:
  level: debug
  console: {enabled: false, format: json}
  labels: {environment: qualification}
  otlp:
    enabled: true
    protocol: http/protobuf
    endpoint: %q
    tls: {enabled: false}
    queue_size: 8
    batch_size: 1
    flush_interval: 20ms
    export_timeout: 100ms
    shutdown_timeout: 500ms
`, strings.TrimPrefix(collectorServer.URL, "http://"))
			index := indexFor(t, store, "100ms")
			engineOpts := serverOptions{backend: store, logging: logging}
			engine := startCandidate(t, engineOpts)
			routes := fmt.Sprintf("  routes:\n    - store: primary\n      target: %s\n      tls: {insecure: true}\n", engine.engineAddress)
			gatewayOpts := serverOptions{role: "gateway", routes: routes, logging: logging}
			gateway := startCandidate(t, gatewayOpts)
			select {
			case <-failed:
			case <-time.After(3 * time.Second):
				t.Fatal("candidate did not reach the unavailable Collector")
			}
			const marker = "synthetic-document-must-not-appear-in-logs"
			var addresses []sink.Address
			for i := range 24 {
				address := addressFor(t, index, fmt.Sprint(i))
				addresses = append(addresses, address)
				operation := put(t, address, `{"value":"`+marker+`"}`, sink.WriteCreate)
				ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
				applied(t, writeAsync(ctx, gateway.client, sink.CompletionWaitUntilApplied, operation), 1)
				cancel()
			}
			found, err := gateway.client.Read(t.Context(), addresses...)
			if err != nil || len(found) != len(addresses) {
				t.Fatalf("business traffic failed during Collector outage: %v", err)
			}
			for _, record := range found {
				if record.Status != sink.ReadFound || !bytes.Contains(record.Document.Payload(), []byte(marker)) {
					t.Fatalf("business state was not preserved: %+v", record)
				}
			}
			ready.Store(true)
			// Wait for the recovered exporter to empty its old queue before the
			// one required failure, so deliberate overflow cannot drop our oracle.
			deadline := time.Now().Add(3 * time.Second)
			for {
				mu.Lock()
				seen := len(records)
				mu.Unlock()
				if seen > 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("Collector recovery required a process restart")
				}
				time.Sleep(10 * time.Millisecond)
			}
			time.Sleep(200 * time.Millisecond)
			duplicate := put(t, addresses[0], `{"value":"`+marker+`"}`, sink.WriteCreate)
			outcome := <-writeAsync(t.Context(), gateway.client, sink.CompletionWaitUntilApplied, duplicate)
			if outcome.err != nil || len(outcome.results) != 1 || outcome.results[0].Status != sink.WritePreconditionFailed {
				t.Fatalf("duplicate create changed semantics: %+v", outcome)
			}
			gateway.stop(t)
			engine.stop(t)
			mu.Lock()
			defer mu.Unlock()
			stopped := map[string]bool{}
			failureLogged, forwarded := false, false
			for _, record := range records {
				attrs := map[string]string{}
				for _, attr := range record.Attributes {
					attrs[attr.Key] = attr.Value.GetStringValue()
				}
				if attrs["service_name"] != "sink" || attrs["instance_id"] == "" || attrs["environment"] != "qualification" || record.TimeUnixNano == 0 {
					t.Fatalf("lost process identity: %v", attrs)
				}
				if strings.Contains(record.String(), marker) || len(record.TraceId) != 0 || len(record.SpanId) != 0 {
					t.Fatal("request contents or tracing context escaped into logs")
				}
				if attrs["event"] == "process_stopped" {
					stopped[attrs["role"]] = true
				}
				if attrs["role"] == "engine" && attrs["method"] == "ForwardStream" {
					forwarded = true
				}
				if attrs["role"] == "gateway" && attrs["event"] == "rpc_completed" && attrs["method"] == "Write" && attrs["failed"] == "1" && attrs["level"] == "warn" {
					failureLogged = true
				}
			}
			if !failureLogged || !forwarded || !stopped["engine"] || !stopped["gateway"] {
				t.Fatalf("missing failure/stream/shutdown diagnostics: failure=%t stream=%t stopped=%v", failureLogged, forwarded, stopped)
			}
			for _, server := range []*candidate{engine, gateway} {
				contents, err := os.ReadFile(server.logPath)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Contains(contents, []byte(`"component":"logging"`)) || bytes.Contains(contents, []byte(marker)) || bytes.Contains(contents, []byte(`"event":"rpc_completed"`)) {
					t.Fatalf("console-only exporter diagnostics missing or normal logs escaped: %s", contents)
				}
				for _, line := range bytes.Split(bytes.TrimSpace(contents), []byte("\n")) {
					var entry map[string]any
					if err := json.Unmarshal(line, &entry); err != nil {
						t.Fatalf("non-structured process diagnostic: %s", line)
					}
				}
			}
		})
	}
}

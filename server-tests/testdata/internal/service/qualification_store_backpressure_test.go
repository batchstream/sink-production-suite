package service_test

import (
	sink "github.com/batchstream/sink/gen/sink"
	"github.com/batchstream/sink/internal/backpressure"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"
)

// Exercise admission through the real RPC adapter for every Native method,
// including the streaming terminal error, without weakening response semantics.
func TestNativeTransportSharesStoreAdmission(t *testing.T) {
	opts := backpressure.Options{Store: "primary", Role: "engine", MaxConcurrent: 1}
	controller, err := backpressure.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	permit, err := controller.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer permit.Release()
	client, _ := nativeRPCFixture(t, false, controller)
	execute := nativeSearchRequest()
	if _, err := client.Execute(t.Context(), execute); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("Execute admission: %v", err)
	}
	count := &sink.CountRequest{Command: execute.Command}
	if _, err := client.Count(t.Context(), count); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("Count admission: %v", err)
	}
	query := &sink.QueryRequest{Command: execute.Command, PageSize: 1}
	if _, err := collectQuery(t.Context(), client, query); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("Query admission: %v", err)
	}
	scan := &sink.ScanRequest{Command: execute.Command, BatchSize: 1}
	if _, err := collectScan(t.Context(), client, scan); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("Scan admission: %v", err)
	}
	permit.Release()
	if response, err := client.Count(t.Context(), count); err != nil || response.GetCount() != 123 {
		t.Fatalf("Count recovery: %v %v", response, err)
	}
	if _, err := collectQuery(t.Context(), client, query); err != nil {
		t.Fatalf("Query recovery: %v", err)
	}
	if response, err := collectScan(t.Context(), client, scan); err != nil || len(response.GetDocuments()) != 1 {
		t.Fatalf("Scan recovery: %v %v", response, err)
	}
	if response, err := client.Execute(t.Context(), execute); err != nil || response.GetStatusCode() != 400 {
		t.Fatalf("Execute semantics: %v %v", response, err)
	}
}

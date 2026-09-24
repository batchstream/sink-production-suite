package service_test

import (
	"context"
	"testing"
	"time"

	sink "github.com/batchstream/sink-protocol/sink/v1"
	"github.com/batchstream/sink/internal/backpressure"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Exercise admission through the real RPC adapter for every Native method,
// including the streaming terminal error, without weakening response semantics.
func TestNativeTransportSharesStoreAdmission(t *testing.T) {
	opts := backpressure.Options{Store: "primary", Role: "engine", MaxConcurrent: 1, MaxQueuedRequests: 2, MaxQueuedBytes: 4096}
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
	query := &sink.QueryRequest{Command: execute.Command, PageSize: 1}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	results := make(chan error, 2)
	go func() { _, err := client.Count(ctx, count); results <- err }()
	go func() { _, err := collectQuery(ctx, client, query); results <- err }()
	registry := prometheus.NewPedanticRegistry()
	if err := registry.Register(controller); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		select {
		case err := <-results:
			t.Fatalf("healthy Query/Count did not wait for shared capacity: %v", err)
		default:
		}
		families, err := registry.Gather()
		if err != nil {
			t.Fatal(err)
		}
		queued := float64(0)
		for _, family := range families {
			if family.GetName() == "sink_store_admission_queued_requests" {
				queued = family.Metric[0].GetGauge().GetValue()
			}
		}
		if queued == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("RPCs never entered the bounded admission FIFO")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := client.Count(t.Context(), count); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("full read queue admission: %v", err)
	}
	cancel()
	for range 2 {
		if err := <-results; status.Code(err) != codes.Canceled {
			t.Fatalf("queued RPC cancellation: %v", err)
		}
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

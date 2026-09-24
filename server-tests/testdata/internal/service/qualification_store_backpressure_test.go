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
// Verify real gRPC serialization, including streaming terminal errors. A full
// execution window queues valid work; only a caller deadline ends that wait.
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
	methods := []string{"Execute", "Count", "Query", "Scan"}
	for _, method := range methods {
		t.Run(method+"/deadline", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()
			if err := callNativeAdmission(ctx, client, method); status.Code(err) != codes.DeadlineExceeded {
				t.Fatalf("full window must wait for the caller deadline, not reject: %v", err)
			}
		})
	}
	registry := prometheus.NewPedanticRegistry()
	if err := registry.Register(controller); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	results := make(chan error, len(methods))
	for _, method := range methods {
		go func() { results <- callNativeAdmission(ctx, client, method) }()
	}
	waitAdmissionTasks(t, registry, len(methods))
	permit.Release()
	for range methods {
		if err := <-results; err != nil {
			t.Fatalf("queued Native call did not recover with its original response semantics: %v", err)
		}
	}
	waitAdmissionTasks(t, registry, 0)
}

func callNativeAdmission(ctx context.Context, client sink.SinkClient, method string) error {
	execute := nativeSearchRequest()
	switch method {
	case "Execute":
		response, err := client.Execute(ctx, execute)
		if err != nil {
			return err
		}
		if response.GetStatusCode() != 400 {
			return fmt.Errorf("Execute response changed: %v", response)
		}
	case "Count":
		request := &sink.CountRequest{Command: execute.Command}
		response, err := client.Count(ctx, request)
		if err != nil {
			return err
		}
		if response.GetCount() != 123 {
			return fmt.Errorf("Count response changed: %v", response)
		}
	case "Query":
		request := &sink.QueryRequest{Command: execute.Command, PageSize: 1}
		_, err := collectQuery(ctx, client, request)
		return err
	case "Scan":
		request := &sink.ScanRequest{Command: execute.Command, BatchSize: 1}
		response, err := collectScan(ctx, client, request)
		if err != nil {
			return err
		}
		if len(response.GetDocuments()) != 1 {
			return fmt.Errorf("Scan response changed: %v", response)
		}
	default:
		return fmt.Errorf("unexpected method %s", method)
	}
	return nil
}

func waitAdmissionTasks(t *testing.T, registry *prometheus.Registry, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		families, err := registry.Gather()
		if err != nil {
			t.Fatal(err)
		}
		for _, family := range families {
			if family.GetName() == "sink_store_admission_queued_tasks" && len(family.Metric) == 1 && family.Metric[0].GetGauge().GetValue() == float64(want) {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("admission task count did not reach %d", want)
}

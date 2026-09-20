package gateway

import (
	"context"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"google.golang.org/genproto/googleapis/rpc/errdetails"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	forward "github.com/batchstream/sink/gen/forward"
	sink "github.com/batchstream/sink/gen/sink"
	"github.com/batchstream/sink/internal/capacity"
	"github.com/batchstream/sink/internal/forwarding"
	"google.golang.org/grpc"
)

type rejectionEngine struct {
	forward.UnimplementedEngineServer
	calls atomic.Int32
}

func (s *rejectionEngine) Forward(req *forward.ForwardRequest, stream grpc.ServerStreamingServer[forward.ForwardResponse]) error {
	s.calls.Add(1)
	for i := range req.GetWrite().GetOperations() {
		result := &sink.WriteResult{OperationIndex: uint32(i), Status: sink.WriteStatus_WRITE_STATUS_APPLIED}
		written := &sink.WriteResponse{Results: []*sink.WriteResult{result}}
		body := &forward.ForwardResponse_Write{Write: written}
		frame := &forward.ForwardResponse{Version: forwarding.Version, Store: req.GetStore(), Response: body}
		if err := stream.Send(frame); err != nil {
			return err
		}
	}
	return nil
}

func TestMemoryPressureRejectsBeforeForwarding(t *testing.T) {
	backend := &rejectionEngine{}
	fixture := fixtureEngine{store: "primary", target: serveEngine(t, backend)}
	server := testGateway(t, 1<<20, fixture)
	counter := &fstest.MapFile{Data: []byte("10000 10000")}
	files := fstest.MapFS{"proc/self/statm": counter}
	opts := capacity.Options{Bytes: 1 << 20, HighPercent: 80, LowPercent: 70, Files: files}
	guard, err := capacity.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	server.memory = guard
	op := put("primary", "retry-local", false)
	request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{op}}
	_, err = collectWrite(t.Context(), server, request)
	if status.Code(err) != codes.ResourceExhausted || backend.calls.Load() != 0 {
		t.Fatalf("pressure rejection reached Engine: calls=%d err=%v", backend.calls.Load(), err)
	}
	server.memory = gatewayMemory(t, 1<<20)
	control, err := collectWrite(t.Context(), server, request)
	if err != nil || control.GetResults()[0].GetStatus() != sink.WriteStatus_WRITE_STATUS_APPLIED || backend.calls.Load() != 1 {
		t.Fatalf("healthy control failed: %v %v calls=%d", control, err, backend.calls.Load())
	}
}

func TestNativeMemoryPressurePreservesScanRetryAndCancellation(t *testing.T) {
	backend := &rejectionEngine{}
	fixture := fixtureEngine{store: "primary", target: serveEngine(t, backend)}
	server := testGateway(t, 1<<20, fixture)
	counter := &fstest.MapFile{Data: []byte("10000 10000")}
	files := fstest.MapFS{"proc/self/statm": counter}
	opts := capacity.Options{Bytes: 1 << 20, HighPercent: 80, LowPercent: 70, Files: files}
	guard, err := capacity.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	server.memory = guard
	server.metrics.registry.MustRegister(guard)
	command := &sink.Command{Uri: "sink://primary", Method: "POST", Path: "/records/_search", ContentType: "application/json", Payload: []byte(`{}`)}
	execute := &sink.ExecuteRequest{Command: command}
	query := &sink.QueryRequest{Command: command}
	count := &sink.CountRequest{Command: command}
	scan := &sink.ScanRequest{Command: command}
	calls := []func(context.Context) error{
		func(ctx context.Context) error { _, err := server.Execute(ctx, execute); return err },
		func(ctx context.Context) error { _, err := collectQuery(ctx, server, query); return err },
		func(ctx context.Context) error { _, err := server.Count(ctx, count); return err },
		func(ctx context.Context) error { _, err := collectScan(ctx, server, scan); return err },
	}
	for i, call := range calls {
		err := call(t.Context())
		if status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("method %d ignored pressure: %v", i, err)
		}
		if i == 3 {
			details := status.Convert(err).Details()
			if len(details) != 1 {
				t.Fatalf("Scan missing safe retry detail: %v", err)
			}
			info, ok := details[0].(*errdetails.ErrorInfo)
			if !ok || info.GetReason() != "SCAN_ADMISSION_REJECTED" || info.GetMetadata()["pool"] != "memory" {
				t.Fatalf("invalid Scan retry detail: %v", details)
			}
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if err := call(ctx); status.Code(err) != codes.Canceled {
			t.Fatalf("cancellation became overload: %v", err)
		}
	}
	if backend.calls.Load() != 0 {
		t.Fatal("rejected native request reached Engine")
	}
	recorder := httptest.NewRecorder()
	server.MetricsHandler().ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	body := recorder.Body.String()
	for _, obsolete := range []string{"sink_gateway_rejected_total", "sink_gateway_in_flight_bytes", "sink_memory_capacity_bytes", "sink_memory_waiting_", "sink_memory_burst_"} {
		if strings.Contains(body, obsolete) {
			t.Fatalf("obsolete metric exported: %s", obsolete)
		}
	}
	if !strings.Contains(body, "sink_memory_pressure") || !strings.Contains(body, "sink_memory_rejected_total") {
		t.Fatal("missing watermark metrics")
	}
}

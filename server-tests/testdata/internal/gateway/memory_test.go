package gateway

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"testing/fstest"

	sink "github.com/batchstream/sink-protocol/sink/v1"
	forward "github.com/batchstream/sink/gen/forward"
	"github.com/batchstream/sink/internal/capacity"
	"github.com/batchstream/sink/internal/forwarding"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func gatewayMemory(t *testing.T, size int64) *capacity.Guard {
	t.Helper()
	counter := &fstest.MapFile{Data: []byte("1 0")}
	files := fstest.MapFS{"proc/self/statm": counter}
	opts := capacity.Options{Bytes: size, HighPercent: 80, LowPercent: 70, Files: files}
	p, err := capacity.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestMemoryAdmissionDoesNotLimitRequestCount(t *testing.T) {
	fixture := fixtureEngine{store: "primary", target: "127.0.0.1:1"}
	server := testGateway(t, 32<<20, fixture)
	server.memory = gatewayMemory(t, 128<<20)
	request := &sink.ReadRequest{}
	body := &forward.ForwardRequest_Read{Read: request}
	wrapped := &forward.ForwardRequest{Request: body}
	releases := make([]func(), 0, 128)
	for range 128 {
		_, release, err := server.begin(t.Context(), wrapped)
		if err != nil {
			t.Fatalf("tiny read was charged its 32 MiB maximum: %v", err)
		}
		releases = append(releases, release)
	}
	for _, release := range releases {
		release()
	}
}

func TestStreamedForwardingUsesActualSize(t *testing.T) {
	backend := testEngine(t, "primary", 32<<20)
	server := testGateway(t, 32<<20, backend)
	// A healthy process can forward large responses without allocation leases.
	server.memory = gatewayMemory(t, 8<<20)
	payload := []byte(`{"value":"` + strings.Repeat("x", 1<<20) + `"}`)
	op := put("primary", "large", true)
	op.GetPut().Document.Payload = payload
	write := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{op}}
	result, err := collectWrite(t.Context(), server, write)
	if err != nil || result.GetResults()[0].GetStatus() != sink.WriteStatus_WRITE_STATUS_APPLIED {
		t.Fatalf("write: %v %v", result, err)
	}
	if !bytes.Equal(result.GetResults()[0].GetDocument().GetPayload(), payload) {
		t.Fatal("returned write payload changed")
	}
	readOp := &sink.ReadOperation{Address: address("primary", "large")}
	read := &sink.ReadRequest{Operations: []*sink.ReadOperation{readOp}}
	response, err := collectRead(t.Context(), server, read)
	if err != nil || !bytes.Equal(response.GetResults()[0].GetDocument().GetPayload(), payload) {
		t.Fatalf("framed read failed: %v", err)
	}
}

type badFrameEngine struct {
	forward.UnimplementedEngineServer
	mode string
}

func (s *badFrameEngine) Forward(req *forward.ForwardRequest, stream grpc.ServerStreamingServer[forward.ForwardResponse]) error {
	read := &sink.ReadResponse{}
	frame := &forward.ForwardResponse{Version: forwarding.Version, Store: req.GetStore(), Response: &forward.ForwardResponse_Read{Read: read}}
	switch s.mode {
	case "rejected":
		stream.SetTrailer(metadata.Pairs(forwarding.NotStartedTrailer, "true"))
		return status.Error(codes.ResourceExhausted, "temporarily unavailable")
	case "uncertain":
		return status.Error(codes.ResourceExhausted, "temporarily unavailable")
	case "wrong-store":
		frame.Store = "another"
	case "wrong-version":
		frame.Version = 0
	case "missing-result":
		return nil
	case "empty-frame":
		frame.Response = nil
	case "wrong-type":
		frame.Response = &forward.ForwardResponse_Write{Write: &sink.WriteResponse{}}
	case "rejection-after-result", "rejection-with-success":
		stream.SetTrailer(metadata.Pairs(forwarding.NotStartedTrailer, "true"))
	case "duplicate-marker":
		stream.SetTrailer(metadata.Pairs(forwarding.NotStartedTrailer, "true", forwarding.NotStartedTrailer, "true"))
		return status.Error(codes.ResourceExhausted, "rejected")
	}
	if err := stream.Send(frame); err != nil {
		return err
	}
	if s.mode == "trailing" {
		return stream.Send(frame)
	}
	if s.mode == "rejection-after-result" {
		return status.Error(codes.ResourceExhausted, "too late to reject")
	}
	return nil
}

func TestForwardRejectionTrailerDistinguishesUnknownMutation(t *testing.T) {
	for _, mode := range []string{"rejected", "uncertain"} {
		t.Run(mode, func(t *testing.T) {
			backend := &badFrameEngine{mode: mode}
			fixture := fixtureEngine{store: "primary", target: serveEngine(t, backend)}
			server := testGateway(t, 4096, fixture)
			operation := put("primary", "rejection", false)
			request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{operation}}
			response, err := collectWrite(t.Context(), server, request)
			if err != nil || len(response.GetResults()) != 1 {
				t.Fatalf("missing operation outcome: %v %v", response, err)
			}
			failure := response.Results[0].GetFailure()
			if failure.GetCode() != sink.FailureCode_FAILURE_CODE_RESOURCE_EXHAUSTED || failure.GetRetryable() != (mode == "rejected") || strings.Contains(failure.GetMessage(), "unknown") != (mode == "uncertain") {
				t.Fatalf("incorrect execution guarantee: %v", failure)
			}
		})
	}
}

func TestGatewayMessageLimitDoesNotAdvertiseWriteNonExecution(t *testing.T) {
	backend := testEngine(t, "primary", 4096)
	server := testGateway(t, 200, backend)
	operation := put("primary", "larger-than-gateway", true)
	operation.GetPut().Document.Payload = []byte(`{"value":"` + strings.Repeat("x", 512) + `"}`)
	request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{operation}}
	response, err := collectWrite(t.Context(), server, request)
	if err != nil || len(response.GetResults()) != 1 {
		t.Fatalf("missing size failure: %v %v", response, err)
	}
	failure := response.Results[0].GetFailure()
	if failure.GetCode() != sink.FailureCode_FAILURE_CODE_RESOURCE_EXHAUSTED || failure.GetRetryable() || !strings.Contains(failure.GetMessage(), "unknown") {
		t.Fatalf("post-execution transport limit became safe rejection: %v", failure)
	}
	readOperation := &sink.ReadOperation{Address: operation.Address}
	read := &sink.ReadRequest{Operations: []*sink.ReadOperation{readOperation}}
	stored, err := backend.core.Read(t.Context(), read)
	if err != nil || stored.Results[0].GetStatus() != sink.ReadStatus_READ_STATUS_FOUND {
		t.Fatalf("Engine did not apply the locally valid write: %v %v", stored, err)
	}
}

func TestTypedStreamRejectsInvalidFramesAndRejectionMarkers(t *testing.T) {
	for _, mode := range []string{"wrong-store", "wrong-version", "missing-result", "empty-frame", "wrong-type", "trailing", "rejection-after-result", "rejection-with-success", "duplicate-marker"} {
		t.Run(mode, func(t *testing.T) {
			backend := &badFrameEngine{mode: mode}
			fixture := fixtureEngine{store: "primary", target: serveEngine(t, backend)}
			server := testGateway(t, 32<<20, fixture)
			route, err := routeFor(server.current, "primary")
			if err != nil {
				t.Fatal(err)
			}
			read := &sink.ReadRequest{}
			request := &forward.ForwardRequest{Request: &forward.ForwardRequest_Read{Read: read}}
			_, notStarted, err := server.forward(context.Background(), route, request)
			if status.Code(err) != codes.Internal || notStarted {
				t.Fatalf("invalid response accepted: notStarted=%v err=%v", notStarted, err)
			}
		})
	}
}

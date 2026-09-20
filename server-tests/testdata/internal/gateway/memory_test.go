package gateway

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"testing/fstest"

	forward "github.com/liran/sink/gen/forward"
	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/capacity"
	"github.com/liran/sink/internal/forwarding"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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
	used := &forward.Budget{}
	final := &forward.ForwardResponse{Version: forwarding.Version, Store: req.GetStore(), Used: used, Complete: true}
	switch s.mode {
	case "wrong-store":
		final.Store = "another"
	case "wrong-version":
		final.Version = 0
	case "missing-final":
		return nil
	case "bad-usage":
		used.Returns = req.GetGrant().GetReturns() + 1
	case "body-in-final":
		read := &sink.ReadResponse{}
		final.Response = &forward.ForwardResponse_Read{Read: read}
	}
	if err := stream.Send(final); err != nil {
		return err
	}
	if s.mode == "trailing" {
		return stream.Send(final)
	}
	return nil
}

func TestTypedStreamRejectsInvalidSettlement(t *testing.T) {
	for _, mode := range []string{"wrong-store", "wrong-version", "missing-final", "bad-usage", "body-in-final", "trailing"} {
		t.Run(mode, func(t *testing.T) {
			backend := &badFrameEngine{mode: mode}
			fixture := fixtureEngine{store: "primary", target: serveEngine(t, backend)}
			server := testGateway(t, 32<<20, fixture)
			route, err := routeFor(server.current, "primary")
			if err != nil {
				t.Fatal(err)
			}
			grant := &forward.Budget{Returns: 4096}
			request := &forward.ForwardRequest{Grant: grant}
			call := forwardCall{route: route, request: request, emit: func(*forward.ForwardResponse) error { return nil }}
			_, err = server.forwardEach(context.Background(), call)
			if status.Code(err) != codes.Internal {
				t.Fatalf("invalid settlement accepted: %v", err)
			}
		})
	}
}

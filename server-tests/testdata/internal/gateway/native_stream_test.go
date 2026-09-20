package gateway

import (
	"testing"

	forward "github.com/batchstream/sink/gen/forward"
	sink "github.com/batchstream/sink/gen/sink"
	"github.com/batchstream/sink/internal/forwarding"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type nativeStreamEngine struct {
	forward.UnimplementedEngineServer
	mode string
}

func (s *nativeStreamEngine) Forward(req *forward.ForwardRequest, stream grpc.ServerStreamingServer[forward.ForwardResponse]) error {
	document := &sink.Document{Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_JSON, Payload: []byte(`{"value":1}`)}
	for index := range 2 {
		complete := index == 1
		documents := []*sink.Document{document}
		if complete {
			documents = nil
		}
		switch s.mode {
		case "missing-complete":
			if complete {
				return nil
			}
		case "empty-data":
			if !complete {
				documents = nil
			}
		case "document-in-complete":
			if complete {
				documents = []*sink.Document{document}
			}
		case "data-after-complete":
			complete = index == 0
			documents = nil
			if !complete {
				documents = []*sink.Document{document}
			}
		}
		frame := &forward.ForwardResponse{Version: forwarding.Version, Store: req.Store}
		if req.GetQuery() != nil {
			result := &sink.QueryResponse{Documents: documents, Complete: complete}
			frame.Response = &forward.ForwardResponse_Query{Query: result}
		} else {
			result := &sink.ScanResponse{Documents: documents, Complete: complete}
			frame.Response = &forward.ForwardResponse_Scan{Scan: result}
		}
		if err := stream.Send(frame); err != nil {
			return err
		}
	}
	if s.mode == "late-error" {
		return status.Error(codes.Unavailable, "connection interrupted")
	}
	return nil
}

func TestNativeStreamCompletionAndLateErrors(t *testing.T) {
	for _, method := range []string{"Query", "Scan"} {
		for _, mode := range []string{"valid", "missing-complete", "empty-data", "document-in-complete", "data-after-complete", "late-error"} {
			t.Run(method+"/"+mode, func(t *testing.T) {
				backend := &nativeStreamEngine{mode: mode}
				fixture := fixtureEngine{store: "primary", target: serveEngine(t, backend)}
				server := testGateway(t, 4096, fixture)
				command := &sink.Command{Uri: "sink://primary/items"}
				var err error
				if method == "Query" {
					request := &sink.QueryRequest{Command: command}
					_, err = collectQuery(t.Context(), server, request)
				} else {
					request := &sink.ScanRequest{Command: command}
					_, err = collectScan(t.Context(), server, request)
				}
				want := codes.Internal
				if mode == "valid" {
					want = codes.OK
				} else if mode == "late-error" {
					want = codes.Unavailable
				}
				if status.Code(err) != want {
					t.Fatalf("stream status=%v want=%v", err, want)
				}
			})
		}
	}
}

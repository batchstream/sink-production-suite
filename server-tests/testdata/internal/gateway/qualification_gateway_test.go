package gateway

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liran/sink-go/uri"
	"github.com/batchstream/sink/internal/testuri"

	forward "github.com/batchstream/sink/gen/forward"
	sink "github.com/batchstream/sink/gen/sink"
	"github.com/batchstream/sink/internal/config"
	"github.com/batchstream/sink/internal/engine"
	"github.com/batchstream/sink/internal/forwarding"
	"github.com/batchstream/sink/internal/merge"
	"github.com/batchstream/sink/internal/service"
	"github.com/batchstream/sink/internal/storage/memory"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fixtureEngine struct {
	store  string
	target string
	core   *service.Server
}

func testEngine(t testing.TB, store string, maximum int) fixtureEngine {
	t.Helper()
	luaOpts := merge.LuaOptions{}
	lua, err := merge.NewLuaEngine(luaOpts)
	if err != nil {
		t.Fatal(err)
	}
	opts := service.Options{Storage: memory.New(), Lua: lua, BoundStore: store, MaxReadBytes: maximum}
	core, err := service.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	batchOpts := service.BatchingOptions{MaxWait: time.Millisecond}
	batched, err := service.NewBatchingServer(core, batchOpts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(batched.Close)
	engineOpts := engine.Options{Service: batched.RPC(), Store: store, MaxReadBytes: maximum}
	backend, err := engine.New(engineOpts)
	if err != nil {
		t.Fatal(err)
	}
	target := serveEngine(t, backend, batched.RPC())
	fixture := fixtureEngine{store: store, target: target, core: core}
	return fixture
}

func serveEngine(t testing.TB, backend forward.EngineServer, public ...sink.SinkServer) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	forward.RegisterEngineServer(server, backend)
	if len(public) > 0 {
		sink.RegisterSinkServer(server, public[0])
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return listener.Addr().String()
}

func routeText(engines ...fixtureEngine) string {
	text := "mode: gateway\nforwarding:\n  routes:\n"
	for _, e := range engines {
		text += fmt.Sprintf("    - store: %s\n      target: %s\n      tls: {insecure: true}\n", e.store, e.target)
	}
	return text
}

func testGateway(t testing.TB, maximum int, engines ...fixtureEngine) *Server {
	t.Helper()
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	if err := os.WriteFile(path, []byte(routeText(engines...)), 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := config.Load(path, "")
	if err != nil {
		t.Fatal(err)
	}
	settings := config.Gateway{Routes: loaded.Gateway.Routes, DNSRefreshInterval: time.Second, IdleTimeout: time.Minute, MaxConnections: 10, MaxFanout: 2}
	request := config.Request{MaxOperations: 1000, MaxReadBytes: maximum}
	opts := Options{Gateway: settings, Request: request, MaxResponseBytes: maximum, MaxMessageBytes: 64 << 20}
	server, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)
	return server
}

func address(store, key string) *sink.RecordAddress {
	kind := uri.StringKey(key)
	recordKey := kind
	result := &sink.RecordAddress{Uri: testuri.Record(store, []string{"db", "items"}, recordKey)}
	return result
}

func put(store, key string, returnDocument bool) *sink.WriteOperation {
	document := &sink.Document{Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_JSON, Payload: []byte(`{"value":1}`)}
	operation := &sink.PutOperation{Document: document, Mode: sink.WriteMode_WRITE_MODE_UPSERT}
	action := &sink.WriteOperation_Put{Put: operation}
	result := &sink.WriteOperation{Address: address(store, key), Action: action, ReturnDocument: returnDocument}
	return result
}

func TestCrossStorePublicRecordsAndPartialFailure(t *testing.T) {
	a := testEngine(t, "a", 4096)
	b := testEngine(t, "b", 4096)
	gateway := testGateway(t, 8192, a, b)
	write := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{put("a", "one", false), put("b", "two", false), put("missing", "three", false), put("a", "one", false)}}
	response, err := collectWrite(t.Context(), gateway, write)
	if err != nil {
		t.Fatal(err)
	}
	for i, result := range response.Results {
		if int(result.OperationIndex) != i {
			t.Fatal(response)
		}
		if i == 2 {
			if result.GetFailure().GetCode() != sink.FailureCode_FAILURE_CODE_INVALID_ARGUMENT {
				t.Fatal(response)
			}
		} else if result.Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
			t.Fatal(response)
		}
	}
	readA := &sink.ReadOperation{Address: address("a", "one")}
	readB := &sink.ReadOperation{Address: address("b", "two")}
	read := &sink.ReadRequest{Operations: []*sink.ReadOperation{readB, readA, readB}}
	found, err := collectRead(t.Context(), gateway, read)
	if err != nil {
		t.Fatal(err)
	}
	for i, result := range found.Results {
		if result.Status != sink.ReadStatus_READ_STATUS_FOUND || int(result.OperationIndex) != i {
			t.Fatal(found)
		}
	}
	deleteA := &sink.DeleteOperation{Address: readA.Address}
	deleteB := &sink.DeleteOperation{Address: readB.Address}
	deletion := &sink.DeleteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.DeleteOperation{deleteA, deleteB}}
	deleted, err := gateway.Delete(t.Context(), deletion)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range deleted.Results {
		if result.Status != sink.DeleteStatus_DELETE_STATUS_APPLIED {
			t.Fatal(deleted)
		}
	}
}

func TestCrossStoreReturnBudgetAppliesPerResult(t *testing.T) {
	a := testEngine(t, "a", 200)
	b := testEngine(t, "b", 200)
	gateway := testGateway(t, 200+2*1280, a, b)
	write := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{put("a", "one", true), put("b", "two", true)}}
	response, err := collectWrite(t.Context(), gateway, write)
	if err != nil {
		t.Fatal(err)
	}
	if response.Results[0].Status != sink.WriteStatus_WRITE_STATUS_APPLIED || response.Results[1].Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
		t.Fatal(response)
	}
	op := &sink.ReadOperation{Address: address("b", "two")}
	req := &sink.ReadRequest{Operations: []*sink.ReadOperation{op}}
	check, err := collectRead(t.Context(), b.core, req)
	if err != nil || check.Results[0].Status != sink.ReadStatus_READ_STATUS_FOUND {
		t.Fatalf("independently streamed mutation missing: %v %v", check, err)
	}
}

func TestCrossStoreReadBudgetAndRepeatedKeys(t *testing.T) {
	a := testEngine(t, "a", 300)
	b := testEngine(t, "b", 300)
	gateway := testGateway(t, 300+3*1280, a, b)
	write := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{put("a", "one", false), put("b", "two", false)}}
	if _, err := collectWrite(t.Context(), gateway, write); err != nil {
		t.Fatal(err)
	}
	one := &sink.ReadOperation{Address: address("a", "one")}
	two := &sink.ReadOperation{Address: address("b", "two")}
	request := &sink.ReadRequest{Operations: []*sink.ReadOperation{one, two, one}}
	response, err := collectRead(t.Context(), gateway, request)
	if err != nil {
		t.Fatal(err)
	}
	// Repeated keys and other Stores each have an independent frame allowance.
	if response.Results[0].Status != sink.ReadStatus_READ_STATUS_FOUND || response.Results[2].Status != sink.ReadStatus_READ_STATUS_FOUND || response.Results[1].Status != sink.ReadStatus_READ_STATUS_FOUND {
		t.Fatal(response)
	}
}

func TestLuaDeclarationsValidatedBeforeAnyStoreWrites(t *testing.T) {
	a := testEngine(t, "a", 4096)
	b := testEngine(t, "b", 4096)
	gateway := testGateway(t, 4096, a, b)
	bad := &sink.LuaProgram{Source: []byte("return 1"), Sha256: make([]byte, 32)}
	write := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{put("a", "one", false), put("b", "two", false)}, LuaPrograms: []*sink.LuaProgram{bad}}
	if _, err := collectWrite(t.Context(), gateway, write); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid declaration accepted: %v", err)
	}
	one := &sink.ReadOperation{Address: address("a", "one")}
	read := &sink.ReadRequest{Operations: []*sink.ReadOperation{one}}
	result, err := collectRead(t.Context(), gateway, read)
	if err != nil || result.Results[0].Status != sink.ReadStatus_READ_STATUS_NOT_FOUND {
		t.Fatalf("partial write before validation: %v %v", result, err)
	}
	source := []byte("return function(current, incoming) return incoming end")
	digest := sha256.Sum256(source)
	declaration := &sink.LuaProgram{Source: source, Sha256: digest[:]}
	write.LuaPrograms = []*sink.LuaProgram{declaration}
	for _, op := range write.Operations {
		reference := &sink.LuaProgram{Sha256: digest[:]}
		merged := &sink.MergeOperation{IncomingDocument: op.GetPut().GetDocument(), LuaProgram: reference}
		op.Action = &sink.WriteOperation_Merge{Merge: merged}
	}
	response, err := collectWrite(t.Context(), gateway, write)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range response.Results {
		if result.Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
			t.Fatal(response)
		}
	}
}

func TestMisroutedEngineCannotWrite(t *testing.T) {
	a := testEngine(t, "a", 4096)
	wrong := fixtureEngine{store: "b", target: a.target}
	gateway := testGateway(t, 4096, wrong)
	req := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{put("b", "one", false)}}
	response, err := collectWrite(t.Context(), gateway, req)
	if err != nil {
		t.Fatal(err)
	}
	if response.Results[0].Status != sink.WriteStatus_WRITE_STATUS_FAILED || response.Results[0].GetFailure().GetRetryable() {
		t.Fatal(response)
	}
	// A core bound to one Store rejects a mixed batch before touching its storage.
	mixed := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{put("a", "one", false), put("b", "two", false)}}
	if _, err := collectWrite(t.Context(), a.core, mixed); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	one := &sink.ReadOperation{Address: address("a", "one")}
	read := &sink.ReadRequest{Operations: []*sink.ReadOperation{one}}
	result, err := collectRead(t.Context(), a.core, read)
	if err != nil || result.Results[0].Status != sink.ReadStatus_READ_STATUS_NOT_FOUND {
		t.Fatalf("misrouted batch wrote a record: %v %v", result, err)
	}
}

type controlledEngine struct {
	forward.UnimplementedEngineServer
	store   string
	calls   atomic.Int64
	entered chan struct{}
	release chan struct{}
	lost    bool
}

func (e *controlledEngine) Forward(req *forward.ForwardRequest, stream grpc.ServerStreamingServer[forward.ForwardResponse]) error {
	ctx := stream.Context()
	e.calls.Add(1)
	if e.entered != nil {
		select {
		case e.entered <- struct{}{}:
		default:
		}
	}
	if e.release != nil {
		select {
		case <-e.release:
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		}
	}
	if e.lost {
		return status.Error(codes.Unavailable, "reply lost after mutation")
	}
	for i := range req.GetWrite().GetOperations() {
		result := &sink.WriteResult{OperationIndex: uint32(i), Status: sink.WriteStatus_WRITE_STATUS_APPLIED}
		written := &sink.WriteResponse{Results: []*sink.WriteResult{result}}
		frame := &forward.ForwardResponse{Version: forwarding.Version, Store: e.store, Response: &forward.ForwardResponse_Write{Write: written}}
		if err := stream.Send(frame); err != nil {
			return err
		}
	}
	return nil
}

func TestLostMutationReplyIsNotReplayedAndKeepsOtherSuccess(t *testing.T) {
	lost := &controlledEngine{store: "a", lost: true}
	a := fixtureEngine{store: "a", target: serveEngine(t, lost)}
	b := testEngine(t, "b", 4096)
	gateway := testGateway(t, 4096, a, b)
	request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{put("a", "one", false), put("b", "two", false)}}
	response, err := collectWrite(t.Context(), gateway, request)
	if err != nil {
		t.Fatal(err)
	}
	if lost.calls.Load() != 1 || !strings.Contains(response.Results[0].GetFailure().GetMessage(), "outcome is unknown") || response.Results[0].GetFailure().GetRetryable() || response.Results[1].Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
		t.Fatalf("unsafe replay or lost partial success: %v", response)
	}
}

func TestConnectionsAreLazyBoundedAndExpire(t *testing.T) {
	a := testEngine(t, "a", 4096)
	b := testEngine(t, "b", 4096)
	gateway := testGateway(t, 4096, a, b)
	if len(gateway.pool.entries) != 0 {
		t.Fatal("connections were eagerly opened")
	}
	gateway.pool.maximum = 1
	routeA := gateway.current.routes["a"]
	routeB := gateway.current.routes["b"]
	first, err := gateway.pool.acquire(routeA)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gateway.pool.acquire(routeB); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("active connection evicted", err)
	}
	gateway.pool.release(first)
	second, err := gateway.pool.acquire(routeB)
	if err != nil {
		t.Fatal(err)
	}
	gateway.pool.release(second)
	gateway.pool.mu.Lock()
	second.idleSince = time.Now().Add(-time.Hour)
	gateway.pool.mu.Unlock()
	gateway.pool.expire()
	if len(gateway.pool.entries) != 0 {
		t.Fatal("idle connection not expired")
	}
}

func TestGatewayAdmissionAndConcurrentBudgets(t *testing.T) {
	a := testEngine(t, "a", 200)
	b := testEngine(t, "b", 200)
	gateway := testGateway(t, 200+2*1280, a, b)
	var work sync.WaitGroup
	for i := range 24 {
		work.Go(func() {
			request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{put("a", fmt.Sprint(i), true), put("b", fmt.Sprint(i), true)}}
			response, err := collectWrite(t.Context(), gateway, request)
			if err != nil {
				t.Error(err)
				return
			}
			if response.Results[0].Status != sink.WriteStatus_WRITE_STATUS_APPLIED || response.Results[1].Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
				t.Error(response)
			}
		})
	}
	work.Wait()

}

func TestGatewayRoutesChangeOnlyAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	initial := fixtureEngine{store: "a", target: "127.0.0.1:1"}
	if err := os.WriteFile(path, []byte(routeText(initial)), 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := config.Load(path, "")
	if err != nil {
		t.Fatal(err)
	}
	opts := Options{Gateway: loaded.Gateway, Request: loaded.Service.Request, MaxMessageBytes: loaded.GRPC.MaxSendMessageBytes}
	running, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer running.Close()
	replacement := fixtureEngine{store: "a", target: "127.0.0.1:2"}
	if err := os.WriteFile(path, []byte(routeText(replacement)), 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err = config.Load(path, "")
	if err != nil {
		t.Fatal(err)
	}
	opts.Gateway = loaded.Gateway
	restarted, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if running.current.routes["a"].Target != initial.target || restarted.current.routes["a"].Target != replacement.target {
		t.Fatal("configuration update must affect only the restarted Gateway")
	}
	opts.Gateway.Routes[0].Target = "127.0.0.1:3"
	if restarted.current.routes["a"].Target != replacement.target {
		t.Fatal("Gateway did not retain its own immutable route snapshot")
	}
}

type nativeFixture struct {
	sink.UnimplementedSinkServer
	cancelled chan struct{}
}

func (n *nativeFixture) Execute(_ context.Context, request *sink.ExecuteRequest) (*sink.ExecuteResponse, error) {
	response := &sink.ExecuteResponse{Success: true, Payload: request.GetCommand().GetPayload(), ContentType: "application/json"}
	return response, nil
}

func (n *nativeFixture) queryResponse(_ context.Context, request *sink.QueryRequest) (*sink.QueryResponse, error) {
	document := &sink.Document{Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_JSON, Payload: []byte(`{"ok":true}`)}
	response := &sink.QueryResponse{Documents: []*sink.Document{document}, HasMore: request.GetPage() == 2}
	return response, nil
}

func (n *nativeFixture) Count(ctx context.Context, request *sink.CountRequest) (*sink.CountResponse, error) {
	if string(request.GetCommand().GetPayload()) == "wait" {
		<-ctx.Done()
		close(n.cancelled)
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	response := &sink.CountResponse{Count: 42}
	return response, nil
}

func (n *nativeFixture) scanResponse(_ context.Context, _ *sink.ScanRequest) (*sink.ScanResponse, error) {
	detail := &errdetails.ErrorInfo{Reason: "admission-marker", Domain: "sink"}
	annotated, err := status.New(codes.ResourceExhausted, "admission full").WithDetails(detail)
	if err != nil {
		return nil, err
	}
	return nil, annotated.Err()
}

func TestNativeForwardingPreservesDetailsAndCancellation(t *testing.T) {
	native := &nativeFixture{cancelled: make(chan struct{})}
	opts := engine.Options{Service: native, Store: "a", MaxReadBytes: 4096}
	backend, err := engine.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	fixture := fixtureEngine{store: "a", target: serveEngine(t, backend)}
	gateway := testGateway(t, 4096, fixture)
	command := &sink.Command{Uri: "sink://a/custom/tenant/partition", Payload: []byte(`{"ok":true}`)}
	execute := &sink.ExecuteRequest{Command: command}
	executed, err := gateway.Execute(t.Context(), execute)
	if err != nil || !executed.GetSuccess() || string(executed.GetPayload()) != string(command.Payload) {
		t.Fatalf("Execute: %v %v", executed, err)
	}
	query := &sink.QueryRequest{Command: command, Page: 2}
	queried, err := collectQuery(t.Context(), gateway, query)
	if err != nil || !queried.GetHasMore() || len(queried.GetDocuments()) != 1 {
		t.Fatalf("Query: %v %v", queried, err)
	}
	count := &sink.CountRequest{Command: command}
	counted, err := gateway.Count(t.Context(), count)
	if err != nil || counted.GetCount() != 42 {
		t.Fatalf("Count: %v %v", counted, err)
	}
	scan := &sink.ScanRequest{Command: command}
	_, err = collectScan(t.Context(), gateway, scan)
	details := status.Convert(err).Details()
	if status.Code(err) != codes.ResourceExhausted || len(details) != 1 || details[0].(*errdetails.ErrorInfo).Reason != "admission-marker" {
		t.Fatalf("lost status detail: %v", err)
	}
	waiting := &sink.Command{Uri: "sink://a", Payload: []byte("wait")}
	count.Command = waiting
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if _, err := gateway.Count(ctx, count); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("deadline not propagated: %v", err)
	}
	select {
	case <-native.cancelled:
	case <-time.After(time.Second):
		t.Fatal("Engine retained cancelled work")
	}
}

func TestSlowForwardingDoesNotBlockHealthyStore(t *testing.T) {
	held := &controlledEngine{store: "a", entered: make(chan struct{}, 1), release: make(chan struct{})}
	a := fixtureEngine{store: "a", target: serveEngine(t, held)}
	b := testEngine(t, "b", 4096)
	gateway := testGateway(t, 4096, a, b)
	request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{put("a", "held", false)}}
	done := make(chan error, 1)
	go func() { _, err := collectWrite(t.Context(), gateway, request); done <- err }()
	select {
	case <-held.entered:
	case <-time.After(time.Second):
		t.Fatal("slow Store did not start")
	}
	healthy := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{put("b", "healthy", false)}}
	response, err := collectWrite(t.Context(), gateway, healthy)
	close(held.release)
	if firstErr := <-done; firstErr != nil {
		t.Fatal(firstErr)
	}
	if err != nil || response.GetResults()[0].GetStatus() != sink.WriteStatus_WRITE_STATUS_APPLIED {
		t.Fatalf("healthy Store blocked: %v %v", response, err)
	}

}

func TestFanoutIsBounded(t *testing.T) {
	entered := make(chan struct{}, 3)
	release := make(chan struct{})
	var fixtures []fixtureEngine
	for _, store := range []string{"a", "b", "c"} {
		backend := &controlledEngine{store: store, entered: entered, release: release}
		fixture := fixtureEngine{store: store, target: serveEngine(t, backend)}
		fixtures = append(fixtures, fixture)
	}
	gateway := testGateway(t, 4096, fixtures...)
	request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{put("a", "one", false), put("b", "two", false), put("c", "three", false)}}
	done := make(chan error, 1)
	go func() { _, err := collectWrite(t.Context(), gateway, request); done <- err }()
	for range 2 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("fanout failed to start")
		}
	}
	select {
	case <-entered:
		t.Error("fanout exceeded configured limit")
	default:
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestEngineRejectsStaleProtocolAndMismatchedStoreBeforeWrites(t *testing.T) {
	a := testEngine(t, "a", 4096)
	opts := engine.Options{Service: a.core.RPC(), Store: "a", MaxReadBytes: 4096}
	backend, err := engine.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		version uint32
		store   string
		body    string
		code    codes.Code
	}{
		{name: "previous protocol", version: 1, store: "a", body: "a", code: codes.FailedPrecondition},
		{name: "wrong envelope Store", version: forwarding.Version, store: "b", body: "a", code: codes.FailedPrecondition},
		{name: "wrong operation Store", version: forwarding.Version, store: "a", body: "b", code: codes.InvalidArgument},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			write := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{put(test.body, test.name, false)}}
			body := &forward.ForwardRequest_Write{Write: write}
			request := &forward.ForwardRequest{Version: test.version, Store: test.store, Request: body}
			output := &collectorStream[forward.ForwardResponse]{ctx: t.Context()}
			var response *forward.ForwardResponse
			output.emit = func(frame *forward.ForwardResponse) error { response = frame; return nil }
			err := backend.Forward(request, output)
			marker := output.trailer.Get(forwarding.NotStartedTrailer)
			if status.Code(err) != test.code || response != nil || len(marker) != 1 || marker[0] != "true" {
				t.Fatalf("request was not rejected before execution: %v, %v", response, err)
			}
			op := &sink.ReadOperation{Address: address("a", test.name)}
			read := &sink.ReadRequest{Operations: []*sink.ReadOperation{op}}
			result, err := collectRead(t.Context(), a.core, read)
			if err != nil || result.Results[0].Status != sink.ReadStatus_READ_STATUS_NOT_FOUND {
				t.Fatalf("rejected request wrote a record: %v, %v", result, err)
			}
		})
	}
}

func (n *nativeFixture) Query(req *sink.QueryRequest, stream grpc.ServerStreamingServer[sink.QueryResponse]) error {
	response, err := n.queryResponse(stream.Context(), req)
	if err != nil {
		return err
	}
	for _, document := range response.Documents {
		frame := &sink.QueryResponse{Documents: []*sink.Document{document}}
		if err := stream.Send(frame); err != nil {
			return err
		}
	}
	final := &sink.QueryResponse{Complete: true, HasMore: response.HasMore}
	return stream.Send(final)
}

func (n *nativeFixture) Scan(req *sink.ScanRequest, stream grpc.ServerStreamingServer[sink.ScanResponse]) error {
	response, err := n.scanResponse(stream.Context(), req)
	if err != nil {
		return err
	}
	for _, document := range response.Documents {
		frame := &sink.ScanResponse{Documents: []*sink.Document{document}}
		if err := stream.Send(frame); err != nil {
			return err
		}
	}
	final := &sink.ScanResponse{Complete: true, NextCursor: response.NextCursor}
	return stream.Send(final)
}

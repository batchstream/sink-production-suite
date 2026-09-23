package service_test

import (
	"context"
	"net"
	"testing"
	"time"

	sink "github.com/batchstream/sink-protocol/sink/v1"
	"github.com/batchstream/sink/internal/backpressure"
	"github.com/batchstream/sink/internal/merge"
	"github.com/batchstream/sink/internal/protocol"
	"github.com/batchstream/sink/internal/service"
	"github.com/batchstream/sink/internal/storage/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func nativeRPCFixture(t *testing.T, silent bool, admission ...*backpressure.Controller) (sink.SinkClient, *nativeFixtureStorage) {
	t.Helper()
	backend := &nativeFixtureStorage{Store: memory.New(), stopped: make(chan struct{}), started: make(chan struct{}), silent: silent}
	luaOptions := merge.LuaOptions{}
	lua, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		t.Fatal(err)
	}
	options := service.Options{BoundStore: "primary", Storage: backend, Lua: lua, MaxReadBytes: 4096}
	if len(admission) > 0 {
		options.Admission = admission[0]
	}
	core, err := service.New(options)
	if err != nil {
		t.Fatal(err)
	}
	batchOptions := service.BatchingOptions{}
	server, err := service.NewBatchingServer(core, batchOptions)
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1 << 20)
	codec := protocol.NewVTProtoCodec()
	grpcServer := grpc.NewServer(grpc.ForceServerCodecV2(codec))
	sink.RegisterSinkServer(grpcServer, server.RPC())
	go func() { _ = grpcServer.Serve(listener) }()
	dialer := func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }
	connection, err := grpc.NewClient("passthrough:///native", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(dialer))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close(); grpcServer.Stop(); server.Close(); _ = listener.Close() })
	return sink.NewSinkClient(connection), backend
}

func nativeSearchRequest() *sink.ExecuteRequest {
	command := &sink.Command{Uri: "sink://primary", Method: "POST", Path: "/products/_search", ContentType: "application/json", Payload: []byte(`{}`)}
	request := &sink.ExecuteRequest{Command: command}
	return request
}

func TestNativeRPCProgressesAlongsideCanceledScan(t *testing.T) {
	client, backend := nativeRPCFixture(t, true)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	request := nativeSearchRequest()
	scan := &sink.ScanRequest{Command: request.Command, BatchSize: 1}
	finished := make(chan error, 1)
	go func() { _, err := collectScan(ctx, client, scan); finished <- err }()
	select {
	case <-backend.started:
	case <-time.After(time.Second):
		t.Fatal("scan did not start")
	}
	_, err := client.Execute(t.Context(), request)
	if err != nil {
		t.Fatalf("independent native request failed: %v", err)
	}
	cancel()
	if err := <-finished; status.Code(err) != codes.Canceled {
		t.Fatalf("cancel=%v", err)
	}
	select {
	case <-backend.stopped:
	case <-time.After(time.Second):
		t.Fatal("canceled scan retained backend resources")
	}
	response, err := client.Execute(t.Context(), request)
	if err != nil || response.GetStatusCode() != 400 || response.GetSuccess() || len(response.GetHeaders()[0].GetValues()) != 2 {
		t.Fatalf("native result=%v err=%v", response, err)
	}
}

func TestNativeScanUsesCallerDeadline(t *testing.T) {
	client, backend := nativeRPCFixture(t, true)
	request := &sink.ScanRequest{Command: nativeSearchRequest().Command}
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	_, err := collectScan(ctx, client, request)
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("scan error=%v", err)
	}
	select {
	case <-backend.stopped:
	case <-time.After(time.Second):
		t.Fatal("timed-out scan retained resources")
	}
}

func TestNativeScanReturnsOneUnaryPageAndReleasesAdmission(t *testing.T) {
	client, _ := nativeRPCFixture(t, false)
	request := &sink.ScanRequest{Command: nativeSearchRequest().Command}
	for range 3 {
		page, err := collectScan(t.Context(), client, request)
		if err != nil || len(page.GetDocuments()) != 1 || len(page.GetNextCursor()) != 0 {
			t.Fatalf("page=%v err=%v", page, err)
		}
	}
	request.Cursor = []byte("invalid")
	_, err := collectScan(t.Context(), client, request)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid cursor=%v", err)
	}
}

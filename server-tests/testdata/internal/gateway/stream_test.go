package gateway

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/batchstream/sink-go/uri"
	forward "github.com/batchstream/sink/gen/forward"
	sink "github.com/batchstream/sink/gen/sink"
	"github.com/batchstream/sink/internal/config"
	"github.com/batchstream/sink/internal/engine"
	"github.com/batchstream/sink/internal/merge"
	"github.com/batchstream/sink/internal/service"
	"github.com/batchstream/sink/internal/storage"
	"github.com/batchstream/sink/internal/storage/memory"
	"github.com/batchstream/sink/internal/testuri"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestTypedForwardingStreamsMoreThanOneMessageBudget(t *testing.T) {
	backend := memory.New()
	request := &sink.ReadRequest{}
	payload := []byte(`{"padding":"` + strings.Repeat("x", 32<<10) + `"}`)
	for i := range 96 {
		address := testuri.Address("primary", []string{"db", "items"}, uri.StringKey(fmt.Sprint(i)))
		document := storage.Document{Encoding: storage.DocumentEncodingJSON, Payload: payload}
		seed := memory.SeedRequest{Address: address, Document: document}
		backend.Seed(seed)
		record := &sink.RecordAddress{Uri: address.String()}
		operation := &sink.ReadOperation{Address: record}
		request.Operations = append(request.Operations, operation)
	}
	luaOptions := merge.LuaOptions{}
	lua, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		t.Fatal(err)
	}
	coreOptions := service.Options{BoundStore: "primary", Storage: backend, Lua: lua, MaxReadBytes: 256 << 10}
	core, err := service.New(coreOptions)
	if err != nil {
		t.Fatal(err)
	}
	engineOptions := engine.Options{Service: core.RPC(), Store: "primary", MaxReadBytes: 256 << 10}
	executor, err := engine.New(engineOptions)
	if err != nil {
		t.Fatal(err)
	}
	private, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	privateServer := grpc.NewServer()
	forward.RegisterEngineServer(privateServer, executor)
	t.Cleanup(privateServer.Stop)
	go privateServer.Serve(private)
	component := "mode: gateway\nforwarding:\n  routes: [{store: primary, target: '" + private.Addr().String() + "', tls: {insecure: true}}]"
	loaded, err := config.Decode(strings.NewReader(component), nil)
	if err != nil {
		t.Fatal(err)
	}
	loaded.Service.Request.MaxReadBytes = 256 << 10
	opts := Options{Gateway: loaded.Gateway, Request: loaded.Service.Request, MaxMessageBytes: 256 << 10, MaxResponseBytes: 256 << 10}
	gateway, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(gateway.Close)
	public, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	publicServer := grpc.NewServer(grpc.MaxSendMsgSize(256 << 10))
	sink.RegisterSinkServer(publicServer, gateway)
	t.Cleanup(publicServer.Stop)
	go publicServer.Serve(public)
	conn, err := grpc.NewClient(public.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	client := sink.NewSinkClient(conn)
	stream, err := client.Read(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if len(frame.Results) != 1 || frame.Results[0].OperationIndex != uint32(count) || frame.Results[0].GetStatus() != sink.ReadStatus_READ_STATUS_FOUND {
			t.Fatalf("invalid relay result %v", frame)
		}
		count++
	}
	if count != 96 {
		t.Fatalf("received %d of 96 results", count)
	}
	if _, exists := privateServer.GetServiceInfo()["sink.v1.Sink"]; exists {
		t.Fatal("Engine exposed a public compatibility service")
	}
}

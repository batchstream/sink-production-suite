package service_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/protocol"
	"github.com/liran/sink/internal/storage/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestBatchingServerAggregatesAcrossGRPCRPCs(t *testing.T) {
	const requestCount = 32
	backend := memory.New()
	for index := range requestCount {
		seed := memory.SeedRequest{
			Address:  storageAddress(batchTestKey(index)),
			Document: storageJSONDocument(`{"value":"grpc"}`),
		}
		backend.Seed(seed)
	}
	observed := &countingStorage{backend: backend}
	server := newBatchingTestServer(t, observed, nil, requestCount)
	listener := bufconn.Listen(1 << 20)
	vtCodec := protocol.NewVTProtoCodec()
	codecOption := grpc.ForceServerCodecV2(vtCodec)
	grpcServer := grpc.NewServer(codecOption)
	sink.RegisterSinkServer(grpcServer, server)
	serveErrors := make(chan error, 1)
	go func() {
		serveErrors <- grpcServer.Serve(listener)
	}()

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return listener.DialContext(ctx)
	}
	dialOption := grpc.WithContextDialer(dialer)
	credentialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	connection, err := grpc.NewClient("passthrough:///sink", dialOption, credentialOption)
	if err != nil {
		grpcServer.Stop()
		server.Close()
		_ = listener.Close()
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	t.Cleanup(func() {
		_ = connection.Close()
		grpcServer.Stop()
		server.Close()
		_ = listener.Close()
		select {
		case <-serveErrors:
		case <-time.After(5 * time.Second):
			t.Error("gRPC server did not stop")
		}
	})
	client := sink.NewSinkClient(connection)

	var waitGroup sync.WaitGroup
	errors := make(chan error, requestCount)
	start := make(chan struct{})
	waitGroup.Add(requestCount)
	for index := range requestCount {
		go func() {
			defer waitGroup.Done()
			<-start
			response, readErr := client.Read(t.Context(), readRequest(batchTestKey(index)))
			if readErr == nil && response.GetResults()[0].GetStatus() != sink.ReadStatus_READ_STATUS_FOUND {
				readErr = errUnexpectedStatus(response.GetResults()[0].GetStatus())
			}
			errors <- readErr
		}()
	}
	close(start)
	waitGroup.Wait()
	close(errors)
	assertNoBatchErrors(t, errors)
	if observed.readCalls.Load() != 1 || observed.maxReadOperations.Load() != requestCount {
		t.Fatalf("storage reads = %d, max operations = %d", observed.readCalls.Load(), observed.maxReadOperations.Load())
	}
}

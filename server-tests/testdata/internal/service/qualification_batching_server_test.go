package service_test

import (
	"github.com/liran/sink/internal/testuri"

	"context"
	"net"
	"sync"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/protocol"
	"github.com/liran/sink/internal/service"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type latencyStorage struct {
	backend     storage.Storage
	delay       time.Duration
	concurrency chan struct{}
}

func (s *latencyStorage) Ping(ctx context.Context) error {
	return s.backend.Ping(ctx)
}

func (s *latencyStorage) Read(ctx context.Context, req storage.ReadRequest) (storage.ReadResponse, error) {
	select {
	case s.concurrency <- struct{}{}:
		defer func() { <-s.concurrency }()
	case <-ctx.Done():
		var response storage.ReadResponse
		return response, ctx.Err()
	}
	timer := time.NewTimer(s.delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return s.backend.Read(ctx, req)
	case <-ctx.Done():
		var response storage.ReadResponse
		return response, ctx.Err()
	}
}

func (s *latencyStorage) Write(ctx context.Context, req storage.WriteRequest) (storage.WriteResponse, error) {
	return s.backend.Write(ctx, req)
}

func (s *latencyStorage) Delete(ctx context.Context, req storage.DeleteRequest) (storage.DeleteResponse, error) {
	return s.backend.Delete(ctx, req)
}

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

func BenchmarkSingleOperationReads(b *testing.B) {
	b.Run("direct", func(b *testing.B) {
		backend := &latencyStorage{
			backend:     memory.New(),
			delay:       500 * time.Microsecond,
			concurrency: make(chan struct{}, 8),
		}
		observed := &countingStorage{backend: backend}
		server := newTestServer(b, observed, nil)
		request := readRequest("benchmark")
		b.SetParallelism(16)
		b.ResetTimer()
		b.RunParallel(func(parallel *testing.PB) {
			for parallel.Next() {
				if _, err := server.Read(context.Background(), request); err != nil {
					b.Errorf("Read() error = %v", err)
				}
			}
		})
		b.StopTimer()
		b.ReportMetric(float64(observed.readCalls.Load())/float64(b.N), "storage_calls/op")
	})

	b.Run("server_batched", func(b *testing.B) {
		backend := &latencyStorage{
			backend:     memory.New(),
			delay:       500 * time.Microsecond,
			concurrency: make(chan struct{}, 8),
		}
		observed := &countingStorage{backend: backend}
		core := newTestServer(b, observed, nil)
		options := service.BatchingOptions{
			MaxWait:             100 * time.Microsecond,
			MaxOperations:       1000,
			MaxBytes:            16 << 20,
			MaxQueuedOperations: 10_000,
			MaxQueuedBytes:      128 << 20,
		}
		server, err := service.NewBatchingServer(core, options)
		if err != nil {
			b.Fatalf("NewBatchingServer() error = %v", err)
		}
		defer server.Close()
		request := readRequest("benchmark")
		b.SetParallelism(16)
		b.ResetTimer()
		b.RunParallel(func(parallel *testing.PB) {
			for parallel.Next() {
				if _, err := server.Read(context.Background(), request); err != nil {
					b.Errorf("Read() error = %v", err)
				}
			}
		})
		b.StopTimer()
		b.ReportMetric(float64(observed.readCalls.Load())/float64(b.N), "storage_calls/op")
	})
}

func (s *latencyStorage) BatchKey(address storage.Address) (string, error) {
	return testuri.BatchKey(address)
}

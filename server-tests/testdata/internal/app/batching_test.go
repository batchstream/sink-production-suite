package app

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/batchstream/sink/internal/testuri"
	"github.com/liran/sink-go/uri"

	sink "github.com/batchstream/sink/gen/sink"
	"github.com/batchstream/sink/internal/config"
	"github.com/twmb/franz-go/pkg/kfake"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestApplicationAlwaysBatchesGRPCRequests(t *testing.T) {
	for _, kafkaEnabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("kafka=%t", kafkaEnabled), func(t *testing.T) {
			kafkaConfig := ""
			if kafkaEnabled {
				broker, err := kfake.NewCluster(kfake.NumBrokers(1))
				if err != nil {
					t.Fatal(err)
				}
				defer broker.Close()
				kafkaConfig = fmt.Sprintf(`kafka:
  enabled: true
  brokers: [%q]
  replication_factor: 1
  min_insync_replicas: 1
  topic:
    name: batching-test
`, broker.ListenAddrs()[0])
			}
			var calls atomic.Int32
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/_mget" {
					http.NotFound(w, r)
					return
				}
				calls.Add(1)
				var body struct {
					Documents []map[string]any `json:"docs"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Errorf("decode backend request: %v", err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				if len(body.Documents) != 2 {
					t.Errorf("backend received %d operations, want two coalesced RPCs", len(body.Documents))
				}
				for _, document := range body.Documents {
					document["found"] = false
				}
				w.Header().Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(body); err != nil {
					t.Errorf("encode backend response: %v", err)
				}
			}))
			defer backend.Close()

			contents := fmt.Sprintf(`mode: %s
grpc:
  address: "127.0.0.1:0"
health: {address: '127.0.0.1:0'}
batching:
  max_operations: 2
  max_wait: 1000ms
`, config.ModeEngine)
			shared := fmt.Sprintf("name: primary\nstorage: {driver: opensearch, search: {endpoints: [%q]}}\n%s", backend.URL, kafkaConfig)
			path := writeConfig(t, contents)
			storePath := writeConfig(t, shared)
			loaded, err := config.Load(path, storePath)
			if err != nil {
				t.Fatal(err)
			}
			options := Options{Config: loaded, Version: "test"}
			app, err := New(t.Context(), options)
			if err != nil {
				t.Fatal(err)
			}
			serveErrors := make(chan error, 1)
			go func() { serveErrors <- app.grpcServer.Serve(app.listener) }()
			defer func() {
				app.Close()
				if err := <-serveErrors; err != nil {
					t.Errorf("serve gRPC: %v", err)
				}
			}()
			gatewayConfig := fmt.Sprintf("mode: gateway\ngrpc: {address: '127.0.0.1:0'}\nhealth: {address: '127.0.0.1:0'}\nforwarding: {routes: [{store: primary, target: %q, tls: {insecure: true}}]}", app.listener.Addr().String())
			gatewayLoaded, err := config.Decode(strings.NewReader(gatewayConfig), nil)
			if err != nil {
				t.Fatal(err)
			}
			gatewayOptions := Options{Config: gatewayLoaded, Version: "test"}
			gateway, err := New(t.Context(), gatewayOptions)
			if err != nil {
				t.Fatal(err)
			}
			gatewayErrors := make(chan error, 1)
			go func() { gatewayErrors <- gateway.grpcServer.Serve(gateway.listener) }()
			defer func() {
				gateway.Close()
				if err := <-gatewayErrors; err != nil {
					t.Error(err)
				}
			}()
			connection, err := grpc.NewClient(gateway.listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			client := sink.NewSinkClient(connection)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			var callers sync.WaitGroup
			for index := range 2 {
				callers.Go(func() {
					keyValue := uri.StringKey(fmt.Sprintf("key-%d", index))
					key := keyValue
					address := &sink.RecordAddress{Uri: testuri.Record("primary", []string{"items"}, key)}
					operation := &sink.ReadOperation{Address: address}
					request := &sink.ReadRequest{Operations: []*sink.ReadOperation{operation}}
					response, err := collectRead(ctx, client, request)
					if err != nil {
						t.Errorf("read: %v", err)
						return
					}
					if len(response.GetResults()) != 1 || response.GetResults()[0].GetStatus() != sink.ReadStatus_READ_STATUS_NOT_FOUND {
						t.Errorf("read response lost its RPC boundary: %v", response)
					}
				})
			}
			callers.Wait()
			if count := calls.Load(); count != 1 {
				t.Fatalf("backend read calls = %d, want one batch", count)
			}
		})
	}
}

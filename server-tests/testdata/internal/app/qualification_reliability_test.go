package app

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/liran/sink/internal/config"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestUnavailableStoreDoesNotBlockStartup(t *testing.T) {
	unavailable := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
	defer unavailable.Close()
	contents := fmt.Sprintf(`mode: engine
grpc:
  address: "127.0.0.1:0"
storage:
  name: failed
  driver: opensearch
  search:
    endpoints: [%q]
`, unavailable.URL)
	loaded, err := config.Decode(strings.NewReader(contents))
	if err != nil {
		t.Fatal(err)
	}
	options := Options{Config: loaded, Version: "test"}
	app, err := New(t.Context(), options)
	if err != nil {
		t.Fatalf("dependency outage prevented startup: %v", err)
	}
	defer app.Close()
	app.health = health.NewServer()
	app.updateHealth(t.Context())
	assertHealthStatus(t, app.health, storageHealthService("failed"), healthpb.HealthCheckResponse_NOT_SERVING)
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	response := httptest.NewRecorder()
	app.serveReadiness(response, request)
	if response.Code != http.StatusOK {
		t.Fatal("dependency outage prevented process readiness")
	}
}

func TestMongoClientStartsWithoutRequiringAvailability(t *testing.T) {
	mongoConfig := config.MongoDB{URI: "mongodb://127.0.0.1:1/?w=1&journal=false"}
	configured := config.Storage{Name: "primary", Driver: config.DriverMongoDB, MongoDB: mongoConfig}
	opened, err := openMongoStorage(t.Context(), configured, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.mongoClient.Disconnect(t.Context())

}

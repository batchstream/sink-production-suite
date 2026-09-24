//go:build integration

package conformance_test

import (
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestSlowStoreDoesNotBlockIndependentWork(t *testing.T) {
	rounds := 6
	if raw := os.Getenv("SINK_SATURATION_ROUNDS"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 2 || value > 1000 {
			t.Fatal("SINK_SATURATION_ROUNDS must be between 2 and 1000")
		}
		rounds = value
	}
	testMemoryStoreSaturation(t, rounds)
}

func (c *candidate) metricSnapshot(t *testing.T) map[string]float64 {
	t.Helper()
	call := httpCall{endpoint: c.metrics, method: http.MethodGet}
	code, body := request(t, call)
	if code != http.StatusOK {
		t.Fatalf("metrics: HTTP %d", code)
	}
	metrics, err := parseMetrics(body)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"sink_in_flight_requests", "go_goroutines", "go_memstats_heap_alloc_bytes"} {
		if _, ok := metrics[name]; !ok {
			t.Fatalf("required resource metric is missing: %s", name)
		}
	}
	selected := make(map[string]float64)
	for name, value := range metrics {
		if name == "go_goroutines" || name == "go_memstats_heap_alloc_bytes" ||
			strings.HasPrefix(name, "sink_memory_") || strings.HasPrefix(name, "sink_in_flight_requests") || strings.HasPrefix(name, "sink_batcher_queued_") || strings.HasPrefix(name, "sink_store_admission_queued_") {
			selected[name] = value
		}
	}
	return selected
}

func (c *candidate) waitIdle(t *testing.T) map[string]float64 {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var metrics map[string]float64
	for time.Now().Before(deadline) {
		metrics = c.metricSnapshot(t)
		idle := true
		for name, value := range metrics {
			if strings.HasPrefix(name, "sink_memory_") {
				continue
			}
			if strings.HasPrefix(name, "sink_") && value != 0 {
				idle = false
			}
		}
		if idle {
			return metrics
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("cancelled work retained execution/queue capacity: %+v", metrics)
	return nil
}

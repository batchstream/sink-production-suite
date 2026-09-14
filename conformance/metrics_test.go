package conformance_test

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

func parseMetrics(body []byte) (map[string]float64, error) {
	metrics := make(map[string]float64)
	for _, line := range strings.Split(string(body), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 || strings.HasPrefix(fields[0], "#") {
			continue
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			return nil, fmt.Errorf("parse metric %s: %w", fields[0], err)
		}
		metrics[fields[0]] = value
	}
	return metrics, nil
}

// These selectors use the exporter's canonical label order: method/pool then
// store. Retain legacy selectors for qualification of historical server builds.
func metricForStore(metrics map[string]float64, selector string, store string) float64 {
	prefix := selector + "{"
	if strings.HasSuffix(selector, "}") {
		prefix = strings.TrimSuffix(selector, "}") + ","
	}
	name := prefix + "store=" + strconv.Quote(store) + "}"
	if value, exists := metrics[name]; exists {
		return value
	}
	return metrics[selector]
}

func TestMetricForStoreRetainsStoreIsolationAndHistoricalSelectors(t *testing.T) {
	cases := []struct {
		name     string
		body     string
		selector string
		want     float64
	}{
		{"legacy queue", `sink_batcher_queued_operations{method="Write"} 8`, `sink_batcher_queued_operations{method="Write"}`, 8},
		{"store queue", "sink_batcher_queued_operations{method=\"Write\",store=\"primary\"} 8\nsink_batcher_queued_operations{method=\"Write\",store=\"secondary\"} 4", `sink_batcher_queued_operations{method="Write"}`, 8},
		{"other store only", `sink_batcher_queued_operations{method="Write",store="secondary"} 4`, `sink_batcher_queued_operations{method="Write"}`, 0},
		{"other method", `sink_batcher_queued_operations{method="Read",store="primary"} 4`, `sink_batcher_queued_operations{method="Write"}`, 0},
		{"legacy publish", `sink_admission_pool_requests{pool="publish"} 1`, `sink_admission_pool_requests{pool="publish"}`, 1},
		{"store publish", `sink_admission_pool_requests{pool="publish",store="primary"} 1`, `sink_admission_pool_requests{pool="publish"}`, 1},
		{"other pool", `sink_admission_pool_requests{pool="execution",store="primary"} 1`, `sink_admission_pool_requests{pool="publish"}`, 0},
		{"legacy batch count", `sink_batcher_operations_count{method="Write"} 2`, `sink_batcher_operations_count{method="Write"}`, 2},
		{"store batch count", `sink_batcher_operations_count{method="Write",store="primary"} 2`, `sink_batcher_operations_count{method="Write"}`, 2},
		{"store zero takes precedence", "sink_batcher_queued_operations{method=\"Write\",store=\"primary\"} 0\nsink_batcher_queued_operations{method=\"Write\"} 8", `sink_batcher_queued_operations{method="Write"}`, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			metrics, err := parseMetrics([]byte(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			if got := metricForStore(metrics, tc.selector, "primary"); got != tc.want {
				t.Fatalf("metric = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestParseMetricsRetainsResourceValuesAndRejectsInvalidSamples(t *testing.T) {
	body := []byte("# HELP sink_in_flight_requests Executing calls\n# TYPE sink_in_flight_requests gauge\nsink_in_flight_requests 2\ngo_memstats_heap_alloc_bytes 1.5e+06\n")
	metrics, err := parseMetrics(body)
	if err != nil || metrics["sink_in_flight_requests"] != 2 || metrics["go_memstats_heap_alloc_bytes"] != 1.5e6 || len(metrics) != 2 {
		t.Fatalf("metrics = %v, error = %v", metrics, err)
	}
	invalid := []byte("sink_in_flight_requests broken\n")
	if _, err := parseMetrics(invalid); err == nil {
		t.Fatal("invalid sample was accepted")
	}
}

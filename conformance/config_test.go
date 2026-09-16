//go:build integration

package conformance_test

import (
	"fmt"
	"strings"
	"time"
)

type admissionQueueOptions struct {
	requests int
	bytes    int
	wait     time.Duration
}

func candidateExecutionConfig(opts serverOptions) string {
	var config strings.Builder
	if opts.executionBytes != 0 {
		fmt.Fprintf(&config, "    max_bytes: %s\n", readableByteSize(opts.executionBytes))
	}
	if queue := opts.admissionQueue; queue != nil {
		fmt.Fprintf(&config, "    queue:\n      max_requests: %d\n      max_bytes: %s\n      max_wait: %s\n",
			queue.requests, readableByteSize(queue.bytes), queue.wait)
	}
	if opts.scanWait != 0 {
		fmt.Fprintf(&config, "    scan:\n      admission_wait: %s\n", opts.scanWait)
	}
	return config.String()
}

func readableByteSize(value int) string {
	for _, unit := range []struct {
		name  string
		bytes int
	}{
		{name: "GiB", bytes: 1 << 30},
		{name: "MiB", bytes: 1 << 20},
		{name: "KiB", bytes: 1 << 10},
	} {
		if value >= unit.bytes && value%unit.bytes == 0 {
			return fmt.Sprintf("%d%s", value/unit.bytes, unit.name)
		}
	}
	return fmt.Sprintf("%dB", value)
}

// The final indexed placeholder copies the same process capacity into the
// independent publishing pool, preserving each conformance scenario's limits.
const groupedCandidateConfig = `mode: %s
grpc:
  address: %q
prometheus:
  address: %q
storage:
  name: primary
  driver: %s
  search:
    endpoints: %s
%s
%s
service:
  request:
    timeout: %ds
    max_read_bytes: %s
    max_operations: %d
  execution:
    max_requests: %[12]d
  merge:
    max_attempts: 50
    lua:
      max_instructions: %d
  batching:
    max_operations: %d
    max_wait: %dms
    queue:
      max_operations: %d
  publish:
    max_requests: %[12]d
shutdown_timeout: 2s
`

const groupedCandidateKafka = `  kafka:
    enabled: true
    brokers: [%q]
    topic:
      name: %s
      partitions: 1
      replication_factor: 1
    consumer:
      group_id: %s-workers
      retry:
        backoff: 10ms
        max_backoff: 100ms
    dead_letter:
      topic: %s.dlq`

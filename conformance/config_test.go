//go:build integration

package conformance_test

import "fmt"

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

// The final indexed placeholder copies the same per-store capacity into the
// independent publishing pool, preserving each conformance scenario's limits.
const groupedCandidateConfig = `mode: %s
grpc:
  address: %q
prometheus:
  address: %q
storages:
  - name: primary
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
    max_requests: %d
    max_requests_per_store: %d
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
    max_requests_per_store: %[12]d
shutdown_timeout: 2s
`

const groupedCandidateKafka = `    kafka:
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

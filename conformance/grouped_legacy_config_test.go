//go:build integration

package conformance_test

// Frozen configuration for historical grouped-schema binaries only.
// Current candidates never select this fixture.
const groupedLegacyCandidateConfig = `mode: %s
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

const groupedLegacyCandidateKafka = `    kafka:
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

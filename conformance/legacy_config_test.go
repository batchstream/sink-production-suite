//go:build integration

package conformance_test

// Historical regression proofs run immutable binaries that predate the grouped
// schema. Only check-regression-sensitivity.sh opts into these fixtures; the
// candidate and deployment examples always use the current schema.
const legacyCandidateConfig = `mode: %s
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
  request_timeout_seconds: %d
  max_read_bytes: %d
  max_operations: %d
  max_merge_attempts: 50
  max_in_flight_requests: %d
  max_store_requests: %d
  lua:
    max_instructions: %d
  batching:
    max_operations: %d
    max_wait_milliseconds: %d
    max_queued_operations: %d
shutdown_timeout_seconds: 2
`

const legacyCandidateKafka = `    kafka:
      enabled: true
      brokers: [%q]
      topic: %s
      group_id: %s-workers
      dead_letter_topic: %s.dlq
      topic_partitions: 1
      topic_replication_factor: 1
      retry_backoff_milliseconds: 10
      max_retry_backoff_milliseconds: 100
`

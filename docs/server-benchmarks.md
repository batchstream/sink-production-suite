# Server benchmark measurements

Run these commands from the production-suite checkout, with `SINK_SERVER_DIR`
set to the candidate server. The suite owns the workloads, resource limits and
measurements used to compare revisions.
`BenchmarkSynchronousMergeMicrobatch` isolates batching and adapter round trips
without external services. The opt-in `BenchmarkSynchronousStorage` exercises
the actual gRPC codec, dispatcher, Lua engine, and disposable MongoDB/OpenSearch
backends, then verifies every writer's persisted counter.

### Gateway routing allocations

Compare record-affinity routing across replica counts with:

```shell
SINK_SERVER_DIR=/path/to/sink bash scripts/server-go.sh test ./internal/gateway -run '^$' -bench '^BenchmarkAffinityRoute$' -benchtime=200ms -benchmem -count=5
```

On an Apple M2 with Go 1.27, reusing the existing stack buffer for the record
identity reduced the benchmark's 3, 16, 64, and 256-replica cases from 48 B and
one allocation per route to zero. The single-replica fast path was already
allocation-free. SHA-256 inputs and owner selection remain unchanged; identities
and endpoints exceeding the buffer still use an allocation without truncation.
This isolates routing allocation, not end-to-end deployment throughput.

### Search connection reuse

Compare the default Go HTTP pool with the Store pool using synchronized bursts
of 16 requests against a local HTTP/1.1 backend:

```shell
SINK_SERVER_DIR=/path/to/sink bash scripts/server-go.sh test ./internal/storage/search -run '^$' -bench '^BenchmarkSearchConnectionReuse$' -benchtime=100x -benchmem -count=5
```

On an Apple M2, five 100-burst samples reduced median time from 570 to 241
microseconds per burst and allocation from 309 to 129 KB per burst. After warmup,
new connections fell from 14 per burst to zero. This isolates HTTP connection
reuse; it does not measure database throughput or deployment capacity. Fixed
iteration counts also keep the default-pool comparison from exhausting local
ephemeral ports during longer runs.

### Payload allocation comparisons

Read benchmarks exercise the adapter against a local HTTP backend for search
and the MongoDB driver's offline wire fixture. Compare revisions with:

```shell
SINK_SERVER_DIR=/path/to/sink bash scripts/server-go.sh test ./internal/storage/search ./internal/storage/mongodb -run '^$' -bench '^(BenchmarkSearchReadDocument|BenchmarkMongoReadDocument)$' -benchtime=50x -benchmem -count=5
SINK_SERVER_DIR=/path/to/sink bash scripts/server-go.sh test ./internal/queue -run '^$' -bench '^BenchmarkMutationEnvelope$' -benchtime=100x -benchmem -count=5
```

On an Apple M2, transferring an already-owned document into the first read
result reduced median allocated bytes as follows (five samples):

| Adapter | Document size | Before (B/op) | After (B/op) | Reduction |
| --- | --- | ---: | ---: | ---: |
| Search | 64 KiB | 322,748 | 246,350 | 23.7% |
| Search | 1 MiB | 4,406,891 | 3,346,838 | 24.1% |
| MongoDB | 64 KiB | 386,795 | 306,979 | 20.6% |
| MongoDB | 1 MiB | 5,909,091 | 4,576,522 | 22.6% |

Repeated results still own separate mutable documents and revisions. The read
measurements include the local harness and do not establish database throughput.
Encoding queue messages directly into their final envelope reduced allocations
from two to one and allocated bytes by 50% for 4 KiB, 64 KiB, and 1 MiB payloads.
The queue wire format is unchanged.


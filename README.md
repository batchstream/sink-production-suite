# Sink production qualification suite

This public repository qualifies [Sink](https://github.com/batchstream/sink) against
a representative commerce-indexing workload using only synthetic fixtures,
public dependencies, and disposable local infrastructure. It does not import
proprietary application packages, use production data, require cloud
credentials, or connect to an external Kubernetes cluster.

## Qualification gates

The [reliability contract](docs/reliability-contract.md) defines the public API
invariants, fault-injection oracles and remaining qualification gaps. Tests use
the candidate's current URI protocol and configuration. They cover ordered
Put/Merge folding, independent caller completion, JSON/BSON fidelity, native
queries and Scan checkpoints, process memory watermarks, and at-least-once Kafka
delivery. [Store backpressure qualification](docs/store-backpressure.md) covers
real-process congestion, bounded queues, offset settlement and recovery.

| Command | Coverage |
| --- | --- |
| `make test-coverage` | Infrastructure-free reference models, fuzz seeds, evidence validation, race checks and coverage floors |
| `make test-candidate` | Candidate unit tests plus suite-owned transport and assembly tests, with race checks and combined coverage floors |
| `make test-server-integration` | Disposable MongoDB, Elasticsearch and OpenSearch adapter tests, plus bounded service working sets |
| `make test-conformance` | Public API models and injected failures through actual candidate processes and search backends, plus SDK contract checks |
| `make test-isolated-quickstart` | Matching SDK against Gateway, Engine, Worker, MongoDB and Kafka |
| `make test-production` | Candidate/conformance gates, seven-Store workloads, a six-minute fault workload, recovery, DLQ replay and MongoDB quorum checks |
| `make test-reliability` | Candidate/conformance gates followed by a two-hour fault workload with higher concurrency and repeated disruptions |

Named test events reject missing, skipped and unfinished tests. The suite retains
exact suite/server revisions, tracked diffs, logs, configurations and fault
observations. Ordinary `go test ./...` requires no external infrastructure;
`SINK_SERVER_DIR=/path/to/sink make test-candidate` also runs without Docker.

Release workloads use the product Worker retry defaults. Normal traffic and
temporary outages must leave no DLQ records; a separate permanent CREATE conflict
verifies DLQ inspection, repair and replay with the original source position.
Successful replay does not delete DLQ records. Every business cycle reconciles
stored state and every consumer group must drain. The two-hour run is reserved
for scheduled or explicitly requested qualification; a short run does not prove
sustained behavior or multi-node production reliability.

## Infrastructure

Docker Compose starts all disposable dependencies on the runner:

- MongoDB 8 replica set
- Elasticsearch 8
- OpenSearch 3
- two independent Apache Kafka clusters
- two Gateways, two Engines per Store, and one Worker per async Store
- seven independent database targets; each Store owns exactly one target

No AWS, EKS, persistent cloud volume, KEDA, or private repository is required.
Kubernetes scheduling and autoscaling belong to deployment validation rather
than the public Sink release contract.

## Local usage

Requirements:

- Go version from `go.mod`
- Docker with Compose v2
- a local checkout of Sink

Run unit, race, and lint gates:

```bash
make test-race
make lint
```

`make fuzz` requires the Go fuzzer to finish baseline replay and start generating
mutations. Go can otherwise report PASS after spending the entire time budget
loading cached inputs. If the gate reports no mutation-based fuzzing, increase
`FUZZ_TIME` above the three-minute default, for example `FUZZ_TIME=5m make fuzz`. Each invocation retains its
log in the printed evidence directory; `FUZZ_PARALLEL` controls workers.

Run the backend matrix without load or active fault injection:

```bash
SINK_SERVER_DIR=/path/to/sink make test-integration
```

For unreleased Scan protocol changes, run conformance against matching local
server and Go client checkouts. The SDK replacement uses a temporary module file:

```bash
SINK_SERVER_DIR=/path/to/sink SINK_GO_DIR=/path/to/sink-go make test-conformance
```

The same `SINK_GO_DIR` option applies to the integration and production runners.

Run the complete non-durability release gate:

```bash
SINK_SERVER_DIR=/path/to/sink make test-production
```

Run the same backend, recovery, and reconciliation checks with two hours of
active workload in disposable infrastructure:

```bash
SINK_SERVER_DIR=/path/to/sink make test-reliability
```

This target allows 30 minutes for its expanded conformance prerequisite;
ordinary conformance allows 30 minutes for the Gateway and Engine lifecycles.
`SINK_CONFORMANCE_TEST_TIMEOUT` can override that aggregate test timeout without changing request deadlines or
the two-hour workload duration.

The nightly workflow lives in this repository. Sink can invoke it using the
pinned reusable workflow and an explicit candidate revision. Standard release
qualification can be tuned with `SINK_SOAK_DURATION`, `SINK_SOAK_CONCURRENCY`,
`SINK_SOAK_MIN_CYCLES`, and `SINK_SOAK_TEST_TIMEOUT`; the default fault sequence
needs at least three minutes of scheduled workload.

Run the optional six-hour test against an already running compatible
environment:

```bash
SINK_ADDRESS=127.0.0.1:18080 \
SINK_SECONDARY_ADDRESS=127.0.0.1:18081 \
SINK_BACKEND_STORES='primary:async,secondary:async,sync-only:sync,elasticsearch-sync:sync,elasticsearch-async:async,mongodb-sync:sync,mongodb-async:async' \
SINK_RUN_SOAK=1 \
SINK_SOAK_DURATION=6h \
SINK_SOAK_CONCURRENCY=16 \
SINK_SOAK_MIN_CYCLES=10000 \
  go test -tags=integration ./integration \
  -run '^TestStorageBackendSoak$' \
  -count=1 \
  -timeout=7h \
  -v
```

Each integration run uses a unique Compose project and image name and waits for
process readiness on both Gateways, per-Store Engine capabilities, and Worker readiness before traffic. It removes only its own
Compose resources and volumes after the run, including failures. Local runs need
the documented fixed ports to be free; evidence files remain in the printed
temporary directory. `go test ./...` remains independent of Docker and backends;
real infrastructure tests require the `integration` build tag and the runner.

## Reusable release workflow

The public reusable workflow accepts an immutable Sink tag or commit:

```yaml
jobs:
  qualify:
    uses: batchstream/sink-production-suite/.github/workflows/release-qualification.yml@SUITE_COMMIT
    with:
      suite_ref: SUITE_COMMIT
      sink_ref: SINK_COMMIT
```

It runs race tests, lint, bounded stateful fuzzing, the seven-store backend
matrix, capacity boundaries, controlled recovery, representative load, lag
checks, and dead-letter inspection/repair/replay without repository secrets. Use the same immutable suite commit for the
workflow reference and `suite_ref` so the workflow definition and test source
cannot drift independently.

## Application semantics

**The application owns business idempotence.** Sink provides at-least-once
asynchronous delivery. The fault-soak workload carries an operation ID and
reconciles persisted state; it does not claim Sink automatically deduplicates
arbitrary business operations. Lua output guards are not a strict VM heap quota.
Network partitions across fault domains, full disks, durable replica elections,
and backup restoration still require deployment-specific qualification.

The representative item and offer models intentionally contain bounded
histories and realistic merge behavior. Replay is deterministic and preserves
deduplication and bounds, but it is not byte-idempotent at capped-history
boundaries: replay may rotate bounded history entries. The suite requires exact
agreement between the Go reference and Lua results while separately enforcing
uniqueness and limits.

## License

MIT

## Current Sink configuration

The harness and `deploy/engines/`, `deploy/workers/`, and `deploy/gateway.yaml`
use the Gateway / single-Store Engine / single-Store Worker architecture. Runtime
fixtures use flat role settings and shared `deploy/stores/` files passed through
`--store-config`. All public clients connect through Gateway; Engine exposes only
private forwarding and health. Deadlines belong to callers. See Sink's
[configuration reference](https://github.com/batchstream/sink/blob/main/docs/configuration.md).
Behavioral regression tests run against the candidate build.

## Store-isolated architecture

`make test-isolated` builds the local candidate with the race detector and starts
Gateway plus independent Engines against distinct Elasticsearch/OpenSearch
backends. It verifies all seven public RPCs, cross-Store partial failure and
returned-document budgets. It also accepts a mutation, stops its Engine, then
starts a Worker against real Kafka to prove independent cold-start execution.
The runner uses disposable containers and preserves its evidence directory.

`make test-conformance` includes these scenarios alongside the public protocol
compatibility and storage failure contracts. No published server or SDK upgrade
is required to test a local candidate.

The production runner enables a `conflict` Compose profile with two additional
Gateways whose primary routes each select a different Engine. The explicit
cross-Engine merge test uses these public endpoints to verify revision conflicts
and preservation of every successful update. Ordinary Gateways retain full DNS
membership and record-key affinity; their normal traffic need not produce a
revision conflict. The conflict and exhaustion metrics remain mandatory gates.

Production qualification also scales the primary Worker group 1 → 3 → 0 → 1
during the reconciled workload. It requires each active member to own partitions,
then verifies persisted business state, zero remaining source lag and empty DLQs
before the separate intentional dead-letter case. Temporary one-off replicas
have no published host ports and are removed by the runner's cleanup trap.
`make test-production` uses a six-minute workload for scaling and the fault
cycle; the two-hour reliability profile also includes scaling. For custom runs,
set `SINK_RUN_SCALING=1 SINK_RUN_RESILIENCE=1` and choose sufficient
`SINK_SOAK_DURATION`/`SINK_SOAK_TEST_TIMEOUT` for the requested fault cycles.

Candidate gates require the HTTP shutdown/readiness and retained Gateway batch
snapshot regressions. Conformance requires loopback DNS tests for healthy
membership changes, sufficient/insufficient drain, stale answers, SERVFAIL during
shutdown, all Engines disappearing and recovery. Expected failure cases must
expose failures and recover without hidden mutation replays. They complement
Kubernetes measurements; see Sink's
[rollout timing guide](https://github.com/batchstream/sink/blob/main/docs/rolling-upgrades.md).

`make test-quorum` runs a separate disposable three-member MongoDB replica set
and a bounded Engine. It elects a different primary during continuous writes,
pauses both secondaries to remove the majority, requires no successful write
acknowledgements during a settled outage window, restores quorum and reconciles
all acknowledged state. The workload uses application sequence IDs to tolerate
unknown mutation outcomes. Production and nightly reliability qualification include this check after
the seven-store workload. It does not certify multi-region failures, disk loss
or backup restoration.

MongoDB containers explicitly set `GLIBC_TUNABLES=glibc.pthread.rseq=1`, matching
Sink's quickstart and avoiding the affected TCMalloc per-CPU path on kernels with
the [upstream rseq compatibility issue](https://github.com/google/tcmalloc/issues/292).
Retain this allocator setting with capacity results. Database startup alone is
insufficient: qualification requires successful reconciliation after load and
faults, and cleanup failures fail the run. See Sink's
[backend environment requirements](https://github.com/batchstream/sink/blob/main/docs/backend-environment.md).

During every storage fault, qualification also performs concurrent conditional
merges, reads, deletes and independent Kafka acceptance through both Gateways for
`secondary` and `mongodb-sync`. These must finish before the failed dependencies
are restored; readiness alone does not establish Store isolation. Every third
fault cycle keeps the primary Kafka broker paused during these checks as well.

## Coverage and logging qualification

`make test-coverage` runs the ordinary race suite, records JSON test events and
statement coverage, and enforces the core-package floors in
`.github/coverage-minimums.json`. Evidence is retained in `.reports/coverage/`
and uploaded by CI. Generated protobuf is excluded; floors are package-specific
so well-covered helpers cannot hide a regression in a critical package. Floors
allow small execution/platform variation and should increase with new tests,
not be lowered to make an unrelated change pass.

Candidate qualification also retains `local.out`, `mongodb.out`,
`elasticsearch.out`, `opensearch.out` and bounded service-integration profiles in
its evidence directories. The combined candidate/component suite is checked against
`.github/candidate-coverage-minimums.json`, including logging. This gate runs
`Test`/`Example` only, so its queue baseline excludes decoder fuzz seeds that
intentionally skip invalid inputs. To inspect the
union, repeat `--profile` with files from the **same candidate revision**:

```sh
python3 scripts/check-coverage.py --profile /path/local.out --profile /path/mongodb.out --report /path/combined.md
```

These profiles instrument Go test processes, not the separately launched
conformance server. The suite's own percentage measures its test helpers and
reference models, not the percentage of Sink's public behavior tested.

`TestProcessLoggingSurvivesCollectorOutage` launches real Engine and Gateway
processes with an unavailable OTLP HTTP receiver. Successful writes and exact
readback must continue. After receiver recovery, application-level failures and
forwarded stream diagnostics must arrive without process restart, document
contents must remain absent, and both processes must flush their final lifecycle
events on graceful exit. Exporter failures remain visible on stderr even when
ordinary console logging is disabled. Candidate component gates additionally require
both OTLP transports, TLS refusal to downgrade, bounded queue loss and shutdown.

## Server test ownership

Sink retains component unit tests, input fuzzers and unit microbenchmarks. This
repository owns server backend and transport integrations, application assembly
tests, stateful scenario fuzzing, cross-component benchmarks, capacity
experiments and performance tooling. See [the runner and ownership
rules](server-tests/README.md) and [integration benchmark commands](docs/server-benchmarks.md).
The reusable `server-qualification.yml` workflow is required by both repositories;
Sink pins its workflow and source revision together. Both repositories enforce
combined coverage floors and named test requirements.

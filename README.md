# Sink production qualification suite

This public repository qualifies [Sink](https://github.com/liran/sink) against
a representative commerce-indexing workload using only synthetic fixtures,
public dependencies, and disposable local infrastructure. It does not import
proprietary application packages, use production data, require cloud
credentials, or connect to an external Kubernetes cluster.

## Release gates

Production incidents from Sink PRs 37 through 41 are now executable public-API
contracts. The [incident matrix and reliability contract](docs/reliability-contract.md)
explain each missed invariant, its deterministic oracle, configuration/model
matrix and remaining qualification gaps. Integration, release and sustained runs
start with `make test-conformance`. Historical pre-fix evidence is documentation;
current CI does not run historical binaries or a regression-sensitivity target.

The [September 16 release coverage review](docs/current-release-review-2026-09-16.md)
maps the latest Scan, admission, Lua and configuration changes to required tests,
including the matching Go SDK. Historical performance-regression proofs are
retained as review context, not current CI jobs.

Admission scenarios negotiate the candidate's `memory` configuration support.
Released servers retain the count/queue assertions. Candidates with demand-based
admission instead prove small-request concurrency, real byte exhaustion without
request access to the completion reserve, cancellation cleanup, Store isolation,
Kafka progress and projected Scan retries. They observe `sink_memory_*` ownership
and waiting gauges; cumulative counters and configured capacity are not treated
as leaked memory. These are active replacement scenarios, not skipped tests.

The suite verifies:

1. Representative item and offer merge programs match the public Go reference
   model, including history limits, deduplication, timestamps, large integers,
   and replay behavior.
2. The same programs produce equal documents through the public Go client,
   multiple Sink server processes, and real storage backends, using explicit
   JSON documents for search and BSON documents for MongoDB.
3. Concurrent merges preserve every successful update while exercising real
   search-engine revision conflicts.
4. Store-owned asynchronous routing works through two independent Kafka
   clusters without changing result order.
5. Stores without Kafka remain available synchronously and reject asynchronous
   requests as retryable unavailable results without publishing anything.
6. MongoDB, Elasticsearch, and OpenSearch pass create, duplicate-create,
   concurrent merge, asynchronous write/delete, and synchronous delete checks,
   including distinct `json`/`bson` identity tags and native datetime types.
7. Accepted Kafka mutations survive worker and broker restarts and retain
   same-record ordering.
8. Active operations recover from a worker SIGKILL, a 45-second OpenSearch outage,
   and a Kafka restart using the product worker retry default. Healthy stores
   remain ready while the affected dependency fails readiness.
9. A representative concurrent load completes without failed operations or
   exhausted merge-conflict retries.
10. Every consumer group drains to zero lag and dead-letter topics remain empty
    for ordinary traffic and temporary outages.
11. The public API rejects oversized asynchronous mutations permanently, counts
    repeated read keys across stores against one output budget, and rejects
    expanded Lua aliases before changing stored data.
12. An intentional permanent CREATE conflict does not suppress the next valid
    update to the same key. The final recovery scenario inspects exactly one DLQ
    record, repairs the conflict, replays it with the Sink CLI, reconciles the
    stored business result, and verifies the original DLQ position is preserved.
13. Applied/visible completion, independent datasets, per-RPC budgets, real
    revision conflicts, queued cancellation, formatted JSON and bounded hot-key
    backend work satisfy the incident regressions with race detection.
14. An independent Go operation model checks mixed Create/Upsert/Replace/Merge,
    permanent failures, reordered/duplicate Reads and duplicate Deletes after
    every RPC across batching configurations and all seven backend stores.
15. PR #44's native Execute, Query, Count and Scan APIs run through the public
    Dataset API on all seven stores: exact pagination, projections, count
    strategies, native errors, BSON preservation and canceled scans.
16. Damaged search pages and approximate totals fail without retries; canceled
    and timed-out Scan requests release backend resources and admission slots. Lost native
    mutation acknowledgements do not replay increments. Oversized responses and
    invalid raw RPCs fail before exposing partial results or reaching storage.
17. Returned writes report each operation's committed value and revision through
    real conflicts and concurrent server replicas. Response budgets belong to
    each original RPC and reject an oversized candidate before its commit.
18. Paged Scan resumes from the last processed cursor after graceful server exit
    or SIGKILL, including a lost page response. Cursors have no expiry and hold no
    backend session between pages; scans observe live data rather than a snapshot.
19. Ascending and descending scans continue through record deletion, insertion
    before/after a checkpoint and updates to unseen records on all seven stores.
    Alternating server replicas, changing page sizes, retrying a saved checkpoint,
    cancellation and cursor corruption preserve the expected remaining records.
20. Missing timeout/shard completion evidence and inconsistent shard counts fail
    Query, Count and Scan without exposing partial output. MongoDB Query rejects
    partial shard results; unordered native writes preserve successful siblings
    while returning the original native error for a failed member.
21. A held synchronous storage request cannot block Kafka Write/Delete
    acceptance for the same store, with default and one-operation batches. A paused
    Kafka broker cannot consume synchronous write capacity; excess publishes
    fail before enqueue, and accepted records drain in order after recovery.
22. Eight independent returned merges coalesce into one execution and stream
    large snapshots or outputs through bounded backend requests, preserving
    each caller's committed document and response budget.
23. The pinned Go SDK exercises round-robin balancing and real loopback DNS
    changes with healthy connections, default and custom refresh intervals,
    scale-in and temporary DNS failure. Required test events prevent an older
    SDK with no matching tests from passing the gate.

Release qualification uses a bounded six-minute active-fault workload with a
three-minute deadline for each business cycle to reconcile. The fixture removes
its former `max_retry_attempts: 30` override and exercises Sink's default retry
rounds. Every normal-workload DLQ must remain empty before the deliberate
permanent-error scenario runs. A successful replay does not delete DLQ records.

The nightly/manual two-hour workflow uses this repository's same orchestration
and assertions at higher concurrency. Each run retains exact suite/server
revisions, resolved Compose configuration, test logs, fault timestamps, container
resource samples, Prometheus samples, and DLQ inspect/replay reports for 14 days.
The standalone script prints its local evidence directory even on failure.
The two-hour run is separate from the release gate; a passing short run does not
imply a completed long run or multi-node production certification.

## Changes since v0.12.1

The [post-release coverage matrix](docs/post-release-review-2026-09-14.md) maps
all changes through the September 14 candidate to required regression evidence.
Production and sustained qualification now run the candidate's complete ordinary
race suite, real MongoDB/Elasticsearch/OpenSearch storage suites and bounded
service integration in addition to the public API suite. Named test events reject
missing or skipped regressions. New public scenarios verify Lua budget isolation,
managed query safety/failover, lookahead byte budgets and BSON fidelity through
returned writes, Kafka and a second server. Historical broken-candidate evidence is retained in the review documents; it is
not an active compatibility gate for the current protocol.

`SINK_SERVER_DIR=/path/to/sink make test-candidate` runs the candidate race gate
without Docker. Ordinary `go test ./...` remains independent of infrastructure.

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
ordinary conformance allows 20 minutes. `SINK_CONFORMANCE_TEST_TIMEOUT` can
override that aggregate test timeout without changing request deadlines or
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
    uses: liran/sink-production-suite/.github/workflows/release-qualification.yml@SUITE_COMMIT
    with:
      suite_ref: SUITE_COMMIT
      sink_ref: v0.9.0
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
fixtures reject retired multi-Store modes and use grouped settings, duration
strings, and readable byte sizes. Pair this suite revision with the Store-isolated
Sink candidate; see Sink's
[configuration migration guide](https://github.com/liran/sink/blob/main/docs/configuration-migration.md).

The suite targets the current URI-only protocol and current configuration. Frozen
old-release configurations and historical-binary regression gates have been
removed. Behavioral regression tests continue to run against the candidate build.

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
[rollout timing guide](https://github.com/liran/sink/blob/main/docs/rolling-upgrades.md).

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
[backend environment requirements](https://github.com/liran/sink/blob/main/docs/backend-environment.md).

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

Candidate qualification also retains `unit.out`, `mongodb.out`,
`elasticsearch.out`, `opensearch.out` and bounded service-integration profiles in
its evidence directories. The candidate ordinary suite is checked against
`.github/candidate-coverage-minimums.json`, including logging. This gate runs
`Test`/`Example` only, so its queue baseline excludes decoder fuzz seeds that
intentionally skip invalid inputs. To inspect the
union, repeat `--profile` with files from the **same candidate revision**:

```sh
python3 scripts/check-coverage.py --profile /path/unit.out --profile /path/mongodb.out --report /path/combined.md
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
ordinary console logging is disabled. Candidate unit gates additionally require
both OTLP transports, TLS refusal to downgrade, bounded queue loss and shutdown.

The process test detected an actual diagnostic gap: memory-managed Gateway
responses reached the outer logging interceptor inside `protocol.ManagedMessage`.
Without unwrapping for observation, a failed Create was reported at debug level
with zero operations/failures. The server regression requires the original
managed response to be returned unchanged while recording its underlying failure.
The standalone process assertion failed on Sink `49d87e2` and passed after the
matching fix; unlike old historical-binary gates, this is a current test oracle.

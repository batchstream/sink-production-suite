# Qualification since Sink v0.13.0

Review target: Sink `1205922ec920a5063dd40abf27a8f88a707b044d` and sink-go
`45a581d538386ec97a5b04a35b4cef026aad7173`. The SDK dependency is pinned to
`v0.6.1-0.20260915090422-45a581d53838`, so qualification uses the exact client
candidate without a developer checkout replacement.

## Coverage of current changes

| Change | Required oracle |
| --- | --- |
| Sink #73: projected Scan pages and bounded Scan admission; sink-go #18: projection and safe retries | All ordinary SDK race tests run, with required anchors for identical-page retries, retry limits, non-retryable failures, cancellation and projection validation. A public test on both search engines forces a real admission timeout, verifies retry recovery and unchanged caller checkpoint, and checks exact page order, projected fields and backend call count. Seven-store public tests check inclusion/exclusion projections across pages, including BSON timestamps. Real MongoDB/search projection tests remain required. |
| Sink #73: shared execution deadlines and per-store byte budgets | All candidate race tests run, with required anchors for Scan deadline sharing, bounded queues, impossible requests, store isolation and cross-store reservation resizing. Existing public cancellation, native response budget and mixed-store saturation tests remain required. |
| Sink #74: Lua allocation and cancellation changes | The complete candidate race matrix remains mandatory, including compilation/conversion cancellation and native/cumulative budget failures. Existing public Lua failure tests compare stored values and revisions, successful siblings and subsequent recovery on both search engines. |
| Sink #75: grouped duration and byte-size configuration | Every current server/worker fixture uses the grouped schema. Historical proofs select the frozen legacy fixture only for revisions that require it; a startup error never satisfies an expected behavioral failure. Sink CI, release and sustained workflow pins must all use this validated suite revision. |
| Sink #76: bounded synchronous admission | New public tests hold real Count calls at storage, queue a burst without taking execution slots or document reservations, and require an independent store to remain available. Separate cases enforce global count, per-store count and input-byte limits, cancel a queued caller, refill released queue capacity and require all queue/execution metrics to drain. Candidate anchors require targeted handoff, cancellation after wakeup and request-deadline consumption. |
| Sink #76: known returning-Put reservations | A public test collects 32 returning Put RPCs under a 128 MiB execution budget, requires one real backend bulk, and verifies each caller's committed document. The candidate unit regression is also mandatory. Existing returned Merge, original-caller budget and conflict tests retain the unknown-output guarantees. |

## Regression sensitivity and evidence

The Count-burst and returning-Put tests must fail against
`3d5b641d3599401a13f9f4ea6b98c61e66110a17` at their specific admission/batch
assertions. These revisions already use grouped configuration. Missing tests,
build errors, failed startup and unrelated assertion failures do not count as
regression evidence.

During development, the four new public tests passed three consecutive runs
with race detection against real Elasticsearch and OpenSearch. Both new
historical assertion failures were reproduced. CI additionally requires the
full seven-store production gate, ordinary race/lint tests, stateful fuzzing and
the complete historical sensitivity gate before merge. JSON events reject
missing, skipped or unfinished required tests and retain candidate revisions.

Release qualification and two-hour sustained fault qualification are separate
runs. The latter exercises worker termination, search outages and broker
restarts while reconciling persisted business state, consumer lag and dead
letters. A short test pass is not evidence of a completed two-hour run. Sink's
constrained-resource throughput measurements remain documented in its
`docs/synchronous-admission-performance.md`; the functional suite uses
deterministic assertions rather than host-dependent throughput thresholds.

These gates qualify synthetic traffic and disposable single-node dependencies.
Production topology, disk exhaustion, multi-node elections and backup recovery
remain deployment-specific checks.

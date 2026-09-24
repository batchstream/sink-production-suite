# Adaptive Store backpressure qualification

These gates run the candidate executable through real Gateway, Engine and Worker
processes and the public SDK. Successful work reaches disposable Elasticsearch
and OpenSearch; a local reverse proxy injects network delay and explicit HTTP 429
responses. It never fabricates successful storage responses or reads private
controller state. The existing workflow runs both OpenSearch 2.17 and 3.8.

| Required test | Evidence |
| --- | --- |
| `TestStoreBackpressureKeepsSharedAdmissionBounded` | A held write consumes the shared single-slot window. Eight writes remain queue-charged, excess work is rejected, Native Count waits in the same bounded FIFO, health remains ready, canceled queued writes never reach storage, and resource growth stays bounded. Released work and Native access recover; duplicate CREATE and invalid queries do not close the window. |
| `TestStoreColdStartQueuesMixedBurstWithoutRejection` | One and four cold Engines receive simultaneous 64-call mixed bursts per Engine. Batch writes, Reads, Execute, Query, Count and Scan complete without admission rejection; task/byte gauges drain and persisted records are verified against both real search backends. |
| `TestStoreBackpressureReplicasConvergeAndRecover` | One and four independent Engines grow from actual backend successes, reduce their windows after injected latency, enter zero-window cooldown after repeated 429s, resume with two shared backend slots, and regain concurrency after the restriction is removed. Persisted records from every writer are verified. |
| `TestStoreBackpressureWorkerRetainsBacklogAndRecovers` | A real Kafka broker retains 32 accepted increments while Worker observes overload and enters cooldown. Offsets do not advance during rejection; after recovery they drain to 32, the stored counter is exactly 32, and the DLQ stays empty. |
| `TestNativeTransportSharesStoreAdmission` | The candidate overlay verifies Execute, Count, Query and Scan wait through a full execution window until caller deadlines, then drain queued calls with unchanged replies. |
| `TestReadyStoreAdmitsColdQueryBurst` | An untrained Engine must advertise at least four usable slots (within its configured ceiling) before readiness. A 64-call Query/Count burst runs with SDK retries disabled and must complete against each real backend without admission rejection or hidden warmup. |
| `TestQueryAndCountAdmissionQueueBoundedAndCancelable` | A held Count occupies the only Store permit. Three read callers wait, a full queue rejects without adding waiters, caller cancellation and deadlines free queue bytes/count without backend execution, and the remaining request executes exactly once after release. |

Existing incident schedules use real Count traffic on a separate empty index to
establish four execution slots before holding a backend request. Warmup uses
four callers and backs off admission rejection, avoiding an RPC rejection storm
on shared CI runners. Cold Query/Count and mixed Engine bursts explicitly select
`coldStore` and require no admission failures under configured task and byte
bounds. Overflow tests still require rejection without adding waiters. Every
Store operation joins the same FIFO, and caller cancellation/deadlines must
remove queued tasks without reaching the backend.
No controller state is overridden and health probes do not train the window.

The local candidate gate additionally requires deterministic virtual-time tests
for an hour-long admission wait without a server-added timeout, original caller
deadlines, byte/count bounds, FIFO head/middle/tail cancellation, permit cleanup,
healthy growth driven by queued demand, real-overload cooldown, and concurrent
release/cancellation under the race detector. Startup still runs the 1/8/100
instance shared-backend simulation; improving cold readiness must not weaken its
startup-overshoot and recovery assertions.
Cross-Store isolation is checked while the slow Store may retain a write backlog;
that backlog must drain after the held request is released. Deadline tests
distinguish a released execution slot from legitimate cooldown.
The storage-failure matrix uses one attempt per fixture retry round and observes
three failures across rounds, preserving offset/DLQ assertions without demanding
22 rapid attempts through congestion backoff. After each fault, four separately
settled increments verify sustained recovery before the next independent fault,
so batched recovery writes cannot carry cooldown escalation into that case.
Production defaults are unchanged.

Run `make test-candidate` for component/transport checks and `make test-conformance`
for the real-process/backend scenarios. Both scripts require the named tests in
JSON test events, so missing or skipped scenarios fail qualification. The
combined service coverage floor remains 91%; the controller has an 85% floor.
The expanded real-process conformance matrix has a 40-minute test deadline;
CI jobs allow 60 minutes for builds, backend startup and evidence collection.

The conformance artifacts contain the exact candidate/suite revisions, test
JSON, process configurations and logs. Test output records completed work,
backend request counts, injected overloads and Kafka offsets. Metrics are read
from the candidate's public Prometheus endpoint; no test-only controller API or
capacity override is used.

This is bounded fault/recovery qualification, not a production capacity benchmark
or proof of a strict global concurrency maximum. The proxy's two-slot phase is a
controlled rejection boundary in front of a real backend. The candidate retains
its internal bulk fanout. Sink's deterministic 1/8/100-controller simulation
covers larger startup populations; this suite adds independent real processes,
transport and persistence checks without duplicating that simulation.

# Qualification since v0.12.1

Baseline: Sink `v0.12.1` (`a1178efaa0aa5e2b491dfc63449495c7b623f827`,
released September 11) through `77bece408ad9101883ca66e6e5227b73ddfa8718`
(68 commits, including merges). Suite baseline:
`235672fbfcbdaa4f9c68e935d58f720d84e99448`. The suite retains its matching SDK
pin `v0.5.2-0.20260912084407-efd5e7355823`; the local SDK checkout is not
silently substituted.

## Required coverage

| Change since the release | Release oracle |
| --- | --- |
| Mandatory batching and create-or-merge semantics | Existing public operation state machines on seven stores and `TestApplicationAlwaysBatchesGRPCRequests`; no removed configuration field is emitted. |
| Shared read buffers, count reservations and failure response budgets | Candidate race gate runs all ordinary tests, requiring the read microbatch, count, original-RPC failure budget and tiny diagnostic regressions. MongoDB/OpenSearch service integration proves bounded working sets on real storage. |
| Worker capacity splitting | Required processor and Kafka worker tests prove bounded splitting, per-record outcomes, offset commits and no replay of successful siblings; existing public crash, ordering and DLQ tests remain required. |
| MongoDB numeric identities and collection collation | Required real MongoDB tests cover numeric IDs and byte identity under non-simple collation. |
| Lua BSON types and exact JSON integers | Required candidate tests cover scalar identities, numeric widths, negative zero, dates, literal dollar objects and precision rejection. New public BSON tests compare type and value bytes after returned synchronous merges and Kafka delivery, reading through the second server. |
| Lua native loops, patterns, formatting, unpacking, stack slots and cumulative utility work | Entire merge/service race matrix runs. New public tests cover twelve operations on both search engines with a small instruction limit, unchanged document/revision after rejection, healthy siblings and a following successful merge. |
| Lua compilation and conversion cancellation | Required candidate tests cover compilation cancellation, conversion deadlines and healthy siblings. Tests assert bounded failure and no mutation rather than relying on process survival. |
| Malformed BSON and UTF-8 documents/identities | Required candidate tests reject unsafe documents before publishing/writing and exercise the vtprotobuf RPC codec. Real search tests prove malformed IDs cannot overwrite a valid Unicode key. |
| Search response identity and action validation | Required `_mget` and bulk tests reject malformed, missing and reordered identities before acknowledgement. Real index-alias lifecycle remains required. |
| Search page bounds and shrinking scans | Real search tests cover result windows, adaptive page sizing and byte budgets. New public Query test proves that a large unreturned lookahead document cannot consume a valid page's budget. |
| Managed query endpoint validation and failover | New public Query/Count/Scan tests reject a mutation path ending in `_search` and compare the complete stored backend response, including its revision. A 503 endpoint followed by a real healthy backend must recover once; Execute must retain the native error without replay. |
| Local search failures and metadata-only replacement | Required candidate tests separate cancellation/oversized local responses from endpoint failures; real replacement tests bound reads of large old documents. |
| MongoDB capability downgrade, uniqueness and native query syntax | Required real MongoDB tests cover conditional bulk durability, native business field/operator contexts, lookahead batching and unique-index outcomes; the race gate requires capability downgrade to remain sticky. |
| Dead-letter replay routing and Lua CLI comparisons | Required candidate CLI tests retain search routing and compare JSON numbers exactly; existing public replay qualification checks persisted business results and original DLQ positions. |

## Enforcement

`make test-production` and `make test-reliability` now start with
`make test-candidate`. It runs **all ordinary candidate tests with race detection**,
not just the named anchors. Only packages that contain tests are selected;
package-only `[no test files]` output is not mistaken for a skipped regression.
Fuzz mutation/seed coverage remains in its separate fuzz gate.

Conformance reuses its disposable Elasticsearch/OpenSearch instances to run the
candidate storage suites, plus OpenSearch service tests. Integration exposes a
dynamic loopback port on its own MongoDB replica set and runs the MongoDB storage
and service suites before public business traffic. Every phase records JSON test
events and rejects missing required tests, skipped tests and incomplete runs.
Candidate logs and revisions are retained with the existing evidence artifacts.
No external databases or credentials are needed.

Three additional historical proofs require specific assertion failures:

- `a03378982c682fee15fa49c42727e09937153a23`: cumulative Lua helper calls escape
  the configured instruction budget.
- `c38f0dba100c12f222b5141567620588005e8b25`: managed Query forwards a mutation endpoint
  to storage instead of rejecting it before execution. The backend can reject
  managed-only query parameters; a backend error is not local route validation.
- `5c208642aea2fa6674f508270afd79eeb0046540`: managed Query fails instead of
  recovering through the healthy endpoint.

A build failure, missing backend or unrelated assertion cannot satisfy these
proofs. The release, CI and scheduled Sink workflow pins must all point to the
same validated immutable suite revision to activate this coverage.

These gates qualify disposable single-node databases and synthetic traffic.
They do not certify multi-node elections, disk exhaustion, production deployment
configuration or a completed two-hour soak.

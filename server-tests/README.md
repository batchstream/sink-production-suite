# Sink test ownership

Sink owns component unit tests. The production suite owns server transport,
assembly, real-backend, end-to-end, fuzz, benchmark and capacity-experiment tests.
Product examples and release build scripts remain with the server; their
automated smoke scenarios are defined by this suite.

Unit tests may use deterministic storage doubles, offline MongoDB wire replies,
or a fake HTTP/Kafka backend to isolate one adapter. Real gRPC/OTLP transport,
Gateway-to-Engine composition and application startup/shutdown belong here.
Do not add exported server APIs or replace production functions just to move a
test. Preserve the existing assertions and shared unit-test helpers.

## Run against a candidate

Set `SINK_SERVER_DIR` to a local Sink checkout (the default is `../sink`), then
run from this repository:

```sh
export SINK_SERVER_DIR=/path/to/sink
make test-candidate                 # unit + local component regressions, race and coverage
make test-server-integration        # disposable MongoDB, Elasticsearch and OpenSearch
make test-conformance               # public APIs and real process failures
make test-isolated-quickstart        # matching SDK against three isolated roles
make fuzz-server                    # actively fuzz envelopes and BSON for 30 seconds each
make benchmark BENCHTIME=1x          # bounded smoke of every in-process benchmark
make benchmark-lua BENCHTIME=1x      # standalone Lua comparisons
make benchmark-memory               # allocator experiment; no ranking threshold
make test-perf build-perf            # test/build the public-RPC workload generator
```

For an individual server test or benchmark, use the same runner:

```sh
bash scripts/server-go.sh test ./internal/logging -race -run '^TestOTLP' -count=1
bash scripts/server-go.sh test ./internal/gateway -tags=integration -run '^TestGatewayDNSWithdrawalDrainBoundary$' -count=1
bash scripts/server-go.sh test ./internal/service -run '^$' -bench '^BenchmarkRecordFolding$' -benchtime=30x -benchmem
```

Backend benchmarks retain the `integration` build tag and need the disposable
backend endpoints described in the integration scripts. See the
[fixed-resource runner](../benchmarks/qualification/README.md),
[Lua comparisons](../benchmarks/lua/README.md) and
[recorded benchmark measurements](../docs/server-benchmarks.md).

## Internal-package access

`testdata/` mirrors the server package paths. Go excludes this directory from
the suite's ordinary `go test ./...`. `scripts/server-go.sh` builds a Go source
overlay that adds these test files to the candidate packages during compilation.
The candidate's production code and remaining unit-test helpers are compiled
unchanged. Private declarations stay private; nothing is copied into or written
to the candidate checkout. Canonical paths also support symlinked checkouts.
The runner rejects files that shadow candidate files, missing packages, and
non-test production overlays. A candidate from before this migration must use
its matching older pinned suite.

The performance tool is a real nested module under `tools/sink-perf`. Its module
path is beneath `github.com/liran/sink` so it can use the candidate's existing
VT codec without changing benchmark serialization. `scripts/perf-go.sh` creates
a temporary module file that replaces Sink with `SINK_SERVER_DIR`; neither
repository's module files are rewritten. The build output is
`.reports/bin/sink-perf`.

Fuzz executables run from an evidence directory, including their cache and
minimized failure corpus, so failures cannot create test files in Sink.

## Gates and evidence

`test-candidate` requires the original named regressions, records `local.out`
and JSON test events, and applies the unchanged combined package floors in
`.github/candidate-coverage-minimums.json`. Sink separately measures its remaining
unit tests with its recalibrated unit-only floors. A lower unit-only percentage
does not relax the combined gate. Backend profiles remain separate; combine
profiles only when they describe the same candidate revision.

`server-qualification.yml` owns the component checks, both OpenSearch versions,
three backend integrations, SDK compatibility, quickstart, release smoke, active
fuzzing and benchmark smoke. Sink calls an immutable workflow revision with the
same `suite_ref`; suite PRs call this workflow directly. Keep all required gates
enabled when moving tests, and use matching server/suite branches for coordinated
changes. Timing benchmarks are executed for correctness without timing-based
pass/fail thresholds.

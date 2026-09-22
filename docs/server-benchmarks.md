# Server integration benchmarks

Run these commands from the production-suite checkout with `SINK_SERVER_DIR`
set to the candidate server. The suite owns cross-component and real-backend
benchmarks, comparison workloads, capacity experiments and fixed-resource load
tests. Sink retains unit microbenchmarks next to the components they measure;
see its [development guide](https://github.com/batchstream/sink/blob/main/docs/development.md#performance-qualification).

## Gateway and Engine

Measure the actual local gRPC path, comparing direct Engine calls with the
Gateway hop using the same batcher and memory storage:

```shell
SINK_SERVER_DIR=/path/to/sink make benchmark BENCHTIME=1x
SINK_SERVER_DIR=/path/to/sink bash scripts/server-go.sh test ./internal/gateway -run '^$' -bench '^BenchmarkGatewaySmallPut$' -benchtime=1s -benchmem -count=5
```

The first command is a bounded correctness smoke. This local latency and
allocation baseline does not establish deployment capacity.

## Real-backend synchronous storage

`BenchmarkSynchronousStorage` exercises the gRPC codec, dispatcher, Lua engine
and disposable MongoDB/OpenSearch backends, then verifies every writer's
persisted counter. Start disposable backends and set `SINK_MONGODB_TEST_URI` or
`SINK_SEARCH_TEST_ENDPOINT`, as described by the integration scripts, before
running the selected backend:

```shell
SINK_SERVER_DIR=/path/to/sink bash scripts/server-go.sh test ./internal/service -tags=integration -run '^$' -bench '^BenchmarkSynchronousStorage$/^mongodb$' -benchtime=1x -benchmem
SINK_SERVER_DIR=/path/to/sink bash scripts/server-go.sh test ./internal/service -tags=integration -run '^$' -bench '^BenchmarkSynchronousStorage$/^opensearch$' -benchtime=1x -benchmem
```

## Workload and resource comparisons

See the [fixed-resource runner](../benchmarks/qualification/README.md) for
repeatable load profiles and [Lua comparisons](../benchmarks/lua/README.md) for
the standalone runtime comparison module. CI executes smoke workloads for correctness;
benchmark timings do not determine pass/fail.

# Benchmarks

All benchmarks are BenchmarkDotNet suites in `Oxia.Client.Benchmarks` (always run in **Release**; allocation figures and tiered-JIT behaviour are
meaningless in Debug), plus two command-line modes of the same executable. Results of a full run: [`docs/benchmark-results.md`](../docs/benchmark-results.md).

```
dotnet run -c Release --project benchmarks/Oxia.Client.Benchmarks -- --filter "*" --job short --memory     # everything (~15 min)
dotnet run -c Release --project benchmarks/Oxia.Client.Benchmarks -- --list flat                           # catalogue
dotnet run -c Release --project benchmarks/Oxia.Client.Benchmarks -- --filter "*Codec*"                    # a group, full (non-short) job
python benchmarks/make-results-doc.py <artifacts-dir> docs/benchmark-results.md                            # regenerate the results document
```

Stop other heavy processes first (the Docker containers idle at ~0.5 MiB and are fine). Do **not** run the test suites at the same time.

## Catalogue

| Class | What it measures | Question it answers |
|---|---|---|
| `Codec.WriteRequestEncodeBenchmarks` | encoding 1/100/1000 puts into a reused buffer writer, hand codec vs Google.Protobuf | is the zero-allocation encoder faster than protoc's, byte-identical? |
| `Codec.WriteResponseParseBenchmarks`, `GetResponseParseBenchmarks` | parsing into a reused / fresh message, contiguous and multi-segment input | does parsing allocate (only the strings/`byte[]` that must exist)? |
| `Common.CompareBenchmarks` | `CompareWithSlash`, hierarchical/natural encoders (encode, span encode, decode) | cost of the multi-shard merge order |
| `Common.HashBenchmarks` | XXH3-64 → uint32 shard hash for short/long/non-ASCII keys | cost of routing a key |
| `Common.FutureBenchmarks`, `GrpcMetricsBenchmarks` | the per-request future; the gRPC client metrics interceptor | per-operation primitives |
| `Internal.ShardManagerBenchmarks` | `ShardManager.Get` with 1/16/256 shards | lock-free routing hot path |
| `Internal.RpcProviderBenchmarks` | write/read execution through the provider with fakes | retry/stream wrapper overhead |
| `Throughput.QueueBenchmarks`, `BatcherAddBenchmarks` | the bounded MPSC batcher queue and the batcher add path | enqueue cost, parking |
| `Throughput.CallPoolBenchmarks` | the pooled operation calls (rent/complete/consume) | per-operation object cost |
| `Throughput.LingerTimerBenchmarks` | wake-up latency of the high-resolution linger timer | does a 1-5 ms linger really take 1-5 ms (Windows tick) |
| `Client.ClientOperationBenchmarks` | single and pipelined Put/Get/Delete through the real gRPC stack against a zero-allocation echo server | client-side cost and allocation per operation |
| `Alloc.PooledReadBenchmarks` | faithful vs pooled `Get` / `RangeScan` | what the opt-in pooled read API saves |

## Command-line modes

* `alloc [scenario ...]` — end-to-end allocation profile: runs Put/Delete/DeleteRange/Get/Scan scenarios for 400 000 operations each and attributes
  allocated bytes to types with sampled GC allocation ticks (so remaining allocations are explained, not just counted).
* `load [options]` — closed/open-loop load generator in the spirit of Go's `oxia perf`: async or sync client, put/get/delete mix, key and value sizes,
  producers, HDR-style latency histogram (percentiles without per-sample allocation), CPU time, GC counts, allocated bytes/op. Servers: the reusable
  Docker `oxia standalone` fixture (`--server docker`), the in-process zero-allocation `RawEchoServer` (`--server echo`, CPU-bound analysis of the client
  alone) or a child-process echo server. `--go <oxia-load.exe>` runs the **Go client on the identical workload** after every run and prints a comparison
  table; build the Go side from the pinned checkout with `pwsh benchmarks/Oxia.Client.Benchmarks/Load/go/build.ps1`.

## Allocation budgets (enforced by tests)

The benchmarks measure; the unit tests (`tests/Oxia.Client.Tests/Alloc`, `Batch/AllocationTests*`, `Throughput`, `Proto/CodecAllocationTests`, run in
Release only) *enforce* the budgets: hand codec encode 0 B, parse into a reused message 0 B (excluding required strings/arrays); client-side per
operation Put ≤ 16 B, Delete ≤ 16 B, DeleteRange ≤ 16 B, Get ≤ 12 B (found, faithful: + the value array), pooled range scan record ≤ 8 B,
notification ≤ 136 B, session keep-alive ≤ 4 B. Every non-zero budget is justified next to its test; see
[`docs/performance-allocation.md`](../docs/performance-allocation.md).

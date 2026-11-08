# Performance of the .NET client (wave 2, Z2: throughput & latency)

Goal: make the client as fast as possible end to end against a real Oxia server without changing observable behaviour, and prove it with numbers --
including a head-to-head against the Go client on the same server. The change list is in [`docs/porting/Z2-throughput.md`](porting/Z2-throughput.md); this file
holds the method, the measurements, what did not help and the recommended settings.

## 1. Summary

* **A 5 ms linger was a 15 ms linger on Windows.** `System.Threading.Timer` has the 15.6 ms OS tick there; the linger timer now waits on a high resolution
  waitable timer (wake-up error 0.1-0.5 ms instead of up to 15 ms). Closed-loop throughput at the default linger with 256 callers went from 16.3k to ~33k ops/s
  (p50 15.5 -> 7.6 ms) and is now level with the Go client on the same server.
* **The client is CPU-efficient and has no allocation of its own on the hot path.** Against an echo server in a separate process the .NET client sustains
  ~1.0 M ops/s (4.9 us CPU per operation) where the Go client reaches ~0.17-0.2 M ops/s (16.8 us/op) with the same parameters. The only allocations left per
  put (150 B) are the decoded response objects of the wire codec (component A, being pooled by Z1); the batcher, the queue, the call pool, the completion
  dispatch and the slow (parked) path allocate nothing.
* Three structural costs were removed: a single lock in the call pool (the most contended point at 1000 callers), a Channel whose parked writers cost ~300 B and
  two thread pool hops each, and a 48 B work item per completed call.
* What would still move the needle is outside the library or needs a contract decision: the thread pool itself (two thirds of the remaining CPU is workers
  spinning for work), and pipelined write batches (+25-30 % on one shard, -15 % on four; needs an ordered retry) -- see sections 7 and 8.

## 2. Method

**Hardware / software.** Intel Core Ultra 9 185H (16 cores / 22 logical), 32 GB, Windows 11 Enterprise 10.0.26100, .NET SDK 10.0.400 (runtime 10.0.14, workstation
non-concurrent GC, TieredPGO on), Go 1.27.0 (auto-downloaded toolchain; the checkout says `go 1.27`), Docker Desktop 29.8.2 (WSL2, 8 CPUs, 16 GB), server
`oxia/oxia:nightly-beta` = `oxia version 8456f61` (the pinned commit), started as `oxia standalone` by the shared `OxiaServerFixture` (one fresh server per run, data on
tmpfs, `--wal-sync-data=false`, 1 shard unless stated, notifications on). **The machine is shared** (other agents build and test on it, Defender is active, Docker Desktop
adds loopback latency), so every figure is the **median of 3 runs interleaved** (base, final, Go, base, ...) of 8 s after a 2 s warm-up; run-to-run noise is
10-20 %. Treat differences below that as noise; the large ones are repeatable.

**Tools** (all in `benchmarks/Oxia.Client.Benchmarks`):

```
# .NET client against a fresh server in the reusable container; add --go to run the Go client on the same workload afterwards
dotnet run -c Release --project benchmarks/Oxia.Client.Benchmarks -- load --server docker --ops put,get --producers 256 --linger 5ms --duration 8s --go C:\Work-Experimental\oxia-load-build\oxia-load.exe
pwsh benchmarks/Oxia.Client.Benchmarks/Load/go/build.ps1          # builds the Go twin (oxia-load.exe) from the pinned checkout, outside the repo
dotnet run -c Release --project benchmarks/Oxia.Client.Benchmarks -- load --server echo-proc ...   # CPU-bound: echo server in a child process
dotnet run -c Release --project benchmarks/Oxia.Client.Benchmarks -- load ... --alloc-profile        # allocated bytes per operation by type
dotnet run -c Release --project benchmarks/Oxia.Client.Benchmarks -- --filter "*Throughput*" --job short --memory   # micro-benchmarks
```

The load generator is modelled on `cmd/perf` of the Go repository: *closed loop* (N producers, each awaits its operation and issues the next, so the number of
operations in flight is N and **throughput = N / latency**) or *open loop* (`--rate`, fixed schedule, latency measured from the intended start, no coordinated
omission). It reports ops/s, p50/p90/p99/p99.9/max (HDR-style histogram, 128 sub-buckets per octave, no allocation per sample), CPU time of the process, GC
counts, allocated bytes (`GC.GetTotalAllocatedBytes(true)`) per operation, thread pool work items per operation, and the CPU seconds of the server container (cgroup).
`oxia-load` prints the same fields for the Go client (CPU from `GetProcessTimes`, allocation from `runtime.MemStats`), so both are produced by the same kind of harness.
"Before" is the tree at commit `33f981b` (all components merged, before wave 2) built with the same load tool.

**How to read the numbers.** A batching client's cycle is `linger + round trip of the batch`; with one batch in flight per shard (the contract) the shard moves at most
`MaxRequestsPerBatch` operations per cycle, and a closed loop with N callers moves N per cycle. So at the default 5 ms linger and a 3 ms server, 256 callers give
`256 / 8 ms = 32k ops/s` -- whatever the client does -- unless the client adds latency of its own. That is what the timer fix removed. With a real server in the
container, a single shard saturates around 60-100k ops/s (the server then burns about one core), so most of the matrix is *server-bound* for both clients; the
CPU-bound comparison therefore uses an echo server.

## 3. What changed (and what each change was worth)

Measured one at a time on the same machine, interleaved runs, echo server in a child process unless stated (`1000 producers, put, linger 1 ms`).

| # | Change | Mechanism | Measured effect |
|---|---|---|---|
| 1 | **High resolution linger timer** (`BatcherTimer.cs`) | one thread, waitable timer `CREATE_WAITABLE_TIMER_HIGH_RESOLUTION`; portable fallback `Monitor.Wait` | wake-up of a 1 ms linger: **15.7 ms -> 1.5 ms**, of a 5 ms linger: **15.9 -> 5.3 ms** (`LingerTimerBenchmarks`). Docker, 256 callers, put+get, 5 ms linger: **16.3k -> 27-34k ops/s**, p50 15.5 -> 7.6 ms. One caller, 5 ms linger: p50 15.6 -> 6.6 ms. |
| 2 | **Call pool without a lock** (`PooledCalls.cs` `CallPool`) | per-thread cache of 32 + bounded lock-free queue; the thread that completes an operation rents the next one | rent+return 28.8 -> 4.1 ns; 16 threads 224 -> 0.7 ns per op; echo, 1000 callers: **857k -> 979k ops/s (+14 %)**, CPU 6.3 -> 5.6 us/op |
| 3 | **Custom batcher queue** (`BatcherQueue.cs`) with pooled parking | CAS+exchange enqueue, FIFO pooled waiters, one reusable consumer signal | enqueue+dequeue 37 -> 20 ns; 8 producers 544 -> 178 ns (capacity 4096), 722 -> 423 ns (22). At capacity 22 with 1000 callers (every add parks): **456k -> 588k ops/s (+29 %)**, **881 -> 313 B/op**, p99 11.0 -> 3.9 ms |
| 4 | **Default queue capacity `max(4096, MaxRequestsPerBatch)`** (Go: GOMAXPROCS = 22) | the queue absorbs what arrives while a full batch is in flight, nobody parks | 1000 callers: capacity 22 -> 1000: **662k -> 1.05 M ops/s (+58 %)**, CPU 9.1 -> 4.8 us/op. 4000 callers: 1000 -> 4096: 652k -> 1.03 M |
| 5 | **Pending adds without async state machines** (`AddObserver`, gate without wrapper, closure-free plain-write path) | pooled observer + FIFO parking fixes the place in line, so the "read lock" is released at once | allocation profile at capacity 22: 320 -> 153 B/op; the rest (ProtoVersion 76 + PutResponse 52 + PutResponse[] 17 B) is the wire codec |
| 6 | **Completed calls are their own thread pool work item** (`IThreadPoolWorkItem`) | no 48 B work item per completion, parked producers and the consumer wake-up likewise | allocation neutral within noise on the measured path (the work item was cheaper than expected); removes the per-wake allocation of the slow path |
| 7 | **FIFO ticket chain in the write gate** (`WriteManager.cs`), plain writes behind pending ephemeral puts (`Sessions.ExecuteOrdered`) | review R1-1/R2-D2/R2-D3: one caller's pipelined writes keep their order | fidelity fix, no measurable cost (the fast path is unchanged) |
| 8 | Long-lived loops without the caller's `ExecutionContext` | review R1-3 | fidelity fix |

Where the remaining time goes (`dotnet-trace`, 1.0 M ops/s, echo-proc, 1000 callers): about **two thirds of the CPU samples are thread pool workers spinning or waiting
for work** (`LowLevelLifoSemaphore.Wait/WaitForSignal`); the library's own frames (batcher loop, stream wrapper, batch completion, call pool) are each 4-10 %. Running the
awaiters' continuations *inline* on the completing thread (a measurement switch, unsafe: user code would run on the batcher loop) shows the floor: **1.5 us/op instead of 4.9**,
but one thread then serialises all the producers' work and the throughput drops from 1.05 M to 0.75 M ops/s. See 7 for the consumer-side knob.

## 4. Results: before / after / Go

__MATRIX__

## 5. Allocation per operation

`load --alloc-profile` (sampled `GCAllocationTick` events, put, 1000 callers, echo server in a child process):

| | B/op | by type |
|---|---|---|
| before (Channel queue, capacity 22) | ~880 | Channel parked writes (BlockedWriteAsyncOperation + state machine boxes ~500), call work items, ProtoVersion 77, PutResponse 52, PutResponse[] 17 |
| after | **153** | ProtoVersion 76, PutResponse 52, PutResponse[] 17 (all decoded response objects of the codec) |

The client's own share is 0 B/op on the fast path **and** on the parked path (asserted by `GateAndOptionsTests.ParkingOnAFullQueue_AllocatesNothing_OnceWarm`, Release only, and by the
existing `AsyncClientImplExtraTests.PutGetDelete_AllocateNothingPerOperation_*`). Gets additionally allocate the value copy (`byte[]`) and the get response objects (component A/Z1).
The Go client allocates 1.65 KB/op in the same workload (`runtime.MemStats`).

## 6. Micro-benchmarks (BenchmarkDotNet, ShortRun, `--filter "*Throughput*"`)

| Benchmark | Before | After |
|---|---|---|
| queue enqueue+dequeue, one thread (capacity 22 / 4096) | Channel 40.2 / 36.7 ns | BatcherQueue **19.5 / 20.5 ns** |
| queue, 8 producers + 1 consumer, per item (22 / 4096) | Channel 722 / 544 ns, 48 / 29 B | BatcherQueue **423 / 178 ns, 0 B** |
| call pool rent+return, one thread | locked stack 28.8 ns | **4.1 ns** |
| call pool, 16 threads, per op | locked stack 223.9 ns | **0.7 ns** |
| linger wake-up, 1 ms | `System.Threading.Timer` 15.70 ms | **1.54 ms** |
| linger wake-up, 5 ms | `System.Threading.Timer` 15.86 ms | **5.33 ms** |
| `BatcherImpl.AddAsync`, queue with room | - | 39 ns (24 B = the benchmark's own batch object) |
| `BatcherImpl.AddAsync`, queue of 2 (parks) | - | 237 ns |

## 7. What did NOT help (measured, then left alone)

| Idea | Result |
|---|---|
| HTTP/2 stream window 4 MiB, socket buffers 1 MiB, `EnableMultipleHttp2Connections` (`TransportTuning.cs`, `--tune h2stream/sndbuf/rcvbuf/multih2`) | 8 KiB values, 512 callers, docker: gets 16.1k -> 16.8k ops/s, puts 7.4k -> 7.2k ops/s: **no effect** (server and Docker Desktop bound). The handler's defaults stay. |
| Server GC (`DOTNET_gcServer=1`), echo-proc | 1.02 M -> 1.06 M ops/s: within noise; allocation is ~150 B/op, GC is not a cost. Workstation non-concurrent (the repo default) stays. |
| `DOTNET_TieredPGO=0` | -5 % (1.01 M -> 0.96 M). Keep the default (on). |
| `DOTNET_TieredCompilation=0` (everything fully optimised up front, no PGO) | **-40 %** (1.08 M -> 0.64 M) and p99 x5. Keep tiering. |
| Completions inline on the completing thread (`--tune inline=true`, unsafe) | CPU 4.9 -> 1.5 us/op but throughput -30 % (one thread runs all the callers' code) and user code on the batcher loop: **rejected**. A chunked dispatch (K completions per work item) would sit between; not done: it trades head-of-line blocking of unrelated callers for fewer thread pool wake-ups. |
| `timeBeginPeriod(1)` | Works (median 2.0 ms for a 1 ms linger, R1) but is process wide, raises the power draw of the host and changes every other timer of the application; the waitable timer is scoped to the linger service. Not used, no opt-in either. |
| ShardManager `Get`/hash | already lock-free, allocation-free and a few tens of ns (`ShardManagerBenchmarks`, component E); it is not in the top of any profile. Untouched. |

## 8. Proposal for the lead: pipelined write batches (NOT enabled, needs a decision)

The contract is one batch in flight per shard. `BatcherFactory.ExperimentalMaxInFlight` (internal, benchmarks only, default 1) lets the loop start the next batch while up to
K-1 earlier ones wait for their response (`--tune pipeline=K`). It keeps the order in which batches are *sent* (the stream serialises sends FIFO) and a Barrier waits for all of them
(`PipelineExperimentTests`), but has **no ordered retry**.

| docker, put only, 2000 callers, linger 1 ms | 1 in flight (contract) | 2 | 4 | 8 |
|---|---|---|---|---|
| 1 shard: ops/s | 73.4k | 91.6k | 94.9k | 95.2k |
| 1 shard: p50 / p99 | 26.2 / 44.7 ms | 21.5 / 31.5 | 20.4 / 33.1 | 20.3 / 32.9 |
| 4 shards: ops/s | 87.1k | - | 73.8k | - |

* Gain: **+25-30 % on a single shard** once the server's round trip per batch exceeds the linger (it does here: 1000-operation batches); **-15 % with 4 shards** (the shards already
  overlap each other on the one HTTP/2 connection; more in flight only adds contention). Nothing at all when the round trip is below the linger (the 5 ms default with a 3 ms server).
* Why it is not safe as is: when batch N fails (stream error, timeout, `ErrShardNotFound`), N+1.. were already on the wire. Each batch retries inside `RpcProvider.ExecuteWrite` on its own,
  so N+1 can be re-sent before N and two writes to the same key reorder; the split path (`WriteRerouter`) has the same problem across batches.
* What a safe version needs: a per-shard **write sequencer** in `RpcProvider` (batch sequence numbers; a retry waits until every lower number has completed or failed permanently; the
  rerouter / failure handling of batch N+1 runs after N's) plus fault-injection tests (kill the stream with K in flight, split with K in flight, timeout of the middle batch). That is a
  change in `RpcProvider`/`WriteStream` (components E/Z1) and in the batcher completion order, ~300 lines + tests.
* Recommendation: **reject for now**. +25 % on one saturated shard does not justify an ordering risk in the core write path; users who need more single-shard throughput get it from more
  shards or a larger `MaxRequestsPerBatch`. Revisit if a deployment with fsync-bound shards (where the server round trip is many linger times) shows up.

## 9. Recommended settings for consumers

* **Do not set anything process-wide for the client.** Defaults: workstation non-concurrent GC is fine (the client allocates ~0 B/op of its own), keep `TieredCompilation` and `TieredPGO` on (turning
  tiering off costs 40 %), no `timeBeginPeriod` (the linger service has its own high resolution timer), no `ThreadPool.SetMinThreads` needed for the client itself (min threads = cores).
* **Concurrency is throughput.** In a closed loop `ops/s = callers / (linger + server round trip)`. To use a 5 ms linger productively keep >= `target_rate x 8 ms` operations in flight
  (e.g. 256 for 32k ops/s, 1000+ for 100k+). Fewer, slower callers are latency bound, not CPU bound.
* **Linger**: 5 ms (default) is honoured to ~0.5 ms now. For latency-sensitive callers use 1 ms (p50 of a lone write 2.5 ms in docker) or 0 (no batching: ~0.7 ms per round trip but one
  round trip per operation).
* **`WithBatchQueueCapacity`** only matters beyond ~4096 concurrently pending operations per shard (back-pressure threshold; 22 would cost -40 %).
* **CPU-constrained hosts**: the thread pool spin-waits for work; `DOTNET_ThreadPool_UnfairSemaphoreSpinLimit=0` (or `System.Threading.ThreadPool.UnfairSemaphoreSpinLimit` in `runtimeconfig.json`) cut the
  CPU per operation from 5.6 to 3.6 us (-36 %) at the price of -11 % peak throughput and a worse p99 (1.9 -> 14 ms). A trade-off to take when the cores are needed elsewhere, not a default.
* Use one client per process and many concurrent callers; do not create a client per request (each owns a connection pool, batchers and a health loop).

## 10. Limits and open points

* The Go client and the .NET client reach the same throughput against the standalone server at moderate concurrency: the server (one shard = one core in the container) is the limit, not the clients.
* Remaining per-operation allocation (150 B for a put) is the decoded `PutResponse` + `ProtoVersion` + array; Z1 is pooling them.
* Not measured: TLS, authentication, a multi-node cluster, Linux/macOS (the linger service falls back to a timed `Monitor.Wait`, whose resolution there is the OS's ~1 ms; the unit tests cover that path).
* Windows only effect: other timers of the client (request timeouts, retry backoff, session keep-alive) still use `System.Threading.Timer` / `Task.Delay`; their precision (15.6 ms) is irrelevant at their
  scale (seconds).

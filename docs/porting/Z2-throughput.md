# Z2 — throughput & latency (wave 2)

Upstream: `github.com/oxia-db/oxia` @ `8456f61e1ada7b786f5ef17798b45fbc17d0d246`. Numbers, methodology and the head-to-head with the Go client are in
[`docs/performance.md`](../performance.md). Everything below preserves the behavioural contract of the port (one batch in flight per shard, per-shard FIFO of
writes, byte/count limits, linger semantics, drop-before-send through `CallContext`, `ErrShardNotFound` re-routing, split gates, retries); the one thing that
would change it (pipelined batches) is **not enabled** and is documented as a proposal.

## Sources (new and changed)

| Upstream file | C# file | Status | Notes (divergences, extra tests) |
|---|---|---|---|
| `oxia/batch/batcher.go` (`callC` channel, `Add`, `Run`, `Close`) | `src/Oxia.Client/Batch/BatcherQueue.cs` (`BatcherQueue`, `ParkedAdd`, `ConsumerSignal`) | new (replaces the Channel of `Batcher.cs`) | Lock-free bounded MPSC ring: CAS on the tail reserves a position (`tail - head < capacity`), one `Interlocked.Exchange` publishes; producers that find it full park on a pooled `IValueTaskSource`, FIFO (new adds queue behind them); the single consumer sleeps on one reusable `ManualResetValueTaskSourceCore`, woken only if it announced it sleeps; `Tick()` wakes it without an item; `Complete()` = Go's `close(closeC)` + `addsDone`. Parked producers and the consumer are woken by handing the **object itself to the thread pool** (`IThreadPoolWorkItem`: no allocation). Tests: `Throughput/BatcherQueueTests.cs` (exact capacity, FIFO incl. parked, wake-ups, exactly-once under 3 capacities × 8–32 producers, close racing with adds ×200). |
| `oxia/batch/batcher.go` (`time.NewTimer(linger)`) | `src/Oxia.Client/Batch/BatcherTimer.cs` (`LingerTimer`, `LingerTimerService`, `NativeTimer`) | new | One background thread per process waits on a **high resolution waitable timer** (Windows 10 1803+, `CREATE_WAITABLE_TIMER_HIGH_RESOLUTION`, ~0.5 ms); other platforms / older Windows: timed `Monitor.Wait` on the same thread. The 15.6 ms Windows tick made a 5 ms linger last ~15 ms (R1-4). `timeBeginPeriod(1)` was **not** used: it is process-wide, raises the power draw of the whole host and would also change every other timer of the application. Tests: `Throughput/LingerTimerTests.cs`. |
| `oxia/batch/batcher.go` | `src/Oxia.Client/Batch/Batcher.cs` | changed | Uses `BatcherQueue` and `LingerTimer`; loop started with `ExecutionContext.SuppressFlow` (R1-3); `ExperimentalMaxInFlight` (measurement switch, default 1). Header updated. |
| `oxia/batch/batcher_factory.go` | `src/Oxia.Client/Batch/BatcherFactory.cs` | changed | Internal `ExperimentalMaxInFlight(+Default)` only. `BatcherChannelBufferSize` (GOMAXPROCS) and the default `QueueCapacity` semantics are unchanged (the ported tests rely on them). |
| `oxia/internal/batch/batcher_factory.go` | `src/Oxia.Client/Internal/Batch/BatcherFactory.cs` | changed | Default queue capacity `max(4096, MaxRequestsPerBatch)` capped at 65 536 (`DefaultQueueCapacity`); Go: GOMAXPROCS. With the Go size every add of a thousand concurrent callers parked. |
| `oxia/internal/batch/write_manager.go` | `src/Oxia.Client/Internal/Batch/WriteManager.cs` | changed | **FIFO ticket chain** for the writes that cannot take the lock-free fast path (R1-1/R2-D2): the ticket is taken in the synchronous part of `AddAsync`, so one caller's pipelined `PutAsync` calls reach the batcher in issue order while a shard holds its writes / forwards (Go's `Put` blocks in `Add`). The fast path reopens only when the chain has drained (`_clearSlowWhenDrained`). The gate no longer wraps a parked batcher add in an async method (FIFO parking already fixes its place in line, so the "read lock" is released at once). Tests: `Throughput/GateAndOptionsTests.cs`, `Review/R1_HoldOrderTests.cs`, `Review/R2_WriteOrderingTests.cs`. |
| `oxia/async_client_impl.go` | `src/Oxia.Client/AsyncClientImpl.cs` | changed (+ `Pooled/PooledDeleteRange.cs`, Z1's file, one line) | `AddPlainWrite` (R2-D3): a plain write queues behind ephemeral puts that wait for their session (`Sessions.ExecuteOrdered`), lock-free and closure-free otherwise; pending adds are watched by the pooled `AddObserver` instead of `*AfterAddAsync` async methods; `WithBatchQueueCapacity` wiring. |
| `oxia/async_client_impl.go` (per-op channel) | `src/Oxia.Client/PooledCalls.cs` | changed | `CallPool` = per-thread cache + bounded lock-free queue (was one lock); pooled calls are their own `IThreadPoolWorkItem`; `AddObserver`; `CallDispatch.Inline` is a benchmark-only experiment switch. Tests: `Throughput/CallPoolTests.cs`. |
| `oxia/sessions.go` | `src/Oxia.Client/Sessions.cs` | changed | `ExecuteOrdered`/`HasWaitingOperations` (R2-D3), nothing else. |
| `oxia/options_client.go` | `src/Oxia.Client/Options/OptionsClient.cs`, `ClientErrors.cs` | changed | EXTENSION `OxiaOptions.WithBatchQueueCapacity(int)` (+ `ClientErrors.InvalidOptionBatchQueueCapacity`). |
| `common/rpc/connection.go` | `src/Oxia.Common/Rpc/Connection.cs` | changed | Health loop started with `SuppressFlow` (R1-3 pattern); `TransportTuning.Apply(handler)`. |
| `common/rpc/connection.go` (grpc-go resolves the proxy from the environment once) | `src/Oxia.Common/Rpc/TransportProxyCache.cs` | new | `IWebProxy` wrapper installed on every `SocketsHttpHandler` that has no explicit proxy: asks `HttpClient.DefaultProxy` once per (scheme, host, port). On Windows the default asks WinHTTP (`WinHttpGetProxyForUrl`) for **every request**, i.e. every Read batch / List / RangeScan / session call (+30 % ops/s, -0.17 ms per get at linger 0). Tests: `Throughput/TransportProxyCacheTests.cs`. |
| — | `src/Oxia.Common/Rpc/TransportTuning.cs` | new | Optional, **off-by-default** transport knobs (`InitialHttp2StreamWindowSize`, `SO_SNDBUF/SO_RCVBUF`, `EnableMultipleHttp2Connections`) used to *measure* them; none helped (docs/performance.md), so the defaults are the handler's. |
| `oxia/internal/shard_manager.go` | `src/Oxia.Client/Internal/ShardManager.cs` | changed | Receive loop started with `SuppressFlow` (R1-3 pattern). `Get`/hash untouched: E's version is already lock-free and allocation-free (re-measured, `ShardManagerBenchmarks`). |
| `oxia/internal/write_stream.go` | `src/Oxia.Client/Internal/WriteStream.cs` | changed (one line) | Receive loop started with `SuppressFlow` (R1-3). |

## Benchmarks and tools

| File | What |
|---|---|
| `benchmarks/Oxia.Client.Benchmarks/Load/**` | End-to-end load generator: `dotnet run -c Release --project benchmarks/Oxia.Client.Benchmarks -- load --help`. Closed loop (N producers) and open loop (`--rate`, latency from the intended start), async/sync client, put/get/delete mix, warm-up, HDR histogram (`LatencyHistogram`, no allocation per sample), GC counts, allocated bytes/op, CPU/op, thread pool work items, server container CPU (cgroup), `--alloc-profile` (sampled allocation by type), servers: `docker` (fresh `oxia standalone` in the reusable container), `echo` (in-process `RawEchoServer`), `echo-proc` (same in a child process: client-only CPU), `host:port`. `--tune name=value` switches single optimisations on/off. |
| `benchmarks/Oxia.Client.Benchmarks/Load/go/**` | `oxia-load`: the same workload and output for the Go client (`build.ps1` copies it outside the repo, adds `replace` directives to the pinned checkout, builds with the auto-downloaded Go 1.27 toolchain). `--go path\oxia-load.exe` runs it after every .NET run on a fresh server and prints both side by side. |
| `benchmarks/Oxia.Client.Benchmarks/Throughput/ThroughputBenchmarks.cs` | `QueueBenchmarks` (BatcherQueue vs the Channel), `CallPoolBenchmarks` (vs the locked pool), `LingerTimerBenchmarks` (wake-up latency), `BatcherAddBenchmarks`. |

## Tests

`tests/Oxia.Client.Tests/Throughput/`: `BatcherQueueTests` (13), `LingerTimerTests` (9; the `highResolution: true` case is a dynamic skip off Windows), `LingerTimerPortableTests` (4 test
methods / 6 cases, Linux validation: the Monitor.Wait path that Linux and macOS run in production, 1 ms / 5 ms lingers, `TicksToMilliseconds`; see [`docs/linux-validation.md`](../linux-validation.md)), `CallPoolTests` (7), `GateAndOptionsTests` (10: ticket chain, retired-gate forwarding order, failing slow-path add,
fast path reopening, queue capacity default/option, request timeout honoured while the first send waits for the connection, zero allocation of the parked path),
`PipelineExperimentTests` (3), `LatencyHistogramTests` (10, the histogram source is linked into the test project), `TransportProxyCacheTests` (4). One assertion-neutral fix to a review test:
`R1_HoldOrderTests` built its failure message with `First(...)`, which throws when the order is right — `FirstOrDefault` (the assertion is unchanged).

## Divergences from Go that matter to a reader of the code

1. The batcher queue is bigger (4096) and not a channel: a full queue still blocks (asynchronously) the producer, in FIFO order.
2. A completed call reaches its awaiter on the thread pool (as before), but without allocating and with one hop fewer for parked producers.
3. The linger timer is accurate to ~0.5 ms on Windows (Go: ns), instead of the 15.6 ms OS tick.
4. Long-lived loops (batchers, shard-map receive loop, write-stream receive loop, connection health loop, linger timer thread) do not capture the ambient
   `ExecutionContext` of the caller that happened to start them. **Not touched, to be handled by their owner:** the loops of `Sessions.cs` (`Task.Run(KeepAliveLoopAsync)`,
   `FollowSplitsOnShardMapChangeAsync`, `CreateSessionWithRetriesAsync`), `Notifications.cs` (`GetNotificationsWithRetriesAsync`), `SequenceUpdates.cs`, `Cache.cs`
   (`cm.RunAsync`) still capture the creating caller's context.

# D — batching, model, metrics

Upstream: `oxia/batch`, `oxia/internal/batch`, `oxia/internal/metrics`, `oxia/internal/model` @ `8456f61e1ada7b786f5ef17798b45fbc17d0d246`.

## Source files

| Upstream file | C# file | Status | Notes (divergences, extra tests) |
|---|---|---|---|
| `oxia/batch/batch.go` | `src/Oxia.Client/Batch/BatchContracts.cs` (`IBatch`) | ported (contract, lead) | `Complete()` is `ValueTask CompleteAsync()`. |
| `oxia/batch/batcher.go` | `src/Oxia.Client/Batch/Batcher.cs` (`BatcherImpl`) + `BatchContracts.cs` (`IBatcher`, `Barrier`) | ported | Bounded `Channel<object>` queue (SingleReader), `Close` = `TryComplete` + flag instead of `addMu`/`closeC`/`addsDone`; ONE reusable `Timer` used only as a wake-up hint for an idle loop (deadline = Stopwatch timestamp, armed lazily, early/stale ticks harmless); batches are recycled through `IRecyclableBatch`. See the header. |
| `oxia/batch/batcher_factory.go` | `src/Oxia.Client/Batch/BatcherFactory.cs` | ported | `GOMAXPROCS` → `Environment.ProcessorCount` (internal settable `BatcherChannelBufferSize`; per-factory `QueueCapacity`). Loop started by calling `RunAsync()` (idempotent). pprof labels dropped, `ct` ignored (as in Go). |
| `oxia/internal/batch/batcher_factory.go` | `src/Oxia.Client/Internal/Batch/BatcherFactory.cs` | ported | Derives from the public factory (Go embeds it). `WriteRerouter` / `ReadRerouter` delegates live here; the rerouters receive copies of the lists. |
| `oxia/internal/batch/manager.go` | `src/Oxia.Client/Internal/Batch/Manager.cs` | ported | `ConcurrentDictionary` + lock-on-create. `Dispose` throws the combined batcher errors (Go returns them). A batcher created after `Dispose` is created closed (Go leaks a live one). |
| `oxia/internal/batch/read_batch.go` | `src/Oxia.Client/Internal/Batch/ReadBatch.cs` | ported | Pooled (`ReadBatchFactory` owns the pool); request/lists/timeout source reused; metrics only when a listener is attached. |
| `oxia/internal/batch/write_batch.go` | `src/Oxia.Client/Internal/Batch/WriteBatch.cs` | ported | Pooled; `prepare` delegate allocated once per pooled batch; `dropCancelledCalls` compacts in place; UTF-8 byte size cached between `CanAdd` and `Add`; `ErrRequestTooLarge` is `ClientErrors.RequestTooLarge` (lead). |
| `oxia/internal/batch/write_manager.go` | `src/Oxia.Client/Internal/Batch/WriteManager.cs` (`WriteManager`, `WriteGate`) | ported | RWMutex read side = in-flight counter (Dekker fast path), write side = "set retired, wait for the counter"; `HandOff` is a non-blocking ordered FIFO per gate (see header); extra ctor overload with an awaitable `forward`. |
| — (no upstream) | `src/Oxia.Client/Internal/Batch/BatchPool.cs` | new | Lock-free single slot + tiny bounded queue; one pooled batch per batcher in practice. |
| — (no upstream) | `src/Oxia.Client/Internal/Batch/ReusableTimeout.cs` | new | One `CancellationTokenSource` per pooled batch (`TryReset`), armed only if the executor does not complete synchronously. |
| `oxia/internal/metrics/metrics.go` | `src/Oxia.Client/Internal/Metrics/Metrics.cs` | ported | `System.Diagnostics.Metrics`, Meter `oxia_client`. Calls are decorated only while an op instrument has a listener (`Instrument.Enabled`); decorator = copy of the call + one delegate. Batch recording is `RecordWrite`/`RecordRead` with a start timestamp (the Go-shaped `WriteCallback()`/`ReadCallback()` remain for the tests). `Metrics` is `IDisposable` (owns its Meter when no `IMeterFactory` is given). |
| `oxia/internal/metrics/timer.go` | `src/Oxia.Client/Internal/Metrics/Timer.cs` (`MetricsTimer`) | ported | `Counter<double>` "ms" + `Counter<long>` (no unit) with the SAME name, exactly as Go (System.Diagnostics.Metrics allows duplicate names). Class renamed (`Timer` clashes with `System.Threading.Timer`). |
| `oxia/internal/metrics/utils.go` | `src/Oxia.Client/Internal/Metrics/Utils.cs` (`MetricsUtils`, `MetricUnit`) | ported | OTel instrument-name validation re-implemented (panic message identical); byte unit is `"By"` (Go `common/metric/unit.go`), not `"bytes"`; `TagList` instead of an attribute set. |
| `oxia/internal/model/call_context.go` | `src/Oxia.Client/Internal/Model/CallContext.cs` | ported (lead) | Verified against the Go code and tests; no change needed. |
| `oxia/internal/model/model.go` | `src/Oxia.Client/Internal/Model/Model.cs` | ported (lead; extended) | Added `Clone()` / internal `CopyFrom()` on the four call classes (Go copied the structs); `InOpIndexOrder` rewritten without lambdas (composite sort keys). |

## Test files

| Upstream test | C# test | Go tests → ported | Notes |
|---|---|---|---|
| `oxia/batch/batcher_test.go` | `tests/Oxia.Client.Tests/Batch/BatcherTests.cs` | 4 → 4 | `TestBatcher`'s 3 subtests are `[Theory]` rows. |
| `oxia/internal/batch/manager_test.go` | `Batch/ManagerTests.cs` | 1 → 1 | |
| `oxia/internal/batch/read_batch_test.go` | `Batch/ReadBatchTests.cs` | 3 → 3 | `TestReadBatchAdd` (4 cases) and `TestReadBatchComplete` (3 cases) are theories. |
| `oxia/internal/batch/utils_test.go` | `Batch/UtilsTests.cs` | 0 → 0 | Helpers only (`add`, `shardId`, `one`, `two`). |
| `oxia/internal/batch/write_batch_test.go` | `Batch/WriteBatchTests.cs` | 10 → 10 | |
| `oxia/internal/batch/write_manager_test.go` | `Batch/WriteManagerTests.cs` | 12 → 12 | The test `forward` (blocking `m.Add`) uses the awaitable ctor overload. |
| `oxia/internal/metrics/metrics_test.go` | `Metrics/MetricsTests.cs` | 2 → 2 (12 subtests as theory rows) | OTel `ManualReader` → `MeterListener` (`Metrics/MetricsTestSupport.cs`). |
| `oxia/internal/metrics/utils_test.go` | `Metrics/UtilsTests.cs` | 5 → 5 | |
| `oxia/internal/model/call_context_test.go` | `Model/CallContextTests.cs` | 5 → 5 | |
| `oxia/internal/model/model_test.go` | `Model/ModelTests.cs` | 7 → 7 | |

Go test functions: 49. Ported: 49. Skipped: 0.

Extra tests (never replace the ported ones): `Batch/BatcherTests.Extra.cs` (many producers exactly-once, ordering, one batch at a time, linger, barrier, close racing with add ×100, back-pressure, failing batch), `Batch/WriteBatchTests.Extra.cs` (end to end through the real batcher: op indexes, recycling, UTF-8 sizes, shard-split ordering scenario, metrics of a real flow), `Batch/WriteManagerTests.Extra.cs` (non-blocking ordered hand-off, add/replace/close stress, error aggregation, closed batchers after dispose), `Batch/ReusableTimeoutTests.Extra.cs`, `Batch/AllocationTests.Extra.cs` (zero allocation of the add → complete → recycle path; **skips itself in Debug**, run `dotnet test -c Release`), `Model/ModelTests.Extra.cs` (Clone/CopyFrom field-drift detection), `Metrics/MetricsTests.Extra.cs`. Test helpers: `Batch/BatchTestUtil.cs` (Eventually/Receive until `TestUtil` exists in TestSupport), `Batch/ProtoAssert.cs`.

## Behavioural contract kept exactly (see the file headers)

* One batch is completed at a time per shard (the loop awaits `CompleteAsync`); the batch is completed when it reaches `maxRequestsPerBatch`, when the next call does not fit (`CanAdd` false → complete + new batch), when the linger elapsed, on a Barrier, or immediately per call when `linger == 0`.
* Close: the open batch and every queued call fail with `ClientErrors.ShuttingDown`; calls added concurrently with or after Close are failed right away through a throwaway batch; a Barrier added to a closed batcher is completed (`Done`) immediately; every call is completed or failed exactly once.
* Write batch: op indexes assigned in `Add` across the three kinds; byte limit via `CanAdd`; `prepare` runs once and drops the calls whose `CallContext` is done (the request is rebuilt keeping the op indexes), `errCallsDropped` when nothing is left; a failure of a request that never reached `prepare` is wrapped with `NotSent`; `ErrShardNotFound` goes to the rerouter (with the op indexes) instead of failing; any other error fails every call.
* Read batch: same shape; `ErrShardNotFound` → `ReadRerouter`.
* WriteManager: hold / retire / predecessorDone / drain with a Barrier, forwarding for removed shards, existing successors do not hold, merges hold until every predecessor handed off, `ShardsReplaced` after Close is ignored.

## Notes for the other components

* **Calls are mutable objects now.** `WriteBatch.Add` sets `OpIndex` on the call it is given, so one call instance must be added to ONE batcher only. Where Go copied a struct (`sendDeleteRangeToSuccessors` handing the same value to several shards) use `DeleteRangeCall.Clone()`.
* **Response objects** passed to `call.Complete(...)` are RECYCLED by the batch after the calls completed (wave 2 / Z1: `WriteBatch`/`ReadBatch` call `Release()` on the pooled response; a call must copy what it keeps, see [`../performance-allocation.md`](../performance-allocation.md)); before Z1 they were not recycled. The `WriteRequest`/`ReadRequest` (and `prepare`, and the timeout token) are reused after `ExecuteWrite`/`ExecuteRead` completes: the executor must not keep them.
* **`ErrShardNotFound`** is detected with `OxiaErrors.Is(err, OxiaErrors.ShardNotFound)`: the executor must surface the server error so that `Is` matches (sentinel itself or wrapped).
* **`System.Threading.Barrier` vs `Oxia.Client.Batch.Barrier`**: with `ImplicitUsings` both are imported; use `using Barrier = Oxia.Client.Batch.Barrier;`. Likewise `Oxia.Client.Internal.Metrics.Metrics` is shadowed by the namespace of the same name inside `namespace Oxia.Client.Internal.*`: alias it (`using OxiaMetrics = Oxia.Client.Internal.Metrics.Metrics;`).
* **`WriteManager`**: use `HandOff` from the write rerouter (never blocks, keeps the order) and the `Func<long, object, ValueTask>` constructor overload for `forward` (it may wait for the shard that receives the call to stop holding). `Dispose()` throws the combined errors of the batchers' `Dispose()` (Go returns them) after closing all of them.
* **`Metrics`** is `IDisposable`; dispose it with the client when no `IMeterFactory` was supplied.

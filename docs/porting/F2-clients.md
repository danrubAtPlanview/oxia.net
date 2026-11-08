# F2 — clients & admin

Upstream: `github.com/oxia-db/oxia` @ `8456f61e1ada7b786f5ef17798b45fbc17d0d246`, package `oxia` (`oxia/*.go`).

## Sources

| Upstream file | C# file | Status | Notes (divergences, extra tests) |
|---|---|---|---|
| `oxia/async_client_impl.go` | `src/Oxia.Client/AsyncClientImpl.cs` | ported | `clientImpl` → `AsyncClientImpl : IAsyncClient` (internal, fields internal+settable like the Go struct literal in the tests). Put/Get/Delete are the hot path: **pooled calls** (`PooledCalls.cs`) instead of a channel + closure per op. Delete-range re-routed to several successors is **cloned per successor** (calls are classes; every batch writes `OpIndex`). `List`/`RangeScan` use a per-shard unbounded queue + pump into the bounded output channel (List 1 = unbuffered, RangeScan 100) because `IExecutor`'s consumer is synchronous (see "Cross-component notes"). `panic("invalid call")` → `InvalidOperationException("invalid call")`. Go `validateKey` (`utf8.ValidString`) → no lone UTF-16 surrogates. |
| `oxia/async_client_impl.go` (the per-op channel/callback + `multiShardGetCallback`) | `src/Oxia.Client/PooledCalls.cs` | ported (new file) | `PooledPutCall`, `PooledDeleteCall`, `PooledGetCall` (`ManualResetValueTaskSourceCore`, bounded pool of 4096), `DeleteRangeCompletion`, `MultiShardGet`/`MultiShardGetCall`. A `Callback` set by a metrics decorator is still invoked by `Complete`. |
| `oxia/async_client_impl.go` (`NewAsyncClient`) + `oxia/sync_client_impl.go` (`NewSyncClient`) + `oxia/admin_client_impl.go` (`NewAdminClient`) + `oxia/cache.go` (`NewCache`) | `src/Oxia.Client/OxiaClient.cs` | ported | Static `OxiaClient`: `CreateAsyncClientAsync` / `CreateSyncClientAsync` (prepends `WithBatchLinger(0)`) / `CreateAdminClient` / `CreateCache<T>` (forwards to F1's `Cache.NewAsync<T>`). Factories take `params ReadOnlySpan<ClientOption>` so they resolve options synchronously and call a private async method; invalid options → faulted `ValueTask`. The construction logic itself is `AsyncClientImpl.NewAsyncClientAsync` (it lives in `async_client_impl.go` upstream). |
| `oxia/sync_client_impl.go` | `src/Oxia.Client/SyncClientImpl.cs` | ported | Talks to the inner client only through `IAsyncClient`; `callContextOption` is `CallContextOptions` (component C). Failures are thrown with `OxiaErrors.ToThrowable`. `select {result, ctx.Done()}` → `Task.WaitAsync(token)`. `getCacheManager` → `GetCacheManagerAsync()` under an async lock. |
| `oxia/admin.go` | `src/Oxia.Client/Admin.cs` | ported | `IAdminClient : IAsyncCloser` over the protoc-generated `Io.Oxia.Proto.V1` messages; `SplitShardResult` keeps `Error` (never thrown); `*uint32` → `uint?`; `(T, error)` → `ValueTask<T>` that throws the mapped `AdminError`. |
| `oxia/admin_client_impl.go` | `src/Oxia.Client/AdminClientImpl.cs` | ported | `executeAdminRPCWithRedirect`: 30 s deadline linked to the caller's token, immediate-retry loop (written inline: identical to `backoff.Retry` with `ZeroBackOff`), redirect on `NodeIsNotLeader` + coordinator-leader hint ≠ current target. Uses the generated client's virtual `CallOptions` overloads (the mock overrides them). |
| `oxia/admin_errors.go` | `src/Oxia.Client/AdminErrors.cs` | ported | `adminError` → `AdminError : Exception, IErrorIs` (`Is` = kind only, `InnerException` = `Unwrap`); `mapAdminError`/`wrapAdminError` in `AdminErrors`. The sentinels `ErrUnauthorized/ErrUnauthenticated/ErrUnknown` are in `ClientErrors.cs` (lead). |
| `oxia/oxia.go` | — | skipped (package doc only) | |

## Tests

| Upstream test file | C# test file | Go tests | Ported | Skipped | Notes |
|---|---|---|---|---|---|
| `oxia/async_client_impl_test.go` | `tests/Oxia.Client.Tests/AsyncClientImplTests.cs` | 29 | 29 | 0 | see the header of the file for the adaptations (callbacks → `Complete`, result channels → `ValueTask`, invalid-UTF-8 keys → lone surrogates, `DeadlineExceeded` → `IsDeadlineExceeded` with a `TimeoutException` fake, goroutine-leak checks → `RunningStreamTasks`) |
| `oxia/async_client_reroute_test.go` | `tests/Oxia.Client.Tests/AsyncClientRerouteTests.cs` | 4 | 4 | 0 | the ephemeral one needs F1's `Sessions`, E's `RpcProvider`, B's `ClientPool` for real (mock session server on `TestGrpcServer`) |
| `oxia/sync_client_impl_test.go` | `tests/Oxia.Client.Tests/SyncClientImplTests.cs` | 5 | 5 | 0 | two tests run the whole client (D, E, A, B, C) against `LeaderServer` (mock `OxiaClientBase` on `TestGrpcServer`) |
| `oxia/admin_client_impl_test.go` | `tests/Oxia.Client.Tests/AdminClientImplTests.cs` | 21 | 21 | 0 | pure fakes (subclass of the generated `OxiaAdminClient`, fake `IClientPool`); needs B's `OxiaErrors.FromGrpcError/GrpcCode/IntoGrpcStatusError` |
| `oxia/admin_errors_test.go` | `tests/Oxia.Client.Tests/AdminErrorsTests.cs` | 2 | 2 | 0 | |
| (extra, no upstream) | `tests/Oxia.Client.Tests/AsyncClientImpl.Extra.cs` | — | — | — | pooled calls reused / reset, idempotent `Complete`, completion from other threads (40k mixed ops), 10k pipelined puts, lone-surrogate key validation, `GetSequenceUpdates` needs a partition key, invalid put options, delete-range without shards, and the **allocation budget** of Put/Get/Delete (Release only; skipped in Debug) |

Totals: 61 Go tests (counting each `t.Run`/table case as part of its parent) ported as 61 test methods (109 xUnit cases with the theory rows and the extras);
all pass (Debug and Release) when run together with the merged components A–E, F1.

Fakes shared by the tests: `tests/Oxia.Client.Tests/Fakes/*.Extra.cs`, namespace `Oxia.Client.Tests.F2` (the Go tests share unexported
helpers across files — `staticShardManager`, `sessionRequestsServer`, … — and other components port the same names; a private namespace avoids
duplicate-type clashes at merge time).

## Benchmarks

`benchmarks/Oxia.Client.Benchmarks/Client/ClientOperationBenchmarks.cs`: single and 1000-way pipelined `PutAsync`/`GetAsync`/`DeleteAsync` against an
in-process `TestGrpcServer` (`MemoryDiagnoser`). The server (`RawEchoServer.cs`) allocates nothing per operation (it only counts the operations of a request
with a protobuf tag scanner and replies with pre-serialised bytes), so the `Allocated` column is the client's own (batcher + codec + gRPC plumbing).
Measured (ShortRun, Intel Core Ultra 9 185H, .NET 10, Release):

| Benchmark | Mean | Allocated |
|---|---|---|
| `SinglePut` / `SingleDelete` (linger 0, one round trip) | ~131 µs | 3.7 KB / 3.4 KB per round trip (gRPC call overhead) |
| `SingleGet` | ~380 µs | 9.7 KB |
| `PutPipelined` (1000 ops per batch, per op) | ~1.3 µs | 164 B |
| `GetPipelined` | ~1.6 µs | 335 B |
| `DeletePipelined` | ~0.8 µs | 64 B |

What those bytes are (GC allocation-tick profile, put): `ProtoVersion` 72 B + `PutResponse` 55 B + `PutResponse[]` 17 B = the **decoded response objects of the
wire codec** (components A/D: "response objects are not recycled by the batch"); F2's own share is the async state machine box of the *slow* path
(`PutAfterAddAsync`, 4 B/op average: only when `AddAsync` of the batcher is incomplete, i.e. its queue is full) -- the fast path allocates **0 B**
(asserted by `AsyncClientImplExtraTests.PutGetDelete_AllocateNothingPerOperation_WhenTheBatcherCompletesInline`, Release only: a batcher that completes inline,
best of 12 warmed rounds; the fake's own response objects are the only allocations). Get additionally allocates the value copy (`byte[]`, 124 B, until the
pooled-result API lands). Recycling the response objects after `Complete` (the pooled calls copy everything they need synchronously) would take
puts/gets/deletes to ~0 B/op end to end.

## Cross-component notes

* `IExecutor.ExecuteList` / `ExecuteRangeScan` take a **synchronous** consumer (`Func<…, Exception?>`), so the consumer cannot wait for room in a
  bounded channel the way Go's blocking `ch <- result` does: List/RangeScan buffer per shard in an unbounded queue and forward to the bounded output
  channel with a pump. The stream is therefore read eagerly (no back-pressure from a slow reader to gRPC). An async-consumer overload
  (`Func<…, ValueTask<Exception?>>`) in `IExecutor` would restore the Go behaviour.
* `WriteManager` is given the awaiting `Func<long, object, ValueTask>` forwarder (`ForwardWriteAsync`: the routing and the "invalid call" failure are
  synchronous, the add to the successors is awaited, a failure to add fails the call); `HandOff` (used by the write rerouter) is synchronous in D, and the
  `ReadRerouter` / ephemeral-put / multi-shard paths use fire-and-observe adds (a failure to enqueue fails the call).
* Calls are mutable classes that a batch writes `OpIndex` into: a delete range re-routed to several successors is cloned per successor with `DeleteRangeCall.Clone()`.
* `admin.proto` is generated `GrpcServices="Client"`: there is no `OxiaAdmin.OxiaAdminBase`, so no mock admin *server* can be hosted by `TestGrpcServer`
  (the Go admin test does not host one either: it mocks the client interface).

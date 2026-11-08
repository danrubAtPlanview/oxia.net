# Component E — shard manager & RPC provider

Upstream: `oxia/internal/*.go` (top level of the package, not `batch`/`model`/`metrics`), pinned at `8456f61e1ada7b786f5ef17798b45fbc17d0d246`.
C# namespace: `Oxia.Client.Internal`.

## Sources

| Upstream file | C# file | Status | Notes (divergences, extra tests) |
|---|---|---|---|
| `oxia/internal/shard_strategy_impl.go` (`shardStrategyImpl`, `NewShardStrategy`) | `src/Oxia.Client/Internal/ShardStrategy.cs` | ported | `Get(key)` keeps the predicate API (tests, custom strategies) but `ShardManager` calls `Hash(key)` once and searches its snapshot when the strategy is a `ShardStrategy` (no closure). `hashFunc` is an optional internal constructor argument (Go tests replace it). |
| `oxia/internal/shard_strategy.go` (`ShardStrategy`, `Shard`, `HashRange`) | `src/Oxia.Client/Internal/InternalContracts.cs` | ported (lead-owned) | Not touched by component E. |
| `oxia/internal/executor.go` (`Executor`) | `src/Oxia.Client/Internal/InternalContracts.cs` | ported (lead-owned) | Implemented by `RpcProvider`. |
| `oxia/internal/convert.go` (`toShard`, `toHashRange`, `hashRange`) | `src/Oxia.Client/Internal/Convert.cs` | ported | Class is `ShardConvert` (a `Convert` class would shadow `System.Convert` in the namespace). `(Shard, error)` -> `TryToShard(..., out Shard, out Exception?)`. |
| `oxia/internal/shard_manager.go` (`shardManagerImpl`, `NewShardManager`, `receive*`, `update`, `recordSuccessors`, `overlap`) | `src/Oxia.Client/Internal/ShardManager.cs` | ported | Lock-free reads: immutable `ShardMap` snapshot (sorted ranges + bucket table + id dictionary + successors + cached `GetAll` array + `Changed` task) published with `Volatile.Write` under a writer-only lock; the listener runs before the new snapshot is visible (Go: under the write lock). `Get` allocates nothing. `GetAll()` returns a shared array (do not modify). `panic("shard not found")` -> `InvalidOperationException`. Initial-wait timeout is a `TimeoutException`. Extra tests: `ShardManagerTests.Extra.cs` (random layouts vs linear scan, concurrency, listener ordering, Changed broadcast, receive loop). Benchmark: `benchmarks/.../Internal/ShardManagerBenchmarks.cs`. |
| `oxia/internal/dizzy_shard_manager.go` | `src/Oxia.Client/Internal/DizzyShardManager.cs` | ported | Go embeds the interface; every member is forwarded explicitly, `Leader` returns the lookup address. |
| `oxia/internal/rpc_provider.go` (`rpcProvider`, `executeWithRetry*`, `executeOnce`, `getWriteStream`, `isRetryableShardRequest`, `errShardHasNoLeader`) | `src/Oxia.Client/Internal/RpcProvider.cs` | ported | First attempt of write/read runs inline and enters the same retry loop only when it fails; failures are thrown (the `Retry` helper of Oxia.Common signals by throwing), shared sentinels are thrown through `OxiaErrors.ToThrowable`; `ErrShardHasNoLeader` is a shared `RpcException` matched by reference; write stream setup honours the request token through the first `Send`; `CloseAsync` also cancels the open write streams; `ExecuteRead` returns the stream's response as is when it is the only one. Extra tests: `RpcProviderTests.Extra.cs` (fakes). Benchmark: `RpcProviderBenchmarks.cs`. |
| `oxia/internal/rpc_provider.go` (`ShardMapTimer`) | `src/Oxia.Client/Internal/ShardMapTimer.cs` | ported | `backoff.Timer` -> `IRetryTimer`; `Changed` is a `Task?` captured at `Start`; no goroutine/channel (awaits `Task.Delay` against `Changed`), reusable across the attempts of one operation. Extra tests in `WriteStreamTests.Extra.cs`. |
| `oxia/internal/write_stream.go` (`streamWrapper`) | `src/Oxia.Client/Internal/WriteStream.cs` | ported | Sends serialised by a `SemaphoreSlim`, pending futures in a growable ring buffer under a separate lock, ONE receive loop per stream, futures completed outside the lock, failed-before-fail ordering kept; a send cut short by the request token kills the stream and surfaces as `OperationCanceledException`; a send on an ended stream is a retryable EOF. Extra tests: `WriteStreamTests.Extra.cs`. |

## Tests

| Upstream file | C# file | Status | Notes (divergences, extra tests) |
|---|---|---|---|
| `oxia/internal/convert_test.go` (1) | `tests/Oxia.Client.Tests/Internal/ConvertTests.cs` | ported 1/1 | |
| `oxia/internal/shard_strategy_impl_test.go` (1) | `tests/Oxia.Client.Tests/Internal/ShardStrategyImplTests.cs` | ported 1/1 | |
| `oxia/internal/dizzy_shard_manager_test.go` (2) | `tests/Oxia.Client.Tests/Internal/DizzyShardManagerTests.cs` | ported 2/2 | Against fakes (needs B: `WaitGroup`, `Retry`, `BackOff`). |
| `oxia/internal/shard_manager_test.go` (11) | `tests/Oxia.Client.Tests/Internal/ShardManagerTests.cs` | ported 11/11 | Subtests (`t.Run`) and the key-sorting loop are `[Theory]` rows. `TryRLock` assertion -> `IsUpdateLockHeld` (`Monitor.IsEntered`). slog `warningsHandler` -> injected `CapturingLogger`. Against fakes (needs B). |
| `oxia/internal/write_stream_test.go` (3) | `tests/Oxia.Client.Tests/Internal/WriteStreamTests.cs` | ported 3/3 | Debug-log capture through an injected logger (Go swaps the process-wide slog default). Against fakes (needs B: `Future`). |
| `oxia/internal/rpc_provider_test.go` (18) | `tests/Oxia.Client.Tests/Internal/RpcProviderTests.cs` | ported 18/18 | Real `ClientPool` / `OxiaClientRpc` over in-process Kestrel servers (`RpcMockServers.Extra.cs`, derived from the generated `OxiaClientBase`), exactly like the Go tests run real gRPC servers: need A (`OxiaClientRpc`) **and** B (`ClientPool`, `Retry`, `Future`, `BackOff`, error mapping). `context.DeadlineExceeded` assertions are `OperationCanceledException` (a token cannot tell a deadline from a cancel). `StreamRequestsFailOnCanceledContext`: gRPC.NET starts the call without I/O, so the `Cancelled` status surfaces from the first read of the stream. |
| — (new) | `tests/Oxia.Client.Tests/Internal/ShardManagerTests.Extra.cs`, `RpcProviderTests.Extra.cs`, `WriteStreamTests.Extra.cs` | extra | See the file headers. |
| — (new) | `tests/Oxia.Client.Tests/Internal/Fakes/FakeRpc.Extra.cs`, `RpcMockServers.Extra.cs` | test infrastructure | `FakeClientPool`, `RecordingClientPool`, `FakeOxiaClientRpc`, `FakeWriteStream`, `FakeServerStream<T>`, `FakeShardManager`, `FakeRpcProvider` (+ the Go mock providers), `CapturingLogger`; the three Go mock gRPC servers and the dial/deadline helpers. |

## Benchmarks

| File | What |
|---|---|
| `benchmarks/Oxia.Client.Benchmarks/Internal/ShardManagerBenchmarks.cs` | `Get` hot path with 1 / 16 / 256 shards vs the Go algorithm (predicate + linear scan), the bare hash, and `GetAll`. Budget: 0 B/op. |
| `benchmarks/Oxia.Client.Benchmarks/Internal/RpcProviderBenchmarks.cs` | `ExecuteWrite` / `ExecuteRead` success path with an in-memory stream. |

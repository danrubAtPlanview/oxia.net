# oxia.net — design contract for the port

.NET 10 / C# 14 port of the **Oxia Go client** (`github.com/oxia-db/oxia`, module `oxia/` + the parts of `common/` it uses).
Upstream reference checkout (read-only; any clone of `github.com/oxia-db/oxia`), pinned at commit
`8456f61e1ada7b786f5ef17798b45fbc17d0d246`.

## Goals (in priority order)

1. **Faithful port.** Same behaviour, same edge cases, same error semantics, same wire protocol. When Go and "idiomatic .NET"
   disagree, Go behaviour wins; the API *shape* is made .NET-friendly only where Go's shape cannot exist (channels, ctx, multiple returns).
2. **All tests ported, never weakened.** Every Go test (`*_test.go` in `oxia/`, the client-relevant parts of `common/`, and
   `tests/client`, `tests/sequence`, `tests/resolver`, …) gets a C# test with the **same name, same scenario, same assertions**.
   If a ported test fails, the *implementation* is wrong (or the port of the test harness is) — never edit an assertion to make
   it pass. A test that genuinely cannot run in .NET is kept as `[Fact(Skip = "reason")]` with the reason documented, not deleted.
3. **Zero allocation / maximum throughput on the hot path** (put/get/delete batching, wire encode/decode, shard routing),
   *without* changing observable behaviour. First make it correct and tested; optimisation is measured with BenchmarkDotNet and
   lands in a later phase. Design for it now (see "Performance rules").
4. **Syncable.** Every ported file starts with a `PORT NOTE` header naming the upstream file(s), the pinned commit, the intent, and
   every deliberate divergence. `PORTING.md` + `docs/porting/*.md` map upstream files → C# files so a future `git diff <old>..<new>`
   on the Go repo can be replayed file by file.

## Layout

```
src/Oxia.Common/      port of github.com/oxia-db/oxia/common (client-relevant subset)   namespaces Oxia.Common.*
src/Oxia.Client/      port of github.com/oxia-db/oxia/oxia                                namespaces Oxia.Client, Oxia.Client.Internal(.Batch|.Model|.Metrics), Oxia.Client.Batch
tests/Oxia.TestSupport/             shared test infra (TestGrpcServer = in-process Kestrel gRPC server; Docker Oxia fixture)
tests/Oxia.Client.Tests/            unit tests: port of oxia/**/*_test.go and common/**/*_test.go (mock servers, no real Oxia)
tests/Oxia.Client.IntegrationTests/ port of tests/client, tests/sequence, tests/resolver, … against a real Oxia server (Docker, reused)
benchmarks/Oxia.Client.Benchmarks/  BenchmarkDotNet
```

Protos: `src/Oxia.Common/Proto/*.proto` are **verbatim copies** of upstream (sync = copy files). protoc (Grpc.Tools) generates
Google.Protobuf classes in `Io.Oxia.Proto.V1` / `Replication` for all of them (admin API is on that cold path; client.proto
generated classes + `OxiaClient.OxiaClientBase` are the **wire oracle and mock-server base in tests**). The hot-path client
messages are *hand-written* in `Oxia.Common.Proto` (`ClientMessages.cs`) with hand-written codecs.
**Never import `Io.Oxia.Proto.V1` and `Oxia.Common.Proto` unaliased in the same file** (identical type names). In tests use
`using G = Io.Oxia.Proto.V1;`.

## File header template (mandatory on every ported file)

```csharp
// =====================================================================================================================
// PORT NOTE
//   Upstream : <path(s) in the Go repo, e.g. oxia/internal/shard_manager.go>
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : <what this file is for, in a few lines — the concept, not a line-by-line summary>
//   Divergences: <every deliberate difference from Go and why: allocation, .NET idiom, missing dependency, …>
// =====================================================================================================================
```
Test files: `Upstream : <the *_test.go>`; list in the header any Go test that is skipped or adapted and why.
Every upstream file maps to exactly one C# file with the Go file's name in PascalCase (`shard_manager.go` → `ShardManager.cs`),
except where a Go file declares several unrelated concepts (then one C# file per concept, each header lists the Go file).
Also add/maintain a row per file in your component's `docs/porting/<component>.md` (format below).

## Mapping rules Go → C#

| Go | C# |
|---|---|
| package `oxia` | `Oxia.Client` ; `oxia/internal` → `Oxia.Client.Internal` ; `oxia/internal/batch` → `Oxia.Client.Internal.Batch` ; `oxia/batch` → `Oxia.Client.Batch` ; `internal/model` → `…Internal.Model` ; `internal/metrics` → `…Internal.Metrics` |
| `common/constant` `common/proto` `common/rpc` `common/time` `common/concurrent` `common/compare` `common/hash` `common/auth` `common/failpoint` | `Oxia.Common.Constant` `.Proto` `.Rpc` `.Time` `.Concurrent` `.Compare` `.Hash` `.Auth` `.Failpoint` |
| exported `Foo` / unexported `foo` | `Foo` public / `Foo` internal (tests see internals via InternalsVisibleTo) |
| `context.Context` | `CancellationToken` (first parameter where Go has ctx, so a trailing `params ReadOnlySpan<T>` stays last) |
| `ctx` deadline/`context.WithTimeout` | `CancellationTokenSource.CancelAfter` linked to the parent (optimised later by a deadline wheel) |
| `ctx.Err()` | `ct.IsCancellationRequested` / `OperationCanceledException(ct)`; `context.DeadlineExceeded` → `TimeoutException` or gRPC `DeadlineExceeded` (`OxiaErrors.IsDeadlineExceeded`) |
| `error` value, sentinel `ErrX`, `errors.Is` | `Exception?`, shared never-thrown `OxiaException` instances, `OxiaErrors.Is(err, sentinel)` (walks InnerException/Aggregate). `errors.Wrap(ErrX,"m")` → `OxiaErrors.Wrap(sentinel,"m")` |
| `(T, error)` returns | async/hot paths: result structs with `Err`, or a `bool TryX(out T, out Exception? err)`; sync client: throws `OxiaErrors.ToThrowable(err)` |
| `errors.Is(err, io.EOF)` | `OxiaErrors.EndOfStream` sentinel / `RecvAsync` returning null |
| `status.Code(err)`, `codes.X` | `OxiaErrors.GrpcCode(err)` , `Grpc.Core.StatusCode.X` |
| `chan T` (result future, buffered 1) | `ValueTask<T>` (pooled `IValueTaskSource` on the hot path) |
| `chan T` (stream, many values) | `System.Threading.Channels.Channel<T>` ; consumers get `ChannelReader<T>` ; Go `close(ch)` → `Writer.TryComplete()` ; unbuffered → bounded capacity 1 |
| `chan struct{}` closed-to-broadcast | `Task` / `TaskCompletionSource` (RunContinuationsAsynchronously) or `CancellationToken` |
| goroutine `go f()` | `Task.Run(f)` / dedicated long-running `async Task` loop; keep one loop per shard exactly where Go has one goroutine |
| `sync.Mutex` / `RWMutex` | `lock` (short sections) ; `SemaphoreSlim`/`AsyncLock` only if the section awaits ; `ReaderWriterLockSlim` only when profiling says so |
| `sync/atomic` | `Interlocked` / `Volatile` |
| `sync.WaitGroup` | `Oxia.Common.Concurrent.WaitGroup` or a counter + TCS |
| `select` | `Task.WhenAny`/`await` over `ChannelReader.WaitToReadAsync`; prefer the cheaper single-source form |
| `io.Closer` | `IAsyncCloser` (`CloseAsync` throws combined error; `DisposeAsync` swallows) ; sync-only closers → `IDisposable` |
| `multierr.Combine/Append` | `AggregateException` (single error stays itself) |
| `slog` | `Microsoft.Extensions.Logging.ILogger` through the static `Oxia.Common.Logging.OxiaLog` (already exists) — log messages/levels copy Go's |
| OTel `metric.MeterProvider` | `System.Diagnostics.Metrics.IMeterFactory?` ; default meter `oxia_client` ; instrument names/units/tags identical |
| `*T` optional field | `T?` ; `*int64` → `long?` |
| `[]byte` | `ReadOnlyMemory<byte>` in, `byte[]?` out (until the pooled-result API lands) |
| `string` keys | `string` (UTF-16); validate with the same rule as `utf8.ValidString` (no lone surrogates); encode UTF-8 on the fly, never cache per-key `byte[]` |
| `tls.Config` | `SslClientAuthenticationOptions` |
| `grpc.DialOption` | `Action<GrpcChannelOptions>` |
| `time.Duration` | `TimeSpan` |
| `testing.T`, `testify` assert/require | xUnit v3 `Assert` (+ small helpers in TestSupport); `t.Run` subtests → `[Theory]`/separate `[Fact]`s named `Parent_Sub`; `t.Parallel()` ignored |
| Go test names `TestFooBar` | C# method `TestFooBar` in class `FooBarTests` named after the Go file (`foo_test.go` → `FooTests`) — keep the Go name verbatim so it can be grepped |

Naming of public API (full list in `Client.cs` / `OptionsContract.cs`): `NewAsyncClient` → `OxiaClient.CreateAsyncClientAsync`,
`NewSyncClient` → `CreateSyncClientAsync`, `NewAdminClient` → `CreateAdminClient`, `NewCache[T]` → `CreateCache<T>`,
`oxia.Version` → `RecordVersion`, `proto.Version` → `ProtoVersion`, Go option funcs → `OxiaOptions.*`, Go `Put/Get/...` → `PutAsync/GetAsync/...`.

## Performance rules (apply from the start; measure later)

* No LINQ, no closures/lambdas, no `params object[]`, no boxing, no `string` concatenation or `string.Format` on per-operation paths.
  Static lambdas/`static` local functions and state-passing overloads only.
* Per-operation objects (calls) are classes designed for pooling (`Reset()`); completion via `IValueTaskSource<T>` /
  `ManualResetValueTaskSourceCore<T>`; never `Task`/`TaskCompletionSource` per op. Pools are `ConcurrentQueue`/`Stack` with a bounded size.
* Spans everywhere on the encode/decode path; `ArrayPool<byte>.Shared`/`stackalloc` for transient buffers; `Utf8.FromUtf16`/`Encoding.UTF8.GetBytes(string, Span<byte>)`
  to encode keys straight into the output buffer; `System.IO.Hashing.XxHash3` for shard routing; `BinaryPrimitives`, `Unsafe`, `MemoryMarshal` freely (AllowUnsafeBlocks is on).
* Locks: keep critical sections tiny; hot read paths (`ShardManager.Get`) must be lock-free (immutable snapshot swapped with `Volatile.Write`) — implement the snapshot as a sorted array + binary search over hash ranges when the shard count is large.
* No exceptions for expected outcomes (key not found, version mismatch): they travel as `Exception?` values / statuses.
* Everything async is `ValueTask`-first and `ConfigureAwait(false)`; never block a thread (`.Result`, `.Wait()`, `GetAwaiter().GetResult()`) in library code.
* Keep semantics: where a Go detail looks like it only exists for ordering/safety (single in-flight batch per shard, ErrShardNotFound re-routing,
  held writes during splits, CallContext drop-before-send), it is **behavioural contract**, not an optimisation target.

## Component ownership (parallel work, disjoint files)

Each component works in its own git worktree/branch and only creates/edits the files listed for it. Cross-component
types are used through the signatures fixed in this repo skeleton. If your code needs a type owned by another component
that does not exist yet, create a **temporary stub in a file named `*.TempStub.cs`** (never committed — add it to `.git/info/exclude`
of your worktree or delete it before your final commit) so you can compile and run your own tests.

| id | component | owns (C# files) | upstream sources | upstream tests to port |
|---|---|---|---|---|
| A | wire codec | `Oxia.Common/Proto/Codec/**`, `Proto/OxiaClientRpc.cs`, `Proto/OxiaClientMethods.cs`, `tests/Oxia.Client.Tests/Proto/**` | `common/proto/client_vtproto.pb.go`, `client_grpc.pb.go`, `marshal.go`, `rpc/oxia_error_streaming_client.go`, `rpc/logging_client_rpc.go` | `common/rpc/logging_client_rpc_test.go` (if applicable) + new oracle/differential/fuzz tests |
| B | common utilities | `Constant/OxiaErrors.Grpc.cs`, `Time/**`, `Concurrent/**`, `Compare/**`, `Rpc/ClientPool.cs` (+`Connection.cs`, `MetadataInterceptor.cs`, `GrpcMetrics*.cs`), `Security/**`, plus fixes to `Hash/Auth/Failpoint` | `common/constant/errors.go`, `time/*`, `concurrent/*`, `compare/*`, `rpc/client_pool.go connection.go metadata.go mock.go grpcmetrics`, `security/tls.go`, `channel/noblock.go`, `hash`, `failpoint`, `auth` | `common/{constant,time,concurrent,compare,hash,failpoint,channel,rpc,security}/**/*_test.go` |
| C | options & public types | `Oxia.Client/Options/**` (replaces `OptionsContract.cs`), `Optional.cs`, `KeyOrder.cs`, `ResultsHeap.cs`, `ProtoUtils.cs`, `Resolver.cs`, `FailureInjection.cs` | `oxia/options_*.go optional.go key_order.go results_heap.go proto_utils.go resolver.go failure_injection.go client.go` | `oxia/{options_*,optional,key_order,resolver,client}_test.go` |
| D | batching | `Oxia.Client/Batch/Batcher*.cs`, `Internal/Batch/**`, `Internal/Metrics/**`, `Internal/Model/**` | `oxia/batch/*.go`, `internal/batch/*.go`, `internal/metrics/*.go`, `internal/model/*.go` | the `*_test.go` next to each |
| E | shard & RPC | `Internal/ShardManager.cs DizzyShardManager.cs ShardStrategy*.cs Convert.cs RpcProvider.cs WriteStream.cs ShardMapTimer.cs` | `oxia/internal/*.go` (top level) | `oxia/internal/*_test.go` |
| F1 | sessions, notifications, sequences, cache | `Oxia.Client/Sessions.cs Notifications.cs SequenceUpdates.cs Cache.cs` | `oxia/sessions.go notifications.go sequence_updates.go cache.go` | `oxia/{sessions,notifications,sequence_updates,cache}_test.go` |
| F2 | clients & admin | `Oxia.Client/AsyncClientImpl.cs SyncClientImpl.cs OxiaClient.cs AdminClient*.cs AdminErrors.cs` | `oxia/async_client_impl.go sync_client_impl.go admin*.go` | `oxia/{async_client_impl,async_client_reroute,sync_client_impl,admin_client_impl,admin_errors}_test.go` |
| T | test infra & integration | `tests/Oxia.TestSupport/**` (except `TestGrpcServer.cs`), `tests/Oxia.Client.IntegrationTests/**` | `tests/client/*`, `tests/sequence`, `tests/resolver`, `tests/assignments` | those |

### Cross-component signatures (fixed)

```csharp
// E
Oxia.Client.Internal.RpcProvider : IRpcProvider
    RpcProvider(CancellationToken ct, string ns, IClientPool clientPool, string serviceAddress, Func<IShardManager?> shardManagerSupplier, Failpoints? failpoints = null)
    static RpcProvider Create(CancellationToken ct, string ns, SslClientAuthenticationOptions? tls, IAuthentication? auth, string serviceAddress,
                              Func<IShardManager?> shardManagerSupplier, params Action<GrpcChannelOptions>[] dialOptions)
Oxia.Client.Internal.ShardManager : IShardManager
    static ValueTask<IShardManager> CreateAsync(IShardStrategy strategy, IRpcProvider rpc, string serviceAddress, string ns, TimeSpan requestTimeout, ShardsReplacedListener? onShardsReplaced)
Oxia.Client.Internal.DizzyShardManager : IShardManager   (same CreateAsync)
Oxia.Client.Internal.ShardStrategy : IShardStrategy       static ShardStrategy New()
Oxia.Client.Internal.ShardMapTimer : IRetryTimer          public Task? Changed { get; set; }
// D
Oxia.Client.Internal.Metrics.Metrics(IMeterFactory? factory)   DecoratePut(PutCall) -> PutCall   DecorateDelete / DecorateDeleteRange / DecorateGet likewise
Oxia.Client.Internal.Batch.BatcherFactory(IExecutor executor, string ns, TimeSpan linger, int maxRequestsPerBatch, Metrics metrics, TimeSpan requestTimeout)
    WriteRerouter WriteRerouter {get;set;}  ReadRerouter ReadRerouter {get;set;}
    IBatcher NewWriteBatcher(CancellationToken ct, long shardId, int maxWriteBatchSize)   IBatcher NewReadBatcher(CancellationToken ct, long shardId)
    delegate void WriteRerouter(long shardId, List<PutCall> puts, List<DeleteCall> deletes, List<DeleteRangeCall> deleteRanges);  delegate void ReadRerouter(List<GetCall> gets);
Oxia.Client.Internal.Batch.WriteManager(CancellationToken ct, Func<CancellationToken,long,IBatcher> batcherFactory, Action<long,object> forward) : IDisposable
    ValueTask AddAsync(long shardId, object call)   void HandOff(long shardId, object call)   void ShardsReplaced(IReadOnlyDictionary<long,long[]> replaced)
Oxia.Client.Internal.Batch.Manager(CancellationToken ct, Func<CancellationToken,long,IBatcher> batcherFactory) : IDisposable     IBatcher Get(long shardId)
// F1
Oxia.Client.Sessions : IAsyncCloser   ctor(CancellationToken ctx, IShardManager sm, IRpcProvider rpc, ClientOptions options)
    void ExecuteWithSessionId(Func<long> shardForKey, Action<long /*shardId*/, long /*sessionId*/, Exception?> callback)
Oxia.Client.NotificationsImpl : INotifications   static ValueTask<NotificationsImpl> CreateAsync(CancellationToken clientCtx, ClientOptions options, IRpcProvider rpc, IShardManager sm)
Oxia.Client.SequenceUpdates   static ChannelReader<string> Create(CancellationToken ctx, CancellationToken clientCtx, string prefixKey, string partitionKey, IRpcProvider rpc, IShardManager sm)
Oxia.Client.CacheManager : IAsyncCloser   static ValueTask<CacheManager> CreateAsync(ISyncClient client, Failpoints? failpoints)
public interface ICache<TValue> : IAsyncCloser {...}  public static class Cache { static ICache<T> New<T>(ISyncClient client, SerializeFunc, DeserializeFunc) }  // F1 defines; F2's SyncClientImpl exposes GetCacheManagerAsync()
// F2
Oxia.Client.OxiaClient (static)  CreateAsyncClientAsync(string serviceAddress, params ReadOnlySpan<ClientOption>)  CreateSyncClientAsync(...)  CreateAdminClient(string adminAddr, SslClientAuthenticationOptions?, IAuthentication?)
Oxia.Client.SyncClientImpl : ISyncClient  ctor(IAsyncClient asyncClient)   (internal NewSyncClient(asyncClient) used by tests)  GetCacheManagerAsync(): ValueTask<CacheManager>
```
A component may add members/overloads, but must not change or remove these.

## Test porting protocol

1. Read the Go test file fully, then the C# types it exercises. Port **every** test function (and every `t.Run` case) — count them; the header of your test file states `Go tests: N, ported: N, skipped: k (reasons)`.
2. Same scenario, same inputs, same assertions. Translate testify (`assert.Equal(expected, actual)` → `Assert.Equal(expected, actual)`, `require.NoError(err)` → no exception, `assert.ErrorIs(err, X)` → `Assert.True(OxiaErrors.Is(err, X))`, `assert.Eventually` → `TestUtil.Eventually(...)` in TestSupport).
3. Go mock gRPC servers → services derived from the generated `OxiaClient.OxiaClientBase` hosted by `TestGrpcServer`.
4. Timing-based tests keep their timings (they were tuned upstream); tolerate CI jitter only by *widening* nothing — if flaky, report instead of changing.
5. Run: `dotnet test tests/Oxia.Client.Tests --filter "FullyQualifiedName~<YourClass>"`. Never run the whole solution's tests from several agents at once inside one directory — you have your own worktree, build there.
6. Extra tests (intent/edge/property/fuzz/concurrency) are welcome in files named `*.Extra.cs`; they never replace ported tests.
7. Port-fidelity bugs found in *other* components: do not fix silently — write the failing test, mark it `[Fact(Skip="BUG <component>: <one line>")]`? **No** — leave it failing and list it in your final report under "Cross-component defects" with the exact test name and observed behaviour.

## `docs/porting/<component>.md` format

```
| Upstream file | C# file | Status | Notes (divergences, extra tests) |
```
`Status`: `ported` | `partial` | `skipped (reason)`. One table per component file; the lead aggregates them into `PORTING.md`.

## Worktree protocol

* Work only inside your worktree; commit early and often on your branch with message `component <id>: <what>`; end commit messages with
  `Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>`.
* Do not edit files you do not own (except to fix a compile error in the skeleton: then tell the lead in your report).
* `dotnet build` the projects you touch (not necessarily the whole solution) with `-nologo -v q`. Warnings are errors.
* Final report: files created, test counts (Go vs ported vs passing), skipped tests with reasons, cross-component defects, open questions.

## Decisions log (lead, during integration)

* **Write-batch pipelining: rejected.** Prototyped by wave 2 (`BatcherFactory.ExperimentalMaxInFlight`, off, internal): +25-30 % on one shard but -15 % on four,
  and without an ordered retry a failing batch behind another can reorder writes to the same key. Go's "one batch in flight per shard" stays the contract.
* **Batcher queue capacity default `max(4096, MaxRequestsPerBatch)`** (Go: `GOMAXPROCS`), exposed as the extension `OxiaOptions.WithBatchQueueCapacity`:
  with 22 slots every add of 1000 callers parked (-40 % throughput). Same extension class as `WithMaxBatchSize`.
* **Linger timer**: a process-wide high-resolution waitable-timer service on Windows (never `timeBeginPeriod`, which is process-wide and raises the power draw of the host).
* **Server image for the integration fixture**: `oxia/oxia:nightly-beta` (built from upstream main; `oxia/oxia:main` on Docker Hub is stale and predates fields the pinned client uses).
* **List/RangeScan back-pressure** follows Go (bounded per-shard channels, async consumer overloads in `IStreamingExecutor`); the first port buffered eagerly.
* **Pooled objects and exactly-once completion**: a pooled call/future must not be touched by the previous round's completer after `Reset`/recycle
  (`Future<T>.Reset` waits for an in-flight publish; see `docs/review-findings.md`, lead-1).
* **Test stability policy**: ported assertions are never loosened; wall-clock-sensitive tests run in the non-parallel `R1 serial` collection, wait on events instead of sleeping,
  and the test process pre-sizes the thread pool (`TestAssemblySetup`).

# End-to-end allocation elimination (wave 2 / Z1)

Goal: drive the steady-state client-side allocations per operation to ~0 B/op end to end — including the response path.  Measured with
BenchmarkDotNet `--memory`, `GC.GetTotalAllocatedBytes`, and an in-process `EventListener` on the runtime's GC allocation ticks (no
dotnet-counters).  **The machine is a noisy, shared one (other agents run builds and test suites on it): `--job short` figures and the
sampled profiles move by 20–50 % between runs; read orders of magnitude and ratios, not decimals.**

## Results

### Before / after, real transport (BenchmarkDotNet `--job short --memory`, `ClientOperationBenchmarks` against the zero-allocation echo server)

The in-process server shares the process, so the figures include Kestrel's own allocations and Grpc.Net.Client's per-message cost, spread over
the operations of a batch (1000 per batch for the pipelined rows, one for the `Single*` rows).

| Benchmark | Before | After | Notes |
|---|---|---|---|
| `PutPipelined` (per op) | 160 B | **25 B** | response graph (`PutResponse` + `ProtoVersion` + lists, ~150 B) recycled; the rest is the slow path of the batcher queue (Z2) and the transport |
| `DeletePipelined` (per op) | 57 B | **15 B** | |
| `GetPipelined` (per op, found, 100-byte value) | 314 B | **163 B** | 124 B of it is the `byte[]` the faithful API must hand over |
| `SinglePut` (one round trip) | 3706 B | 2659 B | transport: ~2.3 KB per write-stream message |
| `SingleDelete` | 3412 B | 2469 B | |
| `SingleGet` | 9681 B | 9049 B | transport: ~9 KB per `Read` call (a new HTTP/2 stream per call) |

Pooled read API (`PooledReadBenchmarks`, 16 reads per `Read` call, 1000-record scans):

| Benchmark | Allocated |
|---|---|
| `GetFaithful` / `GetPooled` (per get) | 682 B / 551 B — dominated by the ~9 KB `Read` call shared by 16 gets (570 B/op); the value itself is 124 B → 0 |
| `ScanFaithful` / `ScanPooled` (per record) | 548 B / **19 B** |

### Our side only, end to end (`EndToEndAllocationTests`, Release, 100 000 operations, 12 operations in flight, best of 3)

A real `AsyncClientImpl` (options, batchers, `RpcProvider`, `StreamWrapper`, `OxiaClientRpc`, metrics interceptor) over `FakeWireInvoker`: the
real serializers run on every request and the real (pooled) deserializers on every response; the fake network allocates nothing per
operation (except one `Task` per waiting read, which the test subtracts).  These are the budgets the tests assert.

| Scenario | Measured | Budget | Why not 0 |
|---|---|---|---|
| Put | 4–11 B/op | 16 | boxes of the pooling async-method builders rented on one thread and returned on another (receive loop → batcher loop), ~1 per batch |
| Delete | 10 B/op | 16 | same |
| DeleteRange (partition key / all shards) | 7–8 B/op | 16 | same (was ~1.4 KB: wait group channel + closure + call per shard) |
| Get, key not found | 5.3 B/op | 12 | the `ServerStreamingClient<T>` wrapper (64 B per `Read` call) |
| Get, found, faithful API | 133 B/op | 136 | **124 B is the required `byte[]`** (100 + 24 header) |
| Get, found, **pooled API** | 5.3 B/op | 12 | as not-found: the value is in a pooled lease |
| RangeScan, 1000 records, **pooled API** | 2.3 B/record | 8 | one reader + tasks + token per scan (~2 KB) |
| Notification delivery | 126 B/notification | 136 | **the public `Notification` (48 B) + its key string (72 B)**; the wire objects are pooled |
| Session keep-alive | 0.0 B/call | 4 | — (the real HTTP/2 unary call is ~3 KB once per tick) |

Process-wide type attribution of the real-transport run (`dotnet run -c Release --project benchmarks/Oxia.Client.Benchmarks -- alloc`, 400 000 ops,
1000 in flight): put/delete/delete-range/get-pooled 15–50 B/op (all `AsyncStateMachineBox` of the slow path of `AddAsync` — Z2's batcher — plus
single boxes of Grpc.Net.Client/Kestrel), get-found 197 B (130 of them the value array), classic scan 542 B/record (value array, key string,
`RangeScanResult` channel segments) vs pooled scan 19 B/record.

## The ownership protocol (rules other code must follow)

Response messages are **rented** by the gRPC deserializer and **released** by their final consumer.

| Message | Rented by | Released by |
|---|---|---|
| `WriteResponse` | `WriteResponseMarshaller` (the write-stream receive loop) | `WriteBatch` after `Handle` (calls completed) |
| `ReadResponse` | `ReadResponseMarshaller` / `ReadResponsePooledMarshaller` | `ReadBatch` after `Handle`; a further chunk of a stream is `Absorb`ed (its gets move into the first response, its shell is released) by `RpcProvider.ReadAttempt` |
| `RangeScanResponse`, `ListResponse` | their marshallers | `RpcProvider.ExecuteRangeScan` / `ExecuteList` after the consumer's task completed |
| `NotificationBatch` | `NotificationBatchMarshaller` | `NotificationsImpl` after the batch was delivered (`nb.Release()`) |
| `KeepAliveResponse`, `CloseSessionResponse` | — one shared field-less instance | never |
| `ShardAssignments`, `CreateSessionResponse`, `GetSequenceUpdatesResponse` | `new` (cold paths) | never |

Rules:

1. **`Release()` exactly once, by the owner, after the last read.**  It `Reset()`s the message: its children (`PutResponse`, `ProtoVersion`, `GetResponse`, …)
   go to the message's `Spares` and are reused by the next parse.  Nothing read *from the objects* (not the objects themselves) may be used afterwards.
   Strings and `byte[]` values copied out of them are immutable/independent and stay valid.
2. **`Release()` is idempotent and a harmless no-op on a message made with `new`** (`PoolState == Plain`): fakes/mocks in tests keep their responses.
   A second `Release()` immediately after the first does nothing; a stale `Release()` after the message was rented again is a protocol violation (as
   with every pool).
3. **Never keep a child.**  A call that must keep part of a response beyond `Complete` copies it: `GetResponse.Clone()` (used by `MultiShardGet`),
   `TakeValueArray()`, `DetachLease()`.  `GetResponse.Value` ownership passes to the caller: `Reset()` only drops the reference.
4. **A response that is never released is not a leak** — it is garbage collected; the pool only misses a reuse.  (A thrown consumer therefore does not
   need a `finally`; `WriteBatch`/`ReadBatch` use one anyway.)
5. **Debug builds detect use after release**: the lists of a released message (`Puts`, `Deletes`, `DeleteRanges`, `Gets`, `Records`, `Keys`, `Notifications`)
   throw `ObjectDisposedException`.  Release builds compile the check out.
6. **Completions must not throw into the batch**: `WriteBatch`/`ReadBatch` wrap every per-call `Complete` (a throwing `Callback` or metrics observer is logged
   and dropped), and a throwing rerouter is logged and dropped, because the batcher's catch-all `batch.Fail(e)` would otherwise complete calls twice — calls
   that may be recycled and owned by other operations by then (review R1-2).
7. Pools are bounded (64 messages per type, 4096 spare children per list, 8192 list capacity; leases: 16 384 objects, kept buffers ≤ 64 KiB within a 32 MiB
   budget), so a burst or one huge response cannot pin memory.

`GetResponse.Value` of the faithful API is still an exact-size `byte[]` created per value (required: the user owns it).  The opt-in pooled API below avoids it.

## API additions (public, `Oxia.Client.Pooled`; no existing signature changed)

```csharp
using Oxia.Client.Pooled;

using PooledGetResult r = await client.GetPooledAsync("key");          // IAsyncClient extension
if (r.Err is null) Process(r.Value.Span);                              // ReadOnlyMemory<byte>, valid until Dispose

ChannelReader<PooledGetResult> scan = client.RangeScanPooledAsync(ct, "/a/", "/a0");
await foreach (var record in scan.ReadAllAsync(ct))
    using (record) Process(record.KeyUtf8, record.Value.Span, record.Version);   // no string, no array per record
```

* `PooledGetResult : IDisposable` — `Value`, `HasValue`, `Key` (string; free for a Get, decoded once on demand for a scan record), `KeyUtf8`, `Version`, `Err`.
  A double `Dispose` (also of a copy of the struct) is ignored (generation check); a stale copy reads an empty `Value`; a result with `Err` holds no buffer;
  a result that is never disposed is not a leak (the buffer is GC'd, just not reused).
* `GetPooledAsync(key, options)` — same semantics and options as `GetAsync`.  A get that needs several shards (comparison other than `Equal`, or an index,
  without a partition key) and any `IAsyncClient` that is not the library's falls back to `GetAsync` and *copies* the value into a lease (same contract, not zero-allocation).
* `RangeScanPooledAsync(ct, min, max, options)` — same shards / key order / `IncludeValue` / `EndInclusive` / error semantics as `RangeScanAsync`; the error is the last
  item; a cancelled token completes the reader.  The reader is single-consumer; with the real executor each shard's scan back-pressures gRPC when 1024 records are queued.
* Mechanism: a read request containing a pooled-value call (or a pooled scan) is sent with `PooledValues`; `OxiaClientRpc` then uses the `ReadPooled` / `RangeScanPooled`
  descriptors (same wire name, different marshaller), whose deserializer copies each record's value (and key) into a `ValueLease` — one buffer from `ArrayPool<byte>` kept by
  the pooled lease object — and the call detaches it.  Classic gets in the same batch get an exact-size array copied out of their lease.
* `ISyncClient` has no pooled variants (it wraps `IAsyncClient` and returns `byte[]` by contract).

## What was removed, and where the remaining bytes come from

| Source | Before | Now | How |
|---|---|---|---|
| Decoded response graph: `PutResponse` + `ProtoVersion` + `PutResponse[]` growth | ~150 B/op | 0 | pooled `WriteResponse` + `Spares`, released by `WriteBatch` |
| `GetResponse` + `ProtoVersion` + list growth | ~190 B/op | 0 | same for `ReadResponse` |
| Value `byte[]` of a get | 124 B | 0 (pooled API) | `ValueLease` |
| Key string + value array per scan record | ~420 B | 0 (pooled API) | `RangeScanPooled` |
| Client identity string per ephemeral version | 40–60 B | 0 after the first | `Utf8StringCache` (64 slots, ASCII ≤ 128 bytes, never record keys) |
| Wait group channel + closure + call per shard of a DeleteRange | ~1.4 KB | ~0 | `PooledDeleteRange` |
| `Future<WriteResponse>` per batch | 107 B | 0 | reused when the receive loop completed it |
| `Task<bool>` + async state machine per message in the metrics interceptor wrappers | ~190 B / message | 0 | `IObservedStreamReader/Writer` + pooling builder |
| Boxes of `RecvAsync`, `SendAsync`, `WriteBatch.CompleteAsync`, `ReadBatch.CompleteAsync` | ~600 B / batch | pooled | `PoolingAsyncValueTaskMethodBuilder` |
| `KeepAliveResponse` / `CloseSessionResponse` per call | 24 B | 0 | one shared instance |
| Notification wire objects (batch, entry, wire notification, lists) | ~200 B / notification | 0 | pooled `NotificationBatch` |

Remaining, and why:

* **The user-facing objects of the faithful API**: `byte[]` values, key strings of range scans, `Notification` + key string.  Required by the API shape; the pooled API removes the first two.
* **Grpc.Net.Client + HttpClient + Kestrel (in the benchmark)**: ~2.3 KB per write-stream message and ~9 KB per `Read` / `RangeScan` call.  Measured type by type
  (`alloc put-single get-single`): per write-stream message ~15 `AsyncStateMachineBox`es (`WriteMessageAsync`, `MoveNextCore`, `ReadMessageAsync`, `Http2Stream.SendDataAsync` / `ReadDataAsync`,
  `StreamWrapper.ReadAsync`, …) plus `WriteQueueEntry`s and a `Task`; per `Read` call additionally a `GrpcCall` (266 B) + `RunCall` box, `HttpRequestMessage` + `HttpRequestHeaders` +
  `HttpContentHeaders`, `HeaderEntry[]` (500 B), `Http2Stream` (224 B), `CallbackNode`s, `CancellationTokenSource`s, `Task`s, the `Metadata`, `AsyncServerStreamingCall` + 3 delegates in the
  metrics interceptor.  These are inside Grpc.Net.Client / System.Net.Http and cannot be removed from our side; they are shared by all the operations of a batch (≈ 0.2 B/op at 1000 ops
  per batch, 190 B/op at 12).  **Not done** (would mean replacing the transport): a custom `HttpContent`/`SocketsHttpHandler` reuse, a hand-rolled HTTP/2 framing for `Read`.  Cheaper
  idea left open: the `Read` rpc could ride the already-open write-stream style connection if the server offered a bidirectional read stream — it does not.
* **Our per-call objects**: `ServerStreamingClient<T>` (64 B per `Read`/`RangeScan`/notification stream call), the interceptor's per-call `AsyncServerStreamingCall` + 3 delegates (~340 B per call; its
  per-message wrappers are allocation-free now), the per-scan reader/tasks (~2 KB per scan).  Pooling the `ServerStreamingClient` was rejected (a double `Dispose` of a pooled wrapper would
  corrupt its next owner for ~0.06 B/op at 1000 gets per call).
* **Pooling-builder boxes crossing threads** (~1 per batch): `PoolingAsyncValueTaskMethodBuilder` keeps boxes in per-thread caches; a box rented on the receive-loop thread and returned on the batcher
  thread migrates between caches and sometimes misses.
* **The slow path of `AddAsync`** (more than `ProcessorCount` operations queued): `AsyncStateMachineBox` of `PutAfterAddAsync` / `AddSlowAsync` / `BlockedWriteAsyncOperation` — 10–40 B/op in the
  1000-in-flight benchmarks.  That is the batcher's (Z2); the allocation tests keep fewer operations in flight.

Analysed and **not done**: *avoiding the transcoding of a response key that equals the request key* — per `client.proto` the key arrives in a get response only "in case of non-exact queries (eg. floor, ceiling)", and
in a put response for server-generated sequence keys (for `Equal` gets the field is absent and the client reuses the request's string), and the deserializer runs before the request is known (no per-call context in the gRPC
`DeserializationContext`), so it would need a per-call `Method` (≈ 200 B per call) to save a string only in the minority case.  Interning of `SecondaryIndexKey` was left out too (index keys can be high-cardinality).

## How to measure

```
dotnet test tests/Oxia.Client.Tests -c Release --filter "FullyQualifiedName~Oxia.Client.Tests.Alloc"      # budgets (needs >= 14 cores; run alone)
OXIA_Z1_PROFILE=1 dotnet test tests/Oxia.Client.Tests -c Release --filter "FullyQualifiedName~AllocationProfileTests" --logger "console;verbosity=detailed"
dotnet run -c Release --project benchmarks/Oxia.Client.Benchmarks -- alloc [put|delete|deleterange|deleterange-pk|get-notfound|get-found|get-pooled|put-single|get-single|scan|scan-pooled]
dotnet run -c Release --project benchmarks/Oxia.Client.Benchmarks -- --filter "*ClientOperationBenchmarks*" "*PooledReadBenchmarks*" --job short --memory
```

The `alloc` runner prints, per scenario, the exact process-wide B/op and the sampled bytes per .NET type (GC allocation-tick events; includes the in-process server's own types).

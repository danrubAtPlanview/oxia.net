# Z1 — end-to-end allocation elimination (wave 2)

Upstream: none of this exists in Go (the Go client allocates a fresh message per `Recv()` and a fresh `[]byte` per value and lets the GC
have them).  Every file below is an addition or an adaptation of a ported file; the behavioural contract of the ported code is unchanged.
The design, the ownership protocol and the measurements are in [`../performance-allocation.md`](../performance-allocation.md).

## New files

| Upstream file | C# file | Status | Notes (divergences, extra tests) |
|---|---|---|---|
| — (no upstream) | `src/Oxia.Common/Proto/Pooling/MessagePool.cs` (`PoolStates`, `PoolGuard`, `BoundedStack<T>`, `MessagePool<T>`, `PoolLimits`) | new | Per-type bounded pools of the top-level response messages (64 slots, lock-protected array stack), the lifecycle states `Plain / Rented / Releasing / Pooled`, the Debug-only use-after-release guard and the trim limits applied when a message goes back (4096 spare children per list, 8192 list capacity). |
| — | `src/Oxia.Common/Proto/Pooling/ValueLease.cs` | new | The pooled buffer behind the opt-in pooled read API: value + UTF-8 key bytes of one record, the record metadata (version, secondary key), a generation that makes a second release harmless, a kept buffer (<= 64 KiB, 32 MiB budget) so the steady state allocates nothing. |
| — | `src/Oxia.Common/Proto/Pooling/Utf8StringCache.cs` | new | 64-slot lock-free cache for repeated short ASCII strings (the client identity of an ephemeral record's version); never used for record keys. |
| `client_vtproto.pb.go` | `src/Oxia.Common/Proto/ClientMessages.cs` (edited) | edited | `Rent()` / `Release()` / `PoolState` on `WriteResponse`, `ReadResponse`, `RangeScanResponse`, `ListResponse`, `NotificationBatch` (the lists became get-only properties over private fields: same public shape); `GetResponse.Lease`, `ValueLength`, `ResolvedKey`, `TakeValueArray()`, `DetachLease()`, `Clone()`; `ReadResponse.Absorb`; `ReadRequest` / `RangeScanRequest.PooledValues`. Everything new is `internal` (the test kit reflects over public members) except `Rent`/`Release`. |
| same | `src/Oxia.Common/Proto/Codec/MessageCodec.cs`, `ReadCodecs.cs`, `ScanCodecs.cs`, `WriteCodecs.cs`, `NotificationCodecs.cs`, `ProtoReader.cs`, `Spares.cs` (edited) | edited | `IPooledMessageCodec<T>` (`Rent()`), the pooled-value codecs `ReadResponsePooledCodec` / `RangeScanResponsePooledCodec` / `GetResponseCodec.MergeFromPooled`, `ProtoReader.ReadStringCached` / `ReadBytesInto`, `Spares.Trim`; R4-3: the chunked writers of `PutRequest` / `DeleteRequest` / `DeleteRangeRequest` / `GetRequest` detect a stale `CachedSize` like the block writers. |
| `client_grpc.pb.go`, `proto_grpc.go` | `src/Oxia.Common/Proto/OxiaClientMethods.cs` (edited) | edited | `CodecMarshaller.CreatePooled` / `CreateShared` / `DeserializePooled`; the response marshallers of the five pooled messages rent; KeepAlive / CloseSession responses are one shared instance; internal `ReadPooled` / `RangeScanPooled` descriptors (internal so the 11 methods / 20 marshallers catalogue is unchanged). |
| `oxia_error_streaming_client.go` | `src/Oxia.Common/Proto/OxiaClientRpc.cs` (edited) | edited | Pooled descriptors selected by `PooledValues`; pooling async builder on `RecvAsync` / `SendAsync`; uses the observed reads / writes of the metrics interceptor; R4-2: `SendAsync` on a server-ended stream throws `TranslatedGrpcException(EndOfStream)` (Go's `io.EOF`, retryable). |
| `grpcmetrics.go` | `src/Oxia.Common/Rpc/GrpcMetrics.cs` (edited) | edited | `IObservedStreamReader` / `IObservedStreamWriter` on the monitored streams (ValueTask + pooled box instead of Task + fresh state machine per message). Counters unchanged. |
| `write_batch.go` | `src/Oxia.Client/Internal/Batch/WriteBatch.cs` (edited) | edited | Releases the response after `Handle`; `SafeComplete` around every per-call completion (R1-2); rerouter exceptions logged and dropped; `CompleteAsync` on the pooling builder. |
| `read_batch.go` | `src/Oxia.Client/Internal/Batch/ReadBatch.cs` (edited) | edited | Same; also sets `ReadRequest.PooledValues` when a pooled-value get is in the batch. |
| `rpc_provider.go` | `src/Oxia.Client/Internal/RpcProvider.cs` (edited) | edited | `ReadResponse.Absorb` when merging chunks; list / range-scan responses released after the (async) consumer's task. |
| `write_stream.go` | `src/Oxia.Client/Internal/WriteStream.cs` (edited) | edited | The per-request `Future<WriteResponse>` is reused when the receive loop completed / failed it. |
| `notifications.go` | `src/Oxia.Client/Notifications.cs` (edited, 1 line) | edited | `nb.Release()` after the batch was delivered. |
| `proto_utils.go` | `src/Oxia.Client/ProtoUtils.cs` (edited, 2 lines) | edited | A classic consumer of a response parsed for the pooled API copies the value out of the lease (`TakeValueArray`). |
| `metrics.go` | `src/Oxia.Client/Internal/Metrics/Metrics.cs`, `Utils.cs` (edited) | edited | Value sizes through `GetResponse.ValueLength` (read before the inner call: a pooled-value call takes the lease). |
| `async_client_impl.go` | `src/Oxia.Client/PooledCalls.cs` (edited), `AsyncClientImpl.cs` (edited), `KeyOrder.cs` (edited) | edited | `MultiShardGet` clones the selected response; `DeleteRangeAsync` uses `PooledDeleteRange` (the old `DeleteRangeCompletion` is unused); the rerouters fail one call at a time (R1-2); `KeyOrder.CompareUtf8` / span overloads for the pooled scan merge. |
| `async_client_impl.go` (Get) | `src/Oxia.Client/Pooled/PooledClientExtensions.cs` | new | `GetPooledAsync` / `RangeScanPooledAsync` extension methods on `IAsyncClient` (public API addition). |
| `client.go` (`GetResult`) | `src/Oxia.Client/Pooled/PooledGetResult.cs` | new | The pooled result struct (`IDisposable`): `Value` as `ReadOnlyMemory<byte>`, `Key` / `KeyUtf8`, `Version`, `Err`. |
| `async_client_impl.go` (Get) | `src/Oxia.Client/Pooled/PooledGetValueCall.cs` | new | The pooled call + `IValueTaskSource` of a pooled get (`IPooledValueCall` marker). |
| `async_client_impl.go` (DeleteRange) | `src/Oxia.Client/Pooled/PooledDeleteRange.cs` | new | One pooled object per DeleteRange: completion source + wait group + one reusable call per shard (Go: a wait group channel + a closure + a call per shard). |
| `async_client_impl.go` (RangeScan), `results_heap.go` | `src/Oxia.Client/Pooled/PooledRangeScan.cs` | new | The pooled scan: per-shard queues, lazy k-way merge by `KeyOrder.CompareUtf8`, back-pressure with a streaming executor. |
| — | `benchmarks/Oxia.Client.Benchmarks/Alloc/AllocEchoServer.cs`, `AllocationTickListener.cs`, `AllocProfileRunner.cs`, `PooledReadBenchmarks.cs` | new | The `alloc` profile (`dotnet run -c Release --project benchmarks/Oxia.Client.Benchmarks -- alloc [scenario ...]`) and the faithful-vs-pooled benchmarks. `Program.cs` dispatches `alloc`. |

## Tests (all new, `tests/Oxia.Client.Tests/Alloc`)

| File | What it pins |
|---|---|
| `ResponsePoolingTests.cs` | The marshallers rent / share, `Release` idempotent and a no-op on plain messages, a rented message is clean and its children recycled, Debug use-after-release, `Absorb`, the client-identity cache, pooled `GetResponse` parsing, `Clone`, a concurrent rent/parse/release stress, trimming of a huge response. |
| `ValueLeaseTests.cs` | Layout / presence, growth, generation guard (stale release of a re-rented lease), clean reuse, many leases, the string cache. |
| `PooledReadApiTests.cs` | `GetPooledAsync` / `RangeScanPooledAsync` over REAL gRPC (`PooledApiTestServer`) vs the faithful API: values, keys, versions, errors, 1 / 2 / 3 shards merge order, partition key, range, `IncludeValue(false)`, a failing shard, cancellation, pooled + classic gets sharing a batch, double Dispose, multi-shard fallback. |
| `CompletionHardeningTests.cs` | R1-2 hardening: throwing callbacks / rerouters never stop the other calls or reach the batcher's catch-all; the batch releases its pooled response and leaves a plain one alone. |
| `EndToEndAllocationTests.cs` | The budgets (Release only, run alone): put, delete, delete-range (partition key / all shards), get not found, get found (faithful), get found (pooled), range scan (1000 records, pooled), notification delivery, session keep-alive — over a real `AsyncClientImpl` on `FakeWireInvoker` (real serializers / deserializers, allocation-free fake network). |
| `FakeWire.cs`, `PooledApiTestServer.cs` | Test infrastructure. |
| `AllocationProfileTests.cs` | Opt-in (`OXIA_Z1_PROFILE=1`, Release) type-by-type allocation attribution over the fake wire. |

Reviewer repro tests fixed by this work: `Review.R4_WriteStreamSendTests` (R4-2), `Review.R4_StaleSizeTests` (R4-3); R1-2 is covered by
`CompletionHardeningTests` (+ the metrics fix on master).

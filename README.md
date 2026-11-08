# oxia.net

A .NET 10 port of the **[Oxia](https://github.com/oxia-db/oxia) Go client**, written to be a faithful, syncable port that is also as fast as the
runtime allows: spans and pooled objects on the hot path, hand-written zero-allocation protobuf codecs, and a lock-free shard router.

* **Faithful.** Same wire protocol, same batching/ordering/retry/session/notification semantics, same sentinel errors and messages.
  All 221 test functions of the Go `oxia/` module, the client-relevant tests of `common/`, and the Go integration tests that exercise the client
  (including TLS and OIDC) are ported with their original names and assertions. See [`PORTING.md`](PORTING.md).
* **Fast.** Client-side allocation per operation is ~0 B (Put 4.5 B, Delete 10 B, Get-not-found 5 B, pooled Get 5 B end to end); throughput is on par with
  or above the Go client against the same server ([`docs/performance.md`](docs/performance.md)).
* **Syncable.** Every ported file starts with a `PORT NOTE` header (upstream path, pinned commit, intent, divergences). Upstream is pinned at
  `8456f61e1ada7b786f5ef17798b45fbc17d0d246`; [`PORTING.md`](PORTING.md) explains how to replay an upstream diff.

## Quick start

```csharp
using Oxia.Client;
using static Oxia.Client.OxiaOptions;

// async client: operations return pending results, the library batches them (BatchLinger, MaxRequestsPerBatch, ...)
await using var client = await OxiaClient.CreateAsyncClientAsync("localhost:6648",
    WithNamespace("default"), WithBatchLinger(TimeSpan.FromMilliseconds(5)));

PutResult put = await client.PutAsync("/users/42", "hello"u8.ToArray());
if (put.Err is not null) throw OxiaErrors.ToThrowable(put.Err);          // errors are values, like Go

GetResult got = await client.GetAsync("/users/42");
if (OxiaErrors.Is(got.Err, ClientErrors.KeyNotFound)) { /* ... */ }

// conditional / ephemeral / partitioned writes: options are allocation-free structs
await client.PutAsync("/lock/a", value, ExpectedRecordNotExists(), Ephemeral(), PartitionKey("a"));
await client.DeleteAsync("/users/42", ExpectedVersionId(put.Version.VersionId));

// streams are ChannelReader<T>; the scan merges all shards in key order
await foreach (GetResult r in client.RangeScanAsync(ct, "/users/", "/users//").ReadAllAsync(ct)) { /* r.Key, r.Value, r.Version, r.Err */ }
```

```csharp
// sync client: CancellationToken first, failures are thrown (OxiaException; OxiaErrors.Is(ex, ClientErrors.KeyNotFound) works)
await using var sync = await OxiaClient.CreateSyncClientAsync("localhost:6648");
var (key, version) = await sync.PutAsync(ct, "/k", value);
var (storedKey, bytes, v) = await sync.GetAsync(ct, "/k", ComparisonFloor());

// notifications, sequence keys, secondary indexes, sessions (ephemeral records), the typed cache and the admin client are all there
await using INotifications n = await sync.GetNotificationsAsync();
await foreach (Notification note in n.Ch.ReadAllAsync(ct)) { /* KeyCreated / KeyModified / KeyDeleted / KeyRangeRangeDeleted / NotificationsMissed */ }
```

Zero-copy reads (opt-in, [`docs/performance-allocation.md`](docs/performance-allocation.md)):

```csharp
using Oxia.Client.Pooled;
using PooledGetResult r = await client.GetPooledAsync("/k");   // value bytes live in a pooled lease until Dispose
ReadOnlyMemory<byte> value = r.Value;
```

### Go → .NET at a glance

| Go | .NET |
|---|---|
| `oxia.NewAsyncClient(addr, opts...)` / `NewSyncClient` / `NewAdminClient` / `NewCache[T]` | `OxiaClient.CreateAsyncClientAsync` / `CreateSyncClientAsync` / `CreateAdminClient` / `CreateCache<T>` |
| `client.Put(key, value, opts...)` returning `<-chan PutResult` | `PutAsync(key, value, opts)` returning `ValueTask<PutResult>` (the result carries `Err`) |
| `(value, err)` of the sync client | thrown `OxiaException`; sentinels via `OxiaErrors.Is(ex, ClientErrors.X)` |
| `context.Context` | `CancellationToken` (first parameter, so the trailing `params ReadOnlySpan<T>` options stay last) |
| `<-chan T` streams (List, RangeScan, GetSequenceUpdates, Notifications) | `ChannelReader<T>` |
| `oxia.Version` | `RecordVersion` |
| `oxia.Ephemeral()`, `oxia.PartitionKey(k)`, … | `OxiaOptions.Ephemeral()`, `OxiaOptions.PartitionKey(k)`, … |
| OpenTelemetry `MeterProvider` | `System.Diagnostics.Metrics` (`oxia_client` meter, same instrument names) |
| `tls.Config`, `grpc.DialOption` | `SslClientAuthenticationOptions`, `Action<GrpcChannelOptions>` |

Full mapping rules: [`DESIGN.md`](DESIGN.md).

## Repository layout

```
src/Oxia.Common/    port of oxia/common (client subset): protos, hand-written wire codecs, errors, backoff, hashing, key encoders, gRPC pool
src/Oxia.Client/    port of oxia/oxia: clients, batching, shard manager, rpc provider, sessions, notifications, cache, admin
tests/Oxia.Client.Tests/             unit tests: ported Go tests + codec oracle tests + allocation/concurrency/review tests
tests/Oxia.Client.IntegrationTests/  ported Go integration tests against a real server (reusable Docker fixture)
tests/Oxia.TestSupport/              in-process gRPC test server, Docker Oxia fixture, mock OIDC provider, test PKI
benchmarks/Oxia.Client.Benchmarks/   BenchmarkDotNet suites + a load generator (and a Go counterpart for head-to-head runs)
docs/                                performance.md, performance-allocation.md, benchmark-results.md, porting/ (per-component file tables)
```

## Running the tests

```
dotnet test tests/Oxia.Client.Tests -c Release                       # ~1 minute; allocation tests only mean something in Release
dotnet test tests/Oxia.Client.IntegrationTests -c Release            # needs Docker (or OXIA_TEST_ADDRESS); OXIA_TEST_CLUSTER=1 adds the cluster profile
```

The integration tests run against **reusable Docker containers** (one idle container hosts many short-lived `oxia` processes, so a run neither
pulls nor starts a container per test, and later runs reuse the same containers). Details, environment variables and the cleanup script:
[`tests/Oxia.Client.IntegrationTests/README.md`](tests/Oxia.Client.IntegrationTests/README.md).

## Benchmarks

```
dotnet run -c Release --project benchmarks/Oxia.Client.Benchmarks -- --filter "*Codec*" --job short --memory
dotnet run -c Release --project benchmarks/Oxia.Client.Benchmarks -- alloc               # end-to-end allocation profile
dotnet run -c Release --project benchmarks/Oxia.Client.Benchmarks -- load --help         # load generator (docker, in-process echo, child-process echo)
```

Results and methodology: [`docs/benchmark-results.md`](docs/benchmark-results.md), [`docs/performance.md`](docs/performance.md).

## Deliberate extensions over the Go client

`OxiaOptions.WithMaxBatchSize`, `OxiaOptions.WithBatchQueueCapacity` (Go uses `GOMAXPROCS`; the default here is `max(4096, MaxRequestsPerBatch)`),
the pooled read API in `Oxia.Client.Pooled`, and `TryGetAsync` on the cache. Every other divergence is listed in the `PORT NOTE` of the file it affects.

## Licence

Apache-2.0, like upstream. See [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE).

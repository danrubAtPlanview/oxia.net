<div align="center">

# oxia.net

**An unofficial .NET 10 client for [Oxia](https://github.com/oxia-db/oxia): a faithful, fast and syncable port of the Go client.**

[Getting started](docs/getting-started.md) |
[Documentation](docs/index.md) |
[Architecture](docs/architecture.md) |
[Performance](docs/performance.md) |
[Changelog](CHANGELOG.md) |
[Upstream Oxia](https://github.com/oxia-db/oxia)

[![CI](https://github.com/danrubAtPlanview/oxia.net/actions/workflows/ci.yml/badge.svg)](https://github.com/danrubAtPlanview/oxia.net/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![.NET 10](https://img.shields.io/badge/.NET-10.0-512BD4.svg)](https://dotnet.microsoft.com/)
[![Discussions](https://img.shields.io/badge/GitHub-Discussions-blue.svg?logo=github)](https://github.com/danrubAtPlanview/oxia.net/discussions)
[![Unofficial](https://img.shields.io/badge/Oxia-unofficial%20community%20port-orange.svg)](#relationship-to-upstream)

</div>

> **Unofficial community port.** oxia.net is not part of, endorsed by, or maintained by the Oxia project or its authors. It is an independent
> .NET port of the Go client in [`oxia-db/oxia`](https://github.com/oxia-db/oxia), pinned at upstream commit
> [`8456f61e1ada7b786f5ef17798b45fbc17d0d246`](https://github.com/oxia-db/oxia/commit/8456f61e1ada7b786f5ef17798b45fbc17d0d246).
> The Oxia name belongs to its owners; this project uses it only to say which system the client talks to. See [Relationship to upstream](#relationship-to-upstream).

[Oxia](https://github.com/oxia-db/oxia) is a scalable metadata store and coordination system for large-scale distributed systems. oxia.net lets .NET applications
use it with the same behaviour as the official Go client: same wire protocol, same batching, ordering, retry, session and notification semantics, same errors.

## Contents

* [Why oxia.net](#why-oxianet)
* [Features](#features)
* [Installation](#installation)
* [Quick start](#quick-start)
* [API at a glance](#api-at-a-glance)
* [Configuration](#configuration)
* [Observability](#observability)
* [Performance](#performance)
* [Testing](#testing)
* [Project status and compatibility](#project-status-and-compatibility)
* [Relationship to upstream](#relationship-to-upstream)
* [Repository layout](#repository-layout)
* [Contributing](#contributing)
* [Community](#community)
* [Security](#security)
* [License](#license)
* [Acknowledgements](#acknowledgements)

## Why oxia.net

* **Faithful.** The goal is that anything that works with the Go client works the same way here. The wire messages are verified byte for byte against the real Go
  implementation; all 221 test functions of the Go `oxia/` module, the client-relevant tests of `common/`, and the Go integration tests that exercise the client (including
  TLS and OIDC) are ported with their original names and assertions. Where the .NET version differs, the difference is deliberate and written down in the `PORT NOTE` of the
  file it affects. See [`PORTING.md`](PORTING.md) and [`docs/review-findings.md`](docs/review-findings.md).
* **Fast.** The hot path is written for the .NET runtime: spans and pooled objects, hand-written zero-allocation protobuf codecs, a lock-free shard router, and a batcher with
  a bounded lock-free queue. The client's own allocation per operation is close to zero (see [Performance](#performance)), and against the same server the throughput measured so far
  is on par with or above the Go client ([`docs/performance.md`](docs/performance.md)).
* **Syncable.** Every ported file starts with a `PORT NOTE` header (upstream path, pinned commit, intent, divergences), and [`PORTING.md`](PORTING.md) maps every upstream file to its
  C# counterpart, so a newer upstream commit can be replayed file by file instead of being re-ported from scratch.

## Features

Feature matrix against the Go client (pinned commit above):

| Feature | Go client | oxia.net | Notes |
|---|:---:|:---:|---|
| Put / Get / Delete / DeleteRange | yes | yes | async (`IAsyncClient`) and sync (`ISyncClient`) clients |
| Conditional writes | yes | yes | `ExpectedVersionId`, `ExpectedRecordNotExists` |
| Ephemeral records and sessions | yes | yes | one session per shard, kept alive in the background, inherited across shard splits |
| Partition keys | yes | yes | `PartitionKey` routes related keys to one shard |
| Sequence keys and sequence updates | yes | yes | `SequenceKeysDeltas`, `GetSequenceUpdates` |
| Secondary indexes | yes | yes | `SecondaryIndex` on writes, `UseIndex` on reads and scans |
| Floor / ceiling / lower / higher gets | yes | yes | `ComparisonFloor()`, `ComparisonCeiling()`, `ComparisonLower()`, `ComparisonHigher()` |
| `List` / `RangeScan` | yes | yes | multi-shard merge in key order, back-pressure through bounded channels |
| Notifications | yes | yes | one feed for all shards; `NotificationsMissed` when the server's retention dropped events |
| Typed cache | yes | yes | `ICache<T>` over the sync client, kept fresh by notifications |
| Admin client | yes | yes | data servers, namespaces, shard split |
| Namespaces | yes | yes | `WithNamespace` |
| TLS | yes | yes | `WithTls(SslClientAuthenticationOptions)`, `tls://` addresses, `TlsOptions` helper (CA, client certificate, server name) |
| Token / OIDC authentication | yes | yes | `WithAuthentication`: static token or per-RPC token supplier, sent as a bearer token |
| Custom service resolver | yes | yes | `WithDialResolver(IServiceResolver)` |
| Shard-split handling | yes | yes | pending writes are re-routed to the successor shards in their original order |
| Key sorting (hierarchical / natural) | yes | yes | taken from the shard assignments of the namespace |
| Metrics | OpenTelemetry | `System.Diagnostics.Metrics` | same instrument names, units and tags; works with OpenTelemetry, `dotnet-counters`, `MeterListener` |
| Logging | `slog` | `Microsoft.Extensions.Logging` | through `OxiaLog.Factory`; off by default |
| Pooled zero-copy reads | - | yes | extension: `GetPooledAsync`, `RangeScanPooledAsync` |
| `WithMaxBatchSize`, `WithBatchQueueCapacity`, cache `TryGetAsync` | - | yes | extensions (see [below](#deliberate-extensions-over-the-go-client)) |

### Deliberate extensions over the Go client

* `OxiaOptions.WithMaxBatchSize` - Go has the setting but no option function to change it.
* `OxiaOptions.WithBatchQueueCapacity` - the number of calls a shard's batcher queues before a caller has to wait (Go uses `GOMAXPROCS`; the default here is `max(4096, MaxRequestsPerBatch)`, see [`docs/performance.md`](docs/performance.md)).
* The pooled read API in `Oxia.Client.Pooled` ([`docs/performance-allocation.md`](docs/performance-allocation.md)).
* `TryGetAsync` on the typed cache, which reports a missing key as a value instead of throwing.

Every other divergence is listed in the `PORT NOTE` of the file it affects and summarised in [`docs/upgrading-from-go.md`](docs/upgrading-from-go.md).

## Installation

**oxia.net is not on NuGet yet.** Until it is, build it from source. (A NuGet package is planned; its id will be announced in the [changelog](CHANGELOG.md).)

Requirements: the [.NET 10 SDK](https://dotnet.microsoft.com/download) (10.0.400 or later). No Docker is needed to use the library; Docker is only used by the integration tests.

```
git clone https://github.com/danrubAtPlanview/oxia.net
cd oxia.net
dotnet build Oxia.slnx -c Release
```

**Option 1: project reference** (simplest; keeps you on the exact commit you built):

```xml
<ItemGroup>
  <ProjectReference Include="path/to/oxia.net/src/Oxia.Client/Oxia.Client.csproj" />
</ItemGroup>
```

**Option 2: pack locally and use a local feed.** `Oxia.Client` depends on `Oxia.Common`, so pack both:

```
dotnet pack src/Oxia.Common -c Release -o ./nupkgs -p:Version=0.1.0-local
dotnet pack src/Oxia.Client -c Release -o ./nupkgs -p:Version=0.1.0-local
# register the folder as a package source (use its absolute path), then reference the package from your project
dotnet nuget add source <absolute-path-to>/nupkgs --name oxia-local
dotnet add package Oxia.Client --version 0.1.0-local
```

The client targets `net10.0`. Its dependencies include `Grpc.Net.Client`, `Google.Protobuf`, `System.IO.Hashing` and `Microsoft.Extensions.Logging.Abstractions`.

## Quick start

Start a server for development (any Oxia server reachable from your machine works; this one is a single process that does not replicate data):

```
docker run -d --name oxia -p 6648:6648 oxia/oxia:nightly-beta oxia standalone
```

### Async client

Operations return a `ValueTask`; the library groups the operations in flight into one batch per shard. Failures are **values** (`Err`), as in the Go client.

```csharp
using System.Text;
using Oxia.Client;
using Oxia.Common.Constant;
using static Oxia.Client.OxiaOptions;

await using IAsyncClient client = await OxiaClient.CreateAsyncClientAsync("localhost:6648",
    WithNamespace("default"), WithBatchLinger(TimeSpan.FromMilliseconds(5)));

PutResult put = await client.PutAsync("/users/42", "hello"u8.ToArray());
if (put.Err is not null) throw OxiaErrors.ToThrowable(put.Err);          // errors are values, like Go

GetResult got = await client.GetAsync("/users/42");
if (OxiaErrors.Is(got.Err, ClientErrors.KeyNotFound)) { /* the record does not exist */ }
else if (got.Err is not null) throw OxiaErrors.ToThrowable(got.Err);
else Console.WriteLine(Encoding.UTF8.GetString(got.Value!));

// Many operations in flight are grouped into batches per shard: do not await them one by one.
var puts = new List<Task<PutResult>>();
for (int i = 0; i < 1000; i++)
{
    puts.Add(client.PutAsync($"/items/{i:D6}", "v"u8.ToArray()).AsTask());
}
await Task.WhenAll(puts);

Exception? err = await client.DeleteAsync("/users/42", ExpectedVersionId(put.Version.VersionId));
if (err is not null && !OxiaErrors.Is(err, ClientErrors.UnexpectedVersionId)) throw OxiaErrors.ToThrowable(err);
```

Create **one client per process** and share it between all callers: each client owns its connections, batchers and background loops.

### Sync client

The sync client takes a `CancellationToken` first and **throws** on failure (`OxiaException`; the Go sentinels are reachable with `OxiaErrors.Is`). Every write is sent
immediately (`BatchLinger` 0), so use the async client for throughput.

```csharp
await using ISyncClient sync = await OxiaClient.CreateSyncClientAsync("localhost:6648");

var (key, version) = await sync.PutAsync(ct, "/k", "value"u8.ToArray());
try
{
    var (storedKey, bytes, v) = await sync.GetAsync(ct, "/k", ComparisonFloor());
}
catch (Exception ex) when (OxiaErrors.Is(ex, ClientErrors.KeyNotFound))
{
    // no record with a key <= "/k"
}
```

### Options: conditional, ephemeral, partitioned, sequenced, indexed

Options are allocation-free structs passed as a trailing `params ReadOnlySpan<T>`.

```csharp
byte[] value = "v"u8.ToArray();

// conditional / ephemeral / partitioned writes
await client.PutAsync("/lock/a", value, ExpectedRecordNotExists(), Ephemeral(), PartitionKey("a"));

// sequence keys: the server appends a sequence number; the result carries the generated key
PutResult p = await client.PutAsync("/seq/", value, PartitionKey("p"), SequenceKeysDeltas(1));
Console.WriteLine(p.Key);
ChannelReader<string> updates = client.GetSequenceUpdates(ct, "/seq/", PartitionKey("p"));

// secondary index
await client.PutAsync("/users/42", value, SecondaryIndex("by-email", "ada@example.com"));
GetResult viaIndex = await client.GetAsync("ada@example.com", UseIndex("by-email"));

// floor / ceiling / lower / higher
GetResult floor = await client.GetAsync("/users/50", ComparisonFloor());
GetResult higher = await client.GetAsync("/users/50", ComparisonHigher());

// range delete
Exception? deleted = await client.DeleteRangeAsync("/tmp/", "/tmp//");
```

### Streams: range scans (with back-pressure)

`RangeScanAsync` returns a `ChannelReader<GetResult>`. It merges all shards in key order, and the bounded channels push back on the server stream when you read slowly.
An error ends the stream as the last item.

```csharp
using System.Threading.Channels;

await foreach (GetResult r in client.RangeScanAsync(ct, "/users/", "/users//").ReadAllAsync(ct))
{
    if (r.Err is not null) throw OxiaErrors.ToThrowable(r.Err);
    Console.WriteLine($"{r.Key} v{r.Version.VersionId} {r.Value!.Length} bytes");
}

// keys only (List is deprecated upstream: use RangeScan without values)
await foreach (GetResult r in client.RangeScanAsync(ct, "/users/", "/users//", IncludeValue(false)).ReadAllAsync(ct))
{
    Console.WriteLine(r.Key);
}
```

### Notifications

```csharp
await using INotifications n = await client.GetNotificationsAsync();
await foreach (Notification note in n.Ch.ReadAllAsync(ct))
{
    // KeyCreated / KeyModified / KeyDeleted / KeyRangeRangeDeleted / NotificationsMissed
    Console.WriteLine($"{note.Type} {note.Key} version={note.VersionId}");
}
```

### Typed cache

`ICache<T>` sits on a sync client, keeps entries in memory and invalidates them from the notification feed. You supply the (de)serialisation.

```csharp
using System.Text.Json;

await using ICache<User> cache = await OxiaClient.CreateCache<User>(sync,
    static value => JsonSerializer.SerializeToUtf8Bytes(value),
    static (data, type) => JsonSerializer.Deserialize(data.Span, type));

await cache.PutAsync(ct, "/users/42", new User("Ada", 36));
(User user, RecordVersion version) = await cache.GetAsync(ct, "/users/42");   // served from memory after the first load
CacheGetResult<User> maybe = await cache.TryGetAsync(ct, "/users/missing");   // no exception when the key is absent

// optimistic read-modify-write: retried while another client changes the record
await cache.ReadModifyUpdateAsync(ct, "/users/42",
    static current => current.TryGet(out User u) ? u with { Age = u.Age + 1 } : new User("Ada", 1));

public record User(string Name, int Age);
```

### Pooled zero-copy reads (opt-in)

The faithful `GetAsync` hands you a `byte[]` you own. The pooled API lends you a buffer instead, so a steady-state read allocates nothing
([`docs/performance-allocation.md`](docs/performance-allocation.md) has the ownership rules):

```csharp
using Oxia.Client.Pooled;

using PooledGetResult r = await client.GetPooledAsync("/k");     // the value lives in a pooled buffer until Dispose
if (r.Err is null && r.HasValue)
{
    ReadOnlyMemory<byte> value = r.Value;                         // valid until r is disposed: copy what you need to keep
    Console.WriteLine(value.Length);
}

await foreach (PooledGetResult rec in client.RangeScanPooledAsync(ct, "/a/", "/a0").ReadAllAsync(ct))
{
    using (rec)
    {
        Console.WriteLine($"{rec.Key} {rec.Value.Length}");
    }
}
```

### TLS and authentication

```csharp
using System.Net.Security;
using Oxia.Common.Auth;
using Oxia.Common.Security;

var tls = new TlsOptions { Enabled = true, TrustedCaFile = "/etc/oxia/ca.pem", ServerName = "oxia.example.com" };
SslClientAuthenticationOptions ssl = tls.TryIntoClientTlsConf()!;

// the supplier is called for every RPC, so a refreshed token is picked up
IAuthentication auth = Authentication.NewTokenAuthenticationWithFunc(
    () => Environment.GetEnvironmentVariable("OXIA_TOKEN")!, requireTransportSecurity: true);

await using IAsyncClient secure = await OxiaClient.CreateAsyncClientAsync("oxia.example.com:6648",
    WithTls(ssl), WithAuthentication(auth));
```

More: [`docs/getting-started.md`](docs/getting-started.md) walks through all of this step by step, [`docs/upgrading-from-go.md`](docs/upgrading-from-go.md) shows Go and C# side by side.

## API at a glance

| Go | .NET |
|---|---|
| `oxia.NewAsyncClient(addr, opts...)` / `NewSyncClient` / `NewAdminClient` / `NewCache[T]` | `OxiaClient.CreateAsyncClientAsync` / `CreateSyncClientAsync` / `CreateAdminClient` / `CreateCache<T>` |
| `client.Put(key, value, opts...)` returning `<-chan PutResult` | `PutAsync(key, value, options)` returning `ValueTask<PutResult>` (the result carries `Err`) |
| `(value, err)` of the sync client | a thrown `OxiaException`; sentinels with `OxiaErrors.Is(ex, ClientErrors.X)` |
| `context.Context` | `CancellationToken` (first parameter, so the trailing `params ReadOnlySpan<T>` options stay last) |
| `<-chan T` streams (`List`, `RangeScan`, `GetSequenceUpdates`, notifications) | `ChannelReader<T>` |
| `oxia.Version` | `RecordVersion` |
| `oxia.Ephemeral()`, `oxia.PartitionKey(k)`, ... | `OxiaOptions.Ephemeral()`, `OxiaOptions.PartitionKey(k)`, ... |
| `oxia.WithBatchLinger(d)` and the other client options | `OxiaOptions.WithBatchLinger(TimeSpan)` and friends |
| `errors.Is(err, oxia.ErrKeyNotFound)` | `OxiaErrors.Is(err, ClientErrors.KeyNotFound)` |
| `io.Closer` | `IAsyncCloser` (`CloseAsync()` throws the combined error, `DisposeAsync()` swallows it) |
| OpenTelemetry `MeterProvider` | `System.Diagnostics.Metrics` (`oxia_client` meter, same instrument names); `WithMeterFactory(IMeterFactory?)` |
| `slog` | `Microsoft.Extensions.Logging` through `OxiaLog.Factory` |
| `tls.Config`, `grpc.DialOption` | `SslClientAuthenticationOptions`, `Action<GrpcChannelOptions>` |

The complete mapping rules are in [`DESIGN.md`](DESIGN.md); the behavioural differences are in [`docs/upgrading-from-go.md`](docs/upgrading-from-go.md).

## Configuration

Client options are passed to `CreateAsyncClientAsync` / `CreateSyncClientAsync`. An invalid value fails the creation with the option error(s); nothing is started.

| Option | Default | Meaning |
|---|---|---|
| `WithNamespace(string)` | `default` | the Oxia namespace the client works in |
| `WithBatchLinger(TimeSpan)` | 5 ms | how long a batcher waits to fill a batch before sending it; `0` sends every request immediately (the sync client always uses `0`) |
| `WithMaxRequestsPerBatch(int)` | 1000 | requests per batch before it is sent; `1` effectively disables batching |
| `WithMaxBatchSize(int)` | 131072 (128 KiB) | maximum size in bytes of a write batch (extension) |
| `WithBatchQueueCapacity(int)` | `max(4096, MaxRequestsPerBatch)`, capped at 65536 | calls a shard's batcher queues before the caller waits (back-pressure; extension) |
| `WithRequestTimeout(TimeSpan)` | 30 s | how long to wait for a response before failing the request |
| `WithSessionTimeout(TimeSpan)` | 15 s | session timeout for ephemeral records |
| `WithIdentity(string)` | random GUID | client identity recorded on ephemeral records |
| `WithTls(SslClientAuthenticationOptions)` | none | TLS settings (a `tls://` address also selects TLS) |
| `WithAuthentication(IAuthentication)` | none | per-RPC credentials, for example a bearer token |
| `WithDialResolver(IServiceResolver)` | none | plug in your own service discovery for `scheme:///endpoint` addresses |
| `WithMeterFactory(IMeterFactory?)` / `WithGlobalMeterProvider()` | the global `oxia_client` meter | where the client metrics are created |
| `WithSessionKeepAliveTicker(TimeSpan)` | `SessionTimeout / 10`, at least 2 s | session heartbeat period (experimental) |
| `WithFailureInjection(IEnumerable<Failure>)` | none | failures a client injects on itself (tests only) |

Every option, its validation error and operational advice are in [`docs/configuration.md`](docs/configuration.md).

## Observability

The client publishes metrics through `System.Diagnostics.Metrics` (meter `oxia_client` for the client, `oxia` for the gRPC client counters), with the instrument names, units and tags of
the Go client, so any OpenTelemetry pipeline can export them:

```csharp
using OpenTelemetry;
using OpenTelemetry.Metrics;

using MeterProvider meters = Sdk.CreateMeterProviderBuilder()
    .AddMeter("oxia_client", "oxia")
    .AddOtlpExporter()
    .Build()!;
```

Logging is off by default (a library does not write to the console unless the host opts in); set `OxiaLog.Factory` before you create a client. Instrument list and logger
categories: [`docs/observability.md`](docs/observability.md).

## Performance

Measured on a Windows 11 workstation (Intel Core Ultra 9 185H, .NET 10.0.14) against `oxia/oxia:nightly-beta` (the pinned upstream commit) in Docker Desktop, one fresh
`oxia standalone` per run, 128-byte values, closed loop; the Go client is the one built from the pinned checkout, run on the identical workload. The machine is shared, so
read these as ratios (run-to-run noise is 10-20 %). Source: [`docs/benchmark-results.md`](docs/benchmark-results.md).

| Scenario | Client | ops/s | p50 ms | p99 ms | client CPU us/op | allocated B/op |
|---|---|---:|---:|---:|---:|---:|
| A: async, 256 producers, put+get 50/50, linger 5 ms | **.NET** | 36,537 | 7.27 | 10.36 | 12.04 | 119 |
| | Go | 35,760 | 7.31 | 9.77 | 10.16 | 1,662 |
| B: async, 1000 producers, put only, linger 5 ms | **.NET** | 88,478 | 10.55 | 22.81 | 4.41 | 6 |
| | Go | 71,296 | 13.24 | 27.26 | 11.03 | 1,972 |
| C: sync client, 64 callers, put+get 50/50 | **.NET** | 10,767 | 6.26 | 7.44 | 18.38 | 791 |
| | Go | 10,213 | 6.29 | 7.31 | 13.51 | 1,812 |

With the server taken out of the picture (an echo server in a child process, 1000 callers, put, linger 1 ms; [`docs/performance.md`](docs/performance.md) scenario H) the .NET client reaches
1.0 M ops/s at 5.6 us CPU and 4 B allocated per operation, the Go client 0.24 M ops/s at 11.8 us and 1,955 B.

Client-side allocation per operation, as measured and asserted by tests in Release (budget in brackets; [`docs/performance-allocation.md`](docs/performance-allocation.md)): Put 4-11 B (16), Delete 10 B (16),
Get of a missing key 5.3 B (12), found Get 133 B (136) of which 124 B is the `byte[]` the faithful API hands over, pooled Get 5.3 B (12), pooled scan 2.3 B per record (8). Micro-benchmarks
([`docs/benchmark-results.md`](docs/benchmark-results.md)): the hand-written encoder serialises 1000 puts in 20.7 us against 34.8 us for protoc's generated code, and parses the response in 34.7 us with no
allocation against 45.3 us and 141 KB.

### Linux, on AWS

The same head-to-head on dedicated Linux instances: two `c6i.2xlarge` (8 vCPU, Amazon Linux 2023, kernel 6.18) in one cluster placement group, an `oxia standalone` server on one and the load generators on the
other. The two clients run on the same client instance one at a time, the server is reset before every run, and the table shows the median of three repetitions (run `20261109-092619-fb44`, built from
commit `5664944`, .NET 10.0.11, Go 1.27, Oxia server at the pinned upstream commit). The full report (environment, min-max, tail latencies, server CPU) is in
**[`docs/benchmark-aws.md`](docs/benchmark-aws.md)**; the method and the always-torn-down environment are described in [`bench/aws/PLAN.md`](bench/aws/PLAN.md).

| Scenario | Client | ops/s | p50 ms | p99 ms | client CPU us/op | allocated B/op |
|---|---|---:|---:|---:|---:|---:|
| A: async, 256 producers, put+get 50/50, linger 5 ms | **.NET** | 42,449 | 6.19 | 8.45 | 15.2 | 115 |
| | Go | 38,683 | 6.62 | 8.52 | 12.1 | 1,626 |
| B: async, 1000 producers, put only, linger 5 ms | **.NET** | 144,765 | 5.96 | 16.52 | 5.2 | 4 |
| | Go | 92,829 | 9.83 | 20.58 | 14.2 | 1,792 |
| C: sync client, 64 callers, put+get 50/50 | **.NET** | 12,010 | 5.60 | 6.42 | 30.5 | 773 |
| | Go | 11,281 | 5.70 | 6.49 | 17.1 | 1,772 |
| D: async, 1000 producers, put+get, linger 1 ms, 4 shards (server-bound) | **.NET** | 401,056 | 2.39 | 5.44 | 7.7 | 121 |
| | Go | 393,484 | 2.42 | 5.44 | 8.8 | 1,661 |
| E: async, 256 producers, put only, 1 KiB values | **.NET** | 50,425 | 4.52 | 10.81 | 17.2 | 33 |
| | Go | 41,773 | 5.73 | 11.80 | 16.4 | 1,860 |

Two scenarios (F: no batching, 64 producers; G: open loop at 20 k ops/s) are in the full report. In short: the .NET client is ahead of or level with the Go client on throughput in every scenario, by the most when
many producers share batches (B +56 %, E +21 %), and converges with it when the server is the limit (D). It is **not** better everywhere: it uses more client CPU per operation when each operation is its own RPC or
the rate is low (C 1.8x, F 2.8x, G 1.6x the Go client's CPU per operation). That was traced to the .NET thread pool (idle workers spinning, about 9 worker wake-ups per unbatched operation):
`DOTNET_ThreadPool_UnfairSemaphoreSpinLimit=0` cuts 12-33 % of the CPU per operation for 5-9 % of closed-loop throughput, see [`docs/performance.md`](docs/performance.md) section 11. The far tail was also
worse in two scenarios (D p99.9 2.6x, A maximum 3.3x) and is not explained yet. One server node, no TLS or authentication, one instance type and region, three repetitions, shared cloud hardware: read
differences below 5-10 % as noise.

What these numbers do **not** show: TLS, authentication, a multi-node cluster, macOS. The Windows figures above are Windows plus Docker Desktop; the Linux figures are from the AWS run.

* Method, what changed, what did not help, recommended settings: [`docs/performance.md`](docs/performance.md)
* Allocation protocol and budgets: [`docs/performance-allocation.md`](docs/performance-allocation.md)
* Every micro-benchmark: [`docs/benchmark-results.md`](docs/benchmark-results.md); how to run them: [`benchmarks/README.md`](benchmarks/README.md)
* Linux benchmark on AWS: [`docs/benchmark-aws.md`](docs/benchmark-aws.md) (how to run it yourself, on demand and always torn down: [`bench/aws/README.md`](bench/aws/README.md))

## Testing

```
dotnet build Oxia.slnx -c Release -warnaserror
dotnet test tests/Oxia.Client.Tests -c Release                                    # unit tests, about a minute; allocation tests only mean something in Release
dotnet test tests/Oxia.Client.IntegrationTests -c Release                         # needs Docker (or OXIA_TEST_ADDRESS)
OXIA_TEST_CLUSTER=1 dotnet test tests/Oxia.Client.IntegrationTests -c Release     # also the coordinator-based cluster tests
```

* **Unit tests** (`tests/Oxia.Client.Tests`): the ported Go tests with in-process gRPC servers, differential, byte-exactness and fuzz tests of the hand-written wire codecs against the protoc-generated
  classes, allocation-budget tests, concurrency stress tests, and regression tests for every defect the independent review found.
* **Integration tests** (`tests/Oxia.Client.IntegrationTests`): the ported Go integration tests against a real server. They use **reusable Docker containers**: one idle container hosts many short-lived `oxia`
  processes, so a run neither pulls nor starts a container per test, and later runs reuse the same containers. Environment variables, the fixture design and the cleanup script:
  [`tests/Oxia.Client.IntegrationTests/README.md`](tests/Oxia.Client.IntegrationTests/README.md).
* **Cluster, TLS and OIDC profiles**: `OXIA_TEST_CLUSTER=1` adds the tests that need a coordinator and three data servers; the TLS and OIDC tests run on that cluster container with generated certificates and a mock
  OIDC provider ([`docs/porting/T2-security.md`](docs/porting/T2-security.md)).
* **Linux validation**: the suite was developed on Windows with Docker Desktop. The Linux results are written up in [`docs/linux-validation.md`](docs/linux-validation.md).
* **CI**: GitHub Actions builds with warnings as errors and runs the unit tests on Linux and Windows, and the integration tests on Linux ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)).

## Project status and compatibility

oxia.net is **pre-release**: the API may still change before 1.0, and no package has been published.

| Aspect | State |
|---|---|
| Go `oxia/` module tests | **221 of 221** test functions ported with their original names and assertions, nothing skipped |
| `common/` (client-relevant packages) | 101 test functions ported (98, plus 3 `disable_trap`): 97 run, 4 skipped with a documented reason (the 3 `disable_trap` build-flavour tests and 1 server-side test) |
| Integration tests | 56 of 56 Go tests of `tests/client`, `tests/sequence`, `tests/resolver`, `tests/assignments`; 9 of 9 TLS / OIDC tests of `tests/security` |
| Name-parity audit | mechanical comparison of every `func Test*` of the in-scope Go test files with the C# test methods ([`PORTING.md`](PORTING.md)) |
| Independent review | four reviewers compared the C# with the Go code; 13 defects found, fixed and pinned by regression tests ([`docs/review-findings.md`](docs/review-findings.md)) |
| Wire protocol | hand-written codecs verified against protoc output and against the real Go `vtproto` (all 34 client messages, 4,148 valid inputs identical, about 12,000 mutated inputs with no differing accept) |
| Runtime | .NET 10 (`net10.0`) |
| Servers | the pinned upstream commit `8456f61e1ada7b786f5ef17798b45fbc17d0d246` (image `oxia/oxia:nightly-beta` of 2026-11-07). Other server versions are not tested; the wire protocol is Oxia's, so newer servers are expected to work as long as they keep the client protocol, but this is not verified |
| Platforms | developed and measured on Windows 11 with Docker Desktop; Linux validation: [`docs/linux-validation.md`](docs/linux-validation.md); macOS: not tested |

## Relationship to upstream

* oxia.net is an **independent community project**. It is not affiliated with the Oxia authors, and it is not an official Oxia client.
* It tracks [`oxia-db/oxia`](https://github.com/oxia-db/oxia) at the pinned commit `8456f61e1ada7b786f5ef17798b45fbc17d0d246` (2026-11-07). The `.proto` files are verbatim copies of the upstream files.
* The code is a port of the Go client, so ported files keep the upstream copyright notice ([`NOTICE`](NOTICE)) and the same license (Apache 2.0).
* Server and Go-client bugs belong to the upstream project; bugs of this port (including "the Go client behaves differently in the same scenario") belong here.
* Syncing with a newer upstream commit follows the procedure in [`PORTING.md`](PORTING.md): diff the Go repository, find the C# counterparts through the per-component tables in `docs/porting/`, port the
  changed tests first, then the implementation.
* The Oxia name and the Oxia logo belong to their owners. This repository does not use the logo.

## Repository layout

```
src/Oxia.Common/    port of oxia/common (client subset): protos, hand-written wire codecs, errors, backoff, hashing, key encoders, gRPC connection pool
src/Oxia.Client/    port of oxia/oxia: clients, batching, shard manager, rpc provider, sessions, notifications, cache, admin, pooled read API
tests/Oxia.Client.Tests/             unit tests: ported Go tests, codec oracle tests, allocation / concurrency / review tests
tests/Oxia.Client.IntegrationTests/  ported Go integration tests against a real server (reusable Docker fixture), TLS and OIDC tests
tests/Oxia.TestSupport/              in-process gRPC test server, Docker Oxia fixture, mock OIDC provider, test PKI
benchmarks/Oxia.Client.Benchmarks/   BenchmarkDotNet suites, an allocation profiler and a load generator (with a Go counterpart for head-to-head runs)
bench/aws/                           plan and tooling for the operator-run Linux benchmark on AWS
bench/cpu-repro/                     local Docker tooling to reproduce and profile the client CPU per operation on Linux
docs/                                documentation (start at docs/index.md); docs/porting/ holds the per-component upstream-to-C# file tables
DESIGN.md, PORTING.md                the design contract of the port, and the upstream sync guide
```

## Contributing

Contributions are welcome. Read [`CONTRIBUTING.md`](CONTRIBUTING.md) first: because this is a port, the rules are specific (a `PORT NOTE` on every ported file, ported tests are never weakened, hot-path changes are
measured). Please also follow the [Code of Conduct](CODE_OF_CONDUCT.md). If you like the project, a star helps others find it.

## Community

* Questions and ideas: [GitHub Discussions](https://github.com/danrubAtPlanview/oxia.net/discussions)
* Bugs and feature requests: [issues](https://github.com/danrubAtPlanview/oxia.net/issues/new/choose)
* [Contributing](CONTRIBUTING.md): how to report issues and submit changes
* [Code of Conduct](CODE_OF_CONDUCT.md): Contributor Covenant 2.1
* [Maintainers](MAINTAINERS.md): who runs the project
* [Changelog](CHANGELOG.md): what changed in each version

For the Oxia server and the official Go client, see the upstream community: <https://github.com/oxia-db/oxia> (the `#oxia` channel on the CNCF Slack).

## Security

Please report vulnerabilities privately, through GitHub's [private vulnerability reporting](https://github.com/danrubAtPlanview/oxia.net/security/advisories/new). Do not open public issues for them.
See [`SECURITY.md`](SECURITY.md) for the policy and for what is in scope.

## License

Licensed under the [Apache License, Version 2.0](LICENSE), like upstream. oxia.net is a port of the Go client of [Oxia](https://github.com/oxia-db/oxia); the upstream copyright notice
(Copyright 2023-2026 The Oxia Authors) is kept in [`NOTICE`](NOTICE).

## Acknowledgements

* **The Oxia Authors** and the maintainers of [`oxia-db/oxia`](https://github.com/oxia-db/oxia), who designed and wrote Oxia and its Go client. This project exists because their client is clear, well tested and
  open source; every behaviour documented here is theirs.
* The projects oxia.net builds on: [Grpc.Net.Client](https://github.com/grpc/grpc-dotnet), [Google.Protobuf](https://github.com/protocolbuffers/protobuf),
  [BenchmarkDotNet](https://benchmarkdotnet.org/) and [xUnit.net](https://xunit.net/).

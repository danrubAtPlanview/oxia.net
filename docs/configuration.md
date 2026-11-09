# Configuration

Everything you can configure on the client, with defaults and validation. The options are the ones of the Go client (`oxia/options_*.go`); the two marked **extension** do not exist in Go. Source of truth:
`src/Oxia.Client/Options/*.cs`.

## Contents

* [Client options](#client-options)
* [Operation options](#operation-options)
* [TLS](#tls)
* [Authentication](#authentication)
* [Custom service resolver](#custom-service-resolver)
* [Metrics and logging](#metrics-and-logging)
* [Admin client](#admin-client)
* [Choosing values](#choosing-values)

## Client options

Passed to `OxiaClient.CreateAsyncClientAsync(address, options...)` and `OxiaClient.CreateSyncClientAsync(address, options...)` as `params ReadOnlySpan<ClientOption>`. The factory validates **all** options first and collects every error: one invalid
option fails the creation with that error, several fail it with an `AggregateException` whose message joins them with `"; "` (like Go's `multierr`); `OxiaErrors.Is(ex, ClientErrors.InvalidOptionRequestTimeout)` finds each member. A rejected option leaves the
previous value untouched, and nothing is started on error.

`serviceAddress` is the `host:port` of any Oxia server (ideally a load-balanced endpoint); it bootstraps the shard assignments. The client fails to be created if it cannot get them within the request timeout. A `tls://` prefix selects TLS; an address with another
scheme (`dns:///name`) goes to the gRPC channel and, with `WithDialResolver`, to your resolver.

| Option | Default | Valid values | Validation error (`ClientErrors.*`) | Meaning |
|---|---|---|---|---|
| `WithNamespace(string)` | `default` | non-empty | `InvalidOptionNamespace` | the Oxia namespace the client works in |
| `WithBatchLinger(TimeSpan)` | 5 ms | `>= 0` | `InvalidOptionBatchLinger` | how long a batcher waits to fill a batch before sending it. `0` disables lingering: every request is sent at once. `CreateSyncClientAsync` prepends `WithBatchLinger(0)` |
| `WithMaxRequestsPerBatch(int)` | 1000 | `> 0` | `InvalidOptionMaxRequestsPerBatch` | requests per batch before it is sent; `1` effectively disables batching |
| `WithMaxBatchSize(int)` (extension) | 131072 (128 KiB) | `> 0` | `InvalidOptionMaxBatchSize` | maximum size in bytes of a write batch; a single put larger than this fails with `ClientErrors.RequestTooLarge` |
| `WithBatchQueueCapacity(int)` (extension) | `max(4096, MaxRequestsPerBatch)`, capped at 65536 | `> 0` | `InvalidOptionBatchQueueCapacity` | how many calls a shard's batcher queues before the caller that adds one waits (back-pressure). Go uses `GOMAXPROCS` slots |
| `WithRequestTimeout(TimeSpan)` | 30 s | `> 0` | `InvalidOptionRequestTimeout` | how long to wait for a response before cancelling and failing the request; also bounds the initial wait for shard assignments |
| `WithSessionTimeout(TimeSpan)` | 15 s | `> 0` | `InvalidOptionSessionTimeout` | the timeout of the sessions that tie ephemeral records to the client; the server deletes the ephemeral records of a client that stops heart-beating for this long |
| `WithIdentity(string)` | a random GUID (lowercase, hyphenated) | non-empty | `InvalidOptionIdentity` | the client identity, recorded on ephemeral records (`RecordVersion.ClientIdentity`). A provenance tag, not a security claim |
| `WithTls(SslClientAuthenticationOptions)` | none | non-null | `InvalidOptionTls` | TLS configuration, see [TLS](#tls) |
| `WithAuthentication(IAuthentication)` | none | non-null | `InvalidOptionAuthentication` | per-RPC credentials, see [Authentication](#authentication) |
| `WithDialResolver(IServiceResolver)` | none | non-null, non-empty `Scheme` | `"ServiceResolver must not be nil"` / `"ServiceResolver.Scheme() must not be empty"` | custom service discovery, see [Custom service resolver](#custom-service-resolver) |
| `WithMeterFactory(IMeterFactory?)` | `null` | any | - | creates the metric instruments from this factory; `null` is the process-wide `oxia_client` meter |
| `WithGlobalMeterProvider()` | (the default) | - | - | same as `WithMeterFactory(null)` |
| `WithSessionKeepAliveTicker(TimeSpan)` | `SessionTimeout / 10`, and at least 2 s in use | any | - | the session heartbeat period. Experimental ("for experimental use only" upstream) |
| `WithFailureInjection(IEnumerable<Failure>)` | none | any | - | makes the client inject failures on itself (`Failure.DizzyShardManager`); for tests only |

The session heartbeat period is computed after all options are applied: `max(ticker, SessionTimeout / 10)`, and the session loop never ticks faster than every 2 s.

## Operation options

Options are trailing `params ReadOnlySpan<...>` arguments made of allocation-free structs; the same factory (`OxiaOptions.*`) can be used wherever an option applies.

| Option | Applies to | Meaning |
|---|---|---|
| `PartitionKey(string)` | Put, Get, Delete, DeleteRange, List, RangeScan, GetSequenceUpdates | route by this key instead of the record key; records that share a partition key live on one shard (needed for sequence keys, and makes a scan or comparison touch one shard) |
| `ExpectedVersionId(long)` | Put, Delete | apply only if the record's current version id equals this; else `ClientErrors.UnexpectedVersionId` |
| `ExpectedRecordNotExists()` | Put | apply only if the record does not exist (expected version id `-1`, `RecordVersion.VersionIdNotExists`) |
| `Ephemeral()` | Put | the record is tied to the client's session and deleted when the session ends |
| `SequenceKeysDeltas(params ulong[])` | Put | the server appends sequence numbers to the key; requires `PartitionKey`, forbids `ExpectedVersionId`, and the first delta must be `> 0` (`ClientErrors.InvalidOptions`) |
| `SecondaryIndex(name, secondaryKey)` | Put (repeatable) | also index the record under `secondaryKey` in the index `name`; the name must not contain `/` |
| `ComparisonEqual()` (default), `ComparisonFloor()`, `ComparisonCeiling()`, `ComparisonLower()`, `ComparisonHigher()` | Get | which record to find: equal, highest key `<=`, lowest key `>=`, highest key `<`, lowest key `>` the given key |
| `UseIndex(name)` | Get, List, RangeScan | interpret keys and key ranges as secondary keys of the index; returned records keep their primary keys; the name must not contain `/` |
| `IncludeValue(bool)` | Get, RangeScan | with `false` only keys and versions come back |
| `ShowInternalKeys(bool)` | List, RangeScan | also return Oxia's internal keys (hidden by default) |
| `EndInclusive()` | List, RangeScan | include the record whose key is the range end; fails with `ClientErrors.EndInclusiveUnsupported` until every server of the shard supports it |

Ranges are `[min, max)`; an empty `max` means "from `min` on". Key order follows the namespace's key sorting (hierarchical by default, or natural), which the client learns from the shard assignments.

The same option can be passed to several operations through implicit conversions (for example `PartitionKey(...)` converts to every operation's option type; `ExpectedVersionId` also to a put option).

## TLS

Set `WithTls(SslClientAuthenticationOptions)`, or use a `tls://host:port` address. The Go `tls.Config` maps to `SslClientAuthenticationOptions`. A ready-made helper, `Oxia.Common.Security.TlsOptions`, is the port of Oxia's TLS configuration struct and builds the options:

| `TlsOptions` property | Meaning |
|---|---|
| `Enabled` | `null` (default): enabled when `CertFile` is set; `true`/`false` force it. `TryIntoClientTlsConf()` returns `null` when TLS is not enabled. With `Enabled = true` and no `CertFile`, the client only verifies the server |
| `TrustedCaFile` | PEM file with the CA certificates to trust (the system roots are used when empty) |
| `ServerName` | the name to verify the server certificate against |
| `CertFile`, `KeyFile` | client certificate and key (PEM), for mutual TLS; both or neither |
| `InsecureSkipVerify` | skip server certificate verification. Never use it in production |
| `MinVersion`, `MaxVersion` | `TlsOptions.VersionTls12`, `VersionTls13`, ... `MinVersion` 0 means TLS 1.2, `MaxVersion` 0 means the latest |
| `CipherSuites` | restricts the cipher suites; supported on Linux and macOS only |
| `ClientAuth` | server side (used by tests that host TLS servers) |

```csharp
var tls = new TlsOptions { Enabled = true, TrustedCaFile = "/etc/oxia/ca.pem", ServerName = "oxia.example.com" };
await using var client = await OxiaClient.CreateAsyncClientAsync("oxia.example.com:6648", OxiaOptions.WithTls(tls.TryIntoClientTlsConf()!));
```

## Authentication

`WithAuthentication(IAuthentication)`. The library provides bearer-token credentials, which is what an OIDC-protected Oxia server expects:

```csharp
IAuthentication fixedToken = Authentication.NewTokenAuthenticationWithToken(token, requireTransportSecurity: true);
IAuthentication refreshing = Authentication.NewTokenAuthenticationWithFunc(() => tokenCache.Current, requireTransportSecurity: true);
```

The supplier of `NewTokenAuthenticationWithFunc` runs for every RPC, so a refreshed token is used without recreating the client. Credentials are sent in the `authorization: Bearer <token>` metadata. With `requireTransportSecurity: true` they are refused over a plaintext
connection (`"error connecting to <target>: ..."`). Implement `IAuthentication` for other schemes. The servers' OIDC settings (issuers, audiences, static keys) are server configuration; see the upstream documentation.

## Custom service resolver

For discovery beyond `host:port` (DNS SRV, a service registry), implement `IServiceResolver` and register it with `WithDialResolver`. It handles a URI scheme; when the service address is `scheme:///endpoint`, the channel asks the resolver for the addresses of `endpoint`:

```csharp
public sealed class StaticResolver : IServiceResolver
{
    public string Scheme => "static";

    public void Resolve(string endpoint, AddressUpdater updater)
    {
        // push the addresses at least once, and again whenever they change; return promptly (start a task for long-running work)
        Exception? err = updater(["oxia-0.example.com:6648", "oxia-1.example.com:6648"]);
    }
}

await using var client = await OxiaClient.CreateAsyncClientAsync("static:///oxia", OxiaOptions.WithDialResolver(new StaticResolver()));
```

`Resolve` may be called concurrently and with different endpoints. If the updater returns an error the resolver should retry with a backoff.

## Metrics and logging

Metrics need no configuration: instruments are created on the `oxia_client` meter (and `oxia` for gRPC counters) and report only while a listener (OpenTelemetry, `dotnet-counters`, a `MeterListener`) is subscribed. Use `WithMeterFactory(IMeterFactory)` to create the meter from your host's factory instead.
Logging is off until you set `OxiaLog.Factory` (before creating clients). Both are described in [observability.md](observability.md).

## Admin client

`OxiaClient.CreateAdminClient(adminAddress, tls = null, authentication = null)` returns an `IAdminClient` for the coordinator's admin API (data servers, namespaces, shard split). It takes the TLS options and authentication directly instead of client options. Failed calls throw an `AdminError`
that maps the server's status; `SplitShardAsync` reports a failed split in `SplitShardResult.Error` instead of throwing.

## Choosing values

Recommendations from the measurements in [performance.md](performance.md) (section 9):

* **Concurrency is throughput.** In a closed loop `ops/s = callers / (linger + server round trip)`. To use a 5 ms linger productively keep at least `target_rate x 8 ms` operations in flight (256 for about 32k ops/s, 1000+ for 100k+). A few slow callers are latency bound, not CPU bound.
* **Linger.** The 5 ms default is honoured to about 0.5 ms (a lone put: p50 6.1 ms). For latency-sensitive callers use 1 ms, or 0 for no batching (a lone put takes about 0.48 ms round trip in Docker; many concurrent callers then reach only a few thousand ops/s because every operation is a round trip of its own).
* **One client per process**, shared by all callers. Each client owns a connection pool, batchers and a health loop.
* **`WithBatchQueueCapacity`** matters only beyond about 4096 concurrently pending operations per shard; the Go-sized queue (22 slots on a 22-core machine) cost about 40 % of the throughput at 1000 callers.
* **Do not set anything process-wide for the client.** Workstation non-concurrent GC is the repository default and fine (the client allocates close to nothing); keep `TieredCompilation` and `TieredPGO` on (turning tiering off cost 40 %); no `timeBeginPeriod`; no `ThreadPool.SetMinThreads` for the client itself.
* **CPU-constrained hosts.** The thread pool spins before it sleeps; `DOTNET_ThreadPool_UnfairSemaphoreSpinLimit=0` (or `System.Threading.ThreadPool.UnfairSemaphoreSpinLimit` in `runtimeconfig.json`) cut the CPU per operation from 5.6 to 3.6 us (-36 %) at the price of -11 % peak throughput and a worse p99.
  A trade-off to take when the cores are needed elsewhere, not a default.
* **Session timeout.** Shorter timeouts detect a crashed client sooner and cost more heartbeats; ephemeral records of a client that crashes disappear after the timeout. The default is 15 s.

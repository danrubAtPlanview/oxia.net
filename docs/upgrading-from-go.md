# Coming from the Go client

A guide for developers who know the Oxia Go client (`github.com/oxia-db/oxia/oxia`) and want to use, port or compare its .NET counterpart. The behaviour is the same; the API shape follows .NET conventions only where Go's shape cannot exist
(channels, `context.Context`, multiple return values). The full mapping table is in [`DESIGN.md`](../DESIGN.md); every divergence is also written in the `PORT NOTE` header of the file it affects.

## Contents

* [Quick mapping](#quick-mapping)
* [Side by side](#side-by-side)
* [Behavioural differences](#behavioural-differences)
* [Extensions that Go does not have](#extensions-that-go-does-not-have)
* [Things that are the same](#things-that-are-the-same)

## Quick mapping

| Go | .NET |
|---|---|
| package `oxia` | namespace `Oxia.Client` (`Oxia.Client.Internal`, `.Batch`, `.Pooled`); helpers in `Oxia.Common.*` |
| `oxia.NewAsyncClient(addr, opts...)` | `await OxiaClient.CreateAsyncClientAsync(addr, opts)` |
| `oxia.NewSyncClient(addr, opts...)` | `await OxiaClient.CreateSyncClientAsync(addr, opts)` |
| `oxia.NewAdminClient(addr, tls, auth)` | `OxiaClient.CreateAdminClient(addr, tls, auth)` (cannot fail) |
| `oxia.NewCache[T](client, ser, de)` | `await OxiaClient.CreateCache<T>(syncClient, ser, de)` |
| `context.Context` | `CancellationToken`, first parameter of the sync client's methods and of stream-returning methods |
| `chan T` result of one operation | `ValueTask<T>` |
| `<-chan T` stream | `ChannelReader<T>` (`await foreach (var x in reader.ReadAllAsync(ct))`) |
| `error` return | `Exception?` value (async client) / thrown `OxiaException` (sync client) |
| `errors.Is(err, oxia.ErrKeyNotFound)` | `OxiaErrors.Is(err, ClientErrors.KeyNotFound)` |
| `oxia.Version` | `RecordVersion` |
| `[]byte` | `ReadOnlyMemory<byte>` in, `byte[]?` out |
| `time.Duration` | `TimeSpan` |
| `io.Closer` (`Close() error`) | `IAsyncCloser` (`CloseAsync()`, and `DisposeAsync()` so `await using` works) |
| `tls.Config` | `SslClientAuthenticationOptions` |
| `grpc.DialOption` | `Action<GrpcChannelOptions>` |
| OpenTelemetry `MeterProvider` | `IMeterFactory?` (`WithMeterFactory`); meter `oxia_client` |
| `slog` | `ILogger` through `OxiaLog.Factory` |

## Side by side

### Creating clients

```go
client, err := oxia.NewAsyncClient("localhost:6648",
    oxia.WithNamespace("default"),
    oxia.WithBatchLinger(5*time.Millisecond))
if err != nil { return err }
defer client.Close()
```

```csharp
await using IAsyncClient client = await OxiaClient.CreateAsyncClientAsync("localhost:6648",
    OxiaOptions.WithNamespace("default"),
    OxiaOptions.WithBatchLinger(TimeSpan.FromMilliseconds(5)));
```

### Put and Get (async client)

```go
res := <-client.Put("/users/42", []byte("hello"))
if res.Err != nil { return res.Err }

got := <-client.Get("/users/42")
if errors.Is(got.Err, oxia.ErrKeyNotFound) { /* ... */ }
```

```csharp
PutResult res = await client.PutAsync("/users/42", "hello"u8.ToArray());
if (res.Err is not null) throw OxiaErrors.ToThrowable(res.Err);

GetResult got = await client.GetAsync("/users/42");
if (OxiaErrors.Is(got.Err, ClientErrors.KeyNotFound)) { /* ... */ }
```

Pipelining works the same way: Go starts many operations and then reads each channel; .NET starts many `ValueTask`s and awaits them (call `.AsTask()` if you want `Task.WhenAll`). Never await a `ValueTask` twice.

### Put and Get (sync client)

```go
key, version, err := sync.Put(ctx, "/k", value)
storedKey, bytes, version, err := sync.Get(ctx, "/k", oxia.ComparisonFloor())
if errors.Is(err, oxia.ErrKeyNotFound) { /* ... */ }
```

```csharp
var (key, version) = await sync.PutAsync(ct, "/k", value);
try
{
    var (storedKey, bytes, v) = await sync.GetAsync(ct, "/k", OxiaOptions.ComparisonFloor());
}
catch (Exception ex) when (OxiaErrors.Is(ex, ClientErrors.KeyNotFound)) { /* ... */ }
```

### Options

```go
client.Put("/lock/a", value, oxia.ExpectedRecordNotExists(), oxia.Ephemeral(), oxia.PartitionKey("a"))
client.Delete("/users/42", oxia.ExpectedVersionId(version.VersionId))
client.Put("/users/42", value, oxia.SecondaryIndex("by-email", "ada@example.com"))
```

```csharp
await client.PutAsync("/lock/a", value, ExpectedRecordNotExists(), Ephemeral(), PartitionKey("a"));
await client.DeleteAsync("/users/42", ExpectedVersionId(version.VersionId));
await client.PutAsync("/users/42", value, SecondaryIndex("by-email", "ada@example.com"));
// with: using static Oxia.Client.OxiaOptions;
```

### Range scan and streams

```go
for r := range client.RangeScan(ctx, "/users/", "/users//") {
    if r.Err != nil { return r.Err }
    use(r.Key, r.Value)
}
```

```csharp
await foreach (GetResult r in client.RangeScanAsync(ct, "/users/", "/users//").ReadAllAsync(ct))
{
    if (r.Err is not null) throw OxiaErrors.ToThrowable(r.Err);
    Use(r.Key, r.Value);
}
```

### Notifications

```go
n, err := client.GetNotifications()
defer n.Close()
for note := range n.Ch() { fmt.Println(note.Type, note.Key) }
```

```csharp
await using INotifications n = await client.GetNotificationsAsync();
await foreach (Notification note in n.Ch.ReadAllAsync(ct)) Console.WriteLine($"{note.Type} {note.Key}");
```

### Cache

```go
cache, err := oxia.NewCache[User](sync,
    func(v any) ([]byte, error) { return json.Marshal(v) },
    func(data []byte, v any) error { return json.Unmarshal(data, v) })
```

```csharp
await using ICache<User> cache = await OxiaClient.CreateCache<User>(sync,
    static value => JsonSerializer.SerializeToUtf8Bytes(value),
    static (data, type) => JsonSerializer.Deserialize(data.Span, type));   // returns the value; Go's func fills a pointer
```

### TLS and authentication

```go
client, err := oxia.NewSyncClient(addr,
    oxia.WithTLS(&tls.Config{ /* ... */ }),
    oxia.WithAuthentication(auth.NewTokenAuthenticationWithFunc(func() string { return token() }, true)))
```

```csharp
var client = await OxiaClient.CreateSyncClientAsync(addr,
    OxiaOptions.WithTls(new SslClientAuthenticationOptions { /* ... */ }),
    OxiaOptions.WithAuthentication(Authentication.NewTokenAuthenticationWithFunc(() => Token(), requireTransportSecurity: true)));
```

### Metrics and logging

```go
oxia.NewAsyncClient(addr, oxia.WithMeterProvider(provider))      // or oxia.WithGlobalMeterProvider()
slog.SetDefault(logger)
```

```csharp
OxiaClient.CreateAsyncClientAsync(addr, OxiaOptions.WithMeterFactory(meterFactory));   // or leave the default and AddMeter("oxia_client")
OxiaLog.Factory = loggerFactory;                                                        // before creating clients
```

## Behavioural differences

These are the deliberate differences, grouped by cause. Each is in a `PORT NOTE`; none changes what a correct Go application observes from the server.

### Asynchrony and threads

* **`PutAsync` and friends never block a thread.** Where Go's call blocks the goroutine (the batcher queue is full, or the shard holds its writes during a split), the returned `ValueTask` simply completes later. The order of one caller's writes is preserved (a FIFO ticket chain,
  [architecture.md](architecture.md#ordering-guarantees)).
* **Sessions are non-blocking too.** Go's mutex, held while the head operation waits for its session, becomes a FIFO queue. As in Go, an ephemeral `PutAsync` issued after `Close` never completes. The queue is unbounded while session creation retries; Go blocks the caller instead.
* **Factories surface option errors as a faulted `ValueTask`**, not as a synchronous return value: invalid options fail the creation with the option error(s), several as an `AggregateException` (Go: `multierr`).
* **Long-lived loops** (the batcher loops, the health loop) run without the caller's `ExecutionContext`; a goroutine inherits nothing, and neither do they.

### Errors and cancellation

* **Sentinels are shared exception instances that are never thrown.** The async client returns them in `Err`; compare with `OxiaErrors.Is(err, ClientErrors.X)`, not with `==`. `OxiaErrors.Is` walks the `InnerException` chain and `AggregateException` members, like `errors.Is`.
* **The sync client throws** `OxiaErrors.ToThrowable(err)`: a fresh `OxiaException` that wraps the sentinel, so `Is` works for both clients.
* **No `context.DeadlineExceeded` versus `context.Canceled`.** A `CancellationToken` cannot tell them apart: both are `OperationCanceledException`. Request timeouts surface as `TimeoutException`; `OxiaErrors.IsDeadlineExceeded` recognises it and gRPC `DeadlineExceeded`.
* **After `Close`**, `List`/`RangeScan` surface `ObjectDisposedException` from the closed pool where Go surfaces a gRPC `Canceled`.
* **`CloseAsync()` throws the combined error** of everything it closed (several as `AggregateException`); `DisposeAsync()` swallows it, which is what `await using` calls.

### Keys and strings

* **Key validation.** Go validates with `utf8.ValidString`; .NET strings are UTF-16, so the equivalent rule is "no lone surrogates". Only `Put`/`Delete` keys and `DeleteRange` bounds are validated, exactly as in Go.
* **Lone surrogates elsewhere** (partition keys, secondary keys, Get keys, scan bounds) are encoded as U+FFFD; Go sends the raw invalid bytes. Invalid UTF-8 received from a server decodes with U+FFFD; Go keeps the raw bytes. Servers and other clients only write valid keys.

### Data and API shape

* **`byte[]` values** come back as `byte[]?` (a new array per value; use the [pooled API](../README.md#pooled-zero-copy-reads-opt-in) to avoid it); inputs are `ReadOnlyMemory<byte>`.
* **Streams are bounded channels.** Go's unbuffered channel is a bounded channel of capacity 1 (`List`); `RangeScan` and notifications use capacity 100, like Go. Reading slowly pushes back on the server stream.
* **`oxia.Version` is `RecordVersion`**, a readonly struct (it avoids `System.Version`); `Ephemeral`, `SessionId` and `ClientIdentity` are properties.
* **Cache.** ristretto is replaced by `CostCache` (a sharded dictionary with an LRU list, a total cost bound of 64 MiB where cost is the serialised length, per-entry TTL, no TinyLFU admission; `Set` is synchronous). `SerializeFunc`/`DeserializeFunc` have .NET signatures
  (`byte[] Serialize(object?)`, `object? Deserialize(ReadOnlyMemory<byte>, Type)`) because there is no `any` pointer to fill. `ModifyFunc` signals failure by throwing (it is not retried). `OxiaClient.CreateCache` is asynchronous; `Cache.New` is a blocking convenience.
* **Admin client.** Calls return the protoc-generated messages (`Io.Oxia.Proto.V1`); `SplitShardAsync` reports a failed split in `SplitShardResult.Error`; other failures throw the mapped `AdminError`.

### Runtime and transport

* **Linger precision on Windows.** `System.Threading.Timer` has a 15.6 ms tick; the batcher uses a process-wide high-resolution timer so a 5 ms linger is honoured to about 0.5 ms. Elsewhere the OS timers are fine.
* **Default queue capacity** per shard batcher is `max(4096, MaxRequestsPerBatch)` instead of `GOMAXPROCS` (the Go-sized queue cost about 40 % of the throughput at 1000 callers). `WithBatchQueueCapacity` overrides it.
* **Addressing.** A `host:port` target is dialled directly and the name resolved on every reconnect (Go's `passthrough:///`); a target with a scheme goes to the channel and to your resolver. `grpc.DialOption`s become `Action<GrpcChannelOptions>`, applied last.
* **TLS.** The connection uses TLS 1.2 and 1.3 by default; `TlsOptions.CipherSuites` works on Linux and macOS only. `InsecureSkipVerify` and `ServerName` are honoured through a certificate validation callback.
* **Proxy.** On Windows the system proxy is asked once per destination and remembered (grpc-go reads the environment once).
* **Failpoints** (test hooks) are always compiled in; the `disable_trap` build flavour of Go does not exist, so its three tests are skipped with that reason.

### Observability

* **Metrics.** Names, units and tags are Go's; the byte unit is `By` (UCUM), as in Go's `common/metric/unit.go`. Go's default is a no-op provider; .NET's default is the global `oxia_client` meter, which costs nothing until a listener subscribes. Timers are pairs of counters with the same name (as in Go).
  See [observability.md](observability.md).
* **Logs.** `slog` becomes `ILogger`; messages and levels copy Go's, but nothing is written until you set `OxiaLog.Factory`.

## Extensions that Go does not have

`OxiaOptions.WithMaxBatchSize`, `OxiaOptions.WithBatchQueueCapacity`, the pooled read API (`Oxia.Client.Pooled`), and `ICache<T>.TryGetAsync`. Nothing else is added to the public API.

## Things that are the same

The wire protocol (verified byte for byte against the Go implementation), batching rules (one batch in flight per shard, linger, request and byte limits), retry classification and leader-hint steering, key sorting and the cross-shard merge, session lifecycle and split inheritance,
the notification protocol (including `NotificationsMissed`), the error messages, the log and metric names, and the test suite: the 221 Go unit tests and the client-relevant integration tests exist under the same names ([`PORTING.md`](../PORTING.md)).

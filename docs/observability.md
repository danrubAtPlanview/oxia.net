# Observability

The client publishes **metrics** through `System.Diagnostics.Metrics` and writes **logs** through `Microsoft.Extensions.Logging`. Both follow the Go client: the instrument names, units and tags are Go's, and the log messages and levels copy Go's `slog` calls.
Source: `src/Oxia.Client/Internal/Metrics/*.cs`, `src/Oxia.Common/Rpc/GrpcMetrics.cs`, `src/Oxia.Common/Logging/OxiaLog.cs`.

## Contents

* [Metrics](#metrics)
* [Client instruments (`oxia_client`)](#client-instruments-oxia_client)
* [gRPC instruments (`oxia`)](#grpc-instruments-oxia)
* [Collecting the metrics](#collecting-the-metrics)
* [Reading the metrics](#reading-the-metrics)
* [Logging](#logging)

## Metrics

Two meters are used:

| Meter | Created by | Content |
|---|---|---|
| `oxia_client` | every client (`Metrics` in `Oxia.Client.Internal.Metrics`) | operation and batch timers and size histograms |
| `oxia` | the gRPC connection pool (`GrpcClientMetricsInterceptor`) | the `grpc_client_*` counters, same series as `go-grpc-prometheus` |

**Nothing costs anything until somebody listens.** `System.Diagnostics.Metrics` instruments are no-ops without a listener. The client goes further: operation calls are only decorated for measurement, and batches only read the clock, while a listener is subscribed
to the corresponding instruments (the check is repeated per call, so a listener attached later is honoured from then on). Go's default is a no-op provider; a .NET `Meter` without listeners behaves the same way.

`WithMeterFactory(IMeterFactory)` creates the `oxia_client` meter from your host's factory (dependency injection); with `null` (the default, also `WithGlobalMeterProvider()`) each client creates a private `Meter` named `oxia_client` and disposes it with the client. Listeners such as OpenTelemetry's attach by name.

A measurement never changes the outcome of an operation: exceptions thrown by a listener's callback are swallowed (review finding R1-2).

## Client instruments (`oxia_client`)

Every measurement carries two tags:

| Tag | Values |
|---|---|
| `type` | `put`, `delete`, `delete_range`, `get` (operation instruments); `write`, `read` (batch instruments) |
| `result` | `success`, `failure` |

A *timer* is the pair Go exposes as two counters with the **same name**: a `Counter<double>` of accumulated milliseconds (unit `ms`) and a `Counter<long>` of the number of measurements (no unit). Derive averages and rates in the backend (sum / count). The OpenTelemetry .NET SDK logs a
"duplicate instrument name" warning for such a pair but exports both; Prometheus names them `<name>_milliseconds_total` and `<name>_total`.

| Instrument | Kind | Unit | Measures |
|---|---|---|---|
| `oxia_client_op` | timer (`Counter<double>` + `Counter<long>`) | `ms` / none | time of one operation, from the moment the call was added to a batch until its callback ran |
| `oxia_client_op_value` | `Histogram<long>` | `By` | size of the value of a `put` or `get` (not recorded for `delete` / `delete_range`) |
| `oxia_client_batch_total` | timer | `ms` / none | time of a batch from its creation until its completion: linger plus execution |
| `oxia_client_batch_exec` | timer | `ms` / none | time of a batch from the moment it was sent until the response was handled |
| `oxia_client_batch_value` | `Histogram<long>` | `By` | total size of the values in a batch (puts of a write batch; values returned by a read batch) |
| `oxia_client_batch_request` | `Histogram<long>` | none | number of requests in a batch (puts + deletes + delete ranges for writes; gets for reads) |

The difference `batch_total - batch_exec` is the time batches spend lingering and queueing: the price of batching. A failed read batch records `0` values and `0` requests (it has no response).

## gRPC instruments (`oxia`)

All are `Counter<long>` with unit `1`, on every call of every pooled connection (health pings included):

| Instrument | Meaning | Extra tags |
|---|---|---|
| `grpc_client_started_total` | RPCs started | |
| `grpc_client_handled_total` | RPCs completed, whatever the outcome | `grpc_code` (`OK`, `Canceled`, `Unavailable`, ...) |
| `grpc_client_msg_received_total` | stream messages received | |
| `grpc_client_msg_sent_total` | stream messages sent | |

Common tags: `grpc_type` (`unary`, `client_stream`, `server_stream`, `bidi_stream`), `grpc_service`, `grpc_method`.

## Collecting the metrics

**OpenTelemetry** (any exporter):

```csharp
using OpenTelemetry;
using OpenTelemetry.Metrics;

using MeterProvider meters = Sdk.CreateMeterProviderBuilder()
    .AddMeter("oxia_client", "oxia")
    .AddOtlpExporter()
    .Build()!;
```

In an ASP.NET Core or generic host, register the meters with `services.AddOpenTelemetry().WithMetrics(m => m.AddMeter("oxia_client", "oxia"))` (package `OpenTelemetry.Extensions.Hosting`).

**`dotnet-counters`** (no code): `dotnet-counters monitor --counters oxia_client,oxia --process-id <pid>`.

**A `MeterListener`** (tests, ad-hoc diagnostics):

```csharp
using System.Diagnostics.Metrics;

using var listener = new MeterListener();
listener.InstrumentPublished = (instrument, l) =>
{
    if (instrument.Meter.Name is "oxia_client" or "oxia") l.EnableMeasurementEvents(instrument);
};
listener.SetMeasurementEventCallback<long>((instrument, value, tags, state) =>
    Console.WriteLine($"{instrument.Name} += {value}"));
listener.SetMeasurementEventCallback<double>((instrument, value, tags, state) =>
    Console.WriteLine($"{instrument.Name} += {value} {instrument.Unit}"));
listener.Start();
```

## Reading the metrics

* **Operation latency as the application sees it**: `oxia_client_op` sum / count per `type`. It includes lingering, so with the default 5 ms linger a put's average is at least a few milliseconds.
* **Server round trip**: `oxia_client_batch_exec` sum / count per `type` (`write` / `read`).
* **Batching efficiency**: `oxia_client_batch_request` (requests per batch) and `oxia_client_batch_value`. Batches far smaller than `MaxRequestsPerBatch` mean the linger expires before the batch fills; raising the number of concurrent callers raises throughput
  ([configuration.md](configuration.md#choosing-values)).
* **Failures**: `result="failure"` on `oxia_client_op`, and `grpc_client_handled_total` by `grpc_code` (`Unavailable` bursts mean a leader election or a network problem; the client retries them, so operations may still succeed).
* **Streams**: `grpc_client_msg_received_total` per `grpc_method` for `Read`, `RangeScan`, `GetNotifications`.

## Logging

Logging is **off by default**: a library must not write to the console unless the host opts in. All components log through the static, replaceable `OxiaLog.Factory` (`Oxia.Common.Logging`), the .NET analogue of Go's process-wide `slog` default logger. **Set it before you create a client**:
components take their logger when they are constructed.

```csharp
using Microsoft.Extensions.Logging;
using Oxia.Common.Logging;

OxiaLog.Factory = LoggerFactory.Create(b => b.AddConsole().SetMinimumLevel(LogLevel.Information));

await using IAsyncClient client = await OxiaClient.CreateAsyncClientAsync("localhost:6648");
```

In a generic host, pass the host's factory: `OxiaLog.Factory = app.Services.GetRequiredService<ILoggerFactory>();`.

Logger categories are `oxia.<component>` (Go's `component` attribute), so they can be filtered with the usual category rules (`"oxia.batcher": "Warning"`):

| Category | Component |
|---|---|
| `oxia.shardManager` | shard assignments stream, shard map updates |
| `oxia.rpcProvider`, `oxia.writeStream` | request execution, retries, write streams |
| `oxia.batcher`, `oxia.batch`, `oxia.write-manager` | batching, shard-split hand-off |
| `oxia.oxia-session-manager`, `oxia.session` | sessions for ephemeral records |
| `oxia.oxia-notifications-manager` | notification subscriptions |
| `oxia.oxia-get-sequence-updates` | sequence updates |
| `oxia.cache-manager` | the typed cache |
| `oxia.client-pool`, `oxia.rpc-connection` | connections and their health loop |
| `oxia.tls` | TLS configuration warnings |

Levels and messages copy Go's. Expensive debug logging is guarded with `IsEnabled(LogLevel.Debug)`, so the hot path never formats strings unless debug logging is on. Do not leave `Debug` enabled in production: it includes per-request lines.

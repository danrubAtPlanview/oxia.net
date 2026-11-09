# Getting started

From zero to a program that talks to an Oxia server, in about ten minutes. You need the [.NET 10 SDK](https://dotnet.microsoft.com/download) and, for a local server, Docker.

## Contents

* [1. Start a server](#1-start-a-server)
* [2. Create a project and reference the client](#2-create-a-project-and-reference-the-client)
* [3. Your first program](#3-your-first-program)
* [4. What you just used](#4-what-you-just-used)
* [5. The sync client](#5-the-sync-client)
* [6. Watching for changes](#6-watching-for-changes)
* [7. Shutting down](#7-shutting-down)
* [8. Troubleshooting](#8-troubleshooting)
* [Next steps](#next-steps)

## 1. Start a server

For development, run Oxia as a single process (it serves clients without a coordinator and does not replicate data, so do not use it in production):

```
docker run -d --name oxia -p 6648:6648 oxia/oxia:nightly-beta oxia standalone
```

`6648` is the client port. The image `oxia/oxia:nightly-beta` is the one the integration tests of this repository use (a build of the pinned upstream commit; the `main` tag on Docker Hub is stale). Remove it with `docker rm -f oxia` when you are done.
For a real deployment (a coordinator and data servers, Kubernetes), follow the [Oxia documentation](https://github.com/oxia-db/oxia); the client only needs the address of any server.

## 2. Create a project and reference the client

oxia.net is not on NuGet yet ([README: Installation](../README.md#installation)), so reference the project from a checkout:

```
git clone https://github.com/danrubAtPlanview/oxia.net
dotnet new console -n OxiaHello
cd OxiaHello
dotnet add reference ../oxia.net/src/Oxia.Client/Oxia.Client.csproj
```

The project must target `net10.0`; `Nullable` and `ImplicitUsings` enabled (the template default) are assumed below.

## 3. Your first program

Replace `Program.cs`:

```csharp
using System.Text;
using System.Threading.Channels;
using Oxia.Client;
using Oxia.Common.Constant;
using static Oxia.Client.OxiaOptions;

await using IAsyncClient client = await OxiaClient.CreateAsyncClientAsync("localhost:6648");

// 1. write and read a record
PutResult put = await client.PutAsync("/greetings/en", Encoding.UTF8.GetBytes("hello"));
if (put.Err is not null) throw OxiaErrors.ToThrowable(put.Err);
Console.WriteLine($"stored {put.Key} at version {put.Version.VersionId}");

GetResult got = await client.GetAsync("/greetings/en");
if (got.Err is not null) throw OxiaErrors.ToThrowable(got.Err);
Console.WriteLine($"read {got.Key} = {Encoding.UTF8.GetString(got.Value!)}");

// 2. a missing key is a value, not an exception
GetResult missing = await client.GetAsync("/greetings/xx");
Console.WriteLine(OxiaErrors.Is(missing.Err, ClientErrors.KeyNotFound) ? "no such key" : "unexpected");

// 3. list what is under /greetings/
await client.PutAsync("/greetings/fr", Encoding.UTF8.GetBytes("bonjour"));
await foreach (GetResult r in client.RangeScanAsync(CancellationToken.None, "/greetings/", "/greetings//").ReadAllAsync())
{
    if (r.Err is not null) throw OxiaErrors.ToThrowable(r.Err);
    Console.WriteLine($"{r.Key} = {Encoding.UTF8.GetString(r.Value!)}");
}

// 4. optimistic concurrency: a counter that many clients can increment
long value = await IncrementAsync(client, "/counters/visits");
Console.WriteLine($"visits = {value}");

// 5. an ephemeral record as a lock: it disappears when this client goes away
PutResult lockPut = await client.PutAsync("/locks/leader", "me"u8.ToArray(), ExpectedRecordNotExists(), Ephemeral());
if (lockPut.Err is null) Console.WriteLine("I am the leader");
else if (OxiaErrors.Is(lockPut.Err, ClientErrors.UnexpectedVersionId)) Console.WriteLine("somebody else is the leader");
else throw OxiaErrors.ToThrowable(lockPut.Err);

static async Task<long> IncrementAsync(IAsyncClient client, string key)
{
    while (true)
    {
        GetResult current = await client.GetAsync(key);
        long next;
        PutOption condition;
        if (OxiaErrors.Is(current.Err, ClientErrors.KeyNotFound))
        {
            next = 1;
            condition = ExpectedRecordNotExists();                       // create only if nobody else did
        }
        else if (current.Err is not null)
        {
            throw OxiaErrors.ToThrowable(current.Err);
        }
        else
        {
            next = long.Parse(Encoding.UTF8.GetString(current.Value!)) + 1;
            condition = ExpectedVersionId(current.Version.VersionId);    // update only if nobody else did
        }

        PutResult result = await client.PutAsync(key, Encoding.UTF8.GetBytes(next.ToString()), condition);
        if (result.Err is null) return next;
        if (!OxiaErrors.Is(result.Err, ClientErrors.UnexpectedVersionId)) throw OxiaErrors.ToThrowable(result.Err);
        // somebody else won the race: read again and retry
    }
}
```

Run it with `dotnet run`. On a fresh server the output is:

```
stored /greetings/en at version 0
read /greetings/en = hello
no such key
/greetings/en = hello
/greetings/fr = bonjour
visits = 1
I am the leader
```

## 4. What you just used

* **One client, shared.** `CreateAsyncClientAsync` connects, waits for the shard assignments of the namespace, and starts the background loops. Create one per process (per namespace) and share it; it is thread-safe.
* **Keys** are strings. Oxia keeps them sorted with `/` as a hierarchy separator (the default "hierarchical" sorting), so `/greetings/` to `/greetings//` is the usual way to say "everything under `/greetings/`". A range is `[min, max)`.
* **Errors are values.** The async client never throws for an operation failure: look at `Err`. Compare it with the sentinels in `ClientErrors` using `OxiaErrors.Is`, never `==`. `OxiaErrors.ToThrowable(err)` turns an error into an exception when you want one. Argument misuse (for example
  a `null` key) still throws.
* **Versions** (`RecordVersion`) tell you what you read. `ExpectedVersionId(v)` makes a put or delete conditional on the record being unchanged; `ExpectedRecordNotExists()` makes a put conditional on the key being absent. Together they give you compare-and-swap, as in `IncrementAsync`.
* **Ephemeral records** (`Ephemeral()`) are tied to the client's session: the server deletes them when the client closes or stops heart-beating (default session timeout 15 s). They are the building block for locks, leader election and membership.
* **Batching.** Operations that are in flight at the same time are grouped into one request per shard after a short linger (5 ms by default). A loop that awaits every call one by one therefore gets one operation per linger; to get throughput, start many operations and await them together:

  ```csharp
  var tasks = new List<Task<PutResult>>();
  for (int i = 0; i < 10_000; i++) tasks.Add(client.PutAsync($"/items/{i:D6}", payload).AsTask());
  await Task.WhenAll(tasks);
  ```

  See [configuration.md](configuration.md#choosing-values) for the knobs and [architecture.md](architecture.md#batching-per-shard) for how batching works.

## 5. The sync client

If you prefer exceptions and sequential code, use the sync client. It takes a `CancellationToken` first and throws `OxiaException` on failure; the same sentinels are reachable with `OxiaErrors.Is`. Every write is sent immediately (no lingering), so it favours latency over throughput.

```csharp
await using ISyncClient sync = await OxiaClient.CreateSyncClientAsync("localhost:6648");

var (key, version) = await sync.PutAsync(ct, "/k", "value"u8.ToArray());
try
{
    var (storedKey, bytes, v) = await sync.GetAsync(ct, "/k");
}
catch (Exception ex) when (OxiaErrors.Is(ex, ClientErrors.KeyNotFound))
{
    // the record does not exist
}
```

The typed cache (`OxiaClient.CreateCache<T>`) is built on the sync client; see the [README](../README.md#typed-cache).

## 6. Watching for changes

Every change applied to the database is published as a notification (when the server's namespace has notifications enabled, which is the default):

```csharp
await using INotifications n = await client.GetNotificationsAsync();
await foreach (Notification note in n.Ch.ReadAllAsync(ct))
{
    Console.WriteLine($"{note.Type} {note.Key} version={note.VersionId}");
}
```

`NotificationType` is `KeyCreated`, `KeyModified`, `KeyDeleted`, `KeyRangeRangeDeleted` or `NotificationsMissed` (the server's retention deleted events the subscription had not read yet).

## 7. Shutting down

`await using` disposes the client: it closes the sessions (ephemeral records are deleted), fails the operations still queued with `ClientErrors.ShuttingDown`, and closes the connections. Call `CloseAsync()` instead of `DisposeAsync()` if you want to see a failure
(it throws the combined error; `DisposeAsync` swallows it).

## 8. Troubleshooting

| Symptom | Likely cause |
|---|---|
| `CreateAsyncClientAsync` fails or times out | the server is not reachable at that address, or the namespace does not exist (`WithNamespace`); the creation waits up to the request timeout (30 s) for the shard assignments |
| it connects, but operations fail with `Unavailable` | the address the server announces for its shards is not reachable from the client. With `oxia standalone` the announced address is the one the client dialled; in a cluster it is the `public` address in the cluster configuration |
| `Unauthenticated` | the server requires a bearer token (`WithAuthentication`) or the token expired; see [configuration.md](configuration.md#authentication) |
| TLS handshake errors | a `tls://` address needs a server certificate the client trusts; see [configuration.md](configuration.md#tls) |
| no logs at all | logging is off by default; set `OxiaLog.Factory` before creating the client ([observability.md](observability.md#logging)) |
| throughput much lower than expected | the loop awaits every operation one by one; start many operations and await them together, or lower the linger |

## Next steps

* [Configuration](configuration.md): every option, with defaults.
* [Architecture](architecture.md): batching, ordering guarantees, shard splits, sessions.
* [Observability](observability.md): metrics and logs.
* [Coming from the Go client](upgrading-from-go.md): side-by-side snippets and the list of differences.
* [Performance](performance.md) and the opt-in [pooled read API](performance-allocation.md).

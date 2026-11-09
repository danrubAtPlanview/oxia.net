# Architecture

How the client works inside, and which guarantees it keeps. The behaviour is the Go client's; this document describes how the .NET code realises it. Where a statement is about a
file, the file's `PORT NOTE` header is the authoritative source (and lists every divergence from Go). Sources used: [`DESIGN.md`](../DESIGN.md), [`docs/porting/`](porting/),
[`docs/performance.md`](performance.md), [`docs/performance-allocation.md`](performance-allocation.md), [`docs/review-findings.md`](review-findings.md).

## Contents

* [Overview](#overview)
* [Life of a write](#life-of-a-write)
* [Life of a read](#life-of-a-read)
* [Batching per shard](#batching-per-shard)
* [Ordering guarantees](#ordering-guarantees)
* [Shard manager and shard splits](#shard-manager-and-shard-splits)
* [Retries and errors](#retries-and-errors)
* [Sessions and ephemeral records](#sessions-and-ephemeral-records)
* [Streams: List, RangeScan, sequence updates, notifications](#streams-list-rangescan-sequence-updates-notifications)
* [Wire codecs](#wire-codecs)
* [Pooling and the ownership protocol](#pooling-and-the-ownership-protocol)
* [Connections](#connections)
* [Project structure](#project-structure)

## Overview

```
 application threads
   │  PutAsync / GetAsync / DeleteAsync / ...            (IAsyncClient; ISyncClient wraps it)
   ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ AsyncClientImpl      validates keys and options, picks the shard(s), hands a pooled call to a batcher           │
│   ├─ ShardManager     lock-free view of the shard map: key (or partition key) ──XXH3──► hash ──► shard          │
│   ├─ WriteManager     one gate + one Batcher per shard for writes (hold / retire / re-route on a shard split)   │
│   ├─ ReadBatcher mgr  one Batcher per shard for reads                                                           │
│   ├─ Sessions         one session per shard for ephemeral records (create, keep alive, follow splits)           │
│   └─ Metrics          oxia_client_* instruments (nothing is recorded unless a listener is attached)             │
└───────────────┬─────────────────────────────────────────────────────────────────────────────────────────────────┘
                │  Batcher: bounded MPSC queue → ONE loop per shard → one batch in flight at a time
                ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ RpcProvider          looks the shard leader up on every attempt, retries with exponential backoff               │
│   ├─ WriteStream      one bidirectional WriteStream per shard, FIFO of pending futures, ONE receive loop        │
│   └─ OxiaClientRpc    hand-written gRPC method descriptors + marshallers (zero-allocation codecs)              │
└───────────────┬─────────────────────────────────────────────────────────────────────────────────────────────────┘
                ▼
        ClientPool ─► Connection (GrpcChannel, keep-alive, health loop, per-RPC credentials)  ─► Oxia data servers
```

Everything above `RpcProvider` is per client; connections are pooled per target address. A client owns background loops (one batcher loop per shard and per kind, the shard-assignment
receive loop, session keep-alives, notification streams), which is why one client per process should be shared.

## Life of a write

`PutAsync(key, value, options)`:

1. **Validate.** The key must be valid UTF-8 (no lone UTF-16 surrogates, the rule of Go's `utf8.ValidString`) and the options must be consistent (for example, sequence keys require a partition key and
   forbid an expected version). Failures come back in `Err` (`ClientErrors.InvalidOptions`), they are not thrown.
2. **Route.** The shard is chosen from the partition key if there is one, otherwise from the key: an XXH3-64 hash truncated to 32 bits is looked up in the sorted hash ranges of the shards
   (`ShardManager.Get`: a binary search over an immutable snapshot, no lock and no allocation).
3. **Call.** A pooled call object (completed through an `IValueTaskSource`, so no `Task` per operation) is filled in and added to the write manager's gate for the shard. An ephemeral put first waits for the
   session of its shard (see [Sessions](#sessions-and-ephemeral-records)).
4. **Batch.** The shard's batcher queues the call; its loop groups calls into a batch ([Batching per shard](#batching-per-shard)).
5. **Send.** The batch is encoded into a `WriteRequest` and sent on the shard's write stream; the leader answers in request order and the receive loop completes the pending future.
6. **Complete.** Each call in the batch is completed from its slot in the `WriteResponse`; the caller's `ValueTask<PutResult>` resolves. The pooled response is then released (see [Pooling](#pooling-and-the-ownership-protocol)).

Delete and DeleteRange follow the same path. A DeleteRange without a partition key goes to every shard.

## Life of a read

`GetAsync(key, options)` goes through a **read batcher** per shard (separate from the write batcher) and the unary-request / server-streaming `Read` RPC; a batch of gets is one gRPC call. A get that has to look at
several shards (a comparison other than `Equal`, or a secondary index, without a partition key) is sent to all of them and the best answer is selected (`MultiShardGet`): for `Floor` the greatest of the floors, and so on.

`List` and `RangeScan` do not use the batcher: they open one server-streaming `List` / `RangeScan` RPC per shard and merge the results ([Streams](#streams-list-rangescan-sequence-updates-notifications)).

## Batching per shard

Each shard has a **batcher** per kind (write, read): a bounded, lock-free multi-producer single-consumer queue and **one loop**, which is the .NET realisation of the single goroutine of the Go batcher.

A batch is completed (sent) when:

* it holds `MaxRequestsPerBatch` calls (default 1000), or
* the next call does not fit (`MaxBatchSize` bytes for writes, default 128 KiB; a single put larger than that fails with `ClientErrors.RequestTooLarge`), or
* `BatchLinger` (default 5 ms) has elapsed since its first call, or
* `BatchLinger` is 0, in which case every call is completed immediately, or
* a barrier arrives (used by shard-split handling).

Properties that are part of the behavioural contract and are kept deliberately:

* **One batch in flight per shard.** The loop awaits the completion of a batch before it starts the next one. This is what keeps the writes of a shard in order. A pipelined variant (several batches in flight) was prototyped, measured
  (+25-30 % on one shard, -15 % on four) and **rejected** because without an ordered retry a failing batch could let a later one overtake it ([`docs/performance.md`](performance.md) section 8).
* **Cancelled calls are dropped before sending.** The batch builds its request just before handing it to gRPC and drops calls whose caller already gave up (`CallContext`).
* **Back-pressure.** When a shard's queue is full (default capacity `max(4096, MaxRequestsPerBatch)`), the add parks the caller on a pooled `IValueTaskSource`, FIFO, so a producer's calls stay in order. `BatchQueueCapacity` only matters beyond ~4096 concurrently pending operations per shard.
* **Close.** The open batch and every queued call fail with `ClientErrors.ShuttingDown`; every call is completed or failed exactly once.
* **Linger precision.** The linger timer is one process-wide service. On Windows it waits on a high-resolution waitable timer (the default 15.6 ms OS tick would make a 5 ms linger take about 15 ms); it never calls `timeBeginPeriod`.
  A linger is a minimum: the batch is sent at the deadline plus the wake-up latency (about 0.1-1 ms).

## Ordering guarantees

What you can rely on (each item is pinned by tests, several by regression tests from the independent review):

| Guarantee | Mechanism |
|---|---|
| Writes to one shard reach the server in the order the batches were completed | one batcher loop per shard, one batch in flight |
| One caller's consecutive `PutAsync` calls (without awaiting) reach the batcher in call order, also while a shard split is being absorbed | FIFO ticket chain taken in the synchronous part of the add (review R1-1) |
| A plain write issued after an ephemeral put does not overtake it | plain writes queue behind a pending ephemeral put (`Sessions.ExecuteOrdered`, review R2-D3) |
| Calls of one producer parked on a full queue stay in order | FIFO parking in the batcher queue |
| A write is never sent after its caller gave up | `prepare` drops cancelled calls right before the request is handed to gRPC |
| Every operation completes exactly once | pooled calls with an exactly-once completion; `Future<T>.Reset` waits for an in-flight publish (review lead-1) |
| `RangeScan` returns records in key order across shards | k-way merge of the shard streams with the namespace's key order (`ResultsHeap`, hierarchical or natural) |

What is **not** guaranteed: ordering between different shards (as in Go, there is no cross-shard ordering), or between reads and writes of different clients.

## Shard manager and shard splits

`ShardManager` keeps the client's view of the shard map of one namespace. One background loop subscribes to `GetShardAssignments` of the service address, retries with an exponential backoff, and applies every response:

* A shard whose id is new **removes every existing shard whose hash range overlaps it** (split or merge). The removed shards are recorded together with the shards that replaced them (their *successors*).
* The `ShardsReplaced` listener is invoked **before** the new map becomes visible, so no operation is routed with the new map before the pending calls of the removed shards were re-routed.
* Reads are lock-free: the state (shards sorted by hash range, id to shard, successors, the `Changed` broadcast) is one immutable snapshot swapped with `Volatile.Write` under a writer-only lock.
* `NamespaceNotFound`, `Unauthenticated` and `PermissionDenied` fail client creation at once; after the first response was applied they are retried like any other error. Creation waits for the first assignments for at most `RequestTimeout`.

The write path absorbs a split with the **write manager** (one gate per shard):

```
 shard 7 removed, shards 12 and 13 replace it
   │
   ├─ writes still pending on shard 7 fail with ErrShardNotFound ──► rerouter ──► HandOff to the successors, in order
   ├─ writes issued after the map changed go to 12/13 directly ─────► but 12 and 13 HOLD them until shard 7 has handed off everything
   └─ shard 7's gate is retired: a barrier is queued; when every batch before it completed, it tells each successor "predecessorDone" ─► holds are released
```

A merge holds writes until **every** predecessor has handed off. Reads are simpler: a read batch that fails with `ErrShardNotFound` is re-routed with the read rerouter.

## Retries and errors

`RpcProvider` looks up the shard leader in the shard manager on **every attempt**. Requests to a shard (write, read, list, range scan) are retried with an exponential backoff (randomised, the `cenkalti/backoff` parameters) while the
error is retryable (`OxiaErrors.IsRetryable`, plus any `Unavailable` status, i.e. a leader that went away). A leader hint in the error steers the next attempt only. A shard missing from the map fails the request with `ShardNotFound` so
that the caller re-routes it. Lists and range scans stop retrying as soon as one response was handed to the consumer. Requests to a fixed target (shard assignments, sequence updates, notifications, sessions) are executed once and fail
fast; their owners (the receive loops) retry them with their own backoff, starting at 1 s for notifications and sequence updates.

Errors are **values** in the hot path (the `Err` of a result, `ValueTask<Exception?>` for deletes), using shared, never-thrown `OxiaException` sentinels (`ClientErrors.KeyNotFound`, `ClientErrors.UnexpectedVersionId`, ...). Compare with
`OxiaErrors.Is(err, sentinel)`, which also finds a sentinel wrapped in an exception chain. The sync client throws `OxiaErrors.ToThrowable(err)`: an `OxiaException` whose inner chain contains the sentinel, so `Is` works for both styles.
Cancellation surfaces as `OperationCanceledException`; deadlines of the request timeout as `TimeoutException` (`OxiaErrors.IsDeadlineExceeded` recognises both the exception and the gRPC status).

## Sessions and ephemeral records

Ephemeral records live as long as the client's session on the shard. `Sessions` keeps **one session per shard**, created lazily by the first ephemeral operation on that shard (exponential backoff, cut short by a change of the shard map),
and kept alive by a heartbeat loop (period `SessionTimeout / 10`, at least 2 s; the backoff resets after every successful heartbeat; `ErrSessionNotFound` forgets the session). When a shard is split, the successors **inherit** the session and its
ephemeral records, so the session is moved to them and kept alive there. `Close` stops the sessions and closes each one on its leader.

`ExecuteWithSessionId` never blocks a thread. Go holds a mutex while the operation at the head waits for its session, serialising what is behind it; here the same logical lock is a FIFO queue: operations that arrive while the head waits are queued, and
whoever establishes the session resumes them. When the session already exists and nothing is queued, the callback runs inline on the caller's thread, lock-free and allocation-free.

## Streams: List, RangeScan, sequence updates, notifications

* **List / RangeScan.** One server-streaming RPC per shard. A `RangeScan` merges the shard streams in key order and delivers `GetResult` items into a bounded `Channel` (capacity 100; `List`: 1, like Go's unbuffered channel). The consumer is asynchronous, so a slow reader
  stops reading the per-shard streams and the pressure reaches the server. A shard's error ends the scan as its last item and is never dropped (the review found that cancelling a scan could truncate it silently, and fixed it).
* **Sequence updates.** A stream to the shard of the partition key (the option is required), reconnecting with backoff, completing when the token is done or the client is closed.
* **Notifications.** One subscription manager per shard multiplexes the notification batches into the single `INotifications.Ch` channel (capacity 100). The first batch of a new subscription only confirms the server-side cursor and is discarded; a resumed
  subscription starts after the last batch received, and if the server's retention dropped notifications in between, a `NotificationsMissed` event is delivered. When a shard is split, its stream is cancelled and the subscriptions to its successors take over from
  the last batch received. The channel completes once every subscription stopped.

## Wire codecs

The client protocol is hand-written for the hot path:

* **Messages** (`Oxia.Common.Proto`, `ClientMessages.cs`) are plain classes designed for reuse (`Reset()`); the protocol is the upstream `client.proto`, whose `.proto` file is a verbatim copy.
* **Codecs** (`Oxia.Common.Proto.Codec`) are stateless structs with static abstract members, so the gRPC marshallers are fully specialised per message. Every codec offers `Size`, `Write` (byte-identical to protoc / Go `vtproto` output), and `Parse` from a
  span or a `ReadOnlySequence<byte>` of any number of segments, into a caller-supplied instance. Nested sizes are memoised by `Size`; truncated or malformed input throws `ProtoParseException`; unknown fields are skipped.
* **gRPC plumbing** (`OxiaClientMethods`, `OxiaClientRpc`) defines the method descriptors with custom marshallers on top of `Grpc.Net.Client`.
* **protoc-generated classes** (`Io.Oxia.Proto.V1`) exist for all protos: the admin API (a cold path) uses them directly, and for the client protocol they are the **wire oracle** and the base of the mock servers in tests.
  Never import `Io.Oxia.Proto.V1` and `Oxia.Common.Proto` unaliased in the same file.
* **Verification.** The hand-written codecs were checked byte for byte against protoc and against the real Go `vtproto` (all 34 client messages, 4,148 valid inputs identical modulo map order, about 12,000 mutated inputs with no accept-differently case); oracle,
  differential and fuzz tests in `tests/Oxia.Client.Tests/Proto` fail loudly when a field is missing after a proto sync.

## Pooling and the ownership protocol

Per-operation objects are pooled so that the steady state allocates (almost) nothing:

* **Calls** (`PooledPutCall`, `PooledDeleteCall`, `PooledGetCall`, ...) are rented from a lock-free `CallPool` (a per-thread cache plus a bounded queue) and completed through `ManualResetValueTaskSourceCore`.
* **Response messages** are rented by the gRPC deserializer and released by their final consumer: `WriteResponse` and `ReadResponse` by the batch after it dispatched the callbacks, `RangeScanResponse` / `ListResponse` by the provider after the consumer's task completed,
  `NotificationBatch` after delivery. Release resets the message and keeps its children as spares for the next parse.
* **The rules** (release exactly once by the owner, never keep a child, completions must not throw into the batch, Debug builds detect use after release, pools are bounded) are in [`docs/performance-allocation.md`](performance-allocation.md); a response that is never
  released is not a leak, only a missed reuse.
* **Pooled reads** (`Oxia.Client.Pooled`) extend this to values and keys: the deserializer copies each value into a `ValueLease` (a buffer from `ArrayPool<byte>`); the caller disposes the `PooledGetResult` (generation-checked, so a double dispose is ignored).

The faithful API still hands out a `byte[]` per value and a `Notification` plus key string per event; these are required by the API shape.

## Connections

`ClientPool` keeps one `Connection` (a `GrpcChannel`) per target. Connections use HTTP/2 keep-alive pings every 10 s with a 3 s timeout (also without active calls), allow responses up to 256 MiB, and run an application-level `grpc.health.v1` check every 10 s (a
`NOT_SERVING` answer drops the connection at once, a failed ping after three consecutive failures). A target without a scheme (`host:port`) is dialled directly and the name is resolved on every (re)connect; `tls://` selects TLS; a target with another scheme
(`dns:///x`, `custom:///x`) is handed to the channel and a custom resolver can be registered with `WithDialResolver`. Per-RPC credentials come from `IAuthentication`; credentials that require transport security are refused over plaintext. On Windows the system HTTP proxy
is asked once per destination and the answer remembered, instead of once per request.

## Project structure

| Project | Namespaces | Ports |
|---|---|---|
| `src/Oxia.Common` | `Oxia.Common.*` (`Constant`, `Proto`, `Rpc`, `Time`, `Concurrent`, `Compare`, `Hash`, `Auth`, `Security`, `Failpoint`, `Logging`) | the client-relevant subset of Go `common/` |
| `src/Oxia.Client` | `Oxia.Client`, `.Batch`, `.Internal` (+ `.Batch`, `.Model`, `.Metrics`), `.Pooled` | Go `oxia/` |

The per-file mapping from upstream to C# (with status and divergence notes) is in [`PORTING.md`](../PORTING.md) and [`docs/porting/`](porting/).

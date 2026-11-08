# Independent fidelity review — findings and dispositions

After the port was functionally complete (all component tests and integration tests green) four reviewers (R1–R4), each given only the code, the Go
source and `DESIGN.md` (no author conclusions), compared the C# against Go in four areas and wrote **failing reproduction tests** for every defect they
could demonstrate. All repro tests live in `tests/Oxia.Client.Tests/Review/` and are now green; they stay as regression tests.

## Confirmed defects (all fixed)

| id | area | defect | fix | pinned by |
|---|---|---|---|---|
| R1-1 / R2-D2 | batching | **Write reordering while a shard split is absorbed**: writes parked on a new shard's hold were released on independent pool hops, so a later write could overtake an earlier one (v2 applied before v1). Go blocks inside `Put`, so one caller's writes always reach the batcher in order. | per-gate FIFO ticket chain taken in the synchronous part of `AddAsync`; the lock-free fast path reopens only once the chain drained | `R1_HoldOrderTests`, `R2_WriteOrderingTests.WritesHeldByANewShard…` |
| R2-D1 | shard map | **Writes racing a split failed with `ErrShardNotFound`**: the shards-replaced listener retired the removed shard's gate before lock-free readers could see its successors (Go blocks readers on the shard-map lock). | successors published (copy-on-write `_successors`) before the listener runs | `R2_ShardManagerListenerWindowTests` |
| R2-D3 | sessions | **A plain write issued after an ephemeral put overtook it** (`Put(k,v1,Ephemeral); Put(k,v2)` applied v2 then v1; `Put(eph); Delete(k)` applied the delete first) because the ephemeral put waits in the sessions FIFO. | plain writes queue behind a pending ephemeral put (`Sessions.ExecuteOrdered`) | `R2_WriteOrderingTests.*Ephemeral*` |
| R1-2 | metrics | **A metrics observer exception changed operation outcomes** and, through the batcher's catch-all re-fail, completed already-recycled pooled calls (ABA). | `Metrics.Record*` never throw; `WriteBatch`/`ReadBatch` complete each call in `SafeComplete`; rerouter exceptions are logged and dropped | `R1_MetricsListenerTests`, `Alloc/CompletionHardeningTests` |
| R1-3 | batching | Batcher loop inherited the `ExecutionContext` (AsyncLocals, `Activity.Current`) of whichever caller created the shard's batcher. | long-lived loops started with `ExecutionContext.SuppressFlow()` | `R1_BatcherLoopTests.TheBatcherLoop_DoesNotInheritTheAmbientContext…` |
| R1-4 | batching | Windows: the linger timer had the 15.6 ms OS tick (a 1 ms linger took ~15 ms; the default 5 ms linger up to ~16 ms). | one process-wide high-resolution waitable-timer service (no `timeBeginPeriod`) | `R1_BatcherLoopTests.ALoneCallWithA1msLinger…`, `Throughput/LingerTimerTests` |
| R3-1 | clients | Cancelling a `RangeScan` truncated it silently (no error item), unlike Go which always delivers the shard error. | merge reads shard channels without the caller token; the final error item is never dropped | `R3_RangeScanCancelTests` |
| R3-2 | clients | `List`/`RangeScan` buffered the whole result set (no back-pressure to the server): ~100 MB held for a 96 MB scan with a consumer that read one record. | async-consumer executor overloads (`IStreamingExecutor`) + bounded per-shard channels, as Go's unbuffered channel | `R3_ScanBackpressureTests` |
| R3-3 | clients | `DeleteRangeAsync` with a null bound returned a `ValueTask` that never completed. | `ArgumentNullException` at the entry point | `R3_ArgumentMisuseTests` |
| R4-1 | transport | `ClientPool.CloseAsync` leaked the TCP connection (the `SocketsHttpHandler` was not disposed with the channel). | `DisposeHttpClient = true` | `R4_ConnectionLifetimeTests` |
| R4-2 | transport | `SendAsync` on a stream the server had already ended threw the final status instead of Go's `io.EOF` (so the write was not retried). | `TranslatedGrpcException(EndOfStream)` unless the failure is client-originated | `R4_WriteStreamSendTests` |
| R4-3 | wire | The chunked (> 16 KiB) writers did not detect a stale `CachedSize` (a message mutated between `Size()` and `WriteTo()` produced a corrupt frame). | size check after `WriteChunked` | `R4_StaleSizeTests` |
| lead-1 | pooling | **`Future<T>.Reset()` raced a late completer** (found chasing a 1-in-25 `NullReferenceException` in the write-stream stress test): a completer preempted between publishing `_ready` and resolving the waiter could, after the waiter consumed the value and the future was recycled and re-armed, resolve the *new* round's waiter with the cleared value. | three-state claim; `Reset()` waits for an in-flight publish | `Lead_FutureReuseRaceTests` (fails within a few rounds on the old code) |

## What the reviewers verified as faithful (so it need not be re-audited on sync)

* **Wire format** — against the *real Go vtproto* (a Go program linked to the pinned module): all 34 client messages, 4148 valid inputs identical (modulo
  map order), ~12 000 mutated inputs with no accept-differently case. Key encoders (hierarchical/natural), `Decode`, `IsInternalKey`, XXH3→uint32 routing
  over 1329 edge keys and 20 000 `CompareWithSlash` pairs match Go exactly (`R4_GoGoldenTests`).
* **Shard map** — differential test of the lock-free `ShardManager` against a literal port of Go's update/successor logic over random layouts and
  update sequences (`R2_ShardManagerDifferentialTests`); sessions under random create delays/failures/splits/close; `StreamWrapper` FIFO under mass cancellation.
* **Multi-shard semantics** — a 1-shard server as the oracle for a 4-shard server on the same random multi-level/non-ASCII keys: scans, lists, secondary
  indexes, Floor/Ceiling/Lower/Higher/Equal gets (also through an index), DeleteRange across shards (`R3_MultiShardDifferentialTests`, integration).
* **Sync-client cancellation** — 1500 racing cancellations each for Put and Delete: every error marked "not sent" was true, every success reached the server.
* Retry classification and leader-hint steering per call kind, cenkalti/backoff arithmetic, `Retry`, cost-cache accounting, option validation order and messages.

## Investigated and left as upstream parity (documented, not changed)

* An ephemeral `PutAsync` after `Close` never completes — Go has the same hazard (`executeWithId` only calls back on a non-cancel context error).
* The sessions queue is unbounded while session creation retries (Go blocks the caller); `PutAsync` is non-blocking by design in this port.
* Lone UTF-16 surrogates in partition keys / secondary keys / Get keys / scan bounds are encoded as U+FFFD (Go sends the raw invalid bytes); only `Put`/`Delete`
  keys and `DeleteRange` bounds are validated, exactly as in Go.
* Invalid UTF-8 from a server decodes with U+FFFD (Go keeps raw bytes); servers and the other clients only write valid keys.
* After `Close`, `List`/`RangeScan` surface `ObjectDisposedException` from the closed pool where Go surfaces a gRPC `Canceled`.

## Test-stability findings (test-side, no product defect)

Load-sensitive tests were made deterministic without touching a ported assertion: sleeps replaced by waits on the awaited event (`R4` window test,
unary-cancellation tests), timing-bound tests moved to the non-parallel `R1 serial` collection (`LingerTimerTests`, `R2_SessionsStressTests`, the 40 ms
retry wake-up test, the latency test) with wider margins, the one-shot dial trigger of `TestConnectionDialsNameOfTLSTarget` given a deadline like Go's
deadline-free `conn.Connect()`, and the allocation-budget tests measure the product (process-wide counter, no async scaffolding). The suite then passed
ten consecutive full Release runs and three Debug runs.

// =====================================================================================================================
// PORT NOTE (independent-review regression/differential test; no Go test counterpart)
//   Upstream : oxia/internal/shard_manager.go (update runs the shards-replaced listener inside the write lock; readers block, then see shards and successors)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Pins review finding R2-D1: writes racing a split must not fail with ErrShardNotFound; the successors have to be visible to lock-free readers before the listener retires the removed shard's gate.
//   See      : docs/review-findings.md
// =====================================================================================================================
// =====================================================================================================================
// R2 review (fidelity) -- shard map publication vs. the ShardsReplaced listener.
//
// Go (oxia/internal/shard_manager.go:306-336, update): recordSuccessors -> onShardsReplaced runs INSIDE s.Lock(), and Get /
// GetSuccessor / GetSuccessors / Exists / Changed all take s.RLock().  Anything that calls the manager while an update is being
// applied therefore BLOCKS until the update is complete and then observes the new shards AND the successors recorded by it.
//
// C# (src/Oxia.Client/Internal/ShardManager.cs:303-357): readers are lock free, and the new snapshot (shards + successors) is
// published with Volatile.Write AFTER the listener returned.  A reader that runs between "WriteManager.ShardsReplaced installed
// the retired gate of the removed shard" and "the snapshot is published" does not block: it sees the OLD snapshot, in which the
// removed shard has no successors yet.
//
// Consequence (async_client_impl.go:134-166 forwardWrite -> sendToSuccessors -> GetSuccessor): a write that was routed to the
// removed shard with the old map (ShardManager.Get returned it just before the update) and is added to the retired gate inside
// that window is forwarded, GetSuccessor misses, and the write FAILS with ErrShardNotFound.  In Go the same interleaving blocks
// in GetSuccessor's RLock and the write is forwarded to the shard that replaced the removed one.
// =====================================================================================================================
using System.Collections.Concurrent;
using Oxia.Client.Internal;
using Oxia.Client.Internal.Batch;
using Oxia.Client.Internal.Model;
using Oxia.Client.Tests.F2;
using Oxia.Common.Constant;
using Oxia.Common.Proto;
using Xunit;
using ShardStrategyImpl = Oxia.Client.Internal.ShardStrategy;

namespace Oxia.Client.Tests.Review;

public class R2_ShardManagerListenerWindowTests
{
    private static CancellationToken TestCt => TestContext.Current.CancellationToken;

    private sealed class Wiring
    {
        public readonly AsyncClientImpl Client = new();
        public readonly ConcurrentDictionary<long, CapturingWriteBatcher> Batchers = new();
        public readonly WriteManager WriteManager;
        public ShardManager ShardManager = null!;

        /// <summary>Runs on the thread applying the update, right after the WriteManager was told about the replaced shards.</summary>
        public Action? AfterWriteManagerNotified;

        public Wiring()
        {
            WriteManager = new WriteManager(CancellationToken.None, (_, shardId) =>
            {
                var b = new CapturingWriteBatcher();
                Batchers[shardId] = b;
                return b;
            }, Client.ForwardWriteAsync);
            Client.WriteBatchManager = WriteManager;
        }

        public void Start(params Shard[] initial)
        {
            ShardManager = new ShardManager(ShardStrategyImpl.New(), null, "service:6648", "default", TimeSpan.FromSeconds(10),
                replaced =>
                {
                    // exactly what AsyncClientImpl.NewAsyncClientAsync wires: c.WriteBatchManager.ShardsReplaced
                    WriteManager.ShardsReplaced(replaced);
                    AfterWriteManagerNotified?.Invoke();
                });
            Client.ShardManager = ShardManager;
            ShardManager.Update(initial);
        }
    }

    // A write routed to shard 0 with the old map is added to the gate of shard 0 after the WriteManager retired it, but before
    // the split is visible.  It must reach the shard that replaced shard 0 (Go: GetSuccessor waits for the update to complete);
    // C# fails it with ErrShardNotFound.
    [Fact]
    public async Task AWriteAddedToTheRetiredGateWhileTheSplitIsBeingPublishedIsNotFailedWithShardNotFound()
    {
        var w = new Wiring();
        w.Start(new Shard(0, "", new HashRange(0, uint.MaxValue)));

        const string key = "some-key";
        Exception? failure = null;
        int completions = 0;
        var call = new PutCall
        {
            Key = key,
            Value = "v"u8.ToArray(),
            Callback = (_, err) =>
            {
                failure = err;
                Interlocked.Increment(ref completions);
            },
        };

        // shard 0 is split in two; whatever the hash of the key is, one of the children owns it
        var children = new[]
        {
            new Shard(1, "", new HashRange(0, uint.MaxValue / 2)),
            new Shard(2, "", new HashRange(uint.MaxValue / 2 + 1, uint.MaxValue)),
        };

        Task racing = Task.CompletedTask;
        w.AfterWriteManagerNotified = () =>
        {
            // another thread: it called ShardManager.Get(key) BEFORE the update started (it got shard 0), and adds the call now
            var started = Task.Run(async () => await w.WriteManager.AddAsync(0, call), TestCt);
            racing = started;
            // In Go this thread blocks in GetSuccessor's RLock until update returns; give it the chance to finish early here
            // (C#: it finishes at once, and the call is already failed).  Bounded, never required to complete.
            SpinWait.SpinUntil(() => started.IsCompleted, TimeSpan.FromMilliseconds(300));
        };

        w.ShardManager.Update(children);
        await racing;

        // Go: the write was forwarded to the shard that replaced shard 0, and waits in its batcher
        long successor = w.ShardManager.Get(key);
        Assert.True(Volatile.Read(ref completions) == 0, "the write was FAILED with: " + failure?.Message);
        Assert.True(w.Batchers.TryGetValue(successor, out var batcher), $"no batcher for successor shard {successor}");
        Assert.Contains(call, batcher!.Calls);
    }

    // The same interleaving without any artificial delay: writers that route with the old map race the update.  Every write
    // issued is either applied to the old shard or forwarded to the new ones; none may fail with ErrShardNotFound.
    // listenerSpin = 0: no artificial delay at all.  listenerSpin > 0: the listener takes a bit longer (the WriteManager creates
    // the gates and batchers of the new shards in it, and a real shard map has many more shards to publish).
    [Theory]
    [InlineData(0)]
    [InlineData(5_000)]
    public async Task ConcurrentWritersNeverSeeShardNotFoundWhileAShardIsSplit(int listenerSpin)
    {
        int failed = 0;
        int total = 0;
        string? firstFailure = null;
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(8);

        for (int round = 0; round < 400 && DateTime.UtcNow < deadline && failed == 0; round++)
        {
            var w = new Wiring();
            w.Start(new Shard(0, "", new HashRange(0, uint.MaxValue)));
            w.Client.Options = new ClientOptions();
            if (listenerSpin > 0) w.AfterWriteManagerNotified = () => Thread.SpinWait(listenerSpin);

            using var stop = new CancellationTokenSource();
            var results = new ConcurrentBag<ValueTask<PutResult>>();
            var writers = new Task[4];
            for (int t = 0; t < writers.Length; t++)
            {
                int id = t;
                writers[t] = Task.Factory.StartNew(() =>
                {
                    int i = 0;
                    while (!stop.IsCancellationRequested)
                    {
                        results.Add(w.Client.PutAsync($"k-{id}-{i++}", "v"u8.ToArray()));
                    }
                }, TaskCreationOptions.LongRunning);
            }

            await Task.Delay(5, TestCt);
            w.ShardManager.Update(
            [
                new Shard(1, "", new HashRange(0, uint.MaxValue / 2)),
                new Shard(2, "", new HashRange(uint.MaxValue / 2 + 1, uint.MaxValue)),
            ]);
            await Task.Delay(5, TestCt);
            stop.Cancel();
            await Task.WhenAll(writers);

            foreach (var r in results)
            {
                total++;
                if (!r.IsCompleted) continue; // pending in a capturing batcher: fine
                var res = await r;
                if (res.Err is not null && OxiaErrors.Is(res.Err, OxiaErrors.ShardNotFound))
                {
                    failed++;
                    firstFailure ??= $"round {round}: {res.Err.Message}";
                }
            }

            w.WriteManager.Dispose();
        }

        Assert.True(failed == 0, $"{failed} of {total} writes failed with ErrShardNotFound during a split ({firstFailure})");
    }
}

// =====================================================================================================================
// REVIEW R1 -- CONTROL tests (they PASS today).  They pin the ordering behaviour that was checked and found faithful to Go, so a
// fix of defect R1-1 (R1_HoldOrderTests) cannot silently break it:
//   * one caller's pipelined puts to one shard are applied in issue order whatever the queue capacity / batch size / linger;
//   * pipelined puts queued behind a frozen shard are rerouted to the successors in issue order (handed off ahead of the writes
//     the successors hold), also through cascaded splits;
//   * every call of a closed write manager completes exactly once (applied, or failed with ErrShuttingDown).
// =====================================================================================================================
using System.Collections.Concurrent;
using System.Text;
using Oxia.Client.Internal.Batch;
using Oxia.Client.Tests.F2;
using Oxia.Common.Constant;
using Oxia.Common.Proto;
using Xunit;
using MetricsImpl = Oxia.Client.Internal.Metrics.Metrics;

namespace Oxia.Client.Tests.Review;

[Collection("R1 serial")]
public class R1_OrderingControlTests
{
    private sealed class LogExecutor : Oxia.Client.Internal.IExecutor
    {
        public readonly List<string> Applied = new();

        public async ValueTask<WriteResponse> ExecuteWrite(CancellationToken ct, long shardId, Func<WriteRequest> prepare)
        {
            await Task.Yield();
            var request = prepare();
            var response = new WriteResponse();
            lock (Applied)
            {
                foreach (var put in request.Puts)
                {
                    Applied.Add(Encoding.UTF8.GetString(put.Value.Span));
                    response.Puts.Add(new PutResponse { Status = Status.Ok, Version = new ProtoVersion(), Key = put.Key });
                }
            }
            return response;
        }

        public ValueTask<ReadResponse> ExecuteRead(CancellationToken ct, ReadRequest request) => throw new NotImplementedException();
        public ValueTask ExecuteList(CancellationToken ct, ListRequest request, Func<ListResponse, Exception?> c) => throw new NotImplementedException();
        public ValueTask ExecuteRangeScan(CancellationToken ct, RangeScanRequest request, Func<RangeScanResponse, Exception?> c) => throw new NotImplementedException();
    }

    [Theory]
    [InlineData(1, 1, 0)]
    [InlineData(2, 7, 1)]
    [InlineData(16, 1000, 1)]
    [InlineData(1, 1000, 0)]
    public async Task PipelinedPutsOfOneCaller_OnOneShard_AreAppliedInOrder(int queueCapacity, int maxPerBatch, int lingerMs)
    {
        var exec = new LogExecutor();
        var c = new AsyncClientImpl { ShardManager = new SplittableShardManager("") };
        var bf = new BatcherFactory(exec, "default", TimeSpan.FromMilliseconds(lingerMs), maxPerBatch, new MetricsImpl(null), TimeSpan.FromSeconds(30))
        {
            QueueCapacity = queueCapacity,
        };
        bf.WriteRerouter = c.RerouteWrites;
        c.WriteBatchManager = new WriteManager(CancellationToken.None,
            (ct, shard) => bf.NewWriteBatcher(ct, shard, OxiaOptions.DefaultMaxBatchSize), c.ForwardWriteAsync);
        const int N = 20000;
        var pending = new List<ValueTask<PutResult>>(N);
        for (int i = 0; i < N; i++) pending.Add(c.PutAsync("k", Encoding.UTF8.GetBytes(i.ToString())));
        foreach (var p in pending) Assert.Null((await p).Err);
        c.WriteBatchManager.Dispose();
        Assert.True(exec.Applied.Select(int.Parse).SequenceEqual(Enumerable.Range(0, N)), "out of order");
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public async Task PipelinedPutsQueuedBeforeCascadedSplits_AreAppliedInOrderPerKey(int variant)
    {
        var shardManager = new SplittableShardManager("");
        var executor = new SplitExecutor { ShardManager = shardManager, FrozenShard = 0 };
        var c = SplitExecutor.NewRerouteTestClient(shardManager, executor);
        try
        {
            const int keys = 10, perKey = 200;
            var pending = new List<ValueTask<PutResult>>();
            for (int i = 0; i < perKey; i++)
                for (int k = 0; k < keys; k++)
                    pending.Add(c.PutAsync("k" + k, Encoding.UTF8.GetBytes(i.ToString("D4"))));
            await F2Util.ReadAsync(executor.FrozenWrite.Reader);
            shardManager.Split(0, 1, 2);
            if (variant >= 2) shardManager.Split(1, 3, 4);
            if (variant >= 3) shardManager.Split(2, 5, 6);

            // writes issued after the splits, each awaited before the next one of the same key (so R1-1 is not exercised)
            var posters = Enumerable.Range(0, keys).Select(k => Task.Run(async () =>
            {
                for (int i = perKey; i < perKey + 30; i++)
                    Assert.Null((await c.PutAsync("k" + k, Encoding.UTF8.GetBytes(i.ToString("D4")))).Err);
            })).ToArray();
            await Task.Delay(30);
            executor.Release.SetResult();
            foreach (var p in pending) Assert.Null((await p).Err);
            await F2Util.WithTimeout(Task.WhenAll(posters), TimeSpan.FromSeconds(30));

            var applied = executor.AppliedWrites();
            for (int k = 0; k < keys; k++)
            {
                var mine = applied.Where(s => s.StartsWith("k" + k + "=")).Select(s => int.Parse(s.Split('=')[1])).ToList();
                Assert.True(mine.SequenceEqual(Enumerable.Range(0, perKey + 30)), $"key k{k} out of order: {string.Join(",", mine.Take(40))}");
            }
        }
        finally
        {
            c.WriteBatchManager.Dispose();
        }
    }

    [Fact]
    public async Task EveryCallOfAClosedWriteManager_CompletesExactlyOnce()
    {
        for (int round = 0; round < 5; round++)
        {
            var exec = new LogExecutor();
            var c = new AsyncClientImpl { ShardManager = new SplittableShardManager("") };
            var bf = new BatcherFactory(exec, "default", TimeSpan.Zero, 50, new MetricsImpl(null), TimeSpan.FromSeconds(30));
            bf.WriteRerouter = c.RerouteWrites;
            c.WriteBatchManager = new WriteManager(CancellationToken.None,
                (ct, shard) => bf.NewWriteBatcher(ct, shard, OxiaOptions.DefaultMaxBatchSize), c.ForwardWriteAsync);

            const int producers = 4, per = 3000;
            var results = new ConcurrentBag<Task<PutResult>>();
            var started = new CountdownEvent(producers);
            var tasks = Enumerable.Range(0, producers).Select(p => Task.Run(() =>
            {
                started.Signal();
                for (int i = 0; i < per; i++)
                    results.Add(c.PutAsync("k" + p, Encoding.UTF8.GetBytes(i.ToString())).AsTask());
            })).ToArray();
            await Task.Run(() => started.Wait());
            await Task.Delay(2 + round);
            c.WriteBatchManager.Dispose();
            await Task.WhenAll(tasks);
            var all = Task.WhenAll(results);
            Assert.Same(all, await Task.WhenAny(all, Task.Delay(TimeSpan.FromSeconds(15))));
            var rs = await all;
            int ok = rs.Count(r => r.Err is null);
            int shutting = rs.Count(r => r.Err is not null && OxiaErrors.Is(r.Err, ClientErrors.ShuttingDown));
            Assert.Equal(producers * per, rs.Length);
            Assert.Equal(rs.Length, ok + shutting);
            lock (exec.Applied) Assert.Equal(ok, exec.Applied.Count);
        }
    }
}

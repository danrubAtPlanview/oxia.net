// =====================================================================================================================
// PORT NOTE (extra tests -- no Go counterpart; they never replace the ported ones)
//   Upstream : oxia/internal/batch/{write_batch,read_batch,batcher_factory}.go behaviour, end to end through a real batcher
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : the pooled batches through the real batcher loop with a fake IExecutor: responses reach the calls, op indexes,
//              batch recycling leaves no state behind, UTF-8 byte size, the shard split scenario (re-routing + WriteManager
//              hold/hand-off keeps the writes of a key in order), the metrics of a real flow.
// =====================================================================================================================
using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Oxia.Client.Batch;
using Oxia.Client.Internal;
using Oxia.Client.Internal.Batch;
using Oxia.Client.Internal.Model;
using Oxia.Client.Tests.Metrics;
using Oxia.Common.Constant;
using Oxia.Common.Proto;
using Xunit;
using OxiaMetrics = Oxia.Client.Internal.Metrics.Metrics;

namespace Oxia.Client.Tests.Batch;

/// <summary>An IExecutor driven by delegates (the Go tests use func fields of the batch factories).</summary>
internal sealed class FakeExecutor : IExecutor
{
    public Func<CancellationToken, long, Func<WriteRequest>, ValueTask<WriteResponse>> Write { get; set; } =
        (_, _, _) => throw new NotImplementedException();

    public Func<CancellationToken, ReadRequest, ValueTask<ReadResponse>> Read { get; set; } =
        (_, _) => throw new NotImplementedException();

    public ValueTask<WriteResponse> ExecuteWrite(CancellationToken ct, long shardId, Func<WriteRequest> prepare) =>
        Write(ct, shardId, prepare);

    public ValueTask<ReadResponse> ExecuteRead(CancellationToken ct, ReadRequest request) => Read(ct, request);

    public ValueTask ExecuteList(CancellationToken ct, ListRequest request, Func<ListResponse, Exception?> listResponseConsumer) =>
        throw new NotImplementedException();

    public ValueTask ExecuteRangeScan(CancellationToken ct, RangeScanRequest request,
        Func<RangeScanResponse, Exception?> rangeScanResponseConsumer) => throw new NotImplementedException();

    /// <summary>A successful answer for a write request: one OK result per operation.</summary>
    public static WriteResponse Ok(WriteRequest request)
    {
        var response = new WriteResponse();
        foreach (var _ in request.Puts) response.Puts.Add(new PutResponse { Status = Status.Ok });
        foreach (var _ in request.Deletes) response.Deletes.Add(new DeleteResponse { Status = Status.Ok });
        foreach (var _ in request.DeleteRanges) response.DeleteRanges.Add(new DeleteRangeResponse { Status = Status.Ok });
        return response;
    }
}

public class WriteBatchExtraTests
{
    private static readonly TimeSpan TenSeconds = TimeSpan.FromSeconds(10);

    private static Internal.Batch.BatcherFactory NewFactory(IExecutor executor, int maxRequests = 100, TimeSpan? linger = null,
        OxiaMetrics? metrics = null) =>
        new(executor, "default", linger ?? TimeSpan.Zero, maxRequests, metrics ?? new OxiaMetrics(null), TimeSpan.FromSeconds(30));

    [Fact]
    public async Task WriteBatcher_DeliversTheResponsesToTheCallsAndNumbersTheOperations()
    {
        var requests = new ConcurrentQueue<(long Shard, List<(string Key, uint OpIndex)> Ops)>();
        var executor = new FakeExecutor
        {
            Write = (ct, shard, prepare) =>
            {
                var request = prepare();
                var ops = new List<(string, uint)>();
                foreach (var p in request.Puts) ops.Add((p.Key, p.OpIndex));
                foreach (var d in request.Deletes) ops.Add((d.Key, d.OpIndex));
                foreach (var r in request.DeleteRanges) ops.Add((r.StartInclusive, r.OpIndex));
                requests.Enqueue((shard, ops));
                return ValueTask.FromResult(FakeExecutor.Ok(request));
            },
        };
        var factory = NewFactory(executor, maxRequests: 5, linger: TimeSpan.FromHours(1));
        var batcher = factory.NewWriteBatcher(CancellationToken.None, 7, 1024);

        var results = new ConcurrentDictionary<string, (object? Response, Exception? Error)>();
        await batcher.AddAsync(new PutCall { Key = "p0", Callback = (r, e) => results["p0"] = (r, e) });
        await batcher.AddAsync(new DeleteCall { Key = "d1", Callback = (r, e) => results["d1"] = (r, e) });
        await batcher.AddAsync(new DeleteRangeCall { MinKeyInclusive = "r2", MaxKeyExclusive = "z", Callback = (r, e) => results["r2"] = (r, e) });
        await batcher.AddAsync(new PutCall { Key = "p3", Callback = (r, e) => results["p3"] = (r, e) });
        await batcher.AddAsync(new DeleteCall { Key = "d4", Callback = (r, e) => results["d4"] = (r, e) });

        await BatchTestUtil.Eventually(() => results.Count == 5, TenSeconds, TimeSpan.FromMilliseconds(1));
        batcher.Dispose();

        Assert.All(results.Values, r =>
        {
            Assert.Null(r.Error);
            Assert.NotNull(r.Response);
        });
        var (shard, ops) = Assert.Single(requests);
        Assert.Equal(7, shard);
        Assert.Equal(new[] { ("p0", 0u), ("p3", 3u), ("d1", 1u), ("d4", 4u), ("r2", 2u) }, ops);
    }

    [Fact]
    public async Task RecycledBatchesAreReused_AndLeaveNoStateBehind()
    {
        var seenRequests = new ConcurrentBag<WriteRequest>();
        var batchSizes = new ConcurrentQueue<(int Puts, int Deletes, int Ranges, uint FirstOpIndex)>();
        var executor = new FakeExecutor
        {
            Write = (ct, shard, prepare) =>
            {
                var request = prepare();
                seenRequests.Add(request);
                batchSizes.Enqueue((request.Puts.Count, request.Deletes.Count, request.DeleteRanges.Count,
                    request.Puts.Count > 0 ? request.Puts[0].OpIndex : request.Deletes[0].OpIndex));
                return ValueTask.FromResult(FakeExecutor.Ok(request));
            },
        };
        var factory = NewFactory(executor, maxRequests: 2, linger: TimeSpan.FromHours(1));
        var batcher = factory.NewWriteBatcher(CancellationToken.None, 1, 1024);

        int completed = 0;
        for (int i = 0; i < 20; i++)
        {
            // Batches alternate between 2 puts and 2 deletes: leftovers of a recycled batch would show up
            if (i % 4 < 2)
            {
                await batcher.AddAsync(new PutCall { Key = "k" + i, Callback = (_, e) => { Assert.Null(e); Interlocked.Increment(ref completed); } });
            }
            else
            {
                await batcher.AddAsync(new DeleteCall { Key = "k" + i, Callback = (_, e) => { Assert.Null(e); Interlocked.Increment(ref completed); } });
            }
        }
        await BatchTestUtil.Eventually(() => Volatile.Read(ref completed) == 20, TenSeconds, TimeSpan.FromMilliseconds(1));
        batcher.Dispose();

        var sizes = batchSizes.ToArray();
        Assert.Equal(10, sizes.Length);
        for (int i = 0; i < sizes.Length; i++)
        {
            var expected = i % 2 == 0 ? (2, 0, 0, 0u) : (0, 2, 0, 0u);
            Assert.Equal(expected, sizes[i]);
        }
        // One pooled batch (and its request) serves all of them
        Assert.Single(seenRequests.Distinct());
    }

    [Fact]
    public void ByteSizeCountsUtf8Bytes_AsGoLenOfAString()
    {
        var factory = new WriteBatchFactory { Metrics = new OxiaMetrics(null), MaxByteSize = 10 };
        var batch = factory.NewBatch(1);

        // "é" is 2 bytes in UTF-8 (1 UTF-16 unit); "𝄞" is 4 bytes (2 UTF-16 units)
        Assert.True(batch.CanAdd(new PutCall { Key = "éé𝄞" })); // 8 bytes
        batch.Add(new PutCall { Key = "éé𝄞" });
        Assert.False(batch.CanAdd(new DeleteCall { Key = "é" + "é" + "é" })); // 8 + 6 > 10
        Assert.True(batch.CanAdd(new DeleteCall { Key = "é" })); // 8 + 2 <= 10
    }

    [Fact]
    public void CanAddDoesNotLeakItsCachedSizeToTheNextAdd()
    {
        var factory = new WriteBatchFactory { Metrics = new OxiaMetrics(null), MaxByteSize = 100 };
        var batch = factory.NewBatch(1);
        var call = new PutCall { Key = "abc", Value = new byte[10] };

        Assert.True(batch.CanAdd(call));
        batch.Add(call);
        // The same (pooled) instance with another size
        call.Value = new byte[50];
        Assert.True(batch.CanAdd(call)); // 13 + 53 <= 100
        batch.Add(call);
        Assert.False(batch.CanAdd(call)); // 13 + 53 + 53 > 100
    }

    [Fact]
    public async Task ReadBatcher_DeliversTheResponsesInOrder()
    {
        var executor = new FakeExecutor
        {
            Read = (ct, request) =>
            {
                var response = new ReadResponse();
                foreach (var get in request.Gets) response.Gets.Add(new GetResponse { Key = get.Key, Value = System.Text.Encoding.UTF8.GetBytes(get.Key) });
                return ValueTask.FromResult(response);
            },
        };
        var factory = NewFactory(executor, maxRequests: 3, linger: TimeSpan.FromHours(1));
        var batcher = factory.NewReadBatcher(CancellationToken.None, 1);

        var results = new ConcurrentDictionary<string, GetResponse?>();
        for (int i = 0; i < 3; i++)
        {
            string key = "key-" + i;
            await batcher.AddAsync(new GetCall { Key = key, Callback = (r, e) => { Assert.Null(e); results[key] = r; } });
        }
        await BatchTestUtil.Eventually(() => results.Count == 3, TenSeconds, TimeSpan.FromMilliseconds(1));
        batcher.Dispose();

        Assert.All(results, kv => Assert.Equal(kv.Key, kv.Value!.Key));
    }

    [Fact]
    public async Task ABatchNeverHandedToTheExecutorIsFailedAsNotSent_AndASentOneIsNot()
    {
        var failure = new TimeoutException("boom");
        var executor = new FakeExecutor
        {
            Write = (ct, shard, prepare) => ValueTask.FromException<WriteResponse>(failure),
        };
        var batcher = NewFactory(executor).NewWriteBatcher(CancellationToken.None, 1, 1024);

        var done = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        await batcher.AddAsync(new PutCall { Key = "a", Callback = (_, e) => done.TrySetResult(e) });
        var err = await done.Task.WaitAsync(TenSeconds);
        batcher.Dispose();

        Assert.True(CallContext.IsNotSent(err));
        Assert.True(OxiaErrors.Is(err, failure));
    }

    [Fact]
    public async Task ShardSplit_ReroutedWritesKeepTheirOrder_AheadOfTheWritesHeldByTheNewShard()
    {
        var applied = new ConcurrentQueue<(long Shard, string Key)>();
        var releaseShard0 = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var shard0Blocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var executor = new FakeExecutor
        {
            Write = async (ct, shard, prepare) =>
            {
                if (shard == 0)
                {
                    // The shard was removed from the shard map: every request fails, after the test said so
                    shard0Blocked.TrySetResult();
                    await releaseShard0.Task;
                    throw OxiaErrors.ShardNotFound;
                }
                var request = prepare();
                foreach (var p in request.Puts) applied.Enqueue((shard, p.Key));
                return FakeExecutor.Ok(request);
            },
        };

        var factory = NewFactory(executor, maxRequests: 3, linger: TimeSpan.Zero);
        WriteManager? manager = null;
        factory.WriteRerouter = (shardId, puts, deletes, deleteRanges) =>
        {
            // F2's rerouteWrites: every call goes to the shard that replaced the removed one
            foreach (var call in ModelExtensions.InOpIndexOrder(puts, deletes, deleteRanges))
            {
                manager!.HandOff(1, call);
            }
        };
        manager = new WriteManager(CancellationToken.None, (ct, shardId) => factory.NewWriteBatcher(ct, shardId, 1024),
            (removed, call) => throw new InvalidOperationException("nothing is routed to the removed shard after the split"));

        int completed = 0;
        void Done(PutResponse? r, Exception? e)
        {
            Assert.Null(e);
            Interlocked.Increment(ref completed);
        }

        // Writes pending on shard 0 (the first one is being sent, the rest wait in the queue / current batch)
        var added = new List<ValueTask>();
        for (int i = 0; i < 6; i++)
        {
            added.Add(manager.AddAsync(0, new PutCall { Key = "old-" + i, Callback = Done }));
        }
        await shard0Blocked.Task.WaitAsync(TenSeconds);

        // The split: shard 0 -> shard 1.  A write issued after the shard map changed goes to shard 1 and is held.
        manager.ShardsReplaced(new Dictionary<long, long[]> { [0] = [1] });
        var heldWrite = manager.AddAsync(1, new PutCall { Key = "new-0", Callback = Done }).AsTask();
        await Task.Delay(100);
        Assert.False(heldWrite.IsCompleted);

        releaseShard0.SetResult();
        foreach (var a in added) await a.AsTask().WaitAsync(TenSeconds);
        await heldWrite.WaitAsync(TenSeconds);
        await BatchTestUtil.Eventually(() => Volatile.Read(ref completed) == 7, TenSeconds, TimeSpan.FromMilliseconds(1));
        manager.Dispose();

        Assert.Equal(
            new[] { "old-0", "old-1", "old-2", "old-3", "old-4", "old-5", "new-0" },
            applied.Select(a => a.Key).ToArray());
        Assert.All(applied, a => Assert.Equal(1, a.Shard));
    }

    [Fact]
    public async Task WriteMetrics_AreRecordedOncePerOperationAndPerBatch_WhenAListenerIsAttached()
    {
        using var meterFactory = new TestMeterFactory();
        var metrics = new OxiaMetrics(meterFactory);
        using var reader = new MetricsReader(metrics.Meter);
        var executor = new FakeExecutor
        {
            Write = (ct, shard, prepare) => ValueTask.FromResult(FakeExecutor.Ok(prepare())),
        };
        var batcher = NewFactory(executor, maxRequests: 3, linger: TimeSpan.FromHours(1), metrics).NewWriteBatcher(CancellationToken.None, 1, 1024);

        int completed = 0;
        for (int i = 0; i < 3; i++)
        {
            await batcher.AddAsync(new PutCall { Key = "k", Value = new byte[10], Callback = (_, e) => Interlocked.Increment(ref completed) });
        }
        await BatchTestUtil.Eventually(() => Volatile.Read(ref completed) == 3, TenSeconds, TimeSpan.FromMilliseconds(1));
        batcher.Dispose();

        var ops = reader.Counter<long>("oxia_client_op");
        var op = Assert.Single(ops);
        Assert.Equal(3, op.Value);
        Assert.Equal("put", op.Attributes["type"]);
        Assert.Equal("success", op.Attributes["result"]);
        Assert.Equal(30, Assert.Single(reader.Histogram("oxia_client_op_value")).Value);

        var batchRequests = Assert.Single(reader.Histogram("oxia_client_batch_request"));
        Assert.Equal(3, batchRequests.Value); // one batch of 3 operations
        Assert.Equal(1UL, batchRequests.Count);
        Assert.Equal("write", batchRequests.Attributes["type"]);
        Assert.Equal(30, Assert.Single(reader.Histogram("oxia_client_batch_value")).Value);
        Assert.Equal(1, Assert.Single(reader.Counter<long>("oxia_client_batch_total")).Value);
        Assert.Equal(1, Assert.Single(reader.Counter<long>("oxia_client_batch_exec")).Value);
    }
}

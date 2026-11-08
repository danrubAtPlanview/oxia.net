// =====================================================================================================================
// PORT NOTE (test support of component F2)
//   Upstream : oxia/async_client_impl_test.go (capturingGetBatcher, capturingWriteBatcher, newGetTestClient, newWriteTestClient,
//              deleteRangeCallAt)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Batchers that only record the calls added to them, and the clients wired to them through the REAL
//              Manager / WriteManager (component D), exactly like the Go tests.
//   Divergences: Go compares `call.Callback(response, err)`; here the call is completed with `call.Complete(response, err)`
//              (the pooled calls of the hot path do not use the Callback field).  `chan model.GetCall` -> Channel.
// =====================================================================================================================
using System.Collections.Concurrent;
using System.Threading.Channels;
using Oxia.Client.Batch;
using Oxia.Client.Internal;
using Oxia.Client.Internal.Batch;
using Oxia.Client.Internal.Model;
using Oxia.Common.Proto;

namespace Oxia.Client.Tests.F2;

internal sealed class CapturingGetBatcher : IBatcher
{
    public Channel<GetCall> Calls { get; }

    public CapturingGetBatcher(int capacity) => Calls = Channel.CreateBounded<GetCall>(Math.Max(capacity, 1));

    public ValueTask AddAsync(object call)
    {
        if (!Calls.Writer.TryWrite((GetCall)call)) throw new InvalidOperationException("captured calls channel is full");
        return ValueTask.CompletedTask;
    }

    public Task RunAsync() => Task.CompletedTask;
    public void Dispose() { }

    public Task<GetCall> TakeAsync() => F2Util.ReadAsync(Calls.Reader);
}

internal sealed class CapturingWriteBatcher : IBatcher
{
    private readonly List<object> _calls = [];

    public List<object> Calls
    {
        get { lock (_calls) return [.. _calls]; }
    }

    public ValueTask AddAsync(object call)
    {
        lock (_calls) _calls.Add(call);
        return ValueTask.CompletedTask;
    }

    public Task RunAsync() => Task.CompletedTask;
    public void Dispose() { }
}

internal static class TestClients
{
    /// <summary>newGetTestClient: a client whose read batchers (one for every shard) capture the get calls.</summary>
    public static (AsyncClientImpl Client, CapturingGetBatcher Batcher) NewGetTestClient(params long[] shards)
    {
        var b = new CapturingGetBatcher(shards.Length);
        var client = new AsyncClientImpl
        {
            ShardManager = new StaticShardManager { Shards = shards },
            ReadBatchManager = new Manager(CancellationToken.None, (_, _) => b),
        };
        return (client, b);
    }

    /// <summary>newWriteTestClient: a client whose write batchers (one per shard) capture the write calls.</summary>
    public static (AsyncClientImpl Client, ConcurrentDictionary<long, CapturingWriteBatcher> Batchers) NewWriteTestClient(IShardManager shardManager)
    {
        var batchers = new ConcurrentDictionary<long, CapturingWriteBatcher>();
        var c = new AsyncClientImpl { ShardManager = shardManager };
        c.WriteBatchManager = new WriteManager(CancellationToken.None, (_, shardId) =>
        {
            var b = new CapturingWriteBatcher();
            batchers[shardId] = b;
            return b;
        }, c.ForwardWriteAsync);
        return (c, batchers);
    }

    /// <summary>deleteRangeCallAt: the only call of the batcher of the shard, which must be a delete range.</summary>
    public static DeleteRangeCall DeleteRangeCallAt(ConcurrentDictionary<long, CapturingWriteBatcher> batchers, long shardId)
    {
        Xunit.Assert.True(batchers.ContainsKey(shardId), $"no batcher for shard {shardId}");
        var calls = batchers[shardId].Calls;
        Xunit.Assert.Single(calls);
        return Xunit.Assert.IsAssignableFrom<DeleteRangeCall>(calls[0]);
    }

    /// <summary>var deleteRangeOK = &amp;proto.DeleteRangeResponse{Status: proto.Status_OK}</summary>
    public static readonly DeleteRangeResponse DeleteRangeOK = new() { Status = Status.Ok };
}

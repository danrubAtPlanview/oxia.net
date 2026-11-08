// =====================================================================================================================
// PORT NOTE (test support of component F2)
//   Upstream : oxia/async_client_impl_test.go (staticExecutor, newScanTestClient, endInclusiveTests),
//              oxia/async_client_reroute_test.go (splitExecutor, newRerouteTestClient)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Fakes of IExecutor.  Go embeds a nil internal.Executor in the fake so that the methods it does not implement
//              panic; here they throw NotImplementedException.  ExecuteWrite's `func() (*WriteRequest, error)` is
//              `Func<WriteRequest>` (a thrown exception is the error).
// =====================================================================================================================
using System.Text;
using Oxia.Client.Internal;
using Oxia.Client.Internal.Batch;
using Oxia.Client.Internal.Model;
using Oxia.Common.Constant;
using Oxia.Common.Proto;
using MetricsImpl = Oxia.Client.Internal.Metrics.Metrics;

namespace Oxia.Client.Tests.F2;

/// <summary>
/// staticExecutor answers the lists and the range scans of each shard with its keys, one per response, with the key as the
/// value of a record, then with its error, if it has one.  It sends the keys whatever the token, like a stream whose
/// responses were already received.
/// </summary>
internal sealed class StaticExecutor : IExecutor
{
    public Dictionary<long, string[]> Keys { get; init; } = new();
    public Dictionary<long, Exception> Errs { get; init; } = new();

    /// <summary>The shards whose scans end only once their token is done, like a scan that is still streaming.</summary>
    public HashSet<long> Slow { get; init; } = new();

    /// <summary>
    /// The shards that include the end of the range of the requests that ask for it, like a server with
    /// FEATURE_RANGE_END_INCLUSIVE: their responses say so, and they send one without keys if they have none.
    /// </summary>
    public HashSet<long> EndInclusive { get; init; } = new();

    public ValueTask<WriteResponse> ExecuteWrite(CancellationToken ct, long shardId, Func<WriteRequest> prepare) => throw new NotImplementedException();
    public ValueTask<ReadResponse> ExecuteRead(CancellationToken ct, ReadRequest request) => throw new NotImplementedException();

    public async ValueTask ExecuteList(CancellationToken ct, ListRequest request, Func<ListResponse, Exception?> listResponseConsumer)
    {
        var (responses, endInclusive) = Responses(request.Shard!.Value, request.EndInclusive);
        foreach (var keys in responses)
        {
            var response = new ListResponse { EndInclusive = endInclusive };
            response.Keys.AddRange(keys);
            var err = listResponseConsumer(response);
            if (err is not null) throw OxiaErrors.ToThrowable(err);
        }
        await End(ct, request.Shard!.Value);
    }

    public async ValueTask ExecuteRangeScan(CancellationToken ct, RangeScanRequest request, Func<RangeScanResponse, Exception?> rangeScanResponseConsumer)
    {
        var (responses, endInclusive) = Responses(request.Shard!.Value, request.EndInclusive);
        foreach (var keys in responses)
        {
            var response = new RangeScanResponse { EndInclusive = endInclusive };
            foreach (var key in keys)
            {
                response.Records.Add(new GetResponse { Key = key, Value = Encoding.UTF8.GetBytes(key), Version = new ProtoVersion() });
            }
            var err = rangeScanResponseConsumer(response);
            if (err is not null) throw OxiaErrors.ToThrowable(err);
        }
        await End(ct, request.Shard!.Value);
    }

    /// <summary>responses returns the keys of the responses of a shard to a list or a range scan, and whether the responses say that they include the end of the range.</summary>
    private (List<string[]> Responses, bool EndInclusive) Responses(long shard, bool requestEndInclusive)
    {
        bool endInclusive = requestEndInclusive && EndInclusive.Contains(shard);
        Keys.TryGetValue(shard, out var keys);
        if ((keys is null || keys.Length == 0) && endInclusive)
        {
            return ([[]], true);
        }
        var responses = new List<string[]>();
        foreach (var key in keys ?? [])
        {
            responses.Add([key]);
        }
        return (responses, endInclusive);
    }

    private async ValueTask End(CancellationToken ct, long shardId)
    {
        if (Slow.Contains(shardId))
        {
            await Task.Delay(Timeout.Infinite, ct);
        }
        if (Errs.TryGetValue(shardId, out var err))
        {
            throw OxiaErrors.ToThrowable(err);
        }
    }

    /// <summary>newScanTestClient</summary>
    public static AsyncClientImpl NewScanTestClient(StaticExecutor executor, params long[] shards)
        => new()
        {
            Options = new ClientOptions { RequestTimeout = TimeSpan.FromMinutes(1) },
            ShardManager = new StaticShardManager { Shards = shards },
            Executor = executor,
        };
}

/// <summary>
/// splitExecutor applies the puts it receives to an in-memory log.  Writes to a shard that is no longer in the client's shard
/// map fail with ErrShardNotFound, as in the RPC provider.  Writes to the frozen shard block until it is released, like a batch
/// that the client keeps retrying while the parent is frozen for the split cutover.
/// </summary>
internal sealed class SplitExecutor : IExecutor
{
    private readonly object _lock = new();
    private readonly List<string> _applied = [];

    public required SplittableShardManager ShardManager { get; init; }
    public long FrozenShard { get; init; }
    /// <summary>Go: frozenWrite chan struct{} (buffered 1): one item per write that reached the frozen shard.</summary>
    public System.Threading.Channels.Channel<bool> FrozenWrite { get; } = System.Threading.Channels.Channel.CreateUnbounded<bool>();
    public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public async ValueTask<WriteResponse> ExecuteWrite(CancellationToken ct, long shardId, Func<WriteRequest> prepare)
    {
        if (shardId == FrozenShard && ShardManager.Exists(FrozenShard))
        {
            FrozenWrite.Writer.TryWrite(true);
            await Release.Task;
        }
        if (!ShardManager.Exists(shardId))
        {
            throw OxiaErrors.ToThrowable(OxiaErrors.ShardNotFound);
        }
        var request = prepare();

        lock (_lock)
        {
            var response = new WriteResponse();
            foreach (var put in request.Puts)
            {
                _applied.Add($"{put.Key}={Encoding.UTF8.GetString(put.Value.Span)}");
                response.Puts.Add(new PutResponse { Status = Status.Ok, Version = new ProtoVersion() });
            }
            return response;
        }
    }

    public ValueTask<ReadResponse> ExecuteRead(CancellationToken ct, ReadRequest request) => throw new NotImplementedException();
    public ValueTask ExecuteList(CancellationToken ct, ListRequest request, Func<ListResponse, Exception?> c) => throw new NotImplementedException();
    public ValueTask ExecuteRangeScan(CancellationToken ct, RangeScanRequest request, Func<RangeScanResponse, Exception?> c) => throw new NotImplementedException();

    public List<string> AppliedWrites()
    {
        lock (_lock) return [.. _applied];
    }

    /// <summary>newRerouteTestClient</summary>
    public static AsyncClientImpl NewRerouteTestClient(SplittableShardManager shardManager, IExecutor executor)
    {
        var c = new AsyncClientImpl { ShardManager = shardManager };
        var batcherFactory = new BatcherFactory(executor, Oxia.Common.Constant.Constants.DefaultNamespace, TimeSpan.FromMilliseconds(1),
            OxiaOptions.DefaultMaxRequestsPerBatch, new MetricsImpl(null), OxiaOptions.DefaultRequestTimeout);
        batcherFactory.WriteRerouter = c.RerouteWrites;
        c.WriteBatchManager = new WriteManager(CancellationToken.None,
            (ct, shard) => batcherFactory.NewWriteBatcher(ct, shard, OxiaOptions.DefaultMaxBatchSize), c.ForwardWriteAsync);
        shardManager.OnShardsReplaced = c.WriteBatchManager.ShardsReplaced;
        return c;
    }
}

/// <summary>endInclusiveTest</summary>
internal sealed class EndInclusiveTest
{
    public required string Name { get; init; }
    public required bool Requested { get; init; }
    public required StaticExecutor Executor { get; init; }
    public string[]? Expected { get; init; }
    public Exception? Err { get; init; }

    /// <summary>
    /// endInclusiveTests returns the cases of the tests of EndInclusive, on the shards 1 and 2.  Go names: "included",
    /// "included, empty shard", "older server", "older server, empty shard", "not requested".
    /// </summary>
    public static EndInclusiveTest[] All() =>
    [
        new()
        {
            Name = "included", Requested = true,
            Executor = new StaticExecutor
            {
                Keys = { [1] = ["a", "b"], [2] = ["c"] },
                EndInclusive = [1, 2],
            },
            Expected = ["a", "b", "c"],
        },
        new()
        {
            Name = "included, empty shard", Requested = true,
            Executor = new StaticExecutor
            {
                Keys = { [1] = ["a"] },
                EndInclusive = [1, 2],
            },
            Expected = ["a"],
        },
        new()
        {
            Name = "older server", Requested = true,
            Executor = new StaticExecutor
            {
                Keys = { [1] = ["a"], [2] = ["c", "d"] },
                EndInclusive = [1],
            },
            Err = ClientErrors.EndInclusiveUnsupported,
        },
        new()
        {
            Name = "older server, empty shard", Requested = true,
            Executor = new StaticExecutor
            {
                Keys = { [1] = ["a"] },
                EndInclusive = [1],
            },
            Err = ClientErrors.EndInclusiveUnsupported,
        },
        new()
        {
            Name = "not requested", Requested = false,
            Executor = new StaticExecutor
            {
                Keys = { [1] = ["a"], [2] = ["c"] },
            },
            Expected = ["a", "c"],
        },
    ];

    public static EndInclusiveTest Get(string name) => All().Single(t => t.Name == name);
}

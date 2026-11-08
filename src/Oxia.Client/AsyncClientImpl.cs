// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/async_client_impl.go   (clientImpl, NewAsyncClient -> NewAsyncClientAsync here / OxiaClient.cs)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The asynchronous client.  Every operation resolves its options, picks the shard(s) with the shard manager and
//              hands a call to the batcher of the shard; the call's callback delivers the result.  Behavioural contract kept
//              from Go (see the comments on each member): key validation before any write; ephemeral puts wait for the
//              session of their shard; delete-range goes to all the shards (or to the one of the partition key); gets with a
//              comparison or an index go to all the shards and the best answer is selected; lists/range-scans fan out to the
//              shards (range-scan merges them in key order) and verify EndInclusive; writes pending on a shard removed by a
//              split are re-routed to the shards that replaced it, in their original order; Close order is sessions, write
//              batches, read batches, shard manager, rpc provider, then cancel, then notifications.
//   Divergences (all deliberate):
//      * `<-chan PutResult` etc. -> pooled ValueTask sources (PooledCalls.cs): Put/Get/Delete allocate nothing in steady
//        state.  A call that cannot be enqueued synchronously (the batcher queue is full / the shard holds its writes) makes
//        the returned ValueTask complete later (Go blocked the caller instead; a thread must never block here).
//      * Model calls are classes: a delete-range re-routed to SEVERAL successors is cloned per successor (Go copied the
//        struct), because every batch writes the OpIndex into the call.
//      * `panic("invalid call")` -> InvalidOperationException("invalid call"); `panic("unknown comparison type: N")` likewise.
//      * IExecutor.ExecuteList/ExecuteRangeScan take a SYNCHRONOUS consumer, so a consumer cannot wait for room in a
//        bounded channel like Go's blocking `ch <- result`.  Each shard therefore buffers its results in an unbounded
//        per-shard queue and a pump forwards them into the (bounded: List 1 = Go's unbuffered, RangeScan 100) output channel,
//        dropping them once the caller's token is done.  The streams are still read eagerly (no back-pressure to gRPC).
//      * `go func()` -> Task.Run; `sync.WaitGroup`/`wg.Go` -> a countdown; goroutine-leak assertions of the Go tests ->
//        RunningStreamTasks (number of List/RangeScan tasks that did not end yet).
//      * The factory resolves the dial options of a resolver through GrpcResolverBuilder.AsDialOption.
// =====================================================================================================================
using System.Threading.Channels;
using Oxia.Client.Internal;
using Oxia.Client.Internal.Batch;
using Oxia.Client.Internal.Model;
using Oxia.Common;
using Oxia.Common.Concurrent;
using Oxia.Common.Constant;
using Oxia.Common.Proto;
using MetricsImpl = Oxia.Client.Internal.Metrics.Metrics;
using RpcProviderImpl = Oxia.Client.Internal.RpcProvider;
using ShardManagerImpl = Oxia.Client.Internal.ShardManager;
using DizzyShardManagerImpl = Oxia.Client.Internal.DizzyShardManager;
using ShardStrategyImpl = Oxia.Client.Internal.ShardStrategy;

namespace Oxia.Client;

internal sealed class AsyncClientImpl : IAsyncClient
{
    // Go: the fields of clientImpl.  Internal and settable: the Go tests build a clientImpl with a literal.
    internal ClientOptions Options = new();
    internal IShardManager ShardManager = null!;
    internal WriteManager WriteBatchManager = null!;
    internal Manager ReadBatchManager = null!;
    internal IExecutor Executor = null!;
    internal Sessions Sessions = null!;
    internal readonly List<NotificationsImpl> Notifications = new();
    internal IRpcProvider RpcProvider = null!;
    /// <summary>Go: ctx -- done once the client is closed.</summary>
    internal CancellationToken Ctx;
    /// <summary>Go: cancel.</summary>
    internal CancellationTokenSource? CtxSource;

    /// <summary>Released with the client (it disposes the Meter it created when no IMeterFactory was supplied).</summary>
    internal MetricsImpl? Metrics;

    private readonly object _notificationsLock = new();
    private readonly Action<long, object> _handOff;
    private int _streamTasks;

    /// <summary>A client with no collaborators, to be filled in field by field (Go: a clientImpl literal in the tests).</summary>
    internal AsyncClientImpl()
    {
        _handOff = HandOffToWriteManager;
    }

    internal AsyncClientImpl(ClientOptions options) : this()
    {
        Options = options;
        CtxSource = new CancellationTokenSource();
        Ctx = CtxSource.Token;
    }

    /// <summary>Number of List / RangeScan tasks started by this client that did not end yet (Go tests: assertGoroutinesEnd).</summary>
    internal int RunningStreamTasks => Volatile.Read(ref _streamTasks);

    // -----------------------------------------------------------------------------------------------------------------
    // Creation (Go: NewAsyncClient, after newClientOptions)
    // -----------------------------------------------------------------------------------------------------------------

    internal static async ValueTask<AsyncClientImpl> NewAsyncClientAsync(ClientOptions options)
    {
        var c = new AsyncClientImpl(options);

        Action<Grpc.Net.Client.GrpcChannelOptions>[] grpcDialOptions = options.Resolver is not null
            ? [GrpcResolverBuilder.AsDialOption(options.Resolver)]
            : [];

        IShardManager? shardManager = null;
        var rpcProvider = RpcProviderImpl.Create(c.Ctx, options.Namespace, options.Tls, options.Authentication,
            options.ServiceAddress, () => shardManager, grpcDialOptions);

        c.Metrics = new MetricsImpl(options.MeterFactory);
        var batcherFactory = new BatcherFactory(
            rpcProvider,
            options.Namespace,
            options.BatchLinger,
            options.MaxRequestsPerBatch,
            c.Metrics,
            options.RequestTimeout);
        batcherFactory.WriteRerouter = c.RerouteWrites;
        batcherFactory.ReadRerouter = c.RerouteReads;
        // The shard manager tells the write batchers when shards are replaced
        c.WriteBatchManager = new WriteManager(c.Ctx,
            (ct, shard) => batcherFactory.NewWriteBatcher(ct, shard, options.MaxBatchSize), c.ForwardWriteAsync);

        try
        {
            if (options.FailureInjection.Contains(Failure.DizzyShardManager))
            {
                shardManager = await DizzyShardManagerImpl.CreateAsync(ShardStrategyImpl.New(), rpcProvider, options.ServiceAddress,
                    options.Namespace, options.RequestTimeout, c.WriteBatchManager.ShardsReplaced).ConfigureAwait(false);
            }
            else
            {
                shardManager = await ShardManagerImpl.CreateAsync(ShardStrategyImpl.New(), rpcProvider, options.ServiceAddress,
                    options.Namespace, options.RequestTimeout, c.WriteBatchManager.ShardsReplaced).ConfigureAwait(false);
            }
        }
        catch
        {
            c.CtxSource!.Cancel();
            try { await rpcProvider.CloseAsync().ConfigureAwait(false); } catch { /* Go: _ = rpcProvider.Close() */ }
            c.Metrics.Dispose();
            throw;
        }
        c.RpcProvider = rpcProvider;
        c.ShardManager = shardManager;

        c.ReadBatchManager = new Manager(c.Ctx, batcherFactory.NewReadBatcher);
        c.Executor = rpcProvider;

        c.Sessions = new Sessions(c.Ctx, c.ShardManager, c.RpcProvider, c.Options);

        return c;
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Re-routing after shard splits
    // -----------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Go: rerouteWrites -- hands off the calls of a batch that failed because its shard was removed from the shard map to
    /// the shards that replaced it, in their original order.  Those shards hold the calls issued after the shard map
    /// changed until the removed shard has handed off all its calls.
    /// </summary>
    internal void RerouteWrites(long removedShardId, List<PutCall> puts, List<DeleteCall> deletes, List<DeleteRangeCall> deleteRanges)
    {
        foreach (var call in ModelExtensions.InOpIndexOrder(puts, deletes, deleteRanges))
        {
            SendToSuccessors(removedShardId, call, _handOff);
        }
    }

    /// <summary>
    /// Go: forwardWrite -- sends a call that was routed to a shard with the shard map from before the shard was removed to the
    /// shards that replaced it, with Add.  The routing (and the "invalid call" failure) is synchronous; the returned task
    /// completes once the successors accepted the call (they may hold it until the removed shard handed off its calls).
    /// </summary>
    internal ValueTask ForwardWriteAsync(long removedShardId, object call)
    {
        List<(long ShardId, object Call)>? sends = null;
        SendToSuccessors(removedShardId, call, (shardId, c) => (sends ??= new List<(long, object)>(1)).Add((shardId, c)));
        return sends is null ? ValueTask.CompletedTask : AddAllAsync(sends);
    }

    private async ValueTask AddAllAsync(List<(long ShardId, object Call)> sends)
    {
        foreach (var (shardId, call) in sends)
        {
            try
            {
                await WriteBatchManager.AddAsync(shardId, call).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                FailCall(call, e);
            }
        }
    }

    /// <summary>
    /// Go: sendToSuccessors -- sends a call for a removed shard to the shard that replaced it for the call's key, or, for a
    /// delete range without a partition key, to all the shards that replaced it.
    /// </summary>
    internal void SendToSuccessors(long removedShardId, object call, Action<long, object> send)
    {
        string key;
        switch (call)
        {
            case PutCall put:
                key = put.PartitionKeyOrKey();
                break;
            case DeleteCall delete:
                key = delete.PartitionKeyOrKey();
                break;
            case DeleteRangeCall deleteRange:
                if (deleteRange.PartitionKey is null)
                {
                    SendDeleteRangeToSuccessors(removedShardId, deleteRange, send);
                    return;
                }
                key = deleteRange.PartitionKey;
                break;
            default:
                throw new InvalidOperationException("invalid call");
        }

        if (ShardManager.GetSuccessor(removedShardId, key, out var shardId))
        {
            send(shardId, call);
        }
        else
        {
            FailCall(call, OxiaErrors.ShardNotFound);
        }
    }

    /// <summary>
    /// Go: sendDeleteRangeToSuccessors -- sends a delete range without a partition key that was sent to a removed shard.
    /// The other shards got their own copy of the delete range when it was issued, so it only goes to the shards that
    /// replaced the removed one.
    /// </summary>
    internal void SendDeleteRangeToSuccessors(long removedShardId, DeleteRangeCall call, Action<long, object> send)
    {
        var successors = ShardManager.GetSuccessors(removedShardId);
        if (successors is null || successors.Length == 0)
        {
            call.Complete(null, OxiaErrors.ShardNotFound);
            return;
        }
        var callback = MultiShardDeleteRangeCallback(successors.Length, call.Complete);
        foreach (var shardId in successors)
        {
            // One copy per shard (Go passed the struct by value): every batch writes its own OpIndex into the call it is given
            var copy = call.Clone();
            copy.Callback = callback;
            send(shardId, copy);
        }
    }

    /// <summary>
    /// Go: multiShardDeleteRangeCallback -- invokes the callback once for a delete range sent to multiple shards: with the
    /// first failure, discarding the remaining responses, or otherwise with the OK response once every shard has responded.
    /// </summary>
    internal static Action<DeleteRangeResponse?, Exception?> MultiShardDeleteRangeCallback(int numShards,
        Action<DeleteRangeResponse?, Exception?> callback)
    {
        var gate = new object();
        int counter = numShards;

        return (response, err) =>
        {
            bool done;
            lock (gate)
            {
                if (counter == 0)
                {
                    // Response already sent, nothing to do
                    return;
                }
                if (err is not null || response!.Status != Status.Ok)
                {
                    counter = 0;
                }
                else
                {
                    counter--;
                }
                done = counter == 0;
            }

            if (done)
            {
                callback(response, err);
            }
        };
    }

    /// <summary>Go: rerouteReads -- the reads of a removed shard are sent again with the current shard map.</summary>
    internal void RerouteReads(List<GetCall> gets)
    {
        foreach (var get in gets)
        {
            long shardId = ShardManager.Get(get.PartitionKeyOrKey());
            var add = ReadBatchManager.Get(shardId).AddAsync(get);
            if (!add.IsCompletedSuccessfully) AddObserver.Observe(add, get);
        }
    }

    private void HandOffToWriteManager(long shardId, object call) => WriteBatchManager.HandOff(shardId, call);

    /// <summary>Adds a call to the batcher of a shard without waiting for room: a failure to add fails the call.</summary>
    internal void AddToWriteManager(long shardId, object call)
    {
        var add = WriteBatchManager.AddAsync(shardId, call);
        if (!add.IsCompletedSuccessfully) AddObserver.Observe(add, call);
    }

    /// <summary>Go: `call.Callback(nil, err)` for any kind of call.</summary>
    internal static void FailCall(object call, Exception err)
    {
        switch (call)
        {
            case PutCall put: put.Complete(null, err); break;
            case DeleteCall delete: delete.Complete(null, err); break;
            case DeleteRangeCall deleteRange: deleteRange.Complete(null, err); break;
            case GetCall get: get.Complete(null, err); break;
            default: throw new InvalidOperationException("invalid call");
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Close
    // -----------------------------------------------------------------------------------------------------------------

    public async ValueTask CloseAsync()
    {
        List<Exception>? errors = null;

        try { await Sessions.CloseAsync().ConfigureAwait(false); } catch (Exception e) { (errors ??= []).Add(e); }
        try { WriteBatchManager.Dispose(); } catch (Exception e) { (errors ??= []).Add(e); }
        try { ReadBatchManager.Dispose(); } catch (Exception e) { (errors ??= []).Add(e); }
        try { ShardManager.Dispose(); } catch (Exception e) { (errors ??= []).Add(e); }
        try { await RpcProvider.CloseAsync().ConfigureAwait(false); } catch (Exception e) { (errors ??= []).Add(e); }
        CtxSource?.Cancel();

        NotificationsImpl[] notifications;
        lock (_notificationsLock)
        {
            notifications = Notifications.ToArray();
        }
        foreach (var n in notifications)
        {
            try { await n.CloseAsync().ConfigureAwait(false); } catch (Exception e) { (errors ??= []).Add(e); }
        }

        try { Metrics?.Dispose(); } catch (Exception e) { (errors ??= []).Add(e); }

        if (errors is not null)
        {
            // multierr: a single error stays itself
            throw errors.Count == 1 ? errors[0] : new AggregateException(errors);
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Writes
    // -----------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Go: validateKey -- checks that a key, or a key range bound, that a write sends is valid UTF-8, as proto3 requires for
    /// strings.  The server doesn't check it, and its key encoders store a key that isn't on another key: e.g. with the
    /// natural key sorting, "\xff\xffoxia/term" is stored on the internal key "__oxia/term".  A .NET string holds UTF-16:
    /// the equivalent of invalid UTF-8 is a lone surrogate (it cannot be encoded).
    /// </summary>
    internal static Exception? ValidateKey(string key)
    {
        if (IsValidUtf16(key)) return null;
        return OxiaErrors.Wrap(ClientErrors.InvalidOptions, $"key {Quote(key)} is not valid UTF-8");
    }

    /// <summary>Go: utf8.ValidString for a UTF-16 string: no lone surrogates.</summary>
    internal static bool IsValidUtf16(string s)
    {
        var span = s.AsSpan();
        int i = span.IndexOfAnyInRange('\uD800', '\uDFFF');
        if (i < 0) return true;
        for (; i < span.Length; i++)
        {
            char c = span[i];
            if (char.IsHighSurrogate(c))
            {
                if (i + 1 < span.Length && char.IsLowSurrogate(span[i + 1]))
                {
                    i++;
                    continue;
                }
                return false;
            }
            if (char.IsLowSurrogate(c)) return false;
        }
        return true;
    }

    private static string Quote(string s)
    {
        var sb = new System.Text.StringBuilder(s.Length + 2).Append('"');
        for (int i = 0; i < s.Length; i++)
        {
            char c = s[i];
            if (char.IsHighSurrogate(c) && i + 1 < s.Length && char.IsLowSurrogate(s[i + 1]))
            {
                sb.Append(c).Append(s[++i]);
            }
            else if (char.IsSurrogate(c) || char.IsControl(c) || c == '"' || c == '\\')
            {
                sb.Append("\\u").Append(((int)c).ToString("x4"));
            }
            else
            {
                sb.Append(c);
            }
        }
        return sb.Append('"').ToString();
    }

    public ValueTask<PutResult> PutAsync(string key, ReadOnlyMemory<byte> value, params ReadOnlySpan<PutOption> options)
    {
        var keyErr = ValidateKey(key);
        if (keyErr is not null) return new ValueTask<PutResult>(new PutResult("", default, keyErr));

        var optionsErr = PutOptions.TryCreate(options, out var opts);
        if (optionsErr is not null) return new ValueTask<PutResult>(new PutResult("", default, optionsErr));

        var call = PooledPutCall.Rent();
        call.Key = key;
        call.Value = value;
        call.ExpectedVersionId = opts.ExpectedVersion;
        call.SequenceKeysDeltas = opts.SequenceKeysDeltas;
        call.PartitionKey = opts.PartitionKey;
        call.CallContext = opts.CallContext;
        if (opts.SecondaryIndexes is not null) call.SecondaryIndexes = ProtoUtils.ToSecondaryIndexes(opts.SecondaryIndexes);
        var result = call.AsValueTask();

        if (opts.Ephemeral)
        {
            call.ClientIdentity = Options.Identity;
            call.StartEphemeral(this);
            return result;
        }

        var add = WriteBatchManager.AddAsync(GetShardForKey(key, opts.PartitionKey), call);
        if (!add.IsCompletedSuccessfully) AddObserver.Observe(add, call);
        return result;
    }

    public ValueTask<Exception?> DeleteAsync(string key, params ReadOnlySpan<DeleteOption> options)
    {
        var keyErr = ValidateKey(key);
        if (keyErr is not null) return new ValueTask<Exception?>(keyErr);

        var opts = DeleteOptions.Create(options);
        var call = PooledDeleteCall.Rent();
        call.Key = key;
        call.ExpectedVersionId = opts.ExpectedVersion;
        call.PartitionKey = opts.PartitionKey;
        call.CallContext = opts.CallContext;
        var result = call.AsValueTask();

        var add = WriteBatchManager.AddAsync(GetShardForKey(key, opts.PartitionKey), call);
        if (!add.IsCompletedSuccessfully) AddObserver.Observe(add, call);
        return result;
    }

    public ValueTask<Exception?> DeleteRangeAsync(string minKeyInclusive, string maxKeyExclusive, params ReadOnlySpan<DeleteRangeOption> options)
    {
        var keyErr = ValidateKey(minKeyInclusive) ?? ValidateKey(maxKeyExclusive);
        if (keyErr is not null) return new ValueTask<Exception?>(keyErr);

        var opts = DeleteRangeOptions.Create(options);
        if (opts.PartitionKey is not null)
        {
            long shardId = GetShardForKey("", opts.PartitionKey);
            return DoSingleShardDeleteRange(shardId, minKeyInclusive, maxKeyExclusive, opts.PartitionKey, opts.CallContext);
        }

        // If there is no partition key, we will make the request to delete-range on all the shards
        var shardIds = ShardManager.GetAll();
        var wg = new WaitGroup(shardIds.Length);

        Action<DeleteRangeResponse?, Exception?> callback = (response, err) =>
        {
            if (err is not null)
            {
                wg.Fail(err);
                return;
            }

            if (response!.Status == Status.Ok)
            {
                wg.Done();
            }
            else
            {
                wg.Fail(ProtoUtils.ToError(response.Status)!);
            }
        };

        foreach (var shardId in shardIds)
        {
            AddToWriteManager(shardId, new DeleteRangeCall
            {
                MinKeyInclusive = minKeyInclusive,
                MaxKeyExclusive = maxKeyExclusive,
                // Shared by the calls to all the shards: once one is sent, the others are not dropped anymore
                CallContext = opts.CallContext,
                Callback = callback,
            });
        }
        return wg.WaitAsync(Ctx);
    }

    private ValueTask<Exception?> DoSingleShardDeleteRange(long shardId, string minKeyInclusive, string maxKeyExclusive,
        string partitionKey, CallContext? callContext)
    {
        var call = new DeleteRangeCompletion
        {
            MinKeyInclusive = minKeyInclusive,
            MaxKeyExclusive = maxKeyExclusive,
            PartitionKey = partitionKey,
            CallContext = callContext,
        };
        var result = call.AsValueTask();
        AddToWriteManager(shardId, call);
        return result;
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Reads
    // -----------------------------------------------------------------------------------------------------------------

    public ValueTask<GetResult> GetAsync(string key, params ReadOnlySpan<GetOption> options)
    {
        var opts = GetOptions.Create(options);
        if (opts.PartitionKey is null &&
            (opts.ComparisonType != KeyComparisonType.Equal || opts.SecondaryIndexName is not null))
        {
            return DoMultiShardGet(key, opts);
        }
        return DoSingleShardGet(key, opts);
    }

    private ValueTask<GetResult> DoSingleShardGet(string key, in GetOptions opts)
    {
        var call = PooledGetCall.Rent();
        call.Key = key;
        call.ComparisonType = opts.ComparisonType;
        call.IncludeValue = opts.IncludeValue;
        call.SecondaryIndexName = opts.SecondaryIndexName;
        call.PartitionKey = opts.PartitionKey;
        var result = call.AsValueTask();

        var add = ReadBatchManager.Get(GetShardForKey(key, opts.PartitionKey)).AddAsync(call);
        if (!add.IsCompletedSuccessfully) AddObserver.Observe(add, call);
        return result;
    }

    /// <summary>Go: compareGetResponse.</summary>
    internal static int CompareGetResponse(KeyOrder order, GetResponse a, GetResponse b)
    {
        if (a.SecondaryIndexKey is not null && b.SecondaryIndexKey is not null)
        {
            int c = order.Compare(a.SecondaryIndexKey, b.SecondaryIndexKey);
            if (c != 0) return c;
        }
        return order.Compare(a.Key ?? "", b.Key ?? "");
    }

    /// <summary>Go: validateComparisonType.</summary>
    internal static Exception? ValidateComparisonType(KeyComparisonType c) => c switch
    {
        KeyComparisonType.Equal or KeyComparisonType.Floor or KeyComparisonType.Lower or KeyComparisonType.Ceiling
            or KeyComparisonType.Higher => null,
        _ => new OxiaException($"unsupported comparison type: {c}"),
    };

    /// <summary>Go: keyNotFound -- the sentinel "nothing selected yet" (compared by reference).</summary>
    internal static readonly GetResponse KeyNotFoundResponse = new() { Status = Status.KeyNotFound };

    /// <summary>Go: selectResponse.</summary>
    internal static GetResponse SelectResponse(KeyComparisonType kc, KeyOrder order, GetResponse selected, GetResponse? response)
    {
        if (response is not null && response.Status == Status.Ok)
        {
            switch (kc)
            {
                case KeyComparisonType.Equal:
                    if (ReferenceEquals(selected, KeyNotFoundResponse)) selected = response;
                    break;

                case KeyComparisonType.Floor:
                case KeyComparisonType.Lower:
                    if (ReferenceEquals(selected, KeyNotFoundResponse) || CompareGetResponse(order, selected, response) < 0)
                        selected = response;
                    break;

                case KeyComparisonType.Ceiling:
                case KeyComparisonType.Higher:
                    if (ReferenceEquals(selected, KeyNotFoundResponse) || CompareGetResponse(order, selected, response) > 0)
                        selected = response;
                    break;

                default:
                    throw new InvalidOperationException($"unknown comparison type: {kc}");
            }
        }
        return selected;
    }

    /// <summary>The keys might get hashed to multiple shards, so we have to check on all shards and then compare the results (Go: doMultiShardGet).</summary>
    private ValueTask<GetResult> DoMultiShardGet(string key, in GetOptions options)
    {
        var comparisonErr = ValidateComparisonType(options.ComparisonType);
        if (comparisonErr is not null) return new ValueTask<GetResult>(ProtoUtils.ToGetResult(null, key, comparisonErr));

        var shards = ShardManager.GetAll();
        var multi = MultiShardGetCallback(key, options.ComparisonType, GetKeyOrder(), shards.Length);
        var result = multi.AsValueTask();

        foreach (var shardId in shards)
        {
            var call = new MultiShardGetCall(multi)
            {
                Key = key,
                ComparisonType = options.ComparisonType,
                IncludeValue = options.IncludeValue,
                SecondaryIndexName = options.SecondaryIndexName,
            };
            var add = ReadBatchManager.Get(shardId).AddAsync(call);
            if (!add.IsCompletedSuccessfully) AddObserver.Observe(add, call);
        }
        return result;
    }

    /// <summary>
    /// Go: multiShardGetCallback -- aggregates the per-shard responses of a multi-shard get: the first error wins and
    /// terminates the result, discarding the remaining responses; otherwise the best response for the comparison type is
    /// selected once every shard has responded.
    /// </summary>
    internal static MultiShardGet MultiShardGetCallback(string key, KeyComparisonType comparisonType, KeyOrder order, int numShards)
        => new(key, comparisonType, order, numShards);

    // -----------------------------------------------------------------------------------------------------------------
    // List
    // -----------------------------------------------------------------------------------------------------------------

    private async Task ListFromShardAsync(CancellationToken ct, string minKeyInclusive, string maxKeyExclusive, ListOptions opts,
        long shardId, ChannelWriter<ListResult> output)
    {
        var request = new ListRequest
        {
            Shard = shardId,
            StartInclusive = minKeyInclusive,
            EndExclusive = maxKeyExclusive,
            SecondaryIndexName = opts.SecondaryIndexName,
            IncludeInternalKeys = opts.ShowInternalKeys,
            EndInclusive = opts.EndInclusive,
        };

        using var retryCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        retryCts.CancelAfter(Options.RequestTimeout);

        // The consumer of the executor is synchronous: the results of the shard are queued here and forwarded to the output
        // by a pump that (like Go's `select { case ch <- result: case <-ctx.Done(): }`) drops them once ct is done
        var local = Channel.CreateUnbounded<ListResult>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });
        var pump = PumpAsync(local.Reader, output, ct);

        // Older servers ignore EndInclusive, and leave the end out: a leader that includes it says so in all its
        // responses, and sends at least one
        bool responded = false;
        Exception? err = null;
        try
        {
            await Executor.ExecuteList(retryCts.Token, request, response =>
            {
                if (request.EndInclusive && !response.EndInclusive)
                {
                    return ClientErrors.EndInclusiveUnsupported;
                }
                responded = true;
                if (response.Keys.Count > 0 && !ct.IsCancellationRequested)
                {
                    local.Writer.TryWrite(new ListResult(response.Keys.ToArray(), null));
                }
                return null;
            }).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            err = e;
        }
        if (err is null && request.EndInclusive && !responded)
        {
            err = ClientErrors.EndInclusiveUnsupported;
        }
        if (err is not null)
        {
            local.Writer.TryWrite(new ListResult(null, err));
        }
        local.Writer.TryComplete();
        await pump.ConfigureAwait(false);
    }

    /// <summary>Forwards the queued results to the output, waiting for room; once ct is done the remaining results are dropped.</summary>
    private static async Task PumpAsync<T>(ChannelReader<T> source, ChannelWriter<T> target, CancellationToken ct)
    {
        bool dropping = false;
        while (await source.WaitToReadAsync().ConfigureAwait(false))
        {
            while (source.TryRead(out var item))
            {
                if (dropping || target.TryWrite(item)) continue;
                try
                {
                    await target.WriteAsync(item, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    dropping = true;
                }
                catch (ChannelClosedException)
                {
                    dropping = true;
                }
            }
        }
    }

    public ChannelReader<ListResult> ListAsync(CancellationToken ct, string minKeyInclusive, string maxKeyExclusive,
        params ReadOnlySpan<ListOption> options)
    {
        // Go: unbuffered channel; a bounded channel of capacity 1 is the closest .NET equivalent
        var channel = Channel.CreateBounded<ListResult>(new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.Wait });
        var opts = ListOptions.Create(options);
        var writer = channel.Writer;

        if (opts.PartitionKey is not null)
        {
            // If the partition key is specified, we only need to make the request to one shard
            long shardId = GetShardForKey("", opts.PartitionKey);
            StartStreamTask(async () =>
            {
                await ListFromShardAsync(ct, minKeyInclusive, maxKeyExclusive, opts, shardId, writer).ConfigureAwait(false);
                writer.TryComplete();
            });
        }
        else
        {
            // Do the list on all shards and aggregate the responses
            var shardIds = ShardManager.GetAll();

            // The channel is completed once no shard can send to it anymore
            int remaining = shardIds.Length;
            if (remaining == 0) writer.TryComplete();
            foreach (var shardId in shardIds)
            {
                StartStreamTask(async () =>
                {
                    try
                    {
                        await ListFromShardAsync(ct, minKeyInclusive, maxKeyExclusive, opts, shardId, writer).ConfigureAwait(false);
                    }
                    finally
                    {
                        if (Interlocked.Decrement(ref remaining) == 0) writer.TryComplete();
                    }
                });
            }
        }

        return channel.Reader;
    }

    // -----------------------------------------------------------------------------------------------------------------
    // RangeScan
    // -----------------------------------------------------------------------------------------------------------------

    /// <summary>Go: rangeScanFromShard -- passes each record of the shard to onResult, with its secondary key when the scan uses an index, or the error of the scan.</summary>
    private async Task RangeScanFromShardAsync(CancellationToken ct, string minKeyInclusive, string maxKeyExclusive,
        RangeScanOptions opts, long shardId, Action<GetResult, string?> onResult)
    {
        var request = new RangeScanRequest
        {
            Shard = shardId,
            StartInclusive = minKeyInclusive,
            EndExclusive = maxKeyExclusive,
            SecondaryIndexName = opts.SecondaryIndexName,
            IncludeInternalKeys = opts.ShowInternalKeys,
            EndInclusive = opts.EndInclusive,
        };
        if (!opts.IncludeValue)
        {
            request.IncludeValues = false;
        }

        using var retryCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        retryCts.CancelAfter(Options.RequestTimeout);

        // As for a list, the responses say whether the leader included the end
        bool responded = false;
        Exception? err = null;
        try
        {
            await Executor.ExecuteRangeScan(retryCts.Token, request, response =>
            {
                if (request.EndInclusive && !response.EndInclusive)
                {
                    return ClientErrors.EndInclusiveUnsupported;
                }
                responded = true;
                foreach (var record in response.Records)
                {
                    if (!opts.IncludeValue)
                    {
                        record.Value = null;
                    }
                    onResult(ProtoUtils.ToGetResult(record, "", null), record.SecondaryIndexKey);
                }
                return null;
            }).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            err = e;
        }
        if (err is null && request.EndInclusive && !responded)
        {
            err = ClientErrors.EndInclusiveUnsupported;
        }
        if (err is not null)
        {
            onResult(new GetResult("", null, default, err), null);
        }
    }

    public ChannelReader<GetResult> RangeScanAsync(CancellationToken ct, string minKeyInclusive, string maxKeyExclusive,
        params ReadOnlySpan<RangeScanOption> options)
    {
        var outCh = Channel.CreateBounded<GetResult>(new BoundedChannelOptions(100) { FullMode = BoundedChannelFullMode.Wait });
        var opts = RangeScanOptions.Create(options);
        var writer = outCh.Writer;

        if (opts.PartitionKey is not null)
        {
            // If the partition key is specified, we only need to make the request to one shard
            long shardId = GetShardForKey("", opts.PartitionKey);
            StartStreamTask(async () =>
            {
                var local = Channel.CreateUnbounded<GetResult>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });
                var pump = PumpAsync(local.Reader, writer, ct);
                try
                {
                    await RangeScanFromShardAsync(ct, minKeyInclusive, maxKeyExclusive, opts, shardId,
                        (result, _) => local.Writer.TryWrite(result)).ConfigureAwait(false);
                }
                finally
                {
                    local.Writer.TryComplete();
                }
                await pump.ConfigureAwait(false);
                writer.TryComplete();
            });
        }
        else
        {
            // Do the scan on all shards and aggregate the responses
            var shardIds = ShardManager.GetAll();
            var channels = new Channel<RangeScanResult>[shardIds.Length];
            var readers = new ChannelReader<RangeScanResult>[shardIds.Length];
            var shardTasks = new Task[shardIds.Length];
            var scanCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            var scanToken = scanCts.Token;

            for (int i = 0; i < shardIds.Length; i++)
            {
                long shardId = shardIds[i];
                var ch = Channel.CreateUnbounded<RangeScanResult>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });
                channels[i] = ch;
                readers[i] = ch.Reader;
                shardTasks[i] = StartStreamTask(async () =>
                {
                    try
                    {
                        await RangeScanFromShardAsync(scanToken, minKeyInclusive, maxKeyExclusive, opts, shardId,
                            (result, secondaryIndexKey) => ch.Writer.TryWrite(new RangeScanResult(result, secondaryIndexKey))).ConfigureAwait(false);
                    }
                    finally
                    {
                        ch.Writer.TryComplete();
                    }
                });
            }

            var order = GetKeyOrder();
            StartStreamTask(async () =>
            {
                try
                {
                    await RangeScanMerger.AggregateAndSortRangeScanAcrossShardsAsync(order, readers, writer, ct).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // ct done while the merge waited for the reader of the output
                }
                finally
                {
                    writer.TryComplete();

                    // The merge stops at the first error: cancel the scans of the other shards, and wait until they end
                    scanCts.Cancel();
                    try { await Task.WhenAll(shardTasks).ConfigureAwait(false); } catch (Exception) { /* the shard tasks do not throw */ }
                    scanCts.Dispose();
                }
            });
        }

        return outCh.Reader;
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Sequence updates, notifications
    // -----------------------------------------------------------------------------------------------------------------

    public ChannelReader<string> GetSequenceUpdates(CancellationToken ct, string prefixKey, params ReadOnlySpan<GetSequenceUpdatesOption> options)
    {
        var opts = GetSequenceUpdatesOptions.Create(options);
        if (opts.PartitionKey is null)
        {
            throw OxiaErrors.Wrap(ClientErrors.InvalidOptions, "partitionKey is required");
        }

        return SequenceUpdates.Create(ct, Ctx, prefixKey, opts.PartitionKey, RpcProvider, ShardManager);
    }

    public async ValueTask<INotifications> GetNotificationsAsync()
    {
        NotificationsImpl nm;
        try
        {
            nm = await NotificationsImpl.CreateAsync(Ctx, Options, RpcProvider, ShardManager).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            throw OxiaErrors.Wrap(e, "Failed to create notification stream");
        }

        lock (_notificationsLock)
        {
            Notifications.Add(nm);
        }

        return nm;
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------------------------------------------------

    internal KeyOrder GetKeyOrder() => KeyOrder.New(ShardManager.KeySorting);

    /// <summary>Go: getShardForKey -- the shard of the partition key if there is one, otherwise of the key.</summary>
    internal long GetShardForKey(string key, string? partitionKey) => ShardManager.Get(partitionKey ?? key);

    private Task StartStreamTask(Func<Task> body)
    {
        Interlocked.Increment(ref _streamTasks);
        return Task.Run(async () =>
        {
            try
            {
                await body().ConfigureAwait(false);
            }
            finally
            {
                Interlocked.Decrement(ref _streamTasks);
            }
        });
    }
}

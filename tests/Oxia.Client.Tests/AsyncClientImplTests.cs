// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/async_client_impl_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests: 29 top-level (TestMultiShardDeleteRangeCallback 3 subtests, TestAsyncClientListEndInclusive 5,
//             TestAsyncClientRangeScanEndInclusive 5), ported: 29, skipped: 0
//   Real components used (not fakes): D's Manager / WriteManager, E's RpcProvider, F1's Sessions / NotificationsImpl, C's options,
//   ProtoUtils, KeyOrder, RangeScanMerger, B's WaitGroup / OxiaErrors.  The shard managers, batchers and executors are fakes
//   (Fakes/*.Extra.cs), as in Go.
//   Adapted  (deliberate, see the comments at each test):
//     * `call.Callback(response, err)` -> `call.Complete(response, err)` (the pooled calls complete through Complete).
//     * The result channels (`<-chan X`, closed after the single value) are ValueTasks: the "channel is closed afterwards"
//       assertions (`_, open := <-resultCh; assert.False(open)`) have no analogue and are replaced by checks that the result
//       was delivered exactly once (MultiShardGet.Delivered) where the callback aggregates.
//     * Keys with invalid UTF-8 ("\xff\xffoxia/term", "a\xff/") -> keys with lone surrogates (the UTF-16 equivalent).
//     * context.DeadlineExceeded -> OxiaErrors.IsDeadlineExceeded (the fake provider throws TimeoutException for an expired
//       token).  Go's unbuffered result channel of List is a bounded(1) channel: at most one result can remain buffered.
//     * assertGoroutinesEnd(t, "(*clientImpl).List") -> the client counts its running List/RangeScan tasks (RunningStreamTasks).
// =====================================================================================================================
using System.Text;
using Oxia.Client.Internal;
using Oxia.Client.Internal.Batch;
using Oxia.Client.Internal.Model;
using Oxia.Common.Constant;
using Oxia.Common.Proto;
using Xunit;
using static Oxia.Client.Tests.F2.TestClients;
using RpcProviderImpl = Oxia.Client.Internal.RpcProvider;

namespace Oxia.Client.Tests.F2;

public class AsyncClientImplTests
{
    private static readonly byte[] V = "v"u8.ToArray();

    /// <summary>requireGetCallbackDoesNotBlock: the callback completes without anyone consuming the result.</summary>
    private static async Task<GetResult> RequireGetCallbackDoesNotBlock(Action callback, ValueTask<GetResult> result)
    {
        var done = Task.Run(callback);
        var finished = await Task.WhenAny(done, Task.Delay(TimeSpan.FromSeconds(1)));
        if (finished == done)
        {
            await done;
            return await result;
        }

        // Consume the result to release the callback before failing the test.
        await result;
        await done;
        Assert.Fail("get callback blocked until the result was consumed");
        return default;
    }

    private static async Task AssertStreamTasksEnd(AsyncClientImpl client, string function)
    {
        await F2Util.EventuallyAsync(() => client.RunningStreamTasks == 0, TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(10),
            $"a task started by {function} did not end");
    }

    [Fact]
    public async Task TestGetCallbackDoesNotBlockWhenSingleShardResultIsAbandoned()
    {
        var (client, batcher) = NewGetTestClient(1);
        var resultCh = client.GetAsync("key-a");
        var call = await batcher.TakeAsync();

        var result = await RequireGetCallbackDoesNotBlock(
            () => call.Complete(new GetResponse { Status = Status.KeyNotFound }, null), resultCh);

        Assert.True(OxiaErrors.Is(result.Err, ClientErrors.KeyNotFound));
    }

    [Fact]
    public async Task TestGetCallbackDoesNotBlockWhenMultiShardResultIsAbandoned()
    {
        var (client, batcher) = NewGetTestClient(1, 2);
        var resultCh = client.GetAsync("key-a", OxiaOptions.ComparisonFloor());
        var firstCall = await batcher.TakeAsync();
        var secondCall = await batcher.TakeAsync();

        firstCall.Complete(new GetResponse { Status = Status.KeyNotFound }, null);
        var result = await RequireGetCallbackDoesNotBlock(
            () => secondCall.Complete(new GetResponse { Status = Status.KeyNotFound }, null), resultCh);

        Assert.True(OxiaErrors.Is(result.Err, ClientErrors.KeyNotFound));
    }

    // The first shard error terminates the result channel; the responses of the remaining shards -- errors included -- must be
    // discarded, not panic with a send on the closed channel (the error path used to fall through and leave the counter
    // sentinel negative, defeating the response-already-sent guard).
    [Fact]
    public async Task TestMultiShardGetCallback_ErrorsAfterFirstAreDiscarded()
    {
        var callback = AsyncClientImpl.MultiShardGetCallback("key-a", KeyComparisonType.Floor, default, 3);
        var result = callback.AsValueTask();

        callback.Callback(null, new Exception("shard-0 failed"));
        Assert.NotNull((await result).Err);

        // does not throw
        callback.Callback(null, new Exception("shard-1 failed"));
        callback.Callback(new GetResponse { Status = Status.KeyNotFound }, null);

        // The result was delivered exactly once (Go: the channel was closed exactly once, after the single error result)
        Assert.Equal(1, callback.Delivered);
    }

    [Fact]
    public async Task TestMultiShardGetCallback_AllShardsRespond()
    {
        var callback = AsyncClientImpl.MultiShardGetCallback("key-a", KeyComparisonType.Floor, default, 3);
        var result = callback.AsValueTask();

        for (int i = 0; i < 3; i++)
        {
            callback.Callback(new GetResponse { Status = Status.KeyNotFound }, null);
        }

        Assert.True(OxiaErrors.Is((await result).Err, ClientErrors.KeyNotFound));
        Assert.Equal(1, callback.Delivered);
    }

    // A get with an index goes to every shard, and the client picks one of their answers by comparing the secondary keys in
    // the key sorting of the namespace.
    [Theory]
    // Natural sorting: "a/y/z" < "a0"
    [InlineData(KeySorting.Natural, "floor", "p1")]
    [InlineData(KeySorting.Natural, "ceiling", "p2")]
    // Hierarchical sorting: "a0" < "a/y/z"
    [InlineData(KeySorting.Hierarchical, "floor", "p2")]
    [InlineData(KeySorting.Hierarchical, "ceiling", "p1")]
    // The key sorting is not known: "a0" < "a/y/z" in the old order
    [InlineData(KeySorting.Unknown, "floor", "p2")]
    [InlineData(KeySorting.Unknown, "ceiling", "p1")]
    public async Task TestMultiShardIndexGetKeySorting(KeySorting keySorting, string comparison, string expected)
    {
        GetResponse[] responses =
        [
            new() { Status = Status.Ok, Version = new ProtoVersion(), Key = "p1", SecondaryIndexKey = "a0" },
            new() { Status = Status.Ok, Version = new ProtoVersion(), Key = "p2", SecondaryIndexKey = "a/y/z" },
        ];
        var comparisonOption = comparison == "floor" ? OxiaOptions.ComparisonFloor() : OxiaOptions.ComparisonCeiling();

        var (client, batcher) = NewGetTestClient(1, 2);
        ((StaticShardManager)client.ShardManager).Sorting = keySorting;
        var resultCh = client.GetAsync("b", comparisonOption, OxiaOptions.UseIndex("idx"));
        foreach (var response in responses)
        {
            var call = await batcher.TakeAsync();
            call.Complete(response, null);
        }

        var result = await resultCh;
        Assert.Null(result.Err);
        Assert.Equal(expected, result.Key);
    }

    [Fact]
    public void TestRerouteDeleteRangeToSuccessors()
    {
        // Shard 0 was split into shards 3 and 4, shards 1 and 2 are unrelated
        var (client, batchers) = NewWriteTestClient(new StaticShardManager
        {
            Shards = [3, 4, 1, 2],
            Successors = new Dictionary<long, long[]> { [0] = [3, 4] },
        });

        int callbacks = 0;
        Exception? result = null;
        var put = new PutCall { Key = "a", OpIndex = 0 };
        var deleteRange = new DeleteRangeCall
        {
            MinKeyInclusive = "a",
            MaxKeyExclusive = "z",
            OpIndex = 1,
            Callback = (_, err) =>
            {
                callbacks++;
                result = err;
            },
        };
        var del = new DeleteCall { Key = "b", OpIndex = 2 };
        client.RerouteWrites(0, [put], [del], [deleteRange]);

        Assert.DoesNotContain(1L, batchers.Keys);
        Assert.DoesNotContain(2L, batchers.Keys);
        Assert.Contains(3L, batchers.Keys);
        var onShard3 = batchers[3].Calls;
        Assert.Equal(3, onShard3.Count);
        Assert.IsType<PutCall>(onShard3[0]);
        Assert.IsType<DeleteRangeCall>(onShard3[1]);
        Assert.IsType<DeleteCall>(onShard3[2]);
        var onShard4 = DeleteRangeCallAt(batchers, 4);

        // The callback is invoked once, after both successors responded
        ((DeleteRangeCall)onShard3[1]).Complete(DeleteRangeOK, null);
        Assert.Equal(0, callbacks);
        onShard4.Complete(DeleteRangeOK, null);
        Assert.Equal(1, callbacks);
        Assert.Null(result);
    }

    [Fact]
    public async Task TestRerouteDeleteRangeWithPartitionKey()
    {
        var shardManager = new StaticShardManager { Shards = [0, 1] };
        var (client, batchers) = NewWriteTestClient(shardManager);
        var resultCh = client.DeleteRangeAsync("a", "z", OxiaOptions.PartitionKey("pk"));
        var call = DeleteRangeCallAt(batchers, 0);

        // Shard 0 was split into shards 2 and 3, and the partition key is now on shard 2
        shardManager.Shards = [2, 3, 1];
        shardManager.Successors = new Dictionary<long, long[]> { [0] = [2, 3] };
        client.RerouteWrites(0, [], [], [call]);

        var rerouted = DeleteRangeCallAt(batchers, 2);
        Assert.DoesNotContain(3L, batchers.Keys);
        Assert.DoesNotContain(1L, batchers.Keys);

        rerouted.Complete(DeleteRangeOK, null);
        Assert.Null(await resultCh);
    }

    [Fact]
    public async Task TestRerouteDeleteRangeWaitsForAllSuccessors()
    {
        var shardManager = new StaticShardManager { Shards = [0, 1] };
        var (client, batchers) = NewWriteTestClient(shardManager);
        var resultCh = client.DeleteRangeAsync("a", "z");
        var call = DeleteRangeCallAt(batchers, 0);
        var onShard1 = DeleteRangeCallAt(batchers, 1);

        // Shard 0 was split into shards 2 and 3. Shard 1 already got its own copy.
        shardManager.Shards = [1, 2, 3];
        shardManager.Successors = new Dictionary<long, long[]> { [0] = [2, 3] };
        client.RerouteWrites(0, [], [], [call]);

        Assert.Single(batchers[1].Calls);
        var onShard2 = DeleteRangeCallAt(batchers, 2);
        var onShard3 = DeleteRangeCallAt(batchers, 3);

        // The failure of the last successor is reported, not the success of the others
        var failure = new Exception("shard-3 failed");
        onShard1.Complete(DeleteRangeOK, null);
        onShard2.Complete(DeleteRangeOK, null);
        onShard3.Complete(null, failure);
        Assert.True(OxiaErrors.Is(await resultCh, failure));
    }

    [Fact]
    public void TestRerouteDeleteRangeWithoutSuccessors()
    {
        var (client, batchers) = NewWriteTestClient(new StaticShardManager { Shards = [1] });

        var results = new List<Exception?>();
        client.RerouteWrites(0, [], [], [new DeleteRangeCall
        {
            MinKeyInclusive = "a",
            MaxKeyExclusive = "z",
            Callback = (_, err) => results.Add(err),
        }]);

        Assert.Empty(batchers);
        Assert.Single(results);
        Assert.True(OxiaErrors.Is(results[0], OxiaErrors.ShardNotFound));
    }

    [Fact]
    public void TestRerouteDeleteRangeWithPartitionKeyWithoutSuccessor()
    {
        var (client, batchers) = NewWriteTestClient(new StaticShardManager { Shards = [1] });
        const string partitionKey = "pk";

        var results = new List<Exception?>();
        client.RerouteWrites(0, [], [], [new DeleteRangeCall
        {
            MinKeyInclusive = "a",
            MaxKeyExclusive = "z",
            PartitionKey = partitionKey,
            Callback = (_, err) => results.Add(err),
        }]);

        Assert.Empty(batchers);
        Assert.Single(results);
        Assert.True(OxiaErrors.Is(results[0], OxiaErrors.ShardNotFound));
    }

    // The shard manager receives the shard assignments in the background, until it is closed.
    [Fact]
    public async Task TestCloseClosesShardManager()
    {
        var shardManager = new StaticShardManager();
        using var cts = new CancellationTokenSource();
        var rpcProvider = RpcProviderImpl.Create(cts.Token, "default", null, null, "localhost:6648", () => shardManager);
        var client = new AsyncClientImpl
        {
            ShardManager = shardManager,
            WriteBatchManager = new WriteManager(cts.Token, (_, _) => throw new InvalidOperationException(), (_, _) => { }),
            ReadBatchManager = new Manager(cts.Token, (_, _) => throw new InvalidOperationException()),
            Sessions = new Sessions(cts.Token, shardManager, rpcProvider, new ClientOptions()),
            RpcProvider = rpcProvider,
            Ctx = cts.Token,
            CtxSource = cts,
        };

        await client.CloseAsync();
        Assert.True(shardManager.Closed);
    }

    [Theory]
    [InlineData("all-ok")]
    [InlineData("first-error-wins")]
    [InlineData("failed-status")]
    public void TestMultiShardDeleteRangeCallback(string name)
    {
        var failure = new Exception("failure");
        var notFound = new DeleteRangeResponse { Status = Status.KeyNotFound };

        DeleteRangeResponse?[] responses;
        Exception?[] errs;
        DeleteRangeResponse? expectedResponse;
        Exception? expectedErr;
        switch (name)
        {
            case "all-ok":
                responses = [DeleteRangeOK, DeleteRangeOK, DeleteRangeOK];
                errs = [null, null, null];
                expectedResponse = DeleteRangeOK;
                expectedErr = null;
                break;
            case "first-error-wins":
                responses = [DeleteRangeOK, null, null];
                errs = [null, failure, new Exception("other failure")];
                expectedResponse = null;
                expectedErr = failure;
                break;
            case "failed-status":
                responses = [notFound, DeleteRangeOK, DeleteRangeOK];
                errs = [null, null, null];
                expectedResponse = notFound;
                expectedErr = null;
                break;
            default:
                throw new ArgumentException(name);
        }

        var gotResponses = new List<DeleteRangeResponse?>();
        var gotErrs = new List<Exception?>();
        var callback = AsyncClientImpl.MultiShardDeleteRangeCallback(3, (response, err) =>
        {
            gotResponses.Add(response);
            gotErrs.Add(err);
        });

        for (int i = 0; i < responses.Length; i++)
        {
            callback(responses[i], errs[i]);
        }

        Assert.Equal(new[] { expectedResponse }, gotResponses);
        Assert.Equal(new[] { expectedErr }, gotErrs);
    }

    // Keys are proto3 strings, which must be valid UTF-8. The server doesn't check it, so the client doesn't send a write of a
    // key, or a range bound, that isn't.   (A .NET string cannot hold invalid UTF-8: lone surrogates are the invalid input.)
    [Fact]
    public async Task TestWriteKeysMustBeValidUTF8()
    {
        var (client, batchers) = NewWriteTestClient(new StaticShardManager { Shards = [0, 1] });

        // Stored on "__oxia/term" with the natural key sorting, and on "a//" with the hierarchical one. The results are there
        // as soon as the calls return: nothing was sent.
        foreach (var key in new[] { "\uD800\uD800oxia/term", "a\uDC00/" })
        {
            ValueTask<PutResult>[] puts =
            [
                client.PutAsync(key, V),
                client.PutAsync(key, V, OxiaOptions.PartitionKey("pk"), OxiaOptions.SequenceKeysDeltas(1)),
            ];
            foreach (var putCh in puts)
            {
                Assert.True(putCh.IsCompletedSuccessfully, key);
                Assert.True(OxiaErrors.Is((await putCh).Err, ClientErrors.InvalidOptions), key);
            }
            ValueTask<Exception?>[] errs =
            [
                client.DeleteAsync(key),
                client.DeleteRangeAsync(key, "z"),
                client.DeleteRangeAsync("", key),
                client.DeleteRangeAsync(key, "z", OxiaOptions.PartitionKey("pk")),
            ];
            foreach (var errCh in errs)
            {
                Assert.True(errCh.IsCompletedSuccessfully, key);
                Assert.True(OxiaErrors.Is(await errCh, ClientErrors.InvalidOptions), key);
            }
        }
        Assert.Empty(batchers);

        // A valid UTF-8 key is sent, whatever its characters
        _ = client.PutAsync("ключ/日本", V);
        _ = client.DeleteAsync("ключ/日本");
        _ = client.DeleteRangeAsync("ключ", "ключ/日本");
        Assert.Contains(0L, batchers.Keys);
        Assert.Equal(3, batchers[0].Calls.Count);
        Assert.Contains(1L, batchers.Keys);
        Assert.Single(batchers[1].Calls);
    }

    [Fact]
    public async Task TestNewAsyncClientFailsOnInvalidOptions()
    {
        IAsyncClient? client = null;
        var err = await Assert.ThrowsAnyAsync<Exception>(async () =>
            client = await OxiaClient.CreateAsyncClientAsync("localhost:0", OxiaOptions.WithRequestTimeout(TimeSpan.Zero)));
        Assert.True(OxiaErrors.Is(err, ClientErrors.InvalidOptionRequestTimeout));
        Assert.Null(client);
    }

    // A write routed with the shard map from before a split, to a shard that never had a write, goes to the shard that took
    // over its key.
    [Fact]
    public async Task TestAsyncClientPutForwardedFromRemovedShard()
    {
        var shardManager = new StaticShardManager
        {
            Shards = [0],
            Successors = new Dictionary<long, long[]> { [0] = [3] },
        };
        var (client, batchers) = NewWriteTestClient(shardManager);
        client.WriteBatchManager.ShardsReplaced(new Dictionary<long, long[]> { [0] = [3] });

        var resultCh = client.PutAsync("a", V);

        Assert.DoesNotContain(0L, batchers.Keys);
        Assert.Contains(3L, batchers.Keys);
        var calls = batchers[3].Calls;
        Assert.Single(calls);
        var put = Assert.IsAssignableFrom<PutCall>(calls[0]);
        Assert.Equal("a", put.Key);
        put.Complete(new PutResponse { Status = Status.Ok, Version = new ProtoVersion() }, null);
        var result = await resultCh;
        Assert.Null(result.Err);
        Assert.Equal("a", result.Key);
    }

    // A write routed to a removed shard that no shard replaced for its key fails.
    [Fact]
    public async Task TestAsyncClientPutToRemovedShardWithoutSuccessor()
    {
        var (client, batchers) = NewWriteTestClient(new StaticShardManager { Shards = [0] });
        client.WriteBatchManager.ShardsReplaced(new Dictionary<long, long[]> { [0] = [3] });

        var result = await client.PutAsync("a", V);
        Assert.True(OxiaErrors.Is(result.Err, OxiaErrors.ShardNotFound));
        Assert.Empty(batchers);
    }

    [Fact]
    public void TestAsyncClientForwardWritePanicsOnRead()
    {
        var (client, _) = NewWriteTestClient(new StaticShardManager { Shards = [0] });
        // Go: PanicsWithValue("invalid call")
        var ex = Assert.Throws<InvalidOperationException>(() => client.ForwardWrite(0, new GetCall { Key = "a" }));
        Assert.Equal("invalid call", ex.Message);
    }

    // An ephemeral write fails when the session can't be established.
    [Fact]
    public async Task TestAsyncClientEphemeralPutFailsWithoutSession()
    {
        var shardManager = new StaticShardManager { Shards = [0] };
        var (client, batchers) = NewWriteTestClient(shardManager);
        using var expired = F2Util.Expired();
        client.Sessions = new Sessions(expired.Token, shardManager, new SessionsTestRpcProvider(),
            new ClientOptions { RequestTimeout = TimeSpan.FromSeconds(1) })
        {
            // An expired context.Context: context.DeadlineExceeded (a token cannot tell it from a cancellation)
            ContextError = new TimeoutException("context deadline exceeded"),
        };

        var result = await client.PutAsync("a", V, OxiaOptions.Ephemeral());
        Assert.True(OxiaErrors.IsDeadlineExceeded(result.Err));
        Assert.Empty(batchers);
    }

    [Fact]
    public async Task TestAsyncClientDeleteRangeFailsOnStatus()
    {
        var (client, batchers) = NewWriteTestClient(new StaticShardManager { Shards = [0, 1] });
        var resultCh = client.DeleteRangeAsync("a", "z");

        DeleteRangeCallAt(batchers, 0).Complete(DeleteRangeOK, null);
        DeleteRangeCallAt(batchers, 1).Complete(new DeleteRangeResponse { Status = Status.SessionDoesNotExist }, null);
        Assert.True(OxiaErrors.Is(await resultCh, ClientErrors.UnknownStatus));
    }

    [Fact]
    public async Task TestAsyncClientDeleteRangeWithPartitionKeyFails()
    {
        var failure = new Exception("failure");
        var (client, batchers) = NewWriteTestClient(new StaticShardManager { Shards = [0, 1] });
        var resultCh = client.DeleteRangeAsync("a", "z", OxiaOptions.PartitionKey("pk"));

        DeleteRangeCallAt(batchers, 0).Complete(null, failure);
        Assert.True(OxiaErrors.Is(await resultCh, failure));
        Assert.DoesNotContain(1L, batchers.Keys);
    }

    // The reads of a removed shard are sent again with the current shard map.
    [Fact]
    public async Task TestAsyncClientRerouteReads()
    {
        // The batcher of the test client holds a call per shard
        var (client, batcher) = NewGetTestClient(2, 3);
        const string partitionKey = "pk";

        client.RerouteReads([new GetCall { Key = "a" }, new GetCall { Key = "b", PartitionKey = partitionKey }]);

        Assert.Equal("a", (await batcher.TakeAsync()).Key);
        Assert.Equal("b", (await batcher.TakeAsync()).Key);
    }

    [Fact]
    public async Task TestAsyncClientGetFailsOnUnsupportedComparison()
    {
        var (client, batcher) = NewGetTestClient(1, 2);

        var result = await client.GetAsync("a", OptionsInternal.ComparisonType((KeyComparisonType)100));
        Assert.NotNull(result.Err);
        Assert.Contains("unsupported comparison type", result.Err.Message);
        Assert.False(batcher.Calls.Reader.TryRead(out _));
    }

    [Fact]
    public void TestSelectResponsePanicsOnUnknownComparison()
    {
        // Go: PanicsWithValue("unknown comparison type: 100")
        var ex = Assert.Throws<InvalidOperationException>(() =>
            AsyncClientImpl.SelectResponse((KeyComparisonType)100, KeyOrder.New(KeySorting.Natural),
                AsyncClientImpl.KeyNotFoundResponse, new GetResponse { Status = Status.Ok }));
        Assert.Equal("unknown comparison type: 100", ex.Message);
    }

    [Fact]
    public async Task TestAsyncClientGetNotificationsFails()
    {
        var provider = new NotificationsTestRpcProvider { Err = OxiaErrors.NotificationsNotEnabled };
        provider.Failures = 1;
        var client = new AsyncClientImpl
        {
            Options = new ClientOptions { RequestTimeout = TimeSpan.FromSeconds(10) },
            ShardManager = new NotificationsTestShardManager { Shards = [0] },
            RpcProvider = provider,
        };

        var err = await Assert.ThrowsAnyAsync<Exception>(async () => await client.GetNotificationsAsync());
        Assert.True(OxiaErrors.Is(err, OxiaErrors.NotificationsNotEnabled));
        Assert.Empty(client.Notifications);
    }

    // A caller of List can stop reading once it cancelled the context, e.g. at the first error: the lists of the other shards
    // end without sending the results they have left, and the channel is closed after them.
    [Fact]
    public async Task TestAsyncClientListEndsOnceItsContextIsDone()
    {
        var failure = new Exception("shard unavailable");
        var client = StaticExecutor.NewScanTestClient(new StaticExecutor
        {
            Errs = new Dictionary<long, Exception> { [1] = failure, [2] = failure, [3] = failure, [4] = failure },
        }, 1, 2, 3, 4);

        using var cts = new CancellationTokenSource();
        var ch = client.ListAsync(cts.Token, "a", "z");
        Assert.True(OxiaErrors.Is((await F2Util.ReadAsync(ch)).Err, failure));

        cts.Cancel();
        await AssertStreamTasksEnd(client, "(*clientImpl).List");

        // Go: `_, open := <-ch; assert.False(open)`.  The unbuffered channel is a bounded(1) channel here: a result that was
        // already buffered when the context was cancelled may still be read, then the channel is closed.
        int remaining = 0;
        using var drain = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (await ch.WaitToReadAsync(drain.Token))
        {
            while (ch.TryRead(out _)) remaining++;
        }
        Assert.True(remaining <= 1, $"{remaining} results remained after the context was done");
    }

    // A range scan across the shards ends at the first error: the scans of the other shards end with it, instead of waiting
    // forever to send the records they have left, or streaming them until the end.
    [Fact]
    public async Task TestAsyncClientRangeScanEndsTheOtherShardsAtTheFirstError()
    {
        var failure = new Exception("shard unavailable");
        var client = StaticExecutor.NewScanTestClient(new StaticExecutor
        {
            Keys = new Dictionary<long, string[]> { [2] = ["a", "b"], [3] = ["c"] },
            Errs = new Dictionary<long, Exception> { [1] = failure },
            Slow = [3],
        }, 1, 2, 3);

        var results = new List<GetResult>();
        await foreach (var result in client.RangeScanAsync(CancellationToken.None, "a", "z").ReadAllAsync())
        {
            results.Add(result);
        }
        Assert.Single(results);
        Assert.True(OxiaErrors.Is(results[0].Err, failure));
        await AssertStreamTasksEnd(client, "(*clientImpl).RangeScan");
    }

    // A list with EndInclusive fails with ErrEndInclusiveUnsupported unless the leader of every shard includes the end of the
    // range, which it says in all its responses, sending one at least: older servers ignore EndInclusive, and leave the end
    // out. The client drops the keys of a response that doesn't say so.
    [Theory]
    [InlineData("included")]
    [InlineData("included, empty shard")]
    [InlineData("older server")]
    [InlineData("older server, empty shard")]
    [InlineData("not requested")]
    public async Task TestAsyncClientListEndInclusive(string name)
    {
        var test = EndInclusiveTest.Get(name);
        var client = StaticExecutor.NewScanTestClient(test.Executor, 1, 2);
        var keys = new List<string>();
        Exception? err = null;
        ListOption[] options = test.Requested ? [OxiaOptions.EndInclusive()] : [];
        // The channel is closed once all the shards responded
        await foreach (var result in client.ListAsync(CancellationToken.None, "a", "z", options).ReadAllAsync())
        {
            if (result.Err is not null)
            {
                err = result.Err;
                continue;
            }
            keys.AddRange(result.Keys!);
        }
        if (test.Err is not null)
        {
            Assert.True(OxiaErrors.Is(err, test.Err));
            Assert.DoesNotContain("c", keys);
        }
        else
        {
            Assert.Null(err);
            Assert.Equal(test.Expected!.OrderBy(k => k, StringComparer.Ordinal), keys.OrderBy(k => k, StringComparer.Ordinal));
        }
    }

    // A range scan with EndInclusive fails like a list.
    [Theory]
    [InlineData("included")]
    [InlineData("included, empty shard")]
    [InlineData("older server")]
    [InlineData("older server, empty shard")]
    [InlineData("not requested")]
    public async Task TestAsyncClientRangeScanEndInclusive(string name)
    {
        var test = EndInclusiveTest.Get(name);
        var client = StaticExecutor.NewScanTestClient(test.Executor, 1, 2);
        RangeScanOption[] options = test.Requested ? [OxiaOptions.EndInclusive()] : [];
        var keys = new List<string>();
        Exception? err = null;
        await foreach (var result in client.RangeScanAsync(CancellationToken.None, "a", "z", options).ReadAllAsync())
        {
            if (result.Err is not null)
            {
                err = result.Err;
                continue;
            }
            keys.Add(result.Key);
        }
        if (test.Err is not null)
        {
            Assert.True(OxiaErrors.Is(err, test.Err));
            Assert.DoesNotContain("c", keys);
        }
        else
        {
            Assert.Null(err);
            Assert.Equal(test.Expected!, keys);
        }
    }

    // An older server ignores include_values and sends the values: the client drops them.
    [Fact]
    public async Task TestAsyncClientRangeScanWithoutValuesDropsValues()
    {
        var executor = new StaticExecutor { Keys = new Dictionary<long, string[]> { [1] = ["a"], [2] = ["b"] } };
        var client = StaticExecutor.NewScanTestClient(executor, 1, 2);

        var results = new List<GetResult>();
        await foreach (var result in client.RangeScanAsync(CancellationToken.None, "a", "z", OxiaOptions.IncludeValue(false)).ReadAllAsync())
        {
            Assert.Null(result.Err);
            results.Add(result);
        }
        Assert.Equal(2, results.Count);
        Assert.Equal("a", results[0].Key);
        Assert.Null(results[0].Value);
        Assert.Equal("b", results[1].Key);
        Assert.Null(results[1].Value);

        await foreach (var result in client.RangeScanAsync(CancellationToken.None, "a", "z").ReadAllAsync())
        {
            Assert.Null(result.Err);
            Assert.Equal(Encoding.UTF8.GetBytes(result.Key), result.Value);
        }
    }
}

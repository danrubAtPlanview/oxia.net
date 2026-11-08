// =====================================================================================================================
// PORT NOTE (extra tests -- no Go counterpart)
//   Upstream : oxia/internal/rpc_provider.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The logic of the rpc provider against FAKES of the pool / client / streams (no network, no wire codec):
//                * retry loop of the shard requests: what is retried, leader hints (and hints for another shard), unknown
//                  shard, an empty leader address, the first attempt running inline and entering the loop when it fails;
//                * list / range scan retry only while nothing was consumed (and the quirk that a retryable consumer error
//                  before the first verified response is retried);
//                * write streams: cached per shard, headers, replaced when the target changes (old one half-closed and marked
//                  failed) or when it failed, the failed request retried on the new stream with `prepare` invoked again;
//                * fixed target requests are executed once and their Oxia errors are translated;
//                * Close closes the pool and cancels the streams.
// =====================================================================================================================
using Grpc.Core;
using Oxia.Client.Internal;
using Oxia.Client.Tests.Internal.Fakes;
using Oxia.Common.Constant;
using Oxia.Common.Failpoint;
using Oxia.Common.Proto;
using Oxia.Common.Rpc;
using Xunit;

namespace Oxia.Client.Tests.Internal;

public class RpcProviderFakeTests
{
    private static CancellationToken TestCt => TestContext.Current.CancellationToken;

    private static RpcException Unavailable(string detail = "transport is closing") => new(new Grpc.Core.Status(StatusCode.Unavailable, detail));

    private static RpcException NotLeader(long shard, string leader) =>
        OxiaErrors.IntoGrpcStatusError(OxiaErrors.NodeIsNotLeader, OxiaErrors.WithLeaderHint(shard, leader))!;

    private sealed class Fixture
    {
        public FakeOxiaClientRpc Rpc { get; } = new();
        public FakeClientPool Pool { get; }
        public FakeShardManager Shards { get; } = new() { LeaderAddress = "leader:1" };
        public RpcProvider Provider { get; }

        public Fixture(Failpoints? failpoints = null, bool noShardManager = false)
        {
            Pool = new FakeClientPool(_ => Rpc);
            Provider = new RpcProvider(TestCt, "default", Pool, "service:6648", () => noShardManager ? null : Shards, failpoints);
        }
    }

    private static ReadRequest ReadOf(long? shard)
    {
        var request = new ReadRequest { Shard = shard };
        request.Gets.Add(new GetRequest { Key = "key" });
        return request;
    }

    private static WriteRequest WriteOf(long shard)
    {
        var request = new WriteRequest { Shard = shard };
        request.Puts.Add(new PutRequest { Key = "key", Value = "value"u8.ToArray() });
        return request;
    }

    private static ReadResponse ReadResponseOf(int gets)
    {
        var response = new ReadResponse();
        for (int i = 0; i < gets; i++) response.Gets.Add(new GetResponse { Status = Oxia.Common.Proto.Status.KeyNotFound });
        return response;
    }

    // ----- reads ----------------------------------------------------------------------------------------------------

    [Fact]
    public async Task ExecuteRead_ASingleResponseIsReturnedAsIs()
    {
        var f = new Fixture();
        var response = ReadResponseOf(2);
        f.Rpc.OnRead = (_, _) => new FakeServerStream<ReadResponse>().Push(response).Complete();

        var result = await f.Provider.ExecuteRead(TestCt, ReadOf(0));

        Assert.Same(response, result);
        Assert.Equal(["leader:1"], f.Pool.Targets);
    }

    [Fact]
    public async Task ExecuteRead_SeveralResponsesAreAggregatedInOrder()
    {
        var f = new Fixture();
        var r1 = ReadResponseOf(1);
        var r2 = ReadResponseOf(2);
        var r3 = ReadResponseOf(1);
        f.Rpc.OnRead = (_, _) => new FakeServerStream<ReadResponse>().Push(r1).Push(r2).Push(r3).Complete();

        var result = await f.Provider.ExecuteRead(TestCt, ReadOf(0));

        Assert.Equal(4, result.Gets.Count);
        Assert.Same(r1.Gets[0], result.Gets[0]);
        Assert.Same(r2.Gets[0], result.Gets[1]);
        Assert.Same(r2.Gets[1], result.Gets[2]);
        Assert.Same(r3.Gets[0], result.Gets[3]);
    }

    [Fact]
    public async Task ExecuteRead_AnEmptyStreamIsAnEmptyResponse()
    {
        var f = new Fixture();
        f.Rpc.OnRead = (_, _) => new FakeServerStream<ReadResponse>().Complete();
        var result = await f.Provider.ExecuteRead(TestCt, ReadOf(0));
        Assert.Empty(result.Gets);
    }

    [Fact]
    public async Task ExecuteRead_WithoutShardGoesToTheServiceAddressAndNeverAsksTheShardManager()
    {
        var f = new Fixture(noShardManager: true);
        f.Rpc.OnRead = (_, _) => new FakeServerStream<ReadResponse>().Push(ReadResponseOf(1)).Complete();
        await f.Provider.ExecuteRead(TestCt, ReadOf(null));
        Assert.Equal(["service:6648"], f.Pool.Targets);
    }

    [Fact]
    public async Task ExecuteRead_RetriesAnUnavailableStreamAndAReadFailureAfterTheFirstResponse()
    {
        var f = new Fixture();
        int calls = 0;
        f.Rpc.OnRead = (_, _) =>
        {
            int call = Interlocked.Increment(ref calls);
            var stream = new FakeServerStream<ReadResponse>();
            if (call == 1) return stream.Fail(Unavailable());
            if (call == 2) return stream.Push(ReadResponseOf(1)).Fail(Unavailable("mid stream")); // some responses, then the failure: the read restarts
            return stream.Push(ReadResponseOf(3)).Complete();
        };

        var result = await f.Provider.ExecuteRead(TestCt, ReadOf(0));

        Assert.Equal(3, calls);
        Assert.Equal(3, result.Gets.Count);
    }

    [Fact]
    public async Task ExecuteRead_ANonRetryableErrorIsNotRetried()
    {
        var f = new Fixture();
        int calls = 0;
        f.Rpc.OnRead = (_, _) =>
        {
            Interlocked.Increment(ref calls);
            return new FakeServerStream<ReadResponse>().Fail(new RpcException(new Grpc.Core.Status(StatusCode.Unknown, "boom")));
        };

        var err = await Assert.ThrowsAsync<RpcException>(async () => await f.Provider.ExecuteRead(TestCt, ReadOf(0)));
        Assert.Equal(StatusCode.Unknown, err.StatusCode);
        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task ExecuteRead_AStreamThatEndsWithEofIsRetried()
    {
        var f = new Fixture();
        int calls = 0;
        f.Rpc.OnRead = (_, _) =>
        {
            int call = Interlocked.Increment(ref calls);
            var stream = new FakeServerStream<ReadResponse>();
            return call == 1 ? stream.Fail(OxiaErrors.EndOfStream) : stream.Push(ReadResponseOf(1)).Complete();
        };

        var result = await f.Provider.ExecuteRead(TestCt, ReadOf(0));
        Assert.Single(result.Gets);
        Assert.Equal(2, calls);
    }

    [Fact]
    public async Task ExecuteRead_FollowsTheLeaderHintOnlyForTheNextAttempt()
    {
        var f = new Fixture();
        f.Shards.LeaderAddress = "old:1";
        int calls = 0;
        f.Rpc.OnRead = (_, _) =>
        {
            int call = Interlocked.Increment(ref calls);
            var stream = new FakeServerStream<ReadResponse>();
            switch (call)
            {
                case 1: return stream.Fail(NotLeader(0, "hinted:2"));       // the node is not the leader and says who is
                case 2:
                    f.Shards.LeaderAddress = "new:3";                        // the hinted node is unreachable, no new hint
                    return stream.Fail(Unavailable("connection refused"));
                default: return stream.Push(ReadResponseOf(1)).Complete();
            }
        };

        await f.Provider.ExecuteRead(TestCt, ReadOf(0));

        // hint -> hinted leader; the failure without a hint -> back to the shard map, which now knows the new leader
        Assert.Equal(["old:1", "hinted:2", "new:3"], f.Pool.Targets);
    }

    [Fact]
    public async Task ExecuteRead_ALeaderHintForAnotherShardIsIgnored()
    {
        var f = new Fixture();
        f.Shards.LeaderAddress = "leader:1";
        int calls = 0;
        f.Rpc.OnRead = (_, _) =>
        {
            int call = Interlocked.Increment(ref calls);
            var stream = new FakeServerStream<ReadResponse>();
            return call == 1 ? stream.Fail(NotLeader(7, "other-shard-leader:9")) : stream.Push(ReadResponseOf(1)).Complete();
        };

        await f.Provider.ExecuteRead(TestCt, ReadOf(0));

        Assert.Equal(["leader:1", "leader:1"], f.Pool.Targets);
    }

    [Fact]
    public async Task ExecuteRead_AnUnknownShardFailsRightAwayWithoutConnecting()
    {
        var f = new Fixture();
        f.Shards.ExistsFunc = static _ => false;

        var err = await Assert.ThrowsAnyAsync<Exception>(async () => await f.Provider.ExecuteRead(TestCt, ReadOf(0)));

        Assert.True(OxiaErrors.Is(err, OxiaErrors.ShardNotFound));
        Assert.Empty(f.Pool.Targets);
    }

    [Fact]
    public async Task ExecuteRead_AShardWithoutLeaderIsRetriedUntilTheLeaderIsKnown()
    {
        var f = new Fixture();
        f.Shards.LeaderAddress = "";
        f.Rpc.OnRead = (_, _) => new FakeServerStream<ReadResponse>().Push(ReadResponseOf(1)).Complete();
        _ = Task.Run(async () =>
        {
            await Task.Delay(250);
            f.Shards.LeaderAddress = "leader:1";
        }, TestCt);

        await f.Provider.ExecuteRead(TestCt, ReadOf(0));

        Assert.DoesNotContain("", f.Pool.Targets); // nothing connects to the empty address
        Assert.Equal(["leader:1"], f.Pool.Targets);
    }

    [Fact]
    public async Task ExecuteRead_ACancelledTokenEndsTheRetriesWithTheCancellation()
    {
        var f = new Fixture();
        f.Rpc.OnRead = (_, _) => new FakeServerStream<ReadResponse>().Fail(Unavailable());
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(250));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await f.Provider.ExecuteRead(cts.Token, ReadOf(0)));
        Assert.True(f.Pool.Targets.Count >= 2);
    }

    // ----- list / range scan ----------------------------------------------------------------------------------------

    [Fact]
    public async Task ExecuteList_IsRetriedUntilTheFirstResponseIsConsumed_ThenFailsWithoutRetry()
    {
        var f = new Fixture();
        int calls = 0;
        f.Rpc.OnList = (_, _) =>
        {
            int call = Interlocked.Increment(ref calls);
            var stream = new FakeServerStream<ListResponse>();
            if (call == 1) return stream.Fail(Unavailable());
            var response = new ListResponse();
            response.Keys.Add("a");
            return stream.Push(response).Fail(Unavailable("mid stream")); // after a consumed response: not retried
        };

        var consumed = new List<string>();
        var err = await Assert.ThrowsAsync<RpcException>(async () => await f.Provider.ExecuteList(TestCt, new ListRequest { Shard = 0 }, r =>
        {
            consumed.AddRange(r.Keys);
            return null;
        }));

        Assert.Equal(StatusCode.Unavailable, err.StatusCode);
        Assert.Equal(2, calls);
        Assert.Equal(["a"], consumed);
    }

    [Fact]
    public async Task ExecuteRangeScan_IsRetriedUntilTheFirstResponseIsConsumed_ThenFailsWithoutRetry()
    {
        var f = new Fixture();
        int calls = 0;
        f.Rpc.OnRangeScan = (_, _) =>
        {
            int call = Interlocked.Increment(ref calls);
            var stream = new FakeServerStream<RangeScanResponse>();
            if (call == 1) return stream.Fail(Unavailable());
            var response = new RangeScanResponse();
            response.Records.Add(new GetResponse { Key = "a" });
            return stream.Push(response).Fail(Unavailable("mid stream"));
        };

        int consumed = 0;
        var err = await Assert.ThrowsAsync<RpcException>(async () => await f.Provider.ExecuteRangeScan(TestCt, new RangeScanRequest { Shard = 0 }, _ =>
        {
            consumed++;
            return null;
        }));

        Assert.Equal(StatusCode.Unavailable, err.StatusCode);
        Assert.Equal(2, calls);
        Assert.Equal(1, consumed);
    }

    [Fact]
    public async Task ExecuteList_ARetryableConsumerErrorBeforeTheFirstVerifiedResponseIsRetried()
    {
        // Go quirk kept: `verified` is set only after the consumer accepted a response, so a consumer that rejects the very first
        // response with a retryable error makes the list start over.
        var f = new Fixture();
        int calls = 0;
        f.Rpc.OnList = (_, _) =>
        {
            Interlocked.Increment(ref calls);
            var response = new ListResponse();
            response.Keys.Add("k");
            return new FakeServerStream<ListResponse>().Push(response).Complete();
        };

        int consumed = 0;
        await f.Provider.ExecuteList(TestCt, new ListRequest { Shard = 0 }, _ => ++consumed == 1 ? OxiaErrors.NodeIsNotLeader : null);

        Assert.Equal(2, calls);
        Assert.Equal(2, consumed);
    }

    [Fact]
    public async Task ExecuteList_AConsumerErrorThatIsNotRetryableIsThrownAsIs()
    {
        var f = new Fixture();
        f.Rpc.OnList = (_, _) =>
        {
            var response = new ListResponse();
            response.Keys.Add("k");
            return new FakeServerStream<ListResponse>().Push(response).Push(response).Complete();
        };
        var failure = new InvalidOperationException("stop");

        var err = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await f.Provider.ExecuteList(TestCt, new ListRequest { Shard = 0 }, _ => failure));
        Assert.Same(failure, err);
    }

    [Fact]
    public async Task ExecuteList_TheStreamIsDisposedWhenTheListEnds()
    {
        var f = new Fixture();
        var stream = new FakeServerStream<ListResponse>().Push(new ListResponse()).Complete();
        f.Rpc.OnList = (_, _) => stream;
        await f.Provider.ExecuteList(TestCt, new ListRequest { Shard = 0 }, static _ => null);
        Assert.True(stream.Disposed);
    }

    // ----- writes ---------------------------------------------------------------------------------------------------

    [Fact]
    public async Task ExecuteWrite_TheStreamOfAShardIsCachedAndCarriesTheHeaders()
    {
        var f = new Fixture();
        var streams = new List<FakeWriteStream>();
        Metadata? headers = null;
        f.Rpc.OnWriteStream = (h, ct) =>
        {
            headers = h;
            var stream = new FakeWriteStream(ct, autoRespond: true);
            streams.Add(stream);
            return stream;
        };

        await f.Provider.ExecuteWrite(TestCt, 3, () => WriteOf(3));
        await f.Provider.ExecuteWrite(TestCt, 3, () => WriteOf(3));
        await f.Provider.ExecuteWrite(TestCt, 4, () => WriteOf(4)); // another shard: another stream

        Assert.Equal(2, streams.Count);
        Assert.Equal(2, streams[0].Sent);
        Assert.Equal(1, streams[1].Sent);
        Assert.Equal("default", headers!.GetValue("namespace"));
        Assert.Equal("4", headers.GetValue("shard-id"));
        Assert.Equal(["leader:1", "leader:1"], f.Pool.Targets); // connects only to create a stream
    }

    [Fact]
    public async Task ExecuteWrite_ATargetChangeReplacesAndHalfClosesTheOldStream()
    {
        var f = new Fixture();
        var streams = new List<FakeWriteStream>();
        f.Rpc.OnWriteStream = (_, ct) =>
        {
            var stream = new FakeWriteStream(ct, autoRespond: true);
            streams.Add(stream);
            return stream;
        };

        await f.Provider.ExecuteWrite(TestCt, 0, () => WriteOf(0));
        f.Shards.LeaderAddress = "leader:2";
        await f.Provider.ExecuteWrite(TestCt, 0, () => WriteOf(0));
        await f.Provider.ExecuteWrite(TestCt, 0, () => WriteOf(0));

        Assert.Equal(2, streams.Count);
        Assert.Equal(1, streams[0].Sent);
        Assert.Equal(2, streams[1].Sent);
        Assert.Equal(1, streams[0].CloseSendCalls);
        Assert.Equal(0, streams[1].CloseSendCalls);
        Assert.Equal(["leader:1", "leader:2"], f.Pool.Targets);
    }

    [Fact]
    public async Task ExecuteWrite_AFailedStreamIsReplacedAndTheRequestRetriedWithPrepareInvokedAgain()
    {
        var f = new Fixture();
        var streams = new List<FakeWriteStream>();
        f.Rpc.OnWriteStream = (_, ct) =>
        {
            var stream = new FakeWriteStream(ct, autoRespond: streams.Count > 0);
            // the first stream dies while the request is in flight
            if (streams.Count == 0) stream.OnSend = static (s, _) => s.FailStream(Unavailable());
            streams.Add(stream);
            return stream;
        };

        int prepared = 0;
        var response = await f.Provider.ExecuteWrite(TestCt, 0, () =>
        {
            Interlocked.Increment(ref prepared);
            return WriteOf(0);
        });

        Assert.NotNull(response);
        Assert.Equal(2, prepared);
        Assert.Equal(2, streams.Count);
        Assert.Equal(1, streams[1].Sent);
    }

    [Fact]
    public async Task ExecuteWrite_AStreamThatEndsIsRetriedLikeAnEof()
    {
        var f = new Fixture();
        int streamCount = 0;
        f.Rpc.OnWriteStream = (_, ct) =>
        {
            bool first = Interlocked.Increment(ref streamCount) == 1;
            var stream = new FakeWriteStream(ct, autoRespond: !first);
            if (first) stream.OnSend = static (s, _) => s.CompleteStream();
            return stream;
        };

        await f.Provider.ExecuteWrite(TestCt, 0, () => WriteOf(0));
        Assert.Equal(2, streamCount);
    }

    [Fact]
    public async Task ExecuteWrite_AFailureWhileSendingFailsTheStreamAndIsRetried()
    {
        var f = new Fixture();
        int streamCount = 0;
        f.Rpc.OnWriteStream = (_, ct) =>
        {
            bool first = Interlocked.Increment(ref streamCount) == 1;
            return new FakeWriteStream(ct, autoRespond: true) { SendError = first ? Unavailable("send failed") : null };
        };

        await f.Provider.ExecuteWrite(TestCt, 0, () => WriteOf(0));
        Assert.Equal(2, streamCount);
    }

    [Fact]
    public async Task ExecuteWrite_APrepareThatThrowsAbortsTheRequestWithoutRetry()
    {
        var f = new Fixture();
        FakeWriteStream? stream = null;
        f.Rpc.OnWriteStream = (_, ct) => stream = new FakeWriteStream(ct, autoRespond: true);
        int prepared = 0;

        var err = await Assert.ThrowsAsync<InvalidOperationException>(async () => await f.Provider.ExecuteWrite(TestCt, 0, () =>
        {
            Interlocked.Increment(ref prepared);
            throw new InvalidOperationException("nothing left to send");
        }));

        Assert.Equal("nothing left to send", err.Message);
        Assert.Equal(1, prepared);
        Assert.Equal(0, stream!.Sent);
    }

    [Fact]
    public async Task ExecuteWrite_AnUnknownShardFailsRightAwayAndAMissingShardManagerToo()
    {
        var f = new Fixture();
        f.Shards.ExistsFunc = static _ => false;
        var err = await Assert.ThrowsAnyAsync<Exception>(async () => await f.Provider.ExecuteWrite(TestCt, 0, () => WriteOf(0)));
        Assert.True(OxiaErrors.Is(err, OxiaErrors.ShardNotFound));

        var g = new Fixture(noShardManager: true);
        err = await Assert.ThrowsAnyAsync<Exception>(async () => await g.Provider.ExecuteWrite(TestCt, 0, () => WriteOf(0)));
        Assert.True(OxiaErrors.Is(err, OxiaErrors.ShardNotFound));
        Assert.Empty(f.Pool.Targets);
        Assert.Empty(g.Pool.Targets);
    }

    [Fact]
    public async Task ExecuteWrite_ACancelledTokenNeverCallsPrepare()
    {
        var f = new Fixture();
        f.Rpc.OnWriteStream = (_, ct) => new FakeWriteStream(ct, autoRespond: true);
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        int prepared = 0;

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await f.Provider.ExecuteWrite(cts.Token, 0, () =>
        {
            Interlocked.Increment(ref prepared);
            return WriteOf(0);
        }));

        Assert.Equal(0, prepared);
    }

    [Fact]
    public async Task ExecuteWrite_ADeadlineWhileWaitingForTheResponseKeepsTheStreamForTheNextRequest()
    {
        var f = new Fixture();
        var streams = new List<FakeWriteStream>();
        f.Rpc.OnWriteStream = (_, ct) =>
        {
            var stream = new FakeWriteStream(ct);
            streams.Add(stream);
            return stream;
        };

        using (var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100)))
        {
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await f.Provider.ExecuteWrite(cts.Token, 0, () => WriteOf(0)));
        }

        // The late response of the abandoned request is consumed (it keeps the FIFO aligned); the next request uses the same stream
        var second = f.Provider.ExecuteWrite(TestCt, 0, () => WriteOf(0)).AsTask();
        var late = new WriteResponse();
        var answer = new WriteResponse();
        answer.Puts.Add(new PutResponse());
        while (streams[0].Sent < 2) await Task.Delay(5, TestCt);
        streams[0].Respond(late);
        streams[0].Respond(answer);

        Assert.Same(answer, await second);
        Assert.Single(streams);
    }

    [Fact]
    public async Task ExecuteWrite_ClosingTheOldStreamFailsAndIsOnlyLogged_TheFailpointCanReplaceTheError()
    {
        int hits = 0;
        var failpoints = Failpoints.New(new Dictionary<string, Func<Exception?>>
        {
            ["getWriteStream.closeOldStream"] = () =>
            {
                Interlocked.Increment(ref hits);
                return null; // the hook has no error: the real one (if any) stays
            },
        });
        var f = new Fixture(failpoints);
        var streams = new List<FakeWriteStream>();
        f.Rpc.OnWriteStream = (_, ct) =>
        {
            var stream = new FakeWriteStream(ct, autoRespond: true) { CloseSendError = Unavailable("close failed") };
            streams.Add(stream);
            return stream;
        };

        await f.Provider.ExecuteWrite(TestCt, 0, () => WriteOf(0));
        f.Shards.LeaderAddress = "leader:2";
        await f.Provider.ExecuteWrite(TestCt, 0, () => WriteOf(0)); // the CloseSend failure is swallowed

        Assert.Equal(1, hits);
        Assert.Equal(1, streams[0].CloseSendCalls);
    }

    // ----- fixed target requests ------------------------------------------------------------------------------------

    [Fact]
    public async Task FixedTarget_RequestsAreExecutedOnceAndOxiaErrorsAreTranslated()
    {
        var f = new Fixture();
        int calls = 0;
        f.Rpc.OnKeepAlive = (_, _) =>
        {
            Interlocked.Increment(ref calls);
            throw NotLeader(0, "new-leader:6648");
        };

        var err = await Assert.ThrowsAnyAsync<Exception>(async () => await f.Provider.KeepAlive(TestCt, "target:1", new SessionHeartbeat()));

        Assert.True(OxiaErrors.Is(err, OxiaErrors.NodeIsNotLeader));
        Assert.Equal(1, calls);
        Assert.Equal(["target:1"], f.Pool.Targets);
    }

    [Fact]
    public async Task FixedTarget_ErrorsThatAreNotOxiaErrorsAreRethrownUnchanged()
    {
        var f = new Fixture();
        var failure = Unavailable("connection refused");
        f.Rpc.OnCreateSession = (_, _) => throw failure;
        f.Rpc.OnCloseSession = (_, _) => ValueTask.FromException<CloseSessionResponse>(failure);

        var err = await Assert.ThrowsAsync<RpcException>(async () => await f.Provider.CreateSession(TestCt, "target:1", new CreateSessionRequest()));
        Assert.Same(failure, err);
        err = await Assert.ThrowsAsync<RpcException>(async () => await f.Provider.CloseSession(TestCt, "target:1", new CloseSessionRequest()));
        Assert.Same(failure, err);
    }

    [Fact]
    public async Task FixedTarget_StreamsAreHandedToTheCaller()
    {
        var f = new Fixture();
        var assignments = new FakeServerStream<ShardAssignments>();
        var updates = new FakeServerStream<GetSequenceUpdatesResponse>();
        var notifications = new FakeServerStream<NotificationBatch>();
        CancellationToken seen = default;
        f.Rpc.OnGetShardAssignments = (r, ct) =>
        {
            seen = ct;
            Assert.Equal("default", r.Namespace);
            return assignments;
        };
        f.Rpc.OnGetSequenceUpdates = (_, _) => updates;
        f.Rpc.OnGetNotifications = (_, _) => notifications;
        using var cts = new CancellationTokenSource();

        Assert.Same(assignments, await f.Provider.GetShardAssignments(cts.Token, "t:1", new ShardAssignmentsRequest { Namespace = "default" }));
        Assert.Equal(cts.Token, seen);
        Assert.Same(updates, await f.Provider.GetSequenceUpdates(TestCt, "t:1", new GetSequenceUpdatesRequest()));
        Assert.Same(notifications, await f.Provider.GetNotifications(TestCt, "t:1", new NotificationsRequest()));
    }

    [Fact]
    public async Task FixedTarget_ConnectionFailuresAreTranslatedLikeAnyOtherError()
    {
        var f = new Fixture();
        f.Pool.ConnectError = OxiaErrors.IntoGrpcStatusError(OxiaErrors.ResourceUnavailable);
        var err = await Assert.ThrowsAnyAsync<Exception>(async () => await f.Provider.GetShardAssignments(TestCt, "t:1", new ShardAssignmentsRequest()));
        Assert.True(OxiaErrors.Is(err, OxiaErrors.ResourceUnavailable));
    }

    // ----- close ----------------------------------------------------------------------------------------------------

    [Fact]
    public async Task CloseAsync_ClosesThePoolAndCancelsTheOpenWriteStreams()
    {
        var f = new Fixture();
        FakeWriteStream? stream = null;
        f.Rpc.OnWriteStream = (_, ct) => stream = new FakeWriteStream(ct, autoRespond: true);
        await f.Provider.ExecuteWrite(TestCt, 0, () => WriteOf(0));
        Assert.False(stream!.Context.IsCancellationRequested);

        await f.Provider.CloseAsync();

        Assert.True(f.Pool.Closed);
        Assert.True(stream.Context.IsCancellationRequested);
    }

    [Fact]
    public async Task TheProviderTokenCancelsTheWriteStreamsAndNotTheRequestToken()
    {
        using var providerCts = new CancellationTokenSource();
        var rpc = new FakeOxiaClientRpc();
        var pool = new FakeClientPool(_ => rpc);
        var shards = new FakeShardManager { LeaderAddress = "leader:1" };
        var provider = new RpcProvider(providerCts.Token, "default", pool, "service:6648", () => shards);
        FakeWriteStream? stream = null;
        rpc.OnWriteStream = (_, ct) => stream = new FakeWriteStream(ct, autoRespond: true);

        // the request token is cancelled after the request: the stream outlives it
        using (var requestCts = new CancellationTokenSource())
        {
            await provider.ExecuteWrite(requestCts.Token, 0, () => WriteOf(0));
            await requestCts.CancelAsync();
        }

        Assert.False(stream!.Context.IsCancellationRequested);
        await providerCts.CancelAsync();
        Assert.True(stream.Context.IsCancellationRequested);
    }
}

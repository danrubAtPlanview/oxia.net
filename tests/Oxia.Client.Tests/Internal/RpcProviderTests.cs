// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/internal/rpc_provider_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests: 18, ported: 18, skipped: 0   (subtests -> [Theory] rows: the 4 shard requests write/read/list/range-scan, the 10
//             error kinds of ExecuteWithRetry_ShardRequestErrors, the 3 session requests, the 5 fixed-target requests)
//   Intent   : retry of the shard requests while the leader is unreachable / the hint is stale / the shard has no leader; what
//              is (not) retried; fail-fast of the fixed-target requests; the write stream goes to the new leader and its setup
//              honours the request deadline; ShardMapTimer fires early; list / range scan consumer errors are not retried.
//   Adaptations:
//      * These tests run against the REAL ClientPool / OxiaClientRpc over in-process Kestrel servers (TestGrpcServer) derived
//        from the generated OxiaClientBase, exactly like the Go tests run real gRPC servers: they need components A (wire
//        codec / OxiaClientRpc) and B (ClientPool, Retry, Future, BackOff).  The ones that never reach the network
//        (unknown shard, no leader, connection failures injected through the pool, ExecuteWithRetry*, ShardMapTimer) run on B only.
//        The fake based equivalents are in RpcProviderFakeTests.Extra.cs.
//      * grpc.WithContextDialer -> SocketsHttpHandler.ConnectCallback (RpcTestHelpers.WithDialer).
//      * context.WithTimeout(...) -> a linked CancellationTokenSource with CancelAfter.  .NET cannot tell a deadline from a
//        cancel, so `ErrorIs(err, context.DeadlineExceeded)` is asserted as OperationCanceledException (and, where the Go test
//        measures time, the elapsed time).
//      * `provider.(*rpcProvider).clientPool = pool` -> the pool is a constructor argument of RpcProvider.
//      * StreamRequestsFailOnCanceledContext: gRPC.NET starts the call without I/O, so the Canceled status of an already
//        cancelled token surfaces from the stream (first RecvAsync with a live token) instead of from the request call.
//      * `status.Code(err)` -> OxiaErrors.GrpcCode(err); `assert.Never` -> RpcTestHelpers.NeverAsync.
// =====================================================================================================================
using Xunit;
using System.Diagnostics;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Oxia.Client.Internal;
using Oxia.Client.Tests.Internal.Fakes;
using Oxia.Common.Concurrent;
using Oxia.Common.Constant;
using Oxia.Common.Failpoint;
using Oxia.Common.Proto;
using Oxia.Common.Rpc;
using G = Io.Oxia.Proto.V1;

namespace Oxia.Client.Tests.Internal;

public class RpcProviderTests
{
    private static CancellationToken TestCt => TestContext.Current.CancellationToken;

    private static async Task ExecuteTestWrite(CancellationToken ct, IRpcProvider provider, long shardId)
    {
        await provider.ExecuteWrite(ct, shardId, () =>
        {
            var request = new WriteRequest { Shard = shardId };
            request.Puts.Add(new PutRequest { Key = "key", Value = "value"u8.ToArray() });
            return request;
        });
    }

    /// <summary>Go: shardRequests(shardId) -- write, read, list, range-scan.</summary>
    private static async Task ExecuteShardRequest(string name, CancellationToken ct, IRpcProvider provider, long shardId)
    {
        switch (name)
        {
            case "write":
                await ExecuteTestWrite(ct, provider, shardId);
                break;
            case "read":
                var read = new ReadRequest { Shard = shardId };
                read.Gets.Add(new GetRequest { Key = "key" });
                await provider.ExecuteRead(ct, read);
                break;
            case "list":
                await provider.ExecuteList(ct, new ListRequest { Shard = shardId }, static _ => null);
                break;
            case "range-scan":
                await provider.ExecuteRangeScan(ct, new RangeScanRequest { Shard = shardId }, static _ => null);
                break;
            default:
                throw new ArgumentException(name);
        }
    }

    private static IRpcProvider NewProvider(IShardManager? shardManager, string serviceAddress, params Action<Grpc.Net.Client.GrpcChannelOptions>[] dialOptions) =>
        RpcProvider.Create(TestCt, Constants.DefaultNamespace, null, null, serviceAddress, () => shardManager, dialOptions);

    // When the leader of a shard goes away, the client keeps sending the requests for that shard to the old leader until it
    // receives the updated shard assignments. These attempts fail with a transport-level Unavailable error, which must be retried
    // until the new leader is known.
    [Theory]
    [InlineData("write")]
    [InlineData("read")]
    [InlineData("list")]
    [InlineData("range-scan")]
    public async Task TestRpcProvider_RetryWhenShardLeaderIsUnreachable(string name)
    {
        long shardId = 0;
        string oldLeader = RpcTestHelpers.UnreachableAddress();
        await using var newLeaderServer = await RpcTestHelpers.StartServerAsync(new TestOxiaClientServer());
        string newLeader = newLeaderServer.Address;

        var shardManager = new FakeShardManager { LeaderAddress = oldLeader };

        // The updated shard assignments arrive while the client is trying to reach the old leader
        ValueTask<Stream> Dialer(string address, CancellationToken ct)
        {
            if (address == oldLeader) shardManager.LeaderAddress = newLeader;
            return RpcTestHelpers.DialTcpAsync(address, ct);
        }

        var provider = NewProvider(shardManager, newLeader, RpcTestHelpers.WithDialer(Dialer));
        try
        {
            using var cts = RpcTestHelpers.WithTimeout(TimeSpan.FromSeconds(10));
            await ExecuteShardRequest(name, cts.Token, provider, shardId);
        }
        finally
        {
            await provider.CloseAsync();
        }
    }

    // A leader hint only steers the next attempt: when the hinted leader is unreachable, the request must go back to the shard
    // assignments, which know the new leader by then.
    [Theory]
    [InlineData("write")]
    [InlineData("read")]
    [InlineData("list")]
    [InlineData("range-scan")]
    public async Task TestRpcProvider_RetryWhenHintedLeaderIsUnreachable(string name)
    {
        long shardId = 0;
        await using var newLeaderServer = await RpcTestHelpers.StartServerAsync(new TestOxiaClientServer());
        string newLeader = newLeaderServer.Address;
        var shardManager = new FakeShardManager();
        await using var oldLeaderServer = await RpcTestHelpers.StartServerAsync(
            new NotLeaderOxiaClientServer(RpcTestHelpers.UnreachableAddress(), () => shardManager.LeaderAddress = newLeader));
        shardManager.LeaderAddress = oldLeaderServer.Address;

        var provider = NewProvider(shardManager, newLeader);
        try
        {
            using var cts = RpcTestHelpers.WithTimeout(TimeSpan.FromSeconds(10));
            await ExecuteShardRequest(name, cts.Token, provider, shardId);
        }
        finally
        {
            await provider.CloseAsync();
        }
    }

    // A leader that was deposed, but is still reachable, keeps the write stream of the client open without answering the writes.
    // Once the shard assignments name the new leader, the writes must go to it. The write in flight to the old leader must not be
    // sent again to the new one: the old leader may apply it.
    [Fact]
    public async Task TestRpcProvider_WritesGoToTheNewLeader()
    {
        long shardId = 0;
        var shardManager = new FakeShardManager();
        var newLeader = new WriteServer();
        await using var newLeaderServer = await RpcTestHelpers.StartServerAsync(newLeader);
        string newLeaderAddress = newLeaderServer.Address;
        var oldLeader = new WriteServer
        {
            Stuck = true,
            // The shard assignments name the new leader while the write is in flight
            OnRequest = () => shardManager.LeaderAddress = newLeaderAddress,
        };
        await using var oldLeaderServer = await RpcTestHelpers.StartServerAsync(oldLeader);
        shardManager.LeaderAddress = oldLeaderServer.Address;

        var provider = NewProvider(shardManager, newLeaderAddress);
        try
        {
            using (var cts = RpcTestHelpers.WithTimeout(TimeSpan.FromMilliseconds(500)))
            {
                // context.DeadlineExceeded
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => ExecuteTestWrite(cts.Token, provider, shardId));
            }

            using (var cts = RpcTestHelpers.WithTimeout(TimeSpan.FromSeconds(5)))
            {
                await ExecuteTestWrite(cts.Token, provider, shardId);
            }

            Assert.Equal(1, oldLeader.Requests);
            Assert.Equal(1, newLeader.Requests);
        }
        finally
        {
            await provider.CloseAsync();
        }
    }

    [Fact]
    public async Task TestRpcProviderWritesGoToTheNewLeaderWhenTheOldStreamFailsToClose()
    {
        Assert.SkipUnless(Failpoints.Enabled, "needs failpoints: disabled by the disable_trap tag");
        long shardId = 0;
        var shardManager = new FakeShardManager();
        var oldLeader = new WriteServer();
        await using var oldLeaderServer = await RpcTestHelpers.StartServerAsync(oldLeader);
        var newLeader = new WriteServer();
        await using var newLeaderServer = await RpcTestHelpers.StartServerAsync(newLeader);
        shardManager.LeaderAddress = oldLeaderServer.Address;
        long closeFailures = 0;
        var failpoints = Failpoints.New(new Dictionary<string, Func<Exception?>>
        {
            ["getWriteStream.closeOldStream"] = () =>
            {
                Interlocked.Increment(ref closeFailures);
                return new InvalidOperationException("injected");
            },
        });

        var provider = new RpcProvider(TestCt, Constants.DefaultNamespace, new ClientPool(null, null), newLeaderServer.Address,
            () => shardManager, failpoints);
        try
        {
            await ExecuteTestWrite(TestCt, provider, shardId);
            shardManager.LeaderAddress = newLeaderServer.Address;
            await ExecuteTestWrite(TestCt, provider, shardId);

            Assert.Equal(1, Interlocked.Read(ref closeFailures));
            Assert.Equal(1, oldLeader.Requests);
            Assert.Equal(1, newLeader.Requests);
        }
        finally
        {
            await provider.CloseAsync();
        }
    }

    // Connecting to a leader that cannot be reached, e.g. because its address is blackholed, can take up to the gRPC connect
    // timeout. Setting up the write stream must not outlast the request, and the request must never be sent after its deadline.
    [Fact]
    public async Task TestRpcProvider_WriteStreamSetupRespectsTheDeadline()
    {
        long shardId = 0;
        var leader = new WriteServer();
        await using var leaderServer = await RpcTestHelpers.StartServerAsync(leader);
        var shardManager = new FakeShardManager { LeaderAddress = leaderServer.Address };

        // Connecting to the leader takes longer than the request timeout
        var connect = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        async ValueTask<Stream> Dialer(string address, CancellationToken ct)
        {
            using var delayCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            var delay = Task.Delay(TimeSpan.FromSeconds(3), delayCts.Token);
            if (ReferenceEquals(await Task.WhenAny(connect.Task, delay), delay)) await delay; // <-ctx.Done(): throws
            await delayCts.CancelAsync();
            return await RpcTestHelpers.DialTcpAsync(address, ct);
        }

        var provider = NewProvider(shardManager, leaderServer.Address, RpcTestHelpers.WithDialer(Dialer));
        try
        {
            using var cts = RpcTestHelpers.WithTimeout(TimeSpan.FromMilliseconds(200));
            var start = Stopwatch.StartNew();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => ExecuteTestWrite(cts.Token, provider, shardId)); // context.DeadlineExceeded
            Assert.True(start.Elapsed < TimeSpan.FromSeconds(2), $"took {start.Elapsed}");

            // Once connected, the request is not sent
            connect.SetResult();
            await RpcTestHelpers.NeverAsync(() => leader.Requests > 0, TimeSpan.FromMilliseconds(300), TimeSpan.FromMilliseconds(10));
        }
        finally
        {
            await provider.CloseAsync();
        }
    }

    private static Exception ShardRequestError(string name) => name switch
    {
        "connection refused" => new RpcException(new Grpc.Core.Status(StatusCode.Unavailable,
            "connection error: desc = \"transport: Error while dialing: dial tcp 127.0.0.1:6648: connect: connection refused\"")),
        "transport closing" => new RpcException(new Grpc.Core.Status(StatusCode.Unavailable, "transport is closing")),
        "stream closed" => OxiaErrors.EndOfStream, // io.EOF
        "node is not leader" => OxiaErrors.IntoGrpcStatusError(OxiaErrors.NodeIsNotLeader)!,
        "server not initialized" => OxiaErrors.IntoGrpcStatusError(OxiaErrors.NotInitialized)!,
        "context canceled" => new RpcException(new Grpc.Core.Status(StatusCode.Cancelled, "context canceled")), // status.FromContextError(context.Canceled).Err()
        "context deadline exceeded" => new RpcException(new Grpc.Core.Status(StatusCode.DeadlineExceeded, "context deadline exceeded")),
        "client connection closing" => new RpcException(new Grpc.Core.Status(StatusCode.Cancelled, "grpc: the client connection is closing")),
        "invalid term" => OxiaErrors.IntoGrpcStatusError(OxiaErrors.InvalidTerm)!,
        "unknown" => new RpcException(new Grpc.Core.Status(StatusCode.Unknown, "unknown")),
        _ => throw new ArgumentException(name),
    };

    [Theory]
    [InlineData("connection refused", true)]
    [InlineData("transport closing", true)]
    [InlineData("stream closed", true)]
    [InlineData("node is not leader", true)]
    [InlineData("server not initialized", true)]
    [InlineData("context canceled", false)]
    [InlineData("context deadline exceeded", false)]
    [InlineData("client connection closing", false)]
    [InlineData("invalid term", false)]
    [InlineData("unknown", false)]
    public async Task TestExecuteWithRetry_ShardRequestErrors(string name, bool retryable)
    {
        var err = ShardRequestError(name);
        int attempts = 0;
        Func<ErrorMetadata?, ValueTask<object?>> operation = _ =>
        {
            attempts++;
            if (attempts == 1) throw OxiaErrors.ToThrowable(err);
            return ValueTask.FromResult<object?>(null);
        };

        if (retryable)
        {
            await RpcProvider.ExecuteWithRetry(TestCt, operation, RpcProvider.IsRetryableShardRequest);
            Assert.Equal(2, attempts);
        }
        else
        {
            await Assert.ThrowsAnyAsync<Exception>(async () => await RpcProvider.ExecuteWithRetry(TestCt, operation, RpcProvider.IsRetryableShardRequest));
            Assert.Equal(1, attempts);
        }
    }

    // Session requests go to a fixed target, so they must not retry an unreachable one: the session looks up the shard leader
    // again before its next attempt.
    [Fact]
    public async Task TestRpcProvider_KeepAliveFailsFastWhenTargetIsUnreachable()
    {
        var provider = NewProvider(null, "");
        try
        {
            using var cts = RpcTestHelpers.WithTimeout(TimeSpan.FromSeconds(10));
            var err = await Record.ExceptionAsync(async () => await provider.KeepAlive(cts.Token, RpcTestHelpers.UnreachableAddress(), new SessionHeartbeat()));
            Assert.Equal(StatusCode.Unavailable, OxiaErrors.GrpcCode(err));
        }
        finally
        {
            await provider.CloseAsync();
        }
    }

    // Session requests go to a fixed target: the shard leader known by the client when the request is sent. After the leader
    // moves, retrying them against the same target would keep failing, so they must fail fast: the session looks up the shard
    // leader again before its next attempt.
    [Theory]
    [InlineData("create-session")]
    [InlineData("keep-alive")]
    [InlineData("close-session")]
    public async Task TestRpcProvider_SessionRequestsFailFastWhenTargetIsNotLeader(string name)
    {
        long requests = 0;
        await using var server = await RpcTestHelpers.StartServerAsync(
            new NotLeaderOxiaClientServer("new-leader:6648", () => Interlocked.Increment(ref requests)));
        string target = server.Address;

        var provider = NewProvider(null, "");
        try
        {
            using var cts = RpcTestHelpers.WithTimeout(TimeSpan.FromSeconds(10));
            var err = await Record.ExceptionAsync(async () =>
            {
                switch (name)
                {
                    case "create-session": await provider.CreateSession(cts.Token, target, new CreateSessionRequest()); break;
                    case "keep-alive": await provider.KeepAlive(cts.Token, target, new SessionHeartbeat()); break;
                    default: await provider.CloseSession(cts.Token, target, new CloseSessionRequest()); break;
                }
            });
            Assert.True(OxiaErrors.Is(err, OxiaErrors.NodeIsNotLeader), $"unexpected error: {err}");
            Assert.Equal(1, Interlocked.Read(ref requests));
        }
        finally
        {
            await provider.CloseAsync();
        }
    }

    // While a shard has no leader, e.g. during an election, its leader address is empty: the client must not connect to it. The
    // shard requests are retried until the leader is known, while the session requests fail fast.
    [Fact]
    public async Task TestRpcProvider_ShardWithoutLeader()
    {
        long shardId = 0;
        string noLeader = "";
        await using var leaderServer = await RpcTestHelpers.StartServerAsync(new TestOxiaClientServer());
        string leader = leaderServer.Address;
        var shardManager = new FakeShardManager { LeaderAddress = noLeader };

        var pool = new RecordingClientPool(new ClientPool(null, null));
        var provider = new RpcProvider(TestCt, Constants.DefaultNamespace, pool, leader, () => shardManager);
        try
        {
            using var cts = RpcTestHelpers.WithTimeout(TimeSpan.FromSeconds(10));

            var err = await Record.ExceptionAsync(async () =>
                await provider.KeepAlive(cts.Token, shardManager.Leader(shardId), new SessionHeartbeat { Shard = shardId }));
            Assert.Equal(StatusCode.Unavailable, OxiaErrors.GrpcCode(err));

            _ = Task.Run(async () =>
            {
                await Task.Delay(TimeSpan.FromMilliseconds(300));
                shardManager.LeaderAddress = leader;
            }, TestCt);
            var read = new ReadRequest { Shard = shardId };
            read.Gets.Add(new GetRequest { Key = "key" });
            await provider.ExecuteRead(cts.Token, read);

            Assert.DoesNotContain("", pool.Targets);
        }
        finally
        {
            await provider.CloseAsync();
        }
    }

    // The requests without a shard go to the service address.
    [Fact]
    public async Task TestRpcProviderExecuteReadWithoutShard()
    {
        await using var server = await RpcTestHelpers.StartServerAsync(new TestOxiaClientServer());
        var provider = NewProvider(null, server.Address);
        try
        {
            var request = new ReadRequest();
            request.Gets.Add(new GetRequest { Key = "key" });
            var response = await provider.ExecuteRead(TestCt, request);
            Assert.Single(response.Gets);
            Assert.Equal(Oxia.Common.Proto.Status.KeyNotFound, response.Gets[0].Status);
        }
        finally
        {
            await provider.CloseAsync();
        }
    }

    // A shard missing from the shard map was split or merged: the shard requests fail right away, so that the caller reroutes them.
    [Theory]
    [InlineData("write")]
    [InlineData("read")]
    [InlineData("list")]
    [InlineData("range-scan")]
    public async Task TestRpcProviderShardRequestsFailOnUnknownShard(string name)
    {
        long shardId = 0;
        var provider = NewProvider(null, "");
        try
        {
            using var cts = RpcTestHelpers.WithTimeout(TimeSpan.FromSeconds(10));
            var err = await Record.ExceptionAsync(() => ExecuteShardRequest(name, cts.Token, provider, shardId));
            Assert.True(OxiaErrors.Is(err, OxiaErrors.ShardNotFound), $"unexpected error: {err}");
        }
        finally
        {
            await provider.CloseAsync();
        }
    }

    [Theory]
    [InlineData("write")]
    [InlineData("read")]
    [InlineData("list")]
    [InlineData("range-scan")]
    public async Task TestRpcProviderShardRequestsFailWhenConnectionFails(string name)
    {
        var errConnect = new InvalidOperationException("invalid target");
        long shardId = 0;
        string leader = "leader:6648";
        var shardManager = new FakeShardManager { LeaderAddress = leader };
        var pool = new RecordingClientPool(new ClientPool(null, null)) { ConnectError = errConnect };
        var provider = new RpcProvider(TestCt, Constants.DefaultNamespace, pool, "", () => shardManager);
        try
        {
            using var cts = RpcTestHelpers.WithTimeout(TimeSpan.FromSeconds(10));
            var err = await Record.ExceptionAsync(() => ExecuteShardRequest(name, cts.Token, provider, shardId));
            Assert.True(OxiaErrors.Is(err, errConnect), $"unexpected error: {err}");
            Assert.Equal([leader], pool.Targets);
        }
        finally
        {
            await provider.CloseAsync();
        }
    }

    // The empty target is the leader of a shard that has no leader: the requests to a fixed target fail fast without connecting to it.
    [Theory]
    [InlineData("shard-assignments")]
    [InlineData("sequence-updates")]
    [InlineData("notifications")]
    [InlineData("create-session")]
    [InlineData("close-session")]
    public async Task TestRpcProviderFixedTargetRequestsFailWithoutLeader(string name)
    {
        var pool = new RecordingClientPool(new ClientPool(null, null));
        var provider = new RpcProvider(TestCt, Constants.DefaultNamespace, pool, "", () => null);
        try
        {
            var err = await Record.ExceptionAsync(async () =>
            {
                switch (name)
                {
                    case "shard-assignments": await provider.GetShardAssignments(TestCt, "", new ShardAssignmentsRequest()); break;
                    case "sequence-updates": await provider.GetSequenceUpdates(TestCt, "", new GetSequenceUpdatesRequest()); break;
                    case "notifications": await provider.GetNotifications(TestCt, "", new NotificationsRequest()); break;
                    case "create-session": await provider.CreateSession(TestCt, "", new CreateSessionRequest()); break;
                    default: await provider.CloseSession(TestCt, "", new CloseSessionRequest()); break;
                }
            });
            Assert.True(OxiaErrors.Is(err, RpcProvider.ErrShardHasNoLeader), $"unexpected error: {err}");
            Assert.Empty(pool.Targets);
        }
        finally
        {
            await provider.CloseAsync();
        }
    }

    [Fact]
    public async Task TestRpcProviderGetSequenceUpdates()
    {
        await using var server = await RpcTestHelpers.StartServerAsync(new TestOxiaClientServer());
        string target = server.Address;
        var provider = NewProvider(null, "");
        try
        {
            // The server does not implement the request: the stream reports it
            using var stream = await provider.GetSequenceUpdates(TestCt, target, new GetSequenceUpdatesRequest { Key = "key" });
            var err = await Record.ExceptionAsync(async () => await stream.RecvAsync(TestCt));
            Assert.Equal(StatusCode.Unimplemented, OxiaErrors.GrpcCode(err));
        }
        finally
        {
            await provider.CloseAsync();
        }
    }

    [Fact]
    public async Task TestRpcProviderStreamRequestsFailOnCanceledContext()
    {
        await using var server = await RpcTestHelpers.StartServerAsync(new TestOxiaClientServer());
        string target = server.Address;
        var provider = NewProvider(null, "");
        try
        {
            using var cts = new CancellationTokenSource();
            await cts.CancelAsync();

            // gRPC.NET starts the call without I/O: the Canceled status surfaces from the stream (read with a live token)
            var err = await Record.ExceptionAsync(async () =>
            {
                using var stream = await provider.GetSequenceUpdates(cts.Token, target, new GetSequenceUpdatesRequest { Key = "key" });
                await stream.RecvAsync(CancellationToken.None);
            });
            Assert.Equal(StatusCode.Cancelled, OxiaErrors.GrpcCode(err));

            err = await Record.ExceptionAsync(async () =>
            {
                using var stream = await provider.GetNotifications(cts.Token, target, new NotificationsRequest());
                await stream.RecvAsync(CancellationToken.None);
            });
            Assert.Equal(StatusCode.Cancelled, OxiaErrors.GrpcCode(err));
        }
        finally
        {
            await provider.CloseAsync();
        }
    }

    [Fact]
    public async Task TestShardMapTimer()
    {
        var changed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var timer = new ShardMapTimer { Changed = changed.Task };
        try
        {
            timer.Start(TimeSpan.FromMilliseconds(1));
            await timer.WaitAsync(TestCt).AsTask().WaitAsync(TimeSpan.FromSeconds(10), TestCt); // the timer did not fire

            timer.Start(TimeSpan.FromHours(1));
            var wait = timer.WaitAsync(TestCt).AsTask();
            changed.SetResult();
            await wait.WaitAsync(TimeSpan.FromSeconds(10), TestCt); // the timer did not fire on the shard map change
        }
        finally
        {
            timer.Stop();
        }
    }

    [Fact]
    public async Task TestExecuteWithRetryWakesUpOnShardMapChange()
    {
        // Without the shard map change, the retry would wait at least 50 ms
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(40));

        var changed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var timer = new ShardMapTimer();
        int attempts = 0;
        int result = await RpcProvider.ExecuteWithRetryTimer(cts.Token, timer, hint =>
        {
            timer.Changed = changed.Task;
            attempts++;
            if (attempts == 1)
            {
                // The shard map changes while the attempt is in progress
                changed.SetResult();
                throw OxiaErrors.ToThrowable(OxiaErrors.NodeIsNotLeader);
            }

            return ValueTask.FromResult(attempts);
        });
        Assert.Equal(2, result);
    }

    // The consumer of the responses of a list or a range scan ends it with an error, which is returned without a retry.
    [Fact]
    public async Task TestRpcProviderListAndRangeScanConsumerError()
    {
        long requests = 0;
        string[] keys = ["a", "b", "c"];
        var service = new TestOxiaClientServer
        {
            ListHook = async (_, stream) =>
            {
                Interlocked.Increment(ref requests);
                foreach (var key in keys)
                {
                    var response = new G.ListResponse();
                    response.Keys.Add(key);
                    await stream.WriteAsync(response);
                }
            },
            RangeScanHook = async (_, stream) =>
            {
                Interlocked.Increment(ref requests);
                foreach (var key in keys)
                {
                    var response = new G.RangeScanResponse();
                    response.Records.Add(new G.GetResponse { Key = key });
                    await stream.WriteAsync(response);
                }
            },
        };
        await using var server = await RpcTestHelpers.StartServerAsync(service);
        string address = server.Address;
        var shardManager = new FakeShardManager { LeaderAddress = address };
        var provider = NewProvider(shardManager, address);
        try
        {
            long shardId = 0;
            var failure = new InvalidOperationException("failure");
            int consumed = 0;
            var err = await Record.ExceptionAsync(async () => await provider.ExecuteList(TestCt, new ListRequest { Shard = shardId }, _ =>
            {
                consumed++;
                return failure;
            }));
            Assert.True(OxiaErrors.Is(err, failure));
            Assert.Equal(1, consumed);
            Assert.Equal(1, Interlocked.Read(ref requests));

            consumed = 0;
            err = await Record.ExceptionAsync(async () => await provider.ExecuteRangeScan(TestCt, new RangeScanRequest { Shard = shardId }, _ =>
            {
                consumed++;
                return failure;
            }));
            Assert.True(OxiaErrors.Is(err, failure));
            Assert.Equal(1, consumed);
            Assert.Equal(2, Interlocked.Read(ref requests));
        }
        finally
        {
            await provider.CloseAsync();
        }
    }
}

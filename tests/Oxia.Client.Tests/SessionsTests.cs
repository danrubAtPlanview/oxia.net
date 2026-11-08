// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/sessions_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests: 8 (TestSessions_FailedSessionStartDoesNotDeadlock, TestSessions_KeepAliveRetryDelayDoesNotGrow,
//             TestSessions_FollowSplit [3 subtests], TestSessions_ShardSplitDuringCreation, TestSessionsFollowSplitsAfterClose,
//             TestSessionsFollowSplitsSkipsSessionNotEstablished, TestClientSessionExecuteWithIdFailsWhenContextExpires,
//             TestClientSessionClose [3 subtests]), ported: 8 (+ 2 subtests as Theory rows, 3+3 rows), skipped: 0.
//   Adaptations (the assertions are the Go ones):
//      * ExecuteWithSessionId never blocks (see Sessions.cs): the helpers wait for the callback through a TaskCompletionSource.
//      * The Go mock gRPC servers (`grpc.NewServer` + `proto.RegisterOxiaClientServer`) are services derived from the generated
//        OxiaClient.OxiaClientBase hosted by TestGrpcServer; the RpcProvider is the real one (RpcProvider.Create).
//      * `clientOptions{requestTimeout: time.Second}` is `new ClientOptions { RequestTimeout = ... }`.
//      * A context.Context that EXPIRED (TestClientSessionExecuteWithIdFailsWhenContextExpires) cannot be told from a cancelled
//        CancellationToken: the test sets Sessions.ContextError (see Sessions.cs).
//      * t.Context() is a token cancelled when the test finishes.
//      * cs.started <- err (TestSessions_FailedSessionStartDoesNotDeadlock) is ClientSession.FailStart(err).
// =====================================================================================================================
using System.Diagnostics;
using System.Threading.Channels;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Oxia.Client.Internal;
using Oxia.Client.Tests.F1Support;
using Oxia.Common.Constant;
using Oxia.Common.Hash;
using Oxia.Common.Proto;
using Oxia.TestSupport;
using Xunit;
using G = Io.Oxia.Proto.V1;

namespace Oxia.Client.Tests;

public class SessionsTests
{
    private static readonly TimeSpan Ten = TimeSpan.FromSeconds(10);

    private static Task<TestGrpcServer> Start<T>(T service) where T : class =>
        TestGrpcServer.StartAsync(app => Microsoft.AspNetCore.Builder.GrpcEndpointRouteBuilderExtensions.MapGrpcService<T>(app),
            s => s.AddSingleton(service));

    private static ClientOptions NewClientOptions(string serviceAddress)
    {
        var err = ClientOptions.TryCreate(serviceAddress, ReadOnlySpan<ClientOption>.Empty, out var options);
        Assert.Null(err);
        return options;
    }

    private static Task<(long ShardId, long SessionId, Exception? Err)> Execute(Sessions s, Func<long> shardForKey)
    {
        var tcs = new TaskCompletionSource<(long, long, Exception?)>(TaskCreationOptions.RunContinuationsAsynchronously);
        s.ExecuteWithSessionId(shardForKey, (shardId, sessionId, err) => tcs.TrySetResult((shardId, sessionId, err)));
        return tcs.Task;
    }

    // An operation hitting a session that failed to start must fail its callback and discard the session — without
    // re-acquiring the sessions lock that the caller already holds, which was a self-deadlock wedging every subsequent
    // ephemeral operation of the client.
    [Fact]
    public async Task TestSessions_FailedSessionStartDoesNotDeadlock()
    {
        using var ctx = new CancellationTokenSource();
        var s = new Sessions(ctx.Token, new StaticShardManager { Shards = new long[] { 1 } }, new UnimplementedRpcProvider(),
            new ClientOptions());

        var cs = s.NewClientSession(1);
        cs.FailStart(new Exception("session start failed"));
        s.SessionsByShard[1] = cs;

        long gotSessionId = 0;
        Exception? gotErr = null;
        var done = Task.Run(() =>
        {
            s.ExecuteWithSessionId(() => 1, (_, sessionId, err) =>
            {
                gotSessionId = sessionId;
                gotErr = err;
            });
        });

        await Wait.WithTimeout(done, Ten, "ExecuteWithSessionId deadlocked on a failed session start");
        Assert.NotNull(gotErr);                 // assert.Error
        Assert.Equal(-1, gotSessionId);         // assert.EqualValues(-1, sessionId)

        // The failed session has been discarded: the next operation will attempt a fresh one
        bool found;
        lock (s.Lock) found = s.SessionsByShard.ContainsKey(1);
        Assert.False(found);
        ctx.Cancel();
    }

    /// <summary>flakyKeepAliveServer fails every other heartbeat, starting with the first one: a long-lived session going through occasional failures, like a node restart, each followed by a recovery.</summary>
    public sealed class FlakyKeepAliveServer : G.OxiaClient.OxiaClientBase
    {
        public readonly Channel<long> Heartbeats = Channel.CreateBounded<long>(100);
        private long _count;

        public override Task<G.CreateSessionResponse> CreateSession(G.CreateSessionRequest request, ServerCallContext context)
            => Task.FromResult(new G.CreateSessionResponse { SessionId = 1 });

        public override async Task<G.KeepAliveResponse> KeepAlive(G.SessionHeartbeat request, ServerCallContext context)
        {
            await Heartbeats.Writer.WriteAsync(Stopwatch.GetTimestamp());
            if (Interlocked.Increment(ref _count) % 2 == 1)
                throw new RpcException(new Grpc.Core.Status(StatusCode.Unavailable, "transient failure"));
            return new G.KeepAliveResponse();
        }
    }

    // A failed heartbeat is retried after a backoff, which a successful heartbeat must reset: otherwise every failure over
    // the lifetime of the session makes the next retry wait longer, until a single failure is enough to make the session
    // expire on the server.
    [Fact]
    public async Task TestSessions_KeepAliveRetryDelayDoesNotGrow()
    {
        var server = new FlakyKeepAliveServer();
        await using var grpc = await Start(server);

        var options = NewClientOptions(grpc.Address);
        var shardManager = new StaticShardManager { LeaderAddress = options.ServiceAddress };
        var rpcProvider = RpcProvider.Create(CancellationToken.None, options.Namespace, null, null, options.ServiceAddress,
            () => shardManager);

        using var cts = new CancellationTokenSource();
        try
        {
            var s = new Sessions(cts.Token, shardManager, rpcProvider, options);
            var result = await Execute(s, () => 0);
            Assert.Null(result.Err);

            async Task<long> NextHeartbeat()
            {
                var read = server.Heartbeats.Reader.ReadAsync().AsTask();
                return await Wait.WithTimeout(read, Ten, "no heartbeat received");
            }

            // A failed heartbeat is retried after the backoff, then a full tick of the keep-alive ticker, which is 2s here.
            // The first backoff is 150ms at the most: without a reset, the one after the 7th failure is 570ms at least.
            for (int failure = 1; failure <= 7; failure++)
            {
                long failedAt = await NextHeartbeat();
                long next = await NextHeartbeat();
                var retryDelay = Stopwatch.GetElapsedTime(failedAt, next) - TimeSpan.FromSeconds(2);
                Assert.True(retryDelay < TimeSpan.FromMilliseconds(500), $"retry delay after failure {failure}: {retryDelay}");
            }
        }
        finally
        {
            cts.Cancel();
            await rpcProvider.CloseAsync();
        }
    }

    private readonly record struct ShardSession(long Shard, long SessionId);

    /// <summary>sessionRequestsServer records the session requests it receives. The id of a session it creates is 10 + the shard id.</summary>
    public class SessionRequestsServer : G.OxiaClient.OxiaClientBase
    {
        public readonly Channel<(long, long)> Heartbeats = Channel.CreateBounded<(long, long)>(100);
        private readonly object _lock = new();
        public readonly List<long> Created = new();
        public readonly List<(long, long)> Closed = new();

        public override Task<G.CreateSessionResponse> CreateSession(G.CreateSessionRequest request, ServerCallContext context)
        {
            lock (_lock) Created.Add(request.Shard);
            return Task.FromResult(new G.CreateSessionResponse { SessionId = 10 + request.Shard });
        }

        public override async Task<G.KeepAliveResponse> KeepAlive(G.SessionHeartbeat request, ServerCallContext context)
        {
            await Heartbeats.Writer.WriteAsync((request.Shard, request.SessionId));
            return new G.KeepAliveResponse();
        }

        public override Task<G.CloseSessionResponse> CloseSession(G.CloseSessionRequest request, ServerCallContext context)
        {
            lock (_lock) Closed.Add((request.Shard, request.SessionId));
            return Task.FromResult(new G.CloseSessionResponse());
        }

        public long[] CreatedSnapshot() { lock (_lock) return Created.ToArray(); }
        public (long, long)[] ClosedSnapshot() { lock (_lock) return Closed.ToArray(); }
    }

    // The shards that replace a split shard inherit its session, which owns the ephemeral records they get from it. The
    // client must keep the session alive on them, use it for their new ephemeral records, and close it there.
    [Theory]
    [InlineData("notified")]
    [InlineData("lookup-first")]
    [InlineData("split-twice")]
    public async Task TestSessions_FollowSplit(string name)
    {
        // {parent, left, right} splits of shard 0 and its children; the shards that replaced shard 0 in the end; whether the
        // client looks up the session of the last of them before the change of the shard map is notified
        (long, long, long)[] splits;
        long[] shards;
        bool lookupFirst;
        switch (name)
        {
            case "notified": splits = new[] { (0L, 1L, 2L) }; shards = new long[] { 1, 2 }; lookupFirst = false; break;
            case "lookup-first": splits = new[] { (0L, 1L, 2L) }; shards = new long[] { 1, 2 }; lookupFirst = true; break;
            case "split-twice": splits = new[] { (0L, 1L, 2L), (1L, 3L, 4L) }; shards = new long[] { 2, 3, 4 }; lookupFirst = true; break;
            default: throw new ArgumentException(name);
        }

        var server = new SessionRequestsServer();
        await using var grpc = await Start(server);

        var options = NewClientOptions(grpc.Address);
        var splittable = new SplittableShardManager(options.ServiceAddress);
        IShardManager shardManager = splittable;
        if (lookupFirst) shardManager = new UnnotifiedShardManager(splittable);
        var rpcProvider = RpcProvider.Create(CancellationToken.None, options.Namespace, null, null, options.ServiceAddress,
            () => shardManager);

        using var cts = new CancellationTokenSource();
        try
        {
            var s = new Sessions(cts.Token, shardManager, rpcProvider, options);
            async Task<long> SessionOn(long shardId)
            {
                var r = await Execute(s, () => shardId);
                Assert.Null(r.Err);   // require.NoError
                return r.SessionId;
            }
            long sessionId = await SessionOn(0);

            foreach (var split in splits) splittable.Split(split.Item1, split.Item2, split.Item3);
            if (lookupFirst)
            {
                Assert.Equal(sessionId, await SessionOn(shards[^1]));
            }

            var missing = new HashSet<(long, long)>();
            var inherited = new List<(long, long)>();
            foreach (long shard in shards)
            {
                missing.Add((shard, sessionId));
                inherited.Add((shard, sessionId));
            }
            var timeout = Task.Delay(Ten);
            while (missing.Count > 0)
            {
                var read = server.Heartbeats.Reader.ReadAsync().AsTask();
                var winner = await Task.WhenAny(read, timeout);
                if (!ReferenceEquals(winner, read))
                    throw new Xunit.Sdk.XunitException("the session is not kept alive on the shards that replaced its shard; missing heartbeats: " + string.Join(", ", missing));
                missing.Remove(await read);
            }
            foreach (long shard in shards)
            {
                Assert.Equal(sessionId, await SessionOn(shard));
            }

            await s.CloseAsync();
            Assert.Equal(new long[] { 0 }, server.CreatedSnapshot());
            Assert.Equal(inherited.OrderBy(x => x).ToArray(), server.ClosedSnapshot().OrderBy(x => x).ToArray());   // ElementsMatch
        }
        finally
        {
            cts.Cancel();
            await rpcProvider.CloseAsync();
        }
    }

    /// <summary>frozenShardServer is a sessionRequestsServer that rejects the creation of the sessions on shard 0 with ErrNodeIsNotLeader, like the parent of a split while the cutover freezes it, and the data servers once it is deleted.</summary>
    public sealed class FrozenShardServer : SessionRequestsServer
    {
        private long _rejected;
        public long Rejected => Interlocked.Read(ref _rejected);

        public override Task<G.CreateSessionResponse> CreateSession(G.CreateSessionRequest request, ServerCallContext context)
        {
            if (request.Shard == 0)
            {
                Interlocked.Increment(ref _rejected);
                throw OxiaErrors.IntoGrpcStatusError(OxiaErrors.NodeIsNotLeader)!;
            }
            return base.CreateSession(request, context);
        }
    }

    // The first ephemeral operation on a shard creates the session there, and holds the sessions lock until the session is
    // established. When the shard is split meanwhile, the session can no longer be created on it: the operation must get one
    // on the shard that took over its key, and release the lock, without waiting for the next attempt of the creation.
    [Fact]
    public async Task TestSessions_ShardSplitDuringCreation()
    {
        var server = new FrozenShardServer();
        await using var grpc = await Start(server);

        var options = NewClientOptions(grpc.Address);
        var shardManager = new SplittableShardManager(options.ServiceAddress);
        var rpcProvider = RpcProvider.Create(CancellationToken.None, options.Namespace, null, null, options.ServiceAddress,
            () => shardManager);

        using var cts = new CancellationTokenSource();
        try
        {
            var s = new Sessions(cts.Token, shardManager, rpcProvider, options);
            Task<(long ShardId, long SessionId, Exception? Err)> ExecuteKey(string key) => Execute(s, () => shardManager.Get(key));

            // The split gives the lower half of the hash range to shard 1, the upper half to shard 2
            string leftKey = "", rightKey = "";
            for (int i = 0; leftKey == "" || rightKey == ""; i++)
            {
                string key = $"key-{i}";
                if (Hasher.Xxh332(key) <= uint.MaxValue / 2) leftKey = key;
                else rightKey = key;
            }

            async Task<(long ShardId, long SessionId, Exception? Err)> Await(Task<(long ShardId, long SessionId, Exception? Err)> t)
                => await Wait.WithTimeout(t, Ten, $"the operation is stuck on the session of the split shard; creations rejected on shard 0: {server.Rejected}");

            var first = ExecuteKey(leftKey);
            // After its 8th attempt, the creation waits 854 ms at least
            await Wait.EventuallyAsync(() => server.Rejected >= 8, TimeSpan.FromSeconds(30), TimeSpan.FromMilliseconds(1));
            long splitAt = Stopwatch.GetTimestamp();
            shardManager.Split(0, 1, 2);
            var second = ExecuteKey(rightKey);
            var firstResult = await Await(first);
            Assert.Equal((1L, 11L, (Exception?)null), firstResult);
            Assert.True(Stopwatch.GetElapsedTime(splitAt) < TimeSpan.FromMilliseconds(500), "the creation waited for its next attempt");
            Assert.Equal((2L, 12L, (Exception?)null), await Await(second));

            await s.CloseAsync();
            Assert.Equal(new long[] { 1, 2 }, server.CreatedSnapshot());
            Assert.Equal(new[] { (1L, 11L), (2L, 12L) }, server.ClosedSnapshot().OrderBy(x => x).ToArray());   // ElementsMatch
        }
        finally
        {
            cts.Cancel();
            await rpcProvider.CloseAsync();
        }
    }

    /// <summary>sessionsTestRpcProvider fails the creation of the sessions with CreateErr, and their closing with CloseErr. It records the requests to close them.</summary>
    internal sealed class SessionsTestRpcProvider : UnimplementedRpcProvider
    {
        public Exception? CreateErr { get; set; }
        public Exception? CloseErr { get; set; }
        public readonly List<string> CloseTargets = new();
        public readonly List<CloseSessionRequest> CloseRequests = new();

        public override ValueTask<CreateSessionResponse> CreateSession(CancellationToken ct, string target, CreateSessionRequest request)
        {
            if (ct.IsCancellationRequested) return ValueTask.FromException<CreateSessionResponse>(new OperationCanceledException(ct));
            if (CreateErr is not null) return ValueTask.FromException<CreateSessionResponse>(CreateErr);
            return ValueTask.FromResult(new CreateSessionResponse { SessionId = 1 });
        }

        public override ValueTask<CloseSessionResponse> CloseSession(CancellationToken ct, string target, CloseSessionRequest request)
        {
            CloseTargets.Add(target);
            CloseRequests.Add(request);
            if (CloseErr is not null) return ValueTask.FromException<CloseSessionResponse>(CloseErr);
            return ValueTask.FromResult(new CloseSessionResponse());
        }
    }

    // A shard split after the sessions were closed is not followed.
    [Fact]
    public void TestSessionsFollowSplitsAfterClose()
    {
        var shardManager = new NotificationsTestShardManager
        {
            Shards = new List<long> { 1, 2 },
            Successors = new Dictionary<long, long[]> { [0] = new long[] { 1, 2 } },
        };
        var s = new Sessions(CancellationToken.None, shardManager, new SessionsTestRpcProvider(),
            new ClientOptions { RequestTimeout = TimeSpan.FromSeconds(1) });
        var cs = s.NewClientSession(0);
        (cs.SessionId, cs.Established) = (10, true);
        s.SessionsByShard[0] = cs;
        s.CancelSessions();

        lock (s.Lock) s.FollowSplits();

        var entry = Assert.Single(s.SessionsByShard);
        Assert.Equal(0, entry.Key);
        Assert.Same(cs, entry.Value);
    }

    // A session that is not established yet has nothing for the shards that replaced its shard to inherit.
    [Fact]
    public void TestSessionsFollowSplitsSkipsSessionNotEstablished()
    {
        var shardManager = new NotificationsTestShardManager
        {
            Shards = new List<long> { 1, 2 },
            Successors = new Dictionary<long, long[]> { [0] = new long[] { 1, 2 } },
        };
        var s = new Sessions(CancellationToken.None, shardManager, new SessionsTestRpcProvider(),
            new ClientOptions { RequestTimeout = TimeSpan.FromSeconds(1) });
        try
        {
            var cs = s.NewClientSession(0);
            s.SessionsByShard[0] = cs;

            lock (s.Lock) s.FollowSplits();

            var entry = Assert.Single(s.SessionsByShard);
            Assert.Equal(0, entry.Key);
            Assert.Same(cs, entry.Value);
            Assert.False(cs.Ctx.IsCancellationRequested);   // assert.NoError(t, cs.ctx.Err())
        }
        finally
        {
            s.CancelSessions();
        }
    }

    // The operations waiting for a session fail once the client context expires, but not when the client is closed.
    [Fact]
    public void TestClientSessionExecuteWithIdFailsWhenContextExpires()
    {
        using var expired = new CancellationTokenSource();
        expired.Cancel();
        var s = new Sessions(expired.Token, new StaticShardManager { Shards = new long[] { 0 } }, new SessionsTestRpcProvider(),
            new ClientOptions { RequestTimeout = TimeSpan.FromSeconds(1) })
        {
            // An expired context.Context: context.DeadlineExceeded (a token cannot tell it from a cancellation)
            ContextError = new TimeoutException("context deadline exceeded"),
        };
        var cs = s.NewClientSession(0);

        var results = new List<Exception?>();
        var err = cs.ExecuteWithId((sessionId, e) =>
        {
            Assert.Equal(-1, sessionId);
            results.Add(e);
        });
        Assert.Null(err);   // require.NoError
        Assert.Single(results);
        Assert.True(OxiaErrors.IsDeadlineExceeded(results[0]));   // assert.ErrorIs(results[0], context.DeadlineExceeded)

        using var live = new CancellationTokenSource();
        s = new Sessions(live.Token, new StaticShardManager { Shards = new long[] { 0 } }, new SessionsTestRpcProvider(),
            new ClientOptions { RequestTimeout = TimeSpan.FromSeconds(1) });
        cs = s.NewClientSession(0);
        s.CancelSessions();
        bool invoked = false;
        err = cs.ExecuteWithId((_, _) => invoked = true);
        Assert.False(invoked, "the callback of a closed session was invoked");
        Assert.Null(err);
        live.Cancel();
    }

    [Theory]
    [InlineData("closed")]
    [InlineData("session-not-found")]
    [InlineData("error")]
    public async Task TestClientSessionClose(string name)
    {
        var errClose = new Exception("close failed");
        Exception? closeErr = name switch { "closed" => null, "session-not-found" => OxiaErrors.SessionNotFound, _ => errClose };
        Exception? expected = name switch { "error" => errClose, _ => null };

        var provider = new SessionsTestRpcProvider { CloseErr = closeErr };
        var s = new Sessions(CancellationToken.None, new StaticShardManager { Shards = new long[] { 0 }, LeaderAddress = "leader:6648" },
            provider, new ClientOptions { RequestTimeout = TimeSpan.FromSeconds(1) });
        try
        {
            var cs = s.NewClientSession(0);
            cs.SessionId = 10;

            Exception? err = null;
            try { await cs.CloseAsync(); } catch (Exception e) { err = e; }
            if (expected is null)
            {
                Assert.Null(err);
            }
            else
            {
                Assert.NotNull(err);
                Assert.True(OxiaErrors.Is(err, expected));   // assert.ErrorIs(err, test.expected)
            }
            Assert.True(cs.Ctx.IsCancellationRequested);     // assert.Error(t, cs.ctx.Err())
            Assert.Equal(new[] { "leader:6648" }, provider.CloseTargets);
            var request = Assert.Single(provider.CloseRequests);
            Assert.Equal(0, request.Shard);
            Assert.Equal(10, request.SessionId);
        }
        finally
        {
            s.CancelSessions();
        }
    }
}

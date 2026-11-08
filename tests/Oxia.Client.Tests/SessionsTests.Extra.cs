// =====================================================================================================================
// PORT NOTE (extra tests -- no Go counterpart)
//   Upstream : none.  Intent/edge tests for the .NET redesign of Sessions.ExecuteWithSessionId (non-blocking, FIFO queue behind
//              a session being created, lock-free allocation-free fast path), plus session-not-found removal and Close semantics
//              that the Go suite does not exercise.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
// =====================================================================================================================
using Oxia.Client.Internal;
using Oxia.Client.Tests.F1Support;
using Oxia.Common.Constant;
using Oxia.Common.Proto;
using Xunit;

namespace Oxia.Client.Tests;

public class SessionsExtraTests
{
    private static readonly TimeSpan Ten = TimeSpan.FromSeconds(10);

    /// <summary>CreateSession of a shard completes when the test opens its gate; records the order of the requests.</summary>
    private sealed class GatedRpcProvider : UnimplementedRpcProvider
    {
        private readonly object _lock = new();
        private readonly Dictionary<long, TaskCompletionSource> _gates = new();
        public readonly List<long> CreateRequests = new();
        public readonly List<(long Shard, long SessionId)> Closed = new();
        public Exception? KeepAliveErr { get; set; }
        public Func<long, Exception?>? CloseErr { get; set; }
        public bool Gated { get; set; } = true;

        private TaskCompletionSource Gate(long shard)
        {
            lock (_lock)
            {
                if (!_gates.TryGetValue(shard, out var g)) _gates[shard] = g = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                return g;
            }
        }

        public void Open(long shard) => Gate(shard).TrySetResult();
        public int CreateCount() { lock (_lock) return CreateRequests.Count; }

        public override async ValueTask<CreateSessionResponse> CreateSession(CancellationToken ct, string target, CreateSessionRequest request)
        {
            lock (_lock) CreateRequests.Add(request.Shard);
            if (Gated) await Gate(request.Shard).Task.WaitAsync(ct).ConfigureAwait(false);
            return new CreateSessionResponse { SessionId = 100 + request.Shard };
        }

        public override ValueTask<KeepAliveResponse> KeepAlive(CancellationToken ct, string target, SessionHeartbeat request)
        {
            if (KeepAliveErr is not null) return ValueTask.FromException<KeepAliveResponse>(OxiaErrors.ToThrowable(KeepAliveErr));
            return ValueTask.FromResult(new KeepAliveResponse());
        }

        public override ValueTask<CloseSessionResponse> CloseSession(CancellationToken ct, string target, CloseSessionRequest request)
        {
            lock (_lock) Closed.Add((request.Shard, request.SessionId));
            var err = CloseErr?.Invoke(request.Shard);
            if (err is not null) return ValueTask.FromException<CloseSessionResponse>(err);
            return ValueTask.FromResult(new CloseSessionResponse());
        }
    }

    private static Sessions NewSessions(CancellationToken ct, GatedRpcProvider provider, params long[] shards)
        => new(ct, new StaticShardManager { Shards = shards, LeaderAddress = "leader:6648" }, provider,
            new ClientOptions { RequestTimeout = TimeSpan.FromSeconds(5) });

    [Fact]
    public void ExecuteWithSessionId_OnAnEstablishedSession_RunsInlineAndDoesNotAllocate()
    {
        using var cts = new CancellationTokenSource();
        var s = NewSessions(cts.Token, new GatedRpcProvider(), 0);
        var cs = s.NewClientSession(0);
        cs.SessionId = 7;
        cs.Established = true;
        s.SessionsByShard[0] = cs;

        long seenShard = -1, seenSession = -1;
        int calls = 0;
        Func<long> shardForKey = static () => 0;
        Action<long, long, Exception?> callback = (shard, session, err) =>
        {
            seenShard = shard;
            seenSession = session;
            calls++;
            Assert.Null(err);
        };

        s.ExecuteWithSessionId(shardForKey, callback);   // inline: nothing to wait for
        Assert.Equal(1, calls);
        Assert.Equal((0L, 7L), (seenShard, seenSession));

        for (int i = 0; i < 1_000; i++) s.ExecuteWithSessionId(shardForKey, callback);   // warm-up
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 10_000; i++) s.ExecuteWithSessionId(shardForKey, callback);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.Equal(0, allocated);
        Assert.Equal(11_001, calls);
        cts.Cancel();
    }

    [Fact]
    public async Task ExecuteWithSessionId_DoesNotBlockWhileTheSessionIsBeingCreated_AndRunsTheCallbacksInOrder()
    {
        var provider = new GatedRpcProvider();
        using var cts = new CancellationTokenSource();
        var s = NewSessions(cts.Token, provider, 1, 2);

        var order = new List<string>();
        var allDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        void Record(string name, long shard, long session, Exception? err)
        {
            Assert.Null(err);
            lock (order)
            {
                order.Add($"{name}:{shard}:{session}");
                if (order.Count == 4) allDone.SetResult();
            }
        }

        // The first operation starts creating the session of shard 1; the following ones -- also those on shard 2 -- wait
        // behind it (Go: they wait for the sessions lock that it holds)
        var returned = Task.Run(() =>
        {
            s.ExecuteWithSessionId(() => 1, (sh, id, e) => Record("A", sh, id, e));
            s.ExecuteWithSessionId(() => 1, (sh, id, e) => Record("B", sh, id, e));
            s.ExecuteWithSessionId(() => 2, (sh, id, e) => Record("C", sh, id, e));
            s.ExecuteWithSessionId(() => 1, (sh, id, e) => Record("D", sh, id, e));
        });
        await Wait.WithTimeout(returned, Ten, "ExecuteWithSessionId blocked on the session being created");   // returns promptly
        await Wait.EventuallyAsync(() => provider.CreateCount() == 1, Ten, TimeSpan.FromMilliseconds(5));
        lock (order) Assert.Empty(order);

        provider.Open(1);
        await Task.Delay(100);
        // C needs the session of shard 2, whose creation only starts now that A and B ran
        await Wait.EventuallyAsync(() => provider.CreateCount() == 2, Ten, TimeSpan.FromMilliseconds(5));
        lock (order) Assert.Equal(new[] { "A:1:101", "B:1:101" }, order);
        provider.Open(2);

        await Wait.WithTimeout(allDone.Task, Ten, "the queued operations did not run");
        lock (order) Assert.Equal(new[] { "A:1:101", "B:1:101", "C:2:102", "D:1:101" }, order);
        Assert.Equal(new long[] { 1, 2 }, provider.CreateRequests.ToArray());
        cts.Cancel();
    }

    [Fact]
    public async Task ExecuteWithSessionId_ManyConcurrentCallers_CreateOneSessionPerShard()
    {
        var provider = new GatedRpcProvider();
        using var cts = new CancellationTokenSource();
        var s = NewSessions(cts.Token, provider, 0, 1);

        const int callers = 200;
        var results = new List<(long Shard, long Session)>();
        var allDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var tasks = new List<Task>();
        for (int i = 0; i < callers; i++)
        {
            long shard = i % 2;
            tasks.Add(Task.Run(() => s.ExecuteWithSessionId(() => shard, (sh, id, err) =>
            {
                Assert.Null(err);
                lock (results)
                {
                    results.Add((sh, id));
                    if (results.Count == callers) allDone.SetResult();
                }
            })));
        }
        await Task.WhenAll(tasks);
        provider.Open(0);
        provider.Open(1);
        await Wait.WithTimeout(allDone.Task, Ten, "not every caller got its session");

        Assert.Equal(callers / 2, results.Count(r => r.Shard == 0 && r.Session == 100));
        Assert.Equal(callers / 2, results.Count(r => r.Shard == 1 && r.Session == 101));
        Assert.Equal(new long[] { 0, 1 }, provider.CreateRequests.OrderBy(x => x).ToArray());
        cts.Cancel();
    }

    [Fact]
    public async Task ClosingTheSessionsDropsTheOperationsWaitingForASession_AndDoesNotHang()
    {
        var provider = new GatedRpcProvider();   // never opened: the session is never established
        using var cts = new CancellationTokenSource();
        var s = NewSessions(cts.Token, provider, 0);
        int invoked = 0;

        s.ExecuteWithSessionId(() => 0, (_, _, _) => Interlocked.Increment(ref invoked));
        s.ExecuteWithSessionId(() => 0, (_, _, _) => Interlocked.Increment(ref invoked));
        await Wait.EventuallyAsync(() => provider.CreateCount() == 1, Ten, TimeSpan.FromMilliseconds(5));

        await Wait.WithTimeout(s.CloseAsync().AsTask(), Ten, "Close hung on an operation waiting for a session");

        // Go: the callbacks of a closed (cancelled) session are not invoked
        await Task.Delay(200);
        Assert.Equal(0, Volatile.Read(ref invoked));
        // ... and the session is closed on its leader even though it was never established
        Assert.Equal(new[] { (0L, 0L) }, provider.Closed.ToArray());
    }

    [Fact]
    public async Task AKeepAliveFailingWithSessionNotFound_ForgetsTheSession_AndTheNextOperationCreatesAFreshOne()
    {
        var provider = new GatedRpcProvider { Gated = false, KeepAliveErr = OxiaErrors.SessionNotFound };
        using var cts = new CancellationTokenSource();
        var s = NewSessions(cts.Token, provider, 0);

        var first = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        s.ExecuteWithSessionId(() => 0, (_, id, _) => first.TrySetResult(id));
        Assert.Equal(100, await Wait.WithTimeout(first.Task, Ten, "no session"));
        Assert.True(s.SessionsByShard.ContainsKey(0));

        // The first heartbeat comes after the keep-alive ticker (at least 2s) and fails: the session is no longer valid
        await Wait.EventuallyAsync(() => !s.SessionsByShard.ContainsKey(0), TimeSpan.FromSeconds(15), TimeSpan.FromMilliseconds(20),
            "the session was not forgotten");

        var second = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        s.ExecuteWithSessionId(() => 0, (_, id, _) => second.TrySetResult(id));
        Assert.Equal(100, await Wait.WithTimeout(second.Task, Ten, "no fresh session"));
        Assert.Equal(2, provider.CreateCount());
        cts.Cancel();
    }

    [Fact]
    public async Task Close_ClosesEverySession_AndCombinesTheErrors()
    {
        var provider = new GatedRpcProvider { Gated = false };
        using var cts = new CancellationTokenSource();
        var s = NewSessions(cts.Token, provider, 0, 1, 2);
        foreach (long shard in new long[] { 0, 1, 2 })
        {
            var done = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
            s.ExecuteWithSessionId(() => shard, (_, id, _) => done.TrySetResult(id));
            await Wait.WithTimeout(done.Task, Ten, "no session");
        }
        var errA = new Exception("close failed on shard 0");
        var errB = new Exception("close failed on shard 2");
        provider.CloseErr = shard => shard switch { 0 => errA, 2 => errB, _ => null };

        var err = await Assert.ThrowsAsync<AggregateException>(async () => await s.CloseAsync());

        Assert.Equal(new Exception[] { errA, errB }.OrderBy(e => e.Message), err.InnerExceptions.OrderBy(e => e.Message));
        Assert.Equal(3, provider.Closed.Count);   // every session was closed even though some failed
    }

    [Fact]
    public async Task Close_WithASingleFailure_ThrowsThatFailure()
    {
        var provider = new GatedRpcProvider { Gated = false };
        using var cts = new CancellationTokenSource();
        var s = NewSessions(cts.Token, provider, 0);
        var done = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        s.ExecuteWithSessionId(() => 0, (_, id, _) => done.TrySetResult(id));
        await Wait.WithTimeout(done.Task, Ten, "no session");
        var errA = new Exception("close failed");
        provider.CloseErr = _ => errA;

        var err = await Assert.ThrowsAnyAsync<Exception>(async () => await s.CloseAsync());

        Assert.Same(errA, err);
    }
}

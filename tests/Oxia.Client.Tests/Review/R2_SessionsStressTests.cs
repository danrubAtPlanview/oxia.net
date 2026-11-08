// =====================================================================================================================
// R2 review (fidelity) -- Sessions.ExecuteWithSessionId (non blocking, FIFO queue behind a session being created).
//
// Randomised stress, many short rounds: caller threads issue sequences of operations against a Sessions whose CreateSession takes a
// random time and fails (retryably) at random, while the shard splits at a random moment (so it may happen before, during or after
// the creation of the session of the parent; a child splits again) and, in the second test, while the sessions are closed.
// Invariants (in Go all of them follow from the sessions lock being held across the callback):
//   * every operation's callback runs EXACTLY ONCE (after Close: at most once -- Go drops the callbacks of cancelled sessions);
//   * the callbacks of the operations issued by one caller run in the order they were issued;
//   * no deadlock; Close returns.
// (Go also serialises the callbacks of different callers under the sessions lock; the C# fast path deliberately does not, see
// the PORT NOTE of Sessions.cs, so that is not asserted.)
// =====================================================================================================================
using System.Collections.Concurrent;
using Grpc.Core;
using Oxia.Client.Tests.F1Support;
using Oxia.Common.Proto;
using Xunit;

namespace Oxia.Client.Tests.Review;

public class R2_SessionsStressTests
{
    private static readonly TimeSpan Ten = TimeSpan.FromSeconds(10);

    private sealed class RandomRpc : UnimplementedRpcProvider
    {
        private readonly Random _rng;
        private readonly double _failureRate;
        private readonly int _maxDelayMs;
        private long _ids;
        public readonly ConcurrentDictionary<long, int> SuccessfulCreates = new();
        public int Closed;

        public RandomRpc(int seed, double failureRate, int maxDelayMs)
        {
            _rng = new Random(seed);
            _failureRate = failureRate;
            _maxDelayMs = maxDelayMs;
        }

        private int Next(int max)
        {
            lock (_rng) return _rng.Next(max);
        }

        public override async ValueTask<CreateSessionResponse> CreateSession(CancellationToken ct, string target, CreateSessionRequest request)
        {
            int delay = Next(_maxDelayMs + 1);
            if (delay > 0) await Task.Delay(delay, ct).ConfigureAwait(false);
            else await Task.Yield();
            if (Next(1000) < _failureRate * 1000) throw new RpcException(new Grpc.Core.Status(StatusCode.Unavailable, "injected"));
            SuccessfulCreates.AddOrUpdate(request.Shard, 1, (_, n) => n + 1);
            return new CreateSessionResponse { SessionId = Interlocked.Increment(ref _ids) };
        }

        public override ValueTask<KeepAliveResponse> KeepAlive(CancellationToken ct, string target, SessionHeartbeat request) =>
            ValueTask.FromResult(new KeepAliveResponse());

        public override ValueTask<CloseSessionResponse> CloseSession(CancellationToken ct, string target, CloseSessionRequest request)
        {
            Interlocked.Increment(ref Closed);
            return ValueTask.FromResult(new CloseSessionResponse());
        }
    }

    private sealed record Seen(int Thread, int Seq, long Shard, long Session, Exception? Err, long Order);

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    public async Task EveryCallbackRunsExactlyOnceAndInOrderPerCaller(int seed)
    {
        const int threads = 4;
        const int perThread = 60;
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(3);
        var master = new Random(seed);
        long sessionsCreated = 0;
        long queuedRounds = 0;
        int rounds = 0;

        for (int round = 0; round < 150 && DateTime.UtcNow < deadline; round++, rounds++)
        {
            var rpc = new RandomRpc(master.Next(), failureRate: 0.1, maxDelayMs: 12);
            var sm = new SplittableShardManager("leader:6648");
            using var cts = new CancellationTokenSource();
            var s = new Sessions(cts.Token, sm, rpc, new ClientOptions { RequestTimeout = TimeSpan.FromSeconds(5) });

            long order = 0;
            var seen = new ConcurrentQueue<Seen>();
            using var go = new ManualResetEventSlim(false);

            var callers = new Task[threads];
            for (int t = 0; t < threads; t++)
            {
                int thread = t;
                int callerSeed = master.Next();
                callers[t] = Task.Factory.StartNew(() =>
                {
                    var rng = new Random(callerSeed);
                    go.Wait();
                    for (int i = 0; i < perThread; i++)
                    {
                        int seq = i;
                        string key = $"key-{thread}-{rng.Next(50)}";
                        s.ExecuteWithSessionId(() => sm.Get(key), (shard, session, err) =>
                        {
                            seen.Enqueue(new Seen(thread, seq, shard, session, err, Interlocked.Increment(ref order)));
                        });
                        if (rng.Next(8) == 0) Thread.Sleep(rng.Next(0, 3));
                    }
                }, TaskCreationOptions.LongRunning);
            }

            go.Set();
            int splitAfter = master.Next(0, 25);
            if (splitAfter > 0) await Task.Delay(splitAfter);
            sm.Split(0, 1, 2);
            if (master.Next(2) == 0)
            {
                await Task.Delay(master.Next(0, 10));
                sm.Split(1, 3, 4);
            }

            await Task.WhenAll(callers);
            await Wait.EventuallyAsync(() => seen.Count >= threads * perThread, Ten, TimeSpan.FromMilliseconds(5),
                $"round {round}: only {seen.Count} of {threads * perThread} callbacks ran (a lost wake-up?)");
            await Task.Delay(10);

            var all = seen.ToArray();
            Assert.True(all.Length == threads * perThread, $"round {round}: {all.Length} callbacks for {threads * perThread} operations");
            Assert.All(all, x => Assert.True(x.Err is null, $"round {round}: callback with error: {x.Err}"));
            Assert.All(all, x => Assert.True(x.Session > 0));
            for (int t = 0; t < threads; t++)
            {
                var mine = all.Where(x => x.Thread == t).OrderBy(x => x.Order).Select(x => x.Seq).ToArray();
                Assert.True(Enumerable.Range(0, perThread).SequenceEqual(mine), $"round {round}: caller {t} saw its callbacks out of order");
            }

            Assert.All(rpc.SuccessfulCreates, kv => Assert.True(kv.Value == 1, $"round {round}: shard {kv.Key}: {kv.Value} sessions created"));
            sessionsCreated += rpc.SuccessfulCreates.Count;
            if (all.Select(a => a.Shard).Distinct().Count() > 1) queuedRounds++;

            await Wait.WithTimeout(s.CloseAsync().AsTask(), Ten, $"round {round}: Close hung");
            cts.Cancel();
        }

        // the scenario must have been exercised: sessions were created on the children as well
        Assert.True(rounds >= 15 && sessionsCreated >= rounds && queuedRounds >= rounds / 5,
            $"weak run: {rounds} rounds, {sessionsCreated} sessions, {queuedRounds} rounds with children");
    }

    [Theory]
    [InlineData(11)]
    [InlineData(12)]
    public async Task CloseWhileOperationsAreQueued_NeverRunsACallbackTwice_AndNeverHangs(int seed)
    {
        const int threads = 4;
        const int perThread = 100;
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(2.5);
        var master = new Random(seed);

        for (int round = 0; round < 150 && DateTime.UtcNow < deadline; round++)
        {
            var rpc = new RandomRpc(master.Next(), failureRate: 0.3, maxDelayMs: 8);
            var sm = new SplittableShardManager("leader:6648");
            using var cts = new CancellationTokenSource();
            var s = new Sessions(cts.Token, sm, rpc, new ClientOptions { RequestTimeout = TimeSpan.FromSeconds(5) });

            var counts = new int[threads * perThread];
            var failures = new ConcurrentQueue<Exception>();
            using var go = new ManualResetEventSlim(false);
            var callers = new Task[threads];
            for (int t = 0; t < threads; t++)
            {
                int thread = t;
                int callerSeed = master.Next();
                callers[t] = Task.Factory.StartNew(() =>
                {
                    var rng = new Random(callerSeed);
                    go.Wait();
                    for (int i = 0; i < perThread; i++)
                    {
                        int slot = thread * perThread + i;
                        string key = $"key-{thread}-{rng.Next(50)}";
                        try
                        {
                            s.ExecuteWithSessionId(() => sm.Get(key), (_, _, _) => Interlocked.Increment(ref counts[slot]));
                        }
                        catch (Exception e)
                        {
                            failures.Enqueue(e);
                        }

                        if (rng.Next(8) == 0) Thread.Sleep(rng.Next(0, 3));
                    }
                }, TaskCreationOptions.LongRunning);
            }

            go.Set();
            await Task.Delay(master.Next(0, 20));
            if (master.Next(2) == 0) sm.Split(0, 1, 2);
            if (master.Next(2) == 0) await Task.Delay(master.Next(0, 5));
            await Wait.WithTimeout(s.CloseAsync().AsTask(), Ten, $"round {round}: Close hung while operations were queued");
            await Wait.WithTimeout(Task.WhenAll(callers), Ten, $"round {round}: an ExecuteWithSessionId call hung after Close");
            await Task.Delay(30);

            Assert.True(failures.IsEmpty, $"round {round}: ExecuteWithSessionId threw {failures.FirstOrDefault()}");
            Assert.All(counts, c => Assert.True(c <= 1, $"round {round}: a callback ran more than once"));
            cts.Cancel();
        }
    }
}

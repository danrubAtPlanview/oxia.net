// =====================================================================================================================
// PORT NOTE (wave 2 Z2 tests -- no Go counterpart)
//   Upstream : oxia/internal/batch/write_manager.go (writeGate.add: Go's Put blocks inside Add, so one goroutine's writes are added in
//              order), oxia/options_client.go (the EXTENSION WithBatchQueueCapacity), oxia/internal/rpc_provider.go (executeWriteStream)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : (1) the FIFO ticket chain of the write gate: writes that cannot take the fast path are added in call order -- also the ones a
//              retired gate forwards, also when one of them fails -- and the fast path reopens once the chain drained; (2) the queue capacity
//              default / option; (3) a request timeout is honoured while the first send of a write stream still waits for the connection;
//              (4) the parked-add path of the batcher allocates (almost) nothing.
// =====================================================================================================================
using System.Collections.Concurrent;
using System.Diagnostics;
using Grpc.Net.Client;
using Oxia.Client.Batch;
using Oxia.Client.Internal;
using Oxia.Client.Internal.Batch;
using Oxia.Common.Proto;
using Xunit;
using Barrier = Oxia.Client.Batch.Barrier;
using F1 = Oxia.Client.Tests.F1Support;

namespace Oxia.Client.Tests.Throughput;

// In the non-parallel collection: ParkingOnAFullQueue_AllocatesNothing_OnceWarm reads the PROCESS-WIDE allocation counter.
[Collection("Allocations")]
public class GateAndOptionsTests
{
    private static readonly TimeSpan Ten = TimeSpan.FromSeconds(10);

    private sealed class RecordingBatcher : IBatcher
    {
        public readonly ConcurrentQueue<object> Added = new();
        public readonly ConcurrentQueue<Barrier> Barriers = new();
        public Func<object, ValueTask>? OnAdd;

        public ValueTask AddAsync(object call)
        {
            if (call is Barrier b)
            {
                Barriers.Enqueue(b);
                return default;
            }
            if (OnAdd is { } hook)
            {
                var t = hook(call);
                Added.Enqueue(call);
                return t;
            }
            Added.Enqueue(call);
            return default;
        }

        public Task RunAsync() => Task.CompletedTask;
        public void Dispose() { }
    }

    // ---- 1. the ticket chain --------------------------------------------------------------------------------------------

    [Fact]
    public async Task WritesForwardedByARetiredGate_AreForwardedInCallOrder()
    {
        for (int iteration = 0; iteration < 100; iteration++)
        {
            var forwarded = new ConcurrentQueue<int>();
            var batchers = new ConcurrentDictionary<long, RecordingBatcher>();
            var manager = new WriteManager(CancellationToken.None, (_, id) => batchers[id] = new RecordingBatcher(),
                async (_, call) =>
                {
                    // forwarding takes a random time: later writes must still not overtake earlier ones
                    if (Random.Shared.Next(3) == 0) await Task.Yield();
                    forwarded.Enqueue((int)call);
                });

            await manager.AddAsync(0, "seed");
            manager.ShardsReplaced(new Dictionary<long, long[]> { [0] = [1] }); // shard 0 is retired: its gate forwards
            var tasks = new List<ValueTask>();
            for (int i = 0; i < 40; i++) tasks.Add(manager.AddAsync(0, i));
            foreach (var t in tasks) await t.AsTask().WaitAsync(Ten);

            Assert.Equal(Enumerable.Range(0, 40), forwarded.ToArray());
            manager.Dispose();
        }
    }

    [Fact]
    public async Task AFailingSlowPathAdd_FailsOnlyItself_AndTheNextOnesStillGoInOrder()
    {
        var batchers = new ConcurrentDictionary<long, RecordingBatcher>();
        var manager = new WriteManager(CancellationToken.None, (_, id) => batchers[id] = new RecordingBatcher(), (_, _) => { });
        await manager.AddAsync(0, -1);
        manager.ShardsReplaced(new Dictionary<long, long[]> { [0] = [1] });
        var removed = batchers[0];
        while (removed.Barriers.IsEmpty) await Task.Yield();

        // the successor holds: the writes take tickets
        var adds = new ValueTask[6];
        batchers[1].OnAdd = call => (int)call == 2 ? throw new InvalidOperationException("no room") : default;
        for (int i = 0; i < adds.Length; i++) adds[i] = manager.AddAsync(1, i);
        removed.Barriers.TryDequeue(out var barrier);
        barrier!.Done(); // released

        for (int i = 0; i < adds.Length; i++)
        {
            if (i == 2) await Assert.ThrowsAsync<InvalidOperationException>(async () => await adds[i]);
            else await adds[i].AsTask().WaitAsync(Ten);
        }
        Assert.Equal([0, 1, 3, 4, 5], batchers[1].Added.Cast<int>().ToArray().Where(x => x >= 0)
            .ToArray()); // call 2 was refused by the batcher hook before it was recorded
        manager.Dispose();
    }

    [Fact]
    public async Task OnceTheChainDrained_TheFastPathReopens()
    {
        var batchers = new ConcurrentDictionary<long, RecordingBatcher>();
        var manager = new WriteManager(CancellationToken.None, (_, id) => batchers[id] = new RecordingBatcher(), (_, _) => { });
        await manager.AddAsync(0, -1);
        manager.ShardsReplaced(new Dictionary<long, long[]> { [0] = [1] });
        var removed = batchers[0];
        while (removed.Barriers.IsEmpty) await Task.Yield();

        var held = manager.AddAsync(1, 0);
        Assert.False(held.IsCompleted);
        removed.Barriers.TryDequeue(out var barrier);
        barrier!.Done();
        await held.AsTask().WaitAsync(Ten);

        // let the ticket bookkeeping finish, then a write completes synchronously again
        await F2.F2Util.EventuallyAsync(() => manager.AddAsync(1, 99).IsCompletedSuccessfully, Ten, TimeSpan.FromMilliseconds(2), "the fast path never reopened");
        manager.Dispose();
    }

    // ---- 2. queue capacity ----------------------------------------------------------------------------------------------

    [Theory]
    [InlineData(1, 4096)]
    [InlineData(1000, 4096)]
    [InlineData(5000, 5000)]
    [InlineData(1_000_000, 65_536)]
    public void TheDefaultQueueCapacity_HoldsAFullBatch_AndIsBounded(int maxRequestsPerBatch, int expected)
    {
        Assert.Equal(expected, Oxia.Client.Internal.Batch.BatcherFactory.DefaultQueueCapacity(maxRequestsPerBatch));
    }

    [Fact]
    public void WithBatchQueueCapacity_IsValidated_AndApplied()
    {
        Assert.Null(ClientOptions.TryCreate("a:1", [OxiaOptions.WithBatchQueueCapacity(10)], out var options));
        Assert.Equal(10, options.BatchQueueCapacity);

        var err = ClientOptions.TryCreate("a:1", [OxiaOptions.WithBatchQueueCapacity(0)], out var rejected);
        Assert.True(Oxia.Common.Constant.OxiaErrors.Is(err, ClientErrors.InvalidOptionBatchQueueCapacity));
        Assert.Equal(0, rejected.BatchQueueCapacity); // the rejected option left the default
    }

    // ---- 3. the request timeout while the connection is being made -----------------------------------------------------

    [Fact]
    public async Task ARequestTimeout_IsHonoured_WhileTheFirstSendWaitsForTheConnection()
    {
        // The connection attempt hangs for good (a black-holed address): only the request token can end the write
        var shardManager = new F1.StaticShardManager { Shards = [0], LeaderAddress = "blackhole.invalid:6648" };
        await using var rpc = RpcProvider.Create(CancellationToken.None, "default", null, null, "blackhole.invalid:6648", () => shardManager,
            static (GrpcChannelOptions options) => options.HttpHandler = new SocketsHttpHandler
            {
                ConnectCallback = static async (_, ct) =>
                {
                    await Task.Delay(Timeout.Infinite, ct).ConfigureAwait(false);
                    throw new InvalidOperationException("unreachable");
                },
            });

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));
        var sw = Stopwatch.StartNew();
        var ex = await Record.ExceptionAsync(async () =>
            await rpc.ExecuteWrite(cts.Token, 0, static () => new WriteRequest { Shard = 0 }).AsTask().WaitAsync(Ten));
        sw.Stop();

        Assert.NotNull(ex);
        Assert.False(ex is TimeoutException, "the write was still waiting for the connection after 10 s: the request timeout was not honoured");
        Assert.True(Oxia.Common.Constant.OxiaErrors.IsDeadlineExceeded(ex) || ex is OperationCanceledException || Oxia.Common.Constant.OxiaErrors.IsCanceled(ex),
            ex.ToString());
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(5), $"took {sw.Elapsed}");
    }

    // ---- 4. allocation of the parked path -------------------------------------------------------------------------------

    [Fact]
    public async Task ParkingOnAFullQueue_AllocatesNothing_OnceWarm()
    {
#if DEBUG
        await Task.CompletedTask;
        return; // allocation figures are meaningless in Debug
#else
        var queue = new BatcherQueue(1, static _ => { });
        async Task Round()
        {
            Assert.Equal(AddStatus.Queued, queue.TryAdd("a", out _));
            Assert.Equal(AddStatus.Parked, queue.TryAdd("b", out var parked));
            Assert.True(queue.TryDequeue(out _)); // frees the room: "b" is moved in and its producer is woken on the pool
            await parked!.AsValueTask();
            Assert.True(queue.TryDequeue(out _));
        }

        for (int i = 0; i < 2000; i++) await Round(); // warm up: pools, thread pool queues, JIT
        long best = long.MaxValue;
        for (int attempt = 0; attempt < 8; attempt++)
        {
            // Process-wide counter: the continuation after `await parked` runs on arbitrary pool threads, so a per-thread counter sampled
            // before and after the loop could belong to two different threads (a meaningless difference).  The class runs in the serial
            // "Allocations" collection, so nothing else allocates meanwhile.
            long before = GC.GetTotalAllocatedBytes(precise: true);
            for (int i = 0; i < 1000; i++) await Round();
            long bytes = GC.GetTotalAllocatedBytes(precise: true) - before;
            best = Math.Min(best, bytes);
        }
        Assert.True(best <= 1000 * 16, $"{best / 1000.0:0.0} B per parked add (best of 8 rounds of 1000)");
#endif
    }
}

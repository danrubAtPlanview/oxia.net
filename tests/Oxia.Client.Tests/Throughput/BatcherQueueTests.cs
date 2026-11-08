// =====================================================================================================================
// PORT NOTE (wave 2 Z2 tests -- no Go counterpart)
//   Upstream : oxia/batch/batcher.go (`callC` is a buffered Go channel); src/Oxia.Client/Batch/BatcherQueue.cs replaces the
//              System.Threading.Channels queue of the batcher with a lock-free bounded MPSC ring.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : What a buffered channel guarantees, for the custom queue: exact capacity, FIFO (also for the producers parked on a full
//              queue), a sleeping consumer is woken by an add / a tick / the close, exactly-once delivery under many producers (per
//              producer order preserved), Close fails the parked producers and drains the reserved items, nothing is lost or
//              duplicated when Close races with the adds.
// =====================================================================================================================
using System.Collections.Concurrent;
using Oxia.Client.Batch;
using Xunit;

namespace Oxia.Client.Tests.Throughput;

public class BatcherQueueTests
{
    private static readonly TimeSpan Ten = TimeSpan.FromSeconds(10);

    private static BatcherQueue NewQueue(int capacity, ConcurrentQueue<object>? failed = null) =>
        new(capacity, call => failed?.Enqueue(call));

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(22)]
    [InlineData(1000)]
    public async Task HoldsExactlyCapacityItems_ThenParks(int capacity)
    {
        var q = NewQueue(capacity);
        for (int i = 0; i < capacity; i++)
        {
            Assert.Equal(AddStatus.Queued, q.TryAdd(i, out var p));
            Assert.Null(p);
        }
        Assert.Equal(capacity, q.Count);

        Assert.Equal(AddStatus.Parked, q.TryAdd("parked", out var parked));
        Assert.NotNull(parked);
        var parkedTask = parked.AsValueTask();
        Assert.False(parkedTask.IsCompleted);
        Assert.Equal(capacity, q.Count); // the parked one is not counted

        // freeing one slot moves the parked item in
        Assert.True(q.TryDequeue(out var first));
        Assert.Equal(0, first);
        Assert.Equal(capacity, q.Count);
        await parkedTask.AsTask().WaitAsync(Ten); // woken on the thread pool
    }

    [Fact]
    public async Task ParkedProducers_AreServedInOrder_AndNewcomersQueueBehindThem()
    {
        var q = NewQueue(2);
        Assert.Equal(AddStatus.Queued, q.TryAdd(0, out _));
        Assert.Equal(AddStatus.Queued, q.TryAdd(1, out _));
        var waits = new List<ValueTask>();
        for (int i = 2; i < 12; i++)
        {
            Assert.Equal(AddStatus.Parked, q.TryAdd(i, out var p));
            waits.Add(p!.AsValueTask());
        }

        // a newcomer after the parked ones must not overtake them even when room appears
        Assert.True(q.TryDequeue(out var item));
        Assert.Equal(0, item);
        Assert.Equal(AddStatus.Parked, q.TryAdd(100, out var late));

        var drained = new List<int> { 0 };
        while (q.TryDequeue(out item)) drained.Add((int)item!);
        // everything queued so far plus the served parked items came out in order; 100 is last
        foreach (var w in waits) await w.AsTask().WaitAsync(Ten);
        await late!.AsValueTask().AsTask().WaitAsync(Ten);
        while (q.TryDequeue(out item)) drained.Add((int)item!);
        Assert.Equal(Enumerable.Range(0, 12).Append(100).ToList(), drained);
    }

    [Fact]
    public async Task ASleepingConsumer_IsWokenByAnAdd_ATick_AndTheClose()
    {
        var q = NewQueue(4);
        var wait = q.WaitToReadAsync();
        Assert.False(wait.IsCompleted);
        Assert.Equal(AddStatus.Queued, q.TryAdd("x", out _));
        Assert.True(await wait.AsTask().WaitAsync(Ten));
        Assert.True(q.TryDequeue(out var x));
        Assert.Equal("x", x);

        wait = q.WaitToReadAsync();
        Assert.False(wait.IsCompleted);
        q.Tick();
        Assert.True(await wait.AsTask().WaitAsync(Ten));
        Assert.False(q.TryDequeue(out _)); // a tick carries no item

        // a tick before the wait is not lost
        q.Tick();
        Assert.True(q.WaitToReadAsync().IsCompletedSuccessfully);

        wait = q.WaitToReadAsync();
        Assert.False(wait.IsCompleted);
        q.Complete();
        await wait.AsTask().WaitAsync(Ten); // woken
        Assert.False(await q.WaitToReadAsync().AsTask().WaitAsync(Ten)); // closed and drained
    }

    [Fact]
    public async Task Close_FailsTheParkedProducers_AndRefusesNewOnes_AndTheReservedItemsAreDrained()
    {
        var failed = new ConcurrentQueue<object>();
        var q = NewQueue(2, failed);
        q.TryAdd("a", out _);
        q.TryAdd("b", out _);
        q.TryAdd("p1", out var p1);
        q.TryAdd("p2", out var p2);

        q.Complete();
        q.Complete(); // idempotent
        await p1!.AsValueTask().AsTask().WaitAsync(Ten);
        await p2!.AsValueTask().AsTask().WaitAsync(Ten);
        Assert.Equal(["p1", "p2"], failed.Cast<string>().ToArray());

        Assert.Equal(AddStatus.Closed, q.TryAdd("late", out _));
        Assert.False(q.IsDrained);
        Assert.Equal("a", q.DequeueClosed());
        Assert.Equal("b", q.DequeueClosed());
        Assert.Null(q.DequeueClosed());
        Assert.True(q.IsDrained);
    }

    [Theory]
    [InlineData(3, 8, 20_000)]
    [InlineData(22, 16, 20_000)]
    [InlineData(4096, 32, 20_000)]
    public async Task ManyProducers_OneConsumer_ExactlyOnce_PerProducerOrder(int capacity, int producers, int perProducer)
    {
        var q = NewQueue(capacity);
        var consumed = new long[producers];
        long total = (long)producers * perProducer;

        var consumer = Task.Run(async () =>
        {
            long n = 0;
            var next = new int[producers];
            while (n < total)
            {
                if (q.TryDequeue(out var item))
                {
                    var (p, seq) = ((int, int))item;
                    Assert.Equal(next[p], seq); // FIFO per producer
                    next[p]++;
                    n++;
                    continue;
                }
                await q.WaitToReadAsync();
            }
            return n;
        });

        var tasks = new Task[producers];
        for (int p = 0; p < producers; p++)
        {
            int producer = p;
            tasks[p] = Task.Run(async () =>
            {
                for (int i = 0; i < perProducer; i++)
                {
                    object item = (producer, i);
                    var status = q.TryAdd(item, out var parked);
                    if (status == AddStatus.Parked) await parked!.AsValueTask();
                    else Assert.Equal(AddStatus.Queued, status);
                }
            });
        }

        await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(60));
        Assert.Equal(total, await consumer.WaitAsync(TimeSpan.FromSeconds(60)));
        Assert.Equal(0, q.Count);
        _ = consumed;
    }

    [Fact]
    public async Task OneProducer_IsNeverReordered_EvenWhenItDoesNotAwaitTheParkedAdds()
    {
        // PutAsync-style pipelining: the caller issues the next add before the previous (parked) one completed
        var q = NewQueue(2);
        const int n = 5000;
        var pending = new List<ValueTask>();
        var received = new List<int>();
        var consumer = Task.Run(async () =>
        {
            while (received.Count < n)
            {
                if (q.TryDequeue(out var item)) received.Add((int)item);
                else await q.WaitToReadAsync();
            }
        });
        for (int i = 0; i < n; i++)
        {
            if (q.TryAdd(i, out var parked) == AddStatus.Parked) pending.Add(parked!.AsValueTask());
        }
        foreach (var p in pending) await p.AsTask().WaitAsync(Ten);
        await consumer.WaitAsync(Ten);
        Assert.Equal(Enumerable.Range(0, n).ToList(), received);
    }

    [Fact]
    public async Task CloseRacingWithAdds_EveryItemIsConsumedOrFailedExactlyOnce()
    {
        for (int iteration = 0; iteration < 200; iteration++)
        {
            var failed = new ConcurrentQueue<object>();
            var q = NewQueue(1 + iteration % 7, failed);
            const int producers = 4, per = 200;
            var queued = new ConcurrentQueue<int>(); // items the producers saw accepted or parked
            var refused = new ConcurrentQueue<int>();
            var consumed = new ConcurrentQueue<int>();
            var stop = false;

            var consumer = Task.Run(async () =>
            {
                while (true)
                {
                    if (q.TryDequeue(out var item)) { consumed.Enqueue((int)item); continue; }
                    if (Volatile.Read(ref stop)) break;
                    if (!await q.WaitToReadAsync()) break;
                }
            });

            var tasks = Enumerable.Range(0, producers).Select(p => Task.Run(async () =>
            {
                for (int i = 0; i < per; i++)
                {
                    int id = p * per + i;
                    var status = q.TryAdd(id, out var parked);
                    if (status == AddStatus.Closed) refused.Enqueue(id);
                    else
                    {
                        queued.Enqueue(id);
                        if (status == AddStatus.Parked) await parked!.AsValueTask();
                    }
                }
            })).ToArray();

            await Task.Delay(iteration % 3);
            q.Complete();
            await Task.WhenAll(tasks).WaitAsync(Ten);
            Volatile.Write(ref stop, true);
            q.Tick();
            await consumer.WaitAsync(Ten);
            // whatever is still reserved is drained by the closing consumer
            while (q.DequeueClosed() is { } rest) consumed.Enqueue((int)rest);

            var all = consumed.Concat(failed.Cast<int>()).Concat(refused).OrderBy(x => x).ToArray();
            Assert.Equal(Enumerable.Range(0, producers * per).ToArray(), all);
        }
    }
}

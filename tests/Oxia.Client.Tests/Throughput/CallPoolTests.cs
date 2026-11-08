// =====================================================================================================================
// PORT NOTE (wave 2 Z2 tests -- no Go counterpart)
//   Upstream : none (oxia/async_client_impl.go allocates a channel + closure per operation; src/Oxia.Client/PooledCalls.cs pools them)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The pool of the call objects (per-thread cache + bounded lock-free queue): an object is never handed to two renters at
//              once (under heavy concurrent rent / return), the pool is bounded, and the pooled calls deliver their result to the
//              awaiter exactly once on a thread pool thread, whichever thread completed them.  The observer of a pending add fails the
//              call only when the add failed.
// =====================================================================================================================
using System.Collections.Concurrent;
using Oxia.Client.Internal.Model;
using Oxia.Common.Constant;
using Oxia.Common.Proto;
using Xunit;

namespace Oxia.Client.Tests.Throughput;

public class CallPoolTests
{
    // The thread cache of CallPool<T> is a static of the closed generic type (one pool instance per type in production), so every test that
    // creates its own pool uses its own item type.
    private class Item
    {
        public int Owner; // 0 = in the pool
    }

    private sealed class ItemA : Item;
    private sealed class ItemB : Item;
    private sealed class ItemC : Item;
    private sealed class ItemD : Item;

    [Fact]
    public void RentReturn_SingleThread_ReusesTheSameObject()
    {
        var pool = new CallPool<ItemA>(64);
        Assert.Null(pool.TryRent());
        var a = new ItemA();
        pool.Return(a);
        Assert.Same(a, pool.TryRent());
        Assert.Null(pool.TryRent());
    }

    [Fact]
    public async Task ThreadCache_SpillsToTheSharedQueue_AndAnotherThreadCanRentThem()
    {
        var pool = new CallPool<ItemB>(1000);
        var items = Enumerable.Range(0, 200).Select(_ => new ItemB()).ToArray();
        foreach (var item in items) pool.Return(item); // 32 stay in this thread's cache, the rest is spilled / bounded

        var rented = new ConcurrentBag<ItemB>();
        await Task.Run(() =>
        {
            ItemB? i;
            while ((i = pool.TryRent()) is not null) rented.Add(i);
        });

        Assert.True(rented.Count >= 100, $"only {rented.Count} items reached the other thread");
        Assert.Equal(rented.Count, rented.Distinct().Count());
    }

    [Fact]
    public void ThePoolIsBounded()
    {
        var pool = new CallPool<ItemC>(8);
        for (int i = 0; i < 1000; i++) pool.Return(new ItemC());
        int rented = 0;
        // everything this thread can see: its own cache (32) plus the shared queue (<= 8)
        while (pool.TryRent() is not null) rented++;
        Assert.InRange(rented, 1, 32 + 8 + 16);
    }

    [Fact]
    public async Task ConcurrentRentAndReturn_NeverGivesTheSameObjectToTwoRenters()
    {
        var pool = new CallPool<ItemD>(256);
        int violations = 0;
        var tasks = Enumerable.Range(0, 16).Select(t => Task.Run(() =>
        {
            for (int i = 0; i < 200_000; i++)
            {
                var item = pool.TryRent() ?? new ItemD();
                if (Interlocked.CompareExchange(ref item.Owner, t + 1, 0) != 0) Interlocked.Increment(ref violations);
                // sometimes hand the object to another thread to return it (the awaiter's thread differs from the renter's)
                if ((i & 7) == 0) Task.Run(() => { Volatile.Write(ref item.Owner, 0); pool.Return(item); });
                else
                {
                    Volatile.Write(ref item.Owner, 0);
                    pool.Return(item);
                }
            }
        })).ToArray();
        await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(60));
        await Task.Delay(100);
        Assert.Equal(0, violations);
    }

    // ---- the pooled calls ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task APooledPut_CompletedFromAnotherThread_ResumesTheAwaiterOnThePool_Once()
    {
        for (int round = 0; round < 200; round++)
        {
            var call = PooledPutCall.Rent();
            call.Key = "k";
            var task = call.AsValueTask();
            var completerThread = 0;
            var awaiterThread = -1;
            var awaiting = Task.Run(async () =>
            {
                await Task.Yield();
                var r = await task;
                awaiterThread = Environment.CurrentManagedThreadId;
                return r;
            });
            await Task.Yield();
            var completer = new Thread(() =>
            {
                completerThread = Environment.CurrentManagedThreadId;
                call.Complete(new PutResponse { Status = Status.Ok, Version = new ProtoVersion { VersionId = 7 } }, null);
                call.Complete(null, new InvalidOperationException("a second Complete must be ignored"));
            });
            completer.Start();
            completer.Join();
            var result = await awaiting.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Null(result.Err);
            Assert.Equal(7, result.Version.VersionId);
            Assert.NotEqual(completerThread, awaiterThread); // never inline on the completing thread
        }
    }

    [Fact]
    public async Task APooledPut_IsReusable_AfterItsResultWasConsumed()
    {
        var first = PooledPutCall.Rent();
        first.Key = "a";
        var t1 = first.AsValueTask();
        first.Complete(null, new OxiaException("boom"));
        var r1 = await t1;
        Assert.NotNull(r1.Err);

        // renting again (same thread: the thread cache) gives the recycled, reset object
        var second = PooledPutCall.Rent();
        Assert.Same(first, second);
        Assert.Null(second.Key == "a" ? second.Key : null); // Reset() cleared the request
        var t2 = second.AsValueTask();
        second.Key = "b";
        second.Complete(new PutResponse { Status = Status.Ok, Version = new ProtoVersion { VersionId = 1 } }, null);
        Assert.Null((await t2).Err);
    }

    // ---- AddObserver -------------------------------------------------------------------------------------------------

    [Fact]
    public async Task AddObserver_FailsTheCall_OnlyWhenTheAddFailed()
    {
        var okCall = PooledPutCall.Rent();
        okCall.Key = "ok";
        var okResult = okCall.AsValueTask();
        var okAdd = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        AddObserver.Observe(new ValueTask(okAdd.Task), okCall);
        okAdd.SetResult();
        await Task.Delay(50);
        Assert.False(okResult.IsCompleted); // a successful add leaves the call alone
        okCall.Complete(new PutResponse { Status = Status.Ok, Version = new ProtoVersion() }, null);
        Assert.Null((await okResult).Err);

        var badCall = PooledPutCall.Rent();
        badCall.Key = "bad";
        var badResult = badCall.AsValueTask();
        var badAdd = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        AddObserver.Observe(new ValueTask(badAdd.Task), badCall);
        badAdd.SetException(new InvalidOperationException("cannot queue"));
        var r = await badResult.AsTask().WaitAsync(TimeSpan.FromSeconds(10));
        Assert.IsType<InvalidOperationException>(r.Err);

        // an add that already failed synchronously
        var syncCall = PooledPutCall.Rent();
        var syncResult = syncCall.AsValueTask();
        AddObserver.Observe(new ValueTask(Task.FromException(new OxiaException("sync failure"))), syncCall);
        Assert.NotNull((await syncResult.AsTask().WaitAsync(TimeSpan.FromSeconds(10))).Err);
    }
}

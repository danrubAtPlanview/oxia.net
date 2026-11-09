// =====================================================================================================================
// PORT NOTE (tests)
//   Upstream : (none -- allocation budgets of the wave 2 / Z1 work, end to end)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The allocation budget of OUR side of the client, per operation, END TO END: a real AsyncClientImpl (options, batchers,
//              rpc provider, write stream wrapper, OxiaClientRpc, metrics interceptor) over FakeWireInvoker, which runs the real
//              serializers and answers through the real deserializers (so the pooled response graphs travel the real path), and itself
//              allocates almost nothing.  Measured process-wide (GC.GetTotalAllocatedBytes) over 100 000 operations after a warm-up that
//              lets the tiered JIT promote the hot paths; the best of 3 measurements counts (a GC or a tiering step can add noise, never
//              remove allocations).  Release builds only (Debug allocates differently): the tests skip themselves in Debug like the
//              other allocation tests.  The tests run alone ("Z1 serial"): the counter is process-wide.
//              Only 12 operations are in flight at a time, far below the batcher queue capacity (max(4096, MaxRequestsPerBatch), independent
//              of the core count): the slow path of AddAsync (a full queue) is the batcher's own cost and is not what is measured here.
//              Linux validation (L): the old "skip below 14 cores" guard was removed (stale since the queue capacity no longer follows
//              ProcessorCount; it disabled these budgets on 4-core ubuntu-latest runners).
//   Budgets  : every non-zero budget is justified at its test.  What the budgets do NOT contain, by design: Grpc.Net.Client and the HTTP/2
//              stack (about 2.3 KB per write-stream message, about 9 KB per Read / RangeScan call, shared by all the operations of a
//              batch -- see docs/performance-allocation.md and `dotnet run -c Release --project benchmarks/Oxia.Client.Benchmarks -- alloc`).
//   Divergences: n/a (new tests).
// =====================================================================================================================
using Oxia.Client.Pooled;
using Oxia.Common.Proto;
using Xunit;

namespace Oxia.Client.Tests.Alloc;

[Collection("Z1 serial")]
public class EndToEndAllocationTests(ITestOutputHelper output)
{
    private const int Window = 12;
    private const int Operations = 100_000;

    private static readonly byte[] Value = new byte[100];
    private static readonly string[] Keys = Enumerable.Range(0, Window).Select(i => "/bench/key-" + i.ToString("D6")).ToArray();

    private static void RequireOptimizedBuild()
    {
#if DEBUG
        Assert.Skip("allocation budgets only mean something in Release (dotnet test -c Release)");
#endif
        // Linux validation (L): this used to skip on machines with fewer than Window + 2 cores ("the batcher queues have ProcessorCount
        // slots").  That premise went stale when the client's queue capacity became max(4096, MaxRequestsPerBatch)
        // (Internal.Batch.BatcherFactory.DefaultQueueCapacity, which FakeWireClient uses): 12 operations in flight fit on any machine, and
        // the skip silently disabled all ten budgets on 4-core CI runners (ubuntu-latest) and small AWS instances.
    }

    /// <summary>
    /// Bytes allocated per operation (process-wide, minus what the fake transport itself allocated), best of three measurements after a warm-up.  <paramref name="rounds"/> runs the given
    /// number of rounds of the scenario inside ONE async method (so the test's own state machine is allocated once, not per round) and
    /// returns the number of operations it performed.
    /// </summary>
    private async Task<double> MeasureAsync(string name, Func<int, Task<long>> rounds, int opsPerRound, int operations = Operations, int warmupDivisor = 1, Func<long>? fakeBytes = null)
    {
        // warm-up: pools, connection state, JIT tiers (the tiering call counting starts after a quiet period: pause between the rounds)
        await rounds(400 / warmupDivisor);
        for (int pause = 0; pause < 4; pause++)
        {
            await rounds(300 / warmupDivisor);
            await Task.Delay(120);
        }

        double best = double.MaxValue;
        for (int attempt = 0; attempt < 3; attempt++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            long fakeBefore = fakeBytes?.Invoke() ?? 0;
            long before = GC.GetTotalAllocatedBytes(precise: true);
            long done = await rounds(Math.Max(1, operations / opsPerRound));
            long allocated = GC.GetTotalAllocatedBytes(precise: true) - before - ((fakeBytes?.Invoke() ?? 0) - fakeBefore);
            double perOp = allocated / (double)done;
            best = Math.Min(best, perOp);
            if (best <= 1) break;
        }
        output.WriteLine($"{name}: {best:F2} B/op");
        return best;
    }

    // ----- writes -------------------------------------------------------------------------------------------------------

    [Fact]
    public async Task Put_AllocatesAtMost16BytesPerOperation()
    {
        RequireOptimizedBuild();
        var wire = new FakeWireClient(Window, intercepted: true);
        var client = wire.Client;
        var tasks = new ValueTask<PutResult>[Window];

        async Task<long> Rounds(int rounds)
        {
            for (int round = 0; round < rounds; round++)
            {
                for (int i = 0; i < Window; i++) tasks[i] = client.PutAsync(Keys[i], Value);
                for (int i = 0; i < Window; i++)
                {
                    var r = await tasks[i];
                    if (r.Err is not null) throw r.Err;
                }
            }
            return (long)rounds * Window;
        }

        double perOp = await MeasureAsync("put", Rounds, Window, fakeBytes: () => wire.Invoker.FakeAllocatedBytes);
        await wire.CloseAsync();

        // Budget 16 B: the 12 puts of a batch share one write-stream message.  What is left per batch (about 30-90 B, i.e. 2-8 B/op) are
        // the boxes of the pooling async method builders that are rented on one thread and returned on another (the receive loop
        // completes the future, the batcher loop continues).  The pending read Task of the FAKE transport is subtracted.  The decoded
        // response (PutResponse + ProtoVersion + the lists: about 160 B/op before the response recycling) is pooled: 0.
        Assert.True(perOp <= 16, $"Put allocated {perOp:F1} B/op");
    }

    [Fact]
    public async Task Delete_AllocatesAtMost16BytesPerOperation()
    {
        RequireOptimizedBuild();
        var wire = new FakeWireClient(Window, intercepted: true);
        var client = wire.Client;
        var tasks = new ValueTask<Exception?>[Window];

        async Task<long> Rounds(int rounds)
        {
            for (int round = 0; round < rounds; round++)
            {
                for (int i = 0; i < Window; i++) tasks[i] = client.DeleteAsync(Keys[i]);
                for (int i = 0; i < Window; i++)
                {
                    var r = await tasks[i];
                    if (r is not null) throw r;
                }
            }
            return (long)rounds * Window;
        }

        double perOp = await MeasureAsync("delete", Rounds, Window, fakeBytes: () => wire.Invoker.FakeAllocatedBytes);
        await wire.CloseAsync();

        // Budget 16 B: same as Put
        Assert.True(perOp <= 16, $"Delete allocated {perOp:F1} B/op");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task DeleteRange_AllocatesAtMost16BytesPerOperation(bool withPartitionKey)
    {
        RequireOptimizedBuild();
        var wire = new FakeWireClient(Window, intercepted: true);
        var client = wire.Client;
        var tasks = new ValueTask<Exception?>[Window];

        async Task<long> Rounds(int rounds)
        {
            for (int round = 0; round < rounds; round++)
            {
                for (int i = 0; i < Window; i++)
                {
                    tasks[i] = withPartitionKey
                        ? client.DeleteRangeAsync("/a", "/b", OxiaOptions.PartitionKey("p"))
                        : client.DeleteRangeAsync("/a", "/b");
                }
                for (int i = 0; i < Window; i++)
                {
                    var r = await tasks[i];
                    if (r is not null) throw r;
                }
            }
            return (long)rounds * Window;
        }

        double perOp = await MeasureAsync(withPartitionKey ? "delete-range (partition key, one shard)" : "delete-range (all shards = one)", Rounds, Window, fakeBytes: () => wire.Invoker.FakeAllocatedBytes);
        await wire.CloseAsync();

        // Budget 16 B: before the pooled PooledDeleteRange (a wait-group channel + a closure + one call per shard) this was about 1.4 KB per op
        Assert.True(perOp <= 16, $"DeleteRange allocated {perOp:F1} B/op");
    }

    // ----- reads --------------------------------------------------------------------------------------------------------

    [Fact]
    public async Task GetNotFound_AllocatesAtMost12BytesPerOperation()
    {
        RequireOptimizedBuild();
        var wire = new FakeWireClient(Window);
        wire.Invoker.GetsNotFound = true;
        var client = wire.Client;
        var tasks = new ValueTask<GetResult>[Window];

        async Task<long> Rounds(int rounds)
        {
            for (int round = 0; round < rounds; round++)
            {
                for (int i = 0; i < Window; i++) tasks[i] = client.GetAsync(Keys[i]);
                for (int i = 0; i < Window; i++)
                {
                    var r = await tasks[i];
                    if (r.Err is null) throw new InvalidOperationException("expected KeyNotFound");
                }
            }
            return (long)rounds * Window;
        }

        double perOp = await MeasureAsync("get (not found)", Rounds, Window);
        await wire.CloseAsync();

        // Budget 12 B: a Read is one server-streaming CALL per batch, and the object of a call that is ours -- the ServerStreamingClient
        // wrapper (64 B) -- is 5 B/op at 12 gets per call (measured 5-6 B/op).  The response graph (ReadResponse + GetResponse list:
        // about 200 B/op before) is pooled.
        Assert.True(perOp <= 12, $"Get (not found) allocated {perOp:F1} B/op");
    }

    [Fact]
    public async Task GetFound_AllocatesOnlyTheValueArray()
    {
        RequireOptimizedBuild();
        var wire = new FakeWireClient(Window);
        var client = wire.Client;
        var tasks = new ValueTask<GetResult>[Window];

        async Task<long> Rounds(int rounds)
        {
            for (int round = 0; round < rounds; round++)
            {
                for (int i = 0; i < Window; i++) tasks[i] = client.GetAsync(Keys[i]);
                for (int i = 0; i < Window; i++)
                {
                    var r = await tasks[i];
                    if (r.Err is not null || r.Value!.Length != 100) throw new InvalidOperationException("unexpected result");
                }
            }
            return (long)rounds * Window;
        }

        double perOp = await MeasureAsync("get (found, faithful API)", Rounds, Window);
        await wire.CloseAsync();

        // The faithful API hands the user a byte[]: 100 bytes + 24 bytes of array header = 124 B/op is REQUIRED; the rest (12 B) is the
        // same per-call overhead as the not-found case.
        Assert.True(perOp <= 124 + 12, $"Get (found) allocated {perOp:F1} B/op");
    }

    [Fact]
    public async Task GetFoundPooled_AllocatesAtMost12BytesPerOperation()
    {
        RequireOptimizedBuild();
        var wire = new FakeWireClient(Window);
        var client = wire.Client;
        var tasks = new ValueTask<PooledGetResult>[Window];

        async Task<long> Rounds(int rounds)
        {
            for (int round = 0; round < rounds; round++)
            {
                for (int i = 0; i < Window; i++) tasks[i] = client.GetPooledAsync(Keys[i]);
                for (int i = 0; i < Window; i++)
                {
                    using var r = await tasks[i];
                    if (r.Err is not null || r.Value.Length != 100) throw new InvalidOperationException("unexpected result");
                }
            }
            return (long)rounds * Window;
        }

        double perOp = await MeasureAsync("get (found, pooled API)", Rounds, Window);
        await wire.CloseAsync();

        // Budget 12 B: only the per-call overhead of the Read stream (see GetNotFound); the value lives in a pooled lease
        Assert.True(perOp <= 12, $"Get (pooled) allocated {perOp:F1} B/op");
    }

    [Fact]
    public async Task RangeScanPooled_1000Records_AllocatesAtMost8BytesPerRecord()
    {
        RequireOptimizedBuild();
        var wire = new FakeWireClient(Window);
        var client = wire.Client;

        async Task<long> Scans(int scans)
        {
            long n = 0;
            for (int scan = 0; scan < scans; scan++)
            {
                var reader = client.RangeScanPooledAsync(CancellationToken.None, "/scan/", "/scan0");
                await foreach (var r in reader.ReadAllAsync())
                {
                    using (r)
                    {
                        if (r.Err is not null) throw r.Err;
                        if (r.Value.Length != 100 || r.KeyUtf8.Length != 18) throw new InvalidOperationException("unexpected record");
                        n++;
                    }
                }
            }
            return n;
        }

        // a "round" here is a whole scan of 1000 records (the warm-up is 10 times shorter in rounds)
        double perRecord = await MeasureAsync("range scan (1000 records, pooled API)", Scans, 1000, warmupDivisor: 10);
        await wire.CloseAsync();

        // Budget 8 B per record: one scan (a reader, a task per shard, a linked token, a few boxes: about 2 KB) is spread over 1000
        // records (measured 2.3 B/record).  Keys and values are bytes in leases: no string and no array per record.
        Assert.True(perRecord <= 8, $"RangeScanPooled allocated {perRecord:F1} B/record");
    }

    // ----- notifications and sessions -----------------------------------------------------------------------------------

    [Fact]
    public async Task NotificationDelivery_AllocatesOnlyTheUserFacingObjects()
    {
        RequireOptimizedBuild();
        var wire = new FakeWireClient(Window);
        wire.Invoker.NotificationsPerBatch = 20;
        var notifications = await NotificationsImpl.CreateAsync(wire.Client.Ctx, wire.Client.Options, wire.Provider, wire.ShardManager);
        var reader = notifications.Ch;

        async Task<long> Rounds(int rounds)
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            for (int round = 0; round < rounds; round++)
            {
                for (int n = 0; n < 200; n++)
                {
                    var notification = await reader.ReadAsync(cts.Token);
                    if (notification.Type != NotificationType.KeyModified) throw new InvalidOperationException("unexpected notification");
                }
            }
            return (long)rounds * 200;
        }

        double perOp = await MeasureAsync("notification delivery", Rounds, 200);
        await notifications.CloseAsync();
        await wire.CloseAsync();

        // Budget 136 B: what the public API itself creates per notification -- the Notification object (48 B) and its key string
        // ("/notifications/key-000001" = 25 chars: 22 + 50 = 72 B) -- plus 16 B of slack (the channel slot / the reader's boxes).  The wire
        // objects (NotificationBatch, NotificationEntry, wire Notification: about 200 B per notification before) are pooled and the batch
        // is released after delivery.
        Assert.True(perOp <= 136, $"Notification delivery allocated {perOp:F1} B/notification");
    }

    [Fact]
    public async Task SessionKeepAlive_AllocatesNothingOnOurSide()
    {
        RequireOptimizedBuild();
        var wire = new FakeWireClient(Window, intercepted: false);
        var heartbeat = new SessionHeartbeat { Shard = 0, SessionId = 77 };

        async Task<long> Rounds(int rounds)
        {
            for (int round = 0; round < rounds; round++)
            {
                for (int i = 0; i < 1000; i++) await wire.Provider.KeepAlive(CancellationToken.None, "fake:1", heartbeat);
            }
            return (long)rounds * 1000;
        }

        double perOp = await MeasureAsync("session keep-alive", Rounds, 1000);
        await wire.CloseAsync();

        // Budget 4 B: serialization (real serializer, reused buffer), the unary call plumbing of OxiaClientRpc and the shared empty
        // response are all allocation free (measured 0.00 B); 4 B is slack for the process-wide counter.  (The real HTTP/2 unary call costs about 3 KB
        // once per keep-alive tick, every few seconds.)
        Assert.True(perOp <= 4, $"KeepAlive allocated {perOp:F1} B/call");
    }
}

// =====================================================================================================================
// PORT NOTE (benchmark support -- no upstream equivalent)
//   Upstream : none
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : `dotnet run -c Release --project benchmarks/Oxia.Client.Benchmarks -- alloc [scenario ...]` -- runs the end-to-end
//              scenarios of the allocation work against the zero-allocation AllocEchoServer and prints, per scenario, the exact
//              process-wide bytes per operation (GC.GetTotalAllocatedBytes) and the type-by-type attribution of the
//              sampled GC allocation ticks (AllocationTickListener).  The process-wide figure includes the in-process server's
//              own HTTP/2 plumbing (Kestrel); the type table tells which share is the client's.
//              Scenarios: put, delete, deleterange, deleterange-pk, get-notfound, get-found, get-pooled, put-single, get-single, scan, scan-pooled
// =====================================================================================================================
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Oxia.Client.Pooled;
using Oxia.TestSupport;

namespace Oxia.Client.Benchmarks.Alloc;

public static class AllocProfileRunner
{
    private const int Pipelined = 1000;

    public static async Task<int> RunAsync(string[] args)
    {
        var all = new[] { "put", "delete", "deleterange", "deleterange-pk", "get-notfound", "get-found", "get-pooled", "put-single", "get-single", "scan", "scan-pooled" };
        var scenarios = args.Length == 0 ? all : args;

        var echo = new AllocEchoServer();
        await using var server = await TestGrpcServer.StartAsync(app => app.MapGrpcService<AllocEchoServer>(), services => services.AddSingleton(echo));
        echo.Address = server.Address;

        var client = await OxiaClient.CreateAsyncClientAsync(server.Address, OxiaOptions.WithBatchLinger(TimeSpan.FromMilliseconds(1)));
        var immediate = await OxiaClient.CreateAsyncClientAsync(server.Address, OxiaOptions.WithBatchLinger(TimeSpan.Zero));
        using var listener = new AllocationTickListener();

        foreach (string scenario in scenarios)
        {
            AllocEchoServer.NotFound = scenario == "get-notfound";
            Func<ValueTask<long>> round = scenario switch
            {
                "put" => () => PutRound(client),
                "delete" => () => DeleteRound(client),
                "deleterange" => () => DeleteRangeRound(client, false),
                "deleterange-pk" => () => DeleteRangeRound(client, true),
                "get-notfound" or "get-found" => () => GetRound(client),
                "get-pooled" => () => GetPooledRound(client),
                "put-single" => () => PutSingle(immediate, 200),
                "get-single" => () => GetSingle(immediate, 200),
                "scan" => () => ScanRound(client, false),
                "scan-pooled" => () => ScanRound(client, true),
                _ => throw new ArgumentException("unknown scenario " + scenario),
            };

            // warm up: pools, connection, JIT tiers
            for (int i = 0; i < 40; i++) await round();
            for (int i = 0; i < 5; i++)
            {
                for (int j = 0; j < 20; j++) await round();
                await Task.Delay(100);
            }

            int rounds = scenario.EndsWith("single", StringComparison.Ordinal) ? 100 : 400;
            long ops = 0;
            GC.Collect();
            long before = GC.GetTotalAllocatedBytes(true);
            listener.Start();
            for (int i = 0; i < rounds; i++) ops += await round();
            listener.Stop();
            long total = GC.GetTotalAllocatedBytes(true) - before;

            Console.WriteLine($"##### {scenario}: {total / (double)ops:F1} B/op process-wide ({ops} ops)");
            Console.WriteLine(listener.Report(scenario, ops));
        }

        await client.CloseAsync();
        await immediate.CloseAsync();
        return 0;
    }

    private static readonly byte[] Value = new byte[100];
    private static readonly string[] Keys = Enumerable.Range(0, Pipelined).Select(i => "key-" + i).ToArray();
    private static readonly ValueTask<PutResult>[] Puts = new ValueTask<PutResult>[Pipelined];
    private static readonly ValueTask<GetResult>[] Gets = new ValueTask<GetResult>[Pipelined];
    private static readonly ValueTask<PooledGetResult>[] PooledGets = new ValueTask<PooledGetResult>[Pipelined];
    private static readonly ValueTask<Exception?>[] Deletes = new ValueTask<Exception?>[Pipelined];

    private static async ValueTask<long> PutRound(IAsyncClient client)
    {
        for (int i = 0; i < Keys.Length; i++) Puts[i] = client.PutAsync(Keys[i], Value);
        for (int i = 0; i < Puts.Length; i++) await Puts[i];
        return Pipelined;
    }

    private static async ValueTask<long> DeleteRound(IAsyncClient client)
    {
        for (int i = 0; i < Keys.Length; i++) Deletes[i] = client.DeleteAsync(Keys[i]);
        for (int i = 0; i < Deletes.Length; i++) await Deletes[i];
        return Pipelined;
    }

    private static async ValueTask<long> DeleteRangeRound(IAsyncClient client, bool partitionKey)
    {
        for (int i = 0; i < Keys.Length; i++)
        {
            Deletes[i] = partitionKey
                ? client.DeleteRangeAsync("a", "b", OxiaOptions.PartitionKey("p"))
                : client.DeleteRangeAsync("a", "b");
        }
        for (int i = 0; i < Deletes.Length; i++) await Deletes[i];
        return Pipelined;
    }

    private static async ValueTask<long> GetRound(IAsyncClient client)
    {
        for (int i = 0; i < Keys.Length; i++) Gets[i] = client.GetAsync(Keys[i]);
        for (int i = 0; i < Gets.Length; i++) await Gets[i];
        return Pipelined;
    }

    private static async ValueTask<long> GetPooledRound(IAsyncClient client)
    {
        for (int i = 0; i < Keys.Length; i++) PooledGets[i] = client.GetPooledAsync(Keys[i]);
        for (int i = 0; i < PooledGets.Length; i++)
        {
            using var r = await PooledGets[i];
            if (r.Err is not null || r.Value.Length != 100) throw new InvalidOperationException("unexpected pooled get result");
        }
        return Pipelined;
    }

    private static async ValueTask<long> PutSingle(IAsyncClient client, int count)
    {
        for (int i = 0; i < count; i++) await client.PutAsync("key", Value);
        return count;
    }

    private static async ValueTask<long> GetSingle(IAsyncClient client, int count)
    {
        for (int i = 0; i < count; i++) await client.GetAsync("key");
        return count;
    }

    private static async ValueTask<long> ScanRound(IAsyncClient client, bool pooled)
    {
        long n = 0;
        if (pooled)
        {
            var reader = client.RangeScanPooledAsync(CancellationToken.None, "/scan/", "/scan0");
            await foreach (var r in reader.ReadAllAsync())
            {
                using (r)
                {
                    if (r.Err is not null) throw r.Err;
                    n++;
                }
            }
        }
        else
        {
            var reader = client.RangeScanAsync(CancellationToken.None, "/scan/", "/scan0");
            await foreach (var r in reader.ReadAllAsync())
            {
                if (r.Err is not null) throw r.Err;
                n++;
            }
        }
        return n;
    }
}

// =====================================================================================================================
// PORT NOTE (benchmark -- no upstream equivalent)
//   Upstream : none (measures oxia/async_client_impl.go Get / RangeScan against the opt-in pooled read API of wave 2 / Z1)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Faithful vs pooled reads end to end against the zero-allocation AllocEchoServer (found 100-byte values, a scan of 1000
//              records): `GetAsync` (a byte[] per value, a string per key) vs `GetPooledAsync` (a lease), `RangeScanAsync` vs
//              `RangeScanPooledAsync`.  `--memory` shows the Allocated column per operation / record.  The in-process server shares the
//              process, so the figures include Kestrel's own per-message allocations (see docs/performance-allocation.md).
//   Run      : dotnet run -c Release --project benchmarks/Oxia.Client.Benchmarks -- --filter "*PooledReadBenchmarks*" --job short
// =====================================================================================================================
using BenchmarkDotNet.Attributes;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Oxia.Client.Pooled;
using Oxia.TestSupport;

namespace Oxia.Client.Benchmarks.Alloc;

[MemoryDiagnoser]
public class PooledReadBenchmarks
{
    private const int Pipelined = 16;   // below the capacity of the batchers' queues: the fast path of AddAsync
    private const int ScanRecords = 1000;

    private TestGrpcServer _server = null!;
    private IAsyncClient _client = null!;
    private string[] _keys = null!;
    private ValueTask<GetResult>[] _gets = null!;
    private ValueTask<PooledGetResult>[] _pooledGets = null!;

    [GlobalSetup]
    public async Task Setup()
    {
        AllocEchoServer.NotFound = false;
        var echo = new AllocEchoServer();
        _server = await TestGrpcServer.StartAsync(app => app.MapGrpcService<AllocEchoServer>(), services => services.AddSingleton(echo));
        echo.Address = _server.Address;
        _client = await OxiaClient.CreateAsyncClientAsync(_server.Address,
            OxiaOptions.WithBatchLinger(TimeSpan.FromMilliseconds(1)), OxiaOptions.WithMaxRequestsPerBatch(Pipelined));
        _keys = Enumerable.Range(0, Pipelined).Select(i => "key-" + i).ToArray();
        _gets = new ValueTask<GetResult>[Pipelined];
        _pooledGets = new ValueTask<PooledGetResult>[Pipelined];
        for (int i = 0; i < 20; i++)
        {
            await GetFaithful();
            await GetPooled();
            await ScanFaithful();
            await ScanPooled();
        }
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _client.CloseAsync();
        await _server.DisposeAsync();
    }

    [Benchmark(OperationsPerInvoke = Pipelined)]
    public async Task GetFaithful()
    {
        for (int i = 0; i < _keys.Length; i++) _gets[i] = _client.GetAsync(_keys[i]);
        for (int i = 0; i < _gets.Length; i++) await _gets[i];
    }

    [Benchmark(OperationsPerInvoke = Pipelined)]
    public async Task GetPooled()
    {
        for (int i = 0; i < _keys.Length; i++) _pooledGets[i] = _client.GetPooledAsync(_keys[i]);
        for (int i = 0; i < _pooledGets.Length; i++)
        {
            using var r = await _pooledGets[i];
        }
    }

    [Benchmark(OperationsPerInvoke = ScanRecords)]
    public async Task ScanFaithful()
    {
        await foreach (var r in _client.RangeScanAsync(CancellationToken.None, "/scan/", "/scan0").ReadAllAsync()) { _ = r; }
    }

    [Benchmark(OperationsPerInvoke = ScanRecords)]
    public async Task ScanPooled()
    {
        await foreach (var r in _client.RangeScanPooledAsync(CancellationToken.None, "/scan/", "/scan0").ReadAllAsync())
        {
            using (r) { }
        }
    }
}

// =====================================================================================================================
// PORT NOTE (benchmark -- no upstream equivalent)
//   Upstream : none (the Go client has no micro-benchmarks of the client operations); measures oxia/async_client_impl.go
//              (Put / Get / Delete) end to end against an in-process mock server.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Allocation budget of the HOT PATH of the async client: PutAsync / GetAsync / DeleteAsync must allocate
//              nothing per operation in steady state on the client side (pooled calls, stack-resolved options, no closures).
//              The server (RawEchoServer, in this process, hosted by TestGrpcServer) allocates nothing per operation, so the
//              "Allocated" column is the client's: batcher + codec + gRPC client plumbing.  It is amortised over the 1000
//              operations of one batch for the *Pipelined* benchmarks; the Single* ones send one operation per round trip
//              (BatchLinger 0), so they include the per-request gRPC overhead.  The share of PutAsync / GetAsync /
//              DeleteAsync themselves is asserted separately (AsyncClientImpl.Extra.cs: 0 B/op with a batcher that completes inline).
//   Run      : dotnet run -c Release --project benchmarks/Oxia.Client.Benchmarks -- --filter "*ClientOperationBenchmarks*"
// =====================================================================================================================
using BenchmarkDotNet.Attributes;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Oxia.TestSupport;

namespace Oxia.Client.Benchmarks.Client;

[MemoryDiagnoser]
public class ClientOperationBenchmarks
{
    private const int Pipelined = 1000;

    private TestGrpcServer _server = null!;
    private IAsyncClient _client = null!;       // BatchLinger 1 ms: the pipelined benchmarks
    private IAsyncClient _immediate = null!;    // BatchLinger 0: one operation per round trip
    private byte[] _value = null!;
    private string[] _keys = null!;
    private ValueTask<PutResult>[] _puts = null!;
    private ValueTask<GetResult>[] _gets = null!;
    private ValueTask<Exception?>[] _deletes = null!;

    [GlobalSetup]
    public async Task Setup()
    {
        var echo = new RawEchoServer();
        _server = await TestGrpcServer.StartAsync(app => app.MapGrpcService<RawEchoServer>(), services => services.AddSingleton(echo));
        echo.Address = _server.Address;

        _client = await OxiaClient.CreateAsyncClientAsync(_server.Address, OxiaOptions.WithBatchLinger(TimeSpan.FromMilliseconds(1)));
        _immediate = await OxiaClient.CreateAsyncClientAsync(_server.Address, OxiaOptions.WithBatchLinger(TimeSpan.Zero));
        _value = new byte[100];
        _keys = Enumerable.Range(0, Pipelined).Select(i => "key-" + i).ToArray();
        _puts = new ValueTask<PutResult>[Pipelined];
        _gets = new ValueTask<GetResult>[Pipelined];
        _deletes = new ValueTask<Exception?>[Pipelined];

        // warm up: pools, connection, JIT
        for (int i = 0; i < 3; i++)
        {
            await PutPipelined();
            await GetPipelined();
            await DeletePipelined();
        }
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _client.CloseAsync();
        await _immediate.CloseAsync();
        await _server.DisposeAsync();
    }

    [Benchmark]
    public async ValueTask<PutResult> SinglePut() => await _immediate.PutAsync("key", _value);

    [Benchmark]
    public async ValueTask<GetResult> SingleGet() => await _immediate.GetAsync("key");

    [Benchmark]
    public async ValueTask<Exception?> SingleDelete() => await _immediate.DeleteAsync("key");

    [Benchmark(OperationsPerInvoke = Pipelined)]
    public async Task PutPipelined()
    {
        for (int i = 0; i < _keys.Length; i++) _puts[i] = _client.PutAsync(_keys[i], _value);
        for (int i = 0; i < _puts.Length; i++) await _puts[i];
    }

    [Benchmark(OperationsPerInvoke = Pipelined)]
    public async Task GetPipelined()
    {
        for (int i = 0; i < _keys.Length; i++) _gets[i] = _client.GetAsync(_keys[i]);
        for (int i = 0; i < _gets.Length; i++) await _gets[i];
    }

    [Benchmark(OperationsPerInvoke = Pipelined)]
    public async Task DeletePipelined()
    {
        for (int i = 0; i < _keys.Length; i++) _deletes[i] = _client.DeleteAsync(_keys[i]);
        for (int i = 0; i < _deletes.Length; i++) await _deletes[i];
    }
}

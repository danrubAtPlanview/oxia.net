// =====================================================================================================================
// PORT NOTE (benchmark -- no upstream equivalent)
//   Upstream : none (the Go client has no micro-benchmarks of the client operations); measures oxia/async_client_impl.go
//              (Put / Get / Delete) end to end against an in-process mock server.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Allocation budget of the HOT PATH of the async client: PutAsync / GetAsync / DeleteAsync must allocate
//              nothing per operation in steady state on the client side (pooled calls, stack-resolved options, no closures).
//              The numbers include the work of the whole pipeline of this process (batcher, codec, gRPC client AND the Kestrel
//              mock server, whose protobuf parsing of the requests allocates a message object + strings per operation), so
//              read them as an upper bound of the client; the server answers with cached responses to keep its share small.
//              The client's own share is asserted by AsyncClientImpl.Extra.cs (0 B/op with a batcher that completes inline).
//              "Allocated" of the *Pipelined* benchmarks is amortised over the 1000 operations of one batch; the Single*
//              ones use one operation per round trip (BatchLinger 0), so they include the per-request gRPC overhead.
//   Run      : dotnet run -c Release --project benchmarks/Oxia.Client.Benchmarks -- --filter "*ClientOperationBenchmarks*"
// =====================================================================================================================
using System.Threading.Channels;
using BenchmarkDotNet.Attributes;
using Grpc.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Oxia.Common.Constant;
using Oxia.TestSupport;
using G = Io.Oxia.Proto.V1;

namespace Oxia.Client.Benchmarks.Client;

/// <summary>A trivial Oxia server: one shard led by itself; every put/delete/delete-range succeeds, every get finds a value.</summary>
public sealed class EchoOxiaServer : G.OxiaClient.OxiaClientBase
{
    private static readonly Google.Protobuf.ByteString Value = Google.Protobuf.ByteString.CopyFrom(new byte[100]);
    // The responses are immutable once built: they are cached by shape so that the server allocates as little as possible
    private readonly System.Collections.Concurrent.ConcurrentDictionary<(int, int, int), G.WriteResponse> _writeResponses = new();
    private readonly System.Collections.Concurrent.ConcurrentDictionary<int, G.ReadResponse> _readResponses = new();

    public string Address { get; set; } = "";

    public override async Task GetShardAssignments(G.ShardAssignmentsRequest request, IServerStreamWriter<G.ShardAssignments> responseStream,
        ServerCallContext context)
    {
        var namespaceAssignment = new G.NamespaceShardsAssignment { ShardKeyRouter = G.ShardKeyRouter.Xxhash3 };
        namespaceAssignment.Assignments.Add(new G.ShardAssignment
        {
            Shard = 0,
            Leader = Address,
            Int32HashRange = new G.Int32HashRange { MinHashInclusive = 0, MaxHashInclusive = uint.MaxValue },
        });
        var assignments = new G.ShardAssignments();
        assignments.Namespaces[Constants.DefaultNamespace] = namespaceAssignment;
        await responseStream.WriteAsync(assignments);
        await Task.Delay(Timeout.Infinite, context.CancellationToken).ContinueWith(_ => { });
    }

    public override async Task WriteStream(IAsyncStreamReader<G.WriteRequest> requestStream, IServerStreamWriter<G.WriteResponse> responseStream,
        ServerCallContext context)
    {
        while (await requestStream.MoveNext(context.CancellationToken))
        {
            var request = requestStream.Current;
            var response = _writeResponses.GetOrAdd((request.Puts.Count, request.Deletes.Count, request.DeleteRanges.Count), static shape =>
            {
                var r = new G.WriteResponse();
                for (int i = 0; i < shape.Item1; i++) r.Puts.Add(new G.PutResponse { Status = G.Status.Ok, Version = new G.Version() });
                for (int i = 0; i < shape.Item2; i++) r.Deletes.Add(new G.DeleteResponse { Status = G.Status.Ok });
                for (int i = 0; i < shape.Item3; i++) r.DeleteRanges.Add(new G.DeleteRangeResponse { Status = G.Status.Ok });
                return r;
            });
            await responseStream.WriteAsync(response);
        }
    }

    public override async Task Read(G.ReadRequest request, IServerStreamWriter<G.ReadResponse> responseStream, ServerCallContext context)
    {
        var response = _readResponses.GetOrAdd(request.Gets.Count, static count =>
        {
            var r = new G.ReadResponse();
            for (int i = 0; i < count; i++) r.Gets.Add(new G.GetResponse { Status = G.Status.Ok, Version = new G.Version(), Value = Value });
            return r;
        });
        await responseStream.WriteAsync(response);
    }
}

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
        var echo = new EchoOxiaServer();
        _server = await TestGrpcServer.StartAsync(app => app.MapGrpcService<EchoOxiaServer>(), services => services.AddSingleton(echo));
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

// =====================================================================================================================
// REVIEW R3 -- reproduction tests (they FAIL until the production code is fixed; they never touch existing tests)
//   Defect   : List / RangeScan read every shard's stream EAGERLY into UNBOUNDED queues, so a slow (or stalled) consumer never
//              back-pressures the server: the whole result set ends up in client memory.  (F2 documented the divergence in the
//              PORT NOTE of AsyncClientImpl.cs; these tests quantify it and pin the behaviour Go has.)
//   Go       : oxia/async_client_impl.go  rangeScanFromShard / listFromShard call the executor with a consumer that BLOCKS on an
//              unbuffered `ch <- rangeScanResult` (per shard) / a 100-slot `outCh` (partition key); the consumer runs on the
//              goroutine that calls stream.Recv(), so a consumer that does not read stops Recv(), HTTP/2 flow control stops the
//              server, and at most ~100 + #shards records are buffered client side.
//   C#       : src/Oxia.Client/AsyncClientImpl.cs  RangeScanFromShardAsync/ListFromShardAsync: IExecutor.ExecuteRangeScan takes a
//              SYNCHRONOUS Func<RangeScanResponse, Exception?>, so the consumer does `Channel.CreateUnbounded<..>().Writer.TryWrite`
//              and RpcProvider.ExecuteRangeScan (Internal/RpcProvider.cs l.272-289) immediately calls RecvAsync again.
// =====================================================================================================================
using System.Threading.Channels;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Oxia.Common.Constant;
using Oxia.TestSupport;
using Xunit;
using G = Io.Oxia.Proto.V1;

namespace Oxia.Client.Tests.Review;

/// <summary>A one-shard leader whose RangeScan / List streams <see cref="Total"/> responses and counts how many were accepted by the transport.</summary>
internal sealed class R3_ScanServer : G.OxiaClient.OxiaClientBase, IAsyncDisposable
{
    private TestGrpcServer? _server;
    private long _written;

    public int Total { get; set; }
    public int ValueSize { get; set; } = 4096;
    public string Address { get; private set; } = "";

    /// <summary>Responses whose WriteAsync completed, i.e. that HTTP/2 flow control let through.</summary>
    public long Written => Interlocked.Read(ref _written);

    public static async Task<R3_ScanServer> StartAsync(int total, int valueSize)
    {
        var s = new R3_ScanServer { Total = total, ValueSize = valueSize };
        s._server = await TestGrpcServer.StartAsync(
            app => app.MapGrpcService<R3_ScanServer>(),
            services => services.AddSingleton(s));
        s.Address = s._server.Address;
        return s;
    }

    public ValueTask DisposeAsync() => _server is null ? ValueTask.CompletedTask : _server.DisposeAsync();

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
        try { await Task.Delay(Timeout.Infinite, context.CancellationToken); } catch (OperationCanceledException) { }
    }

    public override async Task RangeScan(G.RangeScanRequest request, IServerStreamWriter<G.RangeScanResponse> responseStream,
        ServerCallContext context)
    {
        var value = ByteString.CopyFrom(new byte[ValueSize]);
        for (int i = 0; i < Total && !context.CancellationToken.IsCancellationRequested; i++)
        {
            var response = new G.RangeScanResponse();
            response.Records.Add(new G.GetResponse { Status = G.Status.Ok, Key = "k" + i.ToString("D8"), Value = value, Version = new G.Version() });
            await responseStream.WriteAsync(response);
            Interlocked.Increment(ref _written);
        }
    }

    [Obsolete("List is deprecated upstream; the test needs it")]
    public override async Task List(G.ListRequest request,IServerStreamWriter<G.ListResponse> responseStream, ServerCallContext context)
    {
        for (int i = 0; i < Total && !context.CancellationToken.IsCancellationRequested; i++)
        {
            var response = new G.ListResponse();
            // ~ValueSize bytes of keys per response, to make the transport window matter like for a scan with values
            for (int k = 0; k < ValueSize / 32; k++) response.Keys.Add("k" + i.ToString("D8") + "-" + k.ToString("D12") + "-pad");
            await responseStream.WriteAsync(response);
            Interlocked.Increment(ref _written);
        }
    }
}

public class R3_ScanBackpressureTests
{
    /// <summary>
    /// The caller reads ONE record of a ~96 MB scan and then stops (a slow consumer).  Go: the server stalls after the transport
    /// windows (a few MB) because the client stops calling Recv().  C#: the client keeps receiving into an unbounded queue and
    /// the server finishes the whole scan (and the client holds ~96 MB of records).
    /// </summary>
    [Theory]
    [InlineData(false)]   // all shards: per-shard unbounded channel + merge heap
    [InlineData(true)]    // PartitionKey: unbounded local queue + pump
    public async Task RangeScan_SlowConsumer_BackPressuresTheServer(bool partitionKey)
    {
        const int total = 24_000;       // x 4 KiB = ~96 MB, far above any HTTP/2 window
        await using var server = await R3_ScanServer.StartAsync(total, 4096);
        var client = await OxiaClient.CreateAsyncClientAsync(server.Address);
        try
        {
            long before = GC.GetTotalMemory(true);
            using var cts = new CancellationTokenSource();
            var reader = partitionKey
                ? client.RangeScanAsync(cts.Token, "a", "z", OxiaOptions.PartitionKey("p"))
                : client.RangeScanAsync(cts.Token, "a", "z");

            var first = await reader.ReadAsync(cts.Token);
            Assert.Null(first.Err);

            await Task.Delay(TimeSpan.FromSeconds(3));   // the consumer is stuck/slow: nobody reads anymore

            long written = server.Written;
            long heldMb = (GC.GetTotalMemory(false) - before) / (1024 * 1024);
            cts.Cancel();

            // Go bound: ~(100 buffered + 1 per shard) records + what the transport windows hold (a few MB => well under 1/4)
            Assert.True(written < total / 4,
                $"the server was able to send {written} of {total} responses (~{heldMb} MB now held by the client) although the consumer read " +
                "1 record: no back-pressure reaches the server");
        }
        finally
        {
            await client.CloseAsync();
        }
    }

    /// <summary>Same for List (deprecated in Go, but still public): the output channel is 1-slot like Go's unbuffered channel, the shard stream is not.</summary>
    [Fact]
    public async Task List_SlowConsumer_BackPressuresTheServer()
    {
        const int total = 24_000;       // x ~4 KiB of keys = ~96 MB
        await using var server = await R3_ScanServer.StartAsync(total, 4096);
        var client = await OxiaClient.CreateAsyncClientAsync(server.Address);
        try
        {
            long before = GC.GetTotalMemory(true);
            using var cts = new CancellationTokenSource();
            var reader = client.ListAsync(cts.Token, "a", "z");

            var first = await reader.ReadAsync(cts.Token);
            Assert.Null(first.Err);

            await Task.Delay(TimeSpan.FromSeconds(3));

            long written = server.Written;
            long heldMb = (GC.GetTotalMemory(false) - before) / (1024 * 1024);
            cts.Cancel();

            Assert.True(written < total / 4,
                $"the server was able to send {written} of {total} responses (~{heldMb} MB now held by the client) although the consumer read " +
                "1 chunk: no back-pressure reaches the server");
        }
        finally
        {
            await client.CloseAsync();
        }
    }
}

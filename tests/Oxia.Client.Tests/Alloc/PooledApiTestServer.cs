// =====================================================================================================================
// PORT NOTE (test infrastructure)
//   Upstream : none (a mock of the Oxia server; Go tests mock proto.OxiaClientServer the same way)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : An in-process server for the tests of the pooled read API and of the response recycling, over REAL gRPC (Kestrel h2c):
//                * `Shards` shards, all led by the server itself, splitting the 32-bit hash space evenly;
//                * WriteStream answers every put / delete / delete-range with Ok (+ a version);
//                * Read answers "missing*" keys with KeyNotFound and every other key with a value "value-of-<key>" and a version
//                  (VersionId 7, ModificationsCount 2); a comparison other than Equal answers with the stored key set;
//                * RangeScan streams the records "/scan/k-00000" ... of its shard (record i belongs to shard i % Shards), `ScanChunk`
//                  records per message, honouring include_values; a key range [start, end) is applied;
//                * `FailScanOnShard` makes the scan of that shard fail after its first message.
// =====================================================================================================================
using Grpc.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Oxia.Common.Constant;
using Oxia.TestSupport;
using G = Io.Oxia.Proto.V1;

namespace Oxia.Client.Tests.Alloc;

internal sealed class PooledApiTestServer : G.OxiaClient.OxiaClientBase, IAsyncDisposable
{
    private TestGrpcServer? _server;

    public string Address { get; private set; } = "";
    public int Shards { get; init; } = 1;
    public int ScanRecords { get; init; } = 20;
    public int ScanChunk { get; init; } = 3;
    public int FailScanOnShard { get; init; } = -1;

    public static async Task<PooledApiTestServer> StartAsync(int shards = 1, int scanRecords = 20, int scanChunk = 3, int failScanOnShard = -1)
    {
        var s = new PooledApiTestServer { Shards = shards, ScanRecords = scanRecords, ScanChunk = scanChunk, FailScanOnShard = failScanOnShard };
        s._server = await TestGrpcServer.StartAsync(app => app.MapGrpcService<PooledApiTestServer>(), services => services.AddSingleton(s));
        s.Address = s._server.Address;
        return s;
    }

    public ValueTask DisposeAsync() => _server is null ? ValueTask.CompletedTask : _server.DisposeAsync();

    public static string ScanKey(int i) => "/scan/k-" + i.ToString("D5");

    public static byte[] ValueOf(string key) => System.Text.Encoding.UTF8.GetBytes("value-of-" + key);

    public override async Task GetShardAssignments(G.ShardAssignmentsRequest request, IServerStreamWriter<G.ShardAssignments> responseStream,
        ServerCallContext context)
    {
        var assignment = new G.NamespaceShardsAssignment { ShardKeyRouter = G.ShardKeyRouter.Xxhash3 };
        ulong span = (1UL << 32) / (ulong)Shards;
        for (int i = 0; i < Shards; i++)
        {
            ulong min = span * (ulong)i;
            ulong max = i == Shards - 1 ? uint.MaxValue : min + span - 1;
            assignment.Assignments.Add(new G.ShardAssignment
            {
                Shard = i,
                Leader = Address,
                Int32HashRange = new G.Int32HashRange { MinHashInclusive = (uint)min, MaxHashInclusive = (uint)max },
            });
        }
        var assignments = new G.ShardAssignments();
        assignments.Namespaces[Constants.DefaultNamespace] = assignment;
        await responseStream.WriteAsync(assignments);
        try { await Task.Delay(Timeout.Infinite, context.CancellationToken); } catch (OperationCanceledException) { }
    }

    private static G.Version Version(long id = 7) => new() { VersionId = id, ModificationsCount = 2, CreatedTimestamp = 1000, ModifiedTimestamp = 2000 };

    public override async Task WriteStream(IAsyncStreamReader<G.WriteRequest> requestStream, IServerStreamWriter<G.WriteResponse> responseStream,
        ServerCallContext context)
    {
        while (await requestStream.MoveNext(context.CancellationToken))
        {
            var request = requestStream.Current;
            var response = new G.WriteResponse();
            foreach (var put in request.Puts) response.Puts.Add(new G.PutResponse { Status = G.Status.Ok, Version = Version() });
            foreach (var _ in request.Deletes) response.Deletes.Add(new G.DeleteResponse { Status = G.Status.Ok });
            foreach (var _ in request.DeleteRanges) response.DeleteRanges.Add(new G.DeleteRangeResponse { Status = G.Status.Ok });
            await responseStream.WriteAsync(response);
        }
    }

    public override async Task Read(G.ReadRequest request, IServerStreamWriter<G.ReadResponse> responseStream, ServerCallContext context)
    {
        var response = new G.ReadResponse();
        foreach (var get in request.Gets)
        {
            if (get.Key.StartsWith("missing", StringComparison.Ordinal))
            {
                response.Gets.Add(new G.GetResponse { Status = G.Status.KeyNotFound });
                continue;
            }
            var r = new G.GetResponse { Status = G.Status.Ok, Version = Version() };
            if (get.IncludeValue) r.Value = Google.Protobuf.ByteString.CopyFrom(ValueOf(get.Key));
            if (get.ComparisonType != G.KeyComparisonType.Equal) r.Key = get.Key;
            response.Gets.Add(r);
        }
        await responseStream.WriteAsync(response);
    }

    public override async Task RangeScan(G.RangeScanRequest request, IServerStreamWriter<G.RangeScanResponse> responseStream, ServerCallContext context)
    {
        var batch = new G.RangeScanResponse();
        int sent = 0;
        for (int i = 0; i < ScanRecords; i++)
        {
            if (i % Shards != request.Shard) continue;
            string key = ScanKey(i);
            if (string.CompareOrdinal(key, request.StartInclusive) < 0) continue;
            if (request.EndExclusive.Length > 0 && string.CompareOrdinal(key, request.EndExclusive) >= 0) continue;

            var record = new G.GetResponse { Status = G.Status.Ok, Key = key, Version = Version(i + 1) };
            if (!request.HasIncludeValues || request.IncludeValues) record.Value = Google.Protobuf.ByteString.CopyFrom(ValueOf(key));
            batch.Records.Add(record);
            if (batch.Records.Count == ScanChunk)
            {
                await responseStream.WriteAsync(batch);
                batch = new G.RangeScanResponse();
                if (++sent == 1 && FailScanOnShard == request.Shard) throw new RpcException(new Grpc.Core.Status(StatusCode.Internal, "scan failed"));
            }
        }
        if (batch.Records.Count > 0 || sent == 0) await responseStream.WriteAsync(batch);
    }
}

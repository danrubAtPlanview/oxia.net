// =====================================================================================================================
// PORT NOTE (test infrastructure)
//   Upstream : none.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : A few raw gRPC calls through the protoc-generated OxiaClientClient, INDEPENDENT of the client under test.  Used
//              by the fixture's self tests (e.g. "a new server starts empty": the first put of a fresh shard gets version 0)
//              and available to any test that needs to look at the server without the library being tested.
//   Divergences: n/a (new code; wave 2 added the optional RawSecurity argument: TLS / bearer token for secured servers).  Uses the unary `Write` RPC (deprecated upstream but still served) to avoid a write stream.
// =====================================================================================================================
using Grpc.Net.Client;
using G = Io.Oxia.Proto.V1;

namespace Oxia.TestSupport;

public static class RawOxia
{
    private static G.OxiaClient.OxiaClientClient Connect(string address, out GrpcChannel channel, RawSecurity? security = null)
    {
        channel = RawSecurity.CreateChannel(address, security);
        return new G.OxiaClient.OxiaClientClient(channel);
    }

    /// <summary>Puts a record on the given shard; returns the version id the server assigned.</summary>
    public static async Task<long> PutAsync(string address, long shard, string key, byte[] value, CancellationToken ct = default, RawSecurity? security = null)
    {
        var client = Connect(address, out var channel, security);
        using (channel)
        {
            var request = new G.WriteRequest { Shard = shard };
            request.Puts.Add(new G.PutRequest { Key = key, Value = Google.Protobuf.ByteString.CopyFrom(value) });
            var response = await client.WriteAsync(request, cancellationToken: ct).ResponseAsync.ConfigureAwait(false);
            var put = response.Puts[0];
            if (put.Status != G.Status.Ok) throw new InvalidOperationException($"put of '{key}' failed with status {put.Status}");
            return put.Version.VersionId;
        }
    }

    /// <summary>Reads a record from the given shard; null when it does not exist.</summary>
    public static async Task<byte[]?> GetAsync(string address, long shard, string key, CancellationToken ct = default, RawSecurity? security = null)
    {
        var client = Connect(address, out var channel, security);
        using (channel)
        {
            var request = new G.ReadRequest { Shard = shard };
            request.Gets.Add(new G.GetRequest { Key = key, IncludeValue = true });
            using var call = client.Read(request, cancellationToken: ct);
            if (!await call.ResponseStream.MoveNext(ct).ConfigureAwait(false)) return null;
            var get = call.ResponseStream.Current.Gets[0];
            return get.Status == G.Status.Ok ? get.Value.ToByteArray() : null;
        }
    }

    /// <summary>The (shard id, leader address) pairs and key sorting the server announces for the default namespace.</summary>
    public static async Task<(IReadOnlyList<(long Shard, string Leader)> Shards, G.KeySorting KeySorting)> GetAssignmentsAsync(
        string address, string @namespace = "default", CancellationToken ct = default, RawSecurity? security = null)
    {
        var client = Connect(address, out var channel, security);
        using (channel)
        {
            using var call = client.GetShardAssignments(new G.ShardAssignmentsRequest { Namespace = @namespace }, cancellationToken: ct);
            if (!await call.ResponseStream.MoveNext(ct).ConfigureAwait(false)) throw new InvalidOperationException("no assignments");
            var ns = call.ResponseStream.Current.Namespaces[@namespace];
            return (ns.Assignments.Select(a => (a.Shard, a.Leader)).ToList(), ns.KeySorting);
        }
    }
}

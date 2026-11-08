// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/admin.go   (AdminClient interface, SplitShardResult)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The public admin API of the coordinator (data servers, namespaces, shard split).  The message types are the
//              protoc-generated classes of admin.proto / metadata.proto (namespace Io.Oxia.Proto.V1): the admin API is a cold
//              path, so no hand-written messages exist for it.
//   Divergences: (value, error) returns become `ValueTask<T>` that THROWS the mapped admin error (an AdminError: it matches
//              ClientErrors.Unauthorized / Unauthenticated / Unknown with OxiaErrors.Is, and keeps the gRPC failure as
//              InnerException so OxiaErrors.GrpcCode(ex) still works).  SplitShard keeps Go's result object, whose `Error`
//              is set instead of throwing.  context.Context -> CancellationToken (first parameter).  io.Closer -> IAsyncCloser.
//              `*uint32` split point -> `uint?`.
// =====================================================================================================================
using Oxia.Common;
using G = Io.Oxia.Proto.V1;

namespace Oxia.Client;

/// <summary>Client of the admin API of the coordinator (Go: AdminClient).</summary>
public interface IAdminClient : IAsyncCloser
{
    ValueTask<IReadOnlyList<G.DataServerView>> ListDataServersAsync(CancellationToken ct);
    ValueTask<G.DataServerView> GetDataServerAsync(CancellationToken ct, string dataServer);
    ValueTask<G.DataServer> CreateDataServerAsync(CancellationToken ct, G.DataServer dataServer);
    ValueTask<G.DataServer> PatchDataServerAsync(CancellationToken ct, G.DataServer dataServer);
    ValueTask<G.DataServer> DeleteDataServerAsync(CancellationToken ct, string dataServer);
    ValueTask DrainedDataServerAsync(CancellationToken ct, string dataServer);

    ValueTask<G.Namespace> CreateNamespaceAsync(CancellationToken ct, G.Namespace @namespace);
    ValueTask<G.Namespace> PatchNamespaceAsync(CancellationToken ct, G.Namespace @namespace);
    ValueTask<G.Namespace> DeleteNamespaceAsync(CancellationToken ct, string @namespace);
    ValueTask<IReadOnlyList<G.NamespaceView>> ListNamespacesAsync(CancellationToken ct);
    ValueTask<G.NamespaceView> GetNamespaceAsync(CancellationToken ct, string @namespace);

    /// <summary>Splits the shard in two at <paramref name="splitPoint"/> (the midpoint of its hash range when null). Never throws for a failed split: see <see cref="SplitShardResult.Error"/>.</summary>
    ValueTask<SplitShardResult> SplitShardAsync(CancellationToken ct, string @namespace, long shardId, uint? splitPoint);
}

/// <summary>Go: SplitShardResult -- the two child shards of a split, or the error that prevented it.</summary>
public sealed class SplitShardResult
{
    public long LeftChildShardId { get; init; }
    public long RightChildShardId { get; init; }
    public Exception? Error { get; init; }
}

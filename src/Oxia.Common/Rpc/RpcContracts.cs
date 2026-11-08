// =====================================================================================================================
// PORT NOTE
//   Upstream : common/rpc/client_pool.go (ClientPool interface), common/rpc/oxia_error_streaming_client.go,
//              common/proto/client_grpc.pb.go (OxiaClientClient and the *_Client stream interfaces),
//              io.Closer (Go) -> IAsyncCloser (.NET)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The seams between "what the client logic needs from the network" and "how gRPC provides it".  Everything
//              above this layer (rpc provider, sessions, notifications, ...) is written against these interfaces so it can
//              be unit-tested with fakes, exactly like the Go tests mock the generated gRPC client interfaces.
//   Divergence from Go:
//      * `Recv()` returning (msg, io.EOF) becomes `RecvAsync()` returning null at end of stream (io.EOF) and throwing the
//        translated error otherwise (see OxiaErrors.FromGrpcError -- the Oxia* wrappers translate on the way out).
//      * context.Context -> CancellationToken.
//   *** CONTRACT: owned by the lead; implementations are in OxiaClientRpc (wire-codec component) and ClientPool (common
//       utilities component). Do not change signatures without telling the lead. ***
// =====================================================================================================================
using Grpc.Core;
using Oxia.Common.Proto;

namespace Oxia.Common
{
/// <summary>
/// Go's <c>io.Closer</c> for components whose Close may do I/O or return errors.  <see cref="CloseAsync"/> throws the
/// combined error (AggregateException when several, like go.uber.org/multierr); <see cref="IAsyncDisposable.DisposeAsync"/>
/// calls it and swallows the error.
/// </summary>
public interface IAsyncCloser : IAsyncDisposable
{
    ValueTask CloseAsync();

    async ValueTask IAsyncDisposable.DisposeAsync()
    {
        try { await CloseAsync().ConfigureAwait(false); }
        catch { /* Dispose must not throw; callers that care use CloseAsync */ }
    }
}

}

namespace Oxia.Common.Rpc
{
    /// <summary>A server-streaming call (Go: grpc.ServerStreamingClient[T] wrapped by OxiaErrorServerStreamingClient).</summary>
    public interface IServerStreamingClient<T> : IDisposable where T : class
    {
        /// <summary>Next message, or null at end of stream (Go io.EOF).  Failures are thrown as translated Oxia errors.</summary>
        ValueTask<T?> RecvAsync(CancellationToken ct = default);
    }

    /// <summary>The write stream (Go: proto.OxiaClient_WriteStreamClient).</summary>
    public interface IWriteStreamCall : IDisposable
    {
        /// <summary>Go: stream.Send. Messages must be sent one at a time (callers serialise).</summary>
        ValueTask SendAsync(WriteRequest request, CancellationToken ct = default);

        /// <summary>Go: stream.Recv. null at end of stream.</summary>
        ValueTask<WriteResponse?> RecvAsync(CancellationToken ct = default);

        /// <summary>Go: stream.CloseSend.</summary>
        ValueTask CloseSendAsync();

        /// <summary>Go: stream.Context() -- cancelled when the call ends.</summary>
        CancellationToken Context { get; }
    }

    /// <summary>Go: proto.OxiaClientClient (the generated client of the OxiaClient service), expressed over the hand-written messages.</summary>
    public interface IOxiaClientRpc
    {
        /// <param name="headers">gRPC request headers (namespace, shard-id for write streams).</param>
        IWriteStreamCall WriteStream(Metadata? headers, CancellationToken ct);
        IServerStreamingClient<ReadResponse> Read(ReadRequest request, CancellationToken ct);
        IServerStreamingClient<ListResponse> List(ListRequest request, CancellationToken ct);
        IServerStreamingClient<RangeScanResponse> RangeScan(RangeScanRequest request, CancellationToken ct);
        IServerStreamingClient<ShardAssignments> GetShardAssignments(ShardAssignmentsRequest request, CancellationToken ct);
        IServerStreamingClient<GetSequenceUpdatesResponse> GetSequenceUpdates(GetSequenceUpdatesRequest request, CancellationToken ct);
        IServerStreamingClient<NotificationBatch> GetNotifications(NotificationsRequest request, CancellationToken ct);
        ValueTask<CreateSessionResponse> CreateSessionAsync(CreateSessionRequest request, CancellationToken ct);
        ValueTask<KeepAliveResponse> KeepAliveAsync(SessionHeartbeat request, CancellationToken ct);
        ValueTask<CloseSessionResponse> CloseSessionAsync(CloseSessionRequest request, CancellationToken ct);
    }

    /// <summary>Go: rpc.ClientPool (client-relevant subset).  One shared channel per target address.</summary>
    public interface IClientPool : IAsyncCloser
    {
        /// <summary>Go: GetClientRpc(target). Throws a translated Oxia error if the connection cannot be created.</summary>
        IOxiaClientRpc GetClientRpc(string target);

        /// <summary>Go: GetAminRpc(target) [sic]. The generated admin client (cold path, protoc-generated).</summary>
        Io.Oxia.Proto.V1.OxiaAdmin.OxiaAdminClient GetAdminRpc(string target);
    }
}

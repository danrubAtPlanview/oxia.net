// =====================================================================================================================
// PORT NOTE   *** CONTRACT STUB -- owned by the "wire codec" component; replace bodies, keep signatures ***
//   Upstream : common/proto/client_grpc.pb.go (service descriptor + generated client), common/proto/client_vtproto.pb.go
//              (hand-optimised marshalling), common/rpc/oxia_error_streaming_client.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The OxiaClient service over gRPC.NET with CUSTOM MARSHALLERS (Grpc.Core.Marshaller<T> built from a
//              contextual serializer/deserializer) so messages are encoded straight into the transport's IBufferWriter
//              and decoded straight from its ReadOnlySequence<byte> -- no Google.Protobuf objects, no intermediate byte[].
//              Method full names are exactly "/io.oxia.proto.v1.OxiaClient/<Rpc>".
//              Every Recv/Send/unary failure is passed through OxiaErrors.FromGrpcError so callers see Oxia sentinels
//              (this is Go's OxiaErrorServerStreamingClient / OxiaErrorBidiStreamingClient wrappers).
// =====================================================================================================================
using Grpc.Core;
using Oxia.Common.Proto;

namespace Oxia.Common.Rpc;

public sealed class OxiaClientRpc : IOxiaClientRpc
{
    public OxiaClientRpc(CallInvoker invoker) => throw new NotImplementedException();

    public IWriteStreamCall WriteStream(Metadata? headers, CancellationToken ct) => throw new NotImplementedException();
    public IServerStreamingClient<ReadResponse> Read(ReadRequest request, CancellationToken ct) => throw new NotImplementedException();
    public IServerStreamingClient<ListResponse> List(ListRequest request, CancellationToken ct) => throw new NotImplementedException();
    public IServerStreamingClient<RangeScanResponse> RangeScan(RangeScanRequest request, CancellationToken ct) => throw new NotImplementedException();
    public IServerStreamingClient<ShardAssignments> GetShardAssignments(ShardAssignmentsRequest request, CancellationToken ct) => throw new NotImplementedException();
    public IServerStreamingClient<GetSequenceUpdatesResponse> GetSequenceUpdates(GetSequenceUpdatesRequest request, CancellationToken ct) => throw new NotImplementedException();
    public IServerStreamingClient<NotificationBatch> GetNotifications(NotificationsRequest request, CancellationToken ct) => throw new NotImplementedException();
    public ValueTask<CreateSessionResponse> CreateSessionAsync(CreateSessionRequest request, CancellationToken ct) => throw new NotImplementedException();
    public ValueTask<KeepAliveResponse> KeepAliveAsync(SessionHeartbeat request, CancellationToken ct) => throw new NotImplementedException();
    public ValueTask<CloseSessionResponse> CloseSessionAsync(CloseSessionRequest request, CancellationToken ct) => throw new NotImplementedException();
}

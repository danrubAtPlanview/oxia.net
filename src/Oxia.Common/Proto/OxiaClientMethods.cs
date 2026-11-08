// =====================================================================================================================
// PORT NOTE
//   Upstream : common/proto/client_grpc.pb.go  (OxiaClient_ServiceDesc, OxiaClient_*_FullMethodName, the streaming flags)
//              common/proto/proto_grpc.go      (the vtproto CodecV2: Marshal = SizeVT + MarshalToSizedBufferVT into the
//                                               transport buffer, Unmarshal = UnmarshalVT from the received buffer)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The Grpc.Core.Method<TRequest,TResponse> descriptors of the OxiaClient service, with Marshaller<T>s built on the
//              hand-written codecs (Oxia.Common.Proto.Codec): the serializer sets the payload length, encodes straight into the
//              transport's IBufferWriter<byte> and completes; the deserializer decodes straight from the transport's
//              ReadOnlySequence<byte> (a single-segment payload takes the contiguous-span fast path).
//   Divergences: Go registers codecs globally (encoding.RegisterCodec with vtproto) and looks methods up by name in a
//              ServiceDesc; here the descriptors are plain static fields.  Method full names are exactly
//              "/io.oxia.proto.v1.OxiaClient/<Rpc>", identical to the protoc-generated Io.Oxia.Proto.V1.OxiaClient.
//              `List` is deprecated upstream (use RangeScan with include_values=false) and `Write` is deprecated in favour of
//              WriteStream; both descriptors are still provided.
// =====================================================================================================================
using Grpc.Core;
using Oxia.Common.Proto.Codec;

namespace Oxia.Common.Proto;

/// <summary>Builds <see cref="Marshaller{T}"/>s from <see cref="IMessageCodec{TMessage}"/> implementations.</summary>
public static class CodecMarshaller
{
    public static Marshaller<TMessage> Create<TCodec, TMessage>()
        where TCodec : IMessageCodec<TMessage> where TMessage : class
        => Marshallers.Create(
            static (TMessage message, SerializationContext context) => Serialize<TCodec, TMessage>(message, context),
            static (DeserializationContext context) => Deserialize<TCodec, TMessage>(context));

    /// <summary>
    /// A marshaller whose deserializer RENTS the (top-level, response) message from its pool instead of allocating it: the consumer of
    /// the call must give it back with <c>Release()</c> (WriteBatch / ReadBatch / RpcProvider do).  A message nobody releases is simply
    /// garbage collected.
    /// </summary>
    public static Marshaller<TMessage> CreatePooled<TCodec, TMessage>()
        where TCodec : IPooledMessageCodec<TMessage> where TMessage : class
        => Marshallers.Create(
            static (TMessage message, SerializationContext context) => Serialize<TCodec, TMessage>(message, context),
            static (DeserializationContext context) => DeserializePooled<TCodec, TMessage>(context));

    /// <summary>
    /// A marshaller for a response that carries no data (KeepAliveResponse, CloseSessionResponse): the payload is validated and
    /// skipped, and every call gets the one shared (field-less, immutable) instance.
    /// </summary>
    public static Marshaller<TMessage> CreateShared<TCodec, TMessage>(TMessage shared)
        where TCodec : IMessageCodec<TMessage> where TMessage : class
        => Marshallers.Create(
            static (TMessage message, SerializationContext context) => Serialize<TCodec, TMessage>(message, context),
            context =>
            {
                var payload = context.PayloadAsReadOnlySequence();
                var reader = new ProtoReader(in payload);
                TCodec.MergeFrom(ref reader, shared);
                return shared;
            });

    /// <summary>The serializer: Size -> SetPayloadLength -> encode into the transport buffer -> Complete.</summary>
    public static void Serialize<TCodec, TMessage>(TMessage message, SerializationContext context)
        where TCodec : IMessageCodec<TMessage> where TMessage : class
    {
        int size = TCodec.Size(message);
        context.SetPayloadLength(size);
        MessageCodec.WriteSized<TCodec, TMessage>(message, context.GetBufferWriter(), size);
        context.Complete();
    }

    /// <summary>The deserializer: a new message parsed from the transport's payload.</summary>
    public static TMessage Deserialize<TCodec, TMessage>(DeserializationContext context)
        where TCodec : IMessageCodec<TMessage> where TMessage : class
    {
        TMessage message = TCodec.Create();
        var payload = context.PayloadAsReadOnlySequence();
        var reader = new ProtoReader(in payload);
        TCodec.MergeFrom(ref reader, message);
        return message;
    }

    /// <summary>The deserializer of a pooled response: a rented message parsed from the transport's payload.</summary>
    public static TMessage DeserializePooled<TCodec, TMessage>(DeserializationContext context)
        where TCodec : IPooledMessageCodec<TMessage> where TMessage : class
    {
        TMessage message = TCodec.Rent();
        var payload = context.PayloadAsReadOnlySequence();
        var reader = new ProtoReader(in payload);
        TCodec.MergeFrom(ref reader, message);
        return message;
    }
}

/// <summary>Marshallers and method descriptors of the <c>io.oxia.proto.v1.OxiaClient</c> service.</summary>
public static class OxiaClientMethods
{
    public const string ServiceName = "io.oxia.proto.v1.OxiaClient";

    public static readonly Marshaller<ShardAssignmentsRequest> ShardAssignmentsRequestMarshaller = CodecMarshaller.Create<ShardAssignmentsRequestCodec, ShardAssignmentsRequest>();
    public static readonly Marshaller<ShardAssignments> ShardAssignmentsMarshaller = CodecMarshaller.Create<ShardAssignmentsCodec, ShardAssignments>();
    public static readonly Marshaller<WriteRequest> WriteRequestMarshaller = CodecMarshaller.Create<WriteRequestCodec, WriteRequest>();
    public static readonly Marshaller<WriteResponse> WriteResponseMarshaller = CodecMarshaller.CreatePooled<WriteResponseCodec, WriteResponse>();
    public static readonly Marshaller<ReadRequest> ReadRequestMarshaller = CodecMarshaller.Create<ReadRequestCodec, ReadRequest>();
    public static readonly Marshaller<ReadResponse> ReadResponseMarshaller = CodecMarshaller.CreatePooled<ReadResponseCodec, ReadResponse>();
    public static readonly Marshaller<ListRequest> ListRequestMarshaller = CodecMarshaller.Create<ListRequestCodec, ListRequest>();
    public static readonly Marshaller<ListResponse> ListResponseMarshaller = CodecMarshaller.CreatePooled<ListResponseCodec, ListResponse>();
    public static readonly Marshaller<RangeScanRequest> RangeScanRequestMarshaller = CodecMarshaller.Create<RangeScanRequestCodec, RangeScanRequest>();
    public static readonly Marshaller<RangeScanResponse> RangeScanResponseMarshaller = CodecMarshaller.CreatePooled<RangeScanResponseCodec, RangeScanResponse>();
    public static readonly Marshaller<GetSequenceUpdatesRequest> GetSequenceUpdatesRequestMarshaller = CodecMarshaller.Create<GetSequenceUpdatesRequestCodec, GetSequenceUpdatesRequest>();
    public static readonly Marshaller<GetSequenceUpdatesResponse> GetSequenceUpdatesResponseMarshaller = CodecMarshaller.Create<GetSequenceUpdatesResponseCodec, GetSequenceUpdatesResponse>();
    public static readonly Marshaller<NotificationsRequest> NotificationsRequestMarshaller = CodecMarshaller.Create<NotificationsRequestCodec, NotificationsRequest>();
    public static readonly Marshaller<NotificationBatch> NotificationBatchMarshaller = CodecMarshaller.CreatePooled<NotificationBatchCodec, NotificationBatch>();
    public static readonly Marshaller<CreateSessionRequest> CreateSessionRequestMarshaller = CodecMarshaller.Create<CreateSessionRequestCodec, CreateSessionRequest>();
    public static readonly Marshaller<CreateSessionResponse> CreateSessionResponseMarshaller = CodecMarshaller.Create<CreateSessionResponseCodec, CreateSessionResponse>();
    public static readonly Marshaller<SessionHeartbeat> SessionHeartbeatMarshaller = CodecMarshaller.Create<SessionHeartbeatCodec, SessionHeartbeat>();
    public static readonly Marshaller<KeepAliveResponse> KeepAliveResponseMarshaller = CodecMarshaller.CreateShared<KeepAliveResponseCodec, KeepAliveResponse>(new KeepAliveResponse());
    public static readonly Marshaller<CloseSessionRequest> CloseSessionRequestMarshaller = CodecMarshaller.Create<CloseSessionRequestCodec, CloseSessionRequest>();
    public static readonly Marshaller<CloseSessionResponse> CloseSessionResponseMarshaller = CodecMarshaller.CreateShared<CloseSessionResponseCodec, CloseSessionResponse>(new CloseSessionResponse());

    // Pooled-value variants (opt-in pooled read API, see Oxia.Client.Pooled): internal so that the descriptor/marshaller catalogue above
    // stays exactly the 11 methods / 20 marshallers of the service.
    internal static readonly Marshaller<ReadResponse> ReadResponsePooledMarshaller = CodecMarshaller.CreatePooled<ReadResponsePooledCodec, ReadResponse>();
    internal static readonly Marshaller<RangeScanResponse> RangeScanResponsePooledMarshaller = CodecMarshaller.CreatePooled<RangeScanResponsePooledCodec, RangeScanResponse>();

    public static readonly Method<ShardAssignmentsRequest, ShardAssignments> GetShardAssignments =
        new(MethodType.ServerStreaming, ServiceName, "GetShardAssignments", ShardAssignmentsRequestMarshaller, ShardAssignmentsMarshaller);

    /// <summary>Deprecated upstream in favour of <see cref="WriteStream"/>.</summary>
    public static readonly Method<WriteRequest, WriteResponse> Write =
        new(MethodType.Unary, ServiceName, "Write", WriteRequestMarshaller, WriteResponseMarshaller);

    public static readonly Method<WriteRequest, WriteResponse> WriteStream =
        new(MethodType.DuplexStreaming, ServiceName, "WriteStream", WriteRequestMarshaller, WriteResponseMarshaller);

    public static readonly Method<ReadRequest, ReadResponse> Read =
        new(MethodType.ServerStreaming, ServiceName, "Read", ReadRequestMarshaller, ReadResponseMarshaller);

    /// <summary>The Read rpc with values parsed into leases (same wire name; used when <c>ReadRequest.PooledValues</c> is set).</summary>
    internal static readonly Method<ReadRequest, ReadResponse> ReadPooled =
        new(MethodType.ServerStreaming, ServiceName, "Read", ReadRequestMarshaller, ReadResponsePooledMarshaller);

    /// <summary>The RangeScan rpc with values and keys parsed into leases (used when <c>RangeScanRequest.PooledValues</c> is set).</summary>
    internal static readonly Method<RangeScanRequest, RangeScanResponse> RangeScanPooled =
        new(MethodType.ServerStreaming, ServiceName, "RangeScan", RangeScanRequestMarshaller, RangeScanResponsePooledMarshaller);

    /// <summary>Deprecated upstream: use RangeScan with include_values = false.</summary>
    public static readonly Method<ListRequest, ListResponse> List =
        new(MethodType.ServerStreaming, ServiceName, "List", ListRequestMarshaller, ListResponseMarshaller);

    public static readonly Method<RangeScanRequest, RangeScanResponse> RangeScan =
        new(MethodType.ServerStreaming, ServiceName, "RangeScan", RangeScanRequestMarshaller, RangeScanResponseMarshaller);

    public static readonly Method<GetSequenceUpdatesRequest, GetSequenceUpdatesResponse> GetSequenceUpdates =
        new(MethodType.ServerStreaming, ServiceName, "GetSequenceUpdates", GetSequenceUpdatesRequestMarshaller, GetSequenceUpdatesResponseMarshaller);

    public static readonly Method<NotificationsRequest, NotificationBatch> GetNotifications =
        new(MethodType.ServerStreaming, ServiceName, "GetNotifications", NotificationsRequestMarshaller, NotificationBatchMarshaller);

    public static readonly Method<CreateSessionRequest, CreateSessionResponse> CreateSession =
        new(MethodType.Unary, ServiceName, "CreateSession", CreateSessionRequestMarshaller, CreateSessionResponseMarshaller);

    public static readonly Method<SessionHeartbeat, KeepAliveResponse> KeepAlive =
        new(MethodType.Unary, ServiceName, "KeepAlive", SessionHeartbeatMarshaller, KeepAliveResponseMarshaller);

    public static readonly Method<CloseSessionRequest, CloseSessionResponse> CloseSession =
        new(MethodType.Unary, ServiceName, "CloseSession", CloseSessionRequestMarshaller, CloseSessionResponseMarshaller);
}

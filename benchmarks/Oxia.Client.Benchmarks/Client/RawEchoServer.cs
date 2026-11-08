// =====================================================================================================================
// PORT NOTE (benchmark support -- no upstream equivalent)
//   Upstream : none
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : A trivial in-process Oxia server (hosted by TestGrpcServer) that ALLOCATES NOTHING per operation, so that the
//              allocation figures of the client benchmarks are the client's own.  One shard led by the server itself; every put
//              / delete / delete-range succeeds, every get finds a 100-byte value.  Instead of the protoc-generated service
//              base (which would parse every request into message objects) the service binds the three RPCs by hand with
//              marshallers that only COUNT the operations of a request (a tiny protobuf tag scanner) and reply with response
//              bytes pre-serialised per "shape" (number of puts / deletes / delete ranges / gets).
// =====================================================================================================================
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using Grpc.Core;
using G = Io.Oxia.Proto.V1;

namespace Oxia.Client.Benchmarks.Client;

/// <summary>
/// Same shape as a protoc-generated service (ASP.NET Core gRPC binds a handler only if it overrides a virtual method of a base
/// type that carries BindServiceMethod): the base declares the three RPCs, <see cref="RawEchoServer"/> implements them.
/// </summary>
[BindServiceMethod(typeof(RawEchoServer), nameof(RawEchoServer.BindService))]
public abstract class RawEchoBase
{
    public virtual Task GetShardAssignments(G.ShardAssignmentsRequest request, IServerStreamWriter<G.ShardAssignments> responseStream, ServerCallContext context)
        => throw new RpcException(new Status(StatusCode.Unimplemented, ""));

    public virtual Task WriteStream(IAsyncStreamReader<RawEchoServer.Shape> requestStream, IServerStreamWriter<RawEchoServer.Shape> responseStream, ServerCallContext context)
        => throw new RpcException(new Status(StatusCode.Unimplemented, ""));

    public virtual Task Read(RawEchoServer.Shape request, IServerStreamWriter<RawEchoServer.Shape> responseStream, ServerCallContext context)
        => throw new RpcException(new Status(StatusCode.Unimplemented, ""));
}

public sealed class RawEchoServer : RawEchoBase
{
    private const string ServiceName = "io.oxia.proto.v1.OxiaClient";

    private static readonly ConcurrentDictionary<(int, int, int, int), Shape> Shapes = new();

    /// <summary>How many operations of each kind a request carries; doubles as the (pre-serialised) response.</summary>
    public sealed class Shape
    {
        private readonly int _puts, _deletes, _ranges, _gets;
        private byte[]? _writeResponse, _readResponse;

        public Shape(int puts, int deletes, int ranges, int gets)
        {
            _puts = puts;
            _deletes = deletes;
            _ranges = ranges;
            _gets = gets;
        }

        public byte[] WriteResponseBytes => _writeResponse ??= BuildWriteResponse();
        public byte[] ReadResponseBytes => _readResponse ??= BuildReadResponse();

        private byte[] BuildWriteResponse()
        {
            var write = new G.WriteResponse();
            for (int i = 0; i < _puts; i++) write.Puts.Add(new G.PutResponse { Status = G.Status.Ok, Version = new G.Version() });
            for (int i = 0; i < _deletes; i++) write.Deletes.Add(new G.DeleteResponse { Status = G.Status.Ok });
            for (int i = 0; i < _ranges; i++) write.DeleteRanges.Add(new G.DeleteRangeResponse { Status = G.Status.Ok });
            return Google.Protobuf.MessageExtensions.ToByteArray(write);
        }

        private byte[] BuildReadResponse()
        {
            var read = new G.ReadResponse();
            var value = Google.Protobuf.ByteString.CopyFrom(new byte[100]);
            for (int i = 0; i < _gets; i++) read.Gets.Add(new G.GetResponse { Status = G.Status.Ok, Version = new G.Version(), Value = value });
            return Google.Protobuf.MessageExtensions.ToByteArray(read);
        }
    }

    private static Shape Count(ReadOnlySequence<byte> payload)
    {
        // WriteRequest { shard = 1; puts = 2; deletes = 3; delete_ranges = 4 }   ReadRequest { shard = 1; gets = 2 }
        int puts = 0, deletes = 0, ranges = 0;
        var reader = new SequenceReader<byte>(payload);
        while (!reader.End)
        {
            ulong tag = ReadVarint(ref reader);
            int field = (int)(tag >> 3);
            switch ((int)(tag & 7))
            {
                case 0: // varint
                    ReadVarint(ref reader);
                    break;
                case 2: // length-delimited
                    long length = (long)ReadVarint(ref reader);
                    reader.Advance(length);
                    if (field == 2) puts++;
                    else if (field == 3) deletes++;
                    else if (field == 4) ranges++;
                    break;
                default:
                    throw new InvalidOperationException("unexpected wire type");
            }
        }
        // a read request carries its gets in field 2 as well
        return Shapes.GetOrAdd((puts, deletes, ranges, puts), static k => new Shape(k.Item1, k.Item2, k.Item3, k.Item4));
    }

    private static ulong ReadVarint(ref SequenceReader<byte> reader)
    {
        ulong result = 0;
        int shift = 0;
        while (true)
        {
            reader.TryRead(out byte b);
            result |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) return result;
            shift += 7;
        }
    }

    private static readonly Marshaller<Shape> RequestMarshaller = new(
        (_, _) => throw new NotSupportedException(),
        ctx => Count(ctx.PayloadAsReadOnlySequence()));

    private static readonly Marshaller<Shape> WriteResponseMarshaller = new(
        (shape, ctx) => ctx.Complete(shape.WriteResponseBytes),
        _ => throw new NotSupportedException());

    private static readonly Marshaller<Shape> ReadResponseMarshaller = new(
        (shape, ctx) => ctx.Complete(shape.ReadResponseBytes),
        _ => throw new NotSupportedException());

    private static readonly Method<G.ShardAssignmentsRequest, G.ShardAssignments> GetShardAssignmentsMethod = new(
        MethodType.ServerStreaming, ServiceName, "GetShardAssignments",
        Marshallers.Create(m => Google.Protobuf.MessageExtensions.ToByteArray(m), G.ShardAssignmentsRequest.Parser.ParseFrom),
        Marshallers.Create(m => Google.Protobuf.MessageExtensions.ToByteArray(m), G.ShardAssignments.Parser.ParseFrom));

    private static readonly Method<Shape, Shape> WriteStreamMethod = new(
        MethodType.DuplexStreaming, ServiceName, "WriteStream", RequestMarshaller, WriteResponseMarshaller);

    private static readonly Method<Shape, Shape> ReadMethod = new(
        MethodType.ServerStreaming, ServiceName, "Read", RequestMarshaller, ReadResponseMarshaller);

    public string Address { get; set; } = "";

    /// <summary>Like the generated BindService: the service is null when ASP.NET Core only discovers the methods.</summary>
    public static void BindService(ServiceBinderBase binder, RawEchoBase? service)
    {
        binder.AddMethod(GetShardAssignmentsMethod, service == null ? null : new ServerStreamingServerMethod<G.ShardAssignmentsRequest, G.ShardAssignments>(service.GetShardAssignments));
        binder.AddMethod(WriteStreamMethod, service == null ? null : new DuplexStreamingServerMethod<Shape, Shape>(service.WriteStream));
        binder.AddMethod(ReadMethod, service == null ? null : new ServerStreamingServerMethod<Shape, Shape>(service.Read));
    }

    public override async Task GetShardAssignments(G.ShardAssignmentsRequest request, IServerStreamWriter<G.ShardAssignments> responseStream, ServerCallContext context)
    {
        var namespaceAssignment = new G.NamespaceShardsAssignment { ShardKeyRouter = G.ShardKeyRouter.Xxhash3 };
        namespaceAssignment.Assignments.Add(new G.ShardAssignment
        {
            Shard = 0,
            Leader = Address,
            Int32HashRange = new G.Int32HashRange { MinHashInclusive = 0, MaxHashInclusive = uint.MaxValue },
        });
        var assignments = new G.ShardAssignments();
        assignments.Namespaces[Oxia.Common.Constant.Constants.DefaultNamespace] = namespaceAssignment;
        await responseStream.WriteAsync(assignments);
        try { await Task.Delay(Timeout.Infinite, context.CancellationToken); } catch (OperationCanceledException) { }
    }

    public override async Task WriteStream(IAsyncStreamReader<Shape> requestStream, IServerStreamWriter<Shape> responseStream, ServerCallContext context)
    {
        while (await requestStream.MoveNext(context.CancellationToken))
        {
            await responseStream.WriteAsync(requestStream.Current);
        }
    }

    public override Task Read(Shape request, IServerStreamWriter<Shape> responseStream, ServerCallContext context)
        => responseStream.WriteAsync(request);
}

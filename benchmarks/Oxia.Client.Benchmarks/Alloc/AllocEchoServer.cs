// =====================================================================================================================
// PORT NOTE (benchmark support -- no upstream equivalent)
//   Upstream : none
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The in-process server of the end-to-end allocation work (wave 2, Z1): like Client/RawEchoServer.cs it ALLOCATES NOTHING
//              per operation (requests are only counted by a protobuf tag scanner, responses are pre-serialised bytes), and it
//              adds what the allocation tests need beyond it:
//                * Read answers KeyNotFound (NotFound = true) or a found 100-byte value, selectable at run time;
//                * RangeScan answers `ScanRecords` records of 100-byte values in chunks of `ScanChunk`;
//                * WriteStream answers puts / deletes / delete-ranges exactly like RawEchoServer.
//              One shard led by the server itself.
// =====================================================================================================================
using System.Buffers;
using System.Collections.Concurrent;
using Grpc.Core;
using G = Io.Oxia.Proto.V1;

namespace Oxia.Client.Benchmarks.Alloc;

/// <summary>Same shape as a protoc-generated service base: ASP.NET Core gRPC binds a handler only for virtuals of a base with BindServiceMethod.</summary>
[BindServiceMethod(typeof(AllocEchoServer), nameof(AllocEchoServer.BindService))]
public abstract class AllocEchoBase
{
    public virtual Task GetShardAssignments(G.ShardAssignmentsRequest request, IServerStreamWriter<G.ShardAssignments> responseStream, ServerCallContext context)
        => throw new RpcException(new Status(StatusCode.Unimplemented, ""));

    public virtual Task WriteStream(IAsyncStreamReader<AllocEchoServer.Shape> requestStream, IServerStreamWriter<AllocEchoServer.Shape> responseStream, ServerCallContext context)
        => throw new RpcException(new Status(StatusCode.Unimplemented, ""));

    public virtual Task Read(AllocEchoServer.Shape request, IServerStreamWriter<AllocEchoServer.Shape> responseStream, ServerCallContext context)
        => throw new RpcException(new Status(StatusCode.Unimplemented, ""));

    public virtual Task RangeScan(AllocEchoServer.Shape request, IServerStreamWriter<AllocEchoServer.Shape> responseStream, ServerCallContext context)
        => throw new RpcException(new Status(StatusCode.Unimplemented, ""));
}

public sealed class AllocEchoServer : AllocEchoBase
{
    private const string ServiceName = "io.oxia.proto.v1.OxiaClient";

    private static readonly ConcurrentDictionary<(int, int, int, int, bool), Shape> Shapes = new();

    /// <summary>The counts of a request, which double as the pre-serialised response.</summary>
    public sealed class Shape
    {
        private readonly int _puts, _deletes, _ranges, _gets;
        private readonly bool _notFound;
        private byte[]? _writeResponse, _readResponse;

        public Shape(int puts, int deletes, int ranges, int gets, bool notFound)
        {
            _puts = puts;
            _deletes = deletes;
            _ranges = ranges;
            _gets = gets;
            _notFound = notFound;
        }

        public byte[] Raw { get; set; } = Array.Empty<byte>();

        public byte[] WriteResponseBytes => _writeResponse ??= BuildWriteResponse();
        public byte[] ReadResponseBytes => _readResponse ??= BuildReadResponse();

        private byte[] BuildWriteResponse()
        {
            var write = new G.WriteResponse();
            // an ephemeral-looking version (client identity) makes the string path visible
            for (int i = 0; i < _puts; i++) write.Puts.Add(new G.PutResponse { Status = G.Status.Ok, Version = new G.Version { VersionId = 42, ModificationsCount = 1, CreatedTimestamp = 1_700_000_000_000UL, ModifiedTimestamp = 1_700_000_000_001UL } });
            for (int i = 0; i < _deletes; i++) write.Deletes.Add(new G.DeleteResponse { Status = G.Status.Ok });
            for (int i = 0; i < _ranges; i++) write.DeleteRanges.Add(new G.DeleteRangeResponse { Status = G.Status.Ok });
            return Google.Protobuf.MessageExtensions.ToByteArray(write);
        }

        private byte[] BuildReadResponse()
        {
            var read = new G.ReadResponse();
            var value = Google.Protobuf.ByteString.CopyFrom(new byte[100]);
            for (int i = 0; i < _gets; i++)
            {
                read.Gets.Add(_notFound
                    ? new G.GetResponse { Status = G.Status.KeyNotFound }
                    : new G.GetResponse { Status = G.Status.Ok, Version = new G.Version { VersionId = 42, ModificationsCount = 1, CreatedTimestamp = 1_700_000_000_000UL, ModifiedTimestamp = 1_700_000_000_001UL }, Value = value });
            }
            return Google.Protobuf.MessageExtensions.ToByteArray(read);
        }
    }

    /// <summary>Reads answer KeyNotFound instead of a found value.</summary>
    public static volatile bool NotFound;

    /// <summary>Number of records a RangeScan answers.</summary>
    public static int ScanRecords = 1000;

    /// <summary>Records per RangeScanResponse message.</summary>
    public static int ScanChunk = 250;

    private static byte[][]? _scanChunks;

    private static byte[][] ScanChunks
    {
        get
        {
            if (_scanChunks is { } cached) return cached;
            var value = Google.Protobuf.ByteString.CopyFrom(new byte[100]);
            var chunks = new List<byte[]>();
            for (int start = 0; start < ScanRecords; start += ScanChunk)
            {
                var response = new G.RangeScanResponse();
                for (int i = start; i < Math.Min(ScanRecords, start + ScanChunk); i++)
                {
                    response.Records.Add(new G.GetResponse
                    {
                        Status = G.Status.Ok,
                        Key = "/scan/key-" + i.ToString("D8"),
                        Value = value,
                        Version = new G.Version { VersionId = i + 1, ModificationsCount = 1, CreatedTimestamp = 1_700_000_000_000UL, ModifiedTimestamp = 1_700_000_000_001UL },
                    });
                }
                chunks.Add(Google.Protobuf.MessageExtensions.ToByteArray(response));
            }
            return _scanChunks = chunks.ToArray();
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
                case 0:
                    ReadVarint(ref reader);
                    break;
                case 2:
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
        return Shapes.GetOrAdd((puts, deletes, ranges, puts, NotFound), static k => new Shape(k.Item1, k.Item2, k.Item3, k.Item4, k.Item5));
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

    private static readonly Shape ScanRequestShape = new(0, 0, 0, 0, false);

    private static readonly Marshaller<Shape> RequestMarshaller = new((_, _) => throw new NotSupportedException(), ctx => Count(ctx.PayloadAsReadOnlySequence()));
    private static readonly Marshaller<Shape> WriteResponseMarshaller = new((shape, ctx) => ctx.Complete(shape.WriteResponseBytes), _ => throw new NotSupportedException());
    private static readonly Marshaller<Shape> ReadResponseMarshaller = new((shape, ctx) => ctx.Complete(shape.ReadResponseBytes), _ => throw new NotSupportedException());
    private static readonly Marshaller<Shape> RawResponseMarshaller = new((shape, ctx) => ctx.Complete(shape.Raw), _ => throw new NotSupportedException());

    public string Address { get; set; } = "";

    /// <summary>Like the generated BindService: the service is null when ASP.NET Core only discovers the methods.</summary>
    public static void BindService(ServiceBinderBase binder, AllocEchoBase? service)
    {
        var getShardAssignments = new Method<G.ShardAssignmentsRequest, G.ShardAssignments>(
            MethodType.ServerStreaming, ServiceName, "GetShardAssignments",
            Marshallers.Create(m => Google.Protobuf.MessageExtensions.ToByteArray(m), G.ShardAssignmentsRequest.Parser.ParseFrom),
            Marshallers.Create(m => Google.Protobuf.MessageExtensions.ToByteArray(m), G.ShardAssignments.Parser.ParseFrom));
        var writeStream = new Method<Shape, Shape>(MethodType.DuplexStreaming, ServiceName, "WriteStream", RequestMarshaller, WriteResponseMarshaller);
        var read = new Method<Shape, Shape>(MethodType.ServerStreaming, ServiceName, "Read", RequestMarshaller, ReadResponseMarshaller);
        var rangeScan = new Method<Shape, Shape>(MethodType.ServerStreaming, ServiceName, "RangeScan",
            new Marshaller<Shape>((_, _) => throw new NotSupportedException(), _ => ScanRequestShape), RawResponseMarshaller);

        binder.AddMethod(getShardAssignments, service == null ? null : new ServerStreamingServerMethod<G.ShardAssignmentsRequest, G.ShardAssignments>(service.GetShardAssignments));
        binder.AddMethod(writeStream, service == null ? null : new DuplexStreamingServerMethod<Shape, Shape>(service.WriteStream));
        binder.AddMethod(read, service == null ? null : new ServerStreamingServerMethod<Shape, Shape>(service.Read));
        binder.AddMethod(rangeScan, service == null ? null : new ServerStreamingServerMethod<Shape, Shape>(service.RangeScan));
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

    private static Shape[]? _scanShapes;

    public override async Task RangeScan(Shape request, IServerStreamWriter<Shape> responseStream, ServerCallContext context)
    {
        var shapes = _scanShapes ??= ScanChunks.Select(bytes => new Shape(0, 0, 0, 0, false) { Raw = bytes }).ToArray();
        foreach (var shape in shapes) await responseStream.WriteAsync(shape);
    }
}

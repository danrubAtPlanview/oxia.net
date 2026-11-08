// =====================================================================================================================
// PORT NOTE (tests)
//   Upstream : (none -- allocation contract of the hot-path codecs; Go has no equivalent because it marshals into a
//              caller-owned []byte with vtproto)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Pins the zero-allocation promise: encoding a WriteRequest with 1000 PutRequests into a reusable
//              ArrayBufferWriter allocates nothing after warm-up; parsing a WriteResponse (or ReadResponse /
//              NotificationBatch / RangeScanResponse) into a REUSED instance allocates nothing apart from the strings and
//              byte arrays that carry data; the gRPC marshallers add no allocation on the serialize path.
//   Divergences: n/a (new tests)
// =====================================================================================================================
using System.Buffers;
using Grpc.Core;
using Oxia.Common.Proto;
using Oxia.Common.Proto.Codec;
using Xunit;
using Status = Oxia.Common.Proto.Status;

namespace Oxia.Client.Tests.Proto;

public class CodecAllocationTests
{
    private static long Measure(Action action, int warmup = 400, int iterations = 50)
    {
        for (int i = 0; i < warmup; i++) action();
        // let the tiered JIT finish promoting the hot methods before measuring
        for (int round = 0; round < 3; round++)
        {
            for (int i = 0; i < 200; i++) action();
            Thread.Sleep(60);
        }
        for (int i = 0; i < 50; i++) action();
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < iterations; i++) action();
        return (GC.GetAllocatedBytesForCurrentThread() - before) / iterations;
    }

    private static WriteRequest BuildWriteRequest(int puts)
    {
        var req = new WriteRequest { Shard = 12 };
        var value = new byte[100];
        for (int i = 0; i < puts; i++)
        {
            req.Puts.Add(new PutRequest
            {
                Key = "/benchmark/key-" + i.ToString("D8"),
                Value = value,
                ExpectedVersionId = i % 3 == 0 ? i : null,
                SessionId = i % 5 == 0 ? 77 : null,
                PartitionKey = i % 7 == 0 ? "partition" : null,
                OpIndex = (uint)i,
            });
        }
        for (int i = 0; i < 50; i++) req.Deletes.Add(new DeleteRequest { Key = "/del/" + i, ExpectedVersionId = i, OpIndex = (uint)(puts + i) });
        for (int i = 0; i < 5; i++) req.DeleteRanges.Add(new DeleteRangeRequest { StartInclusive = "/a/" + i, EndExclusive = "/b/" + i, OpIndex = (uint)(puts + 50 + i) });
        return req;
    }

    [Fact]
    public void EncodingWriteRequestWith1000Puts_IntoReusableBufferWriter_AllocatesNothing()
    {
        WriteRequest req = BuildWriteRequest(1000);
        var output = new ArrayBufferWriter<byte>(256 * 1024);

        long allocated = Measure(() =>
        {
            output.Clear();
            WriteRequestCodec.Write(req, output);
        });
        Assert.Equal(0, allocated);
        Assert.True(output.WrittenCount > 100_000);
    }

    [Fact]
    public void EncodingThroughTheGrpcSerializer_IsAllocationFree()
    {
        WriteRequest req = BuildWriteRequest(1000);
        var context = new FakeSerializationContext(new ArrayBufferWriter<byte>(256 * 1024));
        Marshaller<WriteRequest> marshaller = OxiaClientMethods.WriteRequestMarshaller;

        long allocated = Measure(() =>
        {
            context.Reset();
            marshaller.ContextualSerializer(req, context);
        });
        Assert.Equal(0, allocated);
        Assert.True(context.Completed);
        Assert.Equal(context.Output.WrittenCount, context.DeclaredLength);
    }

    [Fact]
    public void EncodingSmallMessages_AllocatesNothing()
    {
        var output = new ArrayBufferWriter<byte>(4096);
        var read = new ReadRequest { Shard = 1 };
        for (int i = 0; i < 100; i++) read.Gets.Add(new GetRequest { Key = "/key/" + i, IncludeValue = true, ComparisonType = KeyComparisonType.Floor });
        var delete = new DeleteRequest { Key = "k", ExpectedVersionId = 3 };
        var heartbeat = new SessionHeartbeat { Shard = 1, SessionId = 99 };

        long allocated = Measure(() =>
        {
            output.Clear();
            ReadRequestCodec.Write(read, output);
            output.Clear();
            DeleteRequestCodec.Write(delete, output);
            output.Clear();
            SessionHeartbeatCodec.Write(heartbeat, output);
        });
        Assert.Equal(0, allocated);
    }

    private static byte[] BuildWriteResponseBytes(int puts, bool withKeys)
    {
        var resp = new WriteResponse();
        for (int i = 0; i < puts; i++)
        {
            resp.Puts.Add(new PutResponse
            {
                Status = Status.Ok,
                Version = new ProtoVersion { VersionId = 1_000_000 + i, ModificationsCount = i % 4, CreatedTimestamp = 1_700_000_000_000UL + (ulong)i, ModifiedTimestamp = 1_700_000_000_500UL + (ulong)i, SessionId = i % 2 == 0 ? 123456 : null },
                Key = withKeys ? "generated-" + i : null,
            });
        }
        for (int i = 0; i < 20; i++) resp.Deletes.Add(new DeleteResponse { Status = i % 2 == 0 ? Status.Ok : Status.KeyNotFound });
        for (int i = 0; i < 3; i++) resp.DeleteRanges.Add(new DeleteRangeResponse { Status = Status.Ok });
        var w = new ArrayBufferWriter<byte>();
        WriteResponseCodec.Write(resp, w);
        return w.WrittenSpan.ToArray();
    }

    [Fact]
    public void ParsingWriteResponseIntoAReusedInstance_AllocatesNothing()
    {
        byte[] data = BuildWriteResponseBytes(1000, withKeys: false);
        var reused = new WriteResponse();

        long allocated = Measure(() => WriteResponseCodec.Parse(data, reused));
        Assert.Equal(0, allocated);
        Assert.Equal(1000, reused.Puts.Count);
        Assert.Equal(1_000_999L, reused.Puts[999].Version!.VersionId);
        Assert.Equal(20, reused.Deletes.Count);
        Assert.Equal(3, reused.DeleteRanges.Count);
    }

    [Fact]
    public void ParsingWriteResponseFromAMultiSegmentSequence_IntoAReusedInstance_AllocatesNothing()
    {
        byte[] data = BuildWriteResponseBytes(1000, withKeys: false);
        ReadOnlySequence<byte> seq = Segments.Chunks(data, 1500);
        Assert.False(seq.IsSingleSegment);
        var reused = new WriteResponse();

        long allocated = Measure(() => WriteResponseCodec.Parse(in seq, reused));
        Assert.Equal(0, allocated);
        Assert.Equal(1000, reused.Puts.Count);
        Assert.Equal(1_000_999L, reused.Puts[999].Version!.VersionId);
    }

    [Fact]
    public void ParsingWriteResponse_AllocatesOnlyTheStrings()
    {
        byte[] data = BuildWriteResponseBytes(1000, withKeys: true);
        var reused = new WriteResponse();
        long expected = 0;
        for (int i = 0; i < 1000; i++) expected += 22 + (("generated-" + i).Length * 2 + 7) / 8 * 8; // string header (22 incl. length+terminator rounding) + UTF-16 payload

        long allocated = Measure(() => WriteResponseCodec.Parse(data, reused));
        Assert.True(allocated <= expected + 1000 * 8, $"allocated {allocated} bytes per parse, strings account for about {expected}");
        Assert.Equal("generated-999", reused.Puts[999].Key);
    }

    [Fact]
    public void ParsingReadResponse_AllocatesOnlyTheValueArrays()
    {
        var resp = new ReadResponse();
        long arrays = 0;
        for (int i = 0; i < 500; i++)
        {
            var value = new byte[10 + i % 200];
            arrays += (24 + value.Length + 7) / 8 * 8;
            resp.Gets.Add(new GetResponse { Status = Status.Ok, Value = value, Version = new ProtoVersion { VersionId = i, ModificationsCount = 1 } });
        }
        resp.Gets.Add(new GetResponse { Status = Status.KeyNotFound });
        var w = new ArrayBufferWriter<byte>();
        ReadResponseCodec.Write(resp, w);
        byte[] data = w.WrittenSpan.ToArray();
        var reused = new ReadResponse();

        long allocated = Measure(() => ReadResponseCodec.Parse(data, reused));
        Assert.True(allocated <= arrays, $"allocated {allocated} bytes per parse, value arrays account for {arrays}");
        Assert.Equal(501, reused.Gets.Count);
        Assert.Null(reused.Gets[500].Value);
    }

    [Fact]
    public void ParsingRangeScanResponse_AllocatesOnlyKeysAndValues()
    {
        var resp = new RangeScanResponse { EndInclusive = true };
        long expected = 0;
        for (int i = 0; i < 300; i++)
        {
            string key = "/scan/" + i;
            var value = new byte[64];
            expected += (22 + key.Length * 2 + 7) / 8 * 8 + (24 + 64);
            resp.Records.Add(new GetResponse { Status = Status.Ok, Key = key, Value = value, Version = new ProtoVersion { VersionId = i } });
        }
        var w = new ArrayBufferWriter<byte>();
        RangeScanResponseCodec.Write(resp, w);
        byte[] data = w.WrittenSpan.ToArray();
        var reused = new RangeScanResponse();
        long allocated = Measure(() => RangeScanResponseCodec.Parse(data, reused));
        Assert.True(allocated <= expected + 300 * 8, $"allocated {allocated}, expected about {expected}");
        Assert.True(reused.EndInclusive);
    }

    [Fact]
    public void ParsingNotificationBatch_AllocatesOnlyKeyStrings()
    {
        var batch = new NotificationBatch { Shard = 3, Offset = 99, Timestamp = 123456789 };
        long expected = 0;
        for (int i = 0; i < 200; i++)
        {
            string key = "/notif/" + i;
            expected += (22 + key.Length * 2 + 7) / 8 * 8;
            batch.Notifications.Add(new NotificationEntry { Key = key, Value = new Oxia.Common.Proto.Notification { Type = Oxia.Common.Proto.NotificationType.KeyModified, VersionId = i } });
        }
        var w = new ArrayBufferWriter<byte>();
        NotificationBatchCodec.Write(batch, w);
        byte[] data = w.WrittenSpan.ToArray();
        var reused = new NotificationBatch();
        long allocated = Measure(() => NotificationBatchCodec.Parse(data, reused));
        Assert.True(allocated <= expected + 200 * 8, $"allocated {allocated}, expected about {expected}");
        Assert.Equal(200, reused.Notifications.Count);
    }

    [Fact]
    public void ResetThenParse_RecyclesChildren_WithoutAliasingBetweenParses()
    {
        byte[] first = BuildWriteResponseBytes(10, withKeys: true);
        byte[] second = BuildWriteResponseBytes(4, withKeys: false);
        var reused = new WriteResponse();
        WriteResponseCodec.Parse(first, reused);
        var firstChildren = new HashSet<PutResponse>(reused.Puts, ReferenceEqualityComparer.Instance);
        PutResponse firstChild = reused.Puts[0];
        Assert.Equal("generated-0", firstChild.Key);
        WriteResponseCodec.Parse(second, reused);
        Assert.Equal(4, reused.Puts.Count);
        Assert.All(reused.Puts, p => Assert.Null(p.Key)); // no stale key from the previous parse
        Assert.All(reused.Puts, p => Assert.Contains(p, firstChildren)); // ... and the child object really was recycled
        reused.Reset();
        Assert.Empty(reused.Puts);
        Assert.Empty(reused.Deletes);
    }
}

/// <summary>Minimal in-memory gRPC SerializationContext.</summary>
internal sealed class FakeSerializationContext : SerializationContext
{
    public FakeSerializationContext(ArrayBufferWriter<byte> output) => Output = output;

    public ArrayBufferWriter<byte> Output { get; }
    public int DeclaredLength { get; private set; } = -1;
    public bool Completed { get; private set; }

    public void Reset()
    {
        Output.Clear();
        DeclaredLength = -1;
        Completed = false;
    }

    public override void SetPayloadLength(int payloadLength) => DeclaredLength = payloadLength;
    public override IBufferWriter<byte> GetBufferWriter() => Output;
    public override void Complete() => Completed = true;
    public override void Complete(byte[] payload) => throw new NotSupportedException();
}

/// <summary>Minimal in-memory gRPC DeserializationContext.</summary>
internal sealed class FakeDeserializationContext : DeserializationContext
{
    private readonly ReadOnlySequence<byte> _payload;

    public FakeDeserializationContext(ReadOnlySequence<byte> payload) => _payload = payload;

    public override int PayloadLength => checked((int)_payload.Length);
    public override byte[] PayloadAsNewBuffer() => _payload.ToArray();
    public override ReadOnlySequence<byte> PayloadAsReadOnlySequence() => _payload;
}

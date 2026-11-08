// =====================================================================================================================
// PORT NOTE (tests)
//   Upstream : (none -- new tests for the hand-written codecs; protobuf-go / Google.Protobuf are the oracles)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Targeted wire-format cases that random generation does not reach reliably: hand-crafted inputs (packed and
//              unpacked repeated fields, duplicated and re-ordered fields, wrong wire types, unknown fields inside nested
//              messages, groups, non-minimal / overlong varints, bad lengths), UTF-8 length-prefix boundaries, large
//              values written through tiny output chunks and read through tiny segments, writer error handling.
//   Divergences: n/a (new tests)
// =====================================================================================================================
using System.Buffers;
using System.Text;
using Google.Protobuf;
using Oxia.Common.Proto;
using Oxia.Common.Proto.Codec;
using Xunit;
using G = Io.Oxia.Proto.V1;

namespace Oxia.Client.Tests.Proto;

public class CodecWireTests
{
    private static byte[] Encode<TCodec, T>(T m) where TCodec : IMessageCodec<T> where T : class
    {
        var w = new ArrayBufferWriter<byte>();
        MessageCodec.Write<TCodec, T>(m, w);
        return w.WrittenSpan.ToArray();
    }

    private static T Parse<TCodec, T>(byte[] data) where TCodec : IMessageCodec<T> where T : class, new()
    {
        var m = new T();
        MessageCodec.Parse<TCodec, T>(data, m);
        return m;
    }

    private static T ParseSplit<TCodec, T>(byte[] data) where TCodec : IMessageCodec<T> where T : class, new()
    {
        // every split point must give the same answer
        string first = "";
        T? result = null;
        for (int p = 0; p <= data.Length; p++)
        {
            var seq = Segments.Split(data, p);
            var m = new T();
            MessageCodec.Parse<TCodec, T>(in seq, m);
            string dump = ProtoReflection.Dump(m);
            if (result is null) { result = m; first = dump; }
            else Assert.Equal(first, dump);
        }
        return result!;
    }

    // ---- hand-crafted inputs -----------------------------------------------------------------------------------------

    [Fact]
    public void PackedAndUnpackedSequenceKeyDelta_AreBothAcceptedAndConcatenated()
    {
        byte[] data = new WireBuilder()
            .String(1, "k")
            .Bytes(7, new WireBuilder().RawVarint(1).RawVarint(300).ToArray())   // packed
            .Varint(7, 5)                                                         // unpacked element
            .Bytes(7, new WireBuilder().RawVarint(ulong.MaxValue).ToArray())      // second packed block
            .ToArray();
        var hand = ParseSplit<PutRequestCodec, PutRequest>(data);
        Assert.Equal(new ulong[] { 1, 300, 5, ulong.MaxValue }, hand.SequenceKeyDelta);
        Assert.Equal(new ulong[] { 1, 300, 5, ulong.MaxValue }, G.PutRequest.Parser.ParseFrom(data).SequenceKeyDelta);
    }

    [Fact]
    public void EmptyPackedBlock_IsAbsentField()
    {
        byte[] data = new WireBuilder().Bytes(7, Array.Empty<byte>()).ToArray();
        var hand = Parse<PutRequestCodec, PutRequest>(data);
        Assert.True(hand.SequenceKeyDelta is null || hand.SequenceKeyDelta.Length == 0);
    }

    [Fact]
    public void DuplicateScalarFields_LastWins()
    {
        byte[] data = new WireBuilder().String(1, "first").Varint(3, 7).String(1, "second").Varint(3, 9).ToArray();
        var hand = ParseSplit<PutRequestCodec, PutRequest>(data);
        Assert.Equal("second", hand.Key);
        Assert.Equal(9L, hand.ExpectedVersionId);
        var g = G.PutRequest.Parser.ParseFrom(data);
        Assert.Equal("second", g.Key);
        Assert.Equal(9L, g.ExpectedVersionId);
    }

    [Fact]
    public void DuplicateNestedMessage_IsMerged()
    {
        byte[] data = new WireBuilder()
            .Message(2, new WireBuilder().Varint(1, 5).Varint(2, 6))
            .Message(2, new WireBuilder().Varint(2, 60).Fixed64(3, 99))
            .ToArray();
        var hand = ParseSplit<PutResponseCodec, PutResponse>(data);
        var g = G.PutResponse.Parser.ParseFrom(data);
        Assert.Equal(g.Version.VersionId, hand.Version!.VersionId);
        Assert.Equal(g.Version.ModificationsCount, hand.Version.ModificationsCount);
        Assert.Equal(g.Version.CreatedTimestamp, hand.Version.CreatedTimestamp);
        Assert.Equal(5L, hand.Version.VersionId);
        Assert.Equal(60L, hand.Version.ModificationsCount);
        Assert.Equal(99UL, hand.Version.CreatedTimestamp);
    }

    [Fact]
    public void WrongWireTypeOfKnownField_IsSkippedAsUnknown()
    {
        // PutRequest.key (field 1) sent as a varint, value (field 2) as fixed32, expected_version_id (3) as a string
        byte[] data = new WireBuilder().Varint(1, 12345).Fixed32(2, 7).String(3, "x").String(1, "real").ToArray();
        var hand = ParseSplit<PutRequestCodec, PutRequest>(data);
        Assert.Equal("real", hand.Key);
        Assert.True(hand.Value.IsEmpty);
        Assert.Null(hand.ExpectedVersionId);
        Assert.Equal("real", G.PutRequest.Parser.ParseFrom(data).Key);
    }

    [Fact]
    public void UnknownFieldsInsideNestedMessages_AreSkipped()
    {
        var version = new WireBuilder().Varint(1, 11).Varint(77, 5).Bytes(88, new byte[] { 1, 2, 3 }).Fixed64(99, 1).Fixed32(100, 2)
            .Group(101, new WireBuilder().Varint(1, 1).Group(2, new WireBuilder().Bytes(3, new byte[] { 9 }))).Varint(2, 22);
        var put = new WireBuilder().Varint(1, 1).Message(2, version).Varint(500, 1).String(3, "gen-key");
        byte[] data = new WireBuilder().Message(1, put).Varint(900, 3).Message(1, new WireBuilder().Varint(1, 2)).ToArray();

        var hand = ParseSplit<WriteResponseCodec, WriteResponse>(data);
        var g = G.WriteResponse.Parser.ParseFrom(data);
        Assert.Equal(2, hand.Puts.Count);
        Assert.Equal(Status.KeyNotFound, hand.Puts[0].Status);
        Assert.Equal(11L, hand.Puts[0].Version!.VersionId);
        Assert.Equal(22L, hand.Puts[0].Version!.ModificationsCount);
        Assert.Equal("gen-key", hand.Puts[0].Key);
        Assert.Equal(Status.UnexpectedVersionId, hand.Puts[1].Status);
        Assert.Equal(g.Puts[0].Version.ModificationsCount, hand.Puts[0].Version!.ModificationsCount);
    }

    [Fact]
    public void NonMinimalVarints_AreAccepted()
    {
        // 0 encoded as 0x80 0x00, 1 as 0x81 0x80 0x00, a tag as two bytes
        byte[] data = { 0x88, 0x00, 0x00,                 // field 1 (tag 8 as 2 bytes) varint 0 -> default
                        0x10, 0x81, 0x80, 0x00 };         // field 2 varint 1
        var hand = ParseSplit<ProtoVersionCodec, ProtoVersion>(data);
        Assert.Equal(0L, hand.VersionId);
        Assert.Equal(1L, hand.ModificationsCount);
        var g = G.Version.Parser.ParseFrom(data);
        Assert.Equal(1L, g.ModificationsCount);
    }

    [Fact]
    public void NegativeInt64AndEnum_UseTenByteVarints()
    {
        var v = new ProtoVersion { VersionId = -1, ModificationsCount = long.MinValue, SessionId = -5 };
        byte[] data = Encode<ProtoVersionCodec, ProtoVersion>(v);
        Assert.Equal(1 + 10 + 1 + 10 + 1 + 10, data.Length);
        Assert.Equal(new G.Version { VersionId = -1, ModificationsCount = long.MinValue, SessionId = -5 }.ToByteArray(), data);
        var back = Parse<ProtoVersionCodec, ProtoVersion>(data);
        Assert.Equal(-1L, back.VersionId);
        Assert.Equal(long.MinValue, back.ModificationsCount);
        Assert.Equal(-5L, back.SessionId);

        var dr = new DeleteResponse { Status = (Status)(-2) };
        byte[] d = Encode<DeleteResponseCodec, DeleteResponse>(dr);
        Assert.Equal(new G.DeleteResponse { Status = (G.Status)(-2) }.ToByteArray(), d);
        Assert.Equal((Status)(-2), Parse<DeleteResponseCodec, DeleteResponse>(d).Status);
    }

    [Fact]
    public void MapEntry_OrderMissingPartsAndDuplicates()
    {
        var nsa = new WireBuilder().Message(1, new WireBuilder().Varint(1, 7).String(2, "leader:1")).Varint(2, 1);
        byte[] data = new WireBuilder()
            .Message(1, new WireBuilder().Message(2, nsa).String(1, "value-first"))      // value before key
            .Message(1, new WireBuilder().String(1, "key-only"))                         // no value
            .Message(1, new WireBuilder().Message(2, nsa))                               // no key
            .Message(1, new WireBuilder().String(1, "dup").Message(2, new WireBuilder().Varint(2, 1)))
            .Message(1, new WireBuilder().String(1, "dup").Message(2, new WireBuilder().Varint(2, 0)))   // replaces
            .String(2, "authority-a").String(2, "").String(2, "authority-b")
            .ToArray();
        var hand = ParseSplit<ShardAssignmentsCodec, ShardAssignments>(data);
        var g = G.ShardAssignments.Parser.ParseFrom(data);
        Assert.Equal(g.Namespaces.Keys.OrderBy(k => k), hand.Namespaces.Keys.OrderBy(k => k));
        Assert.Equal(7L, hand.Namespaces["value-first"].Assignments[0].Shard);
        Assert.Empty(hand.Namespaces["key-only"].Assignments);
        Assert.Equal("leader:1", hand.Namespaces[""].Assignments[0].Leader);
        Assert.Equal(ShardKeyRouter.Unknown, hand.Namespaces["dup"].ShardKeyRouter);
        Assert.Equal(g.AllowedAuthorities, hand.AllowedAuthorities);
        Assert.Equal(new[] { "authority-a", "", "authority-b" }, hand.AllowedAuthorities);
    }

    [Fact]
    public void MapEntryWithEmptyKey_IsWrittenLikeVtproto()
    {
        // vtproto always writes the key field of a map entry (here empty); Google.Protobuf C# omits it. Both parse identically.
        var m = new ShardAssignments();
        m.Namespaces[""] = new NamespaceShardsAssignment { ShardKeyRouter = ShardKeyRouter.Xxhash3 };
        byte[] data = Encode<ShardAssignmentsCodec, ShardAssignments>(m);
        Assert.Equal(new byte[] { 0x0A, 0x06, 0x0A, 0x00, 0x12, 0x02, 0x10, 0x01 }, data);
        var g = G.ShardAssignments.Parser.ParseFrom(data);
        Assert.Equal(G.ShardKeyRouter.Xxhash3, g.Namespaces[""].ShardKeyRouter);
        Assert.Equal(ShardKeyRouter.Xxhash3, Parse<ShardAssignmentsCodec, ShardAssignments>(data).Namespaces[""].ShardKeyRouter);
    }

    [Fact]
    public void OptionalFields_AreEmittedWhenPresentEvenIfDefault()
    {
        var put = new PutRequest { Key = "", ExpectedVersionId = 0, SessionId = 0, ClientIdentity = "", PartitionKey = "", OverrideVersionId = 0, OverrideModificationsCount = 0 };
        byte[] data = Encode<PutRequestCodec, PutRequest>(put);
        Assert.Equal(new byte[] { 0x18, 0, 0x20, 0, 0x2A, 0, 0x32, 0, 0x48, 0, 0x50, 0 }, data);
        var g = G.PutRequest.Parser.ParseFrom(data);
        Assert.True(g.HasExpectedVersionId && g.HasSessionId && g.HasClientIdentity && g.HasPartitionKey && g.HasOverrideVersionId && g.HasOverrideModificationsCount);
        var back = Parse<PutRequestCodec, PutRequest>(data);
        Assert.Equal(0L, back.ExpectedVersionId);
        Assert.Equal("", back.PartitionKey);
        Assert.Equal("", back.ClientIdentity);

        // and a default-valued proto3 scalar is omitted
        Assert.Empty(Encode<PutRequestCodec, PutRequest>(new PutRequest()));
        Assert.Empty(Encode<GetRequestCodec, GetRequest>(new GetRequest()));
        Assert.Equal(new byte[] { 0x22, 0x00 }, Encode<GetRequestCodec, GetRequest>(new GetRequest { SecondaryIndexName = "" }));
        Assert.Equal(new byte[] { 0x38, 0x00 }, Encode<RangeScanRequestCodec, RangeScanRequest>(new RangeScanRequest { IncludeValues = false }));
        Assert.Equal(new byte[] { 0x08, 0x00 }, Encode<WriteRequestCodec, WriteRequest>(new WriteRequest { Shard = 0 }));
    }

    [Fact]
    public void GetResponseValue_PresentButEmpty_IsDistinctFromAbsent()
    {
        var absent = new GetResponse { Status = Status.KeyNotFound };
        var empty = new GetResponse { Status = Status.Ok, Value = Array.Empty<byte>() };
        Assert.Equal(new byte[] { 0x08, 0x01 }, Encode<GetResponseCodec, GetResponse>(absent));
        Assert.Equal(new byte[] { 0x1A, 0x00 }, Encode<GetResponseCodec, GetResponse>(empty));
        Assert.Null(Parse<GetResponseCodec, GetResponse>(new byte[] { 0x08, 0x01 }).Value);
        byte[]? parsed = Parse<GetResponseCodec, GetResponse>(new byte[] { 0x1A, 0x00 }).Value;
        Assert.NotNull(parsed);
        Assert.Empty(parsed!);
    }

    [Fact]
    public void NotificationEntry_EmptyKeyAndEmptyValue_AreEmitted()
    {
        var e = new NotificationEntry { Key = "", Value = new Oxia.Common.Proto.Notification() };
        byte[] data = Encode<NotificationEntryCodec, NotificationEntry>(e);
        Assert.Equal(new byte[] { 0x0A, 0x00, 0x12, 0x00 }, data);
        var g = G.NotificationEntry.Parser.ParseFrom(data);
        Assert.True(g.HasKey);
        Assert.NotNull(g.Value);
        var back = Parse<NotificationEntryCodec, NotificationEntry>(data);
        Assert.Equal("", back.Key);
        Assert.NotNull(back.Value);
    }

    [Fact]
    public void FixedWidthFields_UseLittleEndianFixed32And64()
    {
        var hr = new Int32HashRange { MinHashInclusive = 0x01020304, MaxHashInclusive = uint.MaxValue };
        Assert.Equal(new byte[] { 0x0D, 4, 3, 2, 1, 0x15, 0xFF, 0xFF, 0xFF, 0xFF }, Encode<Int32HashRangeCodec, Int32HashRange>(hr));
        var v = new ProtoVersion { CreatedTimestamp = 0x0102030405060708, ModifiedTimestamp = ulong.MaxValue };
        Assert.Equal(new G.Version { CreatedTimestamp = 0x0102030405060708, ModifiedTimestamp = ulong.MaxValue }.ToByteArray(), Encode<ProtoVersionCodec, ProtoVersion>(v));
        var b = new NotificationBatch { Timestamp = 0xFFFFFFFFFFFFFFF0 };
        Assert.Equal(new G.NotificationBatch { Timestamp = 0xFFFFFFFFFFFFFFF0 }.ToByteArray(), Encode<NotificationBatchCodec, NotificationBatch>(b));
    }

    // ---- malformed input ---------------------------------------------------------------------------------------------

    public static IEnumerable<object[]> MalformedInputs()
    {
        yield return new object[] { "zero tag", new byte[] { 0x00, 0x00 } };
        yield return new object[] { "tag only", new byte[] { 0x0A } };
        yield return new object[] { "length beyond end", new byte[] { 0x0A, 0x05, 0x61, 0x62 } };
        yield return new object[] { "length 2GiB", new byte[] { 0x0A, 0xFF, 0xFF, 0xFF, 0xFF, 0x07 } };
        yield return new object[] { "length uint32 max", new byte[] { 0x0A, 0xFF, 0xFF, 0xFF, 0xFF, 0x0F } };
        yield return new object[] { "negative (10 byte) length", new byte[] { 0x0A, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01 } };
        yield return new object[] { "11 byte varint", new byte[] { 0x08, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01 } };
        yield return new object[] { "unterminated varint", new byte[] { 0x08, 0x80, 0x80 } };
        yield return new object[] { "truncated fixed64", new byte[] { 0x19, 1, 2, 3 } };
        yield return new object[] { "truncated fixed32", new byte[] { 0x0D, 1, 2 } };
        yield return new object[] { "wire type 6", new byte[] { 0x0E, 0x00 } };
        yield return new object[] { "wire type 7", new byte[] { 0x0F, 0x00 } };
        yield return new object[] { "stray end-group", new byte[] { 0x0C } };
        yield return new object[] { "unclosed group", new byte[] { 0xA3, 0x06, 0x08, 0x01 } };
        yield return new object[] { "mismatched end-group", new byte[] { 0xA3, 0x06, 0xB4, 0x06 } };
        yield return new object[] { "tag > 32 bits", new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01, 0x00 } };
    }

    [Theory]
    [MemberData(nameof(MalformedInputs))]
    public void MalformedInput_ThrowsProtoParseException_ForSpanAndSequence(string name, byte[] data)
    {
        foreach (IMessageKind kind in new[] { MessageKinds.Get("PutRequest"), MessageKinds.Get("WriteResponse"), MessageKinds.Get("ProtoVersion"), MessageKinds.Get("ShardAssignments") })
        {
            object target = kind.NewHand();
            Assert.True(name.Length > 0); Assert.Throws<ProtoParseException>(() => kind.ParseInto(target, data));
            for (int p = 0; p <= data.Length; p++)
            {
                var seq = Segments.Split(data, p);
                Assert.Throws<ProtoParseException>(() => kind.ParseInto(target, in seq));
            }
        }
    }

    [Fact]
    public void NestedLengthPastEnclosingMessage_Throws()
    {
        // inner message claims 10 bytes but the enclosing message only has 3 more
        byte[] data = new WireBuilder().Raw(0x0A, 0x05, 0x12, 0x0A, 0x01, 0x02, 0x03).Varint(5, 1).ToArray();
        Assert.Throws<ProtoParseException>(() => Parse<WriteResponseCodec, WriteResponse>(data));
        var seq = Segments.Chunks(data, 1);
        Assert.Throws<ProtoParseException>(() => MessageCodec.Parse<WriteResponseCodec, WriteResponse>(in seq, new WriteResponse()));
    }

    [Fact]
    public void FieldReadDoesNotCrossTheEndOfItsMessage()
    {
        // version { version_id = <varint that continues past the nested message end into the next field> }
        byte[] data = new WireBuilder().Raw(0x12, 0x02, 0x08, 0x80).Raw(0x01, 0x20, 0x01).ToArray();
        Assert.Throws<ProtoParseException>(() => Parse<PutResponseCodec, PutResponse>(data));
        Assert.Throws<ProtoParseException>(() => ParseSplit<PutResponseCodec, PutResponse>(data));
    }

    // ---- string length-prefix boundaries -----------------------------------------------------------------------------

    public static IEnumerable<object[]> StringShapes()
    {
        foreach (int n in new[] { 0, 1, 2, 41, 42, 43, 44, 63, 64, 84, 85, 126, 127, 128, 129, 200, 5460, 5461, 5462, 5463, 6000, 20_000 })
        {
            yield return new object[] { "ascii", n };
            yield return new object[] { "euro", n };        // 3-byte chars
            yield return new object[] { "pairs", n };       // 4-byte pairs (n chars = n/2 pairs)
            yield return new object[] { "mixed", n };
        }
    }

    private static string Shape(string shape, int n)
    {
        var sb = new StringBuilder();
        switch (shape)
        {
            case "ascii": return new string('a', n);
            case "euro": return new string('€', n);
            case "pairs":
                for (int i = 0; i + 1 < n; i += 2) sb.Append("\U0001F600");
                if (n % 2 == 1) sb.Append('x');
                return sb.ToString();
            default:
                for (int i = 0; i < n; i++) sb.Append((i % 4) switch { 0 => 'a', 1 => 'é', 2 => '€', _ => 'z' });
                return sb.ToString();
        }
    }

    [Theory]
    [MemberData(nameof(StringShapes))]
    public void StringLengthPrefix_AllBoundaries_MatchGoogleProtobuf(string shape, int n)
    {
        string s = Shape(shape, n);
        var put = new PutRequest { Key = s, PartitionKey = s, ClientIdentity = s };
        byte[] expected = new G.PutRequest { Key = s, PartitionKey = s, ClientIdentity = s }.ToByteArray();

        // roomy destination: the one-pass "reserve the length prefix" path
        Assert.Equal(expected, Encode<PutRequestCodec, PutRequest>(put));

        // exactly sized destination: the worst-case-space check fails near the end and the exact path is taken
        int size = PutRequestCodec.Size(put);
        var exact = new byte[size];
        var w = new ProtoWriter(exact);
        PutRequestCodec.WriteTo(ref w, put);
        Assert.Equal(size, (int)w.BytesWritten);
        Assert.Equal(expected, exact);

        // round trip, including across segment boundaries
        if (expected.Length < 3000) Assert.Equal(s, ParseSplit<PutRequestCodec, PutRequest>(expected).Key);
        var parsed = Parse<PutRequestCodec, PutRequest>(expected);
        Assert.Equal(s, parsed.Key);
        Assert.Equal(s, parsed.PartitionKey);
        Assert.Equal(s, parsed.ClientIdentity);
    }

    [Fact]
    public void LoneSurrogates_AreReplacedLikeGoogleProtobuf()
    {
        string s = "a\uD800b\uDC00c\uD83D";
        byte[] expected = new G.PutRequest { Key = s }.ToByteArray();
        Assert.Equal(expected, Encode<PutRequestCodec, PutRequest>(new PutRequest { Key = s }));
    }

    [Fact]
    public void InvalidUtf8_DecodesWithReplacementCharacters()
    {
        byte[] data = new WireBuilder().Bytes(1, new byte[] { 0x61, 0xFF, 0xC3, 0x28, 0x62 }).ToArray();
        Assert.Equal(Encoding.UTF8.GetString(new byte[] { 0x61, 0xFF, 0xC3, 0x28, 0x62 }), ParseSplit<PutRequestCodec, PutRequest>(data).Key);
    }

    // ---- large values ------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(65_535)]
    [InlineData(65_536)]
    [InlineData(65_537)]
    [InlineData(200_000)]
    [InlineData(1_048_576)]
    [InlineData(1_048_577)]
    [InlineData(5_000_000)]
    public void LargeValues_WriteReadAndMatchGoogleProtobuf(int length)
    {
        var rng = new Random(length);
        var value = new byte[length];
        rng.NextBytes(value);
        var req = new WriteRequest { Shard = 4 };
        req.Puts.Add(new PutRequest { Key = "big", Value = value, SessionId = 3 });
        req.Puts.Add(new PutRequest { Key = "small", Value = new byte[] { 1, 2, 3 } });
        req.Deletes.Add(new DeleteRequest { Key = "d" });

        var oracle = new G.WriteRequest { Shard = 4 };
        oracle.Puts.Add(new G.PutRequest { Key = "big", Value = ByteString.CopyFrom(value), SessionId = 3 });
        oracle.Puts.Add(new G.PutRequest { Key = "small", Value = ByteString.CopyFrom(1, 2, 3) });
        oracle.Deletes.Add(new G.DeleteRequest { Key = "d" });
        byte[] expected = oracle.ToByteArray();

        Assert.Equal(expected.Length, WriteRequestCodec.Size(req));
        Assert.True(expected.AsSpan().SequenceEqual(Encode<WriteRequestCodec, WriteRequest>(req)));

        // through an output that only hands out 4 KiB chunks
        var chunked = new ChunkedBufferWriter(4096);
        WriteRequestCodec.Write(req, chunked);
        Assert.True(expected.AsSpan().SequenceEqual(chunked.ToArray()));
        Assert.True(chunked.GetSpanCalls > length / 70_000, "the writer should have been asked for several spans");

        // read back through 1 KiB segments and through a single span
        var parsed = new WriteRequest();
        var seq = Segments.Chunks(expected, 1024);
        WriteRequestCodec.Parse(in seq, parsed);
        Assert.Equal(4L, parsed.Shard);
        Assert.True(parsed.Puts[0].Value.Span.SequenceEqual(value));
        Assert.Equal("small", parsed.Puts[1].Key);
        Assert.Equal("d", parsed.Deletes[0].Key);

        var resp = new ReadResponse();
        resp.Gets.Add(new GetResponse { Status = Status.Ok, Value = value, Version = new ProtoVersion { VersionId = 1 } });
        byte[] respBytes = Encode<ReadResponseCodec, ReadResponse>(resp);
        var respOracle = new G.ReadResponse();
        respOracle.Gets.Add(new G.GetResponse { Status = G.Status.Ok, Value = ByteString.CopyFrom(value), Version = new G.Version { VersionId = 1 } });
        Assert.True(respOracle.ToByteArray().AsSpan().SequenceEqual(respBytes));
        var respParsed = new ReadResponse();
        var respSeq = Segments.Chunks(respBytes, 777);
        ReadResponseCodec.Parse(in respSeq, respParsed);
        Assert.True(respParsed.Gets[0].Value.AsSpan().SequenceEqual(value));
    }

    [Fact]
    public void MessagesAroundTheBlockWriterLimit_TakeBothPathsAndMatchGoogleProtobuf()
    {
        // the hot request codecs write a message of up to BlockLimit bytes through one reserved block and larger ones through the
        // chunked field-by-field writer; the bytes must be the same on both sides of the limit
        const int limit = ProtoWriter.BlockLimit;
        for (int s = limit - 40; s <= limit + 40; s++)
        {
            string key = new string('k', s - 5) + "é€";                   // multi-byte tail
            byte[] value = new byte[s - 12];

            var req = new WriteRequest { Shard = 3 };
            req.Puts.Add(new PutRequest { Key = "k", Value = value, OpIndex = 1, ExpectedVersionId = 2 });
            req.Puts.Add(new PutRequest { Key = key, ClientIdentity = "c", OpIndex = 2 });
            req.Deletes.Add(new DeleteRequest { Key = key, PartitionKey = "p", OpIndex = 3 });
            req.DeleteRanges.Add(new DeleteRangeRequest { StartInclusive = key, EndExclusive = "z", OpIndex = 4 });
            var read = new ReadRequest { Shard = 3 };
            read.Gets.Add(new GetRequest { Key = key, IncludeValue = true });

            var oracle = new G.WriteRequest { Shard = 3 };
            oracle.Puts.Add(new G.PutRequest { Key = "k", Value = ByteString.CopyFrom(value), OpIndex = 1, ExpectedVersionId = 2 });
            oracle.Puts.Add(new G.PutRequest { Key = key, ClientIdentity = "c", OpIndex = 2 });
            oracle.Deletes.Add(new G.DeleteRequest { Key = key, PartitionKey = "p", OpIndex = 3 });
            oracle.DeleteRanges.Add(new G.DeleteRangeRequest { StartInclusive = key, EndExclusive = "z", OpIndex = 4 });
            var readOracle = new G.ReadRequest { Shard = 3 };
            readOracle.Gets.Add(new G.GetRequest { Key = key, IncludeValue = true });

            Assert.True(oracle.ToByteArray().AsSpan().SequenceEqual(Encode<WriteRequestCodec, WriteRequest>(req)), $"WriteRequest, s = {s}");
            Assert.True(readOracle.ToByteArray().AsSpan().SequenceEqual(Encode<ReadRequestCodec, ReadRequest>(read)), $"ReadRequest, s = {s}");

            // standalone use of the element codecs (WriteTo without the length prefix of the parent)
            var delete = req.Deletes[0];
            Assert.True(oracle.Deletes[0].ToByteArray().AsSpan().SequenceEqual(Encode<DeleteRequestCodec, DeleteRequest>(delete)), $"DeleteRequest, s = {s}");
            var chunked = new ChunkedBufferWriter(32 * 1024);
            WriteRequestCodec.Write(req, chunked);
            Assert.True(oracle.ToByteArray().AsSpan().SequenceEqual(chunked.ToArray()), $"WriteRequest chunked, s = {s}");
        }
    }

    [Fact]
    public void ModifyingAMessageBetweenSizeAndWrite_ThrowsInsteadOfCorruptingTheOutput()
    {
        var put = new PutRequest { Key = "short", Value = new byte[10] };
        int size = PutRequestCodec.Size(put);
        put.Key = "a-key-that-is-much-longer-than-the-size-memoised-above";
        var buffer = new byte[size + 1000];
        Assert.ThrowsAny<Exception>(() =>
        {
            var w = new ProtoWriter(buffer);
            PutRequestCodec.WriteTo(ref w, put);
        });
    }

    [Fact]
    public void ManyElements_RepeatedFields()
    {
        var resp = new WriteResponse();
        for (int i = 0; i < 20_000; i++)
        {
            resp.Puts.Add(new PutResponse { Status = (Status)(i % 5), Version = new ProtoVersion { VersionId = i, ModificationsCount = i * 3L, CreatedTimestamp = (ulong)i } });
        }
        var oracle = new G.WriteResponse();
        foreach (var p in resp.Puts) oracle.Puts.Add(new G.PutResponse { Status = (G.Status)p.Status, Version = new G.Version { VersionId = p.Version!.VersionId, ModificationsCount = p.Version.ModificationsCount, CreatedTimestamp = p.Version.CreatedTimestamp } });
        byte[] expected = oracle.ToByteArray();
        byte[] actual = Encode<WriteResponseCodec, WriteResponse>(resp);
        Assert.True(expected.AsSpan().SequenceEqual(actual));
        var parsed = new WriteResponse();
        var seq = Segments.Chunks(expected, 4096);
        WriteResponseCodec.Parse(in seq, parsed);
        Assert.Equal(20_000, parsed.Puts.Count);
        Assert.Equal(19_999L, parsed.Puts[19_999].Version!.VersionId);
        Assert.Equal((ulong)12_345, parsed.Puts[12_345].Version!.CreatedTimestamp);
    }

    // ---- ownership (analogue of Go TestVtprotoCodec_UnmarshalOwnsTheBytes) -------------------------------------------

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ParsedMessagesOwnTheirBytes_TheTransportBufferCanBeRecycled(bool segmented)
    {
        var resp = new ReadResponse();
        resp.Gets.Add(new GetResponse { Status = Status.Ok, Key = "the-found-key", SecondaryIndexKey = "sec", Value = Enumerable.Repeat((byte)'x', 64 * 1024).ToArray(), Version = new ProtoVersion { ClientIdentity = "client-id" } });
        byte[] wire = Encode<ReadResponseCodec, ReadResponse>(resp);
        byte[] source = (byte[])wire.Clone();

        var parsed = new ReadResponse();
        if (segmented)
        {
            var seq = Segments.Chunks(source, 1000);
            ReadResponseCodec.Parse(in seq, parsed);
        }
        else
        {
            ReadResponseCodec.Parse(source, parsed);
        }
        Array.Fill(source, (byte)0xFF);   // what the next message received into a pooled transport buffer would do

        GetResponse g = parsed.Gets.Single();
        Assert.Equal("the-found-key", g.Key);
        Assert.Equal("sec", g.SecondaryIndexKey);
        Assert.Equal("client-id", g.Version!.ClientIdentity);
        Assert.True(g.Value!.All(b => b == (byte)'x'));
    }

    // ---- writer behaviour --------------------------------------------------------------------------------------------

    [Fact]
    public void FixedBufferWriter_TooSmall_Throws()
    {
        var put = new PutRequest { Key = "some-key", Value = new byte[100] };
        int size = PutRequestCodec.Size(put);
        var small = new byte[size - 1];
        Assert.Throws<InvalidOperationException>(() =>
        {
            var w = new ProtoWriter(small);
            PutRequestCodec.WriteTo(ref w, put);
        });
    }

    [Fact]
    public void Size_IsStableAndEqualsWrittenLength()
    {
        var rng = new Random(77);
        foreach (IMessageKind kind in MessageKinds.All)
        {
            for (int i = 0; i < 30; i++)
            {
                object m = kind.Random(rng, new GenOptions());
                int a = kind.Size(m), b = kind.Size(m);
                Assert.Equal(a, b);
                Assert.Equal(a, kind.Encode(m).Length);
            }
        }
    }

    [Fact]
    public void EncodingTwice_GivesIdenticalBytes_MemoDoesNotGoStale()
    {
        var req = new WriteRequest { Shard = 1 };
        var put = new PutRequest { Key = "k", Value = new byte[] { 1 } };
        req.Puts.Add(put);
        byte[] a = Encode<WriteRequestCodec, WriteRequest>(req);
        put.Key = "a-much-longer-key-than-before";
        put.Value = new byte[300];
        byte[] b = Encode<WriteRequestCodec, WriteRequest>(req);
        Assert.NotEqual(a.Length, b.Length);
        var oracle = new G.WriteRequest { Shard = 1 };
        oracle.Puts.Add(new G.PutRequest { Key = put.Key, Value = ByteString.CopyFrom(new byte[300]) });
        Assert.Equal(oracle.ToByteArray(), b);
    }

    [Fact]
    public void PutRequestSubclass_IsEncodedLikeTheBaseClass()
    {
        // the batcher passes subclasses (PutCall : PutRequest); the codec must only look at the message fields
        var req = new WriteRequest();
        req.Puts.Add(new DerivedPut { Key = "x", Value = new byte[] { 9 }, Extra = "ignored" });
        var oracle = new G.WriteRequest();
        oracle.Puts.Add(new G.PutRequest { Key = "x", Value = ByteString.CopyFrom(9) });
        Assert.Equal(oracle.ToByteArray(), Encode<WriteRequestCodec, WriteRequest>(req));
    }

    private sealed class DerivedPut : PutRequest
    {
        public string Extra = "";
        public override void Reset() { base.Reset(); Extra = ""; }
    }
}

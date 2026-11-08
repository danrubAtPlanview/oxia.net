// =====================================================================================================================
// PORT NOTE (tests)
//   Upstream : (none -- new oracle/differential tests for the hand-written codecs; Go relies on protobuf-go + vtproto)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : For EVERY message of client.proto: seeded random messages (edge values, optional presence, multi-byte UTF-8,
//              empty / large repeated fields, packed deltas, maps) are encoded by the hand codec and parsed by the protoc
//              generated Google.Protobuf classes (alias G), and encoded by Google.Protobuf and parsed by the hand parser;
//              the encodings are required to be byte-identical; chunked output writers, reuse of a parsed-into instance,
//              multi-segment input split at every offset, truncation at every offset and unknown fields are covered.
//   Divergences: n/a (new tests; Go tests: 0 ported, extras: all)
// =====================================================================================================================
using System.Buffers;
using Oxia.Common.Proto;
using Oxia.Common.Proto.Codec;
using Xunit;

namespace Oxia.Client.Tests.Proto;

public class CodecDifferentialTests
{
    public static IEnumerable<object[]> Kinds() => MessageKinds.Names();

    private const int Iterations = 250;

    internal static int Seed(string kind, int salt)
    {
        int h = 17 + salt * 7919;
        foreach (char c in kind) h = h * 31 + c;
        return h;
    }

    [Fact]
    public void AllMessagesOfClientProtoAreRegistered()
    {
        // 35 message types (the 5 enums need no codec); a new message in client.proto must be added to ClientMessages.cs, the codecs and MessageKinds.
        var oracleMessages = typeof(Io.Oxia.Proto.V1.PutRequest).Assembly.GetTypes()
            .Where(t => t.Namespace == "Io.Oxia.Proto.V1" && typeof(Google.Protobuf.IMessage).IsAssignableFrom(t))
            .Select(t => t.Name)
            .Where(n => Io.Oxia.Proto.V1.ClientReflection.Descriptor.MessageTypes.Any(d => d.Name == n))
            .OrderBy(n => n)
            .ToList();
        var registered = MessageKinds.All.Select(k => k.Name == "ProtoVersion" ? "Version" : k.Name).OrderBy(n => n).ToList();
        Assert.Equal(oracleMessages, registered);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    public void HandEncoded_IsParsedByGoogleProtobuf(string kindName)
    {
        IMessageKind kind = MessageKinds.Get(kindName);
        var rng = new Random(Seed(kindName, 1));
        for (int i = 0; i < Iterations; i++)
        {
            object m = kind.Random(rng, new GenOptions { Large = i % 97 == 0 });
            byte[] bytes = kind.Encode(m);
            Assert.True(kind.OracleParseEquals(bytes, m), $"{kindName} #{i}: Google.Protobuf parsed something different.\nhand: {ProtoReflection.Dump(m)}\ngoogle: {kind.OracleText(bytes)}");
        }
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    public void GoogleEncoded_IsParsedByHandParser(string kindName)
    {
        IMessageKind kind = MessageKinds.Get(kindName);
        var rng = new Random(Seed(kindName, 2));
        for (int i = 0; i < Iterations; i++)
        {
            object m = kind.Random(rng, new GenOptions { Large = i % 97 == 0 });
            byte[] bytes = kind.OracleEncode(m);
            object parsed = kind.NewHand();
            kind.ParseInto(parsed, bytes);
            Assert.Equal(ProtoReflection.Dump(m), ProtoReflection.Dump(parsed));
        }
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    public void HandOutput_IsByteIdenticalToGoogleProtobuf(string kindName)
    {
        IMessageKind kind = MessageKinds.Get(kindName);
        var rng = new Random(Seed(kindName, 3));
        for (int i = 0; i < Iterations; i++)
        {
            object m = kind.Random(rng, new GenOptions { Large = i % 97 == 0, Presence = i % 5 == 0 ? 1.0 : 0.5, EmptyMapKeys = false });
            byte[] expected = kind.OracleEncode(m);
            byte[] actual = kind.Encode(m);
            Assert.True(expected.AsSpan().SequenceEqual(actual), $"{kindName} #{i}: bytes differ\nhand: {ProtoReflection.Dump(m)}\nexpected {Convert.ToHexString(expected)}\nactual   {Convert.ToHexString(actual)}");
            Assert.Equal(kind.OracleSize(m), kind.Size(m));
            Assert.Equal(expected.Length, kind.Size(m));
        }
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    public void ChunkedOutputWriter_ProducesTheSameBytes(string kindName)
    {
        IMessageKind kind = MessageKinds.Get(kindName);
        var rng = new Random(Seed(kindName, 4));
        for (int i = 0; i < 80; i++)
        {
            object m = kind.Random(rng, new GenOptions { Large = i % 40 == 0 });
            byte[] expected = kind.Encode(m);
            foreach (int chunk in new[] { 8192, 70_000 })
            {
                Assert.True(expected.AsSpan().SequenceEqual(kind.EncodeChunked(m, chunk)), $"{kindName} #{i} chunk {chunk}");
            }
        }
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    public void ParsingIntoAReusedInstance_ReplacesTheContent(string kindName)
    {
        IMessageKind kind = MessageKinds.Get(kindName);
        var rng = new Random(Seed(kindName, 5));
        object reused = kind.NewHand();
        for (int i = 0; i < 60; i++)
        {
            object m = kind.Random(rng, new GenOptions());
            kind.ParseInto(reused, kind.Encode(m));
            Assert.Equal(ProtoReflection.Dump(m), ProtoReflection.Dump(reused));
        }
        kind.Reset(reused);
        Assert.Equal(ProtoReflection.Dump(kind.NewHand()), ProtoReflection.Dump(reused));
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    public void Reset_RestoresTheDefaultState(string kindName)
    {
        IMessageKind kind = MessageKinds.Get(kindName);
        var rng = new Random(Seed(kindName, 6));
        for (int i = 0; i < 40; i++)
        {
            object m = kind.Random(rng, new GenOptions { Presence = 0.9 });
            kind.Reset(m);
            Assert.Equal(ProtoReflection.Dump(kind.NewHand()), ProtoReflection.Dump(m));
        }
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    public void SequenceSplitAtEveryOffset_ParsesTheSame(string kindName)
    {
        IMessageKind kind = MessageKinds.Get(kindName);
        var rng = new Random(Seed(kindName, 7));
        for (int i = 0; i < 8; i++)
        {
            object m = kind.Random(rng, new GenOptions { Presence = i % 2 == 0 ? 0.9 : 0.5 });
            byte[] data = kind.Encode(m);
            string expected = ProtoReflection.Dump(m);
            object target = kind.NewHand();

            var offsets = new List<int>();
            if (data.Length <= 700) for (int p = 1; p < data.Length; p++) offsets.Add(p);
            else
            {
                for (int p = 1; p < 60; p++) offsets.Add(p);
                for (int p = data.Length - 60; p < data.Length; p++) offsets.Add(p);
                for (int n = 0; n < 400; n++) offsets.Add(rng.Next(1, data.Length));
            }
            foreach (int p in offsets)
            {
                ReadOnlySequence<byte> seq = Segments.Split(data, p);
                kind.ParseInto(target, in seq);
                Assert.True(expected == ProtoReflection.Dump(target), $"{kindName} #{i}: split at {p}/{data.Length} parsed differently");
            }
            // three segments, possibly with empty ones in between
            for (int n = 0; n < 60 && data.Length > 1; n++)
            {
                int a = rng.Next(0, data.Length + 1), b = rng.Next(0, data.Length + 1);
                if (a > b) (a, b) = (b, a);
                ReadOnlySequence<byte> seq = Segments.Split(data, a, b);
                kind.ParseInto(target, in seq);
                Assert.True(expected == ProtoReflection.Dump(target), $"{kindName} #{i}: split at {a},{b}/{data.Length} parsed differently");
            }
            foreach (int size in new[] { 1, 2, 3, 5, 7, 13, 64 })
            {
                if (data.Length > 3000 && size < 7) continue;
                ReadOnlySequence<byte> seq = Segments.Chunks(data, size);
                kind.ParseInto(target, in seq);
                Assert.True(expected == ProtoReflection.Dump(target), $"{kindName} #{i}: chunks of {size} parsed differently");
            }
        }
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    public void TruncatedInput_ThrowsExceptAtFieldBoundaries(string kindName)
    {
        IMessageKind kind = MessageKinds.Get(kindName);
        var rng = new Random(Seed(kindName, 8));
        for (int i = 0; i < 10; i++)
        {
            object m = kind.Random(rng, new GenOptions { Presence = 0.8 });
            byte[] data = kind.Encode(m);
            var boundaries = new HashSet<int>(WireScanner.TopLevelBoundaries(data));
            var ks = new List<int>();
            if (data.Length <= 500) for (int k = 0; k < data.Length; k++) ks.Add(k);
            else
            {
                for (int k = 0; k < 50; k++) ks.Add(k);
                for (int k = data.Length - 50; k < data.Length; k++) ks.Add(k);
                for (int n = 0; n < 300; n++) ks.Add(rng.Next(0, data.Length));
                ks.AddRange(boundaries.Where(b => b < data.Length).Take(30));
            }
            object target = kind.NewHand();
            foreach (int k in ks)
            {
                byte[] prefix = data[..k];
                bool valid = boundaries.Contains(k);
                AssertParse(kind, target, prefix, valid, $"{kindName} #{i}: span prefix {k}/{data.Length}");
                ReadOnlySequence<byte> twoSeg = Segments.Split(prefix, k / 2);
                AssertParseSeq(kind, target, in twoSeg, valid, $"{kindName} #{i}: 2-segment prefix {k}/{data.Length}");
                if (data.Length <= 500)
                {
                    ReadOnlySequence<byte> oneByte = Segments.Chunks(prefix, 1);
                    AssertParseSeq(kind, target, in oneByte, valid, $"{kindName} #{i}: 1-byte segments prefix {k}/{data.Length}");
                }
            }
        }
    }

    private static void AssertParse(IMessageKind kind, object target, byte[] data, bool expectSuccess, string context)
    {
        try
        {
            kind.ParseInto(target, data);
            Assert.True(expectSuccess, context + ": expected ProtoParseException but parsing succeeded");
        }
        catch (ProtoParseException)
        {
            Assert.False(expectSuccess, context + ": a complete prefix was rejected");
        }
    }

    private static void AssertParseSeq(IMessageKind kind, object target, in ReadOnlySequence<byte> data, bool expectSuccess, string context)
    {
        try
        {
            kind.ParseInto(target, in data);
            Assert.True(expectSuccess, context + ": expected ProtoParseException but parsing succeeded");
        }
        catch (ProtoParseException)
        {
            Assert.False(expectSuccess, context + ": a complete prefix was rejected");
        }
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    public void UnknownTopLevelFields_AreSkipped(string kindName)
    {
        IMessageKind kind = MessageKinds.Get(kindName);
        var rng = new Random(Seed(kindName, 9));
        for (int i = 0; i < 60; i++)
        {
            object m = kind.Random(rng, new GenOptions());
            byte[] data = kind.Encode(m);
            var unknownBefore = RandomUnknownFields(rng);
            var unknownAfter = RandomUnknownFields(rng);
            byte[] input = unknownBefore.Concat(data).Concat(unknownAfter).ToArray();
            object parsed = kind.NewHand();
            kind.ParseInto(parsed, input);
            Assert.Equal(ProtoReflection.Dump(m), ProtoReflection.Dump(parsed));

            // and over a multi-segment sequence
            int s1 = rng.Next(0, input.Length + 1), s2 = rng.Next(0, input.Length + 1);
            ReadOnlySequence<byte> seq = Segments.Split(input, Math.Min(s1, s2), Math.Max(s1, s2));
            object parsed2 = kind.NewHand();
            kind.ParseInto(parsed2, in seq);
            Assert.Equal(ProtoReflection.Dump(m), ProtoReflection.Dump(parsed2));
        }
    }

    internal static byte[] RandomUnknownFields(Random rng)
    {
        var b = new WireBuilder();
        int n = rng.Next(0, 5);
        for (int i = 0; i < n; i++)
        {
            int field = rng.Next(20, 5000);
            switch (rng.Next(5))
            {
                case 0: b.Varint(field, ProtoReflection.RandULong(rng)); break;
                case 1: b.Fixed64(field, ProtoReflection.RandULong(rng)); break;
                case 2: b.Bytes(field, ProtoReflection.RandBytes(rng)); break;
                case 3: b.Fixed32(field, ProtoReflection.RandUInt(rng)); break;
                default:
                    b.Group(field, new WireBuilder().Varint(1, 5).Bytes(2, new byte[] { 1, 2, 3 }).Group(3, new WireBuilder().Varint(9, 1)));
                    break;
            }
        }
        return b.ToArray();
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    public void MutatedInput_NeverThrowsAnythingButProtoParseException(string kindName)
    {
        IMessageKind kind = MessageKinds.Get(kindName);
        var rng = new Random(Seed(kindName, 10));
        int rejected = 0, accepted = 0;
        for (int i = 0; i < 120; i++)
        {
            object m = kind.Random(rng, new GenOptions { Presence = 0.8 });
            byte[] data = kind.Encode(m);
            if (data.Length == 0) continue;
            for (int round = 0; round < 12; round++)
            {
                byte[] mutated = (byte[])data.Clone();
                switch (rng.Next(4))
                {
                    case 0: for (int n = rng.Next(1, 4); n > 0; n--) mutated[rng.Next(mutated.Length)] = (byte)rng.Next(256); break;
                    case 1: mutated[rng.Next(mutated.Length)] ^= (byte)(1 << rng.Next(8)); break;
                    case 2: mutated = mutated.Take(rng.Next(mutated.Length)).Concat(new[] { (byte)0xFF, (byte)0xFF, (byte)0xFF }).Concat(mutated.Skip(rng.Next(mutated.Length))).ToArray(); break;
                    default: for (int n = rng.Next(1, 3); n > 0 && mutated.Length > 0; n--) mutated[rng.Next(mutated.Length)] = (byte)(rng.Next(2) == 0 ? 0xFF : 0x7F); break;
                }
                object target = kind.NewHand();
                try
                {
                    if (rng.Next(2) == 0) kind.ParseInto(target, mutated);
                    else
                    {
                        ReadOnlySequence<byte> seq = Segments.Split(mutated, rng.Next(0, mutated.Length + 1));
                        kind.ParseInto(target, in seq);
                    }
                    accepted++;
                }
                catch (ProtoParseException)
                {
                    rejected++;
                }
            }
        }
        Assert.True(accepted + rejected > 0 || kindName is "KeepAliveResponse" or "CloseSessionResponse");
    }
}

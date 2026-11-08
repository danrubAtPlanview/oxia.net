// =====================================================================================================================
// PORT NOTE
//   Upstream : (none) -- extra tests for common/compare/encode.go: golden bytes of the Go encoding, the allocation-free span API
//              (EncodedLength / EncodeTo / TryEncode), unicode keys, internal key ranges, "Encode returns a new array".
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
// =====================================================================================================================
using System.Text;
using Oxia.Common.Compare;
using Xunit;

namespace Oxia.Client.Tests.Common;

public class EncodeExtraTests
{
    private static readonly IEncoder Hierarchical = Encoders.Hierarchical;
    private static readonly IEncoder Natural = Encoders.Natural;

    private static byte[] Bytes(params byte[] bytes) => bytes;

    [Fact]
    public void Hierarchical_GoldenBytes()
    {
        Assert.Equal(Bytes(0x00, 0x00), Hierarchical.Encode(""));
        Assert.Equal(Bytes(0x00, 0x00, 0x61, 0x62, 0x63), Hierarchical.Encode("abc"));
        Assert.Equal(Bytes(0x00, 0x01, 0xff), Hierarchical.Encode("/"));
        Assert.Equal(Bytes(0x00, 0x02, 0xff, 0x61, 0xff, 0x62), Hierarchical.Encode("/a/b"));
        Assert.Equal(Bytes(0x00, 0x02, 0xff, 0x61, 0xff), Hierarchical.Encode("/a/"));
        // a trailing double separator does not create a new level
        Assert.Equal(Bytes(0x00, 0x02, 0xff, 0x61, 0xff, 0xff), Hierarchical.Encode("/a//"));
        Assert.Equal(Bytes(0x00, 0x01, 0x61, 0xff, 0xff), Hierarchical.Encode("a//"));
        Assert.Equal(Bytes(0x00, 0x01, 0xff, 0xff), Hierarchical.Encode("//"));
        // multi-byte UTF-8
        Assert.Equal(Bytes(0x00, 0x01, 0xc3, 0xa9, 0xff, 0xc3, 0xbc), Hierarchical.Encode("é/ü"));
        // internal keys: the top bit of the 2-byte prefix is the marker
        Assert.Equal(Bytes(0x80, 0x01, 0x5f, 0x5f, 0x6f, 0x78, 0x69, 0x61, 0xff, 0x78), Hierarchical.Encode("__oxia/x"));
        Assert.Equal(Bytes(0x80, 0x02, 0x5f, 0x5f, 0x6f, 0x78, 0x69, 0x61, 0xff, 0x78, 0xff, 0x79), Hierarchical.Encode("__oxia/x/y"));
        // beyond the deepest level the count saturates
        var deep = Hierarchical.Encode(new string('/', 40000) + "a");
        Assert.Equal(Bytes(0x7f, 0xff), deep[..2]);
        var deepInternal = Hierarchical.Encode("__oxia" + new string('/', 40000) + "a");
        Assert.Equal(Bytes(0xff, 0xff), deepInternal[..2]);
    }

    [Fact]
    public void Natural_GoldenBytes()
    {
        Assert.Empty(Natural.Encode(""));
        Assert.Equal("abc"u8.ToArray(), Natural.Encode("abc"));
        Assert.Equal("/a/b"u8.ToArray(), Natural.Encode("/a/b"));
        Assert.Equal("é"u8.ToArray(), Natural.Encode("é"));
        Assert.Equal(Bytes(0xff, 0xff, 0x6f, 0x78, 0x69, 0x61, 0x2f, 0x78), Natural.Encode("__oxia/x"));
        Assert.Equal(Bytes(0xff, 0xff, 0x6f, 0x78, 0x69, 0x61, 0x2f), Natural.Encode("__oxia/"));
        // only the exact prefix counts
        Assert.Equal("__oxia"u8.ToArray(), Natural.Encode("__oxia"));
        Assert.Equal("_oxia/x"u8.ToArray(), Natural.Encode("_oxia/x"));
    }

    [Theory]
    [InlineData("natural")]
    [InlineData("hierarchical")]
    public void EncodeAlwaysReturnsANewArray(string name)
    {
        var encoder = Encoders.GetEncoder(name);
        foreach (var key in new[] { "", "a", "/a/b", "__oxia/x" })
        {
            var first = encoder.Encode(key);
            var second = encoder.Encode(key);
            Assert.NotSame(first, second);
            Assert.Equal(first, second);

            if (first.Length > 0)
            {
                first[^1] ^= 0xff; // mutating a result never affects another encoding
                Assert.NotEqual(first, encoder.Encode(key));
            }
        }
    }

    public static TheoryData<string> Keys =>
    [
        "", "a", "ab", "/", "//", "///", "/a", "a/", "/a/", "/a//", "a//b", "/a/b/c/d", "é", "é/ü/ß", "日本語/キー", "😀/👍", "a😀b",
        "__oxia/", "__oxia/x", "__oxia/x/y/", "__oxia", "_oxia/x", "__oxia//", "__oxia/😀/é",
        "this is a longer key of more than a few bytes /with/several/levels/in it/ to cover the vector paths of the transcoder",
        "ASCII-then-ünicode-" + "padding/padding/padding/padding/padding/padding/padding/padding/padding/padding/padding/" + "日本",
    ];

    [Theory]
    [MemberData(nameof(Keys))]
    public void SpanApiMatchesEncode_ForBothEncoders(string key)
    {
        foreach (var encoder in new[] { Hierarchical, Natural })
        {
            var expected = encoder.Encode(key);

            Assert.Equal(expected.Length, encoder.EncodedLength(key));
            Assert.Equal(expected.Length, encoder.EncodedLength(key.AsSpan()));

            // exact-size destination
            var exact = new byte[expected.Length];
            Assert.Equal(expected.Length, encoder.EncodeTo(key, exact));
            Assert.Equal(expected, exact);

            // bigger destination: only the first bytes are written
            var big = new byte[expected.Length + 8];
            Array.Fill(big, (byte)0xAB);
            Assert.Equal(expected.Length, encoder.EncodeTo(key.AsSpan(), big));
            Assert.Equal(expected, big[..expected.Length]);
            Assert.All(big[expected.Length..], b => Assert.Equal(0xAB, b));

            // TryEncode
            var tryBuffer = new byte[expected.Length];
            Assert.True(encoder.TryEncode(key, tryBuffer, out var written));
            Assert.Equal(expected.Length, written);
            Assert.Equal(expected, tryBuffer);
            if (expected.Length > 0)
            {
                Assert.False(encoder.TryEncode(key, new byte[expected.Length - 1], out written));
                Assert.Equal(0, written);
                Assert.Throws<ArgumentException>(() => encoder.EncodeTo(key, new byte[expected.Length - 1]));
            }
        }
    }

    [Theory]
    [MemberData(nameof(Keys))]
    public void DecodeInvertsEncode_ForBothEncoders(string key)
    {
        foreach (var encoder in new[] { Hierarchical, Natural })
        {
            Assert.Equal(key, encoder.Decode(encoder.Encode(key)));
        }
    }

    [Fact]
    public void LoneSurrogatesAreEncodedAsTheReplacementCharacter()
    {
        foreach (var encoder in new[] { Hierarchical, Natural })
        {
            var encoded = encoder.Encode("a\ud800b");
            Assert.Equal("a�b", encoder.Decode(encoded));
            Assert.Equal(encoded.Length, encoder.EncodedLength("a\ud800b"));
        }
    }

    [Fact]
    public void RandomKeys_RoundTripAndMatchTheSpanApi()
    {
        var rnd = new Random(2024);
        var alphabet = new[] { "a", "b", "/", "é", "日", "😀", "_", "o" };
        var sb = new StringBuilder();
        var stack = new byte[512];
        for (var i = 0; i < 3000; i++)
        {
            sb.Clear();
            if (rnd.Next(4) == 0) sb.Append("__oxia/");
            var n = rnd.Next(0, 40);
            for (var j = 0; j < n; j++) sb.Append(alphabet[rnd.Next(alphabet.Length)]);
            var key = sb.ToString();

            foreach (var encoder in new[] { Hierarchical, Natural })
            {
                var encoded = encoder.Encode(key);
                Assert.Equal(key, encoder.Decode(encoded));
                Assert.Equal(key.StartsWith("__oxia/", StringComparison.Ordinal), encoder.IsInternalKey(encoded));

                Assert.True(encoder.TryEncode(key, stack, out var written));
                Assert.True(stack[..written].SequenceEqual(encoded));
            }
        }
    }

    [Fact]
    public void NaturalOrder_IsTheUtf8ByteOrderOfTheKeys_InternalKeysLast()
    {
        var keys = new[] { "a", "b", "é", "z", "日", "😀", "/", "/a", "__oxia/x", "__oxia/a" };
        var sorted = keys.OrderBy(k => Natural.Encode(k), Comparer<byte[]>.Create((x, y) => x.AsSpan().SequenceCompareTo(y))).ToArray();

        Assert.Equal(["__oxia/a", "__oxia/x"], sorted[^2..]);
        Assert.Equal(["/", "/a", "a", "b", "z", "é", "日", "😀"], sorted[..8]);
    }

    [Fact]
    public void HierarchicalOrder_SortsByLevelThenKey_InternalKeysLast()
    {
        var keys = new[] { "/a/b", "/b", "/a", "z", "/a/a", "a", "__oxia/x", "/", "//" };
        var sorted = keys.OrderBy(k => Hierarchical.Encode(k), Comparer<byte[]>.Create((x, y) => x.AsSpan().SequenceCompareTo(y))).ToArray();

        // level 0: a, z ; level 1: "/", "/a", "/b", "//" ("//" has one level: the trailing double separator is ignored, and the
        // separator sorts after every other character) ; level 2: "/a/a", "/a/b" ; internal keys last
        Assert.Equal(["a", "z", "/", "/a", "/b", "//", "/a/a", "/a/b", "__oxia/x"], sorted);
    }

    [Fact]
    public void InternalKeyRange_ContainsExactlyTheInternalKeys()
    {
        var (hStart, hEnd) = Hierarchical.InternalKeyRange();
        Assert.Equal(Bytes(0x80), hStart);
        Assert.Null(hEnd);

        var (nStart, nEnd) = Natural.InternalKeyRange();
        Assert.Equal(Bytes(0xff, 0xff, 0x6f, 0x78, 0x69, 0x61, 0x2f), nStart);
        Assert.Equal(Bytes(0xff, 0xff, 0x6f, 0x78, 0x69, 0x61, 0x30), nEnd);

        foreach (var key in new[] { "__oxia/", "__oxia/x", "__oxia/😀/y" })
        {
            var h = Hierarchical.Encode(key);
            Assert.True(h.AsSpan().SequenceCompareTo(hStart) >= 0);
            var n = Natural.Encode(key);
            Assert.True(n.AsSpan().SequenceCompareTo(nStart) >= 0);
            Assert.True(n.AsSpan().SequenceCompareTo(nEnd) < 0);
        }

        foreach (var key in new[] { "", "a", "__oxia", "_oxia/x", "z/z/z", "😀", new string('/', 50000) + "a" })
        {
            Assert.True(Hierarchical.Encode(key).AsSpan().SequenceCompareTo(hStart) < 0, key.Length > 10 ? "deep" : key);
            Assert.True(Natural.Encode(key).AsSpan().SequenceCompareTo(nStart) < 0, key);
        }
    }

    [Fact]
    public void PrefixRanges_Bounds_GoldenValues()
    {
        var (nl, nu) = Natural.PrefixRanges("ab").Bounds();
        Assert.Equal("ab"u8.ToArray(), nl);
        Assert.Equal("ac"u8.ToArray(), nu);

        var (el, eu) = Natural.PrefixRanges("").Bounds();
        Assert.Empty(el);
        Assert.Null(eu);

        // hierarchical: [level(prefix), prefix) ... [0xFFFF, successor)
        var (hl, hu) = Hierarchical.PrefixRanges("a/").Bounds();
        Assert.Equal(Bytes(0x00, 0x01, 0x61, 0xff), hl);
        Assert.Equal(Bytes(0xff, 0xff, 0x62), hu); // 0x61 0xff -> successor of the encoded prefix is 0x62 (0xff carries)

        // a prefix made only of separators has no successor: its range runs to the end of the level
        var (sl, su) = Hierarchical.PrefixRanges("/").Bounds();
        Assert.Equal(Bytes(0x00, 0x01, 0xff), sl);
        Assert.Null(su);
    }

    [Fact]
    public void PrefixRanges_ContainsIsTolerantOfTooShortKeys()
    {
        var ranges = Hierarchical.PrefixRanges("a");
        Assert.False(ranges.Contains([]));
        Assert.False(ranges.Contains([0x00]));
        Assert.False(ranges.Contains([0x00, 0x00]));
        Assert.True(ranges.Contains([0x00, 0x00, 0x61]));
        Assert.NotNull(ranges.NextStart([]));
        Assert.NotNull(ranges.NextStart([0x00]));
    }

    [Fact]
    public void GetEncoder_ReturnsTheSharedInstances_AndRejectsUnknownNames()
    {
        Assert.Same(Encoders.Natural, Encoders.GetEncoder("natural"));
        Assert.Same(Encoders.Hierarchical, Encoders.GetEncoder("hierarchical"));
        Assert.Equal("unknown encoder ", Assert.Throws<ArgumentException>(() => Encoders.GetEncoder("")).Message);
        Assert.Equal("unknown encoder Natural", Assert.Throws<ArgumentException>(() => Encoders.GetEncoder("Natural")).Message); // case sensitive
    }

    [Fact]
    public void Decode_OfShortInputsIsTheEmptyString()
    {
        Assert.Equal("", Hierarchical.Decode([]));
        Assert.Equal("", Hierarchical.Decode([0x00]));
        Assert.Equal("", Hierarchical.Decode([0x00, 0x00]));
        Assert.Equal("", Natural.Decode([]));
    }
}

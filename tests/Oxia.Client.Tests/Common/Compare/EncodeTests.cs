// =====================================================================================================================
// PORT NOTE
//   Upstream : common/compare/encode_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Encode/Decode round trips, byte order of encoded keys, internal keys, deeply nested keys, AppendEncode, PrefixRanges
//              (NextStart / PrevEnd against a brute-force model), natural Decode not mutating its input, GetEncoder.
//   Go tests: 10 functions (TestEncodeDecode 13 subtests, TestEncodeInternalKeys / TestEncoderIsInternalKeyEmpty /
//              TestEncoderAppendEncodeMatchesEncode / TestPrefixRanges 2 subtests each, TestEncodeDeeplyNestedKeys 4 subtests),
//              ported: 10, skipped: 0.  BenchmarkEncoderNaturalDecode is ported as BenchmarkDotNet
//              (benchmarks/Oxia.Client.Benchmarks/Common/CompareBenchmarks.cs).
//   Divergences:
//      * `t.Run` subtests -> [Theory] rows / loops with the subtest name in the assertion messages.
//      * Go keys are byte strings; the key "a\xffb" (invalid UTF-8) of TestEncoderAppendEncodeMatchesEncode becomes "aÿb"
//        (valid, UTF-8 C3 BF) plus a lone-surrogate key "a\ud800b" (encoded as U+FFFD); Go's "\xff\xffoxia/session/123" literals in
//        TestEncoderNaturalDecodeDoesNotMutateInput are raw byte arrays.
//      * `testing.AllocsPerRun(10, ...) == 0` -> GC.GetAllocatedBytesForCurrentThread over 1000 calls == 0.
//      * `assert.EqualError(err, "unknown encoder unknown")` -> ArgumentException with that exact message.
// =====================================================================================================================
using System.Buffers;
using System.Text;
using Oxia.Common.Compare;
using Oxia.Common.Constant;
using Xunit;

namespace Oxia.Client.Tests.Common;

public class EncodeTests
{
    private static readonly IEncoder Hierarchical = Encoders.Hierarchical;
    private static readonly IEncoder Natural = Encoders.Natural;

    public static TheoryData<string> EncodeDecodeKeys =>
    [
        "aaa", "zzzzz", "", "a", "/", "/aaaa", "/aa/a", "/aaaa/a", "/aaaa/a/a", "/bbbbbbbbbb", "/aaaa/bbbbbbbbbb", "/a/b/a/a/a", "/a/b/a/b",
    ];

    [Theory]
    [MemberData(nameof(EncodeDecodeKeys))]
    public void TestEncodeDecode(string key)
    {
        var e = Hierarchical.Encode(key);
        var d = Hierarchical.Decode(e);
        Assert.Equal(key, d);
    }

    private static int Cmp(byte[] a, byte[] b) => Math.Sign(a.AsSpan().SequenceCompareTo(b));

    [Fact]
    public void TestEncodeCompare()
    {
        static byte[] enc(string key) => Hierarchical.Encode(key);

        Assert.Equal(0, Cmp(enc("aaaaa"), enc("aaaaa")));
        Assert.Equal(-1, Cmp(enc("aaaaa"), enc("zzzzz")));
        Assert.Equal(+1, Cmp(enc("bbbbb"), enc("aaaaa")));

        Assert.Equal(+1, Cmp(enc("aaaaa"), enc("")));
        Assert.Equal(-1, Cmp(enc(""), enc("aaaaaa")));
        Assert.Equal(0, Cmp(enc(""), enc("")));

        Assert.Equal(-1, Cmp(enc("aaaaa"), enc("aaaaaaaaaaa")));
        Assert.Equal(+1, Cmp(enc("aaaaaaaaaaa"), enc("aaa")));

        Assert.Equal(-1, Cmp(enc("a"), enc("/")));
        Assert.Equal(+1, Cmp(enc("/"), enc("a")));

        Assert.Equal(-1, Cmp(enc("/aaaa"), enc("/bbbbb")));
        Assert.Equal(-1, Cmp(enc("/aaaa"), enc("/aa/a")));
        Assert.Equal(-1, Cmp(enc("/aaaa/a"), enc("/aaaa/b")));
        Assert.Equal(+1, Cmp(enc("/aaaa/a/a"), enc("/bbbbbbbbbb")));
        Assert.Equal(+1, Cmp(enc("/aaaa/a/a"), enc("/aaaa/bbbbbbbbbb")));

        Assert.Equal(+1, Cmp(enc("/a/b/a/a/a"), enc("/a/b/a/b")));

        Assert.Equal(-1, Cmp(enc("/a"), enc("/b")));
        Assert.Equal(-1, Cmp(enc("/a"), enc("/a/")));
        Assert.Equal(-1, Cmp(enc("/a/"), enc("/a//")));
        Assert.Equal(-1, Cmp(enc("/a/a-2"), enc("/b/c")));
        Assert.Equal(-1, Cmp(enc("/"), enc("/a")));
        Assert.Equal(-1, Cmp(enc("/a"), enc("//")));
        Assert.Equal(-1, Cmp(enc("/b"), enc("//")));
        Assert.Equal(-1, Cmp(enc("//"), enc("/a/a-1")));

        Assert.Equal(-1, Cmp(enc("//"), enc("/a/a-1")));

        Assert.Equal(-1, Cmp(enc("/a/a-1"), enc("/a//")));
        Assert.Equal(-1, Cmp(enc("/a//"), enc("/b/c")));
    }

    public static TheoryData<string> EncoderNames => ["natural", "hierarchical"];

    [Theory]
    [MemberData(nameof(EncoderNames))]
    public void TestEncodeInternalKeys(string encoderName)
    {
        var encoder = Encoders.GetEncoder(encoderName);
        byte[] enc(string key) => encoder.Encode(key);

        Assert.False(encoder.IsInternalKey(enc("my-key")));
        Assert.False(encoder.IsInternalKey(enc("/my-key")));

        Assert.True(encoder.IsInternalKey(enc("__oxia/")));
        Assert.True(encoder.IsInternalKey(enc("__oxia/xyz")));

        Assert.Equal(-1, Cmp(enc("my-key"), enc("__oxia/xyz")));
        Assert.Equal(-1, Cmp(enc("/my-key"), enc("__oxia/xyz")));
        Assert.Equal(-1, Cmp(enc("/my-key"), enc("__oxia/xyz")));

        const string k = "__oxia/xyz";
        Assert.Equal(k, encoder.Decode(encoder.Encode(k)));
    }

    [Theory]
    [MemberData(nameof(EncoderNames))]
    public void TestEncoderIsInternalKeyEmpty(string encoderName)
    {
        var encoder = Encoders.GetEncoder(encoderName);
        Assert.False(encoder.IsInternalKey(ReadOnlySpan<byte>.Empty)); // nil
        Assert.False(encoder.IsInternalKey([])); // []byte{}
    }

    // A key carries its own separator count in the 2-byte prefix, and that count shares the prefix with the internal-key marker.
    // Keys with more separators than the prefix can hold must not reach into the marker, nor wrap around and come back as a
    // different key.
    public static TheoryData<string, int> DeeplyNestedCases => new()
    {
        { "below the marker", HierarchicalEncoder.MaxSeparatorCount - 1 },
        { "at the marker", HierarchicalEncoder.InternalKeysBitMarker },
        { "past the marker", HierarchicalEncoder.InternalKeysBitMarker + 1 },
        { "wrapping the prefix", 1 << 16 },
    };

    [Theory]
    [MemberData(nameof(DeeplyNestedCases))]
    public void TestEncodeDeeplyNestedKeys(string name, int sepCount)
    {
        var enc = Hierarchical;

        // The trailing "a" keeps the last two bytes from being separators, which would otherwise drop the count by one
        var key = new string('/', sepCount) + "a";

        var encoded = enc.Encode(key);
        Assert.False(enc.IsInternalKey(encoded), name);
        Assert.Equal(key, enc.Decode(encoded));

        // and it still sorts before the internal keys
        Assert.Equal(-1, Cmp(encoded, enc.Encode("__oxia/xyz")));
    }

    [Fact]
    public void TestEncodeDeeplyNestedInternalKeys()
    {
        var enc = Hierarchical;

        var key = Constants.InternalKeyPrefix + new string('/', 1 << 16) + "a";
        var encoded = enc.Encode(key);

        Assert.True(enc.IsInternalKey(encoded));
        Assert.Equal(key, enc.Decode(encoded));
    }

    [Theory]
    [MemberData(nameof(EncoderNames))]
    public void TestEncoderAppendEncodeMatchesEncode(string encoderName)
    {
        var encoder = Encoders.GetEncoder(encoderName);
        List<string> keys =
        [
            "", "a", "/", "//", "/a", "/a/", "/a//", "a//", "/a/b/c", "aÿb", "a\ud800b", new string('/', 1 << 16) + "a",
        ];
        foreach (var key in keys.ToArray())
        {
            keys.Add(Constants.InternalKeyPrefix + key);
        }

        // One buffer for all the keys, as a caller reuses it, after bytes that the encoding must leave as they are
        var prefix = "prefix"u8.ToArray();
        var buf = new ArrayBufferWriter<byte>();
        foreach (var key in keys)
        {
            byte[] expected = [.. prefix, .. encoder.Encode(key)];
            buf.ResetWrittenCount();
            buf.Write(prefix);
            encoder.AppendEncode(buf, key);
            Assert.True(expected.AsSpan().SequenceEqual(buf.WrittenSpan), $"{encoderName}: {key.Substring(0, Math.Min(40, key.Length))}");
        }

        // A buffer with room for the key takes no allocation
        var reused = new ArrayBufferWriter<byte>(256);
        encoder.AppendEncode(reused, "__oxia/a/b//"); // warm up
        reused.ResetWrittenCount();
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++)
        {
            reused.ResetWrittenCount();
            encoder.AppendEncode(reused, "__oxia/a/b//");
        }

        Assert.Equal(0, GC.GetAllocatedBytesForCurrentThread() - before);
    }

    // The keys with a prefix are in a single range with the natural encoder, and in one range per level with the hierarchical one.
    // From a key without the prefix, NextStart and PrevEnd have to move past the key, towards the nearest keys with the prefix,
    // without skipping any.
    [Theory]
    [MemberData(nameof(EncoderNames))]
    public void TestPrefixRanges(string encoderName)
    {
        var encoder = Encoders.GetEncoder(encoderName);

        // Every key of up to 4 bytes made of 'a', 'b' and the separator, and keys deeper than the level can count, each as a regular
        // and an internal key
        var keys = new List<string>();
        void add(string key)
        {
            keys.Add(key);
            keys.Add(Constants.InternalKeyPrefix + key);
            if (key.Length < 4)
            {
                foreach (var c in new[] { "a", "b", "/" }) add(key + c);
            }
        }

        add("");
        foreach (var key in new[] { string.Concat(Enumerable.Repeat("a/", 1 << 15)) + "a", string.Concat(Enumerable.Repeat("b/", 1 << 15)) + "b" })
        {
            keys.Add(key);
            keys.Add(Constants.InternalKeyPrefix + key);
        }

        var encoded = new Dictionary<string, byte[]>(keys.Count);
        foreach (var key in keys) encoded[key] = encoder.Encode(key);
        keys.Sort((a, b) => Cmp(encoded[a], encoded[b]));

        string Short(string s) => s.Length <= 40 ? s : s[..40];

        foreach (var basePrefix in new[] { "", "a", "a/", "a//", "/", "//", "b/a", "ba/" })
        {
            foreach (var prefix in new[] { basePrefix, Constants.InternalKeyPrefix + basePrefix })
            {
                var ranges = encoder.PrefixRanges(prefix);
                var (lower, upper) = ranges.Bounds();

                // The positions of the keys with the prefix
                var withPrefix = new List<int>();
                for (var i = 0; i < keys.Count; i++)
                {
                    var key = keys[i];
                    var hasPrefix = key.StartsWith(prefix, StringComparison.Ordinal);
                    Assert.True(hasPrefix == ranges.Contains(encoded[key]), $"{prefix} {Short(key)}");
                    if (hasPrefix)
                    {
                        withPrefix.Add(i);
                        Assert.True(Cmp(lower, encoded[key]) <= 0, $"{prefix} {Short(key)}");
                        Assert.True(upper is null || Cmp(encoded[key], upper) < 0, $"{prefix} {Short(key)}");
                    }
                }

                for (var i = 0; i < keys.Count; i++)
                {
                    var key = keys[i];
                    if (key.StartsWith(prefix, StringComparison.Ordinal))
                    {
                        continue;
                    }

                    var n = ~withPrefix.BinarySearch(i);
                    var before = withPrefix.GetRange(0, n);
                    var after = withPrefix.GetRange(n, withPrefix.Count - n);

                    var next = ranges.NextStart(encoded[key]);
                    if (next is null)
                    {
                        Assert.True(after.Count == 0, $"{prefix} {Short(key)}");
                    }
                    else
                    {
                        Assert.True(Cmp(next, encoded[key]) > 0, $"{prefix} {Short(key)}");
                        if (after.Count > 0)
                        {
                            Assert.True(Cmp(encoded[keys[after[0]]], next) >= 0, $"{prefix} {Short(key)}");
                        }
                    }

                    var prev = ranges.PrevEnd(encoded[key]);
                    if (prev is null)
                    {
                        Assert.True(before.Count == 0, $"{prefix} {Short(key)}");
                    }
                    else
                    {
                        Assert.True(Cmp(prev, encoded[key]) <= 0, $"{prefix} {Short(key)}");
                        if (before.Count > 0)
                        {
                            Assert.True(Cmp(encoded[keys[before[^1]]], prev) < 0, $"{prefix} {Short(key)}");
                        }
                    }
                }
            }
        }
    }

    // The buffer passed to Decode is storage memory, handed out under an explicit read-only contract: it must not be modified.
    [Fact]
    public void TestEncoderNaturalDecodeDoesNotMutateInput()
    {
        byte[] encoded = [0xff, 0xff, .. "oxia/session/123"u8];
        var original = (byte[])encoded.Clone();

        var decoded = Natural.Decode(encoded);
        Assert.Equal("__oxia/session/123", decoded);
        Assert.Equal(original, encoded);

        // Non-internal keys are returned as-is, also untouched
        var plain = "a/b/c"u8.ToArray();
        var originalPlain = (byte[])plain.Clone();
        Assert.Equal("a/b/c", Natural.Decode(plain));
        Assert.Equal(originalPlain, plain);
    }

    [Fact]
    public void TestGetEncoder()
    {
        var encoder = Encoders.GetEncoder("hierarchical");
        Assert.Equal("hierarchical", encoder.Name);

        encoder = Encoders.GetEncoder("natural");
        Assert.Equal("natural", encoder.Name);

        var err = Assert.Throws<ArgumentException>(() => Encoders.GetEncoder("unknown"));
        Assert.Equal("unknown encoder unknown", err.Message);
    }
}

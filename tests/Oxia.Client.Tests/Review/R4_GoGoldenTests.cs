// =====================================================================================================================
// PORT NOTE (review R4 differential tests against the REAL Go implementation)
//   Upstream : common/compare/encode.go (EncoderHierarchical / EncoderNatural: Encode, Decode, IsInternalKey),
//              common/compare/compare_with_slash.go, common/hash/hash.go (Xxh332 = uint32(xxh3.HashString(key)), zeebo/xxh3 v1.0.2)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The expected values in R4_GoGolden.Data.cs were produced by a Go program linked against the pinned
//              github.com/oxia-db/oxia/common module (go1.27.0) for the keys / pairs generated below, then reduced to a 64-bit
//              digest per line.  The C# implementation must produce the same lines: Encode (both encoders), Decode of the encoded form,
//              IsInternalKey, Xxh332 of the UTF-8 bytes, and CompareWithSlash.  Keys cover the empty key, leading/trailing/double
//              slashes, the internal prefix "__oxia/" (and near misses), non-ASCII (2/3/4-byte UTF-8), 0..70000 separators around
//              the 32767 level cap, and every XXH3 length class up to 1 MB.
//   Regenerating: see R4_GoGolden.Data.cs header.
// =====================================================================================================================
using System.Security.Cryptography;
using System.Text;
using Oxia.Common.Compare;
using Oxia.Common.Hash;
using Xunit;

namespace Oxia.Client.Tests.Review;

public class R4_GoGoldenTests
{
    internal static IEnumerable<string> Keys()
    {
        string[] hand =
        {
            "", "/", "//", "///", "////", "/a", "a/", "a//", "a///", "//a", "///a", "a//b", "a///b", "/a/b/c/", "a/b//", "a//b//", "a/b///", "///a///",
            "__oxia/", "__oxia/x", "__oxia//", "__oxia///", "__oxia/a/b", "__oxia/a/b/", "__oxia/a/b//", "__oxia", "__oxi", "__ox", "_", "__", "__o", "xx__oxia/", "__OXIA/",
            "_/oxia/", "__oxia/__oxia/", "/__oxia/", "a/__oxia/", "__oxia/é", "__oxia/日本",
            "é", "日本語/キー", "\U0001F600/\U0001F600", "ÿ", "퟿", "", "￿", "\u0000", "a\u0000b/c", "\u0001", "\u007f", "\u0080", "߿", "ࠀ",
            "a b", " ", "/ /", "key with spaces/and/slashes", "/user/1/name", "/user/1", "/user/10", "/user/2", "/a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p/q/r/s/t/u/v/w/x/y/z",
        };
        foreach (var k in hand) yield return k;

        int[] counts = { 0, 1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 32, 33, 127, 128, 129, 255, 256, 257, 32766, 32767, 32768, 32769, 40000, 65535, 65536, 65537, 70000 };
        foreach (int n in counts)
        {
            yield return new string('/', n);
            yield return "a" + new string('/', n);
            yield return new string('/', n) + "a";
            yield return string.Concat(Enumerable.Repeat("a/", n));
            yield return string.Concat(Enumerable.Repeat("a/", n)) + "/";
            yield return "__oxia/" + new string('/', n);
            yield return "__oxia/" + string.Concat(Enumerable.Repeat("a/", n));
            yield return "é" + string.Concat(Enumerable.Repeat("/日", n));
        }

        var rng = new Random(20260101);
        var elems = new[] { "a", "b", "/", "c", "/", "/", "d", "E", "0", "_", "é", "ÿ", "日", "\U0001F600" };
        for (int len = 0; len < 400; len++)
        {
            var sb = new StringBuilder();
            if (rng.Next(6) == 0) sb.Append("__oxia/");
            for (int i = 0; i < len; i++) sb.Append(elems[rng.Next(elems.Length)]);
            yield return sb.ToString();
        }

        // every XXH3 length class, pure ASCII and mixed
        int[] lengths = Enumerable.Range(0, 300).Concat(new[] { 511, 512, 513, 1023, 1024, 1025, 1026, 2047, 2048, 2049, 4095, 4096, 4097, 8192, 10000, 100000, 1_000_000 }).ToArray();
        foreach (int len in lengths)
        {
            var chars = new char[len];
            for (int i = 0; i < len; i++) chars[i] = (char)rng.Next('!', '~' + 1);
            yield return new string(chars);
            if (len < 3000)
            {
                var sb = new StringBuilder();
                while (Encoding.UTF8.GetByteCount(sb.ToString()) < len) sb.Append(elems[rng.Next(elems.Length)]);
                yield return sb.ToString();
            }
        }
    }

    /// <summary>The line the Go probe prints for a key (lower-case hex, "true"/"false").</summary>
    internal static string LineFor(string key)
    {
        byte[] h = Encoders.Hierarchical.Encode(key);
        byte[] n = Encoders.Natural.Encode(key);
        return string.Join('\t',
            Hex(Encoding.UTF8.GetBytes(key)), Hex(h), Hex(n), Hasher.Xxh332(Encoding.UTF8.GetBytes(key)),
            Hex(Encoding.UTF8.GetBytes(Encoders.Hierarchical.Decode(h))), Hex(Encoding.UTF8.GetBytes(Encoders.Natural.Decode(n))),
            Encoders.Hierarchical.IsInternalKey(h) ? "true" : "false", Encoders.Natural.IsInternalKey(n) ? "true" : "false");
    }

    internal static string Digest(string line) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(line)), 0, 8).ToLowerInvariant();

    private static string Hex(byte[] bytes) => Convert.ToHexString(bytes).ToLowerInvariant();

    internal static IEnumerable<(byte[] A, byte[] B)> Pairs()
    {
        var rng = new Random(7);
        byte[] alphabet = { (byte)'a', (byte)'b', (byte)'/', (byte)'/', 0, 0xff, (byte)'A', (byte)'.' };
        for (int i = 0; i < 3000; i++)
        {
            byte[] Make() { var b = new byte[rng.Next(0, 9)]; for (int j = 0; j < b.Length; j++) b[j] = alphabet[rng.Next(alphabet.Length)]; return b; }
            var a = Make();
            var b = rng.Next(4) == 0 ? (byte[])a.Clone() : Make();
            yield return (a, b);
        }
    }

    [Fact]
    public void KeyEncodersDecodersAndXxh3_MatchTheGoImplementation()
    {
        string[] golden = R4_GoGolden.KeyDigests;
        var keys = Keys().ToList();
        Assert.Equal(golden.Length, keys.Count);

        var bad = new List<string>();
        for (int i = 0; i < keys.Count; i++)
        {
            if (Digest(LineFor(keys[i])) != golden[i])
            {
                string k = keys[i];
                bad.Add($"#{i} key=[{(k.Length > 40 ? $"len={k.Length} head={k[..16]}" : k)}]");
            }
        }

        Assert.True(bad.Count == 0, $"{bad.Count} of {keys.Count} keys differ from Go: {string.Join("; ", bad.Take(10))}");
    }

    [Fact]
    public void CompareWithSlash_MatchesTheGoImplementation()
    {
        string golden = R4_GoGolden.CompareResults;
        var pairs = Pairs().ToList();
        Assert.Equal(golden.Length, pairs.Count);

        var bad = new List<string>();
        for (int i = 0; i < pairs.Count; i++)
        {
            int ours = KeyCompare.CompareWithSlash(pairs[i].A, pairs[i].B);
            char c = ours < 0 ? '-' : ours > 0 ? '+' : '0';
            if (c != golden[i]) bad.Add($"#{i} a={Hex(pairs[i].A)} b={Hex(pairs[i].B)} ours={c} go={golden[i]}");
        }

        Assert.True(bad.Count == 0, $"{bad.Count} of {pairs.Count} pairs differ from Go: {string.Join("; ", bad.Take(10))}");
    }
}

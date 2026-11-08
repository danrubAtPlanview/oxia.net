// =====================================================================================================================
// PORT NOTE
//   Upstream : common/hash/hash_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Shard routing must be bit-identical to Go's `uint32(xxh3.HashString(key))` (github.com/zeebo/xxh3): XXH3-64, seed 0,
//              default secret, UTF-8 bytes, LOW 32 bits.  The extra vectors below are the reference XXH3_64bits() of the xxHash C
//              library (python-xxhash 4.0.2, xxh3_64_intdigest) for the inputs of the xxHash sanity test (len 0..4096, generated with
//              XSUM_fillTestBuffer: byteGen = 2654435761; buf[i] = byteGen >> 56; byteGen *= 11400714785074694797) and for ASCII /
//              unicode keys; the three Go vectors (foo, bar, baz) agree with the same implementation, which ties the C reference to the
//              Go library.
//   Go tests: 1 (3 subtests), ported: 1, skipped: 0.
// =====================================================================================================================
using System.IO.Hashing;
using System.Text;
using Oxia.Common.Hash;
using Xunit;

namespace Oxia.Client.Tests.Common;

public class HashTests
{
    [Theory]
    [InlineData("foo", 125730186u)]
    [InlineData("bar", 2687685474u)]
    [InlineData("baz", 862947621u)]
    public void TestXxh332(string key, uint expected)
    {
        var hash = Hasher.Xxh332(key);
        Assert.Equal(expected, hash);
    }

    /// <summary>XSUM_fillTestBuffer of the xxHash sanity tests.</summary>
    private static byte[] SanityBuffer(int length)
    {
        const uint prime32 = 2654435761U;
        const ulong prime64 = 11400714785074694797UL;
        var buffer = new byte[length];
        var byteGen = (ulong)prime32;
        for (var i = 0; i < length; i++)
        {
            buffer[i] = (byte)(byteGen >> 56);
            byteGen = unchecked(byteGen * prime64);
        }

        return buffer;
    }

    public static TheoryData<int, ulong, uint> SanityVectors => new()
    {
        { 0, 0x2D06800538D394C2UL, 953390274u },
        { 1, 0xC44BDFF4074EECDBUL, 122612955u },
        { 2, 0x7A9978044CB8A8BBUL, 1287170235u },
        { 3, 0x54247382A8D6B94DUL, 2832644429u },
        { 4, 0xE5DC74BC51848A51UL, 1367640657u },
        { 5, 0xE4243F00720306BBUL, 1912800955u },
        { 8, 0x24CCC9ACAA9F65E4UL, 2862573028u },
        { 9, 0x14D5001C15DD3F2BUL, 366821163u },
        { 15, 0x45556D4D6E1798BCUL, 1847040188u },
        { 16, 0x981B17D36C7498C9UL, 1819580617u },
        { 17, 0x796F5ACD3A60F862UL, 979433570u },
        { 32, 0x9FEADDBDBF57EED3UL, 3210211027u },
        { 64, 0x9CB48487720EC49DUL, 1913570461u },
        { 100, 0x93CD95432B7D483FUL, 729630783u },
        { 128, 0xFCFF24126754D861UL, 1733613665u },
        { 129, 0x98F1B0A679A2CA29UL, 2040711721u },
        { 130, 0x7F6C531CF53EEB30UL, 4114541360u },
        { 160, 0x9D03A319ED4CBD2BUL, 3981229355u },
        { 192, 0xAF9F58E78B8D3587UL, 2341287303u },
        { 239, 0x16CE2B9D3B28805DUL, 992510045u },
        { 240, 0x81C3C2B67F568CCFUL, 2136378575u },
        { 241, 0xC5A639ECD2030E5EUL, 3523415646u },
        { 255, 0xE98F979F4ED8A197UL, 1322819991u },
        { 256, 0x55DE574AD89D0AC5UL, 3634170565u },
        { 512, 0x617E49599013CB6BUL, 2417216363u },
        { 1023, 0x87A8F7B2F2E22496UL, 4074906774u },
        { 1024, 0xDD85C9B5C1109C5CUL, 3239091292u },
        { 1025, 0xD870C0FA13211C6AUL, 320937066u },
        { 2048, 0xDD59E2C3A5F038E0UL, 2783983840u },
        { 4096, 0xE91206429D1F48F9UL, 2636073209u },
    };

    [Theory]
    [MemberData(nameof(SanityVectors))]
    public void Xxh332_OfBytes_MatchesTheReferenceXxh3_64(int length, ulong xxh3, uint low32)
    {
        var data = SanityBuffer(length);

        Assert.Equal(xxh3, XxHash3.HashToUInt64(data)); // the library itself is the canonical XXH3_64bits
        Assert.Equal(low32, (uint)xxh3); // Go: uint32(xxh3.Hash(...))
        Assert.Equal(low32, Hasher.Xxh332(data));
    }

    private static string Pattern(int length)
    {
        const string baseText = "abcdefghijklmnopqrstuvwxyz0123456789/-_";
        var sb = new StringBuilder(length + baseText.Length);
        while (sb.Length < length) sb.Append(baseText);
        return sb.ToString(0, length);
    }

    public static TheoryData<int, uint> AsciiVectors => new()
    {
        { 0, 953390274u },
        { 1, 513166879u },
        { 3, 2301573456u },
        { 4, 1403558032u },
        { 8, 1118397571u },
        { 9, 390434976u },
        { 16, 2937378984u },
        { 17, 3746024143u },
        { 128, 4238307628u },
        { 129, 2377516875u },
        { 240, 3509614491u },
        { 241, 290946403u },
        { 1024, 4141384621u },
    };

    [Theory]
    [MemberData(nameof(AsciiVectors))]
    public void Xxh332_OfAsciiKeys_MatchesTheReference(int length, uint expected)
    {
        var key = Pattern(length);
        Assert.Equal(expected, Hasher.Xxh332(key));
        Assert.Equal(expected, Hasher.Xxh332(key.AsSpan()));
        Assert.Equal(expected, Hasher.Xxh332(Encoding.UTF8.GetBytes(key)));
    }

    [Theory]
    [InlineData("héllo", 2069142303u)]
    [InlineData("日本語キー", 544570926u)]
    [InlineData("😀/👍", 4003786826u)]
    [InlineData("a😀b", 2189501033u)]
    [InlineData("é/ü/ß", 2984420809u)]
    [InlineData("/oxia/ключ/键/🔑", 1937591270u)]
    [InlineData("__oxia/session/00000000000000000001", 1036480091u)]
    public void Xxh332_OfUnicodeKeys_HashesTheUtf8Bytes(string key, uint expected)
    {
        Assert.Equal(expected, Hasher.Xxh332(key));
        Assert.Equal(expected, Hasher.Xxh332(Encoding.UTF8.GetBytes(key)));
    }

    [Fact]
    public void Xxh332_OfKeysLargerThanTheStackBuffer_UsesThePooledPath()
    {
        // 600 chars / 1800 UTF-8 bytes and 300 chars / 600 bytes: both need more than the 512-byte stack buffer
        Assert.Equal(2344808164u, Hasher.Xxh332(string.Concat(Enumerable.Repeat("日本語キー/", 100))));
        Assert.Equal(1171825309u, Hasher.Xxh332(new string('é', 300)));
    }

    [Fact]
    public void Xxh332_StackAndPooledPathsAgreeAroundTheBoundary()
    {
        // Utf8 max byte count = (chars + 1) * 3: 170 chars -> 513 bytes (pooled), 169 chars -> 510 (stack)
        for (var chars = 160; chars <= 180; chars++)
        {
            foreach (var key in new[] { new string('x', chars), new string('é', chars), new string('日', chars) })
            {
                Assert.Equal(Hasher.Xxh332(Encoding.UTF8.GetBytes(key)), Hasher.Xxh332(key));
            }
        }
    }

    [Fact]
    public void Xxh332_RandomStrings_EqualTheHashOfTheirUtf8Bytes()
    {
        var rnd = new Random(7);
        string[] alphabet = ["a", "b", "/", "é", "日", "😀", "_"];
        for (var i = 0; i < 2000; i++)
        {
            var sb = new StringBuilder();
            var n = rnd.Next(0, 300);
            for (var j = 0; j < n; j++)
            {
                sb.Append(alphabet[rnd.Next(alphabet.Length)]);
            }

            var key = sb.ToString();
            Assert.Equal((uint)XxHash3.HashToUInt64(Encoding.UTF8.GetBytes(key)), Hasher.Xxh332(key));
        }
    }

    [Fact]
    public void Xxh332_IsStableAcrossCallsAndThreads()
    {
        var expected = Hasher.Xxh332("/some/key");
        Parallel.For(0, 10_000, _ => Assert.Equal(expected, Hasher.Xxh332("/some/key")));
    }

    [Fact]
    public void Xxh332_DoesNotAllocate()
    {
        var key = "/a/typical/key/of/moderate/length-0123456789";
        var utf8 = Encoding.UTF8.GetBytes(key);
        uint sink = 0;
        for (var i = 0; i < 100; i++) sink += Hasher.Xxh332(key) + Hasher.Xxh332(utf8);

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 10_000; i++) sink += Hasher.Xxh332(key) + Hasher.Xxh332(utf8);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.NotEqual(0u, sink);
        Assert.Equal(0, allocated);
    }
}

// =====================================================================================================================
// PORT NOTE
//   Upstream : common/compare/compare_with_slash_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests: 1, ported: 1, skipped: 0.
//   Divergences: `[]byte("...")` -> UTF-8 bytes of the literal (ReadOnlySpan<byte> via a local helper).
//   Extra tests (not in Go): differential test against a straightforward list-based re-statement of the algorithm.
// =====================================================================================================================
using System.Text;
using Oxia.Common.Compare;
using Xunit;

namespace Oxia.Client.Tests.Common;

public class CompareWithSlashTests
{
    private static int Cmp(string a, string b) => KeyCompare.CompareWithSlash(Encoding.UTF8.GetBytes(a), Encoding.UTF8.GetBytes(b));

    [Fact]
    public void TestCompareWithSlash()
    {
        Assert.Equal(0, Cmp("aaaaa", "aaaaa"));
        Assert.Equal(-1, Cmp("aaaaa", "zzzzz"));
        Assert.Equal(+1, Cmp("bbbbb", "aaaaa"));

        Assert.Equal(+1, Cmp("aaaaa", ""));
        Assert.Equal(-1, Cmp("", "aaaaaa"));
        Assert.Equal(0, Cmp("", ""));

        Assert.Equal(-1, Cmp("aaaaa", "aaaaaaaaaaa"));
        Assert.Equal(+1, Cmp("aaaaaaaaaaa", "aaa"));

        Assert.Equal(-1, Cmp("a", "/"));
        Assert.Equal(+1, Cmp("/", "a"));

        Assert.Equal(-1, Cmp("/aaaa", "/bbbbb"));
        Assert.Equal(-1, Cmp("/aaaa", "/aa/a"));
        Assert.Equal(-1, Cmp("/aaaa/a", "/aaaa/b"));
        Assert.Equal(+1, Cmp("/aaaa/a/a", "/bbbbbbbbbb"));
        Assert.Equal(+1, Cmp("/aaaa/a/a", "/aaaa/bbbbbbbbbb"));

        Assert.Equal(+1, Cmp("/a/b/a/a/a", "/a/b/a/b"));
    }
}

public class CompareWithSlashExtraTests
{
    /// <summary>The Go algorithm restated with arrays and Array.IndexOf / manual comparison (no spans, no vectorisation).</summary>
    private static int Reference(byte[] a, byte[] b)
    {
        var ai = 0;
        var bi = 0;
        while (ai < a.Length && bi < b.Length)
        {
            var idxA = Array.IndexOf(a, (byte)'/', ai);
            var idxB = Array.IndexOf(b, (byte)'/', bi);
            if (idxA < 0 && idxB < 0) return Bytes(a, ai, a.Length, b, bi, b.Length);
            if (idxA < 0) return -1;
            if (idxB < 0) return +1;

            var res = Bytes(a, ai, idxA, b, bi, idxB);
            if (res != 0) return res;
            ai = idxA + 1;
            bi = idxB + 1;
        }

        var remA = a.Length - ai;
        var remB = b.Length - bi;
        return remA < remB ? -1 : remA > remB ? +1 : 0;
    }

    private static int Bytes(byte[] a, int aStart, int aEnd, byte[] b, int bStart, int bEnd)
    {
        var la = aEnd - aStart;
        var lb = bEnd - bStart;
        for (var i = 0; i < Math.Min(la, lb); i++)
        {
            if (a[aStart + i] != b[bStart + i]) return a[aStart + i] < b[bStart + i] ? -1 : +1;
        }

        return la < lb ? -1 : la > lb ? +1 : 0;
    }

    private static byte[] RandomKey(Random rnd)
    {
        var length = rnd.Next(0, 12);
        var key = new byte[length];
        for (var i = 0; i < length; i++)
        {
            key[i] = (byte)(rnd.Next(4) switch { 0 => '/', 1 => 'a', 2 => 'b', _ => 0xC3 });
        }

        return key;
    }

    [Fact]
    public void MatchesTheReferenceOnRandomKeys_AndIsAntisymmetric()
    {
        var rnd = new Random(12345);
        for (var i = 0; i < 20_000; i++)
        {
            var a = RandomKey(rnd);
            var b = RandomKey(rnd);
            var actual = KeyCompare.CompareWithSlash(a, b);

            Assert.Equal(Reference(a, b), actual);
            Assert.Equal(-actual, KeyCompare.CompareWithSlash(b, a));
            Assert.Equal(0, KeyCompare.CompareWithSlash(a, a));
        }
    }

    [Fact]
    public void LongSpansUseTheVectorisedPaths()
    {
        var a = Encoding.UTF8.GetBytes(new string('x', 1000) + "/" + new string('y', 1000));
        var b = (byte[])a.Clone();
        Assert.Equal(0, KeyCompare.CompareWithSlash(a, b));
        b[1500]++;
        Assert.Equal(-1, KeyCompare.CompareWithSlash(a, b));
        Assert.Equal(+1, KeyCompare.CompareWithSlash(b, a));
    }
}

// =====================================================================================================================
// PORT NOTE
//   Upstream : common/compare/compare_with_slash.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Oxia key ordering of the legacy (pre-encoder) hierarchical comparison: '/'-separated levels are compared one
//              span at a time, and a key that still has a '/' sorts AFTER one that has not at the same level ("/a" < "/a/").
//   Divergences: []byte -> ReadOnlySpan<byte> (no allocation, vectorised IndexOf / SequenceCompareTo).  Returns exactly
//              -1 / 0 / +1 like Go's bytes.Compare based implementation (not an arbitrary negative / positive number).
// =====================================================================================================================
namespace Oxia.Common.Compare;

public static class KeyCompare
{
    private const byte Slash = (byte)'/';

    /// <summary>Go: compare.CompareWithSlash(a, b) -> -1 / 0 / +1.</summary>
    public static int CompareWithSlash(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        while (a.Length > 0 && b.Length > 0)
        {
            var idxA = a.IndexOf(Slash);
            var idxB = b.IndexOf(Slash);
            if (idxA < 0 && idxB < 0)
            {
                return Math.Sign(a.SequenceCompareTo(b));
            }

            if (idxA < 0 && idxB >= 0)
            {
                return -1;
            }

            if (idxA >= 0 && idxB < 0)
            {
                return +1;
            }

            // At this point, both slices have '/'
            var spanRes = a[..idxA].SequenceCompareTo(b[..idxB]);
            if (spanRes != 0)
            {
                return Math.Sign(spanRes);
            }

            a = a[(idxA + 1)..];
            b = b[(idxB + 1)..];
        }

        if (a.Length < b.Length)
        {
            return -1;
        }

        if (a.Length > b.Length)
        {
            return +1;
        }

        return 0;
    }
}

// =====================================================================================================================
// PORT NOTE
//   Upstream : (no Go counterpart) -- Go allocates a string per decoded field.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : A small, bounded, lock-free cache for the repeated LOW-CARDINALITY strings of the responses: the client identity of
//              an ephemeral record's version (the same few identities answer every ephemeral put/get), and the names of secondary
//              indexes.  The decoder asks the cache with the UTF-8 span; a hit returns the string that was allocated for the first
//              occurrence, a miss decodes normally and remembers the result.  NEVER used for record keys or values.
//   Design   : 64 direct-mapped slots indexed by XxHash3 of the bytes; a slot holds only the string (entries are immutable, a racing
//              writer just replaces one with an equal-or-other string).  Only short ASCII strings are cached, so a hit is verified with
//              the vectorised Ascii.Equals(bytes, chars) -- no second copy of the bytes is kept.  High-cardinality data simply
//              misses (cost: one hash + one compare on top of the decode it needed anyway).
// =====================================================================================================================
using System.IO.Hashing;
using System.Text;

namespace Oxia.Common.Proto;

internal static class Utf8StringCache
{
    private const int Slots = 64;
    private const int MaxLength = 128;

    private static readonly string?[] Cache = new string?[Slots];

    /// <summary>The string for these UTF-8 bytes; the same instance for repeated short ASCII input.</summary>
    public static string Get(ReadOnlySpan<byte> utf8)
    {
        int length = utf8.Length;
        if (length == 0) return string.Empty;
        if (length > MaxLength) return Encoding.UTF8.GetString(utf8);

        ref string? slot = ref Cache[(int)(XxHash3.HashToUInt64(utf8) & (Slots - 1))];
        string? cached = Volatile.Read(ref slot);
        if (cached is not null && Ascii.Equals(utf8, cached)) return cached;

        string s = Encoding.UTF8.GetString(utf8);
        if (s.Length == length) // pure ASCII: one char per byte, so Ascii.Equals can verify the next hit
        {
            Volatile.Write(ref slot, s);
        }
        return s;
    }
}

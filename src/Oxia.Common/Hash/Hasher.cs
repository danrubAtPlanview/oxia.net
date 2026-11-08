// =====================================================================================================================
// PORT NOTE
//   Upstream : common/hash/hash.go   `func Xxh332(key string) uint32 { return uint32(xxh3.HashString(key)) }`
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Shard routing hash.  XXH3-64 (seed 0, default secret) of the UTF-8 bytes of the key, truncated to its LOW 32
//              bits.  System.IO.Hashing.XxHash3 is bit-compatible with github.com/zeebo/xxh3 (same canonical XXH3_64bits).
//   Performance: no allocation -- short keys are transcoded into a stack buffer, long keys into a pooled array.
//              Callers that already hold UTF-8 bytes use the span overload.
//   Divergences: Go hashes the raw bytes of a string; a .NET string is UTF-16, so the UTF-8 encoding is hashed (a lone surrogate hashes
//              as U+FFFD, EF BF BD -- the client validates keys before routing them).  Verified bit-identical in
//              tests/Oxia.Client.Tests/Common/Hash/HashTests.cs: the Go test vectors (foo / bar / baz) plus reference XXH3_64bits() values
//              (xxHash C library) for inputs of 0..4096 bytes covering every XXH3 length class, ASCII and unicode keys, stack and pooled paths.
// =====================================================================================================================
using System.Buffers;
using System.IO.Hashing;
using System.Text;

namespace Oxia.Common.Hash;

public static class Hasher
{
    private const int StackLimit = 512;

    /// <summary>Go: hash.Xxh332(key).</summary>
    public static uint Xxh332(string key) => Xxh332(key.AsSpan());

    public static uint Xxh332(ReadOnlySpan<char> key)
    {
        if (key.IsEmpty) return unchecked((uint)XxHash3.HashToUInt64(ReadOnlySpan<byte>.Empty));

        int max = Encoding.UTF8.GetMaxByteCount(key.Length);
        if (max <= StackLimit)
        {
            Span<byte> buf = stackalloc byte[StackLimit];
            int n = Encoding.UTF8.GetBytes(key, buf);
            return unchecked((uint)XxHash3.HashToUInt64(buf[..n]));
        }

        byte[] rented = ArrayPool<byte>.Shared.Rent(max);
        try
        {
            int n = Encoding.UTF8.GetBytes(key, rented);
            return unchecked((uint)XxHash3.HashToUInt64(rented.AsSpan(0, n)));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>Hash of already-encoded UTF-8 bytes.</summary>
    public static uint Xxh332(ReadOnlySpan<byte> utf8Key) => unchecked((uint)XxHash3.HashToUInt64(utf8Key));
}

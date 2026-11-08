// =====================================================================================================================
// PORT NOTE   *** CONTRACT STUB -- owned by the "common utilities" component; replace bodies, keep signatures ***
//   Upstream : common/compare/compare_with_slash.go, common/compare/encode.go  (full file, incl. PrefixRanges)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Oxia key ordering.  CompareWithSlash is the legacy (pre-encoder) ordering; the hierarchical/natural
//              encoders map a key to a byte string whose plain lexicographic order equals the server's key order.
//              The client merges multi-shard results using `Encode` + ordinal byte comparison (see keyOrder in the client).
//   Performance: provide span-based, allocation-free overloads (`TryEncode(string, Span<byte>)`, `EncodedLength`) in
//              addition to the Go-shaped API; `Encode` of an internal-prefix-free natural key may NOT alias the string.
// =====================================================================================================================
using System.Text;

namespace Oxia.Common.Compare;

public static class KeyCompare
{
    /// <summary>Go: compare.CompareWithSlash(a, b) -> negative / 0 / positive.</summary>
    public static int CompareWithSlash(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => throw new NotImplementedException();
}

/// <summary>Go: compare.Encoder.</summary>
public interface IEncoder
{
    string Name { get; }
    byte[] Encode(string key);
    /// <summary>Go: AppendEncode(dst, key) -- appends Encode(key) to <paramref name="dst"/> (List-like buffer writer).</summary>
    void AppendEncode(System.Buffers.IBufferWriter<byte> dst, string key);
    /// <summary>Allocation-free: number of bytes Encode(key) produces.</summary>
    int EncodedLength(string key);
    /// <summary>Allocation-free: writes Encode(key) into <paramref name="dst"/> (must be >= EncodedLength) and returns the length.</summary>
    int EncodeTo(string key, Span<byte> dst);
    string Decode(ReadOnlySpan<byte> encodedKey);
    bool IsInternalKey(ReadOnlySpan<byte> encodedKey);
}

public static class Encoders
{
    public static IEncoder Natural => throw new NotImplementedException();
    public static IEncoder Hierarchical => throw new NotImplementedException();

    /// <summary>Go: GetEncoder(name): "hierarchical" | "natural", else throws ArgumentException("unknown encoder X").</summary>
    public static IEncoder GetEncoder(string name) => throw new NotImplementedException();
}

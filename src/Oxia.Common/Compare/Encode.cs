// =====================================================================================================================
// PORT NOTE
//   Upstream : common/compare/encode.go   (Encoder, PrefixRanges, EncoderHierarchical, EncoderNatural, GetEncoder,
//              keyRegionSuccessor, splitLevel, levelKey)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The key encoders map a key to a byte string whose plain lexicographic (unsigned byte) order equals the
//              server's key order.  Hierarchical = 2-byte big-endian '/'-count prefix (top bit = internal key) + the key with
//              '/' replaced by 0xFF; natural = the raw key, with the "__" of the internal prefix "__oxia/" turned into 0xFFFF.
//              The client merges multi-shard results with `Encode` + ordinal byte comparison.
//   Divergences:
//      * string (bytes) -> .NET UTF-16 string; keys are UTF-8 encoded (invalid UTF-16, i.e. lone surrogates, become U+FFFD;
//        the client validates keys with the utf8.ValidString rule before they get here).  Decode returns the UTF-8 decoded string.
//      * Encode ALWAYS returns a new array (Go's natural Encode aliases the string memory for non-internal keys).
//      * AppendEncode(dst []byte, key) -> AppendEncode(IBufferWriter<byte>, key).
//      * Added allocation-free members for the hot path: EncodedLength / EncodeTo / TryEncode (+ ReadOnlySpan<char> overloads).
//        Counting and replacing '/' use the vectorised MemoryExtensions.Count / Replace.
//      * []byte results that Go documents as constants and read-only (InternalKeyRange) are shared arrays here too: callers
//        must not modify them.  `PrefixRanges` interface -> IPrefixRanges (and a method of the same name on IEncoder).
//      * GetEncoder: the unknown-name error is an ArgumentException with the message "unknown encoder <name>".
// =====================================================================================================================
using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using Oxia.Common.Constant;

namespace Oxia.Common.Compare;

/// <summary>Go: compare.Encoder.</summary>
public interface IEncoder
{
    string Name { get; }

    /// <summary>The encoded form of <paramref name="key"/>, in a NEW array.</summary>
    byte[] Encode(string key);

    /// <summary>Go: AppendEncode(dst, key) -- appends Encode(key) to <paramref name="dst"/> (List-like buffer writer).</summary>
    void AppendEncode(IBufferWriter<byte> dst, string key);

    /// <summary>Allocation-free: number of bytes Encode(key) produces.</summary>
    int EncodedLength(string key);

    /// <inheritdoc cref="EncodedLength(string)"/>
    int EncodedLength(ReadOnlySpan<char> key);

    /// <summary>Allocation-free: writes Encode(key) into <paramref name="dst"/> (must be >= EncodedLength) and returns the length.</summary>
    int EncodeTo(string key, Span<byte> dst);

    /// <inheritdoc cref="EncodeTo(string, Span{byte})"/>
    int EncodeTo(ReadOnlySpan<char> key, Span<byte> dst);

    /// <summary>Allocation-free: like <see cref="EncodeTo(ReadOnlySpan{char}, Span{byte})"/> but returns false when <paramref name="dst"/> is too small.</summary>
    bool TryEncode(ReadOnlySpan<char> key, Span<byte> dst, out int written);

    string Decode(ReadOnlySpan<byte> encodedKey);

    bool IsInternalKey(ReadOnlySpan<byte> encodedKey);

    /// <summary>
    /// Go: InternalKeyRange() -- the encoded half-open range [start, end) that contains every internal key.  The region is always
    /// contiguous, so an iterator can seek past it in one jump.  <c>End</c> is null when the region runs to the end of the keyspace
    /// (hierarchical encoder) -- NOT for the natural one, where a user key is stored raw and can sort after the internal keys.
    /// The arrays are shared and read-only.
    /// </summary>
    (byte[] Start, byte[]? End) InternalKeyRange();

    /// <summary>Go: PrefixRanges(prefix) -- locates the keys that start with <paramref name="prefix"/> among the encoded keys.</summary>
    IPrefixRanges PrefixRanges(string prefix);
}

/// <summary>
/// Go: compare.PrefixRanges -- the ranges of encoded keys that hold the keys with a given prefix.  The natural encoder keeps them in a
/// single range.  The hierarchical encoder sorts the keys by level first, so they are in one range per level, and other keys can
/// sort between two of these ranges: to only visit the keys with the prefix, an iterator has to seek from one range to the next.
/// </summary>
public interface IPrefixRanges
{
    /// <summary>[lower, upper), from the start of the first range to the end of the last one.  upper is null when the last range runs to the end of the keyspace.</summary>
    (byte[] Lower, byte[]? Upper) Bounds();

    /// <summary>Whether an encoded key within the bounds is in one of the ranges, which means that it starts with the prefix.</summary>
    bool Contains(ReadOnlySpan<byte> encodedKey);

    /// <summary>The start of the first range after an encoded key that is not in a range, or null when there is none.</summary>
    byte[]? NextStart(ReadOnlySpan<byte> encodedKey);

    /// <summary>The end of the last range before an encoded key that is not in a range, or null when there is none.</summary>
    byte[]? PrevEnd(ReadOnlySpan<byte> encodedKey);
}

public static class Encoders
{
    /// <summary>Go: compare.EncoderNatural.</summary>
    public static IEncoder Natural => NaturalEncoder.Instance;

    /// <summary>
    /// Go: compare.EncoderHierarchical -- ensures that keys of the same level sort together, so the children of a given path are
    /// easy to return.  The encoding prepends 2 bytes with the count of '/'; the slashes are also converted into a special
    /// character that sorts after any other.
    /// </summary>
    public static IEncoder Hierarchical => HierarchicalEncoder.Instance;

    /// <summary>Go: GetEncoder(name): "hierarchical" | "natural", else throws ArgumentException("unknown encoder X").</summary>
    public static IEncoder GetEncoder(string name) => name switch
    {
        "hierarchical" => Hierarchical,
        "natural" => Natural,
        _ => throw new ArgumentException($"unknown encoder {name}"),
    };

    /// <summary>The smallest key that sorts after every key with the given prefix, or null when none exists (an all-0xff or empty prefix).</summary>
    internal static byte[]? KeyRegionSuccessor(ReadOnlySpan<byte> prefix)
    {
        for (var i = prefix.Length - 1; i >= 0; i--)
        {
            if (prefix[i] != byte.MaxValue)
            {
                var end = prefix[..(i + 1)].ToArray();
                end[i]++;
                return end;
            }
        }

        return null;
    }

    internal static bool HasInternalPrefix(ReadOnlySpan<char> key) => key.StartsWith(Constants.InternalKeyPrefix, StringComparison.Ordinal);

    /// <summary>Decodes UTF-8 into a single string allocation, optionally preceded by <paramref name="prefix"/>.</summary>
    internal static string Utf8ToString(ReadOnlySpan<char> prefix, ReadOnlySpan<byte> utf8)
    {
        var total = prefix.Length + Encoding.UTF8.GetCharCount(utf8);
        if (total == 0) return string.Empty;
        if (prefix.IsEmpty) return Encoding.UTF8.GetString(utf8);

        char[]? rented = null;
        Span<char> buffer = total <= 256 ? stackalloc char[256] : (rented = ArrayPool<char>.Shared.Rent(total));
        try
        {
            prefix.CopyTo(buffer);
            Encoding.UTF8.GetChars(utf8, buffer[prefix.Length..]);
            return new string(buffer[..total]);
        }
        finally
        {
            if (rented is not null) ArrayPool<char>.Shared.Return(rented);
        }
    }
}

// ---------------------------------------------------------------------------------------------------------------------

internal sealed class HierarchicalEncoder : IEncoder
{
    internal static readonly HierarchicalEncoder Instance = new();

    internal const byte EncodedSeparator = 0xff;
    internal const int InternalKeysBitMarker = 1 << 15;

    /// <summary>The deepest level the 2-byte prefix can record.  The count shares those bytes with the internal-key marker, so it has to stay below it.</summary>
    internal const int MaxSeparatorCount = InternalKeysBitMarker - 1;

    /// <summary>The bounds returned by InternalKeyRange are constant and read-only, so they are built once.</summary>
    private static readonly byte[] InternalKeyStart = [InternalKeysBitMarker >> 8];

    public string Name => "hierarchical";

    public byte[] Encode(string key)
    {
        var buf = new byte[EncodedLength(key)];
        EncodeHierarchical(key, buf);
        return buf;
    }

    public void AppendEncode(IBufferWriter<byte> dst, string key)
    {
        var length = EncodedLength(key);
        EncodeHierarchical(key, dst.GetSpan(length));
        dst.Advance(length);
    }

    public int EncodedLength(string key) => EncodedLength(key.AsSpan());

    public int EncodedLength(ReadOnlySpan<char> key) => Encoding.UTF8.GetByteCount(key) + 2;

    public int EncodeTo(string key, Span<byte> dst) => EncodeTo(key.AsSpan(), dst);

    public int EncodeTo(ReadOnlySpan<char> key, Span<byte> dst)
    {
        var length = EncodedLength(key);
        if (dst.Length < length) throw new ArgumentException("destination is too small", nameof(dst));
        EncodeHierarchical(key, dst);
        return length;
    }

    public bool TryEncode(ReadOnlySpan<char> key, Span<byte> dst, out int written)
    {
        var length = EncodedLength(key);
        if (dst.Length < length)
        {
            written = 0;
            return false;
        }

        EncodeHierarchical(key, dst);
        written = length;
        return true;
    }

    /// <summary>Writes the encoding of key to buf, which holds at least Encode(key).Length bytes.</summary>
    private static void EncodeHierarchical(ReadOnlySpan<char> key, Span<byte> buf)
    {
        var l = Encoding.UTF8.GetBytes(key, buf[2..]);

        // Vectorised count; the replacement is skipped if we know the separators are not used
        var sepCount = key.Count('/');
        if (sepCount != 0)
        {
            buf.Slice(2, l).Replace((byte)'/', EncodedSeparator);

            if (key.Length >= 2 && key[^1] == '/' && key[^2] == '/')
            {
                // Ignore the trailing separator as it doesn't create a new level
                sepCount--;
            }

            if (sepCount > MaxSeparatorCount)
            {
                // Keys deeper than the prefix can count all share the deepest level, rather than wrapping into the internal-key region
                sepCount = MaxSeparatorCount;
            }

            if (Encoders.HasInternalPrefix(key))
            {
                // All internal keys at the end
                sepCount |= InternalKeysBitMarker;
            }
        }

        BinaryPrimitives.WriteUInt16BigEndian(buf, (ushort)sepCount);
    }

    public string Decode(ReadOnlySpan<byte> encoded)
    {
        if (encoded.Length <= 2)
        {
            return string.Empty;
        }

        var rest = encoded[2..];
        if ((encoded[0] == 0 && encoded[1] == 0) || rest.IndexOf(EncodedSeparator) < 0)
        {
            return Encoding.UTF8.GetString(rest);
        }

        byte[]? rented = null;
        Span<byte> work = rest.Length <= 512 ? stackalloc byte[512] : (rented = ArrayPool<byte>.Shared.Rent(rest.Length));
        try
        {
            var replaced = work[..rest.Length];
            rest.CopyTo(replaced);
            replaced.Replace(EncodedSeparator, (byte)'/');
            return Encoding.UTF8.GetString(replaced);
        }
        finally
        {
            if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
        }
    }

    public bool IsInternalKey(ReadOnlySpan<byte> encodedKey)
    {
        if (encodedKey.Length == 0)
        {
            return false;
        }

        // If the first bit is set, it means it's an internal key
        return (encodedKey[0] & (1 << 7)) != 0;
    }

    /// <summary>
    /// The internal-key marker is the top bit of the 2-byte prefix, so every internal key sorts after every regular one (a regular
    /// key would need 32768 separators to reach that region) -- the region runs to the end of the keyspace.
    /// </summary>
    public (byte[] Start, byte[]? End) InternalKeyRange() => (InternalKeyStart, null);

    /// <summary>
    /// A key is encoded as its level, on 2 bytes, followed by the key with the separators replaced.  The keys with the prefix are
    /// the ones whose bytes after the level start with the encoded prefix: one range per level, from the level of the prefix
    /// itself, which no key with the prefix is below, to the deepest one.
    /// </summary>
    public IPrefixRanges PrefixRanges(string prefix)
    {
        var encoded = Encode(prefix);
        return new HierarchicalPrefixRanges(
            BinaryPrimitives.ReadUInt16BigEndian(encoded),
            encoded[2..],
            Encoders.KeyRegionSuccessor(encoded.AsSpan(2)));
    }

    private sealed class HierarchicalPrefixRanges(ushort minLevel, byte[] prefix, byte[]? prefixEnd) : IPrefixRanges
    {
        public (byte[] Lower, byte[]? Upper) Bounds() => (Start(minLevel), End(ushort.MaxValue));

        public bool Contains(ReadOnlySpan<byte> encodedKey)
        {
            SplitLevel(encodedKey, out _, out var rest);
            return rest.StartsWith(prefix);
        }

        /// <summary>
        /// Takes the level of the key to skip the levels without keys: the next range is in the same level when the key sorts before
        /// its range, and in the next level otherwise.
        /// </summary>
        public byte[]? NextStart(ReadOnlySpan<byte> encodedKey)
        {
            SplitLevel(encodedKey, out var level, out var rest);
            if (level < minLevel) return Start(minLevel);
            if (rest.SequenceCompareTo(prefix) < 0) return Start(level);
            if (level < ushort.MaxValue) return Start((ushort)(level + 1));
            return null;
        }

        /// <summary>Mirrors NextStart: the previous range is in the same level when the key sorts after its range, and in the previous level otherwise.</summary>
        public byte[]? PrevEnd(ReadOnlySpan<byte> encodedKey)
        {
            SplitLevel(encodedKey, out var level, out var rest);
            if (level < minLevel) return null;
            if (prefixEnd is not null && rest.SequenceCompareTo(prefixEnd) >= 0) return End(level);
            if (level > minLevel) return End((ushort)(level - 1));
            return null;
        }

        private byte[] Start(ushort level) => LevelKey(level, prefix);

        /// <summary>The end of the range in a level.  A prefix without a successor, made only of separators, has its range run to the end of the level.</summary>
        private byte[]? End(ushort level)
        {
            if (prefixEnd is not null) return LevelKey(level, prefixEnd);
            if (level < ushort.MaxValue) return LevelKey((ushort)(level + 1), []);
            return null;
        }
    }

    private static byte[] LevelKey(ushort level, ReadOnlySpan<byte> rest)
    {
        var key = new byte[2 + rest.Length];
        BinaryPrimitives.WriteUInt16BigEndian(key, level);
        rest.CopyTo(key.AsSpan(2));
        return key;
    }

    /// <summary>Splits an encoded key into its level and the rest.  A key too short to hold a whole level sorts first in the level its bytes start.</summary>
    private static void SplitLevel(ReadOnlySpan<byte> encodedKey, out ushort level, out ReadOnlySpan<byte> rest)
    {
        Span<byte> header = stackalloc byte[2];
        var n = Math.Min(2, encodedKey.Length);
        encodedKey[..n].CopyTo(header);
        level = BinaryPrimitives.ReadUInt16BigEndian(header);
        rest = encodedKey[n..];
    }
}

// ---------------------------------------------------------------------------------------------------------------------

internal sealed class NaturalEncoder : IEncoder
{
    internal static readonly NaturalEncoder Instance = new();

    internal const byte EncodedInternalPrefix = 0xFF;

    /// <summary>"__oxia/" with its "__" replaced by 0xFFFF.</summary>
    private static readonly byte[] EncodedInternalKeyPrefixBytes = [0xFF, 0xFF, (byte)'o', (byte)'x', (byte)'i', (byte)'a', (byte)'/'];

    private static readonly byte[] EncodedInternalKeyPrefixSuccessor = Encoders.KeyRegionSuccessor(EncodedInternalKeyPrefixBytes)!;

    public string Name => "natural";

    public byte[] Encode(string key)
    {
        var buf = new byte[Encoding.UTF8.GetByteCount(key)];
        EncodeNatural(key, buf);
        return buf;
    }

    public void AppendEncode(IBufferWriter<byte> dst, string key)
    {
        var length = Encoding.UTF8.GetByteCount(key);
        EncodeNatural(key, dst.GetSpan(length));
        dst.Advance(length);
    }

    public int EncodedLength(string key) => Encoding.UTF8.GetByteCount(key);

    public int EncodedLength(ReadOnlySpan<char> key) => Encoding.UTF8.GetByteCount(key);

    public int EncodeTo(string key, Span<byte> dst) => EncodeTo(key.AsSpan(), dst);

    public int EncodeTo(ReadOnlySpan<char> key, Span<byte> dst)
    {
        var length = Encoding.UTF8.GetByteCount(key);
        if (dst.Length < length) throw new ArgumentException("destination is too small", nameof(dst));
        EncodeNatural(key, dst);
        return length;
    }

    public bool TryEncode(ReadOnlySpan<char> key, Span<byte> dst, out int written)
    {
        var length = Encoding.UTF8.GetByteCount(key);
        if (dst.Length < length)
        {
            written = 0;
            return false;
        }

        EncodeNatural(key, dst);
        written = length;
        return true;
    }

    private static void EncodeNatural(ReadOnlySpan<char> key, Span<byte> buf)
    {
        Encoding.UTF8.GetBytes(key, buf);
        if (Encoders.HasInternalPrefix(key))
        {
            // Encode internal keys so that they don't end up in the middle of ascii sorting
            // eg: a DeleteRange('A', 'Z') would also delete the '__oxia/...' keys
            buf[0] = EncodedInternalPrefix;
            buf[1] = EncodedInternalPrefix;
        }
    }

    public string Decode(ReadOnlySpan<byte> encoded)
    {
        // The input is never modified (it may be memory owned by a storage iterator)
        if (IsInternalKey(encoded))
        {
            return Encoders.Utf8ToString("__", encoded[2..]);
        }

        return Encoding.UTF8.GetString(encoded);
    }

    public bool IsInternalKey(ReadOnlySpan<byte> encodedKey) => encodedKey.StartsWith(EncodedInternalKeyPrefixBytes);

    /// <summary>
    /// Internal keys are the "\xff\xffoxia/" prefix region.  A regular key is stored raw, so one whose bytes sort after that prefix
    /// (e.g. "\xff\xffz") lives *after* the internal keys -- the region has a real upper end and cannot be pruned away with an upper bound.
    /// </summary>
    public (byte[] Start, byte[]? End) InternalKeyRange() => (EncodedInternalKeyPrefixBytes, EncodedInternalKeyPrefixSuccessor);

    /// <summary>The keys with the prefix are a single range.</summary>
    public IPrefixRanges PrefixRanges(string prefix)
    {
        var start = Encode(prefix);
        return new NaturalPrefixRange(start, Encoders.KeyRegionSuccessor(start));
    }

    private sealed class NaturalPrefixRange(byte[] start, byte[]? end) : IPrefixRanges
    {
        public (byte[] Lower, byte[]? Upper) Bounds() => (start, end);

        public bool Contains(ReadOnlySpan<byte> encodedKey) => encodedKey.StartsWith(start);

        public byte[]? NextStart(ReadOnlySpan<byte> encodedKey) => encodedKey.SequenceCompareTo(start) < 0 ? start : null;

        public byte[]? PrevEnd(ReadOnlySpan<byte> encodedKey) => end is not null && encodedKey.SequenceCompareTo(end) >= 0 ? end : null;
    }
}

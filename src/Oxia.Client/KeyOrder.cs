// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/key_order.go   (keyOrder, newKeyOrder, sortKey, compareSortKeys, compare)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The order of the keys in the shards of a namespace.  Each shard returns the results of a read in this order,
//              and the client picks or merges the results of the shards in the same order.  The key sorting reported by the
//              server selects an encoder (natural / hierarchical) whose encoded form orders by plain byte comparison; when
//              the server does not report it (older servers) the client keeps the legacy order, CompareWithSlash.
//   Divergences:
//      * `keyOrder{}` (nil encoder) is `default(KeyOrder)`.
//      * Go's compare/compareSortKeys return exactly -1/0/+1 (bytes.Compare); the results are normalised with Math.Sign
//        because span comparison in .NET returns any negative/positive number.
//      * ALLOCATION.  `SortKey(string)` keeps Go's shape (a new byte[] per key) for callers that need to retain the sort
//        key.  The merge heap and `Compare(string, string)` use the allocation-free `SortKeyLength` / `SortKeyTo`
//        (encoder.EncodedLength/EncodeTo straight into a stack or pooled buffer); for the legacy order the "sort key" is
//        the UTF-8 form of the key.
//      * `New(KeySorting)` is Go's `newKeyOrder`; `NewKeyOrder` is an alias with the Go name.
// =====================================================================================================================
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Text;
using Oxia.Common.Compare;
using Oxia.Common.Proto;

namespace Oxia.Client;

/// <summary>Go: keyOrder.</summary>
internal readonly struct KeyOrder
{
    /// <summary>Largest total of two sort keys that Compare(string, string) handles on the stack.</summary>
    private const int StackLimit = 512;

    // The encoder of the keys in the shards, or null for the order of the old storage format, CompareWithSlash
    private readonly IEncoder? _encoder;

    private KeyOrder(IEncoder? encoder) => _encoder = encoder;

    /// <summary>Go: newKeyOrder(keySorting).</summary>
    public static KeyOrder New(KeySorting keySorting) => keySorting switch
    {
        KeySorting.Natural => new KeyOrder(Encoders.Natural),
        KeySorting.Hierarchical => new KeyOrder(Encoders.Hierarchical),
        // Older servers don't report the key sorting: keep the order that the client has always used
        _ => default,
    };

    /// <summary>Alias of <see cref="New"/> with Go's name.</summary>
    public static KeyOrder NewKeyOrder(KeySorting keySorting) => New(keySorting);

    /// <summary>Go: sortKey -- the form of a key that <see cref="CompareSortKeys"/> orders (a new array).</summary>
    public byte[] SortKey(string key) => _encoder is null ? Encoding.UTF8.GetBytes(key) : _encoder.Encode(key);

    /// <summary>Allocation-free: the number of bytes of <see cref="SortKey"/>.</summary>
    public int SortKeyLength(string key) => _encoder is null ? Encoding.UTF8.GetByteCount(key) : _encoder.EncodedLength(key);

    /// <summary>Allocation-free <see cref="SortKey"/>: writes it into <paramref name="dst"/> (>= SortKeyLength) and returns its length.</summary>
    public int SortKeyTo(string key, Span<byte> dst) => _encoder is null ? Encoding.UTF8.GetBytes(key, dst) : _encoder.EncodeTo(key, dst);

    /// <summary>Go: compareSortKeys -- negative, 0 or positive (exactly -1, 0, +1).</summary>
    public int CompareSortKeys(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) =>
        _encoder is null ? Math.Sign(KeyCompare.CompareWithSlash(a, b)) : Math.Sign(a.SequenceCompareTo(b));

    /// <summary>Go: compare -- the order of two keys (-1, 0, +1); allocation-free for keys up to a few hundred bytes.</summary>
    [SkipLocalsInit]
    public int Compare(string a, string b)
    {
        int lengthA = SortKeyLength(a);
        int lengthB = SortKeyLength(b);
        int total = lengthA + lengthB;

        byte[]? rented = null;
        Span<byte> buffer = total <= StackLimit ? stackalloc byte[StackLimit] : (rented = ArrayPool<byte>.Shared.Rent(total));
        try
        {
            int written = SortKeyTo(a, buffer);
            int writtenB = SortKeyTo(b, buffer.Slice(written));
            return CompareSortKeys(buffer.Slice(0, written), buffer.Slice(written, writtenB));
        }
        finally
        {
            if (rented is not null)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }
}

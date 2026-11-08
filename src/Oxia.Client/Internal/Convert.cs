// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/internal/convert.go  (toShard, toHashRange, hashRange)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Converts the wire ShardAssignment of the shard assignments stream into the client's Shard value.  A
//              ShardAssignment without a hash range boundary (a version-skewed or misbehaving server) is rejected with an
//              error instead of crashing the client.
//   Divergences:
//      * The class is called ShardConvert (not Convert) so that it does not shadow System.Convert inside the
//        Oxia.Client.Internal namespace; the file keeps the Go file name.
//      * (Shard, error) returns become `bool TryToShard(..., out Shard, out Exception?)`; the error is created only on the
//        failure path.  Go's `%T` of the nil boundary prints `<nil>`; so does the message here.
// =====================================================================================================================
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using Oxia.Common.Proto;

namespace Oxia.Client.Internal;

internal static class ShardConvert
{
    /// <summary>Go: toShard.</summary>
    public static bool TryToShard(ShardAssignment assignment, out Shard shard, [NotNullWhen(false)] out Exception? error)
    {
        if (!TryToHashRange(assignment, out var hashRange, out error))
        {
            shard = default;
            return false;
        }

        shard = new Shard(assignment.Shard, assignment.Leader, hashRange);
        return true;
    }

    /// <summary>Go: toHashRange -- only the int32 hash range boundary is supported.</summary>
    public static bool TryToHashRange(ShardAssignment assignment, out HashRange hashRange, [NotNullWhen(false)] out Exception? error)
    {
        var boundaries = assignment.Int32HashRange;
        if (boundaries is null)
        {
            hashRange = default;
            error = new InvalidOperationException(string.Create(CultureInfo.InvariantCulture,
                $"shard {assignment.Shard} has an unsupported hash range boundary <nil>"));
            return false;
        }

        hashRange = new HashRange(boundaries.MinHashInclusive, boundaries.MaxHashInclusive);
        error = null;
        return true;
    }

    /// <summary>Go: hashRange(min, max).</summary>
    public static HashRange MakeHashRange(uint minInclusive, uint maxInclusive) => new(minInclusive, maxInclusive);
}

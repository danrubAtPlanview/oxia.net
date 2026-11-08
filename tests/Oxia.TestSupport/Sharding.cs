// =====================================================================================================================
// PORT NOTE (test infrastructure)
//   Upstream : oxiad/common/sharding/shards.go (GenerateShards)  -- server side, used by the Go integration tests
//              (`shardOf` in tests/client/async_client_test.go) to know on which shard of a standalone server a key lands.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Same arithmetic as Go, including the uint32 wrap-around for a single shard (bucketSize overflows to 0).
//   Divergences: none.
// =====================================================================================================================
using Oxia.Common.Hash;

namespace Oxia.TestSupport;

/// <summary>Go: <c>sharding.Shard</c>.</summary>
public readonly record struct ShardRange(long Id, uint Min, uint Max);

public static class Sharding
{
    /// <summary>Go: <c>sharding.GenerateShards(baseId, numShards)</c>.</summary>
    public static IReadOnlyList<ShardRange> GenerateShards(long baseId, uint numShards)
    {
        var shards = new List<ShardRange>((int)numShards);
        unchecked
        {
            uint bucketSize = (uint.MaxValue / numShards) + 1;
            for (uint i = 0; i < numShards; i++)
            {
                uint lowerBound = i * bucketSize;
                uint upperBound = lowerBound + bucketSize - 1;
                if (i == numShards - 1) upperBound = uint.MaxValue;
                shards.Add(new ShardRange(baseId + i, lowerBound, upperBound));
            }
        }

        return shards;
    }

    /// <summary>Go: <c>shardOf(key, numShards)</c> -- the shard of a standalone server that stores the key.</summary>
    public static long ShardOf(string key, uint numShards)
    {
        var code = Hasher.Xxh332(key);
        foreach (var shard in GenerateShards(0, numShards))
        {
            if (shard.Min <= code && code <= shard.Max) return shard.Id;
        }

        throw new InvalidOperationException("no shard for the key");
    }
}

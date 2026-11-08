// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/internal/shard_strategy_impl_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests: 1, ported: 1, skipped: 0
//   Intent   : the predicate of ShardStrategy.Get selects the shards whose inclusive hash range contains the hash of the key.
//   Adaptations: `&shardStrategyImpl{hashFunc: ...}` is the internal constructor taking the hash function.
// =====================================================================================================================
using Xunit;
using Oxia.Client.Internal;

namespace Oxia.Client.Tests.Internal;

public class ShardStrategyImplTests
{
    [Fact]
    public void TestShardStrategy()
    {
        var shardStrategy = new ShardStrategy(static _ => 2);
        var predicate = shardStrategy.Get("foo");

        (uint MinInclusive, uint MaxInclusive, bool Match)[] items =
        [
            (1, 3, true),
            (2, 3, true),
            (1, 2, true),
            (1, 1, false),
            (3, 3, false),
        ];

        foreach (var item in items)
        {
            var shard = new Shard(0, "", new HashRange(item.MinInclusive, item.MaxInclusive));
            Assert.Equal(item.Match, predicate(shard));
        }
    }
}

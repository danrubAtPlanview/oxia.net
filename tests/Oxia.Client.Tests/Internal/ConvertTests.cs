// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/internal/convert_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests: 1, ported: 1, skipped: 0
//   Intent   : toShard converts a wire ShardAssignment into a Shard, and rejects one whose hash range boundary is unset.
//   Adaptations: (Shard, error) is `ShardConvert.TryToShard(..., out Shard, out Exception?)`; `assert.AnError` is "any error".
// =====================================================================================================================
using Xunit;
using Oxia.Client.Internal;
using Oxia.Common.Proto;

namespace Oxia.Client.Tests.Internal;

public class ConvertTests
{
    [Fact]
    public void TestToShard()
    {
        (ShardAssignment Assignment, Shard Shard, bool Err)[] items =
        [
            (new ShardAssignment
            {
                Shard = 1,
                Leader = "leader:1234",
                Int32HashRange = new Int32HashRange { MinHashInclusive = 1, MaxHashInclusive = 2 },
            }, new Shard(1, "leader:1234", ShardConvert.MakeHashRange(1, 2)), false),
            // A ShardAssignment whose hash range boundary is unset, as a version-skewed or misbehaving server can send: it must be
            // rejected rather than crash the client.
            (new ShardAssignment { Shard = 2, Leader = "leader:1234" }, default, true),
        ];

        foreach (var item in items)
        {
            bool ok = ShardConvert.TryToShard(item.Assignment, out var result, out var err);
            if (item.Err)
            {
                Assert.False(ok);
                Assert.NotNull(err);
                continue;
            }

            Assert.True(ok);
            Assert.Null(err);
            Assert.Equal(item.Shard, result);
        }
    }
}

// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/internal/dizzy_shard_manager_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests: 2, ported: 2, skipped: 0
//   Intent   : the dizzy shard manager sends every request to the lookup address; it fails to start like the regular one.
//   Adaptations: the Go mock rpc providers are FakeRpcProvider subclasses (Fakes/FakeRpc.Extra.cs); `sm.Close()` is Dispose(); a
//                failed constructor throws instead of returning (nil, err).  Logic verified against fakes only (needs
//                component B: WaitGroup, Retry, BackOff).
// =====================================================================================================================
using Xunit;
using Oxia.Client.Internal;
using Oxia.Client.Tests.Internal.Fakes;
using Oxia.Common.Constant;

namespace Oxia.Client.Tests.Internal;

public class DizzyShardManagerTests
{
    // Every request goes to the lookup address, whatever the shard leader.
    [Fact]
    public async Task TestDizzyShardManagerLeader()
    {
        var sm = await DizzyShardManager.CreateAsync(ShardStrategy.New(), new SingleAssignmentsRpcProvider(), "lookup:6648",
            "default", TimeSpan.FromSeconds(10), null);
        try
        {
            var shards = sm.GetAll();
            Assert.Single(shards);
            Assert.Equal("lookup:6648", sm.Leader(shards[0]));
        }
        finally
        {
            sm.Dispose();
        }
    }

    [Fact]
    public async Task TestNewDizzyShardManagerFailsWithoutAssignments()
    {
        var provider = new FailingAssignmentsRpcProvider(OxiaErrors.NamespaceNotFound);
        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
            await DizzyShardManager.CreateAsync(ShardStrategy.New(), provider, "lookup:6648", "default", TimeSpan.FromSeconds(10), null));
        Assert.True(OxiaErrors.Is(ex, OxiaErrors.NamespaceNotFound));
    }
}

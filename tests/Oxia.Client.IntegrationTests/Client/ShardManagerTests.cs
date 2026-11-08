// =====================================================================================================================
// PORT NOTE
//   Upstream : tests/client/shard_manager_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Smoke test that the shard manager (shard assignments stream + routing) works against a standalone server.
//   Go tests: 1   ported: 1   skipped: 0
//   Divergences: none (Go `defer` closes are `await using`).
// =====================================================================================================================
using Oxia.TestSupport;
using Xunit;
using static Oxia.Client.IntegrationTests.TestHelpers;

namespace Oxia.Client.IntegrationTests.Client;

public class ShardManagerTests(OxiaServerFixture oxia)
{
    [Fact]
    public async Task TestShardManager_WithStandalone()
    {
        await using var standaloneServer = await oxia.StartAsync();

        // Verify the standalone server is accessible by performing a basic client operation
        await using var client = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr);

        await client.PutAsync(CancellationToken.None, "/test-key", B("test-value"));

        var (_, value, _) = await client.GetAsync(CancellationToken.None, "/test-key");
        Assert.Equal(B("test-value"), value);
    }
}

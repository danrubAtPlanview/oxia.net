// =====================================================================================================================
// PORT NOTE
//   Upstream : tests/client/session_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Regression test for leaked ephemeral keys: a client keeps writing ephemeral records (1000/s for 40 s) while its
//              session keep-alive ticker (16 s) is slower than the session timeout (10 s), so the server expires the session while
//              puts are still in flight; after the client closes, no ephemeral record may remain.
//   Go tests: 1   ported: 1   skipped: 0
//   Divergences:
//     * golang.org/x/time/rate.Limiter -> TestHelpers' RateLimiter (token bucket, same rate and burst).
//     * `_ = client.Put(...)` (the result channel is dropped) is `_ = client.PutAsync(...).AsTask()` so that the pooled completion is
//       consumed.
//     * Runs ~50 s (timings are the Go ones).
// =====================================================================================================================
using System.Diagnostics;
using Oxia.TestSupport;
using Xunit;
using static Oxia.Client.IntegrationTests.TestHelpers;

namespace Oxia.Client.IntegrationTests.Client;

public class SessionTests(OxiaServerFixture oxia)
{
    [Fact]
    public async Task TestSessionEphemeralKeysLeak()
    {
        await using var standaloneServer = await oxia.StartAsync();

        var client = await OxiaClient.CreateAsyncClientAsync(standaloneServer.ServiceAddr,
            // force the server cleanup the session to make the race-condition
            OxiaOptions.WithSessionKeepAliveTicker(TimeSpan.FromSeconds(16)),
            OxiaOptions.WithSessionTimeout(TimeSpan.FromSeconds(10)));

        var after = Stopwatch.StartNew();
        var limiter = new RateLimiter(1000, 1000);
        for (var i = 0; after.Elapsed < TimeSpan.FromSeconds(40); i++)
        {
            await limiter.WaitAsync();
            _ = client.PutAsync($"/session-leak/{i}", Array.Empty<byte>(), OxiaOptions.Ephemeral()).AsTask();
        }

        await client.CloseAsync();

        var syncClient = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr);

        await TestUtil.Eventually(async () =>
        {
            var keys = await syncClient.ListAsync(CancellationToken.None, "/session-leak/", "/session-leak//"); //nolint:staticcheck
            Assert.Empty(keys);
            return true;
        }, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(1));
    }
}

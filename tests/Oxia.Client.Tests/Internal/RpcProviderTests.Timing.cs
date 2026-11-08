// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/internal/rpc_provider_test.go  -- TestExecuteWithRetryWakesUpOnShardMapChange (moved here VERBATIM from RpcProviderTests.cs)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The test asserts that a retry wakes up as soon as the shard map changes: it runs under a 40 ms context while the backoff
//              alone would wait at least 50 ms.  The budget was tuned for Go, whose first call carries no JIT; in a .NET process running
//              ~1800 tests in parallel (and, in Debug, cold JIT) the 40 ms can be spent before the retry loop runs.  The body and the
//              assertion are unchanged; only its home is: a class in the non-parallel collection, so the budget is not eaten by other
//              tests.
// =====================================================================================================================
using Oxia.Client.Internal;
using Oxia.Common.Constant;
using Xunit;

namespace Oxia.Client.Tests.Internal;

[Collection("R1 serial")]
public class RpcProviderTimingTests
{
    [Fact]
    public async Task TestExecuteWithRetryWakesUpOnShardMapChange()
    {
        // Without the shard map change, the retry would wait at least 50 ms
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(40));

        var changed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var timer = new ShardMapTimer();
        int attempts = 0;
        int result = await RpcProvider.ExecuteWithRetryTimer(cts.Token, timer, hint =>
        {
            timer.Changed = changed.Task;
            attempts++;
            if (attempts == 1)
            {
                // The shard map changes while the attempt is in progress
                changed.SetResult();
                throw OxiaErrors.ToThrowable(OxiaErrors.NodeIsNotLeader);
            }

            return ValueTask.FromResult(attempts);
        });
        Assert.Equal(2, result);
    }
}

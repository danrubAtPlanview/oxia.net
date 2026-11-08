// =====================================================================================================================
// PORT NOTE (test helper, no Go counterpart)
//   Upstream : testify's assert.Eventually / require.Eventually and the `select { case <-ch: case <-time.After(...) }`
//              idiom used throughout oxia/batch/*_test.go and oxia/internal/batch/*_test.go.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Small helpers local to the batching tests.  DESIGN.md names `TestUtil.Eventually` in Oxia.TestSupport
//              (component T); until it exists these live here so the component is self-contained.
// =====================================================================================================================
using Xunit;

namespace Oxia.Client.Tests.Batch;

internal static class BatchTestUtil
{
    /// <summary>require.Eventually(t, cond, waitFor, tick).</summary>
    public static async Task Eventually(Func<bool> condition, TimeSpan waitFor, TimeSpan tick, string? message = null)
    {
        var deadline = DateTime.UtcNow + waitFor;
        while (true)
        {
            if (condition()) return;
            if (DateTime.UtcNow >= deadline)
            {
                Assert.Fail(message ?? "Condition never satisfied");
            }
            await Task.Delay(tick);
        }
    }

    /// <summary>Waits for the task, failing the test after <paramref name="timeout"/> (select + time.After in Go).</summary>
    public static async Task<T> Receive<T>(Task<T> task, TimeSpan timeout, string message)
    {
        if (await Task.WhenAny(task, Task.Delay(timeout)) != task)
        {
            Assert.Fail(message);
        }
        return await task;
    }

    public static async Task Receive(Task task, TimeSpan timeout, string message)
    {
        if (await Task.WhenAny(task, Task.Delay(timeout)) != task)
        {
            Assert.Fail(message);
        }
        await task;
    }

    /// <summary>True if the task completes within the timeout.</summary>
    public static async Task<bool> CompletesWithin(Task task, TimeSpan timeout) =>
        await Task.WhenAny(task, Task.Delay(timeout)) == task;
}

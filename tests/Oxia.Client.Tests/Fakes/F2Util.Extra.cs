// =====================================================================================================================
// PORT NOTE (test support of component F2 -- clients & admin)
//   Upstream : oxia/async_client_impl_test.go, async_client_reroute_test.go, sync_client_impl_test.go,
//              admin_client_impl_test.go (the helpers/fakes declared next to the tests)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Small helpers shared by the F2 tests.  All F2 fakes live in namespace Oxia.Client.Tests.F2: the Go tests
//              share unexported helpers across the test files of the package (staticShardManager, sessionRequestsServer, ...)
//              and other components port the same names; keeping F2's copies in their own namespace avoids duplicate-type
//              clashes when the branches are merged.
// =====================================================================================================================
using System.Threading.Channels;

namespace Oxia.Client.Tests.F2;

internal static class F2Util
{
    /// <summary>testify's assert.Eventually / require.Eventually.</summary>
    public static async Task EventuallyAsync(Func<bool> condition, TimeSpan timeout, TimeSpan tick, string message = "condition never became true")
    {
        var deadline = DateTime.UtcNow + timeout;
        while (true)
        {
            if (condition()) return;
            if (DateTime.UtcNow >= deadline) throw new Xunit.Sdk.XunitException("Eventually: " + message);
            await Task.Delay(tick);
        }
    }

    /// <summary>Reads the next item or fails after the timeout (a Go `select { case x := &lt;-ch: case &lt;-time.After(d): }`).</summary>
    public static async Task<T> ReadAsync<T>(ChannelReader<T> reader, TimeSpan? timeout = null)
    {
        using var cts = new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(10));
        try
        {
            return await reader.ReadAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            throw new Xunit.Sdk.XunitException("timed out waiting for a value");
        }
    }

    /// <summary>Waits for a task, failing the test if it takes longer than the timeout.</summary>
    public static async Task<T> WithTimeout<T>(Task<T> task, TimeSpan? timeout = null)
    {
        var limit = timeout ?? TimeSpan.FromSeconds(10);
        var finished = await Task.WhenAny(task, Task.Delay(limit));
        if (finished != task) throw new Xunit.Sdk.XunitException("timed out");
        return await task;
    }

    public static async Task WithTimeout(Task task, TimeSpan? timeout = null)
    {
        var limit = timeout ?? TimeSpan.FromSeconds(10);
        var finished = await Task.WhenAny(task, Task.Delay(limit));
        if (finished != task) throw new Xunit.Sdk.XunitException("timed out");
        await task;
    }

    /// <summary>Go's `ctx, cancel := context.WithDeadline(ctx, time.Now())`: an already expired context.</summary>
    public static CancellationTokenSource Expired()
    {
        var cts = new CancellationTokenSource();
        cts.Cancel();
        return cts;
    }
}

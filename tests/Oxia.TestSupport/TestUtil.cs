// =====================================================================================================================
// PORT NOTE (test infrastructure)
//   Upstream : github.com/stretchr/testify  assert.Eventually / assert.EventuallyWithT / assert.Eventually semantics, plus the
//              Go idioms used by the tests: `select { case v := <-ch: ... case <-time.After(d): ... }`, `v, ok := <-ch`
//              on a possibly closed channel, `assert.Empty(t, ch)`, `assert.ErrorIs(t, err, target)`, random test data.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246 (the tests that use these idioms)
//   Intent   : Keep the ported tests line-by-line recognisable.  testify's Eventually semantics are reproduced exactly:
//              the condition is first evaluated after ONE tick (not immediately), never concurrently with itself, and the
//              wait ends when `waitFor` elapses even if the condition is still running.
//   Divergences: A failing condition assertion (xUnit exception) propagates immediately (testify also fails the test at once
//              but keeps polling; the test result is the same).  Channel receives have a safety timeout (default 60 s) so a
//              bug in the implementation fails a test instead of hanging the run.
// =====================================================================================================================
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Channels;
using Oxia.Common.Constant;
using Xunit;

namespace Oxia.TestSupport;

/// <summary>testify / Go-idiom helpers shared by the unit and integration tests.</summary>
public static class TestUtil
{
    /// <summary>Safety net for receives that Go does without timeout (`&lt;-ch`): a hung test fails instead of blocking the run.</summary>
    public static readonly TimeSpan DefaultReceiveTimeout = TimeSpan.FromSeconds(60);

    // ------------------------------------------------------------------------------------------------------------------
    // assert.Eventually / assert.EventuallyWithT
    // ------------------------------------------------------------------------------------------------------------------

    /// <summary>testify <c>assert.Eventually(t, condition, waitFor, tick)</c>; fails the test with "Condition never satisfied".</summary>
    public static Task Eventually(Func<bool> condition, TimeSpan waitFor, TimeSpan tick, string? message = null) =>
        Eventually(() => Task.FromResult(condition()), waitFor, tick, message);

    /// <summary>Async-condition variant of <see cref="Eventually(Func{bool},TimeSpan,TimeSpan,string)"/>.</summary>
    public static async Task Eventually(Func<Task<bool>> condition, TimeSpan waitFor, TimeSpan tick, string? message = null)
    {
        var timer = Task.Delay(waitFor);
        while (true)
        {
            if (await Task.WhenAny(Task.Delay(tick), timer).ConfigureAwait(false) == timer) break;
            var running = Task.Run(condition);
            if (await Task.WhenAny(running, timer).ConfigureAwait(false) == timer)
            {
                _ = running.ContinueWith(static t => _ = t.Exception, TaskContinuationOptions.OnlyOnFaulted);
                break;
            }

            if (await running.ConfigureAwait(false)) return;
        }

        Assert.Fail("Condition never satisfied" + (message is null ? "" : ": " + message));
    }

    /// <summary>
    /// testify <c>assert.EventuallyWithT</c>: the condition asserts (throws, e.g. xUnit <c>Assert.*</c>) instead of returning a bool;
    /// the wait succeeds on the first tick where it does not throw, otherwise fails with the last assertion error.
    /// </summary>
    public static Task EventuallyWithT(Action condition, TimeSpan waitFor, TimeSpan tick, string? message = null) =>
        EventuallyWithT(() => { condition(); return Task.CompletedTask; }, waitFor, tick, message);

    public static async Task EventuallyWithT(Func<Task> condition, TimeSpan waitFor, TimeSpan tick, string? message = null)
    {
        var timer = Task.Delay(waitFor);
        Exception? last = null;
        while (true)
        {
            if (await Task.WhenAny(Task.Delay(tick), timer).ConfigureAwait(false) == timer) break;
            var running = Task.Run(condition);
            if (await Task.WhenAny(running, timer).ConfigureAwait(false) == timer)
            {
                _ = running.ContinueWith(static t => _ = t.Exception, TaskContinuationOptions.OnlyOnFaulted);
                break;
            }

            try
            {
                await running.ConfigureAwait(false);
                return;
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                last = e;
            }
        }

        Assert.Fail("Condition never satisfied" + (message is null ? "" : ": " + message) + (last is null ? "" : Environment.NewLine + last.Message));
    }

    /// <summary>Polls without the testify "first tick" delay until true; returns false on timeout (no assertion).</summary>
    public static async Task<bool> WaitUntil(Func<bool> condition, TimeSpan timeout, TimeSpan? tick = null)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (true)
        {
            if (condition()) return true;
            if (DateTime.UtcNow >= deadline) return false;
            await Task.Delay(tick ?? TimeSpan.FromMilliseconds(10)).ConfigureAwait(false);
        }
    }

    // ------------------------------------------------------------------------------------------------------------------
    // Random data
    // ------------------------------------------------------------------------------------------------------------------

    private const string Alphanumeric = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    /// <summary>A random alphanumeric string.</summary>
    public static string RandomString(int length, string alphabet = Alphanumeric)
    {
        if (length <= 0) return "";
        return string.Create(length, alphabet, static (span, a) =>
        {
            for (var i = 0; i < span.Length; i++) span[i] = a[Random.Shared.Next(a.Length)];
        });
    }

    public static byte[] RandomBytes(int length)
    {
        var b = new byte[length];
        Random.Shared.NextBytes(b);
        return b;
    }

    /// <summary>A key prefix unique to the call (<c>test-&lt;guid&gt;/</c>) to isolate tests that share a server.</summary>
    public static string UniquePrefix() => "test-" + Guid.NewGuid().ToString("N") + "/";

    public static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    public static string Utf8(byte[]? b) => b is null ? "" : Encoding.UTF8.GetString(b);

    /// <summary>A currently free loopback TCP port (bind-and-release; racy by nature, fine for tests).</summary>
    public static int FreeTcpPort()
    {
        using var l = new TcpListener(IPAddress.Loopback, 0);
        l.Start();
        return ((IPEndPoint)l.LocalEndpoint).Port;
    }

    // ------------------------------------------------------------------------------------------------------------------
    // Errors: Go `assert.ErrorIs(t, err, target)` / `require.NoError` for thrown (sync-client) failures
    // ------------------------------------------------------------------------------------------------------------------

    /// <summary>Go <c>errors.Is(err, target)</c> through <see cref="OxiaErrors.Is"/>, as an assertion.</summary>
    public static void AssertErrorIs(Exception? err, Exception target, string? message = null)
    {
        if (!OxiaErrors.Is(err, target))
        {
            Assert.Fail($"expected an error matching \"{target.Message}\"{(message is null ? "" : " (" + message + ")")} but got: " +
                        (err is null ? "<no error>" : err.ToString()));
        }
    }

    /// <summary>Runs <paramref name="action"/> and returns what it threw (null when it succeeded); never swallows a cancellation of the test.</summary>
    public static async Task<Exception?> CatchAsync(Func<Task> action)
    {
        try
        {
            await action().ConfigureAwait(false);
            return null;
        }
        catch (Exception e)
        {
            return e;
        }
    }

    /// <summary>Synchronous variant of <see cref="CatchAsync"/>.</summary>
    public static Exception? Catch(Action action)
    {
        try
        {
            action();
            return null;
        }
        catch (Exception e)
        {
            return e;
        }
    }

    // ------------------------------------------------------------------------------------------------------------------
    // Go channels
    // ------------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Go <c>v, ok := &lt;-ch</c>: waits for the next value; <c>Ok == false</c> (and a default value) when the channel is completed
    /// and drained.  Fails the test after <paramref name="timeout"/> (default 60 s) to avoid hanging forever.
    /// </summary>
    public static async ValueTask<(bool Ok, T? Value)> Receive<T>(this ChannelReader<T> reader, TimeSpan? timeout = null, CancellationToken ct = default)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(timeout ?? DefaultReceiveTimeout);
        try
        {
            while (await reader.WaitToReadAsync(cts.Token).ConfigureAwait(false))
            {
                if (reader.TryRead(out var value)) return (true, value);
            }

            return (false, default);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            Assert.Fail($"timed out after {(timeout ?? DefaultReceiveTimeout).TotalSeconds:0.#} s waiting to receive from the channel");
            throw;
        }
    }

    /// <summary>Go <c>x := &lt;-ch</c> (the zero value when the channel is closed).</summary>
    public static async ValueTask<T?> Recv<T>(this ChannelReader<T> reader, TimeSpan? timeout = null, CancellationToken ct = default) =>
        (await reader.Receive(timeout, ct).ConfigureAwait(false)).Value;

    /// <summary>
    /// Go <c>select { case v := &lt;-ch: ... case &lt;-time.After(d): ... }</c>: <c>TimedOut</c> when nothing arrived in time.
    /// <c>Closed</c> is true when the channel completed instead (a closed channel is always ready in Go).
    /// </summary>
    public static async ValueTask<(bool TimedOut, bool Closed, T? Value)> ReceiveWithin<T>(this ChannelReader<T> reader, TimeSpan timeout,
        CancellationToken ct = default)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(timeout);
        try
        {
            while (await reader.WaitToReadAsync(cts.Token).ConfigureAwait(false))
            {
                if (reader.TryRead(out var value)) return (false, false, value);
            }

            return (false, true, default);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            return (true, false, default);
        }
    }

    /// <summary>
    /// Go <c>select { case v, ok := &lt;-ch: ... default: ... }</c>: a non-blocking receive.  <c>Ready == false</c> when nothing is
    /// buffered and the channel is still open; when the channel is closed <c>Ready</c> is true with <c>Ok == false</c>.
    /// </summary>
    public static (bool Ready, bool Ok, T? Value) TryReceive<T>(this ChannelReader<T> reader)
    {
        if (reader.TryRead(out var value)) return (true, true, value);
        if (reader.Completion.IsCompleted && !reader.TryPeekAny()) return (true, false, default);
        return (false, false, default);
    }

    private static bool TryPeekAny<T>(this ChannelReader<T> reader) => reader.CanPeek && reader.TryPeek(out _);

    /// <summary>Go <c>assert.Empty(t, ch)</c> / <c>len(ch) == 0</c>: nothing is buffered in the channel.</summary>
    public static bool IsEmpty<T>(this ChannelReader<T> reader)
    {
        if (reader.CanCount) return reader.Count == 0;
        return !(reader.CanPeek && reader.TryPeek(out _));
    }

    /// <summary>Go <c>for v := range ch</c>: everything until the channel completes (60 s safety timeout per item).</summary>
    public static async Task<List<T>> DrainAsync<T>(this ChannelReader<T> reader, CancellationToken ct = default)
    {
        var all = new List<T>();
        while (true)
        {
            var (ok, value) = await reader.Receive(null, ct).ConfigureAwait(false);
            if (!ok) return all;
            all.Add(value!);
        }
    }

    /// <summary>Awaits <paramref name="task"/> for at most <paramref name="timeout"/> (Go: select with time.After); false on timeout.</summary>
    public static async Task<bool> CompletesWithin(this Task task, TimeSpan timeout)
    {
        var timer = Task.Delay(timeout);
        if (await Task.WhenAny(task, timer).ConfigureAwait(false) != task) return false;
        await task.ConfigureAwait(false);
        return true;
    }

    /// <summary>Helper to attach the caller expression to an assertion message.</summary>
    public static void Fail(string message, [CallerMemberName] string? caller = null) => Assert.Fail(caller is null ? message : caller + ": " + message);
}

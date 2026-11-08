// =====================================================================================================================
// PORT NOTE
//   Upstream : tests/client/async_client_test.go (newKey, shardOf), tests/client/session_test.go (golang.org/x/time/rate limiter),
//              testify helpers (assert.ElementsMatch, assert.Equal on result structs)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The small helpers the Go integration tests share, kept in one place so that the ported test bodies stay close to
//              the Go text.
//   Divergences: assert.Equal(oxia.GetResult{...}, got) is split into field assertions (the C# result types are structs holding
//              byte[] / exceptions, whose default Equals is not structural); the error is compared by reference exactly like Go
//              compares the sentinel error values.
// =====================================================================================================================
using System.Diagnostics;
using System.Text;
using Oxia.Common.Constant;
using Xunit;

namespace Oxia.Client.IntegrationTests;

internal static class TestHelpers
{
    /// <summary>Go: <c>newKey()</c> = <c>fmt.Sprintf("/my-key-%d", time.Now().Nanosecond())</c> (nanoseconds within the second).</summary>
    public static string NewKey() => $"/my-key-{(DateTime.UtcNow.Ticks % TimeSpan.TicksPerSecond) * 100}";

    /// <summary>Go: <c>[]byte("...")</c>.</summary>
    public static byte[] B(string s) => Encoding.UTF8.GetBytes(s);

    /// <summary>Go: <c>t.Context()</c>.</summary>
    public static CancellationToken TestCtx => TestContext.Current.CancellationToken;

    /// <summary>Go: <c>assert.Equal(t, oxia.GetResult{Key, Value, Version, Err}, actual)</c>.</summary>
    public static void AssertGetResult(string key, byte[]? value, RecordVersion version, Exception? err, GetResult actual)
    {
        Assert.Equal(key, actual.Key);
        if (value is null) Assert.Null(actual.Value);
        else Assert.Equal(value, actual.Value);
        Assert.Equal(version, actual.Version);
        Assert.Same(err, actual.Err);
    }

    /// <summary>Go: <c>assert.Equal(t, oxia.ListResult{Keys: keys}, actual)</c>.</summary>
    public static void AssertListResult(IReadOnlyList<string> keys, ListResult actual)
    {
        Assert.Null(actual.Err);
        Assert.Equal(keys, actual.Keys!);
    }

    /// <summary>Go: <c>assert.ElementsMatch</c>.</summary>
    public static void AssertElementsMatch(IEnumerable<string> expected, IEnumerable<string> actual) =>
        Assert.Equal(expected.Order(StringComparer.Ordinal).ToList(), actual.Order(StringComparer.Ordinal).ToList());

    /// <summary>Go: <c>assert.ErrorIs(t, err, target)</c> on an exception thrown by the sync client.</summary>
    public static void AssertErrorIs(Exception? thrown, Exception sentinel, string? message = null)
    {
        Assert.NotNull(thrown);
        Assert.True(OxiaErrors.Is(thrown, sentinel),
            $"expected \"{sentinel.Message}\" in the error chain{(message is null ? "" : " (" + message + ")")}, got: {thrown}");
    }

    /// <summary>Go: <c>assert.NoError(t, err)</c> on a failure reported as a value (async client).</summary>
    public static void AssertNoError(Exception? err) => Assert.True(err is null, err?.ToString());
}

/// <summary>Go: <c>golang.org/x/time/rate.NewLimiter(rate.Limit(r), burst)</c> -- a token bucket whose <see cref="WaitAsync"/> is <c>limiter.Wait</c>.</summary>
internal sealed class RateLimiter(double ratePerSecond, int burst)
{
    private readonly object _lock = new();
    private double _tokens = burst;
    private long _last = Stopwatch.GetTimestamp();

    public async Task WaitAsync(CancellationToken ct = default)
    {
        while (true)
        {
            TimeSpan delay;
            lock (_lock)
            {
                var now = Stopwatch.GetTimestamp();
                _tokens = Math.Min(burst, _tokens + (now - _last) / (double)Stopwatch.Frequency * ratePerSecond);
                _last = now;
                if (_tokens >= 1)
                {
                    _tokens -= 1;
                    return;
                }

                delay = TimeSpan.FromSeconds((1 - _tokens) / ratePerSecond);
            }

            await Task.Delay(delay, ct).ConfigureAwait(false);
        }
    }
}

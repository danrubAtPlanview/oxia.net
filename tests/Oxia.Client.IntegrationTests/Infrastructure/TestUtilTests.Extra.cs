// =====================================================================================================================
// PORT NOTE
//   Upstream : none -- extra tests (DESIGN.md "Test porting protocol" 6) of tests/Oxia.TestSupport/TestUtil.cs, the testify / Go
//              channel helpers the ported tests rely on.  No server needed.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Pin down the semantics that matter for fidelity: testify's Eventually evaluates its condition for the first time
//              after ONE tick, never concurrently with itself and gives up at waitFor; Go channel receive idioms on ChannelReader<T>.
//   Divergences: n/a.
// =====================================================================================================================
using System.Diagnostics;
using System.Threading.Channels;
using Oxia.TestSupport;
using Xunit;
using Xunit.Sdk;

namespace Oxia.Client.IntegrationTests.Infrastructure;

public class TestUtilTests
{
    [Fact]
    public async Task Eventually_FirstEvaluationHappensAfterOneTick()
    {
        var sw = Stopwatch.StartNew();
        var firstCallAt = TimeSpan.Zero;
        await TestUtil.Eventually(() =>
        {
            if (firstCallAt == TimeSpan.Zero) firstCallAt = sw.Elapsed;
            return true;
        }, TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(200));
        Assert.True(firstCallAt >= TimeSpan.FromMilliseconds(150), $"first evaluation after {firstCallAt.TotalMilliseconds} ms");
    }

    [Fact]
    public async Task Eventually_RetriesUntilTrue_AndNeverOverlaps()
    {
        var calls = 0;
        var running = 0;
        var overlapped = false;
        await TestUtil.Eventually(async () =>
        {
            if (Interlocked.Increment(ref running) > 1) overlapped = true;
            await Task.Delay(30); // longer than the tick: testify does not start the next evaluation until this one returned
            Interlocked.Decrement(ref running);
            return Interlocked.Increment(ref calls) == 4;
        }, TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(10));
        Assert.Equal(4, calls);
        Assert.False(overlapped);
    }

    [Fact]
    public async Task Eventually_FailsWhenTheConditionNeverHolds()
    {
        var e = await Assert.ThrowsAnyAsync<XunitException>(() =>
            TestUtil.Eventually(() => false, TimeSpan.FromMilliseconds(300), TimeSpan.FromMilliseconds(20)));
        Assert.Contains("Condition never satisfied", e.Message);
    }

    [Fact]
    public async Task Eventually_GivesUpAtTheDeadlineEvenIfTheConditionIsStillRunning()
    {
        var sw = Stopwatch.StartNew();
        await Assert.ThrowsAnyAsync<XunitException>(() =>
            TestUtil.Eventually(async () =>
            {
                await Task.Delay(TimeSpan.FromSeconds(30));
                return true;
            }, TimeSpan.FromMilliseconds(300), TimeSpan.FromMilliseconds(20)));
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task EventuallyWithT_SucceedsOnTheFirstTickWithoutAssertionFailure()
    {
        var attempts = 0;
        await TestUtil.EventuallyWithT(() => Assert.True(++attempts >= 3), TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(10));
        Assert.Equal(3, attempts);
    }

    [Fact]
    public async Task EventuallyWithT_ReportsTheLastAssertionError()
    {
        var e = await Assert.ThrowsAnyAsync<XunitException>(() =>
            TestUtil.EventuallyWithT(() => Assert.Equal(1, 2), TimeSpan.FromMilliseconds(200), TimeSpan.FromMilliseconds(10)));
        Assert.Contains("Condition never satisfied", e.Message);
        Assert.Contains("Assert.Equal() Failure", e.Message);
    }

    [Fact]
    public async Task Channels_ReceiveIdioms()
    {
        var ch = Channel.CreateUnbounded<int>();

        // select { case v := <-ch: ... case <-time.After(d): ... }
        var (timedOut, closed, _) = await ch.Reader.ReceiveWithin(TimeSpan.FromMilliseconds(50));
        Assert.True(timedOut);
        Assert.False(closed);

        // select { case v, ok := <-ch: ... default: ... }
        Assert.False(ch.Reader.TryReceive().Ready);
        Assert.True(ch.Reader.IsEmpty());

        ch.Writer.TryWrite(7);
        Assert.False(ch.Reader.IsEmpty());
        var (ready, ok, value) = ch.Reader.TryReceive();
        Assert.True(ready);
        Assert.True(ok);
        Assert.Equal(7, value);

        ch.Writer.TryWrite(8);
        ch.Writer.TryComplete();
        Assert.Equal((true, 8), await ch.Reader.Receive());

        // a closed channel is always ready and yields the zero value
        var (okAfterClose, zero) = await ch.Reader.Receive();
        Assert.False(okAfterClose);
        Assert.Equal(0, zero);
        var (readyAfterClose, okAfterClose2, _) = ch.Reader.TryReceive();
        Assert.True(readyAfterClose);
        Assert.False(okAfterClose2);
        var (timedOut2, closed2, _) = await ch.Reader.ReceiveWithin(TimeSpan.FromSeconds(1));
        Assert.False(timedOut2);
        Assert.True(closed2);
    }

    [Fact]
    public async Task Channels_DrainCollectsEverythingUntilClose()
    {
        var ch = Channel.CreateUnbounded<string>();
        foreach (var s in new[] { "a", "b", "c" }) ch.Writer.TryWrite(s);
        ch.Writer.TryComplete();
        Assert.Equal(["a", "b", "c"], await ch.Reader.DrainAsync());
    }

    [Fact]
    public void RandomHelpers()
    {
        Assert.Equal(16, TestUtil.RandomString(16).Length);
        Assert.NotEqual(TestUtil.RandomString(32), TestUtil.RandomString(32));
        Assert.Equal("", TestUtil.RandomString(0));
        Assert.Equal(10, TestUtil.RandomBytes(10).Length);
        Assert.StartsWith("test-", TestUtil.UniquePrefix());
        Assert.EndsWith("/", TestUtil.UniquePrefix());
        Assert.NotEqual(TestUtil.UniquePrefix(), TestUtil.UniquePrefix());
        Assert.True(TestUtil.FreeTcpPort() > 0);
    }
}

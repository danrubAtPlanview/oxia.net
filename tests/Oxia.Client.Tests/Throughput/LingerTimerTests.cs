// =====================================================================================================================
// PORT NOTE (wave 2 Z2 tests -- no Go counterpart)
//   Upstream : none (oxia/batch/batcher.go uses a runtime timer; src/Oxia.Client/Batch/BatcherTimer.cs replaces System.Threading.Timer)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The wake-up service of the batchers (high resolution timer on Windows): a wake-up never comes before its deadline, comes
//              soon after it (the whole point: a 5 ms deadline must not become 15 ms), re-arming supersedes, cancel drops, many timers
//              fire in deadline order, and a throwing callback does not kill the service.
// =====================================================================================================================
using System.Diagnostics;
using Oxia.Client.Batch;
using Xunit;
using Xunit.Sdk;

namespace Oxia.Client.Tests.Throughput;

public class LingerTimerTests
{
    private static long Ms(double ms) => (long)(ms * Stopwatch.Frequency / 1000.0);

    private sealed class Probe
    {
        public readonly SemaphoreSlim Fired = new(0);
        public long FiredAt;
        public int Count;
        public readonly LingerTimer Timer;

        public Probe(LingerTimerService service)
        {
            Timer = new LingerTimer(static s =>
            {
                var p = (Probe)s!;
                p.FiredAt = Stopwatch.GetTimestamp();
                Interlocked.Increment(ref p.Count);
                p.Fired.Release();
            }, this, service);
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void WakeUp_NeverBeforeTheDeadline_AndPromptlyAfter(bool highResolution)
    {
        if (highResolution && !OperatingSystem.IsWindows()) return; // the portable path is what runs elsewhere
        var service = new LingerTimerService(highResolution);
        var probe = new Probe(service);

        var late = new List<double>();
        foreach (double delayMs in new[] { 1.0, 2.0, 5.0, 5.0, 5.0, 10.0 })
        {
            long deadline = Stopwatch.GetTimestamp() + Ms(delayMs);
            probe.Timer.Arm(deadline);
            Assert.True(probe.Fired.Wait(TimeSpan.FromSeconds(5)));
            double lateMs = (probe.FiredAt - deadline) * 1000.0 / Stopwatch.Frequency;
            Assert.True(lateMs >= -0.05, $"fired {-lateMs:0.000} ms BEFORE the deadline");
            late.Add(lateMs);
        }

        if (highResolution)
        {
            // The 15.6 ms OS tick is what this service avoids; a loaded CI machine may add some jitter, hence the generous bound on the median.
            late.Sort();
            double median = late[late.Count / 2];
            Assert.True(median < 5.0, $"median lateness {median:0.000} ms: the high resolution timer is not in effect (all: {string.Join(", ", late.Select(l => l.ToString("0.00")))})");
            Assert.True(service.IsHighResolution);
        }
    }

    [Fact]
    public void Rearming_SupersedesTheEarlierArming()
    {
        var service = new LingerTimerService(false);
        var probe = new Probe(service);
        long start = Stopwatch.GetTimestamp();
        probe.Timer.Arm(start + Ms(20));
        probe.Timer.Arm(start + Ms(60));
        Assert.False(probe.Fired.Wait(40), "the superseded arming (20 ms) fired");
        Assert.True(probe.Fired.Wait(TimeSpan.FromSeconds(5)));
        Assert.True(probe.FiredAt - start >= Ms(59.9));
        Assert.False(probe.Fired.Wait(100), "fired twice");
        Assert.Equal(1, probe.Count);
    }

    [Fact]
    public void Cancel_DropsThePendingWakeUp()
    {
        var service = new LingerTimerService(false);
        var probe = new Probe(service);
        probe.Timer.Arm(Stopwatch.GetTimestamp() + Ms(30));
        probe.Timer.Cancel();
        Assert.False(probe.Fired.Wait(200));
        Assert.Equal(0, probe.Count);

        // and it can be armed again
        probe.Timer.Arm(Stopwatch.GetTimestamp() + Ms(1));
        Assert.True(probe.Fired.Wait(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public void ADeadlineInThePast_FiresRightAway()
    {
        var service = new LingerTimerService();
        var probe = new Probe(service);
        probe.Timer.Arm(Stopwatch.GetTimestamp() - Ms(100));
        Assert.True(probe.Fired.Wait(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public void ManyTimers_FireInDeadlineOrder_EvenWhenArmedOutOfOrder()
    {
        var service = new LingerTimerService();
        var order = new List<int>();
        var done = new CountdownEvent(8);
        var timers = new LingerTimer[8];
        for (int i = 0; i < timers.Length; i++)
        {
            timers[i] = new LingerTimer(static s =>
            {
                var (list, index, countdown) = ((List<int>, int, CountdownEvent))s!;
                lock (list) list.Add(index);
                countdown.Signal();
            }, (order, i, done), service);
        }

        long start = Stopwatch.GetTimestamp();
        int[] arming = [5, 1, 7, 3, 0, 6, 2, 4];
        foreach (int i in arming) timers[i].Arm(start + Ms(20 + 15 * i));

        Assert.True(done.Wait(TimeSpan.FromSeconds(10)));
        Assert.Equal([0, 1, 2, 3, 4, 5, 6, 7], order);
    }

    [Fact]
    public void AnEarlierDeadlineArmedWhileWaiting_WakesTheServiceUp()
    {
        var service = new LingerTimerService();
        var slow = new Probe(service);
        var fast = new Probe(service);
        slow.Timer.Arm(Stopwatch.GetTimestamp() + Ms(5000));
        Thread.Sleep(50); // the service is now parked until the 5 s deadline
        long deadline = Stopwatch.GetTimestamp() + Ms(10);
        fast.Timer.Arm(deadline);
        Assert.True(fast.Fired.Wait(TimeSpan.FromSeconds(2)), "the earlier deadline did not wake the service up");
        Assert.False(slow.Fired.Wait(0));
        slow.Timer.Cancel();
    }

    [Fact]
    public void AThrowingCallback_DoesNotKillTheService()
    {
        var service = new LingerTimerService();
        var bad = new LingerTimer(static _ => throw new InvalidOperationException("boom"), null, service);
        var probe = new Probe(service);
        bad.Arm(Stopwatch.GetTimestamp() + Ms(1));
        probe.Timer.Arm(Stopwatch.GetTimestamp() + Ms(20));
        Assert.True(probe.Fired.Wait(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public void ArmingFromManyThreads_FiresEveryTimerOnce()
    {
        var service = new LingerTimerService();
        const int n = 200;
        var probes = new Probe[n];
        for (int i = 0; i < n; i++) probes[i] = new Probe(service);
        Parallel.For(0, n, i => probes[i].Timer.Arm(Stopwatch.GetTimestamp() + Ms(1 + i % 7)));
        foreach (var p in probes) Assert.True(p.Fired.Wait(TimeSpan.FromSeconds(10)));
        Thread.Sleep(50);
        Assert.All(probes, p => Assert.Equal(1, p.Count));
        Assert.Equal(0, service.PendingCount);
    }
}

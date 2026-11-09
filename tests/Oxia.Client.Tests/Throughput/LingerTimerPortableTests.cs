// =====================================================================================================================
// PORT NOTE (Linux validation L tests -- no Go counterpart)
//   Upstream : none (oxia/batch/batcher.go uses a runtime timer; src/Oxia.Client/Batch/BatcherTimer.cs replaces System.Threading.Timer)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The PORTABLE path of the linger wake-up service (a timed Monitor.Wait on the service thread) is the one that runs on
//              Linux and macOS -- the AWS benchmark hosts -- while LingerTimerTests only asserts promptness for the Windows high
//              resolution timer (its `highResolution: true` case returns early elsewhere and the `false` case asserts only "never
//              early").  These tests pin what the portable path must deliver where the OS timers are fine:
//                * the process-wide service picks the portable path off Windows (no native timer is created);
//                * a wake-up for a 1 ms and a 5 ms linger (the default!) never comes early and its median lateness stays far below the
//                  15.6 ms tick the Windows path exists to avoid;
//                * end to end through a real BatcherImpl and the shared service, a lone call completes at linger + a few ms.
//              The lateness bounds are medians over many wake-ups, generous for a loaded CI machine (a healthy Linux host measures
//              ~0.1-1.1 ms: Monitor.Wait takes whole milliseconds and the service rounds the wait UP so it never fires early).
//              On Windows the portable path is deliberately the slow one (the OS tick), so the lateness assertions are skipped there;
//              the high resolution path has its own tests in LingerTimerTests.
// =====================================================================================================================
using System.Diagnostics;
using Oxia.Client.Batch;
using Xunit;

namespace Oxia.Client.Tests.Throughput;

// Wall-clock assertions: run alone, like LingerTimerTests (the rest of the suite would otherwise stall the service thread).
[Collection("R1 serial")]
public class LingerTimerPortableTests(ITestOutputHelper output)
{
    private static long Ms(double ms) => (long)(ms * Stopwatch.Frequency / 1000.0);

    private sealed class Probe
    {
        public readonly SemaphoreSlim Fired = new(0);
        public long FiredAt;
        public readonly LingerTimer Timer;

        public Probe(LingerTimerService service)
        {
            Timer = new LingerTimer(static s =>
            {
                var p = (Probe)s!;
                p.FiredAt = Stopwatch.GetTimestamp();
                p.Fired.Release();
            }, this, service);
        }
    }

    [Fact]
    public void OffWindows_TheServiceUsesThePortablePath_AndNoNativeTimer()
    {
        Assert.SkipWhen(OperatingSystem.IsWindows(), "Windows uses the high resolution waitable timer (LingerTimerTests)");

        // an explicitly requested high resolution timer is ignored where there is none
        foreach (var service in new[] { new LingerTimerService(), new LingerTimerService(true), new LingerTimerService(false) })
        {
            var probe = new Probe(service);
            probe.Timer.Arm(Stopwatch.GetTimestamp() + Ms(1));
            Assert.True(probe.Fired.Wait(TimeSpan.FromSeconds(10)));
            Assert.False(service.IsHighResolution);
        }

        // and so does the process-wide instance the batchers use
        var shared = new Probe(LingerTimerService.Shared);
        shared.Timer.Arm(Stopwatch.GetTimestamp() + Ms(1));
        Assert.True(shared.Fired.Wait(TimeSpan.FromSeconds(10)));
        Assert.False(LingerTimerService.Shared.IsHighResolution);
    }

    [Fact]
    public void TicksToMilliseconds_RoundsUp_AndNeverOverflows()
    {
        long perMs = Stopwatch.Frequency / 1000;
        Assert.Equal(1, LingerTimerService.TicksToMilliseconds(0));           // clamped: a zero wait would spin
        Assert.Equal(1, LingerTimerService.TicksToMilliseconds(1));           // rounded UP: never before the deadline
        Assert.Equal(1, LingerTimerService.TicksToMilliseconds(perMs));
        Assert.Equal(2, LingerTimerService.TicksToMilliseconds(perMs + 1));
        Assert.Equal(5, LingerTimerService.TicksToMilliseconds(5 * perMs));

        // Stopwatch.Frequency is 1e9 on Linux / macOS: `ticks * 1000` overflowed for waits beyond ~106 days (and the wait then collapsed to 1 ms)
        long oneYear = 365L * 24 * 3600 * Stopwatch.Frequency;
        Assert.Equal(int.MaxValue - 1, LingerTimerService.TicksToMilliseconds(oneYear));
        Assert.Equal(int.MaxValue - 1, LingerTimerService.TicksToMilliseconds(long.MaxValue / 2));
        Assert.Equal(int.MaxValue - 1, LingerTimerService.TicksToMilliseconds(long.MaxValue));
        long threeHours = 3L * 3600 * Stopwatch.Frequency;
        Assert.Equal(3 * 3600 * 1000, LingerTimerService.TicksToMilliseconds(threeHours));
    }

    [Theory]
    [InlineData(1.0)]
    [InlineData(5.0)]
    public void PortablePath_WakesUpPromptly_ForTheLingersOfTheBenchmark(double lingerMs)
    {
        Assert.SkipWhen(OperatingSystem.IsWindows(), "the portable path has the OS tick resolution on Windows by design");

        var service = new LingerTimerService();
        var probe = new Probe(service);
        var late = new List<double>();
        for (int i = 0; i < 60; i++)
        {
            long deadline = Stopwatch.GetTimestamp() + Ms(lingerMs);
            probe.Timer.Arm(deadline);
            Assert.True(probe.Fired.Wait(TimeSpan.FromSeconds(10)));
            double lateMs = (probe.FiredAt - deadline) * 1000.0 / Stopwatch.Frequency;
            Assert.True(lateMs >= -0.05, $"fired {-lateMs:0.000} ms BEFORE the deadline");
            late.Add(lateMs);
            Thread.Sleep(1); // idle between wake-ups, like a lightly loaded client: the service thread parks again
        }

        late.Sort();
        double median = late[late.Count / 2];
        output.WriteLine($"linger {lingerMs} ms: lateness min {late[0]:0.000} median {median:0.000} p90 {late[(int)(late.Count * 0.9)]:0.000} max {late[^1]:0.000} ms");
        Assert.True(median < 3.0, $"linger {lingerMs} ms: median lateness {median:0.000} ms (all: {string.Join(", ", late.Select(l => l.ToString("0.00")))})");
    }

    private sealed class ProbeBatch(Action onComplete) : IBatch
    {
        private int _size;
        public bool CanAdd(object call) => true;
        public void Add(object call) => _size++;
        public int Size => _size;
        public ValueTask CompleteAsync()
        {
            onComplete();
            return default;
        }
        public void Fail(Exception error) { }
    }

    [Theory]
    [InlineData(1)]
    [InlineData(5)]
    public async Task ALoneCall_IsSentAtLingerPlusAFewMilliseconds_ThroughTheSharedService(int lingerMs)
    {
        Assert.SkipWhen(OperatingSystem.IsWindows(), "Windows: covered by R1_BatcherLoopTests (1 ms) and the high resolution tests");

        TaskCompletionSource<long> completedAt = null!;
        var factory = new BatcherFactory { Linger = TimeSpan.FromMilliseconds(lingerMs), MaxRequestsPerBatch = 1000 };
        var batcher = factory.NewBatcher(CancellationToken.None, 0, "probe", () => new ProbeBatch(() => completedAt.TrySetResult(Stopwatch.GetTimestamp())));
        var latencies = new List<double>();
        try
        {
            for (int i = 0; i < 60; i++)
            {
                completedAt = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
                long start = Stopwatch.GetTimestamp();
                await batcher.AddAsync("call");
                long end = await completedAt.Task.WaitAsync(TimeSpan.FromSeconds(10));
                latencies.Add(Stopwatch.GetElapsedTime(start, end).TotalMilliseconds);
                await Task.Delay(2); // idle between calls, like a lightly loaded client
            }
        }
        finally
        {
            batcher.Dispose();
        }

        latencies.Sort();
        double median = latencies[latencies.Count / 2];
        output.WriteLine($"linger {lingerMs} ms: latency min {latencies[0]:0.000} median {median:0.000} p90 {latencies[(int)(latencies.Count * 0.9)]:0.000} max {latencies[^1]:0.000} ms");
        Assert.True(median >= lingerMs - 0.05, $"linger={lingerMs}ms: completed before the linger elapsed (median {median:F2} ms)");
        Assert.True(median <= lingerMs + 3.0, $"linger={lingerMs}ms: median latency {median:F2} ms (min {latencies[0]:F2}, max {latencies[^1]:F2})");
    }
}

// =====================================================================================================================
// PORT NOTE (independent-review regression/differential test; no Go test counterpart)
//   Upstream : oxia/batch/batcher.go, oxia/batch/batcher_factory.go (go process.DoWithLabels(... batcher.Run))
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Pins review findings R1-3 (the batcher loop must not inherit the ExecutionContext of the caller that created it; Go goroutines inherit nothing) and R1-4 (a lone call with a 1 ms linger completes within a few ms even on Windows' 15.6 ms timer tick).
//   See      : docs/review-findings.md
// =====================================================================================================================
// =====================================================================================================================
// REVIEW R1 -- reproduction tests.
//   Defect R1-3 : the loop of a batcher is started on the thread of whichever caller created the batcher (the first write to a
//                 shard) and therefore runs for the rest of the client's life inside THAT caller's ExecutionContext: AsyncLocals,
//                 Activity.Current (distributed-tracing parent of every later gRPC span of the shard), culture, claims, a request
//                 scoped HttpContext reachable through an AsyncLocal accessor (kept alive for as long as the client lives).
//     Go      : oxia/batch/batcher_factory.go:55 -- `go process.DoWithLabels(ctx, labels, batcher.Run)` (a goroutine inherits nothing).
//     C#      : src/Oxia.Client/Batch/BatcherFactory.cs:40 (`_ = batcher.RunAsync()` -> LoopAsync() runs inline until its first await,
//               and the state machine captures the ExecutionContext of the creating thread).
//     Expect  : a long-lived background loop runs with a clean context (ExecutionContext.SuppressFlow around the start).
//
//   Defect R1-4 : the linger timer has the granularity of the OS timer.  On Windows a 1 ms linger lasts ~15.6 ms (default linger 5 ms
//                 -> ~16 ms), Go's runtime timer is ~1 ms there.  Every batch of an under-loaded client pays this latency.
//     C#      : src/Oxia.Client/Batch/Batcher.cs:342-349 (ArmTimer -> System.Threading.Timer.Change(ms)).
//     Expect  : the batch of a lone call completes shortly after the linger time (median <= 8 ms for a 1 ms linger).
// =====================================================================================================================
using System.Diagnostics;
using Oxia.Client.Batch;
using Xunit;

namespace Oxia.Client.Tests.Review;

// Asserts on wall-clock latency (median of a lone call with a 1 ms linger): run it alone, not next to ~1800 parallel tests.
[Collection("R1 serial")]
public class R1_BatcherLoopTests
{
    private static readonly AsyncLocal<string?> Ambient = new();

    private sealed class ProbeBatch : IBatch
    {
        private readonly Action _onComplete;
        private int _size;
        public ProbeBatch(Action onComplete) => _onComplete = onComplete;
        public bool CanAdd(object call) => true;
        public void Add(object call) => _size++;
        public int Size => _size;
        public ValueTask CompleteAsync()
        {
            _onComplete();
            return default;
        }
        public void Fail(Exception error) { }
    }

    [Fact]
    public async Task TheBatcherLoop_DoesNotInheritTheAmbientContextOfTheCallerThatCreatedIt()
    {
        var seen = new TaskCompletionSource<string?>(TaskCreationOptions.RunContinuationsAsynchronously);
        Ambient.Value = "request-1"; // the unlucky caller whose write creates the batcher of the shard
        var factory = new BatcherFactory { Linger = TimeSpan.Zero, MaxRequestsPerBatch = 1 };
        var batcher = factory.NewBatcher(CancellationToken.None, 0, "probe", () => new ProbeBatch(() => seen.TrySetResult(Ambient.Value)));
        Ambient.Value = "request-2"; // another caller, much later
        try
        {
            await batcher.AddAsync("call");
            var observed = await seen.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Null(observed); // today: "request-1"
        }
        finally
        {
            batcher.Dispose();
            Ambient.Value = null;
        }
    }

    [Fact]
    public async Task ALoneCallWithA1msLinger_IsSentWithinAFewMilliseconds()
    {
        TaskCompletionSource<long> completedAt = null!;
        var factory = new BatcherFactory { Linger = TimeSpan.FromMilliseconds(1), MaxRequestsPerBatch = 1000 };
        var batcher = factory.NewBatcher(CancellationToken.None, 0, "probe",
            () => new ProbeBatch(() => completedAt.TrySetResult(Stopwatch.GetTimestamp())));
        var latencies = new List<double>();
        try
        {
            for (int i = 0; i < 40; i++)
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
        Assert.True(median <= 8.0, $"linger=1ms: median latency {median:F1} ms (min {latencies[0]:F1}, max {latencies[^1]:F1})");
    }
}

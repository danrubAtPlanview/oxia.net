// =====================================================================================================================
// PORT NOTE (lead's regression test for a defect found while stabilising the suite; no upstream counterpart)
//   Upstream : common/concurrent/future.go has no reuse API -- every Go Future is a fresh channel.  The .NET port added Reset() so the
//              write stream (internal/write_stream.go) can pool its per-request futures (wave 2 / Z1).
//   Intent   : Pins the invariant that makes pooling safe: once Reset() returns, the completer of the PREVIOUS round no longer touches
//              the instance.  Before the fix, a completer preempted between publishing `_ready` and resolving the waiter could, after the
//              waiter had taken the ready fast path, consumed the value, Reset() the future and armed it for a new round, resolve the NEW
//              round's waiter with the cleared value (null) -- seen as a NullReferenceException in
//              R2_WriteStreamStressTests.EveryCompletedRequestGetsItsOwnResponseEvenWhenOthersGaveUp in ~1 of 25 full-suite runs.
//   Method   : two plain threads ping-pong with spin handshakes (no allocation, no scheduler in the way) so the window is hit often.
// =====================================================================================================================
using Oxia.Common.Concurrent;
using Xunit;

namespace Oxia.Client.Tests.Review;

[Collection("R1 serial")]
public class Lead_FutureReuseRaceTests
{
    // xUnit1031 (no blocking waits in tests): the blocking GetResult()s ARE the scenario -- a dedicated thread ping-pongs with two completer
    // threads through spin handshakes, and an `await` per round would move the waiter onto the pool and hide the window under test.
#pragma warning disable xUnit1031
    [Fact]
    public void AReusedFuture_IsNeverResolvedByTheCompleterOfItsPreviousRound()
    {
        if (Environment.ProcessorCount < 2) return;

        const int rounds = 400_000;
        var roundA = new string[rounds];
        var roundB = new string[rounds];
        for (int i = 0; i < rounds; i++)
        {
            roundA[i] = "a" + i;
            roundB[i] = "b" + i;
        }

        var f = new Future<string>(runContinuationsAsynchronously: false);
        int goA = -1, goB = -1, stop = 0;

        var completerA = new Thread(() =>
        {
            for (int i = 0; i < rounds && Volatile.Read(ref stop) == 0; i++)
            {
                var spin = new SpinWait();
                while (Volatile.Read(ref goA) != i && Volatile.Read(ref stop) == 0) spin.SpinOnce(-1);
                if (Volatile.Read(ref stop) != 0) return;
                f.Complete(roundA[i]);
            }
        }) { IsBackground = true };

        var completerB = new Thread(() =>
        {
            for (int i = 0; i < rounds && Volatile.Read(ref stop) == 0; i++)
            {
                var spin = new SpinWait();
                while (Volatile.Read(ref goB) != i && Volatile.Read(ref stop) == 0) spin.SpinOnce(-1);
                if (Volatile.Read(ref stop) != 0) return;
                f.Complete(roundB[i]);
            }
        }) { IsBackground = true };

        completerA.Start();
        completerB.Start();

        string? failure = null;
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(60);
        int done = 0;
        for (int i = 0; i < rounds && failure is null && DateTime.UtcNow < deadline; i++, done++)
        {
            // round A: the waiter takes the ready fast path as soon as the completer has published, consumes the value and recycles
            Volatile.Write(ref goA, i);
            var spin = new SpinWait();
            while (!f.IsCompleted) spin.SpinOnce(-1);
            string a = f.WaitAsync(CancellationToken.None).AsTask().GetAwaiter().GetResult();
            if (!ReferenceEquals(a, roundA[i])) { failure = $"round {i}: round A delivered {a ?? "null"}"; break; }
            f.Reset();

            // round B on the SAME instance: only completerB may resolve it
            var pending = f.WaitAsync(CancellationToken.None);
            Volatile.Write(ref goB, i);
            string b = pending.AsTask().GetAwaiter().GetResult();
            if (!ReferenceEquals(b, roundB[i])) { failure = $"round {i}: round B was resolved with {b ?? "null"} (a stale completer touched the recycled future)"; break; }
            f.Reset();
        }

        Volatile.Write(ref stop, 1);
        Assert.True(failure is null, failure);
        Assert.True(done > 10_000, $"weak run: only {done} rounds");
    }
#pragma warning restore xUnit1031
}

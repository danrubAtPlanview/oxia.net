// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/batch/batcher.go  (the `time.NewTimer(linger)` / `case <-timeout` part of Run) -- no file of its own upstream.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Wake-up service for the linger of the batchers.  A batcher whose loop is idle with an open batch asks to be woken at the
//              batch deadline (`Arm(deadline)`); the callback only writes a tick into the batcher's queue, the loop itself compares the
//              clock with the deadline, so an early, late, duplicated or stale wake-up is harmless (see Batcher.cs).
//   Divergences (Go: runtime timers, ns resolution on every platform; .NET: System.Threading.Timer, whose resolution is the OS tick):
//      * WINDOWS TIMER RESOLUTION.  A process that did not call timeBeginPeriod(1) (since Windows 10 2004 the setting is per process) gets
//        the 15.625 ms system tick: a `Timer` or `Task.Delay` of 1..5 ms fires 1..20 ms late, so a 5 ms linger (the default!) was measured
//        at ~15 ms per batch round (docs/performance.md).  The service therefore waits on a HIGH RESOLUTION waitable timer
//        (CreateWaitableTimerEx with CREATE_WAITABLE_TIMER_HIGH_RESOLUTION, Windows 10 1803+; resolution ~0.5 ms) owned by ONE background
//        thread for the whole process, without touching the process-wide timer resolution (timeBeginPeriod would raise the power
//        consumption of the whole machine and affect every other timer of the host).  Where the high resolution timer is not available
//        (older Windows, other platforms -- there the OS timers are fine) it falls back to a timed Monitor wait on the same thread.
//      * ONE thread, ONE min-heap for all the batchers of the process (a few hundred armed entries at most); the thread starts on the
//        first Arm, is a background thread and never exits (it is parked on the timer).  Arm/Cancel are an O(log n) heap operation under a
//        short lock; no allocation once the heap is warm.  Callbacks run on the service thread and must not block (they write a tick).
//      * A linger is still a MINIMUM: the batch is completed at deadline + wake-up latency (~0.1..1 ms), never before the deadline, because
//        the batcher loop checks its own Stopwatch timestamp.  For linger values >= the platform tick the behaviour is unchanged.
// =====================================================================================================================
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using Microsoft.Win32.SafeHandles;

namespace Oxia.Client.Batch;

/// <summary>A re-armable one-shot wake-up of one batcher; create once, <see cref="Arm"/> many times.</summary>
internal sealed class LingerTimer
{
    private readonly Action<object?> _callback;
    private readonly object? _state;
    private readonly LingerTimerService _service;

    // guarded by the service lock
    internal int Generation;
    internal bool Armed;

    internal LingerTimer(Action<object?> callback, object? state, LingerTimerService? service = null)
    {
        _callback = callback;
        _state = state;
        _service = service ?? LingerTimerService.Shared;
    }

    /// <summary>Wakes the callback up at the Stopwatch timestamp <paramref name="deadline"/> (immediately when it already passed).  Re-arming supersedes an earlier arming.</summary>
    internal void Arm(long deadline) => _service.Arm(this, deadline);

    /// <summary>Drops a pending wake-up (the callback will not run unless it was already firing).</summary>
    internal void Cancel() => _service.Cancel(this);

    internal void Fire() => _callback(_state);
}

internal sealed class LingerTimerService
{
    private static LingerTimerService? _shared;

    /// <summary>The service shared by all the batchers of the process (created on first use, so <see cref="UseHighResolutionTimer"/> can still be set before).</summary>
    internal static LingerTimerService Shared => Volatile.Read(ref _shared) ?? LazyInitializer.EnsureInitialized(ref _shared, static () => new LingerTimerService());

    /// <summary>Test / benchmark switch: false forces the portable (Monitor wait) path on Windows, i.e. the OS tick resolution of System.Threading.Timer.</summary>
    internal static bool UseHighResolutionTimer { get; set; } = true;

    private readonly struct Node(LingerTimer timer, int generation)
    {
        public readonly LingerTimer Timer = timer;
        public readonly int Generation = generation;
    }

    private readonly object _gate = new();
    private readonly PriorityQueue<Node, long> _heap = new();
    private readonly bool _highResolution;
    private SafeWaitHandle? _hrTimer;
    private Thread? _thread;
    private long _armedFor = long.MaxValue;

    internal LingerTimerService(bool? highResolution = null)
    {
        _highResolution = highResolution ?? (UseHighResolutionTimer && OperatingSystem.IsWindows());
    }

    /// <summary>Number of wake-ups waiting (stale ones included); tests.</summary>
    internal int PendingCount
    {
        get
        {
            lock (_gate) return _heap.Count;
        }
    }

    /// <summary>True when the waitable timer path is in use (Windows with the high resolution timer available).</summary>
    internal bool IsHighResolution => _hrTimer is not null;

    internal void Arm(LingerTimer timer, long deadline)
    {
        lock (_gate)
        {
            EnsureThread();
            timer.Generation++;
            timer.Armed = true;
            _heap.Enqueue(new Node(timer, timer.Generation), deadline);
            if (deadline < _armedFor)
            {
                _armedFor = deadline;
                Wake(deadline);
            }
        }
    }

    internal void Cancel(LingerTimer timer)
    {
        lock (_gate)
        {
            timer.Generation++;
            timer.Armed = false;
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // The service thread
    // -----------------------------------------------------------------------------------------------------------------

    private void EnsureThread()
    {
        if (_thread is not null) return;

        if (_highResolution && OperatingSystem.IsWindows())
        {
            _hrTimer = NativeTimer.TryCreate();
        }
        _thread = new Thread(Run) { IsBackground = true, Name = "oxia-linger-timer" };
        _thread.Start();
    }

    private void Run()
    {
        var fire = new List<LingerTimer>(16);
        while (true)
        {
            lock (_gate)
            {
                long now = Stopwatch.GetTimestamp();
                while (_heap.TryPeek(out var node, out long due))
                {
                    if (node.Generation != node.Timer.Generation)
                    {
                        _heap.Dequeue(); // superseded or cancelled
                        continue;
                    }
                    if (due > now) break;
                    _heap.Dequeue();
                    node.Timer.Armed = false;
                    fire.Add(node.Timer);
                }

                if (fire.Count == 0)
                {
                    _armedFor = _heap.TryPeek(out _, out long next) ? next : long.MaxValue;
                    if (_hrTimer is null)
                    {
                        // Portable path: releases the lock while waiting; Arm pulses when an earlier deadline arrives
                        if (_armedFor == long.MaxValue) Monitor.Wait(_gate);
                        else Monitor.Wait(_gate, TicksToMilliseconds(_armedFor - now));
                        continue;
                    }
                    if (_armedFor != long.MaxValue) SetNativeTimer(_armedFor, now);
                }
            }

            if (fire.Count > 0)
            {
                foreach (var timer in fire)
                {
                    try
                    {
                        timer.Fire();
                    }
                    catch (Exception)
                    {
                        // a callback must never kill the service
                    }
                }
                fire.Clear();
                continue;
            }

            NativeTimer.Wait(_hrTimer!);
        }
    }

    /// <summary>Called with the lock held: the earliest deadline changed.</summary>
    private void Wake(long deadline)
    {
        if (_hrTimer is not null) SetNativeTimer(deadline, Stopwatch.GetTimestamp());
        else Monitor.Pulse(_gate);
    }

    private void SetNativeTimer(long deadline, long now)
    {
        // relative due time in 100 ns units (negative = relative)
        long delta = deadline - now;
        long hundredNs = delta <= 0 ? 1 : (long)(delta * (10_000_000.0 / Stopwatch.Frequency));
        NativeTimer.Set(_hrTimer!, -Math.Max(1, hundredNs));
    }

    private static int TicksToMilliseconds(long ticks)
    {
        long ms = (ticks * 1000 + Stopwatch.Frequency - 1) / Stopwatch.Frequency; // round up: never wake before the deadline
        return (int)Math.Clamp(ms, 1, int.MaxValue - 1);
    }
}

/// <summary>Windows high resolution waitable timer (kernel32).</summary>
internal static partial class NativeTimer
{
    private const uint CreateWaitableTimerHighResolution = 0x00000002;
    private const uint TimerAllAccess = 0x1F0003;

    [SupportedOSPlatform("windows")]
    internal static SafeWaitHandle? TryCreate()
    {
        try
        {
            var handle = CreateWaitableTimerExW(IntPtr.Zero, null, CreateWaitableTimerHighResolution, TimerAllAccess);
            if (handle.IsInvalid)
            {
                handle.Dispose();
                return null;
            }
            return handle;
        }
        catch (Exception)
        {
            return null; // kernel32 without the entry point, or any marshalling problem: use the portable path
        }
    }

    internal static void Set(SafeWaitHandle timer, long relativeDueTime100ns)
    {
        SetWaitableTimer(timer, ref relativeDueTime100ns, 0, IntPtr.Zero, IntPtr.Zero, false);
    }

    internal static void Wait(SafeWaitHandle timer) => WaitForSingleObject(timer, 0xFFFFFFFF);

    [LibraryImport("kernel32.dll", SetLastError = true, StringMarshalling = StringMarshalling.Utf16)]
    private static partial SafeWaitHandle CreateWaitableTimerExW(IntPtr attributes, string? name, uint flags, uint desiredAccess);

    [LibraryImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static partial bool SetWaitableTimer(SafeWaitHandle timer, ref long dueTime, int period, IntPtr completionRoutine, IntPtr argument,
        [MarshalAs(UnmanagedType.Bool)] bool resume);

    [LibraryImport("kernel32.dll", SetLastError = true)]
    private static partial uint WaitForSingleObject(SafeWaitHandle handle, uint milliseconds);
}

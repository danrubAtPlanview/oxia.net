// =====================================================================================================================
// PORT NOTE (benchmarks -- no upstream equivalent)
//   Upstream : none.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : One micro-benchmark per throughput change of wave 2 Z2 (docs/performance.md has the numbers), each against the thing it
//              replaced, kept next to the code so that a regression is visible:
//                QueueBenchmarks      -- BatcherQueue (lock free MPSC ring) vs the bounded System.Threading.Channels queue it replaced
//                CallPoolBenchmarks   -- thread-local + lock-free call pool vs the single-lock array stack it replaced
//                LingerTimerBenchmarks-- wake-up latency of the high resolution linger timer vs System.Threading.Timer (Windows: 15.6 ms tick)
//                BatcherAddBenchmarks -- AddAsync of a batcher end to end (queue + loop + batch), fast path and parked path
//   Run      : dotnet run -c Release --project benchmarks/Oxia.Client.Benchmarks -- --filter "*Throughput*" --job short
// =====================================================================================================================
using System.Diagnostics;
using System.Threading.Channels;
using BenchmarkDotNet.Attributes;
using Oxia.Client.Batch;

namespace Oxia.Client.Benchmarks.Throughput;

[MemoryDiagnoser]
public class QueueBenchmarks
{
    private const int Items = 1_000_000;
    private static readonly object Item = new();

    private Channel<object> _channel = null!;
    private BatcherQueue _queue = null!;

    [Params(22, 4096)]
    public int Capacity { get; set; }

    [IterationSetup]
    public void Setup()
    {
        _channel = Channel.CreateBounded<object>(new BoundedChannelOptions(Capacity)
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false,
            FullMode = BoundedChannelFullMode.Wait,
        });
        _queue = new BatcherQueue(Capacity, static _ => { });
    }

    /// <summary>One thread adds and takes (the uncontended cost of one operation through the queue).</summary>
    [Benchmark(Baseline = true, OperationsPerInvoke = Items)]
    public int Channel_SingleThread()
    {
        int n = 0;
        for (int i = 0; i < Items; i++)
        {
            _channel.Writer.TryWrite(Item);
            if (_channel.Reader.TryRead(out _)) n++;
        }
        return n;
    }

    [Benchmark(OperationsPerInvoke = Items)]
    public int BatcherQueue_SingleThread()
    {
        int n = 0;
        for (int i = 0; i < Items; i++)
        {
            _queue.TryAdd(Item, out _);
            if (_queue.TryDequeue(out _)) n++;
        }
        return n;
    }

    /// <summary>8 producer threads, one consumer (this thread); producers that find the queue full park (Channel: WriteAsync, queue: ParkedAdd).</summary>
    [Benchmark(OperationsPerInvoke = Items)]
    public async Task<int> Channel_8Producers()
    {
        const int producers = 8;
        var tasks = new Task[producers];
        for (int p = 0; p < producers; p++)
        {
            tasks[p] = Task.Run(async () =>
            {
                for (int i = 0; i < Items / producers; i++)
                {
                    if (!_channel.Writer.TryWrite(Item)) await _channel.Writer.WriteAsync(Item);
                }
            });
        }
        int n = 0;
        while (n < Items)
        {
            if (_channel.Reader.TryRead(out _)) n++;
            else await _channel.Reader.WaitToReadAsync();
        }
        await Task.WhenAll(tasks);
        return n;
    }

    [Benchmark(OperationsPerInvoke = Items)]
    public async Task<int> BatcherQueue_8Producers()
    {
        const int producers = 8;
        var tasks = new Task[producers];
        for (int p = 0; p < producers; p++)
        {
            tasks[p] = Task.Run(async () =>
            {
                for (int i = 0; i < Items / producers; i++)
                {
                    if (_queue.TryAdd(Item, out var parked) == AddStatus.Parked) await parked!.AsValueTask();
                }
            });
        }
        int n = 0;
        while (n < Items)
        {
            if (_queue.TryDequeue(out _)) n++;
            else await _queue.WaitToReadAsync();
        }
        await Task.WhenAll(tasks);
        return n;
    }
}

/// <summary>The previous call pool: one lock around an array stack.</summary>
internal sealed class LockedPool<T> where T : class
{
    private readonly object _gate = new();
    private readonly T?[] _items;
    private int _count;

    public LockedPool(int capacity) => _items = new T?[capacity];

    public T? TryRent()
    {
        lock (_gate)
        {
            if (_count == 0) return null;
            var item = _items[--_count];
            _items[_count] = null;
            return item;
        }
    }

    public void Return(T item)
    {
        lock (_gate)
        {
            if (_count < _items.Length) _items[_count++] = item;
        }
    }
}

[MemoryDiagnoser]
public class CallPoolBenchmarks
{
    public sealed class Call;

    private readonly LockedPool<Call> _locked = new(4096);
    private readonly CallPool<Call> _pool = new(4096);

    [Benchmark(Baseline = true)]
    public Call LockedPool_RentReturn()
    {
        var c = _locked.TryRent() ?? new Call();
        _locked.Return(c);
        return c;
    }

    [Benchmark]
    public Call CallPool_RentReturn()
    {
        var c = _pool.TryRent() ?? new Call();
        _pool.Return(c);
        return c;
    }

    /// <summary>16 threads renting and returning in a loop: the lock is the contended point (1000 producers of the load tool).</summary>
    [Benchmark(OperationsPerInvoke = 16 * 100_000)]
    public void LockedPool_16Threads() => Parallel.For(0, 16, _ =>
    {
        for (int i = 0; i < 100_000; i++)
        {
            var c = _locked.TryRent() ?? new Call();
            _locked.Return(c);
        }
    });

    [Benchmark(OperationsPerInvoke = 16 * 100_000)]
    public void CallPool_16Threads() => Parallel.For(0, 16, _ =>
    {
        for (int i = 0; i < 100_000; i++)
        {
            var c = _pool.TryRent() ?? new Call();
            _pool.Return(c);
        }
    });
}

/// <summary>Arm -> callback latency for a linger of 1 ms and 5 ms.  On Windows without timeBeginPeriod System.Threading.Timer fires on the 15.6 ms tick.</summary>
public class LingerTimerBenchmarks
{
    private readonly SemaphoreSlim _fired = new(0);
    private LingerTimerService _highRes = null!;
    private LingerTimerService _portable = null!;
    private LingerTimer _highResTimer = null!;
    private LingerTimer _portableTimer = null!;
    private Timer _systemTimer = null!;

    [Params(1, 5)]
    public int LingerMs { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _highRes = new LingerTimerService(true);
        _portable = new LingerTimerService(false);
        _highResTimer = new LingerTimer(static s => ((SemaphoreSlim)s!).Release(), _fired, _highRes);
        _portableTimer = new LingerTimer(static s => ((SemaphoreSlim)s!).Release(), _fired, _portable);
        _systemTimer = new Timer(static s => ((SemaphoreSlim)s!).Release(), _fired, Timeout.Infinite, Timeout.Infinite);
    }

    private long Deadline => Stopwatch.GetTimestamp() + LingerMs * Stopwatch.Frequency / 1000;

    [Benchmark(Baseline = true)]
    public void SystemThreadingTimer()
    {
        _systemTimer.Change(LingerMs, Timeout.Infinite);
        _fired.Wait();
    }

    [Benchmark]
    public void LingerTimer_PortableMonitorWait()
    {
        _portableTimer.Arm(Deadline);
        _fired.Wait();
    }

    [Benchmark]
    public void LingerTimer_HighResolution()
    {
        _highResTimer.Arm(Deadline);
        _fired.Wait();
    }
}

[MemoryDiagnoser]
public class BatcherAddBenchmarks
{
    private sealed class CountingBatch : IBatch
    {
        private int _size;
        public bool CanAdd(object call) => true;
        public void Add(object call) => _size++;
        public int Size => _size;
        public ValueTask CompleteAsync() => default;
        public void Fail(Exception error) { }
    }

    private IBatcher _roomy = null!;
    private IBatcher _tiny = null!;
    private static readonly object Call = new();

    [GlobalSetup]
    public void Setup()
    {
        _roomy = new BatcherFactory { Linger = TimeSpan.Zero, MaxRequestsPerBatch = 1000, QueueCapacity = 4096 }.NewBatcher(CancellationToken.None, 0, "b", static () => new CountingBatch());
        _tiny = new BatcherFactory { Linger = TimeSpan.Zero, MaxRequestsPerBatch = 1000, QueueCapacity = 2 }.NewBatcher(CancellationToken.None, 0, "b", static () => new CountingBatch());
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _roomy.Dispose();
        _tiny.Dispose();
    }

    /// <summary>The queue has room: a CAS and an exchange (the loop on another thread drains it).</summary>
    [Benchmark(OperationsPerInvoke = 1000)]
    public async Task Add_WithRoom()
    {
        for (int i = 0; i < 1000; i++) await _roomy.AddAsync(Call);
    }

    /// <summary>A queue of 2 slots: most adds park and are woken by the loop.</summary>
    [Benchmark(OperationsPerInvoke = 1000)]
    public async Task Add_Parking()
    {
        for (int i = 0; i < 1000; i++) await _tiny.AddAsync(Call);
    }
}

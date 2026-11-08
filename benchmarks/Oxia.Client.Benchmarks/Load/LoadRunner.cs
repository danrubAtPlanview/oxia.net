// =====================================================================================================================
// PORT NOTE (benchmark support -- no upstream equivalent)
//   Upstream : cmd/perf/perf.go (Perf.Run: generateWriteTraffic / generateReadTraffic, rate limited, one goroutine per operation).
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Drives one client with the configured workload and measures it.
//                * CLOSED LOOP (default): N producers, each an async loop `await client.PutAsync(...)` -> next operation.  The number of
//                  operations in flight is the number of producers, so the throughput is concurrency / latency and the producers must be
//                  numerous (>= linger x target rate) to fill the batches.
//                * OPEN LOOP (--rate R): a pacer issues operations on a fixed schedule whatever the completions do (like the Go tool) and the
//                  latency is measured from the INTENDED start time (no coordinated omission).  Completions are tracked with pooled
//                  trackers (ValueTask awaiter + one delegate each), so the generator itself allocates nothing per operation.
//              Measured window: the coordinator snapshots GC counts, allocated bytes (precise), process CPU time, thread-pool work items
//              and the server container CPU at the start and the end of the window; the warm-up before it is not measured.  Per-sample cost
//              is two Stopwatch reads + one array increment (LatencyHistogram), no allocation.
// =====================================================================================================================
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Oxia.Common.Constant;

namespace Oxia.Client.Benchmarks.Load;

internal sealed class LoadRunner
{
    private const int Put = 0, Get = 1, Delete = 2;

    private readonly LoadOptions _o;
    private readonly string[] _keys;
    private readonly byte[] _value;

    // The measured window (Stopwatch timestamps): set by the coordinator; producers record the operations that complete inside it.
    private long _from = long.MaxValue;
    private long _to = long.MaxValue;
    private int _stop;

    public LoadRunner(LoadOptions options)
    {
        _o = options;
        _keys = new string[options.Keys];
        for (int i = 0; i < _keys.Length; i++) _keys[i] = "key-" + i;
        _value = new byte[options.ValueSize];
        for (int i = 0; i < _value.Length; i++) _value[i] = (byte)i;
    }

    // ---------------------------------------------------------------------------------------------------------------------
    // Client construction
    // ---------------------------------------------------------------------------------------------------------------------

    private List<ClientOption> ClientOptions()
    {
        var opts = new List<ClientOption>
        {
            OxiaOptions.WithBatchLinger(_o.Linger),
            OxiaOptions.WithMaxRequestsPerBatch(_o.MaxRequestsPerBatch),
            OxiaOptions.WithMaxBatchSize(_o.MaxBatchSize),
            OxiaOptions.WithRequestTimeout(_o.RequestTimeout),
        };
        Tuning.Apply(_o.Tune, opts);
        return opts;
    }

    // ---------------------------------------------------------------------------------------------------------------------
    // The run
    // ---------------------------------------------------------------------------------------------------------------------

    public async Task<LoadResult> RunAsync(LoadServer server)
    {
        _from = _to = long.MaxValue;
        _stop = 0;
        if (_o.MinThreads > 0) ThreadPool.SetMinThreads(_o.MinThreads, _o.MinThreads);

        if (_o.Preload && (_o.Get || _o.Delete)) await PreloadAsync(server.Address);

        IAsyncClient? asyncClient = null;
        ISyncClient? syncClient = null;
        if (_o.Client == "async") asyncClient = await OxiaClient.CreateAsyncClientAsync(server.Address, ClientOptions().ToArray());
        else syncClient = await OxiaClient.CreateSyncClientAsync(server.Address, ClientOptions().ToArray());

        var producers = new Producer[_o.Producers];
        for (int i = 0; i < producers.Length; i++) producers[i] = new Producer(i, _o.RandomPayload ? _o.ValueSize : 0);

        Task[] tasks;
        OpenLoop? openLoop = null;
        if (_o.Rate > 0)
        {
            openLoop = new OpenLoop(this, asyncClient!, producers);
            tasks = [Task.Factory.StartNew(openLoop.Run, TaskCreationOptions.LongRunning)];
        }
        else
        {
            tasks = new Task[producers.Length];
            for (int i = 0; i < tasks.Length; i++)
            {
                var p = producers[i];
                tasks[i] = asyncClient is not null
                    ? Task.Run(() => AsyncProducerAsync(asyncClient, p))
                    : Task.Run(() => SyncProducerAsync(syncClient!, p));
            }
        }

        // ---- warm-up, window, stop ----
        using var allocProfiler = _o.AllocProfile ? new AllocProfiler() : null;
        await Task.Delay(_o.Warmup);
        var s0 = await TakeAsync(server);
        allocProfiler?.Start();
        Volatile.Write(ref _from, s0.Timestamp);
        await Task.Delay(_o.Duration);
        Volatile.Write(ref _to, Stopwatch.GetTimestamp());
        allocProfiler?.Stop();
        var s1 = await TakeAsync(server);
        Volatile.Write(ref _stop, 1);

        await Task.WhenAll(tasks);
        long errorsTotal = 0;
        var hist = new[] { new LatencyHistogram(), new LatencyHistogram(), new LatencyHistogram() };
        if (openLoop is not null)
        {
            for (int k = 0; k < 3; k++) hist[k].Merge(openLoop.Hist[k]);
            errorsTotal += Volatile.Read(ref openLoop.Errors);
        }
        foreach (var p in producers)
        {
            for (int k = 0; k < 3; k++) hist[k].Merge(p.Hist[k]);
            errorsTotal += p.Errors;
        }

        if (asyncClient is not null) await asyncClient.CloseAsync();
        if (syncClient is not null) await syncClient.CloseAsync();

        var result = BuildResult(hist, errorsTotal, s0, s1);
        allocProfiler?.Print(Console.Out, hist[0].Count + hist[1].Count + hist[2].Count);
        return result;
    }

    private LoadResult BuildResult(LatencyHistogram[] hist, long errors, Snapshot s0, Snapshot s1)
    {
        var all = new LatencyHistogram();
        foreach (var h in hist) all.Merge(h);
        double seconds = (double)(s1.Timestamp - s0.Timestamp) / Stopwatch.Frequency;
        double window = (double)(Volatile.Read(ref _to) - s0.Timestamp) / Stopwatch.Frequency;
        long ops = all.Count;
        double opsD = Math.Max(1, ops);

        var r = new LoadResult();
        r.Add("impl", "dotnet");
        r.Add("client", _o.Client);
        r.Add("ops", ops);
        r.Add("put", hist[Put].Count);
        r.Add("get", hist[Get].Count);
        r.Add("delete", hist[Delete].Count);
        r.Add("errors", errors);
        r.Add("seconds", window, "0.000");
        r.Add("ops_per_s", ops / window, "0");
        r.Add("p50_us", all.Quantile(0.50) / 1000.0, "0");
        r.Add("p90_us", all.Quantile(0.90) / 1000.0, "0");
        r.Add("p99_us", all.Quantile(0.99) / 1000.0, "0");
        r.Add("p999_us", all.Quantile(0.999) / 1000.0, "0");
        r.Add("max_us", all.Max / 1000.0, "0");
        r.Add("mean_us", all.Mean / 1000.0, "0");
        string[] names = ["put", "get", "delete"];
        for (int k = 0; k < 3; k++)
        {
            if (hist[k].Count == 0) continue;
            r.Add($"{names[k]}_p50_us", hist[k].Quantile(0.50) / 1000.0, "0");
            r.Add($"{names[k]}_p99_us", hist[k].Quantile(0.99) / 1000.0, "0");
        }

        double cpu = (s1.Cpu - s0.Cpu).TotalSeconds;
        r.Add("cpu_s", cpu, "0.00");
        r.Add("cpu_us_per_op", cpu * 1e6 / opsD, "0.00");
        if (s0.ServerCpu is { } sc0 && s1.ServerCpu is { } sc1)
        {
            r.Add("server_cpu_s", sc1 - sc0, "0.00");
            r.Add("server_cpu_us_per_op", (sc1 - sc0) * 1e6 / opsD, "0.00");
        }
        r.Add("gc0", s1.Gc0 - s0.Gc0);
        r.Add("gc1", s1.Gc1 - s0.Gc1);
        r.Add("gc2", s1.Gc2 - s0.Gc2);
        r.Add("alloc_bytes_per_op", (s1.Allocated - s0.Allocated) / opsD, "0");
        r.Add("workitems_per_op", (s1.WorkItems - s0.WorkItems) / opsD, "0.00");
        r.Add("lock_contentions", s1.Contentions - s0.Contentions);
        r.Add("threads", s1.Threads);
        _ = seconds;
        return r;
    }

    private readonly record struct Snapshot(long Timestamp, TimeSpan Cpu, long Allocated, int Gc0, int Gc1, int Gc2, long WorkItems, long Contentions,
        int Threads, double? ServerCpu);

    private static async Task<Snapshot> TakeAsync(LoadServer server)
    {
        double? serverCpu = await server.ReadServerCpuSecondsAsync();
        using var self = Process.GetCurrentProcess();
        return new Snapshot(Stopwatch.GetTimestamp(), self.TotalProcessorTime, GC.GetTotalAllocatedBytes(true), GC.CollectionCount(0),
            GC.CollectionCount(1), GC.CollectionCount(2), ThreadPool.CompletedWorkItemCount, Monitor.LockContentionCount, ThreadPool.ThreadCount,
            serverCpu);
    }

    // ---------------------------------------------------------------------------------------------------------------------
    // Preload
    // ---------------------------------------------------------------------------------------------------------------------

    private async Task PreloadAsync(string address)
    {
        var sw = Stopwatch.StartNew();
        var client = await OxiaClient.CreateAsyncClientAsync(address, OxiaOptions.WithBatchLinger(TimeSpan.FromMilliseconds(5)));
        const int Chunk = 5000;
        var pending = new ValueTask<PutResult>[Chunk];
        for (int start = 0; start < _keys.Length; start += Chunk)
        {
            int n = Math.Min(Chunk, _keys.Length - start);
            for (int i = 0; i < n; i++) pending[i] = client.PutAsync(_keys[start + i], _value);
            for (int i = 0; i < n; i++)
            {
                var r = await pending[i];
                if (r.Err is not null) throw new InvalidOperationException("preload failed: " + r.Err.Message);
            }
        }
        await client.CloseAsync();
        Console.Error.WriteLine($"  preloaded {_keys.Length} keys in {sw.ElapsedMilliseconds} ms");
    }

    // ---------------------------------------------------------------------------------------------------------------------
    // Producers
    // ---------------------------------------------------------------------------------------------------------------------

    private sealed class Producer
    {
        public readonly LatencyHistogram[] Hist = [new(), new(), new()];
        public long Errors;
        private ulong _state;
        public readonly byte[]? Payload;

        public Producer(int index, int randomPayloadSize)
        {
            _state = 0x9E3779B97F4A7C15UL ^ ((ulong)(index + 1) * 0xBF58476D1CE4E5B9UL);
            Payload = randomPayloadSize > 0 ? new byte[randomPayloadSize] : null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong Next()
        {
            ulong x = _state;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            return _state = x;
        }

        public ReadOnlyMemory<byte> Value(byte[] shared)
        {
            if (Payload is null) return shared;
            var span = Payload.AsSpan();
            for (int i = 0; i + 8 <= span.Length; i += 8) System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(span[i..], Next());
            return Payload;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int ChooseOp(Producer p)
    {
        if (_o.Put && _o.Get) return (int)(p.Next() % 100) < _o.ReadPercent ? Get : Put;
        if (_o.Put) return Put;
        if (_o.Get) return Get;
        return Delete;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private string ChooseKey(Producer p) => _keys[(int)(p.Next() % (ulong)_keys.Length)];

    private void Record(Producer p, int kind, long t0, long t1, Exception? err)
    {
        if (t1 < Volatile.Read(ref _from) || t1 > Volatile.Read(ref _to)) return;
        if (err is not null)
        {
            p.Errors++;
            return;
        }
        p.Hist[kind].Record((long)((t1 - t0) * (1e9 / Stopwatch.Frequency)));
    }

    private async Task AsyncProducerAsync(IAsyncClient client, Producer p)
    {
        while (Volatile.Read(ref _stop) == 0)
        {
            int kind = ChooseOp(p);
            string key = ChooseKey(p);
            long t0 = Stopwatch.GetTimestamp();
            Exception? err;
            switch (kind)
            {
                case Put:
                    err = (await client.PutAsync(key, p.Value(_value))).Err;
                    break;
                case Get:
                    err = (await client.GetAsync(key)).Err;
                    if (err is not null && OxiaErrors.Is(err, ClientErrors.KeyNotFound)) err = null;
                    break;
                default:
                    err = await client.DeleteAsync(key);
                    if (err is not null && OxiaErrors.Is(err, ClientErrors.KeyNotFound)) err = null;
                    break;
            }
            Record(p, kind, t0, Stopwatch.GetTimestamp(), err);
        }
    }

    private async Task SyncProducerAsync(ISyncClient client, Producer p)
    {
        while (Volatile.Read(ref _stop) == 0)
        {
            int kind = ChooseOp(p);
            string key = ChooseKey(p);
            long t0 = Stopwatch.GetTimestamp();
            Exception? err = null;
            try
            {
                switch (kind)
                {
                    case Put:
                        await client.PutAsync(CancellationToken.None, key, p.Value(_value));
                        break;
                    case Get:
                        await client.GetAsync(CancellationToken.None, key);
                        break;
                    default:
                        await client.DeleteAsync(CancellationToken.None, key);
                        break;
                }
            }
            catch (Exception e) when (kind != Put && OxiaErrors.Is(e, ClientErrors.KeyNotFound))
            {
                // a miss is a legitimate answer
            }
            catch (Exception e)
            {
                err = e;
            }
            Record(p, kind, t0, Stopwatch.GetTimestamp(), err);
        }
    }

    // ---------------------------------------------------------------------------------------------------------------------
    // Open loop
    // ---------------------------------------------------------------------------------------------------------------------

    private sealed class OpenLoop
    {
        private readonly LoadRunner _r;
        private readonly IAsyncClient _client;
        private readonly Producer[] _producers;
        private readonly ConcurrentQueue<Tracker> _pool = new();
        private int _inflight;

        public readonly LatencyHistogram[] Hist = [new(), new(), new()];
        public long Errors;

        public OpenLoop(LoadRunner runner, IAsyncClient client, Producer[] producers)
        {
            _r = runner;
            _client = client;
            _producers = producers;
        }

        public void Run()
        {
            var o = _r._o;
            var rng = _producers[0];
            long start = Stopwatch.GetTimestamp();
            double ticksPerOp = Stopwatch.Frequency / o.Rate;
            long issued = 0;
            while (Volatile.Read(ref _r._stop) == 0)
            {
                long now = Stopwatch.GetTimestamp();
                long due = (long)((now - start) / ticksPerOp) + 1;
                if (issued >= due || Volatile.Read(ref _inflight) >= o.MaxInflight)
                {
                    Thread.Yield();
                    continue;
                }
                while (issued < due && Volatile.Read(ref _inflight) < o.MaxInflight)
                {
                    long intended = start + (long)(issued * ticksPerOp);
                    Issue(rng, intended);
                    issued++;
                }
            }
            // wait for the outstanding operations
            var spin = new SpinWait();
            while (Volatile.Read(ref _inflight) > 0) spin.SpinOnce();
        }

        private void Issue(Producer rng, long intended)
        {
            int kind = _r.ChooseOp(rng);
            string key = _r.ChooseKey(rng);
            if (!_pool.TryDequeue(out var t)) t = new Tracker(this);
            t.Kind = kind;
            t.Intended = intended;
            Interlocked.Increment(ref _inflight);
            switch (kind)
            {
                case Put: t.Start(_client.PutAsync(key, _r._value)); break;
                case Get: t.Start(_client.GetAsync(key)); break;
                default: t.Start(_client.DeleteAsync(key)); break;
            }
        }

        private void Completed(Tracker t, Exception? err)
        {
            long now = Stopwatch.GetTimestamp();
            if (err is not null && t.Kind != Put && OxiaErrors.Is(err, ClientErrors.KeyNotFound)) err = null;
            if (now >= Volatile.Read(ref _r._from) && now <= Volatile.Read(ref _r._to))
            {
                if (err is not null) Interlocked.Increment(ref Errors);
                else Hist[t.Kind].RecordConcurrent((long)((now - t.Intended) * (1e9 / Stopwatch.Frequency)));
            }
            _pool.Enqueue(t);
            Interlocked.Decrement(ref _inflight);
        }

        private sealed class Tracker
        {
            private readonly OpenLoop _owner;
            private ValueTaskAwaiter<PutResult> _put;
            private ValueTaskAwaiter<GetResult> _get;
            private ValueTaskAwaiter<Exception?> _delete;
            public int Kind;
            public long Intended;
            private readonly Action _continuation;

            public Tracker(OpenLoop owner)
            {
                _owner = owner;
                _continuation = OnCompleted;
            }

            public void Start(ValueTask<PutResult> t)
            {
                _put = t.GetAwaiter();
                if (_put.IsCompleted) OnCompleted();
                else _put.UnsafeOnCompleted(_continuation);
            }

            public void Start(ValueTask<GetResult> t)
            {
                _get = t.GetAwaiter();
                if (_get.IsCompleted) OnCompleted();
                else _get.UnsafeOnCompleted(_continuation);
            }

            public void Start(ValueTask<Exception?> t)
            {
                _delete = t.GetAwaiter();
                if (_delete.IsCompleted) OnCompleted();
                else _delete.UnsafeOnCompleted(_continuation);
            }

            private void OnCompleted()
            {
                Exception? err = Kind switch
                {
                    Put => _put.GetResult().Err,
                    Get => _get.GetResult().Err,
                    _ => _delete.GetResult(),
                };
                _owner.Completed(this, err);
            }
        }
    }
}

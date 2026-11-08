// =====================================================================================================================
// PORT NOTE
//   Upstream : (none for Hasher / Future -- new benchmarks); BenchmarkStreamMsgSent of common/rpc/grpcmetrics/grpcmetrics_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Hot-path micro benchmarks of the other common utilities: shard routing hash, the per-operation Future, and the cost of
//              counting one gRPC stream message.
//   Divergences: BenchmarkStreamMsgSent measured the server stream wrapper (SendMsg); the client wrapper increments the same kind of cached
//              counter, which is what is measured here (`stream` and `stream-parallel` sub-benchmarks -> StreamMsgSent / StreamMsgSentParallel).
// =====================================================================================================================
using System.Diagnostics.Metrics;
using System.Text;
using BenchmarkDotNet.Attributes;
using Oxia.Common.Concurrent;
using Oxia.Common.Hash;
using Oxia.Common.Rpc;

namespace Oxia.Client.Benchmarks.Common;

[MemoryDiagnoser]
public class HashBenchmarks
{
    private const string ShortKey = "/a/key";
    private const string TypicalKey = "/some/regular/application/key-123456";
    private static readonly string LongKey = new('k', 1000);
    private readonly byte[] _typicalUtf8 = Encoding.UTF8.GetBytes(TypicalKey);

    [Benchmark]
    public uint Xxh332_Short() => Hasher.Xxh332(ShortKey);

    [Benchmark]
    public uint Xxh332_Typical() => Hasher.Xxh332(TypicalKey);

    [Benchmark]
    public uint Xxh332_TypicalUtf8() => Hasher.Xxh332(_typicalUtf8);

    [Benchmark]
    public uint Xxh332_Long() => Hasher.Xxh332(LongKey);
}

[MemoryDiagnoser]
public class FutureBenchmarks
{
    private readonly Future<long> _future = new();
    private readonly Future<long> _synchronousFuture = new(runContinuationsAsynchronously: false);

    /// <summary>Complete first, then wait (the ready path): what a response that arrives before the caller awaits costs.</summary>
    [Benchmark]
    public long CompleteThenWait()
    {
        _future.Reset();
        _future.Complete(42);
        return _future.WaitAsync(CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <summary>Wait first, then complete (the waiter slot path).</summary>
    [Benchmark]
    public long WaitThenComplete()
    {
        _synchronousFuture.Reset();
        var pending = _synchronousFuture.WaitAsync(CancellationToken.None);
        _synchronousFuture.Complete(42);
        return pending.GetAwaiter().GetResult();
    }

    [Benchmark(Baseline = true)]
    public long TaskCompletionSourceBaseline()
    {
        var tcs = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        tcs.SetResult(42);
        return tcs.Task.GetAwaiter().GetResult();
    }
}

[MemoryDiagnoser]
public class GrpcMetricsBenchmarks
{
    private Meter _meter = null!;
    private GrpcClientMetricsInterceptor.MethodMetrics _metrics = null!;

    [GlobalSetup]
    public void Setup()
    {
        _meter = new Meter("oxia-bench");
        _metrics = new GrpcClientMetricsInterceptor(_meter).GetMethodMetrics(GrpcRpcType.BidiStream, "/replication.OxiaLogReplication/Replicate");
    }

    [GlobalCleanup]
    public void Cleanup() => _meter.Dispose();

    [Benchmark]
    public void StreamMsgSent() => _metrics.Sent();

    [Benchmark]
    public void StreamMsgSentParallel() => Parallel.For(0, 4, _ =>
    {
        for (var i = 0; i < 256; i++) _metrics.Sent();
    });
}

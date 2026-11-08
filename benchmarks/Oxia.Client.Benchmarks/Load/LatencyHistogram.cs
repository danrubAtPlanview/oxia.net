// =====================================================================================================================
// PORT NOTE (benchmark support -- no upstream equivalent)
//   Upstream : none.  The Go perf tool (cmd/perf) uses github.com/bmizerany/perks/quantile (a streaming estimator that allocates).
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : A fixed-size log-linear ("HDR style") latency histogram: values below 256 are exact, above that every power of two is
//              split into 128 linear sub-buckets, so the relative error of any reported quantile is below 0.8 %.  Recording is one
//              integer log2, one array increment: NO allocation per sample (the arrays are allocated once), so the load generator
//              does not disturb the allocation figures of the client under test.  Single-writer `Record` for the closed-loop
//              producers (one histogram each, merged at the end) and `RecordConcurrent` (Interlocked) for the open-loop completions.
//              The Go twin (Load/go/hist.go) uses the same layout, so the two tools' percentiles are computed alike.
// =====================================================================================================================
using System.Numerics;
using System.Runtime.CompilerServices;

namespace Oxia.Client.Benchmarks.Load;

public sealed class LatencyHistogram
{
    private const int SubBits = 7;
    private const int SubCount = 1 << SubBits;       // 128 sub-buckets per power of two
    private const int LinearLimit = SubCount * 2;    // values below 256 are exact
    private const int MaxMsb = 46;                   // 2^47 ns = 39 hours; larger values land in the last bucket
    private const int Size = LinearLimit + (MaxMsb - SubBits) * SubCount;

    private readonly long[] _counts = new long[Size];
    private long _total;
    private long _sum;
    private long _max;

    public long Count => Volatile.Read(ref _total);

    public long Max => Volatile.Read(ref _max);

    public double Mean => Count == 0 ? 0 : (double)Volatile.Read(ref _sum) / Count;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int IndexOf(long value)
    {
        if ((ulong)value < LinearLimit) return (int)value;
        int msb = 63 - BitOperations.LeadingZeroCount((ulong)value);
        if (msb > MaxMsb) return Size - 1;
        int shift = msb - SubBits;
        return LinearLimit + (shift - 1) * SubCount + (int)((value >> shift) - SubCount);
    }

    /// <summary>Highest value that falls into the bucket (HdrHistogram's "highest equivalent value").</summary>
    internal static long HighOf(int index)
    {
        if (index < LinearLimit) return index;
        int k = index - LinearLimit;
        int shift = k / SubCount + 1;
        long sub = k % SubCount + SubCount;
        return (sub << shift) + (1L << shift) - 1;
    }

    /// <summary>Records a value (not thread safe).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Record(long value)
    {
        if (value < 0) value = 0;
        _counts[IndexOf(value)]++;
        _total++;
        _sum += value;
        if (value > _max) _max = value;
    }

    /// <summary>Records a value from any thread.</summary>
    public void RecordConcurrent(long value)
    {
        if (value < 0) value = 0;
        Interlocked.Increment(ref _counts[IndexOf(value)]);
        Interlocked.Increment(ref _total);
        Interlocked.Add(ref _sum, value);
        long seen = Volatile.Read(ref _max);
        while (value > seen)
        {
            long prev = Interlocked.CompareExchange(ref _max, value, seen);
            if (prev == seen) break;
            seen = prev;
        }
    }

    public void Merge(LatencyHistogram other)
    {
        for (int i = 0; i < _counts.Length; i++) _counts[i] += other._counts[i];
        _total += other._total;
        _sum += other._sum;
        if (other._max > _max) _max = other._max;
    }

    public void Reset()
    {
        Array.Clear(_counts);
        _total = 0;
        _sum = 0;
        _max = 0;
    }

    /// <summary>The value at quantile <paramref name="q"/> (0..1): the upper bound of the bucket holding that rank, clamped to the exact max.</summary>
    public long Quantile(double q)
    {
        long total = _total;
        if (total == 0) return 0;
        long target = (long)Math.Ceiling(total * q);
        if (target < 1) target = 1;
        long cumulative = 0;
        for (int i = 0; i < _counts.Length; i++)
        {
            cumulative += _counts[i];
            if (cumulative >= target) return Math.Min(HighOf(i), _max);
        }
        return _max;
    }
}

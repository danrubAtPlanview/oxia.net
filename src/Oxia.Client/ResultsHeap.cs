// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/results_heap.go   (rangeScanResult, ResultAndChannel, newResultAndChannel, ResultHeap)
//              + oxia/async_client_impl.go aggregateAndSortRangeScanAcrossShards (the merge loop that drives the heap)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : A range scan without a partition key asks every shard, each returning its records in key order (or in the
//              order of their secondary keys when scanning an index), and merges the streams with a min-heap that always
//              yields the lowest next record.  The heap orders by secondary key first -- when BOTH results have one -- and
//              then by key, in the key order of the namespace (KeyOrder).
//   Divergences:
//      * Go's `container/heap` over a `[]*ResultAndChannel` (interface Len/Less/Swap/Push/Pop, boxing through `any`) is an
//        array-based binary heap written out directly (ResultHeap.Push/Pop/Fix/Init follow container/heap's up/down
//        exactly, so ties resolve the same way).
//      * ALLOCATION.  There is one ResultAndChannel per source channel for the whole scan and it is REUSED for every
//        record of that channel (Reset re-encodes the sort keys in place into buffers it owns); Go allocates a new
//        ResultAndChannel plus two []byte sort keys per record.  The unbuffered `chan rangeScanResult` of Go is a
//        `ChannelReader<RangeScanResult>`; a closed Go channel is a completed reader.
//      * `rangeScanResult{gr, secondaryIndexKey *string}` is the struct RangeScanResult(Gr, string? SecondaryIndexKey).
//      * The merge loop lives here (RangeScanMerger) instead of in the client so that it is testable on its own (Go tests
//        it from key_order_test.go); the async client calls it.  Go's blocking `outCh <- r` is `await WriteAsync`
//        (backpressure on a bounded channel); `close(outCh)` is TryComplete.  A cancelled merge completes the output
//        quietly; an unexpected failure of a source completes it with that exception.
// =====================================================================================================================
using System.Threading.Channels;

namespace Oxia.Client;

/// <summary>
/// A result of the range scan of one shard.  When the scan uses an index, the shard returns the records in the order of
/// their secondary keys, and the servers send the secondary key with each record.  Older servers don't, and leave it null.
/// (Go: rangeScanResult.)
/// </summary>
internal readonly record struct RangeScanResult(GetResult Gr, string? SecondaryIndexKey);

/// <summary>Go: ResultAndChannel -- the next result of a source channel plus the sort keys the heap compares.</summary>
internal sealed class ResultAndChannel
{
    private byte[] _sortKey = Array.Empty<byte>();
    private int _sortKeyLength;
    private byte[] _sortSecondaryKey = Array.Empty<byte>();
    private int _sortSecondaryKeyLength;

    private ResultAndChannel(ChannelReader<RangeScanResult> ch) => Ch = ch;

    public GetResult Gr { get; private set; }

    /// <summary>The source channel this result came from (read again from it when this result is consumed).</summary>
    public ChannelReader<RangeScanResult> Ch { get; }

    /// <summary>True if the server sent the secondary key of the record.</summary>
    public bool HasSecondaryKey { get; private set; }

    /// <summary>The key of the result in the form that the order of the heap compares.</summary>
    public ReadOnlySpan<byte> SortKey => new(_sortKey, 0, _sortKeyLength);

    /// <summary>The secondary key of the result in the form that the order of the heap compares (empty if none).</summary>
    public ReadOnlySpan<byte> SortSecondaryKey => new(_sortSecondaryKey, 0, _sortSecondaryKeyLength);

    /// <summary>Go: newResultAndChannel(order, r, ch).</summary>
    public static ResultAndChannel New(KeyOrder order, RangeScanResult r, ChannelReader<RangeScanResult> ch)
    {
        var rc = new ResultAndChannel(ch);
        rc.Reset(order, r);
        return rc;
    }

    /// <summary>Replaces the result by the next one of the same channel (Go: a new ResultAndChannel), reusing the buffers.</summary>
    public void Reset(KeyOrder order, RangeScanResult r)
    {
        Gr = r.Gr;
        _sortKeyLength = Encode(order, r.Gr.Key, ref _sortKey);
        if (r.SecondaryIndexKey is { } secondaryKey)
        {
            _sortSecondaryKeyLength = Encode(order, secondaryKey, ref _sortSecondaryKey);
            HasSecondaryKey = true;
        }
        else
        {
            _sortSecondaryKeyLength = 0;
            HasSecondaryKey = false;
        }
    }

    private static int Encode(KeyOrder order, string key, ref byte[] buffer)
    {
        int length = order.SortKeyLength(key);
        if (buffer.Length < length)
        {
            buffer = new byte[Math.Max(length, 2 * buffer.Length)];
        }

        return order.SortKeyTo(key, buffer);
    }
}

/// <summary>Go: ResultHeap -- a min-heap of the next result of each source channel.</summary>
internal sealed class ResultHeap
{
    private readonly KeyOrder _order;
    private int _count;

    public ResultHeap(KeyOrder order, int capacity = 4)
    {
        _order = order;
        Results = new ResultAndChannel[Math.Max(capacity, 1)];
    }

    /// <summary>Go: <c>h := &amp;ResultHeap{results: elements}; heap.Init(h)</c> -- a heap built from existing elements.</summary>
    public ResultHeap(KeyOrder order, ReadOnlySpan<ResultAndChannel> elements) : this(order, elements.Length)
    {
        elements.CopyTo(Results);
        _count = elements.Length;
        Init();
    }

    /// <summary>Go: h.results -- the backing array; only the first <see cref="Count"/> elements are valid and Results[0] is the min.</summary>
    public ResultAndChannel[] Results { get; private set; }

    /// <summary>Go: Len().</summary>
    public int Count => _count;

    /// <summary>The lowest result (Go: h.results[0]).</summary>
    public ResultAndChannel Top => Results[0];

    /// <summary>
    /// Go: Less -- orders the results by their secondary keys, then by their keys.  The results without a secondary key
    /// are ordered by their keys only.
    /// </summary>
    public bool Less(int i, int j)
    {
        var a = Results[i];
        var b = Results[j];
        if (a.HasSecondaryKey && b.HasSecondaryKey)
        {
            int c = _order.CompareSortKeys(a.SortSecondaryKey, b.SortSecondaryKey);
            if (c != 0)
            {
                return c < 0;
            }
        }

        return _order.CompareSortKeys(a.SortKey, b.SortKey) < 0;
    }

    private void Swap(int i, int j) => (Results[i], Results[j]) = (Results[j], Results[i]);

    /// <summary>Go: heap.Init -- establishes the heap invariants after the elements were placed directly in <see cref="Results"/>.</summary>
    public void Init()
    {
        int n = _count;
        for (int i = (n / 2) - 1; i >= 0; i--)
        {
            Down(i, n);
        }
    }

    /// <summary>Go: heap.Push.</summary>
    public void Push(ResultAndChannel x)
    {
        if (_count == Results.Length)
        {
            var bigger = new ResultAndChannel[Results.Length * 2];
            Array.Copy(Results, bigger, _count);
            Results = bigger;
        }

        Results[_count++] = x;
        Up(_count - 1);
    }

    /// <summary>Go: heap.Pop -- removes and returns the lowest result.</summary>
    public ResultAndChannel Pop()
    {
        int n = _count - 1;
        Swap(0, n);
        Down(0, n);
        var x = Results[n];
        Results[n] = null!;   // do not keep the popped element alive
        _count = n;
        return x;
    }

    /// <summary>Go: heap.Fix -- re-establishes the ordering after the element at index <paramref name="i"/> changed.</summary>
    public void Fix(int i)
    {
        if (!Down(i, _count))
        {
            Up(i);
        }
    }

    private void Up(int j)
    {
        while (true)
        {
            int i = (j - 1) / 2;   // parent
            if (i == j || !Less(j, i))
            {
                break;
            }

            Swap(i, j);
            j = i;
        }
    }

    private bool Down(int i0, int n)
    {
        int i = i0;
        while (true)
        {
            int j1 = (2 * i) + 1;
            if (j1 >= n || j1 < 0)
            {
                break;
            }

            int j = j1;   // left child
            int j2 = j1 + 1;
            if (j2 < n && Less(j2, j1))
            {
                j = j2;   // right child
            }

            if (!Less(j, i))
            {
                break;
            }

            Swap(i, j);
            i = j;
        }

        return i > i0;
    }
}

/// <summary>Go: aggregateAndSortRangeScanAcrossShards (async_client_impl.go).</summary>
internal static class RangeScanMerger
{
    /// <summary>
    /// We do range scan on all the shards, and we need to always pick the lowest key across all the shards.  With an
    /// index, the shards return the records in the order of their secondary keys, so we pick the lowest secondary key
    /// first.  Writes the merged records to <paramref name="outCh"/> and completes it when the sources are exhausted, or
    /// right after the first result carrying an error (that result is the last one written).
    /// </summary>
    public static async Task AggregateAndSortRangeScanAcrossShardsAsync(KeyOrder order,
        ChannelReader<RangeScanResult>[] channels, ChannelWriter<GetResult> outCh, CancellationToken ct = default)
    {
        try
        {
            var h = new ResultHeap(order, channels.Length);

            // First make sure we have 1 key from each channel
            foreach (var ch in channels)
            {
                if (await TryReadAsync(ch, ct).ConfigureAwait(false) is { } r)
                {
                    h.Push(ResultAndChannel.New(order, r, ch));
                }
            }

            // Now that we have something from each channel, iterate by picking the result with the lowest key and then
            // reading again from that same channel
            while (h.Count > 0)
            {
                var top = h.Results[0];

                await outCh.WriteAsync(top.Gr, ct).ConfigureAwait(false);

                if (top.Gr.Err is not null)
                {
                    outCh.TryComplete();
                    return;
                }

                // read again from same channel
                if (await TryReadAsync(top.Ch, ct).ConfigureAwait(false) is { } next)
                {
                    top.Reset(order, next);
                    h.Fix(0);
                }
                else
                {
                    h.Pop();
                }
            }

            outCh.TryComplete();
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            outCh.TryComplete();
        }
        catch (Exception e)
        {
            outCh.TryComplete(e);
        }
    }

    /// <summary>Go: `r, ok := &lt;-ch` -- null once the channel is closed and drained.</summary>
    private static async ValueTask<RangeScanResult?> TryReadAsync(ChannelReader<RangeScanResult> reader, CancellationToken ct)
    {
        while (await reader.WaitToReadAsync(ct).ConfigureAwait(false))
        {
            if (reader.TryRead(out var item))
            {
                return item;
            }
        }

        return null;
    }
}

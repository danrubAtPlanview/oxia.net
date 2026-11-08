// =====================================================================================================================
// REVIEW R3 -- reproduction tests (they FAIL until the production code is fixed; they never touch existing tests)
//   Defect   : a RangeScan whose token is cancelled ends SILENTLY (the reader just completes) instead of delivering a
//              GetResult carrying the cancellation error, so the caller cannot tell a truncated scan from a complete one.
//   Go       : oxia/async_client_impl.go
//                * rangeScanFromShard (l.623-646): once ctx is done ExecuteRangeScan fails and `onResult(GetResult{Err: err}, nil)`
//                  is ALWAYS delivered (the send is `outCh <- result` / `ch <- rangeScanResult`, it never selects on ctx.Done)
//                * RangeScan multi-shard (l.665-691): the shard scans run with a derived context; the merge
//                  (aggregateAndSortRangeScanAcrossShards, l.708-742) reads their channels WITHOUT looking at ctx, so it
//                  receives the shard error results; an error result has key "" and sorts first in the heap, so it is emitted
//                  as the next (and last) item and then `close(outCh)`.
//              Contrast with List, whose Go doc says explicitly "Once ctx is done, the channel is closed, possibly before all
//              the keys and without an error" -- RangeScan has no such statement and Go never closes it silently.
//   C#       : src/Oxia.Client/AsyncClientImpl.cs  RangeScanAsync -> RangeScanMerger.AggregateAndSortRangeScanAcrossShardsAsync(order,
//              readers, writer, ct)  (ResultsHeap.cs l.247-299): TryReadAsync(ch, ct)/WriteAsync(.., ct) throw
//              OperationCanceledException as soon as `ct` is cancelled and the catch at l.291 does `outCh.TryComplete()` -- the shard
//              error results never reach the output.  (The partition-key path delivers the error only if the output has room:
//              PumpAsync drops it once ct is done and the output is full.)
// =====================================================================================================================
using System.Threading.Channels;
using Oxia.Client.Tests.F2;
using Xunit;

namespace Oxia.Client.Tests.Review;

public class R3_RangeScanCancelTests
{
    private static async Task<List<GetResult>> DrainAsync(ChannelReader<GetResult> reader)
    {
        var items = new List<GetResult>();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (await reader.WaitToReadAsync(timeout.Token))
        {
            while (reader.TryRead(out var r)) items.Add(r);
        }
        return items;
    }

    /// <summary>Go: the scan of a shard that is still streaming when ctx is cancelled yields an error result; the merge emits it.</summary>
    [Fact]
    public async Task RangeScan_MultiShard_CancelledWhileStreaming_DeliversAnErrorResult()
    {
        var executor = new StaticExecutor
        {
            Keys = new Dictionary<long, string[]> { [1] = ["a", "b"] },
            Slow = [1, 2],
        };
        var client = StaticExecutor.NewScanTestClient(executor, 1, 2);
        using var cts = new CancellationTokenSource();

        var reader = client.RangeScanAsync(cts.Token, "a", "z");
        await Task.Delay(200);
        cts.Cancel();

        var items = await DrainAsync(reader);

        Assert.True(items.Count > 0 && items[^1].Err is not null,
            "Go delivers the cancellation as an error result before closing the channel; the C# reader completed silently with "
            + items.Count + " item(s) and no error, so a truncated scan looks like a complete one");
    }

    /// <summary>Go: with a context that is already done every shard scan fails at once and the error is delivered.</summary>
    [Fact]
    public async Task RangeScan_MultiShard_AlreadyCancelledToken_DeliversAnErrorResult()
    {
        var executor = new StaticExecutor { Slow = [1, 2] };
        var client = StaticExecutor.NewScanTestClient(executor, 1, 2);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var items = await DrainAsync(client.RangeScanAsync(cts.Token, "a", "z"));

        Assert.True(items.Count > 0 && items[^1].Err is not null,
            "Go delivers an error result for a scan started with a done context; the C# reader completed with no item at all");
    }

    /// <summary>
    /// Single shard (PartitionKey): Go blocks the shard scan on `outCh &lt;- result` until the caller reads, then delivers the
    /// error that follows the records.  C#: the pump drops everything (the error included) once the token is done and the
    /// 100-item output is full.
    /// </summary>
    [Fact]
    public async Task RangeScan_PartitionKey_CancelledWithSlowConsumer_StillDeliversTheErrorResult()
    {
        var keys = new string[150];
        for (int i = 0; i < keys.Length; i++) keys[i] = "k" + i.ToString("D4");
        var executor = new StaticExecutor
        {
            Keys = new Dictionary<long, string[]> { [1] = keys },
            Slow = [1],
        };
        var client = StaticExecutor.NewScanTestClient(executor, 1);
        using var cts = new CancellationTokenSource();

        var reader = client.RangeScanAsync(cts.Token, "a", "z", OxiaOptions.PartitionKey("p"));
        await Task.Delay(300);      // the 100-item output is full, the scan is parked on the 101st record
        cts.Cancel();
        await Task.Delay(100);

        var items = await DrainAsync(reader);

        Assert.True(items.Count > 0 && items[^1].Err is not null,
            "Go never drops the error result of a cancelled scan; the C# reader completed after " + items.Count
            + " item(s) with no error (the pump dropped the rest, error included)");
    }
}

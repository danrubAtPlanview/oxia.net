// =====================================================================================================================
// PORT NOTE (extra tests -- no Go counterpart; they never replace the ported ones)
//   Upstream : oxia/internal/metrics/metrics.go behaviour beyond metrics_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : the instruments keep Go's names / units / duplicate-name timers; calls are decorated only while somebody
//              listens (and a listener attached later is honoured); the decorator completes the original call BEFORE recording
//              and honours a replaced Callback (Go copies the struct, so the multi-shard delete-range callback wraps the
//              decorated one); a failed op is tagged failure; the decorated copy carries every field.
// =====================================================================================================================
using System.Diagnostics.Metrics;
using Oxia.Client.Internal.Model;
using Oxia.Common.Constant;
using Oxia.Common.Proto;
using Xunit;
using OxiaMetrics = Oxia.Client.Internal.Metrics.Metrics;

namespace Oxia.Client.Tests.Metrics;

public class MetricsExtraTests
{
    [Fact]
    public void InstrumentsKeepTheNamesAndUnitsOfGo()
    {
        using var factory = new TestMeterFactory();
        var metrics = new OxiaMetrics(factory);
        using var reader = new MetricsReader(metrics.Meter);

        Assert.Equal("oxia_client", metrics.Meter.Name);

        // timers: a Counter<double> in ms AND a Counter<long> without unit, both with the same name
        foreach (var name in new[] { "oxia_client_op", "oxia_client_batch_total", "oxia_client_batch_exec" })
        {
            var published = reader.Published(name);
            Assert.Equal(2, published.Count);
            Assert.Equal("ms", Assert.Single(published.OfType<Counter<double>>()).Unit);
            Assert.Null(Assert.Single(published.OfType<Counter<long>>()).Unit);
        }

        Assert.Equal("By", Assert.Single(reader.Published("oxia_client_op_value").OfType<Histogram<long>>()).Unit);
        Assert.Equal("By", Assert.Single(reader.Published("oxia_client_batch_value").OfType<Histogram<long>>()).Unit);
        Assert.Null(Assert.Single(reader.Published("oxia_client_batch_request").OfType<Histogram<long>>()).Unit);
    }

    [Fact]
    public void WithoutListeners_TheCallsAreNotDecorated()
    {
        using var factory = new TestMeterFactory();
        var metrics = new OxiaMetrics(factory);

        Assert.False(metrics.OpEnabled);
        Assert.False(metrics.BatchEnabled);
        var put = new PutCall();
        var delete = new DeleteCall();
        var range = new DeleteRangeCall();
        var get = new GetCall();
        Assert.Same(put, metrics.DecoratePut(put));
        Assert.Same(delete, metrics.DecorateDelete(delete));
        Assert.Same(range, metrics.DecorateDeleteRange(range));
        Assert.Same(get, metrics.DecorateGet(get));
    }

    [Fact]
    public void AListenerAttachedLater_IsHonouredFromThenOn()
    {
        using var factory = new TestMeterFactory();
        var metrics = new OxiaMetrics(factory);
        var before = new PutCall { Callback = (_, _) => { } };
        Assert.Same(before, metrics.DecoratePut(before));

        using var reader = new MetricsReader(metrics.Meter);
        Assert.True(metrics.OpEnabled);
        Assert.True(metrics.BatchEnabled);
        var after = new PutCall { Callback = (_, _) => { } };
        Assert.NotSame(after, metrics.DecoratePut(after));
    }

    [Fact]
    public void TheDecoratedCallCompletesTheOriginalFirst_ThenRecords()
    {
        using var factory = new TestMeterFactory();
        var metrics = new OxiaMetrics(factory);
        using var reader = new MetricsReader(metrics.Meter);

        var events = new List<string>();
        var original = new PutCall { Key = "k", Value = new byte[3], Callback = (_, _) => events.Add("callback:" + reader.Counter<long>("oxia_client_op").Count) };
        var decorated = metrics.DecoratePut(original);

        decorated.Complete(new PutResponse(), null);

        // when the original callback ran, the op had not been recorded yet (Go: callback(response, err), then the metrics)
        Assert.Equal(new[] { "callback:0" }, events);
        Assert.Equal(1, Assert.Single(reader.Counter<long>("oxia_client_op")).Value);
    }

    [Fact]
    public void TheDecoratedCopyCarriesEveryFieldAndOpIndex()
    {
        using var factory = new TestMeterFactory();
        var metrics = new OxiaMetrics(factory);
        using var reader = new MetricsReader(metrics.Meter);

        var context = new CallContext(CancellationToken.None);
        var original = new PutCall
        {
            Key = "k",
            Value = new byte[] { 1, 2 },
            ExpectedVersionId = 5,
            SessionId = 6,
            ClientIdentity = "me",
            PartitionKey = "pk",
            SequenceKeysDeltas = [1, 2],
            OpIndex = 9,
            CallContext = context,
        };

        var decorated = metrics.DecoratePut(original);

        Assert.NotSame(original, decorated);
        Assert.Equal("k", decorated.Key);
        Assert.Equal(new byte[] { 1, 2 }, decorated.Value.ToArray());
        Assert.Equal(5, decorated.ExpectedVersionId);
        Assert.Equal(6, decorated.SessionId);
        Assert.Equal("me", decorated.ClientIdentity);
        Assert.Equal("pk", decorated.PartitionKey);
        Assert.Equal(new ulong[] { 1, 2 }, decorated.SequenceKeysDeltas);
        Assert.Equal(9u, decorated.OpIndex);
        Assert.Same(context, decorated.CallContext);
    }

    [Fact]
    public void AReplacedCallbackOfTheDecoratedCallWrapsTheDecoratedOne()
    {
        // async_client_impl.go sendDeleteRangeToSuccessors: call.Callback = multiShardDeleteRangeCallback(n, call.Callback)
        using var factory = new TestMeterFactory();
        var metrics = new OxiaMetrics(factory);
        using var reader = new MetricsReader(metrics.Meter);

        DeleteRangeResponse? seen = null;
        var decorated = metrics.DecorateDeleteRange(new DeleteRangeCall { Callback = (r, _) => seen = r });
        var inner = decorated.Callback!;
        int outerCalls = 0;
        decorated.Callback = (r, e) =>
        {
            outerCalls++;
            inner(r, e);
        };

        var response = new DeleteRangeResponse();
        decorated.Complete(response, null);

        Assert.Equal(1, outerCalls);
        Assert.Same(response, seen);
        Assert.Equal(1, Assert.Single(reader.Counter<long>("oxia_client_op")).Value);
    }

    [Fact]
    public void ADecoratedCallCompletesAPooledOriginalThroughItsOverride()
    {
        using var factory = new TestMeterFactory();
        var metrics = new OxiaMetrics(factory);
        using var reader = new MetricsReader(metrics.Meter);

        var pooled = new PooledGet();
        var decorated = metrics.DecorateGet(pooled);
        var response = new GetResponse { Value = new byte[7] };

        decorated.Complete(response, null);

        Assert.Same(response, pooled.Seen);
        Assert.Equal(7, Assert.Single(reader.Histogram("oxia_client_op_value")).Value);
    }

    private sealed class PooledGet : GetCall
    {
        public GetResponse? Seen;
        public override void Complete(GetResponse? response, Exception? error) => Seen = response;
    }

    [Fact]
    public void AFailedOperationIsTaggedFailure_AndItsValueSizeIsZero()
    {
        using var factory = new TestMeterFactory();
        var metrics = new OxiaMetrics(factory);
        using var reader = new MetricsReader(metrics.Meter);

        metrics.DecorateGet(new GetCall { Callback = (_, _) => { } }).Complete(null, OxiaErrors.ShardNotFound);

        var point = Assert.Single(reader.Counter<long>("oxia_client_op"));
        Assert.Equal("failure", point.Attributes["result"]);
        Assert.Equal("get", point.Attributes["type"]);
        Assert.Equal(0, Assert.Single(reader.Histogram("oxia_client_op_value")).Value);
    }

    [Fact]
    public void DisposeIsIdempotent_AndDoesNotDisposeAFactoryMeter()
    {
        using var factory = new TestMeterFactory();
        var shared = new OxiaMetrics(factory);
        shared.Dispose();
        shared.Dispose();

        var own = new OxiaMetrics(null);
        Assert.Equal("oxia_client", own.Meter.Name);
        own.Dispose();
        own.Dispose();
    }
}

// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/internal/metrics/metrics.go  (Metrics, NewMetrics, DecoratePut/Delete/DeleteRange/Get, WriteCallback,
//              ReadCallback, metricContextFunc)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The client metrics, on a Meter named "oxia_client":
//                 timers (Counter<double> "ms" + Counter<long>, same name): oxia_client_op, oxia_client_batch_total,
//                                                                            oxia_client_batch_exec
//                 histograms (long): oxia_client_op_value ("By"), oxia_client_batch_value ("By"), oxia_client_batch_request
//                 tags on every measurement: type = put | delete | delete_range | get | write | read ; result = success | failure
//              An operation is measured from the moment the batch took it (Decorate*, called by Batch.Add) until its callback
//              ran; a batch from its creation (batch_total) and from the moment it is sent (batch_exec).
//   Divergences:
//      * OTel MeterProvider -> IMeterFactory? (null = a private Meter named "oxia_client", disposed by Dispose()).
//        System.Diagnostics.Metrics instruments report only while a MeterListener (OpenTelemetry, dotnet-counters, a test)
//        subscribed to them.  Names, units and tags are Go's; the byte unit is "By" (common/metric/unit.go), not "bytes".
//      * Time: `time.Time` + `time.Since` are Stopwatch timestamps (`long`); the two clock functions are injectable for the
//        tests (Go: newMetrics(provider, timeFunc, sinceFunc)).
//      * ZERO COST WHEN NOBODY LISTENS: Decorate* returns the call itself, undecorated, if no listener is subscribed to the
//        op instruments (behaviourally equivalent: the only effect of the decoration is the measurement), and the batches
//        skip their clock reads when no batch instrument is enabled.  The check is repeated for every call, so a listener
//        attached later is honoured from then on.
//      * Decoration of a call: Go returned a COPY of the call struct whose Callback was wrapped by a closure.  Here (calls are
//        classes) the decorator returns a copy of the call (a plain PutCall/... whose fields were copied with CopyFrom) whose
//        Callback is the instance method of the decorator object: the original call is completed first (`Complete`, so a
//        pooled call resolves its ValueTask), then the metrics are recorded -- exactly Go's order.  Cost when metrics are
//        enabled: 2 small objects per operation (decorator + its delegate); when disabled: none.  Because the decorated call
//        is the one handed to the rerouters, a re-added call is decorated again, like in Go (each decoration records once).
//      * Measurements use TagList on the stack (inline storage): no allocation per measurement.
//      * The batch metrics callbacks of Go (`WriteCallback()` / `ReadCallback()` returned a closure per batch capturing the
//        batch start) are kept for API fidelity and for the tests, but the batches call RecordWrite / RecordRead directly
//        with the start timestamp they keep in a field: no delegate per batch.
//      * Metrics is IDisposable: it disposes the Meter it created itself (a Meter registered globally would leak per client).
// =====================================================================================================================
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Oxia.Client.Internal.Model;
using Oxia.Common.Proto;

namespace Oxia.Client.Internal.Metrics;

/// <summary>Go: the func returned by WriteCallback().</summary>
public delegate void WriteMetricsCallback(long executionStart, WriteRequest request, WriteResponse? response, Exception? error);

/// <summary>Go: the func returned by ReadCallback().</summary>
public delegate void ReadMetricsCallback(long executionStart, ReadRequest request, ReadResponse? response, Exception? error);

/// <summary>Go: metrics.Metrics.</summary>
public sealed class Metrics : IDisposable
{
    /// <summary>Name of the Meter.</summary>
    public const string MeterName = "oxia_client";

    private readonly Func<long> _timeFunc;
    private readonly Func<long, TimeSpan> _sinceFunc;
    private readonly Meter _meter;
    private readonly bool _ownsMeter;

    private readonly MetricsTimer _opTime;
    private readonly Histogram<long> _opValue;

    private readonly MetricsTimer _batchTotalTime;
    private readonly MetricsTimer _batchExecTime;
    private readonly Histogram<long> _batchValue;
    private readonly Histogram<long> _batchRequests;

    /// <summary>Go: NewMetrics(provider).  <paramref name="meterFactory"/> null: the metrics go to a private Meter.</summary>
    public Metrics(IMeterFactory? meterFactory)
        : this(meterFactory, static () => Stopwatch.GetTimestamp(), static start => Stopwatch.GetElapsedTime(start))
    {
    }

    /// <summary>Go: newMetrics(provider, timeFunc, sinceFunc).</summary>
    internal Metrics(IMeterFactory? meterFactory, Func<long> timeFunc, Func<long, TimeSpan> sinceFunc)
    {
        _timeFunc = timeFunc;
        _sinceFunc = sinceFunc;

        if (meterFactory is null)
        {
            _meter = new Meter(MeterName);
            _ownsMeter = true;
        }
        else
        {
            _meter = meterFactory.Create(new MeterOptions(MeterName));
        }

        _opTime = new MetricsTimer(_meter, "oxia_client_op");
        _opValue = MetricsUtils.NewHistogram(_meter, "oxia_client_op_value", MetricUnit.Bytes);

        _batchTotalTime = new MetricsTimer(_meter, "oxia_client_batch_total");
        _batchExecTime = new MetricsTimer(_meter, "oxia_client_batch_exec");
        _batchValue = MetricsUtils.NewHistogram(_meter, "oxia_client_batch_value", MetricUnit.Bytes);
        _batchRequests = MetricsUtils.NewHistogram(_meter, "oxia_client_batch_request", "");
    }

    /// <summary>The Meter the instruments belong to (tests subscribe to it).</summary>
    internal Meter Meter => _meter;

    /// <summary>True if somebody listens to the per-operation instruments: only then are the calls decorated.</summary>
    internal bool OpEnabled => _opTime.Enabled || _opValue.Enabled;

    /// <summary>True if somebody listens to the per-batch instruments: only then do the batches read the clock.</summary>
    internal bool BatchEnabled => _batchTotalTime.Enabled || _batchExecTime.Enabled || _batchValue.Enabled || _batchRequests.Enabled;

    /// <summary>The current timestamp of the metrics clock (Go: timeFunc()).</summary>
    internal long Now() => _timeFunc();

    public void Dispose()
    {
        if (_ownsMeter) _meter.Dispose();
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Operations
    // -----------------------------------------------------------------------------------------------------------------

    /// <summary>Go: DecoratePut.  Returns <paramref name="put"/> itself if nobody listens.</summary>
    public PutCall DecoratePut(PutCall put) => OpEnabled ? new MeteredPutCall(this, put) : put;

    /// <summary>Go: DecorateDelete.</summary>
    public DeleteCall DecorateDelete(DeleteCall deleteCall) => OpEnabled ? new MeteredDeleteCall(this, deleteCall) : deleteCall;

    /// <summary>Go: DecorateDeleteRange.</summary>
    public DeleteRangeCall DecorateDeleteRange(DeleteRangeCall deleteRange) =>
        OpEnabled ? new MeteredDeleteRangeCall(this, deleteRange) : deleteRange;

    /// <summary>Go: DecorateGet.</summary>
    public GetCall DecorateGet(GetCall get) => OpEnabled ? new MeteredGetCall(this, get) : get;

    /// <summary>Records the end of an operation: the time since <paramref name="start"/> and, if known, the value size.</summary>
    private void RecordOp(string requestType, long start, Exception? err, long valueSize)
    {
        var attrs = MetricsUtils.Attrs(requestType, err);
        _opTime.Record(_sinceFunc(start), attrs);
        if (valueSize >= 0)
        {
            _opValue.Record(valueSize, attrs);
        }
    }

    private sealed class MeteredPutCall : PutCall
    {
        private readonly Metrics _metrics;
        private readonly PutCall _inner;
        private readonly long _start;

        public MeteredPutCall(Metrics metrics, PutCall inner)
        {
            _metrics = metrics;
            _inner = inner;
            _start = metrics._timeFunc();
            CopyFrom(inner);
            Callback = OnComplete;
        }

        private void OnComplete(PutResponse? response, Exception? err)
        {
            _inner.Complete(response, err);
            _metrics.RecordOp("put", _start, err, Value.Length);
        }
    }

    private sealed class MeteredDeleteCall : DeleteCall
    {
        private readonly Metrics _metrics;
        private readonly DeleteCall _inner;
        private readonly long _start;

        public MeteredDeleteCall(Metrics metrics, DeleteCall inner)
        {
            _metrics = metrics;
            _inner = inner;
            _start = metrics._timeFunc();
            CopyFrom(inner);
            Callback = OnComplete;
        }

        private void OnComplete(DeleteResponse? response, Exception? err)
        {
            _inner.Complete(response, err);
            _metrics.RecordOp("delete", _start, err, -1);
        }
    }

    private sealed class MeteredDeleteRangeCall : DeleteRangeCall
    {
        private readonly Metrics _metrics;
        private readonly DeleteRangeCall _inner;
        private readonly long _start;

        public MeteredDeleteRangeCall(Metrics metrics, DeleteRangeCall inner)
        {
            _metrics = metrics;
            _inner = inner;
            _start = metrics._timeFunc();
            CopyFrom(inner);
            Callback = OnComplete;
        }

        private void OnComplete(DeleteRangeResponse? response, Exception? err)
        {
            _inner.Complete(response, err);
            _metrics.RecordOp("delete_range", _start, err, -1);
        }
    }

    private sealed class MeteredGetCall : GetCall
    {
        private readonly Metrics _metrics;
        private readonly GetCall _inner;
        private readonly long _start;

        public MeteredGetCall(Metrics metrics, GetCall inner)
        {
            _metrics = metrics;
            _inner = inner;
            _start = metrics._timeFunc();
            CopyFrom(inner);
            Callback = OnComplete;
        }

        private void OnComplete(GetResponse? response, Exception? err)
        {
            _inner.Complete(response, err);
            _metrics.RecordOp("get", _start, err, response?.Value?.Length ?? 0);
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Batches
    // -----------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Go: WriteCallback().  Captures the batch start (now); the returned delegate records a write batch.  Allocates; the
    /// batches use <see cref="RecordWrite"/>.
    /// </summary>
    public WriteMetricsCallback WriteCallback()
    {
        long batchStart = _timeFunc();
        return (executionStart, request, _, err) => RecordWrite(batchStart, executionStart, request, err);
    }

    /// <summary>Go: ReadCallback().</summary>
    public ReadMetricsCallback ReadCallback()
    {
        long batchStart = _timeFunc();
        return (executionStart, _, response, err) => RecordRead(batchStart, executionStart, response, err);
    }

    /// <summary>The body of the Go write callback: batch_total, batch_exec, batch_value, batch_request.</summary>
    public void RecordWrite(long batchStart, long executionStart, WriteRequest request, Exception? err)
    {
        var attrs = MetricsUtils.Attrs("write", err);
        _batchTotalTime.Record(_sinceFunc(batchStart), attrs);
        _batchExecTime.Record(_sinceFunc(executionStart), attrs);
        var (valueSize, requestCount) = MetricsUtils.WriteMetrics(request);
        _batchValue.Record(valueSize, attrs);
        _batchRequests.Record(requestCount, attrs);
    }

    /// <summary>The body of the Go read callback.</summary>
    public void RecordRead(long batchStart, long executionStart, ReadResponse? response, Exception? err)
    {
        var attrs = MetricsUtils.Attrs("read", err);
        _batchTotalTime.Record(_sinceFunc(batchStart), attrs);
        _batchExecTime.Record(_sinceFunc(executionStart), attrs);
        var (valueSize, requestCount) = MetricsUtils.ReadMetrics(response);
        _batchValue.Record(valueSize, attrs);
        _batchRequests.Record(requestCount, attrs);
    }
}

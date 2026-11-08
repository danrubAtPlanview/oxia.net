// =====================================================================================================================
// PORT NOTE
//   Upstream : common/rpc/grpcmetrics/grpcmetrics_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The grpc_client_* counters (started / handled by code / msg_received / msg_sent) of unary, server-streaming and bidi calls,
//              their labels (grpc_type, grpc_service, grpc_method, grpc_code), unknown status codes, and splitMethodName.
//   Go tests: 6 + 1 benchmark.  Ported: TestUnary, TestServerStream, TestBidiStream, TestMethodMetricsHandledWithUnknownCode,
//              TestSplitMethodNameWithoutService (client assertions only); BenchmarkStreamMsgSent -> BenchmarkDotNet
//              (benchmarks/Oxia.Client.Benchmarks/Common/GrpcMetricsBenchmarks.cs).
//              Skipped: TestRegisterPreInitializesSeries (server-side Register(grpc.Server); the server interceptors are not ported, not
//              used by the client).  The server-side assertions of TestUnary / TestServerStream / TestBidiStream are dropped for the same
//              reason.
//   Divergences:
//      * The gRPC testing.TestService is replaced by the OxiaClient service (generated base, hosted by TestGrpcServer): EmptyCall ->
//        KeepAlive, UnaryCall (InvalidArgument) -> CreateSession, StreamingOutputCall -> GetShardAssignments, FullDuplexCall -> WriteStream.
//      * The OTel SDK ManualReader -> a MeterListener on a private Meter per test (no global state, as Go swaps the meter).
//      * attribute.Set.Encoded -> the sorted "key=value" list of the measurement tags.
// =====================================================================================================================
using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Oxia.Common.Rpc;
using Oxia.TestSupport;
using Xunit;
using G = Io.Oxia.Proto.V1;

namespace Oxia.Client.Tests.Common;

/// <summary>Collects the long counters of one Meter, keyed by instrument name and the encoded tag set.</summary>
internal sealed class MetricCollector : IDisposable
{
    private readonly MeterListener _listener = new();
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, long>> _values = new();
    private readonly ConcurrentDictionary<string, (string Description, string? Unit)> _instruments = new();

    public MetricCollector(Meter meter)
    {
        _listener.InstrumentPublished = (instrument, listener) =>
        {
            if (ReferenceEquals(instrument.Meter, meter))
            {
                _instruments[instrument.Name] = (instrument.Description ?? string.Empty, instrument.Unit);
                listener.EnableMeasurementEvents(instrument);
            }
        };
        _listener.SetMeasurementEventCallback<long>((instrument, measurement, tags, _) =>
        {
            var series = _values.GetOrAdd(instrument.Name, _ => new ConcurrentDictionary<string, long>());
            series.AddOrUpdate(Encode(tags), measurement, (_, current) => current + measurement);
        });
        _listener.Start();
    }

    public static string Encode(ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        var parts = new List<string>();
        foreach (var tag in tags) parts.Add($"{tag.Key}={tag.Value}");
        parts.Sort(StringComparer.Ordinal);
        return string.Join(",", parts);
    }

    public IReadOnlyDictionary<string, long> Series(string name)
        => _values.TryGetValue(name, out var series) ? new Dictionary<string, long>(series) : new Dictionary<string, long>();

    public (string Description, string? Unit) Instrument(string name) => _instruments[name];

    public IReadOnlyCollection<string> InstrumentNames => _instruments.Keys.ToArray();

    public void Dispose() => _listener.Dispose();
}

public class GrpcMetricsTests
{
    private const string TestService = "io.oxia.proto.v1.OxiaClient";

    private sealed class TestService_ : G.OxiaClient.OxiaClientBase
    {
        // EmptyCall
        public override Task<G.KeepAliveResponse> KeepAlive(G.SessionHeartbeat request, ServerCallContext context)
            => Task.FromResult(new G.KeepAliveResponse());

        // UnaryCall
        public override Task<G.CreateSessionResponse> CreateSession(G.CreateSessionRequest request, ServerCallContext context)
            => throw new RpcException(new Status(StatusCode.InvalidArgument, "invalid"));

        // StreamingOutputCall: 3 responses
        public override async Task GetShardAssignments(G.ShardAssignmentsRequest request, IServerStreamWriter<G.ShardAssignments> responseStream,
            ServerCallContext context)
        {
            for (var i = 0; i < 3; i++) await responseStream.WriteAsync(new G.ShardAssignments());
        }

        // FullDuplexCall: one response per request
        public override async Task WriteStream(IAsyncStreamReader<G.WriteRequest> requestStream, IServerStreamWriter<G.WriteResponse> responseStream,
            ServerCallContext context)
        {
            while (await requestStream.MoveNext(context.CancellationToken))
            {
                await responseStream.WriteAsync(new G.WriteResponse());
            }
        }
    }

    private sealed class Fixture : IAsyncDisposable
    {
        public required TestGrpcServer Server { get; init; }
        public required GrpcChannel Channel { get; init; }
        public required Meter Meter { get; init; }
        public required MetricCollector Collector { get; init; }
        public required GrpcClientMetricsInterceptor Interceptor { get; init; }
        public required G.OxiaClient.OxiaClientClient Client { get; init; }

        public async ValueTask DisposeAsync()
        {
            Channel.Dispose();
            Collector.Dispose();
            Meter.Dispose();
            await Server.DisposeAsync();
        }
    }

    private static async Task<Fixture> NewTestClient()
    {
        var server = await TestGrpcServer.StartAsync(app => app.MapGrpcService<TestService_>());
        var meter = new Meter("oxia-test-" + Guid.NewGuid());
        var collector = new MetricCollector(meter);
        var interceptor = new GrpcClientMetricsInterceptor(meter);
        var channel = GrpcChannel.ForAddress("http://" + server.Address);
        return new Fixture
        {
            Server = server,
            Channel = channel,
            Meter = meter,
            Collector = collector,
            Interceptor = interceptor,
            Client = new G.OxiaClient.OxiaClientClient(channel.CreateCallInvoker().Intercept(interceptor)),
        };
    }

    /// <summary>Go: labelSet(rpc, method, code...).</summary>
    private static string LabelSet(string rpcType, string method, string? code = null)
    {
        var tags = new List<KeyValuePair<string, object?>>
        {
            new("grpc_type", rpcType),
            new("grpc_service", TestService),
            new("grpc_method", method),
        };
        if (code is not null) tags.Add(new("grpc_code", code));
        return MetricCollector.Encode(tags.ToArray());
    }

    private static void AssertMetrics(MetricCollector collector, string rpcType, string method, long started, long received, long sent, string code)
    {
        long Value(string name, string labels)
            => collector.Series($"grpc_client_{name}_total").GetValueOrDefault(labels);

        var labels = LabelSet(rpcType, method);
        Assert.Equal(started, Value("started", labels));
        Assert.Equal(received, Value("msg_received", labels));
        Assert.Equal(sent, Value("msg_sent", labels));
        Assert.Equal(started, Value("handled", LabelSet(rpcType, method, code)));
    }

    [Fact]
    public async Task TestUnary()
    {
        await using var f = await NewTestClient();

        await f.Client.KeepAliveAsync(new G.SessionHeartbeat());
        AssertMetrics(f.Collector, "unary", "KeepAlive", started: 1, received: 1, sent: 1, code: "OK");

        var err = await Assert.ThrowsAsync<RpcException>(async () => await f.Client.CreateSessionAsync(new G.CreateSessionRequest()));
        Assert.Equal(StatusCode.InvalidArgument, err.StatusCode);
        AssertMetrics(f.Collector, "unary", "CreateSession", started: 1, received: 0, sent: 1, code: "InvalidArgument");
    }

    [Fact]
    public async Task TestServerStream()
    {
        await using var f = await NewTestClient();

        using var stream = f.Client.GetShardAssignments(new G.ShardAssignmentsRequest());
        for (var i = 0; i < 3; i++)
        {
            Assert.True(await stream.ResponseStream.MoveNext(CancellationToken.None));
        }

        Assert.False(await stream.ResponseStream.MoveNext(CancellationToken.None)); // io.EOF

        AssertMetrics(f.Collector, "server_stream", "GetShardAssignments", started: 1, received: 3, sent: 1, code: "OK");
    }

    [Fact]
    public async Task TestBidiStream()
    {
        await using var f = await NewTestClient();

        using var stream = f.Client.WriteStream();
        for (var i = 0; i < 2; i++)
        {
            await stream.RequestStream.WriteAsync(new G.WriteRequest());
            Assert.True(await stream.ResponseStream.MoveNext(CancellationToken.None));
        }

        await stream.RequestStream.CompleteAsync();
        Assert.False(await stream.ResponseStream.MoveNext(CancellationToken.None)); // io.EOF

        AssertMetrics(f.Collector, "bidi_stream", "WriteStream", started: 1, received: 2, sent: 2, code: "OK");
    }

    [Fact(Skip = "Server side of grpcmetrics (Register(grpc.Server), server interceptors) is not used by the client and is not ported")]
    public void TestRegisterPreInitializesSeries()
    {
    }

    [Fact]
    public void TestMethodMetricsHandledWithUnknownCode()
    {
        using var meter = new Meter("oxia-test-" + Guid.NewGuid());
        using var collector = new MetricCollector(meter);
        var interceptor = new GrpcClientMetricsInterceptor(meter);

        interceptor.GetMethodMetrics(GrpcRpcType.Unary, $"/{TestService}/EmptyCall")
            .HandledWith(new RpcException(new Status((StatusCode)100, "unknown")));

        var values = collector.Series("grpc_client_handled_total");
        Assert.Equal(1L, values[LabelSet("unary", "EmptyCall", "Code(100)")]);
    }

    [Fact]
    public void TestSplitMethodNameWithoutService()
    {
        var (service, method) = GrpcClientMetricsInterceptor.SplitMethodName("/EmptyCall");
        Assert.Equal("unknown", service);
        Assert.Equal("unknown", method);
    }

    // ----- extra tests (not in Go) --------------------------------------------------------------------------------------
    [Fact]
    public async Task TheInstrumentsHaveTheGoNamesUnitsAndDescriptions()
    {
        await using var f = await NewTestClient();

        Assert.Equal(("Total number of RPCs started on the client.", "1"), f.Collector.Instrument("grpc_client_started_total"));
        Assert.Equal(("Total number of RPCs completed by the client, regardless of success or failure.", "1"), f.Collector.Instrument("grpc_client_handled_total"));
        Assert.Equal(("Total number of RPC stream messages received by the client.", "1"), f.Collector.Instrument("grpc_client_msg_received_total"));
        Assert.Equal(("Total number of gRPC stream messages sent by the client.", "1"), f.Collector.Instrument("grpc_client_msg_sent_total"));
        Assert.Equal(4, f.Collector.InstrumentNames.Count);
        Assert.Equal("oxia", GrpcClientMetricsInterceptor.MeterName);
    }

    [Fact]
    public async Task TheSeriesAccumulateAcrossCalls()
    {
        await using var f = await NewTestClient();

        for (var i = 0; i < 5; i++) await f.Client.KeepAliveAsync(new G.SessionHeartbeat());
        for (var i = 0; i < 2; i++) await Assert.ThrowsAsync<RpcException>(async () => await f.Client.CreateSessionAsync(new G.CreateSessionRequest()));

        AssertMetrics(f.Collector, "unary", "KeepAlive", started: 5, received: 5, sent: 5, code: "OK");
        AssertMetrics(f.Collector, "unary", "CreateSession", started: 2, received: 0, sent: 2, code: "InvalidArgument");
    }

    [Fact]
    public async Task ABlockingUnaryCallIsCountedToo()
    {
        await using var f = await NewTestClient();

        f.Client.KeepAlive(new G.SessionHeartbeat());
        Assert.Throws<RpcException>(() => f.Client.CreateSession(new G.CreateSessionRequest()));

        AssertMetrics(f.Collector, "unary", "KeepAlive", started: 1, received: 1, sent: 1, code: "OK");
        AssertMetrics(f.Collector, "unary", "CreateSession", started: 1, received: 0, sent: 1, code: "InvalidArgument");
    }

    [Fact]
    public async Task ACancelledStreamIsHandledAsCanceled()
    {
        await using var f = await NewTestClient();
        using var cts = new CancellationTokenSource();

        using var stream = f.Client.WriteStream(cancellationToken: cts.Token);
        await stream.RequestStream.WriteAsync(new G.WriteRequest());
        Assert.True(await stream.ResponseStream.MoveNext(CancellationToken.None));
        cts.Cancel();
        await Assert.ThrowsAnyAsync<Exception>(async () => await stream.ResponseStream.MoveNext(CancellationToken.None));

        AssertMetrics(f.Collector, "bidi_stream", "WriteStream", started: 1, received: 1, sent: 1, code: "Canceled");
    }

    [Fact]
    public void CodeNamesAreTheGoNames()
    {
        string[] expected =
        [
            "OK", "Canceled", "Unknown", "InvalidArgument", "DeadlineExceeded", "NotFound", "AlreadyExists", "PermissionDenied",
            "ResourceExhausted", "FailedPrecondition", "Aborted", "OutOfRange", "Unimplemented", "Internal", "Unavailable", "DataLoss",
            "Unauthenticated",
        ];
        for (var code = 0; code < expected.Length; code++) Assert.Equal(expected[code], GrpcClientMetricsInterceptor.CodeName(code));
        Assert.Equal("Code(17)", GrpcClientMetricsInterceptor.CodeName(17));
        Assert.Equal(GrpcClientMetricsInterceptor.NumCodes, expected.Length);
    }

    [Fact]
    public void ErrorsMapToTheirStatusCode()
    {
        Assert.Equal((int)StatusCode.NotFound, GrpcClientMetricsInterceptor.CodeOf(new RpcException(new Status(StatusCode.NotFound, ""))));
        Assert.Equal((int)StatusCode.Cancelled, GrpcClientMetricsInterceptor.CodeOf(new OperationCanceledException()));
        Assert.Equal((int)StatusCode.Unknown, GrpcClientMetricsInterceptor.CodeOf(new InvalidOperationException()));
    }

    [Fact]
    public void SplitMethodName_HandlesTheUsualForms()
    {
        Assert.Equal(("pkg.Service", "Method"), GrpcClientMetricsInterceptor.SplitMethodName("/pkg.Service/Method"));
        Assert.Equal(("pkg.Service", "Method"), GrpcClientMetricsInterceptor.SplitMethodName("pkg.Service/Method"));
        Assert.Equal(("unknown", "unknown"), GrpcClientMetricsInterceptor.SplitMethodName(""));
    }

    [Fact]
    public void TheDefaultInterceptorIsShared()
    {
        Assert.Same(GrpcClientMetricsInterceptor.Default, GrpcClientMetricsInterceptor.Default);
    }

    [Fact]
    public void CountingAStreamMessageDoesNotAllocate()
    {
        using var meter = new Meter("oxia-test-" + Guid.NewGuid());
        var interceptor = new GrpcClientMetricsInterceptor(meter);
        var m = interceptor.GetMethodMetrics(GrpcRpcType.BidiStream, "/replication.OxiaLogReplication/Replicate");

        for (var i = 0; i < 100; i++)
        {
            m.Sent();
            m.Received();
            m.Handled((int)StatusCode.OK); // builds the cached label set of the code once
        }

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 10_000; i++)
        {
            m.Sent();
            m.Received();
            m.Handled((int)StatusCode.OK);
        }

        Assert.Equal(0, GC.GetAllocatedBytesForCurrentThread() - before);
    }
}

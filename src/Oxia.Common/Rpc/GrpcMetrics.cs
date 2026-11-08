// =====================================================================================================================
// PORT NOTE
//   Upstream : common/rpc/grpcmetrics/grpcmetrics.go   (client side: UnaryClientInterceptor, StreamClientInterceptor,
//              monitoredClientStream, getMethodMetrics, methodMetrics, streamType, splitMethodName)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The same series as github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 for the gRPC client:
//                grpc_client_started_total, grpc_client_handled_total (+ grpc_code), grpc_client_msg_received_total,
//                grpc_client_msg_sent_total, all dimensionless counters with the labels grpc_type (unary | client_stream |
//                server_stream | bidi_stream), grpc_service, grpc_method.  The counters of each method are resolved once and
//                cached, so a stream message costs a single counter increment (no tag allocation).
//                Semantics: unary -> started, sent; on success received; handled(code) with the final status.  Streams ->
//                started; every message written counts as sent, every message read as received; the end of the response
//                stream (io.EOF in Go) is handled(OK), any read error is handled(code of the error); a call that cannot be
//                created is handled(code).
//   Divergences:
//      * OTel Go meter "oxia" (observable counters over striped adders) -> System.Diagnostics.Metrics.Meter "oxia" with
//        synchronous Counter<long> instruments of the same names / unit "1" / descriptions; the host picks them up with a
//        MeterListener or OpenTelemetry's AddMeter("oxia").  The meter can be replaced (tests) through the constructor.
//      * The server half of the package (UnaryServerInterceptor, StreamServerInterceptor, Register) is not used by the client and
//        is not ported.
//      * grpc.ClientStream (SendMsg / RecvMsg) -> IClientStreamWriter / IAsyncStreamReader wrappers; the single request of a
//        server-streaming call is counted as sent when the call is created.  A reader that is never read to its end never
//        reports handled (same as Go, where only RecvMsg reports it).
//      * Go's status.FromError(err).Code() for a non-gRPC error is Unknown; OperationCanceledException is reported as Canceled.
//        Go's code names ("Canceled", ...) are kept for the grpc_code label, `Code(n)` for codes above Unauthenticated.
// =====================================================================================================================
using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Grpc.Core;
using Grpc.Core.Interceptors;

namespace Oxia.Common.Rpc;

/// <summary>Go: the <c>rpcType</c> label values.</summary>
internal enum GrpcRpcType
{
    Unary,
    ClientStream,
    ServerStream,
    BidiStream,
}

/// <summary>The client interceptor that exports the grpc_client_* counters.</summary>
public sealed class GrpcClientMetricsInterceptor : Interceptor
{
    public const string MeterName = "oxia";

    internal const int NumCodes = (int)StatusCode.Unauthenticated + 1;

    internal const string StartedName = "grpc_client_started_total";
    internal const string StartedHelp = "Total number of RPCs started on the client.";
    internal const string HandledName = "grpc_client_handled_total";
    internal const string HandledHelp = "Total number of RPCs completed by the client, regardless of success or failure.";
    internal const string MsgReceivedName = "grpc_client_msg_received_total";
    internal const string ReceivedHelp = "Total number of RPC stream messages received by the client.";
    internal const string MsgSentName = "grpc_client_msg_sent_total";
    internal const string SentHelp = "Total number of gRPC stream messages sent by the client.";

    /// <summary>Go's status code names (<c>codes.Code.String()</c>), indexed by code.</summary>
    private static readonly string[] CodeNames =
    [
        "OK", "Canceled", "Unknown", "InvalidArgument", "DeadlineExceeded", "NotFound", "AlreadyExists", "PermissionDenied",
        "ResourceExhausted", "FailedPrecondition", "Aborted", "OutOfRange", "Unimplemented", "Internal", "Unavailable", "DataLoss",
        "Unauthenticated",
    ];

    private static readonly Lazy<GrpcClientMetricsInterceptor> DefaultInstance = new(() => new GrpcClientMetricsInterceptor(new Meter(MeterName)));

    /// <summary>The interceptor every pooled connection uses (global meter "oxia").</summary>
    public static GrpcClientMetricsInterceptor Default => DefaultInstance.Value;

    private readonly Counter<long> _started;
    private readonly Counter<long> _handled;
    private readonly Counter<long> _received;
    private readonly Counter<long> _sent;
    private readonly ConcurrentDictionary<(GrpcRpcType Type, string FullMethod), MethodMetrics> _methods = new();

    public GrpcClientMetricsInterceptor(Meter meter)
    {
        ArgumentNullException.ThrowIfNull(meter);
        _started = meter.CreateCounter<long>(StartedName, "1", StartedHelp);
        _handled = meter.CreateCounter<long>(HandledName, "1", HandledHelp);
        _received = meter.CreateCounter<long>(MsgReceivedName, "1", ReceivedHelp);
        _sent = meter.CreateCounter<long>(MsgSentName, "1", SentHelp);
    }

    /// <summary>Go: <c>splitMethodName</c> -- "/pkg.Service/Method" -> ("pkg.Service", "Method"), ("unknown", "unknown") when malformed.</summary>
    internal static (string Service, string Method) SplitMethodName(string fullMethod)
    {
        if (fullMethod.StartsWith('/')) fullMethod = fullMethod[1..];
        var i = fullMethod.IndexOf('/');
        return i >= 0 ? (fullMethod[..i], fullMethod[(i + 1)..]) : ("unknown", "unknown");
    }

    internal static string CodeName(int code) => (uint)code < CodeNames.Length ? CodeNames[code] : $"Code({(uint)code})";

    internal static string TypeName(GrpcRpcType type) => type switch
    {
        GrpcRpcType.Unary => "unary",
        GrpcRpcType.ClientStream => "client_stream",
        GrpcRpcType.ServerStream => "server_stream",
        _ => "bidi_stream",
    };

    /// <summary>Go: <c>getMethodMetrics(client = true, t, fullMethod)</c>.</summary>
    internal MethodMetrics GetMethodMetrics(GrpcRpcType type, string fullMethod)
    {
        var key = (type, fullMethod);
        if (_methods.TryGetValue(key, out var existing))
        {
            return existing;
        }

        return _methods.GetOrAdd(key, static (k, self) => new MethodMetrics(self, k.Type, k.FullMethod), this);
    }

    private MethodMetrics Start(GrpcRpcType type, string fullMethod)
    {
        var m = GetMethodMetrics(type, fullMethod);
        m.Started();
        return m;
    }

    /// <summary>The counters of one method on the client side.</summary>
    internal sealed class MethodMetrics
    {
        private readonly GrpcClientMetricsInterceptor _owner;
        private readonly KeyValuePair<string, object?>[] _labels;
        private readonly KeyValuePair<string, object?>[]?[] _handledLabels = new KeyValuePair<string, object?>[]?[NumCodes];

        internal MethodMetrics(GrpcClientMetricsInterceptor owner, GrpcRpcType type, string fullMethod)
        {
            _owner = owner;
            var (service, method) = SplitMethodName(fullMethod);
            _labels =
            [
                new("grpc_type", TypeName(type)),
                new("grpc_service", service),
                new("grpc_method", method),
            ];
        }

        public void Started() => _owner._started.Add(1, _labels);

        public void Received() => _owner._received.Add(1, _labels);

        public void Sent() => _owner._sent.Add(1, _labels);

        /// <summary>Go: <c>handledWith(err)</c>.</summary>
        public void HandledWith(Exception? error) => Handled(error is null ? (int)StatusCode.OK : CodeOf(error));

        public void Handled(int code)
        {
            if ((uint)code < NumCodes)
            {
                var labels = _handledLabels[code] ??= WithCode(code);
                _owner._handled.Add(1, labels);
            }
            else
            {
                _owner._handled.Add(1, WithCode(code));
            }
        }

        private KeyValuePair<string, object?>[] WithCode(int code)
            => [_labels[0], _labels[1], _labels[2], new("grpc_code", CodeName(code))];
    }

    internal static int CodeOf(Exception error) => error switch
    {
        RpcException rpc => (int)rpc.StatusCode,
        OperationCanceledException => (int)StatusCode.Cancelled,
        AggregateException { InnerException: { } inner } => CodeOf(inner),
        _ => (int)StatusCode.Unknown,
    };

    // ----- interceptor ---------------------------------------------------------------------------------------------------
    public override TResponse BlockingUnaryCall<TRequest, TResponse>(
        TRequest request, ClientInterceptorContext<TRequest, TResponse> context, BlockingUnaryCallContinuation<TRequest, TResponse> continuation)
    {
        var m = Start(GrpcRpcType.Unary, context.Method.FullName);
        m.Sent();
        try
        {
            var response = continuation(request, context);
            m.Received();
            m.HandledWith(null);
            return response;
        }
        catch (Exception e)
        {
            m.HandledWith(e);
            throw;
        }
    }

    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        TRequest request, ClientInterceptorContext<TRequest, TResponse> context, AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
    {
        var m = Start(GrpcRpcType.Unary, context.Method.FullName);
        m.Sent();
        var call = continuation(request, context);
        return new AsyncUnaryCall<TResponse>(
            ObserveUnaryAsync(call.ResponseAsync, m), call.ResponseHeadersAsync, call.GetStatus, call.GetTrailers, call.Dispose);
    }

    private static async Task<TResponse> ObserveUnaryAsync<TResponse>(Task<TResponse> response, MethodMetrics m)
    {
        try
        {
            var result = await response.ConfigureAwait(false);
            m.Received();
            m.HandledWith(null);
            return result;
        }
        catch (Exception e)
        {
            m.HandledWith(e);
            throw;
        }
    }

    public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
        TRequest request, ClientInterceptorContext<TRequest, TResponse> context, AsyncServerStreamingCallContinuation<TRequest, TResponse> continuation)
    {
        var m = Start(GrpcRpcType.ServerStream, context.Method.FullName);
        AsyncServerStreamingCall<TResponse> call;
        try
        {
            call = continuation(request, context);
        }
        catch (Exception e)
        {
            m.HandledWith(e);
            throw;
        }

        m.Sent(); // the single request message
        return new AsyncServerStreamingCall<TResponse>(
            new MonitoredStreamReader<TResponse>(call.ResponseStream, m), call.ResponseHeadersAsync, call.GetStatus, call.GetTrailers, call.Dispose);
    }

    public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
        ClientInterceptorContext<TRequest, TResponse> context, AsyncClientStreamingCallContinuation<TRequest, TResponse> continuation)
    {
        var m = Start(GrpcRpcType.ClientStream, context.Method.FullName);
        AsyncClientStreamingCall<TRequest, TResponse> call;
        try
        {
            call = continuation(context);
        }
        catch (Exception e)
        {
            m.HandledWith(e);
            throw;
        }

        return new AsyncClientStreamingCall<TRequest, TResponse>(
            new MonitoredStreamWriter<TRequest>(call.RequestStream, m),
            ObserveUnaryAsync(call.ResponseAsync, m),
            call.ResponseHeadersAsync, call.GetStatus, call.GetTrailers, call.Dispose);
    }

    public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
        ClientInterceptorContext<TRequest, TResponse> context, AsyncDuplexStreamingCallContinuation<TRequest, TResponse> continuation)
    {
        var m = Start(GrpcRpcType.BidiStream, context.Method.FullName);
        AsyncDuplexStreamingCall<TRequest, TResponse> call;
        try
        {
            call = continuation(context);
        }
        catch (Exception e)
        {
            m.HandledWith(e);
            throw;
        }

        return new AsyncDuplexStreamingCall<TRequest, TResponse>(
            new MonitoredStreamWriter<TRequest>(call.RequestStream, m),
            new MonitoredStreamReader<TResponse>(call.ResponseStream, m),
            call.ResponseHeadersAsync, call.GetStatus, call.GetTrailers, call.Dispose);
    }

    /// <summary>Go: monitoredClientStream.RecvMsg.  A read that completes synchronously costs no allocation.</summary>
    private sealed class MonitoredStreamReader<T>(IAsyncStreamReader<T> inner, MethodMetrics m) : IAsyncStreamReader<T>
    {
        public T Current => inner.Current;

        public Task<bool> MoveNext(CancellationToken cancellationToken)
        {
            Task<bool> next;
            try
            {
                next = inner.MoveNext(cancellationToken);
            }
            catch (Exception e)
            {
                m.HandledWith(e);
                throw;
            }

            if (next.IsCompletedSuccessfully)
            {
                Observe(next.Result);
                return next;
            }

            return ObserveAsync(next);
        }

        private async Task<bool> ObserveAsync(Task<bool> next)
        {
            bool hasNext;
            try
            {
                hasNext = await next.ConfigureAwait(false);
            }
            catch (Exception e)
            {
                m.HandledWith(e);
                throw;
            }

            Observe(hasNext);
            return hasNext;
        }

        private void Observe(bool hasNext)
        {
            if (hasNext)
            {
                m.Received();
            }
            else
            {
                // io.EOF
                m.HandledWith(null);
            }
        }
    }

    /// <summary>Go: monitoredClientStream.SendMsg.  A write that completes synchronously costs no allocation.</summary>
    private sealed class MonitoredStreamWriter<T>(IClientStreamWriter<T> inner, MethodMetrics m) : IClientStreamWriter<T>
    {
        public WriteOptions? WriteOptions
        {
            get => inner.WriteOptions;
            set => inner.WriteOptions = value;
        }

        public Task WriteAsync(T message) => Observe(inner.WriteAsync(message));

        public Task WriteAsync(T message, CancellationToken cancellationToken) => Observe(inner.WriteAsync(message, cancellationToken));

        public Task CompleteAsync() => inner.CompleteAsync();

        private Task Observe(Task write)
        {
            if (write.IsCompletedSuccessfully)
            {
                m.Sent();
                return write;
            }

            return ObserveAsync(write);
        }

        private async Task ObserveAsync(Task write)
        {
            await write.ConfigureAwait(false);
            m.Sent();
        }
    }
}

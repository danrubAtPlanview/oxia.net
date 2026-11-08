// =====================================================================================================================
// PORT NOTE
//   Upstream : common/proto/client_grpc.pb.go (the generated OxiaClientClient and its *_Client stream types),
//              common/rpc/oxia_error_streaming_client.go (OxiaErrorServerStreamingClient / OxiaErrorBidiStreamingClient),
//              common/rpc/logging_client_rpc.go (loggingClientRpc: a pure pass-through decorator that logs nothing)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The OxiaClient service over gRPC.NET with CUSTOM MARSHALLERS (Oxia.Common.Proto.OxiaClientMethods) so messages
//              are encoded straight into the transport's IBufferWriter and decoded straight from its ReadOnlySequence -- no
//              Google.Protobuf objects, no intermediate byte[].  Every Recv/Send/unary failure passes through
//              OxiaErrors.FromGrpcError, exactly like Go's Oxia* stream wrappers.
//   Divergences:
//      * Go `Recv() (msg, io.EOF)` is `RecvAsync()` returning null at the end of the stream; failures are thrown.
//      * Error translation KEEPS the original exception: when FromGrpcError maps the failure to an Oxia sentinel, the
//        thrown exception is a TranslatedGrpcException whose Message is the sentinel's, which satisfies
//        OxiaErrors.Is(thrown, sentinel) (it implements IErrorIs) and whose InnerException is the original RpcException,
//        so callers can still read the leader-hint metadata (FromGrpcError(thrown)) and the gRPC status code
//        (GrpcCode(thrown)).  Go loses that information in the wrappers; the .NET callers (RpcProvider) need it.
//        A failure that is not an Oxia error (RpcException without oxia.io details, OperationCanceledException, ...) is
//        rethrown unchanged, as Go returns the same error.
//      * loggingClientRpc is merged into this class (it only forwards); its tests are ported against this class
//        (tests/Oxia.Client.Tests/Proto/LoggingClientRpcTests.cs).
//      * The deprecated unary `Write` is available as WriteAsync (not part of IOxiaClientRpc).
//      * IWriteStreamCall.Context is cancelled when the stream is observed to end (Recv returned null / failed, Send failed,
//        Dispose, or the parent token was cancelled); gRPC.NET offers no "call finished" event to hook otherwise.
// =====================================================================================================================
using System.Diagnostics.CodeAnalysis;
using Grpc.Core;
using Oxia.Common.Constant;
using Oxia.Common.Proto;

namespace Oxia.Common.Rpc;

/// <summary>
/// An Oxia error returned by the server, translated to its sentinel but still carrying the original gRPC failure as
/// <see cref="Exception.InnerException"/>.  <c>OxiaErrors.Is(thrown, Sentinel)</c> is true.
/// </summary>
public sealed class TranslatedGrpcException : OxiaException, IErrorIs
{
    public TranslatedGrpcException(Exception sentinel, Exception original) : base(sentinel.Message, original)
    {
        Sentinel = sentinel;
    }

    /// <summary>The Oxia sentinel (e.g. <see cref="OxiaErrors.NodeIsNotLeader"/>) this failure maps to.</summary>
    public Exception Sentinel { get; }

    bool IErrorIs.Is(Exception target) => OxiaErrors.Is(Sentinel, target);
}

internal static class GrpcErrors
{
    /// <summary>
    /// Exception-filter helper: true (with the exception to throw) when <see cref="OxiaErrors.FromGrpcError"/> maps
    /// <paramref name="ex"/> to a different error; false leaves the original exception propagating untouched.
    /// </summary>
    public static bool TryTranslate(Exception ex, [NotNullWhen(true)] out Exception? translated)
    {
        (Exception? error, _) = OxiaErrors.FromGrpcError(ex);
        if (error is null || ReferenceEquals(error, ex))
        {
            translated = null;
            return false;
        }
        translated = error is OxiaException ? new TranslatedGrpcException(error, ex) : error;
        return true;
    }
}

public sealed class OxiaClientRpc : IOxiaClientRpc
{
    private readonly CallInvoker _invoker;

    public OxiaClientRpc(CallInvoker invoker)
    {
        ArgumentNullException.ThrowIfNull(invoker);
        _invoker = invoker;
    }

    public IWriteStreamCall WriteStream(Metadata? headers, CancellationToken ct)
        => new WriteStreamCall(_invoker.AsyncDuplexStreamingCall(OxiaClientMethods.WriteStream, null, new CallOptions(headers, cancellationToken: ct)), ct);

    public IServerStreamingClient<ReadResponse> Read(ReadRequest request, CancellationToken ct)
        => new ServerStreamingClient<ReadResponse>(_invoker.AsyncServerStreamingCall(OxiaClientMethods.Read, null, new CallOptions(cancellationToken: ct), request));

    public IServerStreamingClient<ListResponse> List(ListRequest request, CancellationToken ct)
        => new ServerStreamingClient<ListResponse>(_invoker.AsyncServerStreamingCall(OxiaClientMethods.List, null, new CallOptions(cancellationToken: ct), request));

    public IServerStreamingClient<RangeScanResponse> RangeScan(RangeScanRequest request, CancellationToken ct)
        => new ServerStreamingClient<RangeScanResponse>(_invoker.AsyncServerStreamingCall(OxiaClientMethods.RangeScan, null, new CallOptions(cancellationToken: ct), request));

    public IServerStreamingClient<ShardAssignments> GetShardAssignments(ShardAssignmentsRequest request, CancellationToken ct)
        => new ServerStreamingClient<ShardAssignments>(_invoker.AsyncServerStreamingCall(OxiaClientMethods.GetShardAssignments, null, new CallOptions(cancellationToken: ct), request));

    public IServerStreamingClient<GetSequenceUpdatesResponse> GetSequenceUpdates(GetSequenceUpdatesRequest request, CancellationToken ct)
        => new ServerStreamingClient<GetSequenceUpdatesResponse>(_invoker.AsyncServerStreamingCall(OxiaClientMethods.GetSequenceUpdates, null, new CallOptions(cancellationToken: ct), request));

    public IServerStreamingClient<NotificationBatch> GetNotifications(NotificationsRequest request, CancellationToken ct)
        => new ServerStreamingClient<NotificationBatch>(_invoker.AsyncServerStreamingCall(OxiaClientMethods.GetNotifications, null, new CallOptions(cancellationToken: ct), request));

    public async ValueTask<CreateSessionResponse> CreateSessionAsync(CreateSessionRequest request, CancellationToken ct)
    {
        using var call = _invoker.AsyncUnaryCall(OxiaClientMethods.CreateSession, null, new CallOptions(cancellationToken: ct), request);
        try
        {
            return await call.ResponseAsync.ConfigureAwait(false);
        }
        catch (Exception ex) when (GrpcErrors.TryTranslate(ex, out Exception? translated))
        {
            throw translated;
        }
    }

    public async ValueTask<KeepAliveResponse> KeepAliveAsync(SessionHeartbeat request, CancellationToken ct)
    {
        using var call = _invoker.AsyncUnaryCall(OxiaClientMethods.KeepAlive, null, new CallOptions(cancellationToken: ct), request);
        try
        {
            return await call.ResponseAsync.ConfigureAwait(false);
        }
        catch (Exception ex) when (GrpcErrors.TryTranslate(ex, out Exception? translated))
        {
            throw translated;
        }
    }

    public async ValueTask<CloseSessionResponse> CloseSessionAsync(CloseSessionRequest request, CancellationToken ct)
    {
        using var call = _invoker.AsyncUnaryCall(OxiaClientMethods.CloseSession, null, new CallOptions(cancellationToken: ct), request);
        try
        {
            return await call.ResponseAsync.ConfigureAwait(false);
        }
        catch (Exception ex) when (GrpcErrors.TryTranslate(ex, out Exception? translated))
        {
            throw translated;
        }
    }

    /// <summary>The deprecated unary <c>Write</c> rpc (Go: OxiaClientClient.Write).  New code uses <see cref="WriteStream"/>.</summary>
    public async ValueTask<WriteResponse> WriteAsync(WriteRequest request, CancellationToken ct)
    {
        using var call = _invoker.AsyncUnaryCall(OxiaClientMethods.Write, null, new CallOptions(cancellationToken: ct), request);
        try
        {
            return await call.ResponseAsync.ConfigureAwait(false);
        }
        catch (Exception ex) when (GrpcErrors.TryTranslate(ex, out Exception? translated))
        {
            throw translated;
        }
    }
}

/// <summary>Go: OxiaErrorServerStreamingClient[T] over grpc.ServerStreamingClient[T].</summary>
internal sealed class ServerStreamingClient<T> : IServerStreamingClient<T> where T : class
{
    private readonly AsyncServerStreamingCall<T> _call;
    private readonly IAsyncStreamReader<T> _reader;

    public ServerStreamingClient(AsyncServerStreamingCall<T> call)
    {
        _call = call;
        _reader = call.ResponseStream;
    }

    public async ValueTask<T?> RecvAsync(CancellationToken ct = default)
    {
        try
        {
            return await _reader.MoveNext(ct).ConfigureAwait(false) ? _reader.Current : null;
        }
        catch (Exception ex) when (GrpcErrors.TryTranslate(ex, out Exception? translated))
        {
            throw translated;
        }
    }

    public void Dispose() => _call.Dispose();
}

/// <summary>Go: OxiaErrorBidiStreamingClient[WriteRequest, WriteResponse] over proto.OxiaClient_WriteStreamClient.</summary>
internal sealed class WriteStreamCall : IWriteStreamCall
{
    // Shared, already-cancelled source handed out as Context after Dispose (a disposed CTS cannot give out its token).
    private static readonly CancellationTokenSource s_cancelled = CreateCancelled();

    private readonly AsyncDuplexStreamingCall<WriteRequest, WriteResponse> _call;
    private readonly CancellationToken _parent;
    private CancellationTokenSource? _context;
    private int _ended;

    public WriteStreamCall(AsyncDuplexStreamingCall<WriteRequest, WriteResponse> call, CancellationToken parent)
    {
        _call = call;
        _parent = parent;
    }

    public CancellationToken Context
    {
        get
        {
            CancellationTokenSource? cts = Volatile.Read(ref _context);
            if (cts is null)
            {
                CancellationTokenSource created = _parent.CanBeCanceled
                    ? CancellationTokenSource.CreateLinkedTokenSource(_parent)
                    : new CancellationTokenSource();
                cts = Interlocked.CompareExchange(ref _context, created, null);
                if (cts is null)
                {
                    cts = created;
                    if (Volatile.Read(ref _ended) != 0) cts.Cancel();
                }
                else
                {
                    created.Dispose();
                }
            }
            return cts.Token;
        }
    }

    private void MarkEnded()
    {
        if (Interlocked.Exchange(ref _ended, 1) == 0) Volatile.Read(ref _context)?.Cancel();
    }

    public async ValueTask SendAsync(WriteRequest request, CancellationToken ct = default)
    {
        try
        {
            if (ct.CanBeCanceled) await _call.RequestStream.WriteAsync(request, ct).ConfigureAwait(false);
            else await _call.RequestStream.WriteAsync(request).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            MarkEnded();
            if (GrpcErrors.TryTranslate(ex, out Exception? translated)) throw translated;
            throw;
        }
    }

    public async ValueTask<WriteResponse?> RecvAsync(CancellationToken ct = default)
    {
        try
        {
            if (await _call.ResponseStream.MoveNext(ct).ConfigureAwait(false)) return _call.ResponseStream.Current;
            MarkEnded();
            return null;
        }
        catch (Exception ex)
        {
            MarkEnded();
            if (GrpcErrors.TryTranslate(ex, out Exception? translated)) throw translated;
            throw;
        }
    }

    public async ValueTask CloseSendAsync()
    {
        try
        {
            await _call.RequestStream.CompleteAsync().ConfigureAwait(false);
        }
        catch (Exception ex) when (GrpcErrors.TryTranslate(ex, out Exception? translated))
        {
            throw translated;
        }
    }

    public void Dispose()
    {
        MarkEnded();
        _call.Dispose();
        CancellationTokenSource? old = Interlocked.Exchange(ref _context, s_cancelled);
        if (old is not null && !ReferenceEquals(old, s_cancelled)) old.Dispose();
    }

    private static CancellationTokenSource CreateCancelled()
    {
        var cts = new CancellationTokenSource();
        cts.Cancel();
        return cts;
    }
}

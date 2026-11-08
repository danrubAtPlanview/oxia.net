// =====================================================================================================================
// PORT NOTE (tests)
//   Upstream : common/rpc/logging_client_rpc_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Go's loggingClientRpc is a pure pass-through decorator over proto.OxiaClientClient (it does not log); the C#
//              port merges it into OxiaClientRpc, so these tests exercise OxiaClientRpc over a fake Grpc.Core.CallInvoker
//              (the analogue of Go's fakeOxiaClientClient): the call result is passed through untouched and a failure of the
//              underlying client is returned as that very error.
//   Go tests: 3, ported: 3, skipped: 0.
//   Divergences:
//      * "assert.Same(response, res)": OxiaClientRpc hands back the very message instance the transport produced.
//      * The Go tests return (value, err) from the fake and assert `res == nil` on error; in .NET a failure is an exception
//        and there is no result to inspect, so the "Nil" assertion becomes "the exception is thrown".
//      * For the two streaming tests the Go fake fails the call creation (`return stream, errTestClientRpc`); a .NET
//        CallInvoker can fail the creation too (it throws), which OxiaClientRpc lets through unchanged (translation only
//        happens on Recv/Send, like Go's Oxia*StreamingClient wrappers).  The stream "Same" check becomes: RecvAsync yields
//        the message instances of the underlying reader.
// =====================================================================================================================
using Grpc.Core;
using Oxia.Common.Constant;
using Oxia.Common.Proto;
using Oxia.Common.Rpc;
using Xunit;

namespace Oxia.Client.Tests.Proto;

public class LoggingClientRpcTests
{
    private static readonly Exception ErrTestClientRpc = new InvalidOperationException("rpc failed");

    [Fact]
    public async Task TestLoggingClientRpcWrite()
    {
        var response = new WriteResponse();
        var client = new OxiaClientRpc(new FakeCallInvoker { Unary = _ => response });
        WriteResponse res = await client.WriteAsync(new WriteRequest(), default);
        Assert.Same(response, res);

        client = new OxiaClientRpc(new FakeCallInvoker { Unary = _ => throw ErrTestClientRpc });
        Exception err = await Assert.ThrowsAnyAsync<Exception>(async () => await client.WriteAsync(new WriteRequest(), default));
        Assert.True(OxiaErrors.Is(err, ErrTestClientRpc));
    }

    [Fact]
    public async Task TestLoggingClientRpcGetNotifications()
    {
        var batch = new NotificationBatch { Shard = 5 };
        var client = new OxiaClientRpc(new FakeCallInvoker { Stream = _ => new FakeStreamReader<NotificationBatch>(batch) });
        using (IServerStreamingClient<NotificationBatch> stream = client.GetNotifications(new NotificationsRequest(), default))
        {
            Assert.NotNull(stream);
            Assert.Same(batch, await stream.RecvAsync());
            Assert.Null(await stream.RecvAsync());
        }

        client = new OxiaClientRpc(new FakeCallInvoker { Stream = _ => throw ErrTestClientRpc });
        Exception err = Assert.ThrowsAny<Exception>(() => client.GetNotifications(new NotificationsRequest(), default));
        Assert.True(OxiaErrors.Is(err, ErrTestClientRpc));
    }

    [Fact]
    public async Task TestLoggingClientRpcGetSequenceUpdates()
    {
        var update = new GetSequenceUpdatesResponse { HighestSequenceKey = "k-0001" };
        var client = new OxiaClientRpc(new FakeCallInvoker { Stream = _ => new FakeStreamReader<GetSequenceUpdatesResponse>(update) });
        using (IServerStreamingClient<GetSequenceUpdatesResponse> stream = client.GetSequenceUpdates(new GetSequenceUpdatesRequest(), default))
        {
            Assert.NotNull(stream);
            Assert.Same(update, await stream.RecvAsync());
            Assert.Null(await stream.RecvAsync());
        }

        client = new OxiaClientRpc(new FakeCallInvoker { Stream = _ => throw ErrTestClientRpc });
        Exception err = Assert.ThrowsAny<Exception>(() => client.GetSequenceUpdates(new GetSequenceUpdatesRequest(), default));
        Assert.True(OxiaErrors.Is(err, ErrTestClientRpc));
    }

    // ---- extras on the same fake ---------------------------------------------------------------------------------------

    [Fact]
    public async Task Extra_RecvFailure_OfANonGrpcError_IsRethrownUnchanged()
    {
        var client = new OxiaClientRpc(new FakeCallInvoker { Stream = _ => new FakeStreamReader<NotificationBatch>(failWith: ErrTestClientRpc) });
        using var stream = client.GetNotifications(new NotificationsRequest(), default);
        Exception err = await Assert.ThrowsAnyAsync<Exception>(async () => await stream.RecvAsync());
        Assert.Same(ErrTestClientRpc, err);
    }

    [Fact]
    public async Task Extra_RecvFailure_OfAnRpcExceptionWithoutOxiaDetails_IsRethrownUnchanged()
    {
        var failure = new RpcException(new Grpc.Core.Status(StatusCode.Unavailable, "connection refused"));
        var client = new OxiaClientRpc(new FakeCallInvoker { Stream = _ => new FakeStreamReader<NotificationBatch>(failWith: failure) });
        using var stream = client.GetNotifications(new NotificationsRequest(), default);
        Exception err = await Assert.ThrowsAnyAsync<Exception>(async () => await stream.RecvAsync());
        Assert.Same(failure, err);
        Assert.Equal(StatusCode.Unavailable, OxiaErrors.GrpcCode(err));
    }

    [Fact]
    public void Extra_DisposeDisposesTheUnderlyingCall()
    {
        var invoker = new FakeCallInvoker { Stream = _ => new FakeStreamReader<NotificationBatch>() };
        var client = new OxiaClientRpc(invoker);
        client.GetNotifications(new NotificationsRequest(), default).Dispose();
        Assert.Equal(1, invoker.DisposedCalls);
    }

    [Fact]
    public async Task Extra_UnaryCall_IsDisposedAfterTheResponse()
    {
        var invoker = new FakeCallInvoker { Unary = _ => new CreateSessionResponse { SessionId = 4 } };
        var client = new OxiaClientRpc(invoker);
        Assert.Equal(4L, (await client.CreateSessionAsync(new CreateSessionRequest(), default)).SessionId);
        Assert.Equal(1, invoker.DisposedCalls);
    }

    [Fact]
    public void Extra_Constructor_RejectsNull() => Assert.Throws<ArgumentNullException>(() => new OxiaClientRpc(null!));
}

internal sealed class FakeStreamReader<T> : IAsyncStreamReader<T> where T : class
{
    private readonly Queue<T> _items;
    private readonly Exception? _failWith;
    private T? _current;

    public FakeStreamReader(params T[] items) : this(items, null) { }
    public FakeStreamReader(Exception failWith) : this(Array.Empty<T>(), failWith) { }
    private FakeStreamReader(T[] items, Exception? failWith)
    {
        _items = new Queue<T>(items);
        _failWith = failWith;
    }

    public T Current => _current!;

    public Task<bool> MoveNext(CancellationToken cancellationToken)
    {
        if (_items.Count > 0)
        {
            _current = _items.Dequeue();
            return Task.FromResult(true);
        }
        return _failWith is null ? Task.FromResult(false) : Task.FromException<bool>(_failWith);
    }
}

/// <summary>A CallInvoker answering unary / server-streaming calls from delegates (the analogue of Go's fakeOxiaClientClient).</summary>
internal sealed class FakeCallInvoker : CallInvoker
{
    /// <summary>Returns the response message (or throws) of a unary call.</summary>
    public Func<object, object>? Unary { get; init; }
    /// <summary>Returns the reader (or throws) of a server-streaming call.</summary>
    public Func<object, object>? Stream { get; init; }
    public int DisposedCalls;

    public override TResponse BlockingUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
        => throw new NotSupportedException();

    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
    {
        object response = Unary!(request);
        return new AsyncUnaryCall<TResponse>(Task.FromResult((TResponse)response), Task.FromResult(new Metadata()),
            () => Grpc.Core.Status.DefaultSuccess, () => new Metadata(), () => Interlocked.Increment(ref DisposedCalls));
    }

    public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
    {
        var reader = (IAsyncStreamReader<TResponse>)Stream!(request);
        return new AsyncServerStreamingCall<TResponse>(reader, Task.FromResult(new Metadata()),
            () => Grpc.Core.Status.DefaultSuccess, () => new Metadata(), () => Interlocked.Increment(ref DisposedCalls));
    }

    public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options)
        => throw new NotSupportedException();

    public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options)
        => throw new NotSupportedException();
}

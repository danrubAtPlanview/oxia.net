// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/internal/write_stream.go  (streamWrapper, newStreamWrapper, Send, handleResponses)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Wraps one client->leader WriteStream of a shard so that many requests can use it one after the other:
//                * Send serialises the sends, remembers the caller's future in a FIFO queue and waits for the response;
//                * ONE receive loop per stream (the Go goroutine) pops the oldest pending future for every response and
//                  completes it (the server answers in request order);
//                * when the stream fails, the wrapper is marked `Failed` BEFORE the pending futures are failed, so that their
//                  retries find a fresh stream; a wrapper whose Send failed is Failed too;
//                * a request is never sent after its context is done: Send checks the token first and calls `prepare` right
//                  before handing the request to gRPC (prepare may drop the operations that must not be sent anymore);
//                * the response of a request whose caller gave up (deadline) is still consumed, to keep the FIFO aligned.
//   Divergences:
//      * Go holds one mutex around "append + stream.Send" and the receive goroutine takes the same mutex.  gRPC.NET's
//        SendAsync must not overlap and awaits the transport, so the sends are serialised with a SemaphoreSlim, while the
//        pending queue has its own tiny lock: the receive loop never waits behind a send blocked on flow control.
//      * The pending queue is a growable ring buffer of Future<WriteResponse> (the Go code re-slices a slice for every
//        response and appends -> constant garbage).
//      * Futures are completed / failed OUTSIDE the queue lock (their continuations must not run under it).
//      * Go's `Send` cannot be interrupted once the request is handed to gRPC; here SendAsync honours the request token (the
//        first send also connects).  If that token fires before SendAsync completes, the state of the HTTP/2 stream is unknown:
//        the wrapper is marked Failed and the stream is cancelled, so a request that missed its deadline is never delivered
//        afterwards (Go gets the same effect by cancelling the stream context when the request ends during the stream setup).
//      * A response without a pending request (a protocol violation; Go panics with an index out of range) fails the stream.
//      * A null from RecvAsync is Go's io.EOF: it fails the pending futures with OxiaErrors.EndOfStream (retryable).
//      * The debug line logs the sizes of the response (Go logs the protobuf text); it is only built when Debug is enabled.
// =====================================================================================================================
using Microsoft.Extensions.Logging;
using Oxia.Common.Concurrent;
using Oxia.Common.Constant;
using Oxia.Common.Logging;
using Oxia.Common.Proto;
using Oxia.Common.Rpc;

namespace Oxia.Client.Internal;

/// <summary>Go: streamWrapper.</summary>
internal sealed class StreamWrapper
{
    private static readonly Action<ILogger, int, int, int, Exception?> LogGotResponse = LoggerMessage.Define<int, int, int>(
        LogLevel.Debug, new EventId(1, "GotResponse"), "got response (puts={Puts}, deletes={Deletes}, deleteRanges={DeleteRanges})");

    private readonly SemaphoreSlim _sendLock = new(1, 1);
    private readonly object _queueLock = new();
    private readonly CancellationTokenSource _cts;
    private readonly ILogger _logger;
    private Future<WriteResponse>?[] _ring = new Future<WriteResponse>?[4];
    private int _head;
    private int _count;
    private volatile bool _failed;

    /// <summary>The shard leader the stream goes to.</summary>
    public string Target { get; }

    public IWriteStreamCall Stream { get; }

    /// <summary>Go: failed.Load() -- the stream failed (or was replaced) and must not be used for new requests.</summary>
    public bool Failed
    {
        get => _failed;
        set => _failed = value;
    }

    /// <summary>
    /// Go: newStreamWrapper.  Wraps a write stream to <paramref name="target"/>.  <paramref name="streamCts"/> is the source of
    /// the token the stream was created with; it is cancelled (and disposed) once the stream ends.
    /// </summary>
    public StreamWrapper(long shard, string target, IWriteStreamCall stream, CancellationTokenSource streamCts, ILogger? logger = null)
    {
        Target = target;
        Stream = stream;
        _cts = streamCts;
        _logger = logger ?? OxiaLog.Create("writeStream");
        Shard = shard;

        // go process.DoWithLabels(stream.Context(), ..., sw.handleResponses)
        _ = Task.Run(HandleResponsesAsync);
    }

    public long Shard { get; }

    /// <summary>
    /// Hands the request returned by <paramref name="prepare"/> to gRPC, and waits for its response.  prepare is invoked
    /// right before, unless <paramref name="ct"/> is done: a request is never sent after its deadline.
    /// </summary>
    public async ValueTask<WriteResponse> Send(CancellationToken ct, Func<WriteRequest> prepare)
    {
        ct.ThrowIfCancellationRequested();
        var req = prepare();

        var f = new Future<WriteResponse>();

        await _sendLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            lock (_queueLock) Enqueue(f);

            try
            {
                await Stream.SendAsync(req, ct).ConfigureAwait(false);
            }
            catch
            {
                _failed = true;
                // The request token fired while the request was being handed over (or the stream is set up): the stream is in
                // an unknown state and the request must not reach the leader later.
                if (ct.IsCancellationRequested) Cancel();
                throw;
            }
        }
        finally
        {
            _sendLock.Release();
        }

        return await f.WaitAsync(ct).ConfigureAwait(false);
    }

    /// <summary>Cancels the stream (Go: the cancel func given to newStreamWrapper).</summary>
    public void Cancel()
    {
        try { _cts.Cancel(); }
        catch (ObjectDisposedException) { /* the stream already ended */ }
    }

    public ValueTask CloseSendAsync() => Stream.CloseSendAsync();

    /// <summary>Go: handleResponses.</summary>
    private async Task HandleResponsesAsync()
    {
        try
        {
            while (true)
            {
                WriteResponse? response = null;
                Exception? err = null;
                try
                {
                    response = await Stream.RecvAsync(_cts.Token).ConfigureAwait(false);
                    if (response is null) err = OxiaErrors.ToThrowable(OxiaErrors.EndOfStream); // io.EOF
                }
                catch (Exception e)
                {
                    err = e;
                }

                if (err is null)
                {
                    if (_logger.IsEnabled(LogLevel.Debug))
                    {
                        LogGotResponse(_logger, response!.Puts.Count, response.Deletes.Count, response.DeleteRanges.Count, null);
                    }

                    Future<WriteResponse>? f;
                    lock (_queueLock) f = TryDequeue();
                    if (f is null)
                    {
                        err = new InvalidOperationException("oxia: received a write response without a pending request");
                    }
                    else
                    {
                        f.Complete(response!);
                        continue;
                    }
                }

                // Before failing the requests, so that their retries use a new stream
                _failed = true;
                Future<WriteResponse>[] pending;
                lock (_queueLock) pending = DrainAll();
                foreach (var f in pending) f.Fail(err);
                return;
            }
        }
        finally
        {
            Cancel();
            try { Stream.Dispose(); }
            catch { /* the stream is gone anyway */ }
            _cts.Dispose();
        }
    }

    // ----- pending queue (ring buffer; callers hold _queueLock) -----------------------------------------------------

    private void Enqueue(Future<WriteResponse> f)
    {
        if (_count == _ring.Length) Grow();
        _ring[(_head + _count) & (_ring.Length - 1)] = f;
        _count++;
    }

    private Future<WriteResponse>? TryDequeue()
    {
        if (_count == 0) return null;
        var f = _ring[_head];
        _ring[_head] = null;
        _head = (_head + 1) & (_ring.Length - 1);
        _count--;
        return f;
    }

    private Future<WriteResponse>[] DrainAll()
    {
        if (_count == 0) return [];
        var all = new Future<WriteResponse>[_count];
        for (int i = 0; i < all.Length; i++) all[i] = TryDequeue()!;
        return all;
    }

    private void Grow()
    {
        var bigger = new Future<WriteResponse>?[_ring.Length * 2];
        for (int i = 0; i < _count; i++) bigger[i] = _ring[(_head + i) & (_ring.Length - 1)];
        _ring = bigger;
        _head = 0;
    }
}

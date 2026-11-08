// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/internal/rpc_provider.go  (rpcProvider, NewRpcProvider, executeWithRetry*, executeOnce, getWriteStream,
//              isRetryableShardRequest, errShardHasNoLeader).  ShardMapTimer lives in ShardMapTimer.cs, the stream wrapper in
//              WriteStream.cs.
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Executes the client's RPCs.
//                * Shard requests (write / read / list / range scan) look the leader up in the shard manager on EVERY attempt
//                  and are retried with an exponential backoff.  What is retried: `OxiaErrors.IsRetryable` plus any Unavailable
//                  status (a leader that went away) -- `isRetryableShardRequest`.  A leader hint in the error steers the next
//                  attempt only (if following it fails without a new hint, the next attempt goes back to the shard map).  A
//                  shard missing from the shard map fails the request with ShardNotFound so that the caller reroutes it.
//                  Lists and range scans stop retrying as soon as one response was handed to the consumer.
//                * Requests to a fixed target (shard assignments, sequence updates, notifications, sessions) are executed ONCE
//                  (`executeOnce`): they fail fast so that their callers can look up a new target.  An empty target is the
//                  leader of a shard that has no leader: nothing connects to it (errShardHasNoLeader, an Unavailable status).
//                * Writes use one write stream per shard, cached in a map; a new one replaces the cached one if that failed or
//                  if the target changed (the old one is half-closed through the `getWriteStream.closeOldStream` failpoint
//                  hook).  The stream outlives the request that created it, but its setup does not: the request token cancels
//                  the setup.  The stream carries the `namespace` and `shard-id` headers.
//   Divergences:
//      * ctx -> CancellationToken, (T, error) -> thrown exceptions on these cold/failure paths (the Retry helper of Oxia.Common
//        signals failures by throwing).  The errors that Go returns as the sentinel are thrown as `OxiaErrors.ToThrowable(sentinel)`
//        (a fresh exception that still satisfies OxiaErrors.Is) -- shared sentinels are never thrown.  errShardHasNoLeader is a
//        shared RpcException instance that IS thrown (it is not an OxiaException, and tests match it by reference).
//      * `context.DeadlineExceeded` and `context.Canceled` are both an OperationCanceledException when a CancellationToken
//        fires: .NET cannot tell a deadline from a cancel.
//      * gRPC.NET creates a call without I/O: the connection to the leader (Go: the blocking part of client.WriteStream) happens
//        during the first Send of the stream, which honours the request token (see WriteStream.cs).
//      * NewRpcProvider(ctx, ns, tls, auth, addr, supplier, dialOptions...) is RpcProvider.Create; the constructor takes the
//        IClientPool (tests wrap it) and the failpoints (Go: the unexported newRpcProvider).
//      * Close also cancels the write streams still open (Go leaves them to die with the closed connections).
//      * The shard-request first attempt runs inline: only a failed attempt allocates the retry machinery (timer, backoff,
//        closures); the Go code always sets it up.  The observable behaviour is the same -- the failed first attempt is handed to
//        the very same retry loop.
//      * ExecuteRead returns the response of the stream as is when the server sent a single one (no copy).
// =====================================================================================================================
using System.Collections.Concurrent;
using System.Globalization;
using System.Net.Security;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;
using Oxia.Common;
using Oxia.Common.Auth;
using Oxia.Common.Constant;
using Oxia.Common.Failpoint;
using Oxia.Common.Logging;
using Oxia.Common.Proto;
using Oxia.Common.Rpc;
using Oxia.Common.Time;

namespace Oxia.Client.Internal;

/// <summary>Go: rpcProvider.</summary>
public sealed class RpcProvider : IRpcProvider
{
    /// <summary>
    /// Go: errShardHasNoLeader -- replaces connecting to the empty leader address of a shard that has no leader, e.g. while one is
    /// being elected.  As an Unavailable status, it is retried by the shard requests only.
    /// </summary>
    internal static readonly RpcException ErrShardHasNoLeader = new(new Grpc.Core.Status(StatusCode.Unavailable, "oxia: shard has no leader"));

    private static readonly Func<Exception?, bool> IsRetryableShardRequestFunc = IsRetryableShardRequest;

    private readonly IClientPool _clientPool;
    private readonly string _serviceAddress;
    private readonly Func<IShardManager?> _shardManagerSupplier;
    private readonly ConcurrentDictionary<long, StreamWrapper> _writeStreams = new();
    private readonly object _writeStreamsLock = new();
    private readonly CancellationToken _ct;
    private readonly string _namespace;
    private readonly Failpoints? _failpoints;
    private readonly ILogger _logger = OxiaLog.Create("rpcProvider");

    /// <summary>Go: newRpcProvider.</summary>
    public RpcProvider(CancellationToken ct, string ns, IClientPool clientPool, string serviceAddress,
        Func<IShardManager?> shardManagerSupplier, Failpoints? failpoints = null)
    {
        _ct = ct;
        _namespace = ns;
        _clientPool = clientPool;
        _serviceAddress = serviceAddress;
        _shardManagerSupplier = shardManagerSupplier;
        _failpoints = failpoints;
    }

    /// <summary>Go: NewRpcProvider.</summary>
    public static RpcProvider Create(CancellationToken ct, string ns, SslClientAuthenticationOptions? tls, IAuthentication? auth,
        string serviceAddress, Func<IShardManager?> shardManagerSupplier, params Action<GrpcChannelOptions>[] dialOptions) =>
        new(ct, ns, new ClientPool(tls, auth, dialOptions), serviceAddress, shardManagerSupplier);

    /// <summary>Go: Close.</summary>
    public async ValueTask CloseAsync()
    {
        try
        {
            await _clientPool.CloseAsync().ConfigureAwait(false);
        }
        finally
        {
            foreach (var sw in _writeStreams.Values) sw.Cancel();
        }
    }

    // ----- targets --------------------------------------------------------------------------------------------------

    /// <summary>Go: getClientByTarget.</summary>
    private IOxiaClientRpc GetClientByTarget(string target)
    {
        if (target.Length == 0) throw ErrShardHasNoLeader;

        try
        {
            return _clientPool.GetClientRpc(target);
        }
        catch (Exception e)
        {
            throw Translate(e);
        }
    }

    /// <summary>Go: shardMapChanged.</summary>
    private Task? ShardMapChanged() => _shardManagerSupplier()?.Changed();

    /// <summary>Go: getTargetByShard.</summary>
    private string GetTargetByShard(long? shardId, ErrorMetadata? hint)
    {
        if (shardId is not { } id) return _serviceAddress;

        var shardManager = _shardManagerSupplier();
        if (shardManager is null || !shardManager.Exists(id))
        {
            throw OxiaErrors.ToThrowable(OxiaErrors.ShardNotFound);
        }

        if (hint is not null && hint.TryGetLeaderHint(out long hintShard, out string leader) && hintShard == id)
        {
            return leader;
        }

        return shardManager.Leader(id);
    }

    // ----- shard requests -------------------------------------------------------------------------------------------

    public ValueTask<WriteResponse> ExecuteWrite(CancellationToken ct, long shardId, Func<WriteRequest> prepare)
    {
        // A change of the shard map ends the wait for the next attempt: once a shard is split, the other shards hold their writes
        // until the batches pending on it are rerouted.
        var timer = new ShardMapTimer();
        return ExecuteWithRetryTimer(ct, timer, async hint =>
        {
            timer.Changed = ShardMapChanged();
            string target = GetTargetByShard(shardId, hint);
            var sw = await GetWriteStream(ct, shardId, target).ConfigureAwait(false);
            return await sw.Send(ct, prepare).ConfigureAwait(false);
        }, IsRetryableShardRequestFunc);
    }

    public ValueTask<ReadResponse> ExecuteRead(CancellationToken ct, ReadRequest request) =>
        ExecuteWithRetry(ct, async hint =>
        {
            string target = GetTargetByShard(request.Shard, hint);
            var client = GetClientByTarget(target);
            using var stream = client.Read(request, ct);

            ReadResponse? response = null;
            while (true)
            {
                var recv = await stream.RecvAsync(ct).ConfigureAwait(false);
                if (recv is null) return response ?? new ReadResponse();

                if (response is null)
                {
                    response = recv;
                }
                else
                {
                    response.Gets.AddRange(recv.Gets);
                }
            }
        }, IsRetryableShardRequestFunc);

    public async ValueTask ExecuteList(CancellationToken ct, ListRequest request, Func<ListResponse, Exception?> listResponseConsumer)
    {
        bool verified = false;
        await ExecuteWithRetry<object?>(ct, async hint =>
        {
            string target = GetTargetByShard(request.Shard, hint);
            var client = GetClientByTarget(target);
            using var stream = client.List(request, ct);
            while (true)
            {
                var response = await stream.RecvAsync(ct).ConfigureAwait(false);
                if (response is null) return null;

                var err = listResponseConsumer(response);
                if (err is not null) throw OxiaErrors.ToThrowable(err);
                verified = true;
            }
        }, err => !verified && IsRetryableShardRequest(err)).ConfigureAwait(false);
    }

    public async ValueTask ExecuteRangeScan(CancellationToken ct, RangeScanRequest request,
        Func<RangeScanResponse, Exception?> rangeScanResponseConsumer)
    {
        bool verified = false;
        await ExecuteWithRetry<object?>(ct, async hint =>
        {
            string target = GetTargetByShard(request.Shard, hint);
            var client = GetClientByTarget(target);
            using var stream = client.RangeScan(request, ct);
            while (true)
            {
                var response = await stream.RecvAsync(ct).ConfigureAwait(false);
                if (response is null) return null;

                var err = rangeScanResponseConsumer(response);
                if (err is not null) throw OxiaErrors.ToThrowable(err);
                verified = true;
            }
        }, err => !verified && IsRetryableShardRequest(err)).ConfigureAwait(false);
    }

    // ----- write streams --------------------------------------------------------------------------------------------

    /// <summary>
    /// Go: getWriteStream -- returns the write stream of the shard to its leader at target.  A new stream replaces the one of the
    /// shard if that one failed, or if it goes to another target: a leader that was replaced, but is still reachable, keeps the
    /// stream open without applying the writes anymore.
    /// </summary>
    private ValueTask<StreamWrapper> GetWriteStream(CancellationToken ct, long shardId, string target)
    {
        if (_writeStreams.TryGetValue(shardId, out var sw) && !sw.Failed && string.Equals(sw.Target, target, StringComparison.Ordinal))
        {
            return new ValueTask<StreamWrapper>(sw);
        }

        return NewWriteStream(ct, shardId, target);
    }

    private async ValueTask<StreamWrapper> NewWriteStream(CancellationToken ct, long shardId, string target)
    {
        var client = GetClientByTarget(target);

        // The stream outlives the request, but its setup must not: connecting to an unreachable target can take up to the gRPC
        // connect timeout.  The stream is created without I/O; the connection is made by its first Send, which honours the
        // request token and fails the stream if the token fires first.
        var streamCts = CancellationTokenSource.CreateLinkedTokenSource(_ct);
        var headers = new Metadata
        {
            { Constants.MetadataNamespace, _namespace },
            { Constants.MetadataShardId, shardId.ToString(CultureInfo.InvariantCulture) },
        };

        IWriteStreamCall stream;
        try
        {
            stream = client.WriteStream(headers, streamCts.Token);
        }
        catch
        {
            streamCts.Dispose();
            throw;
        }

        if (ct.IsCancellationRequested)
        {
            // The request ended during the setup, and cancelled the stream
            streamCts.Cancel();
            stream.Dispose();
            streamCts.Dispose();
            ct.ThrowIfCancellationRequested();
        }

        var sw = new StreamWrapper(shardId, target, stream, streamCts);

        StreamWrapper? old;
        lock (_writeStreamsLock)
        {
            // The batcher of the shard sends one request at a time, so none is waiting for its response on the old stream:
            // closing it cannot make a request fail and be sent again
            if (_writeStreams.TryGetValue(shardId, out old)) old.Failed = true;
            _writeStreams[shardId] = sw;
        }

        if (old is not null)
        {
            Exception? closeError = null;
            try
            {
                await old.CloseSendAsync().ConfigureAwait(false);
            }
            catch (Exception e)
            {
                closeError = e;
            }

            closeError = Failpoints.Fail(_failpoints, "getWriteStream.closeOldStream", closeError);
            if (closeError is not null && _logger.IsEnabled(LogLevel.Warning))
            {
                _logger.LogWarning(closeError, "failed to close old write stream (shard {Shard})", shardId);
            }
        }

        return sw;
    }

    // ----- fixed target requests ------------------------------------------------------------------------------------

    public ValueTask<IServerStreamingClient<ShardAssignments>> GetShardAssignments(CancellationToken ct, string target,
        ShardAssignmentsRequest request)
    {
        try
        {
            return ValueTask.FromResult(GetClientByTarget(target).GetShardAssignments(request, ct));
        }
        catch (Exception e)
        {
            return ValueTask.FromException<IServerStreamingClient<ShardAssignments>>(Translate(e));
        }
    }

    public ValueTask<IServerStreamingClient<GetSequenceUpdatesResponse>> GetSequenceUpdates(CancellationToken ct, string target,
        GetSequenceUpdatesRequest request)
    {
        try
        {
            return ValueTask.FromResult(GetClientByTarget(target).GetSequenceUpdates(request, ct));
        }
        catch (Exception e)
        {
            return ValueTask.FromException<IServerStreamingClient<GetSequenceUpdatesResponse>>(Translate(e));
        }
    }

    public ValueTask<IServerStreamingClient<NotificationBatch>> GetNotifications(CancellationToken ct, string target,
        NotificationsRequest request)
    {
        try
        {
            return ValueTask.FromResult(GetClientByTarget(target).GetNotifications(request, ct));
        }
        catch (Exception e)
        {
            return ValueTask.FromException<IServerStreamingClient<NotificationBatch>>(Translate(e));
        }
    }

    public async ValueTask<CreateSessionResponse> CreateSession(CancellationToken ct, string target, CreateSessionRequest request)
    {
        try
        {
            return await GetClientByTarget(target).CreateSessionAsync(request, ct).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            throw Translate(e);
        }
    }

    public async ValueTask<KeepAliveResponse> KeepAlive(CancellationToken ct, string target, SessionHeartbeat request)
    {
        try
        {
            return await GetClientByTarget(target).KeepAliveAsync(request, ct).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            throw Translate(e);
        }
    }

    public async ValueTask<CloseSessionResponse> CloseSession(CancellationToken ct, string target, CloseSessionRequest request)
    {
        try
        {
            return await GetClientByTarget(target).CloseSessionAsync(request, ct).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            throw Translate(e);
        }
    }

    // ----- retry ----------------------------------------------------------------------------------------------------

    /// <summary>
    /// Go: isRetryableShardRequest -- also retries the Unavailable statuses that do not map to an Oxia error, such as the transport
    /// failures to reach a shard leader that went away (e.g. connection refused).  Shard requests look up the leader on every attempt,
    /// so the retry reaches the new leader once the client receives the updated shard assignments.  Requests to a fixed target must
    /// fail fast instead, so that their callers can look up a new target.
    /// </summary>
    internal static bool IsRetryableShardRequest(Exception? err) =>
        OxiaErrors.IsRetryable(err) || OxiaErrors.GrpcCode(err) == StatusCode.Unavailable;

    /// <summary>Go: executeOnce's error translation -- the exception to throw for a failure of a fixed-target request.</summary>
    private static Exception Translate(Exception e)
    {
        var (err, _) = OxiaErrors.FromGrpcError(e);
        return err is null || ReferenceEquals(err, e) ? e : OxiaErrors.ToThrowable(err);
    }

    /// <summary>Go: executeWithRetry.</summary>
    internal static ValueTask<T> ExecuteWithRetry<T>(CancellationToken ct, Func<ErrorMetadata?, ValueTask<T>> operation,
        Func<Exception?, bool>? isRetryable = null) => ExecuteWithRetryTimer(ct, null, operation, isRetryable);

    /// <summary>Go: executeWithRetryTimer -- executeWithRetry, waiting between the attempts with the given timer, or with a system timer if null.</summary>
    internal static ValueTask<T> ExecuteWithRetryTimer<T>(CancellationToken ct, IRetryTimer? timer,
        Func<ErrorMetadata?, ValueTask<T>> operation, Func<Exception?, bool>? isRetryable = null)
    {
        ErrorMetadata? hint = null;
        var retryable = isRetryable ?? OxiaErrors.IsRetryable;
        return Retry.RetryNotifyWithTimerAsync(async () =>
        {
            try
            {
                return await operation(hint).ConfigureAwait(false);
            }
            catch (Exception e) when (e is not PermanentException)
            {
                // A leader hint only steers the next attempt: if following it fails without a new hint, e.g. because the
                // hinted leader is unreachable, the next attempt goes back to the shard assignments
                var (err, md) = OxiaErrors.FromGrpcError(e);
                hint = md;
                err ??= e;
                bool untranslated = ReferenceEquals(err, e);
                if (!retryable(err)) throw new PermanentException(untranslated ? e : OxiaErrors.ToThrowable(err));
                if (untranslated) throw;
                throw OxiaErrors.ToThrowable(err);
            }
        }, BackOffFactory.NewBackOff(ct), null, timer, ct);
    }
}

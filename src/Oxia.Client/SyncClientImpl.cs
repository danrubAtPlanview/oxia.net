// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/sync_client_impl.go   (syncClientImpl, newSyncClient, callContextOption -> CallContextOptions in the options component)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The synchronous client is a thin layer over the async one (it talks to it ONLY through IAsyncClient):
//                * the writes (Put / Delete / DeleteRange) get a CallContext tied to the caller's token and race the result
//                  against the token.  Once the token is done the write is dropped unless it was already handed to gRPC:
//                  the error is callContext.Cancel() (NotSent when it was dropped), so that a write reported as failed is
//                  never applied later.
//                * Get returns the token's error when the token is done first; List collects the chunks of all the shards,
//                  cancelling the other shards at the first error, and fails with the token's error when it is done.
//                * RangeScan / GetSequenceUpdates / GetNotifications forward.
//   Divergences: failures are THROWN as OxiaErrors.ToThrowable(err) (never the shared sentinel itself); a cancelled token
//              throws OperationCanceledException / the NotSent OxiaException wrapping it (a CancellationToken cannot tell
//              Canceled from DeadlineExceeded).  The cache manager is created lazily under an async lock
//              (GetCacheManagerAsync); `select` on {result, ctx.Done()} -> Task.WaitAsync(token).
// =====================================================================================================================
using System.Threading.Channels;
using Oxia.Client.Internal.Model;
using Oxia.Common;
using Oxia.Common.Constant;

namespace Oxia.Client;

internal sealed class SyncClientImpl : ISyncClient
{
    private readonly SemaphoreSlim _lock = new(1, 1);
    private readonly IAsyncClient _asyncClient;
    private CacheManager? _cacheManager;

    /// <summary>Go: newSyncClient(asyncClient).</summary>
    public SyncClientImpl(IAsyncClient asyncClient) => _asyncClient = asyncClient;

    /// <summary>Go: newSyncClient (the tests substitute an async client that never completes).</summary>
    internal static SyncClientImpl NewSyncClient(IAsyncClient asyncClient) => new(asyncClient);

    /// <summary>Go: getCacheManager -- created on first use, shared afterwards.</summary>
    public async ValueTask<CacheManager> GetCacheManagerAsync()
    {
        await _lock.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_cacheManager is not null)
            {
                return _cacheManager;
            }

            _cacheManager = await CacheManager.CreateAsync(this, null).ConfigureAwait(false);
            return _cacheManager;
        }
        finally
        {
            _lock.Release();
        }
    }

    public async ValueTask CloseAsync()
    {
        await _lock.WaitAsync().ConfigureAwait(false);
        try
        {
            Exception? cacheErr = null;
            if (_cacheManager is not null)
            {
                try { await _cacheManager.CloseAsync().ConfigureAwait(false); } catch (Exception e) { cacheErr = e; }
            }

            Exception? clientErr = null;
            try { await _asyncClient.CloseAsync().ConfigureAwait(false); } catch (Exception e) { clientErr = e; }

            // multierr.Combine(err, c.asyncClient.Close())
            if (cacheErr is not null && clientErr is not null) throw new AggregateException(cacheErr, clientErr);
            if (cacheErr is not null) throw cacheErr;
            if (clientErr is not null) throw clientErr;
        }
        finally
        {
            _lock.Release();
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Writes: the options of the caller come after the CallContext option
    // -----------------------------------------------------------------------------------------------------------------

    public ValueTask<(string InsertedKey, RecordVersion Version)> PutAsync(CancellationToken ct, string key, ReadOnlyMemory<byte> value,
        params ReadOnlySpan<PutOption> options)
    {
        var callContext = new CallContext(ct);
        var merged = new PutOption[options.Length + 1];
        merged[0] = CallContextOptions.ForPut(callContext);
        options.CopyTo(merged.AsSpan(1));
        return PutCoreAsync(ct, callContext, _asyncClient.PutAsync(key, value, merged));
    }

    private static async ValueTask<(string InsertedKey, RecordVersion Version)> PutCoreAsync(CancellationToken ct, CallContext callContext,
        ValueTask<PutResult> pending)
    {
        PutResult r;
        if (pending.IsCompletedSuccessfully)
        {
            r = pending.Result;
        }
        else
        {
            var task = pending.AsTask();
            try
            {
                r = await task.WaitAsync(ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested && !task.IsCompleted)
            {
                throw OxiaErrors.ToThrowable(callContext.Cancel());
            }
        }
        if (r.Err is not null) throw OxiaErrors.ToThrowable(r.Err);
        return (r.Key, r.Version);
    }

    public ValueTask DeleteAsync(CancellationToken ct, string key, params ReadOnlySpan<DeleteOption> options)
    {
        var callContext = new CallContext(ct);
        var merged = new DeleteOption[options.Length + 1];
        merged[0] = CallContextOptions.ForDelete(callContext);
        options.CopyTo(merged.AsSpan(1));
        return ErrorCoreAsync(ct, callContext, _asyncClient.DeleteAsync(key, merged));
    }

    public ValueTask DeleteRangeAsync(CancellationToken ct, string minKeyInclusive, string maxKeyExclusive,
        params ReadOnlySpan<DeleteRangeOption> options)
    {
        var callContext = new CallContext(ct);
        var merged = new DeleteRangeOption[options.Length + 1];
        merged[0] = CallContextOptions.ForDeleteRange(callContext);
        options.CopyTo(merged.AsSpan(1));
        return ErrorCoreAsync(ct, callContext, _asyncClient.DeleteRangeAsync(minKeyInclusive, maxKeyExclusive, merged));
    }

    private static async ValueTask ErrorCoreAsync(CancellationToken ct, CallContext callContext, ValueTask<Exception?> pending)
    {
        Exception? err;
        if (pending.IsCompletedSuccessfully)
        {
            err = pending.Result;
        }
        else
        {
            var task = pending.AsTask();
            try
            {
                err = await task.WaitAsync(ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested && !task.IsCompleted)
            {
                throw OxiaErrors.ToThrowable(callContext.Cancel());
            }
        }
        if (err is not null) throw OxiaErrors.ToThrowable(err);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Reads
    // -----------------------------------------------------------------------------------------------------------------

    public ValueTask<(string StoredKey, byte[]? Value, RecordVersion Version)> GetAsync(CancellationToken ct, string key,
        params ReadOnlySpan<GetOption> options)
        => GetCoreAsync(ct, _asyncClient.GetAsync(key, options));

    private static async ValueTask<(string StoredKey, byte[]? Value, RecordVersion Version)> GetCoreAsync(CancellationToken ct,
        ValueTask<GetResult> pending)
    {
        GetResult r;
        if (pending.IsCompletedSuccessfully)
        {
            r = pending.Result;
        }
        else
        {
            var task = pending.AsTask();
            try
            {
                r = await task.WaitAsync(ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested && !task.IsCompleted)
            {
                throw new OperationCanceledException(ct);
            }
        }
        if (r.Err is not null) throw OxiaErrors.ToThrowable(r.Err);
        return (r.Key, r.Value, r.Version);
    }

    public ValueTask<List<string>> ListAsync(CancellationToken ct, string minKeyInclusive, string maxKeyExclusive,
        params ReadOnlySpan<ListOption> options)
    {
        // Ends the lists of the other shards when one fails
        var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        ChannelReader<ListResult> reader;
        try
        {
            reader = _asyncClient.ListAsync(cts.Token, minKeyInclusive, maxKeyExclusive, options);
        }
        catch
        {
            cts.Dispose();
            throw;
        }
        return ListCoreAsync(ct, cts, reader);
    }

    private static async ValueTask<List<string>> ListCoreAsync(CancellationToken ct, CancellationTokenSource cts, ChannelReader<ListResult> reader)
    {
        try
        {
            var keys = new List<string>();
            while (await reader.WaitToReadAsync().ConfigureAwait(false))
            {
                while (reader.TryRead(out var r))
                {
                    if (r.Err is not null) throw OxiaErrors.ToThrowable(r.Err);

                    if (r.Keys is not null) keys.AddRange(r.Keys);
                }
            }

            // Once ctx is done, the shards drop the keys they have left
            if (ct.IsCancellationRequested) throw new OperationCanceledException(ct);
            return keys;
        }
        finally
        {
            try { cts.Cancel(); } finally { cts.Dispose(); }
        }
    }

    public ChannelReader<GetResult> RangeScanAsync(CancellationToken ct, string minKeyInclusive, string maxKeyExclusive,
        params ReadOnlySpan<RangeScanOption> options)
        => _asyncClient.RangeScanAsync(ct, minKeyInclusive, maxKeyExclusive, options);

    public ChannelReader<string> GetSequenceUpdates(CancellationToken ct, string prefixKey, params ReadOnlySpan<GetSequenceUpdatesOption> options)
        => _asyncClient.GetSequenceUpdates(ct, prefixKey, options);

    public ValueTask<INotifications> GetNotificationsAsync() => _asyncClient.GetNotificationsAsync();
}

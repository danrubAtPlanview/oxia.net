// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/cache.go   (Cache, ModifyFunc, SerializeFunc, DeserializeFunc, NewCache, cacheManager, cacheImpl)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : A typed, local, read-through cache of the records of a SyncClient.  One CacheManager per client owns a single
//              subscription to the notifications of the database and fans every notification out to all the caches created
//              from that client; a notification evicts the changed key (or everything, for range deletions and missed
//              notifications).  Every cache stores DE-SERIALIZED values, bounded by a 64 MiB cost (the length in bytes of the
//              serialized record) with a 5 minute TTL for found records; a "key not found" answer is cached too (cost 0, no TTL).
//              Get loads a missing key from the server; the evictions that race a load are counted per key (loads/KeyLoads) so
//              that a load never caches a record that changed after the server read it.  ReadModifyUpdate retries the
//              conditional Put on ErrUnexpectedVersionId with an exponential backoff.
//   Divergences:
//      * ristretto has no .NET twin: Internal/CostCache.cs is the equivalent bounded cache (see its header).  Because its Sets
//        are synchronous, the RWMutex that cacheImpl uses in Go to make Close wait for an in-flight Set is not needed.
//      * API shape: Cache[Value] methods taking ctx become `ValueTask` methods taking a CancellationToken; failures are THROWN
//        like ISyncClient (sentinels reachable through OxiaErrors.Is).  `TryGetAsync` is an extra non-throwing Get so that
//        a (cached) "key not found" costs no exception.  ModifyFunc returns the new value and signals an error by throwing
//        (the exception is returned unchanged by ReadModifyUpdate, without retrying: backoff.Permanent).
//      * SerializeFunc / DeserializeFunc are non-generic delegates over `object?` / `Type` (Go: `any` / pointer to the value);
//        they allocate (boxing) only on the Put and on the load, never on a hit.
//      * NewCache returns (Cache, error) in Go: here `Cache.NewAsync<T>` (throws) is the implementation, and `Cache.New<T>` is
//        a BLOCKING convenience over it (the contract signature has no ValueTask) -- prefer NewAsync.
//      * slog.Debug "Received notification" carries its attributes inline in the message ("... key=.. type=.. version-id=..").
// =====================================================================================================================
using Microsoft.Extensions.Logging;
using Oxia.Client.Internal;
using Oxia.Common;
using Oxia.Common.Constant;
using Oxia.Common.Failpoint;
using Oxia.Common.Logging;
using Oxia.Common.Time;

namespace Oxia.Client;

/// <summary>ModifyFunc is the transformation function to apply on ReadModifyUpdate.  Throw to fail (not retried).</summary>
public delegate TValue ModifyFunc<TValue>(IOptional<TValue> value);

/// <summary>SerializeFunc is the serialization function. eg: JsonSerializer.SerializeToUtf8Bytes(value, value.GetType()).</summary>
public delegate byte[] SerializeFunc(object? value);

/// <summary>DeserializeFunc is the deserialization function: builds a <paramref name="valueType"/> from the data.
/// eg: JsonSerializer.Deserialize(data.Span, valueType).</summary>
public delegate object? DeserializeFunc(ReadOnlyMemory<byte> data, Type valueType);

/// <summary>The outcome of <see cref="ICache{TValue}.TryGetAsync"/>: either a value and its version, or an error.</summary>
public readonly struct CacheGetResult<TValue>
{
    public CacheGetResult(TValue? value, RecordVersion version, Exception? err) { Value = value; Version = version; Err = err; }
    public TValue? Value { get; }
    public RecordVersion Version { get; }
    /// <summary>ErrKeyNotFound if the record does not exist, or the failure; null on success.</summary>
    public Exception? Err { get; }
}

/// <summary>
/// Cache provides a view of the data stored in Oxia that is locally cached.
/// The cached values are automatically updated when there are updates or deletions.
/// The cache is storing de-serialized object.
/// </summary>
public interface ICache<TValue> : IAsyncCloser
{
    /// <summary>
    /// Associates a value with a key.  Options: ExpectedVersionId, ExpectedRecordNotExists, Ephemeral.
    /// Returns the inserted key and the version of the newly updated record.
    /// Throws ErrUnexpectedVersionId if the expected version id does not match the current version id of the record.
    /// </summary>
    ValueTask<(string InsertedKey, RecordVersion Version)> PutAsync(CancellationToken ct, string key, TValue value,
        params ReadOnlySpan<PutOption> options);

    /// <summary>
    /// Applies atomically the result of the <paramref name="modifyFunc"/> argument into the database.
    /// The modifyFunc will be passed the current value and will return the modified value.
    /// </summary>
    ValueTask ReadModifyUpdateAsync(CancellationToken ct, string key, ModifyFunc<TValue> modifyFunc);

    /// <summary>
    /// Removes the key and its associated value from the data store.  The Delete operation can be made conditional on that
    /// the record hasn't changed from a specific existing version by passing the ExpectedVersionId option.
    /// Throws ErrUnexpectedVersionId if the expected version id does not match the current version id of the record.
    /// </summary>
    ValueTask DeleteAsync(CancellationToken ct, string key, params ReadOnlySpan<DeleteOption> options);

    /// <summary>
    /// Returns the value associated with the specified key.  In addition to the value, a version object is also returned,
    /// with information about the record state.  Throws ErrKeyNotFound if the record does not exist.
    /// </summary>
    ValueTask<(TValue Value, RecordVersion Version)> GetAsync(CancellationToken ct, string key);

    /// <summary>Like <see cref="GetAsync"/> but reports the failure (also ErrKeyNotFound) in the result instead of throwing.</summary>
    ValueTask<CacheGetResult<TValue>> TryGetAsync(CancellationToken ct, string key);
}

public static class Cache
{
    /// <summary>
    /// Creates a new cache object for a specific type (Go: NewCache).  Uses the <paramref name="serializeFunc"/> and
    /// <paramref name="deserializeFunc"/> for SerDe.  The client must be the implementation created by this library
    /// (throws OxiaException "Invalid client implementation" otherwise); the caches of a client share one subscription to
    /// the notifications (throws if the notifications cannot be created).
    /// </summary>
    public static async ValueTask<ICache<T>> NewAsync<T>(ISyncClient client, SerializeFunc serializeFunc, DeserializeFunc deserializeFunc)
    {
        if (client is not SyncClientImpl c) throw new OxiaException("Invalid client implementation");

        var cm = await c.GetCacheManagerAsync().ConfigureAwait(false);
        return cm.NewCache<T>(serializeFunc, deserializeFunc);
    }

    /// <summary>Blocking convenience over <see cref="NewAsync{T}"/> (the fixed cross-component signature has no ValueTask).</summary>
    public static ICache<T> New<T>(ISyncClient client, SerializeFunc serializeFunc, DeserializeFunc deserializeFunc)
        => NewAsync<T>(client, serializeFunc, deserializeFunc).AsTask().GetAwaiter().GetResult();
}

/// <summary>One per sync client: the single subscription to the notifications, shared by all the caches (Go: cacheManager).</summary>
public sealed class CacheManager : IAsyncCloser
{
    internal const string FailpointNewCache = "newCacheImpl.newCache";

    private readonly object _lock = new();
    private readonly ISyncClient? _client;
    private readonly Failpoints? _failpoints;
    private readonly CancellationTokenSource _cts = new();
    private Task _runTask = Task.CompletedTask;

    internal INotifications? Notifications;
    internal readonly List<IInternalCache> Caches = new();
    internal ILogger Log = OxiaLog.Create("cache-manager");

    // Internal: tests build a manager without a client (cacheManager{ctx:..., caches:...}).
    internal CacheManager(ISyncClient? client, Failpoints? failpoints)
    {
        _client = client;
        _failpoints = failpoints;
    }

    /// <summary>The goroutine of Go's newCacheManager, which ends when the notifications end or the manager is closed.</summary>
    internal Task RunTask => _runTask;

    /// <summary>Go: newCacheManager(client, failpoints).  Subscribes to the notifications of the client.</summary>
    public static async ValueTask<CacheManager> CreateAsync(ISyncClient client, Failpoints? failpoints)
    {
        var cm = new CacheManager(client, failpoints);
        try
        {
            cm.Notifications = await client.GetNotificationsAsync().ConfigureAwait(false);
        }
        catch (Exception e)
        {
            cm._cts.Dispose();
            throw OxiaErrors.Wrap(e, "failed to create notifications client");
        }

        cm._runTask = Task.Run(cm.RunAsync);
        return cm;
    }

    private async Task RunAsync()
    {
        var ct = _cts.Token;
        var reader = Notifications!.Ch;
        try
        {
            // A completed channel is Go's `n == nil` (the notifications ended); cancellation is "Cache manager closed"
            while (await reader.WaitToReadAsync(ct).ConfigureAwait(false))
            {
                while (reader.TryRead(out var n)) HandleNotification(n);
            }
        }
        catch (OperationCanceledException)
        {
            // Cache manager closed
        }
    }

    internal void HandleNotification(Notification n)
    {
        lock (_lock)
        {
            if (Log.IsEnabled(LogLevel.Debug))
            {
                Log.LogDebug("Received notification key={Key} type={Type} version-id={VersionId}",
                    n.Key, n.Type.GoString(), n.VersionId);
            }

            for (int i = 0; i < Caches.Count; i++) Caches[i].HandleNotification(n);
        }
    }

    /// <summary>Go: newCache[T](cm, serializeFunc, deserializeFunc).</summary>
    internal ICache<T> NewCache<T>(SerializeFunc serializeFunc, DeserializeFunc deserializeFunc)
    {
        lock (_lock)
        {
            var cache = new CacheImpl<T>(_client!, _failpoints, serializeFunc, deserializeFunc);
            Caches.Add(cache);
            return cache;
        }
    }

    public async ValueTask CloseAsync()
    {
        try { _cts.Cancel(); } catch (ObjectDisposedException) { }

        IInternalCache[] caches;
        lock (_lock) caches = Caches.ToArray();

        List<Exception>? errors = null;
        if (Notifications is not null)
        {
            try { await Notifications.CloseAsync().ConfigureAwait(false); }
            catch (Exception e) { (errors ??= new()).Add(e); }
        }

        foreach (var c in caches)
        {
            try { await c.CloseAsync().ConfigureAwait(false); }
            catch (Exception e) { (errors ??= new()).Add(e); }
        }

        try { await _runTask.ConfigureAwait(false); } catch { /* the loop never fails */ }

        if (errors is null) return;
        if (errors.Count == 1) throw errors[0];
        throw new AggregateException(errors);
    }
}

/// <summary>Go: internalCache.</summary>
internal interface IInternalCache : IAsyncCloser
{
    void HandleNotification(Notification n);
}

/// <summary>What the cache stores for a key: the record, or the knowledge that it does not exist (Go: Optional[valueVersion[Value]]).</summary>
internal sealed class CachedEntry<TValue>
{
    /// <summary>Shared "key not found" marker: caching a negative answer allocates nothing.</summary>
    public static readonly CachedEntry<TValue> Absent = new(default, default, false);

    public readonly TValue? Value;
    public readonly RecordVersion Version;
    public readonly bool Present;

    public CachedEntry(TValue? value, RecordVersion version, bool present)
    {
        Value = value; Version = version; Present = present;
    }
}

// The loads of a key in flight, and the evictions of the key since the first one started.
internal sealed class KeyLoads
{
    public int Count;
    public ulong Evictions;
}

internal sealed class CacheImpl<TValue> : ICache<TValue>, IInternalCache
{
    /// <summary>defaultCacheTTL.</summary>
    internal static readonly TimeSpan DefaultCacheTtl = TimeSpan.FromMinutes(5);
    internal const long MaxCost = 64L * 1024 * 1024;

    private readonly ISyncClient _client;
    private readonly SerializeFunc _serializeFunc;
    private readonly DeserializeFunc _deserializeFunc;
    internal readonly CostCache<CachedEntry<TValue>> ValueCache;

    // The keys that Get is loading from the server, with their evictions: a key evicted while it loads can have changed
    // after the server read the record, and the load must not cache that record then.
    private readonly object _loadsLock = new();
    private readonly Dictionary<string, KeyLoads> _loads = new();

    public CacheImpl(ISyncClient client, Failpoints? failpoints, SerializeFunc serializeFunc, DeserializeFunc deserializeFunc)
    {
        _client = client;
        _serializeFunc = serializeFunc;
        _deserializeFunc = deserializeFunc;
        ValueCache = new CostCache<CachedEntry<TValue>>(MaxCost);

        var err = Failpoints.Fail(failpoints, CacheManager.FailpointNewCache, null);
        if (err is not null)
        {
            ValueCache.Close();
            throw OxiaErrors.ToThrowable(err);
        }
    }

    /// <summary>The number of keys with a load in flight (tests assert it is 0 once the loads are done).</summary>
    internal int PendingLoads
    {
        get { lock (_loadsLock) return _loads.Count; }
    }

    public void HandleNotification(Notification n)
    {
        if (n.Type == NotificationType.KeyRangeRangeDeleted || n.Type == NotificationType.NotificationsMissed)
        {
            // The cache can't list the cached keys between the bounds of the range, so drop all the cached records.
            // Missed notifications can be of any record.
            EvictAll();
            return;
        }
        Evict(n.Key);
    }

    // The eviction and the Set of a load that raced it are ordered by the loads lock: either the load sees the eviction and
    // skips its Set, or the Set is applied before the eviction, which removes it.
    private void Evict(string key)
    {
        lock (_loadsLock)
        {
            if (_loads.TryGetValue(key, out var kl)) kl.Evictions++;
            ValueCache.Del(key);
        }
    }

    private void EvictAll()
    {
        lock (_loadsLock)
        {
            foreach (var kl in _loads.Values) kl.Evictions++;
            ValueCache.Clear();
        }
    }

    public ValueTask<(string InsertedKey, RecordVersion Version)> PutAsync(CancellationToken ct, string key, TValue value,
        params ReadOnlySpan<PutOption> options)
    {
        byte[] data;
        try
        {
            data = _serializeFunc(value);
        }
        catch (Exception e)
        {
            return ValueTask.FromException<(string, RecordVersion)>(OxiaErrors.Wrap(e, "failed to serialize value"));
        }

        ValueTask<(string InsertedKey, RecordVersion Version)> put;
        try
        {
            put = _client.PutAsync(ct, key, data, options);
        }
        catch (Exception e)
        {
            if (!OxiaErrors.Is(e, ClientErrors.UnexpectedVersionId)) Evict(key);
            throw;
        }
        return AfterPutAsync(key, put);
    }

    private async ValueTask<(string InsertedKey, RecordVersion Version)> AfterPutAsync(string key,
        ValueTask<(string InsertedKey, RecordVersion Version)> put)
    {
        Exception? err = null;
        try
        {
            return await put.ConfigureAwait(false);
        }
        catch (Exception e)
        {
            err = e;
            throw;
        }
        finally
        {
            if (!OxiaErrors.Is(err, ClientErrors.UnexpectedVersionId)) Evict(key);
        }
    }

    public ValueTask DeleteAsync(CancellationToken ct, string key, params ReadOnlySpan<DeleteOption> options)
    {
        ValueTask delete;
        try
        {
            delete = _client.DeleteAsync(ct, key, options);
        }
        catch
        {
            Evict(key);
            throw;
        }
        return AfterDeleteAsync(key, delete);
    }

    private async ValueTask AfterDeleteAsync(string key, ValueTask delete)
    {
        try
        {
            await delete.ConfigureAwait(false);
        }
        finally
        {
            Evict(key);
        }
    }

    public async ValueTask<(TValue Value, RecordVersion Version)> GetAsync(CancellationToken ct, string key)
    {
        var r = await TryGetAsync(ct, key).ConfigureAwait(false);
        if (r.Err is not null) throw OxiaErrors.ToThrowable(r.Err);
        return (r.Value!, r.Version);
    }

    public ValueTask<CacheGetResult<TValue>> TryGetAsync(CancellationToken ct, string key)
    {
        if (ValueCache.TryGet(key, out var cached))
        {
            // Allocation-free hit path
            return ValueTask.FromResult(cached.Present
                ? new CacheGetResult<TValue>(cached.Value, cached.Version, null)
                : new CacheGetResult<TValue>(default, default, ClientErrors.KeyNotFound));
        }

        return LoadAsync(ct, key);
    }

    private async ValueTask<CacheGetResult<TValue>> LoadAsync(CancellationToken ct, string key)
    {
        var (kl, evictions) = StartLoad(key);
        try
        {
            byte[]? data;
            RecordVersion existingVersion;
            try
            {
                (_, data, existingVersion) = await _client.GetAsync(ct, key).ConfigureAwait(false);
            }
            catch (Exception e) when (OxiaErrors.Is(e, ClientErrors.KeyNotFound))
            {
                // Use the shared Absent marker instead of wrapping a version: key-not-found is cached with cost 0 and no TTL
                SetLoaded(key, kl, evictions, CachedEntry<TValue>.Absent, 0, TimeSpan.Zero);
                return new CacheGetResult<TValue>(default, default, e);
            }
            catch (Exception e)
            {
                return new CacheGetResult<TValue>(default, default, e);
            }

            data ??= Array.Empty<byte>();
            TValue? value;
            try
            {
                value = (TValue?)_deserializeFunc(data, typeof(TValue));
            }
            catch (Exception e)
            {
                return new CacheGetResult<TValue>(default, default, OxiaErrors.Wrap(e, "failed to deserialize value"));
            }

            SetLoaded(key, kl, evictions, new CachedEntry<TValue>(value, existingVersion, true), data.Length, DefaultCacheTtl);
            return new CacheGetResult<TValue>(value, existingVersion, null);
        }
        finally
        {
            EndLoad(key, kl);
        }
    }

    // Cache the record only if the key was not evicted since the load started, as the eviction can be of a change applied
    // after the server read it.
    private void SetLoaded(string key, KeyLoads kl, ulong evictions, CachedEntry<TValue> entry, long cost, TimeSpan ttl)
    {
        lock (_loadsLock)
        {
            if (kl.Evictions == evictions)
            {
                ValueCache.Set(key, entry, cost, ttl);
                // Apply the Set now (ristretto buffers it; a no-op here)
                ValueCache.Wait();
            }
        }
    }

    // Registers a load of the key, and returns the evictions of the key so far.
    private (KeyLoads, ulong) StartLoad(string key)
    {
        lock (_loadsLock)
        {
            if (!_loads.TryGetValue(key, out var kl))
            {
                kl = new KeyLoads();
                _loads.Add(key, kl);
            }
            kl.Count++;
            return (kl, kl.Evictions);
        }
    }

    private void EndLoad(string key, KeyLoads kl)
    {
        lock (_loadsLock)
        {
            kl.Count--;
            if (kl.Count == 0) _loads.Remove(key);
        }
    }

    public ValueTask ReadModifyUpdateAsync(CancellationToken ct, string key, ModifyFunc<TValue> modifyFunc)
    {
        return Retry.RetryAsync(
            () => ReadModifyUpdateAttemptAsync(ct, key, modifyFunc),
            BackOffFactory.WithContext(new ExponentialBackOff(), ct), ct);
    }

    // One attempt of ReadModifyUpdate: a failure is thrown; PermanentException ends the retries (backoff.Permanent).
    private async ValueTask ReadModifyUpdateAttemptAsync(CancellationToken ct, string key, ModifyFunc<TValue> modifyFunc)
    {
        IOptional<TValue> optValue;
        long versionId;
        var got = await TryGetAsync(ct, key).ConfigureAwait(false);

        if (got.Err is not null)
        {
            if (!OxiaErrors.Is(got.Err, ClientErrors.KeyNotFound)) throw new PermanentException(got.Err);
            optValue = Optional.Empty<TValue>();
            versionId = RecordVersion.VersionIdNotExists;
        }
        else
        {
            optValue = Optional.Of(got.Value!);
            versionId = got.Version.VersionId;
        }

        TValue newValue;
        try
        {
            newValue = modifyFunc(optValue);
        }
        catch (Exception e)
        {
            throw new PermanentException(e);
        }

        try
        {
            await PutWithExpectedVersionAsync(ct, key, newValue, versionId).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            if (OxiaErrors.Is(e, ClientErrors.UnexpectedVersionId))
            {
                // Retry on conflict
                throw;
            }
            throw new PermanentException(e);
        }
    }

    // Not async: the options span cannot live across an await.
    private ValueTask<(string InsertedKey, RecordVersion Version)> PutWithExpectedVersionAsync(CancellationToken ct, string key,
        TValue value, long versionId)
    {
        return PutAsync(ct, key, value, OxiaOptions.ExpectedVersionId(versionId));
    }

    public ValueTask CloseAsync()
    {
        ValueCache.Close();
        return ValueTask.CompletedTask;
    }
}

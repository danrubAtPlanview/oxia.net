// =====================================================================================================================
// PORT NOTE (test support -- component F1)
//   Upstream : the hand-written fakes of oxia/{sessions,notifications,sequence_updates,cache}_test.go and the ones they borrow
//              from oxia/async_client_impl_test.go (staticShardManager), oxia/async_client_reroute_test.go
//              (splittableShardManager) and oxia/sync_client_impl_test.go (neverCompleteAsyncClient).
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Go embeds the interface in the fake struct (`internal.ShardManager`, `internal.RpcProvider`, `SyncClient`) so a
//              fake only implements the methods the test needs; the Unimplemented* base classes play that role.
//   Divergences: namespace Oxia.Client.Tests.F1Support, so that the same-named fakes of the other components' test ports never
//              clash; a Go channel `Changed() <-chan struct{}` is a Task; `nil` channel (never fires) is NeverTask.
// =====================================================================================================================
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Oxia.Client.Internal;
using Oxia.Common;
using Oxia.Common.Constant;
using Oxia.Common.Hash;
using Oxia.Common.Proto;
using Oxia.Common.Rpc;
using Oxia.Common.Time;

namespace Oxia.Client.Tests.F1Support;

internal static class Never
{
    /// <summary>A nil Go channel: never ready.</summary>
    public static readonly Task Task = new TaskCompletionSource().Task;
}

/// <summary>Polling helper (the TestUtil.Eventually of the shared test support is not available yet).</summary>
internal static class Wait
{
    public static async Task EventuallyAsync(Func<bool> condition, TimeSpan timeout, TimeSpan tick, string message = "condition not met")
    {
        var deadline = DateTime.UtcNow + timeout;
        while (true)
        {
            if (condition()) return;
            if (DateTime.UtcNow >= deadline) throw new Xunit.Sdk.XunitException("Eventually: " + message);
            await Task.Delay(tick);
        }
    }

    /// <summary>`select { case v := &lt;-task: ... case &lt;-time.After(timeout): FailNow }`.</summary>
    public static async Task<T> WithTimeout<T>(Task<T> task, TimeSpan timeout, string message)
    {
        var winner = await Task.WhenAny(task, Task.Delay(timeout));
        if (!ReferenceEquals(winner, task)) throw new Xunit.Sdk.XunitException(message);
        return await task;
    }

    public static async Task WithTimeout(Task task, TimeSpan timeout, string message)
    {
        var winner = await Task.WhenAny(task, Task.Delay(timeout));
        if (!ReferenceEquals(winner, task)) throw new Xunit.Sdk.XunitException(message);
        await task;
    }
}

/// <summary>Go's `internal.ShardManager` embedded as a nil interface: every method not overridden "panics".</summary>
internal class UnimplementedShardManager : IShardManager
{
    public virtual long Get(string key) => throw new NotImplementedException();
    public virtual long[] GetAll() => throw new NotImplementedException();
    public virtual string Leader(long shardId) => throw new NotImplementedException();
    public virtual bool Exists(long shardId) => throw new NotImplementedException();
    public virtual long[]? GetSuccessors(long shardId) => throw new NotImplementedException();
    public virtual bool GetSuccessor(long shardId, string key, out long successorShardId) => throw new NotImplementedException();
    public virtual Task Changed() => throw new NotImplementedException();
    public virtual KeySorting KeySorting => throw new NotImplementedException();
    public virtual void Dispose() { }
}

/// <summary>Go's `internal.RpcProvider` embedded as a nil interface.</summary>
internal class UnimplementedRpcProvider : IRpcProvider
{
    public virtual ValueTask<WriteResponse> ExecuteWrite(CancellationToken ct, long shardId, Func<WriteRequest> prepare) => throw new NotImplementedException();
    public virtual ValueTask<ReadResponse> ExecuteRead(CancellationToken ct, ReadRequest request) => throw new NotImplementedException();
    public virtual ValueTask ExecuteList(CancellationToken ct, ListRequest request, Func<ListResponse, Exception?> listResponseConsumer) => throw new NotImplementedException();
    public virtual ValueTask ExecuteRangeScan(CancellationToken ct, RangeScanRequest request, Func<RangeScanResponse, Exception?> rangeScanResponseConsumer) => throw new NotImplementedException();
    public virtual ValueTask<IServerStreamingClient<ShardAssignments>> GetShardAssignments(CancellationToken ct, string target, ShardAssignmentsRequest request) => throw new NotImplementedException();
    public virtual ValueTask<IServerStreamingClient<GetSequenceUpdatesResponse>> GetSequenceUpdates(CancellationToken ct, string target, GetSequenceUpdatesRequest request) => throw new NotImplementedException();
    public virtual ValueTask<IServerStreamingClient<NotificationBatch>> GetNotifications(CancellationToken ct, string target, NotificationsRequest request) => throw new NotImplementedException();
    public virtual ValueTask<CreateSessionResponse> CreateSession(CancellationToken ct, string target, CreateSessionRequest request) => throw new NotImplementedException();
    public virtual ValueTask<KeepAliveResponse> KeepAlive(CancellationToken ct, string target, SessionHeartbeat request) => throw new NotImplementedException();
    public virtual ValueTask<CloseSessionResponse> CloseSession(CancellationToken ct, string target, CloseSessionRequest request) => throw new NotImplementedException();
    public virtual ValueTask CloseAsync() => ValueTask.CompletedTask;
}

/// <summary>Go: staticShardManager -- routes every key to its first shard.</summary>
internal sealed class StaticShardManager : IShardManager
{
    public long[] Shards = Array.Empty<long>();
    public string LeaderAddress = "";
    public Dictionary<long, long[]> Successors = new();
    public KeySorting KeySortingValue { get; set; }
    public bool Closed;

    public void Dispose() => Closed = true;
    public long Get(string key) => Shards[0];
    public long[] GetAll() => Shards;
    public string Leader(long shardId) => LeaderAddress;
    public bool Exists(long shardId) => true;
    public long[]? GetSuccessors(long shardId) => Successors.TryGetValue(shardId, out var s) ? s : null;

    /// <summary>Routes every key to the first successor.</summary>
    public bool GetSuccessor(long shardId, string key, out long successorShardId)
    {
        if (Successors.TryGetValue(shardId, out var s) && s.Length > 0) { successorShardId = s[0]; return true; }
        successorShardId = 0;
        return false;
    }

    public Task Changed() => Never.Task;
    public KeySorting KeySorting => KeySortingValue;
}

/// <summary>Go: splittableShardManager -- a ShardManager whose shards can be split, to simulate the assignments update a client receives after a shard split.</summary>
internal class SplittableShardManager : IShardManager
{
    private readonly object _lock = new();
    private readonly List<Shard> _shards;
    private readonly Dictionary<long, Shard[]> _successors = new();
    private readonly string _leader;
    private TaskCompletionSource _changed = NewChanged();
    public ShardsReplacedListener OnShardsReplaced = _ => { };

    private static TaskCompletionSource NewChanged() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>A single shard, 0, on the whole hash range, led by the given leader.</summary>
    public SplittableShardManager(string leader)
    {
        _leader = leader;
        _shards = new List<Shard> { new(0, "", new HashRange(0, uint.MaxValue)) };
    }

    public void Dispose() { }

    private static Func<Shard, bool> Strategy(string key)
    {
        uint code = Hasher.Xxh332(key);
        return shard => shard.HashRange.MinInclusive <= code && code <= shard.HashRange.MaxInclusive;
    }

    public long Get(string key)
    {
        lock (_lock)
        {
            var predicate = Strategy(key);
            foreach (var shard in _shards) if (predicate(shard)) return shard.Id;
            throw new InvalidOperationException("shard not found");
        }
    }

    public long[] GetAll()
    {
        lock (_lock) return _shards.Select(s => s.Id).ToArray();
    }

    public string Leader(long shardId) => _leader;

    public bool Exists(long shardId)
    {
        lock (_lock) return _shards.Any(s => s.Id == shardId);
    }

    public long[]? GetSuccessors(long shardId)
    {
        lock (_lock) return _successors.TryGetValue(shardId, out var s) ? s.Select(x => x.Id).ToArray() : null;
    }

    public bool GetSuccessor(long shardId, string key, out long successorShardId)
    {
        lock (_lock)
        {
            var predicate = Strategy(key);
            if (_successors.TryGetValue(shardId, out var s))
            {
                foreach (var shard in s)
                {
                    if (predicate(shard)) { successorShardId = shard.Id; return true; }
                }
            }
        }
        successorShardId = 0;
        return false;
    }

    public virtual Task Changed()
    {
        lock (_lock) return _changed.Task;
    }

    public KeySorting KeySorting => KeySorting.Unknown;

    /// <summary>Replaces the parent shard with two shards covering half its hash range each.</summary>
    public void Split(long parent, long left, long right)
    {
        lock (_lock)
        {
            for (int i = 0; i < _shards.Count; i++)
            {
                var shard = _shards[i];
                if (shard.Id != parent) continue;
                var r = shard.HashRange;
                uint mid = r.MinInclusive + (r.MaxInclusive - r.MinInclusive) / 2;
                var children = new[]
                {
                    new Shard(left, "", new HashRange(r.MinInclusive, mid)),
                    new Shard(right, "", new HashRange(mid + 1, r.MaxInclusive)),
                };
                _shards.RemoveAt(i);
                _shards.AddRange(children);
                _successors[parent] = children;
                OnShardsReplaced(new Dictionary<long, long[]> { [parent] = new[] { left, right } });
                var old = _changed;
                _changed = NewChanged();
                old.SetResult();
                return;
            }
        }
    }
}

/// <summary>Go: unnotifiedShardManager -- doesn't notify the changes of its shard map.</summary>
internal sealed class UnnotifiedShardManager : IShardManager
{
    private readonly SplittableShardManager _inner;
    public UnnotifiedShardManager(SplittableShardManager inner) => _inner = inner;
    public void Dispose() { }
    public long Get(string key) => _inner.Get(key);
    public long[] GetAll() => _inner.GetAll();
    public string Leader(long shardId) => _inner.Leader(shardId);
    public bool Exists(long shardId) => _inner.Exists(shardId);
    public long[]? GetSuccessors(long shardId) => _inner.GetSuccessors(shardId);
    public bool GetSuccessor(long shardId, string key, out long successorShardId) => _inner.GetSuccessor(shardId, key, out successorShardId);
    public Task Changed() => Never.Task;
    public KeySorting KeySorting => _inner.KeySorting;
}

/// <summary>Go: notificationsTestShardManager.</summary>
internal sealed class NotificationsTestShardManager : UnimplementedShardManager
{
    private readonly object _lock = new();
    public List<long> Shards = new();
    public Dictionary<long, long[]> Successors = new();
    private TaskCompletionSource? _changed;

    public override long[] GetAll()
    {
        lock (_lock) return Shards.ToArray();
    }

    public override string Leader(long shardId) => "server-1";

    public override bool Exists(long shardId)
    {
        lock (_lock) return Shards.Contains(shardId);
    }

    public override long[]? GetSuccessors(long shardId)
    {
        lock (_lock) return Successors.TryGetValue(shardId, out var s) ? s : null;
    }

    public override Task Changed()
    {
        lock (_lock)
        {
            _changed ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            return _changed.Task;
        }
    }

    /// <summary>Replaces the shard with its successors in the shard map.</summary>
    public void Split(long shard, params long[] successors)
    {
        TaskCompletionSource? old;
        lock (_lock)
        {
            Shards.RemoveAll(s => s == shard);
            Shards.AddRange(successors);
            Successors[shard] = successors;
            old = _changed;
            _changed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }
        old?.SetResult();
    }
}

/// <summary>Records the messages of all the log records, at every level (Go: debugLogs).</summary>
internal sealed class DebugLogs : ILogger
{
    private readonly object _lock = new();
    private readonly List<string> _logged = new();

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        lock (_lock) _logged.Add(formatter(state, exception));
    }

    public List<string> Messages()
    {
        lock (_lock) return new List<string>(_logged);
    }
}

/// <summary>Counts the resets of the backoff (Go: notificationsTestBackOff / resetCountingBackOff).</summary>
internal sealed class ResetCountingBackOff : IBackOff
{
    private readonly IBackOff _inner;
    private int _resets;
    public ResetCountingBackOff(IBackOff inner) => _inner = inner;
    public int Resets => Volatile.Read(ref _resets);
    public TimeSpan NextBackOff() => _inner.NextBackOff();
    public void Reset()
    {
        Interlocked.Increment(ref _resets);
        _inner.Reset();
    }
}

/// <summary>cenkalti backoff.WithMaxRetries.</summary>
internal sealed class MaxRetriesBackOff : IBackOff
{
    private readonly IBackOff _inner;
    private readonly int _maxTries;
    private int _numTries;
    public MaxRetriesBackOff(IBackOff inner, int maxTries) { _inner = inner; _maxTries = maxTries; }

    public TimeSpan NextBackOff()
    {
        if (_maxTries > 0)
        {
            if (_maxTries <= _numTries) return BackOffConstants.Stop;
            _numTries++;
        }
        return _inner.NextBackOff();
    }

    public void Reset()
    {
        _numTries = 0;
        _inner.Reset();
    }
}

/// <summary>Go: testNotifications -- delivers the notifications that the test sends on Channel.</summary>
internal sealed class TestNotifications : INotifications
{
    public readonly Channel<Notification> Channel = System.Threading.Channels.Channel.CreateUnbounded<Notification>();
    private int _closed;
    public bool Closed => Volatile.Read(ref _closed) != 0;
    public ChannelReader<Notification> Ch => Channel.Reader;

    public ValueTask CloseAsync()
    {
        Volatile.Write(ref _closed, 1);
        return ValueTask.CompletedTask;
    }
}

/// <summary>Go: neverCompleteAsyncClient -- never completes the operations.  GetNotificationsAsync returns Notifications and fails with NotificationsErr.</summary>
internal sealed class NeverCompleteAsyncClient : IAsyncClient
{
    public INotifications? Notifications { get; set; }
    public Exception? NotificationsErr { get; set; }
    private long _notificationsRequests;
    public long NotificationsRequests => Interlocked.Read(ref _notificationsRequests);

    public ValueTask CloseAsync() => ValueTask.CompletedTask;

    public ValueTask<PutResult> PutAsync(string key, ReadOnlyMemory<byte> value, params ReadOnlySpan<PutOption> options)
        => new(new TaskCompletionSource<PutResult>().Task);

    public ValueTask<Exception?> DeleteAsync(string key, params ReadOnlySpan<DeleteOption> options)
        => new(new TaskCompletionSource<Exception?>().Task);

    public ValueTask<Exception?> DeleteRangeAsync(string minKeyInclusive, string maxKeyExclusive, params ReadOnlySpan<DeleteRangeOption> options)
        => new(new TaskCompletionSource<Exception?>().Task);

    public ValueTask<GetResult> GetAsync(string key, params ReadOnlySpan<GetOption> options)
        => new(new TaskCompletionSource<GetResult>().Task);

    public ChannelReader<ListResult> ListAsync(CancellationToken ct, string minKeyInclusive, string maxKeyExclusive, params ReadOnlySpan<ListOption> options)
        => throw new NotImplementedException();

    public ChannelReader<GetResult> RangeScanAsync(CancellationToken ct, string minKeyInclusive, string maxKeyExclusive, params ReadOnlySpan<RangeScanOption> options)
        => throw new NotImplementedException();

    public ChannelReader<string> GetSequenceUpdates(CancellationToken ct, string prefixKey, params ReadOnlySpan<GetSequenceUpdatesOption> options)
        => throw new NotImplementedException();

    public ValueTask<INotifications> GetNotificationsAsync()
    {
        Interlocked.Increment(ref _notificationsRequests);
        if (NotificationsErr is not null) return ValueTask.FromException<INotifications>(NotificationsErr);
        return new ValueTask<INotifications>(Notifications!);
    }
}

/// <summary>JSON serializers used as the SerializeFunc / DeserializeFunc of the cache tests (Go: json.Marshal / json.Unmarshal).</summary>
internal static class Json
{
    public static readonly SerializeFunc Marshal = value => JsonSerializer.SerializeToUtf8Bytes(value, value?.GetType() ?? typeof(object));
    public static readonly DeserializeFunc Unmarshal = (data, type) => JsonSerializer.Deserialize(data.Span, type);
}

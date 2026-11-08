// =====================================================================================================================
// PORT NOTE (test fakes -- no Go counterpart as a file; they replace the hand-rolled mocks of oxia/internal/*_test.go)
//   Upstream : oxia/internal/{shard_manager_test,write_stream_test,rpc_provider_test}.go  (failingAssignmentsStream,
//              singleAssignmentsStream, failingAssignmentsRpcProvider, singleAssignmentsRpcProvider,
//              pendingAssignmentsRpcProvider, testWriteStream, testShardManager, recordingClientPool)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Fakes of the seams of the component (IClientPool / IOxiaClientRpc / IWriteStreamCall / IShardManager /
//              IRpcProvider) so that the logic of the shard manager, the rpc provider and the stream wrapper is exercised
//              without any network.  Members that the Go mock embeds as a nil interface (and that panic if called) throw
//              NotImplementedException here.
//   Divergences: Go `Recv() (msg, error)` is `RecvAsync` (null == io.EOF, failures are thrown).
// =====================================================================================================================
using Xunit;
using System.Collections.Concurrent;
using System.Threading.Channels;
using Grpc.Core;
using Oxia.Client.Internal;
using Oxia.Common;
using Oxia.Common.Constant;
using Oxia.Common.Proto;
using Oxia.Common.Rpc;

namespace Oxia.Client.Tests.Internal.Fakes;

/// <summary>A scripted server stream: values and failures are pushed by the test; RecvAsync returns null once completed.</summary>
public sealed class FakeServerStream<T> : IServerStreamingClient<T> where T : class
{
    private readonly Channel<(T? Value, Exception? Error)> _channel = Channel.CreateUnbounded<(T?, Exception?)>();

    public bool Disposed { get; private set; }

    public FakeServerStream<T> Push(T value)
    {
        _channel.Writer.TryWrite((value, null));
        return this;
    }

    /// <summary>The next RecvAsync throws <paramref name="error"/> (a sentinel is wrapped so that it can be thrown).</summary>
    public FakeServerStream<T> Fail(Exception error)
    {
        _channel.Writer.TryWrite((null, error));
        return this;
    }

    /// <summary>The end of the stream (io.EOF): RecvAsync returns null once the pushed values were consumed.</summary>
    public FakeServerStream<T> Complete()
    {
        _channel.Writer.TryComplete();
        return this;
    }

    public async ValueTask<T?> RecvAsync(CancellationToken ct = default)
    {
        try
        {
            var (value, error) = await _channel.Reader.ReadAsync(ct).ConfigureAwait(false);
            if (error is not null) throw OxiaErrors.ToThrowable(error);
            return value;
        }
        catch (ChannelClosedException)
        {
            return null;
        }
    }

    public void Dispose() => Disposed = true;
}

/// <summary>Go: testWriteStream -- counts the requests sent on it, answers each with an empty response if auto-responding.</summary>
public sealed class FakeWriteStream : IWriteStreamCall
{
    private readonly Channel<WriteResponse> _responses = Channel.CreateUnbounded<WriteResponse>();
    private long _sent;
    private int _closeSend;

    public FakeWriteStream(CancellationToken context, bool autoRespond = false)
    {
        Context = context;
        AutoRespond = autoRespond;
    }

    public CancellationToken Context { get; }

    /// <summary>Go: responses != nil -- each Send is answered with an empty response.</summary>
    public bool AutoRespond { get; }

    /// <summary>Go: sendErr.</summary>
    public Exception? SendError { get; set; }

    public long Sent => Interlocked.Read(ref _sent);
    public int CloseSendCalls => Volatile.Read(ref _closeSend);
    public bool Disposed { get; private set; }
    public List<WriteRequest> Requests { get; } = [];

    /// <summary>Overrides CloseSend (Go: old.stream.CloseSend()).</summary>
    public Exception? CloseSendError { get; set; }

    public ValueTask SendAsync(WriteRequest request, CancellationToken ct = default)
    {
        if (SendError is { } err) return ValueTask.FromException(err);
        Interlocked.Increment(ref _sent);
        lock (Requests) Requests.Add(request);
        if (AutoRespond) _responses.Writer.TryWrite(new WriteResponse());
        return ValueTask.CompletedTask;
    }

    /// <summary>Pushes a response by hand (stream that does not auto respond).</summary>
    public void Respond(WriteResponse response) => _responses.Writer.TryWrite(response);

    /// <summary>The stream ends (io.EOF).</summary>
    public void CompleteStream() => _responses.Writer.TryComplete();

    /// <summary>The stream fails with <paramref name="error"/>.</summary>
    public void FailStream(Exception error) => _responses.Writer.TryComplete(error);

    public async ValueTask<WriteResponse?> RecvAsync(CancellationToken ct = default)
    {
        try
        {
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, Context);
            return await _responses.Reader.ReadAsync(linked.Token).ConfigureAwait(false);
        }
        catch (ChannelClosedException e) when (e.InnerException is null)
        {
            return null;
        }
        catch (ChannelClosedException e)
        {
            throw OxiaErrors.ToThrowable(e.InnerException!);
        }
    }

    public ValueTask CloseSendAsync()
    {
        Interlocked.Increment(ref _closeSend);
        return CloseSendError is { } err ? ValueTask.FromException(err) : ValueTask.CompletedTask;
    }

    public void Dispose() => Disposed = true;
}

/// <summary>A configurable IOxiaClientRpc: every call goes to a hook, or throws NotImplementedException.</summary>
public class FakeOxiaClientRpc : IOxiaClientRpc
{
    public Func<Metadata?, CancellationToken, IWriteStreamCall>? OnWriteStream { get; set; }
    public Func<ReadRequest, CancellationToken, IServerStreamingClient<ReadResponse>>? OnRead { get; set; }
    public Func<ListRequest, CancellationToken, IServerStreamingClient<ListResponse>>? OnList { get; set; }
    public Func<RangeScanRequest, CancellationToken, IServerStreamingClient<RangeScanResponse>>? OnRangeScan { get; set; }
    public Func<ShardAssignmentsRequest, CancellationToken, IServerStreamingClient<ShardAssignments>>? OnGetShardAssignments { get; set; }
    public Func<GetSequenceUpdatesRequest, CancellationToken, IServerStreamingClient<GetSequenceUpdatesResponse>>? OnGetSequenceUpdates { get; set; }
    public Func<NotificationsRequest, CancellationToken, IServerStreamingClient<NotificationBatch>>? OnGetNotifications { get; set; }
    public Func<CreateSessionRequest, CancellationToken, ValueTask<CreateSessionResponse>>? OnCreateSession { get; set; }
    public Func<SessionHeartbeat, CancellationToken, ValueTask<KeepAliveResponse>>? OnKeepAlive { get; set; }
    public Func<CloseSessionRequest, CancellationToken, ValueTask<CloseSessionResponse>>? OnCloseSession { get; set; }

    public IWriteStreamCall WriteStream(Metadata? headers, CancellationToken ct) => Hook(OnWriteStream)(headers, ct);
    public IServerStreamingClient<ReadResponse> Read(ReadRequest request, CancellationToken ct) => Hook(OnRead)(request, ct);
    public IServerStreamingClient<ListResponse> List(ListRequest request, CancellationToken ct) => Hook(OnList)(request, ct);
    public IServerStreamingClient<RangeScanResponse> RangeScan(RangeScanRequest request, CancellationToken ct) => Hook(OnRangeScan)(request, ct);
    public IServerStreamingClient<ShardAssignments> GetShardAssignments(ShardAssignmentsRequest request, CancellationToken ct) => Hook(OnGetShardAssignments)(request, ct);
    public IServerStreamingClient<GetSequenceUpdatesResponse> GetSequenceUpdates(GetSequenceUpdatesRequest request, CancellationToken ct) => Hook(OnGetSequenceUpdates)(request, ct);
    public IServerStreamingClient<NotificationBatch> GetNotifications(NotificationsRequest request, CancellationToken ct) => Hook(OnGetNotifications)(request, ct);
    public ValueTask<CreateSessionResponse> CreateSessionAsync(CreateSessionRequest request, CancellationToken ct) => Hook(OnCreateSession)(request, ct);
    public ValueTask<KeepAliveResponse> KeepAliveAsync(SessionHeartbeat request, CancellationToken ct) => Hook(OnKeepAlive)(request, ct);
    public ValueTask<CloseSessionResponse> CloseSessionAsync(CloseSessionRequest request, CancellationToken ct) => Hook(OnCloseSession)(request, ct);

    private static TDelegate Hook<TDelegate>(TDelegate? hook) where TDelegate : class =>
        hook ?? throw new NotImplementedException();
}

/// <summary>Go: recordingClientPool -- records the targets that connections are requested for, and fails to connect with ConnectError, if set.</summary>
public class FakeClientPool : IClientPool
{
    private readonly object _lock = new();
    private readonly List<string> _targets = [];
    private readonly Func<string, IOxiaClientRpc>? _factory;

    public FakeClientPool(Func<string, IOxiaClientRpc>? factory = null) => _factory = factory;

    /// <summary>Go: err -- fails to connect with this error, if set.</summary>
    public Exception? ConnectError { get; set; }

    public bool Closed { get; protected set; }

    public IReadOnlyList<string> Targets
    {
        get { lock (_lock) return [.. _targets]; }
    }

    public IOxiaClientRpc GetClientRpc(string target)
    {
        lock (_lock) _targets.Add(target);
        if (ConnectError is { } err) throw err;
        return Connect(target);
    }

    protected virtual IOxiaClientRpc Connect(string target) =>
        (_factory ?? throw new NotImplementedException()).Invoke(target);

    public Io.Oxia.Proto.V1.OxiaAdmin.OxiaAdminClient GetAdminRpc(string target) => throw new NotImplementedException();

    public virtual ValueTask CloseAsync()
    {
        Closed = true;
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync() => CloseAsync();
}

/// <summary>Go: recordingClientPool wrapping the real pool (<c>ClientPool: provider.clientPool</c>).</summary>
public sealed class RecordingClientPool : FakeClientPool
{
    private readonly IClientPool _inner;

    public RecordingClientPool(IClientPool inner) : base(null) => _inner = inner;

    protected override IOxiaClientRpc Connect(string target) => _inner.GetClientRpc(target);

    public override async ValueTask CloseAsync()
    {
        await base.CloseAsync().ConfigureAwait(false);
        await _inner.CloseAsync().ConfigureAwait(false);
    }
}

/// <summary>Go: testShardManager -- every shard exists, the leader is set by the test, Changed is a task set by the test.</summary>
public sealed class FakeShardManager : IShardManager
{
    private volatile string _leader = "";
    private volatile Task? _changed;

    public string LeaderAddress
    {
        get => _leader;
        set => _leader = value;
    }

    /// <summary>The task returned by <see cref="Changed"/> (Go: Changed() returns nil -- null here is a task that never completes).</summary>
    public Task? ChangedTask
    {
        get => _changed;
        set => _changed = value;
    }

    public Func<long, bool> ExistsFunc { get; set; } = static _ => true;
    public Func<long, string>? LeaderFunc { get; set; }
    public int Leader_Calls;

    public bool Exists(long shardId) => ExistsFunc(shardId);

    public string Leader(long shardId)
    {
        Interlocked.Increment(ref Leader_Calls);
        return LeaderFunc is { } f ? f(shardId) : _leader;
    }

    public Task Changed() => _changed ?? NeverCompletes;

    private static readonly Task NeverCompletes = new TaskCompletionSource().Task;

    public long Get(string key) => throw new NotImplementedException();
    public long[] GetAll() => throw new NotImplementedException();
    public long[]? GetSuccessors(long shardId) => throw new NotImplementedException();
    public bool GetSuccessor(long shardId, string key, out long successorShardId) => throw new NotImplementedException();
    public KeySorting KeySorting => throw new NotImplementedException();
    public void Dispose() { }
}

/// <summary>An IRpcProvider that only serves GetShardAssignments, through a hook (the Go mocks embed a nil RpcProvider).</summary>
public class FakeRpcProvider : IRpcProvider
{
    public Func<CancellationToken, string, ShardAssignmentsRequest, ValueTask<IServerStreamingClient<ShardAssignments>>>? OnGetShardAssignments { get; set; }

    public ValueTask<IServerStreamingClient<ShardAssignments>> GetShardAssignments(CancellationToken ct, string target, ShardAssignmentsRequest request) =>
        (OnGetShardAssignments ?? throw new NotImplementedException())(ct, target, request);

    public ValueTask<IServerStreamingClient<GetSequenceUpdatesResponse>> GetSequenceUpdates(CancellationToken ct, string target, GetSequenceUpdatesRequest request) => throw new NotImplementedException();
    public ValueTask<IServerStreamingClient<NotificationBatch>> GetNotifications(CancellationToken ct, string target, NotificationsRequest request) => throw new NotImplementedException();
    public ValueTask<CreateSessionResponse> CreateSession(CancellationToken ct, string target, CreateSessionRequest request) => throw new NotImplementedException();
    public ValueTask<KeepAliveResponse> KeepAlive(CancellationToken ct, string target, SessionHeartbeat request) => throw new NotImplementedException();
    public ValueTask<CloseSessionResponse> CloseSession(CancellationToken ct, string target, CloseSessionRequest request) => throw new NotImplementedException();
    public ValueTask<WriteResponse> ExecuteWrite(CancellationToken ct, long shardId, Func<WriteRequest> prepare) => throw new NotImplementedException();
    public ValueTask<ReadResponse> ExecuteRead(CancellationToken ct, ReadRequest request) => throw new NotImplementedException();
    public ValueTask ExecuteList(CancellationToken ct, ListRequest request, Func<ListResponse, Exception?> listResponseConsumer) => throw new NotImplementedException();
    public ValueTask ExecuteRangeScan(CancellationToken ct, RangeScanRequest request, Func<RangeScanResponse, Exception?> rangeScanResponseConsumer) => throw new NotImplementedException();
    public ValueTask CloseAsync() => ValueTask.CompletedTask;
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

/// <summary>Shared builders of the shard assignments used by the shard manager tests.</summary>
public static class Assignments
{
    /// <summary>One shard with the default (0, 0) hash range -- Go: `{ShardBoundaries: &amp;Int32HashRange{Int32HashRange: &amp;Int32HashRange{}}}`.</summary>
    public static ShardAssignments Single(string ns, KeySorting keySorting = default)
    {
        var assignments = new ShardAssignments();
        var namespaceAssignments = new NamespaceShardsAssignment { KeySorting = keySorting };
        namespaceAssignments.Assignments.Add(new ShardAssignment { Int32HashRange = new Int32HashRange() });
        assignments.Namespaces[ns] = namespaceAssignments;
        return assignments;
    }
}

/// <summary>Go: failingAssignmentsStream -- sends the shard assignments a number of times, then fails with err.</summary>
public sealed class FailingAssignmentsStream : IServerStreamingClient<ShardAssignments>
{
    private readonly string _namespace;
    private int _assignments;
    private readonly Exception _err;

    public FailingAssignmentsStream(string ns, int assignments, Exception err)
    {
        _namespace = ns;
        _assignments = assignments;
        _err = err;
    }

    public ValueTask<ShardAssignments?> RecvAsync(CancellationToken ct = default)
    {
        if (_assignments == 0) return ValueTask.FromException<ShardAssignments?>(OxiaErrors.ToThrowable(_err));
        _assignments--;
        return ValueTask.FromResult<ShardAssignments?>(Assignments.Single(_namespace));
    }

    public void Dispose() { }
}

/// <summary>Go: failingAssignmentsRpcProvider.</summary>
public sealed class FailingAssignmentsRpcProvider : FakeRpcProvider
{
    public FailingAssignmentsRpcProvider(Exception err, int assignments = 0)
    {
        OnGetShardAssignments = (_, _, request) =>
            ValueTask.FromResult<IServerStreamingClient<ShardAssignments>>(new FailingAssignmentsStream(request.Namespace, assignments, err));
    }
}

/// <summary>Go: singleAssignmentsStream -- sends one set of assignments, then waits for the shard manager to close.</summary>
public sealed class SingleAssignmentsStream : IServerStreamingClient<ShardAssignments>
{
    private readonly CancellationToken _ctx;
    private ShardAssignments? _assignments;

    public SingleAssignmentsStream(CancellationToken ctx, ShardAssignments assignments)
    {
        _ctx = ctx;
        _assignments = assignments;
    }

    public async ValueTask<ShardAssignments?> RecvAsync(CancellationToken ct = default)
    {
        if (Interlocked.Exchange(ref _assignments, null) is { } assignments) return assignments;
        await Task.Delay(Timeout.Infinite, _ctx).ConfigureAwait(false); // <-s.ctx.Done(); return nil, s.ctx.Err()
        return null;
    }

    public void Dispose() { }
}

/// <summary>Go: singleAssignmentsRpcProvider -- sends the given assignments if set, else one shard with the key sorting.</summary>
public sealed class SingleAssignmentsRpcProvider : FakeRpcProvider
{
    public SingleAssignmentsRpcProvider(KeySorting keySorting = default, ShardAssignments? assignments = null)
    {
        OnGetShardAssignments = (ctx, _, request) =>
            ValueTask.FromResult<IServerStreamingClient<ShardAssignments>>(new SingleAssignmentsStream(ctx,
                assignments ?? Assignments.Single(request.Namespace, keySorting)));
    }
}

/// <summary>Go: pendingAssignmentsRpcProvider -- waits for the shard manager to cancel the request of the assignments.</summary>
public sealed class PendingAssignmentsRpcProvider : FakeRpcProvider
{
    private readonly TaskCompletionSource _canceled = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public PendingAssignmentsRpcProvider()
    {
        OnGetShardAssignments = async (ctx, _, _) =>
        {
            try
            {
                await Task.Delay(Timeout.Infinite, ctx).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                _canceled.TrySetResult();
                throw;
            }

            throw new InvalidOperationException("unreachable");
        };
    }

    /// <summary>Completes when the request of the assignments was canceled.</summary>
    public Task Canceled => _canceled.Task;
}

/// <summary>A thread-safe list of the warnings logged (Go: warningsHandler) plus the other levels, for assertions.</summary>
public sealed class CapturingLogger : Microsoft.Extensions.Logging.ILogger
{
    private readonly ConcurrentQueue<(Microsoft.Extensions.Logging.LogLevel Level, string Message)> _entries = new();
    private readonly TaskCompletionSource _warning = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly Microsoft.Extensions.Logging.LogLevel _minLevel;

    public CapturingLogger(Microsoft.Extensions.Logging.LogLevel minLevel = Microsoft.Extensions.Logging.LogLevel.Debug) => _minLevel = minLevel;

    /// <summary>Completes when the first warning was logged (Go: warningsHandler.warnings).</summary>
    public Task Warned => _warning.Task;

    public IReadOnlyCollection<(Microsoft.Extensions.Logging.LogLevel Level, string Message)> Entries => _entries.ToArray();

    public string Text => string.Join('\n', _entries.Select(static e => e.Message));

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => logLevel >= _minLevel;

    public void Log<TState>(Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId, TState state,
        Exception? exception, Func<TState, Exception?, string> formatter)
    {
        if (!IsEnabled(logLevel)) return;
        _entries.Enqueue((logLevel, formatter(state, exception)));
        if (logLevel == Microsoft.Extensions.Logging.LogLevel.Warning) _warning.TrySetResult();
    }
}

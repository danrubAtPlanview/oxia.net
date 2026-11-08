// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/sessions.go   (sessions, clientSession, currentSuccessors)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The client sessions that tie ephemeral records to the life of the client.  There is one session per shard,
//              created lazily by the first ephemeral operation on the shard (createSessionWithRetries: exponential backoff
//              whose wait is cut short by any change of the shard map, because it can be the split of the shard, which ends
//              the creation with ErrShardNotFound), and kept alive by a heartbeat loop (ticker >= 2s; the backoff is reset
//              after every successful heartbeat; ErrSessionNotFound forgets the session).  When a shard is split, the shards
//              that replaced it INHERIT its session (and its ephemeral records), so followSplits -- run on every change of the
//              shard map and, under the lock, when a lookup misses -- moves the session to them, keeps it alive there and
//              cancels the parent's.  ExecuteWithSessionId looks the shard up UNDER the lock for that reason; when the session
//              fails to start it is discarded (never re-acquiring the lock: that was a self-deadlock), and when the shard was
//              split before the session was established the key is looked up again.  Close stops the sessions first, then
//              closes each one on its leader (ErrSessionNotFound is success), combining the errors.
//   Divergences:
//      * ExecuteWithSessionId NEVER BLOCKS.  Go's mutex is held while the operation waits for the session to be established,
//        serialising the operations behind it.  Here that "logical lock" is kept: while the operation at the head waits (the
//        session is being created) the following operations are queued FIFO; whoever establishes (or fails) the session resumes
//        the head and drains the queue under the lock, so the callback may run later, on another thread.  When the session
//        already exists and nothing is queued, the callback runs inline on the caller's thread WITHOUT taking the lock and
//        without allocating (lock-free read of a ConcurrentDictionary; the lock is only taken to create/move/forget sessions).
//      * ExecuteOrdered (review R2-D3): a plain write issued while an ephemeral put waits for its session is queued in the same FIFO, so it
//        cannot overtake it (Go's Put blocks in Add until the ephemeral call is in the batcher).  Inline and lock-free when nothing waits.
//      * `cs.started chan error` -> flags under the session's lock; the operation waiting on it registers as the session's
//        `waiter` (plus a cancellation registration on the session token) and is resumed through OnSessionSettled.
//      * contexts -> CancellationTokenSource (sessions token linked to the client token; session token linked to the
//        sessions token).  A token cannot tell Canceled from DeadlineExceeded, which executeWithId uses to fail the callbacks
//        when the CLIENT context EXPIRES but not when it is closed: `ContextError` (null in production: the client context is
//        only ever cancelled) provides the expiry error; tests set it.
//      * The goroutine of followSplitsOnShardMapChange, createSessionWithRetries and the keep-alive loop are Task.Run loops;
//        the keep-alive ticker is a PeriodicTimer.  The keep-alive loop does not take the sessions/session locks to read the
//        immutable fields of an established session.  On ErrSessionNotFound only the session itself is forgotten (Go deletes
//        whatever session is registered on the shard; the same unless a newer one replaced it).
//      * Close cannot hold the lock across the CloseSession calls: it snapshots the sessions under the lock.
//      * Session creation / close request timeouts surface as TimeoutException (Go: context.DeadlineExceeded).
// =====================================================================================================================
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Oxia.Common;
using Oxia.Common.Constant;
using Oxia.Common.Logging;
using Oxia.Common.Proto;
using Oxia.Common.Time;
using Oxia.Client.Internal;

namespace Oxia.Client;

/// <summary>What happened when an operation tried to use a session (Go: the three outcomes of clientSession.executeWithId).</summary>
internal enum ExecuteResult
{
    /// <summary>The callback was invoked, or dropped because the session was closed.</summary>
    Done,
    /// <summary>The session failed to start: the error is returned and the callback NOT invoked.</summary>
    Failed,
    /// <summary>The session is not established yet.</summary>
    Pending,
}

internal sealed class Sessions : IAsyncCloser
{
    private struct Op
    {
        public Func<long> ShardForKey;
        public Action<long, long, Exception?> Callback;
        public long ShardId;
        public ClientSession? Session;   // the session the operation waits for (Current only)
        public Action? Work;             // a plain operation that only has to run in order (ExecuteOrdered)
    }

    internal readonly string ClientIdentity;
    internal readonly CancellationToken Ctx;
    // SessionsCtx is the parent of the contexts of the sessions, cancelled when they are closed
    internal readonly CancellationToken SessionsCtx;
    private readonly CancellationTokenSource _sessionsCts;
    internal readonly IShardManager ShardManager;
    internal readonly IRpcProvider RpcProvider;
    internal readonly ClientOptions ClientOpts;
    internal ILogger Log;

    /// <summary>Lock-free reads, mutations under <see cref="_sync"/> (Go: sessionsByShard).</summary>
    internal readonly ConcurrentDictionary<long, ClientSession> SessionsByShard = new();

    /// <summary>The error of the client context when it expired instead of being cancelled; null = it was cancelled (production).</summary>
    internal Exception? ContextError;

    // Go: sync.Mutex (embedded)
    internal readonly object Lock = new();
    private object _sync => Lock;

    // The operations queued behind the one that waits for its session (guarded by Lock).  Invariant: Count > 0 => _hasCurrent.
    private readonly Queue<Op> _queue = new();
    private Op _current;
    private bool _hasCurrent;
    // 1 while an operation waits for its session or the queue is not empty: new operations must not overtake them
    private int _blocked;

    public Sessions(CancellationToken ctx, IShardManager shardManager, IRpcProvider rpcProvider, ClientOptions options)
    {
        ClientIdentity = options.Identity;
        Ctx = ctx;
        ShardManager = shardManager;
        RpcProvider = rpcProvider;
        ClientOpts = options;
        Log = OxiaLog.Create("oxia-session-manager");

        _sessionsCts = CancellationTokenSource.CreateLinkedTokenSource(ctx);
        SessionsCtx = _sessionsCts.Token;
        var changed = shardManager.Changed();
        _ = Task.Run(() => FollowSplitsOnShardMapChangeAsync(changed));
    }

    /// <summary>Go: s.cancelSessions().</summary>
    internal void CancelSessions()
    {
        try { _sessionsCts.Cancel(); } catch (ObjectDisposedException) { }
    }

    /// <summary>True while an operation waits for its session or others are queued behind it: writes issued now must queue behind them.</summary>
    internal bool HasWaitingOperations => Volatile.Read(ref _blocked) != 0;

    /// <summary>
    /// Runs <paramref name="work"/> after the operations that wait for a session -- inline when none does.  A write without a session
    /// issued by the caller of an ephemeral put whose session is still being created must not overtake it: Go's Put returns only once the
    /// call is in the batcher of its shard (executeWithSessionId holds the mutex until then), so the next Put of the same goroutine is
    /// added after it.  (Review R2-D3.)
    /// </summary>
    internal void ExecuteOrdered(Action work)
    {
        if (Volatile.Read(ref _blocked) != 0)
        {
            lock (_sync)
            {
                if (_hasCurrent)
                {
                    _queue.Enqueue(new Op { Work = work });
                    return;
                }
            }
        }
        work();
    }

    /// <summary>
    /// Go: executeWithSessionId.  Invokes the callback with the shard that <paramref name="shardForKey"/> returns and the id of
    /// the session on that shard, once the session has been established -- inline when it already is, later (on another
    /// thread) otherwise.  The shard is looked up under the lock: a shard looked up before could be one whose session
    /// followSplits has moved since.  It is looked up again when the shard is split before the session is established on it.
    /// When the session cannot be established the callback gets (shardId, -1, error).
    /// </summary>
    public void ExecuteWithSessionId(Func<long> shardForKey, Action<long, long, Exception?> callback)
    {
        // Fast path: nothing waits for a session, and the shard's session is there
        if (Volatile.Read(ref _blocked) == 0)
        {
            long shardId = shardForKey();
            if (SessionsByShard.TryGetValue(shardId, out var cs) && cs.TryGetEstablished(out long sessionId))
            {
                callback(shardId, sessionId, null);
                return;
            }
        }

        lock (_sync)
        {
            if (_hasCurrent)
            {
                _queue.Enqueue(new Op { ShardForKey = shardForKey, Callback = callback });
                return;
            }
            RunOp(shardForKey, callback, null, 0, guard: false);
            UpdateBlocked();
        }
    }

    // Must be called with the lock held, and with no operation waiting for a session.  Go: the body of executeWithSessionId.
    // Returns when the operation completed, or when it must wait for its session (then it is _current).
    // `guard`: the operation belongs to another caller (it is resumed from the thread that settled the session): the
    // exceptions of its callback are logged instead of thrown.
    private void RunOp(Func<long> shardForKey, Action<long, long, Exception?> callback, ClientSession? session, long shardId, bool guard)
    {
        while (true)
        {
            if (session is null)
            {
                shardId = shardForKey();
                if (!SessionsByShard.TryGetValue(shardId, out session))
                {
                    // The shard may have replaced a shard that was split, and inherited its session
                    FollowSplits();
                    SessionsByShard.TryGetValue(shardId, out session);
                }
                if (session is null)
                {
                    session = StartSession(shardId);
                    SessionsByShard[shardId] = session;
                }
            }

            ExecuteResult result;
            Exception? err;
            try
            {
                result = session.ExecuteWithId(shardId, callback, out err);
            }
            catch (Exception e) when (guard)
            {
                Log.LogError(e, "The callback of an operation with a session failed shard={Shard}", shardId);
                return;
            }

            if (result == ExecuteResult.Done) return;

            if (result == ExecuteResult.Pending)
            {
                _current = new Op { ShardForKey = shardForKey, Callback = callback, ShardId = shardId, Session = session };
                _hasCurrent = true;
                Volatile.Write(ref _blocked, 1);
                if (session.TryArmWaiter(this)) return;

                // The session settled meanwhile: run again with it
                _current = default;
                _hasCurrent = false;
                continue;
            }

            // The session failed to start: forget it, so that the next operation attempts a fresh one
            SessionsByShard.TryRemove(new KeyValuePair<long, ClientSession>(shardId, session));
            session.Cancel();
            if (!OxiaErrors.Is(err, OxiaErrors.ShardNotFound))
            {
                try
                {
                    callback(shardId, -1, err);
                }
                catch (Exception e) when (guard)
                {
                    Log.LogError(e, "The callback of an operation with a session failed shard={Shard}", shardId);
                }
                return;
            }
            // The shard was split before the session was established: the key is now on one of the shards that replaced it
            session.Log.LogInformation("The shard was split before the session was established on it shard={Shard}", shardId);
            session = null;
        }
    }

    private void UpdateBlocked()
    {
        if (!_hasCurrent && _queue.Count == 0) Volatile.Write(ref _blocked, 0);
    }

    /// <summary>Called when a session the head operation waits for left the pending state (established, failed, cancelled).</summary>
    internal void OnSessionSettled(ClientSession cs)
    {
        lock (_sync)
        {
            if (!_hasCurrent || !ReferenceEquals(_current.Session, cs)) return;

            var op = _current;
            _current = default;
            _hasCurrent = false;
            RunOp(op.ShardForKey, op.Callback, op.Session, op.ShardId, guard: true);
            while (!_hasCurrent && _queue.TryDequeue(out var next))
            {
                if (next.Work is { } work)
                {
                    try
                    {
                        work();
                    }
                    catch (Exception e)
                    {
                        Log.LogError(e, "An operation queued behind a session failed");
                    }
                    continue;
                }
                RunOp(next.ShardForKey, next.Callback, null, 0, guard: true);
            }
            UpdateBlocked();
        }
    }

    /// <summary>
    /// Go: followSplitsOnShardMapChange.  Moves the sessions of the shards that were split whenever the shard map changes,
    /// starting with the change that completes <paramref name="changed"/>: the shards that replaced them started to count
    /// their timeout when they were elected, before the client could know them.
    /// </summary>
    private async Task FollowSplitsOnShardMapChangeAsync(Task changed)
    {
        while (true)
        {
            try
            {
                await changed.WaitAsync(SessionsCtx).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
            changed = ShardManager.Changed();
            lock (_sync)
            {
                FollowSplits();
            }
        }
    }

    /// <summary>
    /// Go: followSplits.  Moves the sessions of the shards that were split to the shards that replaced them.  Each of those
    /// inherited the session, with its ephemeral records in their hash range, and expires it unless it is kept alive there.
    /// It must be called with the lock held.
    /// </summary>
    internal void FollowSplits()
    {
        if (SessionsCtx.IsCancellationRequested) return;
        foreach (var entry in SessionsByShard.ToArray())
        {
            long shardId = entry.Key;
            var cs = entry.Value;
            if (ShardManager.Exists(shardId)) continue;

            var successors = CurrentSuccessors(ShardManager, shardId);
            long sessionId;
            bool established;
            lock (cs.Lock)
            {
                sessionId = cs.SessionId;
                established = cs.Established;
            }
            if (successors.Count == 0 || !established) continue;

            cs.Log.LogInformation("Moving the session to the shards that replaced its shard shard={Shard} shards={Shards}",
                shardId, string.Join(",", successors));
            foreach (long successor in successors)
            {
                if (!SessionsByShard.ContainsKey(successor))
                {
                    SessionsByShard[successor] = InheritSession(successor, sessionId);
                }
            }
            SessionsByShard.TryRemove(shardId, out _);
            cs.Cancel();
        }
    }

    /// <summary>
    /// Go: currentSuccessors.  Returns the shards of the shard map that replaced a shard removed from it: the shards that
    /// replaced it may have been split in turn.
    /// </summary>
    internal static List<long> CurrentSuccessors(IShardManager shardManager, long shardId)
    {
        var current = new List<long>();
        AddCurrentSuccessors(shardManager, shardId, current);
        return current;
    }

    private static void AddCurrentSuccessors(IShardManager shardManager, long shardId, List<long> current)
    {
        var successors = shardManager.GetSuccessors(shardId);
        if (successors is null) return;
        foreach (long successor in successors)
        {
            if (shardManager.Exists(successor)) current.Add(successor);
            else AddCurrentSuccessors(shardManager, successor, current);
        }
    }

    /// <summary>Go: inheritSession.  Returns the session of a shard that replaced a split shard, which the shard inherited from it.</summary>
    private ClientSession InheritSession(long shardId, long sessionId)
    {
        var cs = NewClientSession(shardId);
        lock (cs.Lock)
        {
            cs.SetEstablished(sessionId);
        }
        return cs;
    }

    internal ClientSession NewClientSession(long shardId) => new(this, shardId);

    private ClientSession StartSession(long shardId)
    {
        var cs = NewClientSession(shardId);

        if (cs.Log.IsEnabled(LogLevel.Debug)) cs.Log.LogDebug("Creating session shard={Shard}", shardId);
        _ = Task.Run(cs.CreateSessionWithRetriesAsync);
        return cs;
    }

    public async ValueTask CloseAsync()
    {
        // Stop the sessions first: an operation that waits for one to be established holds the lock
        CancelSessions();

        ClientSession[] sessions;
        lock (_sync)
        {
            sessions = SessionsByShard.Values.ToArray();
        }

        List<Exception>? errors = null;
        foreach (var cs in sessions)
        {
            try
            {
                await cs.CloseAsync().ConfigureAwait(false);
            }
            catch (Exception e)
            {
                (errors ??= new()).Add(e);
            }
        }

        try { _sessionsCts.Dispose(); } catch (ObjectDisposedException) { }

        if (errors is null) return;
        if (errors.Count == 1) throw errors[0];
        throw new AggregateException(errors);
    }

    /// <summary>Go: context.DeadlineExceeded.</summary>
    internal static Exception DeadlineExceeded() => new TimeoutException("context deadline exceeded");
}

internal sealed class ClientSession
{
    internal readonly object Lock = new();
    internal readonly long ShardId;
    internal long SessionId;
    internal bool Established;
    internal readonly Sessions Sessions;
    internal ILogger Log;

    // Go: cs.ctx / cs.cancel
    private readonly CancellationTokenSource _cts;
    internal readonly CancellationToken Ctx;

    // Go: cs.started (chan error, buffered 1): a closed channel without a value means established; with an error, failed.
    private bool _startClosed;
    private Exception? _startError;

    // The operation at the head of the sessions' queue, waiting for the session to start (guarded by Lock)
    private Sessions? _waiter;
    private CancellationTokenRegistration _waiterReg;

    internal ClientSession(Sessions sessions, long shardId)
    {
        Sessions = sessions;
        ShardId = shardId;
        Log = OxiaLog.Create("session");
        _cts = CancellationTokenSource.CreateLinkedTokenSource(sessions.SessionsCtx);
        Ctx = _cts.Token;
    }

    /// <summary>Go: cs.cancel() (and the release of the context).</summary>
    internal void Cancel()
    {
        try
        {
            _cts.Cancel();
            _cts.Dispose();
        }
        catch (ObjectDisposedException)
        {
        }
    }

    /// <summary>Whether the session is established (fast path: no lock).</summary>
    internal bool TryGetEstablished(out long sessionId)
    {
        if (Volatile.Read(ref Established))
        {
            sessionId = SessionId;
            return true;
        }
        sessionId = 0;
        return false;
    }

    /// <summary>
    /// Go: executeWithId.  Invokes the callback with the session id, once the session has been established.  When the session
    /// failed to start, it returns <see cref="ExecuteResult.Failed"/> and the error without invoking the callback: the caller --
    /// which already holds the sessions lock -- discards the session, so that re-acquiring that lock here (a self-deadlock) is
    /// never needed.  When the session was cancelled the callback is dropped, unless the client context expired.
    /// <see cref="ExecuteResult.Pending"/> (Go: blocks) when the session is still starting.
    /// </summary>
    internal ExecuteResult ExecuteWithId(long shardId, Action<long, long, Exception?> callback, out Exception? error)
    {
        error = null;
        long sessionId = 0;
        bool started = false;
        lock (Lock)
        {
            if (_startClosed)
            {
                if (_startError is not null)
                {
                    error = _startError;
                    return ExecuteResult.Failed;
                }
                sessionId = SessionId;
                started = true;
            }
        }

        if (started)
        {
            callback(shardId, sessionId, null);
            return ExecuteResult.Done;
        }

        if (Ctx.IsCancellationRequested)
        {
            // cs.ctx.Err() != nil && !errors.Is(cs.ctx.Err(), context.Canceled)
            if (Sessions.Ctx.IsCancellationRequested && Sessions.ContextError is { } expired)
            {
                callback(shardId, -1, expired);
            }
            return ExecuteResult.Done;
        }
        return ExecuteResult.Pending;
    }

    /// <summary>The form of the Go signature: <c>executeWithId(callback func(int64, error)) error</c> (tests; never pending).</summary>
    internal Exception? ExecuteWithId(Action<long, Exception?> callback)
    {
        var result = ExecuteWithId(ShardId, (_, sessionId, err) => callback(sessionId, err), out var error);
        return result switch
        {
            ExecuteResult.Failed => error,
            ExecuteResult.Pending => throw new InvalidOperationException("the session is still starting"),
            _ => null,
        };
    }

    /// <summary>
    /// Registers the sessions as the one to resume once the session leaves the pending state.  Returns false if it already did
    /// (the caller must try again immediately).
    /// </summary>
    internal bool TryArmWaiter(Sessions waiter)
    {
        lock (Lock)
        {
            if (_startClosed || Ctx.IsCancellationRequested) return false;
            _waiter = waiter;
            // The registration may run inline when the token is cancelled meanwhile: it only queues the notification, so
            // that the sessions lock (held by the caller) is never re-entered
            _waiterReg = Ctx.Register(static s => ThreadPool.UnsafeQueueUserWorkItem(static c => c.NotifyWaiter(), (ClientSession)s!, preferLocal: false), this);
            return true;
        }
    }

    /// <summary>Resumes the operation waiting for this session, if any.  Must NOT be called with the session lock held.</summary>
    internal void NotifyWaiter()
    {
        Sessions? waiter;
        CancellationTokenRegistration registration;
        lock (Lock)
        {
            waiter = _waiter;
            _waiter = null;
            registration = _waiterReg;
            _waiterReg = default;
        }
        registration.Dispose();
        waiter?.OnSessionSettled(this);
    }

    /// <summary>Go: cs.started receives err, then is closed.  For tests and createSessionWithRetries.</summary>
    internal void FailStart(Exception err)
    {
        lock (Lock)
        {
            if (_startClosed) return;
            _startError = err;
            _startClosed = true;
        }
        NotifyWaiter();
    }

    internal async Task CreateSessionWithRetriesAsync()
    {
        var backOff = BackOffFactory.NewBackOff(Ctx);
        // A change of the shard map ends the wait for the next attempt: it can be the split of the shard, which ends the creation
        var timer = new ShardMapTimer();
        Exception? err = null;
        try
        {
            await Retry.RetryNotifyWithTimerAsync(async () =>
            {
                timer.Changed = Sessions.ShardManager.Changed();
                await CreateSessionAsync().ConfigureAwait(false);
            }, backOff, OnCreateSessionRetry, timer, Ctx).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            err = e;
        }

        if (err is not null && !OxiaErrors.IsCanceled(err))
        {
            FailStart(err);
        }
    }

    private void OnCreateSessionRetry(Exception err, TimeSpan duration)
    {
        if (!OxiaErrors.IsCanceled(err))
        {
            Log.LogError(err, "Error while creating session shard={Shard} retry-after={RetryAfter}", ShardId, duration);
        }
    }

    private async ValueTask CreateSessionAsync()
    {
        if (!Sessions.ShardManager.Exists(ShardId))
        {
            // The shard was split and deleted: no data server accepts the session on it anymore
            throw new PermanentException(OxiaErrors.ToThrowable(OxiaErrors.ShardNotFound));
        }

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ctx);
        cts.CancelAfter(Sessions.ClientOpts.RequestTimeout);
        CreateSessionResponse response;
        try
        {
            response = await Sessions.RpcProvider.CreateSession(cts.Token, Leader(), new CreateSessionRequest
            {
                Shard = ShardId,
                ClientIdentity = Sessions.ClientIdentity,
                SessionTimeoutMs = (uint)Sessions.ClientOpts.SessionTimeout.TotalMilliseconds,
            }).ConfigureAwait(false);
        }
        catch when (cts.IsCancellationRequested && !Ctx.IsCancellationRequested)
        {
            throw Sessions.DeadlineExceeded();
        }

        lock (Lock)
        {
            SetEstablished(response.SessionId);
        }
        if (Log.IsEnabled(LogLevel.Debug)) Log.LogDebug("Successfully created session shard={Shard} session-id={SessionId}", ShardId, response.SessionId);
        NotifyWaiter();
    }

    /// <summary>Go: setEstablished.  Records the id of the established session, and starts to keep it alive.  Must be called with the lock held.</summary>
    internal void SetEstablished(long sessionId)
    {
        SessionId = sessionId;
        Volatile.Write(ref Established, true);
        _startClosed = true;

        _ = Task.Run(KeepAliveLoopAsync);
    }

    private async Task KeepAliveLoopAsync()
    {
        var backOff = BackOffFactory.NewBackOff(Sessions.Ctx);
        Exception? err = null;
        try
        {
            await Retry.RetryNotifyAsync(async () =>
            {
                try
                {
                    await KeepAliveAsync(backOff).ConfigureAwait(false);
                }
                catch (Exception e) when (OxiaErrors.Is(e, OxiaErrors.SessionNotFound))
                {
                    Log.LogError(e, "Session is no longer valid shard={Shard} session-id={SessionId}", ShardId, SessionId);

                    lock (Sessions.Lock)
                    {
                        Sessions.SessionsByShard.TryRemove(new KeyValuePair<long, ClientSession>(ShardId, this));
                    }
                    throw new PermanentException(e);
                }
            }, backOff, (e, duration) =>
            {
                if (Log.IsEnabled(LogLevel.Debug))
                    Log.LogDebug(e, "Failed to send session heartbeat, retrying later shard={Shard} retry-after={RetryAfter}", ShardId, duration);
            }, Sessions.Ctx).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            err = e;
        }

        if (err is not null && !OxiaErrors.IsCanceled(err))
        {
            Log.LogError(err, "Failed to keep alive session shard={Shard} session-id={SessionId}", ShardId, SessionId);
        }
    }

    private string Leader() => Sessions.ShardManager.Leader(ShardId);

    /// <summary>Go: cs.Close().  Cancels the session and closes it on its leader; ErrSessionNotFound is success.</summary>
    internal async ValueTask CloseAsync()
    {
        Cancel();

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(Sessions.Ctx);
        cts.CancelAfter(Sessions.ClientOpts.RequestTimeout);

        try
        {
            await Sessions.RpcProvider.CloseSession(cts.Token, Leader(), new CloseSessionRequest
            {
                Shard = ShardId,
                SessionId = SessionId,
            }).ConfigureAwait(false);
        }
        catch (Exception e) when (OxiaErrors.Is(e, OxiaErrors.SessionNotFound))
        {
            return;
        }
        catch when (cts.IsCancellationRequested && !Sessions.Ctx.IsCancellationRequested)
        {
            throw Sessions.DeadlineExceeded();
        }
    }

    private async ValueTask KeepAliveAsync(IBackOff backOff)
    {
        var ct = Ctx;
        long shardId = ShardId;
        long sessionId = SessionId;

        var tickTime = Sessions.ClientOpts.SessionKeepAliveTicker;
        if (tickTime < TimeSpan.FromSeconds(2)) tickTime = TimeSpan.FromSeconds(2);

        using var ticker = new PeriodicTimer(tickTime);
        while (true)
        {
            try
            {
                if (!await ticker.WaitForNextTickAsync(ct).ConfigureAwait(false)) return;
            }
            catch (OperationCanceledException)
            {
                // <-ctx.Done()
                return;
            }

            await Sessions.RpcProvider.KeepAlive(ct, Sessions.ShardManager.Leader(shardId),
                new SessionHeartbeat { Shard = shardId, SessionId = sessionId }).ConfigureAwait(false);

            // RetryNotify only resets the backoff when it starts: without this, every failure over the lifetime of the
            // session would make the retry of the next one wait longer
            backOff.Reset();
        }
    }
}

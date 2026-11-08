// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/async_client_impl.go   (the `ch := make(chan GetResult, 1)` + `callback` closure of Get)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The pooled call of the OPT-IN pooled read API: like PooledGetCall (PooledCalls.cs) it is the request, the completion source
//              of the ValueTask it returns and a member of a bounded pool, but its result is a PooledGetResult: it takes the
//              ValueLease the deserializer parked in the GetResponse (the response was parsed in pooled mode because the batch contains
//              this call -- IPooledValueCall), so the value is copied once, from the network buffer into a pooled buffer, and never
//              into a new array.
//   Divergences: n/a (an addition).  If the response did not come with a lease (a fake executor, a server that sent a classic response)
//              the value array is copied into a lease, so the contract is the same.
// =====================================================================================================================
using System.Threading.Tasks.Sources;
using Oxia.Client.Internal.Model;
using Oxia.Common.Proto;

namespace Oxia.Client.Pooled;

/// <summary>Marker: the batch that holds this call asks the transport for pooled values (ReadRequest.PooledValues).</summary>
internal interface IPooledValueCall
{
}

/// <summary>A pooled Get whose result is a <see cref="PooledGetResult"/>.</summary>
internal sealed class PooledGetValueCall : GetCall, IValueTaskSource<PooledGetResult>, IPooledValueCall
{
    private static readonly CallPool<PooledGetValueCall> Pool = new(4096);

    private ManualResetValueTaskSourceCore<PooledGetResult> _core = new() { RunContinuationsAsynchronously = true };
    private int _completed;

    public static PooledGetValueCall Rent() => Pool.TryRent() ?? new PooledGetValueCall();

    public ValueTask<PooledGetResult> AsValueTask() => new(this, _core.Version);

    public override void Complete(GetResponse? response, Exception? error)
    {
        if (Interlocked.Exchange(ref _completed, 1) != 0) return;
        base.Complete(response, error); // a metrics decorator may have wrapped Callback
        _core.SetResult(ToResult(response, Key, error));
    }

    internal static PooledGetResult ToResult(GetResponse? response, string originalKey, Exception? error)
    {
        if (error is not null) return new PooledGetResult(error);

        var statusError = ProtoUtils.ToError(response!.Status);
        if (statusError is not null) return new PooledGetResult(statusError);

        var version = ProtoUtils.ToVersion(response.Version);
        string key = response.Key ?? originalKey;
        ValueLease? lease = response.DetachLease();
        if (lease is not null)
        {
            // A comparison / index get: the stored key came with the response, in the lease
            if (response.Key is null && lease.KeyLength >= 0) key = lease.KeyString();
            return new PooledGetResult(lease, lease.Generation, key, version, null);
        }

        return PooledGetResult.FromArray(key, response.Value, version);
    }

    PooledGetResult IValueTaskSource<PooledGetResult>.GetResult(short token)
    {
        var result = _core.GetResult(token);
        Recycle();
        return result;
    }

    ValueTaskSourceStatus IValueTaskSource<PooledGetResult>.GetStatus(short token) => _core.GetStatus(token);

    void IValueTaskSource<PooledGetResult>.OnCompleted(Action<object?> continuation, object? state, short token,
        ValueTaskSourceOnCompletedFlags flags) => _core.OnCompleted(continuation, state, token, flags);

    private void Recycle()
    {
        _core.Reset();
        Reset();
        Volatile.Write(ref _completed, 0);
        Pool.Return(this);
    }
}

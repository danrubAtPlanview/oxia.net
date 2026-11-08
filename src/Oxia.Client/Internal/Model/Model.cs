// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/internal/model/model.go  (PutCall, DeleteCall, DeleteRangeCall, GetCall, InOpIndexOrder, Convert)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The client-side representation of one pending operation, from `Put(...)` until its callback fires.
//   Divergences (all deliberate, for zero-allocation):
//      * A call IS its own wire request: PutCall : PutRequest, DeleteCall : DeleteRequest, DeleteRangeCall : DeleteRangeRequest,
//        GetCall : GetRequest.  `ToProto()` therefore returns `this` -- no copy per operation, the batch hands the very
//        objects to the codec.  Field names that differ between Go's call and the proto (MinKeyInclusive/StartInclusive,
//        MaxKeyExclusive/EndExclusive, SequenceKeysDeltas/SequenceKeyDelta) are exposed as aliases of the same storage.
//      * Calls are CLASSES (pooled/reused by the hot path) instead of Go structs copied by value.  A pooled subclass overrides
//        `Complete` to resolve a ValueTask directly instead of invoking a heap-allocated delegate.
//      * `Callback func(*Response, error)` becomes `Action<TResponse?, Exception?>`; callers must go through `Complete(...)`.
// =====================================================================================================================
using Oxia.Common.Proto;

namespace Oxia.Client.Internal.Model;

public class PutCall : PutRequest
{
    public CallContext? CallContext;
    public Action<PutResponse?, Exception?>? Callback;

    /// <summary>Delivers the outcome of the call: exactly one of (response, error) is meaningful.</summary>
    public virtual void Complete(PutResponse? response, Exception? error) => Callback?.Invoke(response, error);

    /// <summary>Go field name for <see cref="PutRequest.SequenceKeyDelta"/>.</summary>
    public ulong[]? SequenceKeysDeltas { get => SequenceKeyDelta; set => SequenceKeyDelta = value; }

    /// <summary>The partition key if set, otherwise the record key. Used for shard routing when re-routing after a shard split.</summary>
    public string PartitionKeyOrKey() => PartitionKey ?? Key;

    public PutRequest ToProto() => this;

    public override void Reset()
    {
        base.Reset();
        CallContext = null;
        Callback = null;
    }
}

public class DeleteCall : DeleteRequest
{
    public CallContext? CallContext;
    public Action<DeleteResponse?, Exception?>? Callback;

    public virtual void Complete(DeleteResponse? response, Exception? error) => Callback?.Invoke(response, error);

    public string PartitionKeyOrKey() => PartitionKey ?? Key;

    public DeleteRequest ToProto() => this;

    public override void Reset()
    {
        base.Reset();
        CallContext = null;
        Callback = null;
    }
}

public class DeleteRangeCall : DeleteRangeRequest
{
    /// <summary>Only used for shard routing, NOT sent to the server.</summary>
    public string? PartitionKey;
    public CallContext? CallContext;
    public Action<DeleteRangeResponse?, Exception?>? Callback;

    public virtual void Complete(DeleteRangeResponse? response, Exception? error) => Callback?.Invoke(response, error);

    public string MinKeyInclusive { get => StartInclusive; set => StartInclusive = value; }
    public string MaxKeyExclusive { get => EndExclusive; set => EndExclusive = value; }

    public DeleteRangeRequest ToProto() => this;

    public override void Reset()
    {
        base.Reset();
        PartitionKey = null;
        CallContext = null;
        Callback = null;
    }
}

public class GetCall : GetRequest
{
    public string? PartitionKey;
    public Action<GetResponse?, Exception?>? Callback;

    public virtual void Complete(GetResponse? response, Exception? error) => Callback?.Invoke(response, error);

    public string PartitionKeyOrKey() => PartitionKey ?? Key;

    public GetRequest ToProto() => this;

    public override void Reset()
    {
        base.Reset();
        PartitionKey = null;
        Callback = null;
    }
}

public static class ModelExtensions
{
    /// <summary>
    /// Go: InOpIndexOrder -- the calls of a write batch in the order they were added to it (stable sort by OpIndex;
    /// puts, then deletes, then delete ranges for equal indexes).
    /// </summary>
    public static List<object> InOpIndexOrder(IReadOnlyList<PutCall> puts, IReadOnlyList<DeleteCall> deletes,
        IReadOnlyList<DeleteRangeCall> deleteRanges)
    {
        var calls = new List<(uint OpIndex, object Call)>(puts.Count + deletes.Count + deleteRanges.Count);
        foreach (var p in puts) calls.Add((p.OpIndex, p));
        foreach (var d in deletes) calls.Add((d.OpIndex, d));
        foreach (var r in deleteRanges) calls.Add((r.OpIndex, r));

        // List.Sort is unstable: use an index tiebreaker to get Go's SortStableFunc semantics.
        var order = new int[calls.Count];
        for (int i = 0; i < order.Length; i++) order[i] = i;
        Array.Sort(order, (a, b) =>
        {
            int c = calls[a].OpIndex.CompareTo(calls[b].OpIndex);
            return c != 0 ? c : a.CompareTo(b);
        });

        var ordered = new List<object>(calls.Count);
        foreach (int i in order) ordered.Add(calls[i].Call);
        return ordered;
    }
}

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
//        What Go's ToProto leaves out of the wire message stays out of it: DeleteRangeCall.PartitionKey and GetCall.PartitionKey
//        are fields of the CALL only (routing), not of the proto classes.  Go's PutCall has no OverrideVersionId /
//        OverrideModificationsCount (those are server-side replication fields): callers must leave them null.
//      * Calls are CLASSES (pooled/reused by the hot path) instead of Go structs copied by value.  A pooled subclass overrides
//        `Complete` to resolve a ValueTask directly instead of invoking a heap-allocated delegate.
//      * `Callback func(*Response, error)` becomes `Action<TResponse?, Exception?>`; callers must go through `Complete(...)`.
//      * VALUE-COPY SEMANTICS ARE LOST: Go copied the struct into the batch (`Add(call)` set OpIndex on the copy, the
//        metrics decorator returned another copy, and sendDeleteRangeToSuccessors handed the same value to several
//        shards).  With classes the batch assigns OpIndex on the very object it is given, so one call instance must be
//        added to at most ONE batcher at a time.  `Clone()` is the explicit "copy the struct" (field copy incl. Callback and
//        CallContext, NOT the type: a clone of a pooled subclass is a plain call whose Complete invokes its Callback) for
//        the places where Go relied on the copy.
//      * InOpIndexOrder: a stable sort without lambdas/LINQ (unique composite keys, so Array.Sort is stable).
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

    /// <summary>Go: a copy of the struct (all the fields, the Callback included).</summary>
    public PutCall Clone()
    {
        var clone = new PutCall();
        clone.CopyFrom(this);
        clone.Callback = Callback;
        return clone;
    }

    /// <summary>Copies every field of the call but the Callback.</summary>
    internal void CopyFrom(PutCall src)
    {
        Key = src.Key;
        Value = src.Value;
        ExpectedVersionId = src.ExpectedVersionId;
        SessionId = src.SessionId;
        ClientIdentity = src.ClientIdentity;
        PartitionKey = src.PartitionKey;
        SequenceKeyDelta = src.SequenceKeyDelta;
        SecondaryIndexes = src.SecondaryIndexes;
        OverrideVersionId = src.OverrideVersionId;
        OverrideModificationsCount = src.OverrideModificationsCount;
        OpIndex = src.OpIndex;
        CallContext = src.CallContext;
    }

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

    /// <summary>Go: a copy of the struct (all the fields, the Callback included).</summary>
    public DeleteCall Clone()
    {
        var clone = new DeleteCall();
        clone.CopyFrom(this);
        clone.Callback = Callback;
        return clone;
    }

    /// <summary>Copies every field of the call but the Callback.</summary>
    internal void CopyFrom(DeleteCall src)
    {
        Key = src.Key;
        ExpectedVersionId = src.ExpectedVersionId;
        PartitionKey = src.PartitionKey;
        OpIndex = src.OpIndex;
        CallContext = src.CallContext;
    }

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

    /// <summary>
    /// Go: a copy of the struct (all the fields, the Callback included).  Needed to send one delete range to several shards:
    /// each batch assigns its own OpIndex.
    /// </summary>
    public DeleteRangeCall Clone()
    {
        var clone = new DeleteRangeCall();
        clone.CopyFrom(this);
        clone.Callback = Callback;
        return clone;
    }

    /// <summary>Copies every field of the call but the Callback.</summary>
    internal void CopyFrom(DeleteRangeCall src)
    {
        StartInclusive = src.StartInclusive;
        EndExclusive = src.EndExclusive;
        PartitionKey = src.PartitionKey;
        OpIndex = src.OpIndex;
        CallContext = src.CallContext;
    }

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

    /// <summary>Go: a copy of the struct (all the fields, the Callback included).</summary>
    public GetCall Clone()
    {
        var clone = new GetCall();
        clone.CopyFrom(this);
        clone.Callback = Callback;
        return clone;
    }

    /// <summary>Copies every field of the call but the Callback.</summary>
    internal void CopyFrom(GetCall src)
    {
        Key = src.Key;
        IncludeValue = src.IncludeValue;
        ComparisonType = src.ComparisonType;
        SecondaryIndexName = src.SecondaryIndexName;
        PartitionKey = src.PartitionKey;
    }

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
        int count = puts.Count + deletes.Count + deleteRanges.Count;
        // Sort key = (OpIndex, position in the concatenation): unique, so the unstable Array.Sort behaves like Go's SortStableFunc.
        var keys = new ulong[count];
        var calls = new object[count];
        int n = 0;
        for (int i = 0; i < puts.Count; i++, n++)
        {
            keys[n] = ((ulong)puts[i].OpIndex << 32) | (uint)n;
            calls[n] = puts[i];
        }
        for (int i = 0; i < deletes.Count; i++, n++)
        {
            keys[n] = ((ulong)deletes[i].OpIndex << 32) | (uint)n;
            calls[n] = deletes[i];
        }
        for (int i = 0; i < deleteRanges.Count; i++, n++)
        {
            keys[n] = ((ulong)deleteRanges[i].OpIndex << 32) | (uint)n;
            calls[n] = deleteRanges[i];
        }

        Array.Sort(keys, calls);
        return new List<object>(calls);
    }
}

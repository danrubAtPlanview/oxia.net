// =====================================================================================================================
// PORT NOTE
//   Upstream : github.com/oxia-db/oxia  common/proto/client.proto  (+ generated client.pb.go / client_vtproto.pb.go)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Hand-written, mutable, allocation-conscious data classes for every message of `client.proto` that the
//              Oxia client sends or receives (the OxiaClient service).  They replace the protoc-generated classes on
//              the hot path so that the codecs (Oxia.Common.Proto.Codec, owned by the wire-codec component) can write
//              straight from these objects into the gRPC buffer writer and parse straight into them, with no
//              intermediate Google.Protobuf objects and no ByteString copies.
//   Mapping rules (Go generated  ->  C#):
//      * field names                : snake_case -> PascalCase (`expected_version_id` -> `ExpectedVersionId`).
//      * proto3 scalar              : plain C# value / non-null string ("" default).
//      * `optional` scalar / string : nullable (`long?`, `string?`) -- null == unset (presence is tracked, as with Go pointers).
//      * repeated                   : `List<T>` (get-only, never null) except where noted (`SequenceKeyDelta`, request-side `SecondaryIndexes`).
//      * message field              : nullable reference, null == unset.
//      * bytes                      : request side `ReadOnlyMemory<byte>`; response side `byte[]?` (null == unset, like a Go nil slice).
//      * oneof shard_boundaries     : single nullable property `Int32HashRange`.
//      * map<string, M>             : `Dictionary<string, M>`.
//      * message `Version`          : renamed `ProtoVersion` (avoids clashing with System.Version).
//      * enums                      : PascalCase members, proto enum prefix stripped (Google.Protobuf C# convention).
//   Every message has Reset() so a pooled instance can be reused.  Messages are NOT thread-safe.
//   NOTE: protoc also generates Google.Protobuf classes for the same file in namespace Io.Oxia.Proto.V1 (needed by the
//   admin API and used as the wire-format oracle in tests).  Never import both namespaces unaliased in one file.
//   Wire-codec additions (component A): every nested-capable message has an `internal int CachedSize` memo that
//   Codec `Size()` fills and `WriteTo()` reads (so nested lengths are computed once); response-side messages recycle
//   their children on Reset() through `Spares<T>` (re-parsing into a Reset() instance allocates nothing).  Both are
//   internal and invisible to users of the classes except that a child object must not outlive a Reset() of its parent.
//   Response recycling (wave 2 / Z1, docs/performance-allocation.md): the top-level RESPONSES the gRPC deserializers produce
//   (WriteResponse, ReadResponse, RangeScanResponse, ListResponse, NotificationBatch) come from per-type pools (`Rent()`), and their final
//   consumer gives them back with `Release()` (idempotent; a harmless no-op on a message made with `new`, e.g. by a test fake).  A released
//   message is Reset() -- its children go to the Spares -- so nothing read from it may be used afterwards; Debug builds throw
//   ObjectDisposedException when the lists of a released message are touched.  `PoolState`, `Lease`, `PooledValues` and the helpers are
//   internal (the test kit reflects over the public members).  GetResponse can hold its value / key bytes in a ValueLease (pooled read API).
//   Sync procedure: when client.proto changes upstream, copy the .proto into Proto/, then mirror the field in this
//   file, in the codec, and in the oracle tests (tests/Oxia.Client.Tests/Proto).
// =====================================================================================================================
using Oxia.Common.Proto.Codec;

namespace Oxia.Common.Proto;

/// <summary>proto: <c>Status</c>.</summary>
public enum Status { Ok = 0, KeyNotFound = 1, UnexpectedVersionId = 2, SessionDoesNotExist = 3, InvalidArgument = 4 }

/// <summary>proto: <c>KeyComparisonType</c>.</summary>
public enum KeyComparisonType { Equal = 0, Floor = 1, Ceiling = 2, Lower = 3, Higher = 4 }

/// <summary>proto: <c>ShardKeyRouter</c>.</summary>
public enum ShardKeyRouter { Unknown = 0, Xxhash3 = 1 }

/// <summary>proto: <c>KeySorting</c> (KEY_SORTING_UNKNOWN / NATURAL / HIERARCHICAL).</summary>
public enum KeySorting { Unknown = 0, Natural = 1, Hierarchical = 2 }

/// <summary>proto: <c>NotificationType</c>.</summary>
public enum NotificationType { KeyCreated = 0, KeyModified = 1, KeyDeleted = 2, KeyRangeDeleted = 3 }

public class ShardAssignmentsRequest
{
    public string Namespace = "";
    public void Reset() { Namespace = ""; }
}

public class Int32HashRange
{
    internal int CachedSize;
    public uint MinHashInclusive;
    public uint MaxHashInclusive;
    public void Reset() { MinHashInclusive = 0; MaxHashInclusive = 0; }
}

public class ShardAssignment
{
    internal int CachedSize;
    public long Shard;
    public string Leader = "";
    /// <summary>oneof shard_boundaries { Int32HashRange int32_hash_range = 3; } -- null when no boundary is set.</summary>
    public Int32HashRange? Int32HashRange;
    public void Reset() { Shard = 0; Leader = ""; Int32HashRange = null; }
}

public class NamespaceShardsAssignment
{
    internal int CachedSize;
    public List<ShardAssignment> Assignments { get; } = new();
    public ShardKeyRouter ShardKeyRouter;
    public KeySorting KeySorting;
    public void Reset() { Assignments.Clear(); ShardKeyRouter = default; KeySorting = default; }
}

public class ShardAssignments
{
    public Dictionary<string, NamespaceShardsAssignment> Namespaces { get; } = new();
    public List<string> AllowedAuthorities { get; } = new();
    public void Reset() { Namespaces.Clear(); AllowedAuthorities.Clear(); }
}

public class SecondaryIndex
{
    internal int CachedSize;
    public string IndexName = "";
    public string SecondaryKey = "";
    public void Reset() { IndexName = ""; SecondaryKey = ""; }
}

/// <summary>proto: <c>Version</c> (record version metadata returned by the server).</summary>
public class ProtoVersion
{
    internal int CachedSize;
    public long VersionId;
    public long ModificationsCount;
    public ulong CreatedTimestamp;
    public ulong ModifiedTimestamp;
    public long? SessionId;
    public string? ClientIdentity;
    public void Reset()
    {
        VersionId = 0; ModificationsCount = 0; CreatedTimestamp = 0; ModifiedTimestamp = 0;
        SessionId = null; ClientIdentity = null;
    }
}

public class PutRequest
{
    internal int CachedSize;
    public string Key = "";
    public ReadOnlyMemory<byte> Value;
    public long? ExpectedVersionId;
    public long? SessionId;
    public string? ClientIdentity;
    public string? PartitionKey;
    /// <summary>repeated uint64 sequence_key_delta = 7 (packed). null or empty == absent.</summary>
    public ulong[]? SequenceKeyDelta;
    /// <summary>repeated SecondaryIndex secondary_indexes = 8. null or empty == absent.</summary>
    public List<SecondaryIndex>? SecondaryIndexes;
    public long? OverrideVersionId;
    public long? OverrideModificationsCount;
    public uint OpIndex;

    public virtual void Reset()
    {
        Key = ""; Value = default; ExpectedVersionId = null; SessionId = null; ClientIdentity = null;
        PartitionKey = null; SequenceKeyDelta = null; SecondaryIndexes = null;
        OverrideVersionId = null; OverrideModificationsCount = null; OpIndex = 0;
    }
}

public class PutResponse
{
    internal int CachedSize;
    internal ProtoVersion? SpareVersion;
    public Status Status;
    public ProtoVersion? Version;
    public string? Key;
    public void Reset()
    {
        Status = default;
        if (Version is { } v) { v.Reset(); SpareVersion = v; Version = null; }
        Key = null;
    }
}

public class DeleteRequest
{
    internal int CachedSize;
    public string Key = "";
    public long? ExpectedVersionId;
    public string? PartitionKey;
    public uint OpIndex;
    public virtual void Reset() { Key = ""; ExpectedVersionId = null; PartitionKey = null; OpIndex = 0; }
}

public class DeleteResponse
{
    internal int CachedSize;
    public Status Status;
    public void Reset() { Status = default; }
}

public class DeleteRangeRequest
{
    internal int CachedSize;
    public string StartInclusive = "";
    public string EndExclusive = "";
    public uint OpIndex;
    public virtual void Reset() { StartInclusive = ""; EndExclusive = ""; OpIndex = 0; }
}

public class DeleteRangeResponse
{
    internal int CachedSize;
    public Status Status;
    public void Reset() { Status = default; }
}

public class WriteRequest
{
    public long? Shard;
    public List<PutRequest> Puts { get; } = new();
    public List<DeleteRequest> Deletes { get; } = new();
    public List<DeleteRangeRequest> DeleteRanges { get; } = new();
    public void Reset() { Shard = null; Puts.Clear(); Deletes.Clear(); DeleteRanges.Clear(); }
}

public class WriteResponse
{
    internal int PoolState;
    internal Spares<PutResponse> SparePuts;
    internal Spares<DeleteResponse> SpareDeletes;
    internal Spares<DeleteRangeResponse> SpareDeleteRanges;
    private readonly List<PutResponse> _puts = new();
    private readonly List<DeleteResponse> _deletes = new();
    private readonly List<DeleteRangeResponse> _deleteRanges = new();
    public List<PutResponse> Puts { get { PoolGuard.AssertLive(PoolState); return _puts; } }
    public List<DeleteResponse> Deletes { get { PoolGuard.AssertLive(PoolState); return _deletes; } }
    public List<DeleteRangeResponse> DeleteRanges { get { PoolGuard.AssertLive(PoolState); return _deleteRanges; } }

    public void Reset()
    {
        foreach (var e in _puts) { e.Reset(); SparePuts.Push(e); }
        foreach (var e in _deletes) { e.Reset(); SpareDeletes.Push(e); }
        foreach (var e in _deleteRanges) { e.Reset(); SpareDeleteRanges.Push(e); }
        _puts.Clear(); _deletes.Clear(); _deleteRanges.Clear();
    }

    /// <summary>A response from the pool (the gRPC deserializer's way to get one).  Its consumer must <see cref="Release"/> it exactly once.</summary>
    public static WriteResponse Rent()
    {
        var m = MessagePool<WriteResponse>.TryRent() ?? new WriteResponse();
        m.PoolState = PoolStates.Rented;
        return m;
    }

    /// <summary>
    /// Gives a response obtained from <see cref="Rent"/> back to the pool: it is <see cref="Reset"/> (the children are recycled, so
    /// nothing read from it may be used afterwards).  A no-op for a response made with <c>new</c> and for one already released.
    /// </summary>
    public void Release()
    {
        if (Interlocked.CompareExchange(ref PoolState, PoolStates.Releasing, PoolStates.Rented) != PoolStates.Rented) return;
        Reset();
        SparePuts.Trim(PoolLimits.MaxSpareChildren);
        SpareDeletes.Trim(PoolLimits.MaxSpareChildren);
        SpareDeleteRanges.Trim(PoolLimits.MaxSpareChildren);
        PoolLimits.Trim(_puts); PoolLimits.Trim(_deletes); PoolLimits.Trim(_deleteRanges);
        Volatile.Write(ref PoolState, PoolStates.Pooled);
        MessagePool<WriteResponse>.Return(this);
    }
}

public class GetRequest
{
    internal int CachedSize;
    public string Key = "";
    public bool IncludeValue;
    public KeyComparisonType ComparisonType;
    public string? SecondaryIndexName;
    public virtual void Reset() { Key = ""; IncludeValue = false; ComparisonType = default; SecondaryIndexName = null; }
}

public class GetResponse
{
    internal int CachedSize;
    internal ProtoVersion? SpareVersion;

    /// <summary>
    /// Pooled-read mode only (the request asked for pooled values; the response holds <see cref="PooledGetResponse"/> children): the
    /// value (and, for scans, the key) live in this lease instead of <see cref="Value"/> / <see cref="Key"/>, which stay null.  Whoever
    /// takes the lease owns it; otherwise Reset() gives it back.  (It lives in a subclass so that a classic GetResponse stays 8 bytes smaller.)
    /// </summary>
    internal ValueLease? Lease => (this as PooledGetResponse)?.Held;

    public Status Status;
    public ProtoVersion? Version;
    public byte[]? Value;
    public string? Key;
    public string? SecondaryIndexKey;
    public void Reset()
    {
        Status = default;
        if (Version is { } v) { v.Reset(); SpareVersion = v; Version = null; }
        Value = null; Key = null; SecondaryIndexKey = null;
        if (this is PooledGetResponse { Held: { } lease } pooled) { lease.TryRelease(lease.Generation); pooled.Held = null; }
    }

    /// <summary>Length of the value in bytes whichever way it is held (0 when absent).</summary>
    internal int ValueLength => Value?.Length ?? (Lease is { ValueLength: > 0 } l ? l.ValueLength : 0);

    /// <summary>The key of the record: the parsed string, or the decoded key of the lease (null when the response carried none).</summary>
    internal string? ResolvedKey => Key ?? (Lease is { KeyLength: >= 0 } l ? l.KeyString() : null);

    /// <summary>
    /// The value as a new exact-size array (null when absent), whichever way it is held.  A leased value is copied out and the lease
    /// given back, so the response then holds a plain array like any classic response.
    /// </summary>
    internal byte[]? TakeValueArray()
    {
        if (this is PooledGetResponse { Held: { } lease } pooled)
        {
            Value = lease.CopyValueToArray();
            if (Key is null && lease.KeyLength >= 0) Key = lease.KeyString();
            lease.TryRelease(lease.Generation);
            pooled.Held = null;
        }
        return Value;
    }

    /// <summary>Hands the lease (and with it the duty to release it) to the caller; the response no longer refers to it.</summary>
    internal ValueLease? DetachLease()
    {
        if (this is not PooledGetResponse pooled) return null;
        var lease = pooled.Held;
        pooled.Held = null;
        return lease;
    }

    /// <summary>
    /// A copy that stays valid after the response (and its parent) is released: a multi-shard get keeps the best response of each
    /// shard while the others are still arriving.  The value array is shared (it belongs to whoever reads it), a lease is copied out.
    /// </summary>
    internal GetResponse Clone()
    {
        var copy = new GetResponse { Status = Status, Key = ResolvedKey, SecondaryIndexKey = SecondaryIndexKey };
        copy.Value = Lease is { } lease ? lease.CopyValueToArray() : Value;
        if (Version is { } v)
        {
            copy.Version = new ProtoVersion
            {
                VersionId = v.VersionId, ModificationsCount = v.ModificationsCount, CreatedTimestamp = v.CreatedTimestamp,
                ModifiedTimestamp = v.ModifiedTimestamp, SessionId = v.SessionId, ClientIdentity = v.ClientIdentity,
            };
        }
        return copy;
    }
}

/// <summary>A GetResponse parsed for the pooled read API: its value / key bytes are in <see cref="Held"/> (see <see cref="GetResponse.Lease"/>).</summary>
internal sealed class PooledGetResponse : GetResponse
{
    internal ValueLease? Held;

    /// <summary>The lease of this record, rented on first use.</summary>
    internal ValueLease RentLease() => Held ??= ValueLease.Rent();
}

public class ReadRequest
{
    /// <summary>Opt-in pooled read API: the response values are parsed into <see cref="ValueLease"/>s instead of new arrays.</summary>
    internal bool PooledValues;
    public long? Shard;
    public List<GetRequest> Gets { get; } = new();
    public void Reset() { Shard = null; Gets.Clear(); PooledValues = false; }
}

public class ReadResponse
{
    internal int PoolState;
    internal Spares<GetResponse> SpareGets;
    private readonly List<GetResponse> _gets = new();
    public List<GetResponse> Gets { get { PoolGuard.AssertLive(PoolState); return _gets; } }

    public void Reset()
    {
        foreach (var e in _gets) { e.Reset(); SpareGets.Push(e); }
        _gets.Clear();
    }

    public static ReadResponse Rent()
    {
        var m = MessagePool<ReadResponse>.TryRent() ?? new ReadResponse();
        m.PoolState = PoolStates.Rented;
        return m;
    }

    /// <summary>See <see cref="WriteResponse.Release"/>.  Values already handed to a caller (the arrays, the detached leases) are not affected.</summary>
    public void Release()
    {
        if (Interlocked.CompareExchange(ref PoolState, PoolStates.Releasing, PoolStates.Rented) != PoolStates.Rented) return;
        Reset();
        SpareGets.Trim(PoolLimits.MaxSpareChildren);
        PoolLimits.Trim(_gets);
        Volatile.Write(ref PoolState, PoolStates.Pooled);
        MessagePool<ReadResponse>.Return(this);
    }

    /// <summary>
    /// Appends the gets of <paramref name="other"/> (a further chunk of the same stream) to this response.  When the chunk came from the
    /// pool its children are MOVED here and the chunk is released; a plain chunk keeps its list (the callers of tests may still look at it).
    /// </summary>
    internal void Absorb(ReadResponse other)
    {
        _gets.AddRange(other._gets);
        if (other.PoolState == PoolStates.Rented)
        {
            other._gets.Clear();
            other.Release();
        }
    }
}

public class ListRequest
{
    public long? Shard;
    public string StartInclusive = "";
    public string EndExclusive = "";
    public string? SecondaryIndexName;
    public bool IncludeInternalKeys;
    public bool EndInclusive;
    public void Reset()
    {
        Shard = null; StartInclusive = ""; EndExclusive = ""; SecondaryIndexName = null;
        IncludeInternalKeys = false; EndInclusive = false;
    }
}

public class ListResponse
{
    internal int PoolState;
    private readonly List<string> _keys = new();
    public List<string> Keys { get { PoolGuard.AssertLive(PoolState); return _keys; } }
    public bool EndInclusive;
    public void Reset() { _keys.Clear(); EndInclusive = false; }

    public static ListResponse Rent()
    {
        var m = MessagePool<ListResponse>.TryRent() ?? new ListResponse();
        m.PoolState = PoolStates.Rented;
        return m;
    }

    /// <summary>See <see cref="WriteResponse.Release"/>.  The key strings stay valid (strings are immutable); only the list is recycled.</summary>
    public void Release()
    {
        if (Interlocked.CompareExchange(ref PoolState, PoolStates.Releasing, PoolStates.Rented) != PoolStates.Rented) return;
        Reset();
        PoolLimits.Trim(_keys);
        Volatile.Write(ref PoolState, PoolStates.Pooled);
        MessagePool<ListResponse>.Return(this);
    }
}

public class RangeScanRequest
{
    /// <summary>Opt-in pooled scan API: the record values and keys are parsed into <see cref="ValueLease"/>s instead of new arrays and strings.</summary>
    internal bool PooledValues;
    public long? Shard;
    public string StartInclusive = "";
    public string EndExclusive = "";
    public string? SecondaryIndexName;
    public bool IncludeInternalKeys;
    public bool EndInclusive;
    /// <summary>optional bool include_values = 7; null (unset) means "true" on the server.</summary>
    public bool? IncludeValues;
    public void Reset()
    {
        Shard = null; StartInclusive = ""; EndExclusive = ""; SecondaryIndexName = null;
        IncludeInternalKeys = false; EndInclusive = false; IncludeValues = null; PooledValues = false;
    }
}

public class RangeScanResponse
{
    internal int PoolState;
    internal Spares<GetResponse> SpareRecords;
    private readonly List<GetResponse> _records = new();
    public List<GetResponse> Records { get { PoolGuard.AssertLive(PoolState); return _records; } }
    public bool EndInclusive;
    public void Reset()
    {
        foreach (var e in _records) { e.Reset(); SpareRecords.Push(e); }
        _records.Clear(); EndInclusive = false;
    }

    public static RangeScanResponse Rent()
    {
        var m = MessagePool<RangeScanResponse>.TryRent() ?? new RangeScanResponse();
        m.PoolState = PoolStates.Rented;
        return m;
    }

    /// <summary>See <see cref="ReadResponse.Release"/>.</summary>
    public void Release()
    {
        if (Interlocked.CompareExchange(ref PoolState, PoolStates.Releasing, PoolStates.Rented) != PoolStates.Rented) return;
        Reset();
        SpareRecords.Trim(PoolLimits.MaxSpareChildren);
        PoolLimits.Trim(_records);
        Volatile.Write(ref PoolState, PoolStates.Pooled);
        MessagePool<RangeScanResponse>.Return(this);
    }
}

public class GetSequenceUpdatesRequest
{
    public long Shard;
    public string Key = "";
    public void Reset() { Shard = 0; Key = ""; }
}

public class GetSequenceUpdatesResponse
{
    public string HighestSequenceKey = "";
    public void Reset() { HighestSequenceKey = ""; }
}

public class CreateSessionRequest
{
    public long Shard;
    public uint SessionTimeoutMs;
    public string ClientIdentity = "";
    public void Reset() { Shard = 0; SessionTimeoutMs = 0; ClientIdentity = ""; }
}

public class CreateSessionResponse
{
    public long SessionId;
    public void Reset() { SessionId = 0; }
}

public class SessionHeartbeat
{
    public long Shard;
    public long SessionId;
    public void Reset() { Shard = 0; SessionId = 0; }
}

public class KeepAliveResponse { public void Reset() { } }

public class CloseSessionRequest
{
    public long Shard;
    public long SessionId;
    public void Reset() { Shard = 0; SessionId = 0; }
}

public class CloseSessionResponse { public void Reset() { } }

public class NotificationsRequest
{
    public long Shard;
    public long? StartOffsetExclusive;
    public void Reset() { Shard = 0; StartOffsetExclusive = null; }
}

public class Notification
{
    internal int CachedSize;
    public NotificationType Type;
    public long? VersionId;
    public string? KeyRangeLast;
    public void Reset() { Type = default; VersionId = null; KeyRangeLast = null; }
}

public class NotificationEntry
{
    internal int CachedSize;
    internal Notification? SpareValue;
    /// <summary>optional string key = 1 -- presence is tracked so an empty key is still emitted.</summary>
    public string? Key;
    public Notification? Value;
    public void Reset()
    {
        Key = null;
        if (Value is { } v) { v.Reset(); SpareValue = v; Value = null; }
    }
}

public class NotificationBatch
{
    internal int PoolState;
    internal Spares<NotificationEntry> SpareNotifications;
    public long Shard;
    public long Offset;
    public ulong Timestamp;
    private readonly List<NotificationEntry> _notifications = new();
    public List<NotificationEntry> Notifications { get { PoolGuard.AssertLive(PoolState); return _notifications; } }
    public void Reset()
    {
        Shard = 0; Offset = 0; Timestamp = 0;
        foreach (var e in _notifications) { e.Reset(); SpareNotifications.Push(e); }
        _notifications.Clear();
    }

    public static NotificationBatch Rent()
    {
        var m = MessagePool<NotificationBatch>.TryRent() ?? new NotificationBatch();
        m.PoolState = PoolStates.Rented;
        return m;
    }

    /// <summary>See <see cref="WriteResponse.Release"/>.  The entries (and their strings) must not be used afterwards.</summary>
    public void Release()
    {
        if (Interlocked.CompareExchange(ref PoolState, PoolStates.Releasing, PoolStates.Rented) != PoolStates.Rented) return;
        Reset();
        SpareNotifications.Trim(PoolLimits.MaxSpareChildren);
        PoolLimits.Trim(_notifications);
        Volatile.Write(ref PoolState, PoolStates.Pooled);
        MessagePool<NotificationBatch>.Return(this);
    }
}

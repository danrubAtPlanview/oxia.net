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
//   Sync procedure: when client.proto changes upstream, copy the .proto into Proto/, then mirror the field in this
//   file, in the codec, and in the oracle tests (tests/Oxia.Client.Tests/Proto).
// =====================================================================================================================
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
    public uint MinHashInclusive;
    public uint MaxHashInclusive;
    public void Reset() { MinHashInclusive = 0; MaxHashInclusive = 0; }
}

public class ShardAssignment
{
    public long Shard;
    public string Leader = "";
    /// <summary>oneof shard_boundaries { Int32HashRange int32_hash_range = 3; } -- null when no boundary is set.</summary>
    public Int32HashRange? Int32HashRange;
    public void Reset() { Shard = 0; Leader = ""; Int32HashRange = null; }
}

public class NamespaceShardsAssignment
{
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
    public string IndexName = "";
    public string SecondaryKey = "";
    public void Reset() { IndexName = ""; SecondaryKey = ""; }
}

/// <summary>proto: <c>Version</c> (record version metadata returned by the server).</summary>
public class ProtoVersion
{
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
    public Status Status;
    public ProtoVersion? Version;
    public string? Key;
    public void Reset() { Status = default; Version = null; Key = null; }
}

public class DeleteRequest
{
    public string Key = "";
    public long? ExpectedVersionId;
    public string? PartitionKey;
    public uint OpIndex;
    public virtual void Reset() { Key = ""; ExpectedVersionId = null; PartitionKey = null; OpIndex = 0; }
}

public class DeleteResponse
{
    public Status Status;
    public void Reset() { Status = default; }
}

public class DeleteRangeRequest
{
    public string StartInclusive = "";
    public string EndExclusive = "";
    public uint OpIndex;
    public virtual void Reset() { StartInclusive = ""; EndExclusive = ""; OpIndex = 0; }
}

public class DeleteRangeResponse
{
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
    public List<PutResponse> Puts { get; } = new();
    public List<DeleteResponse> Deletes { get; } = new();
    public List<DeleteRangeResponse> DeleteRanges { get; } = new();
    public void Reset() { Puts.Clear(); Deletes.Clear(); DeleteRanges.Clear(); }
}

public class GetRequest
{
    public string Key = "";
    public bool IncludeValue;
    public KeyComparisonType ComparisonType;
    public string? SecondaryIndexName;
    public virtual void Reset() { Key = ""; IncludeValue = false; ComparisonType = default; SecondaryIndexName = null; }
}

public class GetResponse
{
    public Status Status;
    public ProtoVersion? Version;
    public byte[]? Value;
    public string? Key;
    public string? SecondaryIndexKey;
    public void Reset() { Status = default; Version = null; Value = null; Key = null; SecondaryIndexKey = null; }
}

public class ReadRequest
{
    public long? Shard;
    public List<GetRequest> Gets { get; } = new();
    public void Reset() { Shard = null; Gets.Clear(); }
}

public class ReadResponse
{
    public List<GetResponse> Gets { get; } = new();
    public void Reset() { Gets.Clear(); }
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
    public List<string> Keys { get; } = new();
    public bool EndInclusive;
    public void Reset() { Keys.Clear(); EndInclusive = false; }
}

public class RangeScanRequest
{
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
        IncludeInternalKeys = false; EndInclusive = false; IncludeValues = null;
    }
}

public class RangeScanResponse
{
    public List<GetResponse> Records { get; } = new();
    public bool EndInclusive;
    public void Reset() { Records.Clear(); EndInclusive = false; }
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
    public NotificationType Type;
    public long? VersionId;
    public string? KeyRangeLast;
    public void Reset() { Type = default; VersionId = null; KeyRangeLast = null; }
}

public class NotificationEntry
{
    /// <summary>optional string key = 1 -- presence is tracked so an empty key is still emitted.</summary>
    public string? Key;
    public Notification? Value;
    public void Reset() { Key = null; Value = null; }
}

public class NotificationBatch
{
    public long Shard;
    public long Offset;
    public ulong Timestamp;
    public List<NotificationEntry> Notifications { get; } = new();
    public void Reset() { Shard = 0; Offset = 0; Timestamp = 0; Notifications.Clear(); }
}

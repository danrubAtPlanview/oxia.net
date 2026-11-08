// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/client.go  (AsyncClient, SyncClient, Version, PutResult, GetResult, ListResult, Notifications,
//              NotificationType, Notification, errors)           and  oxia/oxia.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The PUBLIC API of the driver.  Behavioural contract is documented on each member (copied from the Go docs).
//
//   Go -> .NET mapping used by the whole port (see DESIGN.md "Mapping rules"):
//      AsyncClient.Put(...)  <-chan PutResult         -> IAsyncClient.PutAsync(...)  ValueTask<PutResult>   (result carries Err)
//      AsyncClient.Delete    <-chan error             -> ValueTask<Exception?>  (null == success)
//      AsyncClient.List/RangeScan/GetSequenceUpdates  -> ChannelReader<T>  (eager, completed when the stream ends)
//      SyncClient.* (value, err)                      -> ValueTask / ValueTask<tuple>, FAILURES ARE THROWN (see below)
//      context.Context                                -> CancellationToken (first parameter where Go has ctx, so that the
//                                                        trailing `params ReadOnlySpan<TOption>` stays last)
//      variadic options                               -> `params ReadOnlySpan<TOption>` (allocation-free; options are structs)
//      io.Closer                                      -> IAsyncCloser (CloseAsync throws the combined error; DisposeAsync swallows)
//      oxia.Version                                   -> RecordVersion (avoids System.Version)
//      NewAsyncClient / NewSyncClient / NewAdminClient / NewCache -> OxiaClient.CreateAsyncClientAsync / ... (OxiaClient.cs)
//   Error model: IAsyncClient never throws for operation failures -- the error value is in `Err` (Go parity, allocation
//   free for sentinel errors).  ISyncClient throws `OxiaErrors.ToThrowable(err)`: an OxiaException whose InnerException
//   chain contains the sentinel, so `OxiaErrors.Is(ex, ClientErrors.KeyNotFound)` works for both styles.
// =====================================================================================================================
using System.Threading.Channels;
using Oxia.Common;

namespace Oxia.Client;

/// <summary>Version includes some information regarding the state of a record (Go: oxia.Version).</summary>
public readonly struct RecordVersion : IEquatable<RecordVersion>
{
    /// <summary>VersionIdNotExists represent the VersionId of a non-existing record.</summary>
    public const long VersionIdNotExists = -1;

    /// <summary>
    /// An identifier that can be used to refer to a particular version of a record.  Applications shouldn't make
    /// assumptions on the actual values: they are only meaningful for a given key and don't reflect the number of changes.
    /// Use with the ExpectedVersionId option of Put/Delete.
    /// </summary>
    public long VersionId { get; init; }

    /// <summary>The time when the record was last created (re-created after a delete => new value).</summary>
    public ulong CreatedTimestamp { get; init; }

    /// <summary>The time when the record was last modified.</summary>
    public ulong ModifiedTimestamp { get; init; }

    /// <summary>The number of modifications to the record since it was last created.</summary>
    public long ModificationsCount { get; init; }

    /// <summary>Whether the record is ephemeral. See the Ephemeral put option.</summary>
    public bool Ephemeral { get; init; }

    /// <summary>For ephemeral records, the session the record's lifecycle is attached to; 0 for non-ephemeral records.</summary>
    public long SessionId { get; init; }

    private readonly string? _clientIdentity;

    /// <summary>For ephemeral records, the identity of the client that last modified it; empty otherwise.</summary>
    public string ClientIdentity { get => _clientIdentity ?? ""; init => _clientIdentity = value; }

    public bool Equals(RecordVersion o) =>
        VersionId == o.VersionId && CreatedTimestamp == o.CreatedTimestamp && ModifiedTimestamp == o.ModifiedTimestamp &&
        ModificationsCount == o.ModificationsCount && Ephemeral == o.Ephemeral && SessionId == o.SessionId &&
        ClientIdentity == o.ClientIdentity;

    public override bool Equals(object? obj) => obj is RecordVersion v && Equals(v);
    public override int GetHashCode() => HashCode.Combine(VersionId, CreatedTimestamp, ModifiedTimestamp, ModificationsCount, SessionId);
    public static bool operator ==(RecordVersion a, RecordVersion b) => a.Equals(b);
    public static bool operator !=(RecordVersion a, RecordVersion b) => !a.Equals(b);
}

/// <summary>Result of a Put in the async client (Go: PutResult).</summary>
public readonly struct PutResult
{
    public PutResult(string key, RecordVersion version, Exception? err) { Key = key; Version = version; Err = err; }

    /// <summary>The key of the inserted record.</summary>
    public string Key { get; }
    public RecordVersion Version { get; }
    /// <summary>The error if the Put failed; null on success.</summary>
    public Exception? Err { get; }
}

/// <summary>Result of a Get (or one record of a RangeScan) (Go: GetResult).</summary>
public readonly struct GetResult
{
    public GetResult(string key, byte[]? value, RecordVersion version, Exception? err) { Key = key; Value = value; Version = version; Err = err; }

    public string Key { get; }
    public byte[]? Value { get; }
    public RecordVersion Version { get; }
    public Exception? Err { get; }
}

/// <summary>A chunk of keys of a List, or its error (Go: ListResult).</summary>
public readonly struct ListResult
{
    public ListResult(IReadOnlyList<string>? keys, Exception? err) { Keys = keys; Err = err; }
    public IReadOnlyList<string>? Keys { get; }
    public Exception? Err { get; }
}

/// <summary>The type of a notification event (Go: NotificationType; the string forms are part of the contract).</summary>
public enum NotificationType
{
    /// <summary>A record that didn't exist was created.</summary>
    KeyCreated = 0,
    /// <summary>An existing record was modified.</summary>
    KeyModified = 1,
    /// <summary>A record was deleted.</summary>
    KeyDeleted = 2,
    /// <summary>A range of keys was deleted (Go: KeyRangeRangeDeleted).</summary>
    KeyRangeRangeDeleted = 3,
    /// <summary>The subscription missed notifications the server deleted after their retention time. Key and KeyRangeEnd are empty.</summary>
    NotificationsMissed = 4,
}

public static class NotificationTypeExtensions
{
    /// <summary>Go: NotificationType.String() -- "KeyCreated", ..., "Unknown".</summary>
    public static string GoString(this NotificationType t) => t switch
    {
        NotificationType.KeyCreated => "KeyCreated",
        NotificationType.KeyModified => "KeyModified",
        NotificationType.KeyDeleted => "KeyDeleted",
        NotificationType.KeyRangeRangeDeleted => "KeyRangeRangeDeleted",
        NotificationType.NotificationsMissed => "NotificationsMissed",
        _ => "Unknown",
    };
}

/// <summary>One change in the Oxia database (Go: *Notification).</summary>
public sealed class Notification
{
    public Notification(NotificationType type, string key, long versionId, string keyRangeEnd)
    { Type = type; Key = key; VersionId = versionId; KeyRangeEnd = keyRangeEnd; }

    public NotificationType Type { get; }
    public string Key { get; }
    /// <summary>The current VersionId of the record, or -1 for a KeyDeleted event.</summary>
    public long VersionId { get; }
    /// <summary>For KeyRangeRangeDeleted: end (excluded) of the range. Empty covers all the records from Key on.</summary>
    public string KeyRangeEnd { get; }
}

/// <summary>Notifications allow applications to receive the feed of changes happening in the Oxia database (Go: Notifications).</summary>
public interface INotifications : IAsyncCloser
{
    /// <summary>Go: Ch() -- the channel where all the notification events are published; completed on close.</summary>
    ChannelReader<Notification> Ch { get; }
}

/// <summary>
/// Oxia client with methods suitable for asynchronous operations (Go: AsyncClient).  Exposes the same functionality as
/// <see cref="ISyncClient"/> but returns the pending result instead of waiting, letting applications enqueue many operations
/// which the library groups into batches (see WithBatchLinger).
/// All methods return promptly; failures are reported in the result's <c>Err</c> (never thrown), except for argument misuse.
/// </summary>
public interface IAsyncClient : IAsyncCloser
{
    /// <summary>Associates a value with a key. Options: ExpectedVersionId, ExpectedRecordNotExists, Ephemeral, PartitionKey, SequenceKeysDeltas, SecondaryIndex.
    /// Err = ErrUnexpectedVersionId / ErrInvalidOptions (also when the key is not valid UTF-16-surrogate-clean, i.e. not valid UTF-8).</summary>
    ValueTask<PutResult> PutAsync(string key, ReadOnlyMemory<byte> value, params ReadOnlySpan<PutOption> options);

    /// <summary>Removes the key and its value. Err = null on success (also when the key does not exist is NOT success: ErrKeyNotFound), ErrUnexpectedVersionId, ErrInvalidOptions.</summary>
    ValueTask<Exception?> DeleteAsync(string key, params ReadOnlySpan<DeleteOption> options);

    /// <summary>Deletes any records with keys within [min, max). Empty max deletes all records from min on.</summary>
    ValueTask<Exception?> DeleteRangeAsync(string minKeyInclusive, string maxKeyExclusive, params ReadOnlySpan<DeleteRangeOption> options);

    /// <summary>Returns the value and version of a key. Err = ErrKeyNotFound if the record does not exist.</summary>
    ValueTask<GetResult> GetAsync(string key, params ReadOnlySpan<GetOption> options);

    /// <summary>Deprecated upstream: use RangeScan with IncludeValue(false). Streams chunks of keys; the reader completes (without error) when <paramref name="ct"/> is done.</summary>
    ChannelReader<ListResult> ListAsync(CancellationToken ct, string minKeyInclusive, string maxKeyExclusive, params ReadOnlySpan<ListOption> options);

    /// <summary>Scans the existing records with keys within the range, merging shards in key order.</summary>
    ChannelReader<GetResult> RangeScanAsync(CancellationToken ct, string minKeyInclusive, string maxKeyExclusive, params ReadOnlySpan<RangeScanOption> options);

    /// <summary>Subscribes to the updates of a sequential key (requires the PartitionKey option, else throws OxiaException wrapping ErrInvalidOptions).
    /// The reader completes once <paramref name="ct"/> is done or the client is closed.</summary>
    ChannelReader<string> GetSequenceUpdates(CancellationToken ct, string prefixKey, params ReadOnlySpan<GetSequenceUpdatesOption> options);

    /// <summary>Creates a new subscription to the notifications of every change applied to the database.</summary>
    ValueTask<INotifications> GetNotificationsAsync();
}

/// <summary>
/// The main interface to perform operations with Oxia (Go: SyncClient).  Valid until explicitly closed; shareable across
/// threads.  Ephemeral records are deleted when the client is closed or the process crashes.
/// Operation failures are THROWN as OxiaException (sentinel reachable through <c>OxiaErrors.Is</c>); cancellation throws
/// OperationCanceledException.
/// </summary>
public interface ISyncClient : IAsyncCloser
{
    /// <returns>(insertedKey, version)</returns>
    ValueTask<(string InsertedKey, RecordVersion Version)> PutAsync(CancellationToken ct, string key, ReadOnlyMemory<byte> value, params ReadOnlySpan<PutOption> options);

    ValueTask DeleteAsync(CancellationToken ct, string key, params ReadOnlySpan<DeleteOption> options);

    ValueTask DeleteRangeAsync(CancellationToken ct, string minKeyInclusive, string maxKeyExclusive, params ReadOnlySpan<DeleteRangeOption> options);

    /// <returns>(storedKey, value, version)</returns>
    ValueTask<(string StoredKey, byte[]? Value, RecordVersion Version)> GetAsync(CancellationToken ct, string key, params ReadOnlySpan<GetOption> options);

    /// <summary>Deprecated upstream: use RangeScan with IncludeValue(false).</summary>
    ValueTask<List<string>> ListAsync(CancellationToken ct, string minKeyInclusive, string maxKeyExclusive, params ReadOnlySpan<ListOption> options);

    /// <summary>Ordering in the results is respected only if a PartitionKey option is passed (and the keys were inserted with that partition key)... and across shards the client merges in key order.</summary>
    ChannelReader<GetResult> RangeScanAsync(CancellationToken ct, string minKeyInclusive, string maxKeyExclusive, params ReadOnlySpan<RangeScanOption> options);

    ChannelReader<string> GetSequenceUpdates(CancellationToken ct, string prefixKey, params ReadOnlySpan<GetSequenceUpdatesOption> options);

    ValueTask<INotifications> GetNotificationsAsync();
}

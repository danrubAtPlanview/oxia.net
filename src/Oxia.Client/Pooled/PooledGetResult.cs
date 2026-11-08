// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/client.go  GetResult  (the result of Get / one record of RangeScan)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The OPT-IN zero-allocation read result.  GetResult.Value is a `byte[]` the user owns, so a faithful Get must allocate
//              one array per value.  PooledGetResult exposes the value as a ReadOnlyMemory<byte> over a pooled buffer; the user disposes the
//              result when done with the value and the buffer goes back to the pool.  The whole record (value, key bytes, version, secondary
//              key) lives in ONE pooled ValueLease, so the result itself is a small struct (a reference, a generation, the key string of a
//              Get, the error).  Used through IAsyncClient.GetPooledAsync / RangeScanPooledAsync (PooledClientExtensions).
//   Divergences: n/a -- an addition (Go returns a fresh []byte per value).  Rules:
//                * Dispose() exactly once; Value / KeyUtf8 are valid only until then.  Copies of the struct share the lease: a second
//                  Dispose of a copy is detected (generation check) and ignored, and a stale copy reads an empty Value.
//                * A result with Err set (key not found, ...) holds no buffer: Dispose is then a no-op.
//                * A result that is never disposed is not a leak: the buffer is an ordinary array the GC reclaims (it just is not reused).
// =====================================================================================================================
using Oxia.Common.Proto;

namespace Oxia.Client.Pooled;

/// <summary>
/// Result of <see cref="PooledClientExtensions.GetPooledAsync"/> or one record of
/// <see cref="PooledClientExtensions.RangeScanPooledAsync"/>.  <see cref="Value"/> is backed by a pooled buffer: dispose the result
/// when you are done with it.
/// </summary>
public readonly struct PooledGetResult : IDisposable
{
    private readonly ValueLease? _lease;
    private readonly int _generation;
    private readonly string? _key;

    internal PooledGetResult(ValueLease lease, string? key)
    {
        _lease = lease;
        _generation = lease.Generation;
        _key = key;
        Err = null;
    }

    /// <summary>A failed result (no buffer).</summary>
    internal PooledGetResult(Exception err)
    {
        _lease = null;
        _generation = 0;
        _key = "";
        Err = err;
    }

    /// <summary>The error if the operation failed (e.g. ErrKeyNotFound); null on success.</summary>
    public Exception? Err { get; }

    /// <summary>The version of the record.</summary>
    public RecordVersion Version => Live ? ToVersion(_lease!) : default;

    /// <summary>
    /// The key of the record.  For a Get it is the key you asked for (or the stored key when you used a comparison); for a range scan
    /// record it is decoded from the UTF-8 bytes on first use (one string per record: prefer <see cref="KeyUtf8"/> in hot loops).
    /// </summary>
    public string Key
    {
        get
        {
            if (_key is not null) return _key;
            return Live ? _lease!.KeyString() : "";
        }
    }

    /// <summary>The UTF-8 bytes of the key of a range scan record (empty for a Get, whose key you know); valid until Dispose.</summary>
    public ReadOnlySpan<byte> KeyUtf8 => Live ? _lease!.KeyUtf8 : default;

    /// <summary>The value; empty when the record has no value (or IncludeValue(false)).  Valid until <see cref="Dispose"/>.</summary>
    public ReadOnlyMemory<byte> Value => Live ? _lease!.ValueMemory : default;

    /// <summary>True when the record had a value (an empty value is still a value).</summary>
    public bool HasValue => Live && _lease!.HasValue;

    /// <summary>The secondary key of a record of an index scan (null otherwise).</summary>
    internal string? SecondaryIndexKey => Live ? _lease!.SecondaryIndexKey : null;

    private bool Live => _lease is not null && _lease.Generation == _generation;

    /// <summary>Gives the buffer back to the pool.  Safe to call more than once (also on copies of the struct).</summary>
    public void Dispose() => _lease?.TryRelease(_generation);

    private static RecordVersion ToVersion(ValueLease l) => new()
    {
        VersionId = l.VersionId,
        ModificationsCount = l.ModificationsCount,
        CreatedTimestamp = l.CreatedTimestamp,
        ModifiedTimestamp = l.ModifiedTimestamp,
        Ephemeral = l.HasSessionId,
        ClientIdentity = l.ClientIdentity ?? "",
        SessionId = l.SessionId,
    };

    /// <summary>
    /// A result built from a response parsed for the pooled API: it takes the response's lease (or rents one and copies the classic
    /// value array, if the response did not come with one) and the version of the record.  <paramref name="originalKey"/> is the key a
    /// Get asked for (the stored key replaces it when the response carried one); null for a scan record, whose key stays UTF-8 in the
    /// lease until it is asked for.
    /// </summary>
    internal static PooledGetResult FromResponse(GetResponse response, string? originalKey, bool includeValue = true)
    {
        ValueLease? lease = response.DetachLease();
        if (lease is null)
        {
            lease = ValueLease.Rent();
            if (response.Value is { } classic) classic.AsSpan().CopyTo(lease.ReserveValue(classic.Length));
        }

        if (!includeValue) lease.DropValue();
        lease.SetVersion(response.Version);
        lease.SecondaryIndexKey = response.SecondaryIndexKey;

        string? key = response.Key;
        if (key is null && originalKey is not null) key = lease.KeyLength >= 0 ? lease.KeyString() : originalKey;
        return new PooledGetResult(lease, key);
    }

    /// <summary>A pooled result holding a COPY of an exact-size array (used when the response did not come with a lease).</summary>
    internal static PooledGetResult FromArray(string? key, byte[]? value, RecordVersion version)
    {
        var lease = ValueLease.Rent();
        if (value is not null) value.AsSpan().CopyTo(lease.ReserveValue(value.Length));
        lease.VersionId = version.VersionId;
        lease.ModificationsCount = version.ModificationsCount;
        lease.CreatedTimestamp = version.CreatedTimestamp;
        lease.ModifiedTimestamp = version.ModifiedTimestamp;
        lease.HasSessionId = version.Ephemeral;
        lease.SessionId = version.SessionId;
        lease.ClientIdentity = version.ClientIdentity.Length == 0 ? null : version.ClientIdentity;
        return new PooledGetResult(lease, key);
    }
}

// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/client.go  GetResult  (the result of Get / one record of RangeScan)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The OPT-IN zero-allocation read result.  GetResult.Value is a `byte[]` the user owns, so a faithful Get must allocate
//              one array per value.  PooledGetResult exposes the value as a ReadOnlyMemory<byte> over a buffer rented from
//              ArrayPool<byte>.Shared; the user disposes the result when done with the value and the buffer goes back to the pool.
//              Used through IAsyncClient.GetPooledAsync / RangeScanPooledAsync (PooledClientExtensions).
//   Divergences: n/a -- an addition (Go returns a fresh []byte per value).  Rules:
//                * Dispose() exactly once; Value / KeyUtf8 are valid only until then.  Copies of the struct share the lease: a second
//                  Dispose of a copy is detected (generation check) and ignored, and a stale copy reads an empty Value.
//                * A result with Err set (key not found, ...) holds no buffer: Dispose is then a no-op.
//                * A result that is never disposed is not a leak: the buffer is an ordinary array the GC reclaims (it just is not reused).
// =====================================================================================================================
using System.Text;
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

    internal PooledGetResult(ValueLease? lease, int generation, string? key, RecordVersion version, Exception? err)
    {
        _lease = lease;
        _generation = generation;
        _key = key;
        Version = version;
        Err = err;
    }

    /// <summary>A failed result (no buffer).</summary>
    internal PooledGetResult(Exception err) : this(null, 0, "", default, err) { }

    /// <summary>The error if the operation failed (e.g. ErrKeyNotFound); null on success.</summary>
    public Exception? Err { get; }

    /// <summary>The version of the record.</summary>
    public RecordVersion Version { get; }

    /// <summary>
    /// The key of the record.  For a Get it is the key you asked for (or the stored key when you used a comparison); for a range scan
    /// record it is decoded from the UTF-8 bytes on first use (one string per call: prefer <see cref="KeyUtf8"/> in hot loops).
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

    private bool Live => _lease is not null && _lease.Generation == _generation;

    /// <summary>Gives the buffer back to the pool.  Safe to call more than once (also on copies of the struct).</summary>
    public void Dispose() => _lease?.TryRelease(_generation);

    /// <summary>A pooled result holding a COPY of an exact-size array (used when the response did not come with a lease).</summary>
    internal static PooledGetResult FromArray(string? key, byte[]? value, RecordVersion version)
    {
        if (value is null) return new PooledGetResult(null, 0, key, version, null);
        var lease = ValueLease.Rent();
        value.AsSpan().CopyTo(lease.ReserveValue(value.Length));
        return new PooledGetResult(lease, lease.Generation, key, version, null);
    }
}

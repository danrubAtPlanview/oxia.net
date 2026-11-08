// =====================================================================================================================
// PORT NOTE
//   Upstream : (no Go counterpart) -- Go hands the user a fresh []byte per value (`GetResult.Value`).
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The lease behind the OPT-IN pooled read API (Oxia.Client.Pooled): when a read request asks for pooled values the
//              gRPC deserializer copies the value (and, for range scans, the key) of each record into ONE buffer owned by a
//              ValueLease attached to the GetResponse.  The consumer detaches the lease, fills in the record's metadata (version, secondary
//              key) and gives it to the user (PooledGetResult); disposing that releases the lease.  A lease that is never detached (the
//              call was dropped, or a classic consumer copied the value out) goes back when the GetResponse is Reset().
//              Layout of the buffer: [value bytes][key bytes] in the order they were parsed; presence is explicit
//              (ValueLength / KeyLength are -1 when the field was not on the wire).
//   Buffers  : the lease objects are pooled (BoundedStack); a lease KEEPS the buffer it used (<= 64 KiB) across its cycles, so the steady
//              state allocates nothing -- ArrayPool<byte>.Shared alone could not: it holds ~8 arrays per core and a pipelined burst of
//              a thousand reads has a thousand buffers out at once.  The bytes kept this way are bounded by a global budget (32 MiB);
//              beyond it, and for bigger buffers, the buffer is rented from / returned to ArrayPool<byte>.Shared per use.
//   Safety   : every lease has a GENERATION that is bumped by the single successful release; holders remember the generation they
//              were given, so a second Dispose of a COPY of the result struct (or any stale use) is detected and ignored instead of
//              corrupting the pool.
//   Divergences: the faithful API (GetAsync) never sees a lease: it is only requested by pooled calls (ReadRequest / RangeScanRequest
//              .PooledValues), and any classic consumer of a leased response copies the value into an exact-size array first.
// =====================================================================================================================
using System.Buffers;
using System.Text;

namespace Oxia.Common.Proto;

internal sealed class ValueLease
{
    private const int MaxRetainedBufferLength = 64 * 1024;
    private const long RetainedBudget = 32L * 1024 * 1024;

    private static readonly BoundedStack<ValueLease> Pool = new(16384);
    private static long s_retainedBytes;

    private byte[]? _buffer;     // the buffer in use (the retained one, or rented for this use)
    private byte[]? _retained;   // the buffer this lease keeps across cycles (counted in s_retainedBytes)
    private int _used;
    private int _valueOffset;
    private int _keyOffset;
    private int _generation;
    private string? _keyString;

    /// <summary>Length of the value, or -1 if the response had no value.</summary>
    public int ValueLength = -1;

    /// <summary>Length of the UTF-8 key, or -1 if the response carried no key.</summary>
    public int KeyLength = -1;

    // ---- the record metadata the consumer fills in when it takes the lease (so that a result is one reference to one object) ----
    public long VersionId;
    public long ModificationsCount;
    public ulong CreatedTimestamp;
    public ulong ModifiedTimestamp;
    public bool HasSessionId;
    public long SessionId;
    public string? ClientIdentity;
    public string? SecondaryIndexKey;

    public static ValueLease Rent() => Pool.TryPop() ?? new ValueLease();

    public int Generation => Volatile.Read(ref _generation);

    public bool HasValue => ValueLength >= 0;

    public ReadOnlySpan<byte> Value => ValueLength <= 0 ? default : new ReadOnlySpan<byte>(_buffer, _valueOffset, ValueLength);

    public ReadOnlyMemory<byte> ValueMemory => ValueLength <= 0 ? default : new ReadOnlyMemory<byte>(_buffer, _valueOffset, ValueLength);

    public ReadOnlySpan<byte> KeyUtf8 => KeyLength <= 0 ? default : new ReadOnlySpan<byte>(_buffer, _keyOffset, KeyLength);

    /// <summary>The key as a string (decoded once, on first use).</summary>
    public string KeyString() => KeyLength <= 0 ? string.Empty : _keyString ??= Encoding.UTF8.GetString(KeyUtf8);

    /// <summary>Copies the version of the record into the lease.</summary>
    public void SetVersion(ProtoVersion? v)
    {
        if (v is null) return;
        VersionId = v.VersionId;
        ModificationsCount = v.ModificationsCount;
        CreatedTimestamp = v.CreatedTimestamp;
        ModifiedTimestamp = v.ModifiedTimestamp;
        HasSessionId = v.SessionId.HasValue;
        SessionId = v.SessionId ?? 0;
        ClientIdentity = v.ClientIdentity;
    }

    /// <summary>Reserves room for the value in the buffer; the caller fills the returned span.</summary>
    public Span<byte> ReserveValue(int length)
    {
        ValueLength = length;
        if (length == 0) return default;
        _valueOffset = _used;
        return Reserve(length);
    }

    /// <summary>Reserves room for the UTF-8 key in the buffer; the caller fills the returned span.</summary>
    public Span<byte> ReserveKey(int length)
    {
        KeyLength = length;
        _keyString = null;
        if (length == 0) return default;
        _keyOffset = _used;
        return Reserve(length);
    }

    private Span<byte> Reserve(int length)
    {
        int needed = checked(_used + length);
        byte[]? buffer = _buffer;
        if (buffer is null)
        {
            buffer = _buffer = _retained is { } kept && kept.Length >= needed ? kept : ArrayPool<byte>.Shared.Rent(needed);
        }
        else if (buffer.Length < needed)
        {
            byte[] bigger = ArrayPool<byte>.Shared.Rent(needed);
            Buffer.BlockCopy(buffer, 0, bigger, 0, _used);
            if (!ReferenceEquals(buffer, _retained)) ArrayPool<byte>.Shared.Return(buffer);
            buffer = _buffer = bigger;
        }
        var span = new Span<byte>(buffer, _used, length);
        _used = needed;
        return span;
    }

    /// <summary>
    /// Gives the buffer and the lease back, if <paramref name="generation"/> is the current one.  False (and nothing happens) for
    /// a stale holder, e.g. a second Dispose of a copy of the result.
    /// </summary>
    public bool TryRelease(int generation)
    {
        if (Interlocked.CompareExchange(ref _generation, unchecked(generation + 1), generation) != generation) return false;

        byte[]? buffer = _buffer;
        _buffer = null;
        _used = 0;
        _valueOffset = 0;
        _keyOffset = 0;
        ValueLength = -1;
        KeyLength = -1;
        _keyString = null;
        VersionId = 0;
        ModificationsCount = 0;
        CreatedTimestamp = 0;
        ModifiedTimestamp = 0;
        HasSessionId = false;
        SessionId = 0;
        ClientIdentity = null;
        SecondaryIndexKey = null;

        if (buffer is not null && !ReferenceEquals(buffer, _retained))
        {
            // a buffer rented for this use: keep it if the budget allows (replacing a smaller retained one), otherwise give it back
            if (buffer.Length <= MaxRetainedBufferLength && TryRetain(buffer))
            {
                if (_retained is { } old)
                {
                    Interlocked.Add(ref s_retainedBytes, -old.Length);
                    ArrayPool<byte>.Shared.Return(old);
                }
                _retained = buffer;
            }
            else
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        if (!Pool.TryPush(this) && _retained is { } kept)
        {
            // the pool is full and this lease is dropped: its kept buffer goes back too
            _retained = null;
            Interlocked.Add(ref s_retainedBytes, -kept.Length);
            ArrayPool<byte>.Shared.Return(kept);
        }
        return true;
    }

    private static bool TryRetain(byte[] buffer)
    {
        if (Interlocked.Add(ref s_retainedBytes, buffer.Length) <= RetainedBudget) return true;
        Interlocked.Add(ref s_retainedBytes, -buffer.Length);
        return false;
    }

    /// <summary>Forgets the value (IncludeValue(false) against a server that sends it anyway); the buffer is still kept for reuse.</summary>
    public void DropValue() => ValueLength = -1;

    /// <summary>A copy of the value in a new exact-size array (null if the response had no value).</summary>
    public byte[]? CopyValueToArray()
    {
        if (ValueLength < 0) return null;
        return ValueLength == 0 ? Array.Empty<byte>() : Value.ToArray();
    }
}

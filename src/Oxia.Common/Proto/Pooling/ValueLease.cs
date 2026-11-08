// =====================================================================================================================
// PORT NOTE
//   Upstream : (no Go counterpart) -- Go hands the user a fresh []byte per value (`GetResult.Value`).
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The lease behind the OPT-IN pooled read API (Oxia.Client.Pooled): when a read request asks for pooled values the
//              gRPC deserializer copies the value (and, for range scans, the key) of each record into ONE buffer rented from
//              ArrayPool<byte>.Shared and parks it in a ValueLease attached to the GetResponse.  The consumer detaches the lease
//              and gives it to the user (PooledGetResult); disposing it returns the buffer and the lease object to their pools.
//              A lease that is never detached (the call was dropped, or a classic consumer copied the value out) goes back when
//              the GetResponse is Reset().
//              Layout of the buffer: [value bytes][key bytes] in the order they were parsed; presence is explicit
//              (ValueLength / KeyLength are -1 when the field was not on the wire).
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
    private static readonly BoundedStack<ValueLease> Pool = new(16384);

    private byte[]? _buffer;
    private int _used;
    private int _valueOffset;
    private int _keyOffset;
    private int _generation;
    private string? _keyString;

    /// <summary>Length of the value, or -1 if the response had no value.</summary>
    public int ValueLength = -1;

    /// <summary>Length of the UTF-8 key, or -1 if the response carried no key.</summary>
    public int KeyLength = -1;

    public static ValueLease Rent() => Pool.TryPop() ?? new ValueLease();

    public int Generation => Volatile.Read(ref _generation);

    public bool HasValue => ValueLength >= 0;

    public ReadOnlySpan<byte> Value => ValueLength <= 0 ? default : new ReadOnlySpan<byte>(_buffer, _valueOffset, ValueLength);

    public ReadOnlyMemory<byte> ValueMemory => ValueLength <= 0 ? default : new ReadOnlyMemory<byte>(_buffer, _valueOffset, ValueLength);

    public ReadOnlySpan<byte> KeyUtf8 => KeyLength <= 0 ? default : new ReadOnlySpan<byte>(_buffer, _keyOffset, KeyLength);

    /// <summary>The key as a string (decoded once, on first use).</summary>
    public string KeyString() => KeyLength <= 0 ? string.Empty : _keyString ??= Encoding.UTF8.GetString(KeyUtf8);

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
            buffer = _buffer = ArrayPool<byte>.Shared.Rent(needed);
        }
        else if (buffer.Length < needed)
        {
            byte[] bigger = ArrayPool<byte>.Shared.Rent(needed);
            Buffer.BlockCopy(buffer, 0, bigger, 0, _used);
            ArrayPool<byte>.Shared.Return(buffer);
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
        if (buffer is not null) ArrayPool<byte>.Shared.Return(buffer);
        Pool.TryPush(this);
        return true;
    }

    /// <summary>A copy of the value in a new exact-size array (null if the response had no value).</summary>
    public byte[]? CopyValueToArray()
    {
        if (ValueLength < 0) return null;
        return ValueLength == 0 ? Array.Empty<byte>() : Value.ToArray();
    }
}

// =====================================================================================================================
// PORT NOTE
//   Upstream : common/proto/client_vtproto.pb.go (UnmarshalVT -- the field-by-field decoding loops, skip() for unknown
//              fields, ErrIntOverflow / ErrInvalidLength / io.ErrUnexpectedEOF)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Protobuf decoder primitives over a contiguous span or a (possibly multi-segment) ReadOnlySequence<byte>,
//              with nested-message length limits, no allocations apart from the decoded strings / byte arrays.
//   Divergences: vtproto indexes a single []byte.  gRPC.NET hands the payload over as a ReadOnlySequence<byte>, so this
//              reader walks segments itself (a field may straddle any segment boundary).  Truncated or garbled input
//              throws ProtoParseException; unknown fields (incl. groups) are skipped; a known field number with a
//              different wire type is skipped as unknown (protoc behaviour).  Invalid UTF-8 decodes with U+FFFD.
// =====================================================================================================================
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Oxia.Common.Proto.Codec;

/// <summary>Thrown by the codecs when the input is truncated, malformed or exceeds a sanity limit.</summary>
public sealed class ProtoParseException : FormatException
{
    public ProtoParseException(string message) : base(message) { }
}

/// <summary>
/// Forward protobuf reader.  <c>while (r.TryReadTag(out var tag)) switch (tag) { ... default: r.SkipField(tag); }</c> loops until
/// the end of the current message (the whole input, or the nested message entered with <see cref="BeginMessage"/>).
/// </summary>
public ref struct ProtoReader
{
    private const int MaxGroupDepth = 64;

    private ReadOnlySpan<byte> _span;   // current segment
    private int _pos;                   // read index within _span
    private int _limitInSpan;           // readable end within _span: min(_span.Length, _limit - _spanBase)
    private long _spanBase;             // absolute offset of _span[0]
    private long _limit;                // absolute end of the innermost message being read
    private ReadOnlySequence<byte> _sequence;
    private SequencePosition _next;     // next segment of _sequence to load
    private bool _hasMore;
    private int _groupDepth;

    /// <summary>Reads a contiguous buffer.</summary>
    public ProtoReader(ReadOnlySpan<byte> data)
    {
        _span = data;
        _pos = 0;
        _limitInSpan = data.Length;
        _spanBase = 0;
        _limit = data.Length;
        _sequence = default;
        _next = default;
        _hasMore = false;
        _groupDepth = 0;
    }

    /// <summary>Reads a sequence; a single-segment sequence takes exactly the same path as the span constructor.</summary>
    public ProtoReader(in ReadOnlySequence<byte> data)
    {
        _pos = 0;
        _spanBase = 0;
        _limit = data.Length;
        _groupDepth = 0;
        if (data.IsSingleSegment)
        {
            _span = data.FirstSpan;
            _limitInSpan = _span.Length;
            _sequence = default;
            _next = default;
            _hasMore = false;
            return;
        }
        _span = default;
        _limitInSpan = 0;
        _sequence = data;
        _next = data.Start;
        _hasMore = true;
        LoadNextSegment();
    }

    /// <summary>Bytes left before the end of the current message.</summary>
    public readonly long Remaining => _limit - (_spanBase + _pos);

    /// <summary>True when the current message has been fully consumed.</summary>
    public readonly bool AtEnd => _spanBase + _pos >= _limit;

    // ---- segments --------------------------------------------------------------------------------------------------

    private bool LoadNextSegment()
    {
        if (!_hasMore) return false;
        while (true)
        {
            if (!_sequence.TryGet(ref _next, out ReadOnlyMemory<byte> mem, advance: true))
            {
                _hasMore = false;
                return false;
            }
            if (mem.IsEmpty) continue;
            _spanBase += _span.Length;
            _span = mem.Span;
            _pos = 0;
            long room = _limit - _spanBase;
            _limitInSpan = room >= _span.Length ? _span.Length : (room > 0 ? (int)room : 0);
            return true;
        }
    }

    [DoesNotReturn]
    private static void ThrowTruncated() => throw new ProtoParseException("Protocol message was truncated (unexpected end of data).");

    [DoesNotReturn]
    private static void ThrowMalformedVarint() => throw new ProtoParseException("Protocol message contained a malformed varint.");

    [DoesNotReturn]
    private static void ThrowInvalidTag() => throw new ProtoParseException("Protocol message contained an invalid tag.");

    [DoesNotReturn]
    private static void ThrowNegativeLength() => throw new ProtoParseException("Protocol message contained a length that is negative or larger than the data.");

    [MethodImpl(MethodImplOptions.NoInlining)]
    private byte ReadByteSlow()
    {
        while (_pos >= _limitInSpan)
        {
            if (_spanBase + _pos >= _limit) ThrowTruncated();
            if (!LoadNextSegment()) ThrowTruncated();
        }
        return _span[_pos++];
    }

    // ---- tags ------------------------------------------------------------------------------------------------------

    /// <summary>Reads the next tag; false at the end of the current message.  Throws on a zero/overlong tag or truncation.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryReadTag(out uint tag)
    {
        int pos = _pos;
        if (pos < _limitInSpan)
        {
            uint b = _span[pos];
            if (b - 1 < 0x7F)
            {
                _pos = pos + 1;
                tag = b;
                return true;
            }
        }
        return TryReadTagSlow(out tag);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private bool TryReadTagSlow(out uint tag)
    {
        while (_pos >= _limitInSpan)
        {
            if (_spanBase + _pos >= _limit)
            {
                tag = 0;
                return false;
            }
            if (!LoadNextSegment()) ThrowTruncated();
        }
        ulong v = ReadVarint64();
        if (v == 0 || v > uint.MaxValue) ThrowInvalidTag();
        tag = (uint)v;
        return true;
    }

    // ---- varints ---------------------------------------------------------------------------------------------------

    public ulong ReadVarint64()
    {
        int pos = _pos;
        if (_span.Length - pos >= 10)
        {
            ref byte p = ref Unsafe.Add(ref MemoryMarshal.GetReference(_span), pos);
            ulong result = p;
            int i = 1;
            if (result >= 0x80)
            {
                result &= 0x7F;
                int shift = 7;
                while (true)
                {
                    ulong b = Unsafe.Add(ref p, i++);
                    result |= (b & 0x7F) << shift;
                    if (b < 0x80) break;
                    shift += 7;
                    if (shift > 63) ThrowMalformedVarint();
                }
            }
            if (pos + i > _limitInSpan) ThrowTruncated();
            _pos = pos + i;
            return result;
        }
        return ReadVarint64Slow();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private ulong ReadVarint64Slow()
    {
        ulong result = 0;
        for (int shift = 0; shift < 70; shift += 7)
        {
            byte b = ReadByteSlow();
            result |= (ulong)(b & 0x7F) << shift;
            if (b < 0x80) return result;
        }
        ThrowMalformedVarint();
        return 0;
    }

    public long ReadInt64() => (long)ReadVarint64();
    public uint ReadUInt32() => (uint)ReadVarint64();
    public int ReadInt32() => (int)ReadVarint64();
    public bool ReadBool() => ReadVarint64() != 0;

    /// <summary>Reads a length prefix and validates it against the bytes left in the current message.</summary>
    public int ReadLength()
    {
        ulong v = ReadVarint64();
        if (v > int.MaxValue || (long)v > Remaining) ThrowNegativeLength();
        return (int)v;
    }

    // ---- fixed width -----------------------------------------------------------------------------------------------

    public uint ReadFixed32()
    {
        if (_limitInSpan - _pos >= 4)
        {
            uint v = BinaryPrimitives.ReadUInt32LittleEndian(_span.Slice(_pos));
            _pos += 4;
            return v;
        }
        return ReadFixed32Slow();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private uint ReadFixed32Slow()
    {
        uint v = 0;
        for (int i = 0; i < 4; i++) v |= (uint)ReadByteSlow() << (8 * i);
        return v;
    }

    public ulong ReadFixed64()
    {
        if (_limitInSpan - _pos >= 8)
        {
            ulong v = BinaryPrimitives.ReadUInt64LittleEndian(_span.Slice(_pos));
            _pos += 8;
            return v;
        }
        return ReadFixed64Slow();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private ulong ReadFixed64Slow()
    {
        ulong v = 0;
        for (int i = 0; i < 8; i++) v |= (ulong)ReadByteSlow() << (8 * i);
        return v;
    }

    // ---- length-delimited ------------------------------------------------------------------------------------------

    /// <summary>Copies <paramref name="destination"/>.Length bytes out of the input (caller validated the length).</summary>
    private void ReadInto(scoped Span<byte> destination)
    {
        while (!destination.IsEmpty)
        {
            int avail = _limitInSpan - _pos;
            if (avail == 0)
            {
                if (_spanBase + _pos >= _limit || !LoadNextSegment()) ThrowTruncated();
                continue;
            }
            int n = Math.Min(avail, destination.Length);
            _span.Slice(_pos, n).CopyTo(destination);
            _pos += n;
            destination = destination.Slice(n);
        }
    }

    /// <summary>Length-delimited UTF-8 string (empty string without allocation).</summary>
    public string ReadString()
    {
        int len = ReadLength();
        if (len == 0) return string.Empty;
        if (_limitInSpan - _pos >= len)
        {
            string s = Encoding.UTF8.GetString(_span.Slice(_pos, len));
            _pos += len;
            return s;
        }
        return ReadStringSlow(len);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private string ReadStringSlow(int len)
    {
        if (len <= 512)
        {
            Span<byte> tmp = stackalloc byte[512];
            tmp = tmp.Slice(0, len);
            ReadInto(tmp);
            return Encoding.UTF8.GetString(tmp);
        }
        byte[] rented = ArrayPool<byte>.Shared.Rent(len);
        try
        {
            ReadInto(rented.AsSpan(0, len));
            return Encoding.UTF8.GetString(rented, 0, len);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>Length-delimited bytes as an exactly-sized new array (a shared empty array for length 0).</summary>
    public byte[] ReadBytes()
    {
        int len = ReadLength();
        if (len == 0) return Array.Empty<byte>();
        byte[] result = GC.AllocateUninitializedArray<byte>(len);
        if (_limitInSpan - _pos >= len)
        {
            _span.Slice(_pos, len).CopyTo(result);
            _pos += len;
        }
        else
        {
            ReadInto(result);
        }
        return result;
    }

    // ---- nested messages -------------------------------------------------------------------------------------------

    /// <summary>Reads the length prefix of a nested message and restricts reading to its body.  Pass the result to <see cref="EndMessage"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long BeginMessage()
    {
        int len = ReadLength();
        long old = _limit;
        long newLimit = _spanBase + _pos + len;
        _limit = newLimit;
        long room = newLimit - _spanBase;
        if (room < _limitInSpan) _limitInSpan = (int)room;
        return old;
    }

    /// <summary>Leaves the nested message entered with <see cref="BeginMessage"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void EndMessage(long previousLimit)
    {
        _limit = previousLimit;
        long room = previousLimit - _spanBase;
        _limitInSpan = room >= _span.Length ? _span.Length : (int)room;
    }

    /// <summary>Starts a packed block: length prefix + restriction, like <see cref="BeginMessage"/>.</summary>
    public long BeginPacked() => BeginMessage();

    // ---- skipping --------------------------------------------------------------------------------------------------

    private void Skip(long count)
    {
        if (count <= _limitInSpan - _pos)
        {
            _pos += (int)count;
            return;
        }
        if (count > Remaining) ThrowTruncated();
        while (count > 0)
        {
            int avail = _limitInSpan - _pos;
            if (avail == 0)
            {
                if (!LoadNextSegment()) ThrowTruncated();
                continue;
            }
            int n = (int)Math.Min(avail, count);
            _pos += n;
            count -= n;
        }
    }

    /// <summary>Skips the value of a field whose tag was just read (unknown field number or unexpected wire type).</summary>
    public void SkipField(uint tag)
    {
        switch ((int)(tag & 7))
        {
            case ProtoWire.WireVarint:
                ReadVarint64();
                break;
            case ProtoWire.WireFixed64:
                Skip(8);
                break;
            case ProtoWire.WireLengthDelimited:
                Skip(ReadLength());
                break;
            case ProtoWire.WireFixed32:
                Skip(4);
                break;
            case ProtoWire.WireStartGroup:
                SkipGroup(tag >> 3);
                break;
            default:
                throw new ProtoParseException("Protocol message contained an invalid wire type.");
        }
    }

    private void SkipGroup(uint fieldNumber)
    {
        if (++_groupDepth > MaxGroupDepth) throw new ProtoParseException("Protocol message had too many levels of nesting.");
        while (true)
        {
            if (!TryReadTag(out uint tag)) ThrowTruncated();
            if ((tag & 7) == ProtoWire.WireEndGroup)
            {
                if (tag >> 3 != fieldNumber) throw new ProtoParseException("Protocol message end-group tag did not match the start-group tag.");
                break;
            }
            SkipField(tag);
        }
        _groupDepth--;
    }
}

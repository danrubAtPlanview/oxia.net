// =====================================================================================================================
// PORT NOTE
//   Upstream : common/proto/client_vtproto.pb.go (the marshalling primitives: protohelpers.EncodeVarint / SizeOfVarint)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Forward-writing protobuf encoder primitives over an IBufferWriter<byte> (or a fixed Span<byte>).  A message
//              is encoded in two passes: Size() (allocation free, memoises the size of nested messages) then WriteTo(),
//              which writes the fields in ascending field-number order straight into the transport's buffer.
//   Divergences: vtproto marshals BACKWARDS into one pre-sized slice (MarshalToSizedBufferVT).  Here the output is an
//              IBufferWriter<byte> that may hand out several segments (large values), so the encoder goes forward and
//              relies on the memoised nested sizes for the length prefixes.  Strings are transcoded UTF-16 -> UTF-8
//              directly into the destination (length prefix back-filled), never into an intermediate byte[].
// =====================================================================================================================
using System.Buffers;
using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Oxia.Common.Proto.Codec;

/// <summary>
/// Protobuf wire-format constants and size helpers shared by the codecs.
/// </summary>
public static class ProtoWire
{
    public const int WireVarint = 0;
    public const int WireFixed64 = 1;
    public const int WireLengthDelimited = 2;
    public const int WireStartGroup = 3;
    public const int WireEndGroup = 4;
    public const int WireFixed32 = 5;

    /// <summary>Number of bytes of the varint encoding of <paramref name="value"/> (1..10).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int SizeOfVarint(ulong value) => ((63 - BitOperations.LeadingZeroCount(value | 1)) * 9 + 73) >> 6;

    /// <summary>Varint size of a (non-negative) length.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int SizeOfLength(int length) => SizeOfVarint((uint)length);

    /// <summary>Size of a length-delimited field with a one-byte tag whose payload has <paramref name="length"/> bytes.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int SizeOfLengthDelimited(int length) => 1 + SizeOfVarint((uint)length) + length;

    /// <summary>Size of a string field (one-byte tag, length, UTF-8 payload).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int SizeOfString(string value) => SizeOfLengthDelimited(Encoding.UTF8.GetByteCount(value));

    /// <summary>Size of a nested-message field whose body is <paramref name="bodySize"/> bytes.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int SizeOfMessage(int bodySize) => SizeOfLengthDelimited(bodySize);
}

/// <summary>
/// Forward protobuf writer.  Create it over an <see cref="IBufferWriter{T}"/> (<c>new ProtoWriter(output, size)</c>) and call
/// <see cref="Flush"/> once at the end, or over a fixed span (<c>new ProtoWriter(span)</c>, throws when too small).
/// All writers take the one-byte tag (every field number of client.proto is below 16) and emit it first.
/// </summary>
public ref struct ProtoWriter
{
    /// <summary>Largest GetSpan() hint issued for the remainder of a message (big values are written in chunks).</summary>
    private const int MaxChunk = 64 * 1024;

    private readonly IBufferWriter<byte>? _output;
    private Span<byte> _span;
    private int _pos;
    private int _remaining; // estimated bytes still to be written (from the size hint), only used to size GetSpan requests
    private long _flushed;

    /// <param name="output">destination; GetSpan/Advance are called once per ~64 KiB (once for a small message).</param>
    /// <param name="sizeHint">total number of bytes that will be written (the value of Size()), 0 if unknown.</param>
    public ProtoWriter(IBufferWriter<byte> output, int sizeHint = 0)
    {
        _output = output;
        _span = default;
        _pos = 0;
        _remaining = sizeHint;
        _flushed = 0;
    }

    /// <summary>Writes into a fixed buffer; <see cref="InvalidOperationException"/> if it is too small.</summary>
    public ProtoWriter(Span<byte> destination)
    {
        _output = null;
        _span = destination;
        _pos = 0;
        _remaining = 0;
        _flushed = 0;
    }

    /// <summary>Total number of bytes written so far (including those already handed to the output).</summary>
    public readonly long BytesWritten => _flushed + _pos;

    /// <summary>Hands the pending bytes to the output (Advance).  Must be called when done with an IBufferWriter-backed writer.</summary>
    public void Flush()
    {
        if (_output is not null && _pos > 0)
        {
            _output.Advance(_pos);
            _flushed += _pos;
            _remaining -= _pos;
            _pos = 0;
            _span = default;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Ensure(int count)
    {
        if (_span.Length - _pos < count) EnsureSlow(count);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void EnsureSlow(int count)
    {
        if (_output is null) throw new InvalidOperationException("The destination buffer is too small for the message.");
        Flush();
        int hint = Math.Max(count, Math.Min(_remaining, MaxChunk));
        _span = _output.GetSpan(hint);
        if (_span.Length < count) throw new InvalidOperationException("IBufferWriter<byte>.GetSpan returned fewer bytes than requested.");
    }

    // ---- varints ---------------------------------------------------------------------------------------------------

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int PutVarint(ref byte p, ulong value)
    {
        if (value < 0x80)
        {
            p = (byte)value;
            return 1;
        }
        int i = 0;
        while (value >= 0x80)
        {
            Unsafe.Add(ref p, i++) = (byte)(value | 0x80);
            value >>= 7;
        }
        Unsafe.Add(ref p, i) = (byte)value;
        return i + 1;
    }

    /// <summary>Writes a one-byte tag followed by a varint.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteTagVarint(byte tag, ulong value)
    {
        if (_span.Length - _pos >= 11)
        {
            ref byte p = ref Unsafe.Add(ref MemoryMarshal.GetReference(_span), _pos);
            p = tag;
            _pos += 1 + PutVarint(ref Unsafe.Add(ref p, 1), value);
        }
        else
        {
            WriteTagVarintSlow(tag, value);
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void WriteTagVarintSlow(byte tag, ulong value)
    {
        int n = ProtoWire.SizeOfVarint(value);
        Ensure(1 + n);
        ref byte p = ref Unsafe.Add(ref MemoryMarshal.GetReference(_span), _pos);
        p = tag;
        PutVarint(ref Unsafe.Add(ref p, 1), value);
        _pos += 1 + n;
    }

    /// <summary>Writes a varint without a tag (packed elements, lengths).</summary>
    public void WriteVarint(ulong value)
    {
        if (_span.Length - _pos >= 10)
        {
            _pos += PutVarint(ref Unsafe.Add(ref MemoryMarshal.GetReference(_span), _pos), value);
        }
        else
        {
            int n = ProtoWire.SizeOfVarint(value);
            Ensure(n);
            PutVarint(ref Unsafe.Add(ref MemoryMarshal.GetReference(_span), _pos), value);
            _pos += n;
        }
    }

    /// <summary>Writes tag + length prefix of a length-delimited field (string, bytes, nested message, packed block).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteTagLength(byte tag, int length) => WriteTagVarint(tag, (uint)length);

    // ---- fixed width -----------------------------------------------------------------------------------------------

    public void WriteTagFixed32(byte tag, uint value)
    {
        Ensure(5);
        ref byte p = ref Unsafe.Add(ref MemoryMarshal.GetReference(_span), _pos);
        p = tag;
        Unsafe.WriteUnaligned(ref Unsafe.Add(ref p, 1), BitConverter.IsLittleEndian ? value : BinaryPrimitives.ReverseEndianness(value));
        _pos += 5;
    }

    public void WriteTagFixed64(byte tag, ulong value)
    {
        Ensure(9);
        ref byte p = ref Unsafe.Add(ref MemoryMarshal.GetReference(_span), _pos);
        p = tag;
        Unsafe.WriteUnaligned(ref Unsafe.Add(ref p, 1), BitConverter.IsLittleEndian ? value : BinaryPrimitives.ReverseEndianness(value));
        _pos += 9;
    }

    // ---- length-delimited ------------------------------------------------------------------------------------------

    /// <summary>Raw bytes, no tag/length; copies in chunks when the destination cannot hold them contiguously.</summary>
    public void WriteRaw(ReadOnlySpan<byte> data)
    {
        if (data.Length <= _span.Length - _pos)
        {
            data.CopyTo(_span.Slice(_pos));
            _pos += data.Length;
            return;
        }
        WriteRawSlow(data);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void WriteRawSlow(ReadOnlySpan<byte> data)
    {
        while (!data.IsEmpty)
        {
            int avail = _span.Length - _pos;
            if (avail == 0)
            {
                EnsureSlow(Math.Min(data.Length, MaxChunk));
                avail = _span.Length - _pos;
            }
            int n = Math.Min(avail, data.Length);
            data.Slice(0, n).CopyTo(_span.Slice(_pos));
            _pos += n;
            data = data.Slice(n);
        }
    }

    /// <summary>tag + length + bytes.</summary>
    public void WriteBytes(byte tag, ReadOnlySpan<byte> value)
    {
        WriteTagLength(tag, value.Length);
        WriteRaw(value);
    }

    /// <summary>
    /// tag + length + UTF-8 bytes of <paramref name="value"/>.  Short strings are transcoded in one pass straight into the
    /// destination (the length prefix is back-filled); long ones take the exact-size path.  Lone surrogates become U+FFFD.
    /// </summary>
    public void WriteString(byte tag, string value)
    {
        int chars = value.Length;
        if (chars <= 5461)
        {
            int lenBytes = chars <= 42 ? 1 : 2;
            if (_span.Length - _pos >= 1 + lenBytes + chars * 3)
            {
                Span<byte> dst = _span.Slice(_pos);
                int n = Encoding.UTF8.GetBytes(value, dst.Slice(1 + lenBytes));
                dst[0] = tag;
                if (lenBytes == 1)
                {
                    dst[1] = (byte)n;
                    _pos += 2 + n;
                }
                else if (n < 128)
                {
                    dst.Slice(3, n).CopyTo(dst.Slice(2));
                    dst[1] = (byte)n;
                    _pos += 2 + n;
                }
                else
                {
                    dst[1] = (byte)(n | 0x80);
                    dst[2] = (byte)(n >> 7);
                    _pos += 3 + n;
                }
                return;
            }
        }
        WriteStringExact(tag, value);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void WriteStringExact(byte tag, string value)
    {
        int n = Encoding.UTF8.GetByteCount(value);
        WriteTagLength(tag, n);
        if (n == 0) return;
        Ensure(n);
        Encoding.UTF8.GetBytes(value, _span.Slice(_pos));
        _pos += n;
    }

    /// <summary>Packed repeated uint64: tag + byte length + varints.</summary>
    public void WritePackedVarints(byte tag, ReadOnlySpan<ulong> values)
    {
        int bytes = 0;
        foreach (ulong v in values) bytes += ProtoWire.SizeOfVarint(v);
        WriteTagLength(tag, bytes);
        foreach (ulong v in values) WriteVarint(v);
    }
}

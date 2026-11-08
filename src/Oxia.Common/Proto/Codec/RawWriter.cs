// =====================================================================================================================
// PORT NOTE
//   Upstream : common/proto/client_vtproto.pb.go (the straight-line MarshalToSizedBufferVT bodies of the hot request messages)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Straight-line, register-friendly encoding of a message whose exact size is known (the memoised CachedSize):
//              the codec reserves `size` contiguous bytes once (ProtoWriter.Reserve) and writes the fields with a local
//              position into a Span<byte>, instead of going through the ProtoWriter's capacity checks field by field.
//              Utf8Fast adds an ASCII fast path (Ascii.IsValid + vector narrowing) in front of Encoding.UTF8, which dominates
//              the cost of a put (its key) otherwise.
//   Divergences: Go marshals backwards; this writes forwards.  All helpers are bounds-checked (Span indexing) so a message
//              that was modified between Size() and WriteTo() cannot corrupt memory: it throws instead.
// =====================================================================================================================
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Text;

namespace Oxia.Common.Proto.Codec;

/// <summary>UTF-16 &lt;-&gt; UTF-8 helpers with an ASCII fast path.</summary>
internal static class Utf8Fast
{
    /// <summary>Number of bytes of the UTF-8 encoding of <paramref name="s"/> (lone surrogates count as U+FFFD, 3 bytes).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int GetByteCount(string s) => System.Text.Ascii.IsValid(s) ? s.Length : Encoding.UTF8.GetByteCount(s);

    /// <summary>Encodes <paramref name="s"/> into <paramref name="destination"/> (which must be large enough); returns the byte count.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int GetBytes(string s, Span<byte> destination)
    {
        if (s.Length <= destination.Length && System.Text.Ascii.IsValid(s))
        {
            NarrowAscii(s, destination);
            return s.Length;
        }
        return Encoding.UTF8.GetBytes(s, destination);
    }

    /// <summary>Copies ASCII-only UTF-16 to bytes; <paramref name="destination"/> must have at least src.Length bytes.</summary>
    public static void NarrowAscii(ReadOnlySpan<char> src, Span<byte> destination)
    {
        int n = src.Length;
        ref ushort s = ref Unsafe.As<char, ushort>(ref MemoryMarshal.GetReference(src));
        ref byte d = ref MemoryMarshal.GetReference(destination);
        if (n >= 16)
        {
            int last = n - 16;
            int i = 0;
            for (; i < last; i += 16)
            {
                Vector128<ushort> v0 = Vector128.LoadUnsafe(ref s, (nuint)i);
                Vector128<ushort> v1 = Vector128.LoadUnsafe(ref s, (nuint)(i + 8));
                Vector128.Narrow(v0, v1).StoreUnsafe(ref d, (nuint)i);
            }
            // final block, overlapping the previous one when n is not a multiple of 16
            Vector128<ushort> w0 = Vector128.LoadUnsafe(ref s, (nuint)last);
            Vector128<ushort> w1 = Vector128.LoadUnsafe(ref s, (nuint)(last + 8));
            Vector128.Narrow(w0, w1).StoreUnsafe(ref d, (nuint)last);
        }
        else if (n >= 8)
        {
            Vector128<ushort> a = Vector128.LoadUnsafe(ref s, 0);
            Vector128<ushort> b = Vector128.LoadUnsafe(ref s, (nuint)(n - 8));
            Unsafe.WriteUnaligned(ref d, Vector128.Narrow(a, a).AsUInt64().ToScalar());
            Unsafe.WriteUnaligned(ref Unsafe.Add(ref d, n - 8), Vector128.Narrow(b, b).AsUInt64().ToScalar());
        }
        else
        {
            for (int i = 0; i < n; i++) Unsafe.Add(ref d, i) = (byte)Unsafe.Add(ref s, i);
        }
    }
}

/// <summary>Bounds-checked forward writers over a span with an explicit position (each returns the new position).</summary>
internal static class RawWriter
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int Varint(Span<byte> d, int p, ulong v)
    {
        while (v >= 0x80)
        {
            d[p++] = (byte)(v | 0x80);
            v >>= 7;
        }
        d[p++] = (byte)v;
        return p;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int TagVarint(Span<byte> d, int p, byte tag, ulong v)
    {
        d[p++] = tag;
        return Varint(d, p, v);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int TagFixed32(Span<byte> d, int p, byte tag, uint v)
    {
        d[p] = tag;
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(d.Slice(p + 1), v);
        return p + 5;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int TagFixed64(Span<byte> d, int p, byte tag, ulong v)
    {
        d[p] = tag;
        System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(d.Slice(p + 1), v);
        return p + 9;
    }

    /// <summary>tag, UTF-8 length, UTF-8 bytes.</summary>
    public static int String(Span<byte> d, int p, byte tag, string s)
    {
        d[p++] = tag;
        if (s.Length <= 42)
        {
            // at most 126 bytes: a one-byte length, so the content can be encoded first and the length back-filled
            int n = Utf8Fast.GetBytes(s, d.Slice(p + 1));
            d[p] = (byte)n;
            return p + 1 + n;
        }
        int len = Utf8Fast.GetByteCount(s);
        p = Varint(d, p, (uint)len);
        Utf8Fast.GetBytes(s, d.Slice(p, len));
        return p + len;
    }

    /// <summary>tag, length, bytes.</summary>
    public static int Bytes(Span<byte> d, int p, byte tag, ReadOnlySpan<byte> v)
    {
        d[p++] = tag;
        p = Varint(d, p, (uint)v.Length);
        v.CopyTo(d.Slice(p, v.Length));
        return p + v.Length;
    }
}

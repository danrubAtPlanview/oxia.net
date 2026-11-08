// =====================================================================================================================
// PORT NOTE
//   Upstream : common/proto/client_vtproto.pb.go (SizeVT / MarshalVT / MarshalToSizedBufferVT / UnmarshalVT)
//              common/proto/marshal.go (MarshalToBuffer: size first, then marshal into a reusable buffer)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The contract every message codec (Oxia.Common.Proto.Codec.XxxCodec) implements, plus the generic façade
//              (Write / Parse / ToArray) the gRPC marshallers and the tests use.  Every codec offers the same four calls:
//                 int  Size(msg)                               one allocation-free pass; memoises nested sizes in the messages
//                 void Write(msg, IBufferWriter<byte>)         Size + encode, byte-identical to protoc/vtproto output
//                 void Parse(ReadOnlySpan<byte>, msg)          decode INTO msg (msg is Reset() first, so pooled instances work)
//                 void Parse(in ReadOnlySequence<byte>, msg)   same, input may be split in any number of segments
//              Truncated / malformed input throws ProtoParseException; unknown fields are skipped.
//   Divergences: Go returns (n, error) and unmarshals into a fresh message; C# codecs are statics (generic static
//              abstract members, so the gRPC marshallers are fully specialised per message) and parse into a caller-supplied
//              instance.  Nested lengths come from the CachedSize memo that Size() stores: always call Size(msg) (or the
//              façade Write, which does) right before WriteTo()/WriteSized() with no mutation in between.
// =====================================================================================================================
using System.Buffers;

namespace Oxia.Common.Proto.Codec;

/// <summary>Codec of one protobuf message type; implemented by stateless structs so calls through generics are devirtualised.</summary>
public interface IMessageCodec<TMessage> where TMessage : class
{
    static abstract TMessage Create();

    /// <summary>Resets the message so that it can be parsed into again (recycles response-side children).</summary>
    static abstract void Reset(TMessage message);

    /// <summary>Encoded size in bytes.  Also memoises the size of every nested message (read back by <see cref="WriteTo"/>).</summary>
    static abstract int Size(TMessage message);

    /// <summary>Writes the fields (no length prefix).  Requires <see cref="Size"/> to have been called on the same, unmodified message.</summary>
    static abstract void WriteTo(ref ProtoWriter writer, TMessage message);

    /// <summary>Reads fields until the end of the reader's current message and merges them into <paramref name="message"/>.</summary>
    static abstract void MergeFrom(ref ProtoReader reader, TMessage message);
}

/// <summary>Generic entry points over <see cref="IMessageCodec{TMessage}"/> (the per-message codec classes forward to these).</summary>
public static class MessageCodec
{
    /// <summary><c>Size</c> then encode into <paramref name="output"/>.</summary>
    public static void Write<TCodec, TMessage>(TMessage message, IBufferWriter<byte> output)
        where TCodec : IMessageCodec<TMessage> where TMessage : class
    {
        int size = TCodec.Size(message);
        WriteSized<TCodec, TMessage>(message, output, size);
    }

    /// <summary>
    /// Encode into <paramref name="output"/>; <paramref name="size"/> must be the value <c>TCodec.Size(message)</c> returned just before
    /// (no mutation in between).  This is the gRPC serializer's path: it needs the size for SetPayloadLength anyway.
    /// </summary>
    public static void WriteSized<TCodec, TMessage>(TMessage message, IBufferWriter<byte> output, int size)
        where TCodec : IMessageCodec<TMessage> where TMessage : class
    {
        var writer = new ProtoWriter(output, size);
        TCodec.WriteTo(ref writer, message);
        writer.Flush();
    }

    /// <summary>Encodes into a new exactly-sized array (tests, diagnostics; not for hot paths).</summary>
    public static byte[] ToArray<TCodec, TMessage>(TMessage message)
        where TCodec : IMessageCodec<TMessage> where TMessage : class
    {
        int size = TCodec.Size(message);
        var result = new byte[size];
        var writer = new ProtoWriter(result);
        TCodec.WriteTo(ref writer, message);
        if (writer.BytesWritten != size)
        {
            throw new InvalidOperationException($"Codec wrote {writer.BytesWritten} bytes but Size() promised {size}.");
        }
        return result;
    }

    /// <summary>Resets <paramref name="message"/> and parses a contiguous buffer into it.</summary>
    public static void Parse<TCodec, TMessage>(ReadOnlySpan<byte> data, TMessage message)
        where TCodec : IMessageCodec<TMessage> where TMessage : class
    {
        TCodec.Reset(message);
        var reader = new ProtoReader(data);
        TCodec.MergeFrom(ref reader, message);
    }

    /// <summary>Resets <paramref name="message"/> and parses a (possibly multi-segment) sequence into it.</summary>
    public static void Parse<TCodec, TMessage>(in ReadOnlySequence<byte> data, TMessage message)
        where TCodec : IMessageCodec<TMessage> where TMessage : class
    {
        TCodec.Reset(message);
        var reader = new ProtoReader(in data);
        TCodec.MergeFrom(ref reader, message);
    }
}

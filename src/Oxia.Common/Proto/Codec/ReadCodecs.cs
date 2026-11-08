// =====================================================================================================================
// PORT NOTE
//   Upstream : common/proto/client_vtproto.pb.go  (ReadRequest, ReadResponse, GetRequest, GetResponse)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Wire codecs of the read path: batched gets and their responses.  GetResponse is also the record type of
//              RangeScanResponse.
//   Divergences: see MessageCodec.cs.  GetResponse.value is `optional bytes`: a null array is "absent", an empty array is
//              emitted as a zero-length field.  Parsing allocates exactly one array of the value's size, only when the field
//              is present.  Response-side parsing recycles children through Spares.
// =====================================================================================================================
using System.Buffers;
using System.Runtime.InteropServices;
using static Oxia.Common.Proto.Codec.ProtoWire;

namespace Oxia.Common.Proto.Codec;

public readonly struct GetRequestCodec : IMessageCodec<GetRequest>
{
    public static GetRequest Create() => new();
    public static void Reset(GetRequest m) => m.Reset();

    public static int Size(GetRequest m)
    {
        int n = 0;
        if (!string.IsNullOrEmpty(m.Key)) n += SizeOfString(m.Key);
        if (m.IncludeValue) n += 2;
        if (m.ComparisonType != 0) n += 1 + SizeOfVarint((ulong)(long)(int)m.ComparisonType);
        if (m.SecondaryIndexName is { } sn) n += SizeOfString(sn);
        m.CachedSize = n;
        return n;
    }

    public static void WriteTo(ref ProtoWriter w, GetRequest m)
    {
        if (!string.IsNullOrEmpty(m.Key)) w.WriteString(10, m.Key);
        if (m.IncludeValue) w.WriteTagVarint(16, 1);
        if (m.ComparisonType != 0) w.WriteTagVarint(24, (ulong)(long)(int)m.ComparisonType);
        if (m.SecondaryIndexName is { } sn) w.WriteString(34, sn);
    }

    public static void MergeFrom(ref ProtoReader r, GetRequest m)
    {
        while (r.TryReadTag(out uint tag))
        {
            switch (tag)
            {
                case 10: m.Key = r.ReadString(); break;
                case 16: m.IncludeValue = r.ReadBool(); break;
                case 24: m.ComparisonType = (KeyComparisonType)r.ReadInt32(); break;
                case 34: m.SecondaryIndexName = r.ReadString(); break;
                default: r.SkipField(tag); break;
            }
        }
    }

    public static void Write(GetRequest m, IBufferWriter<byte> output) => MessageCodec.Write<GetRequestCodec, GetRequest>(m, output);
    public static void Parse(ReadOnlySpan<byte> data, GetRequest m) => MessageCodec.Parse<GetRequestCodec, GetRequest>(data, m);
    public static void Parse(in ReadOnlySequence<byte> data, GetRequest m) => MessageCodec.Parse<GetRequestCodec, GetRequest>(in data, m);
}

public readonly struct GetResponseCodec : IMessageCodec<GetResponse>
{
    public static GetResponse Create() => new();
    public static void Reset(GetResponse m) => m.Reset();

    public static int Size(GetResponse m)
    {
        int n = 0;
        if (m.Status != 0) n += 1 + SizeOfVarint((ulong)(long)(int)m.Status);
        if (m.Version is { } v) n += SizeOfMessage(ProtoVersionCodec.Size(v));
        if (m.Value is { } value) n = checked(n + SizeOfLengthDelimited(value.Length));
        if (m.Key is { } k) n += SizeOfString(k);
        if (m.SecondaryIndexKey is { } sk) n += SizeOfString(sk);
        m.CachedSize = n;
        return n;
    }

    public static void WriteTo(ref ProtoWriter w, GetResponse m)
    {
        if (m.Status != 0) w.WriteTagVarint(8, (ulong)(long)(int)m.Status);
        if (m.Version is { } v)
        {
            w.WriteTagLength(18, v.CachedSize);
            ProtoVersionCodec.WriteTo(ref w, v);
        }
        if (m.Value is { } value) w.WriteBytes(26, value);
        if (m.Key is { } k) w.WriteString(34, k);
        if (m.SecondaryIndexKey is { } sk) w.WriteString(42, sk);
    }

    public static void MergeFrom(ref ProtoReader r, GetResponse m)
    {
        while (r.TryReadTag(out uint tag))
        {
            switch (tag)
            {
                case 8: m.Status = (Status)r.ReadInt32(); break;
                case 18:
                {
                    ProtoVersion? v = m.Version;
                    if (v is null)
                    {
                        v = m.SpareVersion ?? new ProtoVersion();
                        m.SpareVersion = null;
                        m.Version = v;
                    }
                    long old = r.BeginMessage();
                    ProtoVersionCodec.MergeFrom(ref r, v);
                    r.EndMessage(old);
                    break;
                }
                case 26: m.Value = r.ReadBytes(); break;
                case 34: m.Key = r.ReadString(); break;
                case 42: m.SecondaryIndexKey = r.ReadString(); break;
                default: r.SkipField(tag); break;
            }
        }
    }

    public static void Write(GetResponse m, IBufferWriter<byte> output) => MessageCodec.Write<GetResponseCodec, GetResponse>(m, output);
    public static void Parse(ReadOnlySpan<byte> data, GetResponse m) => MessageCodec.Parse<GetResponseCodec, GetResponse>(data, m);
    public static void Parse(in ReadOnlySequence<byte> data, GetResponse m) => MessageCodec.Parse<GetResponseCodec, GetResponse>(in data, m);
}

public readonly struct ReadRequestCodec : IMessageCodec<ReadRequest>
{
    public static ReadRequest Create() => new();
    public static void Reset(ReadRequest m) => m.Reset();

    public static int Size(ReadRequest m)
    {
        int n = 0;
        if (m.Shard is { } shard) n += 1 + SizeOfVarint((ulong)shard);
        checked
        {
            foreach (GetRequest g in CollectionsMarshal.AsSpan(m.Gets)) n += SizeOfMessage(GetRequestCodec.Size(g));
        }
        return n;
    }

    public static void WriteTo(ref ProtoWriter w, ReadRequest m)
    {
        if (m.Shard is { } shard) w.WriteTagVarint(8, (ulong)shard);
        foreach (GetRequest g in CollectionsMarshal.AsSpan(m.Gets))
        {
            w.WriteTagLength(18, g.CachedSize);
            GetRequestCodec.WriteTo(ref w, g);
        }
    }

    public static void MergeFrom(ref ProtoReader r, ReadRequest m)
    {
        while (r.TryReadTag(out uint tag))
        {
            switch (tag)
            {
                case 8: m.Shard = r.ReadInt64(); break;
                case 18:
                {
                    var g = new GetRequest();
                    long old = r.BeginMessage();
                    GetRequestCodec.MergeFrom(ref r, g);
                    r.EndMessage(old);
                    m.Gets.Add(g);
                    break;
                }
                default: r.SkipField(tag); break;
            }
        }
    }

    public static void Write(ReadRequest m, IBufferWriter<byte> output) => MessageCodec.Write<ReadRequestCodec, ReadRequest>(m, output);
    public static void Parse(ReadOnlySpan<byte> data, ReadRequest m) => MessageCodec.Parse<ReadRequestCodec, ReadRequest>(data, m);
    public static void Parse(in ReadOnlySequence<byte> data, ReadRequest m) => MessageCodec.Parse<ReadRequestCodec, ReadRequest>(in data, m);
}

public readonly struct ReadResponseCodec : IMessageCodec<ReadResponse>
{
    public static ReadResponse Create() => new();
    public static void Reset(ReadResponse m) => m.Reset();

    public static int Size(ReadResponse m)
    {
        int n = 0;
        checked
        {
            foreach (GetResponse g in CollectionsMarshal.AsSpan(m.Gets)) n += SizeOfMessage(GetResponseCodec.Size(g));
        }
        return n;
    }

    public static void WriteTo(ref ProtoWriter w, ReadResponse m)
    {
        foreach (GetResponse g in CollectionsMarshal.AsSpan(m.Gets))
        {
            w.WriteTagLength(10, g.CachedSize);
            GetResponseCodec.WriteTo(ref w, g);
        }
    }

    public static void MergeFrom(ref ProtoReader r, ReadResponse m)
    {
        while (r.TryReadTag(out uint tag))
        {
            switch (tag)
            {
                case 10:
                {
                    GetResponse g = m.SpareGets.TryPop() ?? new GetResponse();
                    long old = r.BeginMessage();
                    GetResponseCodec.MergeFrom(ref r, g);
                    r.EndMessage(old);
                    m.Gets.Add(g);
                    break;
                }
                default: r.SkipField(tag); break;
            }
        }
    }

    public static void Write(ReadResponse m, IBufferWriter<byte> output) => MessageCodec.Write<ReadResponseCodec, ReadResponse>(m, output);
    public static void Parse(ReadOnlySpan<byte> data, ReadResponse m) => MessageCodec.Parse<ReadResponseCodec, ReadResponse>(data, m);
    public static void Parse(in ReadOnlySequence<byte> data, ReadResponse m) => MessageCodec.Parse<ReadResponseCodec, ReadResponse>(in data, m);
}

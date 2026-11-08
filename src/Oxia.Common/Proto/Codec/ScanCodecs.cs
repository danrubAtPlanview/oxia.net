// =====================================================================================================================
// PORT NOTE
//   Upstream : common/proto/client_vtproto.pb.go  (ListRequest, ListResponse, RangeScanRequest, RangeScanResponse)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Wire codecs of the key-range operations: the (deprecated) List stream and RangeScan.
//   Divergences: see MessageCodec.cs.  `optional` fields (shard, secondary_index_name, include_values) are emitted whenever
//              non-null, even when equal to the default.  RangeScanResponse records are GetResponse messages and are
//              recycled through Spares on Reset().
// =====================================================================================================================
using System.Buffers;
using System.Runtime.InteropServices;
using static Oxia.Common.Proto.Codec.ProtoWire;

namespace Oxia.Common.Proto.Codec;

public readonly struct ListRequestCodec : IMessageCodec<ListRequest>
{
    public static ListRequest Create() => new();
    public static void Reset(ListRequest m) => m.Reset();

    public static int Size(ListRequest m)
    {
        int n = 0;
        if (m.Shard is { } shard) n += 1 + SizeOfVarint((ulong)shard);
        if (!string.IsNullOrEmpty(m.StartInclusive)) n += SizeOfString(m.StartInclusive);
        if (!string.IsNullOrEmpty(m.EndExclusive)) n += SizeOfString(m.EndExclusive);
        if (m.SecondaryIndexName is { } sn) n += SizeOfString(sn);
        if (m.IncludeInternalKeys) n += 2;
        if (m.EndInclusive) n += 2;
        return n;
    }

    public static void WriteTo(ref ProtoWriter w, ListRequest m)
    {
        if (m.Shard is { } shard) w.WriteTagVarint(8, (ulong)shard);
        if (!string.IsNullOrEmpty(m.StartInclusive)) w.WriteString(18, m.StartInclusive);
        if (!string.IsNullOrEmpty(m.EndExclusive)) w.WriteString(26, m.EndExclusive);
        if (m.SecondaryIndexName is { } sn) w.WriteString(34, sn);
        if (m.IncludeInternalKeys) w.WriteTagVarint(40, 1);
        if (m.EndInclusive) w.WriteTagVarint(48, 1);
    }

    public static void MergeFrom(ref ProtoReader r, ListRequest m)
    {
        while (r.TryReadTag(out uint tag))
        {
            switch (tag)
            {
                case 8: m.Shard = r.ReadInt64(); break;
                case 18: m.StartInclusive = r.ReadString(); break;
                case 26: m.EndExclusive = r.ReadString(); break;
                case 34: m.SecondaryIndexName = r.ReadString(); break;
                case 40: m.IncludeInternalKeys = r.ReadBool(); break;
                case 48: m.EndInclusive = r.ReadBool(); break;
                default: r.SkipField(tag); break;
            }
        }
    }

    public static void Write(ListRequest m, IBufferWriter<byte> output) => MessageCodec.Write<ListRequestCodec, ListRequest>(m, output);
    public static void Parse(ReadOnlySpan<byte> data, ListRequest m) => MessageCodec.Parse<ListRequestCodec, ListRequest>(data, m);
    public static void Parse(in ReadOnlySequence<byte> data, ListRequest m) => MessageCodec.Parse<ListRequestCodec, ListRequest>(in data, m);
}

public readonly struct ListResponseCodec : IPooledMessageCodec<ListResponse>
{
    public static ListResponse Create() => new();
    public static ListResponse Rent() => ListResponse.Rent();
    public static void Reset(ListResponse m) => m.Reset();

    public static int Size(ListResponse m)
    {
        int n = 0;
        checked
        {
            foreach (string k in CollectionsMarshal.AsSpan(m.Keys)) n += SizeOfString(k);
        }
        if (m.EndInclusive) n += 2;
        return n;
    }

    public static void WriteTo(ref ProtoWriter w, ListResponse m)
    {
        foreach (string k in CollectionsMarshal.AsSpan(m.Keys)) w.WriteString(10, k);
        if (m.EndInclusive) w.WriteTagVarint(16, 1);
    }

    public static void MergeFrom(ref ProtoReader r, ListResponse m)
    {
        while (r.TryReadTag(out uint tag))
        {
            switch (tag)
            {
                case 10: m.Keys.Add(r.ReadString()); break;
                case 16: m.EndInclusive = r.ReadBool(); break;
                default: r.SkipField(tag); break;
            }
        }
    }

    public static void Write(ListResponse m, IBufferWriter<byte> output) => MessageCodec.Write<ListResponseCodec, ListResponse>(m, output);
    public static void Parse(ReadOnlySpan<byte> data, ListResponse m) => MessageCodec.Parse<ListResponseCodec, ListResponse>(data, m);
    public static void Parse(in ReadOnlySequence<byte> data, ListResponse m) => MessageCodec.Parse<ListResponseCodec, ListResponse>(in data, m);
}

public readonly struct RangeScanRequestCodec : IMessageCodec<RangeScanRequest>
{
    public static RangeScanRequest Create() => new();
    public static void Reset(RangeScanRequest m) => m.Reset();

    public static int Size(RangeScanRequest m)
    {
        int n = 0;
        if (m.Shard is { } shard) n += 1 + SizeOfVarint((ulong)shard);
        if (!string.IsNullOrEmpty(m.StartInclusive)) n += SizeOfString(m.StartInclusive);
        if (!string.IsNullOrEmpty(m.EndExclusive)) n += SizeOfString(m.EndExclusive);
        if (m.SecondaryIndexName is { } sn) n += SizeOfString(sn);
        if (m.IncludeInternalKeys) n += 2;
        if (m.EndInclusive) n += 2;
        if (m.IncludeValues.HasValue) n += 2;
        return n;
    }

    public static void WriteTo(ref ProtoWriter w, RangeScanRequest m)
    {
        if (m.Shard is { } shard) w.WriteTagVarint(8, (ulong)shard);
        if (!string.IsNullOrEmpty(m.StartInclusive)) w.WriteString(18, m.StartInclusive);
        if (!string.IsNullOrEmpty(m.EndExclusive)) w.WriteString(26, m.EndExclusive);
        if (m.SecondaryIndexName is { } sn) w.WriteString(34, sn);
        if (m.IncludeInternalKeys) w.WriteTagVarint(40, 1);
        if (m.EndInclusive) w.WriteTagVarint(48, 1);
        if (m.IncludeValues is { } iv) w.WriteTagVarint(56, iv ? 1UL : 0UL);
    }

    public static void MergeFrom(ref ProtoReader r, RangeScanRequest m)
    {
        while (r.TryReadTag(out uint tag))
        {
            switch (tag)
            {
                case 8: m.Shard = r.ReadInt64(); break;
                case 18: m.StartInclusive = r.ReadString(); break;
                case 26: m.EndExclusive = r.ReadString(); break;
                case 34: m.SecondaryIndexName = r.ReadString(); break;
                case 40: m.IncludeInternalKeys = r.ReadBool(); break;
                case 48: m.EndInclusive = r.ReadBool(); break;
                case 56: m.IncludeValues = r.ReadBool(); break;
                default: r.SkipField(tag); break;
            }
        }
    }

    public static void Write(RangeScanRequest m, IBufferWriter<byte> output) => MessageCodec.Write<RangeScanRequestCodec, RangeScanRequest>(m, output);
    public static void Parse(ReadOnlySpan<byte> data, RangeScanRequest m) => MessageCodec.Parse<RangeScanRequestCodec, RangeScanRequest>(data, m);
    public static void Parse(in ReadOnlySequence<byte> data, RangeScanRequest m) => MessageCodec.Parse<RangeScanRequestCodec, RangeScanRequest>(in data, m);
}

public readonly struct RangeScanResponseCodec : IPooledMessageCodec<RangeScanResponse>
{
    public static RangeScanResponse Create() => new();
    public static RangeScanResponse Rent() => RangeScanResponse.Rent();
    public static void Reset(RangeScanResponse m) => m.Reset();

    public static int Size(RangeScanResponse m)
    {
        int n = 0;
        checked
        {
            foreach (GetResponse g in CollectionsMarshal.AsSpan(m.Records)) n += SizeOfMessage(GetResponseCodec.Size(g));
        }
        if (m.EndInclusive) n += 2;
        return n;
    }

    public static void WriteTo(ref ProtoWriter w, RangeScanResponse m)
    {
        foreach (GetResponse g in CollectionsMarshal.AsSpan(m.Records))
        {
            w.WriteTagLength(10, g.CachedSize);
            GetResponseCodec.WriteTo(ref w, g);
        }
        if (m.EndInclusive) w.WriteTagVarint(16, 1);
    }

    public static void MergeFrom(ref ProtoReader r, RangeScanResponse m)
    {
        while (r.TryReadTag(out uint tag))
        {
            switch (tag)
            {
                case 10:
                {
                    GetResponse g = m.SpareRecords.TryPop() ?? new GetResponse();
                    long old = r.BeginMessage();
                    GetResponseCodec.MergeFrom(ref r, g);
                    r.EndMessage(old);
                    m.Records.Add(g);
                    break;
                }
                case 16: m.EndInclusive = r.ReadBool(); break;
                default: r.SkipField(tag); break;
            }
        }
    }

    public static void Write(RangeScanResponse m, IBufferWriter<byte> output) => MessageCodec.Write<RangeScanResponseCodec, RangeScanResponse>(m, output);
    public static void Parse(ReadOnlySpan<byte> data, RangeScanResponse m) => MessageCodec.Parse<RangeScanResponseCodec, RangeScanResponse>(data, m);
    public static void Parse(in ReadOnlySequence<byte> data, RangeScanResponse m) => MessageCodec.Parse<RangeScanResponseCodec, RangeScanResponse>(in data, m);
}

/// <summary>
/// <see cref="RangeScanResponseCodec"/> for a scan that asked for pooled values: the records' values and keys are copied into
/// <see cref="ValueLease"/>s (owned by the consumer) instead of new arrays and strings.  Everything else is the classic codec.
/// </summary>
public readonly struct RangeScanResponsePooledCodec : IPooledMessageCodec<RangeScanResponse>
{
    public static RangeScanResponse Create() => RangeScanResponseCodec.Create();
    public static RangeScanResponse Rent() => RangeScanResponse.Rent();
    public static void Reset(RangeScanResponse m) => m.Reset();
    public static int Size(RangeScanResponse m) => RangeScanResponseCodec.Size(m);
    public static void WriteTo(ref ProtoWriter w, RangeScanResponse m) => RangeScanResponseCodec.WriteTo(ref w, m);

    public static void MergeFrom(ref ProtoReader r, RangeScanResponse m)
    {
        while (r.TryReadTag(out uint tag))
        {
            switch (tag)
            {
                case 10:
                {
                    PooledGetResponse g = m.SpareRecords.TryPop() as PooledGetResponse ?? new PooledGetResponse();
                    long old = r.BeginMessage();
                    GetResponseCodec.MergeFromPooled(ref r, g);
                    r.EndMessage(old);
                    m.Records.Add(g);
                    break;
                }
                case 16: m.EndInclusive = r.ReadBool(); break;
                default: r.SkipField(tag); break;
            }
        }
    }
}

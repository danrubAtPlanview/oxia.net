// =====================================================================================================================
// PORT NOTE
//   Upstream : common/proto/client_vtproto.pb.go  (ShardAssignmentsRequest, ShardAssignments, NamespaceShardsAssignment,
//              ShardAssignment, Int32HashRange: SizeVT / MarshalToSizedBufferVT / UnmarshalVT)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Wire codecs of the shard-assignment messages (GetShardAssignments stream; cold path, cheap anyway).
//   Divergences: see MessageCodec.cs.  Map entries are written key-then-value, always both (like vtproto), in the
//              dictionary's enumeration order.  Cold path: these messages are not recycled on Reset().
// =====================================================================================================================
using System.Buffers;
using System.Runtime.InteropServices;
using System.Text;
using static Oxia.Common.Proto.Codec.ProtoWire;

namespace Oxia.Common.Proto.Codec;

public readonly struct ShardAssignmentsRequestCodec : IMessageCodec<ShardAssignmentsRequest>
{
    public static ShardAssignmentsRequest Create() => new();
    public static void Reset(ShardAssignmentsRequest m) => m.Reset();

    public static int Size(ShardAssignmentsRequest m)
        => string.IsNullOrEmpty(m.Namespace) ? 0 : SizeOfString(m.Namespace);

    public static void WriteTo(ref ProtoWriter w, ShardAssignmentsRequest m)
    {
        if (!string.IsNullOrEmpty(m.Namespace)) w.WriteString(10, m.Namespace);
    }

    public static void MergeFrom(ref ProtoReader r, ShardAssignmentsRequest m)
    {
        while (r.TryReadTag(out uint tag))
        {
            switch (tag)
            {
                case 10: m.Namespace = r.ReadString(); break;
                default: r.SkipField(tag); break;
            }
        }
    }

    public static void Write(ShardAssignmentsRequest m, IBufferWriter<byte> output) => MessageCodec.Write<ShardAssignmentsRequestCodec, ShardAssignmentsRequest>(m, output);
    public static void Parse(ReadOnlySpan<byte> data, ShardAssignmentsRequest m) => MessageCodec.Parse<ShardAssignmentsRequestCodec, ShardAssignmentsRequest>(data, m);
    public static void Parse(in ReadOnlySequence<byte> data, ShardAssignmentsRequest m) => MessageCodec.Parse<ShardAssignmentsRequestCodec, ShardAssignmentsRequest>(in data, m);
}

public readonly struct Int32HashRangeCodec : IMessageCodec<Int32HashRange>
{
    public static Int32HashRange Create() => new();
    public static void Reset(Int32HashRange m) => m.Reset();

    public static int Size(Int32HashRange m)
    {
        int n = 0;
        if (m.MinHashInclusive != 0) n += 5;
        if (m.MaxHashInclusive != 0) n += 5;
        m.CachedSize = n;
        return n;
    }

    public static void WriteTo(ref ProtoWriter w, Int32HashRange m)
    {
        if (m.MinHashInclusive != 0) w.WriteTagFixed32(13, m.MinHashInclusive);
        if (m.MaxHashInclusive != 0) w.WriteTagFixed32(21, m.MaxHashInclusive);
    }

    public static void MergeFrom(ref ProtoReader r, Int32HashRange m)
    {
        while (r.TryReadTag(out uint tag))
        {
            switch (tag)
            {
                case 13: m.MinHashInclusive = r.ReadFixed32(); break;
                case 21: m.MaxHashInclusive = r.ReadFixed32(); break;
                default: r.SkipField(tag); break;
            }
        }
    }

    public static void Write(Int32HashRange m, IBufferWriter<byte> output) => MessageCodec.Write<Int32HashRangeCodec, Int32HashRange>(m, output);
    public static void Parse(ReadOnlySpan<byte> data, Int32HashRange m) => MessageCodec.Parse<Int32HashRangeCodec, Int32HashRange>(data, m);
    public static void Parse(in ReadOnlySequence<byte> data, Int32HashRange m) => MessageCodec.Parse<Int32HashRangeCodec, Int32HashRange>(in data, m);
}

public readonly struct ShardAssignmentCodec : IMessageCodec<ShardAssignment>
{
    public static ShardAssignment Create() => new();
    public static void Reset(ShardAssignment m) => m.Reset();

    public static int Size(ShardAssignment m)
    {
        int n = 0;
        if (m.Shard != 0) n += 1 + SizeOfVarint((ulong)m.Shard);
        if (!string.IsNullOrEmpty(m.Leader)) n += SizeOfString(m.Leader);
        if (m.Int32HashRange is { } hr) n += SizeOfMessage(Int32HashRangeCodec.Size(hr));
        m.CachedSize = n;
        return n;
    }

    public static void WriteTo(ref ProtoWriter w, ShardAssignment m)
    {
        if (m.Shard != 0) w.WriteTagVarint(8, (ulong)m.Shard);
        if (!string.IsNullOrEmpty(m.Leader)) w.WriteString(18, m.Leader);
        if (m.Int32HashRange is { } hr)
        {
            w.WriteTagLength(26, hr.CachedSize);
            Int32HashRangeCodec.WriteTo(ref w, hr);
        }
    }

    public static void MergeFrom(ref ProtoReader r, ShardAssignment m)
    {
        while (r.TryReadTag(out uint tag))
        {
            switch (tag)
            {
                case 8: m.Shard = r.ReadInt64(); break;
                case 18: m.Leader = r.ReadString(); break;
                case 26:
                {
                    var hr = m.Int32HashRange ??= new Int32HashRange();
                    long old = r.BeginMessage();
                    Int32HashRangeCodec.MergeFrom(ref r, hr);
                    r.EndMessage(old);
                    break;
                }
                default: r.SkipField(tag); break;
            }
        }
    }

    public static void Write(ShardAssignment m, IBufferWriter<byte> output) => MessageCodec.Write<ShardAssignmentCodec, ShardAssignment>(m, output);
    public static void Parse(ReadOnlySpan<byte> data, ShardAssignment m) => MessageCodec.Parse<ShardAssignmentCodec, ShardAssignment>(data, m);
    public static void Parse(in ReadOnlySequence<byte> data, ShardAssignment m) => MessageCodec.Parse<ShardAssignmentCodec, ShardAssignment>(in data, m);
}

public readonly struct NamespaceShardsAssignmentCodec : IMessageCodec<NamespaceShardsAssignment>
{
    public static NamespaceShardsAssignment Create() => new();
    public static void Reset(NamespaceShardsAssignment m) => m.Reset();

    public static int Size(NamespaceShardsAssignment m)
    {
        int n = 0;
        foreach (ShardAssignment a in CollectionsMarshal.AsSpan(m.Assignments)) n += SizeOfMessage(ShardAssignmentCodec.Size(a));
        if (m.ShardKeyRouter != 0) n += 1 + SizeOfVarint((ulong)(long)(int)m.ShardKeyRouter);
        if (m.KeySorting != 0) n += 1 + SizeOfVarint((ulong)(long)(int)m.KeySorting);
        m.CachedSize = n;
        return n;
    }

    public static void WriteTo(ref ProtoWriter w, NamespaceShardsAssignment m)
    {
        foreach (ShardAssignment a in CollectionsMarshal.AsSpan(m.Assignments))
        {
            w.WriteTagLength(10, a.CachedSize);
            ShardAssignmentCodec.WriteTo(ref w, a);
        }
        if (m.ShardKeyRouter != 0) w.WriteTagVarint(16, (ulong)(long)(int)m.ShardKeyRouter);
        if (m.KeySorting != 0) w.WriteTagVarint(24, (ulong)(long)(int)m.KeySorting);
    }

    public static void MergeFrom(ref ProtoReader r, NamespaceShardsAssignment m)
    {
        while (r.TryReadTag(out uint tag))
        {
            switch (tag)
            {
                case 10:
                {
                    var a = new ShardAssignment();
                    long old = r.BeginMessage();
                    ShardAssignmentCodec.MergeFrom(ref r, a);
                    r.EndMessage(old);
                    m.Assignments.Add(a);
                    break;
                }
                case 16: m.ShardKeyRouter = (ShardKeyRouter)r.ReadInt32(); break;
                case 24: m.KeySorting = (KeySorting)r.ReadInt32(); break;
                default: r.SkipField(tag); break;
            }
        }
    }

    public static void Write(NamespaceShardsAssignment m, IBufferWriter<byte> output) => MessageCodec.Write<NamespaceShardsAssignmentCodec, NamespaceShardsAssignment>(m, output);
    public static void Parse(ReadOnlySpan<byte> data, NamespaceShardsAssignment m) => MessageCodec.Parse<NamespaceShardsAssignmentCodec, NamespaceShardsAssignment>(data, m);
    public static void Parse(in ReadOnlySequence<byte> data, NamespaceShardsAssignment m) => MessageCodec.Parse<NamespaceShardsAssignmentCodec, NamespaceShardsAssignment>(in data, m);
}

public readonly struct ShardAssignmentsCodec : IMessageCodec<ShardAssignments>
{
    public static ShardAssignments Create() => new();
    public static void Reset(ShardAssignments m) => m.Reset();

    /// <summary>Body size of one map entry { string key = 1; NamespaceShardsAssignment value = 2; }.</summary>
    private static int EntrySize(string key, NamespaceShardsAssignment? value)
    {
        int vs = value is null ? 0 : NamespaceShardsAssignmentCodec.Size(value);
        return SizeOfString(key) + SizeOfMessage(vs);
    }

    public static int Size(ShardAssignments m)
    {
        int n = 0;
        foreach (KeyValuePair<string, NamespaceShardsAssignment> kv in m.Namespaces) n += SizeOfMessage(EntrySize(kv.Key, kv.Value));
        foreach (string s in CollectionsMarshal.AsSpan(m.AllowedAuthorities)) n += SizeOfString(s);
        return n;
    }

    public static void WriteTo(ref ProtoWriter w, ShardAssignments m)
    {
        foreach (KeyValuePair<string, NamespaceShardsAssignment> kv in m.Namespaces)
        {
            NamespaceShardsAssignment? v = kv.Value;
            w.WriteTagLength(10, EntrySize(kv.Key, v));
            w.WriteString(10, kv.Key);
            w.WriteTagLength(18, v?.CachedSize ?? 0);
            if (v is not null) NamespaceShardsAssignmentCodec.WriteTo(ref w, v);
        }
        foreach (string s in CollectionsMarshal.AsSpan(m.AllowedAuthorities)) w.WriteString(18, s);
    }

    public static void MergeFrom(ref ProtoReader r, ShardAssignments m)
    {
        while (r.TryReadTag(out uint tag))
        {
            switch (tag)
            {
                case 10:
                {
                    string key = string.Empty;
                    NamespaceShardsAssignment? value = null;
                    long old = r.BeginMessage();
                    while (r.TryReadTag(out uint t))
                    {
                        switch (t)
                        {
                            case 10: key = r.ReadString(); break;
                            case 18:
                            {
                                value ??= new NamespaceShardsAssignment();
                                long oldValue = r.BeginMessage();
                                NamespaceShardsAssignmentCodec.MergeFrom(ref r, value);
                                r.EndMessage(oldValue);
                                break;
                            }
                            default: r.SkipField(t); break;
                        }
                    }
                    r.EndMessage(old);
                    m.Namespaces[key] = value ?? new NamespaceShardsAssignment();
                    break;
                }
                case 18: m.AllowedAuthorities.Add(r.ReadString()); break;
                default: r.SkipField(tag); break;
            }
        }
    }

    public static void Write(ShardAssignments m, IBufferWriter<byte> output) => MessageCodec.Write<ShardAssignmentsCodec, ShardAssignments>(m, output);
    public static void Parse(ReadOnlySpan<byte> data, ShardAssignments m) => MessageCodec.Parse<ShardAssignmentsCodec, ShardAssignments>(data, m);
    public static void Parse(in ReadOnlySequence<byte> data, ShardAssignments m) => MessageCodec.Parse<ShardAssignmentsCodec, ShardAssignments>(in data, m);
}

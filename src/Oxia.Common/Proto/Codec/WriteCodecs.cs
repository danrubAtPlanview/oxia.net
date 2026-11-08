// =====================================================================================================================
// PORT NOTE
//   Upstream : common/proto/client_vtproto.pb.go  (WriteRequest, PutRequest, DeleteRequest, DeleteRangeRequest, SecondaryIndex,
//              WriteResponse, PutResponse, DeleteResponse, DeleteRangeResponse, Version)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Wire codecs of the write path (the hottest codecs of the client): WriteRequest with its puts / deletes /
//              delete-ranges, the Version metadata, and the WriteResponse the server answers with.
//   Divergences: see MessageCodec.cs.  proto3 scalars are omitted when default; `optional` fields are emitted whenever
//              non-null (even when equal to the default / empty string); sequence_key_delta is packed.  Fields are written
//              in ascending field-number order exactly like vtproto.  Response-side parsing recycles children through the
//              Spares of the message (zero allocation when a Reset() instance is parsed into again).
// =====================================================================================================================
using System.Buffers;
using System.Runtime.InteropServices;
using static Oxia.Common.Proto.Codec.ProtoWire;

namespace Oxia.Common.Proto.Codec;

public readonly struct SecondaryIndexCodec : IMessageCodec<SecondaryIndex>
{
    public static SecondaryIndex Create() => new();
    public static void Reset(SecondaryIndex m) => m.Reset();

    public static int Size(SecondaryIndex m)
    {
        int n = 0;
        if (!string.IsNullOrEmpty(m.IndexName)) n += SizeOfString(m.IndexName);
        if (!string.IsNullOrEmpty(m.SecondaryKey)) n += SizeOfString(m.SecondaryKey);
        m.CachedSize = n;
        return n;
    }

    public static void WriteTo(ref ProtoWriter w, SecondaryIndex m)
    {
        if (!string.IsNullOrEmpty(m.IndexName)) w.WriteString(10, m.IndexName);
        if (!string.IsNullOrEmpty(m.SecondaryKey)) w.WriteString(18, m.SecondaryKey);
    }

    public static void MergeFrom(ref ProtoReader r, SecondaryIndex m)
    {
        while (r.TryReadTag(out uint tag))
        {
            switch (tag)
            {
                case 10: m.IndexName = r.ReadString(); break;
                case 18: m.SecondaryKey = r.ReadString(); break;
                default: r.SkipField(tag); break;
            }
        }
    }

    public static void Write(SecondaryIndex m, IBufferWriter<byte> output) => MessageCodec.Write<SecondaryIndexCodec, SecondaryIndex>(m, output);
    public static void Parse(ReadOnlySpan<byte> data, SecondaryIndex m) => MessageCodec.Parse<SecondaryIndexCodec, SecondaryIndex>(data, m);
    public static void Parse(in ReadOnlySequence<byte> data, SecondaryIndex m) => MessageCodec.Parse<SecondaryIndexCodec, SecondaryIndex>(in data, m);
}

/// <summary>Codec of the proto <c>Version</c> message (C# class <see cref="ProtoVersion"/>).</summary>
public readonly struct ProtoVersionCodec : IMessageCodec<ProtoVersion>
{
    public static ProtoVersion Create() => new();
    public static void Reset(ProtoVersion m) => m.Reset();

    public static int Size(ProtoVersion m)
    {
        int n = 0;
        if (m.VersionId != 0) n += 1 + SizeOfVarint((ulong)m.VersionId);
        if (m.ModificationsCount != 0) n += 1 + SizeOfVarint((ulong)m.ModificationsCount);
        if (m.CreatedTimestamp != 0) n += 9;
        if (m.ModifiedTimestamp != 0) n += 9;
        if (m.SessionId is { } sid) n += 1 + SizeOfVarint((ulong)sid);
        if (m.ClientIdentity is { } ci) n += SizeOfString(ci);
        m.CachedSize = n;
        return n;
    }

    public static void WriteTo(ref ProtoWriter w, ProtoVersion m)
    {
        if (m.VersionId != 0) w.WriteTagVarint(8, (ulong)m.VersionId);
        if (m.ModificationsCount != 0) w.WriteTagVarint(16, (ulong)m.ModificationsCount);
        if (m.CreatedTimestamp != 0) w.WriteTagFixed64(25, m.CreatedTimestamp);
        if (m.ModifiedTimestamp != 0) w.WriteTagFixed64(33, m.ModifiedTimestamp);
        if (m.SessionId is { } sid) w.WriteTagVarint(40, (ulong)sid);
        if (m.ClientIdentity is { } ci) w.WriteString(50, ci);
    }

    public static void MergeFrom(ref ProtoReader r, ProtoVersion m)
    {
        while (r.TryReadTag(out uint tag))
        {
            switch (tag)
            {
                case 8: m.VersionId = r.ReadInt64(); break;
                case 16: m.ModificationsCount = r.ReadInt64(); break;
                case 25: m.CreatedTimestamp = r.ReadFixed64(); break;
                case 33: m.ModifiedTimestamp = r.ReadFixed64(); break;
                case 40: m.SessionId = r.ReadInt64(); break;
                case 50: m.ClientIdentity = r.ReadString(); break;
                default: r.SkipField(tag); break;
            }
        }
    }

    public static void Write(ProtoVersion m, IBufferWriter<byte> output) => MessageCodec.Write<ProtoVersionCodec, ProtoVersion>(m, output);
    public static void Parse(ReadOnlySpan<byte> data, ProtoVersion m) => MessageCodec.Parse<ProtoVersionCodec, ProtoVersion>(data, m);
    public static void Parse(in ReadOnlySequence<byte> data, ProtoVersion m) => MessageCodec.Parse<ProtoVersionCodec, ProtoVersion>(in data, m);
}

public readonly struct PutRequestCodec : IMessageCodec<PutRequest>
{
    public static PutRequest Create() => new();
    public static void Reset(PutRequest m) => m.Reset();

    public static int Size(PutRequest m)
    {
        int n = 0;
        string key = m.Key;
        if (!string.IsNullOrEmpty(key)) n += SizeOfString(key);
        int vl = m.Value.Length;
        if (vl != 0) n = checked(n + SizeOfLengthDelimited(vl));
        if (m.ExpectedVersionId is { } ev) n += 1 + SizeOfVarint((ulong)ev);
        if (m.SessionId is { } sid) n += 1 + SizeOfVarint((ulong)sid);
        if (m.ClientIdentity is { } ci) n += SizeOfString(ci);
        if (m.PartitionKey is { } pk) n += SizeOfString(pk);
        if (m.SequenceKeyDelta is { Length: > 0 } deltas)
        {
            int l = 0;
            foreach (ulong d in deltas) l += SizeOfVarint(d);
            n += SizeOfLengthDelimited(l);
        }
        if (m.SecondaryIndexes is { Count: > 0 } indexes)
        {
            foreach (SecondaryIndex si in CollectionsMarshal.AsSpan(indexes)) n += SizeOfMessage(SecondaryIndexCodec.Size(si));
        }
        if (m.OverrideVersionId is { } ov) n += 1 + SizeOfVarint((ulong)ov);
        if (m.OverrideModificationsCount is { } oc) n += 1 + SizeOfVarint((ulong)oc);
        if (m.OpIndex != 0) n += 1 + SizeOfVarint(m.OpIndex);
        m.CachedSize = n;
        return n;
    }

    public static void WriteTo(ref ProtoWriter w, PutRequest m)
    {
        string key = m.Key;
        if (!string.IsNullOrEmpty(key)) w.WriteString(10, key);
        if (!m.Value.IsEmpty) w.WriteBytes(18, m.Value.Span);
        if (m.ExpectedVersionId is { } ev) w.WriteTagVarint(24, (ulong)ev);
        if (m.SessionId is { } sid) w.WriteTagVarint(32, (ulong)sid);
        if (m.ClientIdentity is { } ci) w.WriteString(42, ci);
        if (m.PartitionKey is { } pk) w.WriteString(50, pk);
        if (m.SequenceKeyDelta is { Length: > 0 } deltas) w.WritePackedVarints(58, deltas);
        if (m.SecondaryIndexes is { Count: > 0 } indexes)
        {
            foreach (SecondaryIndex si in CollectionsMarshal.AsSpan(indexes))
            {
                w.WriteTagLength(66, si.CachedSize);
                SecondaryIndexCodec.WriteTo(ref w, si);
            }
        }
        if (m.OverrideVersionId is { } ov) w.WriteTagVarint(72, (ulong)ov);
        if (m.OverrideModificationsCount is { } oc) w.WriteTagVarint(80, (ulong)oc);
        if (m.OpIndex != 0) w.WriteTagVarint(88, m.OpIndex);
    }

    public static void MergeFrom(ref ProtoReader r, PutRequest m)
    {
        while (r.TryReadTag(out uint tag))
        {
            switch (tag)
            {
                case 10: m.Key = r.ReadString(); break;
                case 18: m.Value = r.ReadBytes(); break;
                case 24: m.ExpectedVersionId = r.ReadInt64(); break;
                case 32: m.SessionId = r.ReadInt64(); break;
                case 42: m.ClientIdentity = r.ReadString(); break;
                case 50: m.PartitionKey = r.ReadString(); break;
                case 58: // packed
                {
                    var values = new List<ulong>();
                    long old = r.BeginPacked();
                    while (!r.AtEnd) values.Add(r.ReadVarint64());
                    r.EndMessage(old);
                    m.SequenceKeyDelta = Append(m.SequenceKeyDelta, values);
                    break;
                }
                case 56: // unpacked element (accepted by every protobuf parser)
                    m.SequenceKeyDelta = Append(m.SequenceKeyDelta, new List<ulong>(1) { r.ReadVarint64() });
                    break;
                case 66:
                {
                    var si = new SecondaryIndex();
                    long old = r.BeginMessage();
                    SecondaryIndexCodec.MergeFrom(ref r, si);
                    r.EndMessage(old);
                    (m.SecondaryIndexes ??= new List<SecondaryIndex>()).Add(si);
                    break;
                }
                case 72: m.OverrideVersionId = r.ReadInt64(); break;
                case 80: m.OverrideModificationsCount = r.ReadInt64(); break;
                case 88: m.OpIndex = r.ReadUInt32(); break;
                default: r.SkipField(tag); break;
            }
        }
    }

    private static ulong[] Append(ulong[]? existing, List<ulong> more)
    {
        if (existing is null || existing.Length == 0) return more.ToArray();
        var result = new ulong[existing.Length + more.Count];
        existing.CopyTo(result, 0);
        more.CopyTo(result, existing.Length);
        return result;
    }

    public static void Write(PutRequest m, IBufferWriter<byte> output) => MessageCodec.Write<PutRequestCodec, PutRequest>(m, output);
    public static void Parse(ReadOnlySpan<byte> data, PutRequest m) => MessageCodec.Parse<PutRequestCodec, PutRequest>(data, m);
    public static void Parse(in ReadOnlySequence<byte> data, PutRequest m) => MessageCodec.Parse<PutRequestCodec, PutRequest>(in data, m);
}

public readonly struct PutResponseCodec : IMessageCodec<PutResponse>
{
    public static PutResponse Create() => new();
    public static void Reset(PutResponse m) => m.Reset();

    public static int Size(PutResponse m)
    {
        int n = 0;
        if (m.Status != 0) n += 1 + SizeOfVarint((ulong)(long)(int)m.Status);
        if (m.Version is { } v) n += SizeOfMessage(ProtoVersionCodec.Size(v));
        if (m.Key is { } k) n += SizeOfString(k);
        m.CachedSize = n;
        return n;
    }

    public static void WriteTo(ref ProtoWriter w, PutResponse m)
    {
        if (m.Status != 0) w.WriteTagVarint(8, (ulong)(long)(int)m.Status);
        if (m.Version is { } v)
        {
            w.WriteTagLength(18, v.CachedSize);
            ProtoVersionCodec.WriteTo(ref w, v);
        }
        if (m.Key is { } k) w.WriteString(26, k);
    }

    public static void MergeFrom(ref ProtoReader r, PutResponse m)
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
                case 26: m.Key = r.ReadString(); break;
                default: r.SkipField(tag); break;
            }
        }
    }

    public static void Write(PutResponse m, IBufferWriter<byte> output) => MessageCodec.Write<PutResponseCodec, PutResponse>(m, output);
    public static void Parse(ReadOnlySpan<byte> data, PutResponse m) => MessageCodec.Parse<PutResponseCodec, PutResponse>(data, m);
    public static void Parse(in ReadOnlySequence<byte> data, PutResponse m) => MessageCodec.Parse<PutResponseCodec, PutResponse>(in data, m);
}

public readonly struct DeleteRequestCodec : IMessageCodec<DeleteRequest>
{
    public static DeleteRequest Create() => new();
    public static void Reset(DeleteRequest m) => m.Reset();

    public static int Size(DeleteRequest m)
    {
        int n = 0;
        if (!string.IsNullOrEmpty(m.Key)) n += SizeOfString(m.Key);
        if (m.ExpectedVersionId is { } ev) n += 1 + SizeOfVarint((ulong)ev);
        if (m.PartitionKey is { } pk) n += SizeOfString(pk);
        if (m.OpIndex != 0) n += 1 + SizeOfVarint(m.OpIndex);
        m.CachedSize = n;
        return n;
    }

    public static void WriteTo(ref ProtoWriter w, DeleteRequest m)
    {
        if (!string.IsNullOrEmpty(m.Key)) w.WriteString(10, m.Key);
        if (m.ExpectedVersionId is { } ev) w.WriteTagVarint(16, (ulong)ev);
        if (m.PartitionKey is { } pk) w.WriteString(26, pk);
        if (m.OpIndex != 0) w.WriteTagVarint(32, m.OpIndex);
    }

    public static void MergeFrom(ref ProtoReader r, DeleteRequest m)
    {
        while (r.TryReadTag(out uint tag))
        {
            switch (tag)
            {
                case 10: m.Key = r.ReadString(); break;
                case 16: m.ExpectedVersionId = r.ReadInt64(); break;
                case 26: m.PartitionKey = r.ReadString(); break;
                case 32: m.OpIndex = r.ReadUInt32(); break;
                default: r.SkipField(tag); break;
            }
        }
    }

    public static void Write(DeleteRequest m, IBufferWriter<byte> output) => MessageCodec.Write<DeleteRequestCodec, DeleteRequest>(m, output);
    public static void Parse(ReadOnlySpan<byte> data, DeleteRequest m) => MessageCodec.Parse<DeleteRequestCodec, DeleteRequest>(data, m);
    public static void Parse(in ReadOnlySequence<byte> data, DeleteRequest m) => MessageCodec.Parse<DeleteRequestCodec, DeleteRequest>(in data, m);
}

public readonly struct DeleteResponseCodec : IMessageCodec<DeleteResponse>
{
    public static DeleteResponse Create() => new();
    public static void Reset(DeleteResponse m) => m.Reset();

    public static int Size(DeleteResponse m)
    {
        int n = m.Status != 0 ? 1 + SizeOfVarint((ulong)(long)(int)m.Status) : 0;
        m.CachedSize = n;
        return n;
    }

    public static void WriteTo(ref ProtoWriter w, DeleteResponse m)
    {
        if (m.Status != 0) w.WriteTagVarint(8, (ulong)(long)(int)m.Status);
    }

    public static void MergeFrom(ref ProtoReader r, DeleteResponse m)
    {
        while (r.TryReadTag(out uint tag))
        {
            switch (tag)
            {
                case 8: m.Status = (Status)r.ReadInt32(); break;
                default: r.SkipField(tag); break;
            }
        }
    }

    public static void Write(DeleteResponse m, IBufferWriter<byte> output) => MessageCodec.Write<DeleteResponseCodec, DeleteResponse>(m, output);
    public static void Parse(ReadOnlySpan<byte> data, DeleteResponse m) => MessageCodec.Parse<DeleteResponseCodec, DeleteResponse>(data, m);
    public static void Parse(in ReadOnlySequence<byte> data, DeleteResponse m) => MessageCodec.Parse<DeleteResponseCodec, DeleteResponse>(in data, m);
}

public readonly struct DeleteRangeRequestCodec : IMessageCodec<DeleteRangeRequest>
{
    public static DeleteRangeRequest Create() => new();
    public static void Reset(DeleteRangeRequest m) => m.Reset();

    public static int Size(DeleteRangeRequest m)
    {
        int n = 0;
        if (!string.IsNullOrEmpty(m.StartInclusive)) n += SizeOfString(m.StartInclusive);
        if (!string.IsNullOrEmpty(m.EndExclusive)) n += SizeOfString(m.EndExclusive);
        if (m.OpIndex != 0) n += 1 + SizeOfVarint(m.OpIndex);
        m.CachedSize = n;
        return n;
    }

    public static void WriteTo(ref ProtoWriter w, DeleteRangeRequest m)
    {
        if (!string.IsNullOrEmpty(m.StartInclusive)) w.WriteString(10, m.StartInclusive);
        if (!string.IsNullOrEmpty(m.EndExclusive)) w.WriteString(18, m.EndExclusive);
        if (m.OpIndex != 0) w.WriteTagVarint(24, m.OpIndex);
    }

    public static void MergeFrom(ref ProtoReader r, DeleteRangeRequest m)
    {
        while (r.TryReadTag(out uint tag))
        {
            switch (tag)
            {
                case 10: m.StartInclusive = r.ReadString(); break;
                case 18: m.EndExclusive = r.ReadString(); break;
                case 24: m.OpIndex = r.ReadUInt32(); break;
                default: r.SkipField(tag); break;
            }
        }
    }

    public static void Write(DeleteRangeRequest m, IBufferWriter<byte> output) => MessageCodec.Write<DeleteRangeRequestCodec, DeleteRangeRequest>(m, output);
    public static void Parse(ReadOnlySpan<byte> data, DeleteRangeRequest m) => MessageCodec.Parse<DeleteRangeRequestCodec, DeleteRangeRequest>(data, m);
    public static void Parse(in ReadOnlySequence<byte> data, DeleteRangeRequest m) => MessageCodec.Parse<DeleteRangeRequestCodec, DeleteRangeRequest>(in data, m);
}

public readonly struct DeleteRangeResponseCodec : IMessageCodec<DeleteRangeResponse>
{
    public static DeleteRangeResponse Create() => new();
    public static void Reset(DeleteRangeResponse m) => m.Reset();

    public static int Size(DeleteRangeResponse m)
    {
        int n = m.Status != 0 ? 1 + SizeOfVarint((ulong)(long)(int)m.Status) : 0;
        m.CachedSize = n;
        return n;
    }

    public static void WriteTo(ref ProtoWriter w, DeleteRangeResponse m)
    {
        if (m.Status != 0) w.WriteTagVarint(8, (ulong)(long)(int)m.Status);
    }

    public static void MergeFrom(ref ProtoReader r, DeleteRangeResponse m)
    {
        while (r.TryReadTag(out uint tag))
        {
            switch (tag)
            {
                case 8: m.Status = (Status)r.ReadInt32(); break;
                default: r.SkipField(tag); break;
            }
        }
    }

    public static void Write(DeleteRangeResponse m, IBufferWriter<byte> output) => MessageCodec.Write<DeleteRangeResponseCodec, DeleteRangeResponse>(m, output);
    public static void Parse(ReadOnlySpan<byte> data, DeleteRangeResponse m) => MessageCodec.Parse<DeleteRangeResponseCodec, DeleteRangeResponse>(data, m);
    public static void Parse(in ReadOnlySequence<byte> data, DeleteRangeResponse m) => MessageCodec.Parse<DeleteRangeResponseCodec, DeleteRangeResponse>(in data, m);
}

public readonly struct WriteRequestCodec : IMessageCodec<WriteRequest>
{
    public static WriteRequest Create() => new();
    public static void Reset(WriteRequest m) => m.Reset();

    public static int Size(WriteRequest m)
    {
        int n = 0;
        if (m.Shard is { } shard) n += 1 + SizeOfVarint((ulong)shard);
        checked
        {
            foreach (PutRequest p in CollectionsMarshal.AsSpan(m.Puts)) n += SizeOfMessage(PutRequestCodec.Size(p));
            foreach (DeleteRequest d in CollectionsMarshal.AsSpan(m.Deletes)) n += SizeOfMessage(DeleteRequestCodec.Size(d));
            foreach (DeleteRangeRequest d in CollectionsMarshal.AsSpan(m.DeleteRanges)) n += SizeOfMessage(DeleteRangeRequestCodec.Size(d));
        }
        return n;
    }

    public static void WriteTo(ref ProtoWriter w, WriteRequest m)
    {
        if (m.Shard is { } shard) w.WriteTagVarint(8, (ulong)shard);
        foreach (PutRequest p in CollectionsMarshal.AsSpan(m.Puts))
        {
            w.WriteTagLength(18, p.CachedSize);
            PutRequestCodec.WriteTo(ref w, p);
        }
        foreach (DeleteRequest d in CollectionsMarshal.AsSpan(m.Deletes))
        {
            w.WriteTagLength(26, d.CachedSize);
            DeleteRequestCodec.WriteTo(ref w, d);
        }
        foreach (DeleteRangeRequest d in CollectionsMarshal.AsSpan(m.DeleteRanges))
        {
            w.WriteTagLength(34, d.CachedSize);
            DeleteRangeRequestCodec.WriteTo(ref w, d);
        }
    }

    public static void MergeFrom(ref ProtoReader r, WriteRequest m)
    {
        while (r.TryReadTag(out uint tag))
        {
            switch (tag)
            {
                case 8: m.Shard = r.ReadInt64(); break;
                case 18:
                {
                    var p = new PutRequest();
                    long old = r.BeginMessage();
                    PutRequestCodec.MergeFrom(ref r, p);
                    r.EndMessage(old);
                    m.Puts.Add(p);
                    break;
                }
                case 26:
                {
                    var d = new DeleteRequest();
                    long old = r.BeginMessage();
                    DeleteRequestCodec.MergeFrom(ref r, d);
                    r.EndMessage(old);
                    m.Deletes.Add(d);
                    break;
                }
                case 34:
                {
                    var d = new DeleteRangeRequest();
                    long old = r.BeginMessage();
                    DeleteRangeRequestCodec.MergeFrom(ref r, d);
                    r.EndMessage(old);
                    m.DeleteRanges.Add(d);
                    break;
                }
                default: r.SkipField(tag); break;
            }
        }
    }

    public static void Write(WriteRequest m, IBufferWriter<byte> output) => MessageCodec.Write<WriteRequestCodec, WriteRequest>(m, output);
    public static void Parse(ReadOnlySpan<byte> data, WriteRequest m) => MessageCodec.Parse<WriteRequestCodec, WriteRequest>(data, m);
    public static void Parse(in ReadOnlySequence<byte> data, WriteRequest m) => MessageCodec.Parse<WriteRequestCodec, WriteRequest>(in data, m);
}

public readonly struct WriteResponseCodec : IMessageCodec<WriteResponse>
{
    public static WriteResponse Create() => new();
    public static void Reset(WriteResponse m) => m.Reset();

    public static int Size(WriteResponse m)
    {
        int n = 0;
        checked
        {
            foreach (PutResponse p in CollectionsMarshal.AsSpan(m.Puts)) n += SizeOfMessage(PutResponseCodec.Size(p));
            foreach (DeleteResponse d in CollectionsMarshal.AsSpan(m.Deletes)) n += SizeOfMessage(DeleteResponseCodec.Size(d));
            foreach (DeleteRangeResponse d in CollectionsMarshal.AsSpan(m.DeleteRanges)) n += SizeOfMessage(DeleteRangeResponseCodec.Size(d));
        }
        return n;
    }

    public static void WriteTo(ref ProtoWriter w, WriteResponse m)
    {
        foreach (PutResponse p in CollectionsMarshal.AsSpan(m.Puts))
        {
            w.WriteTagLength(10, p.CachedSize);
            PutResponseCodec.WriteTo(ref w, p);
        }
        foreach (DeleteResponse d in CollectionsMarshal.AsSpan(m.Deletes))
        {
            w.WriteTagLength(18, d.CachedSize);
            DeleteResponseCodec.WriteTo(ref w, d);
        }
        foreach (DeleteRangeResponse d in CollectionsMarshal.AsSpan(m.DeleteRanges))
        {
            w.WriteTagLength(26, d.CachedSize);
            DeleteRangeResponseCodec.WriteTo(ref w, d);
        }
    }

    public static void MergeFrom(ref ProtoReader r, WriteResponse m)
    {
        while (r.TryReadTag(out uint tag))
        {
            switch (tag)
            {
                case 10:
                {
                    PutResponse p = m.SparePuts.TryPop() ?? new PutResponse();
                    long old = r.BeginMessage();
                    PutResponseCodec.MergeFrom(ref r, p);
                    r.EndMessage(old);
                    m.Puts.Add(p);
                    break;
                }
                case 18:
                {
                    DeleteResponse d = m.SpareDeletes.TryPop() ?? new DeleteResponse();
                    long old = r.BeginMessage();
                    DeleteResponseCodec.MergeFrom(ref r, d);
                    r.EndMessage(old);
                    m.Deletes.Add(d);
                    break;
                }
                case 26:
                {
                    DeleteRangeResponse d = m.SpareDeleteRanges.TryPop() ?? new DeleteRangeResponse();
                    long old = r.BeginMessage();
                    DeleteRangeResponseCodec.MergeFrom(ref r, d);
                    r.EndMessage(old);
                    m.DeleteRanges.Add(d);
                    break;
                }
                default: r.SkipField(tag); break;
            }
        }
    }

    public static void Write(WriteResponse m, IBufferWriter<byte> output) => MessageCodec.Write<WriteResponseCodec, WriteResponse>(m, output);
    public static void Parse(ReadOnlySpan<byte> data, WriteResponse m) => MessageCodec.Parse<WriteResponseCodec, WriteResponse>(data, m);
    public static void Parse(in ReadOnlySequence<byte> data, WriteResponse m) => MessageCodec.Parse<WriteResponseCodec, WriteResponse>(in data, m);
}

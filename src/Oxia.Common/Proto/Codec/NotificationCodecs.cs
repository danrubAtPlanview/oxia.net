// =====================================================================================================================
// PORT NOTE
//   Upstream : common/proto/client_vtproto.pb.go  (NotificationsRequest, Notification, NotificationEntry, NotificationBatch)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Wire codecs of the notification stream.  NotificationBatch is received once per committed write batch of a
//              notifications-enabled namespace, so its parsing is allocation-conscious (children recycled through Spares).
//   Divergences: see MessageCodec.cs.  NotificationEntry.key is `optional` (an empty key is still emitted, exactly like a
//              map entry of the former map<string, Notification> field); NotificationEntry.value is emitted whenever non-null.
// =====================================================================================================================
using System.Buffers;
using System.Runtime.InteropServices;
using static Oxia.Common.Proto.Codec.ProtoWire;

namespace Oxia.Common.Proto.Codec;

public readonly struct NotificationsRequestCodec : IMessageCodec<NotificationsRequest>
{
    public static NotificationsRequest Create() => new();
    public static void Reset(NotificationsRequest m) => m.Reset();

    public static int Size(NotificationsRequest m)
    {
        int n = 0;
        if (m.Shard != 0) n += 1 + SizeOfVarint((ulong)m.Shard);
        if (m.StartOffsetExclusive is { } so) n += 1 + SizeOfVarint((ulong)so);
        return n;
    }

    public static void WriteTo(ref ProtoWriter w, NotificationsRequest m)
    {
        if (m.Shard != 0) w.WriteTagVarint(8, (ulong)m.Shard);
        if (m.StartOffsetExclusive is { } so) w.WriteTagVarint(16, (ulong)so);
    }

    public static void MergeFrom(ref ProtoReader r, NotificationsRequest m)
    {
        while (r.TryReadTag(out uint tag))
        {
            switch (tag)
            {
                case 8: m.Shard = r.ReadInt64(); break;
                case 16: m.StartOffsetExclusive = r.ReadInt64(); break;
                default: r.SkipField(tag); break;
            }
        }
    }

    public static void Write(NotificationsRequest m, IBufferWriter<byte> output) => MessageCodec.Write<NotificationsRequestCodec, NotificationsRequest>(m, output);
    public static void Parse(ReadOnlySpan<byte> data, NotificationsRequest m) => MessageCodec.Parse<NotificationsRequestCodec, NotificationsRequest>(data, m);
    public static void Parse(in ReadOnlySequence<byte> data, NotificationsRequest m) => MessageCodec.Parse<NotificationsRequestCodec, NotificationsRequest>(in data, m);
}

public readonly struct NotificationCodec : IMessageCodec<Notification>
{
    public static Notification Create() => new();
    public static void Reset(Notification m) => m.Reset();

    public static int Size(Notification m)
    {
        int n = 0;
        if (m.Type != 0) n += 1 + SizeOfVarint((ulong)(long)(int)m.Type);
        if (m.VersionId is { } v) n += 1 + SizeOfVarint((ulong)v);
        if (m.KeyRangeLast is { } k) n += SizeOfString(k);
        m.CachedSize = n;
        return n;
    }

    public static void WriteTo(ref ProtoWriter w, Notification m)
    {
        if (m.Type != 0) w.WriteTagVarint(8, (ulong)(long)(int)m.Type);
        if (m.VersionId is { } v) w.WriteTagVarint(16, (ulong)v);
        if (m.KeyRangeLast is { } k) w.WriteString(26, k);
    }

    public static void MergeFrom(ref ProtoReader r, Notification m)
    {
        while (r.TryReadTag(out uint tag))
        {
            switch (tag)
            {
                case 8: m.Type = (NotificationType)r.ReadInt32(); break;
                case 16: m.VersionId = r.ReadInt64(); break;
                case 26: m.KeyRangeLast = r.ReadString(); break;
                default: r.SkipField(tag); break;
            }
        }
    }

    public static void Write(Notification m, IBufferWriter<byte> output) => MessageCodec.Write<NotificationCodec, Notification>(m, output);
    public static void Parse(ReadOnlySpan<byte> data, Notification m) => MessageCodec.Parse<NotificationCodec, Notification>(data, m);
    public static void Parse(in ReadOnlySequence<byte> data, Notification m) => MessageCodec.Parse<NotificationCodec, Notification>(in data, m);
}

public readonly struct NotificationEntryCodec : IMessageCodec<NotificationEntry>
{
    public static NotificationEntry Create() => new();
    public static void Reset(NotificationEntry m) => m.Reset();

    public static int Size(NotificationEntry m)
    {
        int n = 0;
        if (m.Key is { } k) n += SizeOfString(k);
        if (m.Value is { } v) n += SizeOfMessage(NotificationCodec.Size(v));
        m.CachedSize = n;
        return n;
    }

    public static void WriteTo(ref ProtoWriter w, NotificationEntry m)
    {
        if (m.Key is { } k) w.WriteString(10, k);
        if (m.Value is { } v)
        {
            w.WriteTagLength(18, v.CachedSize);
            NotificationCodec.WriteTo(ref w, v);
        }
    }

    public static void MergeFrom(ref ProtoReader r, NotificationEntry m)
    {
        while (r.TryReadTag(out uint tag))
        {
            switch (tag)
            {
                case 10: m.Key = r.ReadString(); break;
                case 18:
                {
                    Notification? v = m.Value;
                    if (v is null)
                    {
                        v = m.SpareValue ?? new Notification();
                        m.SpareValue = null;
                        m.Value = v;
                    }
                    long old = r.BeginMessage();
                    NotificationCodec.MergeFrom(ref r, v);
                    r.EndMessage(old);
                    break;
                }
                default: r.SkipField(tag); break;
            }
        }
    }

    public static void Write(NotificationEntry m, IBufferWriter<byte> output) => MessageCodec.Write<NotificationEntryCodec, NotificationEntry>(m, output);
    public static void Parse(ReadOnlySpan<byte> data, NotificationEntry m) => MessageCodec.Parse<NotificationEntryCodec, NotificationEntry>(data, m);
    public static void Parse(in ReadOnlySequence<byte> data, NotificationEntry m) => MessageCodec.Parse<NotificationEntryCodec, NotificationEntry>(in data, m);
}

public readonly struct NotificationBatchCodec : IPooledMessageCodec<NotificationBatch>
{
    public static NotificationBatch Create() => new();
    public static NotificationBatch Rent() => NotificationBatch.Rent();
    public static void Reset(NotificationBatch m) => m.Reset();

    public static int Size(NotificationBatch m)
    {
        int n = 0;
        if (m.Shard != 0) n += 1 + SizeOfVarint((ulong)m.Shard);
        if (m.Offset != 0) n += 1 + SizeOfVarint((ulong)m.Offset);
        if (m.Timestamp != 0) n += 9;
        checked
        {
            foreach (NotificationEntry e in CollectionsMarshal.AsSpan(m.Notifications)) n += SizeOfMessage(NotificationEntryCodec.Size(e));
        }
        return n;
    }

    public static void WriteTo(ref ProtoWriter w, NotificationBatch m)
    {
        if (m.Shard != 0) w.WriteTagVarint(8, (ulong)m.Shard);
        if (m.Offset != 0) w.WriteTagVarint(16, (ulong)m.Offset);
        if (m.Timestamp != 0) w.WriteTagFixed64(25, m.Timestamp);
        foreach (NotificationEntry e in CollectionsMarshal.AsSpan(m.Notifications))
        {
            w.WriteTagLength(34, e.CachedSize);
            NotificationEntryCodec.WriteTo(ref w, e);
        }
    }

    public static void MergeFrom(ref ProtoReader r, NotificationBatch m)
    {
        while (r.TryReadTag(out uint tag))
        {
            switch (tag)
            {
                case 8: m.Shard = r.ReadInt64(); break;
                case 16: m.Offset = r.ReadInt64(); break;
                case 25: m.Timestamp = r.ReadFixed64(); break;
                case 34:
                {
                    NotificationEntry e = m.SpareNotifications.TryPop() ?? new NotificationEntry();
                    long old = r.BeginMessage();
                    NotificationEntryCodec.MergeFrom(ref r, e);
                    r.EndMessage(old);
                    m.Notifications.Add(e);
                    break;
                }
                default: r.SkipField(tag); break;
            }
        }
    }

    public static void Write(NotificationBatch m, IBufferWriter<byte> output) => MessageCodec.Write<NotificationBatchCodec, NotificationBatch>(m, output);
    public static void Parse(ReadOnlySpan<byte> data, NotificationBatch m) => MessageCodec.Parse<NotificationBatchCodec, NotificationBatch>(data, m);
    public static void Parse(in ReadOnlySequence<byte> data, NotificationBatch m) => MessageCodec.Parse<NotificationBatchCodec, NotificationBatch>(in data, m);
}

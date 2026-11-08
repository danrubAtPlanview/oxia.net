// =====================================================================================================================
// PORT NOTE
//   Upstream : common/proto/client_vtproto.pb.go  (GetSequenceUpdatesRequest/Response, CreateSessionRequest/Response,
//              SessionHeartbeat, KeepAliveResponse, CloseSessionRequest, CloseSessionResponse)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Wire codecs of the session and sequence-update messages (small, infrequent).
//   Divergences: see MessageCodec.cs.
// =====================================================================================================================
using System.Buffers;
using static Oxia.Common.Proto.Codec.ProtoWire;

namespace Oxia.Common.Proto.Codec;

public readonly struct GetSequenceUpdatesRequestCodec : IMessageCodec<GetSequenceUpdatesRequest>
{
    public static GetSequenceUpdatesRequest Create() => new();
    public static void Reset(GetSequenceUpdatesRequest m) => m.Reset();

    public static int Size(GetSequenceUpdatesRequest m)
    {
        int n = 0;
        if (m.Shard != 0) n += 1 + SizeOfVarint((ulong)m.Shard);
        if (!string.IsNullOrEmpty(m.Key)) n += SizeOfString(m.Key);
        return n;
    }

    public static void WriteTo(ref ProtoWriter w, GetSequenceUpdatesRequest m)
    {
        if (m.Shard != 0) w.WriteTagVarint(8, (ulong)m.Shard);
        if (!string.IsNullOrEmpty(m.Key)) w.WriteString(18, m.Key);
    }

    public static void MergeFrom(ref ProtoReader r, GetSequenceUpdatesRequest m)
    {
        while (r.TryReadTag(out uint tag))
        {
            switch (tag)
            {
                case 8: m.Shard = r.ReadInt64(); break;
                case 18: m.Key = r.ReadString(); break;
                default: r.SkipField(tag); break;
            }
        }
    }

    public static void Write(GetSequenceUpdatesRequest m, IBufferWriter<byte> output) => MessageCodec.Write<GetSequenceUpdatesRequestCodec, GetSequenceUpdatesRequest>(m, output);
    public static void Parse(ReadOnlySpan<byte> data, GetSequenceUpdatesRequest m) => MessageCodec.Parse<GetSequenceUpdatesRequestCodec, GetSequenceUpdatesRequest>(data, m);
    public static void Parse(in ReadOnlySequence<byte> data, GetSequenceUpdatesRequest m) => MessageCodec.Parse<GetSequenceUpdatesRequestCodec, GetSequenceUpdatesRequest>(in data, m);
}

public readonly struct GetSequenceUpdatesResponseCodec : IMessageCodec<GetSequenceUpdatesResponse>
{
    public static GetSequenceUpdatesResponse Create() => new();
    public static void Reset(GetSequenceUpdatesResponse m) => m.Reset();

    public static int Size(GetSequenceUpdatesResponse m)
        => string.IsNullOrEmpty(m.HighestSequenceKey) ? 0 : SizeOfString(m.HighestSequenceKey);

    public static void WriteTo(ref ProtoWriter w, GetSequenceUpdatesResponse m)
    {
        if (!string.IsNullOrEmpty(m.HighestSequenceKey)) w.WriteString(10, m.HighestSequenceKey);
    }

    public static void MergeFrom(ref ProtoReader r, GetSequenceUpdatesResponse m)
    {
        while (r.TryReadTag(out uint tag))
        {
            switch (tag)
            {
                case 10: m.HighestSequenceKey = r.ReadString(); break;
                default: r.SkipField(tag); break;
            }
        }
    }

    public static void Write(GetSequenceUpdatesResponse m, IBufferWriter<byte> output) => MessageCodec.Write<GetSequenceUpdatesResponseCodec, GetSequenceUpdatesResponse>(m, output);
    public static void Parse(ReadOnlySpan<byte> data, GetSequenceUpdatesResponse m) => MessageCodec.Parse<GetSequenceUpdatesResponseCodec, GetSequenceUpdatesResponse>(data, m);
    public static void Parse(in ReadOnlySequence<byte> data, GetSequenceUpdatesResponse m) => MessageCodec.Parse<GetSequenceUpdatesResponseCodec, GetSequenceUpdatesResponse>(in data, m);
}

public readonly struct CreateSessionRequestCodec : IMessageCodec<CreateSessionRequest>
{
    public static CreateSessionRequest Create() => new();
    public static void Reset(CreateSessionRequest m) => m.Reset();

    public static int Size(CreateSessionRequest m)
    {
        int n = 0;
        if (m.Shard != 0) n += 1 + SizeOfVarint((ulong)m.Shard);
        if (m.SessionTimeoutMs != 0) n += 1 + SizeOfVarint(m.SessionTimeoutMs);
        if (!string.IsNullOrEmpty(m.ClientIdentity)) n += SizeOfString(m.ClientIdentity);
        return n;
    }

    public static void WriteTo(ref ProtoWriter w, CreateSessionRequest m)
    {
        if (m.Shard != 0) w.WriteTagVarint(8, (ulong)m.Shard);
        if (m.SessionTimeoutMs != 0) w.WriteTagVarint(16, m.SessionTimeoutMs);
        if (!string.IsNullOrEmpty(m.ClientIdentity)) w.WriteString(26, m.ClientIdentity);
    }

    public static void MergeFrom(ref ProtoReader r, CreateSessionRequest m)
    {
        while (r.TryReadTag(out uint tag))
        {
            switch (tag)
            {
                case 8: m.Shard = r.ReadInt64(); break;
                case 16: m.SessionTimeoutMs = r.ReadUInt32(); break;
                case 26: m.ClientIdentity = r.ReadString(); break;
                default: r.SkipField(tag); break;
            }
        }
    }

    public static void Write(CreateSessionRequest m, IBufferWriter<byte> output) => MessageCodec.Write<CreateSessionRequestCodec, CreateSessionRequest>(m, output);
    public static void Parse(ReadOnlySpan<byte> data, CreateSessionRequest m) => MessageCodec.Parse<CreateSessionRequestCodec, CreateSessionRequest>(data, m);
    public static void Parse(in ReadOnlySequence<byte> data, CreateSessionRequest m) => MessageCodec.Parse<CreateSessionRequestCodec, CreateSessionRequest>(in data, m);
}

public readonly struct CreateSessionResponseCodec : IMessageCodec<CreateSessionResponse>
{
    public static CreateSessionResponse Create() => new();
    public static void Reset(CreateSessionResponse m) => m.Reset();

    public static int Size(CreateSessionResponse m) => m.SessionId != 0 ? 1 + SizeOfVarint((ulong)m.SessionId) : 0;

    public static void WriteTo(ref ProtoWriter w, CreateSessionResponse m)
    {
        if (m.SessionId != 0) w.WriteTagVarint(8, (ulong)m.SessionId);
    }

    public static void MergeFrom(ref ProtoReader r, CreateSessionResponse m)
    {
        while (r.TryReadTag(out uint tag))
        {
            switch (tag)
            {
                case 8: m.SessionId = r.ReadInt64(); break;
                default: r.SkipField(tag); break;
            }
        }
    }

    public static void Write(CreateSessionResponse m, IBufferWriter<byte> output) => MessageCodec.Write<CreateSessionResponseCodec, CreateSessionResponse>(m, output);
    public static void Parse(ReadOnlySpan<byte> data, CreateSessionResponse m) => MessageCodec.Parse<CreateSessionResponseCodec, CreateSessionResponse>(data, m);
    public static void Parse(in ReadOnlySequence<byte> data, CreateSessionResponse m) => MessageCodec.Parse<CreateSessionResponseCodec, CreateSessionResponse>(in data, m);
}

public readonly struct SessionHeartbeatCodec : IMessageCodec<SessionHeartbeat>
{
    public static SessionHeartbeat Create() => new();
    public static void Reset(SessionHeartbeat m) => m.Reset();

    public static int Size(SessionHeartbeat m)
    {
        int n = 0;
        if (m.Shard != 0) n += 1 + SizeOfVarint((ulong)m.Shard);
        if (m.SessionId != 0) n += 1 + SizeOfVarint((ulong)m.SessionId);
        return n;
    }

    public static void WriteTo(ref ProtoWriter w, SessionHeartbeat m)
    {
        if (m.Shard != 0) w.WriteTagVarint(8, (ulong)m.Shard);
        if (m.SessionId != 0) w.WriteTagVarint(16, (ulong)m.SessionId);
    }

    public static void MergeFrom(ref ProtoReader r, SessionHeartbeat m)
    {
        while (r.TryReadTag(out uint tag))
        {
            switch (tag)
            {
                case 8: m.Shard = r.ReadInt64(); break;
                case 16: m.SessionId = r.ReadInt64(); break;
                default: r.SkipField(tag); break;
            }
        }
    }

    public static void Write(SessionHeartbeat m, IBufferWriter<byte> output) => MessageCodec.Write<SessionHeartbeatCodec, SessionHeartbeat>(m, output);
    public static void Parse(ReadOnlySpan<byte> data, SessionHeartbeat m) => MessageCodec.Parse<SessionHeartbeatCodec, SessionHeartbeat>(data, m);
    public static void Parse(in ReadOnlySequence<byte> data, SessionHeartbeat m) => MessageCodec.Parse<SessionHeartbeatCodec, SessionHeartbeat>(in data, m);
}

public readonly struct KeepAliveResponseCodec : IMessageCodec<KeepAliveResponse>
{
    public static KeepAliveResponse Create() => new();
    public static void Reset(KeepAliveResponse m) => m.Reset();

    public static int Size(KeepAliveResponse m) => 0;

    public static void WriteTo(ref ProtoWriter w, KeepAliveResponse m) { }

    public static void MergeFrom(ref ProtoReader r, KeepAliveResponse m)
    {
        while (r.TryReadTag(out uint tag)) r.SkipField(tag);
    }

    public static void Write(KeepAliveResponse m, IBufferWriter<byte> output) => MessageCodec.Write<KeepAliveResponseCodec, KeepAliveResponse>(m, output);
    public static void Parse(ReadOnlySpan<byte> data, KeepAliveResponse m) => MessageCodec.Parse<KeepAliveResponseCodec, KeepAliveResponse>(data, m);
    public static void Parse(in ReadOnlySequence<byte> data, KeepAliveResponse m) => MessageCodec.Parse<KeepAliveResponseCodec, KeepAliveResponse>(in data, m);
}

public readonly struct CloseSessionRequestCodec : IMessageCodec<CloseSessionRequest>
{
    public static CloseSessionRequest Create() => new();
    public static void Reset(CloseSessionRequest m) => m.Reset();

    public static int Size(CloseSessionRequest m)
    {
        int n = 0;
        if (m.Shard != 0) n += 1 + SizeOfVarint((ulong)m.Shard);
        if (m.SessionId != 0) n += 1 + SizeOfVarint((ulong)m.SessionId);
        return n;
    }

    public static void WriteTo(ref ProtoWriter w, CloseSessionRequest m)
    {
        if (m.Shard != 0) w.WriteTagVarint(8, (ulong)m.Shard);
        if (m.SessionId != 0) w.WriteTagVarint(16, (ulong)m.SessionId);
    }

    public static void MergeFrom(ref ProtoReader r, CloseSessionRequest m)
    {
        while (r.TryReadTag(out uint tag))
        {
            switch (tag)
            {
                case 8: m.Shard = r.ReadInt64(); break;
                case 16: m.SessionId = r.ReadInt64(); break;
                default: r.SkipField(tag); break;
            }
        }
    }

    public static void Write(CloseSessionRequest m, IBufferWriter<byte> output) => MessageCodec.Write<CloseSessionRequestCodec, CloseSessionRequest>(m, output);
    public static void Parse(ReadOnlySpan<byte> data, CloseSessionRequest m) => MessageCodec.Parse<CloseSessionRequestCodec, CloseSessionRequest>(data, m);
    public static void Parse(in ReadOnlySequence<byte> data, CloseSessionRequest m) => MessageCodec.Parse<CloseSessionRequestCodec, CloseSessionRequest>(in data, m);
}

public readonly struct CloseSessionResponseCodec : IMessageCodec<CloseSessionResponse>
{
    public static CloseSessionResponse Create() => new();
    public static void Reset(CloseSessionResponse m) => m.Reset();

    public static int Size(CloseSessionResponse m) => 0;

    public static void WriteTo(ref ProtoWriter w, CloseSessionResponse m) { }

    public static void MergeFrom(ref ProtoReader r, CloseSessionResponse m)
    {
        while (r.TryReadTag(out uint tag)) r.SkipField(tag);
    }

    public static void Write(CloseSessionResponse m, IBufferWriter<byte> output) => MessageCodec.Write<CloseSessionResponseCodec, CloseSessionResponse>(m, output);
    public static void Parse(ReadOnlySpan<byte> data, CloseSessionResponse m) => MessageCodec.Parse<CloseSessionResponseCodec, CloseSessionResponse>(data, m);
    public static void Parse(in ReadOnlySequence<byte> data, CloseSessionResponse m) => MessageCodec.Parse<CloseSessionResponseCodec, CloseSessionResponse>(in data, m);
}

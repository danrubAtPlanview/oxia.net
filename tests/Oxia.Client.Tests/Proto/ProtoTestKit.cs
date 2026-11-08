// =====================================================================================================================
// PORT NOTE (test infrastructure of the wire-codec component)
//   Upstream : (none -- new oracle/differential test support; Go relies on protobuf-go as its oracle)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Shared helpers of the codec tests: a reflection-based seeded random message generator, a reflection-based
//              mapper hand-written message -> protoc-generated oracle message (Io.Oxia.Proto.V1, alias G), a structural dump
//              used for equality, byte-sequence helpers (multi-segment ReadOnlySequence, chunked IBufferWriter), an
//              independent wire-format scanner / builder.  `IMessageKind` binds one message to its hand codec and its oracle.
//   Divergences: n/a (test code).
// =====================================================================================================================
using System.Buffers;
using System.Collections;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using Google.Protobuf;
using Oxia.Common.Proto;
using Oxia.Common.Proto.Codec;
using G = Io.Oxia.Proto.V1;

namespace Oxia.Client.Tests.Proto;

/// <summary>Knobs of <see cref="ProtoReflection.Generate"/>.</summary>
public sealed class GenOptions
{
    /// <summary>Probability that an optional (nullable) member is present.</summary>
    public double Presence = 0.55;
    /// <summary>Maximum number of elements of a repeated field.</summary>
    public int MaxList = 6;
    /// <summary>Allow values / strings above 64 KiB.</summary>
    public bool Large;
    /// <summary>Generate map entries with an empty key (vtproto writes them; Google.Protobuf C# omits the empty key field).</summary>
    public bool EmptyMapKeys = true;
}

/// <summary>Reflection helpers over the public surface of the hand-written messages.</summary>
public static class ProtoReflection
{
    private const BindingFlags Pub = BindingFlags.Public | BindingFlags.Instance;

    private static readonly long[] LongEdges =
    {
        0, 1, -1, 2, 63, 64, 127, 128, 129, 255, 256, 16383, 16384, 2097151, 2097152, int.MaxValue, int.MinValue,
        (long)uint.MaxValue, (long)uint.MaxValue + 1, long.MaxValue, long.MinValue, long.MaxValue - 1, long.MinValue + 1,
    };

    private static readonly ulong[] ULongEdges =
    {
        0, 1, 127, 128, 16383, 16384, uint.MaxValue, (ulong)uint.MaxValue + 1, ulong.MaxValue, ulong.MaxValue - 1, 1UL << 63,
    };

    private static readonly uint[] UIntEdges = { 0, 1, 127, 128, 16383, 16384, 2097151, 2097152, uint.MaxValue, uint.MaxValue - 1 };

    public static long RandLong(Random rng) => rng.Next(3) switch
    {
        0 => LongEdges[rng.Next(LongEdges.Length)],
        1 => rng.Next(-1000, 1000),
        _ => rng.NextInt64(long.MinValue, long.MaxValue),
    };

    public static ulong RandULong(Random rng) => rng.Next(3) switch
    {
        0 => ULongEdges[rng.Next(ULongEdges.Length)],
        1 => (ulong)rng.Next(0, 1000),
        _ => (ulong)rng.NextInt64(long.MinValue, long.MaxValue) ^ (ulong)rng.NextInt64(),
    };

    public static uint RandUInt(Random rng) => rng.Next(3) switch
    {
        0 => UIntEdges[rng.Next(UIntEdges.Length)],
        1 => (uint)rng.Next(0, 1000),
        _ => (uint)rng.NextInt64(0, (long)uint.MaxValue + 1),
    };

    public static string RandString(Random rng, bool large = false)
    {
        switch (rng.Next(12))
        {
            case 0: return "";
            case 1:
            case 2:
            case 3: return Ascii(rng, rng.Next(1, 21));
            case 4: return Ascii(rng, rng.Next(40, 47));                        // around the 42 char boundary
            case 5: return new string('€', rng.Next(40, 46));              // 3-byte chars: 42 -> 126 bytes, 43 -> 129 bytes
            case 6: return Ascii(rng, rng.Next(80, 320));
            case 7:
            case 8: return Unicode(rng, rng.Next(1, 40));
            case 9: return "key/" + Ascii(rng, rng.Next(0, 30)) + "/" + Unicode(rng, rng.Next(0, 6));
            case 10: return Ascii(rng, rng.Next(5400, 5530));                   // around the 5461 char boundary
            default: return large ? Ascii(rng, rng.Next(70_000, 80_000)) : Ascii(rng, rng.Next(1, 8));
        }
    }

    private static string Ascii(Random rng, int length)
    {
        return string.Create(length, rng.Next(), static (span, seed) =>
        {
            var r = new Random(seed);
            for (int i = 0; i < span.Length; i++) span[i] = (char)r.Next('!', '~' + 1);
        });
    }

    public static string Unicode(Random rng, int codePoints)
    {
        var sb = new StringBuilder();
        for (int i = 0; i < codePoints; i++)
        {
            switch (rng.Next(5))
            {
                case 0: sb.Append((char)rng.Next('a', 'z' + 1)); break;
                case 1: sb.Append((char)rng.Next(0x80, 0x800)); break;                  // 2-byte
                case 2: sb.Append((char)rng.Next(0x800, 0xD800)); break;                // 3-byte (below surrogates)
                case 3: sb.Append((char)rng.Next(0xE000, 0xFFFD)); break;               // 3-byte (above surrogates)
                default: sb.Append(char.ConvertFromUtf32(rng.Next(0x10000, 0x110000))); break; // surrogate pair
            }
        }
        return sb.ToString();
    }

    public static byte[] RandBytes(Random rng, bool large = false)
    {
        int len = rng.Next(14) switch
        {
            0 or 1 => 0,
            2 or 3 => rng.Next(1, 16),
            4 or 5 or 6 => rng.Next(16, 300),
            7 or 8 => rng.Next(300, 5000),
            9 => 127 + rng.Next(0, 3),
            10 => 16383 + rng.Next(0, 3),
            11 => large ? rng.Next(65_000, 140_000) : rng.Next(1, 40),
            _ => rng.Next(0, 100),
        };
        var b = new byte[len];
        rng.NextBytes(b);
        return b;
    }

    // ---- generation ------------------------------------------------------------------------------------------------

    public static T Generate<T>(Random rng, GenOptions? options = null) where T : class, new()
        => (T)GenerateObject(typeof(T), rng, options ?? new GenOptions());

    public static object GenerateObject(Type type, Random rng, GenOptions options)
    {
        object instance = Activator.CreateInstance(type)!;
        Fill(instance, rng, options, 0);
        return instance;
    }

    private static void Fill(object instance, Random rng, GenOptions o, int depth)
    {
        var nullability = new NullabilityInfoContext();
        foreach (FieldInfo f in instance.GetType().GetFields(Pub))
        {
            bool nullable = nullability.Create(f).ReadState == NullabilityState.Nullable;
            object? v = GenValue(f.FieldType, nullable, rng, o, depth);
            f.SetValue(instance, v);
        }
        foreach (PropertyInfo p in instance.GetType().GetProperties(Pub))
        {
            if (p.GetIndexParameters().Length != 0) continue;
            Type t = p.PropertyType;
            if (IsList(t, out Type? elem))
            {
                var list = (IList)p.GetValue(instance)!;
                int n = ListCount(rng, o, depth);
                for (int i = 0; i < n; i++) list.Add(GenValue(elem!, false, rng, o, depth + 1));
            }
            else if (IsDictionary(t, out Type? valueType))
            {
                var dict = (IDictionary)p.GetValue(instance)!;
                int n = rng.Next(0, 4);
                for (int i = 0; i < n; i++)
                {
                    string key = i == 0 && o.EmptyMapKeys && rng.Next(8) == 0 ? "" : "ns-" + i + "-" + RandString(rng);
                    dict[key] = GenValue(valueType!, false, rng, o, depth + 1);
                }
            }
            else if (p.CanWrite)
            {
                p.SetValue(instance, GenValue(t, nullability.Create(p).ReadState == NullabilityState.Nullable, rng, o, depth));
            }
        }
    }

    private static int ListCount(Random rng, GenOptions o, int depth)
    {
        int cap = depth >= 2 ? Math.Min(3, o.MaxList) : o.MaxList;
        return rng.Next(10) switch
        {
            0 or 1 or 2 or 3 => 0,
            4 or 5 or 6 => 1,
            _ => rng.Next(0, cap + 1),
        };
    }

    private static object? GenValue(Type t, bool nullable, Random rng, GenOptions o, int depth)
    {
        Type? underlying = Nullable.GetUnderlyingType(t);
        if (underlying is not null)
        {
            return rng.NextDouble() < o.Presence ? GenValue(underlying, false, rng, o, depth) : null;
        }
        if (nullable && !t.IsValueType && rng.NextDouble() >= o.Presence) return null;

        if (t == typeof(string)) return RandString(rng, o.Large);
        if (t == typeof(long)) return RandLong(rng);
        if (t == typeof(ulong)) return RandULong(rng);
        if (t == typeof(uint)) return RandUInt(rng);
        if (t == typeof(int)) return (int)RandLong(rng);
        if (t == typeof(bool)) return rng.Next(2) == 1;
        if (t.IsEnum)
        {
            if (rng.Next(20) == 0) return Enum.ToObject(t, rng.Next(-3, 40));
            Array values = Enum.GetValues(t);
            return values.GetValue(rng.Next(values.Length));
        }
        if (t == typeof(ReadOnlyMemory<byte>)) return new ReadOnlyMemory<byte>(RandBytes(rng, o.Large));
        if (t == typeof(byte[])) return RandBytes(rng, o.Large);
        if (t == typeof(ulong[]))
        {
            var a = new ulong[rng.Next(0, 6)];
            for (int i = 0; i < a.Length; i++) a[i] = RandULong(rng);
            return a;
        }
        if (IsList(t, out Type? elem))
        {
            var list = (IList)Activator.CreateInstance(t)!;
            int n = ListCount(rng, o, depth);
            for (int i = 0; i < n; i++) list.Add(GenValue(elem!, false, rng, o, depth + 1));
            return list;
        }
        if (t.IsClass)
        {
            object inst = Activator.CreateInstance(t)!;
            Fill(inst, rng, o, depth + 1);
            return inst;
        }
        throw new NotSupportedException("Cannot generate " + t);
    }

    private static bool IsList(Type t, out Type? element)
    {
        if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(List<>))
        {
            element = t.GetGenericArguments()[0];
            return true;
        }
        element = null;
        return false;
    }

    private static bool IsDictionary(Type t, out Type? value)
    {
        if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(Dictionary<,>))
        {
            value = t.GetGenericArguments()[1];
            return true;
        }
        value = null;
        return false;
    }

    // ---- dump (structural equality) --------------------------------------------------------------------------------

    /// <summary>Canonical text of a hand-written message: presence-sensitive (null vs default), null/empty collections equal.</summary>
    public static string Dump(object? value)
    {
        var sb = new StringBuilder();
        DumpTo(sb, value);
        return sb.ToString();
    }

    private static void DumpTo(StringBuilder sb, object? v)
    {
        switch (v)
        {
            case null: sb.Append("null"); return;
            case string s: sb.Append('"').Append(s.Length).Append(':').Append(Convert.ToHexString(Encoding.UTF8.GetBytes(s))).Append('"'); return;
            case ReadOnlyMemory<byte> rom: DumpBytes(sb, rom.Span); return;
            case byte[] bytes: DumpBytes(sb, bytes); return;
            case ulong[] arr: sb.Append('[').AppendJoin(',', arr).Append(']'); return;
            case Enum e: sb.Append(Convert.ToInt64(e)); return;
            case IDictionary d:
                sb.Append('{');
                foreach (DictionaryEntry kv in d)
                {
                    DumpTo(sb, kv.Key);
                    sb.Append("=>");
                    DumpTo(sb, kv.Value);
                    sb.Append(';');
                }
                sb.Append('}');
                return;
            case IList l:
                sb.Append('[');
                foreach (object? e in l)
                {
                    DumpTo(sb, e);
                    sb.Append(',');
                }
                sb.Append(']');
                return;
        }
        Type t = v.GetType();
        if (t.IsPrimitive) { sb.Append(Convert.ToString(v, System.Globalization.CultureInfo.InvariantCulture)); return; }
        sb.Append(t.Name).Append('(');
        foreach (FieldInfo f in t.GetFields(Pub))
        {
            sb.Append(f.Name).Append('=');
            DumpMember(sb, f.GetValue(v), f.FieldType);
            sb.Append(' ');
        }
        foreach (PropertyInfo p in t.GetProperties(Pub))
        {
            if (p.GetIndexParameters().Length != 0) continue;
            sb.Append(p.Name).Append('=');
            DumpMember(sb, p.GetValue(v), p.PropertyType);
            sb.Append(' ');
        }
        sb.Append(')');
    }

    private static void DumpMember(StringBuilder sb, object? member, Type declared)
    {
        if (member is null)
        {
            // a null repeated field (List<T>? / ulong[]?) is the same thing on the wire as an empty one
            bool repeated = declared == typeof(ulong[]) || (declared.IsGenericType && declared.GetGenericTypeDefinition() == typeof(List<>));
            sb.Append(repeated ? "[]" : "null");
            return;
        }
        DumpTo(sb, member);
    }

    private static void DumpBytes(StringBuilder sb, ReadOnlySpan<byte> bytes)
    {
        sb.Append("bytes[").Append(bytes.Length).Append(':');
        sb.Append(Convert.ToHexString(SHA256.HashData(bytes))).Append(']');
    }

    // ---- hand -> oracle --------------------------------------------------------------------------------------------

    public static object ToOracle(object hand, Type oracleType)
    {
        object o = Activator.CreateInstance(oracleType)!;
        foreach (FieldInfo f in hand.GetType().GetFields(Pub)) SetOracle(o, oracleType, f.Name, f.GetValue(hand));
        foreach (PropertyInfo p in hand.GetType().GetProperties(Pub))
        {
            if (p.GetIndexParameters().Length != 0) continue;
            SetOracle(o, oracleType, p.Name, p.GetValue(hand));
        }
        return o;
    }

    private static void SetOracle(object oracle, Type oracleType, string name, object? value)
    {
        PropertyInfo op = oracleType.GetProperty(name) ?? throw new InvalidOperationException($"{oracleType.Name} has no property {name}");
        if (value is null) return;
        Type pt = op.PropertyType;

        if (value is byte[] rawBytes)
        {
            op.SetValue(oracle, ByteString.CopyFrom(rawBytes));
            return;
        }
        if (value is IDictionary dict)
        {
            var target = (IDictionary)op.GetValue(oracle)!;
            Type vt = pt.GetGenericArguments()[1];
            foreach (DictionaryEntry kv in dict) target.Add(kv.Key, ConvertValue(kv.Value, vt));
            return;
        }
        if (value is IList list)
        {
            var target = (IList)op.GetValue(oracle)!;
            Type et = pt.GetGenericArguments()[0];
            foreach (object? e in list) target.Add(ConvertValue(e, et));
            return;
        }
        if (value is ulong[] arr)
        {
            var target = (IList)op.GetValue(oracle)!;
            foreach (ulong e in arr) target.Add(e);
            return;
        }
        op.SetValue(oracle, ConvertValue(value, pt));
    }

    private static object? ConvertValue(object? value, Type target)
    {
        if (value is null) return null;
        if (value is ReadOnlyMemory<byte> rom) return ByteString.CopyFrom(rom.Span);
        if (value is byte[] bytes) return ByteString.CopyFrom(bytes);
        if (value.GetType().IsEnum) return Enum.ToObject(target, Convert.ToInt32(value));
        if (value.GetType().IsPrimitive || value is string) return value;
        // nested message
        return ToOracle(value, target);
    }
}

/// <summary>Hand-codec + oracle binding of one message type, usable without generics from theories.</summary>
public interface IMessageKind
{
    string Name { get; }
    object NewHand();
    object Random(Random rng, GenOptions options);
    int Size(object hand);
    byte[] Encode(object hand);
    byte[] EncodeChunked(object hand, int chunk);
    void WriteInto(object hand, IBufferWriter<byte> output);
    void Reset(object hand);
    void ParseInto(object hand, ReadOnlySpan<byte> data);
    void ParseInto(object hand, in ReadOnlySequence<byte> data);
    byte[] OracleEncode(object hand);
    int OracleSize(object hand);
    /// <summary>true if the oracle parses <paramref name="bytes"/> into a message equal to ToOracle(<paramref name="hand"/>).</summary>
    bool OracleParseEquals(byte[] bytes, object hand);
    /// <summary>Parses with the oracle and returns the oracle message as text (diagnostics).</summary>
    string OracleText(byte[] bytes);
}

public sealed class Kind<TCodec, THand, TOracle> : IMessageKind
    where TCodec : IMessageCodec<THand>
    where THand : class, new()
    where TOracle : class, IMessage<TOracle>, new()
{
    private static readonly MessageParser<TOracle> OracleParser = new(static () => new TOracle());

    public string Name => typeof(THand).Name;
    public object NewHand() => new THand();
    public object Random(Random rng, GenOptions options) => ProtoReflection.Generate<THand>(rng, options);
    public int Size(object hand) => TCodec.Size((THand)hand);

    public byte[] Encode(object hand)
    {
        var w = new ArrayBufferWriter<byte>();
        MessageCodec.Write<TCodec, THand>((THand)hand, w);
        return w.WrittenSpan.ToArray();
    }

    public byte[] EncodeChunked(object hand, int chunk)
    {
        var w = new ChunkedBufferWriter(chunk);
        MessageCodec.Write<TCodec, THand>((THand)hand, w);
        return w.ToArray();
    }

    public void WriteInto(object hand, IBufferWriter<byte> output) => MessageCodec.Write<TCodec, THand>((THand)hand, output);
    public void Reset(object hand) => TCodec.Reset((THand)hand);
    public void ParseInto(object hand, ReadOnlySpan<byte> data) => MessageCodec.Parse<TCodec, THand>(data, (THand)hand);
    public void ParseInto(object hand, in ReadOnlySequence<byte> data) => MessageCodec.Parse<TCodec, THand>(in data, (THand)hand);
    public byte[] OracleEncode(object hand) => ((TOracle)ProtoReflection.ToOracle(hand, typeof(TOracle))).ToByteArray();
    public int OracleSize(object hand) => ((TOracle)ProtoReflection.ToOracle(hand, typeof(TOracle))).CalculateSize();
    public bool OracleParseEquals(byte[] bytes, object hand) => OracleParser.ParseFrom(bytes).Equals((TOracle)ProtoReflection.ToOracle(hand, typeof(TOracle)));
    public string OracleText(byte[] bytes) => OracleParser.ParseFrom(bytes).ToString() ?? "";
}

public static class MessageKinds
{
    public static readonly IMessageKind[] All =
    {
        new Kind<ShardAssignmentsRequestCodec, ShardAssignmentsRequest, G.ShardAssignmentsRequest>(),
        new Kind<Int32HashRangeCodec, Int32HashRange, G.Int32HashRange>(),
        new Kind<ShardAssignmentCodec, ShardAssignment, G.ShardAssignment>(),
        new Kind<NamespaceShardsAssignmentCodec, NamespaceShardsAssignment, G.NamespaceShardsAssignment>(),
        new Kind<ShardAssignmentsCodec, ShardAssignments, G.ShardAssignments>(),
        new Kind<SecondaryIndexCodec, SecondaryIndex, G.SecondaryIndex>(),
        new Kind<ProtoVersionCodec, ProtoVersion, G.Version>(),
        new Kind<PutRequestCodec, PutRequest, G.PutRequest>(),
        new Kind<PutResponseCodec, PutResponse, G.PutResponse>(),
        new Kind<DeleteRequestCodec, DeleteRequest, G.DeleteRequest>(),
        new Kind<DeleteResponseCodec, DeleteResponse, G.DeleteResponse>(),
        new Kind<DeleteRangeRequestCodec, DeleteRangeRequest, G.DeleteRangeRequest>(),
        new Kind<DeleteRangeResponseCodec, DeleteRangeResponse, G.DeleteRangeResponse>(),
        new Kind<WriteRequestCodec, WriteRequest, G.WriteRequest>(),
        new Kind<WriteResponseCodec, WriteResponse, G.WriteResponse>(),
        new Kind<GetRequestCodec, GetRequest, G.GetRequest>(),
        new Kind<GetResponseCodec, GetResponse, G.GetResponse>(),
        new Kind<ReadRequestCodec, ReadRequest, G.ReadRequest>(),
        new Kind<ReadResponseCodec, ReadResponse, G.ReadResponse>(),
        new Kind<ListRequestCodec, ListRequest, G.ListRequest>(),
        new Kind<ListResponseCodec, ListResponse, G.ListResponse>(),
        new Kind<RangeScanRequestCodec, RangeScanRequest, G.RangeScanRequest>(),
        new Kind<RangeScanResponseCodec, RangeScanResponse, G.RangeScanResponse>(),
        new Kind<GetSequenceUpdatesRequestCodec, GetSequenceUpdatesRequest, G.GetSequenceUpdatesRequest>(),
        new Kind<GetSequenceUpdatesResponseCodec, GetSequenceUpdatesResponse, G.GetSequenceUpdatesResponse>(),
        new Kind<CreateSessionRequestCodec, CreateSessionRequest, G.CreateSessionRequest>(),
        new Kind<CreateSessionResponseCodec, CreateSessionResponse, G.CreateSessionResponse>(),
        new Kind<SessionHeartbeatCodec, SessionHeartbeat, G.SessionHeartbeat>(),
        new Kind<KeepAliveResponseCodec, KeepAliveResponse, G.KeepAliveResponse>(),
        new Kind<CloseSessionRequestCodec, CloseSessionRequest, G.CloseSessionRequest>(),
        new Kind<CloseSessionResponseCodec, CloseSessionResponse, G.CloseSessionResponse>(),
        new Kind<NotificationsRequestCodec, NotificationsRequest, G.NotificationsRequest>(),
        new Kind<NotificationCodec, Oxia.Common.Proto.Notification, G.Notification>(),
        new Kind<NotificationEntryCodec, NotificationEntry, G.NotificationEntry>(),
        new Kind<NotificationBatchCodec, NotificationBatch, G.NotificationBatch>(),
    };

    public static IMessageKind Get(string name) => All.Single(k => k.Name == name);

    public static IEnumerable<object[]> Names() => All.Select(k => new object[] { k.Name });
}

// ---- byte plumbing -----------------------------------------------------------------------------------------------------

/// <summary>Builds multi-segment <see cref="ReadOnlySequence{T}"/>s.</summary>
public static class Segments
{
    private sealed class Segment : ReadOnlySequenceSegment<byte>
    {
        public Segment(ReadOnlyMemory<byte> memory) => Memory = memory;

        public Segment Append(ReadOnlyMemory<byte> memory)
        {
            var next = new Segment(memory) { RunningIndex = RunningIndex + Memory.Length };
            Next = next;
            return next;
        }
    }

    public static ReadOnlySequence<byte> Create(IReadOnlyList<byte[]> parts)
    {
        Segment? first = null, last = null;
        foreach (byte[] part in parts)
        {
            // pad the backing array so that segments do not start at offset 0 of their array
            var backing = new byte[part.Length + 3];
            part.CopyTo(backing, 1);
            ReadOnlyMemory<byte> mem = backing.AsMemory(1, part.Length);
            if (first is null) first = last = new Segment(mem);
            else last = last!.Append(mem);
        }
        if (first is null) return ReadOnlySequence<byte>.Empty;
        return new ReadOnlySequence<byte>(first, 0, last!, last!.Memory.Length);
    }

    /// <summary>Splits at the given ascending offsets (empty segments allowed when offsets repeat).</summary>
    public static ReadOnlySequence<byte> Split(byte[] data, params int[] offsets)
    {
        var parts = new List<byte[]>();
        int prev = 0;
        foreach (int o in offsets)
        {
            parts.Add(data[prev..o]);
            prev = o;
        }
        parts.Add(data[prev..]);
        return Create(parts);
    }

    public static ReadOnlySequence<byte> Chunks(byte[] data, int size)
    {
        var parts = new List<byte[]>();
        for (int i = 0; i < data.Length; i += size) parts.Add(data[i..Math.Min(data.Length, i + size)]);
        if (parts.Count == 0) parts.Add(Array.Empty<byte>());
        return Create(parts);
    }
}

/// <summary>An IBufferWriter that hands out small chunks, to exercise the writer's segment-boundary logic.</summary>
public sealed class ChunkedBufferWriter : IBufferWriter<byte>
{
    private readonly int _chunk;
    private readonly List<byte[]> _chunks = new();
    private readonly List<int> _used = new();
    private byte[]? _current;

    public ChunkedBufferWriter(int chunk) => _chunk = chunk;

    public int GetSpanCalls { get; private set; }

    public void Advance(int count)
    {
        if (_current is null) throw new InvalidOperationException("Advance without GetSpan");
        _used[^1] += count;
        if (_used[^1] > _current.Length) throw new InvalidOperationException("Advanced past the span");
        _current = null;
    }

    public Memory<byte> GetMemory(int sizeHint = 0) => throw new NotSupportedException();

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        GetSpanCalls++;
        _current = new byte[Math.Max(sizeHint, _chunk)];
        _chunks.Add(_current);
        _used.Add(0);
        return _current;
    }

    public byte[] ToArray()
    {
        var result = new List<byte>();
        for (int i = 0; i < _chunks.Count; i++) result.AddRange(_chunks[i].AsSpan(0, _used[i]).ToArray());
        return result.ToArray();
    }
}

/// <summary>Independent, schema-less protobuf scanner (does not share code with the codec under test).</summary>
public static class WireScanner
{
    /// <summary>Offsets (including 0 and data.Length) at which a prefix of <paramref name="data"/> is a complete message.</summary>
    public static List<int> TopLevelBoundaries(byte[] data)
    {
        var result = new List<int> { 0 };
        int pos = 0;
        while (pos < data.Length)
        {
            ulong tag = Varint(data, ref pos);
            switch ((int)(tag & 7))
            {
                case 0: Varint(data, ref pos); break;
                case 1: pos += 8; break;
                case 2: { int len = (int)Varint(data, ref pos); pos += len; break; }
                case 5: pos += 4; break;
                default: throw new InvalidOperationException("scanner: unsupported wire type in generated data");
            }
            result.Add(pos);
        }
        if (pos != data.Length) throw new InvalidOperationException("scanner: generated data is itself truncated");
        return result;
    }

    private static ulong Varint(byte[] data, ref int pos)
    {
        ulong r = 0;
        for (int shift = 0; ; shift += 7)
        {
            byte b = data[pos++];
            r |= (ulong)(b & 0x7F) << shift;
            if (b < 0x80) return r;
        }
    }
}

/// <summary>Tiny protobuf byte builder for hand-crafted wire inputs.</summary>
public sealed class WireBuilder
{
    private readonly List<byte> _bytes = new();

    public WireBuilder Raw(params byte[] bytes) { _bytes.AddRange(bytes); return this; }
    public WireBuilder RawVarint(ulong v) { while (v >= 0x80) { _bytes.Add((byte)(v | 0x80)); v >>= 7; } _bytes.Add((byte)v); return this; }
    public WireBuilder Tag(int field, int wireType) => RawVarint((ulong)((field << 3) | wireType));
    public WireBuilder Varint(int field, ulong v) => Tag(field, 0).RawVarint(v);
    public WireBuilder Fixed32(int field, uint v) { Tag(field, 5); _bytes.AddRange(BitConverter.GetBytes(v)); return this; }
    public WireBuilder Fixed64(int field, ulong v) { Tag(field, 1); _bytes.AddRange(BitConverter.GetBytes(v)); return this; }
    public WireBuilder Bytes(int field, byte[] b) { Tag(field, 2).RawVarint((ulong)b.Length); _bytes.AddRange(b); return this; }
    public WireBuilder String(int field, string s) => Bytes(field, Encoding.UTF8.GetBytes(s));
    public WireBuilder Message(int field, WireBuilder inner) => Bytes(field, inner.ToArray());
    public WireBuilder Group(int field, WireBuilder inner) { Tag(field, 3); _bytes.AddRange(inner.ToArray()); Tag(field, 4); return this; }
    public byte[] ToArray() => _bytes.ToArray();
}

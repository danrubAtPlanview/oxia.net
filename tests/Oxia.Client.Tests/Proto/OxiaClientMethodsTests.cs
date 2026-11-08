// =====================================================================================================================
// PORT NOTE (tests)
//   Upstream : common/proto/client_grpc.pb.go (OxiaClient_ServiceDesc: method names, streaming flags)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The Method<TReq,TResp> descriptors of OxiaClientMethods agree with the protoc-generated service descriptor
//              (full names, method types, message types), and every marshaller round-trips random messages through the
//              gRPC serialization contexts (single segment and multi-segment payloads).
//   Divergences: n/a (new tests)
// =====================================================================================================================
using System.Buffers;
using System.Reflection;
using Google.Protobuf;
using Grpc.Core;
using Oxia.Common.Proto;
using Xunit;
using G = Io.Oxia.Proto.V1;

namespace Oxia.Client.Tests.Proto;

public class OxiaClientMethodsTests
{
    private static IEnumerable<(string Name, object Method)> OurMethods()
    {
        foreach (FieldInfo f in typeof(OxiaClientMethods).GetFields(BindingFlags.Public | BindingFlags.Static))
        {
            if (f.FieldType.IsGenericType && f.FieldType.GetGenericTypeDefinition() == typeof(Method<,>))
            {
                yield return (f.Name, f.GetValue(null)!);
            }
        }
    }

    [Fact]
    public void EveryRpcOfTheServiceHasADescriptor_WithTheSameNameAndShape()
    {
        var generated = G.OxiaClient.Descriptor.Methods;
        var ours = OurMethods().ToDictionary(m => m.Name, m => m.Method);
        Assert.Equal(generated.Count, ours.Count);
        Assert.Equal(11, ours.Count);

        foreach (var d in generated)
        {
            Assert.True(ours.TryGetValue(d.Name, out object? method), $"missing descriptor for {d.Name}");
            Type t = method!.GetType();
            string fullName = (string)t.GetProperty("FullName")!.GetValue(method)!;
            var type = (MethodType)t.GetProperty("Type")!.GetValue(method)!;
            string service = (string)t.GetProperty("ServiceName")!.GetValue(method)!;
            Assert.Equal("/io.oxia.proto.v1.OxiaClient/" + d.Name, fullName);
            Assert.Equal("io.oxia.proto.v1.OxiaClient", service);
            Assert.Equal(d.FullName, "io.oxia.proto.v1.OxiaClient." + d.Name);

            MethodType expected = (d.IsClientStreaming, d.IsServerStreaming) switch
            {
                (false, false) => MethodType.Unary,
                (true, false) => MethodType.ClientStreaming,
                (false, true) => MethodType.ServerStreaming,
                _ => MethodType.DuplexStreaming,
            };
            Assert.Equal(expected, type);

            Type[] args = t.GetGenericArguments();
            Assert.Equal(d.InputType.Name, args[0].Name);
            Assert.Equal(d.OutputType.Name, args[1].Name);
        }
    }

    [Fact]
    public void SpecificMethodTypes()
    {
        Assert.Equal(MethodType.ServerStreaming, OxiaClientMethods.GetShardAssignments.Type);
        Assert.Equal(MethodType.Unary, OxiaClientMethods.Write.Type);
        Assert.Equal(MethodType.DuplexStreaming, OxiaClientMethods.WriteStream.Type);
        Assert.Equal(MethodType.ServerStreaming, OxiaClientMethods.Read.Type);
        Assert.Equal(MethodType.ServerStreaming, OxiaClientMethods.List.Type);
        Assert.Equal(MethodType.ServerStreaming, OxiaClientMethods.RangeScan.Type);
        Assert.Equal(MethodType.ServerStreaming, OxiaClientMethods.GetSequenceUpdates.Type);
        Assert.Equal(MethodType.ServerStreaming, OxiaClientMethods.GetNotifications.Type);
        Assert.Equal(MethodType.Unary, OxiaClientMethods.CreateSession.Type);
        Assert.Equal(MethodType.Unary, OxiaClientMethods.KeepAlive.Type);
        Assert.Equal(MethodType.Unary, OxiaClientMethods.CloseSession.Type);
        Assert.Equal("/io.oxia.proto.v1.OxiaClient/WriteStream", OxiaClientMethods.WriteStream.FullName);
    }

    [Fact]
    public void EveryMarshaller_RoundTripsThroughTheGrpcContexts()
    {
        var rng = new Random(2024);
        int marshallers = 0;
        foreach (FieldInfo f in typeof(OxiaClientMethods).GetFields(BindingFlags.Public | BindingFlags.Static))
        {
            if (!f.FieldType.IsGenericType || f.FieldType.GetGenericTypeDefinition() != typeof(Marshaller<>)) continue;
            marshallers++;
            Type messageType = f.FieldType.GetGenericArguments()[0];
            object marshaller = f.GetValue(null)!;
            IMessageKind kind = MessageKinds.Get(messageType.Name);
            var serializer = (Delegate)f.FieldType.GetProperty("ContextualSerializer")!.GetValue(marshaller)!;
            var deserializer = (Delegate)f.FieldType.GetProperty("ContextualDeserializer")!.GetValue(marshaller)!;

            for (int i = 0; i < 40; i++)
            {
                object message = kind.Random(rng, new GenOptions());
                var ctx = new FakeSerializationContext(new ArrayBufferWriter<byte>());
                serializer.DynamicInvoke(message, ctx);
                Assert.True(ctx.Completed, messageType.Name);
                Assert.Equal(ctx.Output.WrittenCount, ctx.DeclaredLength);
                byte[] bytes = ctx.Output.WrittenSpan.ToArray();
                Assert.Equal(kind.Encode(message), bytes);

                foreach (ReadOnlySequence<byte> payload in new[] { new ReadOnlySequence<byte>(bytes), Segments.Chunks(bytes, 5), Segments.Split(bytes, bytes.Length / 2) })
                {
                    object back = deserializer.DynamicInvoke(new FakeDeserializationContext(payload))!;
                    Assert.Equal(ProtoReflection.Dump(message), ProtoReflection.Dump(back));
                }
            }
        }
        Assert.Equal(20, marshallers);
    }
}

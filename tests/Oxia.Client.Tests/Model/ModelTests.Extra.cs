// =====================================================================================================================
// PORT NOTE (extra tests -- no Go counterpart; they never replace the ported ones)
//   Upstream : oxia/internal/model/model.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : the class-based calls lose Go's value-copy semantics; these tests pin down what replaces it: Clone / CopyFrom
//              must copy EVERY field of the wire request (a reflection test fails when a field is added to PutRequest & co
//              without being copied -- the metrics decorator relies on it), what is routing-only stays out of the wire
//              message, Complete goes through the callback, InOpIndexOrder is stable.
// =====================================================================================================================
using System.Reflection;
using Oxia.Client.Internal.Model;
using Oxia.Common.Proto;
using Xunit;

namespace Oxia.Client.Tests.Model;

public class ModelExtraTests
{
    /// <summary>Gives every field of the object a non-default value, recursively simple types only.</summary>
    private static void Populate(object target, Type type)
    {
        int seed = 1;
        foreach (var field in type.GetFields(BindingFlags.Public | BindingFlags.Instance))
        {
            var t = field.FieldType;
            object? value;
            if (t == typeof(string)) value = "v" + seed;
            else if (t == typeof(long?)) value = (long?)(100 + seed);
            else if (t == typeof(long)) value = (long)(100 + seed);
            else if (t == typeof(uint)) value = (uint)(100 + seed);
            else if (t == typeof(bool)) value = true;
            else if (t == typeof(ReadOnlyMemory<byte>)) value = new ReadOnlyMemory<byte>(new byte[] { 1, 2, (byte)seed });
            else if (t == typeof(ulong[])) value = new ulong[] { 7, (ulong)seed };
            else if (t == typeof(List<SecondaryIndex>)) value = new List<SecondaryIndex> { new() };
            else if (t == typeof(KeyComparisonType)) value = KeyComparisonType.Higher;
            else throw new NotSupportedException($"extend Populate: {type.Name}.{field.Name} is a {t}"); // a new field must be copied by Clone
            field.SetValue(target, value);
            seed++;
        }
    }

    private static void AssertSameWireFields(object expected, object actual, Type wireType)
    {
        foreach (var field in wireType.GetFields(BindingFlags.Public | BindingFlags.Instance))
        {
            var e = field.GetValue(expected);
            var a = field.GetValue(actual);
            if (e is ReadOnlyMemory<byte> em)
            {
                Assert.True(em.Span.SequenceEqual(((ReadOnlyMemory<byte>)a!).Span), field.Name);
            }
            else if (e is ulong[] ea)
            {
                Assert.Equal(ea, (ulong[])a!);
            }
            else
            {
                Assert.Equal(e, a);
            }
        }
    }

    [Fact]
    public void PutCall_CloneCopiesEveryField()
    {
        var call = new PutCall { CallContext = new CallContext(CancellationToken.None), Callback = (_, _) => { } };
        Populate(call, typeof(PutRequest));

        var clone = call.Clone();

        Assert.NotSame(call, clone);
        AssertSameWireFields(call, clone, typeof(PutRequest));
        Assert.Same(call.CallContext, clone.CallContext);
        Assert.Same(call.Callback, clone.Callback);
    }

    [Fact]
    public void DeleteCall_CloneCopiesEveryField()
    {
        var call = new DeleteCall { CallContext = new CallContext(CancellationToken.None), Callback = (_, _) => { } };
        Populate(call, typeof(DeleteRequest));

        var clone = call.Clone();

        AssertSameWireFields(call, clone, typeof(DeleteRequest));
        Assert.Same(call.CallContext, clone.CallContext);
        Assert.Same(call.Callback, clone.Callback);
    }

    [Fact]
    public void DeleteRangeCall_CloneCopiesEveryField_IncludingTheRoutingOnlyPartitionKey()
    {
        var call = new DeleteRangeCall { CallContext = new CallContext(CancellationToken.None), Callback = (_, _) => { } };
        Populate(call, typeof(DeleteRangeRequest));
        call.PartitionKey = "routing";

        var clone = call.Clone();

        AssertSameWireFields(call, clone, typeof(DeleteRangeRequest));
        Assert.Equal("routing", clone.PartitionKey);
        Assert.Same(call.CallContext, clone.CallContext);
        Assert.Same(call.Callback, clone.Callback);

        // Each clone has its own op index: one delete range can go to several batches
        clone.OpIndex = 9;
        Assert.NotEqual(call.OpIndex, clone.OpIndex);
    }

    [Fact]
    public void GetCall_CloneCopiesEveryField()
    {
        var call = new GetCall { Callback = (_, _) => { } };
        Populate(call, typeof(GetRequest));
        call.PartitionKey = "routing";

        var clone = call.Clone();

        AssertSameWireFields(call, clone, typeof(GetRequest));
        Assert.Equal("routing", clone.PartitionKey);
        Assert.Same(call.Callback, clone.Callback);
    }

    [Fact]
    public void RoutingOnlyFieldsAreNotFieldsOfTheWireMessages()
    {
        // Go's ToProto leaves them out: the same must hold for the classes that ARE the wire messages
        Assert.Null(typeof(DeleteRangeRequest).GetField("PartitionKey"));
        Assert.Null(typeof(GetRequest).GetField("PartitionKey"));
    }

    [Fact]
    public void Complete_InvokesTheCallback_AndAPooledSubclassCanOverrideIt()
    {
        PutResponse? got = null;
        Exception? gotErr = null;
        var put = new PutCall { Callback = (r, e) => { got = r; gotErr = e; } };
        var response = new PutResponse();
        put.Complete(response, null);
        Assert.Same(response, got);
        Assert.Null(gotErr);

        var error = new Exception("x");
        put.Complete(null, error);
        Assert.Null(got);
        Assert.Same(error, gotErr);

        var overridden = new OverridingPutCall();
        overridden.Complete(response, null);
        Assert.Same(response, overridden.Seen);

        // Complete on a call without callback is a no-op
        new PutCall().Complete(null, error);
    }

    private sealed class OverridingPutCall : PutCall
    {
        public PutResponse? Seen;
        public override void Complete(PutResponse? response, Exception? error) => Seen = response;
    }

    [Fact]
    public void InOpIndexOrder_IsStableForEqualIndexes_AndSortsEachKindBySlot()
    {
        var put = new PutCall { Key = "put", OpIndex = 3 };
        var delete = new DeleteCall { Key = "delete", OpIndex = 3 };
        var range = new DeleteRangeCall { MinKeyInclusive = "range", OpIndex = 3 };
        var first = new DeleteCall { Key = "first", OpIndex = 0 };
        var put2 = new PutCall { Key = "put2", OpIndex = 3 };

        var ordered = ModelExtensions.InOpIndexOrder([put, put2], [delete, first], [range]);

        // equal indexes keep puts, then deletes, then delete ranges (and the order within a list)
        Assert.Equal(new object[] { first, put, put2, delete, range }, ordered);
    }

    [Fact]
    public void InOpIndexOrder_SortsLargeBatches()
    {
        var puts = new List<PutCall>();
        var deletes = new List<DeleteCall>();
        var ranges = new List<DeleteRangeCall>();
        for (uint i = 0; i < 3000; i++)
        {
            switch (i % 3)
            {
                case 0: puts.Add(new PutCall { Key = "p" + i, OpIndex = i }); break;
                case 1: deletes.Add(new DeleteCall { Key = "d" + i, OpIndex = i }); break;
                default: ranges.Add(new DeleteRangeCall { MinKeyInclusive = "r" + i, OpIndex = i }); break;
            }
        }

        var ordered = ModelExtensions.InOpIndexOrder(puts, deletes, ranges);

        Assert.Equal(3000, ordered.Count);
        for (int i = 0; i < ordered.Count; i++)
        {
            uint opIndex = ordered[i] switch
            {
                PutCall p => p.OpIndex,
                DeleteCall d => d.OpIndex,
                DeleteRangeCall r => r.OpIndex,
                _ => throw new InvalidOperationException(),
            };
            Assert.Equal((uint)i, opIndex);
        }
    }

    [Fact]
    public void ResetClearsTheCallFields()
    {
        var put = new PutCall { Key = "k", CallContext = new CallContext(CancellationToken.None), Callback = (_, _) => { } };
        put.Reset();
        Assert.Equal("", put.Key);
        Assert.Null(put.CallContext);
        Assert.Null(put.Callback);

        var range = new DeleteRangeCall { PartitionKey = "p", CallContext = new CallContext(CancellationToken.None), Callback = (_, _) => { } };
        range.Reset();
        Assert.Null(range.PartitionKey);
        Assert.Null(range.CallContext);
        Assert.Null(range.Callback);

        var get = new GetCall { PartitionKey = "p", Callback = (_, _) => { } };
        get.Reset();
        Assert.Null(get.PartitionKey);
        Assert.Null(get.Callback);
    }
}

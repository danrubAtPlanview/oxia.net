// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/internal/model/model_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests : 7 (TestDeleteCallPartitionKey, TestDeleteCallPartitionKeyOrKeyFallback, TestDeleteCallEmptyPartitionKey,
//              TestGetCallPartitionKey, TestGetCallPartitionKeyOrKeyFallback, TestGetCallEmptyPartitionKey, TestInOpIndexOrder)
//              ported: 7   skipped: 0
//   Adaptations: `*string` PartitionKey -> string? (null == nil); `req.GetPartitionKey()` -> req.PartitionKey; a call IS its
//              own proto request, so the Go struct equality of TestInOpIndexOrder (`assert.Equal(PutCall{...}, ...)`) becomes
//              reference identity of the very objects that were passed in (plus their keys / op indexes).
// =====================================================================================================================
using Oxia.Client.Internal.Model;
using Xunit;

namespace Oxia.Client.Tests.Model;

public class ModelTests
{
    [Fact]
    public void TestDeleteCallPartitionKey()
    {
        var partitionKey = "partition-key";
        var call = new DeleteCall
        {
            Key = "key",
            PartitionKey = partitionKey,
        };

        Assert.Equal(partitionKey, call.PartitionKeyOrKey());
        var req = call.ToProto();
        Assert.NotNull(req.PartitionKey);
        Assert.Equal(partitionKey, req.PartitionKey);
    }

    [Fact]
    public void TestDeleteCallPartitionKeyOrKeyFallback()
    {
        var call = new DeleteCall { Key = "key" };

        Assert.Equal("key", call.PartitionKeyOrKey());
        Assert.Null(call.ToProto().PartitionKey);
    }

    [Fact]
    public void TestDeleteCallEmptyPartitionKey()
    {
        var partitionKey = "";
        var call = new DeleteCall
        {
            Key = "key",
            PartitionKey = partitionKey,
        };

        Assert.Equal(partitionKey, call.PartitionKeyOrKey());
        var req = call.ToProto();
        Assert.NotNull(req.PartitionKey);
        Assert.Equal(partitionKey, req.PartitionKey);
    }

    [Fact]
    public void TestGetCallPartitionKey()
    {
        var partitionKey = "partition-key";
        var call = new GetCall
        {
            Key = "key",
            PartitionKey = partitionKey,
        };

        Assert.Equal(partitionKey, call.PartitionKeyOrKey());
    }

    [Fact]
    public void TestGetCallPartitionKeyOrKeyFallback()
    {
        var call = new GetCall { Key = "key" };

        Assert.Equal("key", call.PartitionKeyOrKey());
    }

    [Fact]
    public void TestGetCallEmptyPartitionKey()
    {
        var partitionKey = "";
        var call = new GetCall
        {
            Key = "key",
            PartitionKey = partitionKey,
        };

        Assert.Equal(partitionKey, call.PartitionKeyOrKey());
    }

    [Fact]
    public void TestInOpIndexOrder()
    {
        var put0 = new PutCall { Key = "put-0", OpIndex = 0 };
        var put2 = new PutCall { Key = "put-2", OpIndex = 2 };
        var delete1 = new DeleteCall { Key = "delete-1", OpIndex = 1 };
        var delete4 = new DeleteCall { Key = "delete-4", OpIndex = 4 };
        var range3 = new DeleteRangeCall { MinKeyInclusive = "range-3", OpIndex = 3 };

        var calls = ModelExtensions.InOpIndexOrder(
            [put0, put2],
            [delete1, delete4],
            [range3]);

        Assert.Equal(5, calls.Count);
        Assert.Same(put0, calls[0]);
        Assert.Same(delete1, calls[1]);
        Assert.Same(put2, calls[2]);
        Assert.Same(range3, calls[3]);
        Assert.Same(delete4, calls[4]);

        Assert.Empty(ModelExtensions.InOpIndexOrder([], [], []));
    }
}

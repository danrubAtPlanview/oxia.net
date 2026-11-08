// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/options_put_test.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Go tests: 3, ported: 3, skipped: 0
//   Adaptations: newPutOptions(opts) is PutOptions.TryCreate(span, out result) returning the error; `proto.PutResponse{Status:
//                INVALID_ARGUMENT}` is Oxia.Common.Proto.PutResponse{Status = Status.InvalidArgument}; assert.ErrorIs is
//                OxiaErrors.Is; assert.ErrorContains checks the exception message.
// =====================================================================================================================
using Oxia.Common.Constant;
using Xunit;
using P = Oxia.Common.Proto;
using static Oxia.Client.OxiaOptions;

namespace Oxia.Client.Tests.Options;

public class OptionsPutTests
{
    [Fact]
    public void TestSecondaryIndexNameValidation()
    {
        var err = PutOptions.TryCreate([SecondaryIndex("tenant/users", "email")], out _);
        Assert.NotNull(err);
        Assert.True(OxiaErrors.Is(err, ClientErrors.InvalidOptions));
        Assert.Contains("must not contain '/'", err.Message);

        err = PutOptions.TryCreate([SecondaryIndex("tenant", "users/email")], out _);
        Assert.Null(err);
    }

    [Fact]
    public void TestInvalidArgumentStatus()
    {
        var result = ProtoUtils.ToPutResult("key", new P.PutResponse { Status = P.Status.InvalidArgument });
        Assert.True(OxiaErrors.Is(result.Err, ClientErrors.InvalidOptions));
    }

    [Fact]
    public void TestSequenceKeysDeltasValidation()
    {
        var err = PutOptions.TryCreate([SequenceKeysDeltas(1)], out _);
        Assert.True(OxiaErrors.Is(err, ClientErrors.InvalidOptions));
        Assert.Contains("requires PartitionKey()", err!.Message);

        err = PutOptions.TryCreate([SequenceKeysDeltas(1), PartitionKey("p"), ExpectedVersionId(1)], out _);
        Assert.True(OxiaErrors.Is(err, ClientErrors.InvalidOptions));
        Assert.Contains("does not allow to specify an ExpectedVersionId", err!.Message);

        err = PutOptions.TryCreate([SequenceKeysDeltas(0, 1), PartitionKey("p")], out _);
        Assert.True(OxiaErrors.Is(err, ClientErrors.InvalidOptions));
        Assert.Contains("first delta in sequence keys delta must always be > 0", err!.Message);

        err = PutOptions.TryCreate([SequenceKeysDeltas(1, 0), PartitionKey("p")], out var putOpts);
        Assert.Null(err);
        Assert.Equal(new ulong[] { 1, 0 }, putOpts.SequenceKeysDeltas);
    }
}

// =====================================================================================================================
// PORT NOTE   (extra tests -- not in Go)
//   Upstream : oxia/options_*.go behaviour that the Go tests do not pin down
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The option subtyping/conversion matrix (Go's interface embedding as implicit conversions), the application
//              order (last wins), the defaults, the validation order and messages, Go's %q formatting, the internal call
//              context option, and the zero-allocation property of building and resolving options.
// =====================================================================================================================
using Oxia.Client.Internal.Model;
using Oxia.Common.Constant;
using Oxia.Common.Proto;
using Xunit;
using static Oxia.Client.OxiaOptions;

namespace Oxia.Client.Tests.Options;

public class OptionsMatrixExtraTests
{
    // ---------------------------------------------------------------------------------------------------------------
    // Subtyping
    // ---------------------------------------------------------------------------------------------------------------

    [Fact]
    public void DeleteOption_IsAlsoAPutOption()
    {
        DeleteOption d = ExpectedVersionId(7);
        PutOption p = d;

        Assert.Null(PutOptions.TryCreate([p], out var put));
        Assert.Equal(7L, put.ExpectedVersion);

        Assert.Null(PutOptions.TryCreate([ExpectedVersionId(8)], out put));   // directly, through the implicit conversion
        Assert.Equal(8L, put.ExpectedVersion);

        Assert.Equal(7L, DeleteOptions.Create([d]).ExpectedVersion);
    }

    [Fact]
    public void ExpectedRecordNotExists_IsVersionMinusOne_AndOnlyAPutOption()
    {
        Assert.Null(PutOptions.TryCreate([ExpectedRecordNotExists()], out var put));
        Assert.Equal(RecordVersion.VersionIdNotExists, put.ExpectedVersion);
    }

    [Fact]
    public void ListOption_IsAlsoAGetAndRangeScanOption()
    {
        ListOption u = UseIndex("idx");
        GetOption g = u;
        RangeScanOption r = u;

        Assert.Equal("idx", ListOptions.Create([u]).SecondaryIndexName);
        Assert.Equal("idx", GetOptions.Create([g]).SecondaryIndexName);
        Assert.Equal("idx", RangeScanOptions.Create([r]).SecondaryIndexName);
        Assert.Equal("idx", GetOptions.Create([UseIndex("idx")]).SecondaryIndexName);
        Assert.Equal("idx", RangeScanOptions.Create([UseIndex("idx")]).SecondaryIndexName);
    }

    [Fact]
    public void ReadOption_IsAlsoAGetAndRangeScanOption_AndDefaultsToIncludeValue()
    {
        Assert.True(GetOptions.Create(default).IncludeValue);
        Assert.True(RangeScanOptions.Create(default).IncludeValue);

        Assert.False(GetOptions.Create([IncludeValue(false)]).IncludeValue);
        Assert.False(RangeScanOptions.Create([IncludeValue(false)]).IncludeValue);

        ReadOption ro = IncludeValue(false);
        GetOption g = ro;
        RangeScanOption r = ro;
        Assert.False(GetOptions.Create([g]).IncludeValue);
        Assert.False(RangeScanOptions.Create([r]).IncludeValue);
    }

    [Fact]
    public void BaseOption_IsAcceptedByEveryOperation()
    {
        BaseOption b = PartitionKey("pk");

        Assert.Equal("pk", PutOptions.TryCreate([b], out var put) is null ? put.PartitionKey : null);
        Assert.Equal("pk", DeleteOptions.Create([b]).PartitionKey);
        Assert.Equal("pk", DeleteRangeOptions.Create([b]).PartitionKey);
        Assert.Equal("pk", GetOptions.Create([b]).PartitionKey);
        Assert.Equal("pk", ListOptions.Create([b]).PartitionKey);
        Assert.Equal("pk", RangeScanOptions.Create([b]).PartitionKey);
        Assert.Equal("pk", GetSequenceUpdatesOptions.Create([b]).PartitionKey);
    }

    [Fact]
    public void EmptyPartitionKey_IsASetPartitionKey()
    {
        // Go: a *string pointing to "" is non-nil -- sequence keys accept it
        Assert.Null(PutOptions.TryCreate([SequenceKeysDeltas(1), PartitionKey("")], out var put));
        Assert.Equal("", put.PartitionKey);
    }

    [Fact]
    public void DefaultOptionStructs_AreNoOps()
    {
        Assert.Null(PutOptions.TryCreate([default(PutOption), default(DeleteOption), default(BaseOption)], out var put));
        Assert.Equal(default, put);
        Assert.Equal(DeleteOptions.Create(default), DeleteOptions.Create([default(DeleteOption)]));
        Assert.Equal(GetOptions.Create(default), GetOptions.Create([default(GetOption), default(ListOption), default(ReadOption)]));
        Assert.Equal(RangeScanOptions.Create(default), RangeScanOptions.Create([default(RangeScanOption)]));
        Assert.Equal(ListOptions.Create(default), ListOptions.Create([default(ListOption)]));
        Assert.Equal(default, DeleteRangeOptions.Create([default(DeleteRangeOption)]));
        Assert.Equal(default, GetSequenceUpdatesOptions.Create([default(GetSequenceUpdatesOption)]));
    }

    [Fact]
    public void NullArguments_ThrowArgumentNull()
    {
        Assert.Throws<ArgumentNullException>(() => PartitionKey(null!));
        Assert.Throws<ArgumentNullException>(() => UseIndex(null!));
        Assert.Throws<ArgumentNullException>(() => SecondaryIndex(null!, "k"));
        Assert.Throws<ArgumentNullException>(() => SecondaryIndex("i", null!));
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Application order: last wins
    // ---------------------------------------------------------------------------------------------------------------

    [Fact]
    public void Put_LastOptionWins()
    {
        Assert.Null(PutOptions.TryCreate([PartitionKey("a"), PartitionKey("b"), ExpectedVersionId(1), ExpectedRecordNotExists(), ExpectedVersionId(3)], out var put));
        Assert.Equal("b", put.PartitionKey);
        Assert.Equal(3L, put.ExpectedVersion);

        Assert.Null(PutOptions.TryCreate([ExpectedVersionId(3), ExpectedRecordNotExists()], out put));
        Assert.Equal(-1L, put.ExpectedVersion);

        Assert.Null(PutOptions.TryCreate([SequenceKeysDeltas(5), SequenceKeysDeltas(1, 2), PartitionKey("p")], out put));
        Assert.Equal(new ulong[] { 1, 2 }, put.SequenceKeysDeltas);

        Assert.Null(PutOptions.TryCreate([Ephemeral(), Ephemeral()], out put));
        Assert.True(put.Ephemeral);
        Assert.Null(PutOptions.TryCreate(default, out put));
        Assert.False(put.Ephemeral);
    }

    [Fact]
    public void Put_SecondaryIndexesAccumulateInOrder_AndAllocateOnlyWhenUsed()
    {
        Assert.Null(PutOptions.TryCreate([Ephemeral(), PartitionKey("p")], out var put));
        Assert.Null(put.SecondaryIndexes);

        Assert.Null(PutOptions.TryCreate([SecondaryIndex("i1", "k1"), Ephemeral(), SecondaryIndex("i1", "k2"), SecondaryIndex("i2", "k3")], out put));
        Assert.Equal(
            new[] { new SecondaryIdxOption("i1", "k1"), new SecondaryIdxOption("i1", "k2"), new SecondaryIdxOption("i2", "k3") },
            put.SecondaryIndexes);
    }

    [Fact]
    public void Get_LastOptionWins_AndListOnlyOptionsAreNoOps()
    {
        var o = GetOptions.Create([
            ComparisonFloor(), ComparisonHigher(), IncludeValue(false), IncludeValue(true), IncludeValue(false),
            UseIndex("a"), UseIndex("b"), PartitionKey("x"), PartitionKey("y"),
            ShowInternalKeys(true), EndInclusive()]);

        Assert.Equal(KeyComparisonType.Higher, o.ComparisonType);
        Assert.False(o.IncludeValue);
        Assert.Equal("b", o.SecondaryIndexName);
        Assert.Equal("y", o.PartitionKey);

        Assert.Equal(KeyComparisonType.Equal, GetOptions.Create(default).ComparisonType);
        foreach (var (opt, expected) in new (GetOption, KeyComparisonType)[]
                 {
                     (ComparisonEqual(), KeyComparisonType.Equal),
                     (ComparisonFloor(), KeyComparisonType.Floor),
                     (ComparisonCeiling(), KeyComparisonType.Ceiling),
                     (ComparisonLower(), KeyComparisonType.Lower),
                     (ComparisonHigher(), KeyComparisonType.Higher),
                 })
        {
            Assert.Equal(expected, GetOptions.Create([opt]).ComparisonType);
        }
    }

    [Fact]
    public void Get_InternalComparisonType_IsCarriedThroughUnvalidated()
    {
        var o = GetOptions.Create([OptionsInternal.ComparisonType((KeyComparisonType)100)]);
        Assert.Equal((KeyComparisonType)100, o.ComparisonType);
    }

    [Fact]
    public void ListAndRangeScan_LastOptionWins()
    {
        var l = ListOptions.Create([ShowInternalKeys(true), ShowInternalKeys(false), UseIndex("a"), UseIndex("b"), EndInclusive(), PartitionKey("p")]);
        Assert.False(l.ShowInternalKeys);
        Assert.Equal("b", l.SecondaryIndexName);
        Assert.True(l.EndInclusive);
        Assert.Equal("p", l.PartitionKey);

        var r = RangeScanOptions.Create([IncludeValue(false), IncludeValue(true), ShowInternalKeys(false), ShowInternalKeys(true), UseIndex("i"), EndInclusive(), PartitionKey("q")]);
        Assert.True(r.IncludeValue);
        Assert.True(r.ShowInternalKeys);
        Assert.Equal("i", r.SecondaryIndexName);
        Assert.True(r.EndInclusive);
        Assert.Equal("q", r.PartitionKey);

        var defaults = ListOptions.Create(default);
        Assert.Null(defaults.PartitionKey);
        Assert.Null(defaults.SecondaryIndexName);
        Assert.False(defaults.ShowInternalKeys);
        Assert.False(defaults.EndInclusive);
    }

    [Fact]
    public void Delete_LastOptionWins()
    {
        var d = DeleteOptions.Create([ExpectedVersionId(1), PartitionKey("a"), ExpectedVersionId(2), PartitionKey("b")]);
        Assert.Equal(2L, d.ExpectedVersion);
        Assert.Equal("b", d.PartitionKey);
        Assert.Null(DeleteOptions.Create(default).ExpectedVersion);
        Assert.Null(DeleteOptions.Create(default).PartitionKey);
        Assert.Null(DeleteRangeOptions.Create(default).PartitionKey);
        Assert.Null(GetSequenceUpdatesOptions.Create(default).PartitionKey);
        Assert.Equal("z", GetSequenceUpdatesOptions.Create([PartitionKey("y"), PartitionKey("z")]).PartitionKey);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Validation
    // ---------------------------------------------------------------------------------------------------------------

    [Fact]
    public void Put_ValidationOrderAndMessagesMatchGo()
    {
        // Secondary-index errors come before the sequence-keys errors; the first offending index wins
        var err = PutOptions.TryCreate([SequenceKeysDeltas(0), SecondaryIndex("ok", "k"), SecondaryIndex("a/b", "k"), SecondaryIndex("c/d", "k")], out var result);
        Assert.Equal("secondary index name \"a/b\" must not contain '/': invalid options", err!.Message);
        Assert.True(OxiaErrors.Is(err, ClientErrors.InvalidOptions));
        Assert.Equal(default, result);

        // PartitionKey is checked before ExpectedVersionId, which is checked before the first delta
        err = PutOptions.TryCreate([SequenceKeysDeltas(0), ExpectedVersionId(1)], out _);
        Assert.Equal("usage of sequential keys requires PartitionKey() to be set: invalid options", err!.Message);
        err = PutOptions.TryCreate([SequenceKeysDeltas(0), PartitionKey("p"), ExpectedVersionId(1)], out _);
        Assert.Equal("usage of sequential keys does not allow to specify an ExpectedVersionId: invalid options", err!.Message);
        err = PutOptions.TryCreate([SequenceKeysDeltas(0), PartitionKey("p")], out _);
        Assert.Equal("first delta in sequence keys delta must always be > 0: invalid options", err!.Message);

        // An empty deltas list is not "sequence keys"
        Assert.Null(PutOptions.TryCreate([SequenceKeysDeltas()], out _));
        Assert.Null(PutOptions.TryCreate([ExpectedVersionId(1), ExpectedRecordNotExists(), Ephemeral()], out _));
    }

    [Fact]
    public void GoQuote_FollowsStrconvQuote()
    {
        Assert.Equal("\"a/b\"", PutOptions.GoQuote("a/b"));
        Assert.Equal("\"\"", PutOptions.GoQuote(""));
        Assert.Equal("\"say \\\"hi\\\"\"", PutOptions.GoQuote("say \"hi\""));
        Assert.Equal("\"back\\\\slash\"", PutOptions.GoQuote("back\\slash"));
        Assert.Equal("\"l1\\nl2\\tx\\r\\a\\b\\f\\v\"", PutOptions.GoQuote("l1\nl2\tx\r\a\b\f\v"));
        Assert.Equal("\"\\x00\\x01\\x7f\"", PutOptions.GoQuote("\0\u0001\u007f"));
        Assert.Equal("\"caf\u00e9 \u4e16\u754c\"", PutOptions.GoQuote("caf\u00e9 \u4e16\u754c"));   // printable non-ASCII is kept
        Assert.Equal("\"\\u200b\"", PutOptions.GoQuote("\u200b"));                                  // zero-width space: not printable
        Assert.Equal("\"\\u00a0\"", PutOptions.GoQuote("\u00a0"));                                  // no-break space: not printable
        Assert.Equal("\"\U0001F600\"", PutOptions.GoQuote("\U0001F600"));                          // printable astral rune is kept
        Assert.Equal("\"\\U000e0001\"", PutOptions.GoQuote("\U000E0001"));                         // astral format char: escaped with \U
        Assert.Equal("\"\\ufffd\"", PutOptions.GoQuote("\ud800"));                                  // lone surrogate
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Internal call context options
    // ---------------------------------------------------------------------------------------------------------------

    [Fact]
    public void CallContextOptions_CarryTheCallContextToPutDeleteAndDeleteRange()
    {
        var cc = new CallContext(CancellationToken.None);

        Assert.Null(PutOptions.TryCreate([CallContextOptions.ForPut(cc), PartitionKey("p")], out var put));
        Assert.Same(cc, put.CallContext);
        Assert.Equal("p", put.PartitionKey);

        var d = DeleteOptions.Create([CallContextOptions.ForDelete(cc), ExpectedVersionId(4)]);
        Assert.Same(cc, d.CallContext);
        Assert.Equal(4L, d.ExpectedVersion);

        var dr = DeleteRangeOptions.Create([CallContextOptions.ForDeleteRange(cc)]);
        Assert.Same(cc, dr.CallContext);

        Assert.Null(PutOptions.TryCreate(default, out put));
        Assert.Null(put.CallContext);
        Assert.Null(DeleteOptions.Create(default).CallContext);
        Assert.Null(DeleteRangeOptions.Create(default).CallContext);

        // the call context set by the sync client is not overridden by a later user option of the same operation
        // (the user cannot even construct one), and other options keep their normal precedence
        Assert.Null(PutOptions.TryCreate([CallContextOptions.ForPut(cc), Ephemeral()], out put));
        Assert.Same(cc, put.CallContext);
        Assert.True(put.Ephemeral);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Allocation
    // ---------------------------------------------------------------------------------------------------------------

    private static long Allocated(Action action)
    {
        // warm up (JIT, tiering) then measure
        for (int i = 0; i < 3; i++) action();
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 1000; i++) action();
        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    private static int _sink;

    [Fact]
    public void BuildingAndResolvingOptions_AllocatesNothing()
    {
        // The measured actions are static lambdas (no closure); every option is a struct and the span is an inline array.
        Assert.Equal(0, Allocated(static () =>
        {
            var err = PutOptions.TryCreate([Ephemeral(), PartitionKey("p"), ExpectedVersionId(3), ExpectedRecordNotExists()], out var put);
            _sink += (err is null ? 1 : 0) + (put.Ephemeral ? 1 : 0) + (int)put.ExpectedVersion!.Value;
        }));

        Assert.Equal(0, Allocated(static () =>
        {
            var d = DeleteOptions.Create([ExpectedVersionId(3), PartitionKey("p")]);
            var dr = DeleteRangeOptions.Create([PartitionKey("p")]);
            _sink += (int)d.ExpectedVersion!.Value + dr.PartitionKey!.Length;
        }));

        Assert.Equal(0, Allocated(static () =>
        {
            var g = GetOptions.Create([ComparisonFloor(), IncludeValue(false), UseIndex("idx"), ShowInternalKeys(true), EndInclusive(), PartitionKey("p")]);
            _sink += (int)g.ComparisonType + g.SecondaryIndexName!.Length;
        }));

        Assert.Equal(0, Allocated(static () =>
        {
            var l = ListOptions.Create([UseIndex("idx"), ShowInternalKeys(true), EndInclusive(), PartitionKey("p")]);
            var r = RangeScanOptions.Create([IncludeValue(false), UseIndex("idx"), EndInclusive(), PartitionKey("p")]);
            var s = GetSequenceUpdatesOptions.Create([PartitionKey("p")]);
            _sink += l.SecondaryIndexName!.Length + r.SecondaryIndexName!.Length + s.PartitionKey!.Length;
        }));

        Assert.Equal(0, Allocated(static () =>
        {
            var g = GetOptions.Create(default);
            var p = PutOptions.TryCreate(default, out var put);
            _sink += (g.IncludeValue ? 1 : 0) + (p is null ? 1 : 0) + (put.Ephemeral ? 1 : 0);
        }));
    }

    [Fact]
    public void ResolvingValidOptions_WithSecondaryIndexes_AllocatesOnlyTheIndexList()
    {
        long bytes = Allocated(static () =>
        {
            _ = PutOptions.TryCreate([SecondaryIndex("i", "k")], out var put);
            _sink += put.SecondaryIndexes!.Count;
        });
        Assert.True(bytes > 0);
        // one List<SecondaryIdxOption> (24 bytes header+fields) and its 2-slot backing array: well under 200 bytes per call
        Assert.True(bytes / 1000 < 200, $"{bytes / 1000} bytes per call");
    }

    [Fact]
    public void OptionStructs_AreSmall()
    {
        // kind + long + 2 strings + object: 40 bytes on 64-bit
        Assert.True(System.Runtime.CompilerServices.Unsafe.SizeOf<PutOption>() <= 40);
        Assert.Equal(System.Runtime.CompilerServices.Unsafe.SizeOf<PutOption>(), System.Runtime.CompilerServices.Unsafe.SizeOf<GetOption>());
    }
}

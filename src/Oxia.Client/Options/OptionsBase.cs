// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/options_base.go   (BaseOption, baseOptions, partitionKeyOpt, PartitionKey)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Go's functional options are interfaces with unexported `applyXxx(*xxxOptions)` methods; one option value
//              (e.g. Ephemeral()) implements the apply methods of every operation it is valid for, and an operation
//              accepts `...XxxOption`.  Interface embedding gives the subtyping (DeleteOption embeds PutOption,
//              BaseOption embeds every operation's option, ...).
//   Divergences:
//      * ALLOCATION.  Options are tiny immutable STRUCTS (one per Go option interface) that all wrap the same internal
//        <see cref="OptionPayload"/> (kind + long + two strings + object).  A call such as
//        `PutAsync(k, v, OxiaOptions.Ephemeral(), OxiaOptions.PartitionKey("p"))` therefore allocates nothing: the
//        options travel in a `params ReadOnlySpan<PutOption>` (stack/inline array).  The typed wrappers keep the compiler
//        enforcing which option fits which operation, like Go's per-operation interfaces.
//      * SUBTYPING.  Go's interface embedding is mapped to IMPLICIT CONVERSION operators between the wrappers (they copy
//        the payload): DeleteOption -> PutOption, ListOption -> GetOption / RangeScanOption, ReadOption -> GetOption /
//        RangeScanOption, BaseOption -> every operation option.  Which payload kinds an operation's `Apply` honours is
//        exactly the set of `applyXxx` methods Go defines for that operation (the other kinds are the no-ops of Go,
//        e.g. ShowInternalKeys/EndInclusive on a Get).
//      * `default(XxxOption)` carries OptionKind.None and is ignored by every operation (Go: a nil interface would panic).
//      * The resolved option structs (PutOptions, GetOptions, ...) are plain structs instead of pointers to structs, and
//        `baseOptionsIf.PartitionKey()` becomes the PartitionKey field.  Go's `*string` partition key is `string?`
//        (an empty string is a valid, set, partition key).
//      * PartitionKey(null) throws ArgumentNullException (Go cannot pass nil for a string).
// =====================================================================================================================
namespace Oxia.Client;

/// <summary>The identity of an option; every public option struct wraps one <see cref="OptionPayload"/>.</summary>
internal enum OptionKind : byte
{
    None = 0,
    PartitionKey,
    ExpectedVersionId,
    Ephemeral,
    SequenceKeysDeltas,
    SecondaryIndex,
    ComparisonType,
    IncludeValue,
    UseIndex,
    ShowInternalKeys,
    EndInclusive,
    CallContext,
}

/// <summary>
/// The single payload behind every option: kind + one long + two strings + one object.  Which fields are used depends on
/// the kind (PartitionKey: S1; ExpectedVersionId: Long; SecondaryIndex: S1 = index name, S2 = secondary key;
/// SequenceKeysDeltas: Obj = ulong[]; ComparisonType: Long; IncludeValue / ShowInternalKeys: Long = 0/1; UseIndex: S1;
/// CallContext: Obj).
/// </summary>
internal readonly struct OptionPayload
{
    public readonly OptionKind Kind;
    public readonly long Long;
    public readonly string? S1;
    public readonly string? S2;
    public readonly object? Obj;

    public OptionPayload(OptionKind kind, long l = 0, string? s1 = null, string? s2 = null, object? obj = null)
    {
        Kind = kind;
        Long = l;
        S1 = s1;
        S2 = s2;
        Obj = obj;
    }

    public bool Flag => Long != 0;
}

/// <summary>An option that applies to all the client operations (Go: BaseOption).</summary>
public readonly struct BaseOption
{
    internal readonly OptionPayload P;

    internal BaseOption(in OptionPayload p) => P = p;

    public static implicit operator PutOption(BaseOption o) => new(o.P);
    public static implicit operator DeleteOption(BaseOption o) => new(o.P);
    public static implicit operator DeleteRangeOption(BaseOption o) => new(o.P);
    public static implicit operator GetOption(BaseOption o) => new(o.P);
    public static implicit operator ListOption(BaseOption o) => new(o.P);
    public static implicit operator RangeScanOption(BaseOption o) => new(o.P);
    public static implicit operator GetSequenceUpdatesOption(BaseOption o) => new(o.P);
}

public static partial class OxiaOptions
{
    /// <summary>
    /// Overrides the partition routing with the specified <paramref name="partitionKey"/> instead of the regular record key.
    /// Records with the same partitionKey will always be guaranteed to be co-located in the same Oxia shard.
    /// </summary>
    public static BaseOption PartitionKey(string partitionKey)
    {
        ArgumentNullException.ThrowIfNull(partitionKey);
        return new BaseOption(new OptionPayload(OptionKind.PartitionKey, s1: partitionKey));
    }
}

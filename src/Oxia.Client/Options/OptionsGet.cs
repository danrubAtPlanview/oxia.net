// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/options_get.go   (getOptions, GetOption, newGetOptions, ComparisonEqual/Floor/Ceiling/Lower/Higher,
//              includeValue, ReadOption, IncludeValue)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The options of the Get operation.  `getOptions` embeds `listOptions` in Go, so the list options that make
//              sense for a Get are honoured here too: UseIndex sets SecondaryIndexName; ShowInternalKeys and EndInclusive
//              are explicit no-ops.  IncludeValue defaults to true.
//   Divergences:
//      * Struct options over the shared OptionPayload (see OptionsBase.cs); ReadOption (Go: interface embedding GetOption
//        and RangeScanOption) converts implicitly to both.  The ListOption -> GetOption conversion is in OptionsList.cs.
//      * `newGetOptions` returns the resolved struct (no pointer); GetOptions is a record struct so two resolved sets of
//        options compare by value (Go tests compare the structs with assert.Equal).
// =====================================================================================================================
using Oxia.Common.Proto;

namespace Oxia.Client;

/// <summary>Represents an option for the Get operation (Go: GetOption).</summary>
public readonly struct GetOption
{
    internal readonly OptionPayload P;

    internal GetOption(in OptionPayload p) => P = p;
}

/// <summary>An option of the operations that read records: Get and RangeScan (Go: ReadOption).</summary>
public readonly struct ReadOption
{
    internal readonly OptionPayload P;

    internal ReadOption(in OptionPayload p) => P = p;

    public static implicit operator GetOption(ReadOption o) => new(o.P);
    public static implicit operator RangeScanOption(ReadOption o) => new(o.P);
}

/// <summary>Go: getOptions + newGetOptions(opts).</summary>
internal record struct GetOptions
{
    public string? PartitionKey;
    public string? SecondaryIndexName;
    public KeyComparisonType ComparisonType;
    public bool IncludeValue;   // default true

    public static GetOptions Create(ReadOnlySpan<GetOption> options)
    {
        var o = new GetOptions { IncludeValue = true };
        foreach (ref readonly var opt in options)
        {
            ref readonly var p = ref opt.P;
            switch (p.Kind)
            {
                case OptionKind.PartitionKey:      // partitionKeyOpt.applyGet
                    o.PartitionKey = p.S1;
                    break;
                case OptionKind.ComparisonType:    // getComparisonType.applyGet
                    o.ComparisonType = (KeyComparisonType)p.Long;
                    break;
                case OptionKind.IncludeValue:      // includeValue.applyGet
                    o.IncludeValue = p.Flag;
                    break;
                case OptionKind.UseIndex:          // useIndex.applyGet
                    o.SecondaryIndexName = p.S1;
                    break;
                // showInternalKeys.applyGet / endInclusive.applyGet: no-ops
            }
        }

        return o;
    }
}

/// <summary>
/// Go's unexported option constructors, for tests and internal callers: Go tests build `&amp;getComparisonType{...}` directly,
/// with values (e.g. KeyComparisonType(100)) that the public ComparisonXxx() constructors can never produce.
/// </summary>
internal static class OptionsInternal
{
    /// <summary>Go: &amp;getComparisonType{t} -- no validation of <paramref name="t"/>; GetOptions.Create carries it through unchanged.</summary>
    public static GetOption ComparisonType(KeyComparisonType t) => new(new OptionPayload(OptionKind.ComparisonType, (long)t));
}

public static partial class OxiaOptions
{
    /// <summary>Sets the Get() operation to compare the stored key for equality.</summary>
    public static GetOption ComparisonEqual() => Comparison(KeyComparisonType.Equal);

    /// <summary>Makes the get operation search for the record whose key is the highest key &lt;= to the supplied key.</summary>
    public static GetOption ComparisonFloor() => Comparison(KeyComparisonType.Floor);

    /// <summary>Makes the get operation search for the record whose key is the lowest key >= to the supplied key.</summary>
    public static GetOption ComparisonCeiling() => Comparison(KeyComparisonType.Ceiling);

    /// <summary>Makes the get operation search for the record whose key is strictly &lt; to the supplied key.</summary>
    public static GetOption ComparisonLower() => Comparison(KeyComparisonType.Lower);

    /// <summary>Makes the get operation search for the record whose key is strictly > to the supplied key.</summary>
    public static GetOption ComparisonHigher() => Comparison(KeyComparisonType.Higher);

    private static GetOption Comparison(KeyComparisonType type) => new(new OptionPayload(OptionKind.ComparisonType, (long)type));

    /// <summary>
    /// Includes or excludes the values of the records that Get and RangeScan return.  Without the values, they return the
    /// keys and the versions.  Older servers send the values of a RangeScan anyway, and the client drops them.
    /// </summary>
    public static ReadOption IncludeValue(bool include) => new(new OptionPayload(OptionKind.IncludeValue, include ? 1 : 0));
}

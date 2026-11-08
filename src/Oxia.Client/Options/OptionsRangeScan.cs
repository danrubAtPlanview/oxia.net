// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/options_range_scan.go   (rangeScanOptions, RangeScanOption, newRangeScanOptions)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The options of the RangeScan operation.  `rangeScanOptions` embeds `listOptions` and adds IncludeValue
//              (default true).
//   Divergences: Struct options over the shared OptionPayload (see OptionsBase.cs).  The flat C# struct has the fields of
//              `listOptions` directly; the list options are applied through ListOptions.Apply so List and RangeScan share
//              the exact same semantics.
// =====================================================================================================================
namespace Oxia.Client;

/// <summary>Represents an option for the RangeScan operation (Go: RangeScanOption).</summary>
public readonly struct RangeScanOption
{
    internal readonly OptionPayload P;

    internal RangeScanOption(in OptionPayload p) => P = p;
}

/// <summary>Go: rangeScanOptions + newRangeScanOptions(opts).</summary>
internal record struct RangeScanOptions
{
    public string? PartitionKey;
    public string? SecondaryIndexName;
    public bool ShowInternalKeys;
    public bool EndInclusive;
    public bool IncludeValue;   // default true

    public static RangeScanOptions Create(ReadOnlySpan<RangeScanOption> options)
    {
        var list = new ListOptions();
        bool includeValue = true;
        foreach (ref readonly var opt in options)
        {
            ref readonly var p = ref opt.P;
            if (p.Kind == OptionKind.IncludeValue)   // includeValue.applyRangeScan
            {
                includeValue = p.Flag;
            }
            else
            {
                list.Apply(in p);                    // partitionKeyOpt / useIndex / showInternalKeys / endInclusive .applyRangeScan
            }
        }

        return new RangeScanOptions
        {
            PartitionKey = list.PartitionKey,
            SecondaryIndexName = list.SecondaryIndexName,
            ShowInternalKeys = list.ShowInternalKeys,
            EndInclusive = list.EndInclusive,
            IncludeValue = includeValue,
        };
    }
}

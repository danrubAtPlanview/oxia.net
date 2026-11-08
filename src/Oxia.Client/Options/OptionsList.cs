// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/options_list.go   (listOptions, ListOption, newListOptions, useIndex/UseIndex,
//              showInternalKeys/ShowInternalKeys, endInclusive/EndInclusive)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The options of the List operation.  Go's `ListOption` implements applyList, applyRangeScan AND applyGet, so
//              it is also usable as a GetOption and a RangeScanOption; `listOptions` is embedded in `getOptions` and
//              `rangeScanOptions`.
//   Divergences:
//      * Struct options over the shared OptionPayload (see OptionsBase.cs); the ListOption -> GetOption / RangeScanOption
//        subtyping is an implicit conversion.  Honoured kinds per operation: List/RangeScan: PartitionKey, UseIndex,
//        ShowInternalKeys, EndInclusive; Get: UseIndex only (the others are no-ops, as in Go).
//      * UseIndex(null) throws ArgumentNullException.
//      * ListOptions is a record struct so resolved options compare by value.
// =====================================================================================================================
namespace Oxia.Client;

/// <summary>Represents an option for the List operation; also usable on Get and RangeScan (Go: ListOption).</summary>
public readonly struct ListOption
{
    internal readonly OptionPayload P;

    internal ListOption(in OptionPayload p) => P = p;

    public static implicit operator GetOption(ListOption o) => new(o.P);
    public static implicit operator RangeScanOption(ListOption o) => new(o.P);
}

/// <summary>Go: listOptions + newListOptions(opts).</summary>
internal record struct ListOptions
{
    public string? PartitionKey;
    public string? SecondaryIndexName;
    public bool ShowInternalKeys;
    public bool EndInclusive;

    public static ListOptions Create(ReadOnlySpan<ListOption> options)
    {
        var o = new ListOptions();
        foreach (ref readonly var opt in options)
        {
            o.Apply(in opt.P);
        }

        return o;
    }

    /// <summary>The list options as applied by List and RangeScan (applyList / applyRangeScan).</summary>
    internal void Apply(in OptionPayload p)
    {
        switch (p.Kind)
        {
            case OptionKind.PartitionKey:
                PartitionKey = p.S1;
                break;
            case OptionKind.UseIndex:
                SecondaryIndexName = p.S1;
                break;
            case OptionKind.ShowInternalKeys:
                ShowInternalKeys = p.Flag;
                break;
            case OptionKind.EndInclusive:
                EndInclusive = true;
                break;
        }
    }
}

public static partial class OxiaOptions
{
    /// <summary>
    /// Selects the secondary index used to interpret keys and key ranges in Get, List, and RangeScan operations.  Index
    /// names must not contain '/'.  Returned record keys remain the corresponding primary keys.
    /// </summary>
    public static ListOption UseIndex(string indexName)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        return new ListOption(new OptionPayload(OptionKind.UseIndex, s1: indexName));
    }

    /// <summary>Controls whether to list Oxia's own internal keys.  By default, only user's keys are returned.</summary>
    public static ListOption ShowInternalKeys(bool show) => new(new OptionPayload(OptionKind.ShowInternalKeys, show ? 1 : 0));

    private static readonly ListOption EndInclusiveFlag = new(new OptionPayload(OptionKind.EndInclusive));

    /// <summary>
    /// Makes List and RangeScan include the end of the range: the record whose key is maxKeyExclusive, or with
    /// <see cref="UseIndex"/>, the records whose secondary key is maxKeyExclusive.  An empty maxKeyExclusive is then the
    /// empty key, rather than no bound.  With UseIndex, a range from a key to the same key returns the records whose
    /// secondary key is that key.
    /// The leader of a shard includes the end once all the servers of the shard support it.  Until then, List and
    /// RangeScan fail with <see cref="ClientErrors.EndInclusiveUnsupported"/>, rather than leave the end out.
    /// </summary>
    public static ListOption EndInclusive() => EndInclusiveFlag;
}

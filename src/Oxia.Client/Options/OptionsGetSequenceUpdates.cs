// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/options_get_sequence_updates.go   (getSequenceUpdatesOptions, GetSequenceUpdatesOption,
//              newGetSequenceUpdatesOptions)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The options of the GetSequenceUpdates operation; only the base option PartitionKey applies (and the client
//              requires it).
//   Divergences: Struct options over the shared OptionPayload (see OptionsBase.cs).
// =====================================================================================================================
namespace Oxia.Client;

/// <summary>Represents an option for the GetSequenceUpdates operation (Go: GetSequenceUpdatesOption).</summary>
public readonly struct GetSequenceUpdatesOption
{
    internal readonly OptionPayload P;

    internal GetSequenceUpdatesOption(in OptionPayload p) => P = p;
}

/// <summary>Go: getSequenceUpdatesOptions + newGetSequenceUpdatesOptions(opts).</summary>
internal struct GetSequenceUpdatesOptions
{
    public string? PartitionKey;

    public static GetSequenceUpdatesOptions Create(ReadOnlySpan<GetSequenceUpdatesOption> options)
    {
        var o = new GetSequenceUpdatesOptions();
        foreach (ref readonly var opt in options)
        {
            if (opt.P.Kind == OptionKind.PartitionKey)   // partitionKeyOpt.applyGetSequenceUpdates
            {
                o.PartitionKey = opt.P.S1;
            }
        }

        return o;
    }
}

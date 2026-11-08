// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/options_delete_range.go   (deleteRangeOptions, DeleteRangeOption, newDeleteRangeOptions)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The options of the DeleteRange operation.  Only the base option PartitionKey (and the internal call
//              context of the sync client) apply.
//   Divergences: Struct options over the shared OptionPayload (see OptionsBase.cs).
// =====================================================================================================================
using Oxia.Client.Internal.Model;

namespace Oxia.Client;

/// <summary>Represents an option for the DeleteRange operation (Go: DeleteRangeOption).</summary>
public readonly struct DeleteRangeOption
{
    internal readonly OptionPayload P;

    internal DeleteRangeOption(in OptionPayload p) => P = p;
}

/// <summary>Go: deleteRangeOptions + newDeleteRangeOptions(opts).</summary>
internal struct DeleteRangeOptions
{
    public string? PartitionKey;
    public CallContext? CallContext;

    public static DeleteRangeOptions Create(ReadOnlySpan<DeleteRangeOption> options)
    {
        var o = new DeleteRangeOptions();
        foreach (ref readonly var opt in options)
        {
            ref readonly var p = ref opt.P;
            switch (p.Kind)
            {
                case OptionKind.PartitionKey:        // partitionKeyOpt.applyDeleteRange
                    o.PartitionKey = p.S1;
                    break;
                case OptionKind.CallContext:         // callContextOption.applyDeleteRange
                    o.CallContext = (CallContext?)p.Obj;
                    break;
            }
        }

        return o;
    }
}

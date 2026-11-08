// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/options_delete.go   (deleteOptions, DeleteOption, newDeleteOptions, ExpectedVersionId, expectedVersionId)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : The options of the Delete operation.  Go's `DeleteOption` interface embeds `PutOption` and
//              `expectedVersionId` implements both applyPut and applyDelete: ExpectedVersionId(v) is usable on a Put too.
//   Divergences:
//      * Struct options over the shared OptionPayload (see OptionsBase.cs); the DeleteOption -> PutOption subtyping is an
//        implicit conversion operator.
//      * `newDeleteOptions` returns the resolved struct (no pointer).
// =====================================================================================================================
using Oxia.Client.Internal.Model;

namespace Oxia.Client;

/// <summary>Represents an option for the Delete operation (Go: DeleteOption, which is also a PutOption).</summary>
public readonly struct DeleteOption
{
    internal readonly OptionPayload P;

    internal DeleteOption(in OptionPayload p) => P = p;

    public static implicit operator PutOption(DeleteOption o) => new(o.P);
}

/// <summary>Go: deleteOptions + newDeleteOptions(opts).</summary>
internal struct DeleteOptions
{
    public string? PartitionKey;
    public long? ExpectedVersion;
    public CallContext? CallContext;

    public static DeleteOptions Create(ReadOnlySpan<DeleteOption> options)
    {
        var o = new DeleteOptions();
        foreach (ref readonly var opt in options)
        {
            ref readonly var p = ref opt.P;
            switch (p.Kind)
            {
                case OptionKind.PartitionKey:        // partitionKeyOpt.applyDelete
                    o.PartitionKey = p.S1;
                    break;
                case OptionKind.ExpectedVersionId:   // expectedVersionId.applyDelete
                    o.ExpectedVersion = p.Long;
                    break;
                case OptionKind.CallContext:         // callContextOption.applyDelete
                    o.CallContext = (CallContext?)p.Obj;
                    break;
            }
        }

        return o;
    }
}

public static partial class OxiaOptions
{
    /// <summary>
    /// Marks that the operation should only be successful if the versionId of the record stored in the server matches the
    /// expected one.  Usable on Put and Delete.
    /// </summary>
    public static DeleteOption ExpectedVersionId(long versionId) =>
        new(new OptionPayload(OptionKind.ExpectedVersionId, versionId));
}

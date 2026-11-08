// =====================================================================================================================
// PORT NOTE
//   Upstream : oxia/sync_client_impl.go   (callContextOption + its applyPut / applyDelete / applyDeleteRange)
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : An internal option that ties a write to the context of the SyncClient caller: once the caller's token is
//              done, the write is dropped unless it was sent already.  The sync client prepends it to the caller's options
//              (so a user option can never override it) and the resolved Put/Delete/DeleteRange options carry it as
//              `CallContext`.
//   Divergences: Struct options over the shared OptionPayload (see OptionsBase.cs); one factory per operation instead of
//              one Go type implementing three apply methods.  Prepending is free here: the sync client builds a small
//              stack span, no `append([]PutOption{...}, options...)` copy on the heap.
// =====================================================================================================================
using Oxia.Client.Internal.Model;

namespace Oxia.Client;

/// <summary>Internal options used by the sync client to tie a write to its caller's CancellationToken (Go: callContextOption).</summary>
internal static class CallContextOptions
{
    public static PutOption ForPut(CallContext callContext) => new(new OptionPayload(OptionKind.CallContext, obj: callContext));

    public static DeleteOption ForDelete(CallContext callContext) => new(new OptionPayload(OptionKind.CallContext, obj: callContext));

    public static DeleteRangeOption ForDeleteRange(CallContext callContext) =>
        new(new OptionPayload(OptionKind.CallContext, obj: callContext));
}

// =====================================================================================================================
// PORT NOTE
//   Upstream : common/failpoint/failpoint.go (+ registry.go)   [build tag !disable_trap => always enabled here]
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Test-only fault injection hooks.  A `Failpoints` instance holds named hooks (`Func<Exception?>`); production
//              code calls `Fail(name, err)` at the injection point, which returns the hook's error if it produces one,
//              otherwise the original error.  A null `Failpoints` has no hooks (Go: nil receiver is valid).
//              The client uses two names: "getWriteStream.closeOldStream" and "newCacheImpl.newCache".
//              The process-wide registry (Enable/Disable/Eval) is ported too (server tests use it; harmless for the client).
//   Divergences: methods on a possibly-nil *Failpoints (Trigger / Fail) are static methods taking a nullable instance; `func() error` hooks are
//              `Func<Exception?>`; `Eval[T](name) (T, bool)` is `Eval<T>(name, out T value)`; the `disable_trap` build flavour (failpoint_disable.go,
//              registry_disable.go) is not ported -- `Enabled` is the constant true.  Tests: Common/Failpoint/{FailpointTests,RegistryTests}.cs.
// =====================================================================================================================
using System.Collections.Concurrent;

namespace Oxia.Common.Failpoint;

public sealed class Failpoints
{
    public const bool Enabled = true;

    private readonly IReadOnlyDictionary<string, Func<Exception?>>? _hooks;

    public Failpoints(IReadOnlyDictionary<string, Func<Exception?>>? hooks) => _hooks = hooks;

    /// <summary>Go: failpoint.New(hooks).</summary>
    public static Failpoints New(IReadOnlyDictionary<string, Func<Exception?>>? hooks) => new(hooks);

    /// <summary>Runs the hook of the failpoint and returns its error, or null when it has no hook (null receiver allowed).</summary>
    public static Exception? Trigger(Failpoints? self, string name)
    {
        if (self?._hooks is { } hooks && hooks.TryGetValue(name, out var hook) && hook is not null) return hook();
        return null;
    }

    /// <summary>Returns <paramref name="err"/> unless the hook of the failpoint returns an error, which replaces it.</summary>
    public static Exception? Fail(Failpoints? self, string name, Exception? err) => Trigger(self, name) ?? err;

    // ----- registry.go ---------------------------------------------------------------------------------------------
    private static readonly ConcurrentDictionary<string, object> Registry = new();

    public static void Enable(string name, object value) => Registry[name] = value;
    public static void Disable(string name) => Registry.TryRemove(name, out _);

    public static bool Eval<T>(string name, out T value)
    {
        if (Registry.TryGetValue(name, out var v) && v is T typed) { value = typed; return true; }
        value = default!;
        return false;
    }
}

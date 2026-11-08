// =====================================================================================================================
// PORT NOTE   *** CONTRACT STUB -- owned by the "options & public types" component.  It REPLACES this file with the real
//                 implementation split like Go (OptionsBase.cs, OptionsPut.cs, OptionsGet.cs, OptionsDelete.cs,
//                 OptionsDeleteRange.cs, OptionsList.cs, OptionsRangeScan.cs, OptionsGetSequenceUpdates.cs,
//                 OptionsClient.cs, Optional.cs, Resolver.cs, FailureInjection.cs).  Keep every signature below. ***
//   Upstream : oxia/options_*.go, oxia/optional.go, oxia/resolver.go, oxia/failure_injection.go
//   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
//   Intent   : Go's functional options (interfaces with unexported apply*) become small immutable STRUCTS so a call such as
//              `PutAsync(k, v, OxiaOptions.Ephemeral(), OxiaOptions.PartitionKey("p"))` allocates nothing (params
//              ReadOnlySpan).  A single internal payload (kind + long + two strings + object) backs every option; the
//              per-operation structs are thin typed wrappers so the compiler enforces which option fits which operation,
//              exactly like Go's per-operation option interfaces, with the SAME subtyping:
//                 DeleteOption  -> also a PutOption      (Go: DeleteOption embeds PutOption)   [ExpectedVersionId]
//                 ListOption    -> also GetOption and RangeScanOption                           [UseIndex, ShowInternalKeys, EndInclusive]
//                 ReadOption    -> also GetOption and RangeScanOption                           [IncludeValue]
//                 BaseOption    -> accepted by every operation                                  [PartitionKey]
//              (implemented as implicit conversion operators).
//   Static factories (Go package-level funcs) live in `OxiaOptions` so tests can `using static Oxia.Client.OxiaOptions;`.
// =====================================================================================================================
using System.Diagnostics.Metrics;
using System.Net.Security;
using Oxia.Client.Internal.Model;
using Oxia.Common.Auth;
using Oxia.Common.Proto;

namespace Oxia.Client;

public readonly struct PutOption { }
public readonly struct DeleteOption { public static implicit operator PutOption(DeleteOption o) => default; }
public readonly struct DeleteRangeOption { }
public readonly struct GetOption { }
public readonly struct RangeScanOption { }
public readonly struct ListOption
{
    public static implicit operator GetOption(ListOption o) => default;
    public static implicit operator RangeScanOption(ListOption o) => default;
}
public readonly struct ReadOption
{
    public static implicit operator GetOption(ReadOption o) => default;
    public static implicit operator RangeScanOption(ReadOption o) => default;
}
public readonly struct GetSequenceUpdatesOption { }
public readonly struct BaseOption
{
    public static implicit operator PutOption(BaseOption o) => default;
    public static implicit operator DeleteOption(BaseOption o) => default;
    public static implicit operator DeleteRangeOption(BaseOption o) => default;
    public static implicit operator GetOption(BaseOption o) => default;
    public static implicit operator ListOption(BaseOption o) => default;
    public static implicit operator RangeScanOption(BaseOption o) => default;
    public static implicit operator GetSequenceUpdatesOption(BaseOption o) => default;
}

/// <summary>Go: ClientOption (apply(clientOptions) (clientOptions, error)).</summary>
public sealed class ClientOption { internal Func<ClientOptions, Exception?> Apply = _ => null; }

/// <summary>Go: oxia.Failure (failure_injection.go).</summary>
public enum Failure { DizzyShardManager = 1 }

/// <summary>Go: ServiceResolver (resolver.go).</summary>
public interface IServiceResolver
{
    string Scheme { get; }
    /// <summary>Resolve must return promptly; call <paramref name="updater"/> at least once, and again whenever addresses change.</summary>
    void Resolve(string endpoint, AddressUpdater updater);
}

/// <summary>Go: AddressUpdater -- returns an error if the resolver should try to resolve again.</summary>
public delegate Exception? AddressUpdater(IReadOnlyList<string> addresses);

/// <summary>Go: Optional[T] (optional.go).</summary>
public interface IOptional<T>
{
    bool Present { get; }
    bool Empty { get; }
    bool TryGet(out T value);
    T MustGet();
}

/// <summary>All package-level option constructors of the Go client.</summary>
public static class OxiaOptions
{
    // base
    public static BaseOption PartitionKey(string partitionKey) => throw new NotImplementedException();
    // put
    public static PutOption ExpectedRecordNotExists() => throw new NotImplementedException();
    public static PutOption Ephemeral() => throw new NotImplementedException();
    public static PutOption SequenceKeysDeltas(params ulong[] delta) => throw new NotImplementedException();
    public static PutOption SecondaryIndex(string indexName, string secondaryKey) => throw new NotImplementedException();
    // delete (also usable on put)
    public static DeleteOption ExpectedVersionId(long versionId) => throw new NotImplementedException();
    // get
    public static GetOption ComparisonEqual() => throw new NotImplementedException();
    public static GetOption ComparisonFloor() => throw new NotImplementedException();
    public static GetOption ComparisonCeiling() => throw new NotImplementedException();
    public static GetOption ComparisonLower() => throw new NotImplementedException();
    public static GetOption ComparisonHigher() => throw new NotImplementedException();
    public static ReadOption IncludeValue(bool include) => throw new NotImplementedException();
    // list / range scan
    public static ListOption UseIndex(string indexName) => throw new NotImplementedException();
    public static ListOption ShowInternalKeys(bool show) => throw new NotImplementedException();
    public static ListOption EndInclusive() => throw new NotImplementedException();
    // client
    public static ClientOption WithNamespace(string @namespace) => throw new NotImplementedException();
    public static ClientOption WithBatchLinger(TimeSpan batchLinger) => throw new NotImplementedException();
    public static ClientOption WithMaxRequestsPerBatch(int maxRequestsPerBatch) => throw new NotImplementedException();
    public static ClientOption WithRequestTimeout(TimeSpan requestTimeout) => throw new NotImplementedException();
    /// <summary>Go: WithMeterProvider(provider). .NET: an IMeterFactory (null => the default "oxia_client" Meter, discoverable by OpenTelemetry/MeterListener).</summary>
    public static ClientOption WithMeterFactory(IMeterFactory? meterFactory) => throw new NotImplementedException();
    /// <summary>Go: WithGlobalMeterProvider() -- the process-wide provider; .NET: the default global "oxia_client" Meter (MeterFactory = null).</summary>
    public static ClientOption WithGlobalMeterProvider() => throw new NotImplementedException();
    public static ClientOption WithSessionTimeout(TimeSpan sessionTimeout) => throw new NotImplementedException();
    public static ClientOption WithIdentity(string identity) => throw new NotImplementedException();
    public static ClientOption WithTls(SslClientAuthenticationOptions tls) => throw new NotImplementedException();
    public static ClientOption WithAuthentication(IAuthentication authentication) => throw new NotImplementedException();
    public static ClientOption WithFailureInjection(IEnumerable<Failure> failures) => throw new NotImplementedException();
    public static ClientOption WithSessionKeepAliveTicker(TimeSpan ticker) => throw new NotImplementedException();
    public static ClientOption WithDialResolver(IServiceResolver sr) => throw new NotImplementedException();

    // Defaults (options_client.go)
    public static readonly TimeSpan DefaultBatchLinger = TimeSpan.FromMilliseconds(5);
    public const int DefaultMaxRequestsPerBatch = 1000;
    public const int DefaultMaxBatchSize = 128 * 1024;
    public static readonly TimeSpan DefaultRequestTimeout = TimeSpan.FromSeconds(30);
    public static readonly TimeSpan DefaultSessionTimeout = TimeSpan.FromSeconds(15);
    public const string DefaultNamespace = "default";
}

// ---------------------------------------------------------------------------------------------------------------------
// Resolved (internal) options -- what newPutOptions()/newGetOptions()/... return in Go.
// ---------------------------------------------------------------------------------------------------------------------

/// <summary>Go: secondaryIdxOption.</summary>
internal readonly record struct SecondaryIdxOption(string IndexName, string SecondaryKey);

/// <summary>Go: putOptions + newPutOptions(opts) (validation errors returned, not thrown).</summary>
internal struct PutOptions
{
    public string? PartitionKey;
    public long? ExpectedVersion;
    public bool Ephemeral;
    public ulong[]? SequenceKeysDeltas;
    public List<SecondaryIdxOption>? SecondaryIndexes;
    public CallContext? CallContext;

    /// <summary>Applies the options in order and validates (index names without '/', sequence keys need PartitionKey and no ExpectedVersion and a first delta > 0).
    /// Returns the Go error (OxiaErrors.Wrap(ClientErrors.InvalidOptions, "...")) or null.</summary>
    public static Exception? TryCreate(ReadOnlySpan<PutOption> options, out PutOptions result) => throw new NotImplementedException();
}

internal struct DeleteOptions
{
    public string? PartitionKey;
    public long? ExpectedVersion;
    public CallContext? CallContext;
    public static DeleteOptions Create(ReadOnlySpan<DeleteOption> options) => throw new NotImplementedException();
}

internal struct DeleteRangeOptions
{
    public string? PartitionKey;
    public CallContext? CallContext;
    public static DeleteRangeOptions Create(ReadOnlySpan<DeleteRangeOption> options) => throw new NotImplementedException();
}

internal struct GetOptions
{
    public string? PartitionKey;
    public string? SecondaryIndexName;
    public KeyComparisonType ComparisonType;
    public bool IncludeValue;   // default true
    public static GetOptions Create(ReadOnlySpan<GetOption> options) => throw new NotImplementedException();
}

internal struct ListOptions
{
    public string? PartitionKey;
    public string? SecondaryIndexName;
    public bool ShowInternalKeys;
    public bool EndInclusive;
    public static ListOptions Create(ReadOnlySpan<ListOption> options) => throw new NotImplementedException();
}

internal struct RangeScanOptions
{
    public string? PartitionKey;
    public string? SecondaryIndexName;
    public bool ShowInternalKeys;
    public bool EndInclusive;
    public bool IncludeValue;   // default true
    public static RangeScanOptions Create(ReadOnlySpan<RangeScanOption> options) => throw new NotImplementedException();
}

internal struct GetSequenceUpdatesOptions
{
    public string? PartitionKey;
    public static GetSequenceUpdatesOptions Create(ReadOnlySpan<GetSequenceUpdatesOption> options) => throw new NotImplementedException();
}

/// <summary>Internal options used by the sync client to tie a write to its caller's CancellationToken (Go: callContextOption).</summary>
internal static class CallContextOptions
{
    public static PutOption ForPut(CallContext callContext) => throw new NotImplementedException();
    public static DeleteOption ForDelete(CallContext callContext) => throw new NotImplementedException();
    public static DeleteRangeOption ForDeleteRange(CallContext callContext) => throw new NotImplementedException();
}

/// <summary>Go: clientOptions (value type in Go; here a class, mutated only while options are applied).</summary>
internal sealed class ClientOptions
{
    public string ServiceAddress = "";
    public string Namespace = OxiaOptions.DefaultNamespace;
    public TimeSpan BatchLinger = OxiaOptions.DefaultBatchLinger;
    public int MaxRequestsPerBatch = OxiaOptions.DefaultMaxRequestsPerBatch;
    public int MaxBatchSize = OxiaOptions.DefaultMaxBatchSize;
    public TimeSpan RequestTimeout = OxiaOptions.DefaultRequestTimeout;
    public IMeterFactory? MeterFactory;
    public TimeSpan SessionTimeout = OxiaOptions.DefaultSessionTimeout;
    public string Identity = "";
    public SslClientAuthenticationOptions? Tls;
    public IAuthentication? Authentication;
    public TimeSpan SessionKeepAliveTicker;
    public HashSet<Failure> FailureInjection = new();
    public IServiceResolver? Resolver;

    /// <summary>Go: newClientOptions(serviceAddress, opts...).  All option errors are combined (AggregateException, like multierr) and returned, not thrown.
    /// Computed option: SessionKeepAliveTicker = max(SessionKeepAliveTicker, SessionTimeout/10).</summary>
    public static Exception? TryCreate(string serviceAddress, ReadOnlySpan<ClientOption> opts, out ClientOptions result)
        => throw new NotImplementedException();
}

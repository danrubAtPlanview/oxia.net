# Component C — options & public types

Upstream: `github.com/oxia-db/oxia` module `oxia/`, pinned at `8456f61e1ada7b786f5ef17798b45fbc17d0d246`.

Design in one paragraph: Go's functional options (interfaces with unexported `applyXxx`) are tiny immutable **structs** that all
wrap one internal `OptionPayload` (kind + long + two strings + object). Go's interface embedding (`DeleteOption` embeds
`PutOption`, `ListOption` also applies to Get/RangeScan, `ReadOption` = Get+RangeScan, `BaseOption` = everything) is a set of
implicit conversion operators; each operation's `Create`/`TryCreate` honours exactly the payload kinds for which Go defines
that operation's `applyXxx` (the others are Go's explicit no-ops). A call such as
`PutAsync(k, v, OxiaOptions.Ephemeral(), OxiaOptions.PartitionKey("p"))` allocates nothing.

## Source files

| Upstream file | C# file | Status | Notes (divergences, extra tests) |
|---|---|---|---|
| `oxia/options_base.go` | `src/Oxia.Client/Options/OptionsBase.cs` | ported | `BaseOption`, `OptionKind`, `OptionPayload`, `OxiaOptions.PartitionKey`. `PartitionKey(null)` throws `ArgumentNullException`. |
| `oxia/options_put.go` | `src/Oxia.Client/Options/OptionsPut.cs` | ported | `PutOptions.TryCreate` (validation order + messages identical, Go `%q` reproduced by `GoQuote`); the secondary-index list is allocated only when used; `SequenceKeysDeltas(params ulong[])` keeps the caller's array like Go's slice. |
| `oxia/options_delete.go` | `src/Oxia.Client/Options/OptionsDelete.cs` | ported | `DeleteOption -> PutOption` implicit conversion; `ExpectedVersionId`. |
| `oxia/options_delete_range.go` | `src/Oxia.Client/Options/OptionsDeleteRange.cs` | ported | |
| `oxia/options_get.go` | `src/Oxia.Client/Options/OptionsGet.cs` | ported | `GetOptions` (record struct, `IncludeValue` defaults true); `ReadOption -> GetOption/RangeScanOption`; `OptionsInternal.ComparisonType(t)` = Go's unexported `&getComparisonType{t}` (unvalidated, used by F2's tests). |
| `oxia/options_list.go` | `src/Oxia.Client/Options/OptionsList.cs` | ported | `ListOption -> GetOption/RangeScanOption`; on a Get only `UseIndex` has effect (`ShowInternalKeys`/`EndInclusive` are no-ops). |
| `oxia/options_range_scan.go` | `src/Oxia.Client/Options/OptionsRangeScan.cs` | ported | `IncludeValue` defaults true. |
| `oxia/options_get_sequence_updates.go` | `src/Oxia.Client/Options/OptionsGetSequenceUpdates.cs` | ported | |
| `oxia/sync_client_impl.go` (`callContextOption`) | `src/Oxia.Client/Options/CallContextOptions.cs` | ported | `ForPut/ForDelete/ForDeleteRange(CallContext)`; the only part of that Go file owned by C. |
| `oxia/options_client.go` | `src/Oxia.Client/Options/OptionsClient.cs` | ported | `ClientOptions.TryCreate` accumulates **all** errors (`MultiError : AggregateException`, message joined with `"; "` like `multierr`; a single error stays itself). Meter provider -> `IMeterFactory?` (null = default global `oxia_client` Meter, also what `WithGlobalMeterProvider()` selects). **Extension (not in Go):** `OxiaOptions.WithMaxBatchSize(int)` (Go has the default, the field and `ErrInvalidOptionMaxBatchSize`, but no option function). |
| `oxia/optional.go` | `src/Oxia.Client/Optional.cs` | ported | `IOptional<T>` (public), `Optional.Of/Empty` (internal); the empty optional is a shared instance per `T`; `MustGet` throws `InvalidOperationException("optional empty on MustGet call")`. |
| `oxia/failure_injection.go` | `src/Oxia.Client/FailureInjection.cs` | ported | |
| `oxia/key_order.go` | `src/Oxia.Client/KeyOrder.cs` | ported | `readonly struct KeyOrder` (`default` = legacy `CompareWithSlash` order). `Compare(string,string)` is allocation-free (`EncodedLength/EncodeTo` into a stack / pooled buffer); `SortKey` keeps Go's allocating shape; results normalised to -1/0/+1. |
| `oxia/results_heap.go` (+ the merge loop `aggregateAndSortRangeScanAcrossShards` of `oxia/async_client_impl.go`) | `src/Oxia.Client/ResultsHeap.cs` | ported | Array-based binary heap following `container/heap`'s `up/down` exactly; one reusable `ResultAndChannel` per source channel (`Reset` re-encodes the sort keys in place); merge loop in `RangeScanMerger.AggregateAndSortRangeScanAcrossShardsAsync` (F2 calls it). |
| `oxia/proto_utils.go` | `src/Oxia.Client/ProtoUtils.cs` | ported | Static, allocation-free conversions over `Oxia.Common.Proto`; `ToVersion(null)` returns the default instead of panicking. |
| `oxia/resolver.go` | `src/Oxia.Client/Resolver.cs` | ported | Adapter over `Grpc.Net.Client.Balancer.ResolverFactory`/`Resolver` registered through a `GrpcChannelOptions.ServiceProvider` wrapper (`GrpcResolverBuilder.AsDialOption`). `ServiceResolver.Resolve` runs in `Resolver.Start` (when the channel first connects), not at channel creation. The updater returns an error for an empty list and after dispose. A channel with a custom scheme needs `GrpcChannelOptions.Credentials` (ClientPool's job). |
| `oxia/client.go` (`NotificationType.String`, `Version`) | `src/Oxia.Client/Client.cs` | lead-owned | Only tested here (`ClientTests`, `ProtoUtils.Extra`). |

## Tests

| Upstream test file | C# test file | Go tests | Ported | Skipped |
|---|---|---|---|---|
| `oxia/optional_test.go` | `tests/Oxia.Client.Tests/OptionalTests.cs` | 3 | 3 | 0 |
| `oxia/options_client_test.go` | `tests/Oxia.Client.Tests/Options/OptionsClientTests.cs` | 13 | 13 | 0 |
| `oxia/options_delete_test.go` (client batching/timeout options) | `tests/Oxia.Client.Tests/Options/OptionsDeleteTests.cs` | 4 | 4 | 0 |
| `oxia/options_list_test.go` | `tests/Oxia.Client.Tests/Options/OptionsListTests.cs` | 2 | 2 | 0 |
| `oxia/options_put_test.go` | `tests/Oxia.Client.Tests/Options/OptionsPutTests.cs` | 3 | 3 | 0 |
| `oxia/key_order_test.go` | `tests/Oxia.Client.Tests/KeyOrderTests.cs` | 3 (+8 `t.Run`) | 3 (as 9 facts) | 0 |
| `oxia/resolver_test.go` | `tests/Oxia.Client.Tests/ResolverTests.cs` | 1 | 1 | 0 |
| `oxia/client_test.go` | `tests/Oxia.Client.Tests/ClientTests.cs` | 1 | 1 | 0 |

Extra tests (never replace ported ones): `Options/OptionsMatrix.Extra.cs` (subtyping/conversion matrix, last-wins order,
defaults, validation order/messages, Go `%q`, call-context option, **zero allocation** of building+resolving options with
`GC.GetAllocatedBytesForCurrentThread`), `Options/ClientOptions.Extra.cs` (defaults, ticker computation, multierr semantics,
`WithMaxBatchSize`), `ResultsHeap.Extra.cs` (merge vs reference sort with random data for natural/hierarchical/unknown, with and
without secondary keys, concurrent producers, error/cancel/failure/back-pressure, heap invariants, `KeyOrder` agreement and
allocation), `ProtoUtils.Extra.cs` (status/version/key mappings, `RecordVersion` equality, `NotificationType` strings,
`Optional`), `Resolver.Extra.cs` (endpoint/address parsing, updater contract, provider composition, **end-to-end** gRPC
channel with a custom scheme and delayed address update against an in-process Kestrel server).

Dependency note: `KeyOrderTests`, `ResultsHeap.Extra` and `KeyOrder` need component B's real `Oxia.Common.Compare`
(`Encoders.Natural/Hierarchical`, `KeyCompare.CompareWithSlash`); they were verified against a temporary local
implementation of that contract (never committed).

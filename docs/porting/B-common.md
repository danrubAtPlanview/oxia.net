# Component B — common utilities

Upstream: `github.com/oxia-db/oxia` @ `8456f61e1ada7b786f5ef17798b45fbc17d0d246`, packages under `common/` (client-relevant subset) and
`github.com/cenkalti/backoff/v4 v4.3.0` (the pieces the client uses).
C#: `src/Oxia.Common/**` (namespaces `Oxia.Common.*`), tests `tests/Oxia.Client.Tests/Common/**` (namespace `Oxia.Client.Tests.Common`, one class per
Go test file named after it: `errors_test.go` -> `ErrorsTests`, `client_pool_test.go` -> `ClientPoolTests`, ...; extra tests live in `*.Extra.cs`
files / `*ExtraTests` classes and never replace ported ones), benchmarks `benchmarks/Oxia.Client.Benchmarks/Common/**`.

Status: `ported` | `partial` | `skipped (reason)`. Test columns: Go test functions / C# tests with the same name (xUnit `[Theory]` rows are not counted
separately).

## Sources

| Upstream file | C# file | Status | Notes (divergences, extra tests) |
|---|---|---|---|
| `common/constant/errors.go` (ErrorMetadata, WithLeaderHint, WithCoordinatorLeaderHint, IntoGrpcStatusError, FromGrpcError) | `src/Oxia.Common/Constant/OxiaErrors.Grpc.cs` (the sentinels / `IsRetryable` half is the lead's `OxiaErrors.cs`) | ported | Built on `Grpc.StatusProto` (`GetRpcStatus` / `ToRpcException`) + `Google.Rpc.ErrorInfo`; bytes of `grpc-status-details-bin` identical to Go's. The RpcException is found anywhere in the Inner/Aggregate chain (Go `errors.As`); malformed details never throw; `FromGrpcError` returns non-null (maybe empty) metadata for an oxia.io ErrorInfo, null otherwise. `GrpcCode` / `IsCanceled` / `IsDeadlineExceeded` map `status.Code` / `errors.Is(ctx errors)`. |
| `common/constant/{constants,keys,flag}.go` | `Constant/Constants.cs` | (lead) | not part of this component's files |
| `common/time/backoff.go` | `src/Oxia.Common/Time/BackOff.cs` | ported | `IBackOff`, `IBackOffContext`, `ZeroBackOff`, `BackOffFactory` (`WithContext`, `NewBackOff`, `NewBackOffWithInitialInterval`), `ConcurrentBackOff`. Also cenkalti `backoff.go` / `context.go`. |
| `common/time/jitter.go` | `Time/Jitter.cs` | ported | ticks instead of ns. |
| `common/time/clock.go` (Clock, MockedClock) | — | skipped (not used by client) | `ExponentialBackOff` takes a `TimeProvider` instead. |
| cenkalti `exponential.go` | `Time/ExponentialBackOff.cs` | ported | Same formulas/overflow guard on ticks; `TimeProvider` for cenkalti's `Clock`; `Stop` field; lazy first `Reset()`. |
| cenkalti `retry.go`, `error.go` (Permanent), `timer.go` | `Time/Retry.cs`, `Time/SystemRetryTimer.cs` | ported | Async loop: failure = thrown exception; `PermanentException` found in the Inner/Aggregate chain; both the backoff context token and the explicit `ct` cancel the wait; `SystemRetryTimer` = reusable one-shot `Timer` + `IValueTaskSource` (no allocation per wait but the token registration). Other cenkalti files (tries, ticker, constant/ZeroBackOff variants, notify helpers) — skipped (not used by client). |
| `common/concurrent/future.go` | `Concurrent/Future.cs` | ported | `IValueTaskSource<T>`, first-wins CAS, ready value beats a cancelled token, a cancelled wait leaves the future usable (Go `TestFuture` scenario), `Reset()` for pooling, zero allocation on the success path (test). |
| `common/concurrent/wait_group.go` | `Concurrent/WaitGroup.cs` | ported | bounded `Channel<Exception?>` of `parties` slots, same consumption order as the Go channel; `Fail` beyond the party count is dropped (Go blocks). |
| `common/channel/noblock.go` | `Concurrent/ChannelExtensions.cs` | ported | `PushNoBlock` = `ChannelWriter.TryWrite`. |
| `common/concurrent/{batcher,callback,callback_adaptor,condition,once,utils}.go`, `common/channel/{override_channel,read}.go` | — | skipped (not used by client) | |
| `common/compare/compare_with_slash.go` | `Compare/CompareWithSlash.cs` (class `KeyCompare`) | ported | spans, returns exactly -1/0/+1. |
| `common/compare/encode.go` | `Compare/Encode.cs` | ported | Hierarchical + natural encoders, `PrefixRanges` (`IPrefixRanges`), `InternalKeyRange`, `GetEncoder`; `Encode` always returns a new array; added `EncodedLength`/`EncodeTo`/`TryEncode` (+`ReadOnlySpan<char>` overloads), vectorised `Count`/`Replace` of `/`; keys are UTF-8 encoded from .NET strings. |
| `common/hash/hash.go` | `src/Oxia.Common/Hash/Hasher.cs` | ported (lead) — verified | Bit-identical to `uint32(xxh3.HashString(key))`; header got the verification note. |
| `common/failpoint/failpoint.go`, `registry.go` | `src/Oxia.Common/Failpoint/Failpoints.cs` | ported (lead) — verified | `*_disable.go` (tag `disable_trap`) not ported. |
| `common/auth/{authentication,token}.go` | `src/Oxia.Common/Auth/Authentication.cs` | ported (lead) — tested | no Go tests exist; new `TokenTests`. |
| `common/rpc/client_pool.go` | `src/Oxia.Common/Rpc/ClientPool.cs` | ported | Lazy shared connection per target (lock-free read + locked create), removal + close on health failure, `GetClientRpc` / `GetAdminRpc` (+ `GetHealthRpc`). `GetCoordinationRpc` / `GetReplicationRpc` / `GetPeer` are server side: not ported. |
| `common/rpc/connection.go` | `Rpc/Connection.cs` | ported | `tls://`, `host:port` dialled directly (DNS resolved by `SocketsHttpHandler` on every new connection), scheme targets through the channel's resolver machinery with `Credentials` set and `:authority` = the endpoint of the target (grpc-go behaviour), keepalive 10s/3s/always, 256 MiB receive limit, per-RPC credentials (`UnsafeUseInsecureChannelCallCredentials` when plaintext is allowed), health loop (10s/10s, threshold 3, NOT_SERVING at once, Unimplemented stops). |
| `common/rpc/metadata.go` | `Rpc/MetadataInterceptor.cs` | ported | `MetadataInterceptor` (Grpc.Core interceptor) and `RpcMetadata.MetadataInjectionDialOption` (`Action<GrpcChannelOptions>`: HTTP handler that stamps the headers). |
| `common/rpc/mock.go` | `tests/Oxia.Client.Tests/Common/Rpc/Mock.cs` | ported (test helper) | Lives in the test project; channels -> `System.Threading.Channels`; no gRPC stream interfaces. |
| `common/rpc/grpcmetrics/grpcmetrics.go` | `Rpc/GrpcMetrics.cs` | partial | Client interceptors only (`GrpcClientMetricsInterceptor`, meter `oxia`, counters `grpc_client_{started,handled,msg_received,msg_sent}_total`, unit `1`, labels `grpc_type`/`grpc_service`/`grpc_method`/`grpc_code`, Go code names); server half (`Register`, server interceptors) skipped (not used by client). |
| `common/rpc/{logging_client_rpc,oxia_error_streaming_client}.go` | — | component A | |
| `common/security/tls.go` | `src/Oxia.Common/Security/TlsOptions.cs` | ported | Client and server halves -> `SslClientAuthenticationOptions` / `SslServerAuthenticationOptions` (CA file via custom-root chain validation, host name still verified, `ServerName`, `InsecureSkipVerify`, client cert reloaded per handshake, client-auth levels, min/max version -> `SslProtocols`, cipher suites -> `CipherSuitesPolicy` where the runtime supports it). `TryInto*` return the conf and throw on error. |

## Tests

| Upstream test file | C# test class | Go tests | Ported | Skipped | Notes |
|---|---|---|---|---|---|
| `constant/errors_test.go` | `ErrorsTests` (+ `ErrorsExtraTests`) | 13 | 13 | 0 | `okStatusError` = RpcException with `StatusCode.OK`. Extra: Go-compatible trailer bytes, malformed/foreign/unknown details, wrapped RpcException (incl. leader hints through `OxiaException(inner: rpc)`), leader-hint parsing, `GrpcCode`, `IsCanceled`, `IsDeadlineExceeded`. |
| `time/backoff_test.go` | `BackoffTests` (+ `ExponentialBackOffTests`, `RetryTests`, `SystemRetryTimerTests`) | 2 | 2 | 0 | Extra: cenkalti exponential tests re-written (randomisation, elapsed time with a manual clock, overflow, custom Stop), retry loop semantics (permanent, Stop, cancellation while waiting, notify, timer substitution, token combinations), default timer. |
| `time/jitter_test.go` | `JitterTests` | 1 | 1 | 0 | |
| `concurrent/future_test.go` | `FutureTests` (+ `FutureExtraTests`) | 5 | 5 | 0 | Extra: races between completion/cancellation/wait start, reset/reuse, sentinels, zero allocation. |
| `concurrent/wait_group_test.go` | `WaitGroupTests` (+ `WaitGroupExtraTests`) | 3 | 3 | 0 | |
| `channel/noblock_test.go` | `NoblockTests` | 1 | 1 | 0 | |
| `compare/compare_with_slash_test.go` | `CompareWithSlashTests` (+ extra) | 1 | 1 | 0 | Extra: differential test against a list-based re-statement. |
| `compare/encode_test.go` | `EncodeTests` (+ `EncodeExtraTests`) | 10 | 10 | 0 | 14 Go subtests as `[Theory]` rows; `BenchmarkEncoderNaturalDecode` -> `CompareBenchmarks`. Extra: golden bytes, span API, unicode, order, internal range, prefix range bounds. |
| `compare/compare_with_slash_benchmark_test.go` | `benchmarks/.../Common/CompareBenchmarks.cs` | 10 benchmarks | ported | 0 | BenchmarkDotNet. |
| `hash/hash_test.go` | `HashTests` | 1 (3 subtests) | 1 | 0 | Extra: 30 reference XXH3-64 vectors (xxHash C library) over all length classes, unicode, stack/pooled boundary, no allocation. |
| `failpoint/failpoint_test.go` | `FailpointTests` | 6 | 6 | 0 | |
| `failpoint/failpoint_disable_test.go` | `FailpointTests` | 2 | 0 | 2 | `[Fact(Skip)]`: no `disable_trap` flavour. |
| `failpoint/registry_test.go` | `RegistryTests` | 5 | 5 | 0 | |
| `failpoint/registry_disable_test.go` | `RegistryTests` | 1 | 0 | 1 | `[Fact(Skip)]`: no `disable_trap` flavour. |
| (no Go test) `auth/token.go` | `TokenTests` | 0 | — | — | new tests. |
| `rpc/client_pool_test.go` | `ClientPoolTests` (+ `ClientPoolExtraTests`) | 11 | 11 | 0 | `GetCoordinationRpc`/`GetReplicationRpc` assertions replaced by the remaining getters. Extra: shared connections under concurrency, channel settings, dial options last, auth headers, `tls://`, custom resolver scheme + `:authority`, dynamic resolver, 6 MB responses, close/dispose. |
| `rpc/connection_test.go` | `ConnectionTests` | 4 | 4 | 0 | `WithContextDialer` -> `SocketsHttpHandler.ConnectCallback` (receives the unresolved `DnsEndPoint`). |
| `rpc/metadata_test.go` | `MetadataTests` | 2 | 2 | 0 | Extra: interceptor + dial option against a real server. |
| `rpc/mock_test.go` | `MockTests` | 11 | 11 | 0 | |
| `rpc/grpcmetrics/grpcmetrics_test.go` | `GrpcMetricsTests` | 6 (+1 benchmark) | 5 | 1 | `TestRegisterPreInitializesSeries` skipped (server `Register`); `BenchmarkStreamMsgSent` -> `GrpcMetricsBenchmarks`. Server-side assertions dropped. The gRPC test service is the generated `OxiaClient` base. |
| `security/tls_test.go` | `TlsTests` (+ `TlsExtraTests`) | 16 | 16 | 0 | Extra: real TLS handshakes over loopback sockets (trust, name, skip-verify, client auth, versions, per-handshake reload) and a TLS gRPC server through `ClientPool`. |

## Cross-component notes

* `Connection.ClientRpc` creates `new OxiaClientRpc(CallInvoker)` (component A); the unit tests that call `GetClientRpc` need A's constructor not to throw.
* `GrpcClientMetricsInterceptor.Default` (meter `oxia`) is applied to every pooled connection; the `ClientPool` constructor keeps the Go-shaped signature, so a
  host-supplied `IMeterFactory` cannot reach it yet.

## Totals

* Go test functions in scope: 98 (+ 3 in the `disable_trap` flavour files) — ported 97 and passing; skipped 4 with `[Fact(Skip)]`: `TestFailpointsTriggerDisabled`,
  `TestFailpointsFailDisabled`, `TestEvalDisabled` (no `disable_trap` flavour) and `TestRegisterPreInitializesSeries` (server-side `Register`).
* C# tests under `Oxia.Client.Tests.Common`: 440 (436 passing, 4 skipped), the rest being `[Theory]` rows of the ported tests and the extra tests.
* Not ported (not used by the client): `concurrent/{batcher,callback,callback_adaptor,condition,once,utils}.go` and their tests, `channel/{override_channel,read}.go` and
  tests, `time/clock.go`, the server half of `rpc/grpcmetrics`, `rpc.GetPeer`, `GetCoordinationRpc` / `GetReplicationRpc`.

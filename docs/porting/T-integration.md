# T — test infrastructure & integration tests

Pinned upstream commit `8456f61e1ada7b786f5ef17798b45fbc17d0d246`. How the infrastructure works, its environment variables and the
decisions behind it: [`tests/Oxia.Client.IntegrationTests/README.md`](../../tests/Oxia.Client.IntegrationTests/README.md).

## Test files (Go integration tests -> `tests/Oxia.Client.IntegrationTests`)

| Upstream file | C# file | Status | Notes (divergences, extra tests) |
|---|---|---|---|
| `tests/client/async_client_test.go` (23 tests, 18 sub-tests) | `Client/AsyncClientTests.cs` | ported (23/23; sub-tests as `[Theory]` cases) | Fresh `oxia standalone` per test through `OxiaServerFixture.StartAsync(profile)` (`NumShards`, `KeySorting`, retention -> `OxiaServerProfile`). `assert.ErrorIs(ErrX, err)` asserted in the natural direction. `assert.Equal(GetResult{...})` field by field. |
| `tests/client/sync_client_test.go` (9) | `Client/SyncClientTests.cs` | ported (9/9) | `GetSequenceUpdates` without partition key throws (Client.cs contract) -> asserts the thrown `ErrInvalidOptions`. |
| `tests/client/cache_test.go` (8) | `Client/CacheTests.cs` | ported (8/8), **compiles only once component F1 defines `ICache<T>`/`OxiaClient.CreateCache`** | The cache API shape is ASSUMED (see header and "Assumed APIs" below); the four zero-value-on-error assertions are implied by the exception. `json` -> System.Text.Json. |
| `tests/client/example_test.go` (3) | `Client/ExampleTests.cs` | ported (3/3) | Go binds the fixed `localhost:6648`; here the fixture's address. Output via the test output helper. |
| `tests/client/session_test.go` (1) | `Client/SessionTests.cs` | ported (1/1) | ~50 s test. `rate.Limiter` -> `RateLimiter` (token bucket). |
| `tests/client/shard_manager_test.go` (1) | `Client/ShardManagerTests.cs` | ported (1/1) | |
| `tests/sequence/sequence_test.go` (1) | `Sequence/SequenceTests.cs` | ported (1/1) | |
| `tests/resolver/custom_resolver_test.go` (2) | `Resolver/CustomResolverTests.cs` | ported (2/2) | `StaticResolver`/`DynamicResolver` implement `IServiceResolver`; the standalone server echoes the authority, so `"scheme:///<addr>"` works as in Go. |
| `tests/resolver/helpers_test.go` | `Resolver/Helpers.cs` | ported | `newCoordinatorInstance` -> `OxiaClusterFixture.StartAsync` (real coordinator process). |
| `tests/resolver/target_ip_flip_test.go` (2) | `Resolver/TargetIpFlipTests.cs` | ported (2/2), **skipped unless `OXIA_TEST_CLUSTER=1`** | Needs 2 coordinator-managed clusters with `allowExtraAuthorities`; provided by `OxiaClusterFixture` (cluster container). Not a source-level skip: the full test body is compiled and runs when the variable is set. |
| `tests/assignments/helpers_test.go` | `Assignments/Helpers.cs` | ported | `newDefaultClusterConfig` (RF 3, 1 shard) + coordinator. |
| `tests/assignments/leader_hint_test.go` (6) | `Assignments/LeaderHintTests.cs` | ported (6/6), **skipped unless `OXIA_TEST_CLUSTER=1`** | 3 data servers + coordinator from `OxiaClusterFixture`; see "Cross-component risks" for the error-metadata question. |

Totals: Go tests **56**, ported **56** (all names verbatim), sub-tests 18 ported as theory cases; **8 are opt-in** (cluster), **0 dropped**.
Not ported: see "Out of scope / follow-ups".

## Test infrastructure (no upstream equivalent: the Go tests embed the server in-process)

| Upstream concept | C# file | Status | Notes |
|---|---|---|---|
| `dataserver.NewStandalone(NewTestConfig(dir))`, `Standalone.ServiceAddr/Close` | `tests/Oxia.TestSupport/OxiaServerFixture.cs` (`OxiaServerFixture`, `OxiaServer`) | new | Reusable Docker container with lockable slots; one fresh `oxia standalone` per test. |
| `StandaloneConfig{NumShards, KeySorting, NotificationsEnabled, Storage.Notification.Retention}` | `tests/Oxia.TestSupport/OxiaServerProfile.cs` | new | -> `oxia standalone` flags. |
| `mock.NewServer`, coordinator runtime (`tests/mock`, `oxiad/coordinator`) | `tests/Oxia.TestSupport/OxiaClusterFixture.cs` | new, opt-in | Second reusable container: coordinator + N data servers per cluster slot. |
| (docker plumbing) | `tests/Oxia.TestSupport/Docker/DockerCli.cs`, `Docker/ReusableContainer.cs` | new | Name/label/hash lookup, start/create, `docker port`, exec scripts. |
| (readiness) | `tests/Oxia.TestSupport/OxiaReadiness.cs` | new | Raw `GetShardAssignments` until every shard has a leader. |
| (raw gRPC) | `tests/Oxia.TestSupport/RawOxia.cs` | new | put/get/assignments independent of the client under test. |
| `oxiad/common/sharding/shards.go` | `tests/Oxia.TestSupport/Sharding.cs` | ported | `GenerateShards`, `shardOf`. |
| testify `assert.Eventually`, Go channel idioms | `tests/Oxia.TestSupport/TestUtil.cs` | new | `Eventually`, `EventuallyWithT`, `Receive`/`ReceiveWithin`/`TryReceive`, `RandomString`, ... |
| env configuration | `tests/Oxia.TestSupport/IntegrationEnv.cs` | new | `OXIA_TEST_*`. |
| (cleanup) | `tests/Oxia.TestSupport/scripts/clean-oxia-containers.ps1` | new | Removes `oxia.net.tests=1` containers. |
| fixture self tests | `tests/Oxia.Client.IntegrationTests/Infrastructure/OxiaServerFixtureTests.Extra.cs`, `OxiaClusterFixtureTests.Extra.cs` | new | Raw-gRPC tests of the fixtures (empty server per start, profile applied, 4 concurrent servers, leader election and follower rejection with leader hint, stop all servers). Pass against Docker today. |

## Out of scope / follow-ups

| Upstream | Status | Reason / what is needed |
|---|---|---|
| `tests/security/tls` (`TestClientHandshakeFailByNoTlsConfig`, `TestClientHandshakeByAuthFail`, `TestClientHandshakeWithInsecure`, `TestClientHandshakeSuccess`, `TestOnlyEnablePublicTls`; `TestClusterHandshakeSuccess` is server-side only) | **ported in wave 2 (T2), see [`T2-security.md`](T2-security.md)** (the text of this row is the original follow-up note) | Server: extend `OxiaClusterFixture` with cert copying (`docker exec -i ... cat > file`) and `--tls-*`, `--internal-tls-*`, `--peer-tls-*` flags (the certs in `tests/security/certs` have SAN `127.0.0.1`/`localhost`). Client: the `TLSOptions{CertFile,KeyFile,TrustedCaFile,InsecureSkipVerify}.TryIntoClientTLSConf()` equivalent (component B `Security/**`) to build the `SslClientAuthenticationOptions` for `WithTls`. |
| `tests/security/auth` (OIDC) | **ported in wave 2 (T2), see [`T2-security.md`](T2-security.md)** (original note follows) | Needs an OIDC issuer/JWKS the server container can reach (host-side mock + `host.docker.internal`, or a static key file: `--auth-provider-name oidc --auth-provider-params {...staticKeyFile...}`) and the client `IAuthentication` (component B). |
| `tests/control`, `coordinator`, `scheduler`, `kvscript`, `embedded`, `mock` | skipped (server-side suites) | They test the server/coordinator, not the client. |

## Assumed APIs (to reconcile at merge)

* `OxiaClient.CreateAsyncClientAsync/CreateSyncClientAsync(string, params ReadOnlySpan<ClientOption>)` returning `ValueTask<IAsyncClient>` / `ValueTask<ISyncClient>` (DESIGN.md signatures; the return types are an assumption).
* Cache (F1): `OxiaClient.CreateCache<T>(ISyncClient, SerializeFunc, DeserializeFunc)` -> `ValueTask<ICache<T>>` (as in the F1/F2 worktrees seen during development) with
  `PutAsync(ct, key, value, params PutOption) -> (Key, Version)`, `GetAsync(ct, key) -> (Value, Version)`, `DeleteAsync(ct, key, params DeleteOption)`,
  `ReadModifyUpdateAsync(ct, key, ModifyFunc<T>)`, `IAsyncCloser`; `SerializeFunc(object?) -> byte[]`, `DeserializeFunc(ReadOnlyMemory<byte>, Type) -> object?`,
  `ModifyFunc<T>(IOptional<T>) -> T`. Only `CacheTests.cs` (`NewCache`, `JsonSerde`) and the temporary stub depend on it.
* `INotifications.Ch` is a `ChannelReader<Notification>` that is **completed** when the notifications are closed / the client is closed (Go closes the channel); the tests read `null` (default) after `CloseAsync`.
* Failures of `ISyncClient` / cache calls are thrown as an `OxiaException` whose chain contains the sentinel (`OxiaErrors.Is`).

## Cross-component risks found while porting (not defects of T)

* **Leader-hint tests vs `IOxiaClientRpc`.** Go's `ClientPool.GetClientRpc` returns the *raw* gRPC client (the Oxia error translation is applied only by the
  library's own streams in `rpc_provider.go`), so the test can call `constant.FromGrpcError(err)` on the `Recv()` error and read the leader hint from its
  `ErrorInfo` metadata. `RpcContracts.cs` states that `IOxiaClientRpc` already translates every failure to a sentinel; if the translated exception does not keep
  the `ErrorInfo` metadata reachable by `OxiaErrors.FromGrpcError`, `TestLeaderHint{,List,RangeScan}WithoutClient` will time out. Fix on the A/B side
  (keep the original `RpcException` as `InnerException` and make `FromGrpcError` walk the chain), do not change the tests.
* **Resolver authority.** `TestCustomResolver` / `TestTargetIPFlip*` require the client to connect to the resolved address while sending the URI's endpoint as the
  HTTP/2 `:authority` (grpc-go does this for `scheme:///endpoint`). A standalone server returns that authority as the shard leader.

## Temporary stub used while the client did not exist

The integration tests compile against the real `OxiaClient` (F2) and `ICache<T>` (F1). During development this stub (`tests/Oxia.Client.IntegrationTests/OxiaClient.TempStub.cs`,
excluded through `.git/info/exclude`, never committed) stood in for them; the committed tree does not build until those types exist:

```csharp
namespace Oxia.Client
{
    public static class OxiaClient
    {
        public static ValueTask<IAsyncClient> CreateAsyncClientAsync(string serviceAddress, params ReadOnlySpan<ClientOption> options) => throw new NotImplementedException();
        public static ValueTask<ISyncClient> CreateSyncClientAsync(string serviceAddress, params ReadOnlySpan<ClientOption> options) => throw new NotImplementedException();
        public static ValueTask<ICache<T>> CreateCache<T>(ISyncClient client, SerializeFunc serialize, DeserializeFunc deserialize) => throw new NotImplementedException();
    }
    public delegate byte[] SerializeFunc(object? value);
    public delegate object? DeserializeFunc(ReadOnlyMemory<byte> data, Type type);
    public delegate TValue ModifyFunc<TValue>(IOptional<TValue> existing);
    public interface ICache<TValue> : Oxia.Common.IAsyncCloser
    {
        ValueTask<(string Key, RecordVersion Version)> PutAsync(CancellationToken ct, string key, TValue value, params ReadOnlySpan<PutOption> options);
        ValueTask DeleteAsync(CancellationToken ct, string key, params ReadOnlySpan<DeleteOption> options);
        ValueTask<(TValue Value, RecordVersion Version)> GetAsync(CancellationToken ct, string key);
        ValueTask ReadModifyUpdateAsync(CancellationToken ct, string key, ModifyFunc<TValue> modify);
    }
}
```

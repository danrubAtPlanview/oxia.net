# T2 — security integration tests (TLS and OIDC)

Upstream: `github.com/oxia-db/oxia` @ `8456f61e1ada7b786f5ef17798b45fbc17d0d246`, `tests/security/{tls,auth,certs}`.
C#: `tests/Oxia.Client.IntegrationTests/Security/**` (namespace `Oxia.Client.IntegrationTests.Security`), infrastructure additions in
`tests/Oxia.TestSupport/{OxiaClusterProfile.cs,Security/**}` plus small additive edits of `OxiaClusterFixture.cs`, `OxiaReadiness.cs`, `RawOxia.cs`.
How the Docker fixture works: [`tests/Oxia.Client.IntegrationTests/README.md`](../../tests/Oxia.Client.IntegrationTests/README.md), [`T-integration.md`](T-integration.md).

Status: `ported` | `partial` | `skipped (reason)`.

## Test files

| Upstream file | C# file | Status | Notes (divergences, extra tests) |
|---|---|---|---|
| `tests/security/tls/tls_encryption_test.go` (6 tests) | `Security/TlsEncryptionTests.cs` | ported (6/6, 0 skipped) | `TestClusterHandshakeSuccess`, `TestClientHandshakeFailByNoTlsConfig`, `TestClientHandshakeByAuthFail`, `TestClientHandshakeWithInsecure`, `TestClientHandshakeSuccess`, `TestOnlyEnablePublicTls`; names, scenarios and assertions as in Go. `getClientTLSOption()` -> `TlsOptions` (`Oxia.Common.Security`) -> `TryIntoClientTlsConf()` -> `OxiaOptions.WithTls(...)`. A client that fails to be created throws (Go: `nil, err`): the tests assert the exception and that no client object came out. `TestClusterHandshakeSuccess` asserts nothing in Go besides "the coordinator can be created"; the observable equivalent here is that the coordinator reaches the TLS-secured servers (leader elected, announced over TLS). |
| `tests/security/tls/helpers_test.go` (+ helpers inside `tls_encryption_test.go`: `getPeerTLSOption`, `getClientTLSOption`, `newTLSServer`, `newTLSServerWithInterceptor`) | `Security/SecurityHelpers.cs` (`PkiFixture`, `NewTlsCluster`) | ported | `newTLSServer` x3 + `newDefaultClusterConfig` (RF 3, 1 shard) + `newCoordinatorInstance` = `OxiaClusterFixture.StartAsync("tls", 3, 3, 1, profile)`. `Server.Public/Internal.TLS` = `--tls-*` / `--internal-tls-*`, `Replication.TLS` = `--peer-tls-*`, the coordinator's `NewControllerRPCProviderFactory(tlsConf)` = the coordinator's `--peer-tls-*`; `TLS.Enabled = false` (`TestOnlyEnablePublicTls`) = not passing the flags (`IsEnabled()` is "a cert file is configured"). |
| `tests/security/auth/auth_oidc_test.go` (3 tests) | `Security/AuthOidcTests.cs` | ported (3/3, 0 skipped) | `TestOIDCWithStaticToken` (no token / malformed / unknown issuer / truncated / expired -> `Unauthenticated`, then put+get with a valid token; includes the 3 s sleep for the expired token), `TestOIDCWithPerIssuerConfig`, `TestOIDCWithStaticKeyFile`. `status.Code(errors.Unwrap(err))` -> `OxiaErrors.GrpcCode(err)`. `NewTokenAuthenticationWithToken(token, false)` -> `Authentication.NewTokenAuthenticationWithToken(token, false)` through `OxiaOptions.WithAuthentication`. The token claims are the Go `jwt.RegisteredClaims` (audience array, `nbf` = Go's zero time). |
| `tests/security/auth/helpers_test.go` (+ `newOxiaClusterWithAuth`, `generateRandomStr` in `auth_oidc_test.go`) | `Security/SecurityHelpers.cs` (`NewOidcCluster`, `LegacyOidcParams`, `PerIssuerOidcParams`, `GenerateRandomStr`) | ported | `auth.Options{Provider: oidc, ProviderParams: json}` = `--auth-provider-name oidc --auth-provider-params <json>` (same JSON: legacy `allowedIssueURLs`/`allowedAudiences`, or `issuers{url:{allowedAudiences,userNameClaim,staticKeyFile}}`). |
| `tests/security/certs/*` (`ca`, `peer`, `client` key + cert, `openssl.conf`, `generate_command.txt`) | `tests/Oxia.TestSupport/Security/TestPki.cs` | ported (generated per run) | `CertificateRequest`: RSA 2048, `CN=oxia-ca` (CA:true), `CN=oxia-peer`, `CN=oxia-client`; leafs carry keyUsage digitalSignature+keyEncipherment (critical), EKU serverAuth+clientAuth, SAN `DNS:localhost, IP:127.0.0.1, IP:192.168.1.2`. Valid from "now - 1 day" (host/VM clock drift) for 10 years. Written as PEM (keys PKCS#8) to a temp directory for `TlsOptions` and to the container tmpfs for the servers. |
| `github.com/oauth2-proxy/mockoidc` (`mockoidc.Run`, `Issuer()`, `Keypair.SignJWT`, `Shutdown`) as used by the auth tests | `tests/Oxia.TestSupport/Security/MockOidcProvider.cs` | ported (minimal provider) | Kestrel in the test process: discovery document, JWKS, a token endpoint (not used by the tests), RS256 JWTs signed with `System.Security.Cryptography` + `System.Text.Json` (no JWT package added). Issuer `http://<host>:<port>/oidc` where `<host>` is how the container reaches the test host (see below). |

### Extra tests (not in Go; they never replace ported ones)

| C# file | Tests | What they add |
|---|---|---|
| `Security/TlsEncryptionTests.Extra.cs` | `TestTlsClientPutsAndGetsThroughTls`, `TestTlsSchemeAddress`, `TestTlsServerNameIsVerified`, `TestMutualTlsPresentsTheClientCertificate` | Go only creates and closes the client; these read/write through TLS (sync and async client, bootstrapping from a follower), use a `tls://` address (and refuse it without a trusted CA), check `TlsOptions.ServerName` (`localhost` accepted, a name not in the certificate refused), and mutual TLS (`--tls-client-auth`: the client certificate of `TlsOptions.CertFile/KeyFile` is presented; a client without one is refused). |
| `Security/AuthOidcTests.Extra.cs` | `TestTokenFuncIsEvaluatedForEveryRpc`, `TestTokenOverTlsWithRequiredTransportSecurity`, `TestEveryRpcKindOverTlsAndAuthentication` | `NewTokenAuthenticationWithFunc` is called for every RPC (refreshed token used; a rejected token fails the next RPC with `Unauthenticated`); credentials with `requireTransportSecurity: true` over TLS + OIDC on the same public endpoint (and `Unauthenticated` without a token); put/get/list/range-scan/delete-range/ephemeral put (session create + keep-alive)/notifications all over TLS + a bearer token. |

Totals: Go tests **9** (6 TLS + 3 OIDC), ported **9**, passing **9**, skipped **0**; 7 extra tests, passing. All 16 pass in ~20 s (the Docker
containers already exist), repeatedly (3 consecutive runs) and together with the whole suite, with and without `OXIA_TEST_CLUSTER=1`.

## How the tests run (decisions)

* **Server: `oxia server` + `oxia coordinator`, not `oxia standalone`.** `oxia standalone` (`cmd/standalone/cmd.go`) has no TLS and no
  authentication flags; only `oxia server` has them (`--tls-*` public, `--internal-tls-*`, `--peer-tls-*` replication; `--auth-provider-name`,
  `--auth-provider-params`), and the coordinator takes `--peer-tls-*` for its connection to the servers (`cmd/server/cmd.go`,
  `cmd/coordinator/cmd.go`). The Go tests also run three servers + a coordinator, so the cluster of `OxiaClusterFixture` (the **reused**
  cluster container; no new container, at most 2 in total) is the faithful counterpart.
* **`OxiaClusterProfile`** (new, `tests/Oxia.TestSupport/OxiaClusterProfile.cs`): `Files` (name -> text, written with quoted here-documents
  to `/data/cluster<slot>/files`, mode 600, on the container's tmpfs), `ServerArgs`, `CoordinatorArgs` (the token `{dir}` is replaced by the
  files directory, also inside JSON values), `Probe`. `OxiaClusterFixture.StartAsync(..., profile:)` generates a `run.sh` per process (so
  that an argument can be JSON with quotes), starts it like the other processes and wipes the directory when the cluster is closed or the slot
  is re-acquired. Clusters started with a profile are **not** opt-in (`OXIA_TEST_CLUSTER`), the plain cluster tests still are.
* **`RawSecurity`** (new): TLS configuration and/or bearer-token function for the fixture's own raw gRPC calls (`OxiaReadiness`, `RawOxia`,
  `OxiaCluster.GetLeaderAsync`), independent of the client under test. The readiness check of an OIDC cluster needs a valid token (the
  servers authenticate `GetShardAssignments` too); the test passes one of its own valid tokens, which is not part of any assertion.
* **OIDC provider reachable from the container.** The servers fetch `<issuer>/.well-known/openid-configuration` and the JWKS at start-up
  (legacy and per-issuer without `staticKeyFile`). `OxiaClusterFixture.GetHostAddressAsync()` asks the container: `host.docker.internal` if it
  resolves (Docker Desktop; verified here, it is `192.168.65.254` and a loopback-only listener is reachable), otherwise the default gateway
  (`ip route`, the docker0 bridge on Linux). `MockOidcProvider` listens on loopback on Windows/macOS and on all IPv4 interfaces on Linux.
  The containers are not created with `--add-host=host.docker.internal:host-gateway` (that would change the container definition and orphan
  the existing containers); the gateway fallback makes it unnecessary.
* **Addresses.** The servers' public address is `127.0.0.1:<published port>` (covered by the certificate SAN). A server in a coordinator-managed
  cluster only accepts the authority in the cluster configuration (`oxia: unexpected authority`), so dialling `localhost:<port>` is refused by
  the server (an attempt to add such a test confirmed this; it is a server rule, not a client defect, and the test was dropped).
* **Skips.** All security tests are skipped (not failed) with `OXIA_TEST_ADDRESS` set (no Docker fixture) and, with
  `OXIA_TEST_SKIP_IF_NO_SERVER=1`, when Docker is unavailable. No test is permanently skipped.
* **Platform notes.** `TlsOptions.CipherSuites` (Linux/macOS only in .NET) is not used by any of the 9 Go tests, so no test is skipped for that.
  The expired-token test depends on the container clock being within ~1 s of the host clock (they were identical here).

## Client defects

None found. Everything the tests exercise worked without a change to `src/`: `TlsOptions.TryIntoClientTlsConf()` (CA file, system roots,
`InsecureSkipVerify`, `ServerName`, client certificate), `OxiaOptions.WithTls`, `tls://` addresses, `ClientPool` TLS/per-RPC credentials (plaintext
`UnsafeUseInsecureChannelCallCredentials` and over TLS), `Unauthenticated` surfaced as an `RpcException` reachable with `OxiaErrors.GrpcCode`
and treated as permanent by the shard manager's initial wait (as Go), the write/read/list/scan/session/notification streams under TLS and
authentication. Behaviour worth knowing (same as Go, not defects): a connection that cannot be established (plaintext client against a TLS
server, untrusted CA, credentials that require transport security on a plaintext channel) is retried until the request timeout and then
reported as `oxia: failed to retrieve the initial list of shard assignments: context deadline exceeded`; only `Unauthenticated` /
`PermissionDenied` / namespace-not-found fail the initial wait immediately.

## Not covered

* The admin client over TLS / with a token (`OxiaClient.CreateAdminClient(addr, tls, auth)`): the Go security tests do not use it, and the
  coordinator's public (admin) endpoint has no TLS or auth command line flags (only a `--sconfig` file), so it would need a generated config
  file for the coordinator. The ClientPool/AdminClient unit tests cover the client side.

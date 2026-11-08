# Oxia.Client.IntegrationTests

Port of the Go integration tests that exercise the **client against a real Oxia server**:
`tests/client`, `tests/sequence`, `tests/resolver`, `tests/assignments` of
[oxia-db/oxia](https://github.com/oxia-db/oxia) (pinned commit `8456f61e`). The Go tests start an in-process server
(`dataserver.NewStandalone(dataserver.NewTestConfig(dir))`); here the server is a real `oxia` process running in a
**reusable Docker container** managed by `Oxia.TestSupport.OxiaServerFixture`.

```
dotnet test tests/Oxia.Client.IntegrationTests                       # needs Docker (or OXIA_TEST_ADDRESS)
dotnet test tests/Oxia.Client.IntegrationTests --filter "FullyQualifiedName~AsyncClientTests"
OXIA_TEST_CLUSTER=1 dotnet test tests/Oxia.Client.IntegrationTests    # also the coordinator based tests
```

The first run pulls `oxia/oxia:nightly-beta` (~50 MB) if it is missing and creates one container (~2 s); every later run
(and every other worktree) reuses it.

## Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `OXIA_TEST_ADDRESS` | unset | `host:port` of an **externally started** `oxia standalone` (default profile: 1 shard, hierarchical sorting, notifications on). Docker is not touched. Tests that need another profile (more shards, natural sorting, ...) are skipped; tests that assert a pristine database (version ids from 0, notification order) only pass on a freshly started server. |
| `OXIA_TEST_IMAGE` | `oxia/oxia:nightly-beta` | Server image. Pulled only if missing locally. See "Which image" below. |
| `OXIA_TEST_CLEANUP` | unset | `1` = remove the container(s) when the test run ends. Default: keep them, that *is* the reuse. |
| `OXIA_TEST_CLUSTER` | unset | `1` = also run the tests that need a coordinator + 3 data servers (`LeaderHintTests`, `TargetIpFlipTests`). |
| `OXIA_TEST_SLOTS` | `8` | Max number of standalone servers running at the same time in the container (the container name depends on it). |
| `OXIA_TEST_CLUSTER_SLOTS` | `2` | Max number of clusters running at the same time in the cluster container. |
| `OXIA_TEST_INSTANCE` | unset | Suffix of the container names: run several fully isolated fixtures side by side. |
| `OXIA_TEST_MEMORY` / `OXIA_TEST_CPUS` | `3g` / no limit | `docker run --memory` / `--cpus` of the containers. |
| `OXIA_TEST_SKIP_IF_NO_SERVER` | unset | `1` = skip (instead of fail) the tests when neither Docker nor `OXIA_TEST_ADDRESS` is available. |
| `OXIA_TEST_VERBOSE` | unset | `1` = print every docker command, with timings, to stderr. |

Clean up everything the fixture created (only containers labelled `oxia.net.tests=1` are touched):

```
pwsh tests/Oxia.TestSupport/scripts/clean-oxia-containers.ps1 [-List] [-Image]
```

## How the fixture works

```
 test process (dotnet test)                              Docker (Docker Desktop / Linux)
 ┌────────────────────────────────┐                      ┌───────────────────────────────────────────────┐
 │ OxiaServerFixture (assembly)   │   docker exec -i     │ container oxia-net-tests-<hash>   (reused)    │
 │  StartAsync(profile) ──────────┼──────────────────────►  /data = tmpfs                                 │
 │  - lease a free slot           │   sh -s (lock+start) │  slot 0: oxia standalone -p :6648 --shards 1   │
 │  - wait for leaders (raw gRPC) │                      │  slot 1: oxia standalone -p :6649 --shards 10  │
 │ OxiaServer.Address ────────────┼── 127.0.0.1:<port> ──►  ...    (tail -f /dev/null is PID 1+tini)     │
 │  CloseAsync(): kill + wipe     │                      └───────────────────────────────────────────────┘
 └────────────────────────────────┘
```

* **One container, reused.** Name `oxia-net-tests-<hash of image+slots+memory+cpus>`, label `oxia.net.tests=1`. Lookup order:
  running → reuse; stopped → `docker start`; missing → `docker run -d` (pulling the image only if it is not local). Nothing
  removes it at the end of a run unless `OXIA_TEST_CLEANUP=1`. Typical measured costs (Docker Desktop, Windows): create ~1.8 s,
  reuse ~0.65 s (docker version + inspect + port), start of a server inside it **~0.35 s**, release ~0.25 s.
* **Slots instead of a container per profile.** The container only idles (`tail -f /dev/null` under tini). A *slot* is a
  container port `6648+n` (published on `127.0.0.1` at an ephemeral host port, collision free, read back with `docker port`) and
  a directory on the container's tmpfs. `StartAsync(profile)` leases a free slot, wipes it and starts
  `oxia standalone <profile flags>` there with one `docker exec`, then polls `GetShardAssignments` with the generated
  `OxiaClientClient` (raw gRPC, independent of the client under test) until every shard has a leader.
  `OxiaServer.CloseAsync()` kills the process, wipes the directory and frees the slot.
  Result: **every test gets a fresh, empty server with exactly the configuration its Go counterpart sets**
  (`NumShards`, `KeySorting`, `NotificationsEnabled`, notification retention) — Go's isolation — and a server never outlives its
  test. A profile is one `OxiaServerProfile`: `Default` (1 shard, hierarchical, notifications on), `WithShards(10)`,
  `WithShards(4, ServerKeySorting.Natural)`, `Default with { NotificationsEnabled = false }`,
  `Default with { NotificationsRetention = TimeSpan.FromSeconds(1) }`.
* **Why this instead of one container per profile / shared servers with key prefixes.** The Go tests assert *absolute* server
  state: the first record gets version id 0 (`TestAsyncClientImpl`, `TestAsyncClientImpl_versionId`), notifications arrive in an
  exact order and *every* write of the shard shows up in a subscription (`TestSyncClientImpl_Notifications`), `RangeScan("", "")`
  lists the whole database, and eight different shard/sorting/notification/retention configurations are used. Key prefixes cannot
  isolate any of that, and a container per configuration would exceed the 3-container budget. A process per test inside one
  container gives exact Go semantics at ~0.35 s per test.
* **Why published ephemeral ports work.** `oxia standalone` answers `GetShardAssignments` with the `:authority` header the
  client used as the leader address (`oxiad/dataserver/assignment/assignment_dispatcher.go`, standalone branch) and accepts any
  authority. The address the client dials (`127.0.0.1:<host port>`) is therefore the leader address it gets back: no host
  networking, no fixed port 6648, no advertised-address flag needed.
* **Several test runs at once** (two worktrees, IDE + CLI, parallel agents) share the container safely: a slot is claimed with an
  atomic `mkdir /data/locks/<n>` containing the owner (`pid:machine:starttime`); a lock whose owner process no longer exists is
  reclaimed. If all slots are taken the run waits (up to 5 min).
* **Parallelism.** xUnit runs test classes in parallel (`xunit.runner.json`: at most 6 threads); each test leases its own slot.
  Memory: each `oxia standalone` is started with `--db-cache-size-mb 8 --wal-sync-data=false`, data on tmpfs (container limited
  to 3 GB).
* **Namespaces.** A standalone server serves only the `default` namespace (it cannot create others), so there is no
  `NextNamespace()`; isolation is the fresh server. (The coordinator based cluster could create namespaces through the admin API;
  no ported test needs it.)

### Which image

`oxia/oxia:main` on Docker Hub is **stale** (built 2026-08-30, revision `783841c`): it predates fields the pinned client uses
(e.g. the namespace `key_sorting` in the shard assignments), so the key sorting tests cannot pass against it.
`oxia/oxia:nightly-beta` is built every night from upstream `main`; the build of 2026-11-07 is **exactly the pinned commit
`8456f61e1ada7b786f5ef17798b45fbc17d0d246`** (`oxia --version` inside the container prints it; the fixture logs it when it creates
the container and `OxiaServerFixture.GetServerVersionAsync()` returns it). Because the tag moves, an old container keeps the old
image until you remove it with the clean script; set `OXIA_TEST_IMAGE` to pin another tag or a digest.

## Cluster tests (opt-in: `OXIA_TEST_CLUSTER=1`)

`LeaderHintTests` (6) and `TargetIpFlipTests` (2) need real replication and a coordinator; the Go tests build them in-process
(`mock.NewServer`, `coordruntime.New`). `OxiaClusterFixture` provides the equivalent with a **second reusable container**
(`oxia-net-cluster-<hash>`): a *cluster slot* is one `oxia coordinator` plus N `oxia server` processes started with
`docker exec`, configured from a generated cluster file (`namespaces: default, replicationFactor, initialShardCount`,
`servers: name/public/internal`, `allowExtraAuthorities`). Each server's `public` address in that file is its **published host
address**, so the leaders announced to clients are reachable from the test process; replication and coordinator traffic stay on
loopback inside the container. A cluster starts in ~0.4 s (leader elected); `OxiaCluster.StopAllServersAsync()` is Go's
`server.Close()` on every data server. Without the variable the 8 tests are reported as skipped (`ClusterGate`).

Total containers: at most 2 (servers, cluster).

## Not ported (listed in `docs/porting/T-integration.md`)

* `tests/security/tls` (5 client tests) and `tests/security/auth` (OIDC): they need data servers started with TLS / OIDC options.
  The cluster fixture can be extended (certificates are copied into the container with `docker exec -i ... cat > file`; the
  server flags are `--tls-cert-file/--tls-key-file/--tls-trusted-ca-file`, `--internal-tls-*`, `--peer-tls-*`), but the client
  side needs the `TlsOptions -> SslClientAuthenticationOptions` conversion of component B (`common/security`). The OIDC tests also need
  a token issuer reachable from inside the container. Left for a follow-up.
* Server-side suites (`tests/control`, `coordinator`, `scheduler`, `kvscript`, `embedded`, `mock`): they test the server.

## Writing a test

```csharp
public class MyTests(OxiaServerFixture oxia)               // constructor injection of the assembly fixture
{
    [Fact]
    public async Task TestSomething()
    {
        // Go: dataserver.NewStandalone(config) with config.NumShards = 4 ...
        await using var standaloneServer = await oxia.StartAsync(OxiaServerProfile.WithShards(4));
        var client = await OxiaClient.CreateSyncClientAsync(standaloneServer.ServiceAddr);   // Go: standaloneServer.ServiceAddr()
        ...
        await client.CloseAsync();
        await standaloneServer.CloseAsync();                                                   // Go: standaloneServer.Close()
    }
}
```

Helpers (`Oxia.TestSupport.TestUtil`): `Eventually` / `EventuallyWithT` (testify semantics: first check after one tick),
`Receive` / `Recv` / `ReceiveWithin` / `TryReceive` / `IsEmpty` / `DrainAsync` (Go channel idioms on `ChannelReader<T>`),
`CatchAsync` / `AssertErrorIs`, `RandomString`, `Sharding.ShardOf`. `RawOxia` offers raw gRPC put/get/assignments that do not
depend on the library under test.

## Troubleshooting

* *"Docker is not available"*: start Docker, or set `OXIA_TEST_ADDRESS`, or `OXIA_TEST_SKIP_IF_NO_SERVER=1` to skip.
* A failing start prints the last log lines of the server (`OxiaServer.GetLogAsync()` does the same on demand); inside the
  container: `docker exec <name> tail -n 100 /data/slot<n>/log`.
* Stuck/stale state: `clean-oxia-containers.ps1` and run again (costs ~2 s).
* Everything the fixture did is in `OxiaServerFixture.Events` (printed by the fixture self tests); `OXIA_TEST_VERBOSE=1` traces
  the docker calls.

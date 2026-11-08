# Porting & upstream-sync guide

**Upstream**: https://github.com/oxia-db/oxia — Go module `oxia/` (the client) and the client-relevant parts of `common/`.
**Pinned at**: `8456f61e1ada7b786f5ef17798b45fbc17d0d246` (2026-11-07, "fix(coordinator): bound the status requests that verify a shard (#1886)").
Every ported C# file carries a `PORT NOTE` header repeating this commit, the upstream path(s), the intent and the divergences.

## How to sync with a newer upstream

1. In a Go checkout: `git diff 8456f61e..<new> -- oxia/ common/ tests/client tests/sequence tests/resolver`.
2. For each changed file, look it up in the tables below (`docs/porting/*.md`) to find the C# file(s); the C# file's header
   lists any divergence you must keep (zero-allocation rewrites, .NET mappings).
3. Protos: copy `common/proto/{client,admin,metadata,replication}.proto` over `src/Oxia.Common/Proto/` and mirror new fields in
   `Proto/ClientMessages.cs`, the codec (`Proto/Codec`), and the oracle tests (`tests/Oxia.Client.Tests/Proto`) — the oracle
   tests fail loudly when a field is missing from the hand-written codec.
4. Port new/changed Go tests first (same name, same assertions), then the implementation until they pass.
5. Bump the commit hash in every header (`grep -rl 8456f61e` ) and in this file when the sync is complete.

## Documents

* [`DESIGN.md`](DESIGN.md) — goals, Go→C# mapping rules, performance rules, test-porting protocol.
* `docs/porting/<component>.md` — per-component table: upstream file → C# file → status → notes.
* `tests/Oxia.Client.IntegrationTests/README.md` — how the reusable Docker Oxia server fixture works.
* `benchmarks/README.md` — benchmark catalogue and the allocation budget per operation; results: `docs/benchmark-results.md`.
* `docs/performance.md` (throughput/latency work, .NET vs Go head-to-head), `docs/performance-allocation.md` (zero-allocation protocol and budgets).
* `docs/review-findings.md` — the independent fidelity review: 13 defects found, fixed and pinned by regression tests; what was verified faithful; parity decisions.

## Status (test parity with the Go suite)

| Go test scope | Go test functions | Ported (same names) | Notes |
|---|---|---|---|
| `oxia/` module (client unit tests) | **221** | **221** | components C 30, D 49, E 36, F1 45, F2 61 — nothing skipped |
| `common/` (client-relevant packages) | 98 (+3 `disable_trap`) | 97 run + 4 skipped | skipped: 3 `disable_trap` build-flavour tests, 1 server-side `Register` test |
| `common/rpc/logging_client_rpc_test.go` | 3 | 3 | component A |
| `tests/client`, `tests/sequence`, `tests/resolver`, `tests/assignments` (integration, real server) | 56 | 56 | 48 on the standalone Docker server, 8 on the opt-in coordinator cluster profile (`OXIA_TEST_CLUSTER=1`) |
| `tests/security/{tls,auth}` | 9 | 9 | cluster-profile fixture (`oxia server` has the TLS/auth flags, standalone does not); see `docs/porting/T2-security.md` |

Beyond the ports: differential/byte-exactness/fuzz tests of the hand-written wire codecs against the protoc-generated classes,
allocation-budget tests (Release only), concurrency stress tests, and the reusable-Docker integration fixture self-tests.
Run everything: `dotnet test tests/Oxia.Client.Tests -c Release` and `dotnet test tests/Oxia.Client.IntegrationTests -c Release`
(add `OXIA_TEST_CLUSTER=1` for the cluster profile).

## Upstream files deliberately NOT ported (client closure audit)

Every non-test Go file under `oxia/` has a counterpart (see the component tables). From `common/`, only the client's closure was
ported. Files left out, and why — check this list first when a sync diff touches them:

| Upstream file(s) | Why not ported |
|---|---|
| `common/proto/*.pb.go`, `*_grpc.pb.go`, `*_vtproto.pb.go` (client/admin/metadata/replication/storage) | generated code. Replaced by protoc output (Grpc.Tools, namespace `Io.Oxia.Proto.V1`) for admin/metadata/replication, and by the hand-written `Oxia.Common.Proto` messages + codecs for the client protocol (verified byte-for-byte against both protoc and the real Go vtproto) |
| `common/proto/{metadata_ext,metadata_ext_annotations}.go`, `storage.proto` | helpers/types for the coordinator and data-server metadata; no client code path |
| `common/proto/compat/*` | legacy `io.streamnative.oxia.proto` service names served by the server for old clients |
| `common/concurrent/{callback,callback_adaptor,condition,once,batcher}.go` | not used by the client |
| `common/{cache,commonio,codec,entity,metric,object,process,validation}`, `must.go` | server/CLI infrastructure (`metric` is replaced by `System.Diagnostics.Metrics`; `process.DoWithLabels` pprof labels have no .NET analogue) |
| `common/rpc/grpcmetrics` server half, `common/security` server half | server side (client halves are ported; the server TLS conversion is ported only because tests host TLS servers) |
| `common/failpoint/*_disable*.go` | the `disable_trap` build flavour does not exist here (its 3 tests are `Skip`ped with that reason) |

## Component tables

(aggregated here by the lead after each merge; see the individual files)

* [A — wire codec](docs/porting/A-wire-codec.md)
* [B — common utilities](docs/porting/B-common.md)
* [C — options & public types](docs/porting/C-options.md)
* [D — batching](docs/porting/D-batching.md)
* [E — shard manager & RPC provider](docs/porting/E-shard-rpc.md)
* [F1 — sessions, notifications, sequences, cache](docs/porting/F1-sessions.md)
* [F2 — clients & admin](docs/porting/F2-clients.md)
* [T — test infrastructure & integration tests](docs/porting/T-integration.md)
* [T2 — security (TLS/OIDC) integration tests](docs/porting/T2-security.md)
* [Z2 — throughput & latency](docs/porting/Z2-throughput.md)
* [Z1 — end-to-end allocation elimination (wave 2)](docs/porting/Z1-allocation.md) — design, ownership protocol and measurements: [`docs/performance-allocation.md`](docs/performance-allocation.md)

## Licence

Apache-2.0, same as upstream. Ported files keep the upstream copyright notice in `NOTICE`.

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
* `benchmarks/README.md` — benchmark catalogue and the allocation budget per operation.

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

## Licence

Apache-2.0, same as upstream. Ported files keep the upstream copyright notice in `NOTICE`.

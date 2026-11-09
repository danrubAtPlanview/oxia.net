# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project aims to follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html) once it is published. Until 1.0.0 the public API may still change in minor versions.

The version of the **upstream** Go client this port tracks is recorded separately: see [PORTING.md](PORTING.md) (currently pinned at
`8456f61e1ada7b786f5ef17798b45fbc17d0d246`). A sync with a newer upstream commit is listed here under "Changed".

## [Unreleased]

### Added

* Repository documentation for the public release: `README.md`, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `SECURITY.md`, `MAINTAINERS.md`, and the
  `docs/` guides (index, getting started, architecture, configuration, observability, upgrading from the Go client).
* GitHub configuration: CI workflow (build and unit tests on Linux and Windows, integration tests against the Docker server fixture), a manual-only
  benchmark workflow, Dependabot, issue and pull request templates, `CODEOWNERS`.
* `.editorconfig` and `.gitattributes`.

## [0.1.0] - 2026-11-09

Initial port of the Oxia Go client (`github.com/oxia-db/oxia`, module `oxia/` and the client-relevant part of `common/`) to .NET 10, pinned at upstream
commit `8456f61e1ada7b786f5ef17798b45fbc17d0d246`. Not yet published to NuGet: build from source.

### Added

* **Clients.** `IAsyncClient` (batched, `ValueTask` based, errors as values), `ISyncClient` (failures thrown), `IAdminClient`, and the typed `ICache<T>`, created
  through the static `OxiaClient` factory.
* **Operations.** Put, Get, Delete, DeleteRange; conditional writes (`ExpectedVersionId`, `ExpectedRecordNotExists`); ephemeral records and sessions; partition keys;
  sequence keys (`SequenceKeysDeltas`) and sequence updates; secondary indexes; floor / ceiling / lower / higher / equal gets; `List` and `RangeScan` with back-pressure and
  multi-shard key-order merge; notifications.
* **Transport and security.** TLS (`SslClientAuthenticationOptions`), static or function-supplied bearer-token authentication, custom service resolver
  (`IServiceResolver`), per-shard write streams with retry and leader-hint steering, shard-split handling that keeps the order of a caller's writes.
* **Observability.** `System.Diagnostics.Metrics` instruments with the names, units and tags of the Go client (meter `oxia_client`) and the gRPC client counters (meter `oxia`);
  logging through `Microsoft.Extensions.Logging` via `OxiaLog.Factory`.
* **Hand-written zero-allocation wire codecs** for the client protocol, verified byte-for-byte against protoc output and against the real Go `vtproto` implementation;
  protos are verbatim copies of upstream.
* **Opt-in pooled read API** (`Oxia.Client.Pooled`: `GetPooledAsync`, `RangeScanPooledAsync`, `PooledGetResult`) that removes the per-value and per-record allocations.
* **Extensions over the Go client** (documented in the `PORT NOTE` of the file): `OxiaOptions.WithMaxBatchSize`, `OxiaOptions.WithBatchQueueCapacity`, the pooled read API,
  and `TryGetAsync` on the cache.
* **Tests.** All 221 test functions of the Go `oxia/` module and the client-relevant tests of `common/` ported with their original names and assertions (nothing skipped for `oxia/`);
  the 56 Go integration tests of `tests/client`, `tests/sequence`, `tests/resolver`, `tests/assignments` and the 9 TLS / OIDC tests of `tests/security`, against a real server in
  reusable Docker containers; codec differential / fuzz tests, allocation-budget tests, concurrency stress tests. See [PORTING.md](PORTING.md).
* **Benchmarks.** BenchmarkDotNet suites, an end-to-end allocation profiler, and a load generator with a Go counterpart for head-to-head runs on the same server;
  results in [docs/benchmark-results.md](docs/benchmark-results.md) and [docs/performance.md](docs/performance.md).

### Fixed

Defects found by the independent fidelity review and pinned by regression tests (details in [docs/review-findings.md](docs/review-findings.md)): write reordering while a shard
split is absorbed, writes racing a split failing with `ErrShardNotFound`, a plain write overtaking an earlier ephemeral put, metrics observers changing operation outcomes,
silent truncation of a cancelled `RangeScan`, unbounded buffering in `List`/`RangeScan`, a `DeleteRange` with a null bound that never completed, a leaked TCP connection on close,
a send on an ended write stream not being retried, stale-size detection in the chunked wire writers, and a race between `Future<T>.Reset()` and a late completer.

[Unreleased]: https://github.com/danrubAtPlanview/oxia.net/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/danrubAtPlanview/oxia.net/releases/tag/v0.1.0

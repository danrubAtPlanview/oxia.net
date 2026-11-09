# Contributing to oxia.net

Thank you for contributing to oxia.net, the unofficial .NET port of the [Oxia](https://github.com/oxia-db/oxia) Go client. We appreciate your time and help.
This document explains how to build and test the code, and the few rules that make this repository different from an ordinary library: it is a
**port**, so fidelity to the upstream Go client, test parity and the ability to sync with upstream come first.

## Contents

* [Code of Conduct](#code-of-conduct)
* [Questions, bugs and ideas](#questions-bugs-and-ideas)
* [Development setup](#development-setup)
* [Build and test](#build-and-test)
* [The rules of a port](#the-rules-of-a-port)
* [Syncing with a newer upstream](#syncing-with-a-newer-upstream)
* [Performance and allocation rules](#performance-and-allocation-rules)
* [Code style](#code-style)
* [Documentation](#documentation)
* [Submitting changes](#submitting-changes)
* [What CI checks](#what-ci-checks)
* [AI-assisted contributions](#ai-assisted-contributions)
* [Licence of contributions](#licence-of-contributions)

## Code of Conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md). Be kind and respectful to the members of the community, and take time to
educate others who are seeking help. Harassment of any kind will not be tolerated.

## Questions, bugs and ideas

* **Questions about using the client**: start a [discussion](https://github.com/danrubAtPlanview/oxia.net/discussions).
* **Bugs**: before filing an issue, check the existing issues. Then use the [bug report form](https://github.com/danrubAtPlanview/oxia.net/issues/new/choose)
  and give detailed steps to reproduce, the client commit or version, the .NET runtime, the Oxia server version, and the current behaviour versus
  the expected one. If you can, say whether the **Go client** behaves differently in the same scenario: this port aims to be behaviourally identical,
  so a difference is a bug even when the .NET behaviour looks reasonable.
* **Bugs in the server or in the Go client** belong to [oxia-db/oxia](https://github.com/oxia-db/oxia/issues).
* **Security vulnerabilities**: never in public. See [SECURITY.md](SECURITY.md).
* **Features**: use the [feature request form](https://github.com/danrubAtPlanview/oxia.net/issues/new/choose). New client *behaviour* is normally added upstream
  first and then ported; a feature the Go client does not have is a deliberate extension and needs a good reason (the existing ones are listed in the
  [README](README.md#deliberate-extensions-over-the-go-client)).

## Development setup

You need:

* the **.NET SDK 10.0.400 or later** (`global.json` pins 10.0.400 and rolls forward to newer feature bands);
* **Docker** to run the integration tests (Docker Desktop on Windows/macOS, the native engine on Linux). Without Docker you can still run the unit tests,
  or point the integration tests at a server you started yourself;
* optional: **PowerShell 7** (the helper scripts are `.ps1`), **Go** (only for the head-to-head benchmark against the Go client), a checkout of the upstream
  repository at the pinned commit (only for porting work and for building the Go side of the load generator).

```
git clone https://github.com/danrubAtPlanview/oxia.net
cd oxia.net
dotnet restore Oxia.slnx
```

## Build and test

Warnings are errors in the library and test projects; please keep it that way.

```
dotnet build Oxia.slnx -c Release -warnaserror
dotnet test tests/Oxia.Client.Tests -c Release                      # unit tests, about a minute; allocation tests only mean something in Release
dotnet test tests/Oxia.Client.IntegrationTests -c Release           # needs Docker (or OXIA_TEST_ADDRESS)
OXIA_TEST_CLUSTER=1 dotnet test tests/Oxia.Client.IntegrationTests -c Release   # also the coordinator-based cluster tests
```

(PowerShell: `$env:OXIA_TEST_CLUSTER = "1"` before the command.)

The unit tests use in-process gRPC servers (`Oxia.TestSupport.TestGrpcServer`) and need nothing else.

### The Docker integration suite

The integration tests are the ported Go integration tests (`tests/client`, `tests/sequence`, `tests/resolver`, `tests/assignments`) and the TLS / OIDC security
tests (`tests/security`). They run against a **real Oxia server** in **reusable Docker containers**: one idle container hosts many short-lived `oxia` processes, so
a run neither pulls nor starts a container per test, and every later run (and every other checkout) reuses it. The server image is `oxia/oxia:nightly-beta`
(the build of the pinned upstream commit; the `main` tag on Docker Hub is stale and predates fields the pinned client uses).

Environment variables (`OXIA_TEST_IMAGE`, `OXIA_TEST_ADDRESS`, `OXIA_TEST_CLUSTER`, `OXIA_TEST_CLEANUP`, ...), how the fixture works, the cluster profile and
the cleanup script are documented in [`tests/Oxia.Client.IntegrationTests/README.md`](tests/Oxia.Client.IntegrationTests/README.md). Useful ones:

| Variable | Meaning |
|---|---|
| `OXIA_TEST_ADDRESS=host:port` | use a server you started with `oxia standalone` instead of Docker (tests that need another server profile are skipped) |
| `OXIA_TEST_CLUSTER=1` | also run the tests that need a coordinator and three data servers |
| `OXIA_TEST_SKIP_IF_NO_SERVER=1` | skip instead of fail when neither Docker nor `OXIA_TEST_ADDRESS` is available |
| `OXIA_TEST_CLEANUP=1` | remove the containers when the run ends (default: keep them, that is the reuse) |

Clean up everything the fixture created (only containers labelled `oxia.net.tests=1` are touched): `pwsh tests/Oxia.TestSupport/scripts/clean-oxia-containers.ps1`.

### Test layout

| Project | What it holds |
|---|---|
| `tests/Oxia.Client.Tests` | ported Go unit tests of `oxia/` and `common/`, codec oracle / differential / fuzz tests, allocation-budget tests, concurrency and review regression tests |
| `tests/Oxia.Client.IntegrationTests` | ported Go integration tests against a real server (standalone, cluster, TLS, OIDC) |
| `tests/Oxia.TestSupport` | shared infrastructure: in-process gRPC test server, Docker server fixture, mock OIDC provider, test PKI |

## The rules of a port

The full contract is [DESIGN.md](DESIGN.md); read it before your first change. The rules that reviewers check:

1. **Faithful first.** Same behaviour, same edge cases, same error semantics and messages, same wire protocol as the Go client. When Go and "idiomatic .NET"
   disagree, Go behaviour wins; the API *shape* is made .NET-friendly only where Go's shape cannot exist (channels, `context.Context`, multiple returns).
2. **`PORT NOTE` header on every ported file**, naming the upstream file(s), the pinned commit, the intent, and every deliberate divergence and why:

   ```csharp
   // =====================================================================================================================
   // PORT NOTE
   //   Upstream : <path(s) in the Go repo, e.g. oxia/internal/shard_manager.go>
   //   Pinned at: 8456f61e1ada7b786f5ef17798b45fbc17d0d246
   //   Intent   : <what this file is for, in a few lines>
   //   Divergences: <every deliberate difference from Go and why>
   // =====================================================================================================================
   ```

   A Go file maps to one C# file with the same name in PascalCase (`shard_manager.go` becomes `ShardManager.cs`). Keep the row for the file in the matching
   `docs/porting/<component>.md` table up to date (upstream file, C# file, status, notes).
3. **Tests are ported, never weakened.** Every Go test gets a C# test with the **same name, scenario and assertions** (`TestFooBar` in class `FooBarTests`;
   `t.Run` subtests become `[Theory]` rows or facts named `TestFooBar_Sub`). If a ported test fails, the implementation (or the test harness) is wrong:
   **never edit an assertion to make it pass.** A test that genuinely cannot run in .NET stays as `[Fact(Skip = "reason")]` with the reason documented, not deleted.
   Timing-based tests keep their timings; if one is flaky, report it instead of widening it.
4. **Extra tests are welcome** (intent, edge, property, fuzz, concurrency): put them in `*.Extra.cs` files. They never replace ported tests.
5. **Behavioural contract, not optimisation target.** Where a Go detail looks like it only exists for ordering or safety (one batch in flight per shard,
   `ErrShardNotFound` re-routing, writes held during a shard split, dropping cancelled calls before sending), it is part of the contract. A change that
   alters it needs a regression test and an entry in the "Decisions log" of `DESIGN.md`.
6. **Protos are verbatim.** `src/Oxia.Common/Proto/*.proto` are copies of the upstream files; syncing means copying files. Never edit them by hand.
7. **Never import `Io.Oxia.Proto.V1` and `Oxia.Common.Proto` unaliased in the same file** (identical type names); tests use `using G = Io.Oxia.Proto.V1;`.

## Syncing with a newer upstream

[PORTING.md](PORTING.md) is the procedure; in short:

1. In a Go checkout: `git diff 8456f61e..<new> -- oxia/ common/ tests/client tests/sequence tests/resolver`.
2. For each changed file, find the C# counterpart in `docs/porting/*.md`; its `PORT NOTE` lists the divergences you must keep.
3. Copy changed protos and mirror new fields in `ClientMessages.cs`, the codec and the oracle tests (they fail loudly when the hand-written codec misses a field).
4. Port the new or changed Go tests first (same names, same assertions), then the implementation until they pass.
5. Bump the commit hash in every header (`git grep -l 8456f61e`) and in `PORTING.md` when the sync is complete.

## Performance and allocation rules

The hot path (put / get / delete batching, wire encode / decode, shard routing) is written for zero allocation. On per-operation paths:

* no LINQ, no closures or lambdas, no `params object[]`, no boxing, no string concatenation or `string.Format`; use `static` lambdas or state-passing overloads;
* per-operation objects are classes designed for pooling (`Reset()`), completed through `IValueTaskSource<T>`; never a `Task` or `TaskCompletionSource` per operation;
* spans, `ArrayPool<byte>`, `stackalloc`; no exceptions for expected outcomes (key not found, version mismatch travel as values);
* `ValueTask`-first, `ConfigureAwait(false)`, never block a thread in library code;
* pooled response messages follow the **ownership protocol** in [`docs/performance-allocation.md`](docs/performance-allocation.md): `Release()` exactly once, by the owner,
  after the last read; never keep a child of a response.

If you touch such code, run the allocation-budget tests and the relevant benchmarks in **Release** and put the before / after in the pull request:

```
dotnet test tests/Oxia.Client.Tests -c Release --filter "FullyQualifiedName~Oxia.Client.Tests.Alloc"        # budgets; needs at least 14 cores, run alone (skips itself on small machines)
dotnet run -c Release --project benchmarks/Oxia.Client.Benchmarks -- --filter "*Codec*" --job short --memory
dotnet run -c Release --project benchmarks/Oxia.Client.Benchmarks -- alloc                                  # end-to-end allocation profile
```

The benchmark catalogue and the allocation budgets per operation are in [`benchmarks/README.md`](benchmarks/README.md). Do not loosen a budget to make a test pass;
explain in the pull request why a budget has to change. Benchmark numbers from a shared machine are noisy (10-20 %): quote medians, and say how you measured.

## Code style

[`.editorconfig`](.editorconfig) describes the style the code base already uses: 4 spaces, Allman braces, file-scoped namespaces, usings outside the namespace,
`_camelCase` private fields, PascalCase constants and static readonly fields, `I` prefix on interfaces, LF line endings. Match the surrounding code. CI does not enforce
formatting (hand-aligned tables in the wire codecs would be reformatted); only format the lines you change, and do not mix reformatting with functional changes.

## Documentation

Behaviour, options and operational advice are documented in [`docs/`](docs/index.md). A change that alters a public API, an option, a default, a metric or a documented
behaviour must update the matching document in the same pull request. Every claim in the documentation must be checkable in the code or in the measured results:
do not copy numbers that no benchmark produced.

## Submitting changes

1. **Fork** the repository and create a feature branch from the default branch (`git checkout -b my-change`).
2. **Make the change** with its tests, and run the commands under [Build and test](#build-and-test).
3. **Commit** with a clear message. The history uses a short scope prefix and an imperative summary:

   ```
   batching: keep one caller's writes ordered while a shard split is absorbed

   Longer explanation of the problem and of the fix, wrapped at about 72 columns.
   Mention the Go behaviour you are matching and the test that pins it.
   ```

   Prefer small, focused commits. Sign-off (`git commit -s`, the [Developer Certificate of Origin](https://developercertificate.org/)) is welcome but not required.
4. **Open a pull request** and fill in the template: motivation, modifications, behaviour relative to Go, testing, and AI usage. Link the issue (`Fixes #123`).
5. A maintainer reviews. Expect questions about fidelity to the Go client, about test coverage, and for hot-path changes about allocations.

Quick steps:

```
git checkout -b my-change
# ... edit, then
dotnet build Oxia.slnx -c Release -warnaserror
dotnet test tests/Oxia.Client.Tests -c Release
git commit -m "scope: summary"
git push origin my-change
```

## What CI checks

The [CI workflow](.github/workflows/ci.yml) runs on every pull request and push to the default branch:

| Job | What it runs |
|---|---|
| `build-test` (Linux and Windows) | `dotnet build Oxia.slnx -c Release -warnaserror`, then `dotnet test tests/Oxia.Client.Tests -c Release`; TRX results are uploaded |
| `integration` (Linux) | `dotnet test tests/Oxia.Client.IntegrationTests -c Release` against the Docker server fixture (standalone profile) |
| `integration-cluster` (Linux) | the same project with `OXIA_TEST_CLUSTER=1`: coordinator-based tests plus TLS / OIDC. Marked `continue-on-error` until the Linux validation is complete |

The allocation-budget tests skip themselves on runners with fewer than 14 cores, so a hot-path change must also be run locally. A manual-only
[`benchmarks` workflow](.github/workflows/benchmarks.yml) runs the BenchmarkDotNet codec and common suites on a GitHub-hosted runner; the end-to-end Linux
benchmark on AWS is run by the maintainers (see [`bench/aws`](bench/aws/PLAN.md)) and never needs credentials from contributors.

## AI-assisted contributions

AI tools are welcome. You remain responsible for what you submit: review the code, make sure you can explain why it is correct, edit generated prose for accuracy and
brevity, and describe in the "AI usage" section of the pull request how the tools were used. Coding agents must complete that section accurately, including their own
involvement, and must not claim that the contributor has reviewed the changes unless the contributor confirmed it.

## Licence of contributions

oxia.net is licensed under the [Apache License 2.0](LICENSE), like upstream. By submitting a contribution you agree that it is licensed under the same terms
(Section 5 of the licence). Do not submit code you are not allowed to license this way; ported code keeps the upstream copyright notice recorded in [`NOTICE`](NOTICE).

# Documentation

oxia.net is an unofficial .NET port of the [Oxia](https://github.com/oxia-db/oxia) Go client. This page is the map of everything written about it. If you are new, read the [README](../README.md) and then [Getting started](getting-started.md).

## Using the client

| Document | What it answers |
|---|---|
| [README](../README.md) | what it is, features, installation, quick start, API mapping, performance summary, project status |
| [Getting started](getting-started.md) | a first program step by step: server, project, writes, reads, scans, compare-and-swap, ephemeral locks, notifications, troubleshooting |
| [Configuration](configuration.md) | every client option and operation option with defaults and validation; TLS, authentication, custom resolver; recommended settings |
| [Observability](observability.md) | metrics (instrument names, units, tags), how to collect them with OpenTelemetry, logging categories |
| [Coming from the Go client](upgrading-from-go.md) | side-by-side Go and C# snippets, and the list of behavioural differences |

## Understanding it

| Document | What it answers |
|---|---|
| [Architecture](architecture.md) | batching per shard, ordering guarantees, shard manager and split handling, sessions, notifications, wire codecs, pooling and ownership |
| [Performance](performance.md) | throughput and latency work, the .NET versus Go head-to-head against a real server, what did not help, recommended settings |
| [Allocation](performance-allocation.md) | the zero-allocation design, per-operation budgets, the response ownership protocol, the pooled read API |
| [Benchmark results](benchmark-results.md) | every micro-benchmark (BenchmarkDotNet) and the end-to-end comparison, generated from a full run |
| Linux benchmark on AWS | [`docs/benchmark-aws.md`](benchmark-aws.md) (the environment and method are described in [`bench/aws/PLAN.md`](../bench/aws/PLAN.md)) |
| [Review findings](review-findings.md) | the independent fidelity review: 13 defects found and fixed, what was verified, what is left as upstream parity |
| [Linux validation](linux-validation.md) | the test suite run on Linux (written separately; see the file for its status) |

## Porting and contributing

| Document | What it answers |
|---|---|
| [`DESIGN.md`](../DESIGN.md) | the design contract: goals, Go-to-C# mapping rules, performance rules, test-porting protocol, decisions log |
| [`PORTING.md`](../PORTING.md) | test-parity status, files deliberately not ported, how to sync with a newer upstream |
| [`docs/porting/`](porting/) | per-component tables mapping each upstream file to its C# file, with status and divergences |
| [`CONTRIBUTING.md`](../CONTRIBUTING.md) | build, test, the rules of a port, commit style, what CI checks |
| [`CHANGELOG.md`](../CHANGELOG.md) | notable changes per version |
| [`SECURITY.md`](../SECURITY.md) | how to report a vulnerability, what is in scope |
| [`CODE_OF_CONDUCT.md`](../CODE_OF_CONDUCT.md), [`MAINTAINERS.md`](../MAINTAINERS.md) | community rules and who runs the project |

The per-component files in `docs/porting/`:

* [A: wire codec](porting/A-wire-codec.md)
* [B: common utilities](porting/B-common.md)
* [C: options and public types](porting/C-options.md)
* [D: batching, model, metrics](porting/D-batching.md)
* [E: shard manager and RPC provider](porting/E-shard-rpc.md)
* [F1: sessions, notifications, sequence updates, cache](porting/F1-sessions.md)
* [F2: clients and admin](porting/F2-clients.md)
* [T: test infrastructure and integration tests](porting/T-integration.md)
* [T2: security (TLS and OIDC) integration tests](porting/T2-security.md)
* [Z1: end-to-end allocation elimination](porting/Z1-allocation.md)
* [Z2: throughput and latency](porting/Z2-throughput.md)

## Testing and benchmarking

| Document | What it answers |
|---|---|
| [Integration tests](../tests/Oxia.Client.IntegrationTests/README.md) | the reusable Docker server fixture, environment variables (`OXIA_TEST_*`), the cluster / TLS / OIDC profiles, troubleshooting |
| [Benchmarks](../benchmarks/README.md) | the BenchmarkDotNet catalogue, the `alloc` and `load` command-line modes, allocation budgets |
| [AWS benchmark plan](../bench/aws/PLAN.md) | the on-demand, always-torn-down AWS environment for the Linux head-to-head against the Go client |

## Conventions used in the documents

* Numbers are copied from the measurements in this repository, never invented; the machine and method are stated next to them. Treat them as ratios: the development machine was shared and noisy.
* `PORT NOTE` is the header at the top of every ported source file (upstream path, pinned commit, intent, divergences). It is the authoritative description of what a file does differently from Go.
* The pinned upstream commit is `8456f61e1ada7b786f5ef17798b45fbc17d0d246`.

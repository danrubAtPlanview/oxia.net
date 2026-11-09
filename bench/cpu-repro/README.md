# CPU-per-operation repro (Linux, in Docker)

Local tooling behind the investigation in [`docs/performance.md`](../../docs/performance.md#11-follow-up-linux-why-the-net-client-burns-more-cpu-per-operation-than-go-when-every-operation-is-its-own-rpc)
(section 11): why the .NET client uses more client CPU per operation than the Go client in the scenarios without batching. It runs the **same Linux load-generator binaries as the AWS benchmark** in an
Amazon Linux container against a freshly created `oxia standalone` container, on one Docker network. No AWS account is needed; nothing here is part of the product or of the AWS tooling.

Absolute throughput is that of a Docker Desktop VM and lower than on AWS: compare the CPU per operation, and compare runs of this tool with each other.

Prerequisites: Docker (Linux containers), a POSIX shell (Git Bash works), and the artifacts of the AWS benchmark in `bench/aws/.artifacts/x64`
(`pwsh bench/aws/scripts/Publish-Artifacts.ps1`, which needs a checkout of the Go client, see [`bench/aws/README.md`](../aws/README.md)).

| Script | What it does |
|---|---|
| `cpu.sh setup` | creates the network, the client container (privileged, so that `perf` works; `perf` is installed in it) and a server container |
| `cpu.sh run <dotnet\|go> <A\|B\|C\|D\|F\|G> [--tune k=v] [-- ENV=VAL ...]` | one run of a scenario of `bench/aws/suites/standard.json` against a **fresh** server; prints the `RESULT` line. `DURATION`, `WARMUP` override the 15 s / 3 s |
| `matrix.sh` | Go, .NET default and .NET with `DOTNET_ThreadPool_UnfairSemaphoreSpinLimit=0` for each scenario (`SCENARIOS="F G"` to select) |
| `threads.sh <impl> <scenario>` | CPU per thread (grouped by thread name) during the measured window, from `/proc/<pid>/task` |
| `perf.sh <impl> <scenario>` | flat `perf` profile of the load tool during the measured window (.NET runs with the perf map on, ReadyToRun and W^X off so that JIT frames resolve; `CALLGRAPH=1`, `TOP=n`) |

```sh
bash bench/cpu-repro/cpu.sh setup
bash bench/cpu-repro/cpu.sh run dotnet F
bash bench/cpu-repro/cpu.sh run dotnet F -- DOTNET_ThreadPool_UnfairSemaphoreSpinLimit=0
DURATION=40s SETTLE=18 bash bench/cpu-repro/threads.sh dotnet F
```

Remove the containers afterwards: `docker rm -f oxia-cpu-server oxia-cpu-client; docker network rm oxia-cpu`.
Do not run it while the unit tests or other benchmarks are running: the numbers are CPU measurements.

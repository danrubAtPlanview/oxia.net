# Node side of the AWS benchmark: artifacts, node scripts, local rehearsal

Everything here is the contract of `PLAN.md` section 7 ("Node-side contract"), implemented once and used by the AWS orchestrator **and** by the free
local rehearsal. Paths: `bench/aws/node/*.sh` (run on the instances / in the containers), `bench/aws/scripts/Publish-Artifacts.ps1` (builds the Linux
files on the operator machine), `bench/aws/scripts/Rehearse.ps1` (the same sequence in two local containers).

```
operator machine                                         node (AL2023 instance or rehearsal container)
Publish-Artifacts.ps1 -Arch x64 ──► <OutDir>/ ──S3/mount──► install-artifacts.sh ──► /opt/oxia-bench/bin/*   (flat)
                                                           bootstrap-node.sh server|client
                                                           oxia-server.sh reset|stats      run-load.sh      sysinfo.sh
```

## 1. Artifacts: `Publish-Artifacts.ps1`

```powershell
pwsh bench/aws/scripts/Publish-Artifacts.ps1 -Arch x64|arm64 [-OutDir <dir>] [-OxiaImage oxia/oxia:nightly-beta] [-Force] [-SkipVerify] [-GoRef <checkout>]
```

PowerShell 7, needs `docker`, `dotnet` (SDK of `global.json`), `go` (the toolchain downloads the `go 1.27` toolchain on first use) and `git`. Default `-OutDir`
is `bench/aws/artifacts/<arch>` (git-ignored; a `.gitignore` containing `*` is written into any `-OutDir` that lies inside the repo). Progress goes to the
host stream; **the last output line is the output directory** (`$dir = & Publish-Artifacts.ps1 ...` captures exactly that).

Output (x64 values from the rehearsal run; arm64 is about 10 MiB smaller):

| File | What | Size (x64) |
|---|---|---|
| `oxia` | the Oxia **server**, byte for byte the `/oxia/bin/oxia` of `oxia/oxia:<tag>` for `linux/<arch>` (`docker create --platform` + `docker cp`) | 96.8 MiB |
| `ld-musl-<x86_64\|aarch64>.so.1` | the musl loader from the same image (see below); **must travel with `oxia`** | 0.6 MiB |
| `oxia-load-dotnet` | `benchmarks/Oxia.Client.Benchmarks`, `dotnet publish -c Release -r linux-<x64\|arm64> --self-contained -p:PublishSingleFile=true -p:IncludeNativeLibrariesForSelfExtract=true -p:EnableCompressionInSingleFile=true -p:SatelliteResourceLanguages=en`; use it as `oxia-load-dotnet load ...` | 57.5 MiB |
| `oxia-load-go` | the Go twin, `GOOS=linux GOARCH=<amd64\|arm64> CGO_ENABLED=0 -trimpath -ldflags "-s -w"` (`Load/go/build.ps1 -GoOS linux -GoArch ...`), fully static | 15.7 MiB |
| `node/*.sh` | `bench/aws/node/*.sh`, LF line endings enforced on copy | 60 KiB |
| `SHA256SUMS` | `sha256sum -c` format, relative paths; checked by `install-artifacts.sh` | |
| `VERSIONS.json` | see below | |

Total about 171 MiB (x64), 161 MiB (arm64); a full build takes 25-50 s, a cache hit under 2 s.

### How the Oxia binary is obtained, and proven to run on Amazon Linux 2023

The official image is Alpine based and the binary is built with cgo (`go version -m` says `CGO_ENABLED=1`; `github.com/DataDog/zstd` is cgo), so it is **not static**: its ELF
interpreter is `/lib/ld-musl-x86_64.so.1` (`/lib/ld-musl-aarch64.so.1`), which a glibc distribution such as AL2023 does not have, and it fails with
`cannot execute: required file not found`. Options considered: (a) rebuild from the pinned source with `CGO_ENABLED=0` (a different binary than the image; needs the Go
toolchain and the upstream checkout; changes the zstd implementation), (b) wrap the binary with a loader invocation (changes the process name and arguments), (c) **ship musl's loader
from the same image and install it at the interpreter path** (what Alpine's `musl` package does; musl lives in its own paths and coexists with glibc). (c) was chosen:
the server is the unmodified image binary, its sha256 is recorded, and nothing about the process model changes.

* `Publish-Artifacts.ps1` parses the ELF program headers of the extracted binary (machine type and `PT_INTERP`), copies the loader named there out of the image
  (`docker cp -L`) and fails if the interpreter is anything it does not know how to ship. A static binary (a future image) would need nothing and nothing is shipped.
* `node/lib.sh: ensure_musl_loader` (called by `bootstrap-node.sh server` and by `oxia-server.sh start`) installs the loader at the interpreter path when it is missing.
* Proof: before `VERSIONS.json` is written, the script runs inside `amazonlinux:2023` for the target platform (an arm64 container under qemu on an x64 machine) the real node code
  path `ensure_musl_loader` + `oxia --version`, `oxia-load-go -h`, `oxia-load-dotnet load --server echo ...` and `oxia-load-dotnet load --server echo-proc --go oxia-load-go --go-only ...`
  (the Go twin against the echo server). Anything that fails aborts the publish. `-SkipVerify` omits this (it is also skipped, with a warning, when the platform cannot be executed).
  `VERSIONS.json.verification` records the verification image digest.

### `VERSIONS.json` (schema 1)

```
schema, arch, rid, platform, built_utc, build_host{os,powershell}
client{commit, branch, commit_utc, dirty}
dotnet{sdk, runtime, rid, single_file, compressed, settings_in_effect, publish_args}   settings_in_effect = "workstation GC, concurrent GC off, TieredPGO True" (read from the exe's own output)
go{version, goos, goarch, cgo=false, ref_commit, ref}                                   version = the toolchain that built oxia-load-go (go version -m)
oxia{version_output, image, image_digest, image_id, platform, linkage, musl_loader, source_path_in_image}
verification{performed, image, image_digest, platform, oxia, oxia_load_go, oxia_load_dotnet, verified_utc}
files{ "<relative path>": {sha256, bytes} ... }
```

`oxia.image_digest` is `docker image inspect --format '{{index .RepoDigests 0}}'` (a registry digest, e.g. `oxia/oxia@sha256:5c49035d...`; pass `-OxiaImage oxia/oxia@sha256:...` to pin).

### Caching

A rerun does nothing (prints "up to date") when `-OutDir/VERSIONS.json` is for the same `-Arch`, the same **clean** commit of this repository, the same commit of the Go reference checkout, the
same `-OxiaImage` with an unchanged local image digest, every recorded file still has its recorded sha256 and the node scripts equal their sources. A dirty working tree always rebuilds
(the builds are incremental); `-Force` rebuilds regardless. `VERSIONS.json` is written last, so an interrupted build is never mistaken for a complete one.
The .NET single file is not byte-reproducible across commits (the commit id is embedded), which is why the cache key is the commit and not a content hash.

## 2. Node scripts (`bench/aws/node/`)

All scripts: bash, `set -euo pipefail`, shellcheck-clean (`koalaman/shellcheck`), LF line endings (`bench/aws/.gitattributes`), never prompt, **machine-readable output on stdout, human log on
stderr**, non-zero exit on failure, `--help` prints the full description (the comment block at the top of each file is the reference). Run as root (SSM Run Command does). They find the binaries
via `$OXIA_BENCH_BIN` (default `/opt/oxia-bench/bin`), then next to the script, then one level up (so they also work straight from the unpacked artifact directory, where they sit in `node/`).

| Environment variable | Default | Meaning |
|---|---|---|
| `OXIA_BENCH_HOME` / `OXIA_BENCH_BIN` | `/opt/oxia-bench` / `$OXIA_BENCH_HOME/bin` | install root / where binaries and scripts live |
| `OXIA_BENCH_DATA` | `/data` | server data root: `$OXIA_BENCH_DATA/wal` and `/db` (mount the gp3 volume here) |
| `OXIA_BENCH_STATE` | `/var/lib/oxia-bench` | `node.json`, `server.env`, pid files, .NET self-extract directory |
| `OXIA_BENCH_LOGDIR` | `/var/log/oxia-bench` | `oxia-server.log` (+`.1`), `cpu.log` (+`.1`) |
| `OXIA_BENCH_DATA_DEVICE` | unset | `bootstrap-node.sh server`: `auto` or a device: mkfs.xfs if empty, mount on `$OXIA_BENCH_DATA` (`noatime`) |
| `OXIA_BENCH_SKIP_PACKAGES=1` | | never call dnf/apt |
| `OXIA_BENCH_NO_IMDS=1` | | `sysinfo.sh`: skip the instance-metadata probe |

### `install-artifacts.sh SRC_DIR [DEST_DIR=/opt/oxia-bench/bin]`
Verifies `SRC_DIR/SHA256SUMS`, then copies **flat** into `DEST_DIR`: `oxia`, `ld-musl-*.so.1`, `oxia-load-dotnet`, `oxia-load-go`, `VERSIONS.json`, `SHA256SUMS` and `node/*.sh`, `chmod 0755`. Last stdout line: `DEST_DIR`.
It is the single step between "files arrived on the node" and `bootstrap-node.sh`; run it with `bash <SRC_DIR>/node/install-artifacts.sh <SRC_DIR>`.

### `bootstrap-node.sh server|client`
Idempotent (run it as often as you like), offline once the packages exist. Installs `jq`, `procps-ng`, `util-linux` if missing (dnf on AL2023, apt on Debian/Ubuntu; 3 attempts); sysctl
`net.core.somaxconn=4096`, `net.ipv4.tcp_max_syn_backlog=8192`, `net.core.netdev_max_backlog=16384`, `fs.file-max=2097152`, `vm.swappiness=1`, `vm.max_map_count=1048576` (live +
`/etc/sysctl.d/99-oxia-bench.conf`); `/etc/security/limits.d/99-oxia-bench.conf` (nofile 1048576, root included) and, with systemd, `DefaultLimitNOFILE=1048576`; THP `enabled`/`defrag` = `never`;
directories; server: `$OXIA_BENCH_DATA/{wal,db}`, the musl loader, optionally the data volume. Anything that cannot be applied (read-only `/proc/sys` and `/sys` in a container) is logged as `skipped:`
and recorded, the script still succeeds. Writes `/var/lib/oxia-bench/node.json` (role, time, kernel, CPU, memory, every tuning with its value, `tuning_applied[]`, `tuning_skipped[]`).
stdout: `{"bootstrapped":true,"role":"server","applied":N,"skipped":M}`.

### `oxia-server.sh start|stop|reset|status|stats`

```
oxia-server.sh start|reset [--shards 1] [--sync false] [--notifications true] [--port 6648] [--metrics-port 7648] [--key-sorting hierarchical] [--db-cache-mb N] [--wait 30] [--oxia-arg A]...
oxia-server.sh stop | status | stats --since EPOCH [--until EPOCH]
```

* Runs `oxia standalone --shards N --public-addr 0.0.0.0:<port> --metrics-addr 127.0.0.1:<metrics-port> --data-dir $OXIA_BENCH_DATA/db --wal-dir $OXIA_BENCH_DATA/wal --wal-sync-data=<sync>
  --notifications-enabled=<n> --key-sorting hierarchical` (the flags of `tests/Oxia.TestSupport/OxiaServerFixture.cs` / `cmd/standalone/cmd.go`; no `--db-cache-size-mb` unless asked, so the server default applies).
  Defaults: 1 shard, **WAL fsync off** (the plan's "fast" profile), notifications on (server default).
* **Process model**: PID 1 is systemd -> transient unit `oxia-bench-server.service` (`systemd-run --collect`, `LimitNOFILE=1048576`, `Restart=no`, `TasksMax=infinity`, `TimeoutStopSec=30`, output appended to
  `oxia-server.log`); it is *not* a child of the SSM agent or of the calling shell, so it survives the end of the Run Command. Otherwise (containers) `setsid nohup` + `server.pid`
  (`ulimit -n 1048576` best effort). The sampler is its own unit `oxia-bench-sampler.service` / process. `status` reports `"mode":"systemd"|"fallback"` and the real `nofile_limit` of the process.
* **`reset`** = `stop` (SIGTERM, SIGKILL after 30 s) + `rm -rf $OXIA_BENCH_DATA/{wal,db}` + `start`. Repeatable any number of times, also after the server was killed (`kill -9`) or crashed.
  The per-run logs rotate (`oxia-server.log.1`, `cpu.log.1`), so `cpu.log` always describes the current server only.
* **Readiness** (`--wait`, default 30 s): process alive **and** TCP connect to `127.0.0.1:<port>` **and** `oxia health --host 127.0.0.1 --port <port>` (the binary's own gRPC health probe) succeeds. No other
  dependency. `standalone` creates and elects every shard leader *before* it opens the public port and answers `GetShardAssignments` from the first request (`oxiad/dataserver/standalone.go`), so "port open and healthy"
  implies the shard assignments are served; the load tools additionally wait for them themselves. Typical `ready_ms`: 20-130 ms. On failure the last 30 log lines go to stderr and the server is stopped (exit 1).
* stdout of `start`/`reset`: one JSON line `{"running":true,"pid":..,"mode":..,"port":..,"shards":..,"sync":..,"notifications":..,...,"ready_ms":..}`; `status` prints the same (`{"running":false}` and exit 1 when down).
* **CPU sampler** (`cpu-sampler.sh`, started by `start`, ends with the server): once per second appends `epoch utime_ticks stime_ticks rss_kb total_jiffies idle_jiffies` to `cpu.log` (epoch with microseconds,
  ticks = `getconf CLK_TCK`, process values cover all threads, host values from `/proc/stat` with guest time not double counted and idle = idle+iowait). Pure bash builtins and `/proc` reads: no fork per sample except `sleep`.
* **`stats --since E [--until U]`** (epoch seconds; `--until` defaults to the last sample) prints one JSON line. The counters are interpolated linearly at the window edges, so the 1 s sampling period does not bias windows of 10 s or more:

  | key | meaning |
  |---|---|
  | `server_cpu_pct` | 100 x CPU-seconds / window seconds = percent of **one core** (100 = one core fully busy, maximum 100 x `cores`) |
  | `server_cpu_s` | user+system CPU seconds of the server in the window = core-seconds |
  | `rss_mb` / `rss_mb_end` | peak / last resident set size in MiB within the window |
  | `server_cpu_pct_of_host` | `server_cpu_pct / cores` (0..100) |
  | `host_cpu_pct` | whole-host busy percent from `/proc/stat` deltas (inside a container this is the *VM's* CPU, not the container's) |
  | `window_s`, `cores`, `samples`, `since`, `until`, `user_cpu_s`, `sys_cpu_s`, `window_clipped` | `window_clipped` is true when the requested window is not fully covered by samples (the figures then describe the covered part) |

### `run-load.sh`

```
run-load.sh --impl dotnet|go --addr HOST:PORT [--client async|sync] [--ops put,get,delete] [--read-percent N] [--keys N] [--value-size N] [--producers N]
            [--duration 20s] [--warmup 3s] [--linger 5ms] [--max-requests-per-batch N] [--request-timeout 30s] [--rate R] [--random-payload] [--preload true|false]
            [--min-threads N] [--tune a=b] [--out FILE] [--wait 15]
```

Translates the common flags into `oxia-load-dotnet load --server HOST:PORT --client ... --duration ...` or `oxia-load-go -addr HOST:PORT -client ... -duration ...` (bare numbers are seconds/milliseconds).
Always passes explicitly `--client`, `--ops`, `--read-percent`, `--keys`, `--value-size`, `--producers`, `--duration`, `--warmup` (default 3 s, never 0 unless asked), `--preload=true`, so a default can never differ between the two tools; `--linger`,
`--max-requests-per-batch`, `--request-timeout`, `--rate` are passed only when given (library defaults otherwise, which are identical: 5 ms / 1000 / 30 s). Refuses to run when `HOST:PORT` does not accept a TCP connection within `--wait`
seconds (works with container host names). `--out FILE` keeps `# header lines`, the tool's stdout and then its stderr.

**stdout**: the tool's own summary lines, then `WINDOW since=<epoch s> until=<epoch s>` (the measured window, exact: from the `window_start_ms`/`window_end_ms` keys both tools now put at the end of the RESULT line - the
.NET tool and the Go twin were extended for this; an older tool falls back to an estimate from the process end), then the tool's `RESULT k=v ...` line **last**. stderr: tool stderr (e.g. `preloaded N keys in X`) and this script's log.
**Exit code**: the tool's; 2 usage error; 3 the server does not answer; 4 the tool exited 0 without a RESULT line. Pass the `WINDOW` values to `oxia-server.sh stats --since/--until` on the server.
Environment: sets `HOME=/root` and `DOTNET_BUNDLE_EXTRACT_BASE_DIR=$OXIA_BENCH_STATE/dotnet-extract` if unset (a .NET single file unpacks its native libraries there; an SSM command has no `HOME`), raises the soft `nofile` limit
to the hard limit; other `DOTNET_*` / `GO*` variables are passed through untouched. The runtime settings of `Directory.Build.props` (workstation, non-concurrent GC, TieredPGO) are baked into the single file's `runtimeconfig` and
verified at publish time.

### `sysinfo.sh`
One JSON line on stdout: `schema, collected_utc, role, os{}, cpu{model,logical_cpus,sockets,cores_per_socket,threads_per_core,max_mhz,l3_cache,hypervisor,nproc,cgroup_cpus}, memory{}, kernel_tuning{}, limits{}, runtime{container,systemd},
ec2{instance_type,ami_id,availability_zone,region}|null (IMDSv2; no instance or account ids), data_volume{mount,device,fstype,options,size_gb}, software{oxia{version,sha256}, oxia_load_dotnet{sha256}, oxia_load_go{sha256}, artifacts: VERSIONS.json without files},
node: node.json`. Needs `jq`. Takes ~2 s off EC2 (IMDS timeout).

## 3. The local rehearsal: `Rehearse.ps1`

```powershell
pwsh bench/aws/scripts/Rehearse.ps1 [-Arch x64|arm64] [-Scenario smoke|sync|shards4] [-Repetitions 1] [-NoPin] [-SkipPublish] [-ArtifactsDir <dir>] [-OxiaImage ...] [-BaseImage amazonlinux:2023] [-Cleanup]
```

Free (Docker Desktop only; the first run downloads `amazonlinux:2023`, and every run each node downloads about 90 MB of dnf metadata to install `jq`/`procps-ng`/`util-linux`). It publishes the artifacts (cached), creates the user-defined network `oxia-bench-rehearsal` and the containers
`oxia-bench-rehearsal-server` / `-client` (`--init`, `--rm`, `sleep 3600`, the server on the first half of the Docker VM's CPUs and the client on the second half unless `-NoPin`), bind-mounts the artifacts read-only, runs
`install-artifacts.sh` and `bootstrap-node.sh` on both (concurrently), then for every run `oxia-server.sh reset` -> `run-load.sh --impl dotnet|go` -> `oxia-server.sh stats --since/--until` (order dotnet,go then go,dotnet ... for `-Repetitions`),
`sysinfo.sh` on both nodes (parsed as JSON), and prints the .NET vs Go table plus the raw RESULT lines and the step timings. Exit 0 only if every step succeeded and every run had throughput and no errors.

* Cleanup is in a `finally` (error, Ctrl+C). If the script is killed hard, the containers disappear on their own (`--rm`, `sleep 3600`) and `Rehearse.ps1 -Cleanup` removes the remaining network; the next run also starts by removing leftovers.
  Verified: after a normal run, after a failed run, after `Stop-Process -Force` + `-Cleanup`: `docker ps -a --filter name=oxia-bench`, `docker network ls`, `docker volume ls` show nothing of ours (the rehearsal uses no volumes).
* What it covers: the whole sequence and all node scripts on the real AL2023 userland and on the real artifacts, the fallback (no systemd) path of `oxia-server.sh`, server and client in separate network namespaces with name resolution.
  What it cannot cover: systemd (see below), the sysctl/THP/limits tuning (read-only in unprivileged containers: logged as skipped), instance metadata, real network latency, real disks. Timings are those of a laptop VM, not of c6i instances.
* **The systemd branch** was tested once by hand in a throwaway privileged container with systemd as PID 1 (`amazonlinux:2023` + `dnf install systemd`, `docker run --privileged --tmpfs /run`, then `install-artifacts.sh`, `bootstrap-node.sh server`,
  `oxia-server.sh reset` x3 / `status` / `stats` / `stop`): transient units `oxia-bench-server.service` and `oxia-bench-sampler.service` came up, `Max open files 1048576` was in effect, `stop` left no unit. It is not part of
  `Rehearse.ps1` because `--privileged` lets `bootstrap-node.sh` change sysctl/THP of the *shared Docker Desktop VM* (they were restored afterwards); do not do that on a machine you care about.
* `-Arch arm64` works under qemu (functional check of the arm64 artifacts; the timings mean nothing).

## 4. Notes for the orchestrator (SSM)

1. Per node, once: `aws s3 sync s3://<bucket>/artifacts/ /tmp/artifacts --only-show-errors && bash /tmp/artifacts/node/install-artifacts.sh /tmp/artifacts && /opt/oxia-bench/bin/bootstrap-node.sh server|client`
   (server: put the gp3 data volume on `/data` in user data, or set `OXIA_BENCH_DATA_DEVICE=auto`). Upload the **whole** artifact directory (the musl loader and `SHA256SUMS` are part of it).
2. A scenario run: server `oxia-server.sh reset --shards N --sync true|false` (stdout JSON) -> client `run-load.sh --impl ... --addr <server private ip>:6648 ... --out /tmp/run.out` (stdout: summary, `WINDOW`, `RESULT`; parse the last two lines)
   -> server `oxia-server.sh stats --since <WINDOW.since> --until <WINDOW.until>`. Upload `--out` files, `cpu.log`, `oxia-server.log` with `aws s3 cp` from the node.
3. SSM Run Command runs as root, with no `HOME` and a minimal `PATH` (handled), stdout/stderr are separate and truncated at 24 000 characters: the scripts keep stdout short (1-5 KiB); use `--out` / `aws s3 cp` for the raw logs.
   Background processes started by a Run Command are not guaranteed to survive it; the server and sampler are systemd units precisely for that. Never start a load tool in the background from a Run Command - run it in the foreground (it ends by itself after warm-up + duration + preload).
4. `oxia-server.sh stop` is not needed before `reset`, nor before terminating the instance. The server listens on `0.0.0.0:6648` (the plan's security group admits 6648/6649 from the client SG only); the metrics port 7648 is bound to 127.0.0.1.
5. Exit codes are reliable and `set -e` clean: any non-zero is a failed step. `run-load.sh` exits with the tool's code (e.g. 1 for a failed preload), 3 for an unreachable server.

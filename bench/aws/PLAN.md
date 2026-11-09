# AWS benchmark environment — plan

**Goal.** On demand, spawn a small, isolated AWS environment, measure the **throughput and latency of the .NET client driver and of the Go client driver
on Linux against the same Oxia server under identical conditions**, publish a report, and **tear everything down — always**.

Non-goals: benchmarking the Oxia *server*; a permanent environment; anything that needs inbound network access to the instances.

## 1. Architecture

```
 operator machine (PowerShell 7 + AWS CLI, profile "QA")
        │  CloudFormation (create/delete stack), S3 (artifacts, results), SSM Run Command (no SSH, no key pairs, no inbound ports)
        ▼
 ┌────────────────────────────── one dedicated VPC (10.77.0.0/24), one public subnet, one AZ, cluster placement group ───────────────┐
 │                                                                                                                                    │
 │   SERVER  (Amazon Linux 2023)                          CLIENT (Amazon Linux 2023)                                                  │
 │   oxia standalone  (static binary from the pinned      oxia-load-dotnet  (self-contained single-file, linux-x64|arm64)             │
 │   image, run under systemd, :6648)          ◄──────────  oxia-load-go      (static, same flags, same RESULT line)                  │
 │   WAL+DB on a gp3 volume, sampler of CPU                 runs ONE load process at a time, alternating .NET / Go                    │
 │                                                                                                                                    │
 │   SG: 6648/6649 only from the client SG, nothing else inbound        S3 gateway endpoint · IGW (outbound for SSM)                  │
 └────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
        per-run S3 bucket (artifacts in, results out)  ·  per-run IAM role/instance profile (SSM core + that bucket only)
```

Everything is one CloudFormation stack named `oxia-bench-<runId>`; deleting the stack deletes the environment.

## 2. Methodology (what makes the comparison fair)

* **Same box, same server, same parameters.** The .NET and Go load tools take the same flags and print the same `RESULT k=v …` line. They run **one at a time** on the
  same client instance, never concurrently.
* **Fresh server for every single run** (stop, wipe `/data`, start): no state, compaction or cache carried from the previous run or from the other implementation.
* **Alternating order** (`dotnet, go, go, dotnet, …`) across the repetitions of a scenario so that drift (noisy neighbours, thermal, page cache) does not favour one side.
* **Warm-up** (3 s, not measured), **N repetitions** per scenario (suite-defined, ≥ 3 in `standard`/`full`), report the **median run** plus min/max; the raw RESULT lines are kept.
* **Pinned, recorded versions**: client git commit, .NET runtime version, Go version, Oxia server binary sha256 + `oxia --version` + source image digest, AMI id, kernel,
  instance types, region/AZ — written into `environment.json` next to the results.
* **What is measured** (per run): ops/s, latency p50/p90/p99/p99.9/max (put/get separately), errors, client CPU µs/op, client allocated B/op and GC counts (.NET) / bytes (Go),
  server CPU% over the measured window (sampler on the server).
* **Server not the bottleneck by default**: server runs with `--wal-sync-data=false` ("fast" profile) on a provisioned gp3 volume with enough shards; a `durable`
  scenario (fsync on) is part of `full` so the cost of durability is visible but not mixed into driver comparisons.

## 3. Environment specification (defaults; all overridable)

| Item | Default | Notes |
|---|---|---|
| Region / AZ | `us-west-2`, first AZ that has the instance type | profile `QA` |
| Server | `c6i.2xlarge` (8 vCPU, 16 GiB) | `-Size large` → `c6i.4xlarge` |
| Client | `c6i.2xlarge` | `-Size large` → `c6i.4xlarge`; `-Arch arm64` → `c7g.*` (Graviton) |
| AMI | latest Amazon Linux 2023 (SSM public parameter, x86_64 or arm64) | resolved by CloudFormation at create time, recorded |
| Placement | cluster placement group (same AZ) | `-NoPlacementGroup` if capacity is short |
| Storage | 40 GiB gp3 root + a separate 100 GiB gp3 data volume (6000 IOPS, 500 MB/s; `AWS::EC2::Volume`, because an instance block-device mapping cannot set gp3 throughput) on the server; 30 GiB root on the client | all deleted with the stack |
| Network | dedicated VPC, public subnet, IGW, S3 gateway endpoint; **no inbound rule** except server ← client on 6648/6649 | SSM over the IGW |
| OS tuning | `nofile` 1 048 576, `somaxconn` 4096, `tcp_max_syn_backlog`, THP off, `vm.swappiness=1`, irqbalance default | applied by `bootstrap-*.sh`, recorded |
| Software on the server | Oxia static binary extracted from `oxia/oxia:<tag>` (linux/<arch>) | `-OxiaImage` to pin a digest |
| Software on the client | self-contained .NET load tool + static Go load tool | built on the operator machine from the working tree |

Estimated cost (us-west-2 on-demand list prices, approximate): 2 × c6i.2xlarge ≈ **$0.71/h** including storage. `bench.ps1 Plan` prints the estimate for the chosen suite before anything is created.
Measured on the QA account: stack create + bootstrap **≈ 4 min**, stack delete + verification **≈ 2.5 min**. Estimates: `smoke` ≈ 16 min ≈ **$0.20**, `standard` ≈ 50 min ≈ **$0.60**,
`full` ≈ 1.6 h ≈ **$1.15**; `-Size large` roughly doubles it. The hard stop (dead-man switch) bounds the worst case at `MaxLifetimeMinutes` (default 240 min ≈ $2.9).

## 4. Safety: the environment is ALWAYS torn down

Defence in depth — each layer works if the one above it failed:

| # | Layer | What it guarantees |
|---|---|---|
| 1 | `bench.ps1 Run/All` wraps the whole run in `try/finally` and calls **Down** in the `finally` (errors, Ctrl+C, test failures) | normal teardown |
| 2 | **Down**: empties the bucket, `delete-stack`, waits for `DELETE_COMPLETE`, retries once, then runs a **tag sweep** (`oxia-bench-run=<id>` via Resource Groups Tagging + direct EC2/S3/IAM describe) and **fails loudly (exit 1) if anything is left** | verified teardown |
| 3 | **Dead-man switch on the instances**: `InstanceInitiatedShutdownBehavior=terminate` + `shutdown -h +<MaxLifetimeMinutes>` (default 240) in user-data | instances disappear on their own even if the operator machine dies; cost is bounded |
| 4 | Every resource tagged `oxia-bench=true`, `oxia-bench-run`, `oxia-bench-owner`, `oxia-bench-expires` | everything is findable |
| 5 | **Reap** (`bench.ps1 Reap [-OlderThanMinutes N]`) deletes every `oxia-bench` stack past its `expires` tag (or older than N minutes), including ones from crashed runs | leak recovery; safe to run any time |
| 6 | **Status** lists every bench stack / instance / volume / bucket / role in the region; run it before and after | visibility |
| 7 | Up refuses to start if another live `oxia-bench` stack exists (unless `-AllowParallel`) and checks the vCPU quota | no accidental duplicates |

Failure modes considered: stack stuck in `DELETE_FAILED` (non-empty bucket, ENI in use) → Down empties/detaches and retries, then falls back to `--retain-resources` + explicit
deletes; operator laptop sleeps mid-run → layer 3; SSM agent offline → Down does not depend on SSM; partial `CREATE_FAILED` stack → Down handles `ROLLBACK_COMPLETE`.

## 5. Security

No SSH, no key pairs, no inbound rules from the internet. Access = SSM Run Command (IAM-authenticated, logged in CloudTrail). The instance role is limited to
`AmazonSSMManagedInstanceCore` + read/write on **its own run bucket**. The bucket blocks public access, enforces TLS, and is deleted with the stack. No secrets are stored or
needed. Account ids and ARNs are never written into the repository files; results contain only instance types, versions and metrics.

## 6. Workflow (operator commands)

```powershell
pwsh bench/aws/scripts/bench.ps1 Plan   -Suite standard                  # prints what would be created, estimate, hard stop time. Creates nothing.
pwsh bench/aws/scripts/bench.ps1 All    -Suite smoke                     # Up → Run → Report → Down   (Down is guaranteed)
pwsh bench/aws/scripts/bench.ps1 All    -Suite standard -Size large -Arch arm64
# or step by step:
pwsh bench/aws/scripts/bench.ps1 Up     -Suite full                      # build artifacts, create stack, bootstrap nodes
pwsh bench/aws/scripts/bench.ps1 Run    -Suite full -Scenario A,C        # run (a subset of) a suite on the live environment
pwsh bench/aws/scripts/bench.ps1 Report                                  # results → bench/aws/results/<runId>/report.md (+ docs copy with -Publish)
pwsh bench/aws/scripts/bench.ps1 Down                                    # teardown + verification
pwsh bench/aws/scripts/bench.ps1 Status | Reap
pwsh bench/aws/scripts/bench.ps1 Rehearse                                # the same node scripts in local Linux containers: free, no AWS
```

Suites live in `bench/aws/suites/*.json` (`smoke`, `standard`, `full`); a scenario is a JSON object (client, ops, read %, producers, linger, value size, keys, shards, durations, repetitions,
server fsync). New scenarios need no code change.

## 7. Components and ownership

| Path | Purpose |
|---|---|
| `bench/aws/cfn/bench-stack.yaml` | the whole environment (VPC … instances, bucket, role, SG, dead-man switch) |
| `bench/aws/scripts/bench.ps1` + `lib/*.ps1` | operator entry point: Plan/Up/Run/Report/Down/Status/Reap/Rehearse |
| `bench/aws/scripts/Publish-Artifacts.ps1` | builds the Linux artifacts on the operator machine (`-Arch x64|arm64`) |
| `bench/aws/node/*.sh` | scripts that run **on the instances** (and in the local rehearsal containers) |
| `bench/aws/suites/*.json` | scenario definitions |
| `bench/aws/results/` (git-ignored) | per-run output: `runs.csv`, raw logs, `environment.json`, `report.md` |

### Node-side contract (implemented once, used by AWS and by the rehearsal)

Installed to `/opt/oxia-bench/bin` (client and server both get all scripts); every script exits non-zero on failure and prints machine-readable output on stdout, human logs on stderr.

* `bootstrap-node.sh <server|client>` — idempotent OS tuning (sysctl, limits, THP, tools); writes `/var/lib/oxia-bench/node.json` (kernel, cpu model, cores, memory, tuning applied).
* `oxia-server.sh start|stop|reset|status|stats` — `start --shards N --sync true|false [--notifications true|false] [--port 6648]` runs the pinned `oxia standalone` under systemd (transient unit,
  `LimitNOFILE=1048576`) with data in `/data/{wal,db}`; `reset` = stop + wipe + start + wait until the port answers; `stats --since <epoch>` prints `{"server_cpu_pct":…, "server_cpu_s":…, "rss_mb":…}`
  from the CPU sampler it runs while started. Without systemd (containers) it falls back to `setsid nohup` + pid file.
* `run-load.sh --impl dotnet|go --addr host:port [--client async|sync] [--ops put,get,delete] [--read-percent N] [--keys N] [--value-size N] [--producers N] [--duration Ns] [--warmup Ns]
  [--linger Nms] [--max-requests-per-batch N] [--rate R] [--random-payload] [--out FILE]` — runs the chosen load tool with the common flag set (translated per tool), tees its output to `--out`,
  prints the tool's `RESULT …` line **last** on stdout; exit code = tool exit code.
* `sysinfo.sh` — prints one JSON document with the host facts (instance metadata when present, kernel, lscpu, versions of dotnet runtime/go/oxia tools).

Artifacts layout produced by `Publish-Artifacts.ps1 -Arch <a> -OutDir <dir>` (and uploaded to `s3://<bucket>/artifacts/`):
`oxia` (server), `oxia-load-dotnet`, `oxia-load-go`, `node/*.sh`, `VERSIONS.json` (client commit, dotnet/go versions, oxia version + image digest + sha256 of every binary).

## 8. Phases

| Phase | Scope | Status |
|---|---|---|
| P1 | standalone server, one client box, .NET vs Go, suites smoke/standard/full, x64, Report, guaranteed teardown, Reap/Status, local rehearsal | **this plan** |
| P2 | arm64 (Graviton) matrix, `-Size large`, results comparison across runs (`Compare` command) | scripts are arch-parametrised from P1 |
| P3 | 3-node replicated cluster (coordinator + 3 data servers, one AZ and multi-AZ variants), TLS on, leader-change under load | designed, not built |
| P4 | scheduled run (GitHub Actions OIDC → role) publishing a trend page | not built |

## 9. Validation status

The orchestration was exercised against the real QA account with **stub artifacts** (two `t3.small` instances, 60-minute dead-man switch): stack creation (VPC, S3 endpoint, security groups, bucket, role,
instances, separate data volume), artifact upload and install over SSM, per-run server reset / load / stats round trips, report generation, and the guaranteed teardown, which ended in
`TEARDOWN VERIFIED` and an independent `Status` showing no stacks, instances, volumes, VPCs, buckets or roles. The same self-test caught (and the plan now documents) the `Throughput`
restriction of instance block-device mappings and made the failure path print CloudFormation `DescribeEvents` validation details even though `--on-failure DELETE` removes the failed stack.
Real-binary runs: a one-scenario smoke run, then the full **`standard` suite** (run `20261109-092619-fb44`: 7 scenarios x 2 clients x 3 repetitions = 42 load runs on two `c6i.2xlarge`,
about 66 minutes of wall clock including a 4-minute stack creation and a 3-minute delete, no failed run), again ending in `TEARDOWN VERIFIED`. Its report is published as
[`docs/benchmark-aws.md`](../../docs/benchmark-aws.md). One operational finding: when the operator machine goes to sleep the orchestrator pauses (here for 32 minutes between two repetitions) while the
instances keep running and cost money; the load runs themselves are not affected. Keep the machine awake for the hour, or use the dead-man switch as the backstop it is.

## 10. Risks and open questions

* **IAM permissions of the QA profile** (role/instance-profile creation) are only provable by trying; fallback: pass an existing instance profile with `-InstanceProfile`.
* **Placement-group capacity**: `-NoPlacementGroup` fallback.
* **Noisy neighbours / burst credits**: c6i is not burstable; results still carry run-to-run variance → ≥ 3 repetitions and the median, min/max reported.
* **Docker Hub rate limits** are avoided: the Oxia binary is extracted on the operator machine and shipped through S3.
* **Clock/timezone**: all timestamps UTC; instance clocks are Amazon Time Sync (not used for measurement; durations are monotonic).

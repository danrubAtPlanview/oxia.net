# On-demand AWS benchmark: .NET client vs Go client on Linux

Spins up a small, isolated AWS environment, runs the **.NET and Go client drivers against the same Oxia server on Linux under identical conditions**, writes a report, and
**tears everything down - always**. Design, methodology and safety model: [`PLAN.md`](PLAN.md). Results of published runs: [`docs/benchmark-aws.md`](../../docs/benchmark-aws.md).

```powershell
pwsh bench/aws/scripts/bench.ps1 Plan -Suite standard           # what would be created, estimated time/cost, hard stop. Creates nothing.
pwsh bench/aws/scripts/bench.ps1 All  -Suite smoke    -Force    # Up -> Run -> Report -> Down (Down is guaranteed)   ~16 min,  ~$0.20
pwsh bench/aws/scripts/bench.ps1 All  -Suite standard -Force    # the comparison scenarios, 3 repetitions            ~50 min,  ~$0.60
pwsh bench/aws/scripts/bench.ps1 All  -Suite full     -Force    # + producer sweep, 16 shards, value sizes, fsync   ~1.6 h,   ~$1.15
pwsh bench/aws/scripts/bench.ps1 All  -Suite standard -Size large -Arch arm64 -Force      # 4xlarge Graviton
```

Without `-Force` the script shows the plan and asks you to type `YES`. Results land in `bench/aws/results/<runId>/` (`report.md`, `runs.csv`, `runs.jsonl`, raw logs,
`environment.json`); `Report -Publish` copies the report to `docs/benchmark-aws.md`.

## Prerequisites

* PowerShell 7 (`pwsh`), AWS CLI v2, and an AWS CLI **profile** (default `QA`, override with `-AwsProfile` or `OXIA_BENCH_AWS_PROFILE`; region `us-west-2`, override with `-Region` / `OXIA_BENCH_REGION`).
* To build the Linux artifacts on your machine: .NET SDK 10, Go (the `go` directive of the pinned upstream may need the auto-downloaded toolchain), Docker (extracts the Oxia server binary for the
  target architecture from the `oxia/oxia` image and runs the local rehearsal).
* IAM permissions of the profile (the environment is created and deleted through CloudFormation): `cloudformation:*` on `oxia-bench-*` stacks; `ec2:*` for VPC, subnet, route table, internet gateway,
  VPC endpoint, security group, placement group, instances, volumes (plus `Describe*`); `iam:CreateRole`, `DeleteRole`, `PutRolePolicy`, `DeleteRolePolicy`, `AttachRolePolicy`, `DetachRolePolicy`,
  `CreateInstanceProfile`, `DeleteInstanceProfile`, `AddRoleToInstanceProfile`, `RemoveRoleFromInstanceProfile`, `PassRole`; `s3:*` on `oxia-bench-*` buckets; `ssm:SendCommand`,
  `GetCommandInvocation`, `DescribeInstanceInformation`, `GetParameters` (public AMI parameters); `tag:GetResources`; `service-quotas:GetServiceQuota` (optional).
* Nothing is installed on your machine and nothing outside the stack is touched. There are **no key pairs and no inbound rules**; the instances are managed through SSM.

## Commands

| Command | What it does |
|---|---|
| `Plan` | Prints account/principal, suite, scenarios, instance types, AZ, vCPUs, estimated duration and cost, the hard stop time, and any existing oxia-bench stacks. Creates nothing. |
| `Up` | Builds the Linux artifacts, creates the stack, uploads artifacts to the run bucket, installs and tunes both nodes. If it fails it tears itself down (`-KeepOnFailure` to debug). |
| `Run` | Runs the suite (or `-Scenario A,C`) on the live environment: for every run, reset the server -> run one load tool -> read server stats. Alternating .NET/Go order. |
| `Report` | `runs.csv` + `report.md`; `-Publish` also writes `docs/benchmark-aws.md`. |
| `Down` | Deletes the stack and **verifies** nothing tagged `oxia-bench-run=<id>` remains (stack, instances, volumes, VPC, bucket, role). Exit code 1 if anything is left. |
| `All` | `Up` -> `Run` -> `Report` -> `Down`, with `Down` in a `finally` (errors, Ctrl+C). `-KeepAfterRun` skips the final teardown (the dead-man switch still applies). |
| `Status` | Lists every oxia-bench stack, instance, volume, VPC, bucket and role in the region. Run before and after; it is the leak detector. |
| `Reap` | Deletes oxia-bench stacks past their expiry (`-OlderThanMinutes N`, or `-Everything`), including the leftovers of a crashed run. |
| `Rehearse` | Runs the same node scripts in local Linux containers: free, no AWS. |
| `Suites` | Lists the suites. |

Useful parameters: `-Suite`, `-Scenario`, `-Size small|standard|large`, `-Arch x64|arm64`, `-ServerInstanceType`, `-ClientInstanceType`, `-NoPlacementGroup` (if capacity is short),
`-MaxLifetimeMinutes` (dead-man switch, default 240), `-OxiaImage` (pin the server image, e.g. by digest), `-SkipArtifacts -ArtifactsDir <dir>`, `-AllowParallel`.

## Why it cannot leak

1. `All` and `Up` run teardown in a `finally`. 2. `Down` deletes the stack, then **verifies by tag and by direct describe calls**, and falls back to deleting leftovers by tag in dependency order
   (volumes after detach, VPC endpoints before the VPC, the bucket after emptying it...). 3. The instances shut themselves down (`InstanceInitiatedShutdownBehavior=terminate`) after
   `MaxLifetimeMinutes` even if your machine dies. 4. Everything carries `oxia-bench*` tags including an expiry, so `Reap` can find stragglers. 5. `Up` refuses to start while another
   oxia-bench stack exists.

If a run was interrupted: `pwsh bench/aws/scripts/bench.ps1 Status`, then `Down` (uses the saved state) or `Reap -Everything`.

## Suites

`suites/*.json`: a `defaults` object plus `scenarios` that override it (`client`, `ops`, `readPercent`, `keyCount`, `valueSize`, `producers`, `linger`, `maxRequestsPerBatch`, `rate` for open loop,
`shards`, `serverSync`, `notifications`, `durationSec`, `warmupSec`, `runs`). Add a scenario by adding an object; no code changes. Every scenario runs both implementations, `runs` times each.

## Validating changes to this tooling (almost free)

```powershell
pwsh bench/aws/scripts/SelfTest.ps1                  # tiny t3.small instances + stub load tools; ~7 min, a few cents
pwsh bench/aws/scripts/SelfTest.ps1 -ExerciseSweep   # additionally deletes everything behind CloudFormation's back first, proving the fallback teardown
```

## Files

```
PLAN.md                    design, methodology, safety model, phases
cfn/bench-stack.yaml       the whole environment (one CloudFormation stack per run)
scripts/bench.ps1          entry point;  scripts/SelfTest.ps1  tooling self-test
scripts/lib/*.ps1          Common (aws wrapper, state, sizing, costs), Stack (create/delete/verify/sweep), Ssm (run commands), Suite, Report
scripts/Publish-Artifacts.ps1, scripts/Rehearse.ps1, node/*.sh    Linux artifacts, local rehearsal and the scripts that run on the instances (see README-node.md)
suites/*.json              scenario definitions
```

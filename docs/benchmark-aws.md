# Linux benchmark on AWS: .NET client vs Go client

Run `20261109-092619-fb44` (suite `standard`), generated 2026-11-09T10:33:33Z by `bench/aws/scripts/bench.ps1 Report`.

Both client drivers were run on the **same Linux client instance, one at a time, against the same Oxia server**, with identical parameters; the server was reset
(stopped, data wiped, started) before **every** run and the order of the two implementations alternates between repetitions. The table shows, per scenario and
implementation, the **median repetition by throughput** (other columns are from that same repetition) with the min-max throughput over all repetitions.
Method and safety design: [bench/aws/PLAN.md](../bench/aws/PLAN.md).

## Summary of this run

*(Written by hand after the run; `bench.ps1 Report -Publish` replaces the whole file, so a new publication needs a new summary. The tables below are generated.)*

* **Throughput:** the .NET client is ahead of or level with the Go client in all seven scenarios: +10 % (A, 256 producers, linger 5 ms), **+56 % (B, 1000 producers, put only)**,
  +6 % (C, sync client), +2 % (D, 4 shards: the server is the limit, see below), **+21 % (E, 1 KiB values)**, +1 % (F, no batching), level (G, open loop at 20 k ops/s: both reach the offered rate).
  The large gaps are the batching scenarios with many producers (B, E): there the .NET client needs 5.2 us of client CPU and 4 B allocated per operation against 14.2 us and 1,792 B for the Go client (B).
* **The server is the limit in D:** both clients reach about 400 k ops/s while the Oxia server uses about 3.4 of its 8 cores, so the two converge. D measures the server, not the drivers.
* **Latency:** median latency is equal or better for .NET (B: 5.96 ms against 9.83 ms; E: 4.52 against 5.73). p99 is equal or better for .NET (B: 16.5 ms against 20.6 ms) except in F (14.8 against 13.0 ms, 1.13x). The **far tails are not better**: in the median repetition of D the
  p99.9 is 22.9 ms against 8.8 ms (2.6x) and in A the maximum is 37 ms against 11 ms (3.3x). Single-repetition maxima are noisy, but this is worth watching and has not been investigated.
* **Client CPU per operation is higher for .NET when every operation is its own RPC or the rate is low:** C 30.5 us against 17.1 us (1.8x), F 247 us against 88 us (2.8x), G 109 us against 68 us (1.6x).
  **Investigated afterwards** (local Linux reproduction with the same binaries, [`docs/performance.md`](performance.md) section 11): the excess is mostly the .NET thread pool, not client work. Idle workers
  spin before they sleep (about 28 % of the CPU samples in F are one native spin instruction), and an unbatched operation costs about 8.7 thread pool hops, each a worker wake-up. With
  `DOTNET_ThreadPool_UnfairSemaphoreSpinLimit=0` the CPU per operation fell by 12-33 % in every scenario (locally: C 22 -> 14 us against Go 16, F 301 -> 210 against 158, G 100 -> 79 against 65), at the price
  of 5-9 % closed-loop throughput. It is a trade-off for the application to make (the setting is read at runtime start), not a default; it was **not** applied in the run above. A follow-up run on the same instance types measured it on AWS: see
  *Follow-up run* at the end of this file.
* **Allocation** is lower for .NET in every scenario: 4-173 B per operation in the batching scenarios (A, B, D, E, G), 773 B for the sync client (C) and 4.9 KB without batching (F), against 1.7-6.0 KB for
  Go. The two numbers come from different counters (see the note under *Results*), so read them as an order of magnitude.
* **Limits of the run:** one `oxia standalone` server (no cluster, no TLS, no authentication), one instance type (c6i.2xlarge), one region and availability zone, 3 repetitions per scenario, shared cloud
  hardware (read differences below 5-10 % as noise; the min-max column shows the spread), 128-byte values except 1 KiB in E. The orchestration machine stalled for 32 minutes between two repetitions of G; the load
  generators run on the AWS instances and the server is reset before every run, so the measurements are not affected.

## Environment

| | Server | Client |
|---|---|---|
| instance type | c6i.2xlarge | c6i.2xlarge |
| OS | Amazon Linux 2023.12.20261102 | Amazon Linux 2023.12.20261102 |
| kernel | 6.18.56-125.164.amzn2023.x86_64 | 6.18.56-125.164.amzn2023.x86_64 |
| CPU | Intel(R) Xeon(R) Platinum 8375C CPU @ 2.90GHz (8 vCPU) | Intel(R) Xeon(R) Platinum 8375C CPU @ 2.90GHz (8 vCPU) |
| memory | 15704 MiB | 15704 MiB |
| transparent huge pages / swappiness | never / 1 | never / 1 |
| data volume | /dev/nvme1n1 xfs 100 GB | n/a |

| Component | Version |
|---|---|
| .NET client (this repository) | commit `56649449d39f`, working tree dirty: False |
| .NET runtime / settings | 10.0.11, self-contained single file; workstation GC, concurrent GC off, TieredPGO True |
| Go client | go1.27.0 (CGO off), built from github.com/oxia-db/oxia at `8456f61e1ada` |
| Oxia server | oxia version 8456f61; go: go1.27.3; image `oxia/oxia@sha256:176083a5f0c347dd1e8aff5981306051475a54ec2f33540db094d225f93baf64` |
| Region / AZ / AMI | us-west-2 / us-west-2a / ami-09f02015912e773f7; cluster placement group: True |

## Results

Throughput in ops/s (higher is better); latencies in ms (lower is better); CPU is client CPU per operation; allocation is client-side bytes per operation
(.NET: GC allocation counter; Go: runtime MemStats) - the two are measured by each runtime and are indicative, not byte-identical definitions.

### A - async, 256 producers, put+get 50/50, linger 5 ms, 1 shard

| Impl | ops/s (median) | min-max ops/s | p50 | p99 | p99.9 | max | client CPU us/op | alloc B/op | errors | server CPU (% of 1 core) | runs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **.NET** | 42,449 | 42,177-42,460 | 6.19 | 8.45 | 11.27 | 37.41 | 15.21 | 115 | 0 | 33 | 3 |
| Go | 38,683 | 38,320-38,936 | 6.62 | 8.52 | 10.62 | 11.41 | 12.12 | 1,626 | 0 | 33 | 3 |
| *.NET / Go* | *1.10* | | *0.94* | *0.99* | *1.06* | *3.28* | *1.25* | *0.07* | | |

### B - async, 1000 producers, put only, linger 5 ms, 1 shard

| Impl | ops/s (median) | min-max ops/s | p50 | p99 | p99.9 | max | client CPU us/op | alloc B/op | errors | server CPU (% of 1 core) | runs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **.NET** | 144,765 | 144,506-144,920 | 5.96 | 16.52 | 41.68 | 76.69 | 5.15 | 4 | 0 | 128 | 3 |
| Go | 92,829 | 92,559-92,986 | 9.83 | 20.58 | 41.68 | 92.90 | 14.18 | 1,792 | 0 | 80 | 3 |
| *.NET / Go* | *1.56* | | *0.61* | *0.80* | *1.00* | *0.83* | *0.36* | *0.00* | | |

### C - sync client, 64 callers, put+get 50/50

| Impl | ops/s (median) | min-max ops/s | p50 | p99 | p99.9 | max | client CPU us/op | alloc B/op | errors | server CPU (% of 1 core) | runs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **.NET** | 12,010 | 11,818-12,194 | 5.60 | 6.42 | 7.04 | 8.67 | 30.47 | 773 | 0 | 13 | 3 |
| Go | 11,281 | 11,166-11,672 | 5.70 | 6.49 | 6.88 | 9.19 | 17.14 | 1,772 | 0 | 13 | 3 |
| *.NET / Go* | *1.06* | | *0.98* | *0.99* | *1.02* | *0.94* | *1.78* | *0.44* | | |

### D - async, 1000 producers, put+get, linger 1 ms, 4 shards

| Impl | ops/s (median) | min-max ops/s | p50 | p99 | p99.9 | max | client CPU us/op | alloc B/op | errors | server CPU (% of 1 core) | runs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **.NET** | 401,056 | 401,019-401,796 | 2.39 | 5.44 | 22.94 | 85.81 | 7.73 | 121 | 0 | 338 | 3 |
| Go | 393,484 | 392,198-393,527 | 2.42 | 5.44 | 8.78 | 78.77 | 8.81 | 1,661 | 0 | 341 | 3 |
| *.NET / Go* | *1.02* | | *0.99* | *1.00* | *2.61* | *1.09* | *0.88* | *0.07* | | |

### E - async, 256 producers, put only, 1 KiB values

| Impl | ops/s (median) | min-max ops/s | p50 | p99 | p99.9 | max | client CPU us/op | alloc B/op | errors | server CPU (% of 1 core) | runs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **.NET** | 50,425 | 49,558-50,488 | 4.52 | 10.81 | 54.53 | 68.71 | 17.16 | 33 | 0 | 172 | 3 |
| Go | 41,773 | 41,210-42,025 | 5.73 | 11.80 | 58.98 | 66.14 | 16.39 | 1,860 | 0 | 151 | 3 |
| *.NET / Go* | *1.21* | | *0.79* | *0.92* | *0.92* | *1.04* | *1.05* | *0.02* | | |

### F - async, 64 producers, put+get, linger 0 (no batching)

| Impl | ops/s (median) | min-max ops/s | p50 | p99 | p99.9 | max | client CPU us/op | alloc B/op | errors | server CPU (% of 1 core) | runs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **.NET** | 10,913 | 10,656-10,989 | 5.73 | 14.75 | 16.25 | 27.48 | 247.12 | 4,921 | 0 | 100 | 3 |
| Go | 10,784 | 10,755-10,849 | 5.34 | 13.04 | 14.09 | 15.07 | 88.25 | 6,010 | 0 | 106 | 3 |
| *.NET / Go* | *1.01* | | *1.07* | *1.13* | *1.15* | *1.82* | *2.80* | *0.82* | | |

### G - async, open loop 20k ops/s, put+get (latency under fixed load)

| Impl | ops/s (median) | min-max ops/s | p50 | p99 | p99.9 | max | client CPU us/op | alloc B/op | errors | server CPU (% of 1 core) | runs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **.NET** | 20,000 | 19,999-20,001 | 3.31 | 6.26 | 8.78 | 13.86 | 108.63 | 171 | 0 | 19 | 3 |
| Go | 20,002 | 20,001-20,003 | 3.42 | 6.46 | 8.45 | 11.03 | 68.29 | 1,812 | 0 | 20 | 3 |
| *.NET / Go* | *1.00* | | *0.97* | *0.97* | *1.04* | *1.26* | *1.59* | *0.09* | | |

## How to read this

* A ratio below 1.00 is better for latency/CPU/allocation, above 1.00 is better for throughput. Differences under ~5-10 % are within run-to-run noise on shared cloud hardware; look at min-max.
* When the **server** is the bottleneck (see *server CPU %*, and a throughput that does not move with producers), both clients converge: that measures the server, not the drivers.
* Closed-loop scenarios keep a fixed number of operations in flight; the open-loop scenario fixes the offered rate and measures latency at that load.
* Raw data: `runs.csv` and `runs.jsonl` next to this report (full per-run RESULT lines and server statistics).

## Follow-up run: the .NET thread pool spin-wait switched off

Run `20261109-115842-40ec` (suite `cpu`, commit `e5d54a8`, otherwise the environment above: two `c6i.2xlarge`, one placement group, median of three alternating repetitions, server reset before every run).
It repeats the three scenarios in which the .NET client used more CPU than the Go client, and adds the .NET client with `DOTNET_ThreadPool_UnfairSemaphoreSpinLimit=0` (the spin-wait of idle thread pool workers
off; [`performance.md`](performance.md) section 11). Same Go client, same server, same binaries as above; only that environment variable differs between the two .NET rows.

| Scenario | Client | ops/s | p50 ms | p99 ms | p99.9 ms | client CPU us/op |
|---|---|---:|---:|---:|---:|---:|
| C: sync client, 64 callers | .NET | 11,788 | 5.50 | 6.29 | 7.90 | 24.6 |
| | **.NET, spin limit 0** | 12,121 | 5.50 | 6.42 | 7.11 | **11.9** |
| | Go | 11,660 | 5.64 | 6.39 | 7.08 | 12.7 |
| F: 64 producers, linger 0 | .NET | 11,143 | 5.80 | 14.68 | 16.58 | 237.8 |
| | **.NET, spin limit 0** | 10,082 | 4.49 | 15.60 | 18.61 | **104.5** |
| | Go | 11,311 | 4.92 | 12.78 | 13.70 | 76.2 |
| G: open loop 20 k ops/s | .NET | 19,999 | 3.33 | 6.29 | 8.65 | 109.7 |
| | **.NET, spin limit 0** | 19,997 | 3.42 | 6.46 | 9.50 | **67.5** |
| | Go | 20,001 | 3.31 | 6.13 | 6.98 | 65.8 |

* **CPU per operation fell by 52 % (C), 56 % (F) and 38 % (G).** Against Go that is C 1.93x -> **0.93x**, G 1.67x -> **1.03x** (level), F 3.12x -> **1.37x**. The investigation's explanation holds on AWS: most of the excess was
  idle workers spinning, the effect is larger here than in the local Docker reproduction (12-33 %); why was not investigated.
* **Throughput:** unchanged in C (+3 %, inside the min-max spread) and in the open loop G (the rate is fixed); **-9.5 % in F** (11,143 -> 10,082 ops/s), where 64 callers each wait for a round trip and a worker that has
  gone to sleep needs longer to wake up. The median latency of F is lower (5.80 -> 4.49 ms), the tails are slightly higher (p99.9 16.6 -> 18.6 ms, G 8.65 -> 9.50 ms), so the cost sits in the slow operations (the
  read calls), not in the median.
* **What remains:** the unbatched F is still 1.37x Go's CPU per operation (about 9 thread pool hops per operation against Go's goroutine hand-offs). Everything else is level with or below the Go client.
* The first run above and this one differ for the unchanged .NET rows (C 30.5 -> 24.6 us, F 247 -> 238 us): spin-waiting makes the CPU figure timing dependent, which is another reason to read differences below 10-20 % of
  the CPU per operation as noise at these rates. Cost of the follow-up run: about $0.40, torn down and verified like the first.

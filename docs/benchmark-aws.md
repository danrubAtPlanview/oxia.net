# Linux benchmark on AWS: .NET client vs Go client

**Status: no published run yet.** This page is replaced by the generated report of the first full run
(`pwsh bench/aws/scripts/bench.ps1 All -Suite standard -Force`, then `Report -Publish`).

What the run measures and how it is made fair (same Linux client instance, one implementation at a time, the same Oxia server reset before every run,
alternating order, median of repetitions) is described in [`bench/aws/PLAN.md`](../bench/aws/PLAN.md); how to run it yourself, at a cost of
well under a dollar for the `standard` suite, is in [`bench/aws/README.md`](../bench/aws/README.md).

In the meantime, the local (Windows + Docker) results are in [`benchmark-results.md`](benchmark-results.md) and [`performance.md`](performance.md).

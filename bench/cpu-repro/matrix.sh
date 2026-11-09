#!/bin/bash
# matrix: scenario x variant (Go, .NET default, .NET with UnfairSemaphoreSpinLimit=0), one line per run.   env: SCENARIOS="A B C D F G"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
line() { grep -o "ops_per_s=[0-9]*\|p50_us=[0-9]*\|p99_us=[0-9]*\|cpu_us_per_op=[0-9.]*\|workitems_per_op=[0-9.]*" | awk 'BEGIN{ORS=" "} !seen[$0]++ {print}' ; echo; }
for sc in ${SCENARIOS:-A B C D F G}; do
  for v in "go|" "dotnet|X=1" "dotnet|DOTNET_ThreadPool_UnfairSemaphoreSpinLimit=0"; do
    impl=${v%%|*}; e=${v#*|}
    printf "%s %-7s %-55s " "$sc" "$impl" "$e"
    bash "$HERE/cpu.sh" run $impl $sc -- ${e:+$e} | line
  done
done

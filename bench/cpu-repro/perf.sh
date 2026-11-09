#!/bin/bash
# Flat CPU profile (perf, cpu-clock sampling) of the load tool during its measured window, inside the client container.
# usage: perf.sh <dotnet|go> <A|C|F|G> [--tune k=v] [-- ENV=VAL ...]      env: TOP (lines, default 60), CALLGRAPH=1
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLI=oxia-cpu-client
impl=$1; sc=$2; shift 2
tune=""
if [ "${1:-}" = "--tune" ]; then tune=$2; shift 2; fi
[ "${1:-}" = "--" ] && shift
extra=()
if [ "$impl" = dotnet ]; then extra=(DOTNET_PerfMapEnabled=1 DOTNET_ReadyToRun=0 DOTNET_TieredPGO=0 DOTNET_EnableWriteXorExecute=0); fi
( DURATION=${DURATION:-40s} bash "$HERE/cpu.sh" run $impl $sc ${tune:+--tune $tune} -- "${extra[@]}" "$@" > /tmp/cpu-perf-result.txt 2>&1 ) &
bg=$!
pid=""
for i in $(seq 1 120); do
  pid=$(docker exec -i $CLI bash -s < "$HERE/findpid.sh" 2>/dev/null || true)
  [ -n "$pid" ] && break
  sleep 1
done
[ -n "$pid" ] || { echo "load process not found"; exit 1; }
sleep "${SETTLE:-22}"
g=""; [ "${CALLGRAPH:-}" = 1 ] && g="-g"
docker exec $CLI bash -c "perf record -F 999 -e cpu-clock $g -p $pid -o /tmp/perf.data -- sleep 8 >/dev/null 2>&1; perf report -i /tmp/perf.data --no-children --sort dso,symbol --stdio 2>/dev/null | grep -v '^#' | grep -v '^$' | head -${TOP:-60}"
wait $bg || true
grep -E "^RESULT" /tmp/cpu-perf-result.txt | grep -o "ops_per_s=[0-9]*\|cpu_us_per_op=[0-9.]*" | tr '\n' ' '; echo

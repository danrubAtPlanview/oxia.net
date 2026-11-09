#!/bin/bash
# Per-thread CPU of the load tool during the measured window of one run (inside the client container).
# usage: threads.sh <dotnet|go> <A|C|F|G> [--tune k=v] [-- ENV=VAL ...]
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLI=oxia-cpu-client
SNAP='for d in /proc/PIDX/task/*; do t=${d##*/}; printf "%s %s " "$t" "$(tr " " "_" < $d/comm)"; sed "s/.*) //" $d/stat | awk "{print \$12+\$13}"; done'
impl=$1; sc=$2; shift 2
tune=""
if [ "${1:-}" = "--tune" ]; then tune=$2; shift 2; fi
[ "${1:-}" = "--" ] && shift
envs=()
for e in "$@"; do envs+=(-e "$e"); done

# start (this also recreates the server) in the background: run cpu.sh in a subshell, find the pid afterwards
( bash "$HERE/cpu.sh" run $impl $sc ${tune:+--tune $tune} -- "$@" > /tmp/cpu-threads-result.txt 2>&1 ) &
bg=$!
# wait for the load process to exist and for the warm-up to be over (3 s warm-up + preload)
pid=""
for i in $(seq 1 120); do
  pid=$(docker exec -i $CLI bash -s < "$HERE/findpid.sh" 2>/dev/null || true)
  [ -n "$pid" ] && break
  sleep 1
done
[ -n "$pid" ] || { echo "load process not found"; exit 1; }
sleep "${SETTLE:-9}"
snap1=$(docker exec $CLI bash -c "${SNAP//PIDX/$pid}")
sleep 5
snap2=$(docker exec $CLI bash -c "${SNAP//PIDX/$pid}")
wait $bg || true
cat /tmp/cpu-threads-result.txt | grep -E "^RESULT" | grep -o "ops_per_s=[0-9]*\|cpu_us_per_op=[0-9.]*\|workitems_per_op=[0-9.]*\|threads=[0-9]*" | tr '\n' ' '; echo
# delta per thread (jiffies = 1/100 s), aggregated by thread name
join -j1 <(echo "$snap1" | awk '{print $1, $2, $3}' | sort -k1,1) <(echo "$snap2" | awk '{print $1, $2, $3}' | sort -k1,1) \
  | awk '{d=$5-$3; name=$2; if (d>0) {cpu[name]+=d; cnt[name]++}; tot+=d} END {for (n in cpu) printf "%6.2f cores  %3d threads  %s\n", cpu[n]/100/5, cnt[n], n; printf "%6.2f cores  TOTAL\n", tot/100/5}' \
  | sort -rn | head -12

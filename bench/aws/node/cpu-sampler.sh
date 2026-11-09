#!/usr/bin/env bash
# cpu-sampler.sh --pid PID [--log FILE] [--interval SECONDS]
#
# Appends one line per interval (default 1 s) to FILE (default /var/log/oxia-bench/cpu.log) while PID is alive, then exits:
#
#     epoch utime_ticks stime_ticks rss_kb total_jiffies idle_jiffies
#
#   epoch          seconds since the epoch with microseconds (UTC clock)
#   utime_ticks    user CPU time of the process, all its threads, in clock ticks (getconf CLK_TCK, normally 100/s)
#   stime_ticks    kernel CPU time of the process, same unit
#   rss_kb         resident set size of the process in KiB
#   total_jiffies  whole host: user+nice+system+idle+iowait+irq+softirq+steal  (first line of /proc/stat; guest time is already in user)
#   idle_jiffies   whole host: idle+iowait
#
# oxia-server.sh start launches it; `oxia-server.sh stats --since E [--until E]` turns the samples into CPU figures.
# Pure bash builtins and /proc reads (no fork per sample except `sleep`), so it does not disturb the benchmark.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$here/lib.sh"

pid="" log="$OXIA_BENCH_LOGDIR/cpu.log" interval=1
while (($#)); do
  case $1 in
    --pid) pid=${2:?--pid needs a value}; shift 2 ;;
    --log) log=${2:?--log needs a value}; shift 2 ;;
    --interval) interval=${2:?--interval needs a value}; shift 2 ;;
    -h | --help) usage_from_header "${BASH_SOURCE[0]}"; exit 0 ;;
    *) die "unknown argument '$1' (see --help)" ;;
  esac
done
[[ $pid =~ ^[0-9]+$ ]] || die "--pid PID is required"
mkdir -p "$(dirname "$log")"

page_kb=$(($(getconf PAGESIZE) / 1024))

# sample: sets $line, returns 1 when the process is gone.
sample() {
  local stat rest f label u n s idle iow irq sirq steal
  { stat=$(</proc/"$pid"/stat); } 2>/dev/null || return 1
  rest=${stat##*) } # drop "pid (comm) ": the comm field may contain spaces
  read -r -a f <<<"$rest"
  # f[0] is field 3 (state), so utime (14) = f[11], stime (15) = f[12], rss in pages (24) = f[21]
  [[ ${f[0]} != Z && ${f[0]} != X ]] || return 1
  read -r label u n s idle iow irq sirq steal _ </proc/stat
  : "$label"
  line="${f[11]} ${f[12]} $((f[21] * page_kb)) $((u + n + s + idle + iow + irq + sirq + ${steal:-0})) $((idle + iow))"
}

while :; do
  ts=$(now_epoch)
  sample || break
  printf '%s %s\n' "$ts" "$line" >>"$log"
  sleep "$interval"
done

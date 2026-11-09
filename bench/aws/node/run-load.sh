#!/usr/bin/env bash
# run-load.sh --impl dotnet|go --addr HOST:PORT [options]   - runs ONE load process (the .NET or the Go twin) against an Oxia server.
#
# The two tools take the same workload; this script translates the common flags to each one's syntax, so that a scenario is described
# once.  Options (defaults in brackets; a flag that is not given is passed explicitly where it matters for fairness):
#   --impl dotnet|go          which load tool to run (oxia-load-dotnet load ... | oxia-load-go ...)            [required]
#   --addr HOST:PORT          the server (a container host name works too)                                     [required]
#   --client async|sync       [async]                      --ops put,get,delete  [put,get]   --read-percent N  [50]
#   --keys N [100000]  --value-size BYTES [128]  --producers N [64]  --random-payload
#   --duration 20s  --warmup 3s    measured window and the warm-up before it (bare numbers mean seconds)       [20s, 3s]
#   --linger 5ms  --max-requests-per-batch N  --request-timeout 30s   client options; not given = the library default
#   --rate R                  open loop at R ops/s (async only); not given = closed loop
#   --preload true|false      write all keys before the run when the mix reads or deletes                       [true]
#   --min-threads N, --tune a=b,c=d   .NET only (ThreadPool.SetMinThreads, experimental knobs); ignored for go
#   --out FILE                keep the raw output (tool stdout, then stderr) in FILE
#   --wait SECONDS            how long the server address may take to answer before giving up                  [15]
#
# stdout: the tool's own summary, then   WINDOW since=<epoch s> until=<epoch s>   (the measured window, for `oxia-server.sh stats`),
#         then the tool's   RESULT k=v k=v ...   line LAST.  stderr: the tool's stderr and this script's log.
# exit:   the tool's exit code; 2 usage error; 3 the server address does not answer; 4 the tool exited 0 but printed no RESULT line.
#
# Environment: DOTNET_BUNDLE_EXTRACT_BASE_DIR defaults to $OXIA_BENCH_STATE/dotnet-extract and HOME to /root (an SSM Run Command has no
# HOME, which a .NET single-file app needs to unpack its native libraries); other DOTNET_*/GO* variables are passed through untouched.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$here/lib.sh"

impl="" addr="" client=async ops="put,get" read_percent=50 keys=100000 value_size=128 producers=64 duration=20s warmup=3s
linger="" max_reqs="" req_timeout="" rate="" random_payload=false preload=true min_threads="" tune="" out="" wait_s=15

usage_err() { DIE_CODE=2 die "$*"; }

# --flag=value is accepted as well as --flag value
args=()
for a in "$@"; do
  if [[ $a == --*=* ]]; then args+=("${a%%=*}" "${a#*=}"); else args+=("$a"); fi
done
set -- "${args[@]+"${args[@]}"}"

while (($#)); do
  case $1 in
    --impl) impl=${2:?--impl needs a value}; shift 2 ;;
    --addr) addr=${2:?--addr needs a value}; shift 2 ;;
    --client) client=${2:?--client needs a value}; shift 2 ;;
    --ops) ops=${2:?--ops needs a value}; shift 2 ;;
    --read-percent) read_percent=${2:?--read-percent needs a value}; shift 2 ;;
    --keys) keys=${2:?--keys needs a value}; shift 2 ;;
    --value-size) value_size=${2:?--value-size needs a value}; shift 2 ;;
    --producers) producers=${2:?--producers needs a value}; shift 2 ;;
    --duration) duration=${2:?--duration needs a value}; shift 2 ;;
    --warmup) warmup=${2:?--warmup needs a value}; shift 2 ;;
    --linger) linger=${2:?--linger needs a value}; shift 2 ;;
    --max-requests-per-batch) max_reqs=${2:?--max-requests-per-batch needs a value}; shift 2 ;;
    --request-timeout) req_timeout=${2:?--request-timeout needs a value}; shift 2 ;;
    --rate) rate=${2:?--rate needs a value}; shift 2 ;;
    --random-payload) random_payload=true; shift ;;
    --preload) preload=${2:?--preload needs a value}; shift 2 ;;
    --min-threads) min_threads=${2:?--min-threads needs a value}; shift 2 ;;
    --tune) tune=${2:?--tune needs a value}; shift 2 ;;
    --out) out=${2:?--out needs a value}; shift 2 ;;
    --wait) wait_s=${2:?--wait needs a value}; shift 2 ;;
    -h | --help) usage_from_header "${BASH_SOURCE[0]}"; exit 0 ;;
    *) usage_err "unknown option '$1' (see --help)" ;;
  esac
done

[[ $impl == dotnet || $impl == go ]] || usage_err "--impl dotnet|go is required"
[[ $addr =~ ^[^:]+:[0-9]+$ ]] || usage_err "--addr HOST:PORT is required (got '${addr}')"
[[ $client == async || $client == sync ]] || usage_err "--client must be async or sync"
[[ $preload == true || $preload == false ]] || usage_err "--preload must be true or false"
[[ $ops =~ ^(put|get|delete)(,(put|get|delete))*$ ]] || usage_err "--ops must be a comma separated list of put,get,delete"
for v in read_percent keys value_size producers wait_s; do
  [[ ${!v} =~ ^[0-9]+$ ]] || usage_err "--${v//_/-} must be a non-negative integer (got '${!v}')"
done
[[ -z $max_reqs || $max_reqs =~ ^[0-9]+$ ]] || usage_err "--max-requests-per-batch must be an integer"
[[ -z $rate || $rate =~ ^[0-9]+(\.[0-9]+)?$ ]] || usage_err "--rate must be a number"

# bare numbers are seconds (durations) or milliseconds (linger); anything with a unit is passed on
norm_duration() { # norm_duration VALUE DEFAULT_UNIT
  case $1 in
    0 | *[!0-9.]*) printf '%s' "$1" ;;
    *) printf '%s%s' "$1" "$2" ;;
  esac
}
duration=$(norm_duration "$duration" s)
warmup=$(norm_duration "$warmup" s)
[[ -z $linger ]] || linger=$(norm_duration "$linger" ms)
[[ -z $req_timeout ]] || req_timeout=$(norm_duration "$req_timeout" s)

# ---------------------------------------------------------------------------------------------------------------------
# the command line of each tool
# ---------------------------------------------------------------------------------------------------------------------

if [[ $impl == dotnet ]]; then
  bin=$(find_bin oxia-load-dotnet) || die "oxia-load-dotnet was not found (looked in $OXIA_BENCH_BIN and next to the scripts)"
  cmd=("$bin" load --server "$addr" --client "$client" --ops "$ops" --read-percent "$read_percent" --keys "$keys"
    --value-size "$value_size" --producers "$producers" --duration "$duration" --warmup "$warmup" "--preload=$preload")
  [[ -z $linger ]] || cmd+=(--linger "$linger")
  [[ -z $max_reqs ]] || cmd+=(--max-requests-per-batch "$max_reqs")
  [[ -z $req_timeout ]] || cmd+=(--request-timeout "$req_timeout")
  [[ -z $rate ]] || cmd+=(--rate "$rate")
  [[ $random_payload == false ]] || cmd+=("--random-payload=true")
  [[ -z $min_threads ]] || cmd+=(--min-threads "$min_threads")
  [[ -z $tune ]] || cmd+=(--tune "$tune")
  export HOME="${HOME:-/root}"
  export DOTNET_BUNDLE_EXTRACT_BASE_DIR="${DOTNET_BUNDLE_EXTRACT_BASE_DIR:-$OXIA_BENCH_STATE/dotnet-extract}"
  export DOTNET_NOLOGO=1 DOTNET_CLI_TELEMETRY_OPTOUT=1
  mkdir -p "$DOTNET_BUNDLE_EXTRACT_BASE_DIR" 2>/dev/null || true
else
  bin=$(find_bin oxia-load-go) || die "oxia-load-go was not found (looked in $OXIA_BENCH_BIN and next to the scripts)"
  cmd=("$bin" -addr "$addr" -client "$client" -ops "$ops" -read-percent "$read_percent" -keys "$keys"
    -value-size "$value_size" -producers "$producers" -duration "$duration" -warmup "$warmup" "-preload=$preload")
  [[ -z $linger ]] || cmd+=(-linger "$linger")
  [[ -z $max_reqs ]] || cmd+=(-max-requests-per-batch "$max_reqs")
  [[ -z $req_timeout ]] || cmd+=(-request-timeout "$req_timeout")
  [[ -z $rate ]] || cmd+=(-rate "$rate")
  [[ $random_payload == false ]] || cmd+=("-random-payload=true")
fi
[[ -x $bin ]] || chmod +x "$bin" 2>/dev/null || true

# both runtimes raise the soft descriptor limit themselves; do it here too so the two see the same limit
ulimit -n "$(ulimit -Hn)" 2>/dev/null || true

# ---------------------------------------------------------------------------------------------------------------------
# refuse to run against a server that does not answer
# ---------------------------------------------------------------------------------------------------------------------

host=${addr%:*}
port=${addr##*:}
if ! wait_tcp "$host" "$port" "$wait_s"; then
  DIE_CODE=3 die "the server $addr does not answer (waited ${wait_s}s): is it started (oxia-server.sh reset) and reachable from this node?"
fi

# ---------------------------------------------------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------------------------------------------------

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
log "running: ${cmd[*]}"
t_start=$(now_epoch)
rc=0
"${cmd[@]}" >"$tmp/stdout" 2>"$tmp/stderr" </dev/null || rc=$?
t_end=$(now_epoch)

result=$(grep '^RESULT ' "$tmp/stdout" | tail -n 1 || true)

# The measured window: both tools put window_start_ms / window_end_ms (epoch milliseconds) into the RESULT line.  A tool without them
# (older build): estimate it backwards from the end of the process (the window is the last thing the tool does before it closes the client).
window=""
if [[ $result =~ window_start_ms=([0-9]+) ]]; then
  w_start_ms=${BASH_REMATCH[1]}
  if [[ $result =~ window_end_ms=([0-9]+) ]]; then
    w_end_ms=${BASH_REMATCH[1]}
    window=$(awk -v a="$w_start_ms" -v b="$w_end_ms" 'BEGIN { printf "WINDOW since=%.3f until=%.3f", a / 1000, b / 1000 }')
  fi
fi
if [[ -z $window && -n $result ]]; then
  window=$(awk -v e="$t_end" -v d="$duration" 'BEGIN { s = d; sub(/[a-z]+$/, "", s); u = d; sub(/^[0-9.]+/, "", u);
    f = (u == "ms") ? 0.001 : (u == "m") ? 60 : 1; printf "WINDOW since=%.3f until=%.3f", e - 0.5 - s * f, e - 0.5 }')
fi

if [[ -n $out ]]; then
  mkdir -p "$(dirname "$out")"
  {
    printf '# run-load.sh impl=%s addr=%s\n' "$impl" "$addr"
    printf '# command: %s\n' "${cmd[*]}"
    printf '# started_epoch=%s finished_epoch=%s exit=%s\n' "$t_start" "$t_end" "$rc"
    cat "$tmp/stdout"
    if [[ -s $tmp/stderr ]]; then
      printf '# --- stderr ---\n'
      cat "$tmp/stderr"
    fi
  } >"$out"
fi

[[ ! -s $tmp/stderr ]] || cat "$tmp/stderr" >&2
grep -v '^RESULT ' "$tmp/stdout" || true
if ((rc != 0)); then
  log "$impl exited with code $rc"
  [[ -z $result ]] || printf '%s\n' "$result"
  exit "$rc"
fi
if [[ -z $result ]]; then
  DIE_CODE=4 die "$impl exited 0 but printed no RESULT line"
fi
[[ -z $window ]] || printf '%s\n' "$window"
printf '%s\n' "$result"

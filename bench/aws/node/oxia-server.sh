#!/usr/bin/env bash
# oxia-server.sh start|stop|reset|status|stats   - runs the pinned `oxia standalone` for the benchmarks and measures its CPU.
#
#   oxia-server.sh start  [--shards N] [--sync true|false] [--notifications true|false] [--port 6648] [options]
#   oxia-server.sh stop
#   oxia-server.sh reset  [same options as start]   stop + wipe $OXIA_BENCH_DATA/{wal,db} + start + wait until ready (repeatable)
#   oxia-server.sh status                           JSON on stdout; exit 0 when running, 1 when not
#   oxia-server.sh stats  --since EPOCH [--until EPOCH]
#
# start/reset options (defaults in brackets):
#   --shards N              number of shards                                   [1]
#   --sync true|false       --wal-sync-data (fsync every WAL write)            [false = the "fast" profile of the plan]
#   --notifications BOOL    --notifications-enabled                            [true = the server default]
#   --port N                public gRPC port, bound to 0.0.0.0                 [6648]
#   --metrics-port N        Prometheus port, bound to 127.0.0.1                [7648]
#   --key-sorting S         hierarchical | natural                             [hierarchical]
#   --db-cache-mb N         --db-cache-size-mb                                 [server default]
#   --wait SECONDS          how long to wait for readiness                     [30]
#   --oxia-arg ARG          extra argument for `oxia standalone` (repeatable)
#
# Process model.  When PID 1 is systemd (Amazon Linux 2023) the server runs as the transient unit oxia-bench-server.service
# (systemd-run, LimitNOFILE=1048576, Restart=no, TimeoutStopSec=30) so that it survives the end of an SSM Run Command; otherwise (plain
# containers) as `setsid nohup` + $OXIA_BENCH_STATE/server.pid.  Output goes to $OXIA_BENCH_LOGDIR/oxia-server.log (the previous run's log is
# kept as oxia-server.log.1).
#
# Readiness (start/reset wait at most --wait seconds, default 30): the process is alive, AND its public port accepts TCP connections, AND
# `oxia health --host 127.0.0.1 --port <port>` (the binary's own gRPC health probe) succeeds.  standalone starts every shard leader
# synchronously BEFORE it opens the public port and serves the shard assignments from the first request, so "port open + healthy" means
# the shards are led and assigned; the load tools additionally wait for the assignments themselves.
#
# CPU sampler.  start also launches cpu-sampler.sh (unit oxia-bench-sampler.service, or setsid): once per second it appends
#   epoch utime_ticks stime_ticks rss_kb total_jiffies idle_jiffies      to $OXIA_BENCH_LOGDIR/cpu.log
# (the previous run's file is kept as cpu.log.1; it therefore always describes the current server only).
#
# stats --since E [--until U]  (E, U: epoch seconds, fractions allowed; --until defaults to the last sample) prints ONE JSON line, e.g.
#   {"server_cpu_pct":412.7,"server_cpu_s":41.27,"rss_mb":612.4,"window_s":10.0,...}
#   server_cpu_pct          100 x cpu seconds / window seconds: percent of ONE core, so 100 = one core fully busy and the maximum is
#                           100 x cores (an 8 vCPU host: 800).
#   server_cpu_s            user+system CPU seconds of the server (all threads) in the window = core-seconds
#   server_cpu_pct_of_host  server_cpu_pct / cores
#   host_cpu_pct            whole host busy %, from /proc/stat deltas (idle = idle+iowait), 0..100
#   rss_mb                  peak resident set size (MiB) among the samples of the window;  rss_mb_end: the last one
#   user_cpu_s, sys_cpu_s, window_s, cores, samples, since, until, window_clipped (true when the requested window starts before the
#   first or ends after the last sample)
# Samples are interpolated linearly at the window edges, so a 1 s sampling period does not bias a window of 10 s or more; when --until lies
# after the newest sample, stats waits up to 3 s for the next one.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$here/lib.sh"

UNIT="oxia-bench-server"
SAMPLER_UNIT="oxia-bench-sampler"
STATE_ENV="$OXIA_BENCH_STATE/server.env"
PID_FILE="$OXIA_BENCH_STATE/server.pid"
SAMPLER_PID_FILE="$OXIA_BENCH_STATE/sampler.pid"
SERVER_LOG="$OXIA_BENCH_LOGDIR/oxia-server.log"
CPU_LOG="$OXIA_BENCH_LOGDIR/cpu.log"

# ---------------------------------------------------------------------------------------------------------------------
# options
# ---------------------------------------------------------------------------------------------------------------------

SHARDS=1 SYNC=false NOTIF=true PORT=6648 MPORT=7648 SORTING=hierarchical CACHE_MB="" WAIT=30
EXTRA=()

need_bool() { [[ $2 == true || $2 == false ]] || die "$1 must be true or false (got '$2')"; }
need_int() { [[ $2 =~ ^[0-9]+$ ]] || die "$1 must be a non-negative integer (got '$2')"; }

parse_start_args() {
  EXTRA=()
  while (($#)); do
    case $1 in
      --shards) SHARDS=${2:?--shards needs a value}; shift 2 ;;
      --sync) SYNC=${2:?--sync needs a value}; shift 2 ;;
      --notifications) NOTIF=${2:?--notifications needs a value}; shift 2 ;;
      --port) PORT=${2:?--port needs a value}; shift 2 ;;
      --metrics-port) MPORT=${2:?--metrics-port needs a value}; shift 2 ;;
      --key-sorting) SORTING=${2:?--key-sorting needs a value}; shift 2 ;;
      --db-cache-mb) CACHE_MB=${2:?--db-cache-mb needs a value}; shift 2 ;;
      --wait) WAIT=${2:?--wait needs a value}; shift 2 ;;
      --oxia-arg) EXTRA+=("${2:?--oxia-arg needs a value}"); shift 2 ;;
      *) die "unknown option '$1' (see --help)" ;;
    esac
  done
  need_int --shards "$SHARDS"
  ((SHARDS >= 1)) || die "--shards must be >= 1"
  need_bool --sync "$SYNC"
  need_bool --notifications "$NOTIF"
  need_int --port "$PORT"
  need_int --metrics-port "$MPORT"
  need_int --wait "$WAIT"
  [[ $SORTING == hierarchical || $SORTING == natural ]] || die "--key-sorting must be hierarchical or natural"
  [[ -z $CACHE_MB ]] || need_int --db-cache-mb "$CACHE_MB"
}

# ---------------------------------------------------------------------------------------------------------------------
# state
# ---------------------------------------------------------------------------------------------------------------------

S_PID="" S_MODE="" S_PORT="" S_MPORT="" S_SHARDS="" S_SYNC="" S_NOTIF="" S_SORTING="" S_STARTED="" S_VERSION=""

load_state() {
  [[ -f $STATE_ENV ]] || return 1
  # shellcheck disable=SC1090
  source "$STATE_ENV"
}

is_oxia_pid() { pid_alive "${1:-}" && [[ "$(cat /proc/"$1"/comm 2>/dev/null || true)" == oxia ]]; }

# server_running: true when the recorded server process is alive
server_running() { load_state && is_oxia_pid "$S_PID"; }

# ---------------------------------------------------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------------------------------------------------

status_json() {
  if ! server_running; then
    printf '{"running":false}\n'
    return 1
  fi
  local now nofile listening=false sampler=false
  now=$(now_epoch)
  nofile=$(awk '/^Max open files/ { print $4 }' /proc/"$S_PID"/limits 2>/dev/null || true)
  tcp_open 127.0.0.1 "$S_PORT" 1 && listening=true
  sampler_running && sampler=true
  printf '{"running":true,"pid":%s,"mode":"%s","port":%s,"metrics_port":%s,"shards":%s,"sync":%s,"notifications":%s,"key_sorting":"%s",' \
    "$S_PID" "$S_MODE" "$S_PORT" "$S_MPORT" "$S_SHARDS" "$S_SYNC" "$S_NOTIF" "$S_SORTING"
  awk -v now="$now" -v st="$S_STARTED" 'BEGIN { printf "\"started_epoch\":%s,\"uptime_s\":%.1f,", st, now - st }'
  printf '"listening":%s,"sampler":%s,"nofile_limit":%s,"data_dir":"%s","version":"%s"}\n' \
    "$listening" "$sampler" "${nofile:-null}" "$(json_escape "$OXIA_BENCH_DATA")" "$(json_escape "$S_VERSION")"
}

cmd_status() { status_json; }

sampler_running() {
  local pid
  pid=$(cat "$SAMPLER_PID_FILE" 2>/dev/null || true)
  if [[ $S_MODE == systemd ]]; then systemctl is-active --quiet "$SAMPLER_UNIT.service" 2>/dev/null; else pid_alive "$pid"; fi
}

# ---------------------------------------------------------------------------------------------------------------------
# start
# ---------------------------------------------------------------------------------------------------------------------

LAUNCHED_PID=""

launch_server() { # launch_server OXIA_BIN ARGS... ; sets LAUNCHED_PID and S_MODE (no command substitution: the subshell would lose S_MODE)
  local oxia=$1
  shift
  local pid=""
  if have_systemd; then
    systemctl reset-failed "$UNIT.service" 2>/dev/null || true
    systemd-run --quiet --unit="$UNIT" --collect --description="Oxia standalone server (oxia-bench)" \
      --property=LimitNOFILE=1048576 --property=Restart=no --property=TasksMax=infinity --property=TimeoutStopSec=30 \
      --property=WorkingDirectory="$OXIA_BENCH_STATE" \
      --property=StandardOutput="append:$SERVER_LOG" --property=StandardError=inherit \
      "$oxia" "$@"
    pid=$(systemctl show --property=MainPID --value "$UNIT.service")
    S_MODE=systemd
  else
    rm -f "$PID_FILE"
    (
      ulimit -n 1048576 2>/dev/null || ulimit -n "$(ulimit -Hn)" 2>/dev/null || true
      lead=()
      command -v setsid >/dev/null 2>&1 && lead=(setsid)
      # the wrapper records its own pid, then becomes the server (exec keeps the pid)
      # shellcheck disable=SC2016
      "${lead[@]}" nohup bash -c 'echo $$ >"$1"; shift; exec "$@"' _ "$PID_FILE" "$oxia" "$@" >>"$SERVER_LOG" 2>&1 </dev/null &
    )
    local i
    for ((i = 0; i < 50; i++)); do
      [[ -s $PID_FILE ]] && break
      sleep 0.1
    done
    pid=$(cat "$PID_FILE" 2>/dev/null || true)
    S_MODE=fallback
  fi
  [[ $pid =~ ^[0-9]+$ && $pid -gt 0 ]] || die "could not determine the pid of the server (see $SERVER_LOG)"
  LAUNCHED_PID=$pid
}

launch_sampler() { # launch_sampler PID
  local pid=$1
  if [[ $S_MODE == systemd ]]; then
    systemctl reset-failed "$SAMPLER_UNIT.service" 2>/dev/null || true
    systemd-run --quiet --unit="$SAMPLER_UNIT" --collect --description="CPU sampler of the Oxia server (oxia-bench)" \
      --property=Restart=no --property=StandardOutput=null --property=StandardError=journal \
      --setenv=OXIA_BENCH_HOME="$OXIA_BENCH_HOME" --setenv=OXIA_BENCH_BIN="$OXIA_BENCH_BIN" --setenv=OXIA_BENCH_LOGDIR="$OXIA_BENCH_LOGDIR" \
      "$here/cpu-sampler.sh" --pid "$pid" --log "$CPU_LOG"
  else
    rm -f "$SAMPLER_PID_FILE"
    (
      lead=()
      command -v setsid >/dev/null 2>&1 && lead=(setsid)
      # shellcheck disable=SC2016
      "${lead[@]}" nohup bash -c 'echo $$ >"$1"; shift; exec "$@"' _ "$SAMPLER_PID_FILE" \
        "$here/cpu-sampler.sh" --pid "$pid" --log "$CPU_LOG" >/dev/null 2>&1 </dev/null &
    )
  fi
}

health_ok() { "$1" health --host 127.0.0.1 --port "$2" --timeout 3s >/dev/null 2>&1; }

wait_ready() { # wait_ready OXIA PID PORT SECONDS ; prints the milliseconds it took
  local oxia=$1 pid=$2 port=$3 limit=$4 t0 phase="the process to start"
  t0=$(now_ms)
  while :; do
    if ! pid_alive "$pid"; then
      log "the server exited during start-up; last lines of $SERVER_LOG:"
      tail -n 30 "$SERVER_LOG" >&2 || true
      return 1
    fi
    phase="the port $port to accept connections"
    if tcp_open 127.0.0.1 "$port" 1; then
      phase="oxia health to succeed"
      if health_ok "$oxia" "$port"; then
        printf '%s' "$(($(now_ms) - t0))"
        return 0
      fi
    fi
    if (($(now_ms) - t0 > limit * 1000)); then
      log "gave up after ${limit}s waiting for $phase; last lines of $SERVER_LOG:"
      tail -n 30 "$SERVER_LOG" >&2 || true
      return 1
    fi
    sleep 0.1
  done
}

cmd_start() {
  parse_start_args "$@"
  require_root
  local oxia
  oxia=$(find_bin oxia) || die "the oxia binary was not found (looked in $OXIA_BENCH_BIN, next to the scripts and one level up)"
  [[ -x $oxia ]] || chmod +x "$oxia"
  ensure_musl_loader "$oxia"
  if server_running; then die "the server is already running (pid $S_PID); use reset"; fi
  tcp_open 127.0.0.1 "$PORT" 1 && die "port $PORT is already in use by another process"
  [[ -n $OXIA_BENCH_DATA && $OXIA_BENCH_DATA != / ]] || die "OXIA_BENCH_DATA must not be empty or /"
  mkdir -p "$OXIA_BENCH_DATA/wal" "$OXIA_BENCH_DATA/db" "$OXIA_BENCH_STATE" "$OXIA_BENCH_LOGDIR"

  # per-run logs: the previous ones are kept as .1
  [[ -f $SERVER_LOG ]] && mv -f "$SERVER_LOG" "$SERVER_LOG.1"
  [[ -f $CPU_LOG ]] && mv -f "$CPU_LOG" "$CPU_LOG.1"
  : >"$SERVER_LOG"
  : >"$CPU_LOG"

  local args=(standalone
    --shards "$SHARDS"
    --public-addr "0.0.0.0:$PORT"
    --metrics-addr "127.0.0.1:$MPORT"
    --data-dir "$OXIA_BENCH_DATA/db"
    --wal-dir "$OXIA_BENCH_DATA/wal"
    "--wal-sync-data=$SYNC"
    "--notifications-enabled=$NOTIF"
    --key-sorting "$SORTING")
  [[ -z $CACHE_MB ]] || args+=(--db-cache-size-mb "$CACHE_MB")
  args+=("${EXTRA[@]}")

  S_VERSION=$("$oxia" --version 2>/dev/null | head -n1 || true)
  log "starting: oxia ${args[*]}"
  launch_server "$oxia" "${args[@]}"
  local pid=$LAUNCHED_PID
  S_PID=$pid S_PORT=$PORT S_MPORT=$MPORT S_SHARDS=$SHARDS S_SYNC=$SYNC S_NOTIF=$NOTIF S_SORTING=$SORTING S_STARTED=$(now_epoch)
  {
    local v
    for v in S_PID S_MODE S_PORT S_MPORT S_SHARDS S_SYNC S_NOTIF S_SORTING S_STARTED S_VERSION; do
      printf '%s=%q\n' "$v" "${!v}"
    done
  } >"$STATE_ENV"
  launch_sampler "$pid"

  local ready_ms
  if ! ready_ms=$(wait_ready "$oxia" "$pid" "$PORT" "$WAIT"); then
    cmd_stop || true
    die "the server did not become ready"
  fi
  log "ready after ${ready_ms} ms (pid $pid, $S_MODE, $SHARDS shard(s), wal sync $SYNC, notifications $NOTIF, port $PORT)"
  status_json | sed "s/}\$/,\"ready_ms\":$ready_ms}/"
}

# ---------------------------------------------------------------------------------------------------------------------
# stop / reset
# ---------------------------------------------------------------------------------------------------------------------

kill_and_wait() { # kill_and_wait PID TERM_SECONDS
  local pid=$1 limit=$2 i
  pid_alive "$pid" || return 0
  kill -TERM "$pid" 2>/dev/null || true
  for ((i = 0; i < limit * 10; i++)); do
    pid_alive "$pid" || return 0
    sleep 0.1
  done
  warn "pid $pid ignored SIGTERM for ${limit}s: sending SIGKILL"
  kill -KILL "$pid" 2>/dev/null || true
  for ((i = 0; i < 50; i++)); do
    pid_alive "$pid" || return 0
    sleep 0.1
  done
  return 1
}

cmd_stop() {
  require_root
  local pid="" mode=""
  if load_state; then
    pid=$S_PID
    mode=$S_MODE
  fi
  # the sampler first (it exits by itself once the server is gone, this just makes it deterministic)
  if have_systemd; then systemctl stop "$SAMPLER_UNIT.service" 2>/dev/null || true; fi
  if [[ -f $SAMPLER_PID_FILE ]]; then
    kill_and_wait "$(cat "$SAMPLER_PID_FILE")" 5 || true
    rm -f "$SAMPLER_PID_FILE"
  fi
  if have_systemd && { [[ $mode == systemd || -z $mode ]]; }; then
    systemctl stop "$UNIT.service" 2>/dev/null || true
  fi
  if [[ -n $pid ]] && is_oxia_pid "$pid"; then
    kill_and_wait "$pid" 30 || die "could not stop the server (pid $pid)"
  fi
  rm -f "$STATE_ENV" "$PID_FILE"
  log "stopped"
}

cmd_reset() {
  parse_start_args "$@" # validate before touching anything
  cmd_stop
  [[ -n $OXIA_BENCH_DATA && $OXIA_BENCH_DATA != / ]] || die "OXIA_BENCH_DATA must not be empty or /"
  log "wiping $OXIA_BENCH_DATA/wal and $OXIA_BENCH_DATA/db"
  rm -rf "${OXIA_BENCH_DATA:?}/wal" "${OXIA_BENCH_DATA:?}/db"
  cmd_start "$@"
}

# ---------------------------------------------------------------------------------------------------------------------
# stats
# ---------------------------------------------------------------------------------------------------------------------

cmd_stats() {
  local t_from="" t_to="" file=$CPU_LOG
  while (($#)); do
    case $1 in
      --since) t_from=${2:?--since needs a value}; shift 2 ;;
      --until) t_to=${2:?--until needs a value}; shift 2 ;;
      --log) file=${2:?--log needs a value}; shift 2 ;;
      *) die "unknown option '$1' (see --help)" ;;
    esac
  done
  local num='^[0-9]+(\.[0-9]+)?$'
  [[ $t_from =~ $num ]] || die "--since EPOCH (seconds since the epoch) is required"
  [[ -z $t_to || $t_to =~ $num ]] || die "--until must be an epoch in seconds"
  [[ -s $file ]] || die "no CPU samples in $file (is the server running? start launches the sampler)"
  # the sampler writes once a second: when the window ends after the newest sample, give the next sample (at most 3 s) time to arrive,
  # otherwise the window would be clipped by up to a second and core-seconds under-reported
  if [[ -n $t_to ]]; then
    local waited=0 last
    while ((waited < 15)); do
      last=$(tail -n 1 "$file" | cut -d' ' -f1)
      awk -v l="${last:-0}" -v t="$t_to" 'BEGIN { exit !(l + 0 >= t + 0) }' && break
      server_running || break
      sleep 0.2
      waited=$((waited + 1))
    done
  fi
  local hz cores
  hz=$(getconf CLK_TCK)
  cores=$(nproc)

  awk -v since="$t_from" -v until="$t_to" -v hz="$hz" -v cores="$cores" '
    function interp(arr, t,    i, d) {
      if (t <= E[1]) return arr[1]
      if (t >= E[n]) return arr[n]
      for (i = 2; i <= n; i++) if (E[i] >= t) break
      d = E[i] - E[i - 1]
      if (d <= 0) return arr[i]
      return arr[i - 1] + (arr[i] - arr[i - 1]) * (t - E[i - 1]) / d
    }
    NF >= 6 && $1 ~ /^[0-9]+(\.[0-9]+)?$/ { n++; E[n] = $1; U[n] = $2; S[n] = $3; R[n] = $4; T[n] = $5; I[n] = $6 }
    END {
      if (n < 2) { print "error: fewer than two CPU samples" > "/dev/stderr"; exit 2 }
      ws = since + 0
      we = (until == "") ? E[n] : until + 0
      clipped = 0
      if (ws < E[1] - 0.001) { ws = E[1]; clipped = 1 }
      if (we > E[n] + 0.001) { we = E[n]; if (until != "") clipped = 1 }
      if (we <= ws) { printf "error: the window [%s, %s] holds no samples (samples cover %.3f .. %.3f)\n", since, until, E[1], E[n] > "/dev/stderr"; exit 2 }
      user = (interp(U, we) - interp(U, ws)) / hz
      sys = (interp(S, we) - interp(S, ws)) / hz
      cpu = user + sys
      win = we - ws
      dt = interp(T, we) - interp(T, ws)
      di = interp(I, we) - interp(I, ws)
      host = (dt > 0) ? 100 * (dt - di) / dt : 0
      peak = -1; count = 0; last = R[n]
      for (i = 1; i <= n; i++) if (E[i] >= ws - 0.001 && E[i] <= we + 0.001) { count++; if (R[i] > peak) peak = R[i]; last = R[i] }
      if (peak < 0) { peak = interp(R, ws); last = interp(R, we) }
      printf "{\"server_cpu_pct\":%.2f,\"server_cpu_s\":%.3f,\"rss_mb\":%.1f,\"window_s\":%.3f,\"cores\":%d,", 100 * cpu / win, cpu, peak / 1024, win, cores
      printf "\"server_cpu_pct_of_host\":%.2f,\"host_cpu_pct\":%.2f,\"user_cpu_s\":%.3f,\"sys_cpu_s\":%.3f,\"rss_mb_end\":%.1f,", \
        100 * cpu / win / cores, host, user, sys, last / 1024
      printf "\"samples\":%d,\"since\":%.3f,\"until\":%.3f,\"window_clipped\":%s}\n", count, ws, we, (clipped ? "true" : "false")
    }' "$file"
}

# ---------------------------------------------------------------------------------------------------------------------

case ${1:-} in
  start) shift; cmd_start "$@" ;;
  stop) shift; cmd_stop "$@" ;;
  reset) shift; cmd_reset "$@" ;;
  status) shift; cmd_status "$@" ;;
  stats) shift; cmd_stats "$@" ;;
  -h | --help | help) usage_from_header "${BASH_SOURCE[0]}" ;;
  *) usage_short "${BASH_SOURCE[0]}" >&2; exit 2 ;;
esac

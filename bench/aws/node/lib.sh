# shellcheck shell=bash
# lib.sh - helpers shared by the oxia-bench node scripts.  Sourced, never executed.
#
# Environment (all optional):
#   OXIA_BENCH_HOME     /opt/oxia-bench        install root
#   OXIA_BENCH_BIN      $OXIA_BENCH_HOME/bin   where oxia, oxia-load-dotnet, oxia-load-go and the scripts live
#   OXIA_BENCH_DATA     /data                  server data root: $OXIA_BENCH_DATA/wal and $OXIA_BENCH_DATA/db
#   OXIA_BENCH_STATE    /var/lib/oxia-bench    node.json, server.json, pid files
#   OXIA_BENCH_LOGDIR   /var/log/oxia-bench    oxia-server.log, cpu.log

# Numbers are parsed and printed in the C locale everywhere (EPOCHREALTIME, awk, printf).
export LC_ALL=C

: "${OXIA_BENCH_HOME:=/opt/oxia-bench}"
: "${OXIA_BENCH_BIN:=$OXIA_BENCH_HOME/bin}"
: "${OXIA_BENCH_DATA:=/data}"
: "${OXIA_BENCH_STATE:=/var/lib/oxia-bench}"
: "${OXIA_BENCH_LOGDIR:=/var/log/oxia-bench}"

LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_NAME="$(basename "${0:-oxia-bench}")"

# ---------------------------------------------------------------------------------------------------------------------
# logging: human text on stderr, machine readable output on stdout (callers' business)
# ---------------------------------------------------------------------------------------------------------------------

log() { printf '%s [%s] %s\n' "$(date -u +%H:%M:%S)" "$SCRIPT_NAME" "$*" >&2; }
warn() { log "WARNING: $*"; }
die() { log "ERROR: $*"; exit "${DIE_CODE:-1}"; }

# ---------------------------------------------------------------------------------------------------------------------
# time
# ---------------------------------------------------------------------------------------------------------------------

# Seconds since the epoch with microseconds ("1731140000.123456").
now_epoch() {
  if [[ -n ${EPOCHREALTIME:-} ]]; then printf '%s' "$EPOCHREALTIME"; else date +%s.%N; fi
}

# Milliseconds since the epoch (integer).
now_ms() {
  if [[ -n ${EPOCHREALTIME:-} ]]; then
    local t=${EPOCHREALTIME/./}
    printf '%s' "$((t / 1000))"
  else
    date +%s%3N
  fi
}

iso_utc() { date -u +%Y-%m-%dT%H:%M:%SZ; }

# usage_from_header FILE: prints the leading comment block of a script (without the shebang) as its --help text.
usage_from_header() { awk 'NR == 1 { next } /^#/ { sub(/^# ?/, ""); print; next } { exit }' "$1"; }

# ---------------------------------------------------------------------------------------------------------------------
# environment
# ---------------------------------------------------------------------------------------------------------------------

require_root() { [[ $EUID -eq 0 ]] || die "$SCRIPT_NAME must run as root (SSM Run Command does; in a container use docker exec as root)"; }

# True when PID 1 is systemd and systemd-run is usable (Amazon Linux 2023 instances); false in plain containers.
have_systemd() {
  [[ "$(cat /proc/1/comm 2>/dev/null || true)" == systemd ]] && [[ -d /run/systemd/system ]] && command -v systemd-run >/dev/null 2>&1 && command -v systemctl >/dev/null 2>&1
}

# find_bin NAME: absolute path of a file shipped with the artifacts.  Looks in $OXIA_BENCH_BIN, next to the scripts, one level up (the
# artifact layout has the scripts in node/) and in $OXIA_BENCH_HOME/bin.
find_bin() {
  local name=$1 d
  for d in "$OXIA_BENCH_BIN" "$LIB_DIR" "$LIB_DIR/.." "$OXIA_BENCH_HOME/bin"; do
    if [[ -f $d/$name ]]; then
      (cd "$d" && printf '%s/%s\n' "$PWD" "$name")
      return 0
    fi
  done
  return 1
}

# True when the process exists and is neither a zombie nor dead (kill -0 succeeds on zombies, which nobody reaps in a container without init).
pid_alive() {
  local pid=${1:-} stat
  [[ $pid =~ ^[0-9]+$ ]] || return 1
  [[ -r /proc/$pid/stat ]] || return 1
  { stat=$(</proc/"$pid"/stat); } 2>/dev/null || return 1
  stat=${stat##*) }
  [[ ${stat%% *} != Z && ${stat%% *} != X ]]
}

# ---------------------------------------------------------------------------------------------------------------------
# network
# ---------------------------------------------------------------------------------------------------------------------

# tcp_open HOST PORT [TIMEOUT_SECONDS]: dependency free TCP connect probe (bash /dev/tcp; resolves container host names).
tcp_open() {
  # shellcheck disable=SC2016
  timeout "${3:-2}" bash -c 'exec 3<>"/dev/tcp/$1/$2"' _ "$1" "$2" 2>/dev/null
}

# wait_tcp HOST PORT SECONDS: poll until the port accepts a connection.
wait_tcp() {
  local host=$1 port=$2 limit=$3 t0
  t0=$(now_ms)
  while ! tcp_open "$host" "$port" 2; do
    if (($(now_ms) - t0 > limit * 1000)); then return 1; fi
    sleep 0.2
  done
}

# ---------------------------------------------------------------------------------------------------------------------
# JSON without jq (the scripts that must work before the packages are installed)
# ---------------------------------------------------------------------------------------------------------------------

json_escape() {
  local s=$1
  s=${s//\\/\\\\}
  s=${s//\"/\\\"}
  s=${s//$'\n'/\\n}
  s=${s//$'\r'/\\r}
  s=${s//$'\t'/\\t}
  printf '%s' "$s"
}

# ---------------------------------------------------------------------------------------------------------------------
# the Oxia server binary (taken from the Alpine based oxia/oxia image: dynamically linked against musl)
# ---------------------------------------------------------------------------------------------------------------------

# elf_interpreter FILE: the PT_INTERP path of an ELF executable, nothing for a static one.  The interpreter string sits in the first page.
elf_interpreter() {
  head -c 4096 "$1" 2>/dev/null | grep -a -o -m1 '/lib[0-9]*/ld-[A-Za-z0-9._-]*' | head -n1 || true
}

# ensure_musl_loader FILE: when FILE needs the musl dynamic loader (/lib/ld-musl-<arch>.so.1) and the node does not have it, install the
# copy that Publish-Artifacts.ps1 took from the same image (it ships next to the binary).  musl lives in its own paths, so it coexists with
# glibc; this is what Alpine's own musl package does.
ensure_musl_loader() {
  local bin=$1 interp name src
  interp=$(elf_interpreter "$bin")
  [[ $interp == */ld-musl-* ]] || return 0
  [[ -e $interp ]] && return 0
  name=${interp##*/}
  src=$(find_bin "$name") || die "$bin needs the musl loader $interp and $name is not next to it (re-run Publish-Artifacts.ps1)"
  require_root
  install -D -m 0755 "$src" "$interp"
  ln -sf "$name" "${interp%/*}/${name/ld-musl-/libc.musl-}"
  log "installed the musl loader $interp (needed by $(basename "$bin"))"
}

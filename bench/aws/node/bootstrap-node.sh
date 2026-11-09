#!/usr/bin/env bash
# bootstrap-node.sh server|client   - idempotent OS tuning and tool installation of a benchmark node.
#
# Safe to run any number of times; never prompts.  Needs the internet only to install missing packages (dnf on Amazon Linux 2023 / Fedora,
# apt-get on Debian/Ubuntu); with everything installed it works offline.  In a container /proc/sys and /sys are usually read-only: each
# tuning that cannot be applied is logged ("skipped") and recorded in node.json, and the script still succeeds.
#
# What it does
#   * installs jq, procps-ng (ps/pgrep) and util-linux (setsid/lscpu) when missing
#   * sysctl (live and persisted in /etc/sysctl.d/99-oxia-bench.conf):
#       net.core.somaxconn=4096  net.ipv4.tcp_max_syn_backlog=8192  net.core.netdev_max_backlog=16384  fs.file-max=2097152
#       vm.swappiness=1  vm.max_map_count=1048576
#   * /etc/security/limits.d/99-oxia-bench.conf: nofile 1048576 for every user (root included), and with systemd
#     DefaultLimitNOFILE=1048576 in /etc/systemd/system.conf.d/ (effective for services started after the next daemon-reexec/boot)
#   * transparent huge pages off (enabled and defrag = never)
#   * creates /opt/oxia-bench/bin, /var/lib/oxia-bench, /var/log/oxia-bench and, for the server, $OXIA_BENCH_DATA/{wal,db}
#   * server: the musl loader the Oxia binary needs (see lib.sh ensure_musl_loader); optionally the data volume, see below
#   * writes /var/lib/oxia-bench/node.json: role, time, kernel, cpu, memory, and every tuning with its applied/skipped status
#
# Environment
#   OXIA_BENCH_DATA_DEVICE   "auto" or a block device (e.g. /dev/nvme1n1): format it with xfs when it has no filesystem and mount it on
#                            $OXIA_BENCH_DATA (noatime).  "auto" picks the only unmounted, unpartitioned disk of 50 GiB or more.  Unset =
#                            use $OXIA_BENCH_DATA as it is (the CloudFormation user data may already have mounted the volume).
#   OXIA_BENCH_SKIP_PACKAGES=1   never call the package manager
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$here/lib.sh"

case ${1:-} in
  server | client) role=$1 ;;
  -h | --help) usage_from_header "${BASH_SOURCE[0]}"; exit 0 ;;
  *) usage_short "${BASH_SOURCE[0]}" >&2; exit 2 ;;
esac
require_root

APPLIED=() SKIPPED=()
applied() { APPLIED+=("$1"); log "applied: $1"; }
skipped() { SKIPPED+=("$1"); log "skipped: $1"; }

# ---------------------------------------------------------------------------------------------------------------------
# packages
# ---------------------------------------------------------------------------------------------------------------------

pkg_manager() {
  if command -v dnf >/dev/null 2>&1; then echo dnf
  elif command -v apt-get >/dev/null 2>&1; then echo apt
  elif command -v yum >/dev/null 2>&1; then echo yum
  fi
}

pm_install() { # pm_install MANAGER PACKAGES... ; the package manager's output is shown only when it fails
  local pm=$1 out rc=0
  shift
  out=$(mktemp)
  case $pm in
    dnf | yum) "$pm" install -y -q --setopt=install_weak_deps=False "$@" >"$out" 2>&1 || rc=$? ;;
    apt)
      export DEBIAN_FRONTEND=noninteractive
      { apt-get update -qq && apt-get install -y -qq --no-install-recommends "$@"; } >"$out" 2>&1 || rc=$?
      ;;
  esac
  if ((rc != 0)); then tail -n 20 "$out" >&2; fi
  rm -f "$out"
  return "$rc"
}

install_packages() {
  local missing=() pm
  command -v jq >/dev/null 2>&1 || missing+=(jq)
  command -v ps >/dev/null 2>&1 || missing+=(procps)
  { command -v setsid >/dev/null 2>&1 && command -v lscpu >/dev/null 2>&1; } || missing+=(util-linux)
  if ((${#missing[@]} == 0)); then
    log "packages: jq, procps and util-linux are present"
    return 0
  fi
  if [[ ${OXIA_BENCH_SKIP_PACKAGES:-0} == 1 ]]; then
    skipped "package install (OXIA_BENCH_SKIP_PACKAGES=1); missing: ${missing[*]}"
    return 0
  fi
  pm=$(pkg_manager)
  local list=() m
  case $pm in
    dnf | yum) # RPM package names: procps is procps-ng
      for m in "${missing[@]}"; do
        if [[ $m == procps ]]; then list+=(procps-ng); else list+=("$m"); fi
      done
      ;;
    apt) list=("${missing[@]}") ;;
    *)
      warn "no package manager found; cannot install: ${missing[*]}"
      return 0
      ;;
  esac
  local attempt
  for attempt in 1 2 3; do
    log "installing ${list[*]} with $pm (attempt $attempt)"
    if pm_install "$pm" "${list[@]}"; then
      applied "packages installed: ${list[*]}"
      return 0
    fi
    sleep $((attempt * 3))
  done
  command -v jq >/dev/null 2>&1 || die "could not install ${list[*]} (jq is required by sysinfo.sh)"
  warn "could not install ${list[*]}; continuing because jq is present"
}

# ---------------------------------------------------------------------------------------------------------------------
# kernel and limits
# ---------------------------------------------------------------------------------------------------------------------

SYSCTLS=(
  "net.core.somaxconn=4096"
  "net.ipv4.tcp_max_syn_backlog=8192"
  "net.core.netdev_max_backlog=16384"
  "fs.file-max=2097152"
  "vm.swappiness=1"
  "vm.max_map_count=1048576"
)

apply_sysctls() {
  local conf=/etc/sysctl.d/99-oxia-bench.conf kv key want path cur
  write_file "$conf" "# written by oxia-bench bootstrap-node.sh" "${SYSCTLS[@]}" || warn "cannot write $conf"
  for kv in "${SYSCTLS[@]}"; do
    key=${kv%%=*} want=${kv#*=}
    path=/proc/sys/${key//.//}
    if [[ ! -e $path ]]; then
      skipped "sysctl $key (does not exist in this kernel)"
      continue
    fi
    cur=$(<"$path")
    if [[ $cur == "$want" ]]; then
      applied "sysctl $key=$want (already)"
    elif { echo "$want" >"$path"; } 2>/dev/null; then
      applied "sysctl $key=$want (was $cur)"
    else
      skipped "sysctl $key=$want (read-only /proc/sys; is $cur)"
    fi
  done
}

# write_file PATH LINES...: one argument per line; returns 1 when the file cannot be written
write_file() {
  local f=$1
  shift
  mkdir -p "$(dirname "$f")" 2>/dev/null && printf '%s\n' "$@" >"$f" 2>/dev/null
}

apply_limits() {
  local f=/etc/security/limits.d/99-oxia-bench.conf
  if write_file "$f" "# written by oxia-bench bootstrap-node.sh" \
    "*    soft nofile 1048576" "*    hard nofile 1048576" "root soft nofile 1048576" "root hard nofile 1048576"; then
    applied "limits.d: nofile 1048576 ($f)"
  else
    skipped "limits.d nofile (cannot write $f)"
  fi
  if have_systemd; then
    write_file /etc/systemd/system.conf.d/99-oxia-bench.conf "# written by oxia-bench bootstrap-node.sh" "[Manager]" "DefaultLimitNOFILE=1048576"
    applied "systemd DefaultLimitNOFILE=1048576 (for services started after the next daemon-reexec/boot; the server unit sets LimitNOFILE itself)"
  fi
}

apply_thp() {
  local base=/sys/kernel/mm/transparent_hugepage f cur
  if [[ ! -d $base ]]; then
    skipped "transparent hugepages (no $base)"
    return 0
  fi
  for f in enabled defrag; do
    cur=$(<"$base/$f")
    if [[ $cur == *"[never]"* ]]; then
      applied "thp $f=never (already)"
    elif { echo never >"$base/$f"; } 2>/dev/null; then
      applied "thp $f=never (was ${cur})"
    else
      skipped "thp $f=never (read-only /sys; is ${cur})"
    fi
  done
}

# ---------------------------------------------------------------------------------------------------------------------
# data volume (server)
# ---------------------------------------------------------------------------------------------------------------------

setup_data_volume() {
  local dev=${OXIA_BENCH_DATA_DEVICE:-}
  [[ -n $dev ]] || return 0
  if mountpoint -q "$OXIA_BENCH_DATA" 2>/dev/null; then
    applied "data volume: $OXIA_BENCH_DATA is already a mount point"
    return 0
  fi
  if [[ $dev == auto ]]; then
    local cands
    cands=$(lsblk -dnb -o NAME,SIZE,TYPE,MOUNTPOINT 2>/dev/null | awk '$3 == "disk" && $4 == "" && $2 >= 50*1024*1024*1024 { print "/dev/" $1 }')
    # a disk with partitions or with mounted children is not a data disk
    local d pick=()
    for d in $cands; do
      [[ -z $(lsblk -n -o MOUNTPOINT "$d" 2>/dev/null | tr -d '[:space:]') ]] && pick+=("$d")
    done
    ((${#pick[@]} == 1)) || die "OXIA_BENCH_DATA_DEVICE=auto: expected exactly one free disk >= 50 GiB, found ${#pick[@]} (${pick[*]:-none})"
    dev=${pick[0]}
  fi
  [[ -b $dev ]] || die "OXIA_BENCH_DATA_DEVICE=$dev is not a block device"
  if ! blkid "$dev" >/dev/null 2>&1; then
    log "formatting $dev with xfs"
    mkfs.xfs -q -f "$dev" >&2
  fi
  mkdir -p "$OXIA_BENCH_DATA"
  mount -o noatime "$dev" "$OXIA_BENCH_DATA"
  applied "data volume: $dev mounted on $OXIA_BENCH_DATA (noatime)"
}

# ---------------------------------------------------------------------------------------------------------------------
# node.json
# ---------------------------------------------------------------------------------------------------------------------

write_node_json() {
  local cpu_model cores mem_kb os_pretty kernel arch thp swappiness sysctl_json="{" first=1 kv key path
  cpu_model=$(awk -F': ' '/^model name|^Model name|^Hardware|^cpu model/ { print $2; exit }' /proc/cpuinfo 2>/dev/null || true)
  [[ -n $cpu_model ]] || cpu_model=$(lscpu 2>/dev/null | awk -F': +' '/^Model name/ { print $2; exit }' || true)
  cores=$(nproc)
  mem_kb=$(awk '/^MemTotal/ { print $2 }' /proc/meminfo)
  # shellcheck disable=SC1091
  os_pretty=$( (. /etc/os-release 2>/dev/null && printf '%s' "${PRETTY_NAME:-unknown}") || echo unknown)
  kernel=$(uname -r)
  arch=$(uname -m)
  thp=$(sed -n 's/.*\[\(.*\)\].*/\1/p' /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null || true)
  for kv in "${SYSCTLS[@]}"; do
    key=${kv%%=*}
    path=/proc/sys/${key//.//}
    [[ -e $path ]] || continue
    ((first)) || sysctl_json+=","
    first=0
    sysctl_json+="\"$key\":\"$(<"$path")\""
  done
  sysctl_json+="}"
  swappiness=$(cat /proc/sys/vm/swappiness 2>/dev/null || true)

  local list_applied list_skipped item
  list_applied="" list_skipped=""
  for item in "${APPLIED[@]+"${APPLIED[@]}"}"; do list_applied+="\"$(json_escape "$item")\","; done
  for item in "${SKIPPED[@]+"${SKIPPED[@]}"}"; do list_skipped+="\"$(json_escape "$item")\","; done

  mkdir -p "$OXIA_BENCH_STATE"
  local tmp="$OXIA_BENCH_STATE/node.json.tmp"
  cat >"$tmp" <<EOF
{
  "role": "$role",
  "bootstrapped_utc": "$(iso_utc)",
  "bootstrapped_epoch": $(date +%s),
  "os": "$(json_escape "$os_pretty")",
  "kernel": "$(json_escape "$kernel")",
  "arch": "$arch",
  "cpu_model": "$(json_escape "$cpu_model")",
  "cores": $cores,
  "memory_mb": $((mem_kb / 1024)),
  "systemd": $(have_systemd && echo true || echo false),
  "thp_enabled": "$thp",
  "vm_swappiness": "$swappiness",
  "nofile_soft": "$(ulimit -Sn)",
  "nofile_hard": "$(ulimit -Hn)",
  "data_dir": "$(json_escape "$OXIA_BENCH_DATA")",
  "sysctl": $sysctl_json,
  "tuning_applied": [${list_applied%,}],
  "tuning_skipped": [${list_skipped%,}]
}
EOF
  mv -f "$tmp" "$OXIA_BENCH_STATE/node.json"
  log "wrote $OXIA_BENCH_STATE/node.json"
}

# ---------------------------------------------------------------------------------------------------------------------

log "bootstrapping a $role node"
install_packages
mkdir -p "$OXIA_BENCH_BIN" "$OXIA_BENCH_STATE" "$OXIA_BENCH_LOGDIR"
apply_sysctls
apply_limits
apply_thp
if [[ $role == server ]]; then
  setup_data_volume
  mkdir -p "$OXIA_BENCH_DATA/wal" "$OXIA_BENCH_DATA/db"
  if oxia=$(find_bin oxia); then ensure_musl_loader "$oxia"; else warn "oxia binary not found yet (install-artifacts.sh first)"; fi
fi
# make the shipped files executable (harmless when they are not there yet)
chmod +x "$OXIA_BENCH_BIN"/*.sh "$OXIA_BENCH_BIN"/oxia "$OXIA_BENCH_BIN"/oxia-load-dotnet "$OXIA_BENCH_BIN"/oxia-load-go 2>/dev/null || true
write_node_json
printf '{"bootstrapped":true,"role":"%s","applied":%d,"skipped":%d}\n' "$role" "${#APPLIED[@]}" "${#SKIPPED[@]}"

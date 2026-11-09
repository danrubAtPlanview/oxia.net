#!/usr/bin/env bash
# sysinfo.sh   - prints ONE JSON document on stdout with the facts of this node (logs go to stderr).
#
#   {"schema":1, "collected_utc", "role", "os":{pretty_name,id,version_id,kernel,arch},
#    "cpu":{model,vendor,logical_cpus,sockets,cores_per_socket,threads_per_core,max_mhz,l3_cache,hypervisor,nproc,cgroup_cpus},
#    "memory":{total_mb,swap_mb,thp_enabled,swappiness},
#    "kernel_tuning":{"net.core.somaxconn":...,...}, "limits":{nofile_soft,nofile_hard},
#    "runtime":{container,systemd},
#    "ec2":null | {instance_type,ami_id,availability_zone,region},        (instance metadata v2; no ids of the instance or the account)
#    "data_volume":null | {mount,device,fstype,options,size_gb},
#    "software":{"oxia":{version,sha256}, "oxia_load_dotnet":{sha256}, "oxia_load_go":{sha256}, "artifacts": <VERSIONS.json or null>},
#    "node": <node.json written by bootstrap-node.sh, or null>}
#
# Needs jq (bootstrap-node.sh installs it).  Environment: OXIA_BENCH_NO_IMDS=1 skips the instance metadata probe (2 s timeout off EC2).
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$here/lib.sh"

case ${1:-} in
  -h | --help) usage_from_header "${BASH_SOURCE[0]}"; exit 0 ;;
esac
command -v jq >/dev/null 2>&1 || die "jq is required (bootstrap-node.sh installs it)"

lscpu_out=$(lscpu 2>/dev/null || true)
lscpu_val() { printf '%s\n' "$lscpu_out" | awk -F': +' -v k="$1" '$1 == k { print $2; exit }'; }
slurp() { cat "$1" 2>/dev/null | tr -d '\n' || true; }
sha() { if [[ -f $1 ]]; then sha256sum "$1" | cut -d' ' -f1; fi; }

# ---- os / cpu / memory -----------------------------------------------------------------------------------------------
os_json=$(
  # shellcheck disable=SC1091
  . /etc/os-release 2>/dev/null || true
  jq -n --arg pretty "${PRETTY_NAME:-unknown}" --arg id "${ID:-}" --arg ver "${VERSION_ID:-}" --arg kernel "$(uname -r)" --arg arch "$(uname -m)" \
    '{pretty_name: $pretty, id: $id, version_id: $ver, kernel: $kernel, arch: $arch}'
)

cpu_model=$(lscpu_val "Model name")
[[ -n $cpu_model ]] || cpu_model=$(awk -F': ' '/^model name|^Hardware|^cpu model/ { print $2; exit }' /proc/cpuinfo 2>/dev/null || true)
cgroup_cpus=null
if [[ -r /sys/fs/cgroup/cpu.max ]]; then
  read -r quota period </sys/fs/cgroup/cpu.max || true
  if [[ ${quota:-max} != max && ${period:-0} -gt 0 ]]; then cgroup_cpus=$(awk -v q="$quota" -v p="$period" 'BEGIN { printf "%.2f", q / p }'); fi
fi
cpu_json=$(jq -n --arg model "$cpu_model" --arg vendor "$(lscpu_val "Vendor ID")" --arg logical "$(lscpu_val "CPU(s)")" \
  --arg sockets "$(lscpu_val "Socket(s)")" --arg cps "$(lscpu_val "Core(s) per socket")" --arg tpc "$(lscpu_val "Thread(s) per core")" \
  --arg mhz "$(lscpu_val "CPU max MHz")" --arg l3 "$(lscpu_val "L3 cache")" --arg hyp "$(lscpu_val "Hypervisor vendor")" \
  --argjson nproc "$(nproc)" --argjson cgroup "$cgroup_cpus" \
  '{model: $model, vendor: $vendor, logical_cpus: ($logical | tonumber? // null), sockets: ($sockets | tonumber? // null),
    cores_per_socket: ($cps | tonumber? // null), threads_per_core: ($tpc | tonumber? // null), max_mhz: ($mhz | tonumber? // null),
    l3_cache: $l3, hypervisor: $hyp, nproc: $nproc, cgroup_cpus: $cgroup}')

mem_json=$(jq -n --argjson total "$(awk '/^MemTotal/ { print int($2 / 1024) }' /proc/meminfo)" \
  --argjson swap "$(awk '/^SwapTotal/ { print int($2 / 1024) }' /proc/meminfo)" \
  --arg thp "$(sed -n 's/.*\[\(.*\)\].*/\1/p' /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null || true)" \
  --arg swappiness "$(slurp /proc/sys/vm/swappiness)" \
  '{total_mb: $total, swap_mb: $swap, thp_enabled: $thp, swappiness: $swappiness}')

tuning_json='{}'
for key in net.core.somaxconn net.ipv4.tcp_max_syn_backlog net.core.netdev_max_backlog fs.file-max vm.swappiness vm.max_map_count \
  net.ipv4.tcp_congestion_control net.core.default_qdisc; do
  path=/proc/sys/${key//.//}
  [[ -r $path ]] || continue
  tuning_json=$(jq -c --arg k "$key" --arg v "$(slurp "$path")" '. + {($k): $v}' <<<"$tuning_json")
done

limits_json=$(jq -n --arg soft "$(ulimit -Sn)" --arg hard "$(ulimit -Hn)" '{nofile_soft: $soft, nofile_hard: $hard}')

in_container=false
if [[ -f /.dockerenv ]] || grep -q -E 'docker|containerd|kubepods' /proc/1/cgroup 2>/dev/null; then in_container=true; fi
runtime_json=$(jq -n --argjson c "$in_container" --argjson s "$(have_systemd && echo true || echo false)" '{container: $c, systemd: $s}')

# ---- EC2 instance metadata (v2) ---------------------------------------------------------------------------------------
ec2_json=null
if [[ ${OXIA_BENCH_NO_IMDS:-0} != 1 ]] && command -v curl >/dev/null 2>&1; then
  token=$(curl -s -m 2 --connect-timeout 1 -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token 2>/dev/null || true)
  if [[ -n $token ]]; then
    imds() { curl -s -m 2 --connect-timeout 1 -H "X-aws-ec2-metadata-token: $token" "http://169.254.169.254/latest/meta-data/$1" 2>/dev/null || true; }
    ec2_json=$(jq -n --arg t "$(imds instance-type)" --arg ami "$(imds ami-id)" --arg az "$(imds placement/availability-zone)" \
      --arg region "$(imds placement/region)" '{instance_type: $t, ami_id: $ami, availability_zone: $az, region: $region}')
  fi
fi

# ---- data volume -------------------------------------------------------------------------------------------------------
data_json=null
if [[ -d $OXIA_BENCH_DATA ]]; then
  read -r df_dev df_size df_mount < <(df -P -B1G "$OXIA_BENCH_DATA" | awk 'NR == 2 { print $1, $2, $6 }') || true
  fstype=$(findmnt -n -o FSTYPE --target "$OXIA_BENCH_DATA" 2>/dev/null || true)
  options=$(findmnt -n -o OPTIONS --target "$OXIA_BENCH_DATA" 2>/dev/null | cut -c1-160 || true)
  data_json=$(jq -n --arg mount "${df_mount:-}" --arg dev "${df_dev:-}" --arg fs "$fstype" --arg opt "$options" --arg size "${df_size:-0}" \
    '{mount: $mount, device: $dev, fstype: $fs, options: $opt, size_gb: ($size | tonumber? // null)}')
fi

# ---- software ----------------------------------------------------------------------------------------------------------
oxia_version=""
oxia_sha=""
if oxia=$(find_bin oxia); then
  oxia_version=$("$oxia" --version 2>&1 | head -n1 || true)
  oxia_sha=$(sha "$oxia")
fi
dotnet_sha="" go_sha="" versions_json=null
if p=$(find_bin oxia-load-dotnet); then dotnet_sha=$(sha "$p"); fi
if p=$(find_bin oxia-load-go); then go_sha=$(sha "$p"); fi
if p=$(find_bin VERSIONS.json); then versions_json=$(jq -c 'del(.files)' "$p" 2>/dev/null || echo null); fi
software_json=$(jq -n --arg ov "$oxia_version" --arg os "$oxia_sha" --arg ds "$dotnet_sha" --arg gs "$go_sha" --argjson versions "$versions_json" \
  '{oxia: {version: $ov, sha256: $os}, oxia_load_dotnet: {sha256: $ds}, oxia_load_go: {sha256: $gs}, artifacts: $versions}')

node_json=null
if [[ -f $OXIA_BENCH_STATE/node.json ]]; then node_json=$(jq -c . "$OXIA_BENCH_STATE/node.json" 2>/dev/null || echo null); fi

jq -nc --arg utc "$(iso_utc)" --argjson os "$os_json" --argjson cpu "$cpu_json" --argjson memory "$mem_json" \
  --argjson tuning "$tuning_json" --argjson limits "$limits_json" --argjson runtime "$runtime_json" --argjson ec2 "$ec2_json" \
  --argjson data "$data_json" --argjson software "$software_json" --argjson node "$node_json" \
  '{schema: 1, collected_utc: $utc, role: ($node.role // "unknown"), os: $os, cpu: $cpu, memory: $memory, kernel_tuning: $tuning, limits: $limits,
    runtime: $runtime, ec2: $ec2, data_volume: $data, software: $software, node: $node}'

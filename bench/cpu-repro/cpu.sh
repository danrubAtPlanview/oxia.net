#!/bin/bash
# Local repro of the AWS CPU-per-operation finding (docs/performance.md section 11): one fresh Oxia standalone container + one Amazon Linux
# client container on the same Docker network, running the same Linux load-generator binaries as the AWS benchmark (bench/aws/.artifacts/x64,
# built by `pwsh bench/aws/scripts/Publish-Artifacts.ps1`).
# usage: cpu.sh setup                       (client container is --privileged so that perf works; CLIENT_CPUS limits it, default 8)
#        cpu.sh run <dotnet|go> <A|B|C|D|F|G> [--tune k=v,...] [-- ENV=VAL ...]   prints the RESULT line (cpu_us_per_op, ops_per_s, ...)
#        env: DURATION (default 15s), WARMUP (3s), ART (artifact directory)
# Scenarios are those of bench/aws/suites/standard.json (128-byte values, 100 000 keys, read percent 50).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE/../.."
ART="${ART:-bench/aws/.artifacts/x64}"
NET=oxia-cpu
SRV=oxia-cpu-server
CLI=oxia-cpu-client

fresh_server() {
  docker rm -f $SRV >/dev/null 2>&1 || true
  docker run -d --name $SRV --network $NET oxia/oxia:nightly-beta oxia standalone --shards "${SHARDS:-1}" --public-addr 0.0.0.0:6648 --wal-sync-data=false >/dev/null
  for i in $(seq 1 60); do
    docker exec $SRV oxia health --host 127.0.0.1 --port 6648 >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "server did not become healthy" >&2; exit 1
}

case "${1:-}" in
  setup)
    docker network inspect $NET >/dev/null 2>&1 || docker network create $NET >/dev/null
    docker rm -f $CLI >/dev/null 2>&1 || true
    docker run -d --name $CLI --network $NET --privileged --cpus "${CLIENT_CPUS:-8}" amazonlinux:2023 sleep infinity >/dev/null
    docker cp $ART/oxia-load-dotnet $CLI:/usr/local/bin/oxia-load-dotnet
    docker cp $ART/oxia-load-go $CLI:/usr/local/bin/oxia-load-go
    docker exec $CLI bash -c 'chmod +x /usr/local/bin/oxia-load-dotnet /usr/local/bin/oxia-load-go'
    docker exec $CLI bash -c 'dnf install -y -q procps-ng perf >/dev/null 2>&1 || true'
    fresh_server
    echo "setup done"
    ;;
  run)
    impl=$2; sc=$3; shift 3
    tune=""
    if [ "${1:-}" = "--tune" ]; then tune=$2; shift 2; fi
    [ "${1:-}" = "--" ] && shift
    envs=()
    for e in "$@"; do envs+=(-e "$e"); done
    producers=256; linger=5ms; client=async; rate=""; ops=put,get
    case $sc in
      A) ;;
      B) producers=1000; ops=put ;;
      D) producers=1000; linger=1ms; export SHARDS=4 ;;
      C) client=sync; producers=64 ;;
      F) producers=64; linger=0ms ;;
      G) rate=20000 ;;
      *) echo "unknown scenario $sc" >&2; exit 2 ;;
    esac
    dur=${DURATION:-15s}; warm=${WARMUP:-3s}
    fresh_server
    if [ "$impl" = dotnet ]; then
      cmd=(oxia-load-dotnet load --server $SRV:6648 --client $client --ops $ops --read-percent 50 --keys 100000 --value-size 128 --producers $producers
           --duration "$dur" --warmup "$warm" --preload=true --linger $linger --max-requests-per-batch 1000)
      [ -n "$rate" ] && cmd+=(--rate $rate)
      [ -n "$tune" ] && cmd+=(--tune "$tune")
      envs+=(-e DOTNET_BUNDLE_EXTRACT_BASE_DIR=/tmp/dn -e HOME=/root -e DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1)
    else
      cmd=(oxia-load-go -addr $SRV:6648 -client $client -ops $ops -read-percent 50 -keys 100000 -value-size 128 -producers $producers
           -duration "$dur" -warmup "$warm" -preload=true -linger $linger -max-requests-per-batch 1000)
      [ -n "$rate" ] && cmd+=(-rate $rate)
    fi
    docker exec "${envs[@]}" $CLI "${cmd[@]}" 2>&1 | grep -E "^RESULT|rror|xception" | head -5
    ;;
  *) echo "usage: see header" >&2; exit 2 ;;
esac

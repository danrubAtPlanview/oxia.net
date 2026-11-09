#!/bin/sh
# Removes the reusable Docker containers created by the Oxia .NET test fixture (label oxia.net.tests=1).
# POSIX twin of clean-oxia-containers.ps1 (Linux / macOS / CI), same behaviour and options:
#
#   clean-oxia-containers.sh [--list] [--image]
#
#   --list    only list the containers, do not remove them
#   --image   also remove the server image(s) those containers were created from (only if no other container uses them)
#
# tests/Oxia.TestSupport/OxiaServerFixture keeps its container after a test run so that the next run reuses it. Run this to free the
# resources, to pick up a new server image (after `docker pull`), or after changing OXIA_TEST_SLOTS / OXIA_TEST_IMAGE.
# Only containers carrying the label oxia.net.tests=1 are touched -- nothing else on the machine.
set -eu

list=0
image=0
for arg in "$@"; do
  case "$arg" in
    --list|-l) list=1 ;;
    --image|-i) image=1 ;;
    -h|--help) sed -n '2,13p' "$0"; exit 0 ;;
    *) echo "unknown option: $arg" >&2; exit 2 ;;
  esac
done

if ! command -v docker >/dev/null 2>&1; then
  echo 'docker was not found on PATH' >&2
  exit 1
fi

rows=$(docker ps -a --filter 'label=oxia.net.tests=1' --format '{{.ID}}|{{.Names}}|{{.Status}}|{{.Label "oxia.net.tests.image"}}')
if [ -z "$rows" ]; then
  echo 'No Oxia test containers found.'
  exit 0
fi

images=""
# a here-document keeps the loop in this shell (a pipe would lose $images)
while IFS='|' read -r id name status img; do
  printf '%-34s %-28s %s\n' "$name" "$status" "$img"
  [ -n "$img" ] && images="$images
$img"
  if [ "$list" -eq 0 ]; then
    docker rm -f "$id" >/dev/null
    echo "  removed $name"
  fi
done <<EOF
$rows
EOF

if [ "$image" -eq 1 ] && [ "$list" -eq 0 ]; then
  echo "$images" | sort -u | while read -r img; do
    [ -n "$img" ] && docker rmi "$img" 2>&1 | sed 's/^/  /'
  done
fi

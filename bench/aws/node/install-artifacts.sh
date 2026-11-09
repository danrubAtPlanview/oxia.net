#!/usr/bin/env bash
# install-artifacts.sh SRC_DIR [DEST_DIR]   - installs the output of Publish-Artifacts.ps1 on a node.
#
# SRC_DIR is the artifact directory as downloaded (s3://<bucket>/artifacts/ or `docker cp`): oxia, oxia-load-dotnet, oxia-load-go,
# ld-musl-*.so.1, VERSIONS.json, SHA256SUMS and node/*.sh.  Everything is copied FLAT into DEST_DIR (default /opt/oxia-bench/bin), so the
# scripts sit next to the binaries, and made executable.  When SRC_DIR/SHA256SUMS exists every file is verified first (sha256sum -c);
# a mismatch aborts before anything is installed.  The last line of stdout is DEST_DIR.
#
# This is the one step both the AWS orchestrator and Rehearse.ps1 run between "files arrived on the node" and `bootstrap-node.sh`:
#     bash <SRC_DIR>/node/install-artifacts.sh <SRC_DIR>  &&  /opt/oxia-bench/bin/bootstrap-node.sh server|client
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$here/lib.sh"

case ${1:-} in
  "" | -h | --help) usage_from_header "${BASH_SOURCE[0]}"; exit 0 ;;
esac
src=$(cd "$1" && pwd) || die "SRC_DIR '$1' does not exist"
dest=${2:-$OXIA_BENCH_BIN}
require_root

if [[ -f $src/SHA256SUMS ]]; then
  (cd "$src" && sha256sum --quiet -c SHA256SUMS) >&2 || die "checksum mismatch in $src (SHA256SUMS): the download is corrupt or incomplete"
  log "checksums verified ($(wc -l <"$src/SHA256SUMS") files)"
else
  warn "no SHA256SUMS in $src: files are not verified"
fi

mkdir -p "$dest"
n=0
for f in oxia oxia-load-dotnet oxia-load-go VERSIONS.json SHA256SUMS "$src"/ld-musl-*.so.1 "$src"/node/*.sh; do
  [[ $f == /* ]] || f=$src/$f
  [[ -f $f ]] || continue
  case $f in
    *.json | */SHA256SUMS) install -m 0644 "$f" "$dest/$(basename "$f")" ;;
    *) install -m 0755 "$f" "$dest/$(basename "$f")" ;;
  esac
  n=$((n + 1))
done
((n > 0)) || die "nothing to install in $src"
log "installed $n files into $dest"
printf '%s\n' "$dest"

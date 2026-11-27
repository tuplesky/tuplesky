#!/usr/bin/env bash
# Install Ubuntu packages on a GitHub runner: apt_install.sh PACKAGE...
#
# The runner's apt reads mirror+file:/etc/apt/apt-mirrors.txt, whose first
# entry is Azure's mirror. When that name stops resolving, apt-get update only
# warns and keeps the image's stale indexes, and apt-get install then spends
# minutes failing every download. So: try the runner's sources once, and if
# an index or a download fails, try again with archive.ubuntu.com (or
# ports.ubuntu.com) and security.ubuntu.com named directly, from a sources
# list of our own; the runner's own configuration is left as it is.
set -uo pipefail

[ $# -gt 0 ] || { echo "usage: $0 PACKAGE..." >&2; exit 2; }

opts=(-o Acquire::Retries=2 -o Acquire::http::Timeout=30 -o Acquire::https::Timeout=30)

attempt() {
  local log
  log=$(mktemp)
  # apt-get update exits 0 when an index cannot be fetched; its warnings say so.
  if ! sudo apt-get "${opts[@]}" "$@" update -qq 2>&1 | tee "$log"; then
    rm -f "$log"; return 1
  fi
  if grep -qE '^(W|E): (Failed to fetch|Some index files failed)' "$log"; then
    rm -f "$log"; return 1
  fi
  rm -f "$log"
  sudo DEBIAN_FRONTEND=noninteractive apt-get "${opts[@]}" "$@" \
    install -y -qq --no-install-recommends "${packages[@]}"
}

packages=("$@")
attempt && exit 0

echo "::warning::apt could not use the runner's mirrors; trying the Ubuntu archive directly"
. /etc/os-release
codename=${VERSION_CODENAME:?no VERSION_CODENAME in /etc/os-release}
case "$(dpkg --print-architecture)" in
  amd64 | i386) main=http://archive.ubuntu.com/ubuntu security=http://security.ubuntu.com/ubuntu ;;
  *) main=http://ports.ubuntu.com/ubuntu-ports security=http://ports.ubuntu.com/ubuntu-ports ;;
esac
dir=$(mktemp -d)
mkdir "$dir/parts"
cat > "$dir/sources.list" <<EOF
deb $main $codename main restricted universe
deb $main $codename-updates main restricted universe
deb $security $codename-security main restricted universe
EOF
attempt -o "Dir::Etc::SourceList=$dir/sources.list" -o "Dir::Etc::SourceParts=$dir/parts"
status=$?
rm -rf "$dir"
exit $status

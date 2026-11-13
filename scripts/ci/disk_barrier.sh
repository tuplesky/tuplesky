#!/usr/bin/env bash
# store disk-barrier: turn ext4's write barriers on for the filesystem under
# Docker's data root, where both systems' stores sit (each container's
# writable layer), before the cluster starts.
#
# The hosted runner mounts its root ext4 nobarrier: an fsync or fdatasync
# then sends the device no cache flush (and no FUA write), so a write is
# durable to the device's write-back cache only. With barrier on, ext4 sends
# the flush, and the device's answer is what an fsync waits for. Whether the
# virtual device carries that flush through to stable media is the device's
# business; scripts/ci/disk_probe.py's fdatasync latency, before and after,
# is how a reader judges it.
#
# Prints the mount before and after, and fails the job if the remount did not
# take, so a run labelled disk-barrier never ran without barriers.
set -euo pipefail

root=$(docker info -f '{{.DockerRootDir}}' 2>/dev/null || echo /var/lib/docker)
target=$(findmnt -no TARGET -T "$root")
fstype=$(findmnt -no FSTYPE -T "$root")
echo "Docker's data root $root is on $target ($fstype): $(findmnt -no OPTIONS -T "$root")"
if [ "$fstype" != ext4 ]; then
  echo "::error::store disk-barrier remounts ext4 with barrier; $target is $fstype"
  exit 1
fi
sudo mount -o remount,barrier "$target"
after=$(findmnt -no OPTIONS -T "$root")
echo "After the remount: $after"
case ",$after," in
  *,nobarrier,* | *,barrier=0,*)
    echo "::error::$target is still mounted without barriers after the remount: $after"
    exit 1
    ;;
esac
echo "Write barriers are on for $target"

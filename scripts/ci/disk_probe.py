"""Describe the disk under a directory and time its fdatasync.

usage: python3 disk_probe.py DIR [--count N] [--size BYTES]

For `store: disk`, the voters' and etcd's stores sit in the containers'
writable layer, which Docker keeps under its data root, so DIR is a directory
there. Prints, one line each:

- the filesystem DIR is on (`findmnt -T`): its source, type and options;
- the block device under it (`lsblk`): type, model, transport, rotational,
  and its write cache (`/sys/block/<dev>/queue/write_cache`: "write back"
  means the device acknowledges before the media holds the write unless a
  flush is sent, which fdatasync does; "write through" means it doesn't
  cache);
- the latency of N appends of SIZE bytes, each followed by an fdatasync of
  the file, as p50 / p99 / max and the mean, in ms. An fdatasync that the
  device really flushes takes a fraction of a millisecond or more; one in
  microseconds is not reaching stable media.

The probe file is removed afterwards. Run it before the cluster starts, so the
disk is otherwise idle.
"""

import argparse
import os
import subprocess
import time


def run(*cmd):
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=30).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return ""


def parent_disk(source):
    """The whole disk under a partition or mapped device, as lsblk names it."""
    name = run("lsblk", "-no", "PKNAME", source).splitlines()
    name = name[-1].strip() if name else ""
    if not name:
        name = os.path.basename(source)
    # Device-mapper and the like: walk up until a disk.
    for _ in range(4):
        kind = run("lsblk", "-dno", "TYPE", f"/dev/{name}")
        if kind == "disk" or not kind:
            break
        up = run("lsblk", "-no", "PKNAME", f"/dev/{name}").splitlines()
        up = up[-1].strip() if up else ""
        if not up:
            break
        name = up
    return name


def read(path):
    try:
        with open(path) as f:
            return f.read().strip()
    except OSError:
        return "?"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("dir")
    p.add_argument("--count", type=int, default=1000)
    p.add_argument("--size", type=int, default=4096)
    args = p.parse_args()
    os.makedirs(args.dir, exist_ok=True)

    model = "?"
    try:
        with open("/proc/cpuinfo") as f:
            for line in f:
                if line.startswith("model name"):
                    model = line.split(":", 1)[1].strip()
                    break
    except OSError:
        pass
    print(f"CPU model: {model}")
    fs = run("findmnt", "-no", "SOURCE,FSTYPE,OPTIONS", "-T", args.dir)
    print(f"Filesystem under {args.dir}: {fs or '?'}")
    source = fs.split()[0] if fs else ""
    disk = parent_disk(source) if source.startswith("/dev/") else ""
    if disk:
        info = run("lsblk", "-dno", "TYPE,SIZE,MODEL,TRAN,ROTA", f"/dev/{disk}")
        cache = read(f"/sys/block/{disk}/queue/write_cache")
        fua = read(f"/sys/block/{disk}/queue/fua")
        print(f"Device: /dev/{disk} ({' '.join(info.split()) or '?'}; rotational last); "
              f"write cache: {cache}; FUA: {fua}")
    else:
        print(f"Device: not a block device ({source or '?'})")

    path = os.path.join(args.dir, f"fsync-probe-{os.getpid()}")
    block = b"\0" * args.size
    took = []
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        for _ in range(args.count):
            start = time.perf_counter()
            os.write(fd, block)
            os.fdatasync(fd)
            took.append((time.perf_counter() - start) * 1000.0)
    finally:
        os.close(fd)
        os.unlink(path)
    took.sort()

    def at(q):
        return took[min(len(took) - 1, int(q * len(took)))]

    mean = sum(took) / len(took)
    print(f"fdatasync after a {args.size}-byte append, {args.count} times: "
          f"p50 {at(0.50):.3f} / p99 {at(0.99):.3f} / max {took[-1]:.3f} ms, mean {mean:.3f} ms")


if __name__ == "__main__":
    main()

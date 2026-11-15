#!/usr/bin/env python3
"""Install the checksummed tools from tools/manifest.toml for this platform.

Binary tools are downloaded from their pinned release URLs, verified against
the manifest SHA-256 before extraction and installed into ``--dest``
(default ``~/.cargo/bin``). npm tools are installed with ``npm ci`` from the
committed lockfile, whose integrity hashes pin the packages.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import os
import platform
import stat
import subprocess
import sys
import tarfile
import tomllib
import urllib.request
from pathlib import Path


def current_platform() -> str:
    machine = platform.machine().lower()
    arch = {"x86_64": "x86_64", "amd64": "x86_64", "aarch64": "aarch64", "arm64": "aarch64"}.get(machine)
    system = platform.system().lower()
    if arch is None or system != "linux":
        raise SystemExit(f"unsupported platform {system}/{machine}; the manifest covers Linux x86_64/aarch64")
    return f"{arch}-unknown-linux"


def fetch(url: str, cache: Path) -> bytes:
    cache.mkdir(parents=True, exist_ok=True)
    cached = cache / hashlib.sha256(url.encode()).hexdigest()
    if cached.exists():
        return cached.read_bytes()
    with urllib.request.urlopen(url, timeout=120) as resp:  # noqa: S310 - pinned https release URL
        data = resp.read()
    cached.write_bytes(data)
    return data


def install_binary(tool: dict, plat: str, dest: Path, cache: Path) -> None:
    downloads = [d for d in tool.get("download", []) if d["platform"] == plat]
    if not downloads:
        raise SystemExit(f"{tool['name']}: no download for platform {plat}")
    d = downloads[0]
    data = fetch(d["url"], cache)
    digest = hashlib.sha256(data).hexdigest()
    if digest != d["sha256"]:
        raise SystemExit(f"{tool['name']}: checksum mismatch for {d['url']}: expected {d['sha256']}, got {digest}")
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tar:
        member = tar.getmember(d["member"])
        if not member.isfile():
            raise SystemExit(f"{tool['name']}: archive member {d['member']} is not a regular file")
        extracted = tar.extractfile(member)
        assert extracted is not None
        dest.mkdir(parents=True, exist_ok=True)
        target = dest / Path(d["member"]).name
        target.write_bytes(extracted.read())
        target.chmod(target.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    print(f"installed {tool['name']} {tool['version']} -> {target}")


def install_npm(tool: dict, root: Path) -> None:
    lockfile = root / tool["lockfile"]
    prefix = lockfile.parent
    env = dict(os.environ, PUPPETEER_SKIP_DOWNLOAD="1", PUPPETEER_SKIP_CHROMIUM_DOWNLOAD="1")
    subprocess.run(["npm", "ci", "--no-audit", "--no-fund", "--prefix", str(prefix)], check=True, env=env)
    print(f"installed {tool['name']} {tool['version']} via npm ci in {prefix}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", default="tools/manifest.toml")
    parser.add_argument("--dest", default=str(Path.home() / ".cargo" / "bin"))
    parser.add_argument("--cache", default="tools/downloads")
    parser.add_argument("--only", action="append", help="tool name(s) to install; default all")
    parser.add_argument("--skip-npm", action="store_true")
    args = parser.parse_args()
    root = Path(args.manifest).resolve().parents[1]
    manifest = tomllib.loads(Path(args.manifest).read_text(encoding="utf-8"))
    if manifest.get("version") != 1:
        raise SystemExit("unsupported manifest version")
    plat = current_platform()
    for tool in manifest["tool"]:
        if args.only and tool["name"] not in args.only:
            continue
        if tool.get("kind") == "npm":
            if not args.skip_npm:
                install_npm(tool, root)
            continue
        install_binary(tool, plat, Path(args.dest), Path(args.cache))
    return 0


if __name__ == "__main__":
    sys.exit(main())

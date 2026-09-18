#!/usr/bin/env python3
"""Classify a Git change set as documentation-only or code-bearing.

This is the repository-owned change filter required by design Section 12.4.1
and task-01. It is deliberately conservative:

* Only paths matching the reviewed allowlist in ``docs-only-policy.json`` are
  documentation. Unknown paths, type changes, unmerged entries and renames
  whose old *or* new side is not documentation all require heavy CI.
* The complete change set comes from ``git diff --name-status -z`` over the
  correct comparison for the event (merge-base for pull requests and merge
  groups, before/after for pushes). Truncated API pages are never consulted.
* An empty or unknown comparison, missing history, unsupported event or Git
  failure never yields a documentation-only decision. The script tries to
  recover history (explicit fetch, unshallow) and otherwise selects full CI.
* Event data (SHAs, event names, paths) is passed as arguments and data;
  nothing is interpolated into a shell.

Outputs are ``key=value`` lines (``docs_only``, ``reason``, ``changed_files``)
written to ``--output`` (typically ``$GITHUB_OUTPUT``) and echoed to stdout.
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

ZERO_SHA_RE = re.compile(r"^0{40}$|^0{64}$")
SHA_RE = re.compile(r"^[0-9a-f]{40}$|^[0-9a-f]{64}$")
DOCS_STATUSES = {"A", "M", "D", "R"}  # statuses that can be documentation-only


@dataclass(frozen=True)
class Change:
    """One entry of ``git diff --name-status``."""

    status: str  # single letter: A M D T R C U X B
    path: str  # new path (or the only path)
    old_path: str | None = None  # for renames/copies


@dataclass(frozen=True)
class Decision:
    docs_only: bool
    reason: str
    changed_files: int

    def lines(self) -> list[str]:
        return [
            f"docs_only={'true' if self.docs_only else 'false'}",
            f"reason={self.reason}",
            f"changed_files={self.changed_files}",
        ]


class Policy:
    def __init__(self, patterns: Sequence[str]):
        if not patterns:
            raise ValueError("policy has no documentation_paths")
        self._patterns = [re.compile(p) for p in patterns]

    @classmethod
    def load(cls, path: Path) -> "Policy":
        data = json.loads(path.read_text(encoding="utf-8"))
        if data.get("version") != 1:
            raise ValueError(f"unsupported policy version {data.get('version')!r}")
        return cls(data["documentation_paths"])

    def is_documentation(self, path: str) -> bool:
        if not path or path.startswith("/") or ".." in path.split("/") or "\n" in path:
            return False
        return any(p.match(path) for p in self._patterns)


def parse_name_status_z(raw: bytes) -> list[Change]:
    """Parse NUL-separated ``git diff --name-status -z`` output."""
    fields = raw.split(b"\0")
    if fields and fields[-1] == b"":
        fields.pop()
    changes: list[Change] = []
    i = 0
    while i < len(fields):
        status_field = fields[i].decode("utf-8", errors="surrogateescape")
        if not status_field:
            raise ValueError("empty status field in git output")
        letter = status_field[0]
        if letter in ("R", "C"):
            if i + 2 >= len(fields):
                raise ValueError("truncated rename/copy entry in git output")
            old = fields[i + 1].decode("utf-8", errors="surrogateescape")
            new = fields[i + 2].decode("utf-8", errors="surrogateescape")
            changes.append(Change(letter, new, old))
            i += 3
        else:
            if i + 1 >= len(fields):
                raise ValueError("truncated entry in git output")
            path = fields[i + 1].decode("utf-8", errors="surrogateescape")
            changes.append(Change(letter, path))
            i += 2
    return changes


def classify(changes: Iterable[Change], policy: Policy) -> Decision:
    """Pure classification of an already-computed change set."""
    changes = list(changes)
    if not changes:
        return Decision(False, "empty-change-set", 0)
    for change in changes:
        if change.status not in DOCS_STATUSES:
            return Decision(False, f"non-documentation-status:{change.status}:{change.path}", len(changes))
        if not policy.is_documentation(change.path):
            return Decision(False, f"non-documentation-path:{change.path}", len(changes))
        if change.old_path is not None and not policy.is_documentation(change.old_path):
            return Decision(False, f"non-documentation-rename-source:{change.old_path}", len(changes))
    return Decision(True, "all-paths-are-reviewed-documentation", len(changes))


class Git:
    def __init__(self, repo: Path):
        self.repo = repo

    def run(self, *args: str, check: bool = True) -> subprocess.CompletedProcess[bytes]:
        return subprocess.run(
            ["git", "-C", str(self.repo), *args],
            check=check,
            capture_output=True,
        )

    def object_exists(self, sha: str) -> bool:
        return self.run("cat-file", "-e", f"{sha}^{{commit}}", check=False).returncode == 0

    def is_shallow(self) -> bool:
        out = self.run("rev-parse", "--is-shallow-repository", check=False)
        return out.returncode == 0 and out.stdout.strip() == b"true"

    def try_recover_history(self, shas: Sequence[str], remote: str = "origin") -> None:
        """Best-effort fetch of the exact commits and a bounded unshallow."""
        if self.is_shallow():
            self.run("fetch", "--no-tags", "--unshallow", remote, check=False)
        for sha in shas:
            if not self.object_exists(sha):
                self.run("fetch", "--no-tags", remote, sha, check=False)

    def merge_base(self, a: str, b: str) -> str | None:
        out = self.run("merge-base", a, b, check=False)
        if out.returncode != 0:
            return None
        return out.stdout.decode().strip() or None

    def name_status(self, base: str, head: str) -> list[Change]:
        out = self.run(
            "diff", "--name-status", "-z", "--find-renames", "--no-color", base, head
        )
        return parse_name_status_z(out.stdout)


def comparison_for_event(event: str, base: str | None, head: str | None, before: str | None):
    """Return ("range", base, head) or ("full", reason)."""
    if event in ("schedule", "workflow_dispatch"):
        return ("full", "manual-or-scheduled-full-run")
    if event in ("pull_request", "merge_group"):
        if not (base and head and SHA_RE.match(base) and SHA_RE.match(head)):
            return ("full", f"missing-comparison-for-{event}")
        return ("merge-base", base, head)
    if event == "push":
        if not (before and head and SHA_RE.match(before) and SHA_RE.match(head)):
            return ("full", "missing-comparison-for-push")
        if ZERO_SHA_RE.match(before):
            return ("full", "push-without-before-revision")
        return ("range", before, head)
    return ("full", f"unsupported-event:{event or 'none'}")


def decide(args: argparse.Namespace, policy: Policy) -> Decision:
    plan = comparison_for_event(args.event_name, args.base, args.head, args.before)
    if plan[0] == "full":
        return Decision(False, plan[1], 0)
    git = Git(Path(args.repo))
    mode, left, right = plan
    git.try_recover_history([left, right])
    if not git.object_exists(left) or not git.object_exists(right):
        return Decision(False, "history-unavailable", 0)
    if mode == "merge-base":
        mb = git.merge_base(left, right)
        if mb is None:
            return Decision(False, "merge-base-unavailable", 0)
        left = mb
    try:
        changes = git.name_status(left, right)
    except (subprocess.CalledProcessError, ValueError) as exc:
        return Decision(False, f"git-diff-failed:{type(exc).__name__}", 0)
    return classify(changes, policy)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--event-name", required=True)
    parser.add_argument("--base", help="pull_request/merge_group base SHA")
    parser.add_argument("--head", help="head SHA (PR head, merge-group head or push after)")
    parser.add_argument("--before", help="push before SHA")
    parser.add_argument("--repo", default=".")
    parser.add_argument("--policy", default=str(Path(__file__).with_name("docs-only-policy.json")))
    parser.add_argument("--output", help="file to append key=value lines to (e.g. $GITHUB_OUTPUT)")
    args = parser.parse_args(argv)

    try:
        policy = Policy.load(Path(args.policy))
    except (OSError, ValueError, KeyError) as exc:
        print(f"::error::cannot load docs-only policy: {exc}", file=sys.stderr)
        return 2
    decision = decide(args, policy)
    lines = decision.lines()
    if args.output:
        with open(args.output, "a", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Documentation checks (task-01): links, task IDs/anchors, task graph, Mermaid.

Runs without the Rust toolchain so documentation-only changes can be checked
by the lightweight CI job. Checks:

* Every relative Markdown link resolves to an existing file, and every
  ``#anchor`` resolves to an explicit ``<a id="...">`` or a heading slug.
* Every ``task-*`` identifier mentioned anywhere in the design package refers
  to a task specified in the plan (or to an explicitly retired identifier).
* The plan's review index and task specifications agree: same task set, same
  titles, same direct prerequisites; prerequisites exist; the graph is a DAG;
  the stated task count matches; ``**Design:**`` section references resolve
  to anchors in the design document.
* Every ```mermaid`` block has a known diagram type, balanced quotes and, in
  sequence diagrams, no unescaped ``;`` inside messages (Section 12.4). With
  ``--render``, each block is also rendered with the pinned mermaid-cli.
"""
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

LINK_RE = re.compile(r"(?<!\!)\[([^\]]*)\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
ANCHOR_RE = re.compile(r"<a\s+id=\"([^\"]+)\"\s*></a>")
HEADING_RE = re.compile(r"^(#{1,6})\s+(.*?)\s*#*\s*$")
TASK_ID_RE = re.compile(r"\btask-(?:[a-z]\d{2}|\d{2})\b")
INDEX_ROW_RE = re.compile(r"^\|\s*\[(task-[a-z0-9]+)\]\(#(task-[a-z0-9]+)\)\s*\|\s*(.*?)\s*\|\s*(.*?)\s*\|\s*$")
SPEC_HEADING_RE = re.compile(r"^###\s+(task-[a-z0-9]+):\s+(.*?)\s*$")
PREREQ_RE = re.compile(r"^\*\*Prerequisites:\*\*\s*(.*?)\.?\s*$")
DESIGN_REF_RE = re.compile(r"^\*\*Design:\*\*\s*Sections?\s+(.*?)\.?\s*$")
SECTION_NUM_RE = re.compile(r"\b\d+(?:\.\d+)*\b")
TASK_COUNT_RE = re.compile(r"\*\*Scope:\*\*\s*(\d+)\s+implementation tasks")
MERMAID_TYPES = (
    "flowchart", "graph", "sequenceDiagram", "stateDiagram", "stateDiagram-v2", "classDiagram",
    "erDiagram", "gantt", "pie", "journey", "gitGraph", "mindmap", "timeline", "quadrantChart",
    "requirementDiagram", "C4Context", "xychart-beta", "block-beta", "sankey-beta",
)
RETIRED_TASKS = {"task-s05", "task-s06", "task-s07", "task-s08"}
SKIP_DIRS = {".git", "target", "node_modules", ".cargo"}


class Report:
    def __init__(self) -> None:
        self.errors: list[str] = []
        self.checked_links = 0
        self.checked_mermaid = 0

    def error(self, path: Path, line: int | None, message: str) -> None:
        where = f"{path}:{line}" if line else str(path)
        self.errors.append(f"{where}: {message}")


def github_slug(text: str) -> str:
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)
    text = text.strip().lower()
    text = re.sub(r"[^\w\- ]", "", text)
    return text.replace(" ", "-")


def markdown_files(root: Path) -> list[Path]:
    files = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for name in filenames:
            if name.endswith(".md"):
                files.append(Path(dirpath) / name)
    return sorted(files)


def anchors_of(text: str) -> set[str]:
    anchors = set(ANCHOR_RE.findall(text))
    in_fence = False
    for line in text.splitlines():
        if line.startswith("```"):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        m = HEADING_RE.match(line)
        if m:
            anchors.add(github_slug(m.group(2)))
    return anchors


def check_links(root: Path, files: list[Path], report: Report) -> None:
    cache: dict[Path, set[str]] = {}
    for path in files:
        text = path.read_text(encoding="utf-8")
        in_fence = False
        for lineno, line in enumerate(text.splitlines(), 1):
            if line.startswith("```"):
                in_fence = not in_fence
                continue
            if in_fence:
                continue
            for _label, target in LINK_RE.findall(line):
                report.checked_links += 1
                if target.startswith(("http://", "https://", "mailto:")):
                    if " " in target:
                        report.error(path, lineno, f"malformed external link {target!r}")
                    continue
                if target.startswith("#"):
                    file_part, anchor = "", target[1:]
                else:
                    file_part, _, anchor = target.partition("#")
                dest = path if not file_part else (path.parent / file_part).resolve()
                if not dest.exists():
                    report.error(path, lineno, f"broken link target {target!r}")
                    continue
                if anchor:
                    if dest not in cache:
                        cache[dest] = anchors_of(dest.read_text(encoding="utf-8")) if dest.suffix == ".md" else set()
                    if anchor not in cache[dest]:
                        report.error(path, lineno, f"missing anchor {target!r}")


def parse_plan(plan_path: Path, report: Report):
    text = plan_path.read_text(encoding="utf-8")
    lines = text.splitlines()
    index: dict[str, tuple[str, list[str]]] = {}
    for lineno, line in enumerate(lines, 1):
        m = INDEX_ROW_RE.match(line)
        if not m:
            continue
        task, anchor, title, prereqs = m.groups()
        if task != anchor:
            report.error(plan_path, lineno, f"index row {task} links to #{anchor}")
        prereq_ids = [] if prereqs.strip().lower() == "none" else TASK_ID_RE.findall(prereqs)
        if prereqs.strip().lower() != "none" and not prereq_ids:
            report.error(plan_path, lineno, f"index row {task} has unparsable prerequisites {prereqs!r}")
        if task in index:
            report.error(plan_path, lineno, f"duplicate index row for {task}")
        index[task] = (title, prereq_ids)

    specs: dict[str, tuple[str, list[str], int]] = {}
    spec_anchors: dict[str, int] = {}
    current: str | None = None
    design_refs: list[tuple[int, str]] = []
    for lineno, line in enumerate(lines, 1):
        a = ANCHOR_RE.search(line)
        if a and a.group(1).startswith("task-"):
            spec_anchors[a.group(1)] = lineno
        m = SPEC_HEADING_RE.match(line)
        if m:
            current = m.group(1)
            if current in specs:
                report.error(plan_path, lineno, f"duplicate specification for {current}")
            specs[current] = (m.group(2), [], lineno)
            if spec_anchors.get(current) != lineno - 1:
                report.error(plan_path, lineno, f"specification {current} is not preceded by its anchor")
            continue
        p = PREREQ_RE.match(line.strip())
        if p and current:
            ids = [] if p.group(1).strip().lower() == "none" else TASK_ID_RE.findall(p.group(1))
            if p.group(1).strip().lower() != "none" and not ids:
                report.error(plan_path, lineno, f"{current} has unparsable prerequisites {p.group(1)!r}")
            title, _, at = specs[current]
            specs[current] = (title, ids, at)
        d = DESIGN_REF_RE.match(line.strip())
        if d and current:
            design_refs.append((lineno, d.group(1)))

    for task, (title, prereqs) in index.items():
        if task not in specs:
            report.error(plan_path, None, f"index lists {task} but no specification exists")
            continue
        stitle, sprereqs, at = specs[task]
        if stitle != title:
            report.error(plan_path, at, f"{task} title differs between index ({title!r}) and specification ({stitle!r})")
        if sprereqs != prereqs:
            report.error(plan_path, at, f"{task} prerequisites differ between index {prereqs} and specification {sprereqs}")
    for task, (_t, _p, at) in specs.items():
        if task not in index:
            report.error(plan_path, at, f"specification {task} is missing from the review index")

    graph = {task: prereqs for task, (_t, prereqs, _a) in specs.items()}
    for task, prereqs in graph.items():
        for p in prereqs:
            if p not in graph:
                report.error(plan_path, specs[task][2], f"{task} requires unknown task {p}")
            if p == task:
                report.error(plan_path, specs[task][2], f"{task} requires itself")
    # Cycle detection (iterative DFS).
    state: dict[str, int] = {}
    for start in graph:
        if state.get(start):
            continue
        stack = [(start, iter(graph[start]))]
        state[start] = 1
        while stack:
            node, it = stack[-1]
            nxt = next(it, None)
            if nxt is None:
                state[node] = 2
                stack.pop()
                continue
            if nxt not in graph:
                continue
            if state.get(nxt) == 1:
                report.error(plan_path, specs[node][2], f"prerequisite cycle through {node} -> {nxt}")
            elif not state.get(nxt):
                state[nxt] = 1
                stack.append((nxt, iter(graph[nxt])))

    m = TASK_COUNT_RE.search(text)
    if not m:
        report.error(plan_path, None, "cannot find the stated implementation task count")
    elif int(m.group(1)) != len(specs):
        report.error(plan_path, None, f"stated task count {m.group(1)} but {len(specs)} specifications found")
    return set(specs), design_refs


def check_task_references(files: list[Path], known: set[str], report: Report) -> None:
    allowed = known | RETIRED_TASKS
    for path in files:
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            for task in TASK_ID_RE.findall(line):
                if task not in allowed:
                    report.error(path, lineno, f"reference to unknown task {task}")


def check_design_refs(plan_path: Path, design_path: Path, refs: list[tuple[int, str]], report: Report) -> None:
    anchors = anchors_of(design_path.read_text(encoding="utf-8"))
    for lineno, spec in refs:
        for number in SECTION_NUM_RE.findall(spec):
            anchor = "s" + number.replace(".", "-")
            if anchor not in anchors:
                report.error(plan_path, lineno, f"design section {number} (anchor #{anchor}) does not exist")


def mermaid_blocks(text: str) -> list[tuple[int, str]]:
    blocks = []
    lines = text.splitlines()
    i = 0
    while i < len(lines):
        if lines[i].strip() == "```mermaid":
            start = i + 1
            j = start
            while j < len(lines) and lines[j].strip() != "```":
                j += 1
            blocks.append((start + 1, "\n".join(lines[start:j])))
            i = j + 1
        else:
            i += 1
    return blocks


def check_mermaid_syntax(path: Path, lineno: int, block: str, report: Report) -> None:
    body = [l for l in block.splitlines() if l.strip() and not l.strip().startswith("%%")]
    if not body:
        report.error(path, lineno, "empty mermaid block")
        return
    head = body[0].strip().split()[0]
    if head not in MERMAID_TYPES:
        report.error(path, lineno, f"unknown mermaid diagram type {head!r}")
        return
    for offset, line in enumerate(block.splitlines(), 0):
        if line.count('"') % 2:
            report.error(path, lineno + offset, "unbalanced double quotes in mermaid line")
        if head == "sequenceDiagram":
            unescaped = re.sub(r"#\w+;", "", line)
            if re.search(r"(->>|-->>|->|-->|-x|--x|-\)|--\))\s*[^:]*:.*;", unescaped):
                report.error(path, lineno + offset, "unescaped ';' in sequence message; use #59;")


def render_mermaid(mmdc: str, puppeteer_config: str | None, path: Path, lineno: int, block: str, report: Report) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        src = Path(tmp) / "block.mmd"
        out = Path(tmp) / "block.svg"
        src.write_text(block + "\n", encoding="utf-8")
        cmd = [mmdc, "-i", str(src), "-o", str(out), "--quiet"]
        if puppeteer_config:
            cmd += ["-p", puppeteer_config]
        # One retry. mmdc drives a headless Chrome, which occasionally
        # dies mid-render on a block that renders on every other run; a
        # block that is really malformed fails both attempts the same way.
        for attempt in (1, 2):
            proc = subprocess.run(cmd, capture_output=True, text=True)
            if proc.returncode == 0 and out.exists():
                if attempt > 1:
                    print(f"{path}:{lineno}: mermaid render passed on retry", file=sys.stderr)
                return
            out.unlink(missing_ok=True)
        detail = (proc.stderr or proc.stdout).strip().splitlines()
        report.error(path, lineno, "mermaid render failed twice: " + (detail[-1] if detail else "no output"))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[2]))
    parser.add_argument("--plan", default="docs/design/tuplesky-prs-plan.md")
    parser.add_argument("--design", default="docs/design/tuplesky-design.md")
    parser.add_argument("--render", action="store_true", help="render every mermaid block with mmdc")
    parser.add_argument("--mmdc", default="mmdc")
    parser.add_argument("--puppeteer-config")
    args = parser.parse_args(argv)

    root = Path(args.root).resolve()
    report = Report()
    files = markdown_files(root)
    check_links(root, files, report)
    plan_path = root / args.plan
    design_path = root / args.design
    known: set[str] = set()
    if plan_path.exists():
        known, refs = parse_plan(plan_path, report)
        if design_path.exists():
            check_design_refs(plan_path, design_path, refs, report)
        else:
            report.error(design_path, None, "design document missing")
    else:
        report.error(plan_path, None, "plan document missing")
    check_task_references(files, known, report)
    for path in files:
        for lineno, block in mermaid_blocks(path.read_text(encoding="utf-8")):
            report.checked_mermaid += 1
            check_mermaid_syntax(path, lineno, block, report)
            if args.render:
                render_mermaid(args.mmdc, args.puppeteer_config, path, lineno, block, report)

    for err in report.errors:
        print(f"::error::{err}")
    print(
        f"checked {len(files)} markdown files, {report.checked_links} links, "
        f"{len(known)} tasks, {report.checked_mermaid} mermaid blocks"
        + (" (rendered)" if args.render else "")
        + f": {len(report.errors)} error(s)"
    )
    return 1 if report.errors else 0


if __name__ == "__main__":
    sys.exit(main())

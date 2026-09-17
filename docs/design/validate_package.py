#!/usr/bin/env python3
"""Validate the layered documentation package, not the proposed service."""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from urllib.parse import unquote, urlsplit

from markdown_it import MarkdownIt

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parent.parent
BASE_DESIGN = ROOT / "global-coordination-rust-design-v0.5.md"
BASE_PLAN = ROOT / "global-coordination-rust-pr-plan-v1.2.md"
DESIGN = ROOT / "tuplesky-design-v0.6-update.md"
PLAN = ROOT / "tuplesky-pr-plan-v1.3-update.md"
BASE_IDS = {f"PR-{i:02d}" for i in range(1, 67)} | {
    f"PR-S{i:02d}" for i in range(1, 5)
}
TASK = r"PR-(?:[A-Z])?\d{2}"
GENERATED = {"extension-dependencies.json", "validation-report.json"}
DOCS = [BASE_DESIGN, BASE_PLAN, DESIGN, PLAN, ROOT / "README.md",
        ROOT / "swiftpaxos-upstream-review.md", REPO / "README.md"]


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def task_ids(text: str) -> list[str]:
    return re.findall(rf"\b{TASK}\b", text)


def load_plan(path: Path, baseline: bool) -> dict[str, list[str]]:
    text = path.read_text(encoding="utf-8")
    if baseline:
        rows = re.findall(
            rf"^\| \[({TASK})\]\(#[^)]+\) \| ([^|]+) \| ([^|]+) \|$",
            text, re.M,
        )
        graph = {task: list(dict.fromkeys(task_ids(deps)))
                 for task, _, deps in rows}
    else:
        rows = re.findall(
            r"^\| (PR-[JOMQ]\d{2}) \| ([^|]+) \| ([^|]+) \|$", text, re.M,
        )
        graph = {task: task_ids(deps) for task, deps, _ in rows}
    require(len(rows) == len(graph), f"Duplicate task table rows: {path.name}")
    headings = list(re.finditer(rf"^### ({TASK}):[^\n]*$", text, re.M))
    names = [m.group(1) for m in headings]
    require(len(names) == len(set(names)), f"Duplicate task headings: {path.name}")
    require(set(names) == set(graph), f"Task table/specifications differ: {path.name}")
    for index, heading in enumerate(headings):
        end = headings[index + 1].start() if index + 1 < len(headings) else len(text)
        section = text[heading.end():end]
        match = re.search(r"^\*\*Prerequisites?:\*\*([^\n]*)", section, re.M)
        require(match is not None, f"Missing prerequisites: {heading.group(1)}")
        spec_deps = list(dict.fromkeys(task_ids(match.group(1))))
        require(spec_deps == graph[heading.group(1)],
                f"Prerequisite table/spec mismatch: {heading.group(1)}")
    return graph


def topological_order(graph: dict[str, list[str]], external: set[str] | None = None) -> list[str]:
    external = external or set()
    visited: set[str] = set()
    active: list[str] = []
    order: list[str] = []

    def visit(node: str) -> None:
        if node in external or node in visited:
            return
        require(node in graph, f"Unknown dependency: {node}")
        require(node not in active, f"Dependency cycle: {' -> '.join(active + [node])}")
        active.append(node)
        for dependency in graph[node]:
            visit(dependency)
        active.pop()
        visited.add(node)
        order.append(node)

    for task in graph:
        visit(task)
    return order


def check_sources(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    defined = set(re.findall(r"\*\*\[([BSIR]\d+)\]\*\*", text))
    used: set[str] = set()
    for bracket in re.findall(r"\[([^\]\n]+)\]", text):
        prefix = bracket.split(":", 1)[0]
        if not re.fullmatch(r"[BSIR]\d+(?:-[BSIR]\d+)?(?:, *[BSIR]\d+(?:-[BSIR]\d+)?)*", prefix):
            continue
        used.update(re.findall(r"[BSIR]\d+", prefix))
        for family, start, end in re.findall(r"([BSIR])(\d+)-\1(\d+)", prefix):
            used.update(f"{family}{number}" for number in range(int(start), int(end) + 1))
    require(used <= defined, f"Undefined sources in {path.name}: {sorted(used - defined)}")


def main() -> None:
    imports = json.loads((ROOT / "baseline-imports.json").read_text(encoding="utf-8"))
    expected_names = {BASE_DESIGN.name, BASE_PLAN.name}
    require(len(imports["files"]) == 2 and
            {entry["file"] for entry in imports["files"]} == expected_names,
            "Expected exactly the two baseline imports")
    for entry in imports["files"]:
        data = (ROOT / entry["file"]).read_bytes()
        require(len(data) == entry["bytes"], f"Baseline length changed: {entry['file']}")
        require(hashlib.sha256(data).hexdigest() == entry["sha256"],
                f"Baseline bytes changed: {entry['file']}")
        blob = hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()
        require(blob == entry["git_blob_sha1"], f"Baseline Git blob changed: {entry['file']}")

    parser = MarkdownIt("commonmark").enable("table")
    report: dict[str, object] = {
        "scope": "Documentation, unchanged baseline imports, and complete 89-task dependency graph",
        "not_tested": [
            "Official Mermaid rendering",
            "Rust/Go builds and interoperability",
            "External source and dependency-version verification",
            "Distributed protocol models and service tests",
            "Real storage crash qualification and performance benchmarks",
            "Semantic correctness or completeness of the design and implementation plan",
        ],
        "documents": [],
        "baseline_imports_verified": imports["files"],
    }
    parsed: dict[Path, list] = {}
    anchors: dict[Path, set[str]] = {}
    for path in DOCS:
        text = path.read_text(encoding="utf-8")
        tokens = parser.parse(text)
        parsed[path.resolve()] = tokens
        fences = [t for t in tokens if t.type == "fence"]
        require(len(re.findall(r"^```", text, re.M)) % 2 == 0, f"Unbalanced fences: {path.name}")
        explicit = re.findall(r'<a id="([^"]+)"', text)
        require(len(explicit) == len(set(explicit)), f"Duplicate explicit anchors: {path.name}")
        anchors[path.resolve()] = set(explicit)
        for fence in fences:
            if fence.info.strip() == "mermaid":
                first = fence.content.strip().splitlines()[0]
                require(first.split()[0] in {"flowchart", "sequenceDiagram", "stateDiagram-v2"},
                        f"Unexpected Mermaid declaration: {first}")
                require(fence.content.count('"') % 2 == 0, f"Unbalanced Mermaid quotes: {path.name}")
        report["documents"].append({
            "file": str(path.relative_to(REPO)),
            "bytes": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "headings": sum(t.type == "heading_open" for t in tokens),
            "fences": len(fences),
            "mermaid_blocks_structurally_checked": sum(f.info.strip() == "mermaid" for f in fences),
        })

    for path, tokens in parsed.items():
        for token in tokens:
            for child in token.children or []:
                if child.type != "link_open":
                    continue
                target = child.attrGet("href") or ""
                parts = urlsplit(target)
                if parts.scheme or parts.netloc:
                    continue
                target_path = (path.parent / unquote(parts.path)).resolve() if parts.path else path
                require(target_path.is_relative_to(REPO), f"Link escapes repository: {target}")
                if target_path.parent == ROOT and target_path.name in GENERATED:
                    continue
                require(target_path.is_file(), f"Missing local link from {path.name}: {target}")
                if parts.fragment:
                    require(unquote(parts.fragment) in anchors.get(target_path, set()),
                            f"Missing explicit target anchor from {path.name}: {target}")

    baseline = load_plan(BASE_PLAN, baseline=True)
    extension = load_plan(PLAN, baseline=False)
    require(set(baseline) == BASE_IDS, "Expected original 70 task IDs")
    require(len(extension) == 19 and not (set(extension) & BASE_IDS), "Expected 19 new task IDs")
    base_order = topological_order(baseline)
    extension_order = topological_order(extension, BASE_IDS)
    combined = {task: list(deps) for task, deps in (baseline | extension).items()}
    # v1.3 P0/P4 explicitly extends the release gate; other table rows amend
    # behavior, not prerequisite edges. Never make a base depend on its extension.
    gate_extensions = {"PR-66": ["PR-Q01"]}
    plan_text = PLAN.read_text(encoding="utf-8")
    require(re.search(r"^\| PR-66 \| Add PR-Q01 as a release prerequisite\.", plan_text, re.M) is not None,
            "Documented PR-66 release-gate extension changed")
    for task, deps in gate_extensions.items():
        combined[task].extend(dep for dep in deps if dep not in combined[task])
    combined_order = topological_order(combined)
    require(set(task_ids(plan_text)) <= set(combined), "Unknown task reference in v1.3 plan")
    check_sources(BASE_DESIGN)
    check_sources(DESIGN)
    graph_document = {
        "base_plan": BASE_PLAN.name,
        "base_task_count": len(baseline),
        "new_task_count": len(extension),
        "combined_task_count": len(combined),
        "base_tasks": baseline,
        "new_tasks": extension,
        "optional_tasks": ["PR-J06"],
        "original_gate_extensions": gate_extensions,
        "base_topological_order": base_order,
        "extension_only_topological_order": extension_order,
        "combined_topological_order": combined_order,
        "qualification": "Both task tables match their specifications; the complete graph including PR-66 -> PR-Q01 is acyclic. This is not implementation or protocol verification.",
    }
    (ROOT / "extension-dependencies.json").write_text(json.dumps(graph_document, indent=2) + "\n", encoding="utf-8")
    report.update({
        "base_task_count": len(baseline),
        "extension_task_count": len(extension),
        "combined_task_count": len(combined),
        "combined_dependency_edges": sum(map(len, combined.values())),
        "status": "passed",
        "checks": [
            "Both complete baseline files match import length, SHA-256, and Git blob ID",
            "Markdown parsed including tables", "Fence balance", "Explicit anchors unique",
            "Local document links and explicit fragments resolve", "Source labels defined",
            "Baseline and extension task tables match specifications and prerequisites",
            "Task prerequisites resolve", "Baseline graph is acyclic",
            "Combined 89-task graph with documented release-gate extension is acyclic",
            "Mermaid declaration/quote structure only",
        ],
    })
    (ROOT / "validation-report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": "passed", "base_tasks": len(baseline),
                      "new_tasks": len(extension), "total_tasks": len(combined),
                      "combined_graph": "acyclic", "baseline_imports": "unchanged"}))


if __name__ == "__main__":
    main()

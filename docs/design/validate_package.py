#!/usr/bin/env python3
"""Validate this documentation package; this is not service qualification."""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from urllib.parse import unquote

from markdown_it import MarkdownIt

ROOT = Path(__file__).resolve().parent
BASE_IDS = {f"PR-{i:02d}" for i in range(1, 67)} | {
    f"PR-S{i:02d}" for i in range(1, 5)
}
DOCS = [
    ROOT / "tuplesky-design-v0.6-update.md",
    ROOT / "tuplesky-pr-plan-v1.3-update.md",
    ROOT / "README.md",
    ROOT / "swiftpaxos-upstream-review.md",
]


def main() -> None:
    parser = MarkdownIt("commonmark")
    report: dict[str, object] = {
        "scope": "Documentation and extension-graph validation only",
        "not_tested": [
            "Official Mermaid rendering",
            "Rust/Go builds and interoperability",
            "Distributed protocol models and service tests",
            "Real storage crash qualification",
            "Original full dependency graph, which is not reproduced in this package",
        ],
        "documents": [],
    }
    for path in DOCS:
        text = path.read_text(encoding="utf-8")
        tokens = parser.parse(text)
        fences = [token for token in tokens if token.type == "fence"]
        assert len(re.findall(r"^```", text, re.M)) % 2 == 0, path.name
        anchors = re.findall(r'<a id="([^"]+)"', text)
        assert len(anchors) == len(set(anchors)), f"Duplicate anchors: {path.name}"
        # Links to the generated report and graph are checked after creation.
        for target in re.findall(r"\]\(([^)]+)\)", text):
            if "://" in target or target.startswith("#"):
                continue
            target_path = ROOT / unquote(target.split("#", 1)[0])
            if target_path.name in {"extension-dependencies.json", "validation-report.json"}:
                continue
            assert target_path.is_file(), f"Missing local link: {target}"
        for fence in fences:
            if fence.info == "mermaid":
                first = fence.content.strip().splitlines()[0]
                assert first.split()[0] in {"flowchart", "sequenceDiagram", "stateDiagram-v2"}, first
                assert fence.content.count('"') % 2 == 0, "Unbalanced Mermaid quotes"
        report["documents"].append({
            "file": path.name,
            "bytes": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "headings": sum(t.type == "heading_open" for t in tokens),
            "fences": len(fences),
            "mermaid_blocks_structurally_checked": sum(f.info == "mermaid" for f in fences),
        })

    plan = DOCS[1].read_text(encoding="utf-8")
    rows = re.findall(r"^\| (PR-[JOMQ]\d{2}) \| ([^|]+) \| ([^|]+) \|$", plan, re.M)
    graph = {task: re.findall(r"PR-(?:[A-Z])?\d{2}", deps) for task, deps, _ in rows}
    assert len(rows) == len(graph) == 19, "Expected 19 unique extension tasks"
    headings = set(re.findall(r"^### (PR-[JOMQ]\d{2}):", plan, re.M))
    assert headings == set(graph), "Task table and specifications differ"
    all_ids = BASE_IDS | set(graph)
    referenced = set(re.findall(r"\bPR-(?:[A-Z])?\d{2}\b", plan))
    assert referenced <= all_ids, f"Unknown IDs: {referenced - all_ids}"
    visited: set[str] = set()
    active: set[str] = set()
    order: list[str] = []

    def visit(node: str) -> None:
        if node in BASE_IDS or node in visited:
            return
        assert node not in active, f"Dependency cycle at {node}"
        active.add(node)
        for dependency in graph[node]:
            assert dependency in all_ids, dependency
            visit(dependency)
        active.remove(node)
        visited.add(node)
        order.append(node)

    for task in graph:
        visit(task)
    design = DOCS[0].read_text(encoding="utf-8")
    defined_sources = set(re.findall(r"\*\*\[([BR]\d+)\]\*\*", design))
    used_sources = set(re.findall(r"\[([BR]\d+)(?=\]|:)", design))
    assert used_sources <= defined_sources, "Undefined source labels"
    deps_document = {
        "base_plan": "global-coordination-rust-pr-plan-v1.2.md",
        "base_task_count": 70,
        "new_task_count": len(graph),
        "combined_task_count": 70 + len(graph),
        "new_tasks": graph,
        "optional_tasks": ["PR-J06"],
        "original_gate_extensions": {"PR-66": ["PR-Q01"]},
        "extension_only_topological_order": order,
        "qualification": "Base-plan graph not loaded; only the extension graph was cycle-checked.",
    }
    (ROOT / "extension-dependencies.json").write_text(
        json.dumps(deps_document, indent=2) + "\n", encoding="utf-8"
    )
    report["extension_task_count"] = len(graph)
    report["combined_task_count"] = 70 + len(graph)
    report["status"] = "passed"
    report["checks"] = [
        "Markdown parsed", "Fence balance", "Explicit anchors unique",
        "Local document links resolve", "Source labels defined",
        "Task table matches specifications", "Task references resolve",
        "New-task graph is acyclic", "Mermaid declaration/quote structure only",
    ]
    (ROOT / "validation-report.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps({"status": "passed", "new_tasks": len(graph), "total_tasks": 70 + len(graph)}))


if __name__ == "__main__":
    main()

"""Tests for the documentation checker using synthetic documentation trees."""
from __future__ import annotations

import shutil
import tempfile
import unittest
from pathlib import Path

import check_docs as cd

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]

PLAN = """# Plan

**Scope:** 2 implementation tasks with stable `task-*` identifiers.

| Task | Title | Direct prerequisites |
|---|---|---|
| [task-01](#task-01) | First | None |
| [task-02](#task-02) | Second | task-01 |

<a id="task-01"></a>
### task-01: First

**Prerequisites:** None.  
**Design:** Sections 1, 2.1.

<a id="task-02"></a>
### task-02: Second

**Prerequisites:** task-01.  
**Design:** Sections 2.1.

See the [design](tuplesky-design.md#s2-1) and [readme](../../README.md).
"""

DESIGN = """# Design

<a id="s1"></a>
## 1. One

<a id="s2"></a>
## 2. Two

<a id="s2-1"></a>
### 2.1 Two point one

```mermaid
sequenceDiagram
    A->>B: hello
    B-->>A: reply #59; ok
```

```mermaid
flowchart TD
    X["quoted"] --> Y
```
"""


class Tree:
    def __init__(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        (self.root / "docs/design").mkdir(parents=True)
        (self.root / "README.md").write_text("# readme\n[plan](docs/design/tuplesky-prs-plan.md#task-02)\n")
        self.plan = self.root / "docs/design/tuplesky-prs-plan.md"
        self.design = self.root / "docs/design/tuplesky-design.md"
        self.plan.write_text(PLAN)
        self.design.write_text(DESIGN)

    def run(self) -> int:
        return cd.main(["--root", str(self.root)])

    def cleanup(self):
        self.tmp.cleanup()


class DocsChecks(unittest.TestCase):
    def setUp(self):
        self.tree = Tree()

    def tearDown(self):
        self.tree.cleanup()

    def test_valid_tree_passes(self):
        self.assertEqual(self.tree.run(), 0)

    def test_broken_link_fails(self):
        self.tree.design.write_text(DESIGN + "\n[bad](missing.md)\n")
        self.assertEqual(self.tree.run(), 1)

    def test_missing_anchor_fails(self):
        self.tree.design.write_text(DESIGN + "\n[bad](#s9-9)\n")
        self.assertEqual(self.tree.run(), 1)

    def test_unknown_task_reference_fails(self):
        self.tree.design.write_text(DESIGN + "\nSee task-77.\n")
        self.assertEqual(self.tree.run(), 1)

    def test_retired_task_reference_is_allowed(self):
        self.tree.design.write_text(DESIGN + "\nFormer task-s05 is retired.\n")
        self.assertEqual(self.tree.run(), 0)

    def test_index_and_spec_prerequisite_mismatch_fails(self):
        self.tree.plan.write_text(PLAN.replace("| [task-02](#task-02) | Second | task-01 |", "| [task-02](#task-02) | Second | None |"))
        self.assertEqual(self.tree.run(), 1)

    def test_title_mismatch_fails(self):
        self.tree.plan.write_text(PLAN.replace("### task-02: Second", "### task-02: Other"))
        self.assertEqual(self.tree.run(), 1)

    def test_missing_prerequisite_task_fails(self):
        self.tree.plan.write_text(PLAN.replace("**Prerequisites:** task-01.", "**Prerequisites:** task-09.").replace("| Second | task-01 |", "| Second | task-09 |"))
        self.assertEqual(self.tree.run(), 1)

    def test_cycle_fails(self):
        plan = PLAN.replace("| [task-01](#task-01) | First | None |", "| [task-01](#task-01) | First | task-02 |")
        plan = plan.replace("### task-01: First\n\n**Prerequisites:** None.", "### task-01: First\n\n**Prerequisites:** task-02.")
        self.tree.plan.write_text(plan)
        self.assertEqual(self.tree.run(), 1)

    def test_wrong_task_count_fails(self):
        self.tree.plan.write_text(PLAN.replace("**Scope:** 2 implementation", "**Scope:** 3 implementation"))
        self.assertEqual(self.tree.run(), 1)

    def test_missing_design_section_fails(self):
        self.tree.plan.write_text(PLAN.replace("**Design:** Sections 1, 2.1.", "**Design:** Sections 1, 4.2."))
        self.assertEqual(self.tree.run(), 1)

    def test_spec_without_index_row_fails(self):
        self.tree.plan.write_text(PLAN.replace("| [task-02](#task-02) | Second | task-01 |\n", "").replace("2 implementation", "2 implementation"))
        self.assertEqual(self.tree.run(), 1)

    def test_unescaped_semicolon_in_sequence_message_fails(self):
        self.tree.design.write_text(DESIGN.replace("reply #59; ok", "reply; ok"))
        self.assertEqual(self.tree.run(), 1)

    def test_unknown_mermaid_type_fails(self):
        self.tree.design.write_text(DESIGN.replace("flowchart TD", "flowchartx TD"))
        self.assertEqual(self.tree.run(), 1)

    def test_unbalanced_quote_fails(self):
        self.tree.design.write_text(DESIGN.replace('X["quoted"]', 'X["quoted]'))
        self.assertEqual(self.tree.run(), 1)


class RepositoryDocs(unittest.TestCase):
    def test_repository_documentation_passes(self):
        self.assertEqual(cd.main(["--root", str(REPO)]), 0)

    @unittest.skipUnless(shutil.which("mmdc"), "mermaid-cli not installed")
    def test_repository_mermaid_renders(self):
        self.assertEqual(cd.main(["--root", str(REPO), "--render"]), 0)


if __name__ == "__main__":
    unittest.main()

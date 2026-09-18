"""Tests for the change classifier: pure fixtures plus real Git scenarios."""
from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

import classify_changes as cc

HERE = Path(__file__).resolve().parent
POLICY = cc.Policy.load(HERE / "docs-only-policy.json")


def entries(case: dict) -> list[cc.Change]:
    out = []
    gen = case.get("generate")
    if gen:
        for i in range(gen["count"]):
            out.append(cc.Change(gen["status"][0], gen["template"].format(i=i)))
    for raw in case.get("changes", []):
        if len(raw) == 2:
            out.append(cc.Change(raw[0][0], raw[1]))
        else:
            out.append(cc.Change(raw[0][0], raw[2], raw[1]))
    return out


class FixtureCases(unittest.TestCase):
    def test_fixture_cases(self):
        data = json.loads((HERE / "fixtures" / "classify-cases.json").read_text())
        self.assertGreater(len(data["cases"]), 20)
        for case in data["cases"]:
            with self.subTest(case=case["name"]):
                decision = cc.classify(entries(case), POLICY)
                self.assertEqual(decision.docs_only, case["docs_only"], decision.reason)
                if "reason_prefix" in case:
                    self.assertTrue(decision.reason.startswith(case["reason_prefix"]), decision.reason)

    def test_over_three_hundred_files_are_all_inspected(self):
        many = [cc.Change("M", f"docs/design/n{i}.md") for i in range(400)]
        self.assertTrue(cc.classify(many, POLICY).docs_only)
        many.append(cc.Change("M", "Cargo.toml"))
        decision = cc.classify(many, POLICY)
        self.assertFalse(decision.docs_only)
        self.assertEqual(decision.changed_files, 401)

    def test_policy_rejects_absolute_and_newline_paths(self):
        self.assertFalse(POLICY.is_documentation("/README.md"))
        self.assertFalse(POLICY.is_documentation("README.md\nCargo.toml"))
        self.assertFalse(POLICY.is_documentation(""))


class NameStatusParsing(unittest.TestCase):
    def test_parses_plain_and_rename_entries(self):
        raw = b"M\0README.md\0R100\0docs/a.md\0docs/b.md\0A\0x y.rs\0"
        parsed = cc.parse_name_status_z(raw)
        self.assertEqual(parsed, [
            cc.Change("M", "README.md"),
            cc.Change("R", "docs/b.md", "docs/a.md"),
            cc.Change("A", "x y.rs"),
        ])

    def test_truncated_rename_is_an_error(self):
        with self.assertRaises(ValueError):
            cc.parse_name_status_z(b"R100\0docs/a.md\0")

    def test_empty_output_is_empty(self):
        self.assertEqual(cc.parse_name_status_z(b""), [])


class EventPlanning(unittest.TestCase):
    SHA = "a" * 40
    SHB = "b" * 40

    def test_events(self):
        self.assertEqual(cc.comparison_for_event("schedule", None, None, None)[0], "full")
        self.assertEqual(cc.comparison_for_event("workflow_dispatch", None, None, None)[0], "full")
        self.assertEqual(cc.comparison_for_event("pull_request", self.SHA, self.SHB, None), ("merge-base", self.SHA, self.SHB))
        self.assertEqual(cc.comparison_for_event("merge_group", self.SHA, self.SHB, None), ("merge-base", self.SHA, self.SHB))
        self.assertEqual(cc.comparison_for_event("push", None, self.SHB, self.SHA), ("range", self.SHA, self.SHB))
        self.assertEqual(cc.comparison_for_event("push", None, self.SHB, "0" * 40)[0], "full")
        self.assertEqual(cc.comparison_for_event("push", None, self.SHB, None)[0], "full")
        self.assertEqual(cc.comparison_for_event("pull_request", "not-a-sha", self.SHB, None)[0], "full")
        self.assertEqual(cc.comparison_for_event("issue_comment", None, None, None)[0], "full")
        self.assertEqual(cc.comparison_for_event("", None, None, None)[0], "full")


def git(repo: Path, *args: str, check: bool = True) -> str:
    env = dict(os.environ, GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@example.invalid",
               GIT_COMMITTER_NAME="t", GIT_COMMITTER_EMAIL="t@example.invalid")
    proc = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, env=env, check=check)
    return proc.stdout.strip()


def commit_files(repo: Path, files: dict[str, str | None], message: str) -> str:
    for rel, content in files.items():
        path = repo / rel
        if content is None:
            git(repo, "rm", "-q", rel)
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        git(repo, "add", rel)
    git(repo, "commit", "-q", "-m", message, "--allow-empty")
    return git(repo, "rev-parse", "HEAD")


class GitScenarios(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = Path(self.tmp.name) / "repo"
        self.repo.mkdir()
        git(self.repo, "init", "-q", "-b", "main")
        self.base = commit_files(self.repo, {"README.md": "hi\n", "src/lib.rs": "// code\n", "docs/design/x.md": "x\n"}, "base")

    def tearDown(self):
        self.tmp.cleanup()

    def run_decide(self, event: str, base=None, head=None, before=None, repo=None) -> cc.Decision:
        args = cc.argparse.Namespace(event_name=event, base=base, head=head, before=before, repo=str(repo or self.repo))
        return cc.decide(args, POLICY)

    def test_pull_request_docs_only_uses_merge_base(self):
        git(self.repo, "checkout", "-q", "-b", "feature")
        head = commit_files(self.repo, {"docs/design/x.md": "changed\n"}, "docs")
        git(self.repo, "checkout", "-q", "main")
        main = commit_files(self.repo, {"src/lib.rs": "// main moved on\n"}, "main code")
        decision = self.run_decide("pull_request", base=main, head=head)
        self.assertTrue(decision.docs_only, decision.reason)
        self.assertEqual(decision.changed_files, 1)

    def test_pull_request_with_code_is_not_docs_only(self):
        git(self.repo, "checkout", "-q", "-b", "feature")
        head = commit_files(self.repo, {"docs/design/x.md": "changed\n", "src/lib.rs": "// new\n"}, "mixed")
        decision = self.run_decide("pull_request", base=self.base, head=head)
        self.assertFalse(decision.docs_only)
        self.assertEqual(decision.changed_files, 2)

    def test_rename_code_to_doc_is_heavy(self):
        git(self.repo, "mv", "src/lib.rs", "docs/design/lib.md")
        git(self.repo, "commit", "-q", "-m", "rename")
        head = git(self.repo, "rev-parse", "HEAD")
        decision = self.run_decide("pull_request", base=self.base, head=head)
        self.assertFalse(decision.docs_only)
        self.assertIn("rename-source", decision.reason)

    def test_deletion_and_addition_of_docs(self):
        head = commit_files(self.repo, {"docs/design/x.md": None, "docs/design/y.md": "y\n"}, "swap")
        decision = self.run_decide("push", before=self.base, head=head)
        self.assertTrue(decision.docs_only, decision.reason)
        self.assertEqual(decision.changed_files, 2)

    def test_type_change_is_heavy(self):
        (self.repo / "docs/design/x.md").unlink()
        os.symlink("../../README.md", self.repo / "docs/design/x.md")
        git(self.repo, "add", "docs/design/x.md")
        git(self.repo, "commit", "-q", "-m", "symlink")
        head = git(self.repo, "rev-parse", "HEAD")
        decision = self.run_decide("push", before=self.base, head=head)
        self.assertFalse(decision.docs_only)
        self.assertTrue(decision.reason.startswith("non-documentation-status:T"), decision.reason)

    def test_more_than_three_hundred_files(self):
        files = {f"docs/design/n{i}.md": f"{i}\n" for i in range(320)}
        head = commit_files(self.repo, files, "many")
        decision = self.run_decide("push", before=self.base, head=head)
        self.assertTrue(decision.docs_only)
        self.assertEqual(decision.changed_files, 320)
        files["src/extra.rs"] = "// extra\n"
        head2 = commit_files(self.repo, {"src/extra.rs": "// extra\n"}, "one code")
        decision = self.run_decide("push", before=self.base, head=head2)
        self.assertFalse(decision.docs_only)
        self.assertEqual(decision.changed_files, 321)

    def test_push_without_before_selects_full_ci(self):
        decision = self.run_decide("push", before="0" * 40, head=self.base)
        self.assertFalse(decision.docs_only)
        self.assertEqual(decision.reason, "push-without-before-revision")

    def test_empty_change_set_is_not_docs_only(self):
        head = commit_files(self.repo, {}, "empty")
        decision = self.run_decide("push", before=self.base, head=head)
        self.assertFalse(decision.docs_only)
        self.assertEqual(decision.reason, "empty-change-set")

    def test_unknown_commit_selects_full_ci(self):
        decision = self.run_decide("pull_request", base="c" * 40, head=self.base)
        self.assertFalse(decision.docs_only)
        self.assertEqual(decision.reason, "history-unavailable")

    def test_shallow_clone_recovers_or_selects_full_ci(self):
        head = commit_files(self.repo, {"docs/design/x.md": "v2\n"}, "docs v2")
        head2 = commit_files(self.repo, {"docs/design/x.md": "v3\n"}, "docs v3")
        clone = Path(self.tmp.name) / "shallow"
        subprocess.run(["git", "clone", "-q", "--depth", "1", f"file://{self.repo}", str(clone)], check=True, capture_output=True)
        self.assertEqual(git(clone, "rev-parse", "--is-shallow-repository"), "true")
        decision = self.run_decide("push", before=self.base, head=head2, repo=clone)
        # Either history was recovered (docs-only, two commits worth of changes to one file)
        # or the classifier conservatively selected full CI; it must never guess docs-only
        # without the actual comparison.
        if decision.docs_only:
            self.assertEqual(decision.changed_files, 1)
            self.assertTrue(cc.Git(clone).object_exists(self.base))
        else:
            self.assertIn(decision.reason, {"history-unavailable", "merge-base-unavailable"})
        # And a shallow clone that cannot reach the base must not be docs-only.
        lonely = Path(self.tmp.name) / "lonely"
        subprocess.run(["git", "clone", "-q", "--depth", "1", f"file://{self.repo}", str(lonely)], check=True, capture_output=True)
        git(lonely, "remote", "remove", "origin")
        decision = self.run_decide("push", before=self.base, head=head, repo=lonely)
        self.assertFalse(decision.docs_only)
        self.assertEqual(decision.reason, "history-unavailable")


class Cli(unittest.TestCase):
    def test_writes_github_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out.txt"
            rc = cc.main(["--event-name", "schedule", "--output", str(out)])
            self.assertEqual(rc, 0)
            text = out.read_text()
            self.assertIn("docs_only=false", text)
            self.assertIn("reason=manual-or-scheduled-full-run", text)

    def test_missing_policy_fails(self):
        rc = cc.main(["--event-name", "schedule", "--policy", "/nonexistent/policy.json"])
        self.assertEqual(rc, 2)


if __name__ == "__main__":
    unittest.main()

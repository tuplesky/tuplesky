"""Tests for the stable required CI gate evaluation."""
import unittest

import evaluate_gate as gate


def needs(classify="success", docs="success", build="success"):
    return {"classify": {"result": classify}, "docs": {"result": docs}, "build-test": {"result": build}}


class GateTests(unittest.TestCase):
    def test_all_success(self):
        self.assertTrue(gate.evaluate(needs(), "false")[0])

    def test_docs_only_skip_is_green(self):
        self.assertTrue(gate.evaluate(needs(build="skipped"), "true")[0])

    def test_skip_without_docs_only_is_red(self):
        self.assertFalse(gate.evaluate(needs(build="skipped"), "false")[0])
        self.assertFalse(gate.evaluate(needs(build="skipped"), "")[0])

    def test_failure_cancel_missing_are_red(self):
        for bad in ("failure", "cancelled", None):
            self.assertFalse(gate.evaluate(needs(build=bad), "true")[0], bad)
            self.assertFalse(gate.evaluate(needs(classify=bad), "true")[0], bad)
            self.assertFalse(gate.evaluate(needs(docs=bad), "true")[0], bad)

    def test_missing_job_is_red(self):
        self.assertFalse(gate.evaluate({}, "true")[0])
        self.assertFalse(gate.evaluate({"classify": {"result": "success"}, "docs": {"result": "success"}}, "true")[0])

    def test_classifier_success_with_heavy_failure_is_red(self):
        self.assertFalse(gate.evaluate(needs(build="failure"), "false")[0])


if __name__ == "__main__":
    unittest.main()

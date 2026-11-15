#!/usr/bin/env python3
"""Evaluate the stable required ``CI`` gate from job results (Section 12.4.1).

The gate succeeds only when the classifier and the documentation checks
succeeded and every selected heavy job succeeded. A skipped heavy job is
acceptable solely for a validated documentation-only decision. Failure,
cancellation or a missing result is never converted into success.

Input: ``NEEDS_JSON`` (the ``toJSON(needs)`` context) and ``DOCS_ONLY``
(the classifier output). Exit status 0 means the gate passes.
"""
from __future__ import annotations

import json
import os
import sys

REQUIRED_ALWAYS = ("classify", "docs")
HEAVY = ("build-test",)


def evaluate(needs: dict, docs_only: str) -> tuple[bool, str]:
    for job in REQUIRED_ALWAYS:
        result = needs.get(job, {}).get("result")
        if result != "success":
            return False, f"required job {job!r} result is {result!r}"
    docs_only_valid = docs_only == "true"
    for job in HEAVY:
        result = needs.get(job, {}).get("result")
        if result == "success":
            continue
        if result == "skipped" and docs_only_valid:
            continue
        return False, f"heavy job {job!r} result is {result!r} (docs_only={docs_only!r})"
    return True, "classifier, documentation checks and selected heavy jobs succeeded"


def main() -> int:
    raw = os.environ.get("NEEDS_JSON", "")
    docs_only = os.environ.get("DOCS_ONLY", "")
    try:
        needs = json.loads(raw) if raw else {}
    except json.JSONDecodeError as exc:
        print(f"::error::NEEDS_JSON is not valid JSON: {exc}")
        return 1
    ok, reason = evaluate(needs, docs_only)
    print(("PASS: " if ok else "::error::CI gate failed: ") + reason)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

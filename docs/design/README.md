# TupleSky design review

**Status:** Draft design review; no service implementation is included.

## Reading order

1. [Implementation design v0.6](tuplesky-design-v0.6-update.md): observer, membership, and persistence revision supplement. Start with U0, U8-U9, then U3-U7.
2. [SwiftPaxos upstream-issue amendment](swiftpaxos-upstream-review.md): additional ordering, recovery, and testing requirements from the review of upstream issues #1 and #2. Read with U8-U11.
3. [PR plan v1.3](tuplesky-pr-plan-v1.3-update.md): changes to the original 70-task plan and 19 additional tasks. The amendment extends acceptance criteria without renumbering tasks.
4. [Extension dependency graph](extension-dependencies.json) and [documentation validation report](validation-report.json).

## Baseline dependency: not yet included

The v0.6 and v1.3 files are supplements, not standalone replacements. They depend on `global-coordination-rust-design-v0.5.md` and `global-coordination-rust-pr-plan-v1.2.md` from the earlier design session. Those originals were identified in the author's File Library, but their complete original files are not included in this branch. The two supplements are imported unchanged; the issue review is a separate amendment.

Import the original baseline files before treating this PR as a self-contained implementation specification. Do not reconstruct them from snippets or replace detailed authentication, lease, protocol, and encoding contracts with summaries. The Gemini Multi-Raft alternative is not the baseline.

Use U0.1's replacement map for precedence. Unchanged baseline requirements remain applicable. The issue amendment adds the identified safeguards to the combined design and plan; it does not claim a proof of the protocol or extensions.

## Main decisions

SwiftPaxos C2 remains the protocol, with three voters by default and five maximum per active configuration. Observers have no protocol-level count ceiling but have resource budgets. Kine is a domain-scoped trusted protocol client with regional observer-backed watches and epoch-aware discovery.

A pinned raft-engine journal supplies shared persistence; redb holds materialized state, with Fjall limited to isolated experiments. No Raft consensus is introduced. Native renewals remain replicated. Strict and optional replay-backed materialization have separate qualification gates.

## Local documentation validation

Run from the repository root:

```sh
python -m pip install markdown-it-py
python docs/design/validate_package.py
```

The validator checks Markdown parsing, fences, explicit anchors, local links, source labels, task references, and extension-graph cycles. Mermaid declaration/quote checks are not rendering. It does not validate the missing original dependency graph, compile Rust/Go, execute protocol models, or qualify storage crashes. The dependency installation is a local documentation check, not a production dependency policy.

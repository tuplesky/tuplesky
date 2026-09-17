# TupleSky design review

**Status:** Draft design review; no service implementation is included. Both original baseline files are included, so reviewers do not need the earlier chat or File Library.

## Reading order

1. [Original implementation design v0.5](global-coordination-rust-design-v0.5.md): retained protocol, API, authentication, lease, encoding, recovery, and testing contracts.
2. [Implementation design v0.6 supplement](tuplesky-design-v0.6-update.md): U0.1 identifies replacements; U8-U9 cover journal persistence and U3-U7 cover observers and client-aware membership.
3. [SwiftPaxos upstream-issue amendment](swiftpaxos-upstream-review.md): additional ordering, recovery, and testing requirements from upstream issues #1 and #2.
4. [Original 70-task PR plan v1.2](global-coordination-rust-pr-plan-v1.2.md), then [PR plan v1.3 supplement](tuplesky-pr-plan-v1.3-update.md): original tasks and the 19 additional tasks. The issue amendment adds acceptance criteria without renumbering tasks.

## Provenance and precedence

The original design and plan were imported from the author's complete Markdown attachments with their original filenames and relative cross-links. Their import record is retained in Git history at `2acf4eb724a36dcdb74baeb0c3b13368bc1317eb`.

The documents currently remain layered: the baseline supplies requirements not explicitly replaced by the v0.6 replacement map; v1.3 amends the named tasks and adds the extension tasks; the upstream-issue amendment adds its safeguards. PR-66 additionally requires PR-Q01. Unspecified interactions are review findings, not permission to silently discard a requirement. The Gemini Multi-Raft alternative is not the baseline.

## Main decisions

SwiftPaxos C2 remains selected, with three voters by default and five maximum per active configuration. Non-voting observers have no protocol-level count ceiling, but are subject to resource budgets. Kine is a domain-scoped trusted protocol client with regional observer-backed watches and epoch-aware discovery.

A pinned raft-engine journal supplies shared persistence; redb holds materialized state, with Fjall limited to isolated experiments. No Raft consensus is introduced. Native renewals remain replicated. Strict and optional replay-backed materialization have separate qualification gates.

## Documentation-only scope

Local authoring, consolidation, and validation scripts are excluded from this PR, together with generated dependency graphs, import manifests, validation reports, caches, workspace files, and workflows. The PR diff contains Markdown documentation only.

Earlier descriptions of packaged validation scripts or generated reports in the unchanged revision supplements are historical descriptions of the local preparation package, not files included in this PR. Validation observations are recorded in the PR discussion. Proposed future implementation tooling and CI tasks remain in the plan; excluding local workspace scripts does not delete those requirements.

Documentation checks do not build Rust/Go, prove protocol extensions, qualify storage crashes, certify Kubernetes compatibility, or establish performance. Those remain explicit implementation acceptance gates.

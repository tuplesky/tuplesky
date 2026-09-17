# TupleSky design review

**Status:** Draft design review; no service implementation is included. Both original baseline files are now included unchanged, so reviewers no longer need access to the earlier chat or File Library.

## Reading order

1. [Original implementation design v0.5](global-coordination-rust-design-v0.5.md): retained protocol, API, authentication, lease, encoding, recovery, and testing contracts. Its `Coord` working name and original storage architecture are preserved as historical baseline text.
2. [Implementation design v0.6 supplement](tuplesky-design-v0.6-update.md): apply U0.1's replacement map. Start with U0, U8-U9, then U3-U7 for journal persistence, observers, and client-aware membership.
3. [SwiftPaxos upstream-issue amendment](swiftpaxos-upstream-review.md): additional ordering, recovery, and testing requirements from upstream issues #1 and #2. Read with U8-U11 and the retained baseline protocol sections.
4. [Original 70-task PR plan v1.2](global-coordination-rust-pr-plan-v1.2.md), then [PR plan v1.3 supplement](tuplesky-pr-plan-v1.3-update.md): original tasks, their amendments, and 19 additional tasks. The issue amendment adds acceptance criteria without renumbering tasks.
5. [Dependency graph](extension-dependencies.json), [baseline import manifest](baseline-imports.json), and [documentation validation report](validation-report.json).

## Baseline provenance and precedence

The v0.5 design and v1.2 plan were supplied as complete Markdown attachments and imported byte-for-byte, with their original filenames, formatting, relative cross-links, and task IDs. [The import manifest](baseline-imports.json) records their byte lengths, SHA-256 digests, and Git blob IDs. They were not reconstructed from excerpts. The two revision supplements and upstream-issue amendment are unchanged by this baseline-import commit.

The package is self-contained for reviewing its proposed contracts, but remains layered rather than a consolidated replacement document:

- The baseline provides requirements not explicitly replaced by v0.6. U0.1 identifies replacements; an older single-engine-authority statement is not the current persistence decision.
- The v1.3 plan amends named baseline tasks and adds the extension tasks. Its documented PR-66 release dependency on PR-Q01 applies to the combined graph.
- The upstream-issue amendment adds the identified safeguards and acceptance criteria to the combined design and plan.

Unspecified interactions or apparent conflicts are review findings, not permission to silently discard a requirement. The Gemini Multi-Raft alternative is not the baseline. No protocol-extension proof, implementation, or performance claim follows from assembling the documents.

## Main decisions

SwiftPaxos C2 remains the protocol, with three voters by default and five maximum per active configuration. Observers have no protocol-level count ceiling but have resource budgets. Kine is a domain-scoped trusted protocol client with regional observer-backed watches and epoch-aware discovery.

A pinned raft-engine journal supplies shared persistence; redb holds materialized state, with Fjall limited to isolated experiments. No Raft consensus is introduced. Native renewals remain replicated. Strict and optional replay-backed materialization have separate qualification gates.

## Local documentation validation

Run from the repository root:

```sh
python -m pip install markdown-it-py
python docs/design/validate_package.py
```

The validator now loads both baseline files. It checks imported bytes against the manifest, Markdown parsing/fences, explicit anchors and local document links, source labels, agreement between task tables and specifications, and cycles across all 89 tasks including the documented release-gate extension. It keeps the extension-only order for comparison with the original report.

The unchanged v1.3 supplement's description of the original extension-only validator is historical; this index, validator, and regenerated report describe the expanded check. Mermaid declaration/quote checks are not official rendering. Rust/Go builds, external-source verification, protocol models, storage crash qualification, and benchmarks are not performed by this script. The dependency installation is a local documentation check, not a production dependency policy.

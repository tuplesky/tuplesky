# TupleSky design review

**Status:** Proposed design and implementation plan. No service implementation or completed protocol qualification is included.

The review has two authoritative documents:

| Document | Contents |
|---|---|
| [tuplesky-design.md](tuplesky-design.md) | Architecture, source-mapped protocol and recovery safeguards, APIs, identity, leases, observers, client-aware membership, transport, shared journal/materialization, tests and release gates |
| [tuplesky-prs-plan.md](tuplesky-prs-plan.md) | All 89 task specifications, one index, direct prerequisites, acceptance criteria and optional tracks |

Start with the design's navigation table, then review the corresponding tasks. The original contracts, observer/membership/journal revision and upstream issues #1/#2 safeguards are integrated in place. Earlier files at commit `2acf4eb724a36dcdb74baeb0c3b13368bc1317eb` remain historical provenance, not additional specifications reviewers must combine. No earlier chat or File Library access is required.

Review particularly the distinction between journal durability, materialization and protocol establishment; observer progress/list-watch continuity and strict output authorization; exact configuration evidence and terminal handoff recovery; and the implementation gates for speculative results, replay materialization and read fences. Observer policy replay does not replace the retained per-selected-output authorization barrier.

This PR contains only the two documents and Markdown navigation. Local authoring/consolidation/validation scripts, generated JSON reports or dependency graphs, caches, workspace files and workflows are excluded. Future service test tooling and CI described in the plan remain intended implementation work.

Document checks are not service qualification. Rust/Go builds, full protocol/trace validation, actual engine crash campaigns, supported-platform testing and performance experiments remain explicit acceptance obligations. Dependency/source observations retain their stated review dates rather than claiming a new verification during consolidation.

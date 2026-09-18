# TupleSky design review

**Status:** Proposed design and implementation plan. No service implementation or completed protocol qualification is included.

The review has two authoritative documents:

| Document | Contents |
|---|---|
| [tuplesky-design.md](tuplesky-design.md) | Architecture, source-mapped protocol and recovery safeguards, APIs, identity, leases, observers, client-aware membership, transport, shared journal/materialization, tests and release gates |
| [tuplesky-prs-plan.md](tuplesky-prs-plan.md) | All 89 task specifications, one index, direct prerequisites, acceptance criteria and optional tracks |

Start with the design's navigation table, then review the corresponding tasks. The original contracts, observer/membership/journal revision and upstream issues #1/#2 safeguards are integrated in place. Earlier files at commit `2acf4eb724a36dcdb74baeb0c3b13368bc1317eb` remain historical provenance, not additional specifications reviewers must combine. No earlier chat or File Library access is required.

Review particularly the distinction between journal durability, materialization and protocol establishment; observer progress/list-watch continuity and strict output authorization; exact configuration evidence and terminal handoff recovery; and the implementation gates for speculative results, replay materialization and read fences. Observer policy replay does not replace the retained per-selected-output authorization barrier.

## Performance interpretation: leader locality remains relevant

This is a reading aid for [the quorum rules](tuplesky-design.md#s4-1), [the fixed C2 policy](tuplesky-design.md#s4-2) and [the latency model](tuplesky-design.md#s4-6), not an additional protocol specification. **SwiftPaxos removes an avoidable sequential leader-relay path; it does not remove the leader from the quorum or erase its physical distance.** Every normal-operation fast and slow quorum includes the ballot's leader. "No dominant home region" describes the traffic distribution, not leaderless consensus or region-local write availability.

For a warm, dependency-ready fast-path exchange, the idealized communication budget is the maximum collector-to-voter round trip over the configured fast quorum, including the leader. Thus a Singapore collector and Frankfurt leader still require the Frankfurt exchange even if other voters are nearby. Storage, computation, dependency readiness, queueing and API-edge delivery add costs; geographic distance alone does not determine which evidence is slowest. The slow path also retains leader-guided ordering.

C2 permits one fixed fast quorum per ballot, not a different nearest majority per client. Merely moving leadership within an unchanged fast set does not change that set's idealized maximum RTT; changing the set requires the reviewed higher-ballot recovery. Regional observers offload eligible reads and event distribution, not write-quorum participation.

Review [task-m04](tuplesky-prs-plan.md#task-m04) against these constraints: score the actual leader, whole fast set, client regions and degraded slow path while enforcing failure-domain limits. In [task-62](tuplesky-prs-plan.md#task-62), [task-63](tuplesky-prs-plan.md#task-63) and [task-q01](tuplesky-prs-plan.md#task-q01), inspect per-region results for local and remote leaders, unfavorable fixed-fast placement, and comparably optimized Raft/Multi-Paxos placement. Parallel fan-out is not a claim of placement-independent latency, a fixed percentage improvement, or universally better end-to-end tails.

## Review scope

The design package consists of the two documents and Markdown navigation. Local authoring/consolidation/validation scripts, generated reports, caches and workspace files are not part of the design package. Repository-owned CI workflows and the reviewed change filter are task-01 deliverables. Future service test tooling and CI described in the plan remain intended implementation work.

Document checks are not service qualification. Rust/Go builds, full protocol/trace validation, actual engine crash campaigns, supported-platform testing and performance experiments remain explicit acceptance obligations. Dependency/source observations retain their stated review dates rather than claiming a new verification during consolidation.

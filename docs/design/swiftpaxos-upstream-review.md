# SwiftPaxos upstream issues: implementation safeguards

**Date:** 2026-09-17.  
**Status:** Proposed amendment, not a completed proof or reproduced upstream counterexample.  
**Applies to:** [Design v0.6](tuplesky-design-v0.6-update.md) and [PR plan v1.3](tuplesky-pr-plan-v1.3-update.md).  
**Reference code inspected:** `35c69365f1c7737a08e237bfbaf828ee68897080`.

## 1. Assessment

Upstream [issue #1](https://github.com/imdea-software/swiftpaxos/issues/1) and [issue #2](https://github.com/imdea-software/swiftpaxos/issues/2) report differences between the implementation and paper invariants. In the reviewed discussions, the maintainer acknowledges ordering differences but argues that weaker properties suffice. The discussions do not provide a completed replacement proof or demonstrate an end-to-end consensus safety failure. Do not describe them as either a proven fundamental protocol failure or fully resolved correctness questions.

Retain SwiftPaxos C2, observers, and the raft-engine journal. Do not treat the Go prototype as a correctness oracle. This amendment adds explicit requirements to the existing sections and task acceptance criteria without changing the 89 planned task IDs. Issue status and repository head are not assumed to remain unchanged after the review date.

## 2. Atomic initialization and dependency visibility

Issue #1 describes command A entering the conflict index before its asynchronous handler leaves START. A conflicting B can then depend on A while A is still in START. The maintainer accepts this interleaving, while disputing that it establishes a safety violation and emphasizing dependency-phase ordering.

**Required:** Installing initialized command state and exposing it through dependency lookup must be one atomic logical transition. A single actor is not sufficient if it updates the index, awaits storage, and processes another request before completing the transition.

Journal payload binding, phase, dependencies, and required index changes together. Derived indexes must be rebuilt only from complete authoritative state. Pending ingress and missing-payload placeholders must not masquerade as processed protocol commands.

Enforce the [paper's](https://www.usenix.org/system/files/nsdi24-ryabinin.pdf) normal-operation prerequisites: direct dependencies are ACCEPT or COMMIT before entering ACCEPT; dependencies are committed before entering COMMIT; dependencies have executed before finalized execution. Recovery retains its own source-mapped rules. Preserve the separately validated speculative-result path; speculative state cannot leak into ordinary reads, watch events, or irreversible effects.

## 3. Durable recovery cut and logical outbox

Issue #2 describes old-ballot acknowledgements queued in the batcher when a higher-ballot NewLeaderAck is sent through another path. The maintainer argues that the recovery response must preserve the state underlying the old vote, rather than rely on physical transmission order alone. This reasoning is not itself a proof of TupleSky's asynchronous implementation.

The journal/projection split must prohibit this illustrative composition:

```text
redb is materialized through local sequence 40.
Sequence 41 durably records a vote and its acknowledgement is released.
Recovery reads the sequence-40 database and omits the sequence-41 obligation.
```

This is a design counterexample, not a reproduced upstream trace. A recovery response must summarize authoritative protocol state at a defined durable cut: use the actor's durable state or wait for materialization through that cut and read a consistent snapshot. Persist-before-send alone does not make a lagging projection safe.

Before authorizing the higher-ballot reply, close admission of new old-ballot voting transitions, resolve already-submitted journal work relative to the cut, durably establish the new promise, and include all source-required recovery information. A timeout or missing completion does not prove an append is absent. Reconcile indeterminate persistence or stop serving; do not guess. Retention follows protocol recovery rules, not a blind union of historical dependency sets.

Define the actor's logical publication of immutable evidence separately from physical transmission. Every vote-producing effect carries domain, replica incarnation, boot, membership epoch, ballot, and prerequisite durable-state identity. A delayed completion may update valid bookkeeping without authorizing a new obsolete vote. Same-boot leadership changes require fencing too.

Already-authorized old evidence may arrive late. Never relabel it with a new ballot or combine votes across configurations. Dropping an outbound queue does not recall packets already sent. Historical completion can remain valid after client refresh; retain request identity and deduplication.

Do not require every old packet to reach every peer before recovery. No global cross-domain flush barrier is needed. Shared journal group commits remain compatible with per-domain cuts and effect ordering, subject to the explicit model/refinement review.

## 4. Recovery selection and restart-stable publication

The second part of issue #2 questions recovery map overwrites. The maintainer notes that one leader computes Sync and followers adopt it; they do not merge the reports independently. Different local phases are not by themselves different committed dependency sets. The [inspected recovery code](https://github.com/imdea-software/swiftpaxos/blob/35c69365f1c7737a08e237bfbaf828ee68897080/swift/recovery.go) supports that distinction.

Use source-defined ballot selection and possible-fast-decision recovery, not a generic highest-phase-wins rule. Where eligible accepted-value candidates must agree, validate command/dependency equivalence. Unexpected incompatible accepted candidates stop recovery with diagnostic evidence rather than arbitrary overwrite. Ordinary preaccept disagreement remains valid recovery input.

Make permitted choices deterministic for reproducibility and canonical encoding, but do not mistake determinism for safety. Durably bind the selected recovery result to epoch/ballot before publishing Sync. After a crash, reuse it or enter an authorized new ballot; do not publish an incompatible result under the same identity.

## 5. Membership and observer consequences

Seal reports and terminal recovery must preserve the same voting obligations, including delayed effects and potentially completed requests. Frontend admission closure or an applied KV snapshot alone does not define a safe handoff cut.

Observers still consume only finalized, dependency-complete history. A hash chain detects inconsistency but cannot prove that recovery selected the right history. Source changes after voter failure must preserve completed outcomes and revision/event continuity.

These findings identify no raft-engine defect. TupleSky must compose engine durability, state snapshots, and publication correctly. Journal durability, materialization, and protocol establishment remain distinct events.

## 6. Section and task amendments

| Design area | Additional requirement | Existing tasks |
|---|---|---|
| U8.4 | Atomic command-state/index visibility | PR-19 through PR-23; PR-J01/J03 |
| U8.5; U9 | Durable recovery cut and logical outbox | PR-20; PR-25/26; PR-J03/J05 |
| U10.1 | Same-boot epoch/ballot effect fencing | PR-20; PR-J03/J05 |
| Retained recovery sections | Candidate validation and stable Sync | PR-19; PR-25 through PR-29 |
| U7.2 | Handoff includes delayed voting obligations | PR-54 through PR-57; PR-M03/M05 |
| U3; U11 | Source-failover history continuity | PR-O06; PR-Q01 |

These are added acceptance criteria, not new task IDs or a different consensus protocol.

## 7. Release-blocking regression schedules

| Schedule | Required property |
|---|---|
| Pause A initialization and admit conflicting B | No half-initialized dependency is exposed |
| Leader acknowledgement precedes dependency readiness | No premature phase change or finalized execution |
| Old vote queued across recovery | Required voting state survives send reordering |
| Journal durable; projection held behind | Recovery uses the authoritative cut |
| Old-ballot I/O completes during same-boot recovery | No newly unauthorized obsolete voting effect |
| Recovery reports permuted with valid phase differences | Correct selection and canonical result |
| Crash after Sync publication or during sealing | No incompatible publication or lost completed work |
| Old completion arrives after Kine refresh | No mixed quorum; stable retry result |
| Observer switches source after a completed write and voter loss | Outcome and event continuity preserved |

Exercise production transition/effect code under deterministic schedules, source-mapped bounded models, and trace validation. Add deliberately faulty variants to test the oracle. The researchers' traces have not been rerun here; proposed Rust/Go integration, formal refinement, and crash qualification are not claimed to have passed.

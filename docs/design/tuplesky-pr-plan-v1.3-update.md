# TupleSky: review-friendly PR plan v1.3

## Journal persistence, observers, and client-aware membership

**Date:** 2026-09-17.  
**Baseline:** `global-coordination-rust-pr-plan-v1.2.md` [B2].  
**Design:** [TupleSky design v0.6 revision supplement](tuplesky-design-v0.6-update.md).  
**Format:** Changes to the original plan, not a reprint of its untouched tasks. PR-01 through PR-66 and PR-S01 through PR-S04 keep their identifiers. Nineteen new tasks bring the combined plan to 89 tasks; PR-J06 is an optional, separately gated optimization.

The source baseline already includes source-mapped SwiftPaxos, durable votes, native replicated renewals, Kine watches, checkpoint floors, and sealed membership handoff. This plan extends that implementation; it does not replace it with the Gemini Multi-Raft roadmap. Existing acceptance requirements remain unless explicitly superseded here.

## P0. Integration rules and changes to existing tasks

These changes apply to the named source tasks without renumbering them. Base tasks can land their original bounded increments before later extension tasks complete; production gates below require the combined behavior. Do not turn an extension into a prerequisite of its own base task.

| Existing task(s) | v1.3 change |
|---|---|
| PR-01 | Add exact raft-engine git pin, feature/build audit, source snapshot, and codec smoke test. Keep existing Rust/Go/security pins unless explicitly changed. |
| PR-02, PR-03 | Reserve identities and explicit versioned schema kinds for config epochs, observer frames, read fences, and LocalJournalSeq. Logical retry identity must not change with a configuration hint. |
| PR-S01, PR-S02 | Preserve common codecs/materialization and model testkit; distinguish durable journal, atomic working-state apply, and durable checkpoint contracts. No hidden weakening of commit_durable. |
| PR-07, PR-08, PR-11 | The original redb-only worker is a useful reference increment; production composition becomes the journal-first worker in PR-J03. Retain atomic plan guards, complete events, and boot-fenced output. |
| PR-09 | Keep actual redb byte-fault coverage. It does not replace raft-engine or cross-engine recovery tests in PR-J05. |
| PR-12 | Retries/results/floors remain in common state and transfer through epochs and local journal replay. |
| PR-15 through PR-17 | Native renewal remains replicated; preserve conservative expiry and Kine private TTL mapping. No observer-local expiry or conversion of TTL into LeaseId. |
| PR-19 through PR-29 | Source protocol and dependency/evidence predicates unchanged. Storage effects use the refined journal/materialization completion types; no Raft-style index/term assumptions. |
| PR-30 through PR-33 | Add trusted Kine collector role, sparse group/observer connections, role authorization, and observer/replication traffic budgets. Original Rust collector remains the reference. |
| PR-48 | Original Kine conformance is retained as an integration base. PR-O04 and PR-M02 add observer and direct-collector conformance against a frozen Kine revision. |
| PR-49, PR-50 | Keep common checkpoint export/import. LocalRecoveryCheckpointV1 in PR-J04 is a different artifact and may not be substituted for a common checkpoint or vice versa. |
| PR-51 through PR-53 | Keep quorum-safe protocol forgetting. Local journal compaction cannot bypass those obligations or require all observers to ACK. |
| PR-54 through PR-57 | Keep the modeled seal/terminal-certificate/activation algorithm. PR-M03 wires staging observers and client-visible configuration; do not redesign handoff ad hoc in a placement controller. |
| PR-58 through PR-60 | Carry observer/collector identities, shared shard metadata, journal checkpoints, and epoch continuity through credential, restore, and format workflows. |
| PR-61 through PR-65 | Include role-specific readiness, journal and observer metrics, real-engine crash tests, and supported-platform qualification. Keep baseline security and platform gates. |
| PR-66 | Add PR-Q01 as a release prerequisite. Enabling replay materialization additionally requires PR-J06. |
| PR-S03, PR-S04 | Fjall remains an isolated experiment. Hold the journal/profile/topology constant for state-engine comparisons; keep a separate single-store reference comparison. No engine migration deliverable. |

## P1. New task graph

The dependency file in this package records the same graph. Dependencies on original IDs refer to the original plan. The validation script checks new IDs, references, cycles within the extension graph, and presence of the documented release gate; it does not claim to revalidate the full unavailable original dependency graph.

| New task | Prerequisites | Scope |
|---|---|---|
| PR-J01 | PR-01, PR-02, PR-S01, PR-S02 | Journal contracts, identities, and model |
| PR-J02 | PR-J01 | raft-engine adapter, codec, and durable grouping |
| PR-J03 | PR-J02, PR-08, PR-11 | Journal-first worker and strict materialization |
| PR-J04 | PR-J03, PR-09 | Local checkpoints, replay, and physical retention |
| PR-J05 | PR-J02, PR-J03, PR-J04, PR-09 | Real-engine and composed crash qualification |
| PR-J06 | PR-J04, PR-J05 | Optional replay-backed materialization profile |
| PR-J07 | PR-J03, PR-J05, PR-31 | Shared-group resource and batching evaluation |
| PR-O01 | PR-02, PR-03, PR-28, PR-49 | Finalized stream and observer contract |
| PR-O02 | PR-O01, PR-J03, PR-50 | MVCC observer installation and catch-up |
| PR-O03 | PR-O02, PR-31 | Relay fan-out, source switching, and budgets |
| PR-O04 | PR-O02, PR-48 | Kine observer watches and progress |
| PR-O05 | PR-O04, PR-18 | Historical reads and authoritative observer read fences |
| PR-O06 | PR-O03, PR-O04, PR-O05, PR-J05 | Observer correctness, fault, and scaling campaign |
| PR-M01 | PR-02, PR-19 | Authoritative configuration and discovery contract |
| PR-M02 | PR-M01, PR-33, PR-48 | Kine collector, epoch refresh, and retry parity |
| PR-M03 | PR-M01, PR-O02, PR-57, PR-J04 | Staging/promotion and sealed handoff integration |
| PR-M04 | PR-M03, PR-M02 | Regional placement and ballot-tuning policy |
| PR-M05 | PR-M02, PR-M03, PR-M04, PR-58 | Client/membership mixed-fault campaign |
| PR-Q01 | PR-J07, PR-O06, PR-M05, PR-63, PR-64 | Combined end-to-end qualification report |

Do not insert unnecessary dependency chains between observer routing and fully dynamic membership. Fixed-membership observer previews can use the original pinned configuration. Operational production readiness still requires both.

## P2. Journal implementation tasks

<a id="pr-j01"></a>
### PR-J01: Specify journal identities, records, and recovery model

**Design:** U8.2-U8.4, U9.1-U9.2.  
**Prerequisites:** PR-01, PR-02, PR-S01, PR-S02.

**Implement:** Define StorageStreamId allocation, local incarnation binding, LocalJournalSeq, complete redo records, prerequisite barriers, errors, and versioned digest rules. Extend the common logical storage catalog rather than introducing engine-owned application encodings. Implement a deterministic journal model with durable/volatile images, lost notifications, and indeterminate outcomes. Record how StoreSeq maps to LocalJournalSeq.

**Acceptance:** Property tests reject stream-ID reuse, duplicate/missing sequence, mixed domains, old-boot completion, mutated same-sequence payload, and application plans sharing an invalid old base. Tests prove that KV revision, execution position, configuration epoch, and journal sequence cannot be interchanged by API types. Record limits include the largest permitted atomic command.

**Review boundary:** No physical engine, new consensus protocol, relaxed conflict predicate, or engine conversion.

<a id="pr-j02"></a>
### PR-J02: Integrate the pinned raft-engine with postcard and shared durable writes

**Design:** U8.1, U8.4-U8.5, U8.7.  
**Prerequisite:** PR-J01.

**Implement:** Add coord-journal-raft-engine using the reviewed full git revision. Implement ValueCodec and MessageExt for bounded versioned journal entries, strict decode errors, small engine metadata, and explicit nonempty LogBatch writes with sync enabled. Map byte-count results to caller-owned barrier/sequence identities. Add bounded explicit batch formation and validate the engine's internal group-write behavior without stacking hidden timers.

**Acceptance:** Build on supported Rust/OS targets with audited features. Reopen and verify entries/metadata. Test empty/drain barriers, malformed envelopes, unsupported versions, index/payload mismatch, I/O errors, sync panic fail-stop, and one atomic multi-domain batch. Measure actual sync count in a controlled fixture. No vote publication from an append-before-sync notification.

**Review boundary:** This is storage reuse, not raft-rs integration. No erasable Raft term/index model or assumption that custom postcard support is a built-in upstream feature.

<a id="pr-j03"></a>
### PR-J03: Wire journal-first transitions and strict materialization

**Design:** U8.2-U8.6, U10.1.  
**Prerequisites:** PR-J02, PR-08, PR-11.

**Implement:** Connect the pure actor's persistence effects to the shared journal. Validate guards, serialize each local stream, journal its complete immutable transition, and release JournalDurable only for the matching process generation. Apply durable records through the common materializer using the original strict redb durability profile. Distinguish JournalDurable, Materialized, and Established. Add bounded cross-domain scheduling and projection backpressure.

**Acceptance:** Tests crash or delay between every stage and compare with the original single-store reference. Protocol ACKs require durable records; application/events require their established history. A redb snapshot visible before worker completion cannot leak through a public read. Duplicate journal replay produces neither a second revision nor a second lease renewal. Slow projection does not create an unbounded journal queue.

**Review boundary:** No unsynchronized working-state optimization yet. Keep the stricter profile visibly named and report its extra disk work.

<a id="pr-j04"></a>
### PR-J04: Implement local recovery checkpoints and durable truncation

**Design:** U9.1-U9.5.  
**Prerequisites:** PR-J03, PR-09.

**Implement:** Export the complete local state at a durable materialized sequence, including unresolved voting obligations, into an inactive same-engine checkpoint. Persist files/directories, journal the publication reference, then retire only the covered journal prefix in a later durable operation. Implement recovery from the published checkpoint and contiguous suffix. Keep common SharedCheckpointV1 separate.

**Acceptance:** Crash at creation, file sync, directory sync/rename, pointer publication, truncation, purge, and old-checkpoint deletion. At every point reopen a valid selected baseline plus suffix or fail closed. Missing selected checkpoint, gap in required history, and corruption cannot be treated as a fresh database. A local checkpoint preserves an unresolved vote even when its original redo is reclaimed.

**Review boundary:** No claim that local persistence permits protocol forgetting, quorum loss recovery, or cross-engine migration. Do not copy live mutable database files without a consistent engine boundary.

<a id="pr-j05"></a>
### PR-J05: Qualify the real journal and composed persistence boundary

**Design:** U8.7, U9.6, U11.1.  
**Prerequisites:** PR-J02, PR-J03, PR-J04, PR-09.

**Implement:** Provide filesystem fault injection for the selected raft-engine path; audit operations outside the exposed abstraction and background scheduling. Combine it with actual redb faults, subprocess death, disk-full, sync failure, and recovery-mode tests. Retain independent protocol/state oracles and semantic retry checks.

**Acceptance:** Acknowledged outcomes survive the declared failure model. Sync panic cannot leave unrelated code serving from the same uncertain engine. A model passing is reported separately from physical engine coverage. The report names all uncontrolled operations and platforms rather than claiming perfect deterministic byte coverage. Inject known WAL/projection-ordering bugs to demonstrate detection.

**Review boundary:** No unsupported power-loss guarantee from clean shutdown or process-kill tests alone.

<a id="pr-j06"></a>
### PR-J06: Enable replay-backed working-state materialization (optional)

**Design:** U8.6, U9.  
**Prerequisites:** PR-J04, PR-J05.

**Implement:** Introduce a separate internal atomic-materialize capability without per-transaction state-store synchronization. Preserve durable journal publication and durable local checkpoint creation. Recover by reconstructing a new working generation from the selected checkpoint and suffix; fail closed when those sources are unavailable. Keep the strict profile supported and default until qualification completes.

**Acceptance:** The entire composed fault matrix holds with unsynchronized live state discarded or invalid. No use of commit_durable is satisfied with weaker guarantees. Benchmark end-to-end durable behavior, including checkpoint overhead and recovery time. Only enable the profile after review and a documented measured benefit; it is not a generic unsafe operator toggle.

**Review boundary:** No decrease in public durability, cross-engine dual-authority, arbitrary fallback to old directories, or headline benchmark omitting checkpoint maintenance.

<a id="pr-j07"></a>
### PR-J07: Validate multi-group batching and resource isolation

**Design:** U1, U8.5-U8.7, U10-U11.  
**Prerequisites:** PR-J03, PR-J05, PR-31.

**Implement:** Run many sparse groups plus hot groups on a bounded shard set. Track per-domain scheduling, journal syncs, queue delay, materialization, checkpoint pressure, engine index memory, rewrite bandwidth, and rejection behavior. Make node-wide cache and worker budgets explicit; do not allocate a full thread/cache budget independently to every mostly idle domain.

**Acceptance:** Publish reproducible low-load and saturation results. Demonstrate no intentional idle batch wait, retained within-domain ordering, bounded queue memory, and explicit blast radius for shard failure. Compare with the single-store reference using equivalent guarantees. Report whether a separate engine writer pool helps or merely creates queueing.

**Review boundary:** No universal throughput or latency claim, and no protocol default change justified by one microbenchmark.

## P3. Observer and Kine tasks

<a id="pr-o01"></a>
### PR-O01: Specify finalized frames and observer capabilities

**Design:** U2-U3, U5.2.  
**Prerequisites:** PR-02, PR-03, PR-28, PR-49.

**Implement:** Freeze FinalizedFrameV1, execution/revision/digest/epoch identity, complete event batches, common-state changes, authorization transitions, and capability-specific snapshots. Define exporter readiness and source-switch validation. Add Rust/Go vectors and a reference observer model.

**Acceptance:** Model excludes speculative events, rejects missing history and mixed restore identities, and handles non-KV changes without falsely advancing KV revision. A relay is not advertised as MVCC or promotion-ready. Frames remain bounded without exposing partial revisions.

**Review boundary:** No copying of local voting journals as an observer stream and no claim that a hash is a Byzantine certificate.

<a id="pr-o02"></a>
### PR-O02: Build MVCC observer install, catch-up, and serving lifecycle

**Design:** U3.2-U3.4, U5.  
**Prerequisites:** PR-O01, PR-J03, PR-50.

**Implement:** Add authorized snapshot installation, validated replay cursor, atomic state/events/frontier application, source resumption, and reinstallation when retention is exceeded. Reuse the common materializer and selected storage profile. Enforce observer identity and domain scope; it never sends votes.

**Acceptance:** Kill observer/source during install and replay; verify lineage and stable event results. A current KV revision with stale policy execution position is not considered fully caught up. Wrong-scope subscription and voting attempts fail. A stopped observer does not pin source protocol history or block mutations.

**Review boundary:** No new voter authority, leader eligibility, or automatic promotion.

<a id="pr-o03"></a>
### PR-O03: Add regional relays, bounded fan-out, and source failover

**Design:** U2.2, U3.4.  
**Prerequisites:** PR-O02, PR-31.

**Implement:** Add sparse replication topology, bounded source fan-out, relay buffering, loop prevention, subscription admission, snapshot budgets, and jittered reconnection. Sources can resume the same finalized lineage rather than requiring one permanent exporter node.

**Acceptance:** Slow subscribers and a disconnected region cannot exhaust control/voting queues. Failover neither skips history nor switches to a conflicting prefix. Measure total distribution cost separately from voter NIC cost. Exceeding retention returns an explicit reinstall/compaction requirement.

**Review boundary:** No observer ACK in consensus completion or cluster-wide observer mesh.

<a id="pr-o04"></a>
### PR-O04: Route Kine watches to observers with correct progress

**Design:** U4.  
**Prerequisites:** PR-O02, PR-48.

**Implement:** Freeze the Kine revision/fork and document old versus new backend signatures. Route watches to capable regional observers with fallback. Implement historical replay/live attachment, whole-revision resumption, per-watch ordering, cancellation, and ordered progress markers. Patch the chosen edge where necessary; commit cross-language fixtures.

**Acceptance:** Real API-server tests cover list-then-watch with writes in the gap, future/compacted start, progress with queued events, filters with no matches, partial batches, stream restarts, source failures, and compaction. No SQL polling or per-event token exchange. A source head cannot outrun the Kine delivery frontier.

**Review boundary:** No assumption that WaitForSyncTo exists in newer Kine, that EventBatch exists at the old pin, or that a successful Kubernetes boot proves watch-cache correctness.

<a id="pr-o05"></a>
### PR-O05: Add observer historical reads and authoritative read fences

**Design:** U4.1, U5.  
**Prerequisites:** PR-O04, PR-18.

**Implement:** First serve qualified historical reads with proper authorization. Then add the explicitly ordered ReadFence command and observer wait/snapshot path behind a capability gate. Bind the request's range, options, execution position, revision, and permission outcome; preserve pinned snapshot consistency and compaction errors.

**Acceptance:** Tests prevent reuse of a fence created before a later request, stale policy admission, newer data with an older revision header, and indefinite waits after compaction/source loss. Differential-test Get/List/Count/pagination. Current reads continue through the voter path when the feature is disabled.

**Review boundary:** No unproved SwiftPaxos ReadIndex clone or silent weaker-consistency reads. Authorization is not a static token-only cache.

<a id="pr-o06"></a>
### PR-O06: Qualify observer correctness and regional scaling

**Design:** U11.  
**Prerequisites:** PR-O03, PR-O04, PR-O05, PR-J05.

**Implement:** Combine observer, relay, Kine, policy, compaction, storage, and regional network faults. Test event-only relays and MVCC observers separately. Add steady-state and degraded performance scenarios at increasing observer/subscriber counts.

**Acceptance:** Report complete histories, event delay and read delay separately from mutation latency, bounded queues, and recoverable resumption. Adding an unavailable observer does not change quorum or block writes. Publish supported measured capacity rather than interpreting no protocol cap as unlimited physical resources.

**Review boundary:** No claim of fault-tolerance improvement from observer count or hidden bypass of API-server consistency requirements.

## P4. Membership and client tasks

<a id="pr-m01"></a>
### PR-M01: Define authoritative configuration discovery and epoch records

**Design:** U6.1-U6.3.  
**Prerequisites:** PR-02, PR-19.

**Implement:** Define GroupConfigurationV1, BallotConfigurationV1, authenticated hints, epoch certificate chaining, endpoint generations, and a paginated observer registry. Add trust/rollback validation and configuration subscription/bootstrap messages. Keep the source quorum/evidence predicates authoritative.

**Acceptance:** Bounded models and fixtures reject fabricated higher epochs, wrong voter incarnation, arbitrary fastest-majority fast quorums, and observer credentials voting. Endpoint/certificate rotation alone cannot change membership. The record representation preserves historical verification without live issuer access.

**Review boundary:** Discovery does not authorize a transition and a controller cannot bypass the existing handoff proof.

<a id="pr-m02"></a>
### PR-M02: Make Kine a full epoch-aware trusted collector

**Design:** U2.1, U6.  
**Prerequisites:** PR-M01, PR-33, PR-48.

**Implement:** Port the reviewed collector contract to the domain-scoped Go client, including direct fan-out, completion validation, config refresh, stable retries, deduplication of voter identities, and historical-result handling. Reuse language-neutral traces with the Rust collector. Keep optional local-sidecar composition separate.

**Acceptance:** Rust and Go reach identical decisions over reordered/lost/mixed-ballot/mixed-epoch evidence. No request waits for a sequential directory lookup on its healthy path. A stale client's absence cannot block activation. Partial client death is repaired by replicas, and retrying after epoch change does not execute a second mutation.

**Review boundary:** No trusting a leader response alone, loose majority counters, or admitting arbitrary untrusted users as internal collectors.

<a id="pr-m03"></a>
### PR-M03: Connect observer staging to sealed handoff and activation

**Design:** U7, U9.  
**Prerequisites:** PR-M01, PR-O02, PR-57, PR-J04.

**Implement:** Use the original modeled sealing/terminal-recovery/activation protocol. Prepare non-voters, check terminal-state requirements, persist certificates in the shared journal, publish authoritative config notifications, and continue observer streams across the handoff boundary. Support voter replacement and three-to-five/five-to-three changes.

**Acceptance:** A staged replica cannot vote early; common-state readiness is not mistaken for local protocol recovery. Old members remain fenced after restart. New state preserves retries, revisions, leases, policy, and stream lineage. Total physical copies can exceed five during preparation while each active config obeys its cap.

**Review boundary:** No ad hoc dual-majority algorithm, no rollback after irreversible seal without protocol authority, and no majority-loss rescue by self-promoted observers.

<a id="pr-m04"></a>
### PR-M04: Implement conservative regional placement and quorum tuning

**Design:** U1.3, U7.1, U7.3.  
**Prerequisites:** PR-M03, PR-M02.

**Implement:** Validate hard failure-domain constraints, score leader/fast-quorum choices among current voters, and separately propose longer-term voter moves. Add hysteresis, minimum residence, dry-run explanations, operation identity, rate limits, and operator approval initially. Persist operation status outside exclusively affected tenant dependencies.

**Acceptance:** Noise cannot cause a reconfiguration storm. The optimizer rejects lower-latency layouts violating regional-loss budgets. Leadership/fast-quorum updates use ballots, not silent client quorum rewrites. Interrupted changes resume idempotently. Repair does not require the removed node or every observer to return.

**Review boundary:** No universal latency optimizer or automatic adoption of a configuration solely from measurements.

<a id="pr-m05"></a>
### PR-M05: Qualify client-aware membership under mixed failures

**Design:** U6-U7, U11.2.  
**Prerequisites:** PR-M02, PR-M03, PR-M04, PR-58.

**Implement:** Exercise competing operators, coordinator failure at every handoff stage, stale/isolated clients, delayed old completions, removed-disk resurrection, partial new-quorum installation, credential rotation, observer failure, and journal checkpoint/GC during handoff.

**Acceptance:** No two successors, mixed-epoch quorum, lost completed command, revision rollback, or authority resurrection. A stopped client or observer never becomes a reconfiguration ACK requirement. Record normal and failed handoff interruption separately from two-/three-message normal-operation latency.

**Review boundary:** No absolute availability guarantee when the authorized quorum is unavailable; report DR as a different workflow.

## P5. Combined release task

<a id="pr-q01"></a>
### PR-Q01: Produce the combined durable WAN/Kine qualification report

**Design:** U11; original release gates in Section 23.  
**Prerequisites:** PR-J07, PR-O06, PR-M05, PR-63, PR-64.

**Implement:** Run the fixed and changing membership matrices with realistic Kine object churn, native leases, current reads, observer watches, snapshots, and actual authentication. Publish complete build/config/source identifiers, raw measurements, state/history checks, safety-model coverage, known limits, and supported deployment profiles.

**Acceptance:** The strict journal profile passes the combined gate before PR-66 release. Report actual multi-group sync amortization and event-serving offload. State the chosen Kine pin and any fork patches. Observer read fences are enabled only if their own gate passes. Replay materialization is absent/disabled unless PR-J06 is accepted and included in the same applicable matrix. Preserve redb baseline and isolated Fjall comparison policy.

**Review boundary:** This is evidence assembly and release review, not permission to fix correctness by changing workload semantics or ignoring failing schedules.

## P6. Validation performed for this documentation package

The package includes a machine-readable extension graph and a validation report for its own Markdown structure, local links, explicit task anchors, and new-task dependencies. This does not assert that the proposed service compiles, that Mermaid was rendered by the official renderer, or that the original full task graph was revalidated. Those are implementation/qualification obligations above.

**Source basis:** [B1] and [B2] are the original design/plan identified in the design supplement; R1-R8 there supply the externally verified library facts. Task scope, prerequisite choices, and acceptance tests in this document are proposed engineering work, not upstream implementation claims.

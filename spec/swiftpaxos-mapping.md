# SwiftPaxos source mapping (task-19)

This document freezes how TupleSky's consensus vocabulary maps to its
sources and which parts are TupleSky extensions. It is normative for
`crates/coord-consensus` and for every later consensus task (task-20
onwards): a handler or field that is not in this table is either added
here with its source, or marked `[EXT]` with the design section that
requires it.

## Sources and pin

| Tag | Source | Use |
|---|---|---|
| S1 | Ryabinin, Gotsman, Sutra, *SwiftPaxos*, NSDI 2024, Sections 2-4 and Appendix A | Normative protocol rules: quorums, fast/slow learning, recovery |
| S2 | `imdea-software/swiftpaxos` at commit `35c69365f1c7737a08e237bfbaf828ee68897080` | Prototype reference for concrete handlers and fields (`swift/`, `replica/`); not a correctness oracle |
| X1-X3 | Upstream issues #1/#2 and the inspected `swift/recovery.go` (reviewed 2026-09-17) | Known implementation/invariant scheduling differences; neither a proved protocol flaw nor resolved |

File and line references below are to S2. The paper was unavailable in the
build environment when this mapping was written; rows marked `S1 (design)`
take the paper's rule as restated in the design (Sections 4.1-4.2) and
must be re-checked against the paper text at the task-19 review.

## Vocabulary

| Prototype (S2) | TupleSky | Notes |
|---|---|---|
| `CommandId{ClientId, SeqNum}` | `coord_types::CommandId` (BLAKE3 over retry key and canonical payload) | Section 4.4: identity never depends on transport |
| `int32` ballot, `Leader(ballot, N) = ballot % N`, `NextBallotOf` (`replica/replica.go:571-577`) | `coord_types::ids::Ballot{epoch, number, leader}` | Leader is an explicit field; ballots of different epochs are incomparable |
| `QuorumSet.AQ(ballot)` fixed majority containing the leader (`replica/quorum.go:243-248`, `fixedMajority`) | `BallotConfiguration::c2` with `fast_set` | C2: one fixed fast set per ballot, immutable within it |
| `ThreeQuarters` (`replica/quorum.go:32-42`) | `BallotConfiguration::c1` | C1: any `3N/4 + 1` set including the leader |
| `Majority` (`replica/quorum.go:18-28`) | `BallotConfiguration::slow_size` | `N/2 + 1`, always includes the leader in learning |
| phases `START, PRE_ACCEPT, ACCEPT, COMMIT` (`swift/defs.go`) | `Phase::{Start, PreAccept, Accept, Commit, Executed}` | `Executed` is explicit for the execution guard |
| `Dep []CommandId`, `Dep.Equals` (set equality) | `Vec<CommandId>`, `vote::same_set` | Sets, order-insensitive |
| `SHash` per-key hash log (`swift/dpath.go`) | `FastAck::path: Digest32` | Dependency-path digest; the concrete hash-log construction is task-21 |

## Normal operation

| Source handler / rule | Rule | Rust item | Test |
|---|---|---|---|
| `handlePropose` (`swift/swift.go`): only fast-set members send `MFastAck`; leader assigns `Seqnum` | A fast acknowledgement counts only from a fast-set member; only the leader carries a sequence number | `VoteSet::add` -> `VoteError::NotInFastSet`, `VoteError::ForgedProposal` | `tests/model.rs::learning_is_order_independent_and_rejects_non_c2_evidence` |
| `MsgSet.Add` (`replica/mset.go`): `!q.Contains(repId)` ignored; keyed by replica | Observers and strangers never count; one vote per replica per ballot | `VoteError::NotAVoter`, `VoteError::Duplicate` `[EXT: strict rejection; the prototype silently overwrites a pre-leader duplicate]` | same |
| `MsgSet.Add` size rule: `len(msgs) >= q.Size()-1 && leaderMsg != nil` | The leader's message is a member of every quorum; nothing is learned without it | `VoteSet::learned` returns `None` without the proposal | same (`no-leader-proposal`) |
| client `accept` (`swift/client.go`): `SHashesEq(leader.Checksum, ack.Checksum)` over the fast quorum | Fast learning: fast-set members whose dependency path equals the leader's | `Learned::Fast` | same (`fast-path-with-invalid-evidence`) |
| `acceptFastAndSlowAck` (`swift/swift.go` `newDesc`): `Dep == nil \|\| leaderDep.Equals(dep)` over the slow quorum | Slow learning: adoption acknowledgements, or fast acknowledgements with the leader's dependency set, from a majority including the leader | `Learned::Slow` | same (`slow-path-path-disagreement`) |
| `fastAckFromLeader`: `desc.phase = ACCEPT` after the leader's ack; `TODO: ∀ id' ∈ d. phase[id'] ∈ {ACCEPT, COMMIT}` unenforced | Direct dependencies must be ACCEPT or COMMIT before entering ACCEPT | `guard_accept` `[EXT: enforced; S1 (design) Section 4.7]` | `tests/model.rs::guards_reject_premature_phases_and_the_oracle_detects_their_removal` |
| `getFastAndSlowAcksHandler`: `desc.phase = COMMIT` on quorum; `deliver` waits for delivered deps | Dependencies committed before COMMIT; executed before finalized execution | `guard_commit`, `guard_execute` `[EXT: enforced at COMMIT; the prototype only checks delivery]` | same |
| `fastAckFromLeader` `afterPropagate` (waits for `desc.propose != nil`) | Leader evidence before the payload is held, never applied to a placeholder | `GuardViolation::DependencyUnknown`; actor integration is task-20 | same |
| `commonCaseFastAck`: `r.ballot != msg.Ballot -> return` | Votes of another ballot are ignored | `VoteError::WrongBallot` | learning test |
| `MLightSlowAck` from every non-leader after adopting the leader order | Adoption acknowledgement | `SlowAck`, `Vote::Slow` | learning test |
| Leader `MLightSlowAck` | Never: the leader's message is its proposal | `VoteError::LeaderSlowAck` | `VoteSet::add` |

## Recovery

| Source handler / rule | Rule | Rust item | Test |
|---|---|---|---|
| `handleNewLeader`: `r.ballot >= msg.Ballot -> return`; status `RECOVERING`, stop descriptors, `fillNewLeaderAckN` | A report is produced only for a higher ballot, at a cut where no old-ballot transition is admitted | `RecoveryReport{ballot, committed_ballot, entries}`; the cut discipline is Section 4.8 `[EXT]`, actor in task-20/task-23 | recovery test |
| `MNewLeaderAckN.Cballot` and `handleNewLeaderAckNs`: `U` = reports at `maxCbal`; only their `ACCEPT`/`COMMIT` entries are adopted | Source of state is the highest synchronized ballot among a majority of reports; lower-ballot state is never merged | `select` -> `SyncDecision::source_ballot`, entries only from source-ballot reports | `tests/model.rs::recovery_selection_is_source_defined_and_order_independent` |
| `handleNewLeaderAckNs`: `phases[cmdId] = phase` (map overwrite in arrival order) | `[EXT: replaced]` selection is a function of the report set; equal candidates merge by phase class, differing accepted candidates stop recovery | `RecoveryError::IncompatibleAccepted`, order-independence asserted over all permutations | same (`incompatible-accepted-candidates`) |
| `reinitNewLeaderAckNs`: `Majority` | A majority of reports from voters, one per replica | `RecoveryError::{InsufficientReports, DuplicateReport, NotAVoter, WrongBallot}` | same |
| `fillNewLeaderAckN`: proposes without a descriptor reported as `ACCEPT` with `NOOP` and empty deps | `[EXT: rejected]` a half-initialized command is never reported as accepted; an accepted entry without a durable payload fails recovery | `ReportEntry::payload_present`, `RecoveryError::HalfInitialized` | same (`half-initialized-entry`) |
| `handleSync`: commands with a propose but absent from Sync are re-proposed; phases below ACCEPT become ACCEPT after adoption | Pre-accepted-only commands are re-proposed under the new ballot | `SyncDecision::reproposed` | same |
| `handleSync`: `r.FQ = r.qs.AQ(r.ballot)` | The fast set changes only with the ballot | `BallotConfiguration` is per ballot | quorum test |
| `handleSync`: leader sends `MFastAck`/`MReply`, followers `MLightSlowAck` for adopted commands | Publication after Sync follows the durable table below | `Publication` | publication test |

## Promises and configuration guards (task-20)

| Source handler / rule | Rule | Rust item | Test |
|---|---|---|---|
| `handleNewLeader`: `r.ballot >= msg.Ballot -> return` | Only a strictly higher ballot is promised; a promise in flight already bounds later requests | `BallotState::on_new_leader` -> `PromiseRejection::NotHigher` | `tests/ballot.rs::promise_reply_waits_for_the_row_and_every_batch_before_the_cut` |
| `handleNewLeader`: `r.ballot = msg.Ballot` (volatile) | `[EXT]` the promise is a durable row (`protocol_v1`, `PromiseRecordV1`) written before the reply; recovered at boot | `rows::promise_update`, `BallotState::recover`, `coord_storage::protocol::read_promise` | `tests/ballot.rs::old_messages_cannot_lower_a_recovered_promise` |
| `MNewLeaderAckN` sent right after state change | `[EXT]` the reply requires the promise row and every batch submitted before the cut (Section 4.8) | `PromiseEffects::reply.requires` through the logical outbox | same as first row |
| `MNewLeaderAckN.Cballot` | The synchronized ballot travels with the promise | `ProtocolMessage::Promise::synced`, `BallotState::mark_synced` | same |
| `MNewLeader.Replica` is the candidate, ballot leader derived from it | The ballot's leader must be the sender | `PromiseRejection::LeaderMismatch` | `tests/ballot.rs::wrong_configuration_identity_never_votes` |
| (no epochs in the prototype) | `[EXT]` a ballot of another epoch, a non-voter sender or a non-voting role never votes (Section 10) | `PromiseRejection::{WrongEpoch, NotAVoter, NotVoting}` | same |
| `stopDescs` / `repchan.stop` during recovery | `[EXT]` vote-producing callbacks completing after a same-boot election update bookkeeping but never send (Section 4.8) | `coord_core::outbox::Outbox::release` with `BallotState::promised` | `tests/ballot.rs::a_same_boot_election_fences_obsolete_vote_callbacks` |
| `getCmdDescSeq` / `getDepAndHashes` / `keyInfo` | Initialization binds payload, computes dependencies and publishes the index in one transition; a descriptor without payload is a placeholder invisible to lookups and guards (Section 4.7) | `CommandTable::{expect, initialize, conflicts, phase_of}` | `tests/ballot.rs::initialization_publishes_atomically_and_placeholders_are_invisible` |

## Dependency graph and path evidence (task-21)

| Source handler / rule | Rule | Rust item | Test |
|---|---|---|---|
| `HashLog.Append` (`swift/dpath.go`) | Per-key hash chain over commands in local order; the head is the path evidence a fast acknowledgement carries | `graph::PathLog::append`, `chain`, `CommandRecord::{paths, path}` | `tests/graph.rs::direct_set_equality_differs_from_full_path_evidence` |
| `HashLog.Update` / `recordLeaderHash` / `updateLogs` / `pendingUpd` | The leader's sequence number and digest synchronize the follower's prefix; early leader evidence is applied at the append; the head is recomputed over the pending suffix (no compression) | `PathLog::sync`, `CommandTable::record_leader_path` | `tests/graph.rs::leader_synchronization_aligns_follower_paths` |
| `SHashesEq` (per-key set equality of checksums) | Combined evidence independent of key listing order | `graph::combined_path` | direct-set test |
| client `accept`: checksum equality, not `Dep.Equals` | Direct-set equality is not path equality: equal `deps` with different prefixes yield different evidence and no fast path | `vote::VoteSet::learned` (path), test | direct-set test |
| `deliver`: waits until every `desc.dep` is delivered (direct only) | `[EXT]` exact transitive closure, budgeted per turn (Section 18.2), stopping on a placeholder/unknown dependency | `graph::ClosureCursor::step`, `CommandTable::{closure_start, closure_step}` | `tests/graph.rs::closure_traversal_is_exact_and_incremental` |
| `descPool` / `MaxDescRoutines` / `HISTORY_SIZE` | `[EXT]` bounded capacity refuses new work; unresolved acceptance is never evicted; only executed records retire | `CommandTable::{with_capacity, retire}`, `InitError::Backpressure`, `RetireError` | `tests/graph.rs::backpressure_refuses_new_work_without_deleting_unresolved_acceptance` |
| `history[]` (volatile) | `[EXT]` required dependency state is a `protocol_v1` row per command (Section 5.2) | `rows::{dependency_key, dependency_update, decode_dependency}` | `tests/graph.rs::dependency_rows_round_trip` |

## Leader proposal handlers (task-22)

| Source handler / rule | Rule | Rust item | Test |
|---|---|---|---|
| `ProposeChan` case in `run`: `getDepAndHashes`, `getCmdDescSeq(..., seq = leader)` | An admitted request is initialized atomically with conservative domain dependencies and path evidence before anything is published | `leader::Leader::on_admitted` -> `CommandTable::initialize`, `CONSERVATIVE_KEY` | `tests/leader.rs::proposals_are_published_with_exact_durable_support_and_match_the_model` (golden `fixtures/golden/leader_normal.json`) |
| `handlePropose` on the leader: `MFastAck{Dep, Checksum, Seqnum}` sent to all; `r.seqnum++` | The leader proposal carries its sequence number and goes to every other voter | `ProtocolMessage::Proposal(FastAck{seqnum: Some})` | same |
| `r.repchan.reply` / `MReply` to the client | `[EXT]` the leader reply goes to the trusted frontend and requires the durable proposal/payload state (Section 5.1, "leader reply used for learning") | `ProtocolMessage::LeaderReply`, `PendingSend.requires = [batch]` | same |
| (volatile `proposes`, `history`) | `[EXT]` payload, dependency and proposal rows persisted in one batch (`payload_v1`, `protocol_v1`) | `rows::{payload_update, dependency_update, proposal_update}` | same |
| `r.proposes[cmdId] = propose` (last writer) | `[EXT]` one retry key binds one payload; a different payload under the same key is `RequestIdentityConflict`; a repeated request is a no-op (Section 4.4) | `Rejection::{RequestIdentityConflict, Duplicate}` | `tests/leader.rs::reordered_and_duplicate_requests_cannot_bind_conflicting_payload` |
| leader `desc.phase = ACCEPT` via `fastAckFromLeader` on itself | The leader adopts its own order only once the proposal is durable and every dependency is at least ACCEPT; COMMIT/execution are the learner's | `Leader::advance_pending` under `guard_accept` | `tests/leader.rs::premature_phases_are_blocked_while_dependencies_lag` |
| `r.status != NORMAL -> return` in `handlePropose` | A higher promise stops proposing; unreleased proposals under the old ballot are fenced | `Leader::is_leading`, outbox release with `BallotState::promised` | `tests/leader.rs::a_higher_promise_stops_proposing_and_fences_unreleased_proposals` |
| `commonCaseFastAck` / `handleLightSlowAck` on the leader | Acknowledgements are collected per command; learning is not decided here (task-24) | `Leader::collect` -> `VoteSet` | `tests/leader.rs::votes_are_collected_but_never_learned_here` |

## Follower vote and adoption handlers (task-23)

| Source handler / rule | Rule | Rust item | Test |
|---|---|---|---|
| `handlePropose` on a follower: `!r.FQ.Contains(r.Id) -> no MFastAck`; else `MFastAck{Dep, Checksum}` | Only a fast-set member votes fast; the vote is published to every other voter and the frontend once payload, dependencies and path evidence are durable (Section 5.1) | `follower::Follower::on_admitted`, `PendingSend.requires = [batch]` | `tests/follower.rs::fast_votes_wait_for_durable_payload_dependencies_and_path` |
| `fastAckFromLeader`: `afterPropagate.Call` (waits for `desc.propose`) | A leader proposal before the payload is held; the placeholder is invisible to lookups and guards | `Follower::on_proposal` -> `CommandTable::expect`, `HeldProposal` | `tests/follower.rs::a_proposal_before_the_payload_is_held_against_an_invisible_placeholder` |
| `fastAckFromLeader`: `desc.phase = ACCEPT`, `desc.dep = dep` when `neq`; `TODO` guard | Adoption of the leader's order requires every dependency at least ACCEPT (explicit guard); the adopted order is persisted before the slow acknowledgement | `Follower::advance_pending` -> `CommandTable::accept`, `dependency_update` | same, and `tests/follower.rs::equal_direct_dependencies_are_not_learning_and_guards_are_explicit` |
| `MLightSlowAck` after adoption (`sendSlowAck`) | The slow acknowledgement goes to every other voter and the frontend requiring the adoption batch | `ProtocolMessage::SlowAck`, `Follower::publish_to_voters_and_frontend` | proposal-before-payload test |
| `recordLeaderHash` on the leader's `MFastAck` | The leader's per-key path digests synchronize the follower's logs when the proposal arrives | `FastAck::paths`, `CommandTable::record_leader_path` | `tests/follower.rs::conflict_arrival_permutations_converge_on_the_leader_order` |
| `commonCaseFastAck`: `msg.Ballot != r.ballot -> return`; leader identity by `r.leader()` | A proposal from a non-leader or another ballot is foreign | `FollowerRejection::ForeignProposal` | same |
| (volatile descriptors) | `[EXT]` after a crash the table is rebuilt from durable dependency rows; a durable vote is a fact, an undurable one was never sent (Section 4.7) | `CommandTable::restore`, `Follower::recover` | `tests/follower.rs::a_crash_between_state_and_vote_preserves_the_learning_obligation` |
| `acceptFastAndSlowAck` dep equality | Equal direct dependencies are collected as evidence, never acted on; learning is task-24 | `Follower::collect` -> `VoteSet` | equal-direct-deps test |

## Slow learning and ordered application (task-24)

| Source handler / rule | Rule | Rust item | Test |
|---|---|---|---|
| `getFastAndSlowAcksHandler` on the slow set (`slowPathH`, `SQ`): `desc.phase = COMMIT` | Conservative learning: the leader's order adopted by a majority including the leader, and every dependency committed (guard) | `VoteSet::learned_slow`, `learner::Learner::commit_learned` | `crates/coord-storage/tests/cluster.rs` (3/5 voters) |
| `deliver`: execute once every `desc.dep` is delivered; leader `Seqnum` | Execution in leader sequence order once every dependency executed, at the next execution position | `Learner::next_executable` | same |
| `desc.cmd.Execute(r.State)` then `repchan.reply` | `[EXT]` application goes through the common materializer with the retry binding; the outcome is sealed into `EstablishedResult` and only then published; no single reply establishes anything (Sections 4.5, 17.4) | `Learner::established`, `coord_storage::apply::Applier`, `Effect::Established` | `cluster.rs::one_leader_response_cannot_establish_success` |
| (no watches in the prototype) | `[EXT]` a revision's complete event set reaches watches only after the durable commit | `Applier::apply` -> `WatchHub::publish` | `cluster.rs::watch_events_follow_irrevocable_application` |
| `optExec` / speculative delivery on the leader | Out of scope here (task-29); the learner never executes on a proposal alone | review boundary | one-leader-response test |

## Recovery summaries and payload transfer (task-25)

| Source handler / rule | Rule | Rust item | Test |
|---|---|---|---|
| `fillNewLeaderAckN`: iterates volatile `cmdDescs` | `[EXT]` the report is built from the actor's durable ledger (records whose batches completed `JournalDurable`), never from in-memory phases or a lagging projection (Section 4.8) | `summary::DurableLedger::report`, `Follower::report`, `Leader::report` | `tests/recovery.rs::reports_come_from_durable_state_at_the_cut_not_in_memory_phases` |
| `MNewLeaderAckN` as one message | `[EXT]` bounded verified pages binding replica, ballots, page, total and digest; an incomplete or corrupt transfer never counts (Section 19.3) | `summary::{paginate, ReportPage, ReportAssembler}` | `tests/recovery.rs::pages_assemble_only_when_complete_and_verified` |
| `fillNewLeaderAckN`: `NOOP` `ACCEPT` for proposals without descriptors | `[EXT: rejected]` a command known by identity only is absent from the report; its payload is fetched and rehashed against the identity before it exists (Section 4.7) | `Follower::{missing_payloads, request_payloads}`, `ProtocolMessage::{PayloadRequest, PayloadResponse}`, `FollowerRejection::PayloadIdentityMismatch` | `tests/recovery.rs::a_missing_payload_is_fetched_and_rehashed_never_fabricated` |
| `handleNewLeader`: `stopDescs`, status `RECOVERING` | A promise for a higher ballot fences late old-ballot proposals and unreleased sends; the durable rows and promise survive a crash and reproduce the report | `FollowerRejection::StaleBallot`, `Follower::recover` | `tests/recovery.rs::old_ballot_work_is_held_across_recovery_and_required_state_survives_a_crash` |
| `handleNewLeaderAckNs` phase merge | Legal phase differences select; incompatible accepted candidates from real reports are diagnosed | `recovery::select` over `DurableLedger` reports | `tests/recovery.rs::legal_phase_differences_select_and_incompatible_candidates_are_diagnosed` |

## Durable publication obligations (design Section 5.1)

| Publication | Required durable records | Rust item |
|---|---|---|
| New promise / recovery response | promised ballot, complete recovery state at the cut | `Publication::PromiseOrRecoveryResponse` |
| Fast acknowledgement | payload, vote, path/order evidence, stable prerequisite dependencies | `Publication::FastAck` |
| Leader reply used for learning | recoverable proposal/payload state | `Publication::LeaderReply` |
| Slow acknowledgement | adopted leader order, prerequisite acceptance | `Publication::SlowAck` |
| Finalized application result | established command, atomic application/deduplication outcome | `Publication::FinalizedResult` |
| Checkpoint readiness | checkpoint, identity, recovery-floor metadata | `Publication::CheckpointReady` |

The prototype has no durability: every row above is `[EXT]` required by
Sections 5.1 and 4.8. Issuing a write or producing a message establishes
nothing; only completion events do (task-04 barriers, task-20 wiring).

## Quorum table (design Section 4.2)

| Voters | Slow majority | C2 fixed fast set | C1 alternative |
|---:|---:|---:|---:|
| 3 | 2 | 2 | 3 |
| 5 | 3 | 3 | 4 |

`BallotConfiguration::{slow_size, fast_size}`; frozen by
`tests/model.rs::quorum_policy_matches_the_design_table`. Any two C1 fast
quorums intersect in a majority (`fast_quorums_intersect_in_majority`);
C2 has a single fast set.

## Extensions and rejected prototype behaviors

| Item | Status | Reason |
|---|---|---|
| Dependency-phase guards at ACCEPT, COMMIT and execution | `[EXT]` enforced | Section 4.7; the prototype leaves the ACCEPT guard as a `TODO` (X1) |
| Strict duplicate/observer rejection with a reason | `[EXT]` | Section 4.3: never count an identity twice |
| Order-independent Sync selection with an agreement check | `[EXT]` replaces map overwrite | Section 4.9 (X2, X3) |
| No fabricated `NOOP`/`ACCEPT` placeholder for un-initialized proposals | `[EXT]` rejected | Section 4.7: placeholders cannot masquerade as processed commands |
| Durable cut and publication obligations | `[EXT]` | Sections 4.8, 5.1 |
| Epoch on every ballot and configuration | `[EXT]` | Section 10.5; membership is outside the paper |
| Speculative result path, Kine collector, observers | out of scope here | Sections 4.5, 6.6, 6.7; later tasks |

## Bounded models and counterexamples

`crates/coord-consensus/tests/model.rs` explores every permutation of the
vote sets and report sets above, and a guard schedule from Section 21.6
with the guard enforced and deliberately removed. Frozen results live in
`crates/coord-consensus/fixtures/counterexamples/`:

* `learning_scenarios.json`: fast path with interleaved invalid evidence,
  slow path on path disagreement, the arbitrary-fastest-majority rejection
  and the missing-leader case;
* `recovery_scenarios.json`: legitimate phase differences, the
  highest-phase-wins result that is *not* chosen (recorded as the
  counterexample), incompatible accepted candidates and a half-initialized
  entry;
* `guard_removed_counterexample.json`: the trace with the guard enforced
  (violation at the leader-evidence step) and removed (the oracle finds a
  command committed while its dependency is below ACCEPT).

## Not claimed

Finite models are not a proof; they are regressions of the source rules
and of the extensions. Upstream implementation/invariant differences are
neither proved fundamental nor assumed resolved (Section 4.10). No
performance or optimization claim is made from these models.

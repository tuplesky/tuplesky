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

## Recovery selection and activation (task-26)

| Source handler / rule | Rule | Rust item | Test |
|---|---|---|---|
| `recover` channel in `run`: `MNewLeader` to all, `handleNewLeader` on self | The candidate promises itself durably first; the `NewLeader` to the others requires that row | `Follower::campaign` | `tests/activation.rs::learned_outcomes_survive_a_lost_leader_and_lost_commit_notifications` |
| `reinitNewLeaderAckNs` (majority), `handleNewLeaderAckNs` | Selection runs on a majority of complete reports from promising voters (own included) | `campaign::Campaign::try_select` -> `recovery::select` | same; `tests/activation.rs::permuted_reports_give_the_same_selection` |
| `sender.SendToAll(sync)` right after selection | `[EXT]` the selection is bound to the ballot in `protocol_v1` before any Sync is published; after a crash it is republished, never reselected (Section 4.9) | `rows::SyncRecordV1`, `Campaign::{bound, resumed}`, `Follower::resume_campaign` | `tests/activation.rs::a_crash_after_binding_the_sync_republishes_the_same_result` |
| `handleSync`: `r.ballot > msg.Ballot -> return`; adopts higher ballots implicitly | `[EXT: stricter]` a Sync is adopted only for exactly the promised ballot and only from its leader | `Follower::on_sync` -> `FollowerRejection::SyncRejected` | `tests/activation.rs::competing_campaigns_and_delayed_replies_cannot_establish_divergence` |
| `handleSync`: `r.cballot = msg.Ballot`, `r.FQ = AQ(ballot)`, descriptors reset, entries installed in dependency order | The synchronized ballot is persisted, the ballot's fast set activated, ballot-scoped votes reset, entries installed under the guards (missing payloads fetched first) | `Follower::{on_sync, advance_sync}`, `BallotConfiguration::c2_default` `[EXT: default fast-set rule until task-m01]` | learned-outcomes test |
| `handleSync` on the leader: `MFastAck` per Sync command; re-propose the rest | The new leader re-proposes every Sync entry with the selected dependencies in dependency order, then the re-proposed commands | `Leader::from_recovered`, `Leader::repropose` | learned-outcomes test |
| `handleSync` after a promise; voting messages of the new ballot delivered before the Sync | `[EXT]` a proposal or acknowledgement of the promised-but-unsynchronized ballot is held (bounded by the table capacity) and replayed after the Sync instead of being dropped; delivery is not ordered across peers | `Follower::{stash_until_sync, replay_awaiting}` | learned-outcomes test (reordered inboxes) |
| Executed commands after a restart | `[EXT]` the executed identities are durable with the application rows (Section 6.5); recovery restores the execution frontier and a re-proposal never regresses a learned or executed phase | `Follower::restore_execution`, `CommandTable::restore_executed`, `Follower::advance_pending` | learned-outcomes test |
| (roles are fixed in the prototype) | `[EXT]` roles convert through the recovered state: learned outcomes, execution frontier, payloads and bindings survive; a deposed leader carries a pending Sync into its follower role | `role::RecoveredState`, `Leader::{deposed, into_recovered}`, `Follower::{from_recovered, into_recovered, won}` | competing-campaigns test |

## Bounded speculation and result release (task-29)

| Source behavior | Rule | Rust item | Test |
|---|---|---|---|
| leader `optExec`: execute at proposal time, reply to the client before commit (`deliver` with `r.optExec && r.Id == r.leader()`) | `[EXT: stricter]` the leader computes a *tentative* result over a disposable overlay (durable view plus the tentative plans of every earlier unexecuted proposal in its order); nothing tentative reaches state, events, watches, sessions or credentials, and the overlay is bounded and discarded on any role change | `coord_storage::speculate::{speculate, Overlay, SpeculationLimits}`, `Leader::{next_speculable, speculated, decline_speculation, tentative}` | `crates/coord-storage/tests/recovery.rs::released_results_precede_materialization_and_equal_the_established_ones` |
| client learns from the leader's reply once the fast or slow quorum agrees | The release gate: a tentative result leaves the replica only when the command and its whole prefix are committed by the learning predicate, so command, closed predecessor order, authorization (the view the plan was authorized on, unchanged by a KV-only prefix) and the exact result are determined by durable evidence; the released capability carries the established result and the exact response, never events | `speculation::ReleaseGate`, `coord_core::capability::ReleasedResult`, `Effect::Released` | same; `a_fast_result_followed_by_a_leader_crash_before_commit_propagation_recovers_the_same_outcome` (released before materialization, reproduced by recovery) |
| (no such check in the prototype) | `[EXT]` a materialized outcome that disagrees with a released tentative result halts the replica (`LearnError::Speculation`); it never serves two answers | `Speculation::reconcile`, `Leader::applied` | forced-slow comparison (released equals established on both paths) |
| speculation across leaders | `[EXT]` a deposed or lost leader's tentative results never surface: the new ballot may order the same commands differently and the old leader released nothing that was not learned | `Speculation::clear` on role change, gate stops at the first unlearned proposal | `tentative_results_of_a_lost_leader_never_surface` |
| (n/a) | Only KV operations at the current revision without leases are speculated; leases, explicit-revision reads, compaction, Kine and internal commands, and invocations the retry layer would not admit as new, take the finalized path | `speculate::speculable`, `SpeculationRefused` | released-results test (lease operations are never released early) |

No extra WAN commit phase is added, the learning predicates are unchanged,
and durability is not weakened: a speculative release needs exactly the
evidence a final one needs, minus the materialization round trip.

## Full fast-path learning evidence (task-28)

| Source handler / rule | Rule | Rust item | Test |
|---|---|---|---|
| client `accept` (`swift/client.go`): `SHashesEq(leader.Checksum, ack.Checksum)` over `FQ`, `handleFastAndSlowAcks` | Fast learning: the leader's proposal and, from every other member of the ballot's fixed C2 fast set, a fast acknowledgement whose path evidence equals the leader's; the learned dependencies are the leader's, exactly as on the slow path | `VoteSet::learned` -> `Learned::Fast`, `Learner::commit_learned` with `LearningMode::Full` | `tests/model.rs::learning_is_order_independent_and_rejects_non_c2_evidence` (mixed ballot and epoch, duplicates, forged proposals), `crates/coord-storage/tests/recovery.rs::forced_slow_and_fast_learning_yield_equal_results` |
| replica `getFastAndSlowAcksHandler`: `desc.phase = COMMIT` on either quorum | Replicas commit through the same predicate; the established result records which path learned it (measurement only); a forced slow path (`LearningMode::SlowOnly`) gives identical orders, rows, retained results and digests | `Learner::{set_mode, learned_fast}`, `EstablishedResult::fast_path` | forced-slow test |
| `fastAckFromLeader`: `desc.dep = dep` on adoption; `recordLeaderHash` | `[EXT]` adoption records the leader's path evidence in the dependency row, so a recovery report carries the replica's own path for a pre-accepted command and the leader's once adopted | `CommandTable::adopt`, `Follower::{advance_pending, advance_sync}`, `ReportEntry::{path, keys}`, `SyncEntry::path` | possible-fast tests |
| `handleNewLeaderAckNs` keeps only `ACCEPT`/`COMMIT` (the leader's own rows cover its fast replies while it lives) | `[EXT: paper-derived]` possible fast decisions: when the source leader is not among the reports, a command every reporting member of the source ballot's fast set pre-accepted with the same path may have been learned fast (any majority contains such a member) and is adopted with that order; a member that never saw it, saw a different path, ordered an adopted conflicting command after it, or reached one through a different path proves no fast decision was possible, and the command is re-proposed. No highest-phase priority | `recovery::select` -> `possible_fast_decisions` | `tests/model.rs::possible_fast_decisions_are_recovered_from_the_fixed_fast_set`, `crates/coord-storage/tests/recovery.rs::a_fast_result_followed_by_a_leader_crash_before_commit_propagation_recovers_the_same_outcome` |
| `handleSync` install | The bound selection carries the path evidence it was chosen with; a restart of the candidate republishes it unchanged | `Follower::advance_sync`, `Campaign::resumed` | fast-result-then-crash test (restart tail) |

The paper's recovery appendix was not reachable while this was written;
the possible-fast rule is the Fast Paxos recovery rule instantiated for the
fixed C2 set (fast set and majority intersect in at least one member) and
is recorded as `[EXT: paper-derived]` until checked against the appendix.
Speculative overlays and public fast responses (task-29) are not part of
this task: nothing here releases a result before materialization.

## Crash-recovery qualification on the real engine (task-27)

`crates/coord-storage/tests/recovery.rs` runs the production machines and
the common materializer over `RedbEngine` on a fault-injecting backend
(fidelity level B) under a seeded logical network (level A). A crash
freezes the backend and derives the surviving image (torn, reordered
unsynced tail included); the next process rebuilds its role from the
projection alone through `coord_storage::protocol::read_protocol`.

| Schedule (design Sections 5.1, 12.2, 21.6) | Property | Test |
|---|---|---|
| Lost leader, follower restart with a torn tail, campaign by a voter that never stored the payloads | Acknowledged outputs, retry digests and lease/policy rows survive; nothing executes twice; client retries answer identically; the observed history is linearizable | `acknowledged_outputs_retry_digests_and_lease_state_survive_a_lost_leader` |
| Leader isolated from both followers | A minority establishes nothing and holds no unacknowledged rows; the majority recovers and serves; the stale leader's proposals are refused | `a_minority_cannot_write` |
| Dependency rows deliberately omitted on one voter | The restart check (executed identities versus application frontier) fails closed; a replica ignoring it halts on `PositionMismatch`; the oracle rejects the recovered majority's answers | `a_deliberately_omitted_durable_record_is_detected` |
| Crash after the Sync row is bound and published; crash before it is durable | The bound decision is republished, never reselected under the same ballot; without a bound row a valid new ballot is entered | `a_crash_after_publishing_the_sync_preserves_the_same_ballot_choice` |
| Permuted promise/report deliveries | The same bound selection on every seed | `permuted_report_deliveries_select_the_same_result_on_the_real_engine` |
| Follower restart at every consensus persist batch (before it reaches the engine, and after it is durable but before the completion event), then recovery under the restarted node's ballot | Every node reproduces the acknowledged order, rows and retry records | `follower_restarts_at_every_persist_boundary_reproduce_the_acknowledged_outcomes` |

Extensions the qualification forced (focused review, not hidden):

| Source behavior | Rule | Rust item |
|---|---|---|
| (no restart in the prototype) | `[EXT]` a restarted replica recovers its promise, dependency rows, bound selections, payloads and executed identities from the projection; the executed identities must account for the application frontier | `protocol::read_protocol`, `RecoveredProtocol::{resumable_sync, executed_through}` |
| `handleNewLeaderAckNs` selects from identities | `[EXT]` a candidate that lacks the payload of a selected command fetches it from the reporting voters before the result is bound; the new leader re-proposes from payloads | `Follower::advance_campaign`, `Campaign::{promised, payloads_requested}` |
| payload transfer | `[EXT]` payload requests and responses are published under the promised ballot: they are not voting transitions and a promise for a higher ballot must not fence them | `Follower::{request_payloads, serve_payloads}`, `Leader::serve_payloads` |

Precise coverage: the reference `StoreWorker` over redb with protocol and
application rows in one projection. Journal-first composition (task-j03,
task-j05), quorum-certified checkpoints (task-30) and lagging-replica
catch-up (task-50) are qualified by those tasks; crash points here are the
consensus persist batches (the materializer's own crash matrix is tasks
11/12/15). Reference redb evidence does not qualify raft-engine.

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
| Client-side learning (`swift/client.go` `accept`, `acceptFastAndSlowAck`) at the trusted collector | `coord-collector` `Collector::on_evidence` over `VoteSet::learned` (task-33); release additionally requires the leader's release gate | `[EXT]` on top of the source predicate; `spec/collector-v1.md` |
| Kine collector epoch integration, observers | out of scope here | Sections 4.5, 6.6, 6.7; later tasks |

## Quorum-certified checkpoint floors (task-52)

Outside the paper entirely: SwiftPaxos says nothing about forgetting, and
the prototype never trims. The protocol is modeled here because it rests
on the same intersection argument the paper's quorums do, and because
getting it wrong loses obligations rather than availability.

| Item | Status | Where |
|---|---|---|
| `FloorCandidate`: configuration, executed prefix, subject digest | `[EXT]` | Section 5.3 |
| `Readiness`: a durable promise never to vote from below a position, recorded only once the checkpoint it names is durably held | `[EXT]` | Section 5.3; possession alone certifies nothing |
| `ReadinessLedger`: one row per voter; moves up, never down, never two subjects at one position | `[EXT]` | the rule uniqueness rests on |
| `activate`: a majority of the configuration's voters, all for one candidate | `[EXT]` | Section 5.3 |
| `discover`: the highest readiness a majority reports | `[EXT]` | the intersection is between two majorities of one voter set |
| `FenceVerdict::Retained`: a message at or below a held floor is answered from the retained outcome | `[EXT]` | Section 5.3: pre-floor messages cannot resurrect forgotten state |
| `FloorLedger`: the held floor, never lowered | `[EXT]` | Section 5.3 |
| Ballot or leader as part of a floor | `[EXT]` rejected | a floor belongs to a configuration and outlives every term in it |
| Copying a snapshot to a majority as activation | `[EXT]` rejected | Section 5.3, and the frozen counterexample |

The durable side is `coord_checkpoint::floor` (task-53):
`CheckpointReadinessV1` is the promise row, `ActivatedFloorV1` the
certificate, and the `TrimmedFloorV1` it yields is task-51's floor
unchanged -- so the fence, the bounded trimming and the deletions are the
same code on both paths.

## Sealed membership handoff (task-54)

Also outside the paper: SwiftPaxos is fixed-membership, and the prototype
has no configuration change at all. Modeled here because the composition
it has to be safe against is the one the paper's recovery already
handles -- a coordinator that dies mid-operation and is replaced -- and
because the failure mode is losing obligations, not availability.

| Item | Status | Where |
|---|---|---|
| `Transition`: the configuration left, the one entered, and a subject binding the domain and the exact successor incarnations | `[EXT]` | Section 10.3.2 |
| `Stance` and `StanceLedger`: one old voter, one stance per transition, never reversed | `[EXT]` | the rule a seal and a cancellation not both certifying rests on |
| `SealCertificate`: a majority of the old voters have fenced ordinary voting for the whole old configuration, across ballots | `[EXT]` | Section 10.3.2; handoff-only recovery stays possible |
| `CancellationCertificate`: a majority refused instead, available only before any seal | `[EXT]` | Section 10.3.2 |
| `TerminalCertificate`: selected only after the seal, from a majority agreeing on one root and one successor | `[EXT]` | Section 4.8, 10.3.2; mixed roots refused, never merged |
| `ActivationCertificate`: a majority of the *successor* installed that exact root | `[EXT]` | Section 10.3.2 |
| `resume`: the stage a replacement coordinator continues from, chosen from durable records alone | `[EXT]` | Section 10.3.2: not the last in-memory lifecycle label |
| `HandoffError::FencedByAnother`: a fence another transition left is still a fence | `[EXT]` | one transition per domain |
| A lifecycle label, a timeout or a missing local record as evidence | `[EXT]` rejected | Section 10.3.2 |
| An applied KV view or a closed frontend as terminal state | `[EXT]` rejected | Section 10.3.2, and the frozen counterexample |
| Raft joint consensus | out of scope | Section 10.3.2: stop-and-transfer, explicitly modeled |

The durable side is task-55: `rows::SealRecordV1` at key `epoch \|\| 0x04`
in `protocol_v1`, written once and never rewritten or removed.
`BallotState::seal` produces it and publishes the seal report through the
logical outbox requiring the row *and* every batch submitted before the
cut, exactly as `on_new_leader` does -- the Section 4.8 rule, applied to
a fence. `BallotState::recover_sealed` reads it back, and while it is
there `on_new_leader` refuses every ballot of the configuration:
`PromiseRejection::Sealed`. A higher ballot is not an exception to a
fence.

| Item | Status | Where |
|---|---|---|
| `SealRecordV1`, one per replica per configuration | `[EXT]` | Section 10.3.2 |
| Seal report published at the authoritative cut | `[EXT]` | Section 4.8; work learned immediately before sealing is inside it |
| `PromiseRejection::Sealed` for every ballot of a sealed configuration | `[EXT]` | Section 10.3.2 |
| A seal cleared by a timeout, a missing local row or a retry | `[EXT]` rejected | Section 10.3.2; no method clears one |
| A seal trimmed as settled history | `[EXT]` rejected | Section 17.16.5; the row is retained like a promise |

The terminal certificate is task-56: `coord_checkpoint::handoff`.
`TerminalStateV1::terminal_root` binds the boundary, the shared
checkpoint root of the terminal common state, a digest of the
source-defined selection over the reports at the seal cut, the activated
floor lineage and the exact successor incarnations -- so "mixed
evidence" is not a judgement call and racing successor sets cannot both
be certified, because a different successor set is a different root.

| Item | Status | Where |
|---|---|---|
| `TerminalStateV1`, bound by one root | `[EXT]` | Sections 4.8, 10.3.2 |
| `closure_root` over the `SyncDecision` at the seal cut | `[EXT]` on top of the source selection | Section 4.9; a latent old completion stays represented |
| `select_certificate`: a majority of sealed old voters, one root | `[EXT]` | delegates the quorum rule to `handoff::select_terminal` |
| `publish_certificate`: republishes, never becomes another | `[EXT]` | Section 10.3.2: reuse an already selected certificate |
| A full KV snapshot as the terminal state | `[EXT]` rejected | Section 17.6: complete bytes alone prove nothing |

Activation is task-57, in the same module:

| Item | Status | Where |
|---|---|---|
| `TerminalInstallV1`, written only from an install receipt with the certificate's root and boundary | `[EXT]` | Section 10.3.2; evidence of holding the state |
| `activate_successor`: the successor set comes from the certificate, not from a caller | `[EXT]` | an activation cannot name a successor the old quorum did not certify |
| `publish_handoff_activation`: republishes, never becomes another | `[EXT]` | a duplicate activation is a no-op, not a second grant |
| `LocalEvidence`: what one store answers, combined with gathered stances | `[EXT]` | Section 10.3.2; `resume` decides, from records |
| A coordinator's assurance that a replica installed | `[EXT]` rejected | Section 17.6 |

The credential lifecycle underneath all of it is task-58. The paper has
a fixed replica set with no credentials at all, so every row is an
extension; what they share is that none of them is allowed to become a
second way to be a voter.

| Item | Status | Where |
|---|---|---|
| `RenewalPolicy::decide`: `Wait`, `Due`, `Expired`, and no fourth answer | `[EXT]` | Section 10.4; an outage is survived by the window, never by the deadline |
| `due_at` jitter derived from the node's own identity | `[EXT]` | a re-derived jitter would move a node's deadline across a restart |
| `retire_at`: the earlier of the replaced leaf's expiry and a bounded overlap | `[EXT]` | Section 10.4; a key rotated away from is not usable for its natural life |
| `session_deadline` from the credential a session was *bound* under | `[EXT]` | Section 20.4: a renewal does not extend a warm session |
| `IdentityBinder::expires_at` plus `Limits::max_connection_age` | `[EXT]` | Section 10.4; the binder says how long what it admitted stays admissible |
| `Membership::classify_credential`: one rule, five named cases | `[EXT]` | Section 20.4; the binder binds `Renewal` and nothing else |
| The refusal a peer sees is undifferentiated; `coordd inspect` names it | `[EXT]` | Section 20.4; the distinctions are the node operator's, not a caller's |
| `Generation::adopt`: the manifest stamp advances forwards only | `[EXT]` | Section 20.4; a root stamped past the credential is the cloned-disk fence |
| `StreamAllocator::adopt`: the stream is carried forward, not reallocated | `[EXT]` | Section 17.3.1; a fresh stream would look like a lost prefix |
| Append, read and replay take the generation from the current mapping | `[EXT]` | Section 17.3.1; a carried stream holds two generations of records |
| Writing the new generation into the projection database | `[EXT]` rejected | it commits the previous run's uncommitted work and pushes `M` past `J` |
| A credential that bypasses expiry for availability | `[EXT]` rejected | Section 10.4 |

Backup and restore is task-59, and it is the furthest from the paper of
anything here: the paper's replica set is fixed and its history is never
rewound.

| Item | Status | Where |
|---|---|---|
| `BackupManifestV1` binds the artifact root it names | `[EXT]` | Section 17.16; a repointed backup index fails verification |
| `plan_restore`: only a `SharedCheckpointV1` | `[EXT]` | Section 17.16.1; the three artifacts are not interchangeable |
| A successor cluster identity, never the source's | `[EXT]` | Section 5.4; a rewound history behind a name callers hold promises from |
| `FencingAttestationV1` bound to abandoned, successor and backup | `[EXT]` | Section 7.4; a record of an out-of-band action, not the action |
| `config_v1` and `policy_v1` not carried | `[EXT]` | Section 5.4: never reuse stale voting authority |
| Sessions and grants invalidated, leases revoked, attachments detached | `[EXT]` | Section 5.4 |
| Retries and floors restored at the boundary | `[EXT]` | dropping a retained result turns a retry into a second execution |
| The successor's own configuration epoch in the execution frontier | `[EXT]` | the donor's epoch belongs to a configuration this store does not hold |
| An automatic minority force-new-cluster preserving identity | `[EXT]` rejected | Section 5.4 |
| A zero-loss promise beyond the backup's RPO | `[EXT]` rejected | Section 5.4; the plan states the recovery point |

Format and capability upgrades are task-60. The paper has one version of
everything and no upgrade, so these are all extensions; what they share
is that no version or capability is ever guessed at.

| Item | Status | Where |
|---|---|---|
| `Format`: nine independently versioned formats with decoder windows | `[EXT]` | Section 11.2; an upgraded transport never changes a retry identity |
| A version outside the window is refused in the direction it is outside | `[EXT]` | Section 17.7: unsupported format fails before admission |
| Unanimity of the configured voters to activate a feature | `[EXT]` | Section 13; a minority that cannot take part is not behind, it is incapable |
| A majority of support reports | `[EXT]` rejected | contrast `floor::activate`, where a majority is right |
| Silence counted as support, or a report that shrinks | `[EXT]` rejected | the silent voter is the one that might be an old binary |
| An active feature a build does not know, decoded to a smaller set | `[EXT]` rejected | it would conclude it may serve when it may not |
| Offline migration: stage, rewrite, activate, `CURRENT` last | `[EXT]` | Section 17.7; an interruption preserves the valid selection |
| A migration carries this node's `protocol_v1`; an install refuses it | `[EXT]` | Section 17.10; a promise is not something to be replaced |
| Cross-engine migration, or a live voter savepoint rollback | `[EXT]` rejected | Sections 17.7, 17.13 |
| Any operation that lowers a format or deactivates a feature | `[EXT]` rejected | rollback is the old binary, or a restore |

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
* `possible_fast_scenarios.json` (task-28): a possible fast decision
  adopted from the fast-set member's order, the source leader present, a
  member order that differs from the leader's, and disagreeing members;
* `guard_removed_counterexample.json`: the trace with the guard enforced
  (violation at the leader-evidence step) and removed (the oracle finds a
  command committed while its dependency is below ACCEPT).

`crates/coord-consensus/tests/floor.rs` (task-52) does the same for the
checkpoint floor: every assignment of a readiness script to each of
three, four and five voters, every certification those promises allow,
and every majority read of them.

* `floor_scenarios.json`: the worlds explored, what was certified, the
  positions at which two subjects were both certified (none) and the
  majority reads that discovered less than a certified floor (none);
* `floor_counterexamples.json`: the three rules removed one at a time --
  a voter ready for two subjects at one position (two majorities certify
  different state at one executed prefix), a recovery that reads one
  report instead of a majority (it misses the floor), and possession
  counted as readiness (a signer crashes and votes from a baseline the
  cluster has forgotten below).

`crates/coord-consensus/tests/handoff.rs` (task-54) runs the transition
to every point at which a coordinator can die -- every assignment of a
stance script to each of three old voters, crossed with how much the
coordinator made durable -- and asks `resume` where to carry on.

* `handoff_scenarios.json`: the worlds explored, how many resumed at
  each stage, the worlds in which a fence was cleared (none) and the
  worlds in which a seal and a cancellation both certified (none);
* `handoff_counterexamples.json`: five rules removed one at a time -- a
  voter reversing its stance (a fence cleared by a retry), a terminal
  certificate selected before the fence (binding a state the old
  configuration can still move past), mixed terminal roots merged (a
  history nobody agreed on), a minority of the successor activating (a
  successor serving while most of it holds nothing), and an
  installation of another root counting.

## Not claimed

Finite models are not a proof; they are regressions of the source rules
and of the extensions. Upstream implementation/invariant differences are
neither proved fundamental nor assumed resolved (Section 4.10). No
performance or optimization claim is made from these models.

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

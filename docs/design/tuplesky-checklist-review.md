# SwiftPaxos correctness checklist review

**Status:** Review record, 2026-09-28. Not a specification: the tasks it gives rise to are specified in [the plan](tuplesky-prs-plan.md), as [task-d18](tuplesky-prs-plan.md#task-d18) through [task-d32](tuplesky-prs-plan.md#task-d32).

**What was reviewed:** the implementation stack at `d4bd28c` (task-d08 at its head) and the Jepsen client, its stress driver and their findings at `377d0a0`. The checklist is "TupleSky SwiftPaxos implementation correctness checklist" (GPT-6, 2026-09-28): 56 items in nine sections (A contract, P protocol, E evidence, D durability, B bounds, R repair, G reclamation, S application semantics, M membership) and a 15-row failure-test matrix, under one central gate: every state reachable under admission limits stays recoverable within configured limits once a valid quorum and stable communication return, without the original client, an excluded replica, deleted evidence or a manual increase in limits.

## Method

- Five reviewers each took two or three sections and read the code, the design, the plan, the notes and the source mapping, changing nothing.
- An item counts as covered only with a code location and a test; a document's claim alone does not count.
- The two safety bugs below were reproduced with new tests against `d4bd28c`. Both fail. They are not in this change; they open [task-d18](tuplesky-prs-plan.md#task-d18) and [task-d19](tuplesky-prs-plan.md#task-d19).
- The other findings rest on reading. Their key facts were checked, but they are not yet shown by a test; each task opens with one.
- The paper's recovery appendix could not be consulted, so the possible-fast rule is still unchecked against it; that is task-d19's first step.

## Verdict

The central gate is not met.

| Status | Items |
|---|---|
| Covered | 11 |
| Covered in libraries only | 2 |
| Partial | 26 |
| Gap | 15 |
| Planned, not built | 2 |

## Confirmed by a failing test

### A Sync can lower a durable promise (D1, D4)

- **Where:** `Follower::on_sync` accepts a Sync whose ballot equals the durable promise and never checks for a promise in flight, which `may_vote` does. `BallotState::mark_synced` then writes `{promised: durable, synced}`.
- **Scenario:**
  1. r1 has promised P1 durably.
  2. A candidate asks for P3. r1 queues the row `{P3}` and publishes `Promise(P3)` once that row is durable.
  3. P1's delayed Sync arrives and queues `{P1, P1}`. The journal applies rows in order, so the P1 row lands last.
  4. After a crash r1 recovers with P1 promised and synced, and votes in P1 after promising P3.
- **Test:** `a_sync_behind_a_promise_in_flight_does_not_lower_the_durable_promise`, for `coord-consensus/tests/activation.rs`. The durable row reads `promised: (1, 2)` after r1 published `Promise (3, 0)`.
- **Owner:** [task-d18](tuplesky-prs-plan.md#task-d18).

### Recovery can drop a slow decision that counted a fast acknowledgement (P5, P6, S6)

- **Where:** `VoteSet::learned` and `learned_slow` count a fast-set member's fast acknowledgement toward the slow majority when its dependencies equal the leader's. Recovery keeps a PRE-ACCEPT only when every reporting fast-set member pre-accepted it with the same path.
- **Scenario:** five voters; ballot 0 is led by r0 with fast set {r0, r1, r2}.
  1. x is learned slow with dependencies [y], from r0's proposal, r1's fast acknowledgement and r3's adoption.
  2. r0 and r3 fail. r2 leads ballot 1 on reports from r1, r2 and r4.
  3. The one member of the deciding quorum among them is r1, at PRE-ACCEPT. r2 never saw x, so x is re-proposed with new dependencies, although its result may have been released.

  With three voters the fast set is a majority and the possible-fast rule covers the case.
- **Test:** `a_slow_decision_counting_a_fast_ack_survives_recovery`, for `coord-consensus/tests/model.rs`. `learned()` returns `Slow { deps: [y] }` and `select` re-proposes x.
- **Relation to Jepsen:** fits the unexplained five-node stop `release-record-mismatch(c96f0e70)`, which is not shown to be its cause.
- **Owner:** [task-d19](tuplesky-prs-plan.md#task-d19).

## Found by reading, not yet shown by a test

| Finding | Items | Evidence | Owner |
|---|---|---|---|
| The largest Sync is not shown to fit its row | B1, B4 | The one proof uses three reports and no admission digest; a campaign selects over up to five; `sync_pending` is never cleared; binding is `expect("bounded")` | [task-d20](tuplesky-prs-plan.md#task-d20) |
| A voter's silent refusal leaves a collector entry pending for good | E1, R1, R5, A5 | `RequestFactsConflict`, an identity conflict and a forgotten-payload duplicate produce no effect; entries leave only when they settle | [task-d22](tuplesky-prs-plan.md#task-d22) |
| A ballot change voids collector evidence with nothing asking again | R5, E4 | The collector starts a new vote set and re-offers nothing | [task-d22](tuplesky-prs-plan.md#task-d22) |
| Client outcomes blur executed and not admitted | A4, R3 | Withheld output answers `NOT_ADMITTED`, which the SDK treats as a definite failure; `ResolveRequest` never reads the durable record | [task-d23](tuplesky-prs-plan.md#task-d23) |
| A Sync entry cannot enter a full table | B3 | The placeholder is dropped under backpressure; only catch-up passes capacity | [task-d24](tuplesky-prs-plan.md#task-d24) |
| A recovery cycle stops re-proposal silently | P4 | The ordering loop in `Leader::from_recovered` breaks without an error | [task-d21](tuplesky-prs-plan.md#task-d21) |
| Nothing reclaims history | A2, G4, B8 | Per-boot bindings and the history set grow; payload, executed, protocol, session and retry rows are never trimmed by `coordd` | [task-d26](tuplesky-prs-plan.md#task-d26), [task-d27](tuplesky-prs-plan.md#task-d27) |
| Catch-up is slower than the domain | B7, B9 | About 100 commands a second, one durable install and one execution each | [task-d25](tuplesky-prs-plan.md#task-d25) |
| Report pages are sent once on a lane that drops | B5 | Nothing asks for a missing page | [task-d28](tuplesky-prs-plan.md#task-d28) |
| Configuration and quorum edges | M1, P3 | The validator accepts two and four voters; recovery hard-codes `c2_default` | [task-d31](tuplesky-prs-plan.md#task-d31) |
| Plan and harness disagree with the design | A1, G5 | The v1.5 amendment said task-d13 waits on catch-up (corrected in this change); the harness initializes over a wiped voter, against [Section 5.4](tuplesky-design.md#s5-4) | [task-d29](tuplesky-prs-plan.md#task-d29) |

## Item by item

| Item | Status | Evidence | Gap | Owner |
|---|---|---|---|---|
| A1 Failure assumptions | Partial | Quarantine on corruption: `durable_prefix_corruption_is_quarantined_not_repaired`, `disk_quarantine_stops_serving_and_never_recovers_in_place` | No single failure model; a diverged node serves after a restart; what restores progress is not argued | task-d29, task-d13 |
| A2 Resource scope | Gap | Limits for request, response, session, table, checkpoints | No separate memory, durable, application, queue or temporary-file limits; history grows | task-d26, task-d27 |
| A3 State transitions | Partial | `publication_obligations_follow_the_durability_table` | No single definition of the transitions; retirement unspecified for bindings and the collector window | task-d29 |
| A4 Client outcomes | Gap | `saturation_after_dispatch_never_becomes_a_refusal` | No retired outcome; `NOT_ADMITTED` for executed commands; `Unknown` is final | task-d23 |
| A5 Recovery ownership | Gap | `a_callers_deadline_does_not_discard_the_delivery_obligation` | Refused entries and follower-only commands have no trigger or escalation | task-d22, task-d29 |
| P1 Identities, determinism, conflicts | Covered | `reordered_and_duplicate_requests_cannot_bind_conflicting_payload`, `forced_slow_and_fast_learning_yield_equal_results` | Every command conflicts through the conservative key | — |
| P2 Quorum shape | Covered | `quorum_policy_matches_the_design_table` | — | — |
| P3 Fixed fast quorum | Covered | `VoteError::NotInFastSet`; model scenario `arbitrary-fastest-majority-is-not-c2` | Latent: recovery hard-codes `c2_default` | task-d31 |
| P4 Dependencies agree, acyclic | Partial | Guards; `a_follower_behind_a_full_leader_executes_in_the_leaders_order` | A recovery cycle stops re-proposal silently | task-d21 |
| P5 Vote revision | Gap | — | Confirmed bug | task-d19 |
| P6 Recovery keeps committed outcomes | Gap | `possible_fast_decisions_are_recovered_from_the_fixed_fast_set` (three voters) | Confirmed bug; unchecked against Appendix A | task-d19 |
| P7 Transitions map to the specification | Partial | `spec/swiftpaxos-mapping.md` | No refinement argument for slow counting; no model joins learning with recovery | task-d19 |
| E1 Immutable binding | Partial | `other_facts_under_the_same_identity_are_a_conflict_and_replay_nothing` | Collector entry stays pending after a silent refusal | task-d22 |
| E2 Voter counting | Partial | `voter_identities_are_counted_rather_than_connections` | A leader reply is counted under the collector's own admission digest | task-d22 |
| E3 Message schedules | Partial | `duplicate_and_reordered_messages_converge` | No schedule exploration of the real machines | task-d30 |
| E4 Stale evidence | Partial | `a_late_old_ballot_completion_updates_bookkeeping_but_never_authorizes_a_new_vote` | Standalone and Kine collectors stall after `WrongBallot` | task-d22, task-m02 |
| E5 Input and CPU bounds | Partial | `payload_transfer_is_bounded_in_both_directions_and_still_covers_everything` | Receive-side reassembly unbudgeted; linear window scans | task-d26 |
| D1 Publication ordering | Gap | `promise_reply_waits_for_the_row_and_every_batch_before_the_cut` | Confirmed bug | task-d18 |
| D2 Authoritative recovery cut | Partial | `a_reclaimed_prefix_is_recovered_from_the_image_and_the_suffix` (library) | The serving path attaches the live store, never installs from the checkpoint | task-j05, task-d27 |
| D3 Atomic publication | Covered in libraries | `a_crash_at_each_publication_step_leaves_a_valid_selection` | Real filesystem qualification | task-j05 |
| D4 Restart fencing | Gap | `old_messages_cannot_lower_a_recovered_promise` | Confirmed bug; a diverged node restarts and serves | task-d18, task-d13 |
| D5 Application atomicity | Covered | `crash_between_materialization_and_notification_never_duplicates` | — | — |
| D6 Storage errors | Partial | `sync_failure_inside_a_write_panics_and_fail_stops_until_reopen` | Tail corruption tolerated by default; no lost-storage runbook; no composed ENOSPC test | task-j05, task-d29 |
| B1 Reachable-state budget | Gap | `the_largest_table_gives_a_sync_that_fits_a_row_and_a_frame` | Three reports, no digest, no pending growth | task-d20 |
| B2 Peak accounting | Gap | Entries only | No bytes; candidate pages, clones, encodings uncounted | task-d26 |
| B3 Completion capacity | Partial | `backpressure_refuses_new_work_without_deleting_unresolved_acceptance` | No reservation; Sync entries refused by a full table; undecided records never drain | task-d24 |
| B4 Concrete headroom | Gap | — | The one bound is unproven; no streaming or spill | task-d20 |
| B5 Streaming correctness | Partial | `pages_assemble_only_when_complete_and_verified` | No page retransmission | task-d28 |
| B6 Dependency expansion | Partial | `closure_steps_charge_every_edge_and_frontier_entry` | `advance_sync` rescans every pending entry each round | task-d26 |
| B7 Scheduling progress | Gap | Catch-up on the bulk lane | Control lane shared and dropping; catch-up slower than the domain | task-d25, task-d28 |
| B8 Disk headroom | Gap | Pending-file cleanup | No reserve; images grow with history | task-d27 |
| B9 Conditional liveness | Gap | `a_follower_cut_off_for_thirty_seconds_serves_within_ten_of_the_heal` (rate only) | No argument; two documented stalls | task-d29, task-d24 |
| R1 Retry semantics | Partial | `retained_answer`; collector re-attach | New-admission re-presentation dropped; `retry()` after `Unknown` sends nothing | task-d22, task-d23 |
| R2 Payload availability | Covered | `a_missing_payload_is_fetched_and_rehashed_never_fabricated` | — | — |
| R3 Repair-cache expiry | Partial | `daemon/tests/repair.rs` | Neither half held stalls; window eviction becomes `Unknown` | task-d22, task-d23 |
| R4 Delivery fairness | Partial | `the_offer_budget_is_spread_across_commands` | Relies on the hold outlasting the retry schedule | task-d22 |
| R5 End-to-end escalation | Gap | — | No escalation for an entry holding neither half | task-d22 |
| R6 Equivalent ingress | Partial | `a_local_submission_and_a_wire_submission_produce_the_same_round` | Remote evidence dies with its connection; frontends without a voter do not follow ballots | task-d22, task-m02 |
| G1 Retirement justification | Partial | `CommandTable::retire` retires executed commands only | Tombstones and the forgetting window go by recency | task-d29 |
| G2 Recovery versus catch-up | Covered in libraries | `a_local_image_is_this_nodes_whole_storage` | Learner install unwired | task-d32 |
| G3 Safe frontier | Partial | `a_trim_keeps_every_dependency_a_retained_command_can_reach` (library) | A forgotten dependency stops catch-up | task-d27 |
| G4 Bounded retirement metadata | Gap | `a_stale_retirement_never_lowers_the_floor` | No row is reclaimed | task-d27 |
| G5 Offline participants | Partial | task-d08 catch-up | No path behind a future floor; harness re-creates a wiped voter | task-d32, task-d29 |
| G6 Snapshot fencing | Partial | `a_root_whose_selected_generation_holds_obligations_is_never_staged_over` | Learner installs unwired | task-d32, task-m03 |
| S1 Conflict oracle | Covered | Conservative key | Finer predicates out of scope | — |
| S2 Determinism | Covered | `authority_epochs_fence_former_leaders_and_only_advance` | Session expiry checked at admission only | — |
| S3 Global observables | Partial | `watch_events_follow_irrevocable_application` | Fast-path rate under the total chain unmeasured | task-64 |
| S4 Reads | Covered | Every Kine read is an ordered command | Weaker modes unsupported; ReadFence is task-o05 | — |
| S5 Regional read and watch | Planned | Non-voters refused a vote | Observers unbuilt | task-o01 to task-o06 |
| S6 Speculation and responses | Partial | `tentative_results_of_a_lost_leader_never_surface` | Release on `learned()` inherits the P5 bug | task-d19 |
| S7 Cancellation | Covered | `cancellation_preserves_identity_and_outcome_resolution` | — | — |
| M1 Supported configurations | Partial | `quorum_policy_matches_the_design_table` | Two and four voters accepted | task-d31 |
| M2 Reconfiguration | Planned | Handoff model | Unwired | task-m03, task-m05 |
| M3 Stale members | Partial | Seals survive restart; outside signers refused | End-to-end left-behind-disk tests ignored | task-m03 |
| M4 Failover placement | Covered | `two_candidates_at_once_end_with_one_leader` | Placement is task-m04 | — |
| M5 Unsupported operations | Covered | `a_replacement_in_an_edited_genesis_is_quarantined_before_anything_is_adopted` | Explicitly unsupported, as asked | — |

The references to task-o01 through task-o06 and task-m02 through task-m05 are to tasks already in the plan.

## Failure-test matrix

The deterministic clusters run three voters at table capacity 32 and five voters at 8 to 64; no row runs at both sizes, and no harness checks budgets or asserts progress after healing.

| # | Scenario | Status | Evidence and gap | Owner |
|---|---|---|---|---|
| 1 | Fill capacity, fail the leader | Partial | `an_election_after_more_history_than_the_table_holds_asks_for_nothing_executed` (three voters); Sync bound unproven | task-d20, task-d30 |
| 2 | Different maximum tentative sets | Partial | `a_follower_whose_table_filled_while_it_could_not_learn_catches_up` (five voters) | task-d30 |
| 3 | Dense conflicts, early missing dependency | Partial | `a_follower_asks_first_for_what_the_leader_committed_in_its_order` | task-d30 |
| 4 | Ack before payload, hold expires, repeat | Partial | `a_duplicate_after_the_hold_expired_repairs_the_callers_evidence` (three voters) | task-d30 |
| 5 | Client or collector dies mid-dissemination | Gap | Client detach only | task-d22, task-d30 |
| 6 | Crash around write, publication, materialization | Storage level | `crash_at_every_write_and_sync_boundary`; not in a cluster | task-j05 |
| 7 | Crash between mutation, marker, response | Component | `crash_between_materialization_and_notification_never_duplicates` | — |
| 8 | Crash during checkpoint, truncation, install | Component | `a_crash_between_the_certificate_and_the_floor_deletes_nothing` | task-j05 |
| 9 | Repeated interrupted elections | Partial | `competing_campaigns_and_delayed_replies_cannot_establish_divergence`; flat budgets unshown | task-d28, task-d30 |
| 10 | Delayed old-ballot messages | Partial | `old_ballot_work_is_held_across_recovery`; the promise bug is a gap here | task-d18, task-d30 |
| 11 | Replica offline through many checkpoints | Partial | task-d08 catch-up and the follower-out driver; no floor to test | task-d27, task-d32 |
| 12 | Loss beyond the repair-cache window | Partial | `a_refused_repair_falls_back_to_the_durable_record`; neither half stalls | task-d22, task-d30 |
| 13 | Disk full, fsync failure, slow materializer | Partial | `enospc_fails_the_commit` (engine only) | task-j05 |
| 14 | Lost response, same-ID and new-payload retries | Partial | `lost_response_with_the_same_identity_returns_the_same_result`; not under faults at three or five voters | task-d23, task-d30 |
| 15 | Partition, lease expiry, regional reads, membership change | Gap | None | task-64 |

## Order

- **Safety first.** task-d18 through task-d21 go ahead of further Jepsen conclusions. Every task here is a prerequisite of task-64.
- **Then ownership:** task-d22 through task-d25.
- **Then bounds:** task-d26 through task-d28, and task-d32 after task-d27.
- **Then the contract and simulation:** task-d29 through task-d31.
- **task-d25 waits on a decision:** leaving catch-up as it is, or windowed installation.

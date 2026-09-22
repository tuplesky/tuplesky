//! task-j03 acceptance: the journal-first pipeline composed end to end.
//!
//! The model journal and the model engine make the scheduling explicit
//! (scripted append and commit outcomes, exact append counts, the whole
//! durable image of the projection for comparison); `journaled_real.rs`
//! runs the same composition over the pinned raft-engine journal and a real
//! redb projection. Neither file is the composed fault qualification, which
//! is task-j05.

use std::collections::{BTreeMap, BTreeSet};

use coord_consensus::rows::{PromiseRecordV1, dependency_update, payload_update, promise_update};
use coord_consensus::{CommandRecord, PayloadRecordV1, Phase};
use coord_core::effect::{
    ApplyBase, BarrierId, BootId, EffectContext, PeerId, PersistBatch, StoreUpdate,
};
use coord_core::event::{StorageError, StorageEvent};
use coord_core::outbox::{BarrierAllocator, Outbox, PendingSend, ReleaseError};
use coord_journal_api::failure::JournalFailure;
use coord_journal_api::record::RecordError;
use coord_journal_api::stream::ShardId;
use coord_storage::journaled::{
    CutError, DomainStatus, JournalLimits, JournaledError, JournaledStore, Submission,
    SubmitRefused, TransitionKind,
};
use coord_storage::protocol::read_protocol;
use coord_storage::views::ViewBudget;
use coord_store_api::engine::OrderedRead;
use coord_store_testkit::journal::{AppendScript, ModelJournal};
use coord_store_testkit::model::{CommitScript, ModelEngine};
use coord_types::identity::Digest32;
use coord_types::ids::*;
use coord_types::{CommandId, RetryKey};

const CLUSTER: ClusterId = ClusterId([0x11; 16]);
const REPLICA: ReplicaId = ReplicaId([0x22; 16]);
const A: DomainId = DomainId([0xa1; 16]);
const B: DomainId = DomainId([0xb2; 16]);
const C: DomainId = DomainId([0xc3; 16]);
const BOOT: BootId = BootId([0x77; 16]);

fn inc() -> ReplicaIncarnation {
    ReplicaIncarnation::new(1).unwrap()
}

fn epoch() -> ConfigurationEpoch {
    ConfigurationEpoch::new(1).unwrap()
}

fn ballot(number: u64) -> Ballot {
    Ballot {
        epoch: epoch(),
        number,
        leader: REPLICA,
    }
}

fn seq(n: u64) -> LocalJournalSeq {
    LocalJournalSeq::new(n).unwrap()
}

fn position(n: u64) -> ExecutionPosition {
    ExecutionPosition::new(n).unwrap()
}

fn command(tag: u8) -> CommandId {
    CommandId(Digest32([tag; 32]))
}

fn shard() -> ShardId {
    ShardId::new(0).unwrap()
}

/// One node: a shared model journal and one model projection per domain.
struct World {
    store: JournaledStore<ModelJournal, ModelEngine>,
    barriers: BarrierAllocator,
}

impl World {
    fn with_domains(domains: &[DomainId]) -> Self {
        let mut store = JournaledStore::open(
            ModelJournal::new(),
            CLUSTER,
            REPLICA,
            inc(),
            BOOT,
            JournalLimits::default(),
        )
        .unwrap();
        for domain in domains {
            store.attach(*domain, shard(), ModelEngine::new()).unwrap();
        }
        World {
            store,
            barriers: BarrierAllocator::new(inc(), BOOT),
        }
    }

    fn new() -> Self {
        World::with_domains(&[A])
    }

    fn protocol(
        &mut self,
        domain: DomainId,
        ballot: Ballot,
        updates: Vec<StoreUpdate>,
    ) -> BarrierId {
        let barrier = self.barriers.allocate();
        self.store
            .submit(Submission {
                domain,
                ballot,
                kind: TransitionKind::Protocol,
                batch: PersistBatch {
                    barrier,
                    base: None,
                    updates,
                },
            })
            .unwrap();
        barrier
    }

    /// An application outcome. Its ballot is in the epoch of the base it
    /// extends: the record binds base configuration, context epoch and
    /// ballot epoch to one value, and a projection that has never executed
    /// a command is still at the initial configuration.
    fn application(
        &mut self,
        domain: DomainId,
        number: u64,
        position: ExecutionPosition,
        updates: Vec<StoreUpdate>,
    ) -> BarrierId {
        let barrier = self.barriers.allocate();
        let base = self.store.application_base(domain).unwrap();
        self.store
            .submit(Submission {
                domain,
                ballot: Ballot {
                    epoch: base.configuration,
                    number,
                    leader: REPLICA,
                },
                kind: TransitionKind::Application {
                    position,
                    revision: None,
                    result_digest: Digest32([position.get() as u8; 32]),
                },
                batch: PersistBatch {
                    barrier,
                    base: Some(base),
                    updates,
                },
            })
            .unwrap();
        barrier
    }
}

fn promise(number: u64) -> Vec<StoreUpdate> {
    vec![
        promise_update(
            epoch(),
            &PromiseRecordV1 {
                promised: ballot(number),
                synced: ballot(number),
            },
        )
        .unwrap(),
    ]
}

fn retry_key(sequence: u64) -> RetryKey {
    RetryKey {
        cluster_id: CLUSTER,
        domain_id: A,
        session_id: SessionId([0x5; 16]),
        client_instance_id: ClientInstanceId([0x6; 16]),
        request_sequence: RequestSequence::new(sequence).unwrap(),
    }
}

fn kv_update(key: &[u8], value: &[u8]) -> StoreUpdate {
    StoreUpdate {
        collection: coord_store_api::registry::Collection::KvCurrentV1.id(),
        key: key.to_vec(),
        value: Some(value.to_vec()),
    }
}

#[test]
fn journal_durability_and_materialization_are_separate_facts_and_visibility_follows_the_second() {
    let mut world = World::new();
    // Attaching writes the stream's genesis and this boot's lifecycle
    // record, so the first transition of the domain takes sequence three.
    assert_eq!(world.store.frontiers(A).unwrap().durable(), seq(2));
    assert_eq!(world.store.frontiers(A).unwrap().materialized(), seq(2));

    let barrier = world.protocol(A, ballot(7), promise(7));
    let appended = world.store.append_pending().unwrap();
    assert_eq!(appended.journaled, 1);
    assert_eq!(appended.materialized, 0);
    assert_eq!(
        appended.events,
        vec![StorageEvent::JournalDurable {
            barrier_id: barrier,
            journal_seq: seq(3),
        }]
    );
    assert!(
        appended.written.is_some(),
        "the engine byte count is evidence"
    );

    // The record is durable in the journal and still invisible: the gated
    // view proves the materialized frontier, not the journal's.
    let gated = world.store.reader(A).unwrap().snapshot().unwrap();
    assert_eq!(
        coord_storage::protocol::read_promise(gated.view(), epoch()).unwrap(),
        None
    );
    assert_eq!(gated.store_seq().journal_seq(), seq(2));
    assert_eq!(world.store.frontiers(A).unwrap().durable(), seq(3));
    assert_eq!(world.store.frontiers(A).unwrap().materialized(), seq(2));
    assert_eq!(world.store.unmaterialized(A), 1);

    let applied = world.store.materialize().unwrap();
    assert_eq!(
        applied.events,
        vec![StorageEvent::Materialized {
            barrier_id: barrier,
            journal_seq: seq(3),
        }]
    );
    let gated = world.store.reader(A).unwrap().snapshot().unwrap();
    assert_eq!(
        coord_storage::protocol::read_promise(gated.view(), epoch())
            .unwrap()
            .unwrap()
            .promised,
        ballot(7)
    );
    assert_eq!(gated.store_seq().journal_seq(), seq(3));
    assert_eq!(world.store.frontiers(A).unwrap().materialized(), seq(3));
}

#[test]
fn one_synced_group_write_carries_at_most_one_batch_from_each_stream() {
    let mut world = World::with_domains(&[A, B, C]);
    let before = world.store.journal().appends();
    let first = world.protocol(A, ballot(1), promise(1));
    let second = world.protocol(A, ballot(2), promise(2));
    let other = world.protocol(B, ballot(1), promise(1));
    let third = world.protocol(C, ballot(1), promise(1));

    let report = world.store.append_pending().unwrap();
    assert_eq!(
        report.appends, 1,
        "one grouped engine write for three streams"
    );
    assert_eq!(world.store.journal().appends() - before, 1);
    assert_eq!(report.journaled, 3);
    let durable: BTreeSet<BarrierId> = report
        .events
        .iter()
        .filter_map(StorageEvent::barrier)
        .collect();
    assert_eq!(durable, BTreeSet::from([first, other, third]));
    // The second transition of A stays queued: initially a stream carries
    // at most one uncompleted authoritative batch.
    assert_eq!(world.store.queued(A), 1);

    let report = world.store.append_pending().unwrap();
    assert_eq!(report.journaled, 1);
    assert_eq!(
        report.events,
        vec![StorageEvent::JournalDurable {
            barrier_id: second,
            journal_seq: seq(4),
        }]
    );
    // Every stream keeps its own sequence space; nothing is derived from
    // the engine's byte count.
    for domain in [A, B, C] {
        world.store.materialize().unwrap();
        assert_eq!(
            world.store.frontiers(domain).unwrap().materialized(),
            world.store.frontiers(domain).unwrap().durable()
        );
    }
    assert_eq!(world.store.frontiers(A).unwrap().durable(), seq(4));
    assert_eq!(world.store.frontiers(B).unwrap().durable(), seq(3));
}

#[test]
fn a_record_over_the_group_budget_takes_the_bounded_large_record_path_alone() {
    let mut world = World::with_domains(&[A, B]);
    let big = vec![kv_update(b"big", &vec![0x5a; 400 * 1024])];
    let large = world.protocol(A, ballot(1), big);
    let small = world.protocol(B, ballot(1), promise(1));
    let report = world.store.append_pending().unwrap();
    assert_eq!(report.appends, 1);
    assert_eq!(report.journaled, 1);
    assert_eq!(
        report.events,
        vec![StorageEvent::JournalDurable {
            barrier_id: large,
            journal_seq: seq(3),
        }],
        "the oversized record is admitted alone, never split"
    );
    let report = world.store.append_pending().unwrap();
    assert_eq!(report.journaled, 1);
    assert_eq!(report.events[0].barrier(), Some(small));
}

#[test]
fn an_ambiguous_append_is_reconciled_from_the_actual_durable_head_and_never_blind_retried() {
    for applied in [true, false] {
        let mut world = World::new();
        let barrier = world.protocol(A, ballot(1), promise(1));
        world
            .store
            .journal_mut()
            .script_append(AppendScript::Indeterminate { applied });
        let report = world.store.append_pending().unwrap();
        assert!(report.indeterminate);
        assert!(
            report.events.is_empty(),
            "an unknown outcome completes nothing and fails nothing"
        );
        assert_eq!(world.store.status(A), Some(DomainStatus::JournalUncertain));

        // Nothing new is admitted and no cut may be taken while submitted
        // work is unresolved: a timeout never proves absence.
        let next = world.barriers.allocate();
        assert_eq!(
            world.store.submit(Submission {
                domain: A,
                ballot: ballot(2),
                kind: TransitionKind::Protocol,
                batch: PersistBatch {
                    barrier: next,
                    base: None,
                    updates: promise(2),
                },
            }),
            Err(SubmitRefused::NotReady(DomainStatus::JournalUncertain))
        );
        assert!(matches!(
            world.store.recovery_cut(A),
            Err(CutError::WorkOutstanding { .. })
        ));

        let appends = world.store.journal().appends();
        let report = world.store.reconcile(A).unwrap();
        assert_eq!(
            world.store.journal().appends(),
            appends,
            "reconciliation reads the durable head; it never rewrites the batch"
        );
        if applied {
            assert_eq!(
                report.events,
                vec![StorageEvent::JournalDurable {
                    barrier_id: barrier,
                    journal_seq: seq(3),
                }]
            );
            assert_eq!(world.store.frontiers(A).unwrap().durable(), seq(3));
        } else {
            assert_eq!(
                report.events,
                vec![StorageEvent::Failed {
                    barrier_id: barrier,
                    error: StorageError::DefinitelyNotCommitted,
                }]
            );
            assert_eq!(world.store.frontiers(A).unwrap().durable(), seq(2));
        }
        assert_eq!(world.store.status(A), Some(DomainStatus::Ready));
    }
}

#[test]
fn an_ambiguous_projection_commit_is_resolved_from_the_stamp_not_by_retrying_the_batch() {
    for applied in [true, false] {
        let mut world = World::new();
        let barrier = world.protocol(A, ballot(1), promise(1));
        world.store.append_pending().unwrap();
        world
            .store
            .projection(A)
            .unwrap()
            .script_commit(CommitScript::Indeterminate { applied });
        let report = world.store.materialize().unwrap();
        assert!(report.indeterminate);
        assert!(report.events.is_empty());
        assert_eq!(
            world.store.status(A),
            Some(DomainStatus::MaterializationUncertain)
        );
        // Native visibility may run ahead of the completion callback; the
        // gate holds such a view instead of publishing it.
        let held = world.store.reader(A).unwrap().snapshot();
        match held {
            Ok(gated) => assert_eq!(gated.store_seq().journal_seq(), seq(2)),
            Err(e) => assert!(
                matches!(e, coord_storage::ViewError::AheadOfCompletion { .. }),
                "unexpected view error: {e:?}"
            ),
        }

        let report = world.store.reconcile(A).unwrap();
        assert_eq!(
            report.events,
            vec![StorageEvent::Materialized {
                barrier_id: barrier,
                journal_seq: seq(3),
            }],
            "present or absent, the durable record is materialized exactly once"
        );
        assert_eq!(world.store.status(A), Some(DomainStatus::Ready));
        assert_eq!(world.store.frontiers(A).unwrap().materialized(), seq(3));
    }
}

#[test]
fn a_recovery_cut_held_behind_materialization_still_summarizes_every_voting_obligation() {
    let mut world = World::new();
    // Sequence 3: a promise that materializes normally.
    world.protocol(A, ballot(7), promise(7));
    world.store.flush().unwrap();
    // Sequence 4: a vote that is durable in the journal but deliberately
    // left unmaterialized, exactly the Section 4.8 counterexample.
    let vote = command(0xa1);
    let record = CommandRecord {
        phase: Phase::Accept,
        deps: Vec::new(),
        keys: vec![b"k".to_vec()],
        payload: Some(Digest32([0x5; 32])),
        paths: Vec::new(),
        path: Digest32([0x6; 32]),
    };
    world.protocol(
        A,
        ballot(7),
        vec![
            dependency_update(epoch(), &vote, &record).unwrap(),
            payload_update(
                &vote,
                &PayloadRecordV1 {
                    retry_key: retry_key(1),
                    logical: vec![0xaa],
                },
            )
            .unwrap(),
        ],
    );
    world.store.append_pending().unwrap();
    assert_eq!(world.store.frontiers(A).unwrap().materialized(), seq(3));
    assert_eq!(world.store.frontiers(A).unwrap().durable(), seq(4));

    // The projection alone omits the obligation.
    let gated = world.store.reader(A).unwrap().snapshot().unwrap();
    let materialized = read_protocol(gated.view(), epoch(), ViewBudget::default()).unwrap();
    assert!(materialized.records.is_empty());
    assert_eq!(materialized.promise.unwrap().promised, ballot(7));

    // The authoritative cut includes it without waiting for the projection.
    let cut = world.store.recovery_cut(A).unwrap();
    assert_eq!(cut.materialized(), seq(3));
    assert_eq!(cut.durable(), seq(4));
    let summary = read_protocol(&cut, epoch(), ViewBudget::default()).unwrap();
    assert_eq!(
        summary.promise.as_ref().map(|p| p.promised),
        Some(ballot(7))
    );
    assert_eq!(summary.records, vec![(vote, record)]);

    // Materializing the suffix produces exactly the same summary: the cut
    // anticipated the state, it did not invent any.
    world.store.materialize().unwrap();
    let gated = world.store.reader(A).unwrap().snapshot().unwrap();
    assert_eq!(
        read_protocol(gated.view(), epoch(), ViewBudget::default()).unwrap(),
        summary
    );
}

#[test]
fn a_late_old_ballot_completion_updates_bookkeeping_but_never_authorizes_a_new_vote() {
    let mut world = World::new();
    let old = world.protocol(A, ballot(7), promise(7));
    // A second old-ballot transition is queued but not submitted yet.
    let queued = world.protocol(A, ballot(7), promise(7));
    // The first append is submitted and its outcome is unknown when the
    // replica promises a higher ballot.
    world
        .store
        .journal_mut()
        .script_append(AppendScript::Indeterminate { applied: true });
    let report = world.store.append_pending().unwrap();
    assert!(report.indeterminate);

    let mut outbox = Outbox::new(BOOT);
    outbox.publish(PendingSend {
        context: EffectContext {
            domain: A,
            replica_incarnation: inc(),
            boot_id: BOOT,
            configuration: epoch(),
            ballot: ballot(7),
            required_journal_seq: seq(3),
        },
        requires: vec![old],
        to: PeerId {
            replica: ReplicaId([0x33; 16]),
            incarnation: inc(),
        },
        frame: vec![1, 2, 3],
    });

    // Admission of the obsolete ballot closes. Queued old-ballot work is
    // refused explicitly, so no barrier is left waiting; the append already
    // submitted is left to its own outcome.
    let refused = world.store.fence(A, ballot(9)).unwrap();
    assert_eq!(
        refused,
        vec![StorageEvent::Failed {
            barrier_id: queued,
            error: StorageError::DefinitelyNotCommitted,
        }]
    );

    // The late completion of the already-submitted append arrives.
    let report = world.store.reconcile(A).unwrap();
    assert_eq!(
        report.events,
        vec![StorageEvent::JournalDurable {
            barrier_id: old,
            journal_seq: seq(3),
        }]
    );
    for event in &report.events {
        assert!(outbox.observe(event), "durability bookkeeping is updated");
    }
    assert!(outbox.is_durable(&old));
    // It authorizes nothing new: the vote is dropped as obsolete.
    assert!(outbox.release(&ballot(9)).is_empty());
    let dropped = outbox.take_dropped();
    assert_eq!(dropped.len(), 1);
    assert_eq!(dropped[0].1, ReleaseError::ObsoleteBallot);

    // A newly submitted old-ballot transition is refused outright.
    let another = world.barriers.allocate();
    assert_eq!(
        world.store.submit(Submission {
            domain: A,
            ballot: ballot(7),
            kind: TransitionKind::Protocol,
            batch: PersistBatch {
                barrier: another,
                base: None,
                updates: promise(7),
            },
        }),
        Err(SubmitRefused::ObsoleteBallot {
            promised: ballot(9)
        })
    );

    // The obligation is nonetheless part of the domain's durable cut.
    world.store.materialize().unwrap();
    let cut = world.store.recovery_cut(A).unwrap();
    assert_eq!(
        read_protocol(&cut, epoch(), ViewBudget::default())
            .unwrap()
            .promise
            .unwrap()
            .promised,
        ballot(7)
    );
}

#[test]
fn fencing_refuses_a_retained_application_whose_base_names_a_refused_position() {
    let mut world = World::new();
    world.protocol(A, ballot(1), promise(1));
    world.store.flush().unwrap();
    // An old-ballot application is queued, and a newer-ballot successor
    // is planned against the position it would establish.
    let obsolete = world.application(A, 1, position(1), vec![kv_update(b"k", b"v1")]);
    let successor = world.application(A, 2, position(2), vec![kv_update(b"k", b"v2")]);
    let before = world.store.application_base(A).unwrap();
    assert_eq!(before.execution_position, position(2));

    // Fencing refuses the obsolete application. Its position is gone
    // from the history the stream will hold, so the successor, which
    // named that position as its base, no longer extends anything: kept,
    // it would be journaled and then quarantine the domain at
    // materialization. It is refused too, so it is replanned. The promise
    // is in the applications' own epoch, so the successor is refused for
    // its base alone, not as obsolete.
    let promised = Ballot {
        epoch: before.configuration,
        number: 2,
        leader: REPLICA,
    };
    let refused = world.store.fence(A, promised).unwrap();
    let failed: BTreeSet<BarrierId> = refused
        .iter()
        .map(|e| match e {
            StorageEvent::Failed {
                barrier_id,
                error: StorageError::DefinitelyNotCommitted,
            } => *barrier_id,
            other => panic!("unexpected {other:?}"),
        })
        .collect();
    assert_eq!(failed, BTreeSet::from([obsolete, successor]));
    assert_eq!(world.store.queued(A), 0);
    assert_eq!(world.store.queued_bytes(A), 0);
    // The base the domain reports is the journaled one, and a command
    // replanned from it goes through the whole pipeline.
    let base = world.store.application_base(A).unwrap();
    assert_eq!(base.execution_position, ExecutionPosition::ZERO);
    let replanned = world.application(A, 2, position(1), vec![kv_update(b"k", b"v1")]);
    let report = world.store.flush().unwrap();
    assert_eq!(world.store.status(A), Some(DomainStatus::Ready));
    assert!(
        report.events.iter().any(|e| matches!(
            e,
            StorageEvent::Materialized { barrier_id, .. } if *barrier_id == replanned
        )),
        "{:?}",
        report.events
    );
    assert_eq!(
        world.store.application_base(A).unwrap().execution_position,
        position(1)
    );
}

#[test]
fn a_boot_record_that_is_definitely_not_written_leaves_the_domain_detached() {
    // The domain was inserted before its boot record was appended and
    // stayed attached and ready when the append definitely failed: the
    // caller got an error, a retry got `AlreadyAttached`, and readers and
    // submissions served a boot the stream never recorded.
    let mut journal = ModelJournal::new();
    journal.script_append(AppendScript::DefinitelyNotCommitted);
    let mut store = JournaledStore::open(
        journal,
        CLUSTER,
        REPLICA,
        inc(),
        BOOT,
        JournalLimits::default(),
    )
    .unwrap();
    assert!(matches!(
        store.attach(A, shard(), ModelEngine::new()),
        Err(JournaledError::Journal(JournalFailure::Definite(_)))
    ));
    assert_eq!(store.status(A), None, "nothing serves an unrecorded boot");
    assert!(store.reader(A).is_none());
    assert!(store.attached().is_empty());

    // The retry attaches afresh and records the boot.
    let stream = store.attach(A, shard(), ModelEngine::new()).unwrap();
    assert_eq!(store.status(A), Some(DomainStatus::Ready));
    assert_eq!(
        store.journal().records(stream).len(),
        2,
        "genesis and boot are durable"
    );
}

#[test]
fn fencing_one_domain_never_drains_packets_or_blocks_another_domain() {
    let mut world = World::with_domains(&[A, B]);
    // B keeps proposing while A is fenced; the two share one journal and
    // one grouped write, and nothing waits for a cross-domain barrier.
    let fenced = world.store.fence(A, ballot(9)).unwrap();
    assert!(fenced.is_empty());
    assert_eq!(world.store.fenced_at(A), Some(ballot(9)));
    assert_eq!(world.store.fenced_at(B), None);

    let old = world.barriers.allocate();
    assert!(matches!(
        world.store.submit(Submission {
            domain: A,
            ballot: ballot(1),
            kind: TransitionKind::Protocol,
            batch: PersistBatch {
                barrier: old,
                base: None,
                updates: promise(1),
            },
        }),
        Err(SubmitRefused::ObsoleteBallot { .. })
    ));
    let fresh = world.protocol(A, ballot(9), promise(9));
    let unrelated = world.protocol(B, ballot(1), promise(1));
    let report = world.store.flush().unwrap();
    assert_eq!(report.appends, 1, "one group still serves both domains");
    assert_eq!(report.journaled, 2);
    assert_eq!(report.materialized, 2);
    let completed: BTreeSet<BarrierId> = report
        .events
        .iter()
        .filter_map(StorageEvent::barrier)
        .collect();
    assert_eq!(completed, BTreeSet::from([fresh, unrelated]));

    // A's cut is available while B has work of its own queued: no global
    // drain and no cross-domain election barrier.
    world.protocol(B, ballot(1), promise(2));
    assert!(world.store.recovery_cut(A).is_ok());
    assert_eq!(world.store.queued(B), 1);
}

#[test]
fn replay_restores_the_exact_state_results_and_events_from_the_records_alone() {
    // Two identical runs. The reference materializes as it goes; the
    // replayed one crashes with the journal ahead of the projection and
    // recovers at attach from the durable records only.
    let reference = run_workload(true);
    let replayed = run_workload(false);
    assert_eq!(reference, replayed);
    assert!(
        reference.iter().any(|(collection, ..)| *collection
            == coord_store_api::registry::Collection::EventsV1.id().0),
        "the workload writes revision events"
    );
}

/// Journal a fixed workload of protocol transitions and application
/// outcomes. When `materialize_inline` is false the projection is left
/// behind and the pipeline is reopened, which replays `(M, J]`.
fn run_workload(materialize_inline: bool) -> Vec<(u16, Vec<u8>, Vec<u8>)> {
    let mut world = World::new();
    let events = coord_store_api::registry::Collection::EventsV1.id();
    let executed = coord_store_api::registry::Collection::ExecutedV1.id();
    for step in 1..=4u64 {
        world.protocol(A, ballot(step), promise(step));
        if materialize_inline {
            world.store.flush().unwrap();
        } else {
            world.store.append_pending().unwrap();
        }
        world.application(
            A,
            step,
            position(step),
            vec![
                kv_update(format!("key-{step}").as_bytes(), b"value"),
                StoreUpdate {
                    collection: events,
                    key: step.to_be_bytes().to_vec(),
                    value: Some(vec![step as u8; 8]),
                },
                StoreUpdate {
                    collection: executed,
                    key: command(step as u8).as_bytes().to_vec(),
                    value: Some(step.to_be_bytes().to_vec()),
                },
            ],
        );
        if materialize_inline {
            world.store.flush().unwrap();
        } else {
            world.store.append_pending().unwrap();
        }
    }
    let frontiers = world.store.frontiers(A).unwrap();
    if materialize_inline {
        assert_eq!(frontiers.materialized(), frontiers.durable());
    } else {
        assert!(frontiers.materialized() < frontiers.durable());
    }
    // The boot ends. The next one recovers from what is actually durable.
    let (journal, mut engines) = world.store.into_parts();
    let engine = engines.pop().expect("one domain").1;
    let mut next = JournaledStore::open(
        journal,
        CLUSTER,
        REPLICA,
        inc(),
        BootId([0x88; 16]),
        JournalLimits::default(),
    )
    .unwrap();
    next.attach(A, shard(), engine).unwrap();
    let recovered = next.frontiers(A).unwrap();
    assert_eq!(recovered.materialized(), recovered.durable());
    assert_eq!(next.status(A), Some(DomainStatus::Ready));
    next.projection(A).unwrap().durable_rows()
}

#[test]
fn a_projection_ahead_of_the_journal_is_refused_rather_than_repaired() {
    let mut world = World::new();
    world.protocol(A, ballot(1), promise(1));
    world.store.flush().unwrap();
    let (journal, mut engines) = world.store.into_parts();
    let engine = engines.pop().expect("one domain").1;
    // A fresh journal with the same identity has no records at all, so the
    // surviving projection is ahead of it.
    let mut next = JournaledStore::open(
        ModelJournal::new(),
        CLUSTER,
        REPLICA,
        inc(),
        BOOT,
        JournalLimits::default(),
    )
    .unwrap();
    assert!(matches!(
        next.attach(A, shard(), engine),
        Err(JournaledError::Quarantined(_))
    ));
    drop(journal);
}

/// Steps of the atomic-initialization exploration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Step {
    /// Read the conflict lookup and submit the initialization.
    Propose(usize),
    /// Write the journal group.
    Journal,
    /// Apply durable records to the projection.
    Materialize,
}

/// The conflict lookup a proposer performs: the commands installed at the
/// view that touch any of `keys`. A placeholder (a dependency row with no
/// bound payload) is never a processed command and never appears here.
fn conflict_lookup<V: OrderedRead>(view: &V, keys: &[Vec<u8>]) -> Vec<CommandId> {
    let recovered = read_protocol(view, epoch(), ViewBudget::default())
        .expect("installed state is complete at every yield point");
    recovered
        .records
        .iter()
        .filter(|(_, r)| r.payload.is_some() && r.keys.iter().any(|k| keys.contains(k)))
        .map(|(id, _)| *id)
        .collect()
}

/// Run one schedule of two conflicting proposals over the journal-first
/// pipeline, checking the invariants after every step. `split_install`
/// deliberately writes the dependency row and the payload row in two
/// separate transitions, which the checker must catch.
fn run_schedule(
    schedule: &[Step],
    split_install: bool,
) -> Result<BTreeMap<usize, Vec<CommandId>>, String> {
    let mut world = World::new();
    let keys = vec![b"k".to_vec()];
    let mut deps: BTreeMap<usize, Vec<CommandId>> = BTreeMap::new();
    let mut half: BTreeMap<usize, StoreUpdate> = BTreeMap::new();
    let mut journaled: BTreeSet<CommandId> = BTreeSet::new();
    let mut proposed: BTreeSet<usize> = BTreeSet::new();
    let mut steps: Vec<Step> = schedule.to_vec();
    // Drive to completion after the scripted prefix so every schedule ends
    // in the same terminal state.
    for _ in 0..8 {
        steps.extend([
            Step::Propose(0),
            Step::Propose(1),
            Step::Journal,
            Step::Materialize,
        ]);
    }
    for step in steps {
        match step {
            Step::Propose(i) => {
                if proposed.contains(&i) && !half.contains_key(&i) {
                    continue;
                }
                // A serial domain actor proposes only when its previous
                // transition is durable and applied.
                if world.store.queued(A) > 0 || world.store.unmaterialized(A) > 0 {
                    continue;
                }
                let gated = world.store.reader(A).unwrap().snapshot().unwrap();
                if let Some(payload) = half.remove(&i) {
                    drop(gated);
                    world.protocol(A, ballot(1), vec![payload]);
                    continue;
                }
                let id = command(if i == 0 { 0xa1 } else { 0xb2 });
                let found = conflict_lookup(gated.view(), &keys);
                drop(gated);
                let record = CommandRecord {
                    phase: Phase::PreAccept,
                    deps: found.clone(),
                    keys: keys.clone(),
                    payload: Some(Digest32([i as u8; 32])),
                    paths: Vec::new(),
                    path: Digest32([0x9; 32]),
                };
                let dependency = dependency_update(epoch(), &id, &record).unwrap();
                let payload = payload_update(
                    &id,
                    &PayloadRecordV1 {
                        retry_key: retry_key(i as u64 + 1),
                        logical: vec![i as u8],
                    },
                )
                .unwrap();
                deps.insert(i, found);
                proposed.insert(i);
                journaled.insert(id);
                if split_install {
                    half.insert(i, payload);
                    world.protocol(A, ballot(1), vec![dependency]);
                } else {
                    world.protocol(A, ballot(1), vec![dependency, payload]);
                }
            }
            Step::Journal => {
                world.store.append_pending().unwrap();
            }
            Step::Materialize => {
                world.store.materialize().unwrap();
            }
        }
        let gated = world.store.reader(A).unwrap().snapshot().unwrap();
        match read_protocol(gated.view(), epoch(), ViewBudget::default()) {
            Ok(recovered) => {
                for (id, _) in &recovered.records {
                    if !journaled.contains(id) {
                        return Err(format!("{id:?} is visible without a durable record"));
                    }
                }
            }
            Err(e) => return Err(format!("half-installed state is visible: {e}")),
        }
    }
    Ok(deps)
}

/// Every distinct arrangement of the multiset of steps.
fn arrangements(items: &[Step]) -> Vec<Vec<Step>> {
    if items.is_empty() {
        return vec![Vec::new()];
    }
    let mut out = Vec::new();
    let mut used: Vec<Step> = Vec::new();
    for (i, item) in items.iter().enumerate() {
        if used.contains(item) {
            continue;
        }
        used.push(*item);
        let mut rest = items.to_vec();
        rest.remove(i);
        for mut tail in arrangements(&rest) {
            let mut one = vec![*item];
            one.append(&mut tail);
            out.push(one);
        }
    }
    out
}

#[test]
fn atomic_initialization_and_conflict_lookup_hold_under_every_yielding_schedule() {
    let prefix = [
        Step::Propose(0),
        Step::Propose(1),
        Step::Journal,
        Step::Journal,
        Step::Materialize,
        Step::Materialize,
    ];
    let schedules = arrangements(&prefix);
    assert_eq!(schedules.len(), 180, "every interleaving is explored");
    for schedule in &schedules {
        let deps = run_schedule(schedule, false).unwrap_or_else(|e| panic!("{schedule:?}: {e}"));
        let first = deps.get(&0).cloned().unwrap_or_default();
        let second = deps.get(&1).cloned().unwrap_or_default();
        let a_on_b = first.contains(&command(0xb2));
        let b_on_a = second.contains(&command(0xa1));
        assert!(
            a_on_b ^ b_on_a,
            "{schedule:?}: conflicting proposals must be ordered exactly one way"
        );
    }
}

#[test]
fn a_split_installation_is_detected_by_the_same_checker() {
    let prefix = [
        Step::Propose(0),
        Step::Propose(1),
        Step::Journal,
        Step::Journal,
        Step::Materialize,
        Step::Materialize,
    ];
    let caught = arrangements(&prefix)
        .iter()
        .filter(|schedule| run_schedule(schedule, true).is_err())
        .count();
    assert!(
        caught > 0,
        "splitting the installation must be visible to the invariant checker"
    );
}

#[test]
fn a_stale_application_base_is_refused_before_anything_is_journaled() {
    let mut world = World::new();
    world.application(A, 1, position(1), vec![kv_update(b"a", b"1")]);
    world.store.flush().unwrap();
    let barrier = world.barriers.allocate();
    let stale = ApplyBase {
        configuration: ConfigurationEpoch::ZERO,
        execution_position: ExecutionPosition::ZERO,
    };
    let appends = world.store.journal().appends();
    assert_eq!(
        world.store.submit(Submission {
            domain: A,
            ballot: Ballot {
                epoch: ConfigurationEpoch::ZERO,
                number: 1,
                leader: REPLICA,
            },
            kind: TransitionKind::Application {
                position: position(2),
                revision: None,
                result_digest: Digest32([2; 32]),
            },
            batch: PersistBatch {
                barrier,
                base: Some(stale),
                updates: vec![kv_update(b"a", b"2")],
            },
        }),
        Err(SubmitRefused::StaleBase {
            expected: ApplyBase {
                configuration: ConfigurationEpoch::ZERO,
                execution_position: position(1),
            }
        })
    );
    assert_eq!(world.store.journal().appends(), appends);
    // A batch from another boot is refused just as definitely.
    assert_eq!(
        world.store.submit(Submission {
            domain: A,
            ballot: ballot(1),
            kind: TransitionKind::Protocol,
            batch: PersistBatch {
                barrier: BarrierId {
                    node_generation: inc(),
                    boot_id: BootId([0xee; 16]),
                    sequence: 9,
                },
                base: None,
                updates: promise(1),
            },
        }),
        Err(SubmitRefused::WrongBoot)
    );
}

#[test]
fn the_pipeline_never_reports_a_store_sequence_that_is_not_the_materialized_record() {
    let mut world = World::new();
    world.protocol(A, ballot(1), promise(1));
    world.store.flush().unwrap();
    let applied = world.store.applied(A).unwrap();
    assert_eq!(applied.materialized, seq(3));
    assert_eq!(
        applied.store_seq().journal_seq(),
        world.store.frontiers(A).unwrap().materialized()
    );
    let gated = world.store.reader(A).unwrap().snapshot().unwrap();
    assert_eq!(gated.meta().stamp.journal_seq, seq(3));
    assert_eq!(gated.meta().stamp.last_batch_digest, applied.last_digest);
}

#[test]
fn a_projection_that_refuses_a_materialization_holds_the_durable_record_and_retries_it() {
    let mut world = World::new();
    let barrier = world.protocol(A, ballot(1), promise(1));
    world.store.append_pending().unwrap();
    world
        .store
        .projection(A)
        .unwrap()
        .script_commit(CommitScript::DefinitelyNotCommitted);
    let report = world.store.materialize().unwrap();
    assert_eq!(report.materialized, 0);
    assert!(
        report.events.is_empty(),
        "a durable journal record is never reported as a failed batch"
    );
    assert_eq!(
        world.store.status(A),
        Some(DomainStatus::MaterializationDeferred)
    );
    assert_eq!(world.store.unmaterialized(A), 1);
    // No new work is admitted while the projection is behind.
    let blocked = world.barriers.allocate();
    assert_eq!(
        world.store.submit(Submission {
            domain: A,
            ballot: ballot(2),
            kind: TransitionKind::Protocol,
            batch: PersistBatch {
                barrier: blocked,
                base: None,
                updates: promise(2),
            },
        }),
        Err(SubmitRefused::NotReady(
            DomainStatus::MaterializationDeferred
        ))
    );
    // The held record is applied as soon as the projection accepts it.
    let report = world.store.materialize().unwrap();
    assert_eq!(
        report.events,
        vec![StorageEvent::Materialized {
            barrier_id: barrier,
            journal_seq: seq(3),
        }]
    );
    assert_eq!(world.store.status(A), Some(DomainStatus::Ready));
}

#[test]
fn queued_bytes_bound_admission_and_accepted_work_is_never_evicted() {
    let mut world = World::new();
    let big = vec![kv_update(b"wide", &vec![0x11; 200 * 1024])];
    let first = world.protocol(A, ballot(1), big.clone());
    assert!(world.store.queued_bytes(A) > 200 * 1024);
    // A second batch of the same size passes the count bound but not the
    // byte bound of a deliberately small budget.
    let small = JournalLimits {
        max_queued_bytes_per_domain: 256 * 1024,
        ..JournalLimits::default()
    };
    let mut tight =
        JournaledStore::open(ModelJournal::new(), CLUSTER, REPLICA, inc(), BOOT, small).unwrap();
    tight.attach(A, shard(), ModelEngine::new()).unwrap();
    let mut barriers = BarrierAllocator::new(inc(), BOOT);
    tight
        .submit(Submission {
            domain: A,
            ballot: ballot(1),
            kind: TransitionKind::Protocol,
            batch: PersistBatch {
                barrier: barriers.allocate(),
                base: None,
                updates: big.clone(),
            },
        })
        .unwrap();
    assert_eq!(
        tight.submit(Submission {
            domain: A,
            ballot: ballot(1),
            kind: TransitionKind::Protocol,
            batch: PersistBatch {
                barrier: barriers.allocate(),
                base: None,
                updates: big,
            },
        }),
        Err(SubmitRefused::QueueFull)
    );
    // The already accepted transition is untouched and still completes.
    assert_eq!(tight.queued(A), 1);
    let report = tight.flush().unwrap();
    assert_eq!(report.journaled, 1);
    assert_eq!(report.materialized, 1);
    assert_eq!(world.store.queued(A), 1);
    let report = world.store.flush().unwrap();
    assert_eq!(report.events[0].barrier(), Some(first));
}

#[test]
fn a_batch_that_could_never_become_a_valid_record_is_refused_at_submission() {
    let mut world = World::new();
    let barrier = world.barriers.allocate();
    assert_eq!(
        world.store.submit(Submission {
            domain: A,
            ballot: ballot(1),
            kind: TransitionKind::Protocol,
            batch: PersistBatch {
                barrier,
                base: None,
                updates: vec![kv_update(b"huge", &vec![0x7; 5 * 1024 * 1024])],
            },
        }),
        Err(SubmitRefused::Record(RecordError::ValueTooLong))
    );
    let many: Vec<StoreUpdate> = (0..40u32)
        .map(|i| kv_update(&i.to_be_bytes(), &vec![0x7; 128 * 1024]))
        .collect();
    assert_eq!(
        world.store.submit(Submission {
            domain: A,
            ballot: ballot(1),
            kind: TransitionKind::Protocol,
            batch: PersistBatch {
                barrier,
                base: None,
                updates: many,
            },
        }),
        Err(SubmitRefused::Record(RecordError::TooLarge))
    );
    assert_eq!(world.store.queued(A), 0);
}

#[test]
fn a_projection_error_never_loses_durable_redo_or_its_completions() {
    // The pending list is the only in-memory record of what the
    // projection still owes for records that are already durable in the
    // journal. Taking it and then failing a pre-commit step dropped that
    // responsibility while the domain stayed ready, so a later record
    // could stamp the projection past a record it had never applied, and
    // a restart would replay only what followed the stamp.
    for fault in ["begin_write", "lower_update", "meta_write"] {
        let mut world = World::new();
        // J1 is durable but not yet materialized. It carries a row of
        // its own, so a record that is skipped leaves a hole that a
        // later record cannot paper over.
        let mut first_batch = promise(1);
        first_batch.push(kv_update(b"row-from-j1", b"v"));
        let first = world.protocol(A, ballot(1), first_batch);
        let report = world.store.append_pending().unwrap();
        assert!(
            report.events.iter().any(|e| matches!(
                e,
                StorageEvent::JournalDurable { barrier_id, .. } if *barrier_id == first
            )),
            "{fault}: the record is durable"
        );

        let engine = world.store.projection(A).unwrap();
        match fault {
            "begin_write" => engine.inject_begin_write_error(),
            // The first write of the transaction is the first lowered
            // update; the projection metadata is written last.
            "lower_update" => engine.inject_write_error_at(1),
            // Two updates are lowered, then the metadata is written.
            _ => engine.inject_write_error_at(3),
        }
        let failed = world.store.materialize();
        assert!(failed.is_err(), "{fault}: the projection failed");

        // The redo is still owed, and the domain has not silently moved
        // on: the same records are applied when the fault clears.
        world.store.projection(A).unwrap().clear_injected_faults();
        let before = world.store.frontiers(A).unwrap();
        assert!(
            before.materialized() < before.durable(),
            "{fault}: nothing was materialized"
        );

        // A later record must not be stamped over the unapplied one.
        let mut second_batch = promise(2);
        second_batch.push(kv_update(b"row-from-j2", b"v"));
        world.protocol(A, ballot(2), second_batch);
        world.store.flush().unwrap();
        let after = world.store.frontiers(A).unwrap();
        assert_eq!(
            after.materialized(),
            after.durable(),
            "{fault}: both records were applied, in order"
        );
        // Both barriers completed, the first one included: its journal
        // completion was produced before the failing stage and is not
        // discarded with the error.
        let rows = world.store.projection(A).unwrap().durable_rows();
        for expected in [b"row-from-j1".as_slice(), b"row-from-j2".as_slice()] {
            assert!(
                rows.iter().any(|(_, key, _)| key.as_slice() == expected),
                "{fault}: {} was applied, not dropped",
                String::from_utf8_lossy(expected)
            );
        }
    }
}

#[test]
fn a_command_after_replay_continues_from_the_recovered_position() {
    // attach() set the application frontiers from the projection as it
    // was found, and replay never moved them. Recovered rows looked
    // right while the store still expected the pre-crash base: the next
    // correctly planned command was refused as stale, and a command
    // planned from application_base() carried a base the journal record
    // could no longer extend.
    let mut world = World::new();
    world.protocol(A, ballot(1), promise(1));
    world.store.flush().unwrap();
    // An application outcome is journaled but the projection never
    // commits it: the ordinary crash cut.
    world.application(A, 1, position(1), vec![kv_update(b"k", b"v1")]);
    world.store.append_pending().unwrap();
    let frontiers = world.store.frontiers(A).unwrap();
    assert!(frontiers.materialized() < frontiers.durable());

    let (journal, mut engines) = world.store.into_parts();
    let engine = engines.pop().expect("one domain").1;
    let mut next = JournaledStore::open(
        journal,
        CLUSTER,
        REPLICA,
        inc(),
        BootId([0x88; 16]),
        JournalLimits::default(),
    )
    .unwrap();
    next.attach(A, shard(), engine).unwrap();
    assert_eq!(next.status(A), Some(DomainStatus::Ready));

    // The recovered history, not the projection as it was found.
    let base = next.application_base(A).expect("attached");
    assert_eq!(
        base.execution_position,
        position(1),
        "the replayed outcome is the base the next command extends"
    );

    // And the next command actually succeeds at the next position.
    let barrier = BarrierId {
        node_generation: inc(),
        boot_id: BootId([0x88; 16]),
        sequence: 9,
    };
    next.submit(Submission {
        domain: A,
        ballot: Ballot {
            epoch: base.configuration,
            number: 2,
            leader: REPLICA,
        },
        kind: TransitionKind::Application {
            position: position(2),
            revision: None,
            result_digest: Digest32([2; 32]),
        },
        batch: PersistBatch {
            barrier,
            base: Some(base),
            updates: vec![kv_update(b"k", b"v2")],
        },
    })
    .expect("the command extends the recovered history");
    let report = next.flush().unwrap();
    assert!(
        report.events.iter().any(|e| matches!(
            e,
            StorageEvent::Materialized { barrier_id, .. } if *barrier_id == barrier
        )),
        "{:?}",
        report.events
    );
    assert_eq!(
        next.application_base(A).unwrap().execution_position,
        position(2)
    );
}

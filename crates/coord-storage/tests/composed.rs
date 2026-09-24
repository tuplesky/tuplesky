//! Acceptance for the composed application path (task-j08).
//!
//! The same planner, admission and batch lowering feed two coordinators:
//! the reference path, where the projection is itself the durable
//! record, and the journal-first path, where the record reaches the
//! shared journal first and the projection afterwards. These hold that
//! the first is true -- that they really are the same logic -- and that
//! the second's extra moment between "recorded" and "readable" is never
//! reported as the command having happened.
//!
//! The completion rule is the whole point. Submitting says the batch was
//! taken. A flush that did not fail says something was lowered. An event
//! naming the barrier includes `JournalDurable`, which on the
//! journal-first path means the record is safe and the state nobody can
//! read yet. Only the matching barrier's own `Materialized` says the
//! command has happened, and reporting an outcome on any of the others
//! publishes a revision the next reader will not find.

use coord_consensus::PayloadRecordV1;
use coord_core::effect::{BootId, PersistBatch};
use coord_core::outbox::BarrierAllocator;
use coord_journal_api::stream::ShardId;
use coord_state::policy::{Action, KeyInterval, PolicyRule};
use coord_storage::journaled::{JournalLimits, JournaledStore, TransitionKind};
use coord_storage::policy::{bootstrap_session, rule_update};
use coord_storage::{
    Applier, ApplyOutcome, GroupLimits, JournaledDomain, Persistence, StoreWorker, complete, submit,
};
use coord_store_testkit::journal::ModelJournal;
use coord_store_testkit::model::{CommitScript, ModelEngine};
use coord_types::ids::*;
use coord_types::logical_v1::*;
use coord_types::{CommandId, RetryKey};

const NS: NamespaceId = NamespaceId([5; 16]);
const SESSION: SessionId = SessionId([3; 16]);
const CLIENT: ClientInstanceId = ClientInstanceId([4; 16]);
const ALICE: PrincipalId = PrincipalId([0xa; 16]);
const CLUSTER: ClusterId = ClusterId([1; 16]);
const DOMAIN: DomainId = DomainId([2; 16]);
const REPLICA: ReplicaId = ReplicaId([7; 16]);
const BOOT: BootId = BootId([1; 16]);

fn inc() -> ReplicaIncarnation {
    ReplicaIncarnation::new(1).unwrap()
}

fn retry_key(seq: u64) -> RetryKey {
    RetryKey {
        cluster_id: CLUSTER,
        domain_id: DOMAIN,
        session_id: SESSION,
        client_instance_id: CLIENT,
        request_sequence: RequestSequence::new(seq).unwrap(),
    }
}

fn req(op: CanonicalOperation) -> LogicalRequest {
    let mut r = LogicalRequest::new(NS, op);
    r.canonicalize();
    r
}

fn put(key: &[u8], value: &[u8]) -> LogicalRequest {
    req(CanonicalOperation::Put(PutOp {
        key: key.to_vec(),
        value: value.to_vec(),
        lease: None,
        prev_kv: false,
    }))
}

fn payload(seq: u64, request: &LogicalRequest) -> (CommandId, PayloadRecordV1) {
    let command = CommandId::derive(&retry_key(seq), request).unwrap();
    (
        command,
        PayloadRecordV1 {
            retry_key: retry_key(seq),
            logical: postcard::to_allocvec(request).unwrap(),
        },
    )
}

/// Alice's session and the policy that lets her write: the same
/// bootstrap for both coordinators, so a difference between them is a
/// difference in the coordinator and not in what they were given.
fn bootstrap() -> Vec<coord_core::effect::StoreUpdate> {
    let mut updates = bootstrap_session(&SESSION, ALICE, 1 << 20, true).unwrap();
    for (i, action) in Action::ALL.iter().enumerate() {
        updates.push(
            rule_update(
                &PolicyRuleId([i as u8 + 1; 16]),
                &PolicyRule {
                    principal: ALICE,
                    action: *action,
                    namespace: NS,
                    interval: KeyInterval::all(),
                },
            )
            .unwrap(),
        );
    }
    updates
}

/// The reference driver: one worker, the projection is the record.
fn reference() -> Applier<StoreWorker<ModelEngine>> {
    let mut worker =
        StoreWorker::open(ModelEngine::new(), BOOT, inc(), GroupLimits::default()).unwrap();
    let mut alloc = BarrierAllocator::new(inc(), BOOT);
    worker
        .submit(PersistBatch {
            barrier: alloc.allocate(),
            base: Some(worker.application_base()),
            updates: bootstrap(),
        })
        .unwrap();
    worker.flush().unwrap();
    Applier::new(worker, alloc).unwrap()
}

/// The serving driver: one shared journal, one attached projection.
fn journaled() -> Applier<JournaledDomain<ModelJournal, ModelEngine>> {
    let mut store = JournaledStore::open(
        ModelJournal::new(),
        CLUSTER,
        REPLICA,
        inc(),
        BOOT,
        JournalLimits::default(),
    )
    .unwrap();
    store
        .attach(DOMAIN, ShardId::new(0).unwrap(), ModelEngine::new())
        .unwrap();
    // A transition's ballot is in the epoch of the configuration its
    // base names: the record binds base configuration, context epoch and
    // ballot epoch to one value, and a projection that has never
    // executed a command is still at the initial configuration. In a
    // real node the replica's ballot is already in the committed epoch,
    // so this holds without saying; here it has to be said.
    let base = store.application_base(DOMAIN).unwrap();
    let mut domain = JournaledDomain::new(
        store,
        DOMAIN,
        Ballot {
            epoch: base.configuration,
            number: 0,
            leader: REPLICA,
        },
    )
    .expect("attached");
    let mut alloc = BarrierAllocator::new(inc(), BOOT);

    // The bootstrap is an application transition like any other: it
    // carries a base and moves the frontier, and it goes through the
    // journal because the journal is this profile's only writer.
    domain
        .submit(
            PersistBatch {
                barrier: alloc.allocate(),
                base: Some(base),
                updates: bootstrap(),
            },
            TransitionKind::Application {
                position: base.execution_position.checked_next().unwrap(),
                revision: None,
                result_digest: coord_types::identity::Digest32([0; 32]),
            },
        )
        .unwrap();
    domain.lower().unwrap();
    Applier::new(domain, alloc).unwrap()
}

/// The same failure-free workload through both drivers produces the same
/// logical results, the same revisions and the same retained outcomes.
///
/// If this ever diverges, the two paths are not sharing the planner and
/// the admission logic after all, and one of them is quietly a second
/// implementation.
#[test]
fn the_reference_and_journaled_drivers_agree_on_a_failure_free_workload() {
    let mut reference = reference();
    let mut journaled = journaled();

    let mut seen = Vec::new();
    for seq in 1..=8u64 {
        let request = put(format!("k{}", seq % 3).as_bytes(), b"v");
        let (command, record) = payload(seq, &request);

        let a = reference
            .apply(command, &record)
            .expect("reference applied");
        let b = journaled
            .apply(command, &record)
            .expect("journaled applied");

        assert_eq!(
            a, b,
            "the drivers disagreed on command {seq}: {a:?} vs {b:?}"
        );
        seen.push(a);
    }

    // Every command took its own position and produced its own revision,
    // so the agreement above is agreement about real work.
    let positions: Vec<_> = seen.iter().map(|o| o.position).collect();
    let mut sorted = positions.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), positions.len(), "positions repeated");
    assert!(seen.iter().all(|o| o.revision.is_some()));

    // And both hands hold the same view afterwards.
    assert_eq!(
        reference.kv_revision().unwrap(),
        journaled.kv_revision().unwrap()
    );
}

/// A record that is durable in the journal but whose projection has not
/// taken it is not an applied command.
///
/// This is the case the reference path cannot have and the journal-first
/// path has on every command: the moment between the record being safe
/// and the state being readable. Reporting the outcome there would hand
/// a caller a revision, and publish a watch event for it, that the very
/// next read would not find.
#[test]
fn a_journaled_record_whose_projection_is_held_is_not_yet_applied() {
    let mut applier = journaled();
    let revision_before = applier.kv_revision().unwrap();

    // An application transition with real updates behind it. What it
    // writes does not matter here; that it is recorded before it is
    // readable does.
    let base = applier.store().application_base();
    let barrier = applier.alloc().allocate();
    let position = base.execution_position.checked_next().unwrap();
    let pending = coord_storage::Pending {
        barrier,
        position,
        revision: None,
        result_digest: coord_types::identity::Digest32([3; 32]),
    };
    submit(
        applier.store_mut(),
        PersistBatch {
            barrier,
            base: Some(base),
            updates: vec![
                rule_update(
                    &PolicyRuleId([0x5a; 16]),
                    &PolicyRule {
                        principal: ALICE,
                        action: Action::Read,
                        namespace: NS,
                        interval: KeyInterval::all(),
                    },
                )
                .unwrap(),
            ],
        },
        TransitionKind::Application {
            position,
            revision: None,
            result_digest: pending.result_digest,
        },
    )
    .expect("submitted");

    // The projection will not take it. Everything else runs as it
    // normally would, so the lowering inside `complete` is the one that
    // journals the record -- which means `complete` sees this barrier's
    // own `JournalDurable` go past, which is the event a weaker rule
    // would mistake for the completion.
    applier
        .store_mut()
        .store_mut()
        .projection(DOMAIN)
        .unwrap()
        .script_commit(CommitScript::Indeterminate { applied: false });

    let outcome = complete(applier.store_mut(), &pending).expect("completion decided");
    assert_eq!(
        outcome,
        ApplyOutcome::Indeterminate,
        "a record the projection has not taken was reported as applied"
    );

    // The record really did reach the journal -- so this is the
    // recorded-but-not-readable moment and not a failure to record --
    // and the projection is still owed it.
    assert!(
        applier.store().store().unmaterialized(DOMAIN) > 0
            || applier.store().store().status(DOMAIN)
                == Some(coord_storage::journaled::DomainStatus::MaterializationUncertain),
        "the record was not durable and owed: {:?}",
        applier.store().store().status(DOMAIN)
    );

    // Nothing became readable, and the pending application is intact:
    // the identity, position and result it was planned with are still
    // exactly what it will report once it does materialize.
    assert_eq!(applier.kv_revision().unwrap(), revision_before);
    assert_eq!(pending.position, position);
    assert_eq!(pending.barrier, barrier);

    // And it is still owed: reconciliation resolves it rather than the
    // command being replaced by a new plan.
    applier
        .store_mut()
        .store_mut()
        .projection(DOMAIN)
        .unwrap()
        .clear_injected_faults();
    let resolved = applier.store_mut().reconcile().expect("reconciled");
    assert!(
        resolved.events.iter().any(|e| matches!(
            e,
            coord_core::event::StorageEvent::Materialized { barrier_id, .. }
                if *barrier_id == barrier
        )),
        "the owed materialization never arrived: {:?}",
        resolved.events
    );
}

/// Another batch's completion is not this application's completion.
///
/// The shared pipeline lowers whole groups, so a flush routinely carries
/// events for work that is not ours. A completion rule that accepted any
/// successful flush, or any event at all, would report an outcome for a
/// command whose own record had not landed.
#[test]
fn another_batchs_completion_never_completes_this_application() {
    let mut applier = journaled();
    let base = applier.store().application_base();
    let revision = applier.kv_revision().unwrap();

    // A pending application that is never submitted: nothing will ever
    // materialize its barrier, however much other work completes.
    let orphan = coord_storage::Pending {
        barrier: coord_core::effect::BarrierId {
            node_generation: inc(),
            boot_id: BOOT,
            sequence: 9_999,
        },
        position: base.execution_position,
        revision: None,
        result_digest: coord_types::identity::Digest32([9; 32]),
    };

    // Meanwhile a real command goes through and completes.
    let request = put(b"other", b"v");
    let (command, record) = payload(1, &request);
    let applied = applier.apply(command, &record).expect("the other command");
    assert!(applied.revision.is_some());
    assert!(applier.kv_revision().unwrap() > revision);

    // The orphan is still not complete, and the driver says so rather
    // than taking the other command's materialization for its own.
    let refused = complete(applier.store_mut(), &orphan);
    assert!(
        refused.is_err(),
        "another batch's completion was taken as this one's: {refused:?}"
    );
}

/// A command that has been applied is resolved from its retained result
/// on a retry, never executed a second time.
///
/// This is what makes a crash between the record and the reply safe: the
/// retry finds the outcome that already exists, with the same position,
/// revision and identity, rather than planning a replacement.
#[test]
fn a_retry_finds_the_retained_outcome_and_does_not_execute_again() {
    for label in ["reference", "journaled"] {
        let request = put(b"k", b"v");
        let (command, record) = payload(1, &request);

        let (first, second, revision_after) = if label == "reference" {
            let mut a = reference();
            let first = a.apply(command, &record).unwrap();
            let after = a.kv_revision().unwrap();
            let second = a.apply(command, &record).unwrap();
            (first, second, (after, a.kv_revision().unwrap()))
        } else {
            let mut a = journaled();
            let first = a.apply(command, &record).unwrap();
            let after = a.kv_revision().unwrap();
            let second = a.apply(command, &record).unwrap();
            (first, second, (after, a.kv_revision().unwrap()))
        };

        assert_eq!(first, second, "{label}: the retry produced a new outcome");
        assert_eq!(
            revision_after.0, revision_after.1,
            "{label}: the retry executed the command again"
        );
    }
}

/// A projection that definitely refused a materialization has not failed
/// the command: the record is durable in the journal, and what is owed
/// is the materialization, not a replacement plan.
///
/// This is the trap the completion rule exists for, from the other side.
/// The coordinator deliberately emits *no* event for this case -- a
/// durable journal record is never reported as a failed batch -- so a
/// completion rule that read "no event for my barrier and nothing
/// queued" as an error would turn an owed materialization into a
/// corruption report, and one that read it as a definite noncommit would
/// execute the command a second time.
#[test]
fn a_refused_projection_commit_owes_a_materialization_not_a_new_plan() {
    let mut applier = journaled();
    let revision_before = applier.kv_revision().unwrap();

    let base = applier.store().application_base();
    let barrier = applier.alloc().allocate();
    let position = base.execution_position.checked_next().unwrap();
    let pending = coord_storage::Pending {
        barrier,
        position,
        revision: None,
        result_digest: coord_types::identity::Digest32([4; 32]),
    };
    submit(
        applier.store_mut(),
        PersistBatch {
            barrier,
            base: Some(base),
            updates: vec![
                rule_update(
                    &PolicyRuleId([0x5b; 16]),
                    &PolicyRule {
                        principal: ALICE,
                        action: Action::Read,
                        namespace: NS,
                        interval: KeyInterval::all(),
                    },
                )
                .unwrap(),
            ],
        },
        TransitionKind::Application {
            position,
            revision: None,
            result_digest: pending.result_digest,
        },
    )
    .expect("submitted");

    applier
        .store_mut()
        .store_mut()
        .projection(DOMAIN)
        .unwrap()
        .script_commit(CommitScript::DefinitelyNotCommitted);

    // One refusal is deferred and then redone: the command that was
    // recorded is the command that happens, and it keeps its own
    // barrier. It is never reported as a batch to plan again.
    let outcome = complete(applier.store_mut(), &pending).expect("completion decided");
    assert_ne!(
        outcome,
        ApplyOutcome::Replan,
        "an owed materialization was reported as a command to plan again"
    );
    let ApplyOutcome::Applied(events) = &outcome else {
        panic!("the deferred record was never materialized: {outcome:?}");
    };
    assert!(events.iter().any(|e| matches!(
        e,
        coord_core::event::StorageEvent::Materialized { barrier_id, .. } if *barrier_id == barrier
    )));
    assert_eq!(applier.store().store().unmaterialized(DOMAIN), 0);
    let _ = revision_before;

    let settled = outcome;
    assert!(matches!(settled, ApplyOutcome::Applied(_)));
}

/// A projection that keeps refusing does not hold the caller for ever.
///
/// The record is durable either way, so what the caller is owed is an
/// answer: reconcile. Spinning here would turn one slow projection into
/// a stalled replica, and reporting a failure would replace a command
/// that already exists in the journal.
#[test]
fn a_projection_that_keeps_refusing_yields_rather_than_spinning() {
    let mut applier = journaled();
    let base = applier.store().application_base();
    let barrier = applier.alloc().allocate();
    let position = base.execution_position.checked_next().unwrap();
    let pending = coord_storage::Pending {
        barrier,
        position,
        revision: None,
        result_digest: coord_types::identity::Digest32([5; 32]),
    };
    submit(
        applier.store_mut(),
        PersistBatch {
            barrier,
            base: Some(base),
            updates: vec![
                rule_update(
                    &PolicyRuleId([0x5c; 16]),
                    &PolicyRule {
                        principal: ALICE,
                        action: Action::Read,
                        namespace: NS,
                        interval: KeyInterval::all(),
                    },
                )
                .unwrap(),
            ],
        },
        TransitionKind::Application {
            position,
            revision: None,
            result_digest: pending.result_digest,
        },
    )
    .expect("submitted");

    // Refuse far more times than the completion will wait.
    for _ in 0..512 {
        applier
            .store_mut()
            .store_mut()
            .projection(DOMAIN)
            .unwrap()
            .script_commit(CommitScript::DefinitelyNotCommitted);
    }

    let outcome = complete(applier.store_mut(), &pending).expect("completion decided");
    assert_eq!(
        outcome,
        ApplyOutcome::Indeterminate,
        "a persistently refused materialization was not left to reconciliation"
    );
    // The record is still durable and still owed: nothing was lost and
    // nothing was replaced.
    assert_eq!(applier.store().store().unmaterialized(DOMAIN), 1);
}

/// A failure whose outcome is unknown sends the caller to reconcile, not
/// to plan again.
///
/// Neither coordinator emits this today: both report a failed batch only
/// as `DefinitelyNotCommitted`, which is a guard rejection before the
/// record existed. `StorageError::Indeterminate` is nevertheless a
/// defined outcome, and the difference between the two decides whether a
/// command that may already have happened is executed a second time. So
/// the rule is stated against the seam itself rather than left to
/// whichever coordinator happens to produce it first.
#[test]
fn a_failure_of_unknown_outcome_is_reconciled_rather_than_replanned() {
    use coord_core::effect::{ApplyBase, BarrierId};
    use coord_core::event::{StorageError, StorageEvent};
    use coord_storage::{Lowered, Refused};

    /// A coordinator that answers one lowering with the failure under
    /// test and nothing else.
    struct Reports(StorageError, BarrierId, bool);

    impl Persistence for Reports {
        type Reader = coord_store_testkit::model::ModelReader;

        fn boot(&self) -> BootId {
            BOOT
        }
        fn application_base(&self) -> ApplyBase {
            unreachable!("completion reads no base")
        }
        fn reader(&self) -> coord_storage::GatedReader<Self::Reader> {
            unreachable!("completion reads no view")
        }
        fn queued(&self) -> usize {
            usize::from(!self.2)
        }
        fn unmaterialized(&self) -> usize {
            0
        }
        fn submit(&mut self, _: PersistBatch, _: TransitionKind) -> Result<(), Refused> {
            unreachable!("nothing is submitted here")
        }
        fn recovered(
            &self,
            _: coord_types::ids::ConfigurationEpoch,
            _: coord_storage::views::ViewBudget,
        ) -> Result<coord_storage::protocol::RecoveredProtocol, coord_store_api::engine::EngineError>
        {
            unreachable!("completion recovers nothing")
        }
        fn lower(&mut self) -> Result<Lowered, coord_store_api::engine::EngineError> {
            self.2 = true;
            Ok(Lowered {
                events: vec![StorageEvent::Failed {
                    barrier_id: self.1,
                    error: self.0,
                }],
                indeterminate: false,
            })
        }
        fn reconcile(&mut self) -> Result<Lowered, coord_store_api::engine::EngineError> {
            unreachable!("completion does not reconcile")
        }
    }

    let barrier = BarrierId {
        node_generation: inc(),
        boot_id: BOOT,
        sequence: 1,
    };
    let pending = coord_storage::Pending {
        barrier,
        position: ExecutionPosition::new(1).unwrap(),
        revision: None,
        result_digest: coord_types::identity::Digest32([6; 32]),
    };

    // A guard rejection: the record never existed, so the command is
    // planned again.
    let mut definite = Reports(StorageError::DefinitelyNotCommitted, barrier, false);
    assert_eq!(
        complete(&mut definite, &pending).expect("decided"),
        ApplyOutcome::Replan
    );

    // Anything else: the record may exist, so reconciliation decides and
    // the command is not replaced.
    for unknown in [
        StorageError::Indeterminate,
        StorageError::Quarantine,
        StorageError::NoSpace,
    ] {
        let mut uncertain = Reports(unknown, barrier, false);
        assert_eq!(
            complete(&mut uncertain, &pending).expect("decided"),
            ApplyOutcome::Indeterminate,
            "{unknown:?} was treated as a definite noncommit"
        );
    }
}

/// A boot that ends after a command's record is durable, but before the
/// projection has taken it, recovers the command -- not a hole where it
/// was, and not a second execution of it.
///
/// This is the crash the journal-first profile exists to survive. The
/// record is the authority, so the next boot replays what the projection
/// owes and the command's own outcome comes back: the same position, the
/// same revision, the same identity. A retry after that finds the
/// retained result rather than planning a replacement.
#[test]
fn a_boot_that_ends_between_the_record_and_the_projection_recovers_the_command() {
    let mut applier = journaled();
    let request = put(b"survivor", b"v");
    let (command, record) = payload(1, &request);

    // The command is planned and its record journaled, but the
    // projection is not allowed to take it.
    let base = applier.store().application_base();
    let barrier = applier.alloc().allocate();
    let position = base.execution_position.checked_next().unwrap();
    let pending = coord_storage::Pending {
        barrier,
        position,
        revision: None,
        result_digest: coord_types::identity::Digest32([7; 32]),
    };
    submit(
        applier.store_mut(),
        PersistBatch {
            barrier,
            base: Some(base),
            updates: vec![
                rule_update(
                    &PolicyRuleId([0x5d; 16]),
                    &PolicyRule {
                        principal: ALICE,
                        action: Action::Read,
                        namespace: NS,
                        interval: KeyInterval::all(),
                    },
                )
                .unwrap(),
            ],
        },
        TransitionKind::Application {
            position,
            revision: None,
            result_digest: pending.result_digest,
        },
    )
    .expect("submitted");
    applier
        .store_mut()
        .store_mut()
        .append_pending()
        .expect("the record reaches the journal");
    let owed = applier.store().store().unmaterialized(DOMAIN);
    assert_eq!(owed, 1, "the projection should still owe the record");
    let durable_before = applier
        .store()
        .store()
        .frontiers(DOMAIN)
        .expect("attached")
        .durable();

    // The boot ends here, with the record durable and the projection
    // behind it.
    let (journal, mut engines): (ModelJournal, Vec<(DomainId, ModelEngine)>) =
        applier.into_store().into_store().into_parts();
    let engine = engines.pop().expect("one domain").1;

    // The next boot recovers from what is actually durable: attaching
    // replays the suffix the projection owed.
    let mut next = JournaledStore::open(
        journal,
        CLUSTER,
        REPLICA,
        inc(),
        BootId([0x88; 16]),
        JournalLimits::default(),
    )
    .expect("reopened");
    next.attach(DOMAIN, ShardId::new(0).unwrap(), engine)
        .expect("attached");
    let recovered = next.frontiers(DOMAIN).expect("attached");
    assert_eq!(
        recovered.materialized(),
        recovered.durable(),
        "the projection did not catch up with the journal it had"
    );
    // The frontier advances by the boot's own lifecycle record -- the
    // new boot records itself in the stream as it attaches -- but never
    // goes backwards: everything the previous boot made durable is still
    // there underneath it.
    assert!(
        recovered.durable() >= durable_before,
        "the recovered journal lost what was durable: {durable_before:?} then {:?}",
        recovered.durable()
    );
    assert_eq!(next.unmaterialized(DOMAIN), 0, "something is still owed");

    // And the recovered node executes the command exactly once: the
    // first application is a real one, and the retry after it resolves
    // from the retained result.
    let base = next.application_base(DOMAIN).expect("attached");
    let domain = JournaledDomain::new(
        next,
        DOMAIN,
        Ballot {
            epoch: base.configuration,
            number: 0,
            leader: REPLICA,
        },
    )
    .expect("attached");
    let mut applier =
        Applier::new(domain, BarrierAllocator::new(inc(), BootId([0x88; 16]))).expect("applier");

    let first = applier.apply(command, &record).expect("applied");
    let revision = applier.kv_revision().unwrap();
    let retry = applier.apply(command, &record).expect("retried");
    assert_eq!(first, retry, "the retry produced a different outcome");
    assert_eq!(
        applier.kv_revision().unwrap(),
        revision,
        "the retry executed the command a second time"
    );
}

/// A higher promise closes admission without losing what is already
/// durable, and a late completion from the ballot it fenced does not
/// newly authorize anything.
///
/// Fencing is about what may still *enter* the record. Work that is
/// already in the journal is an obligation this replica has taken on,
/// and a new promise does not release it: the cut a recovering ballot
/// takes has to keep summarizing it. What the fence does stop is a
/// transition of the older ballot being admitted afterwards -- that
/// would be this replica voting under a term it has already given up.
#[test]
fn a_higher_promise_fences_admission_without_discarding_durable_obligations() {
    let mut applier = journaled();
    let old = applier.store().ballot();

    // An application of the current ballot reaches the journal, and the
    // projection is behind it.
    let base = applier.store().application_base();
    let barrier = applier.alloc().allocate();
    let position = base.execution_position.checked_next().unwrap();
    submit(
        applier.store_mut(),
        PersistBatch {
            barrier,
            base: Some(base),
            updates: vec![
                rule_update(
                    &PolicyRuleId([0x5e; 16]),
                    &PolicyRule {
                        principal: ALICE,
                        action: Action::Read,
                        namespace: NS,
                        interval: KeyInterval::all(),
                    },
                )
                .unwrap(),
            ],
        },
        TransitionKind::Application {
            position,
            revision: None,
            result_digest: coord_types::identity::Digest32([8; 32]),
        },
    )
    .expect("submitted");
    applier
        .store_mut()
        .store_mut()
        .append_pending()
        .expect("journaled");
    let durable_before = applier
        .store()
        .store()
        .frontiers(DOMAIN)
        .expect("attached")
        .durable();
    assert_eq!(applier.store().store().unmaterialized(DOMAIN), 1);

    // A higher promise arrives while the projection still lags.
    let newer = Ballot {
        epoch: old.epoch,
        number: old.number + 1,
        leader: REPLICA,
    };
    applier
        .store_mut()
        .store_mut()
        .fence(DOMAIN, newer)
        .expect("fenced");

    // What was durable is still durable, and still owed: a promise does
    // not release an obligation this replica already took on.
    assert_eq!(
        applier
            .store()
            .store()
            .frontiers(DOMAIN)
            .expect("attached")
            .durable(),
        durable_before,
        "fencing discarded a durable record"
    );
    assert_eq!(applier.store().store().unmaterialized(DOMAIN), 1);

    // A transition of the fenced ballot is refused rather than recorded:
    // admitting it would be this replica acting under a term it gave up.
    let stale = applier.alloc().allocate();
    let refused = submit(
        applier.store_mut(),
        PersistBatch {
            barrier: stale,
            base: None,
            updates: vec![
                rule_update(
                    &PolicyRuleId([0x5f; 16]),
                    &PolicyRule {
                        principal: ALICE,
                        action: Action::Read,
                        namespace: NS,
                        interval: KeyInterval::all(),
                    },
                )
                .unwrap(),
            ],
        },
        TransitionKind::Protocol,
    );
    assert!(
        refused.is_err(),
        "a transition of the fenced ballot was admitted: {refused:?}"
    );

    // Under the new promise the domain serves again, and the record the
    // old ballot left behind is materialized rather than dropped.
    applier.store_mut().set_ballot(newer);
    let settled = applier.store_mut().lower().expect("lowered");
    assert!(
        settled.events.iter().any(|e| matches!(
            e,
            coord_core::event::StorageEvent::Materialized { barrier_id, .. }
                if *barrier_id == barrier
        )),
        "the fenced ballot's durable record was never materialized: {:?}",
        settled.events
    );
    assert_eq!(applier.store().store().unmaterialized(DOMAIN), 0);
}

/// A replica recovers what the *journal* holds, not what the projection
/// has caught up with.
///
/// This is the trap the persistence seam exists to close. On the
/// journal-first profile a promise is durable the moment the journal has
/// it, and the projection may be behind by any number of records. A
/// replica that rebuilt its ballot state from the projection would come
/// back not knowing about a promise it had already made -- and would
/// then be free to accept a ballot it had promised not to.
///
/// `Persistence::recovered` is the only way through the seam, and on
/// this coordinator it reads the authoritative cut. Swapping it for the
/// projection snapshot fails here.
#[test]
fn a_replica_recovers_a_promise_the_projection_has_not_caught_up_with() {
    use coord_consensus::rows::{PromiseRecordV1, promise_update};
    use coord_storage::protocol::read_protocol;
    use coord_storage::views::ViewBudget;

    let mut applier = journaled();
    let epoch = applier.store().application_base().configuration;
    let promised = Ballot {
        epoch,
        number: 9,
        leader: REPLICA,
    };

    // A promise, journaled and deliberately left unmaterialized.
    let barrier = applier.alloc().allocate();
    applier
        .store_mut()
        .submit(
            PersistBatch {
                barrier,
                base: None,
                updates: vec![
                    promise_update(
                        epoch,
                        &PromiseRecordV1 {
                            promised,
                            synced: promised,
                        },
                    )
                    .unwrap(),
                ],
            },
            TransitionKind::Protocol,
        )
        .expect("accepted");
    applier
        .store_mut()
        .store_mut()
        .append_pending()
        .expect("journaled");

    // The projection alone does not have it, and would say this replica
    // promised nothing.
    let gated = applier.store().reader().snapshot().expect("snapshot");
    assert!(
        read_protocol(gated.view(), epoch, ViewBudget::default())
            .expect("readable")
            .promise
            .is_none(),
        "the projection caught up on its own; this test is no longer about a lagging one"
    );
    drop(gated);

    // The seam does, without waiting for materialization.
    let recovered = applier
        .store()
        .recovered(epoch, ViewBudget::default())
        .expect("recoverable");
    assert_eq!(
        recovered.promise.expect("the promise is durable").promised,
        promised,
        "recovery forgot a promise the journal already held"
    );
}

/// The journal-first coordinator follows the ballot its replica's
/// machine is at: a follower's transitions carry the leader and number
/// it actually voted under, not its own replica as leader. The epoch
/// stays the one the application base names, because that is what an
/// application transition is checked against when it is recorded.
#[test]
fn the_journal_stamps_the_ballot_the_machine_is_at() {
    use coord_storage::Persistence;

    let mut applier = journaled();
    let epoch = applier.store().application_base().configuration;
    let leader = ReplicaId([0x01; 16]);
    applier.store_mut().follow_ballot(Ballot {
        epoch: ConfigurationEpoch::new(9).unwrap(),
        number: 4,
        leader,
    });
    assert_eq!(
        applier.store().ballot(),
        Ballot {
            epoch,
            number: 4,
            leader,
        }
    );
}

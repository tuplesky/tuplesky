//! Worker fixtures over the model engine and redb: visibility before
//! completion, definite guard rejection, indeterminate commit, lost/old-boot
//! completion, and protocol stamps not invalidating the application base.

use coord_core::effect::{ApplyBase, BarrierId, BootId, CollectionId, PersistBatch, StoreUpdate};
use coord_core::event::{StorageError, StorageEvent};
use coord_core::outbox::BarrierAllocator;
use coord_storage::{FlushOutcome, GroupLimits, StoreWorker, SubmitError, ViewError, WorkerState};
use coord_storage_redb::{Generation, OpenOptions, StoreIdentity};
use coord_store_api::engine::{LocalEngine, OrderedRead};
use coord_store_api::registry::Collection;
use coord_store_testkit::model::{CommitScript, ModelEngine};
use coord_types::ids::*;

const BOOT_A: BootId = BootId([0xa; 16]);
const BOOT_B: BootId = BootId([0xb; 16]);
const KV: CollectionId = Collection::KvCurrentV1.id();
const PROTO: CollectionId = Collection::ProtocolV1.id();

fn inc() -> ReplicaIncarnation {
    ReplicaIncarnation::new(1).unwrap()
}

fn app_batch(barrier: BarrierId, base: ApplyBase, key: &[u8], value: &[u8]) -> PersistBatch {
    PersistBatch {
        barrier,
        base: Some(base),
        updates: vec![StoreUpdate {
            collection: KV,
            key: key.to_vec(),
            value: Some(value.to_vec()),
        }],
    }
}

fn proto_batch(barrier: BarrierId, key: &[u8]) -> PersistBatch {
    PersistBatch {
        barrier,
        base: None,
        updates: vec![StoreUpdate {
            collection: PROTO,
            key: key.to_vec(),
            value: Some(b"vote".to_vec()),
        }],
    }
}

fn durable_of(o: &FlushOutcome) -> Vec<BarrierId> {
    o.events
        .iter()
        .filter_map(|e| {
            if let StorageEvent::JournalDurable { barrier_id, .. } = e {
                Some(*barrier_id)
            } else {
                None
            }
        })
        .collect()
}

fn failed_of(o: &FlushOutcome) -> Vec<(BarrierId, StorageError)> {
    o.events
        .iter()
        .filter_map(|e| {
            if let StorageEvent::Failed { barrier_id, error } = e {
                Some((*barrier_id, *error))
            } else {
                None
            }
        })
        .collect()
}

fn generic_fixtures<E: LocalEngine>(engine: E) -> StoreWorker<E> {
    let mut worker = StoreWorker::open(engine, BOOT_A, inc(), GroupLimits::default()).unwrap();
    let mut alloc = BarrierAllocator::new(inc(), BOOT_A);
    let reader = worker.reader();

    // Visibility before completion: a submitted batch is invisible until a
    // flush completes; then the view's stamp proves coverage.
    let b1 = alloc.allocate();
    worker
        .submit(app_batch(b1, worker.application_base(), b"k1", b"v1"))
        .unwrap();
    let before = reader.snapshot().unwrap();
    assert_eq!(before.view().get(KV, b"k1").unwrap(), None);
    assert_eq!(before.store_seq().journal_seq().get(), 0);
    let o = worker.flush().unwrap();
    assert_eq!(o.committed, 1);
    assert_eq!(durable_of(&o), vec![b1]);
    assert!(
        o.events.iter().any(
            |e| matches!(e, StorageEvent::Materialized { barrier_id, .. } if *barrier_id == b1)
        )
    );
    let after = reader.snapshot().unwrap();
    assert_eq!(after.view().get(KV, b"k1").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(after.store_seq().journal_seq().get(), 1);
    assert_eq!(worker.application_base().execution_position.get(), 1);

    // Definite guard rejection: a stale base is refused inside the
    // transaction and changes nothing; a correct base in the same group lands.
    let stale = ApplyBase {
        configuration: ConfigurationEpoch::ZERO,
        execution_position: ExecutionPosition::ZERO,
    };
    let b2 = alloc.allocate();
    let b3 = alloc.allocate();
    worker
        .submit(app_batch(b2, stale, b"k2", b"stale"))
        .unwrap();
    worker
        .submit(app_batch(b3, worker.application_base(), b"k3", b"v3"))
        .unwrap();
    let o = worker.flush().unwrap();
    assert_eq!(o.rejected, 1);
    assert_eq!(o.committed, 1);
    assert_eq!(
        failed_of(&o),
        vec![(b2, StorageError::DefinitelyNotCommitted)]
    );
    assert_eq!(durable_of(&o), vec![b3]);
    let v = reader.snapshot().unwrap();
    assert_eq!(v.view().get(KV, b"k2").unwrap(), None);
    assert_eq!(v.view().get(KV, b"k3").unwrap(), Some(b"v3".to_vec()));
    assert_eq!(worker.application_base().execution_position.get(), 2);

    // Unrelated protocol stamps advance the store sequence but not the
    // application frontier, so the predecessor stays valid.
    let base = worker.application_base();
    let p1 = alloc.allocate();
    worker
        .submit(proto_batch(p1, b"epoch0/ballot1/cmd"))
        .unwrap();
    let o = worker.flush().unwrap();
    assert_eq!(durable_of(&o), vec![p1]);
    assert_eq!(
        worker.application_base(),
        base,
        "protocol batch does not move the application frontier"
    );
    assert_eq!(worker.meta().stamp.store_seq().journal_seq().get(), 3);
    let b4 = alloc.allocate();
    worker.submit(app_batch(b4, base, b"k4", b"v4")).unwrap();
    let o = worker.flush().unwrap();
    assert_eq!(
        durable_of(&o),
        vec![b4],
        "application batch still extends its predecessor"
    );

    // Bounded grouping: many small batches land in bounded groups, in order.
    let mut barriers = Vec::new();
    for i in 0..150u32 {
        let b = alloc.allocate();
        barriers.push(b);
        worker
            .submit(app_batch(
                b,
                worker_base_after(&worker, i),
                &i.to_be_bytes(),
                b"x",
            ))
            .unwrap();
    }
    let mut seen = Vec::new();
    let mut groups = 0;
    loop {
        let o = worker.flush().unwrap();
        if o.committed == 0 && o.rejected == 0 {
            break;
        }
        groups += 1;
        assert!(o.committed <= 64, "group bounded by records");
        seen.extend(durable_of(&o));
    }
    assert_eq!(seen, barriers);
    assert!(groups >= 3);

    // Old-boot batches are refused outright.
    let mut old = BarrierAllocator::new(inc(), BOOT_B);
    assert_eq!(
        worker.submit(app_batch(
            old.allocate(),
            worker.application_base(),
            b"z",
            b"z"
        )),
        Err(SubmitError::WrongBoot)
    );
    // Oversized single batch refused.
    let huge = PersistBatch {
        barrier: alloc.allocate(),
        base: None,
        updates: vec![StoreUpdate {
            collection: KV,
            key: b"h".to_vec(),
            value: Some(vec![0; 9 * 1024 * 1024]),
        }],
    };
    assert_eq!(worker.submit(huge), Err(SubmitError::BatchTooLarge));
    worker
}

/// Base for the i-th sequential application batch when they are all queued
/// before flushing: bases chain from the worker's current frontier.
fn worker_base_after<E: LocalEngine>(worker: &StoreWorker<E>, i: u32) -> ApplyBase {
    let base = worker.application_base();
    ApplyBase {
        configuration: base.configuration,
        execution_position: ExecutionPosition::new(base.execution_position.get() + u64::from(i))
            .unwrap(),
    }
}

#[test]
fn model_engine_fixtures() {
    generic_fixtures(ModelEngine::new());
}

#[test]
fn redb_engine_fixtures_and_recovery_after_lost_completion() {
    let dir = tempfile::tempdir().unwrap();
    let identity = StoreIdentity {
        cluster_id: ClusterId([1; 16]),
        domain_id: DomainId([2; 16]),
        replica_id: ReplicaId([3; 16]),
        incarnation: inc(),
    };
    let options = OpenOptions {
        cache_bytes: 8 * 1024 * 1024,
    };
    let generation = Generation::create(dir.path(), identity, options).unwrap();
    // The root lock stays held for as long as the engine is in use.
    let (engine, _lock, _manifest) = generation.into_parts();
    let worker = generic_fixtures(engine);
    let expected_meta = *worker.meta();
    // Lost completion: the process dies after the commit but before the
    // machine consumed the events. A new boot recovers the durable frontier.
    let mut engine = worker.into_engine();
    engine.reopen().unwrap();
    let worker_b = StoreWorker::open(engine, BOOT_B, inc(), GroupLimits::default()).unwrap();
    assert_eq!(
        *worker_b.meta(),
        expected_meta,
        "recovered stamp and frontier match the last durable commit"
    );
    let v = worker_b.reader().snapshot().unwrap();
    assert_eq!(v.view().get(KV, b"k4").unwrap(), Some(b"v4".to_vec()));
}

#[test]
fn indeterminate_commit_blocks_until_reconciled_present_or_absent() {
    let engine = ModelEngine::new();
    let mut worker = StoreWorker::open(engine, BOOT_A, inc(), GroupLimits::default()).unwrap();
    let mut alloc = BarrierAllocator::new(inc(), BOOT_A);
    let reader = worker.reader();

    // Indeterminate but actually applied.
    let b1 = alloc.allocate();
    worker
        .submit(app_batch(b1, worker.application_base(), b"k1", b"v1"))
        .unwrap();
    worker
        .engine_mut()
        .script_commit(CommitScript::Indeterminate { applied: true });
    let o = worker.flush().unwrap();
    assert!(o.indeterminate);
    assert!(
        o.events.is_empty(),
        "no fact is published while the outcome is unknown"
    );
    assert_eq!(*worker.state(), WorkerState::NeedsReconcile);
    assert!(matches!(
        worker.submit(app_batch(
            alloc.allocate(),
            worker.application_base(),
            b"x",
            b"x"
        )),
        Err(SubmitError::NotReady(_))
    ));
    assert!(worker.flush().is_err());
    // The view gate refuses a snapshot that is ahead of the completed frontier.
    assert!(matches!(
        reader.snapshot(),
        Err(ViewError::AheadOfCompletion { .. })
    ));
    let o = worker.reconcile().unwrap();
    assert_eq!(durable_of(&o), vec![b1]);
    assert_eq!(*worker.state(), WorkerState::Ready);
    assert_eq!(
        reader.snapshot().unwrap().view().get(KV, b"k1").unwrap(),
        Some(b"v1".to_vec())
    );

    // Indeterminate and absent: reported as definitely not committed.
    let b2 = alloc.allocate();
    worker
        .submit(app_batch(b2, worker.application_base(), b"k2", b"v2"))
        .unwrap();
    worker
        .engine_mut()
        .script_commit(CommitScript::Indeterminate { applied: false });
    let o = worker.flush().unwrap();
    assert!(o.indeterminate);
    let o = worker.reconcile().unwrap();
    assert_eq!(
        failed_of(&o),
        vec![(b2, StorageError::DefinitelyNotCommitted)]
    );
    assert_eq!(
        reader.snapshot().unwrap().view().get(KV, b"k2").unwrap(),
        None
    );
    assert_eq!(worker.application_base().execution_position.get(), 1);

    // Definite noncommit from the engine: reported without reconciliation.
    let b3 = alloc.allocate();
    worker
        .submit(app_batch(b3, worker.application_base(), b"k3", b"v3"))
        .unwrap();
    worker
        .engine_mut()
        .script_commit(CommitScript::DefinitelyNotCommitted);
    let o = worker.flush().unwrap();
    assert_eq!(
        failed_of(&o),
        vec![(b3, StorageError::DefinitelyNotCommitted)]
    );
    assert_eq!(*worker.state(), WorkerState::Ready);
}

#[test]
fn diverged_durable_metadata_quarantines() {
    // Another writer (or a copied disk) changed the stamp behind the worker.
    let mut engine = ModelEngine::new();
    let boot_worker =
        StoreWorker::open(ModelEngine::new(), BOOT_A, inc(), GroupLimits::default()).unwrap();
    drop(boot_worker);
    let mut worker = StoreWorker::open(
        std::mem::take(&mut engine),
        BOOT_A,
        inc(),
        GroupLimits::default(),
    )
    .unwrap();
    let mut alloc = BarrierAllocator::new(inc(), BOOT_A);
    worker
        .submit(app_batch(
            alloc.allocate(),
            worker.application_base(),
            b"k",
            b"v",
        ))
        .unwrap();
    worker.flush().unwrap();
    // Tamper: overwrite the stamp row out of band.
    {
        use coord_store_api::engine::WriteTxn;
        let mut tx = worker.engine_mut().begin_write().unwrap();
        tx.put(
            Collection::MetaV1.id(),
            coord_store_api::registry::meta_fields::APPLIED_STAMP,
            b"garbage",
        )
        .unwrap();
        tx.commit_durable().unwrap();
    }
    worker
        .submit(app_batch(
            alloc.allocate(),
            worker.application_base(),
            b"k2",
            b"v2",
        ))
        .unwrap();
    assert!(worker.flush().is_err());
    assert_eq!(*worker.state(), WorkerState::Quarantined);
    assert!(matches!(
        worker.reader().snapshot(),
        Err(ViewError::Quarantined)
    ));
}

/// A model engine whose `begin_write` or `snapshot` can be made to fail
/// once, to drive the worker's pre-commit and reconciliation error paths.
struct FlakyEngine {
    inner: ModelEngine,
    fail_begin_write: bool,
    fail_snapshot: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

#[derive(Clone)]
struct FlakyReader {
    inner: <ModelEngine as LocalEngine>::Reader,
    fail_snapshot: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl coord_store_api::engine::SnapshotSource for FlakyReader {
    type View =
        <<ModelEngine as LocalEngine>::Reader as coord_store_api::engine::SnapshotSource>::View;
    fn snapshot(&self) -> Result<Self::View, coord_store_api::engine::EngineError> {
        if self.fail_snapshot.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(coord_store_api::engine::EngineError::new(
                coord_store_api::engine::ErrorClass::Io,
                "injected snapshot failure",
            ));
        }
        self.inner.snapshot()
    }
}

impl LocalEngine for FlakyEngine {
    type Reader = FlakyReader;
    type Write<'a> = <ModelEngine as LocalEngine>::Write<'a>;
    fn reader(&self) -> FlakyReader {
        FlakyReader {
            inner: self.inner.reader(),
            fail_snapshot: self.fail_snapshot.clone(),
        }
    }
    fn begin_write(&mut self) -> Result<Self::Write<'_>, coord_store_api::engine::EngineError> {
        if self.fail_begin_write {
            return Err(coord_store_api::engine::EngineError::new(
                coord_store_api::engine::ErrorClass::Io,
                "injected begin_write failure",
            ));
        }
        self.inner.begin_write()
    }
}

#[test]
fn pre_commit_failure_keeps_the_group_queued_and_retryable() {
    let engine = FlakyEngine {
        inner: ModelEngine::new(),
        fail_begin_write: false,
        fail_snapshot: Default::default(),
    };
    let mut worker = StoreWorker::open(engine, BOOT_A, inc(), GroupLimits::default()).unwrap();
    let mut alloc = BarrierAllocator::new(inc(), BOOT_A);
    let b1 = alloc.allocate();
    let b2 = alloc.allocate();
    worker
        .submit(app_batch(b1, worker.application_base(), b"k1", b"v1"))
        .unwrap();
    worker.submit(proto_batch(b2, b"p")).unwrap();
    assert_eq!(worker.queued(), 2);
    worker.engine_mut().fail_begin_write = true;
    let err = worker.flush().unwrap_err();
    assert!(err.to_string().contains("injected"), "{err}");
    assert_eq!(
        *worker.state(),
        WorkerState::Ready,
        "a definite pre-commit failure is not quarantine"
    );
    assert_eq!(
        worker.queued(),
        2,
        "the dequeued group is back in the queue"
    );
    // Nothing was published for the batches: their barriers stay pending.
    worker.engine_mut().fail_begin_write = false;
    let o = worker.flush().unwrap();
    assert_eq!(
        durable_of(&o),
        vec![b1, b2],
        "retried in the original order"
    );
    assert_eq!(worker.queued(), 0);
}

#[test]
fn reconciliation_read_error_keeps_the_pending_group() {
    let fail_snapshot = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let engine = FlakyEngine {
        inner: ModelEngine::new(),
        fail_begin_write: false,
        fail_snapshot: fail_snapshot.clone(),
    };
    let mut worker = StoreWorker::open(engine, BOOT_A, inc(), GroupLimits::default()).unwrap();
    let mut alloc = BarrierAllocator::new(inc(), BOOT_A);
    let b1 = alloc.allocate();
    worker
        .submit(app_batch(b1, worker.application_base(), b"k1", b"v1"))
        .unwrap();
    worker
        .engine_mut()
        .inner
        .script_commit(CommitScript::Indeterminate { applied: true });
    assert!(worker.flush().unwrap().indeterminate);
    fail_snapshot.store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(worker.reconcile().is_err());
    assert_eq!(
        *worker.state(),
        WorkerState::NeedsReconcile,
        "a transient read error leaves reconciliation retryable"
    );
    fail_snapshot.store(false, std::sync::atomic::Ordering::SeqCst);
    let o = worker.reconcile().unwrap();
    assert_eq!(durable_of(&o), vec![b1]);
    assert_eq!(*worker.state(), WorkerState::Ready);
}

#[test]
fn partially_missing_durable_metadata_is_corruption() {
    use coord_store_api::engine::WriteTxn;
    use coord_store_api::registry::meta_fields;
    let meta = Collection::MetaV1.id();
    // Stamp without frontier.
    let mut engine = ModelEngine::new();
    let worker = StoreWorker::open(engine, BOOT_A, inc(), GroupLimits::default()).unwrap();
    engine = worker.into_engine();
    {
        let mut tx = engine.begin_write().unwrap();
        let stamp = coord_store_api::envelope::AppliedStamp::new(
            coord_store_api::seq::StoreSeq::INITIAL,
            coord_types::identity::Digest32([0; 32]),
        );
        tx.put(
            meta,
            meta_fields::APPLIED_STAMP,
            &stamp.to_envelope().unwrap(),
        )
        .unwrap();
        tx.commit_durable().unwrap();
    }
    let err = match StoreWorker::open(engine, BOOT_B, inc(), GroupLimits::default()) {
        Ok(_) => panic!("stamp without frontier must not open"),
        Err(e) => e,
    };
    assert_eq!(err.class, coord_store_api::engine::ErrorClass::Corrupt);
    // Frontier without stamp.
    let mut engine = ModelEngine::new();
    {
        let mut tx = engine.begin_write().unwrap();
        tx.put(meta, meta_fields::EXECUTION_FRONTIER, b"\x00\x00")
            .unwrap();
        tx.commit_durable().unwrap();
    }
    let err = match StoreWorker::open(engine, BOOT_B, inc(), GroupLimits::default()) {
        Ok(_) => panic!("frontier without stamp must not open"),
        Err(e) => e,
    };
    assert_eq!(err.class, coord_store_api::engine::ErrorClass::Corrupt);
}

#[test]
fn a_rejection_decided_before_a_later_failure_is_not_lost_with_it() {
    // A batch refused for a stale base is finished: it is not requeued,
    // so its event is the only thing that will ever complete its
    // barrier. Returning the error from a later step of the same group
    // discarded that event, and the waiter was left waiting for a batch
    // that had already been refused.
    let engine = ModelEngine::new();
    let mut worker = generic_fixtures(engine);
    let mut alloc = BarrierAllocator::new(inc(), BOOT_A);

    let stale = ApplyBase {
        configuration: ConfigurationEpoch::ZERO,
        execution_position: ExecutionPosition::ZERO,
    };
    let refused = alloc.allocate();
    let doomed = alloc.allocate();
    worker
        .submit(app_batch(refused, stale, b"k-stale", b"x"))
        .unwrap();
    worker
        .submit(app_batch(doomed, worker.application_base(), b"k-ok", b"y"))
        .unwrap();
    // The batch after the rejection fails while lowering.
    worker.engine_mut().inject_write_error_at(1);
    let failed = worker.flush();
    assert!(failed.is_err(), "the group failed: {failed:?}");
    worker.engine_mut().clear_injected_faults();

    // The refusal still has to be reported: the next flush carries it,
    // together with the batch that was requeued and now succeeds.
    let o = worker.flush().unwrap();
    assert!(
        failed_of(&o)
            .iter()
            .any(|(b, e)| *b == refused && *e == StorageError::DefinitelyNotCommitted),
        "the rejection decided before the failure: {:?}",
        failed_of(&o)
    );
    assert!(
        durable_of(&o).contains(&doomed),
        "the requeued batch lands: {:?}",
        durable_of(&o)
    );
}

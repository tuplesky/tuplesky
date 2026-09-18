//! Crash matrix over the real redb engine: crash at every write/sync
//! boundary, sync errors, ENOSPC, durable-prefix corruption, destructor
//! flush prevention, subprocess death and generation-directory faults.

use std::cell::RefCell;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;

use coord_core::effect::{BootId, CollectionId, PersistBatch, StoreUpdate};
use coord_core::outbox::BarrierAllocator;
use coord_redb_faultkit::{FaultBackend, FaultPlan, Op, Shared, Tail};
use coord_storage::{GroupLimits, StoreWorker, WorkerState};
use coord_storage_redb::{Generation, OpenError, OpenOptions, RedbEngine, StoreIdentity};
use coord_store_api::engine::{LocalEngine, OrderedRead, SnapshotSource, WriteTxn};
use coord_store_api::registry::Collection;
use coord_store_testkit::scenario::{StoreScenarioV1, replay};
use coord_types::ids::*;

const KV: CollectionId = Collection::KvCurrentV1.id();
const HIST: CollectionId = Collection::KvHistoryV1.id();
const CACHE: usize = 4 * 1024 * 1024;

fn boot(n: u8) -> BootId {
    BootId([n; 16])
}

fn inc() -> ReplicaIncarnation {
    ReplicaIncarnation::new(1).unwrap()
}

fn fresh(plan: FaultPlan) -> (RedbEngine, Arc<Shared>) {
    let (backend, shared) = FaultBackend::new(Vec::new(), plan);
    (
        RedbEngine::create_on_backend(backend, CACHE).unwrap(),
        shared,
    )
}

fn reopen(image: Vec<u8>, plan: FaultPlan) -> Result<(RedbEngine, Arc<Shared>), String> {
    let (backend, shared) = FaultBackend::new(image, plan);
    RedbEngine::from_backend(backend, CACHE)
        .map(|e| (e, shared))
        .map_err(|e| e.to_string())
}

fn key(i: u64) -> Vec<u8> {
    format!("key-{i:04}").into_bytes()
}

/// Run `count` application transactions, each writing one KV row and one
/// history row. Returns the number of acknowledged (durably committed)
/// transactions; stops at the first failure.
fn workload(worker: &mut StoreWorker<RedbEngine>, count: u64) -> u64 {
    let mut alloc = BarrierAllocator::new(inc(), worker.boot());
    let mut acked = 0;
    for i in 1..=count {
        let batch = PersistBatch {
            barrier: alloc.allocate(),
            base: Some(worker.application_base()),
            updates: vec![
                StoreUpdate {
                    collection: KV,
                    key: key(i),
                    value: Some(vec![i as u8; 300]),
                },
                StoreUpdate {
                    collection: HIST,
                    key: key(i),
                    value: Some(vec![i as u8; 100]),
                },
            ],
        };
        if worker.submit(batch).is_err() {
            break;
        }
        match worker.flush() {
            Ok(o) if o.committed == 1 => acked += 1,
            _ => break,
        }
    }
    acked
}

/// State recovered from an image: the stamp sequence and how many
/// transactions are fully present (both rows) with no partial ones.
fn recovered_state(engine: &RedbEngine, max: u64) -> (u64, u64) {
    let view = engine.reader().snapshot().unwrap();
    let meta = coord_storage::lowering::DurableMeta::read(&view).unwrap();
    let mut present = 0;
    for i in 1..=max {
        let kv = view.get(KV, &key(i)).unwrap().is_some();
        let hist = view.get(HIST, &key(i)).unwrap().is_some();
        assert_eq!(kv, hist, "transaction {i} is torn: kv={kv} hist={hist}");
        if kv {
            assert_eq!(
                present,
                i - 1,
                "transaction {i} present but an earlier one is missing"
            );
            present = i;
        }
    }
    (meta.stamp.store_seq.journal_seq().get(), present)
}

#[test]
fn crash_at_every_write_and_sync_boundary_reopens_only_permitted_states() {
    const TXNS: u64 = 5;
    // Baseline: count the operations of setup and of the whole workload.
    let (engine, shared) = fresh(FaultPlan::default());
    let mut worker = StoreWorker::open(engine, boot(1), inc(), GroupLimits::default()).unwrap();
    let setup_ops = shared.ops();
    assert_eq!(workload(&mut worker, TXNS), TXNS);
    let workload_ops = shared.ops() - setup_ops;
    assert!(
        workload_ops > 20,
        "workload should span many operations: {workload_ops}"
    );
    drop(worker);

    let mut checked = 0;
    for k in 1..=workload_ops {
        for tail in [Tail::None, Tail::All, Tail::Seeded(k)] {
            let (engine, shared) = fresh(FaultPlan::default());
            let mut worker =
                StoreWorker::open(engine, boot(2), inc(), GroupLimits::default()).unwrap();
            assert_eq!(shared.ops(), setup_ops, "setup is deterministic");
            shared.set_plan(FaultPlan {
                crash_after: Some(setup_ops + k),
                tail,
                ..FaultPlan::default()
            });
            let acked = workload(&mut worker, TXNS);
            assert!(shared.is_frozen(), "k={k}: backend must be frozen");
            let image = shared.crash_image(tail);
            drop(worker);
            let (engine, _) = reopen(image, FaultPlan::default())
                .unwrap_or_else(|e| panic!("k={k} tail={tail:?}: reopen failed: {e}"));
            let (seq, present) = recovered_state(&engine, TXNS);
            assert!(
                acked <= seq && seq <= acked + 1,
                "k={k} tail={tail:?}: acked={acked} seq={seq}"
            );
            assert_eq!(seq, present, "k={k} tail={tail:?}: stamp and rows disagree");
            // A new boot continues from the recovered state.
            let mut next =
                StoreWorker::open(engine, boot(3), inc(), GroupLimits::default()).unwrap();
            assert_eq!(next.application_base().execution_position.get(), present);
            assert_eq!(workload(&mut next, 1), 1);
            checked += 1;
        }
    }
    assert_eq!(checked, workload_ops * 3);
}

#[test]
fn sync_error_is_indeterminate_and_the_crash_image_is_all_or_nothing() {
    let (engine, shared) = fresh(FaultPlan::default());
    let mut worker = StoreWorker::open(engine, boot(1), inc(), GroupLimits::default()).unwrap();
    assert_eq!(workload(&mut worker, 2), 2);
    let ops_after_two = shared.ops();
    // The next sync ordinal is the first sync of transaction 3.
    let log_len = shared.log().len();
    drop(worker);
    let _ = log_len;
    for tail in [Tail::None, Tail::All, Tail::Seeded(7)] {
        let (engine, shared) = fresh(FaultPlan::default());
        let mut worker = StoreWorker::open(engine, boot(1), inc(), GroupLimits::default()).unwrap();
        assert_eq!(workload(&mut worker, 2), 2);
        // Find the first sync after the acknowledged prefix by running the
        // baseline's op count: the next sync is the first Sync op beyond it.
        let base = shared.ops();
        assert_eq!(base, ops_after_two);
        shared.set_plan(FaultPlan {
            fail_sync_at: Some(base + first_sync_offset(&shared)),
            tail,
            ..FaultPlan::default()
        });
        let acked_third = workload(&mut worker, 1);
        assert_eq!(acked_third, 0, "a failed sync must not acknowledge");
        assert!(matches!(
            worker.state(),
            WorkerState::NeedsReconcile | WorkerState::Ready | WorkerState::Quarantined
        ));
        // The process cannot trust the engine after an I/O error: crash and reopen.
        shared.crash();
        let image = shared.crash_image(tail);
        drop(worker);
        let (engine, _) = reopen(image, FaultPlan::default()).unwrap();
        let (seq, present) = recovered_state(&engine, 3);
        assert!((2..=3).contains(&seq), "tail={tail:?}: seq={seq}");
        assert_eq!(seq, present);
    }
}

/// Offset from the current op count to the next Sync in a fresh workload
/// transaction (measured on a throwaway run).
fn first_sync_offset(_shared: &Shared) -> u64 {
    let (engine, probe) = fresh(FaultPlan::default());
    let mut worker = StoreWorker::open(engine, boot(9), inc(), GroupLimits::default()).unwrap();
    assert_eq!(workload(&mut worker, 2), 2);
    let before = probe.log().len();
    let ops_before = probe.ops();
    assert_eq!(workload(&mut worker, 1), 1);
    let log = probe.log();
    let mut ordinal = ops_before;
    for op in &log[before..] {
        match op {
            Op::Write { .. } | Op::SetLen { .. } => ordinal += 1,
            Op::Sync => {
                ordinal += 1;
                return ordinal - ops_before;
            }
            _ => {}
        }
    }
    panic!("no sync in a transaction");
}

#[test]
fn enospc_fails_the_commit_without_losing_acknowledged_state() {
    let (engine, shared) = fresh(FaultPlan::default());
    let mut worker = StoreWorker::open(engine, boot(1), inc(), GroupLimits::default()).unwrap();
    assert_eq!(workload(&mut worker, 3), 3);
    let base = shared.ops();
    shared.set_plan(FaultPlan {
        enospc: Some((base + 1, base + 1_000)),
        ..FaultPlan::default()
    });
    assert_eq!(
        workload(&mut worker, 1),
        0,
        "no space: the commit must fail"
    );
    // Space returns; the engine may be poisoned by the earlier I/O error,
    // so the process restarts from the durable image.
    shared.crash();
    let image = shared.crash_image(Tail::All);
    drop(worker);
    let (engine, _) = reopen(image, FaultPlan::default()).unwrap();
    let (seq, present) = recovered_state(&engine, 4);
    assert_eq!(
        (seq, present),
        (3, 3),
        "only the three acknowledged transactions exist"
    );
    let mut worker = StoreWorker::open(engine, boot(2), inc(), GroupLimits::default()).unwrap();
    assert_eq!(workload(&mut worker, 2), 2);
}

#[test]
fn durable_prefix_corruption_is_quarantined_not_repaired() {
    let (engine, shared) = fresh(FaultPlan::default());
    let mut worker = StoreWorker::open(engine, boot(1), inc(), GroupLimits::default()).unwrap();
    assert_eq!(workload(&mut worker, 8), 8);
    drop(worker);
    let clean = shared.durable();
    // A clean image passes integrity.
    let (mut engine, _) = reopen(clean.clone(), FaultPlan::default()).unwrap();
    engine.verify_integrity().unwrap();
    drop(engine);
    // Corrupt many committed regions (not a torn tail: these bytes were
    // synced long ago).
    let mut corrupt = clean.clone();
    let start = corrupt.len() / 4;
    let mut i = start;
    while i < corrupt.len() {
        corrupt[i] ^= 0xa5;
        i += 251;
    }
    let outcome = match reopen(corrupt, FaultPlan::default()) {
        Err(e) => Err(format!("open: {e}")),
        Ok((mut engine, _)) => match engine.verify_integrity() {
            Err(e) => Err(format!("integrity: {e}")),
            Ok(()) => {
                // Even if integrity passed, reading every row must fail or
                // return the exact committed data; anything else is silent
                // repair.
                let view = engine.reader().snapshot();
                match view {
                    Err(e) => Err(format!("snapshot: {e}")),
                    Ok(view) => {
                        let mut intact = true;
                        for i in 1..=8u64 {
                            match view.get(KV, &key(i)) {
                                Ok(Some(v)) if v == vec![i as u8; 300] => {}
                                _ => intact = false,
                            }
                        }
                        if intact {
                            Ok(())
                        } else {
                            Err("rows unreadable".to_owned())
                        }
                    }
                }
            }
        },
    };
    assert!(
        outcome.is_err(),
        "corruption of the durable prefix must be detected, not repaired into a different state"
    );
}

#[test]
fn destructor_cannot_flush_after_a_simulated_crash() {
    let (engine, shared) = fresh(FaultPlan::default());
    let mut worker = StoreWorker::open(engine, boot(1), inc(), GroupLimits::default()).unwrap();
    assert_eq!(workload(&mut worker, 2), 2);
    let base = shared.ops();
    // Crash in the middle of the third transaction's writes.
    shared.set_plan(FaultPlan {
        crash_after: Some(base + 2),
        tail: Tail::None,
        ..FaultPlan::default()
    });
    assert_eq!(workload(&mut worker, 1), 0);
    assert!(shared.is_frozen());
    let durable_at_crash = shared.durable();
    let rejected_before = shared.log().iter().filter(|o| **o == Op::Rejected).count();
    // Dropping the worker drops the Database; any destructor write or sync
    // is rejected and the durable image is untouched.
    drop(worker);
    assert_eq!(
        shared.durable(),
        durable_at_crash,
        "destructor modified the durable image"
    );
    let rejected_after = shared.log().iter().filter(|o| **o == Op::Rejected).count();
    assert!(rejected_after >= rejected_before);
    let (engine, _) = reopen(durable_at_crash, FaultPlan::default()).unwrap();
    assert_eq!(recovered_state(&engine, 3), (2, 2));
}

#[test]
fn scenario_fixture_survives_seeded_tails() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../coord-store-testkit/fixtures/store_scenario_v1.json");
    let scenario: StoreScenarioV1 =
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    for seed in 1..=4u64 {
        let (mut engine, shared) = fresh(FaultPlan::default());
        let shared = Rc::new(RefCell::new(shared));
        let crashes = Rc::new(RefCell::new(0u64));
        let outcome = {
            let shared = shared.clone();
            let crashes = crashes.clone();
            replay(&mut engine, &scenario, |engine| {
                let s = shared.borrow().clone();
                s.crash();
                *crashes.borrow_mut() += 1;
                let image = s.crash_image(Tail::Seeded(seed * 1000 + *crashes.borrow()));
                let (backend, next) = FaultBackend::new(image, FaultPlan::default());
                *engine = RedbEngine::from_backend(backend, CACHE).unwrap();
                *shared.borrow_mut() = next;
            })
            .unwrap()
        };
        assert!(*crashes.borrow() > 0);
        assert!(
            outcome.matches_oracle,
            "seed {seed}: committed state must survive seeded tails"
        );
        assert_eq!(outcome.matches_expected, Some(true));
    }
}

fn identity() -> StoreIdentity {
    StoreIdentity {
        cluster_id: ClusterId([1; 16]),
        domain_id: DomainId([2; 16]),
        replica_id: ReplicaId([3; 16]),
        incarnation: inc(),
    }
}

fn options() -> OpenOptions {
    OpenOptions { cache_bytes: CACHE }
}

/// Child entry point: with `COORD_FAULT_CHILD_ROOT` set, create a
/// generation, commit five transactions and abort without any cleanup.
#[test]
fn child_entry_point() {
    let Ok(root) = std::env::var("COORD_FAULT_CHILD_ROOT") else {
        return;
    };
    let generation = Generation::create(Path::new(&root), identity(), options()).unwrap();
    let (engine, _lock, _manifest) = generation.into_parts();
    let mut worker = StoreWorker::open(engine, boot(1), inc(), GroupLimits::default()).unwrap();
    assert_eq!(workload(&mut worker, 5), 5);
    eprintln!("child: five transactions acknowledged");
    std::process::abort();
}

#[test]
fn subprocess_death_keeps_every_acknowledged_transaction_on_real_files() {
    let dir = tempfile::tempdir().unwrap();
    let exe = std::env::current_exe().unwrap();
    let status = std::process::Command::new(exe)
        .args(["child_entry_point", "--exact", "--nocapture"])
        .env("COORD_FAULT_CHILD_ROOT", dir.path())
        .env_remove("RUST_BACKTRACE")
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&status.stderr);
    assert!(
        stderr.contains("five transactions acknowledged"),
        "child did not reach the abort: {stderr}"
    );
    assert!(!status.status.success(), "child must have aborted");
    // The parent opens through the lifecycle: the lock died with the child.
    let generation = Generation::open_existing(dir.path(), identity(), options()).unwrap();
    let (engine, _lock, _) = generation.into_parts();
    assert_eq!(recovered_state(&engine, 6), (5, 5));
}

#[test]
fn generation_directory_faults_fail_closed() {
    // Death after the database was created but before the manifest.
    let dir = tempfile::tempdir().unwrap();
    let gen_dir = dir.path().join("gen-000001");
    std::fs::create_dir_all(&gen_dir).unwrap();
    std::fs::write(gen_dir.join("domain.redb"), b"partial").unwrap();
    assert!(matches!(
        Generation::open_existing(dir.path(), identity(), options()),
        Err(OpenError::NotInitialized)
    ));
    assert!(
        matches!(
            Generation::create(dir.path(), identity(), options()),
            Err(OpenError::AlreadyInitialized)
        ),
        "a half-created root is quarantined, never reinitialized"
    );

    // Death after manifest and database but before CURRENT.
    let dir = tempfile::tempdir().unwrap();
    {
        let g = Generation::create(dir.path(), identity(), options()).unwrap();
        drop(g);
    }
    std::fs::remove_file(dir.path().join("CURRENT")).unwrap();
    assert!(matches!(
        Generation::open_existing(dir.path(), identity(), options()),
        Err(OpenError::NotInitialized)
    ));
    assert!(matches!(
        Generation::create(dir.path(), identity(), options()),
        Err(OpenError::AlreadyInitialized)
    ));

    // CURRENT written but the generation directory vanished.
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("CURRENT"), b"gen-000001").unwrap();
    assert!(matches!(
        Generation::open_existing(dir.path(), identity(), options()),
        Err(OpenError::MissingGeneration(_))
    ));
    assert!(matches!(
        Generation::create(dir.path(), identity(), options()),
        Err(OpenError::AlreadyInitialized)
    ));

    // A stray temp manifest next to a good one is ignored; a truncated
    // manifest is corruption.
    let dir = tempfile::tempdir().unwrap();
    drop(Generation::create(dir.path(), identity(), options()).unwrap());
    let gen_dir = dir.path().join("gen-000001");
    std::fs::write(gen_dir.join("manifest.tmp"), b"junk").unwrap();
    Generation::open_existing(dir.path(), identity(), options()).unwrap();
    let good = std::fs::read(gen_dir.join("manifest.v1")).unwrap();
    std::fs::write(gen_dir.join("manifest.v1"), &good[..good.len() / 2]).unwrap();
    assert!(matches!(
        Generation::open_existing(dir.path(), identity(), options()),
        Err(OpenError::Manifest(_))
    ));
}

#[test]
fn write_failure_is_reported_and_leaves_state_consistent() {
    let (engine, shared) = fresh(FaultPlan::default());
    let mut worker = StoreWorker::open(engine, boot(1), inc(), GroupLimits::default()).unwrap();
    assert_eq!(workload(&mut worker, 2), 2);
    let base = shared.ops();
    shared.set_plan(FaultPlan {
        fail_write_at: Some(base + 1),
        ..FaultPlan::default()
    });
    assert_eq!(workload(&mut worker, 1), 0);
    shared.crash();
    let image = shared.crash_image(Tail::All);
    drop(worker);
    let (engine, _) = reopen(image, FaultPlan::default()).unwrap();
    assert_eq!(recovered_state(&engine, 3), (2, 2));
    // The unused-writer path: a transaction that only reads never touches the backend.
    let mut engine = engine;
    let tx = engine.begin_write().unwrap();
    let _ = tx.get(KV, &key(1)).unwrap();
    tx.abort().unwrap();
}

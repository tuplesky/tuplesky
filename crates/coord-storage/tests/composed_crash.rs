//! task-j05 step 3: the composed crash matrix.
//!
//! The domains' real `JournaledStore` over the pinned raft-engine journal
//! on the journal fault kit, and a redb projection on the redb fault kit,
//! the two kits on one power line: a crash scheduled at any counted
//! operation of either device halts both at that instant, and the next
//! boot sees each device's durable image plus the same tail policy's
//! survivors of what it had not synced. Under `strict` and under `replay`
//! (task-j06), every such crash must recover:
//!
//! - **the journal's state:** the recovered projection's rows are exactly
//!   those of the same crashed journal replayed into an empty projection;
//! - **every acknowledged step, by an independent check:** a step whose
//!   flush returned before the crash has its protocol row and its
//!   application rows (the key, the event, the executed row) in the
//!   recovered projection, and no step past the last one submitted has
//!   any.
//!
//! The crash points are the workload's, after the journal is created and
//! the domain attached: a crash while the journal is created is step 1's
//! matrix (`coord-journal-faultkit`), and one while the projection is
//! created is the initialization path's.

use std::num::NonZeroU32;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use coord_core::effect::{BootId, PersistBatch, StoreUpdate};
use coord_core::outbox::BarrierAllocator;
use coord_journal_api::stream::ShardId;
use coord_journal_faultkit as jkit;
use coord_journal_raft_engine::{JournalIdentity, JournalOptions, RaftEngineJournal};
use coord_redb_faultkit as rkit;
use coord_storage::journaled::{
    DomainStatus, DurableCadence, JournalLimits, JournaledStore, Submission, TransitionKind,
};
use coord_storage_redb::RedbEngine;
use coord_store_api::engine::{LocalEngine, OrderedRead, ScanRequest};
use coord_store_api::registry::Collection;
use coord_types::identity::Digest32;
use coord_types::ids::*;

const CLUSTER: ClusterId = ClusterId([0x41; 16]);
const REPLICA: ReplicaId = ReplicaId([0x42; 16]);
const A: DomainId = DomainId([0xa1; 16]);
const FIRST_BOOT: BootId = BootId([0x01; 16]);
const NEXT_BOOT: BootId = BootId([0x02; 16]);
const CACHE: usize = 4 << 20;
/// Steps the workload journals and materializes, one flush each.
const STEPS: u64 = 4;

fn inc() -> ReplicaIncarnation {
    ReplicaIncarnation::new(1).unwrap()
}

fn identity() -> JournalIdentity {
    JournalIdentity {
        cluster: CLUSTER,
        replica: REPLICA,
    }
}

fn shard() -> ShardId {
    ShardId::new(0).unwrap()
}

type Rows = Vec<(u16, Vec<u8>, Vec<u8>)>;

fn protocol_key(step: u64) -> Vec<u8> {
    step.to_be_bytes().to_vec()
}

fn kv_key(step: u64) -> Vec<u8> {
    format!("key-{step}").into_bytes()
}

fn event_key(step: u64) -> Vec<u8> {
    step.to_be_bytes().to_vec()
}

fn executed_key(step: u64) -> Vec<u8> {
    vec![step as u8; 32]
}

fn update(collection: Collection, key: Vec<u8>, value: Vec<u8>) -> StoreUpdate {
    StoreUpdate {
        collection: collection.id(),
        key,
        value: Some(value),
    }
}

/// Every row of every registered collection, in collection and key order.
fn rows<J: coord_journal_api::engine::JournalEngine, E: LocalEngine>(
    store: &JournaledStore<J, E>,
    domain: DomainId,
) -> Rows {
    let gated = store.reader(domain).unwrap().snapshot().unwrap();
    let mut out = Vec::new();
    for collection in Collection::ALL {
        let mut resume = None;
        loop {
            let page = gated
                .view()
                .scan_page(
                    collection.id(),
                    &ScanRequest {
                        resume_after: resume.clone(),
                        ..ScanRequest::all(256, 1 << 20)
                    },
                )
                .unwrap();
            for row in &page.rows {
                out.push((collection.id().0, row.key.clone(), row.value.clone()));
            }
            match page.rows.last() {
                Some(last) if !page.exhausted => resume = Some(last.key.clone()),
                _ => break,
            }
        }
    }
    out
}

fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

/// Which device a crash is scheduled on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Device {
    Journal,
    Projection,
}

/// What one boot of the workload saw.
#[derive(Debug, Default)]
struct Boot {
    /// The journal's and the projection's counted operations once the
    /// domain was attached.
    setup: (u64, u64),
    /// Steps whose flush returned `Ok` before the crash.
    acked: u64,
    /// Steps submitted, acknowledged or not.
    attempted: u64,
}

/// Journal and materialize [`STEPS`] steps, one flush each, until the
/// first failure or the crash. `crash` schedules one on a device, counted
/// from the end of the setup. `believe_after_crash` is the negative
/// control's: it counts a flush that returned after the crash as an
/// acknowledgement, which a halted journal's syncs make look successful.
fn workload(
    journal_dir: &Path,
    replay: Option<DurableCadence>,
    crash: Option<(Device, u64)>,
    believe_after_crash: bool,
) -> (Boot, Arc<jkit::Shared>, Arc<rkit::Shared>) {
    let power = Arc::new(AtomicBool::new(false));
    let (fs, journal_kit) = jkit::FaultFs::with_power(jkit::FaultPlan::default(), power.clone());
    let (backend, projection_kit) =
        rkit::FaultBackend::with_power(Vec::new(), rkit::FaultPlan::default(), power.clone());
    let mut boot = Boot::default();
    let ran = catch_unwind(AssertUnwindSafe(|| {
        let journal = RaftEngineJournal::create_with_file_system(
            journal_dir,
            identity(),
            &JournalOptions::default(),
            fs,
        )
        .unwrap();
        let engine = RedbEngine::create_on_backend(backend, CACHE).unwrap();
        let mut store = JournaledStore::open(
            journal,
            CLUSTER,
            REPLICA,
            inc(),
            FIRST_BOOT,
            JournalLimits::default(),
        )
        .unwrap();
        if let Some(cadence) = replay {
            store.replay_projection(cadence).unwrap();
        }
        store.attach(A, shard(), engine).unwrap();
        boot.setup = (journal_kit.ops(), projection_kit.ops());
        match crash {
            Some((Device::Journal, k)) => journal_kit.set_plan(jkit::FaultPlan {
                crash_after: Some(boot.setup.0 + k),
                ..jkit::FaultPlan::default()
            }),
            Some((Device::Projection, k)) => projection_kit.set_plan(rkit::FaultPlan {
                crash_after: Some(boot.setup.1 + k),
                ..rkit::FaultPlan::default()
            }),
            None => {}
        }
        let mut barriers = BarrierAllocator::new(inc(), FIRST_BOOT);
        for step in 1..=STEPS {
            boot.attempted = step;
            if !submit_step(&mut store, &mut barriers, step) {
                break;
            }
            let flushed = store.flush();
            // An acknowledgement observed after the crash was never given:
            // the process died before it.
            if flushed.is_err() || (power.load(Ordering::SeqCst) && !believe_after_crash) {
                break;
            }
            boot.acked = step;
        }
        let (journal, engines) = store.into_parts();
        drop(journal);
        drop(engines);
    }));
    // The setup itself never faults; a panic past it is the store's
    // reaction to a device that failed under it, and ends the boot.
    if ran.is_err() {
        assert!(
            power.load(Ordering::SeqCst),
            "the boot panicked without a crash"
        );
    }
    (boot, journal_kit, projection_kit)
}

/// Submit one step's protocol transition and application outcome;
/// whether both were taken.
fn submit_step(
    store: &mut JournaledStore<RaftEngineJournal<jkit::FaultFs>, RedbEngine>,
    barriers: &mut BarrierAllocator,
    step: u64,
) -> bool {
    let protocol = store.submit(Submission {
        domain: A,
        ballot: Ballot {
            epoch: ConfigurationEpoch::ZERO,
            number: step,
            leader: REPLICA,
        },
        kind: TransitionKind::Protocol,
        batch: PersistBatch {
            barrier: barriers.allocate(),
            base: None,
            updates: vec![update(
                Collection::ProtocolV1,
                protocol_key(step),
                vec![step as u8; 24],
            )],
        },
    });
    let Some(base) = store.application_base(A) else {
        return false;
    };
    let application = store.submit(Submission {
        domain: A,
        ballot: Ballot {
            epoch: base.configuration,
            number: step,
            leader: REPLICA,
        },
        kind: TransitionKind::Application {
            position: ExecutionPosition::new(step).unwrap(),
            revision: Some(KvRevision::new(step).unwrap()),
            result_digest: Digest32([step as u8; 32]),
        },
        batch: PersistBatch {
            barrier: barriers.allocate(),
            base: Some(base),
            updates: vec![
                update(Collection::KvCurrentV1, kv_key(step), b"value".to_vec()),
                update(Collection::EventsV1, event_key(step), vec![step as u8; 12]),
                update(
                    Collection::ExecutedV1,
                    executed_key(step),
                    step.to_be_bytes().to_vec(),
                ),
            ],
        },
    });
    protocol.is_ok() && application.is_ok()
}

/// The next boot over a crashed journal directory and projection image
/// (empty: a fresh projection), with every record the journal holds
/// replayed into the projection before it serves.
fn recover(journal_dir: &Path, image: Vec<u8>, replay: Option<DurableCadence>) -> Rows {
    let journal =
        RaftEngineJournal::open_existing(journal_dir, identity(), &JournalOptions::default())
            .expect("the journal recovers its actual valid records");
    let engine = if image.is_empty() {
        let (backend, _) = rkit::FaultBackend::new(Vec::new(), rkit::FaultPlan::default());
        RedbEngine::create_on_backend(backend, CACHE).unwrap()
    } else {
        let (backend, _) = rkit::FaultBackend::new(image, rkit::FaultPlan::default());
        RedbEngine::from_backend(backend, CACHE).expect("the projection reopens")
    };
    let mut next = JournaledStore::open(
        journal,
        CLUSTER,
        REPLICA,
        inc(),
        NEXT_BOOT,
        JournalLimits::default(),
    )
    .unwrap();
    if let Some(cadence) = replay {
        next.replay_projection(cadence).unwrap();
    }
    next.attach(A, shard(), engine)
        .expect("the projection attaches");
    let recovered = next.frontiers(A).unwrap();
    assert_eq!(recovered.materialized(), recovered.durable());
    assert_eq!(next.status(A), Some(DomainStatus::Ready));
    rows(&next, A)
}

/// The independent check: every acknowledged step's rows are there, and
/// no step past the last one submitted left any.
fn check_steps(rows: &Rows, boot: &Boot, case: &str) {
    let has = |collection: Collection, key: &[u8]| {
        rows.iter()
            .any(|(c, k, _)| *c == collection.id().0 && k == key)
    };
    for step in 1..=boot.acked {
        for (collection, key) in [
            (Collection::ProtocolV1, protocol_key(step)),
            (Collection::KvCurrentV1, kv_key(step)),
            (Collection::EventsV1, event_key(step)),
            (Collection::ExecutedV1, executed_key(step)),
        ] {
            assert!(
                has(collection, &key),
                "{case}: acknowledged step {step} lost its {} row",
                collection.name()
            );
        }
    }
    for step in boot.attempted + 1..=STEPS {
        assert!(
            !has(Collection::KvCurrentV1, &kv_key(step)),
            "{case}: step {step} was never submitted and has a row"
        );
    }
}

fn composed_matrix(replay: Option<DurableCadence>) {
    let probe = tempfile::tempdir().unwrap();
    let (boot, journal_kit, projection_kit) =
        workload(&probe.path().join("journal"), replay, None, false);
    assert_eq!(boot.acked, STEPS, "the workload runs to the end unfaulted");
    let ops = [
        (Device::Journal, journal_kit.ops() - boot.setup.0),
        (Device::Projection, projection_kit.ops() - boot.setup.1),
    ];
    for (device, n) in ops {
        assert!(n >= STEPS, "{device:?} spans {n} operations");
    }

    let (mut cases, mut lost_unacked, mut with_executed) = (0, 0, 0);
    for (device, n) in ops {
        for k in 1..=n {
            for tail in [
                (jkit::Tail::None, rkit::Tail::None),
                (jkit::Tail::All, rkit::Tail::All),
                (jkit::Tail::Seeded(k), rkit::Tail::Seeded(k)),
            ] {
                let case = format!("{device:?} crash at {k} of {n}, {:?}", tail.0);
                let dir = tempfile::tempdir().unwrap();
                let journal_dir = dir.path().join("journal");
                let (boot, journal_kit, projection_kit) =
                    workload(&journal_dir, replay, Some((device, k)), false);
                assert!(
                    journal_kit.is_frozen() && projection_kit.is_frozen(),
                    "{case}: one crash halts both devices"
                );
                journal_kit.materialize(tail.0).unwrap();
                let image = projection_kit.crash_image(tail.1);
                let reference_dir = dir.path().join("reference");
                copy_tree(&journal_dir, &reference_dir);

                let recovered = recover(&journal_dir, image, replay);
                let replayed = recover(&reference_dir, Vec::new(), replay);
                assert_eq!(
                    recovered, replayed,
                    "{case}: the recovered projection is the journal's state"
                );
                check_steps(&recovered, &boot, &case);

                cases += 1;
                let kept = recovered
                    .iter()
                    .filter(|(c, ..)| *c == Collection::KvCurrentV1.id().0)
                    .count() as u64;
                if kept < boot.attempted {
                    lost_unacked += 1;
                }
                if recovered
                    .iter()
                    .any(|(c, ..)| *c == Collection::ExecutedV1.id().0)
                {
                    with_executed += 1;
                }
            }
        }
    }
    eprintln!(
        "composed matrix ({}): {cases} crashes over {ops:?}; {lost_unacked} lost a step \
         submitted and not acknowledged, {with_executed} recovered executed rows",
        if replay.is_some() { "replay" } else { "strict" },
    );
    // The matrix reaches both outcomes: crashes that lose a submitted step
    // the journal never made durable, and crashes that keep executed rows.
    assert!(lost_unacked > 0, "no crash lost an unacknowledged step");
    assert!(with_executed > 0, "no crash left executed rows to recover");
}

/// The strict profile: every projection commit is durable.
#[test]
fn a_crash_of_journal_and_projection_together_recovers_every_acknowledged_step() {
    composed_matrix(None);
}

/// The replay-backed profile: the projection's commits are working
/// commits, durable every third, so a crash also loses commits redb
/// reported done, and the next boot replays them from the journal.
#[test]
fn a_crash_of_journal_and_projection_together_under_replay_recovers_every_acknowledged_step() {
    composed_matrix(Some(DurableCadence {
        commits: NonZeroU32::new(3).unwrap(),
        records: NonZeroU32::new(u32::MAX).unwrap(),
    }));
}

/// The independent check has teeth: a harness that believed the flushes
/// returning after a journal crash -- the halted device reports their
/// syncs done, and none of it reached the disk -- claims steps the
/// recovered projection does not hold, and the check says so.
#[test]
fn an_acknowledgement_believed_after_the_crash_is_caught() {
    let probe = tempfile::tempdir().unwrap();
    let (boot, journal_kit, _) = workload(&probe.path().join("journal"), None, None, false);
    let n = journal_kit.ops() - boot.setup.0;
    let mut caught = 0;
    for k in 1..=n {
        let dir = tempfile::tempdir().unwrap();
        let journal_dir = dir.path().join("journal");
        let (boot, journal_kit, projection_kit) =
            workload(&journal_dir, None, Some((Device::Journal, k)), true);
        journal_kit.materialize(jkit::Tail::None).unwrap();
        let recovered = recover(
            &journal_dir,
            projection_kit.crash_image(rkit::Tail::None),
            None,
        );
        if catch_unwind(AssertUnwindSafe(|| {
            check_steps(&recovered, &boot, "control")
        }))
        .is_err()
        {
            caught += 1;
        }
    }
    eprintln!("negative control: caught {caught} of {n} journal crashes");
    assert!(caught > 0, "no believed acknowledgement was caught");
}

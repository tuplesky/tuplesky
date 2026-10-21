//! task-j03 acceptance on the real engines: the journal-first pipeline over
//! the pinned raft-engine journal and a durable redb projection.
//!
//! These cases prove the composition itself - one synced grouped journal
//! write serving several domains, then one atomic durable projection commit
//! per domain, and replay of `(M, J]` from the journal's actual valid
//! records after the projection loses everything unsynced. Injected
//! journal, filesystem and process faults are task-j05; nothing here claims
//! power-loss qualification.

use std::num::NonZeroU32;
use std::path::Path;

use coord_core::effect::{BootId, PersistBatch, StoreUpdate};
use coord_core::outbox::BarrierAllocator;
use coord_journal_api::stream::ShardId;
use coord_journal_raft_engine::{JournalIdentity, JournalOptions, RaftEngineJournal};
use coord_redb_faultkit::{FaultBackend, FaultPlan, Tail};
use coord_storage::journaled::{
    DomainStatus, DurableCadence, JournalLimits, JournaledStore, Submission, TransitionKind,
};
use coord_storage_redb::RedbEngine;
use coord_store_api::engine::{OrderedRead, ScanRequest};
use coord_store_api::registry::Collection;
use coord_types::identity::Digest32;
use coord_types::ids::*;

const CLUSTER: ClusterId = ClusterId([0x31; 16]);
const REPLICA: ReplicaId = ReplicaId([0x32; 16]);
const A: DomainId = DomainId([0xa1; 16]);
const B: DomainId = DomainId([0xb2; 16]);
const FIRST_BOOT: BootId = BootId([0x01; 16]);
const NEXT_BOOT: BootId = BootId([0x02; 16]);
const CACHE: usize = 4 << 20;

fn inc() -> ReplicaIncarnation {
    ReplicaIncarnation::new(1).unwrap()
}

fn ballot(number: u64) -> Ballot {
    Ballot {
        epoch: ConfigurationEpoch::ZERO,
        number,
        leader: REPLICA,
    }
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

fn kv(key: &[u8], value: &[u8]) -> StoreUpdate {
    StoreUpdate {
        collection: Collection::KvCurrentV1.id(),
        key: key.to_vec(),
        value: Some(value.to_vec()),
    }
}

fn event(revision: u64) -> StoreUpdate {
    StoreUpdate {
        collection: Collection::EventsV1.id(),
        key: revision.to_be_bytes().to_vec(),
        value: Some(vec![revision as u8; 12]),
    }
}

fn executed(tag: u8, position: u64) -> StoreUpdate {
    StoreUpdate {
        collection: Collection::ExecutedV1.id(),
        key: vec![tag; 32],
        value: Some(position.to_be_bytes().to_vec()),
    }
}

/// Every row of a projection, as (collection, key, value).
type Rows = Vec<(u16, Vec<u8>, Vec<u8>)>;

/// Every row of every registered collection, in collection and key order.
fn rows<E: coord_store_api::engine::LocalEngine>(
    store: &JournaledStore<RaftEngineJournal, E>,
    domain: DomainId,
) -> Vec<(u16, Vec<u8>, Vec<u8>)> {
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

/// Journal a fixed workload of protocol transitions and application
/// outcomes into a fresh journal directory, then end the boot. When
/// `materialize_inline` is false the projection is deliberately left behind
/// and everything it never synced is lost, so the next boot has to replay.
fn run(dir: &Path, materialize_inline: bool) -> Vec<(u16, Vec<u8>, Vec<u8>)> {
    let journal = RaftEngineJournal::create(dir, identity(), &JournalOptions::default()).unwrap();
    let (backend, shared) = FaultBackend::new(Vec::new(), FaultPlan::default());
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
    store.attach(A, shard(), engine).unwrap();
    let mut barriers = BarrierAllocator::new(inc(), FIRST_BOOT);
    for step in 1..=5u64 {
        store
            .submit(Submission {
                domain: A,
                ballot: ballot(step),
                kind: TransitionKind::Protocol,
                batch: PersistBatch {
                    barrier: barriers.allocate(),
                    base: None,
                    updates: vec![StoreUpdate {
                        collection: Collection::ProtocolV1.id(),
                        key: step.to_be_bytes().to_vec(),
                        value: Some(vec![step as u8; 24]),
                    }],
                },
            })
            .unwrap();
        let base = store.application_base(A).unwrap();
        store
            .submit(Submission {
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
                        kv(format!("key-{step}").as_bytes(), b"value"),
                        event(step),
                        executed(step as u8, step),
                    ],
                },
            })
            .unwrap();
        for _ in 0..2 {
            if materialize_inline {
                store.flush().unwrap();
            } else {
                store.append_pending().unwrap();
            }
        }
    }
    let frontiers = store.frontiers(A).unwrap();
    if materialize_inline {
        assert_eq!(frontiers.materialized(), frontiers.durable());
    } else {
        assert!(frontiers.materialized() < frontiers.durable());
    }

    // The boot ends: the journal directory is released and the projection
    // keeps only what it actually synced.
    let (journal, engines) = store.into_parts();
    drop(journal);
    drop(engines);
    let image = shared.crash_image(Tail::None);

    let journal = RaftEngineJournal::open_existing(dir, identity(), &JournalOptions::default())
        .expect("the journal recovers its actual valid records");
    let (backend, _) = FaultBackend::new(image, FaultPlan::default());
    let engine = RedbEngine::from_backend(backend, CACHE).unwrap();
    let mut next = JournaledStore::open(
        journal,
        CLUSTER,
        REPLICA,
        inc(),
        NEXT_BOOT,
        JournalLimits::default(),
    )
    .unwrap();
    next.attach(A, shard(), engine).unwrap();
    let recovered = next.frontiers(A).unwrap();
    assert_eq!(recovered.materialized(), recovered.durable());
    assert_eq!(next.status(A), Some(DomainStatus::Ready));
    rows(&next, A)
}

#[test]
fn replay_over_the_real_journal_and_projection_restores_the_same_rows_as_inline_materialization() {
    let inline = tempfile::tempdir().unwrap();
    let replayed = tempfile::tempdir().unwrap();
    let reference = run(&inline.path().join("journal"), true);
    let recovered = run(&replayed.path().join("journal"), false);
    assert_eq!(reference, recovered);
    assert!(
        reference
            .iter()
            .any(|(collection, ..)| *collection == Collection::EventsV1.id().0),
        "the workload writes revision events"
    );
    assert!(
        reference
            .iter()
            .any(|(collection, ..)| *collection == Collection::ExecutedV1.id().0),
        "the workload records executed identities"
    );
}

#[test]
fn two_domains_share_one_synced_journal_write_and_keep_separate_durable_projections() {
    let dir = tempfile::tempdir().unwrap();
    let journal = RaftEngineJournal::create(
        &dir.path().join("journal"),
        identity(),
        &JournalOptions::default(),
    )
    .unwrap();
    let mut store = JournaledStore::open(
        journal,
        CLUSTER,
        REPLICA,
        inc(),
        FIRST_BOOT,
        JournalLimits::default(),
    )
    .unwrap();
    for domain in [A, B] {
        let (backend, _) = FaultBackend::new(Vec::new(), FaultPlan::default());
        store
            .attach(
                domain,
                shard(),
                RedbEngine::create_on_backend(backend, CACHE).unwrap(),
            )
            .unwrap();
    }
    let before = store.journal().stats().unwrap();
    let mut barriers = BarrierAllocator::new(inc(), FIRST_BOOT);
    for domain in [A, B] {
        store
            .submit(Submission {
                domain,
                ballot: ballot(1),
                kind: TransitionKind::Protocol,
                batch: PersistBatch {
                    barrier: barriers.allocate(),
                    base: None,
                    updates: vec![kv(b"shared", b"row")],
                },
            })
            .unwrap();
    }
    let report = store.flush().unwrap();
    let after = store.journal().stats().unwrap();
    assert_eq!(report.appends, 1);
    assert_eq!(report.journaled, 2);
    assert_eq!(report.materialized, 2);
    assert_eq!(
        after.groups - before.groups,
        1,
        "both domains ride one grouped engine write"
    );
    assert_eq!(after.records - before.records, 2);
    assert_eq!(
        after.syncs - before.syncs,
        1,
        "one journal sync for the group"
    );
    assert_eq!(
        report.commits, 2,
        "the strict profile still commits each projection durably: this is not one fsync overall"
    );
    // The two streams are distinct and their projections independent.
    let attached = store.attached();
    assert_eq!(attached.len(), 2);
    assert_ne!(attached[0].1, attached[1].1);
    for domain in [A, B] {
        assert_eq!(
            rows(&store, domain)
                .iter()
                .filter(|(collection, ..)| *collection == Collection::KvCurrentV1.id().0)
                .count(),
            1
        );
    }
}

/// How many `KvCurrentV1` rows a projection holds, read from the engine
/// itself.
fn kv_rows(engine: &RedbEngine) -> usize {
    use coord_store_api::engine::{LocalEngine, SnapshotSource};
    let snapshot = engine.reader().snapshot().unwrap();
    snapshot
        .scan_page(
            Collection::KvCurrentV1.id(),
            &ScanRequest::all(1024, 1 << 20),
        )
        .unwrap()
        .rows
        .len()
}

/// Copy a directory tree: the journal as the crash left it, for a second
/// replay.
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

/// Journal and materialize `steps` protocol transitions and application
/// outcomes, one flush each, until the first failure.
fn materialize_steps(store: &mut JournaledStore<RaftEngineJournal, RedbEngine>, steps: u64) {
    let mut barriers = BarrierAllocator::new(inc(), FIRST_BOOT);
    materialize_range(store, &mut barriers, 1..=steps);
}

/// [`materialize_steps`] for the steps in `range`, with `barriers`;
/// whether every one was materialized.
fn materialize_range(
    store: &mut JournaledStore<RaftEngineJournal, RedbEngine>,
    barriers: &mut BarrierAllocator,
    range: std::ops::RangeInclusive<u64>,
) -> bool {
    for step in range {
        let protocol = store.submit(Submission {
            domain: A,
            ballot: ballot(step),
            kind: TransitionKind::Protocol,
            batch: PersistBatch {
                barrier: barriers.allocate(),
                base: None,
                updates: vec![StoreUpdate {
                    collection: Collection::ProtocolV1.id(),
                    key: step.to_be_bytes().to_vec(),
                    value: Some(vec![step as u8; 24]),
                }],
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
                    kv(format!("key-{step}").as_bytes(), b"value"),
                    event(step),
                    executed(step as u8, step),
                ],
            },
        });
        if protocol.is_err() || application.is_err() || store.flush().is_err() {
            return false;
        }
    }
    true
}

/// task-d48: the projection commits in one phase under the journal, and a
/// crash at any of its writes or syncs -- with nothing, everything, or a
/// seeded torn and reordered subset of what it had not synced surviving
/// -- recovers to the journal's state. The next boot's rows are exactly
/// those of the same journal replayed into an empty projection, so a
/// commit the crash tore is rolled back and carried forward again from
/// the journal, and nothing the journal holds is missing (the executed
/// rows a resolve is answered from among them).
#[test]
fn a_crash_anywhere_in_a_one_phase_projection_commit_recovers_the_journals_state() {
    crash_matrix(None);
}

/// task-j06: the same matrix under the replay-backed profile. The
/// projection's commits are working commits, durable every third one, so
/// a crash also loses whole commits redb reported done; the next boot
/// replays them from the journal before the domain serves, and its rows
/// are still exactly the journal's.
#[test]
fn a_crash_anywhere_under_the_replay_profile_recovers_the_journals_state() {
    crash_matrix(Some(DurableCadence {
        commits: NonZeroU32::new(3).unwrap(),
        records: NonZeroU32::new(u32::MAX).unwrap(),
    }));
}

fn crash_matrix(replay: Option<DurableCadence>) {
    const STEPS: u64 = 4;
    let probe_dir = tempfile::tempdir().unwrap();
    let measure = |dir: &Path, plan: Option<(u64, Tail)>| {
        let journal =
            RaftEngineJournal::create(dir, identity(), &JournalOptions::default()).unwrap();
        let (backend, shared) = FaultBackend::new(Vec::new(), FaultPlan::default());
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
        let setup = shared.ops();
        if let Some((k, tail)) = plan {
            shared.set_plan(FaultPlan {
                crash_after: Some(setup + k),
                tail,
                ..FaultPlan::default()
            });
        }
        materialize_steps(&mut store, STEPS);
        let (journal, engines) = store.into_parts();
        assert!(
            engines.iter().all(|(_, e)| e.commits_in_one_phase()),
            "attached under the journal, the projection commits in one phase"
        );
        // What a crash after the last commit would leave, taken before the
        // engines close: a clean close is itself a durable commit.
        let image = shared.crash_image(Tail::All);
        drop(journal);
        drop(engines);
        (setup, shared, image)
    };
    let (setup, probe, image) = measure(&probe_dir.path().join("journal"), None);
    let workload_ops = probe.ops() - setup;
    assert!(
        workload_ops > 10,
        "the workload spans {workload_ops} operations"
    );

    let boot = |journal_dir: &Path, image: Vec<u8>| {
        let journal =
            RaftEngineJournal::open_existing(journal_dir, identity(), &JournalOptions::default())
                .expect("the journal recovers its actual valid records");
        let engine = if image.is_empty() {
            let (backend, _) = FaultBackend::new(Vec::new(), FaultPlan::default());
            RedbEngine::create_on_backend(backend, CACHE).unwrap()
        } else {
            let (backend, _) = FaultBackend::new(image, FaultPlan::default());
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
    };

    // Under the replay profile the workload ends on working commits that
    // redb reported done and a crash loses, every write the process issued
    // surviving or not: redb holds a `Durability::None` commit's pages
    // until the next durable one. Under the strict profile it loses none.
    let (backend, _) = FaultBackend::new(image, FaultPlan::default());
    let crashed = RedbEngine::from_backend(backend, CACHE).unwrap();
    let kept = kv_rows(&crashed);
    if replay.is_some() {
        assert!(kept < STEPS as usize, "{kept} of {STEPS} kv rows survived");
    } else {
        assert_eq!(kept, STEPS as usize);
    }

    let mut with_executed = 0;
    for k in 1..=workload_ops {
        for tail in [Tail::None, Tail::All, Tail::Seeded(k)] {
            let dir = tempfile::tempdir().unwrap();
            let journal_dir = dir.path().join("journal");
            let (_, shared, _) = measure(&journal_dir, Some((k, tail)));
            assert!(shared.is_frozen(), "k={k}: the crash happened");
            let image = shared.crash_image(tail);
            let reference_dir = dir.path().join("reference");
            copy_tree(&journal_dir, &reference_dir);
            let recovered = boot(&journal_dir, image);
            let replayed = boot(&reference_dir, Vec::new());
            assert_eq!(
                recovered, replayed,
                "k={k} tail={tail:?}: the recovered projection is the journal's state"
            );
            if recovered
                .iter()
                .filter(|(c, ..)| *c == Collection::ExecutedV1.id().0)
                .count()
                > 0
            {
                with_executed += 1;
            }
        }
    }
    assert!(
        with_executed > 0,
        "some crashes left executed rows to recover"
    );
}

/// task-j06 over redb: `sync_projections` makes every working commit
/// durable, so a crash that keeps nothing unsynced keeps all of them, and
/// the next boot replays nothing.
#[test]
fn sync_projections_makes_redbs_working_commits_durable() {
    redbs_working_commits_made_durable_by(|store| {
        store.sync_projections().unwrap();
    });
}

/// task-j06 review: so does the idle sync, once a request for a durable
/// commit has met no commit -- the time bound on a domain that went idle.
#[test]
fn an_idle_sync_makes_redbs_working_commits_durable() {
    redbs_working_commits_made_durable_by(|store| {
        store.request_durable_projection();
        store.sync_idle_projections().unwrap();
    });
}

/// Materialize working commits on redb, show a crash would lose some of
/// them, `sync`, and show a crash then loses none.
fn redbs_working_commits_made_durable_by(
    sync: impl FnOnce(&mut JournaledStore<RaftEngineJournal, RedbEngine>),
) {
    const STEPS: u64 = 3;
    let dir = tempfile::tempdir().unwrap();
    let journal = RaftEngineJournal::create(
        &dir.path().join("journal"),
        identity(),
        &JournalOptions::default(),
    )
    .unwrap();
    let (backend, shared) = FaultBackend::new(Vec::new(), FaultPlan::default());
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
    store
        .replay_projection(DurableCadence {
            commits: NonZeroU32::new(u32::MAX).unwrap(),
            records: NonZeroU32::new(u32::MAX).unwrap(),
        })
        .unwrap();
    store.attach(A, shard(), engine).unwrap();
    materialize_steps(&mut store, STEPS);
    let frontiers = store.frontiers(A).unwrap();
    assert!(store.projection_durable(A).unwrap() < frontiers.materialized());
    let (backend, _) = FaultBackend::new(shared.crash_image(Tail::None), FaultPlan::default());
    assert!(kv_rows(&RedbEngine::from_backend(backend, CACHE).unwrap()) < STEPS as usize);

    sync(&mut store);
    assert_eq!(
        store.projection_durable(A).unwrap(),
        frontiers.materialized()
    );
    let (backend, _) = FaultBackend::new(shared.crash_image(Tail::None), FaultPlan::default());
    assert_eq!(
        kv_rows(&RedbEngine::from_backend(backend, CACHE).unwrap()),
        STEPS as usize
    );
}

/// task-j06 over redb, the publication's own matrix: under the
/// replay-backed profile with no cadence at all, so the only durable
/// projection commits are the ones a checkpoint publication forces, a
/// crash at every write and sync of the projection -- before, inside and
/// after the publication, with nothing, everything or a seeded subset of
/// the unsynced writes surviving -- leaves a projection that continues
/// the journal from the baseline the journal selects (`C <= M_durable`),
/// and the next boot recovers exactly the rows a run without a crash had
/// at the same journal head. Without the forced sync, every crash after
/// the pointer is durable leaves the projection below the retired prefix.
#[test]
fn a_crash_anywhere_around_a_publication_under_the_replay_profile_continues_the_journal() {
    use coord_journal_api::CheckpointPointerV1;
    use std::collections::BTreeMap;
    let never = DurableCadence {
        commits: NonZeroU32::new(u32::MAX).unwrap(),
        records: NonZeroU32::new(u32::MAX).unwrap(),
    };
    // The rows to compare, less the stamp: its digest is of a record of
    // this journal's own stream, which another journal does not share,
    // and `validate_projection` has already checked it against the
    // journal it belongs to.
    let unstamped = |mut rows: Rows| {
        rows.retain(|(c, key, _)| {
            !(*c == Collection::MetaV1.id().0
                && key.as_slice() == coord_store_api::registry::meta_fields::APPLIED_STAMP)
        });
        rows
    };
    // The workload, and what the projection held at each journal head it
    // passed through.
    let workload = |store: &mut JournaledStore<RaftEngineJournal, RedbEngine>,
                    seen: &mut BTreeMap<u64, Rows>| {
        let mut barriers = BarrierAllocator::new(inc(), FIRST_BOOT);
        let mut note = |store: &JournaledStore<RaftEngineJournal, RedbEngine>| {
            let materialized = store.frontiers(A).unwrap().materialized().get();
            seen.insert(materialized, unstamped(rows(store, A)));
        };
        for step in 1..=2 {
            if !materialize_range(store, &mut barriers, step..=step) {
                return;
            }
            note(store);
        }
        let pointer = CheckpointPointerV1 {
            origin: store.origin(A).unwrap(),
            represented: store.frontiers(A).unwrap().materialized(),
            format: 1,
            manifest_digest: Digest32([0x5a; 32]),
            checkpoint_id: Digest32([0x5b; 32]),
        };
        if store.publish_checkpoint(A, &pointer).is_err() {
            return;
        }
        note(store);
        for step in 3..=4 {
            if !materialize_range(store, &mut barriers, step..=step) {
                return;
            }
            note(store);
        }
    };
    let measure = |dir: &Path, plan: Option<(u64, Tail)>| {
        let journal =
            RaftEngineJournal::create(dir, identity(), &JournalOptions::default()).unwrap();
        let (backend, shared) = FaultBackend::new(Vec::new(), FaultPlan::default());
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
        store.replay_projection(never).unwrap();
        store.attach(A, shard(), engine).unwrap();
        let setup = shared.ops();
        if let Some((k, tail)) = plan {
            shared.set_plan(FaultPlan {
                crash_after: Some(setup + k),
                tail,
                ..FaultPlan::default()
            });
        }
        let mut seen = BTreeMap::new();
        workload(&mut store, &mut seen);
        drop(store);
        (setup, shared, seen)
    };
    let probe_dir = tempfile::tempdir().unwrap();
    let (setup, probe, reference) = measure(&probe_dir.path().join("journal"), None);
    let workload_ops = probe.ops() - setup;
    assert_eq!(reference.len(), 5, "four steps and a publication");

    let mut published = 0;
    for k in 1..=workload_ops {
        for tail in [Tail::None, Tail::All, Tail::Seeded(k)] {
            let dir = tempfile::tempdir().unwrap();
            let journal_dir = dir.path().join("journal");
            let (_, shared, crashed_seen) = measure(&journal_dir, Some((k, tail)));
            assert!(shared.is_frozen(), "k={k}: the crash happened");
            let image = shared.crash_image(tail);
            let journal = RaftEngineJournal::open_existing(
                &journal_dir,
                identity(),
                &JournalOptions::default(),
            )
            .unwrap();
            let engine = if image.is_empty() {
                let (backend, _) = FaultBackend::new(Vec::new(), FaultPlan::default());
                RedbEngine::create_on_backend(backend, CACHE).unwrap()
            } else {
                let (backend, _) = FaultBackend::new(image, FaultPlan::default());
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
            next.replay_projection(never).unwrap();
            let baseline = next
                .recovery_baseline(A)
                .unwrap()
                .map_or(LocalJournalSeq::ZERO, |p| p.represented);
            if baseline > LocalJournalSeq::ZERO {
                published += 1;
            }
            next.validate_projection(A, &engine, baseline)
                .unwrap_or_else(|e| {
                    panic!("k={k} tail={tail:?}: the projection does not continue the journal from {baseline:?}: {e}")
                });
            next.attach_with_baseline(A, shard(), engine, baseline)
                .unwrap_or_else(|e| panic!("k={k} tail={tail:?}: {e:?}"));
            let recovered = next.frontiers(A).unwrap();
            assert_eq!(recovered.materialized(), recovered.durable());
            assert_eq!(next.status(A), Some(DomainStatus::Ready));
            // This boot's own lifecycle record is the head; the workload's
            // last record is the one before it.
            let head = recovered.durable().get() - 1;
            assert!(
                crashed_seen.keys().all(|seen| *seen <= head),
                "k={k} tail={tail:?}: the journal lost a record the crashed run materialized"
            );
            assert_eq!(
                Some(&unstamped(rows(&next, A))),
                reference.get(&head),
                "k={k} tail={tail:?}: the recovered projection is the journal's state at {head}"
            );
        }
    }
    assert!(
        published > 0,
        "some crashes came after the pointer was durable"
    );
}

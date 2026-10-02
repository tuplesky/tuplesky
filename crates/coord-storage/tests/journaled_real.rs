//! task-j03 acceptance on the real engines: the journal-first pipeline over
//! the pinned raft-engine journal and a durable redb projection.
//!
//! These cases prove the composition itself - one synced grouped journal
//! write serving several domains, then one atomic durable projection commit
//! per domain, and replay of `(M, J]` from the journal's actual valid
//! records after the projection loses everything unsynced. Injected
//! journal, filesystem and process faults are task-j05; nothing here claims
//! power-loss qualification.

use std::path::Path;

use coord_core::effect::{BootId, PersistBatch, StoreUpdate};
use coord_core::outbox::BarrierAllocator;
use coord_journal_api::stream::ShardId;
use coord_journal_raft_engine::{JournalIdentity, JournalOptions, RaftEngineJournal};
use coord_redb_faultkit::{FaultBackend, FaultPlan, Tail};
use coord_storage::journaled::{
    DomainStatus, JournalLimits, JournaledStore, Submission, TransitionKind,
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
    for step in 1..=steps {
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
            return;
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
            return;
        }
    }
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
        drop(journal);
        drop(engines);
        (setup, shared)
    };
    let (setup, probe) = measure(&probe_dir.path().join("journal"), None);
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
        next.attach(A, shard(), engine)
            .expect("the projection attaches");
        let recovered = next.frontiers(A).unwrap();
        assert_eq!(recovered.materialized(), recovered.durable());
        assert_eq!(next.status(A), Some(DomainStatus::Ready));
        rows(&next, A)
    };

    let mut with_executed = 0;
    for k in 1..=workload_ops {
        for tail in [Tail::None, Tail::All, Tail::Seeded(k)] {
            let dir = tempfile::tempdir().unwrap();
            let journal_dir = dir.path().join("journal");
            let (_, shared) = measure(&journal_dir, Some((k, tail)));
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

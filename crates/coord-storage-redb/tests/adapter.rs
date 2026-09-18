//! Conformance, fixture replay and fail-closed lifecycle for the redb adapter.

use std::path::{Path, PathBuf};

use coord_storage_redb::{Generation, OpenError, OpenOptions, RedbEngine, StoreIdentity};
use coord_store_api::engine::{LocalEngine, OrderedRead, ScanRequest, SnapshotSource, WriteTxn};
use coord_store_api::registry::Collection;
use coord_store_testkit::conformance::{ConformanceHarness, ScriptedOutcome, run_all};
use coord_store_testkit::scenario::{Step, StoreScenarioV1, replay};
use coord_types::ids::*;

fn identity() -> StoreIdentity {
    StoreIdentity {
        cluster_id: ClusterId([1; 16]),
        domain_id: DomainId([2; 16]),
        replica_id: ReplicaId([3; 16]),
        incarnation: ReplicaIncarnation::new(1).unwrap(),
    }
}

fn options() -> OpenOptions {
    OpenOptions {
        cache_bytes: 8 * 1024 * 1024,
    }
}

/// Harness: crash-and-reopen drops the handle and reopens the generation.
/// Real disk faults are task-09; the fault hooks are reported as skipped.
struct RedbHarness {
    generation: Option<Generation>,
}

impl RedbHarness {
    fn create(root: &Path) -> Self {
        let generation = Generation::create(root, identity(), options()).unwrap();
        RedbHarness {
            generation: Some(generation),
        }
    }
}

impl ConformanceHarness for RedbHarness {
    type Engine = RedbEngine;
    fn engine(&mut self) -> &mut RedbEngine {
        self.generation.as_mut().unwrap().engine()
    }
    fn script_next_commit(&mut self, _: ScriptedOutcome) -> bool {
        false
    }
    fn inject_iterator_error(&mut self, _: usize) -> bool {
        false
    }
    fn crash_and_reopen(&mut self) {
        self.engine().reopen().unwrap();
    }
}

#[test]
fn conformance_suite_passes_and_fault_hooks_are_reported_as_skipped() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = RedbHarness::create(dir.path());
    let report = run_all(&mut h);
    assert!(report.failed().is_empty(), "{report:?}");
    assert_eq!(
        report.skipped(),
        vec!["iterator_errors", "commit_outcomes"],
        "real fault injection arrives with task-09"
    );
}

#[test]
fn scenario_fixture_replays_to_the_model_digest() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../coord-store-testkit/fixtures/store_scenario_v1.json");
    let scenario: StoreScenarioV1 =
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    assert!(
        scenario
            .steps
            .iter()
            .any(|s| matches!(s, Step::CrashReopen))
    );
    let dir = tempfile::tempdir().unwrap();
    let mut h = RedbHarness::create(dir.path());
    let outcome = replay(h.engine(), &scenario, |engine| engine.reopen().unwrap()).unwrap();
    assert!(outcome.matches_oracle, "engine rows differ from the oracle");
    assert_eq!(
        outcome.matches_expected,
        Some(true),
        "digest differs from the model engine's frozen digest"
    );
}

#[test]
fn cross_collection_atomicity_with_read_your_writes_scans() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = RedbHarness::create(dir.path());
    let reader = h.engine().reader();
    let before = reader.snapshot().unwrap();
    {
        let mut tx = h.engine().begin_write().unwrap();
        tx.put(Collection::KvCurrentV1.id(), b"k1", b"v").unwrap();
        tx.put(Collection::KvHistoryV1.id(), b"k1@1", b"v").unwrap();
        tx.put(Collection::EventsV1.id(), b"1/0", b"put").unwrap();
        let page = tx
            .scan_page(Collection::KvHistoryV1.id(), &ScanRequest::all(10, 1 << 20))
            .unwrap();
        assert_eq!(
            page.rows.len(),
            1,
            "scan inside the transaction sees its own write"
        );
        assert_eq!(
            tx.get(Collection::EventsV1.id(), b"1/0").unwrap(),
            Some(b"put".to_vec())
        );
        assert_eq!(
            reader
                .snapshot()
                .unwrap()
                .get(Collection::KvCurrentV1.id(), b"k1")
                .unwrap(),
            None,
            "not visible before commit"
        );
        tx.commit_durable().unwrap();
    }
    assert_eq!(
        before.get(Collection::KvCurrentV1.id(), b"k1").unwrap(),
        None,
        "pinned snapshot"
    );
    // Outstanding handles keep the file open: a crash reopen fails closed
    // until they are gone (a second opener is excluded, never tolerated).
    assert!(h.engine().reopen().is_err());
    drop(before);
    drop(reader);
    h.engine().reopen().unwrap();
    let view = h.engine().reader().snapshot().unwrap();
    for (c, k) in [
        (Collection::KvCurrentV1, b"k1".as_slice()),
        (Collection::KvHistoryV1, b"k1@1"),
        (Collection::EventsV1, b"1/0"),
    ] {
        assert!(
            view.get(c.id(), k).unwrap().is_some(),
            "{} row survived reopen",
            c.name()
        );
    }
}

#[test]
fn open_never_creates_and_create_never_reinitializes() {
    let dir = tempfile::tempdir().unwrap();
    assert!(matches!(
        Generation::open_existing(dir.path(), identity(), options()),
        Err(OpenError::NotInitialized)
    ));
    assert!(
        !dir.path().join("CURRENT").exists(),
        "open must not create anything"
    );
    let g = Generation::create(dir.path(), identity(), options()).unwrap();
    assert_eq!(g.manifest().generation, 1);
    drop(g);
    assert!(matches!(
        Generation::create(dir.path(), identity(), options()),
        Err(OpenError::AlreadyInitialized)
    ));
    let g = Generation::open_existing(dir.path(), identity(), options()).unwrap();
    assert_eq!(g.manifest().engine, "redb");
}

#[test]
fn duplicate_open_is_excluded_by_the_root_lock() {
    let dir = tempfile::tempdir().unwrap();
    let first = Generation::create(dir.path(), identity(), options()).unwrap();
    assert!(matches!(
        Generation::open_existing(dir.path(), identity(), options()),
        Err(OpenError::Busy)
    ));
    assert!(matches!(
        Generation::create(dir.path(), identity(), options()),
        Err(OpenError::Busy)
    ));
    drop(first);
    Generation::open_existing(dir.path(), identity(), options()).unwrap();
}

#[test]
fn wrong_identity_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    drop(Generation::create(dir.path(), identity(), options()).unwrap());
    let mut wrong = identity();
    wrong.cluster_id = ClusterId([9; 16]);
    assert!(matches!(
        Generation::open_existing(dir.path(), wrong, options()),
        Err(OpenError::IdentityMismatch("cluster_id"))
    ));
    let mut wrong = identity();
    wrong.domain_id = DomainId([9; 16]);
    assert!(matches!(
        Generation::open_existing(dir.path(), wrong, options()),
        Err(OpenError::IdentityMismatch("domain_id"))
    ));
    let mut wrong = identity();
    wrong.replica_id = ReplicaId([9; 16]);
    assert!(matches!(
        Generation::open_existing(dir.path(), wrong, options()),
        Err(OpenError::IdentityMismatch("replica_id"))
    ));
    let mut wrong = identity();
    wrong.incarnation = ReplicaIncarnation::new(2).unwrap();
    assert!(matches!(
        Generation::open_existing(dir.path(), wrong, options()),
        Err(OpenError::IdentityMismatch("incarnation"))
    ));
    // The correct identity still opens.
    Generation::open_existing(dir.path(), identity(), options()).unwrap();
}

#[test]
fn damaged_files_fail_closed() {
    let dir = tempfile::tempdir().unwrap();
    drop(Generation::create(dir.path(), identity(), options()).unwrap());
    let gen_dir = dir.path().join("gen-000001");
    let manifest = gen_dir.join("manifest.v1");
    let db = gen_dir.join("domain.redb");

    // Corrupt manifest (flipped byte).
    let good = std::fs::read(&manifest).unwrap();
    let mut bad = good.clone();
    bad[5] ^= 0xff;
    std::fs::write(&manifest, &bad).unwrap();
    assert!(matches!(
        Generation::open_existing(dir.path(), identity(), options()),
        Err(OpenError::Manifest(_))
    ));
    std::fs::write(&manifest, &good).unwrap();

    // Manifest from another engine name.
    let mut foreign = coord_storage_redb::StoreManifestV1::decode(&good).unwrap();
    foreign.engine = "fjall".to_owned();
    std::fs::write(&manifest, foreign.encode()).unwrap();
    assert!(matches!(
        Generation::open_existing(dir.path(), identity(), options()),
        Err(OpenError::EngineMismatch(_))
    ));
    // Manifest claiming another generation number.
    let mut other_gen = coord_storage_redb::StoreManifestV1::decode(&good).unwrap();
    other_gen.generation = 2;
    std::fs::write(&manifest, other_gen.encode()).unwrap();
    assert!(matches!(
        Generation::open_existing(dir.path(), identity(), options()),
        Err(OpenError::GenerationMismatch)
    ));
    std::fs::write(&manifest, &good).unwrap();

    // Missing manifest.
    std::fs::rename(&manifest, gen_dir.join("manifest.bak")).unwrap();
    assert!(matches!(
        Generation::open_existing(dir.path(), identity(), options()),
        Err(OpenError::Manifest(_))
    ));
    std::fs::rename(gen_dir.join("manifest.bak"), &manifest).unwrap();

    // Empty database file.
    let good_db = std::fs::read(&db).unwrap();
    std::fs::write(&db, b"").unwrap();
    assert!(matches!(
        Generation::open_existing(dir.path(), identity(), options()),
        Err(OpenError::EmptyDatabase)
    ));
    // Garbage database file.
    std::fs::write(&db, b"not a redb file at all").unwrap();
    assert!(matches!(
        Generation::open_existing(dir.path(), identity(), options()),
        Err(OpenError::Corrupt(_))
    ));
    // Missing database file.
    std::fs::remove_file(&db).unwrap();
    assert!(matches!(
        Generation::open_existing(dir.path(), identity(), options()),
        Err(OpenError::MissingDatabase)
    ));
    std::fs::write(&db, &good_db).unwrap();
    assert!(!db.exists() || Generation::open_existing(dir.path(), identity(), options()).is_ok());

    // CURRENT pointing at a missing generation.
    std::fs::write(dir.path().join("CURRENT"), b"gen-000009").unwrap();
    assert!(matches!(
        Generation::open_existing(dir.path(), identity(), options()),
        Err(OpenError::MissingGeneration(_))
    ));
    std::fs::write(dir.path().join("CURRENT"), b"gen-000001").unwrap();

    // A manifest copied over a foreign database: in-database identity
    // record disagrees, so the manifest cannot lend identity.
    let other = tempfile::tempdir().unwrap();
    let mut other_identity = identity();
    other_identity.domain_id = DomainId([7; 16]);
    drop(Generation::create(other.path(), other_identity, options()).unwrap());
    std::fs::copy(&manifest, other.path().join("gen-000001/manifest.v1")).unwrap();
    assert!(matches!(
        Generation::open_existing(other.path(), identity(), options()),
        Err(OpenError::IdentityRecordMismatch("domain_id"))
    ));
}

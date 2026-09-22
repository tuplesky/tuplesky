//! Conformance, fixture replay, physical-layout isolation and fail-closed
//! lifecycle for the experimental fjall adapter: the same suites and
//! scenario fixture as the model and redb adapters, without semantic change.

use std::num::NonZeroU32;
use std::ops::Bound;
use std::path::{Path, PathBuf};

use coord_storage_fjall::{FjallEngine, FjallGeneration, FjallOpenOptions, GROUPS, group_of};
use coord_storage_redb::{Generation as RedbGeneration, OpenError, OpenOptions, StoreIdentity};
use coord_store_api::engine::{
    Direction, ErrorClass, LocalEngine, OrderedRead, ScanRequest, SnapshotSource, WriteTxn,
};
use coord_store_api::registry::{Collection, meta_fields};
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

fn options() -> FjallOpenOptions {
    FjallOpenOptions {
        cache_bytes: 8 * 1024 * 1024,
    }
}

/// Harness: crash-and-reopen drops every handle and reopens the
/// generation; fjall's own flush/compaction internals are not fault
/// injected, so those hooks are reported as skipped.
struct FjallHarness {
    generation: Option<FjallGeneration>,
}

impl FjallHarness {
    fn create(root: &Path) -> Self {
        let generation = FjallGeneration::create(root, identity(), options()).unwrap();
        FjallHarness {
            generation: Some(generation),
        }
    }
}

impl ConformanceHarness for FjallHarness {
    type Engine = FjallEngine;
    fn engine(&mut self) -> &mut FjallEngine {
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
    let mut h = FjallHarness::create(dir.path());
    let report = run_all(&mut h);
    assert!(report.failed().is_empty(), "{report:?}");
    assert_eq!(
        report.skipped(),
        vec!["iterator_errors", "commit_outcomes"],
        "no deterministic fault injection into fjall internals is claimed"
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
    let mut h = FjallHarness::create(dir.path());
    let outcome = replay(h.engine(), &scenario, |engine| engine.reopen().unwrap()).unwrap();
    assert!(outcome.matches_oracle, "engine rows differ from the oracle");
    assert_eq!(
        outcome.matches_expected,
        Some(true),
        "digest differs from the model engine's frozen digest"
    );
}

#[test]
fn cross_keyspace_atomicity_with_read_your_writes_and_pinned_snapshots() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = FjallHarness::create(dir.path());
    let reader = h.engine().reader();
    let before = reader.snapshot().unwrap();
    // Three collections in three different physical groups.
    assert_ne!(
        group_of(Collection::KvCurrentV1),
        group_of(Collection::LeaseV1)
    );
    assert_ne!(
        group_of(Collection::LeaseV1),
        group_of(Collection::ExecutedV1)
    );
    {
        let mut tx = h.engine().begin_write().unwrap();
        tx.put(Collection::KvCurrentV1.id(), b"k1", b"v").unwrap();
        tx.put(Collection::LeaseV1.id(), b"l1", b"lease").unwrap();
        tx.put(Collection::ExecutedV1.id(), b"c1", b"done").unwrap();
        let page = tx
            .scan_page(Collection::LeaseV1.id(), &ScanRequest::all(10, 1 << 20))
            .unwrap();
        assert_eq!(
            page.rows.len(),
            1,
            "scan inside the transaction sees its own write"
        );
        assert_eq!(
            tx.get(Collection::ExecutedV1.id(), b"c1").unwrap(),
            Some(b"done".to_vec())
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
    let after = reader.snapshot().unwrap();
    for (c, k) in [
        (Collection::KvCurrentV1, b"k1".as_slice()),
        (Collection::LeaseV1, b"l1"),
        (Collection::ExecutedV1, b"c1"),
    ] {
        assert!(after.get(c.id(), k).unwrap().is_some(), "{}", c.name());
    }
    // An aborted transaction leaves nothing, in any group.
    {
        let mut tx = h.engine().begin_write().unwrap();
        tx.put(Collection::KvCurrentV1.id(), b"k2", b"v").unwrap();
        tx.delete(Collection::LeaseV1.id(), b"l1").unwrap();
        assert_eq!(tx.get(Collection::LeaseV1.id(), b"l1").unwrap(), None);
        tx.abort().unwrap();
    }
    let view = reader.snapshot().unwrap();
    assert_eq!(view.get(Collection::KvCurrentV1.id(), b"k2").unwrap(), None);
    assert!(view.get(Collection::LeaseV1.id(), b"l1").unwrap().is_some());
    // Outstanding handles keep the database open: a crash reopen fails
    // closed until they are gone.
    assert!(h.engine().reopen().is_err());
    drop((before, after, view, reader));
    h.engine().reopen().unwrap();
    let view = h.engine().reader().snapshot().unwrap();
    for (c, k) in [
        (Collection::KvCurrentV1, b"k1".as_slice()),
        (Collection::LeaseV1, b"l1"),
        (Collection::ExecutedV1, b"c1"),
    ] {
        assert!(
            view.get(c.id(), k).unwrap().is_some(),
            "{} row survived reopen",
            c.name()
        );
    }
    assert_eq!(view.get(Collection::KvCurrentV1.id(), b"k2").unwrap(), None);
}

/// Collections sharing one physical keyspace never see each other's rows:
/// unbounded, prefix-edge, reverse and cursor-resumed scans stay inside the
/// logical collection and physical prefixes never leak.
#[test]
fn grouped_collections_are_isolated_by_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = FjallHarness::create(dir.path());
    // KvHistoryV1 and EventsV1 are adjacent ids in the same group.
    let a = Collection::KvHistoryV1;
    let b = Collection::EventsV1;
    assert_eq!(group_of(a), group_of(b));
    assert_eq!(b.id().0, a.id().0 + 1);
    {
        let mut tx = h.engine().begin_write().unwrap();
        // Edge keys: empty, all-0xff, and a key that would sort after the
        // next collection's prefix if prefixes leaked.
        for (c, k) in [
            (a, b"".as_slice()),
            (a, b"\xff\xff\xff"),
            (a, b"m"),
            (b, b""),
            (b, b"\x00"),
            (b, b"m"),
            (b, b"\xff"),
        ] {
            tx.put(c.id(), k, c.name().as_bytes()).unwrap();
        }
        tx.commit_durable().unwrap();
    }
    let view = h.engine().reader().snapshot().unwrap();
    let all = |c: Collection, direction: Direction| {
        let mut req = ScanRequest::all(100, 1 << 20);
        req.direction = direction;
        view.scan_page(c.id(), &req).unwrap()
    };
    let fwd = all(a, Direction::Forward);
    assert!(fwd.exhausted);
    assert_eq!(
        fwd.rows.iter().map(|r| r.key.clone()).collect::<Vec<_>>(),
        vec![b"".to_vec(), b"m".to_vec(), b"\xff\xff\xff".to_vec()]
    );
    assert!(
        fwd.rows.iter().all(|r| r.value == a.name().as_bytes()),
        "rows carry only their own collection's values"
    );
    let rev = all(b, Direction::Reverse);
    assert_eq!(
        rev.rows.iter().map(|r| r.key.clone()).collect::<Vec<_>>(),
        vec![
            b"\xff".to_vec(),
            b"m".to_vec(),
            b"\x00".to_vec(),
            b"".to_vec()
        ]
    );
    // Cursor resume in both directions stays inside the collection.
    let mut req = ScanRequest::all(1, 1 << 20);
    req.resume_after = Some(b"m".to_vec());
    let page = view.scan_page(a.id(), &req).unwrap();
    assert_eq!(page.rows.len(), 1);
    assert_eq!(page.rows[0].key, b"\xff\xff\xff");
    req.resume_after = Some(b"\xff\xff\xff".to_vec());
    let page = view.scan_page(a.id(), &req).unwrap();
    assert!(page.rows.is_empty() && page.exhausted);
    let mut req = ScanRequest::all(10, 1 << 20);
    req.direction = Direction::Reverse;
    req.resume_after = Some(b"\x00".to_vec());
    let page = view.scan_page(b.id(), &req).unwrap();
    assert_eq!(page.rows.len(), 1);
    assert_eq!(page.rows[0].key, b"");
    // An explicit interval past every key of the collection is empty even
    // though the neighbour collection has rows there physically.
    let mut req = ScanRequest::all(10, 1 << 20);
    req.lower = Bound::Excluded(b"\xff\xff\xff".to_vec());
    let page = view.scan_page(a.id(), &req).unwrap();
    assert!(page.rows.is_empty() && page.exhausted);
    // Every collection is grouped exactly once.
    let mut grouped: Vec<Collection> = GROUPS.iter().flat_map(|(_, m)| m.iter().copied()).collect();
    grouped.sort_by_key(|c| c.id().0);
    grouped.dedup();
    assert_eq!(grouped.len(), Collection::ALL.len());
}

/// Budgets aggregate key and value bytes; an oversized next row in an
/// empty page is a typed limit; a key that would exceed the engine's key
/// limit once prefixed is refused before any write.
#[test]
fn byte_budgets_and_engine_limits_are_typed() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = FjallHarness::create(dir.path());
    let c = Collection::PayloadV1.id();
    {
        let mut tx = h.engine().begin_write().unwrap();
        tx.put(c, b"a", &[1; 100]).unwrap();
        tx.put(c, b"b", &[2; 100]).unwrap();
        let too_long = vec![0u8; 65536 - 1];
        let err = tx.put(c, &too_long, b"v").unwrap_err();
        assert_eq!(err.class, ErrorClass::Limit);
        assert!(tx.get(c, &too_long).unwrap_err().class == ErrorClass::Limit);
        tx.commit_durable().unwrap();
    }
    let view = h.engine().reader().snapshot().unwrap();
    let mut req = ScanRequest::all(10, 150);
    let page = view.scan_page(c, &req).unwrap();
    assert_eq!(
        page.rows.len(),
        1,
        "second row would exceed the aggregate budget"
    );
    assert!(!page.exhausted);
    req.max_bytes = NonZeroU32::new(50).unwrap();
    assert_eq!(
        view.scan_page(c, &req).unwrap_err().class,
        ErrorClass::Limit
    );
    req.max_bytes = NonZeroU32::new(1000).unwrap();
    req.resume_after = Some(b"a".to_vec());
    let page = view.scan_page(c, &req).unwrap();
    assert_eq!(page.rows.len(), 1);
    assert!(page.exhausted);
    assert_eq!(
        view.get(c, &[0u8; 70_000]).unwrap_err().class,
        ErrorClass::Limit
    );
}

#[test]
fn open_never_creates_and_create_never_reinitializes() {
    let dir = tempfile::tempdir().unwrap();
    assert!(matches!(
        FjallGeneration::open_existing(dir.path(), identity(), options()),
        Err(OpenError::NotInitialized)
    ));
    assert!(
        !dir.path().join("CURRENT").exists(),
        "open must not create anything"
    );
    let g = FjallGeneration::create(dir.path(), identity(), options()).unwrap();
    assert_eq!(g.manifest().generation, 1);
    assert_eq!(g.manifest().engine, "fjall");
    drop(g);
    assert!(matches!(
        FjallGeneration::create(dir.path(), identity(), options()),
        Err(OpenError::AlreadyInitialized)
    ));
    let g = FjallGeneration::open_existing(dir.path(), identity(), options()).unwrap();
    assert!(g.directory().join("fjall").is_dir());
}

#[test]
fn duplicate_open_is_excluded_by_the_root_lock() {
    let dir = tempfile::tempdir().unwrap();
    let first = FjallGeneration::create(dir.path(), identity(), options()).unwrap();
    assert!(matches!(
        FjallGeneration::open_existing(dir.path(), identity(), options()),
        Err(OpenError::Busy)
    ));
    drop(first);
    FjallGeneration::open_existing(dir.path(), identity(), options()).unwrap();
}

#[test]
fn wrong_identity_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    drop(FjallGeneration::create(dir.path(), identity(), options()).unwrap());
    let mut wrong = identity();
    wrong.cluster_id = ClusterId([9; 16]);
    assert!(matches!(
        FjallGeneration::open_existing(dir.path(), wrong, options()),
        Err(OpenError::IdentityMismatch("cluster_id"))
    ));
    let mut wrong = identity();
    wrong.incarnation = ReplicaIncarnation::new(2).unwrap();
    assert!(matches!(
        FjallGeneration::open_existing(dir.path(), wrong, options()),
        Err(OpenError::IdentityMismatch("incarnation"))
    ));
    FjallGeneration::open_existing(dir.path(), identity(), options()).unwrap();
}

/// A redb root is refused by this adapter and a fjall root by the redb
/// adapter: the engine recorded at creation is authoritative, with no
/// conversion or cross-engine image.
#[test]
fn wrong_engine_fails_closed_in_both_directions() {
    let redb_root = tempfile::tempdir().unwrap();
    drop(
        RedbGeneration::create(
            redb_root.path(),
            identity(),
            OpenOptions {
                cache_bytes: 8 * 1024 * 1024,
            },
        )
        .unwrap(),
    );
    assert!(matches!(
        FjallGeneration::open_existing(redb_root.path(), identity(), options()),
        Err(OpenError::EngineMismatch(_))
    ));
    let fjall_root = tempfile::tempdir().unwrap();
    drop(FjallGeneration::create(fjall_root.path(), identity(), options()).unwrap());
    assert!(matches!(
        RedbGeneration::open_existing(
            fjall_root.path(),
            identity(),
            OpenOptions {
                cache_bytes: 8 * 1024 * 1024,
            },
        ),
        Err(OpenError::EngineMismatch(_))
    ));
}

#[test]
fn damaged_or_missing_data_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    drop(FjallGeneration::create(dir.path(), identity(), options()).unwrap());
    let gen_dir = dir.path().join("gen-000001");
    let manifest = gen_dir.join("manifest.v1");
    let db = gen_dir.join("fjall");

    // Corrupt manifest (flipped byte).
    let good = std::fs::read(&manifest).unwrap();
    let mut bad = good.clone();
    bad[5] ^= 0xff;
    std::fs::write(&manifest, &bad).unwrap();
    assert!(matches!(
        FjallGeneration::open_existing(dir.path(), identity(), options()),
        Err(OpenError::Manifest(_))
    ));
    // Manifest claiming another generation number.
    let mut other_gen = coord_storage_redb::StoreManifestV1::decode(&good).unwrap();
    other_gen.generation = 2;
    std::fs::write(&manifest, other_gen.encode()).unwrap();
    assert!(matches!(
        FjallGeneration::open_existing(dir.path(), identity(), options()),
        Err(OpenError::GenerationMismatch)
    ));
    std::fs::write(&manifest, &good).unwrap();

    // Database directory present but empty: never a fresh database.
    std::fs::rename(&db, gen_dir.join("fjall.bak")).unwrap();
    std::fs::create_dir(&db).unwrap();
    assert!(matches!(
        FjallGeneration::open_existing(dir.path(), identity(), options()),
        Err(OpenError::EmptyDatabase)
    ));
    assert!(
        std::fs::read_dir(&db).unwrap().next().is_none(),
        "a refused open must not initialize the directory"
    );
    // Garbage in place of the database.
    std::fs::write(db.join("version"), b"not a fjall database").unwrap();
    assert!(matches!(
        FjallGeneration::open_existing(dir.path(), identity(), options()),
        Err(OpenError::Corrupt(_))
    ));
    // Missing database directory.
    std::fs::remove_dir_all(&db).unwrap();
    assert!(matches!(
        FjallGeneration::open_existing(dir.path(), identity(), options()),
        Err(OpenError::MissingDatabase)
    ));
    std::fs::rename(gen_dir.join("fjall.bak"), &db).unwrap();
    FjallGeneration::open_existing(dir.path(), identity(), options()).unwrap();

    // A manifest copied over a foreign database: the in-database identity
    // record disagrees, so the manifest cannot lend identity.
    let other = tempfile::tempdir().unwrap();
    let mut other_identity = identity();
    other_identity.domain_id = DomainId([7; 16]);
    drop(FjallGeneration::create(other.path(), other_identity, options()).unwrap());
    std::fs::copy(&manifest, other.path().join("gen-000001/manifest.v1")).unwrap();
    assert!(matches!(
        FjallGeneration::open_existing(other.path(), identity(), options()),
        Err(OpenError::IdentityRecordMismatch("domain_id"))
    ));
}

/// A database whose identity record names another durability profile is
/// refused even when its manifest (the same identity, this profile) would be
/// accepted: the record inside the database is authoritative for the profile
/// that wrote it. This crate has one profile, so the record is rewritten
/// directly.
#[test]
fn wrong_profile_in_identity_record_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut generation = FjallGeneration::create(dir.path(), identity(), options()).unwrap();
        let mut tx = generation.engine().begin_write().unwrap();
        tx.put(
            Collection::MetaV1.id(),
            meta_fields::PROFILE,
            b"some-other-profile-v1",
        )
        .unwrap();
        tx.commit_durable().unwrap();
    }
    assert!(matches!(
        FjallGeneration::open_existing(dir.path(), identity(), options()),
        Err(OpenError::IdentityRecordMismatch("profile"))
    ));
}

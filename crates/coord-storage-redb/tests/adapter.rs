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
    assert!(
        !h.engine().is_quarantined(),
        "refused before closing anything"
    );
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

/// A snapshot keeps each table it has read open for its life (task-d58).
/// The table it keeps is its own transaction's: a commit after the first
/// read is invisible to the second, for point reads and scans alike, and a
/// new snapshot sees it.
#[test]
fn a_snapshot_keeps_its_tables_and_sees_only_its_own_state() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = RedbHarness::create(dir.path());
    let write = |h: &mut RedbHarness, key: &[u8], value: &[u8]| {
        let mut tx = h.engine().begin_write().unwrap();
        tx.put(Collection::KvCurrentV1.id(), key, value).unwrap();
        tx.commit_durable().unwrap();
    };
    write(&mut h, b"a", b"1");
    let reader = h.engine().reader();
    let pinned = reader.snapshot().unwrap();
    // The first read opens the table.
    assert_eq!(
        pinned.get(Collection::KvCurrentV1.id(), b"a").unwrap(),
        Some(b"1".to_vec())
    );
    write(&mut h, b"a", b"2");
    write(&mut h, b"b", b"1");
    for _ in 0..2 {
        assert_eq!(
            pinned.get(Collection::KvCurrentV1.id(), b"a").unwrap(),
            Some(b"1".to_vec()),
            "the kept table is the snapshot's"
        );
        assert_eq!(
            pinned.get(Collection::KvCurrentV1.id(), b"b").unwrap(),
            None
        );
        let page = pinned
            .scan_page(Collection::KvCurrentV1.id(), &ScanRequest::all(10, 1 << 20))
            .unwrap();
        assert_eq!(page.rows.len(), 1);
    }
    // Another table of the same snapshot opens on its own first read.
    assert_eq!(
        pinned.get(Collection::EventsV1.id(), b"none").unwrap(),
        None
    );
    let fresh = reader.snapshot().unwrap();
    assert_eq!(
        fresh.get(Collection::KvCurrentV1.id(), b"a").unwrap(),
        Some(b"2".to_vec())
    );
    assert_eq!(
        fresh.get(Collection::KvCurrentV1.id(), b"b").unwrap(),
        Some(b"1".to_vec())
    );
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
    assert!(
        !dir.path().join("lock").exists(),
        "open must not create the lock file"
    );
    // A root that does not exist at all stays absent after a failed open.
    let absent = dir.path().join("missing");
    assert!(matches!(
        Generation::open_existing(&absent, identity(), options()),
        Err(OpenError::NotInitialized)
    ));
    assert!(!absent.exists(), "open must not create the root directory");
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

#[test]
fn failed_reopen_quarantines_the_engine_until_a_reopen_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = RedbHarness::create(dir.path());
    let db = dir.path().join("gen-000001").join("domain.redb");
    // Hide the file: the reopen cannot verify it, and the placeholder must
    // not become a usable database.
    std::fs::rename(&db, dir.path().join("hidden")).unwrap();
    assert!(h.engine().reopen().is_err());
    assert!(h.engine().is_quarantined());
    assert!(
        h.engine().reader().snapshot().is_err(),
        "no reads while quarantined"
    );
    assert!(
        h.engine().begin_write().is_err(),
        "no writes while quarantined"
    );
    // Restoring the file lets a later reopen succeed and serve again.
    std::fs::rename(dir.path().join("hidden"), &db).unwrap();
    h.engine().reopen().unwrap();
    assert!(!h.engine().is_quarantined());
    h.engine().reader().snapshot().unwrap();
    let mut tx = h.engine().begin_write().unwrap();
    tx.put(Collection::KvCurrentV1.id(), b"q", b"1").unwrap();
    tx.commit_durable().unwrap();
}

#[test]
fn profile_identity_record_is_verified_on_open() {
    let dir = tempfile::tempdir().unwrap();
    drop(Generation::create(dir.path(), identity(), options()).unwrap());
    let db_path = dir.path().join("gen-000001").join("domain.redb");
    {
        let db = redb::Database::open(&db_path).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn
                .open_table(coord_storage_redb::engine::table_definition(
                    Collection::MetaV1,
                ))
                .unwrap();
            table
                .insert(b"profile".as_slice(), b"other".as_slice())
                .unwrap();
        }
        txn.commit().unwrap();
    }
    assert!(matches!(
        Generation::open_existing(dir.path(), identity(), options()),
        Err(OpenError::IdentityRecordMismatch("profile"))
    ));
}

/// A store of another layout is refused with its format, not as damage
/// (#243 review): its manifest lists the registry before task-d113's step 2
/// and its database records format 2, or none (format 1). The open and a
/// staging over it both name the format, and neither changes a byte of it.
#[test]
fn a_store_of_another_format_is_refused_with_its_format_and_left_as_it_was() {
    let dir = tempfile::tempdir().unwrap();
    drop(Generation::create(dir.path(), identity(), options()).unwrap());
    let generation = dir.path().join("gen-000001");
    let db_path = generation.join("domain.redb");
    let manifest_path = generation.join("manifest.v1");
    // The registry before step 2: no pending payloads, no decided records.
    let mut manifest = coord_storage_redb::StoreManifestV1::read(&manifest_path).unwrap();
    manifest.collections.retain(|(id, _)| {
        *id != Collection::PendingPayloadV1.id().0 && *id != Collection::DecidedV1.id().0
    });
    manifest.write(&manifest_path).unwrap();
    let set_format = |format: Option<u32>| {
        let db = redb::Database::open(&db_path).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn
                .open_table(coord_storage_redb::engine::table_definition(
                    Collection::MetaV1,
                ))
                .unwrap();
            match format {
                Some(f) => {
                    table
                        .insert(b"format_version".as_slice(), f.to_be_bytes().as_slice())
                        .unwrap();
                }
                None => {
                    table.remove(b"format_version".as_slice()).unwrap();
                }
            }
        }
        txn.commit().unwrap();
    };
    for (recorded, named) in [(Some(2), 2), (None, 1)] {
        set_format(recorded);
        let before = std::fs::read(&db_path).unwrap();
        let reason = |e: OpenError| match e {
            OpenError::Format(reason) => reason,
            other => panic!("format {named}: refused as {other:?}, not by its format"),
        };
        let opened = reason(
            Generation::open_existing(dir.path(), identity(), options())
                .err()
                .expect("a store of another format opened"),
        );
        assert!(
            opened.contains(&format!(
                "store format {named} is not this build's ({})",
                coord_store_api::registry::STORE_FORMAT_VERSION
            )),
            "{opened}"
        );
        let staged = reason(
            coord_storage_redb::InactiveGeneration::stage(dir.path(), identity(), options())
                .err()
                .expect("a staging over a store of another format went ahead"),
        );
        assert_eq!(staged, opened);
        assert!(
            std::fs::read(&db_path).unwrap() == before,
            "format {named}: the refusal changed the store"
        );
    }
    // The same list with this build's format recorded is not another
    // build's store: that stays damage.
    set_format(Some(coord_store_api::registry::STORE_FORMAT_VERSION));
    assert!(matches!(
        Generation::open_existing(dir.path(), identity(), options()),
        Err(OpenError::Corrupt(_))
    ));
}

/// A store of another layout that was not closed cleanly, the upgrade after
/// a crash, is refused as another build's store, not as damage (#247
/// review). redb's read-only open won't read a file that needs a repair,
/// so its format can't be named, and the refusal says why. It changes
/// nothing: the file needs the same repair after it as before.
#[test]
fn a_store_of_another_format_not_closed_cleanly_is_refused_as_another_builds() {
    let dir = tempfile::tempdir().unwrap();
    drop(Generation::create(dir.path(), identity(), options()).unwrap());
    let generation = dir.path().join("gen-000001");
    let db_path = generation.join("domain.redb");
    let manifest_path = generation.join("manifest.v1");
    let mut manifest = coord_storage_redb::StoreManifestV1::read(&manifest_path).unwrap();
    manifest.collections.retain(|(id, _)| {
        *id != Collection::PendingPayloadV1.id().0 && *id != Collection::DecidedV1.id().0
    });
    manifest.write(&manifest_path).unwrap();
    // A commit, then the file as a kill leaves it: the bytes taken while
    // the database is still open, before a clean close writes its state.
    let killed = {
        let db = redb::Database::open(&db_path).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn
                .open_table(coord_storage_redb::engine::table_definition(
                    Collection::MetaV1,
                ))
                .unwrap();
            table
                .insert(b"format_version".as_slice(), 2u32.to_be_bytes().as_slice())
                .unwrap();
        }
        txn.commit().unwrap();
        std::fs::read(&db_path).unwrap()
    };
    std::fs::write(&db_path, &killed).unwrap();
    assert!(
        matches!(
            redb::ReadOnlyDatabase::open(&db_path),
            Err(redb::DatabaseError::RepairAborted)
        ),
        "the file needs a repair: the test shows nothing"
    );
    let reason = match Generation::open_existing(dir.path(), identity(), options()) {
        Err(OpenError::Format(reason)) => reason,
        Err(other) => panic!("refused as {other:?}, not as another build's store"),
        Ok(_) => panic!("a store of another format opened"),
    };
    assert!(
        reason.contains("a store of another build that was not closed cleanly"),
        "{reason}"
    );
    assert!(
        std::fs::read(&db_path).unwrap() == killed,
        "the refusal changed the store"
    );
}

/// Staging over a selected generation applies the checks an open does: a
/// database swapped in under an intact manifest is refused, and nothing is
/// staged, rather than the generation being silently replaced and pruned.
#[test]
fn staging_refuses_a_selected_generation_that_open_would_refuse() {
    let dir = tempfile::tempdir().unwrap();
    drop(Generation::create(dir.path(), identity(), options()).unwrap());
    let db_path = dir.path().join("gen-000001").join("domain.redb");

    // A database created for another domain, with an empty protocol_v1,
    // swapped in under identity's intact manifest.
    let other = tempfile::tempdir().unwrap();
    let mut other_identity = identity();
    other_identity.domain_id = DomainId([7; 16]);
    drop(Generation::create(other.path(), other_identity, options()).unwrap());
    std::fs::copy(
        other.path().join("gen-000001").join("domain.redb"),
        &db_path,
    )
    .unwrap();
    assert!(matches!(
        Generation::open_existing(dir.path(), identity(), options()),
        Err(OpenError::IdentityRecordMismatch("domain_id"))
    ));
    let staged = coord_storage_redb::InactiveGeneration::stage(dir.path(), identity(), options());
    assert!(
        matches!(staged, Err(OpenError::IdentityRecordMismatch("domain_id"))),
        "staging accepted a generation open refuses"
    );
    assert!(!dir.path().join("gen-000002").exists());
    assert_eq!(
        std::fs::read(dir.path().join("CURRENT")).unwrap(),
        b"gen-000001"
    );

    // With the original database back, staging proceeds as before.
    let fresh = tempfile::tempdir().unwrap();
    drop(Generation::create(fresh.path(), identity(), options()).unwrap());
    std::fs::copy(
        fresh.path().join("gen-000001").join("domain.redb"),
        &db_path,
    )
    .unwrap();
    let staged =
        coord_storage_redb::InactiveGeneration::stage(dir.path(), identity(), options()).unwrap();
    assert_eq!(staged.generation(), 2);
}

/// An authorized key replacement adopts this node's own durable state,
/// and a root that has moved past the presented generation does not
/// (task-58; design Section 20.4).
#[test]
fn an_authorized_replacement_adopts_the_state_and_a_stale_one_is_fenced() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut generation = Generation::create(dir.path(), identity(), options()).unwrap();
        let mut txn = generation.engine().begin_write().unwrap();
        txn.put(Collection::KvCurrentV1.id(), b"k", b"v").unwrap();
        txn.commit_durable().unwrap();
    }

    let mut replaced = identity();
    replaced.incarnation = ReplicaIncarnation::new(2).unwrap();
    // Before adoption the root is still the old generation's, and the
    // new credential does not simply open it: the stamp is a real
    // fence, not decoration.
    assert!(matches!(
        Generation::open_existing(dir.path(), replaced, options()),
        Err(OpenError::IdentityMismatch("incarnation"))
    ));
    // What would be adopted is readable without adopting it, and reading
    // it writes nothing: the journal's stream is carried from it first,
    // so a crash before the manifest moves reads the same answer again.
    for _ in 0..2 {
        assert_eq!(
            Generation::adoption_pending(dir.path(), replaced).unwrap(),
            Some(ReplicaIncarnation::new(1).unwrap())
        );
    }
    assert_eq!(
        Generation::adopt(dir.path(), replaced).unwrap(),
        Some(ReplicaIncarnation::new(1).unwrap()),
        "the replacement did not adopt anything, or did not say what from"
    );
    // Idempotent: a restart after an adoption adopts nothing further.
    assert_eq!(Generation::adopt(dir.path(), replaced).unwrap(), None);
    assert_eq!(
        Generation::adoption_pending(dir.path(), replaced).unwrap(),
        None
    );

    // And the state is the node's own, not a fresh root.
    let mut generation = Generation::open_existing(dir.path(), replaced, options()).unwrap();
    assert_eq!(generation.manifest().generation, 1, "a generation was lost");
    let snapshot = generation.engine().reader().snapshot().unwrap();
    assert_eq!(
        snapshot.get(Collection::KvCurrentV1.id(), b"k").unwrap(),
        Some(b"v".to_vec()),
        "an authorized replacement lost this node's durable state"
    );
    drop(generation);

    // The old credential cannot come back: this is the cloned-disk case,
    // and adopting backwards would let a replaced node serve as the
    // voter it no longer is.
    assert!(matches!(
        Generation::adopt(dir.path(), identity()),
        Err(OpenError::IdentityMismatch("incarnation"))
    ));
    assert!(matches!(
        Generation::open_existing(dir.path(), identity(), options()),
        Err(OpenError::IdentityMismatch("incarnation"))
    ));

    // Nor may adoption paper over a different replica or origin: a
    // replacement changes the key generation and nothing else.
    for wrong in [
        StoreIdentity {
            cluster_id: ClusterId([9; 16]),
            ..replaced
        },
        StoreIdentity {
            domain_id: DomainId([9; 16]),
            ..replaced
        },
        StoreIdentity {
            replica_id: ReplicaId([9; 16]),
            ..replaced
        },
    ] {
        assert!(
            Generation::adopt(dir.path(), wrong).is_err(),
            "adoption accepted a root belonging to somebody else"
        );
    }
}

/// One update: a put of `value`, or a delete.
fn update(c: Collection, key: &[u8], value: Option<&[u8]>) -> coord_core::effect::StoreUpdate {
    coord_core::effect::StoreUpdate {
        collection: c.id(),
        key: key.to_vec(),
        value: value.map(<[u8]>::to_vec),
    }
}

/// Every row of `c` a snapshot of `engine` holds.
fn rows_of(engine: &RedbEngine, c: Collection) -> Vec<(Vec<u8>, Vec<u8>)> {
    let view = engine.reader().snapshot().unwrap();
    view.scan_page(c.id(), &ScanRequest::all(1000, 1 << 20))
        .unwrap()
        .rows
        .into_iter()
        .map(|row| (row.key, row.value))
        .collect()
}

/// A batch of updates opens each table once for the transaction
/// (task-d86), and leaves what one put or delete each, in order, leaves:
/// across collections, with a key written twice and a key put then
/// deleted within the batch, and a delete of a key written before it.
#[test]
fn a_batch_of_updates_leaves_what_one_call_each_leaves() {
    let batch = [
        update(Collection::KvCurrentV1, b"a", Some(b"1")),
        update(Collection::KvHistoryV1, b"a@1", Some(b"1")),
        update(Collection::KvCurrentV1, b"b", Some(b"2")),
        update(Collection::KvCurrentV1, b"a", Some(b"3")),
        update(Collection::EventsV1, b"1/0", Some(b"put")),
        update(Collection::KvCurrentV1, b"c", Some(b"4")),
        update(Collection::KvCurrentV1, b"c", None),
        update(Collection::KvCurrentV1, b"old", None),
        update(Collection::KvHistoryV1, b"a@2", Some(b"3")),
    ];
    let collections = [
        Collection::KvCurrentV1,
        Collection::KvHistoryV1,
        Collection::EventsV1,
    ];
    let run = |batched: bool| {
        let dir = tempfile::tempdir().unwrap();
        let mut h = RedbHarness::create(dir.path());
        {
            let mut tx = h.engine().begin_write().unwrap();
            tx.put(Collection::KvCurrentV1.id(), b"old", b"0").unwrap();
            tx.commit_durable().unwrap();
        }
        {
            let mut tx = h.engine().begin_write().unwrap();
            if batched {
                tx.apply_updates(batch.iter()).unwrap();
            } else {
                for u in &batch {
                    match &u.value {
                        Some(v) => tx.put(u.collection, &u.key, v).unwrap(),
                        None => tx.delete(u.collection, &u.key).unwrap(),
                    }
                }
            }
            // Read-your-writes after the batch, with its tables closed.
            assert_eq!(
                tx.get(Collection::KvCurrentV1.id(), b"a").unwrap(),
                Some(b"3".to_vec())
            );
            tx.commit_durable().unwrap();
        }
        collections
            .iter()
            .map(|c| rows_of(h.engine(), *c))
            .collect::<Vec<_>>()
    };
    let one_each = run(false);
    assert_eq!(
        one_each[0],
        vec![
            (b"a".to_vec(), b"3".to_vec()),
            (b"b".to_vec(), b"2".to_vec())
        ]
    );
    assert_eq!(run(true), one_each);
}

/// A batch naming a collection the registry does not know is refused,
/// as a put to it is.
#[test]
fn a_batch_with_an_unregistered_collection_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = RedbHarness::create(dir.path());
    let mut tx = h.engine().begin_write().unwrap();
    let unknown = coord_core::effect::StoreUpdate {
        collection: coord_core::effect::CollectionId(u16::MAX),
        key: b"k".to_vec(),
        value: Some(b"v".to_vec()),
    };
    let error = tx.apply_updates([&unknown]).unwrap_err();
    assert_eq!(
        error.class,
        coord_store_api::engine::ErrorClass::Unsupported
    );
}

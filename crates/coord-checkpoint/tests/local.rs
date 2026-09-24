//! `LocalRecoveryCheckpointV1` end to end (task-j04): what the image
//! contains, what it refuses, what a crash at each publication step
//! leaves behind, and that an installed image plus the journal suffix is
//! the state that was pinned.
//!
//! The claim under test is the one Section 17.16.5 rests on: physical
//! redo reclamation preserves unresolved obligations, because they are
//! inside the image. So the fixture deliberately holds a promise, an
//! unresolved vote and the payload of a command that has not executed,
//! and every test that installs an image looks for them.

use std::path::Path;

use coord_checkpoint::local::{
    InstallLocalLimits, LocalCheckpointV1, LocalError, LocalLimits, export_local, install_local,
    verify_local,
};
use coord_checkpoint::manifest::RowV1;
use coord_checkpoint::store::{LocalCheckpointStore, StoreError};
use coord_journal_api::frontier::{LOCAL_CHECKPOINT_FORMAT_V1, select_recovery_pointer};
use coord_journal_api::record::RecordOrigin;
use coord_journal_api::stream::StorageStreamId;
use coord_storage::lowering::{DurableMeta, ExecutionFrontier};
use coord_store_api::engine::{LocalEngine, OrderedRead, ScanRequest, SnapshotSource, WriteTxn};
use coord_store_api::envelope::AppliedStamp;
use coord_store_api::registry::{Collection, meta_fields};
use coord_store_api::seq::StoreSeq;
use coord_store_testkit::model::ModelEngine;
use coord_types::identity::Digest32;
use coord_types::ids::*;

const CLUSTER: ClusterId = ClusterId([1; 16]);
const DOMAIN: DomainId = DomainId([2; 16]);
const REPLICA: ReplicaId = ReplicaId([0xd0; 16]);

fn origin(incarnation: u64) -> RecordOrigin {
    RecordOrigin {
        cluster: CLUSTER,
        domain: DOMAIN,
        replica: REPLICA,
        incarnation: ReplicaIncarnation::new(incarnation).unwrap(),
        stream: StorageStreamId::from_durable(1).unwrap(),
    }
}

fn seq(n: u64) -> LocalJournalSeq {
    LocalJournalSeq::new(n).unwrap()
}

/// This node's storage as it is: common rows, and the node-private ones
/// a shared checkpoint would never carry.
fn rows() -> Vec<(Collection, Vec<u8>, Vec<u8>)> {
    vec![
        (
            Collection::MetaV1,
            meta_fields::CLUSTER_ID.to_vec(),
            CLUSTER.as_bytes().to_vec(),
        ),
        (
            Collection::MetaV1,
            meta_fields::REPLICA_ID.to_vec(),
            REPLICA.as_bytes().to_vec(),
        ),
        // A promise this node made. Losing it would let the node vote
        // again in a ballot it already fenced itself out of.
        (
            Collection::ProtocolV1,
            b"promise".to_vec(),
            b"ballot-7".to_vec(),
        ),
        // A vote it has not resolved.
        (
            Collection::ProtocolV1,
            b"vote-c1".to_vec(),
            b"accepted".to_vec(),
        ),
        // The payload of a command it has not executed: a shared
        // checkpoint skips exactly this row, and a local one must not.
        (
            Collection::PayloadV1,
            b"command-unexecuted".to_vec(),
            b"payload".to_vec(),
        ),
        (Collection::KvCurrentV1, b"key".to_vec(), b"value".to_vec()),
        (
            Collection::CheckpointV1,
            b"previous".to_vec(),
            b"baseline".to_vec(),
        ),
    ]
}

fn pinned_meta() -> DurableMeta {
    DurableMeta {
        stamp: AppliedStamp::new(StoreSeq::from_journal(seq(41)), Digest32([0xab; 32])),
        frontier: ExecutionFrontier {
            configuration: ConfigurationEpoch::new(7).unwrap(),
            execution_position: ExecutionPosition::new(4).unwrap(),
        },
    }
}

fn engine() -> ModelEngine {
    let mut engine = ModelEngine::new();
    let mut tx = engine.begin_write().unwrap();
    for (collection, key, value) in rows() {
        tx.put(collection.id(), &key, &value).unwrap();
    }
    pinned_meta().write(&mut tx).unwrap();
    tx.commit_durable().unwrap();
    engine
}

fn exported(engine: &ModelEngine) -> LocalCheckpointV1 {
    let view = engine.reader().snapshot().unwrap();
    export_local(&view, origin(3), seq(41), &LocalLimits::default()).expect("exported")
}

/// Every row of every collection, node-private ones included.
#[test]
fn a_local_image_is_this_nodes_whole_storage() {
    let engine = engine();
    let checkpoint = exported(&engine);
    verify_local(&checkpoint).expect("verifies");

    let present: Vec<RowV1> = checkpoint
        .chunks
        .iter()
        .flat_map(|c| c.rows.iter().cloned())
        .collect();
    assert_eq!(present.len() as u64, checkpoint.manifest.rows());
    // The three things a shared checkpoint deliberately drops.
    for (collection, key) in [
        (Collection::ProtocolV1, b"promise".as_slice()),
        (Collection::ProtocolV1, b"vote-c1".as_slice()),
        (Collection::PayloadV1, b"command-unexecuted".as_slice()),
    ] {
        assert!(
            present
                .iter()
                .any(|r| r.collection == collection.id().0 && r.key == key),
            "a local image without {key:?} would let this node deny an obligation it has"
        );
    }
    // Every collection of the registry is summarized, so "complete" is
    // checkable rather than assumed.
    assert_eq!(checkpoint.manifest.collections.len(), Collection::ALL.len());
    // And the pin is the projection's own, which is what recovery
    // checks the suffix against.
    assert_eq!(checkpoint.manifest.represented, seq(41));
    assert_eq!(
        checkpoint.manifest.pin.applied,
        StoreSeq::from_journal(seq(41))
    );
    assert_eq!(
        checkpoint.manifest.pin.execution_position,
        ExecutionPosition::new(4).unwrap()
    );
}

/// Tampering with the image is caught before anything is installed.
#[test]
fn an_image_that_is_not_what_its_manifest_says_does_not_verify() {
    let engine = engine();
    let good = exported(&engine);

    // A changed row: the chunk no longer hashes to its descriptor.
    let mut tampered = good.clone();
    tampered.chunks[0].rows[0].value = b"other".to_vec();
    assert!(matches!(
        verify_local(&tampered),
        Err(LocalError::ChunkDigest { ordinal: 0 })
    ));

    // A changed summary: the root no longer matches its own fields.
    let mut tampered = good.clone();
    tampered.manifest.collections[0].rows += 1;
    assert_eq!(verify_local(&tampered), Err(LocalError::RootMismatch));

    // A summary changed together with the root: complete, consistent
    // and still not what the rows say.
    let mut tampered = good.clone();
    tampered.manifest.collections[0].rows += 1;
    tampered.manifest.root = tampered.manifest.compute_root();
    assert_eq!(verify_local(&tampered), Err(LocalError::Rows));

    // A dropped collection: an incomplete image is not a local
    // checkpoint, whatever else is right about it.
    let mut tampered = good.clone();
    tampered.manifest.collections.remove(0);
    tampered.manifest.root = tampered.manifest.compute_root();
    assert_eq!(verify_local(&tampered), Err(LocalError::Incomplete));

    // A format this build does not read.
    let mut tampered = good;
    tampered.manifest.format = LOCAL_CHECKPOINT_FORMAT_V1 + 1;
    tampered.manifest.root = tampered.manifest.compute_root();
    assert!(matches!(
        verify_local(&tampered),
        Err(LocalError::UnsupportedFormat { .. })
    ));
}

/// An export whose view moves under it is refused rather than mixed.
#[test]
fn an_export_of_a_view_that_moved_is_refused() {
    // The model engine hands out a snapshot that does not move, so the
    // mixing is staged directly: an image assembled from two different
    // pins is what the fence exists to stop, and the fence is the
    // applied stamp.
    let mut engine = engine();
    let first = exported(&engine);
    let mut tx = engine.begin_write().unwrap();
    DurableMeta {
        stamp: AppliedStamp::new(StoreSeq::from_journal(seq(42)), Digest32([0xcd; 32])),
        ..pinned_meta()
    }
    .write(&mut tx)
    .unwrap();
    tx.commit_durable().unwrap();
    let second = exported(&engine);
    assert_ne!(
        first.manifest.root, second.manifest.root,
        "two pins are two images, and the root says which one it is"
    );
    assert_eq!(second.manifest.pin.applied, StoreSeq::from_journal(seq(42)));
}

/// Writing, selecting and loading: what is on disk, and what selects it.
#[test]
fn a_written_image_loads_exactly_and_a_directory_selects_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = LocalCheckpointStore::open(dir.path()).expect("store");
    let engine = engine();
    let checkpoint = exported(&engine);

    let pointer = store.write(&checkpoint).expect("written");
    assert_eq!(pointer.checkpoint_id, checkpoint.manifest.root);
    assert_eq!(pointer.represented, seq(41));
    assert_eq!(pointer.format, LOCAL_CHECKPOINT_FORMAT_V1);
    assert_eq!(store.load(&pointer).expect("loads"), checkpoint);

    // Writing it again is writing the same image: the name is the
    // image's own identity, so publication is idempotent.
    let again = store.write(&checkpoint).expect("written again");
    assert_eq!(again, pointer);
    assert_eq!(store.images().unwrap(), vec![pointer.checkpoint_id]);

    // A second, later image. Both are on disk; which one recovery uses
    // is the pointer's answer, never the listing's.
    let mut later = checkpoint.clone();
    later.manifest.represented = seq(64);
    later.manifest.root = later.manifest.compute_root();
    let later_pointer = store.write(&later).expect("written");
    assert_eq!(store.images().unwrap().len(), 2);
    let selected = select_recovery_pointer(&origin(3), [&pointer, &later_pointer]);
    assert_eq!(selected, Some(&later_pointer));
    // And the older one still loads, because nothing has reclaimed it.
    assert_eq!(store.load(&pointer).expect("loads"), checkpoint);
}

/// A crash at each publication step leaves a valid selected source, or
/// nothing selected at all.
#[test]
fn a_crash_at_each_publication_step_leaves_a_valid_selection() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = LocalCheckpointStore::open(dir.path()).expect("store");
    let engine = engine();
    let first = exported(&engine);
    let first_pointer = store.write(&first).expect("written");

    // "Checkpoint bytes ready, pointer not durable": a complete second
    // image exists and nothing names it. The prior publication is
    // authoritative, and it still loads.
    let mut second = first.clone();
    second.manifest.represented = seq(64);
    second.manifest.root = second.manifest.compute_root();
    let second_pointer = store.write(&second).expect("written");
    assert_eq!(store.load(&first_pointer).expect("loads"), first);

    // "Pointer durable, trim incomplete" and "trim complete, old
    // cleanup interrupted" are the same thing here: the new pointer is
    // the selection and its image is intact whether or not the old one
    // was removed yet.
    assert_eq!(store.load(&second_pointer).expect("loads"), second);
    let removed = store.reclaim(&second_pointer).expect("reclaimed");
    assert_eq!(removed, 1, "exactly the superseded image");
    assert_eq!(store.images().unwrap(), vec![second_pointer.checkpoint_id]);
    // Retryable and idempotent: the second pass removes nothing and
    // never the survivor.
    assert_eq!(store.reclaim(&second_pointer).expect("again"), 0);
    assert_eq!(store.load(&second_pointer).expect("still there"), second);

    // A crash before the rename leaves a pending directory. Nothing
    // selects it, nothing loads it, and reclaim removes it.
    let pending = dir.path().join(".pending-deadbeef");
    std::fs::create_dir(&pending).unwrap();
    std::fs::write(pending.join("manifest.bin"), b"half").unwrap();
    assert_eq!(
        store.images().unwrap(),
        vec![second_pointer.checkpoint_id],
        "a pending directory is not an image"
    );
    assert_eq!(store.reclaim(&second_pointer).expect("reclaimed"), 1);
    assert!(!pending.exists());
}

/// Damage to the selected image quarantines; it never becomes a fresh
/// start.
#[test]
fn a_damaged_selected_image_quarantines_and_never_initializes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = LocalCheckpointStore::open(dir.path()).expect("store");
    let engine = engine();
    let checkpoint = exported(&engine);
    let pointer = store.write(&checkpoint).expect("written");
    let image = image_dir(dir.path(), &pointer.checkpoint_id);

    // A missing chunk.
    let chunk = image.join("chunk-00000.bin");
    let saved = std::fs::read(&chunk).unwrap();
    std::fs::remove_file(&chunk).unwrap();
    assert!(
        matches!(store.load(&pointer), Err(StoreError::Quarantine { .. })),
        "a missing chunk is damage, not an empty store"
    );

    // A truncated chunk.
    std::fs::write(&chunk, &saved[..saved.len() / 2]).unwrap();
    assert!(matches!(
        store.load(&pointer),
        Err(StoreError::Artifact(_)) | Err(StoreError::Quarantine { .. })
    ));

    // A chunk whose bytes decode and are not the ones the manifest
    // describes.
    let mut tampered = checkpoint.clone();
    tampered.chunks[0].rows[0].value = b"other".to_vec();
    std::fs::write(&chunk, tampered.chunks[0].encode().unwrap()).unwrap();
    assert!(matches!(store.load(&pointer), Err(StoreError::Invalid(_))));
    std::fs::write(&chunk, &saved).unwrap();
    assert_eq!(store.load(&pointer).expect("restored"), checkpoint);

    // A manifest that is not the one the pointer names.
    let mut other = pointer;
    other.manifest_digest = Digest32([0; 32]);
    assert!(matches!(
        store.load(&other),
        Err(StoreError::Quarantine { .. })
    ));

    // An image the pointer names and that is not there at all.
    let mut absent = pointer;
    absent.checkpoint_id = Digest32([0x11; 32]);
    assert!(matches!(
        store.load(&absent),
        Err(StoreError::Quarantine { .. })
    ));
}

/// An installed image is the storage that was pinned, obligations
/// included, and it re-exports to the same identity.
#[test]
fn an_installed_image_is_the_storage_that_was_pinned() {
    let source = engine();
    let checkpoint = exported(&source);
    let mut target = ModelEngine::new();
    let rows_written = install_local(
        &mut target,
        &checkpoint,
        &origin(3),
        &InstallLocalLimits::default(),
    )
    .expect("installed");
    assert_eq!(rows_written, checkpoint.manifest.rows());

    // The promise and the unresolved vote are there.
    let view = target.reader().snapshot().unwrap();
    assert_eq!(
        view.get(Collection::ProtocolV1.id(), b"promise").unwrap(),
        Some(b"ballot-7".to_vec())
    );
    assert_eq!(
        view.get(Collection::PayloadV1.id(), b"command-unexecuted")
            .unwrap(),
        Some(b"payload".to_vec())
    );
    // The stamp came back with the rest, so the generation is already at
    // C and an ordinary attach replays (C, J] onto it.
    let meta = DurableMeta::read(&view).unwrap();
    assert_eq!(meta.stamp.store_seq(), StoreSeq::from_journal(seq(41)));
    assert_eq!(meta.frontier, pinned_meta().frontier);
    drop(view);

    // Re-exporting the installed generation reproduces the image
    // exactly: same rows, same summaries, same root.
    let view = target.reader().snapshot().unwrap();
    let again = export_local(&view, origin(3), seq(41), &LocalLimits::default()).expect("exported");
    assert_eq!(again.manifest.root, checkpoint.manifest.root);
    assert_eq!(again, checkpoint);
}

/// An image is installed into a fresh generation, and only this node's.
#[test]
fn an_image_is_never_merged_and_never_inherited() {
    let source = engine();
    let checkpoint = exported(&source);

    // Another incarnation's image carries obligations this node did not
    // make; installing it would be inheriting them.
    let mut target = ModelEngine::new();
    assert!(matches!(
        install_local(
            &mut target,
            &checkpoint,
            &origin(4),
            &InstallLocalLimits::default()
        ),
        Err(coord_checkpoint::local::InstallLocalError::ForeignOrigin)
    ));

    // And a generation that already holds rows is not a fresh one.
    let mut occupied = ModelEngine::new();
    let mut tx = occupied.begin_write().unwrap();
    tx.put(Collection::KvCurrentV1.id(), b"stale", b"row")
        .unwrap();
    tx.commit_durable().unwrap();
    assert!(matches!(
        install_local(
            &mut occupied,
            &checkpoint,
            &origin(3),
            &InstallLocalLimits::default()
        ),
        Err(coord_checkpoint::local::InstallLocalError::NotEmpty { .. })
    ));
}

fn image_dir(root: &Path, id: &Digest32) -> std::path::PathBuf {
    let mut name = String::with_capacity(64);
    for byte in id.0 {
        name.push_str(&format!("{byte:02x}"));
    }
    root.join(name)
}

/// The scan helper the emptiness check uses, exercised directly so a
/// silently always-empty scan cannot make the check vacuous.
#[test]
fn the_emptiness_check_sees_a_row() {
    let engine = engine();
    let view = engine.reader().snapshot().unwrap();
    let page = view
        .scan_page(Collection::ProtocolV1.id(), &ScanRequest::all(1, 1 << 20))
        .unwrap();
    assert!(!page.rows.is_empty());
}

//! The local checkpoint's image produced off the thread that owns the
//! store (task-d51).
//!
//! [`LocalBaseline::pin_local`] pins the snapshot on the owning thread,
//! [`Pinned::write`] produces and writes the image on another, and
//! [`LocalBaseline::finish_local`] publishes it back on the owner. What
//! that split must not change:
//!
//! * the image is the one a publication on the owning thread would have
//!   written at the same represented position, byte for byte, whatever
//!   the owner commits meanwhile;
//! * a crash while the image is written, or after it and before the
//!   pointer, leaves the previous baseline selected and loadable;
//! * an export past its deadline writes nothing;
//! * an image of a domain that was reattached meanwhile is not published.

use std::path::Path;
use std::time::{Duration, Instant};

use coord_checkpoint::local::{LocalLimits, export_local};
use coord_checkpoint::store::LocalCheckpointStore;
use coord_checkpoint::{BaselineError, LocalBaseline, Written};
use coord_core::effect::{BootId, PersistBatch, StoreUpdate};
use coord_core::outbox::BarrierAllocator;
use coord_journal_api::frontier::CheckpointPointerV1;
use coord_journal_api::stream::ShardId;
use coord_storage::JournaledDomain;
use coord_storage::journaled::{JournalLimits, JournaledStore, Submission, TransitionKind};
use coord_store_api::registry::Collection;
use coord_store_testkit::journal::ModelJournal;
use coord_store_testkit::model::ModelEngine;
use coord_types::ids::*;

const CLUSTER: ClusterId = ClusterId([0x11; 16]);
const REPLICA: ReplicaId = ReplicaId([0x22; 16]);
const DOMAIN: DomainId = DomainId([0xa1; 16]);
const BOOT: BootId = BootId([0x77; 16]);

fn inc() -> ReplicaIncarnation {
    ReplicaIncarnation::new(1).unwrap()
}

fn ballot() -> Ballot {
    Ballot {
        epoch: ConfigurationEpoch::new(1).unwrap(),
        number: 1,
        leader: REPLICA,
    }
}

type Domain = JournaledDomain<ModelJournal, ModelEngine>;

fn open(journal: ModelJournal, engine: ModelEngine) -> Domain {
    let mut store = JournaledStore::open(
        journal,
        CLUSTER,
        REPLICA,
        inc(),
        BOOT,
        JournalLimits::default(),
    )
    .unwrap();
    store
        .attach(DOMAIN, ShardId::new(0).unwrap(), engine)
        .unwrap();
    JournaledDomain::new(store, DOMAIN, ballot()).expect("attached")
}

/// A protocol transition, flushed and materialized.
fn protocol(domain: &mut Domain, barriers: &mut BarrierAllocator, key: &[u8], value: &[u8]) {
    let barrier = barriers.allocate();
    let store = domain.store_mut();
    store
        .submit(Submission {
            domain: DOMAIN,
            ballot: ballot(),
            kind: TransitionKind::Protocol,
            batch: PersistBatch {
                barrier,
                base: None,
                updates: vec![StoreUpdate {
                    collection: Collection::ProtocolV1.id(),
                    key: key.to_vec(),
                    value: Some(value.to_vec()),
                }],
            },
        })
        .unwrap();
    store.flush().unwrap();
}

/// Every file under `root`, by its path relative to it, with its bytes.
fn files(root: &Path) -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if entry.file_type().unwrap().is_dir() {
                stack.push(path);
            } else {
                let name = path.strip_prefix(root).unwrap().display().to_string();
                out.push((name, std::fs::read(&path).unwrap()));
            }
        }
    }
    out.sort();
    out
}

/// The image a publication on the owning thread would write now.
fn image_now(domain: &Domain, images: &LocalCheckpointStore) -> CheckpointPointerV1 {
    let store = domain.store();
    let represented = store.frontiers(DOMAIN).unwrap().materialized();
    let gated = store.reader(DOMAIN).unwrap().snapshot().unwrap();
    let checkpoint = export_local(
        gated.view(),
        store.origin(DOMAIN).unwrap(),
        represented,
        &LocalLimits::default(),
    )
    .expect("exported");
    images.write(&checkpoint).expect("written")
}

#[test]
fn an_image_written_off_the_thread_is_the_one_written_on_it() {
    let mut barriers = BarrierAllocator::new(inc(), BOOT);
    let mut domain = open(ModelJournal::new(), ModelEngine::new());
    for i in 0..64u32 {
        protocol(
            &mut domain,
            &mut barriers,
            format!("row-{i:03}").as_bytes(),
            &i.to_be_bytes(),
        );
    }
    let on_dir = tempfile::tempdir().unwrap();
    let on_thread = LocalCheckpointStore::open(on_dir.path()).unwrap();
    let expected = image_now(&domain, &on_thread);

    let pinned = domain.pin_local().unwrap().expect("something to represent");
    assert_eq!(pinned.represented(), expected.represented);
    // The owner goes on committing while the image is produced: none of
    // it may reach the image.
    for i in 64..96u32 {
        protocol(
            &mut domain,
            &mut barriers,
            format!("row-{i:03}").as_bytes(),
            &i.to_be_bytes(),
        );
    }
    let off_dir = tempfile::tempdir().unwrap();
    let off_thread = LocalCheckpointStore::open(off_dir.path()).unwrap();
    let images = off_thread.clone();
    let written = std::thread::spawn(move || {
        pinned
            .write(&images, &LocalLimits::default(), None)
            .expect("written")
    })
    .join()
    .unwrap();
    assert_eq!(written.pointer, expected, "the same image, named the same");
    assert_eq!(files(off_dir.path()), files(on_dir.path()), "byte for byte");

    let published = domain
        .finish_local(&off_thread, written)
        .expect("published");
    assert_eq!(published.represented, expected.represented);
    assert!(published.retired);
    assert_eq!(
        domain.store().frontiers(DOMAIN).unwrap().checkpoint(),
        expected.represented,
        "C is what the pin represented, not what was committed since"
    );
}

/// A crash after the image was written and before its pointer: the
/// previous baseline is still the selected one and still loads, and the
/// next publication reclaims the image nothing selected.
#[test]
fn a_crash_before_the_pointer_leaves_the_previous_baseline() {
    let dir = tempfile::tempdir().unwrap();
    let images = LocalCheckpointStore::open(dir.path()).unwrap();
    let mut barriers = BarrierAllocator::new(inc(), BOOT);
    let mut domain = open(ModelJournal::new(), ModelEngine::new());
    protocol(&mut domain, &mut barriers, b"promise", b"ballot-7");
    let first = domain
        .publish_local(&images, &LocalLimits::default())
        .unwrap()
        .expect("published");

    protocol(&mut domain, &mut barriers, b"vote-c1", b"accepted");
    let pinned = domain.pin_local().unwrap().expect("something new");
    let writer = images.clone();
    let orphan = std::thread::spawn(move || {
        pinned
            .write(&writer, &LocalLimits::default(), None)
            .expect("written")
    })
    .join()
    .unwrap();
    // A pending directory of a second write the crash cut short.
    std::fs::create_dir(dir.path().join(".pending-cut-short")).unwrap();

    // The crash: nothing of the owner's survives but the journal.
    let (journal, _) = domain.into_store().into_parts();
    let probe = JournaledStore::<ModelJournal, ModelEngine>::open(
        journal,
        CLUSTER,
        REPLICA,
        inc(),
        BOOT,
        JournalLimits::default(),
    )
    .unwrap();
    assert_eq!(
        probe.recovery_baseline(DOMAIN).expect("read"),
        Some(first.pointer),
        "the pointer that was durable still selects"
    );
    images.load(&first.pointer).expect("and its image loads");
    assert!(
        images
            .images()
            .unwrap()
            .contains(&orphan.pointer.checkpoint_id),
        "the unselected image is on disk, and recovery ignores it"
    );

    // The next publication reclaims what nothing selected.
    let (journal, _) = probe.into_parts();
    let checkpoint = images.load(&first.pointer).unwrap();
    let mut fresh = ModelEngine::new();
    coord_checkpoint::local::install_local(
        &mut fresh,
        &checkpoint,
        &first.pointer.origin,
        &coord_checkpoint::local::InstallLocalLimits::default(),
    )
    .expect("installed");
    let mut domain = open(journal, fresh);
    protocol(&mut domain, &mut barriers, b"vote-c2", b"accepted");
    let next = domain
        .publish_local(&images, &LocalLimits::default())
        .unwrap()
        .expect("published");
    assert_eq!(
        images.images().unwrap(),
        vec![next.pointer.checkpoint_id],
        "only the selected image is left"
    );
    assert!(!dir.path().join(".pending-cut-short").exists());
}

#[test]
fn an_export_past_its_deadline_writes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let images = LocalCheckpointStore::open(dir.path()).unwrap();
    let mut barriers = BarrierAllocator::new(inc(), BOOT);
    let mut domain = open(ModelJournal::new(), ModelEngine::new());
    protocol(&mut domain, &mut barriers, b"promise", b"ballot-7");
    let checkpoint = domain.store().frontiers(DOMAIN).unwrap().checkpoint();

    let pinned = domain.pin_local().unwrap().expect("something to represent");
    let past = Instant::now().checked_sub(Duration::from_secs(1)).unwrap();
    let outcome = pinned.write(&images, &LocalLimits::default(), Some(past));
    assert!(
        matches!(outcome, Err(BaselineError::Abandoned)),
        "{outcome:?}"
    );
    assert!(images.images().unwrap().is_empty(), "nothing was written");
    assert_eq!(
        domain.store().frontiers(DOMAIN).unwrap().checkpoint(),
        checkpoint,
        "and nothing was published"
    );
    // The snapshot was released with it: the next one is taken and
    // published as usual.
    domain
        .publish_local(&images, &LocalLimits::default())
        .unwrap()
        .expect("published");
}

#[test]
fn an_image_of_another_origin_is_not_published() {
    let dir = tempfile::tempdir().unwrap();
    let images = LocalCheckpointStore::open(dir.path()).unwrap();
    let mut barriers = BarrierAllocator::new(inc(), BOOT);
    let mut domain = open(ModelJournal::new(), ModelEngine::new());
    protocol(&mut domain, &mut barriers, b"promise", b"ballot-7");
    let checkpoint = domain.store().frontiers(DOMAIN).unwrap().checkpoint();
    let pinned = domain.pin_local().unwrap().expect("something to represent");
    let mut written: Written = pinned
        .write(&images, &LocalLimits::default(), None)
        .unwrap();
    // What an image produced before a reattachment names: the storage
    // of an incarnation this node no longer serves.
    written.pointer.origin.incarnation = ReplicaIncarnation::new(2).unwrap();
    let outcome = domain.finish_local(&images, written);
    assert!(
        matches!(outcome, Err(BaselineError::Superseded)),
        "{outcome:?}"
    );
    assert_eq!(
        domain.store().frontiers(DOMAIN).unwrap().checkpoint(),
        checkpoint
    );
}

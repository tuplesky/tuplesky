//! The publication order of Section 17.16.3, composed (task-j04).
//!
//! The five steps are spread across three crates on purpose -- the
//! snapshot and the image are storage's and the filesystem's, the
//! pointer and the retirement are the journal's -- so this is where they
//! are driven together and where the properties that span them are
//! checked:
//!
//! * the pointer is durable *before* anything is retired, so a crash
//!   between them leaves a baseline and a journal that still holds the
//!   prefix it represents;
//! * the publication record is newer than `C` and stays in the suffix,
//!   so a recovered stream always contains the evidence of its own
//!   baseline;
//! * the selected image plus the retained suffix reconstructs exactly
//!   the state that existed when the suffix ran out -- obligations
//!   included;
//! * an image that is not covered by materialization is refused before
//!   it is published, not discovered at recovery.

use coord_checkpoint::local::{InstallLocalLimits, LocalLimits, export_local, install_local};
use coord_checkpoint::store::LocalCheckpointStore;
use coord_core::effect::{BarrierId, BootId, PersistBatch, StoreUpdate};
use coord_core::outbox::BarrierAllocator;
use coord_journal_api::frontier::CheckpointPointerV1;
use coord_journal_api::record::RecordOrigin;
use coord_journal_api::stream::ShardId;
use coord_storage::journaled::{
    JournalLimits, JournaledError, JournaledStore, Submission, TransitionKind,
};
use coord_storage::lowering::DurableMeta;
use coord_store_api::engine::OrderedRead;
use coord_store_api::registry::Collection;
use coord_store_testkit::journal::ModelJournal;
use coord_store_testkit::model::ModelEngine;
use coord_types::identity::Digest32;
use coord_types::ids::*;

const CLUSTER: ClusterId = ClusterId([0x11; 16]);
const REPLICA: ReplicaId = ReplicaId([0x22; 16]);
const DOMAIN: DomainId = DomainId([0xa1; 16]);
const BOOT: BootId = BootId([0x77; 16]);

fn inc() -> ReplicaIncarnation {
    ReplicaIncarnation::new(1).unwrap()
}

fn shard() -> ShardId {
    ShardId::new(0).unwrap()
}

fn ballot(number: u64) -> Ballot {
    Ballot {
        epoch: ConfigurationEpoch::new(1).unwrap(),
        number,
        leader: REPLICA,
    }
}

/// One node: the shared model journal, one model projection.
struct Node {
    store: JournaledStore<ModelJournal, ModelEngine>,
    barriers: BarrierAllocator,
}

impl Node {
    fn open(journal: ModelJournal, engine: ModelEngine) -> Self {
        let mut store = JournaledStore::open(
            journal,
            CLUSTER,
            REPLICA,
            inc(),
            BOOT,
            JournalLimits::default(),
        )
        .unwrap();
        store.attach(DOMAIN, shard(), engine).unwrap();
        Node {
            store,
            barriers: BarrierAllocator::new(inc(), BOOT),
        }
    }

    fn new() -> Self {
        Node::open(ModelJournal::new(), ModelEngine::new())
    }

    /// A protocol transition: an obligation this node takes on.
    fn protocol(&mut self, key: &[u8], value: &[u8]) -> BarrierId {
        let barrier = self.barriers.allocate();
        self.store
            .submit(Submission {
                domain: DOMAIN,
                ballot: ballot(1),
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
        self.store.flush().unwrap();
        barrier
    }

    fn origin(&self) -> RecordOrigin {
        RecordOrigin {
            cluster: CLUSTER,
            domain: DOMAIN,
            replica: REPLICA,
            incarnation: inc(),
            stream: self.store.attached()[0].1,
        }
    }

    /// Steps 1 and 2: pin a snapshot and write a complete image.
    fn write_image(&self, store: &LocalCheckpointStore) -> CheckpointPointerV1 {
        let represented = self.store.frontiers(DOMAIN).unwrap().materialized();
        let gated = self.store.reader(DOMAIN).unwrap().snapshot().unwrap();
        let checkpoint = export_local(
            gated.view(),
            self.origin(),
            represented,
            &LocalLimits::default(),
        )
        .expect("exported");
        store.write(&checkpoint).expect("written")
    }

    fn protocol_rows(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let gated = self.store.reader(DOMAIN).unwrap().snapshot().unwrap();
        let mut request = coord_store_api::engine::ScanRequest::all(1024, 1 << 20);
        let mut out = Vec::new();
        loop {
            let page = gated
                .view()
                .scan_page(Collection::ProtocolV1.id(), &request)
                .unwrap();
            for row in &page.rows {
                out.push((row.key.clone(), row.value.clone()));
            }
            if page.exhausted {
                return out;
            }
            request.resume_after = page.rows.last().map(|r| r.key.clone());
        }
    }
}

/// The whole cycle: publish, retire, lose the projection, come back from
/// the image and the suffix.
#[test]
fn a_reclaimed_prefix_is_recovered_from_the_image_and_the_suffix() {
    let dir = tempfile::tempdir().expect("tempdir");
    let images = LocalCheckpointStore::open(dir.path()).expect("store");
    let mut node = Node::new();

    // Obligations this node takes on before the checkpoint. They are in
    // the journal prefix that is about to be retired, so after the
    // retirement the image is the only thing that still holds them.
    node.protocol(b"promise", b"ballot-7");
    node.protocol(b"vote-c1", b"accepted");

    let pointer = node.write_image(&images);
    let published = node
        .store
        .publish_checkpoint(DOMAIN, &pointer)
        .expect("published");
    assert_eq!(published.published, pointer.represented);
    assert!(published.retired, "the prefix through C was retired");
    assert_eq!(
        node.store.frontiers(DOMAIN).unwrap().checkpoint(),
        pointer.represented,
        "C moved only once the pointer was durable"
    );

    // More obligations after the checkpoint: these are the suffix.
    node.protocol(b"vote-c2", b"accepted");
    let expected = node.protocol_rows();
    assert_eq!(expected.len(), 3);

    // The publication record is still in the stream, so the recovered
    // journal contains the evidence of its own baseline.
    let (journal, projections) = node.store.into_parts();
    let baseline = {
        let probe = JournaledStore::<ModelJournal, ModelEngine>::open(
            journal,
            CLUSTER,
            REPLICA,
            inc(),
            BOOT,
            JournalLimits::default(),
        )
        .unwrap();
        let found = probe.recovery_baseline(DOMAIN).expect("read");
        (found, probe.into_parts().0)
    };
    let (found, journal) = baseline;
    assert_eq!(found, Some(pointer), "the durable pointer selects");
    drop(projections);

    // The projection is gone. Recovery installs the selected image into
    // a fresh one and attaches it; the retained suffix replays onto it.
    let checkpoint = images.load(&pointer).expect("loads");
    let mut fresh = ModelEngine::new();
    install_local(
        &mut fresh,
        &checkpoint,
        &RecordOrigin {
            cluster: CLUSTER,
            domain: DOMAIN,
            replica: REPLICA,
            incarnation: inc(),
            stream: pointer.origin.stream,
        },
        &InstallLocalLimits::default(),
    )
    .expect("installed");
    let recovered = Node::open(journal, fresh);

    assert_eq!(
        recovered.protocol_rows(),
        expected,
        "the image plus the suffix is the storage that existed, obligations included"
    );
}

/// An image the projection has not caught up to is refused before it is
/// published.
#[test]
fn a_checkpoint_beyond_materialization_is_never_published() {
    let dir = tempfile::tempdir().expect("tempdir");
    let images = LocalCheckpointStore::open(dir.path()).expect("store");
    let mut node = Node::new();
    node.protocol(b"promise", b"ballot-7");

    let mut pointer = node.write_image(&images);
    // A pointer claiming a sequence the projection has not applied
    // represents obligations its image does not contain.
    pointer.represented = LocalJournalSeq::new(pointer.represented.get() + 10).unwrap();
    assert!(matches!(
        node.store.publish_checkpoint(DOMAIN, &pointer),
        Err(JournaledError::Frontier(_))
    ));
    // Nothing moved, and nothing was retired.
    assert_eq!(
        node.store.frontiers(DOMAIN).unwrap().checkpoint(),
        LocalJournalSeq::ZERO
    );
    assert_eq!(node.store.recovery_baseline(DOMAIN).unwrap(), None);
}

/// A pointer of another origin is not this node's baseline.
#[test]
fn a_pointer_of_another_origin_publishes_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let images = LocalCheckpointStore::open(dir.path()).expect("store");
    let mut node = Node::new();
    node.protocol(b"promise", b"ballot-7");

    let mut pointer = node.write_image(&images);
    pointer.origin.replica = ReplicaId([0x99; 16]);
    assert!(matches!(
        node.store.publish_checkpoint(DOMAIN, &pointer),
        Err(JournaledError::Quarantined(_))
    ));
    assert_eq!(node.store.recovery_baseline(DOMAIN).unwrap(), None);
}

/// A node that never published a checkpoint has no baseline, and that is
/// not an empty store.
#[test]
fn no_published_pointer_is_not_a_fresh_start() {
    let mut node = Node::new();
    node.protocol(b"promise", b"ballot-7");
    assert_eq!(node.store.recovery_baseline(DOMAIN).unwrap(), None);
    // The whole journal is still the redo, and the projection still has
    // the obligation.
    let gated = node.store.reader(DOMAIN).unwrap().snapshot().unwrap();
    assert!(
        gated
            .view()
            .get(Collection::ProtocolV1.id(), b"promise")
            .unwrap()
            .is_some()
    );
    let meta = DurableMeta::read(gated.view()).unwrap();
    assert_ne!(meta.stamp.last_batch_digest(), Digest32([0; 32]));
}

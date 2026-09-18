//! Installing a shared checkpoint into an inactive generation: what the
//! install refuses, what it writes, and what a crash at each install or
//! pointer step leaves behind.
//!
//! The donor is exported once (task-49) and installed into learners that
//! differ from it in identity and history. The invariant every crash test
//! asserts is the same one: whatever state survives is either a complete,
//! selected generation whose re-export reproduces the donor's root, or a
//! state that fails closed; never a silently truncated store, never the
//! donor's identity, never an obligation the learner did not make itself.

use coord_checkpoint::export::{CheckpointOrigin, ExportLimits, export_shared};
use coord_checkpoint::install::{
    ChunkSet, InstallError, InstallLimits, InstallRequirements, install_shared, installed_baseline,
};
use coord_checkpoint::manifest::{SharedCheckpointV1, SharedManifestV1};
use coord_checkpoint::verify::VerifyError;
use coord_redb_faultkit::{FaultBackend, FaultPlan, Tail};
use coord_state::plan::KvEventKind;
use coord_state::view::KvEntry;
use coord_storage::codecs::{
    self, EventRecordV1, ExecutedRecordV1, HistoryRecordV1, RetryFloorV1, RetryRecordV1,
};
use coord_storage::lowering::{DurableMeta, ExecutionFrontier};
use coord_storage_redb::lifecycle::{ActivateStep, InactiveGeneration};
use coord_storage_redb::{Generation, OpenError, OpenOptions, RedbEngine, StoreIdentity};
use coord_store_api::engine::{LocalEngine, OrderedRead, ScanRequest, SnapshotSource, WriteTxn};
use coord_store_api::envelope::AppliedStamp;
use coord_store_api::registry::{Collection, meta_fields};
use coord_store_api::seq::StoreSeq;
use coord_store_testkit::model::{CommitScript, ModelEngine};
use coord_types::identity::{CommandId, Digest32, RetryKey};
use coord_types::ids::*;

const CLUSTER: ClusterId = ClusterId([1; 16]);
const DOMAIN: DomainId = DomainId([2; 16]);
const NS: NamespaceId = NamespaceId([5; 16]);
const SESSION: SessionId = SessionId([3; 16]);
const CLIENT: ClientInstanceId = ClientInstanceId([4; 16]);
const DONOR: ReplicaId = ReplicaId([0xd0; 16]);
const LEARNER: ReplicaId = ReplicaId([0x1e; 16]);
const CACHE: usize = 4 << 20;
/// Configuration epoch of the donor's boundary.
const EPOCH: u64 = 7;

fn rev(n: u64) -> KvRevision {
    KvRevision::new(n).unwrap()
}

fn pos(n: u64) -> ExecutionPosition {
    ExecutionPosition::new(n).unwrap()
}

fn epoch(n: u64) -> ConfigurationEpoch {
    ConfigurationEpoch::new(n).unwrap()
}

fn command(n: u8) -> CommandId {
    CommandId(Digest32([n; 32]))
}

fn incarnation(n: u64) -> ReplicaIncarnation {
    ReplicaIncarnation::new(n).unwrap()
}

fn entry(value: &[u8], create: u64, modified: u64, version: u64) -> KvEntry {
    KvEntry {
        value: value.to_vec(),
        create_revision: rev(create),
        mod_revision: rev(modified),
        version,
        lease: None,
        lease_generation: None,
    }
}

fn identity(replica: ReplicaId, incarnation_number: u64) -> StoreIdentity {
    StoreIdentity {
        cluster_id: CLUSTER,
        domain_id: DOMAIN,
        replica_id: replica,
        incarnation: incarnation(incarnation_number),
    }
}

fn options() -> OpenOptions {
    OpenOptions { cache_bytes: CACHE }
}

fn origin() -> CheckpointOrigin {
    CheckpointOrigin {
        cluster: CLUSTER,
        domain: DOMAIN,
    }
}

fn requirements() -> InstallRequirements {
    InstallRequirements {
        cluster: CLUSTER,
        domain: DOMAIN,
        minimum_configuration: epoch(EPOCH),
    }
}

/// Chunks small enough that a realistic donor spans several of them.
fn export_limits() -> ExportLimits {
    ExportLimits {
        chunk_target_bytes: 2048,
        ..ExportLimits::default()
    }
}

/// Small batches so an install of this donor takes many durable commits and
/// a crash has many places to land.
fn install_limits() -> InstallLimits {
    InstallLimits {
        rows_per_commit: 4,
        bytes_per_commit: 4096,
    }
}

type Row = (Collection, Vec<u8>, Vec<u8>);

/// The donor's common state at the boundary: execution position 4, KV
/// revision 5, retention floor 3, lease authority 2, configuration epoch 7.
fn common_rows() -> Vec<Row> {
    let mut rows: Vec<Row> = Vec::new();
    let mut put = |c: Collection, k: Vec<u8>, v: Vec<u8>| rows.push((c, k, v));
    put(
        Collection::MetaV1,
        meta_fields::KV_REVISION.to_vec(),
        codecs::encode_counter(5).unwrap(),
    );
    put(
        Collection::MetaV1,
        meta_fields::RETENTION_FLOOR.to_vec(),
        codecs::encode_counter(3).unwrap(),
    );
    put(
        Collection::MetaV1,
        meta_fields::LEASE_AUTHORITY.to_vec(),
        codecs::encode_counter(2).unwrap(),
    );
    put(
        Collection::ConfigV1,
        b"epoch".to_vec(),
        b"configuration-7".to_vec(),
    );
    for (n, position, revision) in [(1u8, 1u64, 1u64), (2, 2, 3), (3, 3, 4), (4, 4, 5)] {
        put(
            Collection::PayloadV1,
            command(n).as_bytes().to_vec(),
            format!("payload-{n}").into_bytes(),
        );
        put(
            Collection::ExecutedV1,
            codecs::executed_key(&command(n)),
            codecs::encode_executed(&ExecutedRecordV1 {
                position: pos(position),
                revision: Some(rev(revision)),
                result_digest: Digest32([n; 32]),
            })
            .unwrap(),
        );
    }
    put(
        Collection::KvCurrentV1,
        codecs::current_key(&NS, b"a"),
        codecs::encode_current(&entry(b"a3", 1, 3, 3)).unwrap(),
    );
    put(
        Collection::KvCurrentV1,
        codecs::current_key(&NS, b"b"),
        codecs::encode_current(&entry(b"b5", 4, 5, 2)).unwrap(),
    );
    let history = |v: &[u8], c: u64, m: u64, ver: u64| {
        codecs::encode_history(&HistoryRecordV1 {
            entry: Some(entry(v, c, m, ver)),
        })
        .unwrap()
    };
    for (key, revision, version) in [
        (&b"a"[..], 3u64, 3u64),
        (&b"b"[..], 4, 1),
        (&b"b"[..], 5, 2),
    ] {
        put(
            Collection::KvHistoryV1,
            codecs::history_key(&NS, key, rev(revision)),
            history(key, 1, revision, version),
        );
    }
    let event = |k: &[u8], e: KvEntry| {
        codecs::encode_event(&EventRecordV1 {
            kind: KvEventKind::Put,
            namespace: NS,
            key: k.to_vec(),
            entry: Some(e),
            prev: None,
        })
        .unwrap()
    };
    for (revision, key) in [(3u64, &b"a"[..]), (4, &b"b"[..]), (5, &b"b"[..])] {
        put(
            Collection::EventsV1,
            codecs::event_key(rev(revision), 0),
            event(key, entry(key, 1, revision, 1)),
        );
    }
    let retry = RetryKey {
        cluster_id: CLUSTER,
        domain_id: DOMAIN,
        session_id: SESSION,
        client_instance_id: CLIENT,
        request_sequence: RequestSequence::new(1).unwrap(),
    };
    put(
        Collection::RetryV1,
        codecs::retry_key(&retry),
        codecs::encode_retry(&RetryRecordV1 {
            command_id: command(1),
            position: pos(1),
            revision: Some(rev(1)),
            response: b"ok".to_vec(),
            result_digest: Digest32([1; 32]),
        })
        .unwrap(),
    );
    put(
        Collection::RetryFloorV1,
        codecs::retry_floor_key(&SESSION, &CLIENT),
        codecs::encode_retry_floor(&RetryFloorV1 {
            floor: RequestSequence::ZERO,
            width: 16,
        })
        .unwrap(),
    );
    let lease = LeaseId([7; 16]);
    put(
        Collection::LeaseV1,
        codecs::lease_row_key(&lease),
        b"lease-record".to_vec(),
    );
    put(
        Collection::LeaseKeysV1,
        codecs::lease_key(&lease, &NS, b"a"),
        b"lease-key".to_vec(),
    );
    put(
        Collection::SessionV1,
        codecs::session_key(&SESSION),
        b"session-record".to_vec(),
    );
    put(Collection::PolicyV1, b"rule".to_vec(), b"policy".to_vec());
    put(
        Collection::AuthGrantV1,
        codecs::grant_key(&Digest32([9; 32])),
        b"grant".to_vec(),
    );
    // Bulk rows so the artifact spans several chunks and the install takes
    // many bounded commits.
    for i in 0..12u32 {
        put(
            Collection::KvCurrentV1,
            codecs::current_key(&NS, format!("bulk-{i:04}").as_bytes()),
            codecs::encode_current(&entry(&vec![i as u8; 512], 1, 3, 1)).unwrap(),
        );
    }
    rows
}

/// The donor's own private rows: identity, an obligation, a local checkpoint
/// pointer and a journal stamp. None of it may reach a learner.
fn donor_private_rows() -> Vec<Row> {
    vec![
        (
            Collection::MetaV1,
            meta_fields::CLUSTER_ID.to_vec(),
            CLUSTER.as_bytes().to_vec(),
        ),
        (
            Collection::MetaV1,
            meta_fields::DOMAIN_ID.to_vec(),
            DOMAIN.as_bytes().to_vec(),
        ),
        (
            Collection::MetaV1,
            meta_fields::REPLICA_ID.to_vec(),
            DONOR.as_bytes().to_vec(),
        ),
        (
            Collection::ProtocolV1,
            vec![0, 0, 0, 0, 0, 0, 0, 7, 0xff],
            b"donor-promise".to_vec(),
        ),
        (
            Collection::CheckpointV1,
            b"donor-local".to_vec(),
            b"donor-pointer".to_vec(),
        ),
    ]
}

/// A donor engine holding the common state plus its own private rows.
fn donor_engine() -> ModelEngine {
    let mut engine = ModelEngine::new();
    let mut tx = engine.begin_write().unwrap();
    for (c, k, v) in common_rows().into_iter().chain(donor_private_rows()) {
        tx.put(c.id(), &k, &v).unwrap();
    }
    let seq = LocalJournalSeq::new(41).unwrap();
    DurableMeta {
        stamp: AppliedStamp {
            store_seq: StoreSeq::from_journal(seq),
            journal_seq: seq,
            last_batch_digest: Digest32([0xab; 32]),
        },
        frontier: ExecutionFrontier {
            configuration: epoch(EPOCH),
            execution_position: pos(4),
        },
    }
    .write(&mut tx)
    .unwrap();
    tx.commit_durable().unwrap();
    engine
}

fn donor_checkpoint() -> SharedCheckpointV1 {
    let engine = donor_engine();
    let view = engine.reader().snapshot().unwrap();
    export_shared(&view, origin(), &export_limits()).unwrap()
}

fn encoded_chunks(checkpoint: &SharedCheckpointV1) -> Vec<Vec<u8>> {
    checkpoint
        .chunks
        .iter()
        .map(|c| c.encode().unwrap())
        .collect()
}

/// A chunk set holding every chunk of `checkpoint`.
fn chunk_set(checkpoint: &SharedCheckpointV1) -> ChunkSet {
    let mut set = ChunkSet::for_manifest(&checkpoint.manifest).unwrap();
    for bytes in encoded_chunks(checkpoint) {
        set.accept(&checkpoint.manifest, bytes).unwrap();
    }
    assert!(set.missing().is_empty());
    set
}

/// Write the identity record a created generation carries, for engines built
/// outside the generation lifecycle (model and fault-backed engines).
fn seed_identity<E: LocalEngine>(engine: &mut E, replica: ReplicaId) {
    let mut tx = engine.begin_write().unwrap();
    let meta = Collection::MetaV1.id();
    tx.put(meta, meta_fields::CLUSTER_ID, CLUSTER.as_bytes())
        .unwrap();
    tx.put(meta, meta_fields::DOMAIN_ID, DOMAIN.as_bytes())
        .unwrap();
    tx.put(meta, meta_fields::REPLICA_ID, replica.as_bytes())
        .unwrap();
    tx.commit_durable().unwrap();
}

/// Re-export the common state of an engine, with the donor's export limits.
fn reexport<E: LocalEngine>(engine: &E) -> SharedCheckpointV1 {
    let view = engine.reader().snapshot().unwrap();
    export_shared(&view, origin(), &export_limits()).unwrap()
}

/// Neither an open generation nor a staging is `Debug`; these unwrap the
/// failure of one without printing the success value.
fn stage_error(result: Result<InactiveGeneration, OpenError>) -> OpenError {
    match result {
        Ok(_) => panic!("staging should have been refused"),
        Err(e) => e,
    }
}

fn open_error(result: Result<Generation, OpenError>) -> OpenError {
    match result {
        Ok(_) => panic!("open should have failed closed"),
        Err(e) => e,
    }
}

fn current_pointer(root: &std::path::Path) -> String {
    String::from_utf8(std::fs::read(root.join("CURRENT")).unwrap()).unwrap()
}

/// A learner root with generation 1 created and closed.
fn learner_root(base: &std::path::Path) -> std::path::PathBuf {
    let root = base.join("learner");
    let generation = Generation::create(&root, identity(LEARNER, 3), options()).unwrap();
    drop(generation);
    root
}

// ---------------------------------------------------------------------------
// The install itself
// ---------------------------------------------------------------------------

#[test]
fn verified_chunks_install_into_an_inactive_generation_and_the_pointer_selects_it() {
    let donor = donor_checkpoint();
    assert!(
        donor.chunks.len() > 2,
        "the donor should span several chunks"
    );
    let dir = tempfile::tempdir().unwrap();
    let root = learner_root(dir.path());

    let mut staged = InactiveGeneration::stage(&root, identity(LEARNER, 3), options()).unwrap();
    assert_eq!(staged.generation(), 2);
    assert_eq!(staged.previous().unwrap().generation, 1);
    let installed = install_shared(
        staged.engine(),
        &donor.manifest,
        chunk_set(&donor),
        &requirements(),
        &install_limits(),
    )
    .unwrap();
    assert_eq!(installed.receipt.root, donor.manifest.root);
    assert_eq!(installed.receipt.boundary, donor.manifest.boundary);
    assert_eq!(installed.receipt.configuration, epoch(EPOCH));
    assert!(installed.commits > 1, "the install commits in batches");
    // Until activation the old generation is still the selected one.
    assert_eq!(current_pointer(&root), "gen-000001");

    let mut generation = staged.activate().unwrap();
    assert_eq!(current_pointer(&root), "gen-000002");
    assert_eq!(generation.manifest().generation, 2);
    assert_eq!(generation.manifest().replica_id, LEARNER);

    // The learner now holds exactly the donor's common state: re-exporting it
    // reproduces the donor's root, chunk for chunk.
    let again = reexport(generation.engine());
    assert_eq!(again.manifest.root, donor.manifest.root);
    assert_eq!(again.manifest, donor.manifest);
    assert_eq!(again.chunks, donor.chunks);

    let view = generation.engine().reader().snapshot().unwrap();
    let receipt = installed_baseline(&view).unwrap().unwrap();
    assert_eq!(receipt, installed.receipt);
    // The boundary is reconciled: catch-up resumes at the donor's position.
    let meta = DurableMeta::read(&view).unwrap();
    assert_eq!(meta.frontier.execution_position, pos(4));
    assert_eq!(meta.frontier.configuration, epoch(EPOCH));
    assert_eq!(codecs::read_kv_revision(&view).unwrap(), rev(5));
    assert_eq!(codecs::read_retention_floor(&view).unwrap(), rev(3));
    assert_eq!(
        codecs::read_lease_authority(&view).unwrap(),
        LeaseAuthorityEpoch::new(2).unwrap()
    );
    // The applied stamp is the learner's own: no donor journal position.
    assert_eq!(meta.stamp, DurableMeta::initial().stamp);
    drop(view);
    drop(generation);

    // The selected generation opens as the learner's own, and only as that.
    let mut reopened = Generation::open_existing(&root, identity(LEARNER, 3), options()).unwrap();
    assert_eq!(reopened.manifest().generation, 2);
    let view = reopened.engine().reader().snapshot().unwrap();
    assert_eq!(
        installed_baseline(&view).unwrap().unwrap().root,
        donor.manifest.root
    );
    drop(view);
    assert!(
        Generation::open_existing(&root, identity(DONOR, 3), options()).is_err(),
        "the donor's identity opens nothing"
    );
}

#[test]
fn the_learner_inherits_no_identity_obligation_or_stamp_from_the_donor() {
    let donor = donor_checkpoint();
    let dir = tempfile::tempdir().unwrap();
    let root = learner_root(dir.path());
    let mut staged = InactiveGeneration::stage(&root, identity(LEARNER, 3), options()).unwrap();
    install_shared(
        staged.engine(),
        &donor.manifest,
        chunk_set(&donor),
        &requirements(),
        &install_limits(),
    )
    .unwrap();
    let mut generation = staged.activate().unwrap();
    let view = generation.engine().reader().snapshot().unwrap();
    let meta = Collection::MetaV1.id();

    // Identity: the learner's own, never the donor's.
    let replica = view.get(meta, meta_fields::REPLICA_ID).unwrap().unwrap();
    assert_eq!(replica.as_slice(), LEARNER.as_bytes());
    assert_ne!(replica.as_slice(), DONOR.as_bytes());
    let stored_incarnation = view.get(meta, meta_fields::INCARNATION).unwrap().unwrap();
    assert_eq!(stored_incarnation.as_slice(), &incarnation(3).to_be_bytes());

    // Obligations: none. An installed baseline never lets a learner vote from
    // promises it did not make.
    let protocol = view
        .scan_page(Collection::ProtocolV1.id(), &ScanRequest::all(16, 1 << 20))
        .unwrap();
    assert!(protocol.rows.is_empty(), "no promise or vote was installed");

    // The donor's private checkpoint row did not travel either.
    assert!(
        view.get(Collection::CheckpointV1.id(), b"donor-local")
            .unwrap()
            .is_none()
    );
    drop(view);
    drop(generation);

    // Nor can a staging pretend to be another replica of the same root.
    let err = stage_error(InactiveGeneration::stage(
        &root,
        identity(DONOR, 3),
        options(),
    ));
    assert!(
        matches!(err, OpenError::RootIdentityMismatch("replica_id")),
        "{err}"
    );
}

#[test]
fn a_missing_or_corrupt_chunk_blocks_the_install() {
    let donor = donor_checkpoint();
    let chunks = encoded_chunks(&donor);
    let manifest = &donor.manifest;

    // Missing: the set refuses to produce a sequence, and the install stops.
    let mut set = ChunkSet::for_manifest(manifest).unwrap();
    for (i, bytes) in chunks.iter().enumerate() {
        if i == 1 {
            continue;
        }
        set.accept(manifest, bytes.clone()).unwrap();
    }
    assert_eq!(set.missing(), vec![1]);
    let dir = tempfile::tempdir().unwrap();
    let root = learner_root(dir.path());
    let mut staged = InactiveGeneration::stage(&root, identity(LEARNER, 3), options()).unwrap();
    let err = install_shared(
        staged.engine(),
        manifest,
        set,
        &requirements(),
        &install_limits(),
    )
    .unwrap_err();
    assert_eq!(err, InstallError::MissingChunk { ordinal: 1 });
    // Nothing was written: the staged generation is still empty, and the old
    // generation is still the selected one.
    let view = staged.engine().reader().snapshot().unwrap();
    assert!(installed_baseline(&view).unwrap().is_none());
    assert!(
        view.scan_page(Collection::KvCurrentV1.id(), &ScanRequest::all(1, 4096),)
            .unwrap()
            .rows
            .is_empty()
    );
    drop(view);
    assert_eq!(current_pointer(&root), "gen-000001");
    staged.abandon().unwrap();

    // Corrupt: a flipped byte, a truncation and a duplicate are all rejected
    // as they arrive, before any of them can reach the store.
    let mut set = ChunkSet::for_manifest(manifest).unwrap();
    let mut flipped = chunks[0].clone();
    let last = flipped.len() - 1;
    flipped[last] ^= 0x01;
    assert!(matches!(
        set.accept(manifest, flipped).unwrap_err(),
        InstallError::CorruptChunk { .. }
    ));
    let mut truncated = chunks[1].clone();
    truncated.truncate(truncated.len() / 2);
    assert_eq!(
        set.accept(manifest, truncated).unwrap_err(),
        InstallError::MalformedChunk,
        "a chunk that does not decode names no ordinal"
    );
    set.accept(manifest, chunks[0].clone()).unwrap();
    assert_eq!(
        set.accept(manifest, chunks[0].clone()).unwrap_err(),
        InstallError::UnexpectedChunk { ordinal: 0 }
    );

    // A chunk that is whole but belongs to no descriptor of this manifest.
    let other = donor_checkpoint();
    let mut lone = ChunkSet::for_manifest(&SharedManifestV1 {
        chunks: manifest.chunks[..1].to_vec(),
        ..manifest.clone()
    })
    .unwrap();
    assert_eq!(
        lone.accept(
            &SharedManifestV1 {
                chunks: manifest.chunks[..1].to_vec(),
                ..manifest.clone()
            },
            encoded_chunks(&other)[1].clone(),
        )
        .unwrap_err(),
        InstallError::UnexpectedChunk { ordinal: 1 }
    );
}

#[test]
fn a_tampered_manifest_blocks_the_install_even_with_every_chunk_present() {
    let donor = donor_checkpoint();
    let dir = tempfile::tempdir().unwrap();
    let root = learner_root(dir.path());
    let mut staged = InactiveGeneration::stage(&root, identity(LEARNER, 3), options()).unwrap();

    // A boundary raised without rehashing: the root no longer matches.
    let mut tampered = donor.manifest.clone();
    tampered.boundary.kv_revision = rev(99);
    let mut set = ChunkSet::for_manifest(&tampered).unwrap();
    for bytes in encoded_chunks(&donor) {
        set.accept(&tampered, bytes).unwrap();
    }
    let err = install_shared(
        staged.engine(),
        &tampered,
        set,
        &requirements(),
        &install_limits(),
    )
    .unwrap_err();
    assert_eq!(err, InstallError::Verify(VerifyError::RootMismatch));

    // A root recomputed over a dropped collection summary: the schema check
    // catches what the digest no longer can.
    let mut dropped = donor.manifest.clone();
    dropped.collections.pop();
    dropped.root = dropped.compute_root();
    let mut set = ChunkSet::for_manifest(&dropped).unwrap();
    for bytes in encoded_chunks(&donor) {
        set.accept(&dropped, bytes).unwrap();
    }
    let err = install_shared(
        staged.engine(),
        &dropped,
        set,
        &requirements(),
        &install_limits(),
    )
    .unwrap_err();
    assert_eq!(err, InstallError::Verify(VerifyError::CollectionsMismatch));
    let view = staged.engine().reader().snapshot().unwrap();
    assert!(installed_baseline(&view).unwrap().is_none());
}

#[test]
fn a_foreign_or_stale_artifact_is_refused() {
    let donor = donor_checkpoint();
    let dir = tempfile::tempdir().unwrap();
    let root = learner_root(dir.path());
    let mut staged = InactiveGeneration::stage(&root, identity(LEARNER, 3), options()).unwrap();

    // Another cluster's artifact, required by a node of this one.
    let foreign = {
        let mut manifest = donor.manifest.clone();
        manifest.cluster = ClusterId([0xff; 16]);
        manifest.root = manifest.compute_root();
        manifest
    };
    let mut set = ChunkSet::for_manifest(&foreign).unwrap();
    for bytes in encoded_chunks(&donor) {
        set.accept(&foreign, bytes).unwrap();
    }
    assert_eq!(
        install_shared(
            staged.engine(),
            &foreign,
            set,
            &requirements(),
            &install_limits()
        )
        .unwrap_err(),
        InstallError::OriginMismatch { field: "cluster" }
    );

    // An artifact from below the epoch the node was admitted under.
    let stale = InstallRequirements {
        minimum_configuration: epoch(EPOCH + 1),
        ..requirements()
    };
    assert_eq!(
        install_shared(
            staged.engine(),
            &donor.manifest,
            chunk_set(&donor),
            &stale,
            &install_limits()
        )
        .unwrap_err(),
        InstallError::StaleConfiguration {
            found: epoch(EPOCH),
            minimum: epoch(EPOCH + 1),
        }
    );

    // A target generation of another domain, even when the caller's
    // requirements agree with the artifact.
    let other_root = dir.path().join("other-domain");
    let mut other = Generation::create(
        &other_root,
        StoreIdentity {
            domain_id: DomainId([0x9; 16]),
            ..identity(LEARNER, 1)
        },
        options(),
    )
    .unwrap();
    assert_eq!(
        install_shared(
            other.engine(),
            &donor.manifest,
            chunk_set(&donor),
            &requirements(),
            &install_limits()
        )
        .unwrap_err(),
        InstallError::OriginMismatch { field: "domain" }
    );

    // An engine with no identity record of its own cannot be lent one.
    let mut anonymous = ModelEngine::new();
    assert_eq!(
        install_shared(
            &mut anonymous,
            &donor.manifest,
            chunk_set(&donor),
            &requirements(),
            &install_limits()
        )
        .unwrap_err(),
        InstallError::TargetIdentityMissing { field: "cluster" }
    );
}

#[test]
fn an_install_never_overwrites_state_obligations_or_an_earlier_install() {
    let donor = donor_checkpoint();

    // A store that already holds common state.
    let mut occupied = ModelEngine::new();
    seed_identity(&mut occupied, LEARNER);
    let mut tx = occupied.begin_write().unwrap();
    tx.put(
        Collection::KvCurrentV1.id(),
        &codecs::current_key(&NS, b"mine"),
        b"local",
    )
    .unwrap();
    tx.commit_durable().unwrap();
    assert_eq!(
        install_shared(
            &mut occupied,
            &donor.manifest,
            chunk_set(&donor),
            &requirements(),
            &install_limits()
        )
        .unwrap_err(),
        InstallError::TargetNotEmpty {
            collection: Collection::KvCurrentV1.id().0
        }
    );

    // A store that holds protocol obligations: promises and votes are never
    // replaced or reset by an install.
    let mut voter = ModelEngine::new();
    seed_identity(&mut voter, LEARNER);
    let mut tx = voter.begin_write().unwrap();
    tx.put(Collection::ProtocolV1.id(), b"promise", b"ballot")
        .unwrap();
    tx.commit_durable().unwrap();
    assert_eq!(
        install_shared(
            &mut voter,
            &donor.manifest,
            chunk_set(&donor),
            &requirements(),
            &install_limits()
        )
        .unwrap_err(),
        InstallError::ExistingObligations
    );

    // A store whose boundary rows are already set.
    let mut running = ModelEngine::new();
    seed_identity(&mut running, LEARNER);
    let mut tx = running.begin_write().unwrap();
    tx.put(
        Collection::MetaV1.id(),
        meta_fields::KV_REVISION,
        &codecs::encode_counter(2).unwrap(),
    )
    .unwrap();
    tx.commit_durable().unwrap();
    assert_eq!(
        install_shared(
            &mut running,
            &donor.manifest,
            chunk_set(&donor),
            &requirements(),
            &install_limits()
        )
        .unwrap_err(),
        InstallError::TargetNotEmpty {
            collection: Collection::MetaV1.id().0
        }
    );

    // A store that already carries a receipt: a second install is refused
    // rather than layered on top of the first.
    let mut installed_once = ModelEngine::new();
    seed_identity(&mut installed_once, LEARNER);
    install_shared(
        &mut installed_once,
        &donor.manifest,
        chunk_set(&donor),
        &requirements(),
        &install_limits(),
    )
    .unwrap();
    assert!(matches!(
        install_shared(
            &mut installed_once,
            &donor.manifest,
            chunk_set(&donor),
            &requirements(),
            &install_limits()
        )
        .unwrap_err(),
        InstallError::AlreadyInstalled
    ));
}

#[test]
fn a_root_whose_selected_generation_holds_obligations_is_never_staged_over() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("voter");
    let mut generation = Generation::create(&root, identity(LEARNER, 1), options()).unwrap();
    let mut tx = generation.engine().begin_write().unwrap();
    tx.put(Collection::ProtocolV1.id(), b"promise", b"ballot")
        .unwrap();
    tx.commit_durable().unwrap();
    drop(generation);

    let err = stage_error(InactiveGeneration::stage(
        &root,
        identity(LEARNER, 2),
        options(),
    ));
    assert!(matches!(err, OpenError::ExistingObligations), "{err}");
    // Nothing was created: the root still holds exactly generation 1.
    assert_eq!(current_pointer(&root), "gen-000001");
    assert!(!root.join("gen-000002").exists());

    // A staging may not move the incarnation backwards either.
    let fresh = dir.path().join("fresh");
    let generation = Generation::create(&fresh, identity(LEARNER, 5), options()).unwrap();
    drop(generation);
    let err = stage_error(InactiveGeneration::stage(
        &fresh,
        identity(LEARNER, 4),
        options(),
    ));
    assert!(
        matches!(err, OpenError::RootIdentityMismatch("incarnation")),
        "{err}"
    );
}

// ---------------------------------------------------------------------------
// Crashes during the install
// ---------------------------------------------------------------------------

#[test]
fn an_indeterminate_commit_stops_the_install_and_leaves_no_baseline() {
    let donor = donor_checkpoint();
    for applied in [false, true] {
        let mut engine = ModelEngine::new();
        seed_identity(&mut engine, LEARNER);
        engine.script_commit(CommitScript::Durable);
        engine.script_commit(CommitScript::Indeterminate { applied });
        let err = install_shared(
            &mut engine,
            &donor.manifest,
            chunk_set(&donor),
            &requirements(),
            &install_limits(),
        )
        .unwrap_err();
        assert!(matches!(err, InstallError::Commit(_)), "{err}");
        // Whatever reached the disk, no install completed.
        engine.crash_and_reopen();
        let view = engine.reader().snapshot().unwrap();
        assert!(installed_baseline(&view).unwrap().is_none());
        drop(view);
        // And the half-filled store cannot be installed into again: only a
        // fresh generation is a legal target.
        assert!(matches!(
            install_shared(
                &mut engine,
                &donor.manifest,
                chunk_set(&donor),
                &requirements(),
                &install_limits()
            )
            .unwrap_err(),
            InstallError::TargetNotEmpty { .. }
        ));
    }
}

#[test]
fn a_crash_at_every_write_of_the_install_leaves_a_complete_baseline_or_none() {
    let donor = donor_checkpoint();
    // Count the writes one honest install performs.
    let (backend, shared) = FaultBackend::new(Vec::new(), FaultPlan::default());
    let mut engine = RedbEngine::create_on_backend(backend, CACHE).unwrap();
    seed_identity(&mut engine, LEARNER);
    let setup = shared.ops();
    install_shared(
        &mut engine,
        &donor.manifest,
        chunk_set(&donor),
        &requirements(),
        &install_limits(),
    )
    .unwrap();
    let total = shared.ops() - setup;
    assert!(total > 8, "the install should span many writes: {total}");
    drop(engine);

    let mut complete = 0;
    let mut absent = 0;
    let mut closed = 0;
    for k in 1..=total {
        let tail = Tail::Seeded(k);
        let (backend, shared) = FaultBackend::new(Vec::new(), FaultPlan::default());
        let mut engine = RedbEngine::create_on_backend(backend, CACHE).unwrap();
        seed_identity(&mut engine, LEARNER);
        let setup = shared.ops();
        shared.set_plan(FaultPlan {
            crash_after: Some(setup + k),
            tail,
            ..FaultPlan::default()
        });
        let outcome = install_shared(
            &mut engine,
            &donor.manifest,
            chunk_set(&donor),
            &requirements(),
            &install_limits(),
        );
        // A crash after the install's last write still lets it report
        // success; what that success then promises is checked below.
        let succeeded = outcome.is_ok();
        let image = shared.crash_image(tail);
        drop(engine);

        // The next process over the crash image.
        let (backend, _) = FaultBackend::new(image, FaultPlan::default());
        let Ok(engine) = RedbEngine::from_backend(backend, CACHE) else {
            // A torn image the engine refuses is a closed failure, never a
            // silently truncated store.
            assert!(
                !succeeded,
                "k={k}: a successful install left an unopenable store"
            );
            closed += 1;
            continue;
        };
        let Ok(view) = engine.reader().snapshot() else {
            assert!(
                !succeeded,
                "k={k}: a successful install left an unreadable store"
            );
            closed += 1;
            continue;
        };
        match installed_baseline(&view) {
            Ok(None) => {
                // No receipt: the install never completed, so nothing may be
                // selected from it. A reported success must never land here.
                assert!(!succeeded, "k={k}: a successful install left no receipt");
                absent += 1;
            }
            Ok(Some(receipt)) => {
                // A receipt exists only after every row and the boundary were
                // durable: the state must reproduce the donor exactly.
                assert_eq!(receipt.root, donor.manifest.root, "k={k}");
                drop(view);
                assert_eq!(
                    reexport(&engine).manifest.root,
                    donor.manifest.root,
                    "k={k}: a receipt promises the whole checkpoint"
                );
                complete += 1;
            }
            Err(_) => {
                assert!(
                    !succeeded,
                    "k={k}: a successful install left a corrupt receipt"
                );
                closed += 1;
            }
        }
    }
    assert!(absent > 0, "some crashes land before the receipt");
    assert!(complete > 0, "some crashes land after it: {complete}");
    // A crash may also leave an image the engine refuses to open; that is a
    // closed failure too, never a silently truncated store.
    let _ = closed;
}

#[test]
fn a_failed_write_during_the_install_fails_closed() {
    let donor = donor_checkpoint();
    let (backend, shared) = FaultBackend::new(Vec::new(), FaultPlan::default());
    let mut engine = RedbEngine::create_on_backend(backend, CACHE).unwrap();
    seed_identity(&mut engine, LEARNER);
    let setup = shared.ops();
    shared.set_plan(FaultPlan {
        fail_write_at: Some(setup + 3),
        tail: Tail::None,
        ..FaultPlan::default()
    });
    let err = install_shared(
        &mut engine,
        &donor.manifest,
        chunk_set(&donor),
        &requirements(),
        &install_limits(),
    )
    .unwrap_err();
    assert!(
        matches!(err, InstallError::Commit(_) | InstallError::Engine(_)),
        "{err}"
    );
    if let Ok(view) = engine.reader().snapshot() {
        assert!(installed_baseline(&view).unwrap().is_none());
    }
}

// ---------------------------------------------------------------------------
// Crashes around the active pointer
// ---------------------------------------------------------------------------

#[test]
fn a_crash_at_each_activation_step_leaves_a_valid_selection() {
    let donor = donor_checkpoint();
    for step in [
        ActivateStep::SyncData,
        ActivateStep::WriteManifest,
        ActivateStep::WritePointer,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let root = learner_root(dir.path());
        let mut staged = InactiveGeneration::stage(&root, identity(LEARNER, 3), options()).unwrap();
        install_shared(
            staged.engine(),
            &donor.manifest,
            chunk_set(&donor),
            &requirements(),
            &install_limits(),
        )
        .unwrap();
        staged.activate_interrupted(step).unwrap();

        // The next process opens whatever the pointer selects.
        let mut generation =
            Generation::open_existing(&root, identity(LEARNER, 3), options()).unwrap();
        let view = generation.engine().reader().snapshot().unwrap();
        let baseline = installed_baseline(&view).unwrap();
        match step {
            ActivateStep::WritePointer => {
                // Selected: the complete installed generation.
                assert_eq!(generation.manifest().generation, 2);
                assert_eq!(baseline.unwrap().root, donor.manifest.root);
                drop(view);
                assert_eq!(
                    reexport(generation.engine()).manifest.root,
                    donor.manifest.root
                );
            }
            _ => {
                // Not selected: the previous generation, untouched and empty.
                assert_eq!(generation.manifest().generation, 1);
                assert!(baseline.is_none());
                drop(view);
                // The staging is still on disk but nothing points at it, and
                // a retry stages the next number and succeeds.
                assert!(root.join("gen-000002").exists());
                let mut staged = generation
                    .stage_next(identity(LEARNER, 3), options())
                    .unwrap();
                assert_eq!(staged.generation(), 3);
                install_shared(
                    staged.engine(),
                    &donor.manifest,
                    chunk_set(&donor),
                    &requirements(),
                    &install_limits(),
                )
                .unwrap();
                let generation = staged.activate().unwrap();
                assert_eq!(current_pointer(&root), "gen-000003");
                // The abandoned staging is reclaimed, the selection is not.
                assert_eq!(generation.prune_unselected().unwrap(), vec![1, 2]);
                assert!(root.join("gen-000003").exists());
            }
        }
    }
}

#[test]
fn an_error_after_selection_never_falls_back_to_the_old_generation() {
    let donor = donor_checkpoint();
    let dir = tempfile::tempdir().unwrap();
    let root = learner_root(dir.path());
    let mut staged = InactiveGeneration::stage(&root, identity(LEARNER, 3), options()).unwrap();
    install_shared(
        staged.engine(),
        &donor.manifest,
        chunk_set(&donor),
        &requirements(),
        &install_limits(),
    )
    .unwrap();
    drop(staged.activate().unwrap());
    assert_eq!(current_pointer(&root), "gen-000002");

    // The previous generation is still on disk and still perfectly readable,
    // but it is no longer selected: damaging the new one fails closed instead
    // of quietly serving the old state.
    assert!(root.join("gen-000001/domain.redb").exists());
    for damage in ["manifest", "database"] {
        let saved = match damage {
            "manifest" => {
                let path = root.join("gen-000002/manifest.v1");
                let mut bytes = std::fs::read(&path).unwrap();
                bytes[0] ^= 0xff;
                let original = std::fs::read(&path).unwrap();
                std::fs::write(&path, &bytes).unwrap();
                (path, original)
            }
            _ => {
                let path = root.join("gen-000002/domain.redb");
                let original = std::fs::read(&path).unwrap();
                std::fs::write(&path, b"not a database").unwrap();
                (path, original)
            }
        };
        let err = open_error(Generation::open_existing(
            &root,
            identity(LEARNER, 3),
            options(),
        ));
        assert!(
            matches!(err, OpenError::Manifest(_) | OpenError::Corrupt(_)),
            "{damage}: {err}"
        );
        assert_eq!(
            current_pointer(&root),
            "gen-000002",
            "{damage}: a failed open never reverts the pointer"
        );
        std::fs::write(&saved.0, &saved.1).unwrap();
    }
    // Restored, the selected generation is the installed one again.
    let mut generation = Generation::open_existing(&root, identity(LEARNER, 3), options()).unwrap();
    assert_eq!(generation.manifest().generation, 2);
    let view = generation.engine().reader().snapshot().unwrap();
    assert_eq!(
        installed_baseline(&view).unwrap().unwrap().root,
        donor.manifest.root
    );
    drop(view);

    // A further catch-up stages generation 3 on top of the selected one.
    let staged = generation
        .stage_next(identity(LEARNER, 3), options())
        .unwrap();
    assert_eq!(staged.generation(), 3);
    assert_eq!(staged.previous().unwrap().generation, 2);
    staged.abandon().unwrap();
    assert_eq!(current_pointer(&root), "gen-000002");
}

#[test]
fn an_abandoned_staging_leaves_the_selection_and_the_root_reusable() {
    let donor = donor_checkpoint();
    let dir = tempfile::tempdir().unwrap();
    let root = learner_root(dir.path());
    let staged = InactiveGeneration::stage(&root, identity(LEARNER, 3), options()).unwrap();
    let directory = staged.directory().to_path_buf();
    staged.abandon().unwrap();
    assert!(!directory.exists());
    assert_eq!(current_pointer(&root), "gen-000001");

    let mut staged = InactiveGeneration::stage(&root, identity(LEARNER, 4), options()).unwrap();
    assert_eq!(staged.generation(), 2);
    install_shared(
        staged.engine(),
        &donor.manifest,
        chunk_set(&donor),
        &requirements(),
        &install_limits(),
    )
    .unwrap();
    let generation = staged.activate().unwrap();
    assert_eq!(generation.manifest().incarnation, incarnation(4));
    drop(generation);
    // The manifest incarnation is the learner's newest, and the old one no
    // longer opens the root.
    assert!(Generation::open_existing(&root, identity(LEARNER, 3), options()).is_err());
    assert!(Generation::open_existing(&root, identity(LEARNER, 4), options()).is_ok());
}

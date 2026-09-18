//! Equal common state hashes equally across nodes that differ in private
//! rows, garbage-collection progress, unexecuted payloads, insertion order
//! and engine; different common state does not; chunks verify and every
//! tampering is caught; a view that moves is refused; the root is frozen.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use coord_checkpoint::export::{CheckpointOrigin, ExportError, ExportLimits, export_shared};
use coord_checkpoint::manifest::ArtifactError;
use coord_checkpoint::manifest::{
    CHUNK_TARGET_BYTES, ChunkDescriptorV1, ChunkV1, MAX_CHUNKS, MAX_MANIFEST_BYTES,
    SHARED_CHECKPOINT_FORMAT_V1, SharedCheckpointV1, SharedManifestV1, kinds,
};
use coord_checkpoint::verify::{VerifyError, verify_shared};
use coord_state::plan::KvEventKind;
use coord_state::policy::{Action, KeyInterval, PolicyRule};
use coord_state::view::KvEntry;
use coord_storage::codecs::{
    self, EventRecordV1, ExecutedRecordV1, HistoryRecordV1, RetryFloorV1, RetryRecordV1,
};
use coord_storage::lowering::{DurableMeta, ExecutionFrontier};
use coord_storage::policy::{bootstrap_session, rule_update};
use coord_storage_redb::{Generation, OpenOptions, StoreIdentity};
use coord_store_api::engine::{
    EngineError, LocalEngine, OrderedRead, RowPage, ScanRequest, SnapshotSource, WriteTxn,
};
use coord_store_api::envelope::AppliedStamp;
use coord_store_api::registry::{Collection, meta_fields};
use coord_store_api::seq::StoreSeq;
use coord_store_testkit::model::{Misbehavior, ModelEngine};
use coord_types::identity::{CommandId, Digest32, RetryKey};
use coord_types::ids::*;
use coord_types::wire_v1::FrameReader;
use serde::{Deserialize, Serialize};

const CLUSTER: ClusterId = ClusterId([1; 16]);
const DOMAIN: DomainId = DomainId([2; 16]);
const NS: NamespaceId = NamespaceId([5; 16]);
const SESSION: SessionId = SessionId([3; 16]);
const CLIENT: ClientInstanceId = ClientInstanceId([4; 16]);
const ALICE: PrincipalId = PrincipalId([0xa; 16]);

fn origin() -> CheckpointOrigin {
    CheckpointOrigin {
        cluster: CLUSTER,
        domain: DOMAIN,
    }
}

fn rev(n: u64) -> KvRevision {
    KvRevision::new(n).unwrap()
}

fn pos(n: u64) -> ExecutionPosition {
    ExecutionPosition::new(n).unwrap()
}

fn command(n: u8) -> CommandId {
    CommandId(Digest32([n; 32]))
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

type Row = (Collection, Vec<u8>, Vec<u8>);

/// How a node's projection differs while its common state is the same.
#[derive(Clone, Copy, Default)]
struct Profile {
    /// Local journal/materialization stamp.
    stamp: u64,
    /// History and events already collected to the floor.
    gc_done: bool,
    /// Rows written in reverse order, with a written-then-deleted row.
    reversed: bool,
    /// A payload of an unexecuted (post-boundary) command present.
    unexecuted_payload: bool,
    /// Node-private rows (identity, promises, checkpoint metadata).
    private_rows: bool,
    /// Extra bulk rows for chunking.
    bulk_rows: usize,
}

/// The common logical state every node shares, plus the profile's
/// private differences. Boundary: execution position 3, KV revision 3,
/// retention floor 2, lease authority 1.
fn rows(profile: Profile) -> Vec<Row> {
    let mut rows: Vec<Row> = Vec::new();
    let put = |rows: &mut Vec<Row>, c: Collection, k: Vec<u8>, v: Vec<u8>| rows.push((c, k, v));
    // Common frontiers (meta_v1 is private, but the boundary is read from it).
    put(
        &mut rows,
        Collection::MetaV1,
        meta_fields::KV_REVISION.to_vec(),
        codecs::encode_counter(3).unwrap(),
    );
    put(
        &mut rows,
        Collection::MetaV1,
        meta_fields::RETENTION_FLOOR.to_vec(),
        codecs::encode_counter(2).unwrap(),
    );
    put(
        &mut rows,
        Collection::MetaV1,
        meta_fields::LEASE_AUTHORITY.to_vec(),
        codecs::encode_counter(1).unwrap(),
    );
    put(
        &mut rows,
        Collection::ConfigV1,
        b"epoch".to_vec(),
        b"config-epoch-1".to_vec(),
    );
    // Three executed commands and their payloads.
    for (n, revision) in [(1u8, 1u64), (2, 2), (3, 3)] {
        put(
            &mut rows,
            Collection::PayloadV1,
            command(n).as_bytes().to_vec(),
            format!("payload-{n}").into_bytes(),
        );
        put(
            &mut rows,
            Collection::ExecutedV1,
            codecs::executed_key(&command(n)),
            codecs::encode_executed(&ExecutedRecordV1 {
                position: pos(u64::from(n)),
                revision: Some(rev(revision)),
                result_digest: Digest32([n; 32]),
            })
            .unwrap(),
        );
    }
    if profile.unexecuted_payload {
        put(
            &mut rows,
            Collection::PayloadV1,
            command(4).as_bytes().to_vec(),
            b"payload-4-unexecuted".to_vec(),
        );
    }
    // KV: a=v1 @1, a=v2 @2, b=v3 @3.
    put(
        &mut rows,
        Collection::KvCurrentV1,
        codecs::current_key(&NS, b"a"),
        codecs::encode_current(&entry(b"v2", 1, 2, 2)).unwrap(),
    );
    put(
        &mut rows,
        Collection::KvCurrentV1,
        codecs::current_key(&NS, b"b"),
        codecs::encode_current(&entry(b"v3", 3, 3, 1)).unwrap(),
    );
    let history = |v: &[u8], c: u64, m: u64, ver: u64| {
        codecs::encode_history(&HistoryRecordV1 {
            entry: Some(entry(v, c, m, ver)),
        })
        .unwrap()
    };
    if !profile.gc_done {
        put(
            &mut rows,
            Collection::KvHistoryV1,
            codecs::history_key(&NS, b"a", rev(1)),
            history(b"v1", 1, 1, 1),
        );
    }
    put(
        &mut rows,
        Collection::KvHistoryV1,
        codecs::history_key(&NS, b"a", rev(2)),
        history(b"v2", 1, 2, 2),
    );
    put(
        &mut rows,
        Collection::KvHistoryV1,
        codecs::history_key(&NS, b"b", rev(3)),
        history(b"v3", 3, 3, 1),
    );
    let event = |k: &[u8], e: Option<KvEntry>, p: Option<KvEntry>| {
        codecs::encode_event(&EventRecordV1 {
            kind: KvEventKind::Put,
            namespace: NS,
            key: k.to_vec(),
            entry: e,
            prev: p,
        })
        .unwrap()
    };
    if !profile.gc_done {
        put(
            &mut rows,
            Collection::EventsV1,
            codecs::event_key(rev(1), 0),
            event(b"a", Some(entry(b"v1", 1, 1, 1)), None),
        );
    }
    put(
        &mut rows,
        Collection::EventsV1,
        codecs::event_key(rev(2), 0),
        event(
            b"a",
            Some(entry(b"v2", 1, 2, 2)),
            Some(entry(b"v1", 1, 1, 1)),
        ),
    );
    put(
        &mut rows,
        Collection::EventsV1,
        codecs::event_key(rev(3), 0),
        event(b"b", Some(entry(b"v3", 3, 3, 1)), None),
    );
    // Retry results, floors, leases, sessions, policy, grants.
    let key = RetryKey {
        cluster_id: CLUSTER,
        domain_id: DOMAIN,
        session_id: SESSION,
        client_instance_id: CLIENT,
        request_sequence: RequestSequence::new(1).unwrap(),
    };
    put(
        &mut rows,
        Collection::RetryV1,
        codecs::retry_key(&key),
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
        &mut rows,
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
        &mut rows,
        Collection::LeaseV1,
        codecs::lease_row_key(&lease),
        b"lease-record".to_vec(),
    );
    put(
        &mut rows,
        Collection::LeaseKeysV1,
        codecs::lease_key(&lease, &NS, b"a"),
        b"lease-key".to_vec(),
    );
    for u in bootstrap_session(&SESSION, ALICE, 16, true).unwrap() {
        let c = Collection::from_id(u.collection).unwrap();
        put(&mut rows, c, u.key, u.value.unwrap());
    }
    let rule = rule_update(
        &PolicyRuleId([8; 16]),
        &PolicyRule {
            principal: ALICE,
            action: Action::Read,
            namespace: NS,
            interval: KeyInterval {
                lower: vec![],
                upper: None,
            },
        },
    )
    .unwrap();
    put(
        &mut rows,
        Collection::PolicyV1,
        rule.key,
        rule.value.unwrap(),
    );
    put(
        &mut rows,
        Collection::AuthGrantV1,
        codecs::grant_key(&Digest32([9; 32])),
        b"grant".to_vec(),
    );
    for i in 0..profile.bulk_rows {
        put(
            &mut rows,
            Collection::KvCurrentV1,
            codecs::current_key(&NS, format!("bulk-{i:06}").as_bytes()),
            codecs::encode_current(&entry(&vec![i as u8; 1024], 3, 3, 1)).unwrap(),
        );
    }
    // Node-private rows never enter the digest.
    if profile.private_rows {
        put(
            &mut rows,
            Collection::MetaV1,
            meta_fields::CLUSTER_ID.to_vec(),
            CLUSTER.as_bytes().to_vec(),
        );
        put(
            &mut rows,
            Collection::MetaV1,
            meta_fields::REPLICA_ID.to_vec(),
            vec![profile.stamp as u8; 16],
        );
        put(
            &mut rows,
            Collection::ProtocolV1,
            vec![0, 0, 0, 0, 0, 0, 0, 1, 0xff],
            b"promise".to_vec(),
        );
        put(
            &mut rows,
            Collection::CheckpointV1,
            b"local".to_vec(),
            b"pointer".to_vec(),
        );
    }
    if profile.reversed {
        rows.reverse();
    }
    rows
}

fn seed<E: LocalEngine>(engine: &mut E, profile: Profile) {
    if profile.reversed {
        // A row that once existed and was deleted: layout noise.
        let mut tx = engine.begin_write().unwrap();
        tx.put(
            Collection::KvCurrentV1.id(),
            &codecs::current_key(&NS, b"zz"),
            b"gone",
        )
        .unwrap();
        tx.commit_durable().unwrap();
        let mut tx = engine.begin_write().unwrap();
        tx.delete(
            Collection::KvCurrentV1.id(),
            &codecs::current_key(&NS, b"zz"),
        )
        .unwrap();
        tx.commit_durable().unwrap();
    }
    let mut tx = engine.begin_write().unwrap();
    for (c, k, v) in rows(profile) {
        tx.put(c.id(), &k, &v).unwrap();
    }
    let seq = LocalJournalSeq::new(profile.stamp.max(1)).unwrap();
    DurableMeta {
        stamp: AppliedStamp {
            store_seq: StoreSeq::from_journal(seq),
            journal_seq: seq,
            last_batch_digest: Digest32([profile.stamp as u8; 32]),
        },
        frontier: ExecutionFrontier {
            configuration: ConfigurationEpoch::new(1).unwrap(),
            execution_position: pos(3),
        },
    }
    .write(&mut tx)
    .unwrap();
    tx.commit_durable().unwrap();
}

fn export<E: LocalEngine>(engine: &E, limits: &ExportLimits) -> SharedCheckpointV1 {
    let view = engine.reader().snapshot().unwrap();
    export_shared(&view, origin(), limits).unwrap()
}

fn encoded_chunks(cp: &SharedCheckpointV1) -> Vec<Vec<u8>> {
    cp.chunks.iter().map(|c| c.encode().unwrap()).collect()
}

fn model(profile: Profile) -> ModelEngine {
    let mut engine = ModelEngine::new();
    seed(&mut engine, profile);
    engine
}

#[test]
fn equal_common_state_hashes_equally_across_private_differences_and_engines() {
    let a = model(Profile {
        stamp: 40,
        gc_done: false,
        reversed: false,
        unexecuted_payload: true,
        private_rows: true,
        bulk_rows: 0,
    });
    let b = model(Profile {
        stamp: 97,
        gc_done: true,
        reversed: true,
        unexecuted_payload: false,
        private_rows: false,
        bulk_rows: 0,
    });
    let limits = ExportLimits::default();
    let ca = export(&a, &limits);
    let cb = export(&b, &limits);
    assert_eq!(ca.manifest.root, cb.manifest.root);
    assert_eq!(ca.manifest, cb.manifest);
    assert_eq!(ca.chunks, cb.chunks);
    verify_shared(&ca.manifest, &encoded_chunks(&ca)).unwrap();
    // The same common state on the production engine.
    let dir = tempfile::tempdir().unwrap();
    let mut generation = Generation::create(
        &dir.path().join("store"),
        StoreIdentity {
            cluster_id: CLUSTER,
            domain_id: DOMAIN,
            replica_id: ReplicaId([3; 16]),
            incarnation: ReplicaIncarnation::new(1).unwrap(),
        },
        OpenOptions {
            cache_bytes: 8 * 1024 * 1024,
        },
    )
    .unwrap();
    seed(
        generation.engine(),
        Profile {
            stamp: 5,
            gc_done: true,
            reversed: false,
            unexecuted_payload: true,
            private_rows: false,
            bulk_rows: 0,
        },
    );
    let cr = export(generation.engine(), &limits);
    assert_eq!(cr.manifest.root, ca.manifest.root);
    assert_eq!(cr.chunks, ca.chunks);

    // Required common collections are present, private ones absent.
    let summary = |c: Collection| {
        ca.manifest
            .collections
            .iter()
            .find(|s| s.collection == c.id().0)
            .map(|s| s.rows)
    };
    for c in [
        Collection::RetryV1,
        Collection::RetryFloorV1,
        Collection::PolicyV1,
        Collection::SessionV1,
        Collection::LeaseV1,
        Collection::LeaseKeysV1,
        Collection::KvHistoryV1,
        Collection::EventsV1,
        Collection::PayloadV1,
        Collection::ExecutedV1,
        Collection::AuthGrantV1,
    ] {
        assert!(summary(c).unwrap() > 0, "{c:?}");
    }
    for c in [
        Collection::MetaV1,
        Collection::ProtocolV1,
        Collection::CheckpointV1,
    ] {
        assert_eq!(summary(c), None, "{c:?}");
    }
    // Canonical normalization: one history version at or below the floor
    // per key, no event below the floor, only executed payloads.
    assert_eq!(summary(Collection::KvHistoryV1), Some(2));
    assert_eq!(summary(Collection::EventsV1), Some(2));
    assert_eq!(summary(Collection::PayloadV1), Some(3));
    assert_eq!(ca.manifest.boundary.execution_position, pos(3));
    assert_eq!(ca.manifest.boundary.kv_revision, rev(3));
    assert_eq!(ca.manifest.boundary.retention_floor, rev(2));
    assert_eq!(ca.manifest.boundary.lease_authority.get(), 1);
    assert_eq!(ca.manifest.configuration.get(), 1);
    assert_eq!(ca.manifest.format, SHARED_CHECKPOINT_FORMAT_V1);
    assert_eq!(ca.chunks.len(), 1);
}

#[test]
fn different_common_state_hashes_differently() {
    let base = model(Profile::default());
    let limits = ExportLimits::default();
    let root = export(&base, &limits).manifest.root;
    // A changed current value.
    let mut changed = model(Profile::default());
    let mut tx = changed.begin_write().unwrap();
    tx.put(
        Collection::KvCurrentV1.id(),
        &codecs::current_key(&NS, b"b"),
        &codecs::encode_current(&entry(b"v3-other", 3, 3, 1)).unwrap(),
    )
    .unwrap();
    tx.commit_durable().unwrap();
    assert_ne!(export(&changed, &limits).manifest.root, root);
    // An extra retained retry result.
    let mut more = model(Profile::default());
    let mut tx = more.begin_write().unwrap();
    let key = RetryKey {
        cluster_id: CLUSTER,
        domain_id: DOMAIN,
        session_id: SESSION,
        client_instance_id: CLIENT,
        request_sequence: RequestSequence::new(2).unwrap(),
    };
    tx.put(
        Collection::RetryV1.id(),
        &codecs::retry_key(&key),
        &codecs::encode_retry(&RetryRecordV1 {
            command_id: command(2),
            position: pos(2),
            revision: Some(rev(2)),
            response: b"ok".to_vec(),
            result_digest: Digest32([2; 32]),
        })
        .unwrap(),
    )
    .unwrap();
    tx.commit_durable().unwrap();
    assert_ne!(export(&more, &limits).manifest.root, root);
    // A different policy rule.
    let mut policy = model(Profile::default());
    let mut tx = policy.begin_write().unwrap();
    let rule = rule_update(
        &PolicyRuleId([8; 16]),
        &PolicyRule {
            principal: ALICE,
            action: Action::Write,
            namespace: NS,
            interval: KeyInterval {
                lower: vec![],
                upper: None,
            },
        },
    )
    .unwrap();
    tx.put(rule.collection, &rule.key, &rule.value.unwrap())
        .unwrap();
    tx.commit_durable().unwrap();
    assert_ne!(export(&policy, &limits).manifest.root, root);
    // Another domain's identity in the manifest.
    let view = base.reader().snapshot().unwrap();
    let other = export_shared(
        &view,
        CheckpointOrigin {
            cluster: CLUSTER,
            domain: DomainId([9; 16]),
        },
        &limits,
    )
    .unwrap();
    assert_ne!(other.manifest.root, root);
}

#[test]
fn rows_beyond_the_boundary_are_refused() {
    let limits = ExportLimits::default();
    let mut engine = model(Profile::default());
    let mut tx = engine.begin_write().unwrap();
    tx.put(
        Collection::ExecutedV1.id(),
        &codecs::executed_key(&command(9)),
        &codecs::encode_executed(&ExecutedRecordV1 {
            position: pos(9),
            revision: None,
            result_digest: Digest32([9; 32]),
        })
        .unwrap(),
    )
    .unwrap();
    tx.commit_durable().unwrap();
    let view = engine.reader().snapshot().unwrap();
    assert_eq!(
        export_shared(&view, origin(), &limits).unwrap_err(),
        ExportError::BeyondBoundary {
            collection: Collection::ExecutedV1.id().0
        }
    );
    let mut engine = model(Profile::default());
    let mut tx = engine.begin_write().unwrap();
    tx.put(
        Collection::EventsV1.id(),
        &codecs::event_key(rev(9), 0),
        b"late",
    )
    .unwrap();
    tx.commit_durable().unwrap();
    let view = engine.reader().snapshot().unwrap();
    assert_eq!(
        export_shared(&view, origin(), &limits).unwrap_err(),
        ExportError::BeyondBoundary {
            collection: Collection::EventsV1.id().0
        }
    );
    let mut engine = model(Profile::default());
    let mut tx = engine.begin_write().unwrap();
    tx.put(
        Collection::KvHistoryV1.id(),
        &codecs::history_key(&NS, b"c", rev(9)),
        b"late",
    )
    .unwrap();
    tx.commit_durable().unwrap();
    let view = engine.reader().snapshot().unwrap();
    assert_eq!(
        export_shared(&view, origin(), &limits).unwrap_err(),
        ExportError::BeyondBoundary {
            collection: Collection::KvHistoryV1.id().0
        }
    );
    // A row the exporter decodes must decode.
    let mut engine = model(Profile::default());
    let mut tx = engine.begin_write().unwrap();
    tx.put(
        Collection::ExecutedV1.id(),
        &codecs::executed_key(&command(8)),
        b"garbage",
    )
    .unwrap();
    tx.commit_durable().unwrap();
    let view = engine.reader().snapshot().unwrap();
    assert_eq!(
        export_shared(&view, origin(), &limits).unwrap_err(),
        ExportError::Corrupt {
            collection: Collection::ExecutedV1.id().0
        }
    );
}

#[test]
fn chunks_are_bounded_verified_and_every_tampering_is_caught() {
    let engine = model(Profile {
        bulk_rows: 3000,
        ..Profile::default()
    });
    let limits = ExportLimits::default();
    let cp = export(&engine, &limits);
    assert!(cp.chunks.len() >= 3, "{}", cp.chunks.len());
    let encoded = encoded_chunks(&cp);
    for (d, e) in cp.manifest.chunks.iter().zip(&encoded) {
        assert!(e.len() <= CHUNK_TARGET_BYTES + 1024 + 64, "{}", e.len());
        assert_eq!(d.bytes as usize, e.len());
    }
    assert_eq!(
        verify_shared(&cp.manifest, &encoded).unwrap(),
        cp.manifest.root
    );
    // Smaller chunks: same root? No: chunk descriptors are part of the
    // root, so the chunking is part of the artifact identity; rows are
    // identical.
    let small = export(
        &engine,
        &ExportLimits {
            chunk_target_bytes: 64 * 1024,
            ..limits
        },
    );
    assert!(small.chunks.len() > cp.chunks.len());
    let rows_a: Vec<_> = cp.chunks.iter().flat_map(|c| c.rows.clone()).collect();
    let rows_b: Vec<_> = small.chunks.iter().flat_map(|c| c.rows.clone()).collect();
    assert_eq!(rows_a, rows_b);
    verify_shared(&small.manifest, &encoded_chunks(&small)).unwrap();

    // Tampering.
    let mut flipped = encoded.clone();
    flipped[1][100] ^= 1;
    assert_eq!(
        verify_shared(&cp.manifest, &flipped),
        Err(VerifyError::ChunkDigest { ordinal: 1 })
    );
    let mut missing = encoded.clone();
    missing.pop();
    assert_eq!(
        verify_shared(&cp.manifest, &missing),
        Err(VerifyError::ChunkSequence)
    );
    let mut swapped = encoded.clone();
    swapped.swap(0, 1);
    assert_eq!(
        verify_shared(&cp.manifest, &swapped),
        Err(VerifyError::ChunkDigest { ordinal: 0 })
    );
    let mut root = cp.manifest.clone();
    root.root = Digest32([0; 32]);
    assert_eq!(
        verify_shared(&root, &encoded),
        Err(VerifyError::RootMismatch)
    );
    let mut format = cp.manifest.clone();
    format.format = 2;
    format.root = format.compute_root();
    assert_eq!(
        verify_shared(&format, &encoded),
        Err(VerifyError::UnsupportedFormat { found: 2 })
    );
    let mut counts = cp.manifest.clone();
    counts.collections[6].rows += 1;
    counts.root = counts.compute_root();
    assert!(matches!(
        verify_shared(&counts, &encoded),
        Err(VerifyError::CountMismatch { .. })
    ));
    let mut fewer = cp.manifest.clone();
    fewer.collections.pop();
    fewer.root = fewer.compute_root();
    assert_eq!(
        verify_shared(&fewer, &encoded),
        Err(VerifyError::CollectionsMismatch)
    );
    // Reordered rows inside a chunk, re-signed: order is enforced.
    let mut reordered = cp.clone();
    reordered.chunks[0].rows.swap(0, 1);
    let re = encoded_chunks(&reordered);
    reordered.manifest.chunks[0].digest = ChunkV1::digest_of(&re[0]);
    reordered.manifest.chunks[0].first = (
        reordered.chunks[0].rows[0].collection,
        reordered.chunks[0].rows[0].key.clone(),
    );
    reordered.manifest.root = reordered.manifest.compute_root();
    assert_eq!(
        verify_shared(&reordered.manifest, &re),
        Err(VerifyError::OutOfOrder { ordinal: 0 })
    );
    // A private row smuggled in, re-signed: foreign collection.
    let mut foreign = cp.clone();
    foreign.chunks[0].rows.insert(
        0,
        coord_checkpoint::manifest::RowV1 {
            collection: Collection::MetaV1.id().0,
            key: b"applied_stamp".to_vec(),
            value: b"x".to_vec(),
        },
    );
    let fe = encoded_chunks(&foreign);
    foreign.manifest.chunks[0].digest = ChunkV1::digest_of(&fe[0]);
    foreign.manifest.chunks[0].rows += 1;
    foreign.manifest.chunks[0].bytes = fe[0].len() as u32;
    foreign.manifest.chunks[0].first = (Collection::MetaV1.id().0, b"applied_stamp".to_vec());
    foreign.manifest.root = foreign.manifest.compute_root();
    assert_eq!(
        verify_shared(&foreign.manifest, &fe),
        Err(VerifyError::ForeignCollection {
            collection: Collection::MetaV1.id().0
        })
    );
    // Frames round trip.
    let mut reader = FrameReader::new();
    reader.push(&cp.manifest.frame().unwrap());
    reader.push(&cp.chunks[0].frame().unwrap());
    let f1 = reader.next_frame().unwrap().unwrap();
    let f2 = reader.next_frame().unwrap().unwrap();
    assert_eq!(f1.kind, kinds::MANIFEST);
    assert_eq!(f2.kind, kinds::CHUNK);
    assert_eq!(SharedManifestV1::from_frame(&f1).unwrap(), cp.manifest);
    assert_eq!(ChunkV1::from_frame(&f2).unwrap(), cp.chunks[0]);
    assert!(ChunkV1::from_frame(&f1).is_err());
    let mut trailing = f2.clone();
    trailing.payload.push(0);
    assert!(ChunkV1::from_frame(&trailing).is_err());

    // A manifest with many chunks still frames: the round trip is over the
    // whole descriptor list, not one descriptor.
    let many = with_descriptors(&cp.manifest, MAX_CHUNKS / 2, 0);
    assert!(many.encode().unwrap().len() > CHUNK_TARGET_BYTES / 4);
    let mut reader = FrameReader::new();
    reader.push(&many.frame().unwrap());
    let framed = reader.next_frame().unwrap().unwrap();
    assert_eq!(framed.kind, kinds::MANIFEST);
    assert_eq!(SharedManifestV1::from_frame(&framed).unwrap(), many);
}

/// `manifest` with `count` synthetic chunk descriptors whose boundary keys
/// are `key_bytes` long each. The root is recomputed, so what comes back
/// is a manifest that is internally consistent and differs from the
/// original only in how much it has to say.
fn with_descriptors(
    manifest: &SharedManifestV1,
    count: usize,
    key_bytes: usize,
) -> SharedManifestV1 {
    let mut out = manifest.clone();
    out.chunks = (0..count)
        .map(|i| ChunkDescriptorV1 {
            ordinal: i as u32,
            rows: 1,
            bytes: 64,
            first: (0, vec![0xa1; key_bytes]),
            last: (0, vec![0xa2; key_bytes]),
            digest: Digest32([i as u8; 32]),
        })
        .collect();
    out.root = out.compute_root();
    out
}

#[test]
fn a_manifest_that_no_snapshot_frame_can_carry_is_neither_exported_nor_verified() {
    let engine = model(Profile::default());
    let cp = export(&engine, &ExportLimits::default());
    let encoded = encoded_chunks(&cp);
    verify_shared(&cp.manifest, &encoded).unwrap();

    // Enough descriptors on their own. The chunk count is what the
    // artifact permits, and it is still more than one frame holds.
    let too_many = with_descriptors(&cp.manifest, MAX_CHUNKS, 0);
    assert_eq!(too_many.frame(), Err(ArtifactError::TooLarge));
    assert_eq!(
        verify_shared(&too_many, &encoded),
        Err(VerifyError::ManifestTooLarge)
    );

    // Or few descriptors with long boundary keys. A count bound cannot
    // catch this one: each descriptor names its chunk's first and last
    // key, and those are rows, not counters.
    let long_keys = with_descriptors(&cp.manifest, 64, 16 * 1024);
    assert!(long_keys.chunks.len() < MAX_CHUNKS);
    assert_eq!(long_keys.frame(), Err(ArtifactError::TooLarge));
    assert_eq!(
        verify_shared(&long_keys, &encoded),
        Err(VerifyError::ManifestTooLarge)
    );

    // The bound is the frame's, so a manifest just under it still travels.
    let biggest = with_descriptors(&cp.manifest, 32, 16 * 1024);
    let bytes = biggest.encode().unwrap();
    assert!(bytes.len() <= MAX_MANIFEST_BYTES, "{}", bytes.len());
    let mut reader = FrameReader::new();
    reader.push(&biggest.frame().unwrap());
    let framed = reader.next_frame().unwrap().unwrap();
    assert_eq!(SharedManifestV1::from_frame(&framed).unwrap(), biggest);
    // Its chunks are synthetic, so verification stops at the sequence, not
    // at the size.
    assert_eq!(
        verify_shared(&biggest, &encoded),
        Err(VerifyError::ChunkSequence)
    );

    // And an export refuses to hand one back. Long keys chunked one row at
    // a time are the cheap way there: the rows are ordinary and well
    // within their bounds, and it is the descriptors naming each chunk's
    // first and last key that outgrow the frame.
    let long = model_with_long_keys(64, 16 * 1024);
    let view = long.reader().snapshot().unwrap();
    assert_eq!(
        export_shared(
            &view,
            origin(),
            &ExportLimits {
                chunk_target_bytes: 1,
                ..ExportLimits::default()
            }
        ),
        Err(ExportError::ManifestTooLarge)
    );
    // The same rows under the real chunk target pack into few chunks and
    // export, verify and frame as they should: the bound is on what the
    // manifest has to say, not on the state.
    let packed = export(&long, &ExportLimits::default());
    verify_shared(&packed.manifest, &encoded_chunks(&packed)).unwrap();
    packed.manifest.frame().unwrap();
    // The requested chunk ceiling never raises the artifact's own.
    assert_eq!(ExportLimits::default().max_chunks, MAX_CHUNKS);
    let wide = export(
        &engine,
        &ExportLimits {
            chunk_target_bytes: 1,
            max_chunks: usize::MAX,
            ..ExportLimits::default()
        },
    );
    assert!(wide.chunks.len() <= MAX_CHUNKS);
    verify_shared(&wide.manifest, &encoded_chunks(&wide)).unwrap();
}

/// A domain whose keys are long. A chunk descriptor names its chunk's
/// first and last key, so key length is manifest size.
fn model_with_long_keys(rows: usize, key_bytes: usize) -> ModelEngine {
    let mut engine = model(Profile::default());
    let mut tx = engine.begin_write().unwrap();
    for i in 0..rows {
        let mut key = format!("long-{i:06}").into_bytes();
        key.resize(key_bytes, b'k');
        tx.put(
            Collection::KvCurrentV1.id(),
            &codecs::current_key(&NS, &key),
            &codecs::encode_current(&entry(b"v", 3, 3, 1)).unwrap(),
        )
        .unwrap();
    }
    tx.commit_durable().unwrap();
    engine
}

/// A view wrapper that mutates the engine through another handle on the
/// second page scan: an honest pinned snapshot never sees it.
struct MutatingView<V> {
    inner: V,
    engine: Arc<Mutex<ModelEngine>>,
    scans: Mutex<u32>,
}

impl<V: OrderedRead> OrderedRead for MutatingView<V> {
    fn get(
        &self,
        c: coord_core::effect::CollectionId,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, EngineError> {
        self.inner.get(c, key)
    }
    fn scan_page(
        &self,
        c: coord_core::effect::CollectionId,
        r: &ScanRequest,
    ) -> Result<RowPage, EngineError> {
        let mut scans = self.scans.lock().unwrap();
        *scans += 1;
        if *scans == 2 {
            let mut engine = self.engine.lock().unwrap();
            let mut tx = engine.begin_write().unwrap();
            tx.put(
                Collection::KvCurrentV1.id(),
                &codecs::current_key(&NS, b"late"),
                &codecs::encode_current(&entry(b"late", 3, 3, 1)).unwrap(),
            )
            .unwrap();
            let seq = LocalJournalSeq::new(500).unwrap();
            tx.put(
                Collection::MetaV1.id(),
                meta_fields::APPLIED_STAMP,
                &AppliedStamp {
                    store_seq: StoreSeq::from_journal(seq),
                    journal_seq: seq,
                    last_batch_digest: Digest32([0xee; 32]),
                }
                .to_envelope()
                .unwrap(),
            )
            .unwrap();
            tx.commit_durable().unwrap();
        }
        self.inner.scan_page(c, r)
    }
}

#[test]
fn mutation_during_export_cannot_mix_views() {
    let limits = ExportLimits::default();
    let expected = export(&model(Profile::default()), &limits).manifest.root;
    for misbehaviors in [&[][..], &[Misbehavior::MixedSnapshots][..]] {
        let mut engine = ModelEngine::misbehaving(misbehaviors);
        seed(&mut engine, Profile::default());
        let snapshot = engine.reader().snapshot().unwrap();
        let shared = Arc::new(Mutex::new(engine));
        let view = MutatingView {
            inner: snapshot,
            engine: shared,
            scans: Mutex::new(0),
        };
        let result = export_shared(&view, origin(), &limits);
        if misbehaviors.is_empty() {
            assert_eq!(
                result.unwrap().manifest.root,
                expected,
                "pinned view unaffected"
            );
        } else {
            assert_eq!(result.unwrap_err(), ExportError::ViewChanged);
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct CheckpointFixture {
    schema: String,
    format: u16,
    root_context: String,
    chunk_context: String,
    kinds: Vec<(String, u16)>,
    root_hex: String,
    chunk_digests_hex: Vec<String>,
    collections: Vec<(u16, u64, u64)>,
    manifest_hex: String,
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

#[test]
fn shared_checkpoint_fixture_is_frozen() {
    let cp = export(&model(Profile::default()), &ExportLimits::default());
    let fixture = CheckpointFixture {
        schema: "shared_checkpoint_v1".to_owned(),
        format: SHARED_CHECKPOINT_FORMAT_V1,
        root_context: coord_types::HashDomain::SharedCheckpointRoot
            .context()
            .to_owned(),
        chunk_context: coord_types::HashDomain::SharedCheckpointChunk
            .context()
            .to_owned(),
        kinds: vec![
            ("Manifest".to_owned(), kinds::MANIFEST),
            ("Chunk".to_owned(), kinds::CHUNK),
        ],
        root_hex: hex(&cp.manifest.root.0),
        chunk_digests_hex: cp
            .manifest
            .chunks
            .iter()
            .map(|c| hex(&c.digest.0))
            .collect(),
        collections: cp
            .manifest
            .collections
            .iter()
            .map(|c| (c.collection, c.rows, c.bytes))
            .collect(),
        manifest_hex: hex(&cp.manifest.encode().unwrap()),
    };
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/shared_checkpoint_v1.json");
    if std::env::var_os("COORD_CHECKPOINT_WRITE_FIXTURES").is_some() {
        let mut json = serde_json::to_string_pretty(&fixture).unwrap();
        json.push('\n');
        std::fs::write(&path, json).unwrap();
        return;
    }
    let stored: CheckpointFixture =
        serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    assert_eq!(
        stored, fixture,
        "shared checkpoint fixture drifted; the canonical traversal and root are frozen"
    );
}

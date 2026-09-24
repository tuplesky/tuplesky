//! task-59 acceptance: a backup manifest binds the artifact it names; a
//! restore establishes a *new* cluster, requires an explicit external
//! fencing action, states its RPO rather than implying zero loss, and
//! carries no voting authority, no session and no lease out of the
//! cluster it replaces. An observer or common snapshot is not a voter's
//! local checkpoint and cannot be offered as one.

use coord_checkpoint::export::{CheckpointOrigin, ExportLimits, export_shared};
use coord_checkpoint::install::{ChunkSet, InstallLimits, InstallRequirements, install_shared};
use coord_checkpoint::manifest::{SharedCheckpointV1, SharedManifestV1};
use coord_checkpoint::restore::{
    Artifact, BackupManifestV1, Disposition, FencingAttestationV1, RestoreError, RestorePlan,
    plan_restore, restore_shared, restored_baseline,
};
use coord_state::plan::KvEventKind;
use coord_state::view::KvEntry;
use coord_storage::codecs::{
    self, EventRecordV1, ExecutedRecordV1, HistoryRecordV1, RetryRecordV1,
};
use coord_storage::lowering::{DurableMeta, ExecutionFrontier};
use coord_store_api::engine::{LocalEngine, OrderedRead, ScanRequest, SnapshotSource, WriteTxn};
use coord_store_api::envelope::AppliedStamp;
use coord_store_api::registry::{Collection, meta_fields};
use coord_store_api::seq::StoreSeq;
use coord_store_testkit::model::ModelEngine;
use coord_types::identity::{CommandId, Digest32, RetryKey};
use coord_types::ids::*;

const SOURCE: ClusterId = ClusterId([1; 16]);
const SUCCESSOR: ClusterId = ClusterId([0xc5; 16]);
const DOMAIN: DomainId = DomainId([2; 16]);
const NS: NamespaceId = NamespaceId([5; 16]);
const SESSION: SessionId = SessionId([3; 16]);
const CLIENT: ClientInstanceId = ClientInstanceId([4; 16]);
const DONOR: ReplicaId = ReplicaId([0xd0; 16]);
const RESTORER: ReplicaId = ReplicaId([0x5e; 16]);
const LEASE: LeaseId = LeaseId([7; 16]);
const SOURCE_EPOCH: u64 = 7;
const SUCCESSOR_EPOCH: u64 = 1;
const TAKEN_AT: u64 = 1_700_000_000;

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

fn leased(value: &[u8], create: u64, modified: u64) -> KvEntry {
    KvEntry {
        lease: Some(LEASE),
        lease_generation: Some(LeaseGeneration::new(3).unwrap()),
        ..entry(value, create, modified, 1)
    }
}

type Row = (Collection, Vec<u8>, Vec<u8>);

/// The source cluster's common state at the boundary: execution
/// position 4, KV revision 5, retention floor 3, lease authority 2,
/// configuration epoch 7. One key is attached to a lease, which is what
/// makes the restore's detachment rule observable.
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
    // The old cluster's voters and their keys.
    put(
        Collection::ConfigV1,
        b"epoch-7".to_vec(),
        b"old-voters".to_vec(),
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
        codecs::current_key(&NS, b"plain"),
        codecs::encode_current(&entry(b"a3", 1, 3, 3)).unwrap(),
    );
    put(
        Collection::KvCurrentV1,
        codecs::current_key(&NS, b"held"),
        codecs::encode_current(&leased(b"b5", 4, 5)).unwrap(),
    );
    put(
        Collection::KvHistoryV1,
        codecs::history_key(&NS, b"plain", rev(3)),
        codecs::encode_history(&HistoryRecordV1 {
            entry: Some(entry(b"a3", 1, 3, 3)),
        })
        .unwrap(),
    );
    put(
        Collection::EventsV1,
        codecs::event_key(rev(3), 0),
        codecs::encode_event(&EventRecordV1 {
            kind: KvEventKind::Put,
            namespace: NS,
            key: b"plain".to_vec(),
            entry: Some(entry(b"a3", 1, 3, 3)),
            prev: None,
        })
        .unwrap(),
    );
    let retry = RetryKey {
        cluster_id: SOURCE,
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
        codecs::encode_retry_floor(&coord_storage::codecs::RetryFloorV1 {
            floor: RequestSequence::ZERO,
            width: 16,
        })
        .unwrap(),
    );
    put(
        Collection::LeaseV1,
        codecs::lease_row_key(&LEASE),
        b"lease-record".to_vec(),
    );
    put(
        Collection::LeaseKeysV1,
        codecs::lease_key(&LEASE, &NS, b"held"),
        b"lease-key".to_vec(),
    );
    put(
        Collection::SessionV1,
        codecs::session_key(&SESSION),
        b"session-record".to_vec(),
    );
    put(
        Collection::AuthGrantV1,
        codecs::grant_key(&Digest32([9; 32])),
        b"grant".to_vec(),
    );
    put(Collection::PolicyV1, b"rule".to_vec(), b"policy".to_vec());
    rows
}

fn source_engine() -> ModelEngine {
    let mut engine = ModelEngine::new();
    let mut tx = engine.begin_write().unwrap();
    for (c, k, v) in common_rows() {
        tx.put(c.id(), &k, &v).unwrap();
    }
    for (key, value) in [
        (meta_fields::CLUSTER_ID, SOURCE.as_bytes().to_vec()),
        (meta_fields::DOMAIN_ID, DOMAIN.as_bytes().to_vec()),
        (meta_fields::REPLICA_ID, DONOR.as_bytes().to_vec()),
    ] {
        tx.put(Collection::MetaV1.id(), key, &value).unwrap();
    }
    tx.put(
        Collection::ProtocolV1.id(),
        &[0, 0, 0, 0, 0, 0, 0, 7, 0xff],
        b"old-promise",
    )
    .unwrap();
    DurableMeta {
        stamp: AppliedStamp::new(
            StoreSeq::from_journal(LocalJournalSeq::new(41).unwrap()),
            Digest32([0xab; 32]),
        ),
        frontier: ExecutionFrontier {
            configuration: epoch(SOURCE_EPOCH),
            execution_position: pos(4),
        },
    }
    .write(&mut tx)
    .unwrap();
    tx.commit_durable().unwrap();
    engine
}

fn snapshot() -> SharedCheckpointV1 {
    let engine = source_engine();
    let view = engine.reader().snapshot().unwrap();
    export_shared(
        &view,
        CheckpointOrigin {
            cluster: SOURCE,
            domain: DOMAIN,
        },
        &ExportLimits {
            chunk_target_bytes: 1024,
            ..ExportLimits::default()
        },
    )
    .unwrap()
}

fn chunk_set(checkpoint: &SharedCheckpointV1) -> ChunkSet {
    let mut set = ChunkSet::for_manifest(&checkpoint.manifest).unwrap();
    for chunk in &checkpoint.chunks {
        set.accept(&checkpoint.manifest, chunk.encode().unwrap())
            .unwrap();
    }
    set
}

/// A fresh generation of `cluster`, as the storage lifecycle creates one.
fn fresh(cluster: ClusterId) -> ModelEngine {
    let mut engine = ModelEngine::new();
    let mut tx = engine.begin_write().unwrap();
    let meta = Collection::MetaV1.id();
    tx.put(meta, meta_fields::CLUSTER_ID, cluster.as_bytes())
        .unwrap();
    tx.put(meta, meta_fields::DOMAIN_ID, DOMAIN.as_bytes())
        .unwrap();
    tx.put(meta, meta_fields::REPLICA_ID, RESTORER.as_bytes())
        .unwrap();
    tx.commit_durable().unwrap();
    engine
}

fn backup(artifact: &SharedManifestV1) -> BackupManifestV1 {
    BackupManifestV1::of(artifact, TAKEN_AT)
}

fn attestation(backup: &BackupManifestV1) -> FencingAttestationV1 {
    FencingAttestationV1 {
        abandoned: backup.source,
        successor: SUCCESSOR,
        backup: backup.root,
        action: "revoked the old cluster's node certificates, ticket DR-91".into(),
        at: TAKEN_AT + 600,
    }
}

fn plan(backup: &BackupManifestV1, fencing: &FencingAttestationV1) -> RestorePlan {
    plan_restore(
        backup,
        Artifact::Shared,
        SUCCESSOR,
        epoch(SUCCESSOR_EPOCH),
        Some(fencing),
    )
    .expect("an attested restore into a new cluster")
}

fn limits() -> InstallLimits {
    InstallLimits {
        rows_per_commit: 4,
        bytes_per_commit: 4096,
    }
}

fn rows_of<E: LocalEngine>(engine: &mut E, collection: Collection) -> Vec<(Vec<u8>, Vec<u8>)> {
    let view = engine.reader().snapshot().unwrap();
    view.scan_page(collection.id(), &ScanRequest::all(1024, 1 << 20))
        .unwrap()
        .rows
        .into_iter()
        .map(|r| (r.key, r.value))
        .collect()
}

/// A restore establishes a new cluster holding the backup's KV history
/// and nothing that belonged to the old cluster's authority.
#[test]
fn a_restore_establishes_a_new_cluster_and_carries_only_what_survives_it() {
    let checkpoint = snapshot();
    let backup = backup(&checkpoint.manifest);
    let fencing = attestation(&backup);
    let plan = plan(&backup, &fencing);
    let mut engine = fresh(SUCCESSOR);
    let restored = restore_shared(
        &mut engine,
        &checkpoint.manifest,
        chunk_set(&checkpoint),
        &plan,
        &fencing,
        &limits(),
    )
    .expect("the restore");

    // What the backup is for.
    let current = rows_of(&mut engine, Collection::KvCurrentV1);
    assert_eq!(current.len(), 2, "the KV rows were not restored");
    assert!(!rows_of(&mut engine, Collection::KvHistoryV1).is_empty());
    assert!(!rows_of(&mut engine, Collection::EventsV1).is_empty());
    // Retries stay: dropping a retained result turns a caller's retry
    // into a second execution, which is worse than a stale answer.
    assert!(!rows_of(&mut engine, Collection::RetryV1).is_empty());
    assert!(!rows_of(&mut engine, Collection::RetryFloorV1).is_empty());

    // The old cluster's voting authority. This is the concrete meaning
    // of "never reuse stale voting authority": the restored store holds
    // no certificate naming any voter, so membership can only come from
    // the successor's own genesis.
    assert!(
        rows_of(&mut engine, Collection::ConfigV1).is_empty(),
        "the restore carried the old cluster's configuration"
    );
    // And the authorization decisions made under it. The successor's
    // genesis writes its own trust rules and permissions.
    assert!(
        rows_of(&mut engine, Collection::PolicyV1).is_empty(),
        "the restore carried the old cluster's policy"
    );
    assert!(
        rows_of(&mut engine, Collection::ProtocolV1).is_empty(),
        "a shared artifact carried protocol state"
    );

    // Sessions, grants and leases established against a cluster that no
    // longer exists.
    for collection in [
        Collection::SessionV1,
        Collection::AuthGrantV1,
        Collection::LeaseV1,
        Collection::LeaseKeysV1,
    ] {
        assert!(
            rows_of(&mut engine, collection).is_empty(),
            "{collection:?} survived the restore"
        );
    }

    // And the key the revoked lease held is detached rather than left
    // attached to an authority that can never expire it.
    let held = current
        .iter()
        .find(|(k, _)| k == &codecs::current_key(&NS, b"held"))
        .expect("the leased key was restored");
    let decoded = codecs::decode_current(&held.1).unwrap();
    assert_eq!(decoded.value, b"b5", "the value was not restored");
    assert_eq!(
        (decoded.lease, decoded.lease_generation),
        (None, None),
        "a restored key is still held by a lease nobody can renew"
    );
    assert_eq!(restored.receipt.detached, 1);

    // The boundary continues, so the new cluster's own history is not
    // rewound within itself; the epoch is the successor's, because the
    // configuration it would otherwise claim is one it deliberately
    // does not hold.
    let view = engine.reader().snapshot().unwrap();
    let meta = DurableMeta::read(&view).unwrap();
    assert_eq!(meta.frontier.execution_position, pos(4));
    assert_eq!(meta.frontier.configuration, epoch(SUCCESSOR_EPOCH));
    assert_eq!(
        codecs::read_kv_revision(&view).unwrap(),
        rev(5),
        "the KV revision did not continue"
    );

    // And the store can always say it is a restored one.
    let receipt = restored_baseline(&view).unwrap().expect("a receipt");
    assert_eq!(receipt.source, SOURCE);
    assert_eq!(receipt.successor, SUCCESSOR);
    assert_eq!(receipt.taken_at, TAKEN_AT);
    assert_eq!(receipt.captured, checkpoint.manifest.root);
    assert!(
        receipt.fencing.contains("DR-91"),
        "the receipt forgot what it was told had been done"
    );
    let dropped: u64 = receipt.dropped.iter().map(|(_, n)| n).sum();
    assert_eq!(dropped, 6, "dropped rows: {:?}", receipt.dropped);
}

/// The plan states what is lost instead of implying a clean restore.
#[test]
fn the_plan_states_its_rpo_and_every_disposition() {
    let checkpoint = snapshot();
    let backup = backup(&checkpoint.manifest);
    let fencing = attestation(&backup);
    let plan = plan(&backup, &fencing);

    assert_eq!(plan.rpo.taken_at, TAKEN_AT);
    assert_eq!(plan.rpo.execution_position, pos(4));
    assert_eq!(plan.rpo.kv_revision, rev(5));
    assert_eq!(plan.kv, Disposition::RestoredAtBoundary);
    assert_eq!(plan.retries, Disposition::RestoredAtBoundary);
    assert_eq!(plan.configurations, Disposition::NotCarried);
    assert_eq!(plan.sessions, Disposition::Invalidated);
    assert_eq!(plan.leases, Disposition::Revoked);
    assert_eq!(plan.watches, Disposition::Resynchronized);
    assert_eq!(plan.successor, SUCCESSOR);
    assert_ne!(plan.successor, plan.source);
}

/// Every way a restore could quietly become ordinary recovery is
/// refused, and named.
#[test]
fn a_restore_that_would_look_like_recovery_is_refused() {
    let checkpoint = snapshot();
    let backup = backup(&checkpoint.manifest);
    let fencing = attestation(&backup);

    // The same identity: a rewound history behind a name callers
    // already hold promises from.
    assert_eq!(
        plan_restore(
            &backup,
            Artifact::Shared,
            SOURCE,
            epoch(SUCCESSOR_EPOCH),
            Some(&FencingAttestationV1 {
                successor: SOURCE,
                ..fencing.clone()
            }),
        ),
        Err(RestoreError::SameCluster)
    );

    // No external action at all.
    assert_eq!(
        plan_restore(
            &backup,
            Artifact::Shared,
            SUCCESSOR,
            epoch(SUCCESSOR_EPOCH),
            None
        ),
        Err(RestoreError::Unfenced)
    );

    // An attestation about some other cluster, some other successor or
    // some other backup is not this restore's.
    for (what, wrong) in [
        (
            "abandoned",
            FencingAttestationV1 {
                abandoned: ClusterId([0xee; 16]),
                ..fencing.clone()
            },
        ),
        (
            "successor",
            FencingAttestationV1 {
                successor: ClusterId([0xee; 16]),
                ..fencing.clone()
            },
        ),
        (
            "backup",
            FencingAttestationV1 {
                backup: Digest32([0xee; 32]),
                ..fencing.clone()
            },
        ),
    ] {
        assert_eq!(
            plan_restore(
                &backup,
                Artifact::Shared,
                SUCCESSOR,
                epoch(SUCCESSOR_EPOCH),
                Some(&wrong)
            ),
            Err(RestoreError::FencingMismatch { field: what }),
            "an attestation with the wrong {what} was accepted"
        );
    }

    // An attestation that names no action is a box somebody ticked.
    for action in ["", "   "] {
        assert_eq!(
            plan_restore(
                &backup,
                Artifact::Shared,
                SUCCESSOR,
                epoch(SUCCESSOR_EPOCH),
                Some(&FencingAttestationV1 {
                    action: action.into(),
                    ..fencing.clone()
                })
            ),
            Err(RestoreError::FencingActionUnusable)
        );
    }

    // Section 17.16.1: the artifacts are not interchangeable. A local
    // checkpoint is one incarnation's obligations and an observer
    // snapshot may not even be full MVCC; neither is a cluster.
    for offered in [Artifact::Local, Artifact::Observer] {
        assert_eq!(
            plan_restore(
                &backup,
                offered,
                SUCCESSOR,
                epoch(SUCCESSOR_EPOCH),
                Some(&fencing)
            ),
            Err(RestoreError::NotACommonSnapshot { offered })
        );
    }

    // A backup index pointed at different bytes.
    let mut tampered = backup;
    tampered.captured = Digest32([0xaa; 32]);
    assert_eq!(
        plan_restore(
            &tampered,
            Artifact::Shared,
            SUCCESSOR,
            epoch(SUCCESSOR_EPOCH),
            Some(&fencing)
        ),
        Err(RestoreError::BackupRootMismatch)
    );
    let mut old = backup;
    old.format = 99;
    old.root = old.compute_root();
    assert_eq!(
        plan_restore(
            &old,
            Artifact::Shared,
            SUCCESSOR,
            epoch(SUCCESSOR_EPOCH),
            Some(&fencing)
        ),
        Err(RestoreError::UnsupportedFormat { found: 99 })
    );
}

/// The restore carried out is the restore admitted, into a generation of
/// the successor and nowhere else.
#[test]
fn the_artifact_the_target_and_the_attestation_must_all_be_the_planned_ones() {
    let checkpoint = snapshot();
    let backup = backup(&checkpoint.manifest);
    let fencing = attestation(&backup);
    let plan = plan(&backup, &fencing);

    // A generation still stamped with the cluster being abandoned. This
    // is the restore-in-place mistake, and the identity record is where
    // it is caught.
    let mut wrong = fresh(SOURCE);
    assert_eq!(
        restore_shared(
            &mut wrong,
            &checkpoint.manifest,
            chunk_set(&checkpoint),
            &plan,
            &fencing,
            &limits()
        ),
        Err(RestoreError::TargetIdentityMismatch { field: "cluster" })
    );

    // A generation a restore already wrote: a restore writes a whole
    // cluster's state and has nothing to merge with.
    let mut twice = fresh(SUCCESSOR);
    restore_shared(
        &mut twice,
        &checkpoint.manifest,
        chunk_set(&checkpoint),
        &plan,
        &fencing,
        &limits(),
    )
    .expect("the first restore");
    assert_eq!(
        restore_shared(
            &mut twice,
            &checkpoint.manifest,
            chunk_set(&checkpoint),
            &plan,
            &fencing,
            &limits()
        ),
        Err(RestoreError::AlreadyRestored),
        "a restore was written over a restored generation"
    );

    // A generation that already holds a catch-up install is likewise
    // not a restore target: an install and a restore are two different
    // answers to "what is this store", and a store cannot be both.
    let mut installed = fresh(SOURCE);
    install_shared(
        &mut installed,
        &checkpoint.manifest,
        chunk_set(&checkpoint),
        &InstallRequirements {
            cluster: SOURCE,
            domain: DOMAIN,
            minimum_configuration: epoch(SOURCE_EPOCH),
        },
        &limits(),
    )
    .expect("an ordinary catch-up install");
    let in_place = plan_restore(
        &backup,
        Artifact::Shared,
        SOURCE,
        epoch(SUCCESSOR_EPOCH),
        Some(&fencing),
    );
    assert_eq!(
        in_place,
        Err(RestoreError::SameCluster),
        "restoring in place was planned at all"
    );

    // A different artifact with the same shape.
    let other = {
        let mut engine = source_engine();
        let mut tx = engine.begin_write().unwrap();
        tx.put(
            Collection::KvCurrentV1.id(),
            &codecs::current_key(&NS, b"extra"),
            &codecs::encode_current(&entry(b"x", 1, 3, 1)).unwrap(),
        )
        .unwrap();
        tx.commit_durable().unwrap();
        let view = engine.reader().snapshot().unwrap();
        export_shared(
            &view,
            CheckpointOrigin {
                cluster: SOURCE,
                domain: DOMAIN,
            },
            &ExportLimits {
                chunk_target_bytes: 1024,
                ..ExportLimits::default()
            },
        )
        .unwrap()
    };
    let mut target = fresh(SUCCESSOR);
    assert_eq!(
        restore_shared(
            &mut target,
            &other.manifest,
            chunk_set(&other),
            &plan,
            &fencing,
            &limits()
        ),
        Err(RestoreError::ArtifactMismatch { field: "root" })
    );

    // And somebody else's attestation.
    assert_eq!(
        restore_shared(
            &mut target,
            &checkpoint.manifest,
            chunk_set(&checkpoint),
            &plan,
            &FencingAttestationV1 {
                backup: Digest32([0xee; 32]),
                ..fencing.clone()
            },
            &limits()
        ),
        Err(RestoreError::FencingMismatch { field: "backup" })
    );

    // The planned restore, into the planned target, still works after
    // all of that: none of the refusals left the generation unusable.
    restore_shared(
        &mut target,
        &checkpoint.manifest,
        chunk_set(&checkpoint),
        &plan,
        &fencing,
        &limits(),
    )
    .expect("the planned restore");
}

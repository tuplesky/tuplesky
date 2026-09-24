//! Backup manifests and restore-as-a-new-cluster (task-59; design
//! Sections 5.4, 7.4, 17.16, 22.2).
//!
//! Ordinary recovery preserves identity and everything this cluster
//! acknowledged. A restore does neither, and the whole module exists to
//! keep the two from being confused with each other.
//!
//! What a restore is:
//!
//! * **A new cluster.** The restored store is stamped with a *successor*
//!   cluster identity, never the one the backup came from. Restoring
//!   under the source identity would put a rewound history behind a name
//!   callers already have promises from, and the promises would still
//!   look valid. [`plan_restore`] refuses it outright.
//! * **Fenced by something outside this system.** A restore is safe only
//!   once the old cluster cannot still be serving, and nothing here can
//!   establish that: the old voters may be partitioned from this
//!   operator and perfectly healthy. So the plan requires a
//!   [`FencingAttestationV1`] naming exactly the abandoned cluster, the
//!   successor and this backup. It is a record of an out-of-band action,
//!   not the action; what it buys is that skipping the action has to be
//!   a deliberate lie rather than an omission.
//! * **Bounded by the backup's RPO.** [`RestorePlan::rpo`] states the
//!   boundary and when the snapshot was pinned. Everything after it is
//!   gone, and the plan says so rather than implying a zero-loss
//!   restore.
//!
//! What a restore carries, and what it deliberately drops:
//!
//! | State | Disposition | Why |
//! |---|---|---|
//! | KV rows, history, events | restored at the boundary | it is what the backup is for |
//! | retries and retry floors | restored at the boundary | dropping them turns a caller's retry into a second execution |
//! | configuration epochs and certificates | not carried | they name the old voters and their keys; the successor's genesis says who votes here |
//! | policy: trust rules and permissions | not carried | they are the abandoned cluster's authorization decisions; the successor's genesis writes its own |
//! | sessions and auth grants | invalidated | a session established against the old cluster is not a session here |
//! | leases and their reverse index | revoked | nobody can renew a lease granted by a cluster that no longer exists |
//! | lease attachments on restored keys | detached | a key attached to a revoked lease would be held by an authority that cannot expire it |
//! | promises, votes, obligations | never present | `protocol_v1` is node-private and no shared artifact carries it |
//!
//! Dropping the configuration rows is what "never reuse stale voting
//! authority" means concretely: the restored store holds no certificate
//! naming any voter, so the successor cluster's membership comes from
//! its own genesis (task-42) and from nowhere else. The execution
//! frontier it starts at carries the *successor's* configuration epoch
//! for the same reason -- the position and the KV revision continue so
//! the new cluster's history is not rewound within itself, but the epoch
//! numbering is the successor's.
//!
//! Finally, the artifacts of Section 17.16.1 are not interchangeable and
//! [`plan_restore`] will not let them be. A `SharedCheckpointV1` is
//! common state; a `LocalRecoveryCheckpointV1` is one incarnation's
//! obligations; an observer snapshot is a declared capability's view and
//! may not even be full MVCC. Only the first restores a cluster, and
//! none of them recreates an existing voter's local state.

use std::fmt;

use coord_storage::codecs;
use coord_storage::lowering::{DurableMeta, ExecutionFrontier};
use coord_store_api::engine::{
    CollectionId, CommitFailure, EngineError, LocalEngine, OrderedRead, SnapshotSource, WriteTxn,
};
use coord_store_api::envelope::StoreEnvelopeV1;
use coord_store_api::registry::{Collection, meta_fields};
use coord_types::identity::{Digest32, HashDomain};
use coord_types::ids::{ClusterId, ConfigurationEpoch, DomainId};
use serde::{Deserialize, Serialize};

use crate::install::{ChunkSet, InstallLimits, installed_baseline};
use crate::manifest::{CheckpointBoundary, ChunkV1, SharedManifestV1};
use crate::verify::{VerifyError, verify_shared};

/// Format of a backup manifest.
pub const BACKUP_FORMAT_V1: u16 = 1;
/// Record kind of the restore receipt inside `checkpoint_v1`.
pub const RESTORED_RECORD_KIND: u16 = 0x0009;
/// Schema version of the restore receipt.
pub const RESTORED_SCHEMA_VERSION: u16 = 1;
/// Key of the restore receipt inside `checkpoint_v1`.
pub const RESTORED_KEY: &[u8] = b"restored_v1";
/// Longest operator reference a fencing attestation may carry.
pub const MAX_FENCING_ACTION_BYTES: usize = 512;

/// What a backup is: one `SharedCheckpointV1` of one domain, with where
/// it came from and when it was pinned.
///
/// The manifest is the thing an operator keeps beside the bytes. It is
/// not a second copy of the artifact's own manifest: it binds that
/// artifact's root, so a backup index cannot be pointed at different
/// bytes, and it records the wall clock, which exists only to state the
/// RPO and never orders anything.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupManifestV1 {
    /// Format.
    pub format: u16,
    /// Cluster the snapshot was taken from.
    pub source: ClusterId,
    /// Domain.
    pub domain: DomainId,
    /// Configuration epoch the boundary was reached under, in the source
    /// cluster's numbering. Evidence about the snapshot, never authority
    /// in the restored one.
    pub configuration: ConfigurationEpoch,
    /// Boundary the snapshot represents.
    pub boundary: CheckpointBoundary,
    /// Root of the `SharedCheckpointV1` this manifest is about.
    pub captured: Digest32,
    /// Wall clock at which the snapshot was pinned, for the RPO
    /// statement. Nothing is ordered by it.
    pub taken_at: u64,
    /// Root over everything above.
    pub root: Digest32,
}

impl BackupManifestV1 {
    /// A manifest for `artifact`, taken at `taken_at`.
    pub fn of(artifact: &SharedManifestV1, taken_at: u64) -> Self {
        let mut manifest = BackupManifestV1 {
            format: BACKUP_FORMAT_V1,
            source: artifact.cluster,
            domain: artifact.domain,
            configuration: artifact.configuration,
            boundary: artifact.boundary,
            captured: artifact.root,
            taken_at,
            root: Digest32([0; 32]),
        };
        manifest.root = manifest.compute_root();
        manifest
    }

    /// The root: format, origin, boundary, captured root and time.
    pub fn compute_root(&self) -> Digest32 {
        HashDomain::BackupManifest.digest(&[
            &self.format.to_be_bytes(),
            self.source.as_bytes(),
            self.domain.as_bytes(),
            &self.configuration.to_be_bytes(),
            &self.boundary.execution_position.to_be_bytes(),
            &self.boundary.kv_revision.to_be_bytes(),
            &self.boundary.retention_floor.to_be_bytes(),
            &self.boundary.lease_authority.to_be_bytes(),
            &self.captured.0,
            &self.taken_at.to_be_bytes(),
        ])
    }

    /// Whether the manifest is well formed and binds its own contents.
    pub fn verify(&self) -> Result<(), RestoreError> {
        if self.format != BACKUP_FORMAT_V1 {
            return Err(RestoreError::UnsupportedFormat { found: self.format });
        }
        if self.root != self.compute_root() {
            return Err(RestoreError::BackupRootMismatch);
        }
        Ok(())
    }
}

/// An operator's record that the source cluster has been isolated.
///
/// This is not a fence. The fence is whatever was actually done --
/// revoking the old cluster's credentials, taking its load balancer
/// away, powering its nodes off -- and this system cannot observe any of
/// it. What the attestation does is make the operator name the cluster
/// being abandoned, the cluster replacing it and the exact backup, so
/// that a restore performed without the out-of-band action is a
/// deliberate false statement rather than a step somebody forgot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FencingAttestationV1 {
    /// The cluster that has been isolated and must not serve again.
    pub abandoned: ClusterId,
    /// The cluster identity the restore establishes.
    pub successor: ClusterId,
    /// The backup this attestation is for.
    pub backup: Digest32,
    /// The operator's reference to what was actually done.
    pub action: String,
    /// When it was done.
    pub at: u64,
}

impl FencingAttestationV1 {
    /// The subject an operator is asserting: the three identities and
    /// nothing else. Exposed so an attestation can be countersigned by
    /// whatever authority a deployment has; nothing here requires one.
    pub fn subject(&self) -> Digest32 {
        HashDomain::FencingAttestation.digest(&[
            self.abandoned.as_bytes(),
            self.successor.as_bytes(),
            &self.backup.0,
        ])
    }
}

/// Which artifact of Section 17.16.1 was offered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Artifact {
    /// A `SharedCheckpointV1`: common state at a boundary.
    Shared,
    /// A `LocalRecoveryCheckpointV1`: one incarnation's whole storage,
    /// obligations included.
    Local,
    /// An observer snapshot: a declared capability's view.
    Observer,
}

/// What happens to one class of state in a restore.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Disposition {
    /// Written as the backup holds it, at the backup's boundary.
    RestoredAtBoundary,
    /// Not written: it named the old cluster's authority.
    NotCarried,
    /// Not written: it was established against a cluster that no longer
    /// exists.
    Invalidated,
    /// Not written, and every attachment to it cleared.
    Revoked,
    /// Callers re-establish it against the new cluster; nothing is
    /// carried.
    Resynchronized,
}

/// What the restore loses, stated rather than implied.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rpo {
    /// When the snapshot was pinned.
    pub taken_at: u64,
    /// The last execution position the restored cluster will hold.
    pub execution_position: coord_types::ids::ExecutionPosition,
    /// The last KV revision it will hold.
    pub kv_revision: coord_types::ids::KvRevision,
}

/// An admitted restore: what it will write, under which identity, and
/// what it will not carry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestorePlan {
    /// The cluster being abandoned.
    pub source: ClusterId,
    /// The cluster identity the restored store is stamped with.
    pub successor: ClusterId,
    /// Domain.
    pub domain: DomainId,
    /// The successor's own configuration epoch: where its numbering
    /// starts, from its genesis, not from the donor's.
    pub configuration: ConfigurationEpoch,
    /// The boundary the restored store will represent.
    pub boundary: CheckpointBoundary,
    /// Root of the artifact this plan admits and no other.
    pub captured: Digest32,
    /// Root of the backup manifest, which is what a fencing attestation
    /// names.
    pub backup: Digest32,
    /// What is lost.
    pub rpo: Rpo,
    /// KV rows, history and events.
    pub kv: Disposition,
    /// Retained retry results and floors.
    pub retries: Disposition,
    /// Configuration epochs and certificates, and the trust rules and
    /// permissions authorized under them.
    pub configurations: Disposition,
    /// Sessions and the grants bound to them.
    pub sessions: Disposition,
    /// Leases and their reverse index.
    pub leases: Disposition,
    /// Watches.
    pub watches: Disposition,
}

/// Why a restore was refused or stopped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RestoreError {
    /// The backup manifest's format is not one this build restores.
    UnsupportedFormat {
        /// Found.
        found: u16,
    },
    /// The backup manifest does not bind its own contents.
    BackupRootMismatch,
    /// Something other than a common snapshot was offered as a backup.
    NotACommonSnapshot {
        /// What was offered.
        offered: Artifact,
    },
    /// The restore would stamp the store with the cluster the backup
    /// came from.
    SameCluster,
    /// No fencing attestation was supplied.
    Unfenced,
    /// The attestation names another cluster or another backup.
    FencingMismatch {
        /// Which field.
        field: &'static str,
    },
    /// The attestation's operator reference is absent or beyond the
    /// bound.
    FencingActionUnusable,
    /// The artifact's own manifest disagrees with the backup manifest.
    ArtifactMismatch {
        /// Which field.
        field: &'static str,
    },
    /// The artifact failed verification.
    Verify(VerifyError),
    /// A chunk does not decode.
    MalformedChunk,
    /// A row names a collection no shared artifact may carry.
    ForeignCollection {
        /// Collection.
        collection: u16,
    },
    /// The target generation is not a fresh one of the successor
    /// cluster.
    TargetIdentityMismatch {
        /// Which field.
        field: &'static str,
    },
    /// The target generation carries no identity record of its own.
    TargetIdentityMissing {
        /// Which field.
        field: &'static str,
    },
    /// The target generation already holds state.
    TargetNotEmpty {
        /// Collection.
        collection: u16,
    },
    /// The target generation already carries an install or restore
    /// receipt.
    AlreadyRestored,
    /// The engine failed.
    Engine(EngineError),
    /// A durable commit failed; the staged generation must be abandoned.
    Commit(CommitFailure),
}

impl fmt::Display for RestoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RestoreError::Verify(e) => write!(f, "verify: {e}"),
            RestoreError::Engine(e) => write!(f, "engine: {e}"),
            RestoreError::Commit(e) => write!(f, "commit: {e}"),
            other => write!(f, "{other:?}"),
        }
    }
}

impl std::error::Error for RestoreError {}

impl From<EngineError> for RestoreError {
    fn from(e: EngineError) -> Self {
        RestoreError::Engine(e)
    }
}

impl From<VerifyError> for RestoreError {
    fn from(e: VerifyError) -> Self {
        RestoreError::Verify(e)
    }
}

/// Admit a restore, or say why not.
///
/// Nothing is read from a store and nothing is written: this is the
/// decision, and it is separate from carrying it out so that a rehearsal
/// can take it and print it. `successor_configuration` is the epoch the
/// successor cluster's own genesis establishes.
pub fn plan_restore(
    backup: &BackupManifestV1,
    offered: Artifact,
    successor: ClusterId,
    successor_configuration: ConfigurationEpoch,
    fencing: Option<&FencingAttestationV1>,
) -> Result<RestorePlan, RestoreError> {
    backup.verify()?;
    // Section 17.16.1: the three artifacts are not interchangeable. A
    // local checkpoint is one incarnation's obligations and an observer
    // snapshot may not even be full MVCC; neither is a cluster.
    if offered != Artifact::Shared {
        return Err(RestoreError::NotACommonSnapshot { offered });
    }
    // Restoring under the source identity is the one thing that makes a
    // rewound history indistinguishable from the live one.
    if successor == backup.source {
        return Err(RestoreError::SameCluster);
    }
    let Some(fencing) = fencing else {
        return Err(RestoreError::Unfenced);
    };
    if fencing.abandoned != backup.source {
        return Err(RestoreError::FencingMismatch { field: "abandoned" });
    }
    if fencing.successor != successor {
        return Err(RestoreError::FencingMismatch { field: "successor" });
    }
    if fencing.backup != backup.root {
        return Err(RestoreError::FencingMismatch { field: "backup" });
    }
    if fencing.action.trim().is_empty() || fencing.action.len() > MAX_FENCING_ACTION_BYTES {
        return Err(RestoreError::FencingActionUnusable);
    }
    Ok(RestorePlan {
        source: backup.source,
        successor,
        domain: backup.domain,
        configuration: successor_configuration,
        boundary: backup.boundary,
        captured: backup.captured,
        backup: backup.root,
        rpo: Rpo {
            taken_at: backup.taken_at,
            execution_position: backup.boundary.execution_position,
            kv_revision: backup.boundary.kv_revision,
        },
        kv: Disposition::RestoredAtBoundary,
        retries: Disposition::RestoredAtBoundary,
        configurations: Disposition::NotCarried,
        sessions: Disposition::Invalidated,
        leases: Disposition::Revoked,
        watches: Disposition::Resynchronized,
    })
}

/// Whether a collection's rows are written by a restore.
///
/// One function, so the plan an operator reads and the rows that land on
/// disk cannot drift apart.
const fn carried(collection: Collection) -> bool {
    !matches!(
        collection,
        // The old cluster's voters and their keys, and the trust rules
        // and permissions it authorized under. The successor's genesis
        // writes its own; carrying these would leave the abandoned
        // cluster's authorization decisions in force here.
        Collection::ConfigV1
            | Collection::PolicyV1
            // Established against a cluster that no longer exists.
            | Collection::SessionV1
            | Collection::AuthGrantV1
            // Nobody can renew them.
            | Collection::LeaseV1
            | Collection::LeaseKeysV1
    )
}

/// What one completed restore did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Restored {
    /// The receipt now durable in the generation.
    pub receipt: RestoredV1,
    /// Durable transactions used.
    pub commits: u32,
}

/// The durable receipt of a completed restore, held in `checkpoint_v1`.
///
/// Node-private, like the install receipt, and it grants nothing. What it
/// is for is that a restored store can always say it is one: which
/// cluster it came from, at which boundary, how much was dropped, and
/// what the RPO was. A store that cannot answer that will eventually be
/// treated as an ordinary one.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestoredV1 {
    /// Backup format restored.
    pub format: u16,
    /// Cluster the backup came from.
    pub source: ClusterId,
    /// Cluster this store now belongs to.
    pub successor: ClusterId,
    /// Domain.
    pub domain: DomainId,
    /// The successor's configuration epoch the store starts at.
    pub configuration: ConfigurationEpoch,
    /// Boundary the store represents.
    pub boundary: CheckpointBoundary,
    /// Verified root of the artifact restored.
    pub captured: Digest32,
    /// When the snapshot was pinned.
    pub taken_at: u64,
    /// The operator's fencing reference, kept so a restored cluster can
    /// say what it was told had been done.
    pub fencing: String,
    /// Rows written.
    pub rows: u64,
    /// Rows deliberately not written, by collection identifier.
    pub dropped: Vec<(u16, u64)>,
    /// Restored keys whose lease attachment was cleared.
    pub detached: u64,
}

impl RestoredV1 {
    /// Encode as a `checkpoint_v1` row value.
    pub fn encode(&self) -> Result<Vec<u8>, EngineError> {
        let payload = postcard::to_allocvec(self).map_err(|_| {
            EngineError::new(
                coord_store_api::engine::ErrorClass::Limit,
                "restore receipt encode",
            )
        })?;
        StoreEnvelopeV1 {
            record_kind: RESTORED_RECORD_KIND,
            schema_version: RESTORED_SCHEMA_VERSION,
            payload,
        }
        .encode()
    }

    /// Decode a `checkpoint_v1` row value.
    pub fn decode(bytes: &[u8]) -> Result<Self, EngineError> {
        let env = StoreEnvelopeV1::decode(bytes)?;
        if env.record_kind != RESTORED_RECORD_KIND || env.schema_version != RESTORED_SCHEMA_VERSION
        {
            return Err(EngineError::new(
                coord_store_api::engine::ErrorClass::Corrupt,
                "unexpected restore receipt record",
            ));
        }
        let (record, rest): (RestoredV1, &[u8]) =
            postcard::take_from_bytes(&env.payload).map_err(|_| {
                EngineError::new(
                    coord_store_api::engine::ErrorClass::Corrupt,
                    "restore receipt decode",
                )
            })?;
        if !rest.is_empty() {
            return Err(EngineError::new(
                coord_store_api::engine::ErrorClass::Corrupt,
                "trailing restore receipt bytes",
            ));
        }
        Ok(record)
    }
}

/// The restore receipt of a store, or `None` when it is not a restored
/// one. A corrupt receipt is an error, never an absence.
pub fn restored_baseline<V: OrderedRead>(view: &V) -> Result<Option<RestoredV1>, RestoreError> {
    match view.get(Collection::CheckpointV1.id(), RESTORED_KEY)? {
        None => Ok(None),
        Some(bytes) => Ok(Some(RestoredV1::decode(&bytes)?)),
    }
}

/// Carry out an admitted restore into the engine of an inactive
/// generation of the *successor* cluster.
///
/// On success the generation holds the backup's common state at its
/// boundary, minus everything [`carried`] excludes, stamped with the
/// successor's configuration epoch and carrying the receipt. On any
/// error nothing may be selected.
pub fn restore_shared<E: LocalEngine>(
    engine: &mut E,
    artifact: &SharedManifestV1,
    chunks: ChunkSet,
    plan: &RestorePlan,
    fencing: &FencingAttestationV1,
    limits: &InstallLimits,
) -> Result<Restored, RestoreError> {
    // 1. Complete, well-formed bytes, and the artifact the plan admitted
    // rather than some other one with the same shape.
    let encoded = chunks.into_ordered().map_err(convert_install)?;
    let root = verify_shared(artifact, &encoded)?;
    if root != plan.captured {
        return Err(RestoreError::ArtifactMismatch { field: "root" });
    }
    if artifact.cluster != plan.source {
        return Err(RestoreError::ArtifactMismatch { field: "cluster" });
    }
    if artifact.domain != plan.domain {
        return Err(RestoreError::ArtifactMismatch { field: "domain" });
    }
    if artifact.boundary != plan.boundary {
        return Err(RestoreError::ArtifactMismatch { field: "boundary" });
    }
    // The attestation carried out is the attestation admitted: a plan
    // and a different operator's record of a different isolation are not
    // one restore.
    if fencing.backup != plan.backup {
        return Err(RestoreError::FencingMismatch { field: "backup" });
    }
    if fencing.abandoned != plan.source {
        return Err(RestoreError::FencingMismatch { field: "abandoned" });
    }
    if fencing.successor != plan.successor {
        return Err(RestoreError::FencingMismatch { field: "successor" });
    }

    // 2. The target: a fresh generation of the successor cluster. The
    // identity is checked against the successor and never against the
    // artifact, because disagreeing with the artifact is the point.
    {
        let view = engine.reader().snapshot()?;
        check_target(&view, plan)?;
    }

    // 3. The rows, in chunk order, through bounded durable transactions.
    let mut commits = 0u32;
    let mut rows_total = 0u64;
    let mut detached = 0u64;
    let mut dropped: Vec<(u16, u64)> = Vec::new();
    let mut pending_rows = 0u32;
    let mut pending_bytes = 0usize;
    let mut txn = engine.begin_write()?;
    for bytes in &encoded {
        let chunk = ChunkV1::decode(bytes).map_err(|_| RestoreError::MalformedChunk)?;
        for row in &chunk.rows {
            let collection = Collection::from_id(CollectionId(row.collection))
                .filter(|c| c.in_common_hash())
                .ok_or(RestoreError::ForeignCollection {
                    collection: row.collection,
                })?;
            if !carried(collection) {
                count(&mut dropped, row.collection);
                continue;
            }
            // A restored key attached to a lease nobody can renew would
            // be held forever by an authority that no longer exists, so
            // the attachment goes with the lease.
            let value = if collection == Collection::KvCurrentV1 {
                match detach(&row.value)? {
                    Some(rewritten) => {
                        detached += 1;
                        rewritten
                    }
                    None => row.value.clone(),
                }
            } else {
                row.value.clone()
            };
            txn.put(collection.id(), &row.key, &value)?;
            rows_total += 1;
            pending_rows += 1;
            pending_bytes += row.key.len() + value.len();
            if pending_rows >= limits.rows_per_commit.max(1)
                || pending_bytes >= limits.bytes_per_commit.max(1)
            {
                txn.commit_durable().map_err(RestoreError::Commit)?;
                commits += 1;
                pending_rows = 0;
                pending_bytes = 0;
                txn = engine.begin_write()?;
            }
        }
    }
    if pending_rows > 0 {
        txn.commit_durable().map_err(RestoreError::Commit)?;
        commits += 1;
        txn = engine.begin_write()?;
    }

    // 4. The boundary and the receipt, in one final durable transaction.
    // The receipt is last, so it exists only for a complete restore.
    let receipt = RestoredV1 {
        format: BACKUP_FORMAT_V1,
        source: plan.source,
        successor: plan.successor,
        domain: plan.domain,
        configuration: plan.configuration,
        boundary: plan.boundary,
        captured: root,
        taken_at: plan.rpo.taken_at,
        fencing: fencing.action.clone(),
        rows: rows_total,
        dropped,
        detached,
    };
    let meta = Collection::MetaV1.id();
    txn.put(
        meta,
        meta_fields::KV_REVISION,
        &codecs::encode_counter(plan.boundary.kv_revision.get())?,
    )?;
    txn.put(
        meta,
        meta_fields::RETENTION_FLOOR,
        &codecs::encode_counter(plan.boundary.retention_floor.get())?,
    )?;
    txn.put(
        meta,
        meta_fields::LEASE_AUTHORITY,
        &codecs::encode_counter(plan.boundary.lease_authority.get())?,
    )?;
    // The position and revision continue, so the new cluster's own
    // history is not rewound within itself; the epoch is the
    // successor's, because the artifact's epoch belongs to a
    // configuration this store deliberately does not hold.
    DurableMeta {
        stamp: DurableMeta::initial().stamp,
        frontier: ExecutionFrontier {
            configuration: plan.configuration,
            execution_position: plan.boundary.execution_position,
        },
    }
    .write(&mut txn)?;
    txn.put(
        Collection::CheckpointV1.id(),
        RESTORED_KEY,
        &receipt.encode()?,
    )?;
    txn.commit_durable().map_err(RestoreError::Commit)?;
    commits += 1;
    Ok(Restored { receipt, commits })
}

fn count(dropped: &mut Vec<(u16, u64)>, collection: u16) {
    match dropped.iter_mut().find(|(c, _)| *c == collection) {
        Some((_, n)) => *n += 1,
        None => dropped.push((collection, 1)),
    }
}

/// Clear a current row's lease attachment, or `None` where it had none.
fn detach(value: &[u8]) -> Result<Option<Vec<u8>>, RestoreError> {
    let mut entry = codecs::decode_current(value)?;
    if entry.lease.is_none() && entry.lease_generation.is_none() {
        return Ok(None);
    }
    entry.lease = None;
    entry.lease_generation = None;
    Ok(Some(codecs::encode_current(&entry)?))
}

/// The target must be a fresh generation of the *successor* cluster with
/// no state, no obligations and no earlier install or restore.
fn check_target<V: OrderedRead>(view: &V, plan: &RestorePlan) -> Result<(), RestoreError> {
    let meta = Collection::MetaV1.id();
    let identity = |field: &'static str, key: &[u8]| -> Result<Vec<u8>, RestoreError> {
        view.get(meta, key)?
            .ok_or(RestoreError::TargetIdentityMissing { field })
    };
    if identity("cluster", meta_fields::CLUSTER_ID)?.as_slice() != plan.successor.as_bytes() {
        return Err(RestoreError::TargetIdentityMismatch { field: "cluster" });
    }
    if identity("domain", meta_fields::DOMAIN_ID)?.as_slice() != plan.domain.as_bytes() {
        return Err(RestoreError::TargetIdentityMismatch { field: "domain" });
    }
    identity("replica", meta_fields::REPLICA_ID)?;
    // Neither a restore nor a catch-up install may already have
    // happened here: a restore writes a whole cluster's state and has
    // nothing to merge with.
    if restored_baseline(view)?.is_some()
        || installed_baseline(view).map_err(convert_install)?.is_some()
    {
        return Err(RestoreError::AlreadyRestored);
    }
    for collection in Collection::ALL {
        if (collection.in_common_hash() || collection == Collection::ProtocolV1)
            && !empty(view, collection)?
        {
            return Err(RestoreError::TargetNotEmpty {
                collection: collection.id().0,
            });
        }
    }
    for key in [
        meta_fields::APPLIED_STAMP,
        meta_fields::EXECUTION_FRONTIER,
        meta_fields::KV_REVISION,
        meta_fields::RETENTION_FLOOR,
        meta_fields::LEASE_AUTHORITY,
    ] {
        if view.get(meta, key)?.is_some() {
            return Err(RestoreError::TargetNotEmpty {
                collection: Collection::MetaV1.id().0,
            });
        }
    }
    Ok(())
}

fn empty<V: OrderedRead>(view: &V, collection: Collection) -> Result<bool, RestoreError> {
    let page = view.scan_page(
        collection.id(),
        &coord_store_api::engine::ScanRequest::all(1, 1 << 16),
    )?;
    Ok(page.rows.is_empty())
}

fn convert_install(e: crate::install::InstallError) -> RestoreError {
    match e {
        crate::install::InstallError::Verify(v) => RestoreError::Verify(v),
        crate::install::InstallError::MissingChunk { ordinal } => {
            RestoreError::Verify(VerifyError::ChunkDigest { ordinal })
        }
        crate::install::InstallError::Engine(e) => RestoreError::Engine(e),
        _ => RestoreError::MalformedChunk,
    }
}

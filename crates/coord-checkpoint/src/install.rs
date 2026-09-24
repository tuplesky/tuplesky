//! Installing a verified `SharedCheckpointV1` into an inactive generation
//! (task-50; design Sections 10.3, 17.6, 17.13).
//!
//! The install is the importing half of the artifact task-49 exports. It
//! runs against the engine of a generation that nothing selects yet, so
//! every failure and every crash leaves the previously selected state
//! untouched; the physical selection (directory sync, manifest, active
//! pointer) belongs to the storage lifecycle and happens only after this
//! returns success. [`select_installed`] is the selection step for an
//! install: it consumes the [`Installed`] a successful install returned and
//! re-reads the receipt from the staged generation before activating it, so
//! a generation whose install failed, was interrupted or never ran cannot
//! be selected through it. `InactiveGeneration::activate` itself stays
//! receipt-agnostic, because an offline migration selects a generation that
//! no install filled.
//!
//! What is checked before a single row is written:
//!
//! * **format, hashes, order, counts and bounds**, by
//!   [`crate::verify::verify_shared`] over the manifest and the complete
//!   chunk set. A [`ChunkSet`] additionally rejects a chunk whose ordinal,
//!   length or digest disagrees with its descriptor as it arrives, and
//!   refuses to produce a chunk sequence while any chunk is missing, so a
//!   missing or corrupt chunk blocks the install rather than truncating it.
//! * **origin**: the artifact's cluster and domain must equal both what the
//!   caller requires and the identity record the target generation already
//!   carries. A target without its own identity record is refused.
//! * **configuration**: an artifact from below the caller's admission epoch
//!   is stale and refused.
//! * **schema**: the collection summaries must be exactly this build's
//!   common registry (checked by verification) and every row's collection
//!   must be common; node-private collections are never written.
//! * **target state**: every common collection must be empty, `protocol_v1`
//!   must hold no obligations, the boundary rows of `meta_v1` must be
//!   absent and no install receipt may exist. The install never overwrites
//!   a store that already holds state.
//!
//! What the install writes: the verified rows, in chunk order, through
//! bounded durable transactions, and then one final durable transaction
//! with the boundary of `meta_v1` (KV revision, retention floor, lease
//! authority, execution frontier), a local applied stamp that represents
//! nothing yet, and the [`InstalledCheckpointV1`] receipt in
//! `checkpoint_v1`. The receipt is therefore written last and exists only
//! for a complete install: a crash at any earlier point leaves a generation
//! that [`installed_baseline`] reports as not installed.
//!
//! What the install never does: it writes no identity row, so the learner
//! keeps its own cluster/domain/replica/incarnation and inherits none of
//! the donor's; it writes no `protocol_v1` row, so no promise, vote or
//! obligation is created or reset and the target cannot vote from an
//! installed baseline; it carries no donor applied stamp or journal
//! sequence; it promotes nothing, since the configuration epoch it records
//! is the boundary's applied frontier and not an admission, a role or a
//! membership change; and it converts nothing between engines. Post-boundary
//! commands and unresolved closure are transferred separately (task-25);
//! a complete artifact is not by itself proof of install eligibility
//! against the quorum-safe recovery floor (task-53), which the caller
//! establishes. This is not `LocalRecoveryCheckpointV1` (task-j04) and
//! cannot recreate an existing voter's state.

use std::fmt;

use coord_storage::codecs;
use coord_storage::lowering::{DurableMeta, ExecutionFrontier};
use coord_storage_redb::lifecycle::InactiveGeneration;
use coord_storage_redb::{Generation, OpenError};
use coord_store_api::engine::{
    CollectionId, CommitFailure, EngineError, LocalEngine, OrderedRead, ScanRequest,
    SnapshotSource, WriteTxn,
};
use coord_store_api::envelope::StoreEnvelopeV1;
use coord_store_api::registry::{Collection, meta_fields};
use coord_types::identity::Digest32;
use coord_types::ids::{ClusterId, ConfigurationEpoch, DomainId};
use serde::{Deserialize, Serialize};

use crate::manifest::{CheckpointBoundary, ChunkV1, MAX_CHUNKS, SharedManifestV1};
use crate::verify::{VerifyError, verify_shared};

/// Record kind of the install receipt inside `checkpoint_v1`.
pub const INSTALLED_RECORD_KIND: u16 = 0x0001;
/// Schema version of the install receipt.
pub const INSTALLED_SCHEMA_VERSION: u16 = 1;
/// Key of the install receipt inside `checkpoint_v1`.
pub const INSTALLED_KEY: &[u8] = b"installed_shared_v1";

/// What the installing node requires of an artifact before it is allowed
/// anywhere near the store. Complete, well-hashed bytes are not eligibility.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InstallRequirements {
    /// Cluster/restore identity the node belongs to.
    pub cluster: ClusterId,
    /// Domain being caught up.
    pub domain: DomainId,
    /// Lowest configuration epoch accepted: the epoch the node was admitted
    /// under. An older artifact is stale and refused.
    pub minimum_configuration: ConfigurationEpoch,
}

/// Bounds of one install.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InstallLimits {
    /// Rows written before a durable commit.
    pub rows_per_commit: u32,
    /// Key plus value bytes written before a durable commit.
    pub bytes_per_commit: usize,
}

impl Default for InstallLimits {
    fn default() -> Self {
        InstallLimits {
            rows_per_commit: 4096,
            bytes_per_commit: 4 * 1024 * 1024,
        }
    }
}

/// The durable receipt of a completed install, held in `checkpoint_v1`.
/// It is node-private: it never enters a common digest, and it grants no
/// authority. Its presence is the only evidence that an install finished.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstalledCheckpointV1 {
    /// Artifact format installed.
    pub format: u16,
    /// Cluster of the artifact (equal to the node's own).
    pub cluster: ClusterId,
    /// Domain of the artifact (equal to the node's own).
    pub domain: DomainId,
    /// Configuration epoch the boundary was reached under.
    pub configuration: ConfigurationEpoch,
    /// Boundary the store now represents; catch-up resumes here.
    pub boundary: CheckpointBoundary,
    /// Verified root of the artifact.
    pub root: Digest32,
    /// Chunks installed.
    pub chunks: u32,
    /// Rows installed.
    pub rows: u64,
}

impl InstalledCheckpointV1 {
    /// Encode as a `checkpoint_v1` row value.
    pub fn encode(&self) -> Result<Vec<u8>, EngineError> {
        let payload = postcard::to_allocvec(self).map_err(|_| {
            EngineError::new(
                coord_store_api::engine::ErrorClass::Limit,
                "install receipt encode",
            )
        })?;
        StoreEnvelopeV1 {
            record_kind: INSTALLED_RECORD_KIND,
            schema_version: INSTALLED_SCHEMA_VERSION,
            payload,
        }
        .encode()
    }

    /// Decode a `checkpoint_v1` row value.
    pub fn decode(bytes: &[u8]) -> Result<Self, EngineError> {
        let env = StoreEnvelopeV1::decode(bytes)?;
        if env.record_kind != INSTALLED_RECORD_KIND
            || env.schema_version != INSTALLED_SCHEMA_VERSION
        {
            return Err(EngineError::new(
                coord_store_api::engine::ErrorClass::Corrupt,
                "unexpected install receipt record",
            ));
        }
        let (record, rest): (InstalledCheckpointV1, &[u8]) =
            postcard::take_from_bytes(&env.payload).map_err(|_| {
                EngineError::new(
                    coord_store_api::engine::ErrorClass::Corrupt,
                    "install receipt decode",
                )
            })?;
        if !rest.is_empty() {
            return Err(EngineError::new(
                coord_store_api::engine::ErrorClass::Corrupt,
                "trailing install receipt bytes",
            ));
        }
        Ok(record)
    }
}

/// What one completed install did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Installed {
    /// The receipt now durable in the generation.
    pub receipt: InstalledCheckpointV1,
    /// Durable transactions used.
    pub commits: u32,
}

/// Why an install was refused or stopped. Every variant is a hard stop; none
/// leaves a partially installed generation eligible for selection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InstallError {
    /// The artifact failed verification.
    Verify(VerifyError),
    /// A chunk of the manifest was never supplied.
    MissingChunk {
        /// Ordinal.
        ordinal: u32,
    },
    /// A supplied chunk claims an ordinal the manifest does not describe, or
    /// one already supplied.
    UnexpectedChunk {
        /// Ordinal.
        ordinal: u32,
    },
    /// A supplied chunk's bytes disagree with its descriptor.
    CorruptChunk {
        /// Ordinal.
        ordinal: u32,
    },
    /// A supplied chunk does not decode at all, so it names no ordinal and
    /// belongs to no descriptor.
    MalformedChunk,
    /// The artifact's cluster or domain differs from what was required or
    /// from the target generation's own identity record.
    OriginMismatch {
        /// Which field.
        field: &'static str,
    },
    /// The artifact predates the configuration epoch the node accepts.
    StaleConfiguration {
        /// Artifact epoch.
        found: ConfigurationEpoch,
        /// Lowest accepted epoch.
        minimum: ConfigurationEpoch,
    },
    /// The target generation carries no identity record of its own, so no
    /// origin could be checked; an install never lends one.
    TargetIdentityMissing {
        /// Which field.
        field: &'static str,
    },
    /// The target generation already holds state in this collection.
    TargetNotEmpty {
        /// Collection.
        collection: u16,
    },
    /// The target generation holds protocol obligations; an install never
    /// replaces or resets promises and votes.
    ExistingObligations,
    /// The target generation already carries an install receipt.
    AlreadyInstalled,
    /// The engine failed.
    Engine(EngineError),
    /// A durable commit failed. An indeterminate outcome leaves the staged
    /// generation unusable: it must be abandoned, never selected.
    Commit(CommitFailure),
}

impl fmt::Display for InstallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InstallError::Verify(e) => write!(f, "verify: {e}"),
            InstallError::Engine(e) => write!(f, "engine: {e}"),
            InstallError::Commit(e) => write!(f, "commit: {e}"),
            other => write!(f, "{other:?}"),
        }
    }
}

impl std::error::Error for InstallError {}

impl From<EngineError> for InstallError {
    fn from(e: EngineError) -> Self {
        InstallError::Engine(e)
    }
}

impl From<VerifyError> for InstallError {
    fn from(e: VerifyError) -> Self {
        InstallError::Verify(e)
    }
}

/// The chunks of one manifest, collected as they arrive.
///
/// Each chunk is checked against its descriptor (ordinal, encoded length,
/// digest and self-described ordinal) at the moment it is accepted, and the
/// set refuses to produce a sequence while any chunk is missing. A donor
/// that skips, duplicates, truncates or corrupts a chunk therefore blocks
/// the install instead of installing a hole.
#[derive(Clone, Debug)]
pub struct ChunkSet {
    slots: Vec<Option<Vec<u8>>>,
}

impl ChunkSet {
    /// An empty set sized for `manifest`.
    pub fn for_manifest(manifest: &SharedManifestV1) -> Result<Self, InstallError> {
        if manifest.chunks.len() > MAX_CHUNKS {
            return Err(InstallError::Verify(VerifyError::ChunkSequence));
        }
        Ok(ChunkSet {
            slots: vec![None; manifest.chunks.len()],
        })
    }

    /// Accept one encoded chunk, checking it against its descriptor.
    pub fn accept(
        &mut self,
        manifest: &SharedManifestV1,
        encoded: Vec<u8>,
    ) -> Result<u32, InstallError> {
        // An undecodable chunk names no ordinal, so it belongs to no
        // descriptor of any manifest.
        let chunk = ChunkV1::decode(&encoded).map_err(|_| InstallError::MalformedChunk)?;
        let ordinal = chunk.ordinal;
        let Some(descriptor) = manifest.chunks.get(ordinal as usize) else {
            return Err(InstallError::UnexpectedChunk { ordinal });
        };
        if descriptor.ordinal != ordinal {
            return Err(InstallError::UnexpectedChunk { ordinal });
        }
        if descriptor.bytes as usize != encoded.len()
            || ChunkV1::digest_of(&encoded) != descriptor.digest
        {
            return Err(InstallError::CorruptChunk { ordinal });
        }
        let slot = &mut self.slots[ordinal as usize];
        if slot.is_some() {
            return Err(InstallError::UnexpectedChunk { ordinal });
        }
        *slot = Some(encoded);
        Ok(ordinal)
    }

    /// Ordinals not yet accepted.
    pub fn missing(&self) -> Vec<u32> {
        self.slots
            .iter()
            .enumerate()
            .filter(|(_, s)| s.is_none())
            .map(|(i, _)| i as u32)
            .collect()
    }

    /// The complete chunk sequence in ordinal order, or the first missing
    /// ordinal.
    pub fn into_ordered(self) -> Result<Vec<Vec<u8>>, InstallError> {
        let mut out = Vec::with_capacity(self.slots.len());
        for (i, slot) in self.slots.into_iter().enumerate() {
            match slot {
                Some(bytes) => out.push(bytes),
                None => return Err(InstallError::MissingChunk { ordinal: i as u32 }),
            }
        }
        Ok(out)
    }
}

/// The install receipt of a store, or `None` when no install completed.
/// A corrupt receipt is an error, never an absence.
pub fn installed_baseline<V: OrderedRead>(
    view: &V,
) -> Result<Option<InstalledCheckpointV1>, InstallError> {
    match view.get(Collection::CheckpointV1.id(), INSTALLED_KEY)? {
        None => Ok(None),
        Some(bytes) => Ok(Some(InstalledCheckpointV1::decode(&bytes)?)),
    }
}

/// Why [`select_installed`] did not select a staged generation.
#[derive(Debug)]
pub enum SelectError {
    /// The staged generation carries no install receipt: its install never
    /// completed there. The staging was abandoned.
    NotInstalled,
    /// The staged generation's receipt is not the one the install returned,
    /// so the [`Installed`] belongs to another generation. The staging was
    /// abandoned.
    ReceiptMismatch,
    /// The receipt could not be read or decoded. The staging was abandoned.
    Receipt(InstallError),
    /// The lifecycle refused or failed the activation itself.
    Activate(OpenError),
}

impl fmt::Display for SelectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SelectError::NotInstalled => f.write_str("staged generation holds no install receipt"),
            SelectError::ReceiptMismatch => {
                f.write_str("staged generation's receipt differs from the completed install")
            }
            SelectError::Receipt(e) => write!(f, "install receipt: {e}"),
            SelectError::Activate(e) => write!(f, "activate: {e}"),
        }
    }
}

impl std::error::Error for SelectError {}

/// Select a staged generation that an install completed into.
///
/// `installed` is only produced by a successful [`install_shared`], so a
/// caller that caught a failed or indeterminate install has nothing to pass
/// here. The receipt is still re-read from the staged engine and must equal
/// the one returned: a receipt from another staging, or a staging whose
/// final commit never landed, is refused. A refused staging is abandoned,
/// since nothing may ever select it; if removing its directory fails, it
/// stays unreferenced, which is the state a crash before activation leaves
/// and which the next staging steps over.
pub fn select_installed(
    mut staged: InactiveGeneration,
    installed: Installed,
) -> Result<Generation, SelectError> {
    let durable = staged
        .engine()
        .reader()
        .snapshot()
        .map_err(InstallError::from)
        .and_then(|view| installed_baseline(&view));
    let refusal = match durable {
        Ok(Some(receipt)) if receipt == installed.receipt => None,
        Ok(Some(_)) => Some(SelectError::ReceiptMismatch),
        Ok(None) => Some(SelectError::NotInstalled),
        Err(e) => Some(SelectError::Receipt(e)),
    };
    if let Some(refusal) = refusal {
        let _ = staged.abandon();
        return Err(refusal);
    }
    staged.activate().map_err(SelectError::Activate)
}

/// Install a verified artifact into the engine of an inactive generation.
///
/// On success the generation holds exactly the artifact's common state at
/// its boundary plus the receipt; the caller may then select it with
/// [`select_installed`]. On any
/// error nothing may be selected: the staged generation is abandoned and
/// the previously selected one remains in force.
pub fn install_shared<E: LocalEngine>(
    engine: &mut E,
    manifest: &SharedManifestV1,
    chunks: ChunkSet,
    requirements: &InstallRequirements,
    limits: &InstallLimits,
) -> Result<Installed, InstallError> {
    // 1. Complete, well-formed bytes: every chunk present and verified
    // against the manifest, and the manifest against itself.
    let encoded = chunks.into_ordered()?;
    let root = verify_shared(manifest, &encoded)?;

    // 2. Origin and configuration required by the caller.
    if manifest.cluster != requirements.cluster {
        return Err(InstallError::OriginMismatch { field: "cluster" });
    }
    if manifest.domain != requirements.domain {
        return Err(InstallError::OriginMismatch { field: "domain" });
    }
    if manifest.configuration < requirements.minimum_configuration {
        return Err(InstallError::StaleConfiguration {
            found: manifest.configuration,
            minimum: requirements.minimum_configuration,
        });
    }

    // 3. Origin and emptiness of the target generation itself.
    {
        let view = engine.reader().snapshot()?;
        check_target(&view, manifest)?;
    }

    // 4. The rows, in chunk order, through bounded durable transactions.
    let mut commits = 0u32;
    let mut rows_total = 0u64;
    let mut pending_rows = 0u32;
    let mut pending_bytes = 0usize;
    let mut txn = engine.begin_write()?;
    for bytes in &encoded {
        let chunk = ChunkV1::decode(bytes).map_err(|_| InstallError::MalformedChunk)?;
        for row in &chunk.rows {
            let collection = Collection::from_id(CollectionId(row.collection))
                .filter(|c| c.in_common_hash())
                .ok_or(InstallError::Verify(VerifyError::ForeignCollection {
                    collection: row.collection,
                }))?;
            txn.put(collection.id(), &row.key, &row.value)?;
            rows_total += 1;
            pending_rows += 1;
            pending_bytes += row.bytes();
            if pending_rows >= limits.rows_per_commit.max(1)
                || pending_bytes >= limits.bytes_per_commit.max(1)
            {
                txn.commit_durable().map_err(InstallError::Commit)?;
                commits += 1;
                pending_rows = 0;
                pending_bytes = 0;
                txn = engine.begin_write()?;
            }
        }
    }
    if pending_rows > 0 {
        txn.commit_durable().map_err(InstallError::Commit)?;
        commits += 1;
        txn = engine.begin_write()?;
    }

    // 5. The boundary and the receipt, in one final durable transaction. The
    // receipt is the last durable write of the install, so it exists only
    // for a complete one.
    let receipt = InstalledCheckpointV1 {
        format: manifest.format,
        cluster: manifest.cluster,
        domain: manifest.domain,
        configuration: manifest.configuration,
        boundary: manifest.boundary,
        root,
        chunks: manifest.chunks.len() as u32,
        rows: rows_total,
    };
    let meta = Collection::MetaV1.id();
    txn.put(
        meta,
        meta_fields::KV_REVISION,
        &codecs::encode_counter(manifest.boundary.kv_revision.get())?,
    )?;
    txn.put(
        meta,
        meta_fields::RETENTION_FLOOR,
        &codecs::encode_counter(manifest.boundary.retention_floor.get())?,
    )?;
    txn.put(
        meta,
        meta_fields::LEASE_AUTHORITY,
        &codecs::encode_counter(manifest.boundary.lease_authority.get())?,
    )?;
    // The execution frontier is common state at the boundary; the applied
    // stamp is local and starts over, because this node has replayed no
    // journal of its own and inherits none of the donor's.
    DurableMeta {
        stamp: DurableMeta::initial().stamp,
        frontier: ExecutionFrontier {
            configuration: manifest.configuration,
            execution_position: manifest.boundary.execution_position,
        },
    }
    .write(&mut txn)?;
    txn.put(
        Collection::CheckpointV1.id(),
        INSTALLED_KEY,
        &receipt.encode()?,
    )?;
    txn.commit_durable().map_err(InstallError::Commit)?;
    commits += 1;
    Ok(Installed { receipt, commits })
}

/// The target must be this node's own freshly created generation for this
/// cluster and domain, with no state, no obligations and no earlier install.
fn check_target<V: OrderedRead>(view: &V, manifest: &SharedManifestV1) -> Result<(), InstallError> {
    let meta = Collection::MetaV1.id();
    let identity = |field: &'static str, key: &[u8]| -> Result<Vec<u8>, InstallError> {
        view.get(meta, key)?
            .ok_or(InstallError::TargetIdentityMissing { field })
    };
    if identity("cluster", meta_fields::CLUSTER_ID)?.as_slice() != manifest.cluster.as_bytes() {
        return Err(InstallError::OriginMismatch { field: "cluster" });
    }
    if identity("domain", meta_fields::DOMAIN_ID)?.as_slice() != manifest.domain.as_bytes() {
        return Err(InstallError::OriginMismatch { field: "domain" });
    }
    // The node keeps its own replica identity; the artifact carries none.
    identity("replica", meta_fields::REPLICA_ID)?;
    if installed_baseline(view)?.is_some() {
        return Err(InstallError::AlreadyInstalled);
    }
    if !empty(view, Collection::ProtocolV1)? {
        return Err(InstallError::ExistingObligations);
    }
    for collection in Collection::ALL {
        if collection.in_common_hash() && !empty(view, collection)? {
            return Err(InstallError::TargetNotEmpty {
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
            return Err(InstallError::TargetNotEmpty {
                collection: Collection::MetaV1.id().0,
            });
        }
    }
    Ok(())
}

fn empty<V: OrderedRead>(view: &V, collection: Collection) -> Result<bool, EngineError> {
    let page = view.scan_page(collection.id(), &ScanRequest::all(1, 1024 * 1024))?;
    Ok(page.rows.is_empty())
}

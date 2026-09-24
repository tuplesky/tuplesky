//! `LocalRecoveryCheckpointV1`: one incarnation's complete logical
//! storage at a local journal sequence (task-j04; design Sections
//! 17.16.1-17.16.4).
//!
//! It is the other artifact of Section 17.16.1, and almost the opposite
//! of [`crate::manifest::SharedCheckpointV1`]:
//!
//! | | shared | local |
//! |---|---|---|
//! | contents | common state only | *every* collection, node-private included |
//! | normalization | history and events to the replicated floor | none: it is this node's storage as it is |
//! | identity | cluster and domain | the exact `(cluster, domain, replica, incarnation)` origin |
//! | boundary | execution position and KV revision | a `LocalJournalSeq` |
//! | authority | catch-up lineage for a learner | replaces this node's own redo, through published lineage |
//!
//! The difference in contents is the whole point. A shared checkpoint
//! deliberately carries no promise, no unresolved vote and no stamp,
//! because a learner must inherit none of them. A local checkpoint is
//! what lets a replica reclaim journal prefix without forgetting an
//! obligation, so it has to carry exactly those: a promise the node made,
//! a vote it has not resolved, a payload of a command it has not
//! executed. Anything missing from it is something the node would deny
//! having done after the prefix that proved it was retired.
//!
//! Nothing here normalizes, and that is deliberate too. Two replicas of
//! one domain are not expected to produce equal local roots, so there is
//! nothing to converge; the root exists to bind an image to the pointer
//! that selects it, not to compare nodes.
//!
//! This is a same-engine lifecycle artifact. It is not a migration
//! interface, it is not an engine-conversion tool, and restoring a
//! shared or observer image cannot take its place: neither carries this
//! node's obligations.

use std::fmt;

use coord_journal_api::frontier::LOCAL_CHECKPOINT_FORMAT_V1;
use coord_journal_api::record::RecordOrigin;
use coord_storage::lowering::DurableMeta;
use coord_store_api::engine::{EngineError, OrderedRead, ScanRequest};
use coord_store_api::registry::Collection;
use coord_store_api::seq::StoreSeq;
use coord_types::identity::{Digest32, HashDomain};
use coord_types::ids::{ConfigurationEpoch, ExecutionPosition, LocalJournalSeq};
use serde::{Deserialize, Serialize};

use crate::manifest::{
    ArtifactError, CHUNK_TARGET_BYTES, ChunkDescriptorV1, ChunkV1, CollectionSummaryV1, MAX_CHUNKS,
    MAX_MANIFEST_BYTES, MAX_ROW_KEY_BYTES, MAX_ROW_VALUE_BYTES, RowV1,
};

/// What the checkpoint was pinned at: the applied stamp and execution
/// frontier of the snapshot it was read from.
///
/// It is evidence that the image is one consistent state and not a
/// mixture, and it is what recovery checks the journal suffix against.
/// It is node-private, which is exactly why no shared artifact carries
/// it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LocalPin {
    /// Store sequence of the applied stamp.
    pub applied: StoreSeq,
    /// Digest of the applied stamp.
    pub stamp_digest: Digest32,
    /// Configuration epoch of the execution frontier.
    pub configuration: ConfigurationEpoch,
    /// Execution position the projection had applied.
    pub execution_position: ExecutionPosition,
}

/// The manifest of a local recovery checkpoint.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalManifestV1 {
    /// Format.
    pub format: u16,
    /// The exact incarnation whose storage this is.
    pub origin: RecordOrigin,
    /// Represented sequence `C`: every local obligation through it is
    /// inside this image.
    pub represented: LocalJournalSeq,
    /// What the image was pinned at.
    pub pin: LocalPin,
    /// One summary per collection, in identifier order (every
    /// collection appears, possibly with zero rows).
    pub collections: Vec<CollectionSummaryV1>,
    /// Chunk descriptors in ordinal order.
    pub chunks: Vec<ChunkDescriptorV1>,
    /// Root digest over everything above.
    pub root: Digest32,
}

impl LocalManifestV1 {
    /// The root: format, origin, represented sequence, pin, collection
    /// summaries and chunk descriptors. Row contents enter only through
    /// the chunk digests.
    ///
    /// Under its own hash domain, so a local root can never be mistaken
    /// for a shared one even if the two ever described the same rows.
    pub fn compute_root(&self) -> Digest32 {
        let mut parts: Vec<Vec<u8>> = vec![
            self.format.to_be_bytes().to_vec(),
            self.origin.canonical_bytes().to_vec(),
            self.represented.to_be_bytes().to_vec(),
            self.pin.applied.journal_seq().to_be_bytes().to_vec(),
            self.pin.stamp_digest.0.to_vec(),
            self.pin.configuration.to_be_bytes().to_vec(),
            self.pin.execution_position.to_be_bytes().to_vec(),
        ];
        for c in &self.collections {
            let mut part = Vec::with_capacity(18);
            part.extend_from_slice(&c.collection.to_be_bytes());
            part.extend_from_slice(&c.rows.to_be_bytes());
            part.extend_from_slice(&c.bytes.to_be_bytes());
            parts.push(part);
        }
        for d in &self.chunks {
            let mut part = Vec::with_capacity(44);
            part.extend_from_slice(&d.ordinal.to_be_bytes());
            part.extend_from_slice(&d.rows.to_be_bytes());
            part.extend_from_slice(&d.bytes.to_be_bytes());
            part.extend_from_slice(&d.digest.0);
            parts.push(part);
        }
        let refs: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
        HashDomain::LocalCheckpointRoot.digest(&refs)
    }

    /// Total rows.
    pub fn rows(&self) -> u64 {
        self.collections.iter().map(|c| c.rows).sum()
    }

    /// Portable encoding, held to [`MAX_MANIFEST_BYTES`].
    pub fn encode(&self) -> Result<Vec<u8>, ArtifactError> {
        let bytes = postcard::to_allocvec(self).map_err(|_| ArtifactError::TooLarge)?;
        if bytes.len() > MAX_MANIFEST_BYTES {
            return Err(ArtifactError::TooLarge);
        }
        Ok(bytes)
    }

    /// Exact decoding with the bound checked before allocation. It does
    /// no structural verification; that is [`verify_local`].
    pub fn decode(bytes: &[u8]) -> Result<Self, ArtifactError> {
        if bytes.len() > MAX_MANIFEST_BYTES {
            return Err(ArtifactError::TooLarge);
        }
        postcard::take_from_bytes::<Self>(bytes)
            .map_err(|_| ArtifactError::Malformed)
            .and_then(|(value, rest)| {
                if rest.is_empty() {
                    Ok(value)
                } else {
                    Err(ArtifactError::TrailingBytes)
                }
            })
    }

    /// Digest of the encoded manifest: what a [`CheckpointPointerV1`]
    /// names, so a pointer selects one exact manifest and not merely a
    /// directory whose name matches.
    ///
    /// [`CheckpointPointerV1`]: coord_journal_api::frontier::CheckpointPointerV1
    pub fn digest(&self) -> Result<Digest32, ArtifactError> {
        Ok(manifest_digest(&self.encode()?))
    }
}

/// A complete local checkpoint in memory.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalCheckpointV1 {
    /// Manifest.
    pub manifest: LocalManifestV1,
    /// Chunks in ordinal order.
    pub chunks: Vec<ChunkV1>,
}

/// Bounds of one local export.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LocalLimits {
    /// Rows per engine page.
    pub page_rows: u32,
    /// Bytes per engine page (must admit the largest row).
    pub page_bytes: u32,
    /// Encoded bytes at which a chunk is closed.
    pub chunk_target_bytes: usize,
    /// Most chunks before the export is refused.
    pub max_chunks: usize,
}

impl Default for LocalLimits {
    fn default() -> Self {
        LocalLimits {
            page_rows: 1024,
            page_bytes: (MAX_ROW_KEY_BYTES + MAX_ROW_VALUE_BYTES) as u32 * 2,
            chunk_target_bytes: CHUNK_TARGET_BYTES,
            max_chunks: MAX_CHUNKS,
        }
    }
}

/// Why a local export or verification stopped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LocalError {
    /// The engine failed.
    Engine(EngineError),
    /// The applied stamp or execution frontier changed during the
    /// export: the view is not pinned and its rows cannot be mixed.
    ViewChanged,
    /// A row key or value exceeds the artifact bounds.
    RowTooLarge {
        /// Collection.
        collection: u16,
    },
    /// More chunks than the limit.
    TooManyChunks,
    /// The manifest does not fit what carries it.
    ManifestTooLarge,
    /// The format is not one this build reads.
    UnsupportedFormat {
        /// What the manifest says.
        format: u16,
    },
    /// The root does not match the manifest's own fields.
    RootMismatch,
    /// A chunk is missing, duplicated or out of order.
    Chunks,
    /// A chunk's bytes do not hash to its descriptor.
    ChunkDigest {
        /// Ordinal.
        ordinal: u32,
    },
    /// Descriptor counts, bounds or boundary keys disagree with the
    /// chunk.
    Descriptor {
        /// Ordinal.
        ordinal: u32,
    },
    /// Rows are not in canonical order, or a collection summary
    /// disagrees with the rows.
    Rows,
    /// A collection of the registry is missing from the summaries, or
    /// one that is not in the registry is present. A local checkpoint is
    /// complete or it is not a local checkpoint.
    Incomplete,
}

impl fmt::Display for LocalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LocalError::Engine(e) => write!(f, "engine: {e}"),
            other => write!(f, "{other:?}"),
        }
    }
}

impl std::error::Error for LocalError {}

impl From<EngineError> for LocalError {
    fn from(e: EngineError) -> Self {
        LocalError::Engine(e)
    }
}

/// Raw bytes of the applied stamp and execution frontier rows.
type Fence = (Option<Vec<u8>>, Option<Vec<u8>>);

/// Raw bytes of the two rows that prove the view did not move.
fn fence<V: OrderedRead>(view: &V) -> Result<Fence, EngineError> {
    use coord_store_api::registry::meta_fields;
    let meta = Collection::MetaV1.id();
    Ok((
        view.get(meta, meta_fields::APPLIED_STAMP)?,
        view.get(meta, meta_fields::EXECUTION_FRONTIER)?,
    ))
}

struct Chunker {
    target: usize,
    max_chunks: usize,
    current: Vec<RowV1>,
    current_bytes: usize,
    chunks: Vec<ChunkV1>,
    descriptors: Vec<ChunkDescriptorV1>,
}

impl Chunker {
    fn push(&mut self, row: RowV1) -> Result<(), LocalError> {
        let size = row.bytes() + 8;
        if !self.current.is_empty() && self.current_bytes + size > self.target {
            self.close()?;
        }
        self.current_bytes += size;
        self.current.push(row);
        Ok(())
    }

    fn close(&mut self) -> Result<(), LocalError> {
        if self.current.is_empty() {
            return Ok(());
        }
        if self.chunks.len() >= self.max_chunks {
            return Err(LocalError::TooManyChunks);
        }
        let rows = std::mem::take(&mut self.current);
        self.current_bytes = 0;
        let ordinal = self.chunks.len() as u32;
        let chunk = ChunkV1 { ordinal, rows };
        let encoded = chunk.encode().map_err(|_| LocalError::RowTooLarge {
            collection: chunk.rows[0].collection,
        })?;
        let first = &chunk.rows[0];
        let last = &chunk.rows[chunk.rows.len() - 1];
        self.descriptors.push(ChunkDescriptorV1 {
            ordinal,
            rows: chunk.rows.len() as u32,
            bytes: encoded.len() as u32,
            first: (first.collection, first.key.clone()),
            last: (last.collection, last.key.clone()),
            digest: ChunkV1::digest_of(&encoded),
        });
        self.chunks.push(chunk);
        Ok(())
    }
}

/// Export this incarnation's complete logical storage from `view`, which
/// must be a pinned snapshot whose materialization covers `represented`.
///
/// Every collection of the registry, every row, exactly as stored. There
/// is no filter and no normalization: a row this node holds and this
/// image omits is an obligation the node would deny having after the
/// journal prefix that proved it is reclaimed.
///
/// The caller owns the claim that `represented` is covered. This reads
/// storage and cannot see the journal; what it does is refuse to produce
/// an image from a view that moved under it, by re-reading the applied
/// stamp and execution frontier between collections.
pub fn export_local<V: OrderedRead>(
    view: &V,
    origin: RecordOrigin,
    represented: LocalJournalSeq,
    limits: &LocalLimits,
) -> Result<LocalCheckpointV1, LocalError> {
    let fence_before = fence(view)?;
    let meta = DurableMeta::read(view)?;
    let pin = LocalPin {
        applied: meta.stamp.store_seq(),
        stamp_digest: meta.stamp.last_batch_digest(),
        configuration: meta.frontier.configuration,
        execution_position: meta.frontier.execution_position,
    };
    let mut chunker = Chunker {
        target: limits.chunk_target_bytes.max(1),
        max_chunks: limits.max_chunks.clamp(1, MAX_CHUNKS),
        current: Vec::new(),
        current_bytes: 0,
        chunks: Vec::new(),
        descriptors: Vec::new(),
    };
    let mut summaries = Vec::new();
    for collection in Collection::ALL {
        let (rows, bytes) = traverse(view, collection, limits, &mut chunker)?;
        summaries.push(CollectionSummaryV1 {
            collection: collection.id().0,
            rows,
            bytes,
        });
        if fence(view)? != fence_before {
            return Err(LocalError::ViewChanged);
        }
    }
    chunker.close()?;
    let mut manifest = LocalManifestV1 {
        format: LOCAL_CHECKPOINT_FORMAT_V1,
        origin,
        represented,
        pin,
        collections: summaries,
        chunks: chunker.descriptors,
        root: Digest32([0; 32]),
    };
    manifest.root = manifest.compute_root();
    if manifest.encode().is_err() {
        return Err(LocalError::ManifestTooLarge);
    }
    Ok(LocalCheckpointV1 {
        manifest,
        chunks: chunker.chunks,
    })
}

/// Traverse one collection; returns rows and bytes included.
fn traverse<V: OrderedRead>(
    view: &V,
    collection: Collection,
    limits: &LocalLimits,
    chunker: &mut Chunker,
) -> Result<(u64, u64), LocalError> {
    let id = collection.id();
    let mut request = ScanRequest::all(limits.page_rows, limits.page_bytes);
    let mut rows = 0u64;
    let mut bytes = 0u64;
    loop {
        let page = view.scan_page(id, &request)?;
        for engine_row in &page.rows {
            if engine_row.key.len() > MAX_ROW_KEY_BYTES
                || engine_row.value.len() > MAX_ROW_VALUE_BYTES
            {
                return Err(LocalError::RowTooLarge { collection: id.0 });
            }
            let row = RowV1 {
                collection: id.0,
                key: engine_row.key.clone(),
                value: engine_row.value.clone(),
            };
            rows += 1;
            bytes += row.bytes() as u64;
            chunker.push(row)?;
        }
        if page.exhausted {
            return Ok((rows, bytes));
        }
        match page.rows.last() {
            Some(last) => request.resume_after = Some(last.key.clone()),
            None => {
                return Err(EngineError::new(
                    coord_store_api::engine::ErrorClass::Corrupt,
                    "empty non-exhausted page",
                )
                .into());
            }
        }
    }
}

/// Verify a local checkpoint against its own manifest, so nothing is
/// installed on the strength of bytes alone.
///
/// It recomputes the root and every chunk digest, and checks format,
/// completeness (every registry collection summarized), chunk order and
/// uniqueness, descriptor agreement, row order and per-collection counts.
/// It deliberately checks nothing about the *node*: whether this image
/// belongs to this incarnation is the pointer's question, not the
/// artifact's.
pub fn verify_local(checkpoint: &LocalCheckpointV1) -> Result<(), LocalError> {
    let manifest = &checkpoint.manifest;
    if manifest.format != LOCAL_CHECKPOINT_FORMAT_V1 {
        return Err(LocalError::UnsupportedFormat {
            format: manifest.format,
        });
    }
    if manifest.root != manifest.compute_root() {
        return Err(LocalError::RootMismatch);
    }
    // Complete, or it is not a local checkpoint: every collection of the
    // registry appears exactly once, in identifier order.
    let expected: Vec<u16> = Collection::ALL.iter().map(|c| c.id().0).collect();
    let present: Vec<u16> = manifest.collections.iter().map(|c| c.collection).collect();
    if present != expected {
        return Err(LocalError::Incomplete);
    }
    if manifest.chunks.len() != checkpoint.chunks.len() {
        return Err(LocalError::Chunks);
    }
    let mut counted: Vec<(u64, u64)> = vec![(0, 0); expected.len()];
    let mut previous: Option<(u16, Vec<u8>)> = None;
    for (index, chunk) in checkpoint.chunks.iter().enumerate() {
        let ordinal = index as u32;
        let descriptor = &manifest.chunks[index];
        if chunk.ordinal != ordinal || descriptor.ordinal != ordinal {
            return Err(LocalError::Chunks);
        }
        if chunk.rows.is_empty() {
            return Err(LocalError::Descriptor { ordinal });
        }
        let encoded = chunk
            .encode()
            .map_err(|_| LocalError::Descriptor { ordinal })?;
        if ChunkV1::digest_of(&encoded) != descriptor.digest {
            return Err(LocalError::ChunkDigest { ordinal });
        }
        if encoded.len() as u32 != descriptor.bytes
            || chunk.rows.len() as u32 != descriptor.rows
            || (chunk.rows[0].collection, chunk.rows[0].key.clone()) != descriptor.first
            || (
                chunk.rows[chunk.rows.len() - 1].collection,
                chunk.rows[chunk.rows.len() - 1].key.clone(),
            ) != descriptor.last
        {
            return Err(LocalError::Descriptor { ordinal });
        }
        for row in &chunk.rows {
            let here = (row.collection, row.key.clone());
            if previous.as_ref().is_some_and(|p| *p >= here) {
                return Err(LocalError::Rows);
            }
            let Some(slot) = expected.iter().position(|c| *c == row.collection) else {
                return Err(LocalError::Rows);
            };
            counted[slot].0 += 1;
            counted[slot].1 += row.bytes() as u64;
            previous = Some(here);
        }
    }
    for (summary, (rows, bytes)) in manifest.collections.iter().zip(counted) {
        if summary.rows != rows || summary.bytes != bytes {
            return Err(LocalError::Rows);
        }
    }
    Ok(())
}

/// The digest of an encoded manifest, computed from the bytes rather
/// than from a decoded value: a reader checks what it read against the
/// pointer before it trusts the decode.
pub fn manifest_digest(encoded: &[u8]) -> Digest32 {
    HashDomain::LocalCheckpointRoot.digest(&[encoded])
}

/// Why a local image could not be installed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InstallLocalError {
    /// The image does not verify.
    Invalid(LocalError),
    /// The target generation is not empty. A local image is the whole of
    /// this node's storage, so it is installed into a fresh generation
    /// and never merged into one that already holds rows.
    NotEmpty {
        /// First collection found non-empty.
        collection: u16,
    },
    /// The image belongs to another incarnation.
    ForeignOrigin,
    /// A row names a collection the registry does not have.
    ForeignCollection {
        /// Collection.
        collection: u16,
    },
    /// The engine failed.
    Engine(EngineError),
    /// A durable transaction did not commit.
    Commit(coord_store_api::engine::CommitFailure),
}

impl fmt::Display for InstallLocalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InstallLocalError::Engine(e) => write!(f, "engine: {e}"),
            InstallLocalError::Invalid(e) => write!(f, "invalid local checkpoint: {e}"),
            other => write!(f, "{other:?}"),
        }
    }
}

impl std::error::Error for InstallLocalError {}

impl From<EngineError> for InstallLocalError {
    fn from(e: EngineError) -> Self {
        InstallLocalError::Engine(e)
    }
}

/// Bounds of one local install.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InstallLocalLimits {
    /// Rows per durable transaction.
    pub rows_per_commit: u32,
    /// Bytes per durable transaction.
    pub bytes_per_commit: usize,
}

impl Default for InstallLocalLimits {
    fn default() -> Self {
        InstallLocalLimits {
            rows_per_commit: 4096,
            bytes_per_commit: 4 * 1024 * 1024,
        }
    }
}

/// Write a verified local image into an empty generation, restoring this
/// incarnation's storage as it was at the represented sequence.
///
/// Unlike [`crate::install::install_shared`] this writes *everything*,
/// identity and `protocol_v1` included, because it is the same node's
/// own state and not a donor's: a promise it made, a vote it has not
/// resolved and the stamp it was pinned at are precisely what must come
/// back. The stamp comes back with the rest, so the generation is
/// already at `C` and the ordinary attach replays `(C, J]` onto it
/// without anything here having to know about the journal.
///
/// `origin` is this node's, and an image of another incarnation is
/// refused: a local checkpoint is not a migration interface, and
/// installing someone else's obligations would be inheriting them.
pub fn install_local<E: coord_store_api::engine::LocalEngine>(
    engine: &mut E,
    checkpoint: &LocalCheckpointV1,
    origin: &RecordOrigin,
    limits: &InstallLocalLimits,
) -> Result<u64, InstallLocalError> {
    use coord_store_api::engine::{CollectionId, SnapshotSource, WriteTxn};

    verify_local(checkpoint).map_err(InstallLocalError::Invalid)?;
    if checkpoint.manifest.origin != *origin {
        return Err(InstallLocalError::ForeignOrigin);
    }
    {
        let view = engine.reader().snapshot()?;
        for collection in Collection::ALL {
            // One row, and room for it: a byte budget too small to
            // return the first row would report every collection empty
            // and make this check say nothing at all.
            let request = ScanRequest::all(1, (MAX_ROW_KEY_BYTES + MAX_ROW_VALUE_BYTES) as u32);
            if !view.scan_page(collection.id(), &request)?.rows.is_empty() {
                return Err(InstallLocalError::NotEmpty {
                    collection: collection.id().0,
                });
            }
        }
    }
    let mut rows = 0u64;
    let mut pending_rows = 0u32;
    let mut pending_bytes = 0usize;
    let mut txn = engine.begin_write()?;
    for chunk in &checkpoint.chunks {
        for row in &chunk.rows {
            let collection = Collection::from_id(CollectionId(row.collection)).ok_or(
                InstallLocalError::ForeignCollection {
                    collection: row.collection,
                },
            )?;
            txn.put(collection.id(), &row.key, &row.value)?;
            rows += 1;
            pending_rows += 1;
            pending_bytes += row.bytes();
            if pending_rows >= limits.rows_per_commit.max(1)
                || pending_bytes >= limits.bytes_per_commit.max(1)
            {
                txn.commit_durable().map_err(InstallLocalError::Commit)?;
                pending_rows = 0;
                pending_bytes = 0;
                txn = engine.begin_write()?;
            }
        }
    }
    // The last transaction is always committed, even when empty: an
    // image of nothing is still an installed image, and the caller must
    // not have to tell "no rows" from "not written".
    txn.commit_durable().map_err(InstallLocalError::Commit)?;
    Ok(rows)
}

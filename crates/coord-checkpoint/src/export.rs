//! Canonical export from one pinned view (design Sections 17.6, 17.12).
//!
//! The traversal reads the common collections of the registry in
//! identifier order, each in unsigned key order through bounded pages, and
//! decides per row whether it is common state at the boundary:
//!
//! * `payload_v1`: only commands present in `executed_v1` (an unexecuted
//!   payload is post-boundary evidence the donor supplies separately);
//! * `kv_history_v1`: for every key, the newest version at or below the
//!   replicated retention floor plus every newer version (what local
//!   garbage collection converges to, whatever its progress);
//! * `events_v1`: only revisions at or above the floor;
//! * every other common collection: every row.
//!
//! Rows past the boundary (an executed or retry record above the
//! execution position, a KV revision above the frontier) are an error, and
//! the applied stamp and execution frontier are re-read between
//! collections so a view that moved is refused, never mixed.

use std::fmt;

use coord_storage::codecs;
use coord_storage::lowering::DurableMeta;
use coord_store_api::engine::{EngineError, OrderedRead, ScanRequest};
use coord_store_api::registry::{Collection, meta_fields};
use coord_types::ids::{ClusterId, DomainId, KvRevision};
use coord_types::ordered_key;

use crate::manifest::{
    CHUNK_TARGET_BYTES, CheckpointBoundary, ChunkDescriptorV1, ChunkV1, CollectionSummaryV1,
    MAX_CHUNKS, MAX_ROW_KEY_BYTES, MAX_ROW_VALUE_BYTES, RowV1, SHARED_CHECKPOINT_FORMAT_V1,
    SharedCheckpointV1, SharedManifestV1,
};

/// The identity the exporting domain binds into the manifest. No node
/// identity, incarnation or boot is ever part of it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckpointOrigin {
    /// Cluster/restore identity.
    pub cluster: ClusterId,
    /// Domain.
    pub domain: DomainId,
}

/// Bounds of one export.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExportLimits {
    /// Rows per engine page.
    pub page_rows: u32,
    /// Bytes per engine page (must admit the largest row).
    pub page_bytes: u32,
    /// Encoded bytes at which a chunk is closed.
    pub chunk_target_bytes: usize,
    /// Most chunks before the export is refused.
    pub max_chunks: usize,
}

impl Default for ExportLimits {
    fn default() -> Self {
        ExportLimits {
            page_rows: 1024,
            page_bytes: (MAX_ROW_KEY_BYTES + MAX_ROW_VALUE_BYTES) as u32 * 2,
            chunk_target_bytes: CHUNK_TARGET_BYTES,
            max_chunks: MAX_CHUNKS,
        }
    }
}

/// Why an export stopped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExportError {
    /// The engine failed.
    Engine(EngineError),
    /// The applied stamp or execution frontier changed during the export:
    /// the view is not pinned and its rows cannot be mixed.
    ViewChanged,
    /// A row lies beyond the closed boundary.
    BeyondBoundary {
        /// Collection.
        collection: u16,
    },
    /// A row key or value exceeds the artifact bounds.
    RowTooLarge {
        /// Collection.
        collection: u16,
    },
    /// A row of a collection the exporter decodes did not decode.
    Corrupt {
        /// Collection.
        collection: u16,
    },
    /// More chunks than the limit.
    TooManyChunks,
}

impl fmt::Display for ExportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExportError::Engine(e) => write!(f, "engine: {e}"),
            other => write!(f, "{other:?}"),
        }
    }
}

impl std::error::Error for ExportError {}

impl From<EngineError> for ExportError {
    fn from(e: EngineError) -> Self {
        ExportError::Engine(e)
    }
}

/// Raw bytes of the applied stamp and execution frontier rows.
type Fence = (Option<Vec<u8>>, Option<Vec<u8>>);

/// Raw bytes of the two rows that prove the view did not move.
fn fence<V: OrderedRead>(view: &V) -> Result<Fence, EngineError> {
    let meta = Collection::MetaV1.id();
    Ok((
        view.get(meta, meta_fields::APPLIED_STAMP)?,
        view.get(meta, meta_fields::EXECUTION_FRONTIER)?,
    ))
}

struct ChunkWriter {
    target: usize,
    max_chunks: usize,
    current: Vec<RowV1>,
    current_bytes: usize,
    chunks: Vec<ChunkV1>,
    descriptors: Vec<ChunkDescriptorV1>,
}

impl ChunkWriter {
    fn push(&mut self, row: RowV1) -> Result<(), ExportError> {
        let size = row.bytes() + 8;
        if !self.current.is_empty() && self.current_bytes + size > self.target {
            self.close()?;
        }
        self.current_bytes += size;
        self.current.push(row);
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExportError> {
        if self.current.is_empty() {
            return Ok(());
        }
        if self.chunks.len() >= self.max_chunks {
            return Err(ExportError::TooManyChunks);
        }
        let rows = std::mem::take(&mut self.current);
        self.current_bytes = 0;
        let ordinal = self.chunks.len() as u32;
        let chunk = ChunkV1 { ordinal, rows };
        let encoded = chunk.encode().map_err(|_| ExportError::RowTooLarge {
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

/// Per-collection canonical filter state.
struct HistoryState {
    group: Option<(Vec<u8>, Vec<u8>)>,
    /// The newest version at or below the floor seen for `group`.
    below: Option<RowV1>,
}

fn corrupt(collection: Collection) -> ExportError {
    ExportError::Corrupt {
        collection: collection.id().0,
    }
}

fn beyond(collection: Collection) -> ExportError {
    ExportError::BeyondBoundary {
        collection: collection.id().0,
    }
}

/// Export the common state of `view` at its closed boundary.
pub fn export_shared<V: OrderedRead>(
    view: &V,
    origin: CheckpointOrigin,
    limits: &ExportLimits,
) -> Result<SharedCheckpointV1, ExportError> {
    let fence_before = fence(view)?;
    let meta = DurableMeta::read(view)?;
    let boundary = CheckpointBoundary {
        execution_position: meta.frontier.execution_position,
        kv_revision: codecs::read_kv_revision(view)?,
        retention_floor: codecs::read_retention_floor(view)?,
        lease_authority: codecs::read_lease_authority(view)?,
    };
    let mut writer = ChunkWriter {
        target: limits.chunk_target_bytes.max(1),
        max_chunks: limits.max_chunks.max(1),
        current: Vec::new(),
        current_bytes: 0,
        chunks: Vec::new(),
        descriptors: Vec::new(),
    };
    let mut summaries = Vec::new();
    for collection in Collection::ALL {
        if !collection.in_common_hash() {
            continue;
        }
        let (rows, bytes) = traverse(view, collection, &boundary, limits, &mut writer)?;
        summaries.push(CollectionSummaryV1 {
            collection: collection.id().0,
            rows,
            bytes,
        });
        if fence(view)? != fence_before {
            return Err(ExportError::ViewChanged);
        }
    }
    writer.close()?;
    let mut manifest = SharedManifestV1 {
        format: SHARED_CHECKPOINT_FORMAT_V1,
        cluster: origin.cluster,
        domain: origin.domain,
        configuration: meta.frontier.configuration,
        boundary,
        collections: summaries,
        chunks: writer.descriptors,
        root: coord_types::identity::Digest32([0; 32]),
    };
    manifest.root = manifest.compute_root();
    Ok(SharedCheckpointV1 {
        manifest,
        chunks: writer.chunks,
    })
}

/// Traverse one collection; returns rows and bytes included.
fn traverse<V: OrderedRead>(
    view: &V,
    collection: Collection,
    boundary: &CheckpointBoundary,
    limits: &ExportLimits,
    writer: &mut ChunkWriter,
) -> Result<(u64, u64), ExportError> {
    let id = collection.id();
    let mut request = ScanRequest::all(limits.page_rows, limits.page_bytes);
    let mut history = HistoryState {
        group: None,
        below: None,
    };
    let mut rows = 0u64;
    let mut bytes = 0u64;
    let mut include = |row: RowV1, writer: &mut ChunkWriter| -> Result<(), ExportError> {
        rows += 1;
        bytes += row.bytes() as u64;
        writer.push(row)
    };
    loop {
        let page = view.scan_page(id, &request)?;
        for engine_row in &page.rows {
            if engine_row.key.len() > MAX_ROW_KEY_BYTES
                || engine_row.value.len() > MAX_ROW_VALUE_BYTES
            {
                return Err(ExportError::RowTooLarge { collection: id.0 });
            }
            let row = RowV1 {
                collection: id.0,
                key: engine_row.key.clone(),
                value: engine_row.value.clone(),
            };
            match collection {
                Collection::PayloadV1 => {
                    // Only executed commands are closed state.
                    if view.get(Collection::ExecutedV1.id(), &row.key)?.is_some() {
                        include(row, writer)?;
                    }
                }
                Collection::ExecutedV1 => {
                    let record =
                        codecs::decode_executed(&row.value).map_err(|_| corrupt(collection))?;
                    if record.position > boundary.execution_position {
                        return Err(beyond(collection));
                    }
                    if record.revision.is_some_and(|r| r > boundary.kv_revision) {
                        return Err(beyond(collection));
                    }
                    include(row, writer)?;
                }
                Collection::RetryV1 => {
                    let record =
                        codecs::decode_retry(&row.value).map_err(|_| corrupt(collection))?;
                    if record.position > boundary.execution_position {
                        return Err(beyond(collection));
                    }
                    include(row, writer)?;
                }
                Collection::KvCurrentV1 => {
                    let entry =
                        codecs::decode_current(&row.value).map_err(|_| corrupt(collection))?;
                    if entry.mod_revision > boundary.kv_revision {
                        return Err(beyond(collection));
                    }
                    include(row, writer)?;
                }
                Collection::KvHistoryV1 => {
                    let decoded =
                        ordered_key::decode_history(&row.key).map_err(|_| corrupt(collection))?;
                    let revision = decoded.revision.ok_or_else(|| corrupt(collection))?;
                    if revision > boundary.kv_revision {
                        return Err(beyond(collection));
                    }
                    let group = (decoded.namespace.as_bytes().to_vec(), decoded.key);
                    if history.group.as_ref() != Some(&group) {
                        if let Some(kept) = history.below.take() {
                            include(kept, writer)?;
                        }
                        history.group = Some(group);
                    }
                    if revision <= boundary.retention_floor {
                        // Ascending revisions: the last one at or below the
                        // floor is the newest.
                        history.below = Some(row);
                    } else {
                        if let Some(kept) = history.below.take() {
                            include(kept, writer)?;
                        }
                        include(row, writer)?;
                    }
                }
                Collection::EventsV1 => {
                    let (revision, _) =
                        codecs::decode_event_key(&row.key).map_err(|_| corrupt(collection))?;
                    if revision > boundary.kv_revision {
                        return Err(beyond(collection));
                    }
                    if revision >= boundary.retention_floor || revision == KvRevision::ZERO {
                        include(row, writer)?;
                    }
                }
                _ => include(row, writer)?,
            }
        }
        if page.exhausted {
            break;
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
    if let Some(kept) = history.below.take() {
        include(kept, writer)?;
    }
    Ok((rows, bytes))
}

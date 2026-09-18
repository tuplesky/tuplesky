//! The frozen `SharedCheckpointV1` artifact (design Sections 17.6 and
//! 17.16.1).
//!
//! The manifest names what the checkpoint is (cluster, domain, epoch and
//! boundary) and what it contains (per-collection counts and per-chunk
//! digests); the root is the digest of exactly those fields, so equal
//! common state produces an equal root whatever the exporting node, its
//! incarnation, boot, stamps or physical layout. Rows are
//! `(collection, key, value)` in canonical order; a chunk is hashed by its
//! encoded bytes.

use std::fmt;

use coord_store_api::envelope::MAX_ENVELOPE_PAYLOAD;
use coord_types::identity::{Digest32, HashDomain};
use coord_types::ids::{
    ClusterId, ConfigurationEpoch, DomainId, ExecutionPosition, KvRevision, LeaseAuthorityEpoch,
};
use coord_types::wire_v1::{Frame, WireError, encode_frame};
use serde::{Deserialize, Serialize};

/// Artifact format version. Bumped only by a reviewed schema change;
/// unrelated to the record, wire and local checkpoint formats.
pub const SHARED_CHECKPOINT_FORMAT_V1: u16 = 1;
/// Target encoded size of one chunk (design Section 17.6).
pub const CHUNK_TARGET_BYTES: usize = 1024 * 1024;
/// Largest encoded chunk accepted: the target plus one maximal row.
pub const MAX_CHUNK_BYTES: usize =
    CHUNK_TARGET_BYTES + MAX_ROW_KEY_BYTES + MAX_ROW_VALUE_BYTES + 64;
/// Longest row key (an escaped ordered key of a maximal logical key).
pub const MAX_ROW_KEY_BYTES: usize = 32 * 1024;
/// Longest row value (an envelope at its payload limit).
pub const MAX_ROW_VALUE_BYTES: usize = MAX_ENVELOPE_PAYLOAD + 16;
/// Most chunks in one checkpoint.
pub const MAX_CHUNKS: usize = 1 << 20;

/// Raw kinds of the snapshot range (class limit 1 MiB + 64 KiB).
pub mod kinds {
    /// A `SharedManifestV1`.
    pub const MANIFEST: u16 = 0x0600;
    /// One `ChunkV1`.
    pub const CHUNK: u16 = 0x0601;
}

/// Schema version of the snapshot frames.
pub const VERSION: u16 = 1;

/// The closed boundary the checkpoint represents.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CheckpointBoundary {
    /// Highest established execution position applied.
    pub execution_position: ExecutionPosition,
    /// KV revision frontier.
    pub kv_revision: KvRevision,
    /// Replicated retention floor history and events are normalized to.
    pub retention_floor: KvRevision,
    /// Replicated lease expiry authority epoch.
    pub lease_authority: LeaseAuthorityEpoch,
}

/// Row count and bytes of one common collection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionSummaryV1 {
    /// Frozen collection identifier.
    pub collection: u16,
    /// Rows included.
    pub rows: u64,
    /// Key plus value bytes included.
    pub bytes: u64,
}

/// One chunk's descriptor.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkDescriptorV1 {
    /// Position in the checkpoint, from zero.
    pub ordinal: u32,
    /// Rows in the chunk.
    pub rows: u32,
    /// Encoded chunk bytes.
    pub bytes: u32,
    /// Collection and key of the first row.
    pub first: (u16, Vec<u8>),
    /// Collection and key of the last row.
    pub last: (u16, Vec<u8>),
    /// Digest of the encoded chunk.
    pub digest: Digest32,
}

/// The manifest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedManifestV1 {
    /// Format.
    pub format: u16,
    /// Cluster/restore identity.
    pub cluster: ClusterId,
    /// Domain.
    pub domain: DomainId,
    /// Configuration epoch the boundary was reached under.
    pub configuration: ConfigurationEpoch,
    /// Boundary.
    pub boundary: CheckpointBoundary,
    /// One summary per common collection, in identifier order (every
    /// common collection appears, possibly with zero rows).
    pub collections: Vec<CollectionSummaryV1>,
    /// Chunk descriptors in ordinal order.
    pub chunks: Vec<ChunkDescriptorV1>,
    /// Root digest over everything above.
    pub root: Digest32,
}

impl SharedManifestV1 {
    /// The root: format, identity, boundary, collection summaries and
    /// chunk descriptors (ordinal, rows, bytes, digest). Row contents enter
    /// only through the chunk digests; nothing node-private enters at all.
    pub fn compute_root(&self) -> Digest32 {
        let mut parts: Vec<Vec<u8>> = vec![
            self.format.to_be_bytes().to_vec(),
            self.cluster.as_bytes().to_vec(),
            self.domain.as_bytes().to_vec(),
            self.configuration.to_be_bytes().to_vec(),
            self.boundary.execution_position.to_be_bytes().to_vec(),
            self.boundary.kv_revision.to_be_bytes().to_vec(),
            self.boundary.retention_floor.to_be_bytes().to_vec(),
            self.boundary.lease_authority.to_be_bytes().to_vec(),
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
        HashDomain::SharedCheckpointRoot.digest(&refs)
    }

    /// Total rows.
    pub fn rows(&self) -> u64 {
        self.collections.iter().map(|c| c.rows).sum()
    }

    /// Portable encoding.
    pub fn encode(&self) -> Result<Vec<u8>, ArtifactError> {
        postcard::to_allocvec(self).map_err(|_| ArtifactError::TooLarge)
    }

    /// Exact decoding (no structural verification; see `verify`).
    pub fn decode(bytes: &[u8]) -> Result<Self, ArtifactError> {
        exact(bytes)
    }

    /// Encode as a snapshot frame.
    pub fn frame(&self) -> Result<Vec<u8>, ArtifactError> {
        let payload = self.encode()?;
        encode_frame(kinds::MANIFEST, VERSION, &payload).map_err(ArtifactError::Wire)
    }

    /// Decode from a snapshot frame.
    pub fn from_frame(frame: &Frame) -> Result<Self, ArtifactError> {
        check_frame(frame, kinds::MANIFEST)?;
        exact(&frame.payload)
    }
}

/// One row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowV1 {
    /// Frozen collection identifier.
    pub collection: u16,
    /// Ordered key bytes.
    pub key: Vec<u8>,
    /// Value bytes (a `StoreEnvelopeV1` or the collection's frozen value).
    pub value: Vec<u8>,
}

impl RowV1 {
    /// Key plus value bytes.
    pub fn bytes(&self) -> usize {
        self.key.len() + self.value.len()
    }
}

/// One chunk: rows in canonical order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkV1 {
    /// Ordinal (repeated so a chunk file is self-describing).
    pub ordinal: u32,
    /// Rows.
    pub rows: Vec<RowV1>,
}

impl ChunkV1 {
    /// Portable encoding.
    pub fn encode(&self) -> Result<Vec<u8>, ArtifactError> {
        let bytes = postcard::to_allocvec(self).map_err(|_| ArtifactError::TooLarge)?;
        if bytes.len() > MAX_CHUNK_BYTES {
            return Err(ArtifactError::TooLarge);
        }
        Ok(bytes)
    }

    /// Exact decoding with the chunk bound checked before allocation.
    pub fn decode(bytes: &[u8]) -> Result<Self, ArtifactError> {
        if bytes.len() > MAX_CHUNK_BYTES {
            return Err(ArtifactError::TooLarge);
        }
        exact(bytes)
    }

    /// Digest of encoded chunk bytes.
    pub fn digest_of(encoded: &[u8]) -> Digest32 {
        HashDomain::SharedCheckpointChunk.digest(&[encoded])
    }

    /// Encode as a snapshot frame.
    pub fn frame(&self) -> Result<Vec<u8>, ArtifactError> {
        let payload = self.encode()?;
        encode_frame(kinds::CHUNK, VERSION, &payload).map_err(ArtifactError::Wire)
    }

    /// Decode from a snapshot frame.
    pub fn from_frame(frame: &Frame) -> Result<Self, ArtifactError> {
        check_frame(frame, kinds::CHUNK)?;
        Self::decode(&frame.payload)
    }
}

/// The whole checkpoint in memory: manifest plus chunks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SharedCheckpointV1 {
    /// Manifest.
    pub manifest: SharedManifestV1,
    /// Chunks in ordinal order.
    pub chunks: Vec<ChunkV1>,
}

/// Why an artifact could not be encoded or decoded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArtifactError {
    /// Larger than its bound.
    TooLarge,
    /// Not a valid encoding.
    Malformed,
    /// Bytes after the encoding.
    TrailingBytes,
    /// Frame problem.
    Wire(WireError),
}

impl fmt::Display for ArtifactError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ArtifactError::Wire(e) => write!(f, "frame: {e}"),
            other => write!(f, "{other:?}"),
        }
    }
}

impl std::error::Error for ArtifactError {}

fn exact<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, ArtifactError> {
    let (value, rest): (T, &[u8]) =
        postcard::take_from_bytes(bytes).map_err(|_| ArtifactError::Malformed)?;
    if !rest.is_empty() {
        return Err(ArtifactError::TrailingBytes);
    }
    Ok(value)
}

fn check_frame(frame: &Frame, kind: u16) -> Result<(), ArtifactError> {
    if frame.kind != kind {
        return Err(ArtifactError::Wire(WireError::UnsupportedKind {
            kind: frame.kind,
        }));
    }
    if frame.version != VERSION {
        return Err(ArtifactError::Wire(WireError::MalformedPayload));
    }
    Ok(())
}

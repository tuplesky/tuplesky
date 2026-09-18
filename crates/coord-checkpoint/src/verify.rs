//! Verification of a `SharedCheckpointV1` (design Section 17.6: "row
//! order/uniqueness/counts/bounds, hashes and indexes" before install).

use std::fmt;

use coord_store_api::registry::Collection;
use coord_types::identity::Digest32;

use crate::manifest::{
    ChunkV1, MAX_CHUNK_BYTES, MAX_CHUNKS, MAX_ROW_KEY_BYTES, MAX_ROW_VALUE_BYTES,
    SHARED_CHECKPOINT_FORMAT_V1, SharedManifestV1,
};

/// Why a checkpoint failed verification. Every variant is a hard stop.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VerifyError {
    /// Unsupported format.
    UnsupportedFormat {
        /// Format found.
        found: u16,
    },
    /// The manifest's root is not the digest of its fields.
    RootMismatch,
    /// The collection summaries are not exactly the common registry in
    /// order.
    CollectionsMismatch,
    /// Chunk count or ordinals disagree with the descriptors.
    ChunkSequence,
    /// A chunk exceeds its bound.
    ChunkTooLarge {
        /// Ordinal.
        ordinal: u32,
    },
    /// A chunk's encoded digest differs from its descriptor.
    ChunkDigest {
        /// Ordinal.
        ordinal: u32,
    },
    /// A chunk's descriptor (rows, bytes, first, last) is wrong.
    ChunkDescriptor {
        /// Ordinal.
        ordinal: u32,
    },
    /// An empty chunk.
    EmptyChunk {
        /// Ordinal.
        ordinal: u32,
    },
    /// A row of a collection that is not common, or unregistered.
    ForeignCollection {
        /// Collection.
        collection: u16,
    },
    /// Rows are not strictly ascending by (collection, key).
    OutOfOrder {
        /// Ordinal of the offending chunk.
        ordinal: u32,
    },
    /// A row key or value exceeds its bound.
    RowTooLarge {
        /// Ordinal.
        ordinal: u32,
    },
    /// A collection's rows or bytes differ from its summary.
    CountMismatch {
        /// Collection.
        collection: u16,
    },
}

impl fmt::Display for VerifyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for VerifyError {}

/// Verify a manifest against its chunks (given as encoded bytes in ordinal
/// order, exactly as transferred). Returns the verified root.
pub fn verify_shared(
    manifest: &SharedManifestV1,
    encoded_chunks: &[Vec<u8>],
) -> Result<Digest32, VerifyError> {
    if manifest.format != SHARED_CHECKPOINT_FORMAT_V1 {
        return Err(VerifyError::UnsupportedFormat {
            found: manifest.format,
        });
    }
    if manifest.compute_root() != manifest.root {
        return Err(VerifyError::RootMismatch);
    }
    let common: Vec<u16> = Collection::ALL
        .iter()
        .filter(|c| c.in_common_hash())
        .map(|c| c.id().0)
        .collect();
    let listed: Vec<u16> = manifest.collections.iter().map(|c| c.collection).collect();
    if listed != common {
        return Err(VerifyError::CollectionsMismatch);
    }
    if manifest.chunks.len() != encoded_chunks.len() || manifest.chunks.len() > MAX_CHUNKS {
        return Err(VerifyError::ChunkSequence);
    }
    let mut rows = vec![0u64; manifest.collections.len()];
    let mut bytes = vec![0u64; manifest.collections.len()];
    let mut previous: Option<(u16, Vec<u8>)> = None;
    for (i, (descriptor, encoded)) in manifest.chunks.iter().zip(encoded_chunks).enumerate() {
        let ordinal = i as u32;
        if descriptor.ordinal != ordinal {
            return Err(VerifyError::ChunkSequence);
        }
        if encoded.len() > MAX_CHUNK_BYTES {
            return Err(VerifyError::ChunkTooLarge { ordinal });
        }
        if ChunkV1::digest_of(encoded) != descriptor.digest {
            return Err(VerifyError::ChunkDigest { ordinal });
        }
        let chunk = ChunkV1::decode(encoded).map_err(|_| VerifyError::ChunkDigest { ordinal })?;
        if chunk.ordinal != ordinal {
            return Err(VerifyError::ChunkSequence);
        }
        let Some(first) = chunk.rows.first() else {
            return Err(VerifyError::EmptyChunk { ordinal });
        };
        let last = &chunk.rows[chunk.rows.len() - 1];
        if descriptor.rows as usize != chunk.rows.len()
            || descriptor.bytes as usize != encoded.len()
            || descriptor.first != (first.collection, first.key.clone())
            || descriptor.last != (last.collection, last.key.clone())
        {
            return Err(VerifyError::ChunkDescriptor { ordinal });
        }
        for row in &chunk.rows {
            let Some(index) = common.iter().position(|c| *c == row.collection) else {
                return Err(VerifyError::ForeignCollection {
                    collection: row.collection,
                });
            };
            if row.key.len() > MAX_ROW_KEY_BYTES || row.value.len() > MAX_ROW_VALUE_BYTES {
                return Err(VerifyError::RowTooLarge { ordinal });
            }
            let position = (row.collection, row.key.clone());
            if previous.as_ref().is_some_and(|p| *p >= position) {
                return Err(VerifyError::OutOfOrder { ordinal });
            }
            previous = Some(position);
            rows[index] += 1;
            bytes[index] += row.bytes() as u64;
        }
    }
    for (i, summary) in manifest.collections.iter().enumerate() {
        if summary.rows != rows[i] || summary.bytes != bytes[i] {
            return Err(VerifyError::CountMismatch {
                collection: summary.collection,
            });
        }
    }
    Ok(manifest.root)
}

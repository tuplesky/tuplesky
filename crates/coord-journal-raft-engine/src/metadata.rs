//! Indexed key/value metadata (design Sections 17.3.1-17.3.2, 17.16.4).
//!
//! raft-engine keeps a small in-memory-indexed key/value map per region.
//! The reserved region [`METADATA_REGION`] (`0`, never a stream) holds the
//! journal identity, the allocator high-water mark and one row per stream
//! mapping. Each stream's own region holds one `pointer` row naming the
//! sequence of its latest durable checkpoint publication. Every value is a
//! bounded, versioned postcard [`MetadataValueV1`]; payloads and history
//! never enter the indexed KV.

use core::fmt;

use coord_journal_api::stream::{StorageStreamId, StreamMappingV1};
use coord_types::ids::{ClusterId, ReplicaId};
use serde::{Deserialize, Serialize};

/// Region holding journal-wide metadata. Zero is never a stream.
pub const METADATA_REGION: u64 = 0;
/// Journal directory format version.
pub const JOURNAL_FORMAT_V1: u16 = 1;
/// Largest metadata value accepted.
pub const MAX_METADATA_VALUE_BYTES: usize = 256;

/// Key of the identity row in the metadata region.
pub const IDENTITY_KEY: &[u8] = b"identity";
/// Key of the allocator high-water row in the metadata region.
pub const HIGH_WATER_KEY: &[u8] = b"high_water";
/// Prefix of stream mapping rows in the metadata region.
pub const STREAM_KEY_PREFIX: &[u8] = b"stream/";
/// Key of the pointer row in a stream's region.
pub const POINTER_KEY: &[u8] = b"pointer";

/// Identity a journal directory was created for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalIdentityV1 {
    /// Directory format.
    pub format: u16,
    /// Cluster/restore identity.
    pub cluster: ClusterId,
    /// Replica.
    pub replica: ReplicaId,
}

/// Versioned metadata value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataValueV1 {
    /// Journal identity.
    Identity(JournalIdentityV1),
    /// Allocator high-water mark.
    HighWater(u64),
    /// Stream mapping.
    Mapping(StreamMappingV1),
    /// Sequence of the latest checkpoint publication record in a stream.
    Pointer(u64),
}

/// Why a metadata value was rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetadataError {
    /// Value longer than [`MAX_METADATA_VALUE_BYTES`].
    TooLarge,
    /// Not a valid encoding.
    Malformed,
    /// Bytes remained after a complete value.
    TrailingBytes,
    /// A different variant than the key requires.
    WrongKind,
}

impl fmt::Display for MetadataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            MetadataError::TooLarge => "metadata value too large",
            MetadataError::Malformed => "metadata value malformed",
            MetadataError::TrailingBytes => "trailing bytes after metadata value",
            MetadataError::WrongKind => "metadata value of the wrong kind",
        };
        f.write_str(text)
    }
}

impl std::error::Error for MetadataError {}

/// Mapping row key of a stream.
pub fn stream_key(stream: StorageStreamId) -> Vec<u8> {
    let mut key = STREAM_KEY_PREFIX.to_vec();
    key.extend_from_slice(&stream.get().to_be_bytes());
    key
}

/// Exclusive end of the mapping-row key range.
pub fn stream_key_end() -> Vec<u8> {
    let mut key = STREAM_KEY_PREFIX.to_vec();
    key.extend_from_slice(&[0xff; 9]);
    key
}

/// Encode a value.
pub fn encode_value(value: &MetadataValueV1) -> Result<Vec<u8>, MetadataError> {
    let bytes = postcard::to_allocvec(value).map_err(|_| MetadataError::TooLarge)?;
    if bytes.len() > MAX_METADATA_VALUE_BYTES {
        return Err(MetadataError::TooLarge);
    }
    Ok(bytes)
}

/// Decode a value exactly.
pub fn decode_value(bytes: &[u8]) -> Result<MetadataValueV1, MetadataError> {
    if bytes.len() > MAX_METADATA_VALUE_BYTES {
        return Err(MetadataError::TooLarge);
    }
    let (value, rest): (MetadataValueV1, &[u8]) =
        postcard::take_from_bytes(bytes).map_err(|_| MetadataError::Malformed)?;
    if !rest.is_empty() {
        return Err(MetadataError::TrailingBytes);
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use coord_journal_api::stream::{ShardId, StreamKey};
    use coord_types::ids::{DomainId, ReplicaIncarnation};

    #[test]
    fn values_round_trip_exactly() {
        let mapping = StreamMappingV1 {
            stream: StorageStreamId::FIRST,
            key: StreamKey {
                cluster: ClusterId([1; 16]),
                domain: DomainId([2; 16]),
                incarnation: ReplicaIncarnation::new(3).unwrap(),
            },
            shard: ShardId::new(1).unwrap(),
            retired: false,
        };
        for v in [
            MetadataValueV1::Identity(JournalIdentityV1 {
                format: JOURNAL_FORMAT_V1,
                cluster: ClusterId([1; 16]),
                replica: ReplicaId([9; 16]),
            }),
            MetadataValueV1::HighWater(u64::MAX),
            MetadataValueV1::Mapping(mapping),
            MetadataValueV1::Pointer(7),
        ] {
            let bytes = encode_value(&v).unwrap();
            assert_eq!(decode_value(&bytes), Ok(v));
            let mut trailing = bytes.clone();
            trailing.push(0);
            assert_eq!(decode_value(&trailing), Err(MetadataError::TrailingBytes));
            assert_eq!(
                decode_value(&bytes[..bytes.len() - 1]),
                Err(MetadataError::Malformed)
            );
        }
        assert_eq!(
            decode_value(&[0; MAX_METADATA_VALUE_BYTES + 1]),
            Err(MetadataError::TooLarge)
        );
        assert!(stream_key(StorageStreamId::FIRST) < stream_key_end());
        assert!(stream_key(StorageStreamId::from_durable(u64::MAX).unwrap()) < stream_key_end());
    }
}

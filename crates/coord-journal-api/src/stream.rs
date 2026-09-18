//! Stream identities and durable allocation (design Section 17.3.1).
//!
//! Every local `(cluster, domain, replica_incarnation)` owns one
//! [`StorageStreamId`]. Identifiers are allocated from a persisted
//! high-water mark, so two domains, two incarnations of one domain, or two
//! clusters on one shard never share an identifier and a retired identifier
//! is never handed out again while old files or evidence may still name it.
//! Nothing here hashes an identity into a `u64`.
//!
//! An allocation is usable only after its mapping (and the high-water mark)
//! is durable: [`StreamAllocator::allocate`] yields a `Reserved` stream,
//! [`StreamAllocator::mapping_durable`] promotes it, and
//! [`StreamAllocator::usable`] refuses anything else.

use alloc::collections::BTreeMap;
use alloc::vec::Vec;
use core::fmt;

use coord_types::error::CounterOverflow;
use coord_types::ids::{ClusterId, DomainId, ReplicaIncarnation};
use serde::{Deserialize, Serialize};

/// Largest shard index plus one. A small bounded shard set shares disks
/// across domains and defines the real failure blast radius.
pub const MAX_SHARDS: u16 = 64;

/// Identity of one journal stream on this node. Zero is never a stream.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StorageStreamId(u64);

impl StorageStreamId {
    /// The first identifier an allocator hands out.
    pub const FIRST: StorageStreamId = StorageStreamId(1);

    /// Rebuild an identifier from durable mapping metadata. Zero is
    /// rejected; the allocator additionally checks it against the persisted
    /// high-water mark.
    pub const fn from_durable(raw: u64) -> Result<Self, StreamError> {
        if raw == 0 {
            Err(StreamError::ZeroStream)
        } else {
            Ok(StorageStreamId(raw))
        }
    }

    /// Raw value for the engine's region/stream mapping.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Debug for StorageStreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StorageStreamId({})", self.0)
    }
}

/// Shard a stream is placed on.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ShardId(u16);

impl ShardId {
    /// Construct, rejecting indexes at or above [`MAX_SHARDS`].
    pub const fn new(index: u16) -> Result<Self, StreamError> {
        if index >= MAX_SHARDS {
            Err(StreamError::ShardOutOfRange { index })
        } else {
            Ok(ShardId(index))
        }
    }

    /// Raw index.
    pub const fn get(self) -> u16 {
        self.0
    }
}

impl fmt::Debug for ShardId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ShardId({})", self.0)
    }
}

/// The local identity a stream is allocated for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StreamKey {
    /// Cluster/restore identity.
    pub cluster: ClusterId,
    /// Domain.
    pub domain: DomainId,
    /// Replica incarnation; a new incarnation gets a new stream.
    pub incarnation: ReplicaIncarnation,
}

/// Persisted allocator high-water mark: the largest identifier ever
/// allocated. Recovered before any allocation; never lowered.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct StreamHighWater(u64);

impl StreamHighWater {
    /// No stream allocated yet.
    pub const NONE: StreamHighWater = StreamHighWater(0);

    /// Rebuild from durable metadata.
    pub const fn from_durable(raw: u64) -> Self {
        StreamHighWater(raw)
    }

    /// Raw value.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Durable mapping metadata of one stream. Persisted (with the high-water
/// mark) before the stream is used; kept while the stream is retired so the
/// identifier stays reserved.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamMappingV1 {
    /// Allocated identifier.
    pub stream: StorageStreamId,
    /// Identity the stream serves.
    pub key: StreamKey,
    /// Shard placement.
    pub shard: ShardId,
    /// Whether the stream was retired (files may still exist; never reuse).
    pub retired: bool,
}

/// Lifecycle state of an allocated stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StreamState {
    /// Allocated in memory; the mapping is not yet durable. Unusable.
    Reserved,
    /// Mapping durable; appends may use the stream.
    Durable,
    /// Retired; the identifier stays reserved forever.
    Retired,
}

/// Why an allocation, restore or use was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StreamError {
    /// Zero is never a stream identifier.
    ZeroStream,
    /// Shard index at or above [`MAX_SHARDS`].
    ShardOutOfRange {
        /// Offending index.
        index: u16,
    },
    /// The key already owns a stream (possibly retired); allocation is
    /// never repeated for the same identity.
    KeyAlreadyMapped {
        /// Existing stream.
        stream: StorageStreamId,
    },
    /// The stream is allocated but its mapping is not durable yet.
    MappingNotDurable,
    /// The stream is unknown to this allocator.
    Unknown,
    /// The stream is retired.
    Retired,
    /// Durable metadata names one identifier twice.
    DuplicateStream,
    /// Durable metadata maps one key to two streams.
    DuplicateKey,
    /// Durable metadata names an identifier above the persisted high-water
    /// mark: a nested allocation the allocator never made.
    AboveHighWater,
    /// The identifier space is exhausted; identifiers never wrap.
    Overflow,
}

impl fmt::Display for StreamError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StreamError::ZeroStream => f.write_str("zero is not a stream"),
            StreamError::ShardOutOfRange { index } => write!(f, "shard {index} out of range"),
            StreamError::KeyAlreadyMapped { stream } => {
                write!(f, "key already mapped to {stream:?}")
            }
            StreamError::MappingNotDurable => f.write_str("stream mapping not durable"),
            StreamError::Unknown => f.write_str("unknown stream"),
            StreamError::Retired => f.write_str("stream retired"),
            StreamError::DuplicateStream => f.write_str("duplicate stream identifier"),
            StreamError::DuplicateKey => f.write_str("duplicate stream key"),
            StreamError::AboveHighWater => f.write_str("stream above high-water mark"),
            StreamError::Overflow => f.write_str("stream identifier space exhausted"),
        }
    }
}

impl core::error::Error for StreamError {}

impl From<CounterOverflow> for StreamError {
    fn from(_: CounterOverflow) -> Self {
        StreamError::Overflow
    }
}

/// The stream allocator of one node.
#[derive(Clone, Debug, Default)]
pub struct StreamAllocator {
    high_water: StreamHighWater,
    by_key: BTreeMap<StreamKey, StorageStreamId>,
    streams: BTreeMap<StorageStreamId, (StreamMappingV1, StreamState)>,
}

impl StreamAllocator {
    /// Fresh allocator (genesis).
    pub const fn new() -> Self {
        StreamAllocator {
            high_water: StreamHighWater::NONE,
            by_key: BTreeMap::new(),
            streams: BTreeMap::new(),
        }
    }

    /// Rebuild from durable metadata. Every restored mapping is `Durable`
    /// (or `Retired`); duplicates and identifiers above the high-water mark
    /// are rejected rather than repaired.
    pub fn restore(
        high_water: StreamHighWater,
        mappings: impl IntoIterator<Item = StreamMappingV1>,
    ) -> Result<Self, StreamError> {
        let mut allocator = StreamAllocator {
            high_water,
            by_key: BTreeMap::new(),
            streams: BTreeMap::new(),
        };
        for mapping in mappings {
            if mapping.stream.get() > high_water.get() {
                return Err(StreamError::AboveHighWater);
            }
            if allocator.streams.contains_key(&mapping.stream) {
                return Err(StreamError::DuplicateStream);
            }
            if allocator.by_key.contains_key(&mapping.key) {
                return Err(StreamError::DuplicateKey);
            }
            let state = if mapping.retired {
                StreamState::Retired
            } else {
                StreamState::Durable
            };
            allocator.by_key.insert(mapping.key, mapping.stream);
            allocator.streams.insert(mapping.stream, (mapping, state));
        }
        Ok(allocator)
    }

    /// Persisted high-water mark.
    pub const fn high_water(&self) -> StreamHighWater {
        self.high_water
    }

    /// Allocate a stream for `key`. The returned mapping and the new
    /// high-water mark must be durable before [`Self::mapping_durable`] is
    /// called; until then the stream is `Reserved` and unusable. A key that
    /// already owns a stream, retired or not, is refused.
    pub fn allocate(
        &mut self,
        key: StreamKey,
        shard: ShardId,
    ) -> Result<StreamMappingV1, StreamError> {
        if let Some(stream) = self.by_key.get(&key) {
            return Err(StreamError::KeyAlreadyMapped { stream: *stream });
        }
        let raw = self
            .high_water
            .get()
            .checked_add(1)
            .ok_or(StreamError::Overflow)?;
        let stream = StorageStreamId(raw);
        let mapping = StreamMappingV1 {
            stream,
            key,
            shard,
            retired: false,
        };
        self.high_water = StreamHighWater(raw);
        self.by_key.insert(key, stream);
        self.streams
            .insert(stream, (mapping, StreamState::Reserved));
        Ok(mapping)
    }

    /// Record that the mapping of `stream` is durable.
    pub fn mapping_durable(&mut self, stream: StorageStreamId) -> Result<(), StreamError> {
        match self.streams.get_mut(&stream) {
            None => Err(StreamError::Unknown),
            Some((_, state @ StreamState::Reserved)) => {
                *state = StreamState::Durable;
                Ok(())
            }
            Some((_, StreamState::Durable)) => Ok(()),
            Some((_, StreamState::Retired)) => Err(StreamError::Retired),
        }
    }

    /// The mapping of a stream that may be appended to: known, durable and
    /// not retired.
    pub fn usable(&self, stream: StorageStreamId) -> Result<&StreamMappingV1, StreamError> {
        match self.streams.get(&stream) {
            None => Err(StreamError::Unknown),
            Some((_, StreamState::Reserved)) => Err(StreamError::MappingNotDurable),
            Some((_, StreamState::Retired)) => Err(StreamError::Retired),
            Some((mapping, StreamState::Durable)) => Ok(mapping),
        }
    }

    /// Retire a stream. Its identifier and key stay reserved; the retired
    /// mapping must be persisted like any other.
    pub fn retire(&mut self, stream: StorageStreamId) -> Result<StreamMappingV1, StreamError> {
        match self.streams.get_mut(&stream) {
            None => Err(StreamError::Unknown),
            Some((_, StreamState::Reserved)) => Err(StreamError::MappingNotDurable),
            Some((mapping, state)) => {
                mapping.retired = true;
                *state = StreamState::Retired;
                Ok(*mapping)
            }
        }
    }

    /// State of a stream, if allocated.
    pub fn state(&self, stream: StorageStreamId) -> Option<StreamState> {
        self.streams.get(&stream).map(|(_, s)| *s)
    }

    /// Stream owned by `key`, if any (retired included).
    pub fn lookup(&self, key: &StreamKey) -> Option<StorageStreamId> {
        self.by_key.get(key).copied()
    }

    /// Every mapping in identifier order (what must be persisted).
    pub fn mappings(&self) -> Vec<StreamMappingV1> {
        self.streams.values().map(|(m, _)| *m).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(domain: u8, incarnation: u64) -> StreamKey {
        StreamKey {
            cluster: ClusterId([1; 16]),
            domain: DomainId([domain; 16]),
            incarnation: ReplicaIncarnation::new(incarnation).unwrap(),
        }
    }

    #[test]
    fn identifiers_are_distinct_across_domains_and_incarnations() {
        let mut a = StreamAllocator::new();
        let shard = ShardId::new(0).unwrap();
        let s1 = a.allocate(key(1, 1), shard).unwrap().stream;
        let s2 = a.allocate(key(2, 1), shard).unwrap().stream;
        let s3 = a.allocate(key(1, 2), shard).unwrap().stream;
        assert_eq!(s1, StorageStreamId::FIRST);
        assert!(s1 < s2 && s2 < s3);
        assert_eq!(a.high_water(), StreamHighWater(3));
        assert_eq!(
            a.allocate(key(1, 1), shard),
            Err(StreamError::KeyAlreadyMapped { stream: s1 })
        );
    }

    #[test]
    fn reserved_streams_are_unusable_until_durable() {
        let mut a = StreamAllocator::new();
        let m = a.allocate(key(1, 1), ShardId::new(3).unwrap()).unwrap();
        assert_eq!(a.usable(m.stream), Err(StreamError::MappingNotDurable));
        assert_eq!(a.state(m.stream), Some(StreamState::Reserved));
        a.mapping_durable(m.stream).unwrap();
        assert_eq!(a.usable(m.stream), Ok(&m));
        assert_eq!(
            a.usable(StorageStreamId::from_durable(9).unwrap()),
            Err(StreamError::Unknown)
        );
    }

    #[test]
    fn retired_identifiers_are_never_recycled() {
        let mut a = StreamAllocator::new();
        let shard = ShardId::new(0).unwrap();
        let m = a.allocate(key(1, 1), shard).unwrap();
        assert_eq!(a.retire(m.stream), Err(StreamError::MappingNotDurable));
        a.mapping_durable(m.stream).unwrap();
        let retired = a.retire(m.stream).unwrap();
        assert!(retired.retired);
        assert_eq!(a.usable(m.stream), Err(StreamError::Retired));
        assert_eq!(
            a.allocate(key(1, 1), shard),
            Err(StreamError::KeyAlreadyMapped { stream: m.stream })
        );
        let next = a.allocate(key(1, 2), shard).unwrap().stream;
        assert!(next > m.stream);
        // Restoring from the persisted mappings keeps the reservation.
        let restored = StreamAllocator::restore(a.high_water(), a.mappings()).unwrap();
        assert_eq!(restored.state(m.stream), Some(StreamState::Retired));
        assert_eq!(restored.state(next), Some(StreamState::Durable));
        assert_eq!(restored.lookup(&key(1, 1)), Some(m.stream));
    }

    #[test]
    fn restore_rejects_duplicates_and_nested_allocation() {
        let shard = ShardId::new(0).unwrap();
        let m1 = StreamMappingV1 {
            stream: StorageStreamId::from_durable(1).unwrap(),
            key: key(1, 1),
            shard,
            retired: false,
        };
        let same_id = StreamMappingV1 {
            key: key(2, 1),
            ..m1
        };
        let same_key = StreamMappingV1 {
            stream: StorageStreamId::from_durable(2).unwrap(),
            ..m1
        };
        let above = StreamMappingV1 {
            stream: StorageStreamId::from_durable(3).unwrap(),
            key: key(3, 1),
            ..m1
        };
        let hw = StreamHighWater::from_durable(2);
        assert_eq!(
            StreamAllocator::restore(hw, [m1, same_id]).err(),
            Some(StreamError::DuplicateStream)
        );
        assert_eq!(
            StreamAllocator::restore(hw, [m1, same_key]).err(),
            Some(StreamError::DuplicateKey)
        );
        assert_eq!(
            StreamAllocator::restore(hw, [m1, above]).err(),
            Some(StreamError::AboveHighWater)
        );
        assert_eq!(
            StorageStreamId::from_durable(0),
            Err(StreamError::ZeroStream)
        );
        assert_eq!(
            ShardId::new(MAX_SHARDS),
            Err(StreamError::ShardOutOfRange { index: MAX_SHARDS })
        );
    }

    #[test]
    fn exhausted_identifier_space_stops() {
        let mut a = StreamAllocator::restore(StreamHighWater::from_durable(u64::MAX), []).unwrap();
        assert_eq!(
            a.allocate(key(1, 1), ShardId::new(0).unwrap()),
            Err(StreamError::Overflow)
        );
        assert_eq!(a.high_water(), StreamHighWater::from_durable(u64::MAX));
    }
}

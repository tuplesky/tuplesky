//! The narrow journal-engine contract (design Sections 17.3.1-17.3.3, 17.8
//! and 17.16.3).
//!
//! Normative semantics an adapter must provide:
//!
//! 1. `append_group` returns success only after the whole nonempty group is
//!    synced; the engine's byte count is reported as evidence, and the exact
//!    per-stream sequences and barriers come from the caller-owned
//!    [`GroupWrite`], never from the engine.
//! 2. Records of one stream are stored at their `LocalJournalSeq` as the
//!    entry index; payload and history live in entries, indexed metadata
//!    stays small.
//! 3. `durable_head` and `read_suffix` report the actual valid recovered
//!    records. A decode error is a failure, never end of data or optional
//!    absence.
//! 4. `retire_prefix` may retire only entries represented by a durably
//!    published checkpoint pointer, and the compaction is itself synced.
//!    Engine purge suggestions never authorize loss of required history.
//! 5. Stream mapping metadata is durable before a stream is used.
//! 6. Failures are typed: definite only with specific noncommit evidence,
//!    otherwise indeterminate; corruption quarantines.

use alloc::vec::Vec;
use core::num::NonZeroU32;

use coord_types::ids::LocalJournalSeq;

use crate::failure::{JournalError, JournalFailure};
use crate::frontier::CheckpointPointerV1;
use crate::group::{GroupReceipt, GroupWrite};
use crate::record::JournalRecordV1;
use crate::stream::{StorageStreamId, StreamHighWater, StreamMappingV1};

/// Budget of one suffix read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadBudget {
    /// Maximum records in the page.
    pub max_records: NonZeroU32,
    /// Maximum encoded bytes in the page.
    pub max_bytes: NonZeroU32,
}

impl ReadBudget {
    /// Construct; zero budgets are raised to one.
    pub fn new(max_records: u32, max_bytes: u32) -> Self {
        ReadBudget {
            max_records: NonZeroU32::new(max_records.max(1)).expect("non-zero"),
            max_bytes: NonZeroU32::new(max_bytes.max(1)).expect("non-zero"),
        }
    }
}

/// One page of a stream's suffix, in sequence order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordPage {
    /// Records, contiguous and ascending.
    pub records: Vec<JournalRecordV1>,
    /// Whether the durable head was reached.
    pub exhausted: bool,
}

/// A shared journal engine (one per shard set), driven by the journal
/// worker. Not an actor-facing type: actors see barriers and events.
pub trait JournalEngine {
    /// Append a nonempty group with sync before success. The receipt maps
    /// success to the caller's exact stream sequences and barriers.
    fn append_group(&mut self, group: &GroupWrite) -> Result<GroupReceipt, JournalFailure>;

    /// Durable head of a stream as recovered from valid records
    /// (`LocalJournalSeq::ZERO` for an empty stream).
    fn durable_head(&self, stream: StorageStreamId) -> Result<LocalJournalSeq, JournalError>;

    /// Read records strictly after `after`, bounded by `budget`.
    fn read_suffix(
        &self,
        stream: StorageStreamId,
        after: LocalJournalSeq,
        budget: ReadBudget,
    ) -> Result<RecordPage, JournalError>;

    /// Retire entries of `stream` through `pointer.represented`. The pointer
    /// must already be durable in the journal; the compaction is synced.
    fn retire_prefix(
        &mut self,
        stream: StorageStreamId,
        pointer: &CheckpointPointerV1,
    ) -> Result<(), JournalFailure>;

    /// Durable stream mapping metadata and its high-water mark.
    fn mappings(&self) -> Result<(StreamHighWater, Vec<StreamMappingV1>), JournalError>;

    /// Persist a mapping and the new high-water mark before the stream is
    /// used.
    fn persist_mapping(
        &mut self,
        high_water: StreamHighWater,
        mapping: &StreamMappingV1,
    ) -> Result<(), JournalFailure>;
}

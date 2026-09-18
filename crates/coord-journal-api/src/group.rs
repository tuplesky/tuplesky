//! Grouped writes and exact completion mapping (design Section 17.3.3).
//!
//! A [`GroupWrite`] is a nonempty, bounded batch of entries from several
//! streams (at most one entry per stream, matching the one-outstanding-batch
//! rule). The engine's write returns a byte count; the caller turns that
//! success into exact per-stream completions and barriers through
//! [`GroupWrite::receipt`]. Larger valid atomic records use the separately
//! bounded [`GroupLimits::LARGE_RECORD`] path rather than illegal splitting.

use alloc::vec::Vec;
use core::fmt;

use coord_core::effect::BarrierId;
use coord_core::event::StorageEvent;
use coord_types::ids::LocalJournalSeq;

use crate::head::WrittenBytes;
use crate::record::{JournalRecordV1, MAX_RECORD_BYTES, RecordError};
use crate::stream::StorageStreamId;

/// Bounds of one group.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct GroupLimits {
    /// Most records across all entries.
    pub max_records: usize,
    /// Most encoded bytes across all entries.
    pub max_bytes: usize,
}

impl GroupLimits {
    /// Initial fair-scheduler targets: 64 transitions, 256 KiB.
    pub const DEFAULT: GroupLimits = GroupLimits {
        max_records: 64,
        max_bytes: 256 * 1024,
    };
    /// The admitted path for one large atomic record.
    pub const LARGE_RECORD: GroupLimits = GroupLimits {
        max_records: 1,
        max_bytes: MAX_RECORD_BYTES,
    };
}

/// Why an entry or group was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum GroupError {
    /// An entry without records.
    EmptyEntry,
    /// A group without entries has no receipt and is never written.
    EmptyGroup,
    /// A record's origin names another stream.
    StreamMismatch,
    /// Records are not contiguous ascending.
    NotContiguous,
    /// A record's predecessor is not the previous record's digest.
    ChainBroken,
    /// The stream already has an entry in this group.
    StreamAlreadyInGroup,
    /// The barrier already names an entry in this group.
    DuplicateBarrier,
    /// The group's record budget would be exceeded.
    TooManyRecords,
    /// The group's byte budget would be exceeded.
    TooManyBytes,
    /// A record failed to encode.
    Record(RecordError),
}

impl fmt::Display for GroupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GroupError::Record(e) => write!(f, "record: {e}"),
            other => write!(f, "{other:?}"),
        }
    }
}

impl core::error::Error for GroupError {}

impl From<RecordError> for GroupError {
    fn from(e: RecordError) -> Self {
        GroupError::Record(e)
    }
}

/// One stream's contribution to a group: contiguous chained records under
/// one barrier.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupEntry {
    barrier: BarrierId,
    stream: StorageStreamId,
    records: Vec<JournalRecordV1>,
    bytes: usize,
}

impl GroupEntry {
    /// Build an entry, checking stream, contiguity and the digest chain.
    pub fn new(
        barrier: BarrierId,
        stream: StorageStreamId,
        records: Vec<JournalRecordV1>,
    ) -> Result<Self, GroupError> {
        let Some(first) = records.first() else {
            return Err(GroupError::EmptyEntry);
        };
        let mut bytes = 0usize;
        let mut expected_seq = first.seq();
        let mut expected_pred = first.predecessor();
        for r in &records {
            if r.origin().stream != stream {
                return Err(GroupError::StreamMismatch);
            }
            if r.seq() != expected_seq {
                return Err(GroupError::NotContiguous);
            }
            if r.predecessor() != expected_pred {
                return Err(GroupError::ChainBroken);
            }
            bytes = bytes.saturating_add(r.encoded_len()?);
            expected_seq = r
                .seq()
                .checked_next()
                .map_err(|_| GroupError::NotContiguous)?;
            expected_pred = r.digest();
        }
        Ok(GroupEntry {
            barrier,
            stream,
            records,
            bytes,
        })
    }

    /// Barrier.
    pub const fn barrier(&self) -> BarrierId {
        self.barrier
    }
    /// Stream.
    pub const fn stream(&self) -> StorageStreamId {
        self.stream
    }
    /// Records.
    pub fn records(&self) -> &[JournalRecordV1] {
        &self.records
    }
    /// First sequence.
    pub fn first(&self) -> LocalJournalSeq {
        self.records[0].seq()
    }
    /// Last sequence.
    pub fn last(&self) -> LocalJournalSeq {
        self.records[self.records.len() - 1].seq()
    }
    /// Encoded bytes.
    pub const fn bytes(&self) -> usize {
        self.bytes
    }
}

/// A nonempty bounded multi-stream write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupWrite {
    limits: GroupLimits,
    entries: Vec<GroupEntry>,
    records: usize,
    bytes: usize,
}

impl GroupWrite {
    /// Empty group under `limits`; it must receive an entry before it can
    /// be written.
    pub const fn new(limits: GroupLimits) -> Self {
        GroupWrite {
            limits,
            entries: Vec::new(),
            records: 0,
            bytes: 0,
        }
    }

    /// Add an entry. Refused when its stream or barrier is already in the
    /// group or a budget would be exceeded (the group is left unchanged).
    pub fn push(&mut self, entry: GroupEntry) -> Result<(), GroupError> {
        if self.entries.iter().any(|e| e.stream == entry.stream) {
            return Err(GroupError::StreamAlreadyInGroup);
        }
        if self.entries.iter().any(|e| e.barrier == entry.barrier) {
            return Err(GroupError::DuplicateBarrier);
        }
        let records = self.records + entry.records.len();
        if records > self.limits.max_records {
            return Err(GroupError::TooManyRecords);
        }
        let bytes = self.bytes.saturating_add(entry.bytes);
        if bytes > self.limits.max_bytes {
            return Err(GroupError::TooManyBytes);
        }
        self.records = records;
        self.bytes = bytes;
        self.entries.push(entry);
        Ok(())
    }

    /// Limits.
    pub const fn limits(&self) -> GroupLimits {
        self.limits
    }
    /// Entries in submission order.
    pub fn entries(&self) -> &[GroupEntry] {
        &self.entries
    }
    /// Whether the group has no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    /// Total records.
    pub const fn record_count(&self) -> usize {
        self.records
    }
    /// Total encoded bytes.
    pub const fn bytes(&self) -> usize {
        self.bytes
    }

    /// Map a successful synced write to the exact completions the caller
    /// reserved. `written` is recorded as evidence only.
    pub fn receipt(&self, written: WrittenBytes) -> Result<GroupReceipt, GroupError> {
        if self.entries.is_empty() {
            return Err(GroupError::EmptyGroup);
        }
        Ok(GroupReceipt {
            written,
            completions: self
                .entries
                .iter()
                .map(|e| JournalCompletion {
                    barrier: e.barrier,
                    stream: e.stream,
                    first: e.first(),
                    last: e.last(),
                })
                .collect(),
        })
    }
}

/// Exact durable completion of one entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct JournalCompletion {
    /// Barrier completed.
    pub barrier: BarrierId,
    /// Stream.
    pub stream: StorageStreamId,
    /// First durable sequence of the entry.
    pub first: LocalJournalSeq,
    /// Last durable sequence of the entry.
    pub last: LocalJournalSeq,
}

impl JournalCompletion {
    /// The storage fact delivered to the actor: `JournalDurable`, never
    /// `Materialized` and never establishment.
    pub const fn storage_event(&self) -> StorageEvent {
        StorageEvent::JournalDurable {
            barrier_id: self.barrier,
            journal_seq: self.last,
        }
    }
}

/// Result of a successful group write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupReceipt {
    /// Engine byte count (evidence, not a sequence).
    pub written: WrittenBytes,
    /// One completion per entry, in submission order.
    pub completions: Vec<JournalCompletion>,
}

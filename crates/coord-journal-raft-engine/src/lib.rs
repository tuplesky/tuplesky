//! Pinned raft-engine mapping of the shared journal contract (task-j02;
//! design Sections 16.4, 17.3.1-17.3.3, 17.15).
//!
//! This crate maps `coord-journal-api` onto the reviewed `tikv/raft-engine`
//! revision with `default-features = false`:
//!
//! * [`codec`]: a bounded postcard `ValueCodec` for `JournalRecordV1` and
//!   the `MessageExt` that makes the record's `LocalJournalSeq` the engine
//!   entry index. Decoding is exact (digest re-derived, trailing bytes and
//!   oversized values refused); the engine's protobuf, bincode and JSON
//!   codecs are never used.
//! * [`metadata`]: the small indexed key/value metadata kept in the
//!   reserved region `0` (journal identity, allocator high-water mark, one
//!   row per stream mapping) and the per-stream pointer row. Payloads and
//!   history live in entries, never in the indexed KV.
//! * [`journal`]: [`journal::RaftEngineJournal`], the `JournalEngine`
//!   implementation: explicit create / open-existing with identity and
//!   format checks, validation of every group against the accepted durable
//!   head before anything is submitted, one nonempty `LogBatch` per group
//!   written with `sync = true`, the engine's byte count mapped to the
//!   caller-owned per-stream completions, retirement only through a
//!   pointer durable in the stream and an explicitly synced compaction,
//!   and fail-stop: a panic or error inside a submitted write leaves the
//!   shared engine uncertain, so every later call refuses until a fresh
//!   open recovers the actual valid records.
//!
//! No raft-rs, `Ready`, terms, Raft conflict truncation, custom physical
//! WAL or observer replication exists here, and engine purge suggestions
//! never delete logical history: [`journal::RaftEngineJournal::maintain`]
//! only reports them.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod codec;
pub mod journal;
pub mod metadata;

pub use codec::{RecordCodec, RecordExt};
pub use journal::{
    JournalIdentity, JournalOptions, OpenError, RaftEngineJournal, RecoveryPolicy, WriteStats,
};
pub use metadata::{JOURNAL_FORMAT_V1, JournalIdentityV1, METADATA_REGION};

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "production";

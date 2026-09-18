//! Shared journal contract (task-j01; design Sections 5.2, 17.3.1-17.3.2,
//! 17.16, 18).
//!
//! This crate is the narrow port between common storage and the shared
//! journal (task-j02 maps it onto the pinned raft-engine). It contains no
//! engine, runtime or actor types and performs no I/O:
//!
//! * [`stream`]: `StorageStreamId`, the durably allocated identity of one
//!   local `(cluster, domain, replica_incarnation)` stream, and the
//!   [`stream::StreamAllocator`] whose identifiers come from a persisted
//!   high-water mark, never from hashing and never recycled; a mapping must
//!   be durable before its stream may be used.
//! * [`head`]: `StreamPosition`, `WrittenBytes` (an engine byte count that
//!   is deliberately not a sequence) and [`head::StreamHead`], the accepted
//!   durable head of one stream with at most one uncompleted authoritative
//!   batch, definite versus indeterminate outcomes and explicit
//!   reconciliation of an uncertain append.
//! * [`record`]: `JournalRecordV1`, the immutable complete record bound by
//!   origin/incarnation, local sequence, format, predecessor digest and its
//!   own digest, with typed bodies for protocol transitions, application
//!   outcomes, local checkpoint publication and lifecycle metadata. The
//!   durable encoding is versioned independently of the native wire.
//! * [`group`]: `GroupWrite`, a nonempty bounded multi-stream batch whose
//!   success maps to exact caller-owned per-stream completions and barriers.
//! * [`failure`]: `JournalFailure`, definite (before append) versus
//!   indeterminate (after submission) failures and their storage-event
//!   classes.
//! * [`frontier`]: the `C <= M <= J` frontiers, the applied frontier mapped
//!   one-to-one to `StoreSeq`, and `CheckpointPointerV1`, the durable pointer
//!   that (not a directory listing) selects recovery state.
//! * [`engine`]: the [`engine::JournalEngine`] trait a physical adapter
//!   implements.
//!
//! `JournalDurable`, `Materialized` and protocol establishment are distinct
//! facts. Nothing in this crate produces an `EstablishedResult`; a durable or
//! materialized record is evidence the learning predicate may use, never a
//! learned outcome. The state engine's `commit_durable` is untouched.
#![forbid(unsafe_code)]
#![no_std]
#![warn(missing_docs)]
extern crate alloc;

pub mod engine;
pub mod failure;
pub mod frontier;
pub mod group;
pub mod head;
pub mod record;
pub mod stream;

pub use engine::{JournalEngine, ReadBudget, RecordPage};
pub use failure::{JournalError, JournalErrorClass, JournalFailure};
pub use frontier::{AppliedFrontier, CheckpointPointerV1, FrontierError, Frontiers};
pub use group::{GroupEntry, GroupError, GroupLimits, GroupReceipt, GroupWrite, JournalCompletion};
pub use head::{
    HeadError, HeadState, PendingBatch, Reconciled, StreamHead, StreamPosition, WrittenBytes,
};
pub use record::{
    JournalRecordV1, LifecycleRecordV1, RecordBody, RecordDraft, RecordError, RecordExpectation,
    RecordOrigin, TransitionContext,
};
pub use stream::{
    ShardId, StorageStreamId, StreamAllocator, StreamError, StreamHighWater, StreamKey,
    StreamMappingV1, StreamState,
};

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "core";

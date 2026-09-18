//! Portable state-engine contract (task-s01; design Sections 16.3, 17.8-17.10).
//!
//! This crate is the narrow port between common storage and a physical
//! engine. It contains no engine, runtime or actor types:
//!
//! * [`engine`]: bounded ordered reads over one pinned cross-collection
//!   snapshot, a unique non-`Send` writer whose only success is
//!   `commit_durable`, and typed definite/indeterminate failures. Weaker
//!   capabilities (atomic working state without per-transaction sync,
//!   durable checkpoint publication, journal durability) are *separate*
//!   traits reserved for later tasks; nothing can offer them through
//!   `commit_durable`.
//! * [`registry`]: the frozen logical collection identifiers of Section
//!   17.10, assigned explicitly and never recycled.
//! * [`envelope`]: `StoreEnvelopeV1`, the bounded value envelope every row
//!   carries, and the applied stamp stored in `meta_v1`.
//! * [`seq`]: `StoreSeq`, the local applied stamp. It maps one-to-one to the
//!   local journal sequence and is never a public order, execution position
//!   or input to common hashes.
#![forbid(unsafe_code)]
#![no_std]
#![warn(missing_docs)]
extern crate alloc;

pub mod engine;
pub mod envelope;
pub mod registry;
pub mod seq;

pub use engine::{
    CommitFailure, Direction, EngineError, ErrorClass, LocalEngine, OrderedRead, Row, RowPage,
    ScanRequest, SnapshotSource, WriteTxn,
};
pub use envelope::{AppliedStamp, StoreEnvelopeV1};
pub use registry::Collection;
pub use seq::StoreSeq;

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "core";

//! redb physical mapping (task-07; design Sections 5.2, 17.1, 17.9-17.11,
//! 17.13).
//!
//! This crate maps the `coord-store-api` contract onto one redb database per
//! domain generation: byte tables per logical collection, one cross-table
//! read transaction as the pinned snapshot, and the unique write
//! transaction committed with `Durability::Immediate` plus two-phase commit
//! (quick repair off). Codecs, guards, MVCC, retries and checkpoints stay in
//! common code; nothing here interprets row contents.
//!
//! [`lifecycle`] implements the fail-closed generation lifecycle: explicit
//! creation only, an exclusive root lock, a checksummed manifest and an
//! in-database identity record that must both match the expected origin,
//! domain, replica, incarnation, generation and engine before a database is
//! served. Missing, empty, corrupt or mismatched files are errors, never a
//! cue to create or reinitialize. This is the strict single-store reference
//! foundation; journal-first production arrives with task-j03.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod engine;
pub mod lifecycle;
pub mod manifest;

pub use engine::{RedbEngine, RedbReader, RedbView, RedbWrite};
pub use lifecycle::{Generation, OpenError, OpenOptions, StoreIdentity};
pub use manifest::StoreManifestV1;

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "production";

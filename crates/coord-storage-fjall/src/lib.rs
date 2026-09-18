//! Experimental single-writer Fjall mapping of the portable state-engine
//! contract (task-s03; design Sections 16.1, 17.9, 17.11, 17.13).
//!
//! One domain generation is one pinned `SingleWriterTxDatabase`. The
//! seventeen logical collections are grouped into four physical keyspaces;
//! every physical key carries its two-byte `CollectionId` prefix and every
//! range is clamped to that prefix, so prefixes never leak and unbounded
//! ranges stay inside their logical collection. The pinned snapshot is one
//! cross-keyspace `read_tx`; the unique writer is the serialized
//! cross-keyspace transaction committed with an explicit
//! `Some(PersistMode::SyncAll)`, never the engine's implicit default. Fjall
//! journals and syncs the whole batch before it becomes visible, so a
//! commit either returns durable success or an indeterminate failure that
//! poisons the database.
//!
//! [`lifecycle`] is the same fail-closed generation lifecycle as the redb
//! reference (explicit creation, exclusive root lock, checksummed manifest
//! and in-database identity record) with the engine name `fjall`, so a redb
//! root opened here, or a fjall root opened by the redb adapter, fails with
//! an engine mismatch rather than being served or reinitialized.
//!
//! This crate is an experiment composition for fresh isolated storage. It
//! is never linked into production artifacts (role `test-only`, enforced by
//! `cargo xtask check-deps`), performs no conversion or cross-engine image,
//! and claims nothing about speed or crash coverage of fjall's own
//! flush/compaction internals.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod engine;
pub mod lifecycle;

pub use engine::{
    FEATURES, FjallEngine, FjallReader, FjallView, FjallWrite, GROUPS, LAYOUT_NAME, group_of,
};
pub use lifecycle::{ENGINE_NAME, FjallGeneration, FjallOpenOptions, PROFILE_NAME};

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "test-only";

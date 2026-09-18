//! Common storage coordination (task-08; design Sections 17.3, 17.8-17.10).
//!
//! `StoreWorker<E>` is the strict single-writer reference coordinator over
//! any `coord-store-api` engine. It never names a physical engine: common
//! code calls `commit_durable`, not redb.
//!
//! * Batches ([`coord_core::effect::PersistBatch`]) are submitted with their
//!   boot-scoped barrier. A batch from another boot is refused at once.
//! * Application batches carry an [`coord_core::effect::ApplyBase`]; the
//!   worker validates it against the durable execution frontier inside the
//!   transaction (guard rejection is definite and leaves no change).
//!   Protocol batches (no base) advance the local stamp without touching the
//!   application frontier, so they never invalidate an application
//!   predecessor.
//! * Bounded grouping: `flush` lowers up to `max_records`/`max_bytes` of
//!   queued batches into one durable transaction (no idle timer), stamping
//!   the projection with the store sequence and the group digest.
//! * In this reference increment the durable commit is the only
//!   persistence, so a successful flush reports both `JournalDurable` and
//!   `Materialized` with the same sequence. task-j03 splits them.
//! * An indeterminate commit blocks further work until [`StoreWorker::reconcile`]
//!   reads the semantic stamp back and decides presence or absence; byte
//!   batches are never blindly retried.
//! * The durable-view gate ([`view::GatedReader`]) hands out a snapshot only
//!   together with the stamp it proves and refuses snapshots ahead of the
//!   completed frontier.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod lowering;
pub mod view;
pub mod worker;

pub use lowering::{GroupDigest, batch_digest};
pub use view::{GatedReader, GatedView, ViewError};
pub use worker::{FlushOutcome, GroupLimits, StoreWorker, SubmitError, WorkerState};

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "production";

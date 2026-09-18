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
//! * [`codecs`], [`materialize`] and [`views`] (task-11) own the common row
//!   schemas of `kv_current_v1`, `kv_history_v1`, `events_v1` and the KV
//!   frontier rows, lower an `ApplyPlan` into one atomic batch, and build
//!   the planner's owned `ReadView` (current entries, historical snapshot at
//!   a fixed revision selecting versions before limits, compaction floor)
//!   plus bounded pages and per-revision event sets from a gated snapshot.
//!   Physical engines never implement MVCC themselves.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod codecs;
pub mod lowering;
pub mod materialize;
pub mod view;
pub mod views;
pub mod worker;

pub use lowering::{GroupDigest, batch_digest};
pub use materialize::{ApplyOutcome, apply_plan, plan_to_batch};
pub use view::{GatedReader, GatedView, ViewError};
pub use views::{ViewBudget, ViewBuildError, build_read_view, events_at, scan_current_page};
pub use worker::{FlushOutcome, GroupLimits, StoreWorker, SubmitError, WorkerState};

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "production";

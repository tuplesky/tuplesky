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
//! * [`watch`] (task-13): the watch hub with a subscribe-first registration
//!   frontier (replay strictly through the frontier, live strictly after),
//!   whole-revision batches, bounded per-watch queues that close slow
//!   consumers with a resume point instead of skipping, progress that never
//!   overtakes delivered events, per-output authorization and resumable
//!   cancellation. [`sync`] swaps its primitives for loom under `cfg(loom)`.
//! * [`compaction`] (task-14): the replicated retention floor lowered to
//!   explicit view/watch holds gives the effective floor; incremental,
//!   budgeted history and event garbage collection keeps the newest version
//!   or tombstone at or below that floor per key plus everything newer, and
//!   persists its cursors so it resumes after a crash.
//! * Native leases (task-15): [`codecs`] adds the `lease_v1` record and the
//!   `lease_keys_v1` reverse-index row (generation and bound mod revision),
//!   [`materialize`] lowers lease writes and bindings into the same atomic
//!   batch as the entries they govern, and [`views`] loads the lease
//!   records a request names or its entries reference plus, for a
//!   revocation, the reverse index and the entries it points at.
//!   task-16 adds the `lease_authority` frontier row, the internal-command
//!   view (`build_internal_view`) for conditional expiration and the
//!   paginated `active_leases` listing a recovering scheduler arms from.
//! * Sessions and policy (task-18): `session_v1` holds the full
//!   `SessionRecord`, `policy_v1` trust rules (prefix `0x01`) and
//!   per-principal permission rules (prefix `0x02 || principal`),
//!   `auth_grant_v1` grant commitments keyed by digest. `build_authorized_view`
//!   loads the session, its trust rule and its principal's rules at the
//!   same snapshot as the entries, so denial and revocation are ordered at
//!   execution; retry admission and result resolution require an
//!   executable session, so a retired or rule-invalidated session cannot
//!   read cached outcomes.
//! * Protocol rows (task-20): [`protocol`] reads the epoch's durable
//!   promise row so a rebooted replica recovers its promise from the
//!   projection.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod codecs;
pub mod compaction;
pub mod lowering;
pub mod materialize;
pub mod policy;
pub mod protocol;
pub mod retry;
pub mod sync;
pub mod view;
pub mod views;
pub mod watch;
pub mod worker;

pub use compaction::{GcBudget, GcPlan, HoldGuard, RetentionHolds, plan_gc};
pub use lowering::{GroupDigest, batch_digest};
pub use materialize::{ApplyOutcome, apply_plan, plan_to_batch};
pub use retry::{Admission, Resolution, RetryBinding};
pub use view::{GatedReader, GatedView, ViewError};
pub use views::{
    ActiveLeasePage, StoredEvent, ViewBudget, ViewBuildError, active_leases, active_leases_page,
    build_authorized_view, build_internal_view, build_read_view, events_at, load_authorization,
    scan_current_page, stored_events_at,
};
pub use watch::{CloseReason, WatchBatch, WatchHub, WatchId, WatchItem, WatchSpec};
pub use worker::{FlushOutcome, GroupLimits, StoreWorker, SubmitError, WorkerState};

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "production";

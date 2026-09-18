//! Pure KV and transaction planner (task-10; design Sections 6.1-6.3, 17.4).
//!
//! The planner consumes a canonical `logical_v1` request and an owned,
//! bounded [`ReadView`] built by common storage at an established
//! predecessor, and returns a deterministic [`ApplyPlan`]: the logical
//! mutations, the complete event set sharing one revision, the response and
//! the execution advance. It holds no engine handle, reads no clock and
//! never mutates anything itself; materialization (task-11) rechecks the
//! plan's base and applies it atomically.
//!
//! Revision rules (Section 6.2): a branch that changes KV receives exactly
//! one new revision; read-only, failed-comparison and no-op outcomes do
//! not. A same-value put is still a mutation; a delete finding no keys is
//! not. Every planned command advances execution by one position.
//!
//! Semantic limits (Section 19.3) are checked before any plan is produced,
//! so a rejected request leaves nothing to undo.
//!
//! Native leases (task-15; Section 7.1): [`lease::LeaseRecord`] carries the
//! owner principal, generation, TTL and attachment accounting. Grant,
//! attach, detach, revoke and time-to-live are planned here against the
//! lease records and reverse index the view carries; ownership is checked
//! against the view's principal, a revocation deletes exactly the current
//! attachments in one revision, and count/byte quotas are rechecked on
//! every write of an attached key. Lease errors are recorded outcomes
//! (they advance execution and are retained for retries), not planner
//! errors.
//!
//! Renewal and expiry (task-16; Sections 7.2-7.3): `LeaseKeepAlive` is a
//! replicated renewal incrementing the record's renewal sequence, the
//! [`internal::InternalCommand`]s establish the expiry authority epoch and
//! expire a lease only when generation, renewal sequence and epoch all
//! match, and [`expiry::LeaseScheduler`] arms conservative deadlines from
//! observation ticks under documented clock assumptions without ever
//! consulting a clock inside application.
#![forbid(unsafe_code)]
#![no_std]
#![warn(missing_docs)]
extern crate alloc;

pub mod expiry;
pub mod internal;
pub mod lease;
pub mod limits;
pub mod plan;
pub mod planner;
pub mod view;

pub use expiry::{
    ClockAssumptions, ExpiryOutcome, LeaseObservation, LeaseScheduler, LeaseSnapshot,
    RemainingEstimate, TimerRequest,
};
pub use internal::InternalCommand;
pub use lease::{LeasePurpose, LeaseRecord, LeaseStatus, attachment_cost};
pub use limits::PlanLimits;
pub use plan::{ApplyPlan, KvEvent, KvEventKind, Mutation, Outcome, RangeItem, Response};
pub use planner::{PlanError, plan, plan_internal};
pub use view::{HistoricalView, KvEntry, ReadView, historical_revisions};

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "core";

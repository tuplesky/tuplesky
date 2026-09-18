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
#![forbid(unsafe_code)]
#![no_std]
#![warn(missing_docs)]
extern crate alloc;

pub mod limits;
pub mod plan;
pub mod planner;
pub mod view;

pub use limits::PlanLimits;
pub use plan::{ApplyPlan, KvEvent, KvEventKind, Mutation, Outcome, RangeItem, Response};
pub use planner::{PlanError, plan};
pub use view::{HistoricalView, KvEntry, ReadView};

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "core";

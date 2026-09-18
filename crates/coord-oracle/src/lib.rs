//! Independent history oracle (task-06; design Sections 6, 12.3, 21.1).
//!
//! The oracle is a *separate* implementation of the domain's semantics. It
//! shares only the frozen `logical_v1` operation types with production and
//! never calls the production planner. It checks whole-domain histories
//! because revisions, transactions, leases and policy couple keys: a
//! per-key decomposition would miss a wrongly shared revision or a
//! transaction that appeared half-applied.
//!
//! * [`model`]: the reference KV/transaction model with dense revisions,
//!   compaction floor and retry deduplication. Extending the oracle to
//!   leases, sessions and policy means adding variants and rules here, not
//!   splitting the history.
//! * [`history`]: observations (invocations, responses, watch batches).
//! * [`check`]: structural checks (revision uniqueness, retry consistency)
//!   and the complete-domain linearizability search with pending-operation
//!   treatment; watch batches are constraints on the revision they claim.
//! * [`report`]: the correctness verdict and, separately, latency statistics.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod check;
pub mod history;
pub mod model;
pub mod report;

pub use check::check_history;
pub use history::{History, Observation, OpId, WatchEvent, WatchEventKind};
pub use model::{KvEntry, KvModel, ModelResponse, Outcome, RangeItem};
pub use report::{LatencyReport, Verdict, Violation};

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "test-only";

//! Deterministic simulation world (task-05 fills this crate).
//!
//! The simulator owns runnable queues, timers, deliveries, disk completions,
//! process generations and external responses (design Section 12). It is a
//! development dependency only: the dependency-policy check rejects any
//! production crate that links it.
#![forbid(unsafe_code)]

pub mod ports;

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "test-only";

/// Re-exported so the workspace skeleton has one cross-crate edge to verify.
pub use coord_types::CRATE_ROLE as TYPES_CRATE_ROLE;

//! The matched native WAN benchmark harness (task-62; design Sections
//! 14.1, 14.3, 21.5, 22.3).
//!
//! It drives a domain that `coord-harness` provisioned, over the client
//! path a Rust caller actually has, and reports what it measured
//! together with what it did not.
//!
//! * [`workload`] generates the offered work from a seed: writes, point
//!   reads, conditional writes on a configurable number of hot keys,
//!   multi-key transactions and bounded scans.
//! * [`caller`] is one connection and one bound session, with the SDK
//!   deciding what each answer means.
//! * [`run`] schedules arrivals against absolute instants so that a
//!   caller falling behind widens the reported wait instead of quietly
//!   stretching the measurement window.
//! * [`report`] is the published shape: offered against achieved load,
//!   three separate latency distributions per operation kind, refusals
//!   by bounded reason, this host's resources, and the domain's own
//!   stage metrics stated as absent with the reason they are absent.
//!
//! What it is not: a claim about any topology it was not run on, a
//! substitute for the daemon's own instrumentation, and never a headline
//! without the durability the operator named. Impairment -- delay, loss,
//! asymmetry, region loss -- is applied outside this process by
//! `scripts/bench/wan-topology.sh`, and the shape that was applied is
//! recorded in the report as a declaration rather than inferred here.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod caller;
pub mod report;
pub mod run;
pub mod workload;

pub use caller::{Answer, Caller, CallerError};
pub use report::{Absent, Measured, WanRunV1};
pub use run::{RunError, RunSpec, run};
pub use workload::{Kind, Mix, Workload};

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "test-only";

//! Fresh local storage experiments (task-s04; design Sections 17.12-17.14,
//! 21 and 22.3).
//!
//! This crate replays the committed fixtures and a protocol-shaped
//! workload against the model engine, the redb reference and the
//! experimental Fjall adapter, and reports what each one cost. It is
//! measurement and replay, not an engine decision:
//!
//! * [`workload`] generates one logical workload from a seed: overwrites,
//!   deletes, multi-index transactions, leases with attachments and
//!   revocations, repeated invocations that must return the retained
//!   result, bounded reads and explicit retention floors.
//! * [`driver`] drives it through the production worker, planner, retry,
//!   authorization and watch paths, so both engines execute exactly the
//!   same common work and only the state adapter differs.
//! * [`trial`] initializes a fresh run directory, prefills, validates,
//!   warms and then measures with maintenance active and pinned snapshots
//!   held across churn, and ends with a same-engine reopen.
//! * [`manifest`] records source, lock, fixture, engine, version,
//!   features, profile, layout, cache, budgets, seeds, host and filesystem
//!   with every run; [`measure`] reports percentiles, variation, counters
//!   and process resources, and never reports an unavailable metric as
//!   zero.
//! * [`mod@compare`] checks semantics first and only then reports cost, only
//!   between engines that write durable bytes, with the engine order
//!   alternated and every caveat attached.
//! * [`runroot`] allocates absent run roots under an experiment directory
//!   and refuses to allocate inside, or remove, anything that carries a
//!   store lifecycle.
//!
//! What this is not: production engine qualification, a WAN or Kubernetes
//! measurement, a migration or a mixed-engine deployment. Production
//! remains redb-only; no speed claim may be made without the measurements
//! recorded here, and a semantic difference disqualifies a speed claim
//! rather than earning a footnote.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod compare;
pub mod driver;
pub mod manifest;
pub mod measure;
pub mod runroot;
pub mod trial;
pub mod workload;

pub use compare::{CAVEATS, ComparisonReportV1, Verdict, compare};
pub use driver::{Domain, DriveError, Observable};
pub use manifest::{EngineKind, StoreExperimentV1, TrialLabel};
pub use runroot::RunRoot;
pub use trial::{TrialError, TrialReport, TrialSpec, committed_fixture, replay_fixture, run_trial};
pub use workload::{WorkloadSpec, generate};

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "test-only";

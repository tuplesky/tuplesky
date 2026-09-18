//! Deterministic simulation world (task-05; design Sections 12 and 21.1).
//!
//! The world owns runnable queues, timers, deliveries, disk completions,
//! process generations and external inputs, advancing logical time to the
//! next event. Events are ordered by `(virtual_tick, insertion_sequence)`,
//! so ties are explicit and a replay of the same bundle produces the same
//! trace digest and the same visible history.
//!
//! * [`rng`]: a seeded ChaCha12 generator with named substreams, so one
//!   consumer's draws never perturb another's fault schedule.
//! * [`schedule`]: the ordered discrete-event queue.
//! * [`storage`]: a per-node volatile/durable storage model with controlled
//!   completion delay and failure; a crash discards everything volatile.
//! * [`network`]: delay, loss, duplication and partitions per link.
//! * [`world`]: runs [`coord_core::DeterministicMachine`]s behind the models.
//! * [`replay`]: versioned replay bundles (build/config/lock identity plus
//!   the explicit fault schedule), incompatible-version rejection and
//!   schedule minimization.
//! * [`actors`] and [`oracle`]: reference actors including a deliberately
//!   faulty one that omits a durable prerequisite, and the independent
//!   checker that catches it.
//!
//! This crate is development-only (role `test-only`). Its seeds are never
//! production entropy, and the dependency policy keeps it out of production
//! artifacts. Logical simulation is not real-engine or packet-level
//! coverage (those are task-09 and task-32).
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod actors;
pub mod network;
pub mod oracle;
pub mod replay;
pub mod rng;
pub mod schedule;
pub mod storage;
pub mod world;

pub use replay::{BuildIdentity, ReplayBundleV1, ReplayError, Scenario};
pub use world::{NodeId, RunReport, World};

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "test-only";

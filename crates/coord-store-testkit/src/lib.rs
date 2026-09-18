//! Storage model engine and conformance kit (task-s02; design Sections 17.9,
//! 17.12, 17.14, 21.1-21.2).
//!
//! * [`model`]: a deterministic in-memory engine implementing the
//!   `coord-store-api` contract with pinned views, one writer, scripted
//!   commit outcomes (durable, definitely-not-committed, indeterminate) and
//!   crash/reopen. Switchable [`model::Misbehavior`]s (torn writes, mixed
//!   snapshots, reversed bounds, swallowed iterator errors, false
//!   durability, early visibility) exist only so the conformance suite can
//!   prove it detects them.
//! * [`conformance`]: black-box checks any adapter must pass: ordered
//!   access, transactions, publication/durability. Each check is named and
//!   reports independently; the kit never assumes an engine is honest.
//! * [`scenario`]: versioned `StoreScenarioV1` logical fixtures generated
//!   from a seed, replayed against any engine and compared with an
//!   independent map oracle and a frozen digest.
//! * [`journal`] (task-j01): a deterministic model of the
//!   `coord-journal-api` contract with mapping-before-use, head/chain
//!   validation before append, scripted durable/definite/indeterminate
//!   outcomes and retirement only under a durable pointer.
//! * [`initialization`] (task-j01): every interleaving of two conflicting
//!   proposals over the model journal, proving that initialized state and
//!   its conflict-index visibility are one durable transition, that
//!   placeholders never masquerade as processed commands and that the
//!   dependency-phase guards read installed durable state.
//!
//! Events of the model are distinct kinds (visible, durable, checkpoint
//! published, reopened); none of them is protocol establishment. The kit is
//! not real-engine crash qualification (task-09) and is never linked into
//! production.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod conformance;
pub mod initialization;
pub mod journal;
pub mod model;
pub mod scenario;

pub use conformance::{ConformanceHarness, ConformanceReport, ScriptedOutcome, run_all};
pub use initialization::{InitMisbehavior, Violation, WorldConfig, explore};
pub use journal::{AppendScript, JournalEvent, ModelJournal};
pub use model::{Misbehavior, ModelEngine, ModelEvent};
pub use scenario::{StoreScenarioV1, replay};

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "test-only";

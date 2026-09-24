//! Deterministic core contracts (task-04; design Sections 4.8, 5.1, 18).
//!
//! The same `step(Event) -> Vec<Effect>` runs under production I/O and under
//! the simulator. This crate fixes the vocabulary between them:
//!
//! * [`machine`]: the [`DeterministicMachine`] trait and the injected
//!   [`ClockSnapshot`]; no ambient time, randomness or I/O exists here.
//! * [`event`]: owned input events with provenance. A decoded frame cannot be
//!   turned into an authenticated peer event without transport-issued
//!   [`event::PeerProvenance`], and a request cannot be admitted without an
//!   [`capability::AdmissionReceipt`].
//! * [`effect`]: owned effects, [`PersistBatch`] and boot-scoped
//!   [`BarrierId`]s. `SendWhenDurable` effects name every barrier they
//!   require plus an [`EffectContext`] binding domain, incarnation, boot,
//!   epoch and ballot.
//! * [`outbox`]: the actor-owned logical outbox. An effect is released only
//!   when *all* its barriers completed successfully in the *current boot* and
//!   its ballot is not obsolete. Wrong-boot, duplicate, failed and
//!   obsolete-ballot completions can update bookkeeping but never newly
//!   authorize a send.
//! * [`capability`]: sealed establishment and admission capabilities.
//!   `JournalDurable`, `Materialized` and `LocalCheckpointPublished` are
//!   storage facts; protocol establishment is a separate, privately
//!   constructed value.
//! * [`ports`]: the clock and entropy port traits. Production ports live in
//!   the runtime crate and deterministic test ports in the test-only
//!   `coord-sim` crate; this crate never reads the OS clock or entropy.
//!
//! Types support learning predicates but do not prove them (task-19+).
#![forbid(unsafe_code)]
#![no_std]
#![warn(missing_docs)]
extern crate alloc;

pub mod capability;
pub mod effect;
pub mod event;
pub mod machine;
pub mod outbox;
pub mod ports;

pub use capability::{AdmissionReceipt, EstablishError, EstablishedResult, EstablishmentEvidence};
pub use effect::{
    BarrierId, BootId, CollectionId, Effect, EffectContext, PersistBatch, StoreUpdate, TimerId,
};
pub use event::{Event, StorageError, StorageEvent};
pub use machine::{ClockSnapshot, DeterministicMachine};
pub use outbox::{Outbox, ReleaseError};

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "core";

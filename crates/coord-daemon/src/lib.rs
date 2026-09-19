//! The reference daemon composition (task-43; design Sections 22.1, 22.2,
//! 23 G3).
//!
//! This is the *reference preview*: it composes the roles the earlier
//! tasks built into supervised processes with a strict configuration, a
//! startup/readiness/shutdown lifecycle and secret-safe diagnostics. It
//! is a fixed-membership, reference-storage preview and makes no general
//! production claim; the journal, observer and client integration gates
//! remain later tasks.
//!
//! * [`config`]: strict typed TOML with unknown-field rejection. The
//!   local capability must cover the active semantic limits, and the
//!   production dependency graph must exclude test keys and bypasses; a
//!   configuration that fails either is refused before anything starts.
//! * [`role`]: the role set a process runs (voter, frontend, observer,
//!   auth broker, node issuer) and which listeners and identities each
//!   needs.
//! * [`supervise`]: bounded supervised workers with restart budgets, so
//!   a crashing worker is retried within a bound and then quarantines the
//!   process rather than spinning.
//! * [`lifecycle`]: the process lifecycle. Readiness distinguishes
//!   transport liveness from *fresh-quorum* consensus readiness: a voter
//!   that only has cached leadership is not ready to serve. Disk
//!   quarantine drains and stops rather than serving corrupt state.
//! * [`fanout`]: sending a planned submission to every committed voter,
//!   at the incarnation the configuration names rather than one the plan
//!   carries.
//! * [`pending`]: the request streams held open while the collector
//!   establishes their results, matched to deliveries by invocation and
//!   by the connection that asked.
//! * [`serve`]: what the serving loop does with one ingress -- whether
//!   the stream the frame arrived on is answered, held, held and fanned
//!   out, kept as a watch's output, or closed.
//! * [`startup`]: the startup sequence of Section 22.1 (Boot,
//!   StorageValidated, IdentityValidated, MembershipChecked,
//!   ProtocolRecovered), which no step may skip, and the production
//!   genesis store that pins the manifest digest in `meta_v1`.
//! * [`diagnostics`]: a redacted diagnostics snapshot; tokens, keys and
//!   user data never appear.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod config;
pub mod diagnostics;
pub mod fanout;
pub mod lifecycle;
pub mod listen;
pub mod pending;
pub mod role;
pub mod serve;
pub mod startup;
pub mod supervise;

pub use config::{Config, ConfigError, Limits, ListenConfig, capability_covers};
pub use diagnostics::{Diagnostics, Redacted};
pub use fanout::{Dispatched, PeerFanOut, dispatch};
pub use lifecycle::{Lifecycle, Phase, QuarantineReason, Readiness, ReadyGate};
pub use listen::{BindFailure, BoundListeners, bind_listeners};
pub use pending::{Pending, Undeliverable};
pub use role::{Role, RoleSet};
pub use serve::{Step, step};
pub use startup::{NodeJournal, Startup, StartupError, StartupPhase, StoreGenesis};
pub use supervise::{RestartBudget, Supervisor, WorkerError, WorkerId};

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "production";

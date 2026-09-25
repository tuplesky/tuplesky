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
//! * [`identity`]: reading this node's own credentials, once, at
//!   startup, and refusing there what would otherwise fail at the first
//!   handshake -- an empty trust bundle, a chain with nothing to
//!   present, a private key other accounts can read.
//! * [`lifecycle`]: the process lifecycle. Readiness distinguishes
//!   transport liveness from *fresh-quorum* consensus readiness: a voter
//!   that only has cached leadership is not ready to serve. Disk
//!   quarantine drains and stops rather than serving corrupt state.
//! * [`fanout`]: offering a planned submission to every committed voter,
//!   at the incarnation the configuration names rather than one the plan
//!   carries, over the wire or -- for a voter running here -- through its
//!   own ingress.
//! * [`mailbox`]: that ingress: a voter's bounded local queue, built
//!   from the committed membership by the runtime that runs the voter,
//!   so a co-located frontend can skip the network without skipping
//!   anything the network established.
//! * [`voter`]: the door in front of one voter: a submission from a
//!   collector on the peer plane and one from a collector in this very
//!   process are parsed by the same reader, admitted by the same
//!   boundary and stepped through the same machine, and the voter's own
//!   evidence reaches the collector as a frame with its committed
//!   identity on it rather than as a local success.
//! * [`node`]: driving one voter -- events into the protocol machine,
//!   and the effects it returns carried out: batches persisted, sends
//!   held until their barriers are durable and their boot still holds,
//!   evidence and releases published to the trusted collector.
//! * [`pending`]: the request streams held open while the collector
//!   establishes their results, matched to deliveries by invocation and
//!   by the connection that asked.
//! * [`parked`]: a voter's evidence for a command whose submitter it
//!   does not know yet, held under a bounded window and depth that are
//!   performance controls rather than correctness boundaries -- what is
//!   let go is published again when the submission arrives (task-c02).
//! * [`settle`]: completing what a collector half holds from this
//!   node's own durable record of the command's execution, the same
//!   record that answers a caller's retry (task-c02).
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
pub mod identity;
pub mod lifecycle;
pub mod listen;
pub mod mailbox;
pub mod metrics;
pub mod node;
pub mod parked;
pub mod pending;
pub mod role;
pub mod serve;
pub mod settle;
pub mod startup;
pub mod supervise;
pub mod voter;

pub use config::{Config, ConfigError, Limits, ListenConfig, RenewalConfig, capability_covers};
pub use diagnostics::{Diagnostics, Redacted};
pub use fanout::{
    Dispatched, LocalIngress, NotQueued, PeerFanOut, Queued, Route, Saturated, dispatch,
};
pub use identity::{
    ChainRefusal, IdentityError, load as load_identity, verify as verify_identity, verify_chain,
};
pub use lifecycle::{Lifecycle, Phase, QuarantineReason, Readiness, ReadyGate};
pub use listen::{BindFailure, BoundListeners, bind_listeners};
pub use mailbox::{Ingress, IngressBudget, LocalRoute};
pub use metrics::{
    Durability, Frontiers, Headroom, Lane, LaneReading, Latency, Measure, MetricsSnapshot,
    Recorder, ShardIndex, ShardReading, Stage, StageMetrics, StageReading, Unavailable,
};
pub use node::{DriveError, Machine, Node, Outbound};
pub use pending::{Pending, Undeliverable};
pub use role::{Role, RoleSet};
pub use serve::{Step, step};
pub use startup::{NodeJournal, Startup, StartupError, StartupPhase, StoreGenesis};
pub use supervise::{RestartBudget, Supervisor, WorkerError, WorkerId};
pub use voter::{Refused, Voter};

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "production";

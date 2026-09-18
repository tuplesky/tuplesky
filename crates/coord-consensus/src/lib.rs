//! Source-exact SwiftPaxos building blocks (task-19; design Sections 4,
//! 5.1, 18.1, 21.6).
//!
//! Every item here maps to a handler, field or rule of the SwiftPaxos paper
//! or of the pinned prototype (`imdea-software/swiftpaxos` at
//! `35c69365f1c7737a08e237bfbaf828ee68897080`), or is marked as a TupleSky
//! extension, in `spec/swiftpaxos-mapping.md`. The crate holds no I/O, no
//! clock and no actor: it is the pure vocabulary and the predicates that
//! the consensus actor (task-20 onwards) and the bounded models use.
//!
//! * [`quorum`]: ballot configurations with the leader-inclusive fixed C2
//!   fast set (or the C1 three-quarters class), slow majorities and the
//!   intersection property.
//! * [`phase`]: command phases and the source normal-operation guards
//!   (dependencies ACCEPT/COMMIT before ACCEPT, committed before COMMIT,
//!   executed before execution).
//! * [`vote`]: fast/slow acknowledgements, the leader proposal, vote sets
//!   that reject non-members, observers, duplicates and wrong ballots, and
//!   the fast (dependency-path) and slow (adopted order) learning
//!   predicates.
//! * [`recovery`]: new-leader reports at a durable cut and the
//!   source-defined, order-independent Sync selection that never merges by
//!   highest phase and stops on incompatible accepted candidates.
//! * [`publication`]: the durable records each publication requires
//!   (Section 5.1).
//! * [`ballot`] (task-20): the durable promise state of one replica under
//!   its configuration identity: a promise for a higher ballot is persisted
//!   first and its reply released only when that row and every batch
//!   submitted before the cut are durable; recovered promises are never
//!   lowered by old messages; a wrong epoch, a non-voter sender, a ballot
//!   whose leader is not the sender, or a non-voting role never votes; every
//!   effect carries the boot, epoch and ballot it was produced under so the
//!   logical outbox fences obsolete votes across a same-boot election.
//! * [`commands`] (task-20): command descriptors whose initialization
//!   (payload binding, dependencies, conflict index) is one transition;
//!   placeholders created by early leader evidence are invisible to
//!   conflict lookups and to the dependency-phase guards.
//! * [`rows`], [`messages`]: the promise row of `protocol_v1` and the
//!   postcard-encoded protocol messages of this increment.
#![forbid(unsafe_code)]
#![no_std]
#![warn(missing_docs)]
extern crate alloc;

pub mod ballot;
pub mod commands;
pub mod messages;
pub mod phase;
pub mod publication;
pub mod quorum;
pub mod recovery;
pub mod rows;
pub mod vote;

pub use ballot::{
    BallotState, ConfigurationIdentity, PromiseEffects, PromiseInFlight, PromiseOutcome,
    PromiseRejection, ReplicaRole, SyncRejection,
};
pub use commands::{CommandRecord, CommandTable, InitError};
pub use messages::ProtocolMessage;
pub use phase::{GuardViolation, Phase, guard_accept, guard_commit, guard_execute};
pub use publication::{DurableRecord, Publication};
pub use quorum::{BallotConfiguration, ConfigurationError, FastQuorumClass};
pub use recovery::{RecoveryError, RecoveryReport, ReportEntry, SyncDecision, SyncEntry, select};
pub use rows::{PromiseRecordV1, decode_promise, encode_promise, promise_key, promise_update};
pub use vote::{FastAck, Learned, SlowAck, Vote, VoteError, VoteSet};

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "core";

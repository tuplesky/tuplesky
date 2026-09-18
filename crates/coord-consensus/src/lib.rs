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
#![forbid(unsafe_code)]
#![no_std]
#![warn(missing_docs)]
extern crate alloc;

pub mod phase;
pub mod publication;
pub mod quorum;
pub mod recovery;
pub mod vote;

pub use phase::{GuardViolation, Phase, guard_accept, guard_commit, guard_execute};
pub use publication::{DurableRecord, Publication};
pub use quorum::{BallotConfiguration, ConfigurationError, FastQuorumClass};
pub use recovery::{RecoveryError, RecoveryReport, ReportEntry, SyncDecision, SyncEntry, select};
pub use vote::{FastAck, Learned, SlowAck, Vote, VoteError, VoteSet};

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "core";

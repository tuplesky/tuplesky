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
//! * [`commands`] (task-20/21): command descriptors whose initialization
//!   (payload binding, dependencies, per-key path digests, conflict index)
//!   is one transition; placeholders created by early leader evidence are
//!   invisible to conflict lookups and to the dependency-phase guards;
//!   capacity backpressure refuses new work without evicting unresolved
//!   acceptance.
//! * [`graph`] (task-21): per-key path logs (hash chains anchored at the
//!   leader-synchronized prefix) whose heads are the dependency-path
//!   evidence of fast acknowledgements, strictly stronger than direct-set
//!   equality, and exact, budgeted closure traversal.
//! * [`leader`] (task-22): the normal-operation leader machine: an
//!   admitted request is initialized atomically, its payload, dependency
//!   and proposal rows are persisted in one batch, and the proposal to the
//!   voters and the reply to the frontend are released only once that
//!   batch is durable; a retry key already bound to another payload is
//!   `RequestIdentityConflict`; the leader adopts its own order only when
//!   the proposal is durable and every dependency is at least ACCEPT; a
//!   higher promise stops proposing. No learning happens here.
//! * [`follower`] (task-23): the normal-operation follower machine: an
//!   admitted request is initialized atomically and, for a fast-set
//!   member, its fast acknowledgement is published only once payload,
//!   dependencies and path evidence are durable; a leader proposal that
//!   arrives before the payload is held against a placeholder invisible to
//!   lookups; adoption of the leader's order waits for every dependency to
//!   be at least ACCEPT and publishes the slow acknowledgement only once
//!   the adopted order is durable; duplicates converge; the table is
//!   rebuilt from durable rows after a crash.
//! * [`learner`] (task-24): the conservative slow learner: a command
//!   commits when the leader's order is adopted by a majority including
//!   the leader and its dependencies are committed, executes in leader
//!   sequence order once every dependency executed, and the materializer's
//!   outcome is sealed into an `EstablishedResult` at the next execution
//!   position. A single leader reply establishes nothing.
//! * [`summary`] (task-25): the durable ledger an actor keeps of its own
//!   journal-durable command records, the recovery report built from it
//!   at the cut (never from in-memory phases or a lagging projection),
//!   bounded verified report pages and their assembler; payload transfer
//!   with identity rehash so a missing payload is fetched, never
//!   fabricated.
//! * [`campaign`], [`role`] (task-26): a candidate promises itself, asks
//!   for promises and reports, runs the source selection on a majority of
//!   complete reports, binds the Sync durably to the ballot before
//!   publishing it, and activates the recovered ballot; followers adopt a
//!   Sync only for the ballot they promised, install its entries under the
//!   guards, and re-acknowledge the new leader's proposals; roles convert
//!   through [`role::RecoveredState`] so learned outcomes, execution
//!   frontier and retries survive the change.
//! * [`floor`] (task-52): quorum-certified checkpoint floors. A voter
//!   records readiness only when it durably holds the checkpoint a
//!   candidate names, and readiness is a promise never to vote from
//!   below that position; a majority of the configuration's voters
//!   certifies the floor; every permitted recovery reads a majority and
//!   therefore intersects the signers, so it always discovers the
//!   highest activated floor; a message at or below a held floor is
//!   answered from the retained outcome rather than re-creating trimmed
//!   rows. Discovery reads promises, not certificates: a signer
//!   promised before any certificate existed and keeps the promise
//!   whether or not it ever saw one. Nothing here consults a ballot: a floor belongs to a
//!   configuration and outlives every term in it.
//! * [`handoff`] (task-54): the sealed membership handoff. A voter
//!   records one stance per transition and never reverses it, so a seal
//!   and a cancellation can never both certify and no retry clears a
//!   fence; a terminal certificate is selected only after the seal, by
//!   a majority of the old voters agreeing on one root; the successor
//!   activates only once a majority of it has installed that exact
//!   root. `resume` chooses where a replacement coordinator continues
//!   from durable records alone -- there is no lifecycle label to
//!   consult, and no path from a fence back to `Stable`.
//! * [`rows`], [`messages`]: the promise, payload, dependency and proposal
//!   rows and the postcard-encoded protocol messages of this increment.
#![forbid(unsafe_code)]
#![no_std]
#![warn(missing_docs)]
extern crate alloc;

pub mod ballot;
pub mod campaign;
pub mod commands;
pub mod floor;
pub mod follower;
pub mod graph;
pub mod handoff;
pub mod leader;
pub mod learner;
pub mod messages;
pub mod phase;
pub mod publication;
pub mod quorum;
pub mod recovery;
pub mod role;
pub mod rows;
pub mod speculation;
pub mod summary;
pub mod vote;

pub use ballot::{
    BallotState, ConfigurationIdentity, PromiseEffects, PromiseInFlight, PromiseOutcome,
    PromiseRejection, ReplicaRole, SyncRejection,
};
pub use campaign::Campaign;
pub use commands::{CommandRecord, CommandTable, InitError, Initialized, RetireError};
pub use floor::{
    ActivatedFloor, ActivationError, Discovered, FenceVerdict, FloorCandidate, FloorConflict,
    FloorInstall, FloorLedger, Readiness, ReadinessError, ReadinessLedger, activate, discover,
};
pub use follower::{Follower, FollowerConfig, FollowerRejection, HeldProposal};
pub use graph::{
    Closure, ClosureCursor, ClosureProgress, PathLog, chain, combined_path, empty_path,
};
pub use handoff::{
    ActivationCertificate, CancellationCertificate, Evidence, HandoffError, InstallRecord,
    SealCertificate, Stage, Stance, StanceError, StanceLedger, StanceRecord, TerminalCertificate,
    TerminalReport, Transition, cancel, resume, seal, select_terminal,
};
pub use leader::{
    CONSERVATIVE_KEY, FenceReason, Leader, LeaderConfig, MAX_PROPOSAL_ATTEMPTS, Proposal, Rejection,
};
pub use learner::{AppliedOutcome, LearnError, Learner, LearningMode};
pub use messages::{PathAnchors, ProtocolMessage};
pub use phase::{GuardViolation, Phase, guard_accept, guard_commit, guard_execute};
pub use publication::{DurableRecord, Publication};
pub use quorum::{BallotConfiguration, ConfigurationError, EpochVoters, FastQuorumClass};
pub use recovery::{RecoveryError, RecoveryReport, ReportEntry, SyncDecision, SyncEntry, select};
pub use role::{PendingReport, RecoveredState};
pub use rows::{
    PayloadRecordV1, PromiseRecordV1, ProposalRecordV1, SYNC_KIND, SYNC_SCHEMA_VERSION,
    SyncRecordV1, decode_dependency, decode_payload, decode_promise, decode_proposal, decode_sync,
    dependency_key, dependency_update, encode_dependency, encode_payload, encode_promise,
    encode_proposal, encode_sync, payload_key, payload_update, promise_key, promise_update,
    proposal_key, proposal_update, sync_key, sync_update,
};
pub use speculation::{
    DEFAULT_SPECULATION_BOUND, ReleaseGate, Speculation, SpeculationMismatch, SpeculationRequest,
    TentativeOutcome,
};
pub use summary::{
    DurableLedger, MAX_PAGE_ENTRIES, MAX_REPORT_PAGES, PageError, ReportAssembler, ReportPage,
    paginate,
};
pub use vote::{FastAck, Learned, SlowAck, Vote, VoteError, VoteSet};

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "core";

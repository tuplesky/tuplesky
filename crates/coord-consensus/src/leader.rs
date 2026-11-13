//! Normal-operation leader (task-22; design Sections 4.1-4.8, 5.1, 18;
//! prototype `handlePropose` on the leader, `getDepAndHashes`, the
//! leader's `MFastAck` with `Seqnum` and `MReply`).
//!
//! The leader is a [`DeterministicMachine`]: it reads no clock and does no
//! I/O. For an admitted request it derives the command identity, refuses
//! a retry key already bound to a different payload, initializes the
//! command atomically (payload binding, conservative domain dependencies,
//! path evidence, conflict index), persists the payload, dependency and
//! proposal rows in one batch, and publishes the proposal to every other
//! voter and the reply to the frontend through the logical outbox
//! requiring that batch, so every leader reply has exact durable support.
//! The leader adopts its own order (ACCEPT) only once the proposal is
//! durable and every direct dependency is at least ACCEPT; COMMIT and
//! execution stay with the learner (task-24). A promise for a higher
//! ballot stops proposing and fences unreleased proposals.
//!
//! Follower acknowledgements are collected per command and nothing more:
//! learning is not decided here.

use alloc::collections::{BTreeMap, BTreeSet, VecDeque};
use alloc::vec::Vec;
use core::ops::Bound;

use coord_core::capability::{AdmissionFacts, ReleasedResult, admission_digest};
use coord_core::effect::{BarrierId, BootId, Effect, PeerId, PersistBatch, StoreUpdate};
use coord_core::event::{Event, StorageError, StorageEvent};
use coord_core::machine::DeterministicMachine;
use coord_core::outbox::{BarrierAllocator, Outbox, PendingSend};
use coord_types::identity::Digest32;
use coord_types::ids::{Ballot, ExecutionPosition, LocalJournalSeq, ReplicaId, ReplicaIncarnation};
use coord_types::wire_v1::{MessageV1, decode_stream};
use coord_types::{CommandId, RetryKey};
use serde::{Deserialize, Serialize};

use crate::ballot::{BallotState, ConfigurationIdentity, PromiseRejection, ReplicaRole};
use crate::commands::{CommandTable, InitError};
use crate::digest::{self, DigestMap};
use crate::learner::{AppliedOutcome, LearnError, Learner, LearningMode};
use crate::messages::{
    FramedProposal, MAX_PAYLOAD_TRANSFER, MAX_PROPOSAL_FRAME_BYTES, PathAnchors, ProtocolMessage,
    SubmissionRefusal,
};
use crate::phase::Phase;
use crate::quorum::BallotConfiguration;
use crate::recovery::{RecoveryReport, SyncDecision};
use crate::role::RecoveredState;
use crate::rows::{
    PayloadRecordV1, PromiseRecordV1, ProposalRecordV1, dependency_update, payload_update,
    proposal_update,
};
use crate::speculation::{ReleaseGate, Speculation, SpeculationRequest, TentativeOutcome};
use crate::summary::DurableLedger;
use crate::vote::{FastAck, MissedFast, MissedLog, SlowAck, Vote, VoteError, VoteSet};

/// The single conservative conflict key: every command in a domain
/// conflicts with every other (design Section 3, reference contract).
pub const CONSERVATIVE_KEY: &[u8] = b"*";

/// Static configuration of a leader.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaderConfig {
    /// Configuration identity of this replica.
    pub identity: ConfigurationIdentity,
    /// The ballot (and its fixed fast set) this leader leads.
    pub quorum: BallotConfiguration,
    /// Genesis ballot of the epoch (implicitly promised).
    pub genesis: Ballot,
    /// The trusted frontend every leader reply goes to.
    pub frontend: PeerId,
    /// Command table capacity.
    pub capacity: usize,
}

/// A refused or dropped input, recorded for the caller (never an effect).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Rejection {
    /// Not the leader of the promised ballot.
    NotLeading {
        /// The promised ballot.
        promised: Ballot,
    },
    /// The request frame did not decode to exactly one request.
    MalformedRequest,
    /// The retry key is already bound to another payload.
    RequestIdentityConflict {
        /// Retry key.
        retry_key: RetryKey,
        /// Command bound first.
        bound: CommandId,
    },
    /// The same command was presented again under the same facts. Not a
    /// fault: the reply this leader already published for it is offered
    /// to the submitter again (task-c02).
    Duplicate(CommandId),
    /// The command was presented again after it executed and its record
    /// or payload went to history: refused as `Forgotten`, with nothing
    /// repaired (task-d33). Apart from [`Rejection::Duplicate`] so that a
    /// trace tells the refusal from an evidence repair.
    Forgotten(CommandId),
    /// The retry key is bound to this command, but the presentation
    /// carries other admission facts or another acknowledged floor. The
    /// identity is shared; the request is not, and nothing is replayed
    /// for it.
    RequestFactsConflict {
        /// Command.
        command: CommandId,
        /// Digest of the facts this leader proposed the command under.
        accepted: Digest32,
    },
    /// The command table holds this command under another payload
    /// digest. Nothing is replayed for it.
    PayloadConflict(CommandId),
    /// An exact duplicate could not repair delivery of this leader's
    /// reply, and why. The command itself is unaffected.
    ReplayRefused {
        /// Command.
        command: CommandId,
        /// Why.
        why: crate::replay::ReplayRefusal,
    },
    /// The command table is full.
    Backpressure,
    /// A proposal batch was definitely rejected and is being presented
    /// again unchanged under a new barrier.
    ProposalRetried(CommandId),
    /// A service command presented again while still unlearned: its
    /// proposal was published to the other voters again, unchanged.
    ProposalRepublished(CommandId),
    /// This leader's acceptance of a command was not counted: its payload
    /// row is neither durable here nor in the batch the acceptance rests
    /// on (task-d90).
    NoPayloadRow(CommandId),
    /// The leader stopped leading; a new election recovers the role.
    Fenced(FenceReason),
    /// A peer message was rejected.
    Vote(VoteError),
    /// A `NewLeader` was rejected.
    Promise(PromiseRejection),
    /// A peer frame did not decode.
    MalformedPeerMessage,
    /// A `SealRequest` was rejected (task-55).
    Seal(crate::ballot::SealRejection),
    /// The configuration is sealed (or a seal row is in flight): the
    /// request was not proposed, because ordinary service of this
    /// configuration is over and the transition is what must finish
    /// (task-55).
    Sealed {
        /// The transition the seal is for.
        transition: crate::handoff::Transition,
    },
}

/// A report owed once every batch submitted before the cut is durable.
/// A recovery report this replica owes a candidate.
type ReportDue = crate::role::PendingReport;

/// The leader's proposal state for one command.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Proposal {
    /// Command.
    pub command: CommandId,
    /// Barrier of the payload/dependency/proposal batch.
    pub barrier: BarrierId,
    /// Leader sequence number.
    pub seqnum: u64,
    /// Ordered dependencies.
    pub deps: Vec<CommandId>,
    /// Path evidence.
    pub path: Digest32,
    /// Per-key path anchors published with the proposal.
    pub paths: PathAnchors,
    /// The admission digest the proposal carries, kept so a re-send is
    /// the same proposal even after the record has been retired.
    pub admission: Digest32,
    /// Whether the batch is durable.
    pub durable: bool,
    /// The rows of the batch, kept so a definitely rejected write can be
    /// presented again unchanged under a new barrier.
    pub updates: Vec<StoreUpdate>,
    /// Persistence attempts made for this proposal.
    pub attempts: u32,
    /// Whether the batch writes the leader's acceptance too: a
    /// re-proposal carries its ACCEPT row in the proposal batch, and so
    /// does a fresh proposal whose dependency guard passes when it is
    /// proposed (task-d89); any other fresh proposal writes it once the
    /// guard passes (task-d19).
    pub accepts: bool,
    /// Whether this command has executed.
    ///
    /// Recorded here rather than read from the command table, because
    /// the table is allowed to forget an executed record to make room
    /// and a forgotten record reports no phase at all. A proposal that
    /// asked the table would then read "not executed yet" for a command
    /// that executed long ago -- and would be speculated over, and its
    /// result released a second time to a caller that already has it.
    pub executed: bool,
    /// Whether the proposal's first send went out with its batch: queued
    /// behind it, and released once it is durable. A re-proposal past
    /// the first `REPROPOSE_BATCH` is not, and goes out from
    /// [`Leader::resend_unvoted`] instead, unless a definite rejection of
    /// its batch has it presented again first, which publishes it.
    pub published: bool,
    /// The [`Leader::resend_unvoted`] call count when the first send went
    /// out (task-d49): once the batch was durable, for a published
    /// proposal, or the call that sent it, for one that was not. A re-send
    /// is due an interval of calls after it, the first included.
    pub sent: Option<u64>,
}

/// Why the leader stopped leading. A fenced leader admits nothing and
/// votes no more; the role is recovered by a new election.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FenceReason {
    /// A proposal write's outcome is unknown, so its payload, dependency
    /// and proposal rows may or may not be durable. Continuing to lead
    /// could omit that state from a later recovery cut.
    ProposalUnresolved {
        /// The command whose write is unresolved.
        command: CommandId,
        /// What storage reported.
        error: StorageError,
    },
    /// A definitely rejected proposal was presented again as often as
    /// allowed without becoming durable.
    ProposalRetriesExhausted {
        /// The command.
        command: CommandId,
    },
}

/// How often a definitely rejected proposal batch is presented again
/// before the leader stops leading.
pub const MAX_PROPOSAL_ATTEMPTS: u32 = 3;

/// How many re-proposals a new leader publishes at once; the rest go out
/// through [`Leader::resend_unvoted`] (task-d07).
pub const REPROPOSE_BATCH: usize = 32;

/// How many proposals one call of [`Leader::resend_unvoted`] sends to one
/// voter at most (task-d07).
pub const RESEND_PER_VOTER: usize = 16;

/// The most calls of [`Leader::resend_unvoted`] a proposal a voter has
/// not adopted waits between two of its re-sends to that voter.
///
/// A re-send is for a proposal that was lost: refused by a full lane, or
/// sent before the voter was linked. It does not tell a lost proposal
/// from one the voter is still working through, or from one whose
/// adoption is queued behind this leader's own work, and on a busy
/// domain it is almost always the second or the third. Sent again on
/// every call, the same proposals went out four times a second to a
/// voter that already held them, each came back as another adoption
/// this leader refused as a duplicate -- over 8000 in two minutes on a
/// three-voter domain -- and the leader's control lane to the voter
/// that was behind filled, so what was dropped was the fresh proposals
/// and commits that would have let it catch up (task-d33). So the gap
/// between two re-sends of one proposal to one voter doubles, from one
/// call to this many, and is never shorter than the voter's re-send
/// interval (task-d49): a voter that is merely behind is sent one at most
/// this often.
///
/// Four calls of `coordd`'s 250 ms timer is one second, which is how long
/// a voter holding work it cannot execute waits before it fetches a
/// peer's executed history instead (`coord_daemon::catch_up::STILL_FOR`).
/// A longer gap would turn a proposal that really was lost into a
/// history fetch on the bulk lane.
///
/// The `NewLeader` asked again of a voter that has not joined is not
/// paced by this: one small frame per such voter per call, which an idle
/// voter costs on every re-send tick and which is cheap enough not to
/// need it.
pub const RESEND_BACKOFF_CAP: u32 = 4;

/// The longest re-send interval, in calls of [`Leader::resend_unvoted`]
/// (task-d49): the cap on what a voter's observed vote latency raises it
/// to. One second at `coordd`'s 250 ms timer, as [`RESEND_BACKOFF_CAP`],
/// and for the same reason: a voter holding work it cannot execute for
/// longer fetches executed history instead.
pub const RESEND_INTERVAL_CAP: u64 = 4;

/// How many of a voter's latest vote latencies its re-send interval is
/// taken from (task-d49).
pub const RESEND_LATENCY_SAMPLES: usize = 128;

/// How many calls of [`Leader::resend_unvoted`] after its first send a
/// decided proposal is re-sent to a voter that has not acknowledged it
/// at its usual back-off (task-d49). Past it, the proposal is handed off:
/// it is left mostly to catch-up, and trickled.
///
/// A decided proposal needs no vote, so what it is re-sent for is the
/// voter: one that lacks it holds everything ordered after it. Eight
/// calls of `coordd`'s 250 ms timer are two seconds, twice
/// `coord_daemon::catch_up::STILL_FOR`: a voter that still lacks it has
/// held that work, its frontier still, long enough to have asked a peer
/// for executed history, which carries the command (task-d08). At the
/// floor interval that is three re-sends, one, two and four calls after
/// the first send. Re-sending a voter that far behind everything it
/// lacks would load it with frames it cannot use yet: one slow pass
/// fills its control lane, and each re-send meets the same full lane
/// (task-d46).
///
/// Catch-up does not cover every voter that lacks one, though: a voter
/// that missed both the submission and the proposal holds no work to be
/// still on, and on a quiet domain nothing after it arrives to make it
/// ask. So a handed-off proposal is still re-sent, one a voter a call
/// and each one this many calls apart: a trickle that costs a voter
/// that is far behind four frames a second, and reaches one that came
/// back from an outage. A proposal that is not decided is never handed
/// off: its vote may be the one the leader still needs (task-d15).
pub const RESEND_HANDOFF_CALLS: u64 = 8;

/// What this leader sent of its fresh proposals and its frontier, by
/// kind (task-d102): the bytes the batched round's frames save, and what
/// is left. A re-send, a re-presentation and an answer to a follower's
/// ask aren't counted here; [`ResendCounts`] counts them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SentBytes {
    /// Proposals sent, one to each follower, framed or not.
    pub proposals: u64,
    /// Their bytes, without the payload records relayed proposals carry:
    /// the votes, and each frame's header and length prefixes.
    pub proposal_bytes: u64,
    /// The payload records relayed proposals carried.
    pub payload_bytes: u64,
    /// Proposal frames sent: more than one proposal in one message.
    pub frames: u64,
    /// The proposals those frames carried.
    pub framed: u64,
    /// Frontier announcements sent (`Committed`), one to each follower.
    pub frontiers: u64,
    /// Their bytes.
    pub frontier_bytes: u64,
}

/// The bytes of the payload records `message` carries: a relayed
/// proposal's, or a frame's relayed entries'.
fn message_payload_bytes(message: &ProtocolMessage) -> usize {
    let size = |p: &PayloadRecordV1| postcard::experimental::serialized_size(p).unwrap_or(0);
    match message {
        ProtocolMessage::RelayedProposal { payload, .. } => size(payload),
        ProtocolMessage::ProposalFrame { proposals, .. } => proposals
            .iter()
            .filter_map(|p| p.payload.as_ref())
            .map(size)
            .sum(),
        _ => 0,
    }
}

impl SentBytes {
    /// Add `other`'s counts to these.
    pub fn add(&mut self, other: &SentBytes) {
        self.proposals += other.proposals;
        self.proposal_bytes += other.proposal_bytes;
        self.payload_bytes += other.payload_bytes;
        self.frames += other.frames;
        self.framed += other.framed;
        self.frontiers += other.frontiers;
        self.frontier_bytes += other.frontier_bytes;
    }
}

/// A follower's proposals of this turn, waiting for the turn to end
/// (task-d102): the frame [`Leader::cut_frames`] sends it.
#[derive(Clone, Debug)]
struct QueuedFrame {
    context: coord_core::effect::EffectContext,
    ballot: Ballot,
    requires: Vec<BarrierId>,
    proposals: Vec<FramedProposal>,
}

/// What [`Leader::resend_unvoted`] sent and what came of it (task-d49),
/// counted since the leader began leading; [`Leader::take_resend_counts`]
/// hands them over and starts again.
///
/// A re-send is classified when it goes out, by what the leader knew of
/// the voter's answer then: [`ResendCounts::decided`],
/// [`ResendCounts::acknowledged`] or [`ResendCounts::unanswered`].
/// Whether an unanswered one was lost or late shows only afterwards: the
/// voter's adoption arrives once either way, and a late one arrives
/// again, as a duplicate, when the voter answers the re-send from what it
/// kept. [`ResendCounts::lost`] is what is left.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResendCounts {
    /// First sends of re-proposals held back from the first batch: not
    /// re-sends.
    pub deferred: u64,
    /// First sends of held-back re-proposals that went out as a voter's
    /// adoptions arrived, not on the re-send timer (the takeover gap on
    /// #98): each voter is kept at [`RESEND_PER_VOTER`] of them
    /// unanswered.
    #[serde(default)]
    pub topped_up: u64,
    /// Re-sends of a proposal the leader had decided.
    pub decided: u64,
    /// Re-sends of an undecided proposal to a voter that had acknowledged
    /// it on the fast path, which does not say it holds the proposal
    /// (task-d07).
    pub acknowledged: u64,
    /// Re-sends of an undecided proposal to a voter that had not answered
    /// it at all.
    pub unanswered: u64,
    /// Adoptions counted from a voter after a re-send of the proposal to
    /// it.
    pub answered: u64,
    /// Adoptions refused as duplicates from a voter the proposal was
    /// re-sent to: its first adoption was on its way when the re-send
    /// went out.
    pub late: u64,
    /// Decided proposals handed off, per voter, once
    /// [`RESEND_HANDOFF_CALLS`] old: re-sent since only as a trickle,
    /// and counted under [`ResendCounts::decided`] when they are.
    pub handed_off: u64,
    /// Votes refused as duplicates, fast or slow, re-sent or not.
    pub duplicate_votes: u64,
    /// Calls of [`Leader::resend_unvoted`] that looked for something to
    /// send (task-d59).
    #[serde(default)]
    pub calls: u64,
    /// Proposals those calls looked at, over every voter (task-d59): the
    /// ones past each voter's highest adoption, and the undecided ones
    /// before it. A voter that answers costs what it has not answered
    /// yet, not every proposal kept until the history sweep.
    #[serde(default)]
    pub scanned: u64,
    /// Copies of proposals, first sends and retries, that left before
    /// their batch was durable (task-d98): one per follower each went to
    /// early.
    #[serde(default)]
    pub early: u64,
}

impl ResendCounts {
    /// Re-sends: every send from [`Leader::resend_unvoted`] but the
    /// deferred first ones.
    pub const fn resent(&self) -> u64 {
        self.decided + self.acknowledged + self.unanswered
    }

    /// Adoptions after a re-send that were not late: the voter lacked the
    /// proposal, or its adoption of it was lost.
    pub const fn lost(&self) -> u64 {
        self.answered.saturating_sub(self.late)
    }

    /// Add `other`'s counts to these.
    pub const fn add(&mut self, other: &ResendCounts) {
        self.deferred += other.deferred;
        self.topped_up += other.topped_up;
        self.decided += other.decided;
        self.acknowledged += other.acknowledged;
        self.unanswered += other.unanswered;
        self.answered += other.answered;
        self.late += other.late;
        self.handed_off += other.handed_off;
        self.duplicate_votes += other.duplicate_votes;
        self.calls += other.calls;
        self.scanned += other.scanned;
        self.early += other.early;
    }
}

/// What one send from [`Leader::resend_unvoted`] is (task-d49).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Resend {
    /// The first send of a proposal held back from its batch.
    Deferred,
    /// A re-send of a decided proposal.
    Decided,
    /// A re-send of a decided proposal past [`RESEND_HANDOFF_CALLS`]:
    /// the voter's one trickled this call.
    HandedOff,
    /// A re-send of one the voter acknowledged on the fast path.
    Acknowledged,
    /// A re-send of one the voter has not answered.
    Unanswered,
}

/// A voter's first sends of held-back re-proposals ([`Leader::top_up`]).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct FirstSends {
    /// The highest sequence number in `held_back` its first sends reached.
    through: Option<u64>,
    /// The ones sent that it has not adopted yet.
    out: BTreeSet<CommandId>,
}

/// One proposal's re-sends to one voter (task-d49).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Resent {
    /// The call from which it may be re-sent again.
    next: u64,
    /// The back-off gap last waited, in calls (`RESEND_BACKOFF_CAP`).
    gap: u32,
    /// Whether it was handed off (`RESEND_HANDOFF_CALLS`): re-sent since
    /// only as a trickle.
    handed_off: bool,
}

/// The leader machine of one domain.
#[derive(Debug)]
pub struct Leader {
    config: LeaderConfig,
    boot: Option<BootId>,
    alloc: Option<BarrierAllocator>,
    outbox: Option<Outbox>,
    ballots: BallotState,
    table: CommandTable,
    bindings: BTreeMap<RetryKey, CommandId>,
    proposals: DigestMap<CommandId, Proposal>,
    /// The proposals not yet both durable and executed, by sequence
    /// number (task-d46): what the per-vote and per-event passes read,
    /// instead of every proposal, which `proposals` keeps until the
    /// history sweep. Kept exact by [`Leader::put_proposal`],
    /// [`Leader::settle`] and the sweep.
    unsettled: BTreeSet<(u64, CommandId)>,
    /// Where the release gate's walk starts (task-d111's cut (e)): every
    /// unexecuted proposal numbered below it has had its result released.
    /// The gate passes over those, so [`Leader::release_ready`] starts
    /// here rather than at the first unsettled proposal, and walks only
    /// as far as the gate goes. A proposal entered below it lowers it
    /// ([`Leader::put_proposal`]).
    release_from: u64,
    /// Proposals the release gate was handed (task-d111's cut (e)), for
    /// the test that bounds them.
    #[cfg(test)]
    release_walked: core::cell::Cell<u64>,
    /// Each proposal's current batch (task-d46): what a storage event is
    /// matched with, instead of a pass over every proposal.
    by_barrier: BTreeMap<BarrierId, CommandId>,
    /// The durable proposals by sequence number (task-d59), entered as
    /// each becomes durable and left as it is replaced or swept: what
    /// [`Leader::resend_unvoted`] walks past each voter's highest
    /// adoption, instead of sorting every proposal `proposals` keeps
    /// until the history sweep.
    durable: BTreeSet<(u64, CommandId)>,
    /// Per voter, the durable proposal with the highest sequence number
    /// it adopted (task-d59), raised as adoptions are counted and as a
    /// proposal a voter adopted becomes durable. Never below the truth;
    /// above it only once the proposal it names was swept or its votes
    /// replaced, which [`Leader::adopted_through`] finds and corrects.
    adopted_through: BTreeMap<ReplicaId, (u64, CommandId)>,
    votes: DigestMap<CommandId, VoteSet>,
    /// Acknowledgements that reached this leader before it had proposed
    /// the command they are about, held until it does.
    early_votes: BTreeMap<CommandId, Vec<Vote>>,
    /// The same commands in arrival order, so the oldest is the one
    /// dropped when the hold is full.
    early_order: VecDeque<CommandId>,
    payloads: BTreeMap<CommandId, PayloadRecordV1>,
    /// Payloads this replica knows to be durable without having proposed
    /// them in this ballot: what it recovered, or made durable as a
    /// follower. A leader that serves only its own durable proposals
    /// cannot serve the payload of a recovered command it has no reason
    /// to propose again -- one it already executed -- and a voter that
    /// lacks it would ask for ever (task-d05).
    served_payloads: BTreeSet<CommandId>,
    ledger: DurableLedger,
    learner: Learner,
    seqnum: u64,
    report_due: Option<ReportDue>,
    /// The pages of the last report sent a candidate, which a lost page
    /// is answered from (task-d28).
    served_report: Option<crate::summary::ServedReport>,
    /// The ballot this replica last reported for and the commands its
    /// report named (task-d24).
    report_cut: Option<(Ballot, BTreeSet<CommandId>)>,
    pending_sync: Option<(ReplicaId, SyncDecision)>,
    speculation: Speculation,
    rejections: Vec<Rejection>,
    /// The commit frontier last announced to the other voters (task-d74).
    /// A follower learns only from it, so it goes out in the step that
    /// moves it, or at the end of that turn while `frontier_at_turn_end`.
    announced: Option<u64>,
    /// Whether the frontier a step moves waits for the end of the node's
    /// turn ([`Leader::set_frontier_at_turn_end`]).
    frontier_at_turn_end: bool,
    /// The command the last admitted request became, once derived
    /// (task-d60, step 3): the voter files where its evidence is owed
    /// under it instead of decoding the request a second time.
    admitted: Option<CommandId>,
    /// Whether the next admitted request's command is marked as answered
    /// on the leader's release alone once it is derived (task-d100, step
    /// A): the submission carried the mark, and the voter set this
    /// before stepping it, so the command is derived once, here (#204
    /// review).
    mark_next_admitted: bool,
    /// Whether the next admitted request was relayed to this voter alone
    /// as its ballot's leader (task-d102, task-d100's step B): set by the
    /// voter beside `mark_next_admitted`, so a leader proposes the
    /// command with its payload and a voter that isn't leading says so.
    mark_next_relayed: bool,
    fenced: Option<FenceReason>,
    /// Entries of the selection this leader was handed that no order
    /// keeps: a dependency cycle, which is an invariant violation
    /// (task-d21). A leader holding one proposes and leads nothing.
    recovery_cycle: Option<Vec<CommandId>>,
    /// The reply this leader published to the frontend for each command
    /// it still remembers, kept so an exact duplicate submission can
    /// offer it to the submitter again (task-c02). Never recovered: the
    /// outbox it went through is this boot's.
    replay: crate::replay::EvidenceStore,
    /// The batches carrying this leader's acceptance of a command, until
    /// they are durable: each is then its own adoption, counted like a
    /// follower's (task-d19).
    own_adoptions: BTreeMap<BarrierId, OwnAdoption>,
    /// The selection this leader leads from, handed to its follower when
    /// it is deposed (task-d34): an entry whose payload this leader lacked
    /// was never proposed, and the follower installs it from here.
    selection: Option<SyncDecision>,
    /// The voters known to follow this leader's ballot: itself, and
    /// those that have voted in it. A promise made during the campaign
    /// does not count: the Sync that answered it is one unacknowledged
    /// frame. The others missed the campaign -- down, cut off, or
    /// leading an older ballot themselves -- or promised in it and lost
    /// the Sync, and nothing else would ever tell them of this ballot:
    /// its proposals are foreign to them, and a quiet domain sends them
    /// nothing at all. They are asked again (`resend_unvoted`) and
    /// answered with the Sync (task-d33), until a vote shows they
    /// installed it. One that installed it with nothing to vote on yet
    /// ignores the ask.
    joined: BTreeSet<ReplicaId>,
    /// Calls of `resend_unvoted` so far: the clock its back-off is
    /// counted on.
    resend_calls: u64,
    /// Re-proposals held back from a new leader's first batch
    /// (`REPROPOSE_BATCH`), by sequence number, until every voter has had
    /// its first send of them. Empty but after a takeover.
    held_back: BTreeSet<(u64, CommandId)>,
    /// Per voter, the held-back re-proposals sent to it that it has not
    /// adopted yet, and how far in `held_back` its first sends reached
    /// (the takeover gap on #98). Each adoption that arrives tops the
    /// voter up to [`RESEND_PER_VOTER`] unanswered, so the backlog goes
    /// out at the pace of a round trip; paced by `resend_unvoted`'s timer
    /// alone it went out 16 a voter every 250 ms, 64 commands a second.
    first_sends: BTreeMap<ReplicaId, FirstSends>,
    /// Per proposal and voter it was re-sent to: when it may be re-sent
    /// again, and how often it was. Kept only while the proposal is held
    /// and the voter has not adopted it.
    resent: DigestMap<(CommandId, ReplicaId), Resent>,
    /// Per voter, how many calls of `resend_unvoted` its latest adoptions
    /// took to arrive after the proposal's first send, newest last and at
    /// most `RESEND_LATENCY_SAMPLES`: what its re-send interval is taken
    /// from (task-d49). Only adoptions of proposals not re-sent to it, so
    /// an answer to a re-send is never read as a slow answer to the first
    /// send.
    latency: BTreeMap<ReplicaId, VecDeque<u64>>,
    /// Adoptions counted after a re-send, by proposal and voter, with the
    /// calls they took after the first send: a latency sample once a
    /// second copy shows the first was the answer to the first send, and
    /// late (task-d49).
    /// Kept one call after the adoption, with the call count it came at.
    answered: DigestMap<(CommandId, ReplicaId), (u64, u64)>,
    /// What the re-sends did, until the driver takes it (task-d49).
    counts: ResendCounts,
    /// Why each command established on the slow path missed the fast
    /// one, until the driver counts it (task-d62).
    missed: MissedLog,
    /// The commands committed since the driver last took them, while it
    /// asked for them (task-d62).
    learned: Option<Vec<CommandId>>,
    /// Whether this leader sends a fresh proposal before its batch is
    /// durable, to each follower that cannot make a fast quorum from it
    /// (task-d98, design Sections 4.3 and 4.8). The driver sets it from
    /// the replicated feature, `EarlyProposal`; it is off until then.
    early: bool,
    /// Whether a proposal batch of this leadership has been durable. No
    /// proposal leaves early before one has: a genesis leader that
    /// restarts on a store holding none comes back leading its ballot
    /// (`coordd`'s `fresh`) and numbers its proposals from zero again,
    /// over orders its followers may already hold (Section 4.8's fourth
    /// condition). Batches are durable in the order they were submitted,
    /// so once one is, every restart finds it.
    early_ready: bool,
    /// The followers known to hold `EarlyProposal`'s activation durably
    /// (task-d99): an early send goes only to one of them. `None` reads
    /// every follower as holding it, which only a build made for task-d98's
    /// local reading sets, and the tests of the send itself.
    early_holders: Option<BTreeSet<ReplicaId>>,
    /// The commands whose collector answers on the release alone
    /// (task-d100, step A), marked by the voter before each submission
    /// of one is stepped: no reply and no copy of this leader's own
    /// acknowledgement goes to the frontend for them, and their release
    /// carries the decided payload's admission digest.
    answer_on_release: BTreeSet<CommandId>,
    /// The marked commands whose submission was relayed to this leader
    /// alone (task-d102): every proposal of one carries its payload
    /// ([`ProtocolMessage::RelayedProposal`]), since the followers never
    /// got the submission. A subset of `answer_on_release`, let go with
    /// it.
    relayed: BTreeSet<CommandId>,
    /// Whether a turn's fresh proposals wait for the turn's end, to go to
    /// each follower in one frame (task-d102): set by the node while
    /// `Feature::BatchedRound` is active and it lowers its persists in
    /// groups.
    batch_frames: bool,
    /// Each follower's proposals of this turn, while `batch_frames`.
    frames: BTreeMap<ReplicaId, QueuedFrame>,
    /// What the fresh proposals and the frontier cost, by kind.
    sent_bytes: SentBytes,
    /// Marked commands this leader executed and was sent again as exact
    /// duplicates, with the admission digest of the payload it decided
    /// (task-d100, step A): their collector lost the release, and the
    /// daemon publishes it again, rebuilt from this node's retry record,
    /// since the release left the machine as an effect and nothing here
    /// kept it.
    release_again: Vec<(CommandId, RetryKey, Digest32)>,
}

/// This leader's adoption of its own order, published and waiting on the
/// batch carrying its acceptance row (task-d19).
#[derive(Clone, Debug)]
struct OwnAdoption {
    command: CommandId,
    vote: Vote,
    /// The barriers its publication required: if one of them fails, the
    /// outbox drops the publication and it is made again.
    requires: Vec<BarrierId>,
}

/// Raise `voter`'s highest adoption to `key` if it is higher (task-d59).
fn raise(
    through: &mut BTreeMap<ReplicaId, (u64, CommandId)>,
    voter: ReplicaId,
    key: (u64, CommandId),
) {
    let top = through.entry(voter).or_insert(key);
    if key > *top {
        *top = key;
    }
}

/// The commands a chain of re-proposals follows once `ready`, entries of
/// `decision` that were waiting on re-proposed commands, are placed after
/// `last` (task-d34): those of `last` and `ready` that no other of them
/// depends on, over the selected dependencies.
fn ready_tails(decision: &SyncDecision, last: &[CommandId], ready: &[CommandId]) -> Vec<CommandId> {
    let set: BTreeSet<CommandId> = last.iter().chain(ready).copied().collect();
    let depended: BTreeSet<CommandId> = set
        .iter()
        .filter_map(|c| decision.entries.get(c))
        .flat_map(|e| e.deps.iter().copied())
        .collect();
    let mut tails: Vec<CommandId> = Vec::new();
    for c in last.iter().chain(ready) {
        if !depended.contains(c) && !tails.contains(c) {
            tails.push(*c);
        }
    }
    tails
}

impl Leader {
    /// A leader from its configuration, the recovered promise row and the
    /// application's durable execution position. The learner resumes from
    /// that position, so the next command it establishes is the one the
    /// applier will plan; starting at zero would make every later command
    /// mismatch the storage frontier.
    pub fn new(
        config: LeaderConfig,
        durable_promise: Option<PromiseRecordV1>,
        executed_through: ExecutionPosition,
    ) -> Self {
        Leader::new_sealed(config, durable_promise, None, executed_through)
    }

    /// A leader that also recovers the durable seal of its
    /// configuration (task-55).
    ///
    /// Leading is a role within a configuration and the fence is the
    /// configuration's, so a leader of a sealed epoch is as fenced as
    /// any other voter: it admits no promise and proposes nothing new.
    pub fn new_sealed(
        config: LeaderConfig,
        durable_promise: Option<PromiseRecordV1>,
        durable_seal: Option<crate::rows::SealRecordV1>,
        executed_through: ExecutionPosition,
    ) -> Self {
        let ballots = BallotState::recover_sealed(
            config.identity.clone(),
            config.genesis,
            durable_promise,
            durable_seal,
        );
        let table = CommandTable::with_capacity(config.capacity);
        Leader {
            replay: crate::replay::EvidenceStore::new(config.capacity),
            selection: None,
            // Every voter of the genesis ballot is in it from the start.
            joined: config.identity.voters.iter().copied().collect(),
            resend_calls: 0,
            held_back: BTreeSet::new(),
            first_sends: BTreeMap::new(),
            resent: digest::map(),
            latency: BTreeMap::new(),
            answered: digest::map(),
            counts: ResendCounts::default(),
            missed: MissedLog::default(),
            learned: None,
            early: false,
            early_ready: false,
            early_holders: None,
            answer_on_release: BTreeSet::new(),
            relayed: BTreeSet::new(),
            batch_frames: false,
            frames: BTreeMap::new(),
            sent_bytes: SentBytes::default(),
            release_again: Vec::new(),
            config,
            boot: None,
            alloc: None,
            outbox: None,
            ballots,
            table,
            bindings: BTreeMap::new(),
            proposals: digest::map(),
            unsettled: BTreeSet::new(),
            release_from: 0,
            #[cfg(test)]
            release_walked: core::cell::Cell::new(0),
            by_barrier: BTreeMap::new(),
            durable: BTreeSet::new(),
            adopted_through: BTreeMap::new(),
            votes: digest::map(),
            early_votes: BTreeMap::new(),
            early_order: VecDeque::new(),
            payloads: BTreeMap::new(),
            served_payloads: BTreeSet::new(),
            ledger: DurableLedger::new(),
            learner: Learner::new(executed_through),
            seqnum: 0,
            report_due: None,
            served_report: None,
            report_cut: None,
            pending_sync: None,
            speculation: Speculation::new(),
            rejections: Vec::new(),
            announced: None,
            frontier_at_turn_end: false,
            admitted: None,
            mark_next_admitted: false,
            mark_next_relayed: false,
            fenced: None,
            recovery_cycle: None,
            own_adoptions: BTreeMap::new(),
        }
    }

    /// Whether a higher ballot was promised: this replica no longer leads
    /// and should convert to a follower (a Sync received meanwhile is
    /// carried over).
    pub fn deposed(&self) -> bool {
        self.ballots.promised() != self.config.quorum.ballot()
    }

    /// The ballot configuration this leader was activated under.
    pub fn config_quorum(&self) -> BallotConfiguration {
        self.config.quorum.clone()
    }

    /// The Sync received while still holding the leader role, if any.
    pub fn pending_sync(&self) -> Option<&(ReplicaId, SyncDecision)> {
        self.pending_sync.as_ref()
    }

    /// Give up the role: everything durable or learned, nothing
    /// ballot-scoped.
    pub fn into_recovered(self) -> RecoveredState {
        RecoveredState {
            report_due: self.report_due,
            served_report: self.served_report,
            report_cut: self.report_cut,
            identity: self.config.identity,
            ballots: self.ballots,
            table: self.table,
            ledger: self.ledger,
            payloads: self.payloads,
            bindings: self.bindings,
            // A durable proposal whose batch wrote the payload row: a
            // re-proposal of an entry this leader held without its
            // payload wrote none, and its follower would report a payload
            // it cannot serve (task-d90, point 4; protocol_sim row 20).
            served_payloads: self
                .proposals
                .values()
                .filter(|p| p.durable && writes_payload(&p.updates))
                .map(|p| p.command)
                .chain(self.served_payloads)
                .collect(),
            learner: self.learner,
            boot: self.boot,
            alloc: self.alloc,
            outbox: self.outbox,
            frontend: self.config.frontend,
            capacity: self.config.capacity,
            synced_selection: self.selection,
            arrived: BTreeSet::new(),
        }
    }

    /// Become the leader of the recovered ballot after a won campaign:
    /// every Sync entry is re-proposed under the new ballot in dependency
    /// order with the selected dependencies, and commands selected for
    /// re-proposal follow in identity order.
    pub fn from_recovered(
        state: RecoveredState,
        quorum: BallotConfiguration,
        decision: &SyncDecision,
    ) -> (Self, Vec<Effect>) {
        let identity = state.identity.clone();
        let mut leader = Leader {
            config: LeaderConfig {
                identity: state.identity,
                genesis: quorum.ballot(),
                quorum,
                frontend: state.frontend,
                capacity: state.capacity,
            },
            boot: state.boot,
            alloc: state.alloc,
            outbox: state.outbox,
            ballots: state.ballots,
            table: state.table,
            bindings: state.bindings,
            proposals: digest::map(),
            unsettled: BTreeSet::new(),
            release_from: 0,
            #[cfg(test)]
            release_walked: core::cell::Cell::new(0),
            by_barrier: BTreeMap::new(),
            durable: BTreeSet::new(),
            adopted_through: BTreeMap::new(),
            votes: digest::map(),
            early_votes: BTreeMap::new(),
            early_order: VecDeque::new(),
            own_adoptions: BTreeMap::new(),
            payloads: state.payloads,
            served_payloads: state.served_payloads,
            ledger: state.ledger,
            learner: state.learner,
            seqnum: 0,
            report_due: state.report_due,
            served_report: state.served_report,
            report_cut: state.report_cut,
            pending_sync: None,
            speculation: Speculation::new(),
            rejections: Vec::new(),
            announced: None,
            frontier_at_turn_end: false,
            admitted: None,
            mark_next_admitted: false,
            mark_next_relayed: false,
            fenced: None,
            recovery_cycle: None,
            replay: crate::replay::EvidenceStore::new(state.capacity),
            selection: Some(decision.clone()),
            // Only this leader: a voter the campaign heard from has
            // promised, but its Sync is one unacknowledged frame, and
            // counted as joined it would never be asked again if that
            // frame were lost (task-d33). A vote at this ballot joins it.
            joined: BTreeSet::from([identity.replica]),
            resend_calls: 0,
            held_back: BTreeSet::new(),
            first_sends: BTreeMap::new(),
            resent: digest::map(),
            latency: BTreeMap::new(),
            answered: digest::map(),
            counts: ResendCounts::default(),
            missed: MissedLog::default(),
            learned: None,
            early: false,
            early_ready: false,
            early_holders: None,
            answer_on_release: BTreeSet::new(),
            relayed: BTreeSet::new(),
            batch_frames: false,
            frames: BTreeMap::new(),
            sent_bytes: SentBytes::default(),
            release_again: Vec::new(),
        };
        // Dependency order among the entries: a command follows every
        // dependency that is itself an entry. A cycle is an invariant
        // violation (task-d21): the candidate halts before binding such a
        // selection, and a leader handed one anyway proposes nothing and
        // leads no further, rather than propose part of it and wait on the
        // rest for ever.
        let order = match crate::recovery::entry_order(decision) {
            Ok(order) => order,
            Err(cycle) => {
                leader.recovery_cycle = Some(cycle);
                return (leader, Vec::new());
            }
        };
        let mut effects = Vec::new();
        // The tail of the recovered order, not the largest identity: a
        // command identifier says nothing about execution order, and
        // chaining re-proposals after an arbitrary entry lets a
        // re-proposed command become executable before an earlier one.
        //
        // Nor an entry this leader has executed: every voter that could
        // report it may have retired it, so it can sit far behind what
        // this leader executed since.
        //
        // Nor any one candidate alone. The last entry this leader has not
        // executed, the last commands it committed and has not executed
        // yet, and the last command it executed are each the tail when
        // the table is right, and each has been wrong: a command that
        // executed with no executed row came back unexecuted after a
        // restart, and a chain after it forked from everything executed
        // since (task-d12, stress run d12-11). Depending on a command that
        // is already behind the tail orders nothing wrongly, so the chain
        // starts after all of them.
        //
        // Nor an entry that follows a re-proposed command: an acceptance
        // the selection carries may name a dependency it re-proposes
        // (task-d34, protocol_sim row 10, three voters, seed 58). Such an
        // entry comes after that command's re-proposal, and chaining the
        // re-proposals after it made a cycle; chaining them after the
        // command alone left the entry and the next re-proposal both
        // following it, two branches of one key's order that voters
        // executed in different orders.
        let mut dependents: BTreeMap<CommandId, Vec<CommandId>> = BTreeMap::new();
        for (c, e) in &decision.entries {
            for d in &e.deps {
                dependents.entry(*d).or_default().push(*c);
            }
        }
        // For each entry that follows a re-proposed command, the
        // re-proposed commands it follows.
        //
        // Not one this leader committed: it is not proposed again (below),
        // so nothing would ever re-propose an entry waiting on it or chain
        // after one. The entry was left out of the order while the
        // re-proposals went on after the command it follows: two branches
        // of one key's order, which a voter that joined late executed the
        // other way round (task-d33, protocol_sim row 4, three voters,
        // seed 11). It is accepted here already, so the entry is proposed
        // with the others.
        let mut awaits: BTreeMap<CommandId, BTreeSet<CommandId>> = BTreeMap::new();
        for r in decision
            .reproposed
            .iter()
            .filter(|r| leader.table.phase_of(r) < Some(Phase::Commit))
        {
            let mut stack = dependents.get(r).cloned().unwrap_or_default();
            while let Some(e) = stack.pop() {
                if awaits.entry(e).or_default().insert(*r) {
                    stack.extend(dependents.get(&e).into_iter().flatten().copied());
                }
            }
        }
        let mut last: Vec<CommandId> = Vec::new();
        if let Some(tail) = order
            .iter()
            .rev()
            .find(|c| leader.table.phase_of(c) < Some(Phase::Executed) && !awaits.contains_key(c))
        {
            last.push(*tail);
        }
        for c in leader
            .table
            .committed_tails(CONSERVATIVE_KEY)
            .into_iter()
            .chain(leader.table.last_executed())
        {
            if !last.contains(&c) {
                last.push(c);
            }
        }
        // Published in a first batch; the rest go out oldest first as each
        // voter's adoptions come back (`top_up`), and through the re-send
        // for what is lost. A new leader after a long
        // history used to put its whole selection on each follower's
        // control lane in one pass, and the lane refused dozens of them
        // (task-d07).
        let mut published = 0usize;
        // An entry waiting on a re-proposed command is re-proposed right
        // after it, below: before it, its dependency is not accepted here
        // and the acceptance guard refuses it.
        for c in order.iter().filter(|c| !awaits.contains_key(c)) {
            let entry = &decision.entries[c];
            // A command selected as committed that this leader executed
            // has nothing left to decide: the Sync carries the commit, and
            // every voter installing it commits the command without a vote.
            // Proposing it again only spent the lanes -- a new leader after
            // a long history proposed hundreds of such commands in one
            // pass, and the lanes refused the ones it had not executed
            // along with them (task-d05).
            if entry.phase >= Phase::Commit && leader.table.phase_of(c) == Some(Phase::Executed) {
                continue;
            }
            let deps = entry.deps.clone();
            effects.extend(leader.repropose_counted(*c, deps, &mut published));
        }
        let mut done: BTreeSet<CommandId> = BTreeSet::new();
        for c in &decision.reproposed {
            // The entries that follow `c` and nothing re-proposed after it:
            // the chain goes on after the last of them, not after `c`.
            done.insert(*c);
            let ready: Vec<CommandId> = order
                .iter()
                .filter(|e| {
                    awaits
                        .get(e)
                        .is_some_and(|rs| rs.contains(c) && rs.is_subset(&done))
                })
                .copied()
                .collect();
            if leader.table.phase_of(c).is_none() {
                effects.extend(leader.repropose_entries(decision, &ready, &mut published));
                if !ready.is_empty() {
                    last = ready_tails(decision, &last, &ready);
                }
                continue;
            }
            // A command this leader committed was decided in an earlier
            // ballot, with the dependencies it has here. A reporter behind
            // the source ballot put it in `reproposed` because the voters
            // that executed it retired it. Chaining it after the tail
            // would give it other dependencies than the ones it was
            // decided with, and anchoring the next proposal after it
            // would fork the order at a command executed long ago.
            //
            // The entries that follow it are still this ballot's to
            // propose: they were held out of the first pass, and leaving
            // them here left them proposed nowhere, installed at ACCEPT
            // with nothing to vote on, and everything chained after them
            // waiting (task-d34 review). They name it among their
            // dependencies, so they order after it everywhere.
            if leader.table.phase_of(c) >= Some(Phase::Commit) {
                effects.extend(leader.repropose_entries(decision, &ready, &mut published));
                if !ready.is_empty() {
                    last = ready_tails(decision, &last, &ready);
                }
                continue;
            }
            let deps = core::mem::replace(&mut last, alloc::vec![*c]);
            if !ready.is_empty() {
                last = ready_tails(decision, &last, &ready);
            }
            effects.extend(leader.repropose_counted(*c, deps, &mut published));
            effects.extend(leader.repropose_entries(decision, &ready, &mut published));
        }
        // What the candidate took in after cutting its own report, which
        // its selection cannot name, is proposed after the recovered
        // order as a fresh command would be (task-d33). This replica
        // acknowledged none of it, and a command a quorum accepted, or a
        // fast quorum could have learned, is in the selection, whose
        // reports intersect every such quorum: what is left was decided
        // nowhere.
        for c in &state.arrived {
            if decision.entries.contains_key(c)
                || decision.reproposed.contains(c)
                || leader.table.phase_of(c) != Some(Phase::PreAccept)
            {
                continue;
            }
            let deps = core::mem::replace(&mut last, alloc::vec![*c]);
            effects.extend(leader.repropose_counted(*c, deps, &mut published));
        }
        // The first fresh proposal of this ballot follows the recovered
        // order's tail. Re-proposing moved nothing in the table, so its
        // latest command is still the payload that reached this voter
        // last, which can sit in the middle of that order: a command
        // proposed after it would not wait for the tail on a replica
        // still behind it, and would execute first there (task-d06).
        if !last.is_empty() {
            leader.table.anchor_all(CONSERVATIVE_KEY, &last);
        }
        effects.extend(leader.release());
        (leader, effects)
    }

    /// Drop the durable records, payloads, proposals and votes of
    /// forgotten commands, as a follower does (task-d05). A forgotten
    /// command executed long ago: nothing is decided, released or served
    /// for it any more.
    ///
    /// Due as a follower's is (task-d110): past a multiple of the table,
    /// once the records added and the commands executed since the last
    /// sweep come to more than half of what it kept.
    ///
    /// Each execution first drops the history of what the table has
    /// forgotten since, in the order it executed (task-d111's cut (d)),
    /// so the sweep below is a backstop, as at a follower.
    fn forget_history(&mut self, executed: CommandId) {
        self.ledger.executed(executed);
        let table = &self.table;
        let gone = self.ledger.take_forgotten(
            |c| table.forgotten(c),
            crate::follower::FORGET_PER_EXECUTION,
            self.config
                .capacity
                .saturating_mul(crate::follower::HISTORY_SWEEP),
        );
        for c in gone {
            self.forget_one(c);
        }
        #[cfg(debug_assertions)]
        if self
            .ledger
            .executions()
            .is_multiple_of(crate::follower::FORGET_CHECK_EVERY)
        {
            self.check_forgetting();
        }
        // Due at the threshold, or once the table forgets the last
        // command the queue let go unforgotten (#221 review).
        let owed = self
            .ledger
            .owed_sweep()
            .is_some_and(|c| self.table.forgotten(c));
        if !owed
            && !self.ledger.sweep_due(
                self.config
                    .capacity
                    .saturating_mul(crate::follower::HISTORY_SWEEP),
            )
        {
            return;
        }
        let table = &self.table;
        self.ledger.retain(|c| !table.forgotten(c));
        self.payloads.retain(|c, _| !table.forgotten(c));
        self.served_payloads.retain(|c| !table.forgotten(c));
        self.proposals.retain(|c, _| !table.forgotten(c));
        self.answer_on_release.retain(|c| table.record(c).is_some());
        let marked = &self.answer_on_release;
        self.relayed.retain(|c| marked.contains(c));
        let proposals = &self.proposals;
        self.unsettled.retain(|(_, c)| proposals.contains_key(c));
        self.durable.retain(|(_, c)| proposals.contains_key(c));
        self.held_back.retain(|(_, c)| proposals.contains_key(c));
        self.by_barrier.retain(|_, c| proposals.contains_key(c));
        self.votes.retain(|c, _| !table.forgotten(c));
        // A retry of a forgotten command is refused from the table's
        // executed answer (`propose`), so its binding is not needed for
        // that, and kept it grew with every key ever submitted (task-d26).
        self.bindings.retain(|_, c| !table.forgotten(c));
        self.ledger.swept();
    }

    /// Drop what this leader keeps about `command`, which the table has
    /// forgotten, from every map [`Leader::forget_history`]'s sweep walks,
    /// with the indexes over its proposal (task-d111's cut (d)).
    fn forget_one(&mut self, command: CommandId) {
        if let Some(key) = self.ledger.forget(&command)
            && self.bindings.get(&key) == Some(&command)
        {
            self.bindings.remove(&key);
        }
        let payload = self.payloads.remove(&command);
        self.served_payloads.remove(&command);
        if let Some(proposal) = self.proposals.remove(&command) {
            self.unsettled.remove(&(proposal.seqnum, command));
            self.durable.remove(&(proposal.seqnum, command));
            self.held_back.remove(&(proposal.seqnum, command));
            if self.by_barrier.get(&proposal.barrier) == Some(&command) {
                self.by_barrier.remove(&proposal.barrier);
            }
        }
        self.answer_on_release.remove(&command);
        self.relayed.remove(&command);
        self.votes.remove(&command);
        if let Some(payload) = payload
            && self.bindings.get(&payload.retry_key) == Some(&command)
        {
            self.bindings.remove(&payload.retry_key);
        }
    }

    /// Debug check of cut (d) against the sweep it replaces, as a
    /// follower's (`Follower::check_forgetting`).
    #[cfg(debug_assertions)]
    fn check_forgetting(&self) {
        let queued = self.ledger.forget_queued();
        let kept = |c: &CommandId| {
            !self.table.forgotten(c) || queued.contains(c) || self.ledger.sweep_owes(c)
        };
        debug_assert!(
            self.ledger.records().all(|(c, _)| kept(c))
                && self.payloads.keys().all(kept)
                && self.served_payloads.iter().all(kept)
                && self.proposals.keys().all(kept)
                && self.votes.keys().all(kept)
                && self.bindings.values().all(kept),
            "a forgotten command's history was kept and is not in the forget queue"
        );
        debug_assert!(
            self.held_back
                .iter()
                .all(|(_, c)| self.proposals.contains_key(c)),
            "a held-back re-proposal outlived its proposal"
        );
    }

    /// Propose a known command under this ballot with `deps`. With
    /// `publish` false the proposal is recorded and made durable but not
    /// sent: [`Leader::resend_unvoted`] sends it, paced (task-d07).
    /// Re-propose `entries` of `decision`, in the order given, with their
    /// selected dependencies (task-d34): the entries that waited on a
    /// re-proposed command, once it is.
    fn repropose_entries(
        &mut self,
        decision: &SyncDecision,
        entries: &[CommandId],
        published: &mut usize,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();
        for c in entries {
            let entry = &decision.entries[c];
            if entry.phase >= Phase::Commit && self.table.phase_of(c) == Some(Phase::Executed) {
                continue;
            }
            effects.extend(self.repropose_counted(*c, entry.deps.clone(), published));
        }
        effects
    }

    /// [`Leader::repropose`], published with its batch while fewer than
    /// `REPROPOSE_BATCH` have been, and counted in `published` only if a
    /// proposal was made. Counted before, a command this leader executed
    /// and retired, which `repropose` makes nothing of, used up a place in
    /// the first batch (the takeover gap on #98).
    fn repropose_counted(
        &mut self,
        command: CommandId,
        deps: Vec<CommandId>,
        published: &mut usize,
    ) -> Vec<Effect> {
        let made = self.repropose(command, deps, *published < REPROPOSE_BATCH);
        if !made.is_empty() {
            *published += 1;
        }
        made
    }

    fn repropose(
        &mut self,
        command: CommandId,
        deps: Vec<CommandId>,
        publish: bool,
    ) -> Vec<Effect> {
        let Some(boot) = self.boot else {
            return Vec::new();
        };
        if self.table.phase_of(&command).is_none() {
            return Vec::new();
        }
        if self.table.record(&command).is_none() {
            // Executed here and already retired: there is no record to
            // propose from, and nothing to decide. The selection says the
            // command is committed (the candidate marks what it executed),
            // so every voter installing it commits it without a vote.
            return Vec::new();
        }
        // Re-proposal installs the order this ballot chose. A command that
        // is only accepted locally, from a lower synchronized ballot, must
        // take the new dependencies: the proposal carries them, so leaving
        // the table and the durable row on the old ones would make this
        // leader learn from evidence about a different order. Committed and
        // executed records keep theirs.
        if self.table.phase_of(&command) < Some(Phase::Commit)
            && self.table.accept(command, deps.clone()).is_err()
        {
            return Vec::new();
        }
        let ballot = self.config.quorum.ballot();
        let epoch = self.config.identity.epoch;
        let seqnum = self.seqnum;
        self.seqnum += 1;
        let record = self.table.record(&command).expect("known").clone();
        let barrier = self.alloc.as_mut().expect("booted").allocate();
        let mut updates = alloc::vec![
            dependency_update(epoch, &command, &record).expect("bounded"),
            proposal_update(
                epoch,
                &command,
                &ProposalRecordV1 {
                    ballot,
                    seqnum,
                    deps: deps.clone(),
                    path: record.path,
                },
            )
            .expect("bounded"),
        ];
        // The payload row too, where it is not durable here yet (task-d90):
        // the batch is this leader's acceptance, and the acceptance has to
        // rest on a payload a recovery can re-execute from. A payload
        // fetched for the selection was written in a batch no vote
        // required, and one still in flight when this replica took the
        // lead is no batch this leader follows.
        if !self.served_payloads.contains(&command)
            && let Some(payload) = self
                .payloads
                .get(&command)
                .filter(|p| Some(p.admission_digest()) == record.payload)
        {
            updates.push(payload_update(&command, payload).expect("bounded"));
        }
        self.ledger.stage(barrier, command, record.clone());
        let proposal = FastAck {
            replica: self.config.identity.replica,
            ballot,
            command,
            deps: deps.clone(),
            paths: record.paths.clone(),
            path: record.path,
            admission: record.payload.unwrap_or_else(|| admission_digest(None, 0)),
            seqnum: Some(seqnum),
        };
        if publish {
            let context = self.ballots.context(boot, ballot, LocalJournalSeq::ZERO);
            // Encoded once for every voter it goes to (task-d61).
            let frame = self.proposal_message(proposal.clone()).encode();
            let outbox = self.outbox.as_mut().expect("booted");
            for voter in &self.config.identity.voters {
                if *voter == self.config.identity.replica {
                    continue;
                }
                outbox.publish(PendingSend {
                    context,
                    requires: alloc::vec![barrier],
                    to: PeerId {
                        replica: *voter,
                        incarnation: ReplicaIncarnation::ZERO,
                    },
                    frame: frame.clone(),
                });
            }
        }
        let admission = proposal.admission;
        let mut set = VoteSet::new(self.config.quorum.clone(), command);
        set.add(Vote::Fast(proposal)).expect("leader proposal");
        self.votes.insert(command, set);
        self.adopt_early_votes(command);
        self.put_proposal(Proposal {
            command,
            barrier,
            seqnum,
            deps,
            path: record.path,
            paths: record.paths.clone(),
            admission,
            durable: false,
            updates: updates.clone(),
            attempts: 1,
            accepts: true,
            executed: false,
            published: publish,
            sent: None,
        });
        // The row is ACCEPT at this ballot (or a decision): once it is
        // durable, it is this leader's adoption of its own order.
        self.adopt_own(command, barrier);
        alloc::vec![Effect::Persist(PersistBatch {
            barrier,
            base: None,
            updates,
        })]
    }

    /// Send each voter, again, the proposals of this ballot it has not
    /// adopted, oldest first and at most `per_voter` of them (task-d07).
    ///
    /// The protocol assumes a proposal reaches every voter, and the
    /// transport drops a frame by design when a lane is full or the peer
    /// is not linked yet, so re-sending is the protocol's job. A voter
    /// that never received a proposal holds everything after it -- every
    /// later proposal depends on it through the conservative key -- and
    /// executes nothing more until a Sync realigns it.
    ///
    /// What a voter lacks is read from its adoption acknowledgements, the
    /// only votes that say it received a proposal: a fast acknowledgement
    /// goes out when the payload arrives, proposal or not, and a voter
    /// that fast-acknowledged a command it never received the proposal of
    /// used to be credited with it -- and with every proposal before it.
    /// The dependency chain is total, so a voter that adopted a proposal
    /// holds every earlier one, but that says nothing about which of its
    /// acknowledgements reached this leader: one for an earlier proposal
    /// can be lost while a later one is counted. So a proposal this
    /// leader has not committed is sent until the voter's adoption of it
    /// arrives, however far past it the voter's counted adoptions reach;
    /// without it the leader may never learn the command, and everything
    /// chained after it waits (task-d15). Only a proposal the leader has
    /// committed is skipped once the voter adopts a later one: the voter
    /// it still lacks may have executed and retired the command and keep
    /// no record to answer from. Only durable proposals go: a proposal
    /// still becoming durable has its first send queued behind its batch
    /// already.
    ///
    /// Paced by the caller, which calls it on a timer rather than per
    /// event; bounded per voter per call, so a voter that is gone costs
    /// at most `per_voter` frames each time.
    ///
    /// A re-send goes out only once its answer is due (task-d49): an
    /// interval of calls after the proposal's last send, the first send
    /// included, and longer after each re-send (`RESEND_BACKOFF_CAP`).
    /// Sent on the first call after it, a proposal that went out a moment
    /// before was re-sent to a voter that already held it and whose vote
    /// was on its way, and that vote came back twice, the second refused
    /// as a duplicate: about two a command with one client. The interval
    /// is the voter's own: the calls its recent adoptions took to arrive
    /// (a high percentile, [`RESEND_LATENCY_SAMPLES`] of them), between
    /// one call and [`RESEND_INTERVAL_CAP`]. The window is the voter's
    /// first `per_voter` proposals that are due, so a proposal waiting
    /// out its interval does not keep a lost one behind it from being
    /// sent.
    ///
    /// A decided proposal is re-sent to a voter at that back-off only
    /// until it is [`RESEND_HANDOFF_CALLS`] old. After that it is mostly
    /// the voter's to fetch from executed history (task-d08): the leader
    /// needs nothing from it, and a voter that is that far behind is
    /// loaded by every frame it cannot use yet. It is still trickled, one
    /// a voter a call, each that many calls apart, for a voter that has
    /// nothing to catch up on to ask for it.
    pub fn resend_unvoted(&mut self, per_voter: usize) -> Vec<Effect> {
        let Some(boot) = self.boot else {
            return Vec::new();
        };
        if !self.is_leading() || per_voter == 0 {
            return Vec::new();
        }
        let call = self.resend_calls;
        self.resend_calls += 1;
        {
            let (proposals, votes) = (&self.proposals, &self.votes);
            self.resent.retain(|(command, voter), _| {
                proposals.contains_key(command)
                    && !votes.get(command).is_some_and(|v| v.adopted_by(voter))
            });
            // A second copy comes back within a round trip of the first,
            // or never: a call is long enough to wait for it.
            self.answered.retain(|_, (_, at)| *at + 1 > call);
        }
        let me = self.config.identity.replica;
        let ballot = self.config.quorum.ballot();
        let context = self.ballots.context(boot, ballot, LocalJournalSeq::ZERO);
        let voters: Vec<ReplicaId> = self
            .config
            .identity
            .voters
            .iter()
            .copied()
            .filter(|v| *v != me)
            .collect();
        let throughs: Vec<Option<(u64, CommandId)>> =
            voters.iter().map(|v| self.adopted_through(v)).collect();
        let mut chosen: Vec<(CommandId, ReplicaId, Resend)> = Vec::new();
        let mut scanned = 0u64;
        for (voter, through) in voters.iter().zip(&throughs) {
            let interval = self.resend_interval(voter);
            let mut taken = 0;
            let mut trickled = false;
            // Before the voter's highest adoption only an undecided
            // proposal can be due, and every undecided one is unsettled;
            // past it, any durable one.
            let below = through
                .map(|top| self.unsettled.range(..=top))
                .into_iter()
                .flatten();
            let past = match through {
                Some(top) => self
                    .durable
                    .range((Bound::Excluded(*top), Bound::Unbounded)),
                None => self.durable.range::<(u64, CommandId), _>(..),
            };
            for (seqnum, command) in below.chain(past) {
                if taken == per_voter {
                    break;
                }
                scanned += 1;
                if !self.proposals[command].durable || self.adopted(command, voter) {
                    continue;
                }
                if let Some(kind) = self.resend_kind(
                    *seqnum,
                    command,
                    voter,
                    through.map(|(t, _)| t),
                    (call, interval),
                    &mut trickled,
                ) {
                    taken += 1;
                    chosen.push((*command, *voter, kind));
                }
            }
        }
        debug_assert_eq!(chosen, self.resend_by_sort(per_voter, call));
        self.counts.calls += 1;
        self.counts.scanned += scanned;
        let mut sends = Vec::new();
        for (command, voter, kind) in chosen {
            match kind {
                Resend::Deferred => {
                    let p = self.proposals.get_mut(&command).expect("in the order");
                    p.sent.get_or_insert(call);
                    self.counts.deferred += 1;
                    // Out to the voter now: it counts in the window
                    // `top_up` keeps, and is not sent it twice.
                    self.first_sends
                        .entry(voter)
                        .or_default()
                        .out
                        .insert(command);
                }
                Resend::HandedOff => {
                    let r = self.resent.entry((command, voter)).or_insert(Resent {
                        next: 0,
                        gap: 0,
                        handed_off: false,
                    });
                    if !r.handed_off {
                        r.handed_off = true;
                        self.counts.handed_off += 1;
                    }
                    r.next = call + RESEND_HANDOFF_CALLS;
                    self.counts.decided += 1;
                }
                kind => {
                    let interval = self.resend_interval(&voter);
                    let r = self.resent.entry((command, voter)).or_insert(Resent {
                        next: 0,
                        gap: 0,
                        handed_off: false,
                    });
                    r.gap = if r.gap == 0 {
                        1
                    } else {
                        r.gap.saturating_mul(2).min(RESEND_BACKOFF_CAP)
                    };
                    r.next = call + u64::from(r.gap).max(interval);
                    match kind {
                        Resend::Decided => self.counts.decided += 1,
                        Resend::Acknowledged => self.counts.acknowledged += 1,
                        _ => self.counts.unanswered += 1,
                    }
                }
            }
            let p = &self.proposals[&command];
            let frame = self.proposal_frame(p);
            let to = PeerId {
                replica: voter,
                incarnation: ReplicaIncarnation::ZERO,
            };
            sends.push(PendingSend {
                context,
                requires: alloc::vec![p.barrier],
                to,
                frame,
            });
            sends.extend(self.own_adoption_send(command, to));
        }
        // A voter that has not promised this ballot is asked to, with the
        // frontier this leader executed through, which a late joiner has
        // to fetch (task-d33). Its promise is answered with the Sync.
        let executed = self.learner.executed_through();
        for voter in &self.config.identity.voters {
            if self.joined.contains(voter) {
                continue;
            }
            sends.push(PendingSend {
                context,
                requires: Vec::new(),
                to: PeerId {
                    replica: *voter,
                    incarnation: ReplicaIncarnation::ZERO,
                },
                frame: ProtocolMessage::NewLeader { ballot, executed }.encode(),
            });
        }
        if sends.is_empty() {
            return Vec::new();
        }
        let outbox = self.outbox.as_mut().expect("booted");
        for send in sends {
            outbox.publish(send);
        }
        self.release()
    }

    /// A voter's promise of this leader's own ballot, after its campaign:
    /// a voter the campaign missed or whose Sync was lost, answering
    /// `resend_unvoted`'s ask. It is sent the Sync this leader leads
    /// from, which it installs as the voters the campaign heard from did
    /// (task-d33).
    fn on_late_promise(
        &mut self,
        from: ReplicaId,
        ballot: Ballot,
        replica: ReplicaId,
    ) -> Vec<Effect> {
        let Some(boot) = self.boot else {
            return Vec::new();
        };
        if replica != from
            || ballot != self.config.quorum.ballot()
            || !self.config.identity.voters.contains(&replica)
            || !self.is_leading()
        {
            return Vec::new();
        }
        let Some(decision) = self.selection.clone() else {
            return Vec::new();
        };
        let context = self.ballots.context(boot, ballot, LocalJournalSeq::ZERO);
        let outbox = self.outbox.as_mut().expect("booted");
        outbox.publish(PendingSend {
            context,
            requires: Vec::new(),
            to: PeerId {
                replica,
                incarnation: ReplicaIncarnation::ZERO,
            },
            frame: ProtocolMessage::Sync(decision).encode(),
        });
        self.release()
    }

    /// This leader's adoption of `command`, to be sent to `to` again beside
    /// its proposal, once it has counted it: a voter the first send
    /// missed would otherwise lack the leader's copy of the slow majority
    /// (task-d19). Sent as it was first published, context and barriers
    /// included.
    fn own_adoption_send(&self, command: CommandId, to: PeerId) -> Option<PendingSend> {
        let me = self.config.identity.replica;
        if !self.votes.get(&command).is_some_and(|v| v.adopted_by(&me)) {
            return None;
        }
        self.replay.kept(&command).iter().find_map(|e| {
            matches!(
                ProtocolMessage::decode(&e.frame),
                Ok(ProtocolMessage::SlowAck(ref ack)) if ack.replica == me
            )
            .then(|| PendingSend {
                context: e.context,
                requires: e.requires.clone(),
                to,
                frame: e.frame.clone(),
            })
        })
    }

    /// The proposal frame for `p`, as it was first published.
    fn proposal_frame(&self, p: &Proposal) -> Vec<u8> {
        self.proposal_message(FastAck {
            replica: self.config.identity.replica,
            ballot: self.config.quorum.ballot(),
            command: p.command,
            deps: p.deps.clone(),
            paths: p.paths.clone(),
            path: p.path,
            admission: p.admission,
            seqnum: Some(p.seqnum),
        })
        .encode()
    }

    /// Answer a follower's ask for proposals of this ballot (task-d09):
    /// each durable one named is sent to it again, as a re-send would.
    /// A command this leader did not propose in the ballot, or whose
    /// batch is not durable yet, is not answered.
    fn serve_proposals(
        &mut self,
        to: ReplicaId,
        ballot: Ballot,
        commands: &[CommandId],
    ) -> Vec<Effect> {
        let Some(boot) = self.boot else {
            return Vec::new();
        };
        if !self.is_leading() || ballot != self.config.quorum.ballot() {
            return Vec::new();
        }
        let context = self.ballots.context(boot, ballot, LocalJournalSeq::ZERO);
        let to = PeerId {
            replica: to,
            incarnation: ReplicaIncarnation::ZERO,
        };
        let sends: Vec<PendingSend> = commands
            .iter()
            .filter_map(|c| self.proposals.get(c))
            .filter(|p| p.durable)
            .take(crate::messages::MAX_PROPOSAL_ASK)
            .flat_map(|p| {
                core::iter::once(PendingSend {
                    context,
                    requires: alloc::vec![p.barrier],
                    to,
                    frame: self.proposal_frame(p),
                })
                .chain(self.own_adoption_send(p.command, to))
            })
            .collect();
        if sends.is_empty() {
            return Vec::new();
        }
        let outbox = self.outbox.as_mut().expect("booted");
        for send in sends {
            outbox.publish(send);
        }
        self.release()
    }

    /// The highest sequence number of this ballot whose whole prefix is
    /// committed here with its batch durable, if any (task-d09).
    ///
    /// Every sequence number below `seqnum` went to a proposal of this
    /// ballot. One that is no longer in `proposals` was forgotten, which
    /// only happens to a command executed long ago. So the prefix ends
    /// just before the lowest proposal that is either not durable or not
    /// committed.
    pub fn committed_through(&self) -> Option<u64> {
        debug_assert!(self.indexes_agree());
        self.commit_frontier()
    }

    /// [`Leader::committed_through`], without the index check: read on
    /// every peer and storage event (task-d74), where the check, which
    /// rebuilds every index, would be the whole cost of a debug build.
    fn commit_frontier(&self) -> Option<u64> {
        // A settled proposal is durable and executed, so the first open
        // one is unsettled (task-d46), and `unsettled` is in sequence
        // order, so the first open one found is the lowest.
        let open = self
            .unsettled
            .iter()
            .map(|(_, c)| &self.proposals[c])
            .find(|p| {
                !(p.durable
                    && (p.executed || self.table.phase_of(&p.command) >= Some(Phase::Commit)))
            })
            .map(|p| p.seqnum);
        match open {
            Some(first) => first.checked_sub(1),
            None => self.seqnum.checked_sub(1),
        }
    }

    /// The read index of a read arriving now (task-d50; design Section
    /// 6.3): this ballot's next sequence number. Every command a read
    /// must see that was learned in this ballot was proposed below it.
    ///
    /// `None` while this leader is not leading, and while it has proposed
    /// nothing in its ballot: what earlier ballots learned is ordered
    /// ahead of this ballot's first proposal (the chain is total,
    /// task-d06), so a read waits for one of them to execute, and with
    /// none there is nothing to wait for that proves it.
    pub fn read_index(&self) -> Option<u64> {
        (self.is_leading() && self.seqnum > 0).then_some(self.seqnum)
    }

    /// Whether every proposal of this ballot below `index` has executed
    /// here (task-d50). A proposal no longer held executed long ago.
    pub fn executed_below(&self, index: u64) -> bool {
        debug_assert!(self.indexes_agree());
        // The unsettled set is in sequence order, and a settled proposal
        // is executed (task-d46).
        self.unsettled
            .iter()
            .take_while(|(seqnum, _)| *seqnum < index)
            .all(|(_, c)| {
                self.proposals[c].executed || self.table.phase_of(c) >= Some(Phase::Executed)
            })
    }

    /// Announce the commit frontier if it moved since it was last
    /// announced (task-d74).
    ///
    /// A follower's acknowledgements go to the leader and its frontend
    /// only, so the frontier is the one way a follower learns that a
    /// command it adopted is committed. Announced on the re-send timer
    /// alone, a follower's execution trailed the leader's by up to that
    /// interval; announced in the step that moves it, by one message.
    ///
    /// While [`Leader::set_frontier_at_turn_end`] is on, the node calls
    /// it once at the end of its turn instead, so each voter gets one
    /// `Committed` frame a turn however many batches turned durable in it.
    pub fn announce_advanced(&mut self) -> Vec<Effect> {
        if !self.frontier_owed() {
            return Vec::new();
        }
        self.announce_committed()
    }

    /// Whether the commit frontier moved since it was last announced.
    pub fn frontier_owed(&self) -> bool {
        if self.boot.is_none() || !self.is_leading() {
            return false;
        }
        let through = self.commit_frontier();
        through.is_some() && through > self.announced
    }

    /// Hold the frontier a step moves for the end of the node's turn
    /// (#242 follow-up): the node then calls [`Leader::announce_advanced`]
    /// once a turn, where it lowers the turn's group.
    ///
    /// Announced in every step that moves it, a new leader's backlog sent
    /// each voter a `Committed` frame for every re-proposal batch that
    /// turned durable: at 823 behind, 828 frames a voter in a second,
    /// past what its control lane takes. Once a turn, a voter gets at most
    /// one a turn, and still in the turn that moved the frontier. The node
    /// sets it while it lowers in groups, since that is where its turn
    /// ends; off, the frontier goes out in the step that moves it.
    pub fn set_frontier_at_turn_end(&mut self, on: bool) {
        self.frontier_at_turn_end = on;
    }

    /// Tell every other voter this ballot's commit frontier (task-d09).
    ///
    /// Learning is otherwise all-to-all: a follower commits a command
    /// from the acknowledgements it receives itself, each published once
    /// on a lane that drops frames by design, and nothing publishes a
    /// missed one again to it. The leader's commit is a decision under
    /// the crash-fault model, and the proposal the follower adopted
    /// carries the leader's dependencies, so a follower that adopted a
    /// proposal at or below the frontier may commit it without the
    /// acknowledgements it missed.
    ///
    /// Paced by the caller with the re-send. The frame is small and
    /// carries the whole frontier, so one that is dropped is repaired by
    /// the next. It needs no barrier: the frontier counts only proposals
    /// whose batch is durable.
    pub fn announce_committed(&mut self) -> Vec<Effect> {
        let Some(boot) = self.boot else {
            return Vec::new();
        };
        if !self.is_leading() {
            return Vec::new();
        }
        let Some(through) = self.committed_through() else {
            return Vec::new();
        };
        self.announced = Some(through);
        let me = self.config.identity.replica;
        let ballot = self.config.quorum.ballot();
        let context = self.ballots.context(boot, ballot, LocalJournalSeq::ZERO);
        let frame = ProtocolMessage::Committed { ballot, through }.encode();
        let outbox = self.outbox.as_mut().expect("booted");
        for voter in &self.config.identity.voters {
            if *voter == me {
                continue;
            }
            self.sent_bytes.frontiers += 1;
            self.sent_bytes.frontier_bytes += frame.len() as u64;
            outbox.publish(PendingSend {
                context,
                requires: Vec::new(),
                to: PeerId {
                    replica: *voter,
                    incarnation: ReplicaIncarnation::ZERO,
                },
                frame: frame.clone(),
            });
        }
        self.release()
    }

    /// Send `from` again the pages of the report this replica sent it
    /// for `ballot` that it asks for (task-d28), from the same version.
    fn answer_report_pages(
        &mut self,
        from: ReplicaId,
        ballot: Ballot,
        asked: &[u32],
    ) -> Vec<Effect> {
        let (Some(served), Some(boot)) = (self.served_report.as_ref(), self.boot) else {
            return Vec::new();
        };
        let pages = served.answer(from, ballot, asked);
        if pages.is_empty() {
            return Vec::new();
        }
        let to = served.to;
        let context = self.ballots.context(boot, ballot, LocalJournalSeq::ZERO);
        let outbox = self.outbox.as_mut().expect("booted");
        for page in pages {
            outbox.publish(PendingSend {
                context,
                requires: Vec::new(),
                to,
                frame: ProtocolMessage::ReportPage(page).encode(),
            });
        }
        self.release()
    }

    /// Deliver the report owed to a candidate once every batch before the
    /// cut is durable.
    fn deliver_due_report(&mut self) -> Vec<Effect> {
        let Some(due) = self.report_due.clone() else {
            return Vec::new();
        };
        let all_durable = self
            .outbox
            .as_ref()
            .is_some_and(|o| due.requires.iter().all(|b| o.is_durable(b)));
        if !all_durable {
            return Vec::new();
        }
        self.report_due = None;
        let Some(boot) = self.boot else {
            return Vec::new();
        };
        let report = self.report(due.ballot);
        self.report_cut = Some((
            due.ballot,
            report.entries.iter().map(|e| e.command).collect(),
        ));
        let context = self
            .ballots
            .context(boot, due.ballot, LocalJournalSeq::ZERO);
        let outbox = self.outbox.as_mut().expect("booted");
        // The pages are kept: one that is lost is asked for again, and
        // answered from this version, never a regenerated one (task-d28).
        let served = crate::summary::ServedReport::new(due.to, &report);
        for page in &served.pages {
            outbox.publish(PendingSend {
                context,
                requires: Vec::new(),
                to: due.to,
                frame: ProtocolMessage::ReportPage(page.clone()).encode(),
            });
        }
        self.served_report = Some(served);
        self.release()
    }

    /// The durable ledger (journal-durable records only).
    pub const fn ledger(&self) -> &DurableLedger {
        &self.ledger
    }

    /// What this leader keeps of its history beside the ledger, map by
    /// map (#221 review).
    pub fn history_maps(&self) -> crate::summary::HistoryMaps {
        crate::summary::HistoryMaps {
            payloads: self.payloads.len(),
            served_payloads: self.served_payloads.len(),
            votes: self.votes.len(),
            bindings: self.bindings.len(),
            bare_bindings: self.ledger.bare_bindings(),
            answer_on_release: self.answer_on_release.len(),
            relayed: self.relayed.len(),
            proposals: self.proposals.len(),
            ..crate::summary::HistoryMaps::default()
        }
    }

    /// Retry keys bound to a command here (task-d26): the commands still
    /// live or recently retired, never every key submitted.
    pub fn bindings_held(&self) -> usize {
        self.bindings.len()
    }

    /// The recovery report for `ballot` from durable state at this cut.
    ///
    /// History this replica keeps nothing else about is left out, as a
    /// follower leaves it out (task-d05).
    pub fn report(&self, ballot: Ballot) -> RecoveryReport {
        let mut report =
            self.ledger
                .report(self.config.identity.replica, ballot, self.ballots.synced());
        crate::follower::report_durable_payloads(&mut report, &self.served_payloads);
        // The report is labelled with the ballot this leader synchronized
        // to, and its selection is that ballot's state, whether or not its
        // re-proposal batches are durable yet, or were made at all for an
        // entry whose payload it lacks (task-d34). Reported from the rows
        // alone, a command it selected at ACCEPT read as this replica's
        // older PRE-ACCEPT, or as never accepted, at the source ballot,
        // and the next selection re-proposed a command another voter had
        // executed.
        if let Some(selection) = self
            .selection
            .as_ref()
            .filter(|d| d.ballot == self.ballots.synced())
        {
            crate::follower::overlay_selected(&mut report, selection.entries.iter());
        }
        report.entries.retain(|e| !self.table.forgotten(&e.command));
        crate::follower::report_executed_as_committed(&mut report, &self.table);
        report.entries.sort_by_key(|e| e.command);
        report
    }

    /// The next command to execute through the materializer, if any.
    pub fn next_executable(&self) -> Option<CommandId> {
        self.learner
            .next_executable(&self.table, |c| self.proposals.get(c).map(|p| p.seqnum))
    }

    /// The table's size and its oldest unexecuted record's wait, in this
    /// leader's proposal order (task-d25, #98).
    pub fn table_waits(&self) -> crate::TableWaits {
        self.table
            .waits(|c| self.proposals.get(c).map(|p| p.seqnum))
    }

    /// The durable payload of a proposed command.
    pub fn payload(&self, command: &CommandId) -> Option<&PayloadRecordV1> {
        self.payloads.get(command)
    }

    /// The execution frontier.
    pub const fn executed_through(&self) -> ExecutionPosition {
        self.learner.executed_through()
    }

    /// Count this replica's executions durable only as its runtime says
    /// they are ([`Self::executions_durable_through`]), not when they are
    /// applied (task-d90). For a runtime whose execution groups reach
    /// the journal after their outcomes are taken.
    pub fn track_execution_durability(&mut self) {
        self.table.track_execution_durability();
    }

    /// This replica's journal holds its executions through `position`
    /// (task-d90): those may now be forgotten, in their turn.
    pub fn executions_durable_through(&mut self, position: ExecutionPosition) {
        let undurable = self
            .learner
            .executed_through()
            .get()
            .saturating_sub(position.get());
        self.table.executions_durable_except(undurable);
    }

    /// The materializer applied `command`: seal and publish the established
    /// result, then learn whatever the new phase enables.
    pub fn applied(
        &mut self,
        command: CommandId,
        outcome: &AppliedOutcome,
    ) -> Result<Vec<Effect>, LearnError> {
        let result = self.learner.established(
            &mut self.table,
            command,
            self.config.identity.epoch,
            self.config.quorum.ballot(),
            outcome,
        )?;
        // Read before the history sweep can take its votes.
        if !result.fast_path() {
            let reason = self.missed_fast(&command);
            self.missed.note(command, reason, self.config.capacity);
        }
        self.learn();
        self.forget_history(command);
        // A command whose result was released speculatively is already
        // disclosed; releasing it again would answer one caller twice.
        // A command whose result was *not* is only disclosed here.
        //
        // This is the final path the speculative one is an optimization
        // of, and without it a command the companion declined -- one
        // that is not speculable, one over the overlay's budget, one
        // whose authorized view could not be built -- would execute,
        // become durable, and never be answered. Its caller would wait
        // on a disclosure nothing was going to assemble.
        //
        // It rests on more evidence than the speculative release, not
        // less: the command is committed and its whole prefix has
        // executed before it. That is why it carries `speculative:
        // false`. It goes out as the command executes, not at its
        // group's `JournalDurable` (task-d90, design Section 4.5): it
        // rests on recovery reproducing the decision and its inputs, and
        // the establishment above waits for the group instead, since a
        // voter forgets only executions its journal holds.
        let released = self.speculation.released(&command);
        self.speculation
            .reconcile(command, outcome.position, outcome.result_digest)
            .map_err(LearnError::Speculation)?;
        if let Some(proposal) = self.proposals.get_mut(&command) {
            proposal.executed = true;
        }
        self.settle(command);
        let mut effects = alloc::vec![Effect::Established(result.clone())];
        // A marked command's collector answers only on a final release
        // (task-d100, step A), so it goes out even where the command was
        // speculated: its speculative release was never sent
        // ([`Leader::release_ready`]).
        let marked = self.answer_on_release.contains(&command);
        if self.is_leading() && (!released || marked) {
            let release = ReleasedResult::from_gate(result, outcome.response.clone(), false);
            effects.push(Effect::Released(self.with_admission(release)));
        }
        effects.extend(self.release_ready());
        Ok(effects)
    }

    fn learn(&mut self) {
        // A fresh proposal is ACCEPT here as soon as it is proposed when
        // its guard passes (task-d89), but the leader's fast vote is the
        // proposal itself: a follower that saw the submission first can
        // acknowledge before the proposal batch is durable, and a fast
        // quorum counted over a volatile proposal is one a restarted
        // leader's report would deny. So nothing this leader proposed is
        // committed until its batch is durable, as it was when the
        // acceptance waited for that batch. The same holds for the
        // adoptions an early proposal brings back before the batch is
        // durable (task-d98): they are counted, and nothing is committed
        // over them until the batch is.
        let proposals = &self.proposals;
        let committed = self.learner.commit_learned_where(
            &mut self.table,
            |c| self.votes.get(c),
            |c| proposals.get(c).is_none_or(|p| p.durable),
        );
        if let Some(learned) = self.learned.as_mut() {
            learned.extend(committed);
        }
    }

    /// Keep the commands this leader commits from its votes until
    /// [`Leader::take_learned`] takes them (task-d62): a driver timing
    /// how long a command takes from learned to released starts here.
    /// Off unless asked for, since a caller that never takes them would
    /// hold every command.
    pub fn observe_learning(&mut self) {
        self.learned.get_or_insert_with(Vec::new);
    }

    /// The marked commands whose release the daemon publishes again,
    /// each with its retry key and the decided payload's admission
    /// digest, since this was last called (task-d100, step A).
    pub fn take_release_again(&mut self) -> Vec<(CommandId, RetryKey, Digest32)> {
        core::mem::take(&mut self.release_again)
    }

    /// The commands committed from votes since this was last called,
    /// in commit order (task-d62); empty unless observed.
    pub fn take_learned(&mut self) -> Vec<CommandId> {
        self.learned
            .as_mut()
            .map(core::mem::take)
            .unwrap_or_default()
    }

    /// Debug check of the release gate's frontier (task-d111's cut (e))
    /// against the walk from the first unsettled proposal it replaces:
    /// every unexecuted proposal before it was released, so the gate
    /// passes over them as it would have from the start.
    #[cfg(debug_assertions)]
    fn check_release_from(&self) {
        for (seqnum, c) in &self.unsettled {
            if *seqnum >= self.release_from {
                break;
            }
            let executed =
                self.proposals[c].executed || self.table.phase_of(c) >= Some(Phase::Executed);
            debug_assert!(
                executed || self.speculation.released(c),
                "an unexecuted proposal below the release frontier was not released"
            );
        }
    }

    /// Unexecuted proposals in the leader's order.
    fn unexecuted_in_order(&self) -> Vec<CommandId> {
        debug_assert!(self.indexes_agree());
        // An unexecuted proposal is unsettled, and the set is in sequence
        // order already (task-d46).
        self.unsettled
            .iter()
            .filter(|(_, c)| {
                !self.proposals[c].executed && self.table.phase_of(c) < Some(Phase::Executed)
            })
            .map(|(_, c)| *c)
            .collect()
    }

    /// Enter `proposal`, keeping the indexes over proposals exact: a
    /// proposal replacing one of the same command takes its place in
    /// them (task-d46).
    fn put_proposal(&mut self, proposal: Proposal) {
        let command = proposal.command;
        self.release_from = self.release_from.min(proposal.seqnum);
        if let Some(old) = self.proposals.get(&command) {
            self.unsettled.remove(&(old.seqnum, command));
            self.durable.remove(&(old.seqnum, command));
            self.held_back.remove(&(old.seqnum, command));
            self.by_barrier.remove(&old.barrier);
        }
        if !(proposal.durable && proposal.executed) {
            self.unsettled.insert((proposal.seqnum, command));
        }
        if proposal.durable {
            self.durable.insert((proposal.seqnum, command));
        }
        if !proposal.published {
            self.held_back.insert((proposal.seqnum, command));
        }
        self.by_barrier.insert(proposal.barrier, command);
        self.proposals.insert(command, proposal);
    }

    /// Leave the unsettled set once `command`'s proposal is both durable
    /// and executed (task-d46).
    fn settle(&mut self, command: CommandId) {
        if let Some(p) = self.proposals.get(&command)
            && p.durable
            && p.executed
        {
            self.unsettled.remove(&(p.seqnum, command));
        }
    }

    /// Whether the indexes over proposals say what a pass over every
    /// proposal would; checked in debug builds.
    fn indexes_agree(&self) -> bool {
        let unsettled: BTreeSet<(u64, CommandId)> = self
            .proposals
            .values()
            .filter(|p| !(p.durable && p.executed))
            .map(|p| (p.seqnum, p.command))
            .collect();
        let by_barrier: BTreeMap<BarrierId, CommandId> = self
            .proposals
            .values()
            .map(|p| (p.barrier, p.command))
            .collect();
        let durable: BTreeSet<(u64, CommandId)> = self
            .proposals
            .values()
            .filter(|p| p.durable)
            .map(|p| (p.seqnum, p.command))
            .collect();
        unsettled == self.unsettled && by_barrier == self.by_barrier && durable == self.durable
    }

    /// The next proposal to speculate (task-29), if the bound allows and
    /// every earlier unexecuted proposal has a tentative outcome.
    pub fn next_speculable(&self) -> Option<SpeculationRequest> {
        if !self.is_leading() {
            return None;
        }
        self.speculation
            .next_request(&self.unexecuted_in_order(), self.learner.executed_through())
    }

    /// Record a tentative outcome; releases whatever the learned prefix
    /// now allows.
    pub fn speculated(&mut self, outcome: TentativeOutcome) -> Vec<Effect> {
        if !self.proposals.contains_key(&outcome.command) {
            return Vec::new();
        }
        self.speculation.record(outcome);
        self.release_ready()
    }

    /// The companion refused to speculate the command (not speculable or
    /// over budget): the chain stops there until it executes.
    pub fn decline_speculation(&mut self, command: CommandId) {
        self.speculation.decline(command);
    }

    /// Bound on unexecuted proposals speculated ahead (zero disables).
    pub const fn set_speculation_bound(&mut self, bound: usize) {
        self.speculation.set_bound(bound);
    }

    /// The tentative outcome of a command, if computed and not executed.
    pub fn tentative(&self, command: &CommandId) -> Option<&TentativeOutcome> {
        self.speculation.outcome(command)
    }

    /// Release tentative results whose command and whole prefix are
    /// learned (the release gate).
    fn release_ready(&mut self) -> Vec<Effect> {
        if !self.is_leading() {
            return Vec::new();
        }
        #[cfg(debug_assertions)]
        self.check_release_from();
        let table = &self.table;
        let learner = &self.learner;
        let committed = |c: &CommandId| table.phase_of(c) >= Some(Phase::Commit);
        let fast = |c: &CommandId| learner.learned_fast(c);
        let gate = ReleaseGate {
            epoch: self.config.identity.epoch,
            ballot: self.config.quorum.ballot(),
            committed: &committed,
            fast: &fast,
        };
        // The unexecuted proposals from the frontier on, in order, read
        // only as far as the gate walks (task-d111's cut (e)). The last
        // one it took is where the walk stopped, or the last it passed.
        let proposals = &self.proposals;
        let last = core::cell::Cell::new(None);
        let walk = self
            .unsettled
            .range((self.release_from, CommandId(Digest32([0; 32])))..)
            .filter(|(_, c)| !proposals[c].executed && table.phase_of(c) < Some(Phase::Executed))
            .inspect(|entry| {
                last.set(Some(**entry));
                #[cfg(test)]
                self.release_walked.set(self.release_walked.get() + 1);
            })
            .map(|(_, c)| *c);
        let released = gate.release(&mut self.speculation, walk);
        // Every unexecuted proposal before the last one taken was
        // released; the last one too, if the walk went past it.
        if let Some((seqnum, c)) = last.get() {
            self.release_from = if self.speculation.released(&c) {
                seqnum.saturating_add(1)
            } else {
                seqnum
            };
        }
        match released {
            // A marked command's collector holds a speculative release and
            // answers only on the final one (task-d100, step A), which
            // goes out whether or not this one did: it is not sent
            // (#204 review).
            Ok(released) => released
                .into_iter()
                .filter(|r| {
                    !(r.speculative()
                        && self.answer_on_release.contains(&r.established().command()))
                })
                .map(|r| Effect::Released(self.with_admission(r)))
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    /// Mark `command` as answered by its collector on the release alone
    /// (task-d100, step A). The voter calls this before stepping each
    /// submission of a marked command, so the mark is in place for the
    /// proposal that submission leads to.
    pub fn mark_answer_on_release(&mut self, command: CommandId) {
        self.answer_on_release.insert(command);
    }

    /// Mark the command the next admitted request becomes as answered on
    /// the release alone, once this replica derives it (task-d100, step
    /// A). The voter calls this before stepping a marked submission, so
    /// the mark is in place for the proposal or acknowledgement that
    /// step leads to, and the command is derived only here (#204
    /// review). A request refused before its command is held marks
    /// nothing.
    pub fn mark_next_admitted(&mut self) {
        self.mark_next_admitted = true;
    }

    /// Mark the next admitted request as relayed to this voter alone as
    /// its ballot's leader (task-d102, task-d100's step B): marked as
    /// answered on the release too, proposed with its payload, and
    /// refused with [`SubmissionRefusal::NotLeading`] if this voter
    /// isn't leading.
    pub fn mark_next_relayed(&mut self) {
        self.mark_next_admitted = true;
        self.mark_next_relayed = true;
    }

    /// Whether `command`'s proposals carry its payload (task-d102).
    pub fn relays(&self, command: &CommandId) -> bool {
        self.relayed.contains(command)
    }

    /// The proposal message for `proposal`: with the command's payload
    /// when its submission was relayed to this leader alone (task-d102),
    /// so a follower that never got the submission adopts it without
    /// asking for the payload.
    fn proposal_message(&self, proposal: FastAck) -> ProtocolMessage {
        let payload = self
            .relayed
            .contains(&proposal.command)
            .then(|| self.payloads.get(&proposal.command))
            .flatten();
        match payload {
            Some(payload) => ProtocolMessage::RelayedProposal {
                vote: proposal,
                payload: payload.clone(),
            },
            None => ProtocolMessage::Proposal(proposal),
        }
    }

    /// Whether a turn's fresh proposals wait for its end, to go to each
    /// follower in one frame (task-d102). The node sets it while
    /// `Feature::BatchedRound` is active and it lowers persists in
    /// groups; turned off, whatever is queued leaves at the next
    /// [`Leader::cut_frames`].
    pub fn set_batch_frames(&mut self, on: bool) {
        self.batch_frames = on;
    }

    /// Whether proposals wait for [`Leader::cut_frames`].
    pub fn frames_queued(&self) -> bool {
        !self.frames.is_empty()
    }

    /// What this leader sent of its fresh proposals and its frontier.
    pub fn sent_bytes(&self) -> SentBytes {
        self.sent_bytes
    }

    /// Send each follower the proposals this turn queued for it
    /// (task-d102): one proposal as the message it is, more in one
    /// [`ProtocolMessage::ProposalFrame`], split where a frame would pass
    /// [`MAX_PROPOSAL_FRAME_BYTES`]. Each waits on every batch its
    /// proposals wait on, which in a turn's group are durable together.
    /// The node calls it where its loop lowers the turn's persists.
    pub fn cut_frames(&mut self) -> Vec<Effect> {
        if self.frames.is_empty() {
            return Vec::new();
        }
        for (voter, queued) in core::mem::take(&mut self.frames) {
            self.publish_frame(voter, queued);
        }
        self.release()
    }

    fn queue_frame(
        &mut self,
        voter: ReplicaId,
        context: coord_core::effect::EffectContext,
        ballot: Ballot,
        requires: Vec<BarrierId>,
        entry: FramedProposal,
    ) {
        // A frame is of one ballot and one context: a change between two
        // proposals sends what was queued under the first.
        if let Some(queued) = self.frames.get(&voter)
            && (queued.context != context || queued.ballot != ballot)
        {
            let queued = self.frames.remove(&voter).expect("present");
            self.publish_frame(voter, queued);
        }
        let queued = self.frames.entry(voter).or_insert_with(|| QueuedFrame {
            context,
            ballot,
            requires: Vec::new(),
            proposals: Vec::new(),
        });
        for barrier in requires {
            if !queued.requires.contains(&barrier) {
                queued.requires.push(barrier);
            }
        }
        queued.proposals.push(entry);
    }

    fn publish_frame(&mut self, voter: ReplicaId, queued: QueuedFrame) {
        let QueuedFrame {
            context,
            ballot,
            requires,
            proposals,
        } = queued;
        let mut chunk: Vec<FramedProposal> = Vec::new();
        let mut size = 0usize;
        for entry in proposals {
            let len = postcard::experimental::serialized_size(&entry).unwrap_or(usize::MAX);
            if !chunk.is_empty() && size.saturating_add(len) > MAX_PROPOSAL_FRAME_BYTES {
                self.send_frame(
                    voter,
                    context,
                    ballot,
                    &requires,
                    core::mem::take(&mut chunk),
                );
                size = 0;
            }
            size = size.saturating_add(len);
            chunk.push(entry);
        }
        if !chunk.is_empty() {
            self.send_frame(voter, context, ballot, &requires, chunk);
        }
    }

    fn send_frame(
        &mut self,
        voter: ReplicaId,
        context: coord_core::effect::EffectContext,
        ballot: Ballot,
        requires: &[BarrierId],
        mut chunk: Vec<FramedProposal>,
    ) {
        let count = chunk.len() as u64;
        let message = if chunk.len() == 1 {
            let entry = chunk.pop().expect("one");
            let (vote, payload) = entry.into_vote(self.config.identity.replica, ballot);
            match payload {
                Some(payload) => ProtocolMessage::RelayedProposal { vote, payload },
                None => ProtocolMessage::Proposal(vote),
            }
        } else {
            self.sent_bytes.frames += 1;
            self.sent_bytes.framed += count;
            ProtocolMessage::ProposalFrame {
                replica: self.config.identity.replica,
                ballot,
                proposals: chunk,
            }
        };
        let payload_bytes = message_payload_bytes(&message);
        let frame = message.encode();
        self.sent_bytes.proposals += count;
        self.sent_bytes.proposal_bytes += (frame.len() - payload_bytes) as u64;
        self.sent_bytes.payload_bytes += payload_bytes as u64;
        if let Some(outbox) = self.outbox.as_mut() {
            outbox.publish(PendingSend {
                context,
                requires: requires.to_vec(),
                to: PeerId {
                    replica: voter,
                    incarnation: ReplicaIncarnation::ZERO,
                },
                frame,
            });
        }
    }

    /// Drop `command`'s mark unless this leader's table holds it: the
    /// submission that marked it was refused before the command was
    /// initialized (backpressure, a ballot this leader doesn't lead, a
    /// retired key), and nothing here will ever release it (#204
    /// review). A later submission marks it again.
    pub fn keep_mark_if_held(&mut self, command: &CommandId) {
        if self.table.record(command).is_none() {
            self.answer_on_release.remove(command);
            self.relayed.remove(command);
        }
    }

    /// Whether `command`'s collector answers it on the release alone.
    pub fn answers_on_release(&self, command: &CommandId) -> bool {
        self.answer_on_release.contains(command)
    }

    /// `released` with the decided payload's admission digest, for a
    /// command answered on the release alone; unchanged otherwise, and
    /// unchanged where this leader no longer holds the command's
    /// record, since a release that names no admission is one its
    /// collector holds rather than answers (task-d100, step A).
    fn with_admission(&self, released: ReleasedResult) -> ReleasedResult {
        let command = released.established().command();
        if !self.answer_on_release.contains(&command) {
            return released;
        }
        match self.table.record(&command).and_then(|r| r.payload) {
            Some(admission) => released.with_admission(admission),
            None => released,
        }
    }

    /// Choose the learning predicates (full, or the forced slow path for
    /// comparison runs); carried across role changes and restarts.
    pub const fn set_learning(&mut self, mode: LearningMode) {
        self.learner.set_mode(mode);
    }

    /// The learning mode in effect.
    pub const fn learning(&self) -> LearningMode {
        self.learner.mode()
    }

    /// Send each fresh proposal before its batch is durable, to the
    /// followers that cannot make a fast quorum from it (task-d98). Only
    /// for a domain that has activated `EarlyProposal`, which every
    /// configured voter's build has to support: a follower built before
    /// task-d74 hears the other followers' fast acknowledgements, and with
    /// an early proposal they make a fast quorum over a vote this leader
    /// may lose (design Section 4.3). It belongs to the node, not the
    /// role: a driver sets it again on every leader it makes.
    pub const fn set_early_proposal(&mut self, on: bool) {
        self.early = on;
    }

    /// Whether early proposals are on (see [`Leader::set_early_proposal`]).
    pub const fn early_proposal(&self) -> bool {
        self.early
    }

    /// The followers an early proposal may go to: those known to hold the
    /// activation durably, by their `FeatureHeld` since their last link
    /// and in this ballot (task-d99, design Section 13's window rule). A
    /// follower whose store does not hold it may still restart on a build
    /// that counts an early proposal toward a fast quorum. `None` reads
    /// every follower as holding it.
    pub fn set_early_holders(&mut self, holders: Option<BTreeSet<ReplicaId>>) {
        self.early_holders = holders;
    }

    /// Whether this leader's proposal may go to `voter` before its batch
    /// is durable (design Sections 4.3 and 4.8).
    ///
    /// Only to a follower that cannot make a fast quorum from it: the
    /// proposal is the leader's fast vote, and a follower counts it with
    /// its own fast acknowledgement, if it is in the fast set, and nothing
    /// else, since acknowledgements reach only the leader and the frontend
    /// (task-d74). Where those two make a fast quorum -- the fast-set
    /// follower at three voters under C2 -- an early proposal would let
    /// that follower commit over a vote this leader may yet lose, so it
    /// waits for the batch, as every copy did before. That is every
    /// follower at five voters under C2, and every one under C1.
    ///
    /// And only once a proposal batch of this leadership is durable (see
    /// `early_ready`).
    fn sends_early_to(&self, voter: &ReplicaId) -> bool {
        if !self.early || !self.early_ready || *voter == self.config.identity.replica {
            return false;
        }
        if self
            .early_holders
            .as_ref()
            .is_some_and(|holders| !holders.contains(voter))
        {
            return false;
        }
        let quorum = &self.config.quorum;
        1 + usize::from(quorum.fast_eligible(voter)) < quorum.fast_size()
    }

    /// Each other voter a proposal on `barrier` goes to, with what its
    /// send waits on: nothing when it may leave early, the batch
    /// otherwise.
    fn proposal_sends(&mut self, barrier: BarrierId) -> Vec<(ReplicaId, Vec<BarrierId>)> {
        let me = self.config.identity.replica;
        let voters: Vec<ReplicaId> = self
            .config
            .identity
            .voters
            .iter()
            .copied()
            .filter(|v| *v != me)
            .collect();
        voters
            .into_iter()
            .map(|voter| {
                if self.sends_early_to(&voter) {
                    self.counts.early += 1;
                    (voter, Vec::new())
                } else {
                    (voter, alloc::vec![barrier])
                }
            })
            .collect()
    }

    /// Whether this replica leads the promised ballot: it votes in this
    /// epoch (a non-voting role never proposes), its identity agrees with
    /// the ballot configuration, the promised ballot is the configured
    /// one and no promise is in flight, it has not been fenced, and its
    /// configuration is not sealed. The seal is read from the promise
    /// state on every call rather than copied into `fenced`, so a leader
    /// recovered from a durable seal row is fenced from its first step
    /// and a leader whose seal row failed is not (task-55).
    pub fn is_leading(&self) -> bool {
        let identity = &self.config.identity;
        let quorum = &self.config.quorum;
        self.fenced.is_none()
            && self.recovery_cycle.is_none()
            && !self.ballots.is_fenced()
            && identity.role == ReplicaRole::Voter
            && identity.epoch == quorum.epoch()
            && identity.voters == *quorum.voters()
            && quorum.is_voter(&identity.replica)
            && self.ballots.promised() == quorum.ballot()
            && quorum.ballot().leader == identity.replica
            && self.ballots.in_flight().is_none()
    }

    /// The entries of this leader's selection that no order keeps, if
    /// the selection held a dependency cycle (task-d21).
    pub fn recovery_cycle(&self) -> Option<&[CommandId]> {
        self.recovery_cycle.as_deref()
    }

    /// Why the leader stopped leading, if it did.
    pub const fn fenced(&self) -> Option<FenceReason> {
        self.fenced
    }

    fn fence(&mut self, reason: FenceReason) {
        if self.fenced.is_none() {
            self.fenced = Some(reason);
            self.rejections.push(Rejection::Fenced(reason));
        }
    }

    /// The command table (phases, dependencies, paths).
    pub const fn table(&self) -> &CommandTable {
        &self.table
    }

    /// The promise state.
    pub const fn ballots(&self) -> &BallotState {
        &self.ballots
    }

    /// The proposal for a command, if any.
    pub fn proposal(&self, command: &CommandId) -> Option<&Proposal> {
        self.proposals.get(command)
    }

    /// Collected acknowledgements for a command.
    pub fn votes(&self, command: &CommandId) -> Option<&VoteSet> {
        self.votes.get(command)
    }

    /// Take the rejections recorded since the last call.
    pub fn take_rejections(&mut self) -> Vec<Rejection> {
        core::mem::take(&mut self.rejections)
    }

    /// Sends still waiting for durability.
    pub fn pending_sends(&self) -> usize {
        self.outbox.as_ref().map_or(0, |o| o.held())
    }

    fn outstanding(&self) -> Vec<BarrierId> {
        let mut out: Vec<BarrierId> = self
            .proposals
            .values()
            .filter(|p| !p.durable)
            .map(|p| p.barrier)
            .collect();
        out.sort();
        out
    }

    /// Every batch this leader submitted and has not seen resolve: the
    /// proposal batches, including one re-presented under a new barrier,
    /// and the acceptance batches `advance_pending` writes separately.
    /// A proposal is marked durable in the same turn its acceptance
    /// batch is submitted, so a cut over proposals alone would release a
    /// report while that ACCEPT row is still volatile.
    fn cut(&self) -> Vec<BarrierId> {
        let mut out = self.outstanding();
        out.extend(self.ledger.outstanding());
        out.sort();
        out.dedup();
        out
    }

    fn on_admitted(&mut self, frame: &[u8], admission: AdmissionFacts) -> Vec<Effect> {
        self.admitted = None;
        let effects = self.propose(frame, Some(admission));
        self.mark_next_relayed = false;
        if core::mem::take(&mut self.mark_next_admitted)
            && let Some(command) = self.admitted
        {
            self.keep_mark_if_held(&command);
        }
        effects
    }

    /// Whether an admitted request reaches the derivation of its command
    /// here: the checks [`Leader::propose`] makes before it decodes.
    pub fn takes_admission(&self) -> bool {
        self.boot.is_some() && self.ballots.seal_held().is_none() && self.is_leading()
    }

    /// The command the last admitted request became, if it was derived.
    pub const fn admitted(&self) -> Option<CommandId> {
        self.admitted
    }

    /// Propose one of the service's own commands.
    ///
    /// This is the path the scheduler's expiry candidates take, and it
    /// exists because there is no verifier for them: a lease expiring
    /// is not somebody's request, so there is no credential to check,
    /// no session to name and no receipt to mint. The command is
    /// accepted with no admission at all, which
    /// [`PayloadRecordV1::admission`] has always had a case for, and
    /// that absence is exactly what execution keys on -- every
    /// submission a collector makes carries a receipt, so a caller can
    /// never reach this planner however it spells its payload.
    ///
    /// Narrow on purpose. Only the two service operations may travel
    /// this way; anything else is refused here rather than proposed, so
    /// this is one door for two named actions and not an internal
    /// command endpoint.
    ///
    /// Safety of the thing itself is not this door's to provide. Every
    /// field of an expiry is a condition the state machine rechecks
    /// against replicated state, so a stale candidate -- a renewed
    /// lease, a rebound key, a superseded authority epoch -- executes
    /// as a no-op at its own position rather than deleting anything.
    pub fn propose_service(&mut self, frame: &[u8]) -> Vec<Effect> {
        let permitted = match decode_stream(frame).as_deref() {
            Ok([MessageV1::Request(r)]) => r.logical().is_ok_and(|l| {
                matches!(
                    l.operation,
                    coord_types::logical_v1::CanonicalOperation::EstablishLeaseAuthority { .. }
                        | coord_types::logical_v1::CanonicalOperation::ExpireLease { .. }
                )
            }),
            _ => false,
        };
        if !permitted {
            self.rejections.push(Rejection::MalformedRequest);
            return Vec::new();
        }
        if let Some(effects) = self.republish_service(frame) {
            return effects;
        }
        self.propose(frame, None)
    }

    /// Publish the proposal of a service command presented again while
    /// it is still unlearned, and say so; `None` for anything else, which
    /// [`Leader::propose`] then handles as a first presentation or a
    /// duplicate.
    ///
    /// A service command has no caller and no collector to retry it:
    /// its scheduler presenting the same frame again is the only thing
    /// that can ever get it to a quorum after its first proposal was
    /// lost on the way -- a failed send, a peer that was not connected
    /// yet. Refusing that presentation as a duplicate, as a client's
    /// repeat is refused, would leave the command bound here and heard
    /// nowhere else, for as long as this leader leads. So the sends are
    /// published again: the same proposal, at the same ballot, sequence
    /// number and dependencies, under the barrier that already carries
    /// its rows, which are not written a second time. A voter that did
    /// receive the first copy acknowledges the same thing again. A
    /// command already learned needs no proposal and is left to the
    /// duplicate path.
    fn republish_service(&mut self, frame: &[u8]) -> Option<Vec<Effect>> {
        let boot = self.boot?;
        if !self.is_leading() {
            return None;
        }
        let request = match decode_stream(frame).ok()?.as_slice() {
            [MessageV1::Request(r)] => r.clone(),
            _ => return None,
        };
        let (_, command) = request.command().ok()?;
        if self.bindings.get(&request.retry_key) != Some(&command) {
            return None;
        }
        let proposal = self.proposals.get(&command)?;
        if proposal.executed || self.table.phase_of(&command) >= Some(Phase::Commit) {
            return None;
        }
        let admission = self
            .table
            .record(&command)
            .and_then(|r| r.payload)
            .unwrap_or_else(|| admission_digest(None, 0));
        let ballot = self.config.quorum.ballot();
        let ack = FastAck {
            replica: self.config.identity.replica,
            ballot,
            command,
            deps: proposal.deps.clone(),
            paths: proposal.paths.clone(),
            path: proposal.path,
            admission,
            seqnum: Some(proposal.seqnum),
        };
        let barrier = proposal.barrier;
        let context = self.ballots.context(boot, ballot, LocalJournalSeq::ZERO);
        // Encoded once for every voter it goes to (task-d61).
        let frame = self.proposal_message(ack.clone()).encode();
        let outbox = self.outbox.as_mut()?;
        for voter in &self.config.identity.voters {
            if *voter == self.config.identity.replica {
                continue;
            }
            outbox.publish(PendingSend {
                context,
                requires: alloc::vec![barrier],
                to: PeerId {
                    replica: *voter,
                    incarnation: ReplicaIncarnation::ZERO,
                },
                frame: frame.clone(),
            });
        }
        self.rejections
            .push(Rejection::ProposalRepublished(command));
        Some(self.release())
    }

    fn propose(&mut self, frame: &[u8], admission: Option<AdmissionFacts>) -> Vec<Effect> {
        let _timed = crate::scratch::time(crate::scratch::Region::LPropose);
        let Some(boot) = self.boot else {
            return Vec::new();
        };
        if let Some(transition) = self.ballots.seal_held() {
            self.rejections.push(Rejection::Sealed { transition });
            return Vec::new();
        }
        if !self.is_leading() {
            self.rejections.push(Rejection::NotLeading {
                promised: self.ballots.promised(),
            });
            // A submission relayed to this voter alone is answered: its
            // collector re-routes at once rather than at its fallback
            // (task-d102). The command is not held, so it is not marked.
            if self.mark_next_relayed
                && let Some(command) = relayed_command(frame)
            {
                return self.refuse(command, SubmissionRefusal::NotLeading);
            }
            return Vec::new();
        }
        let request = match decode_stream(frame).as_deref() {
            Ok([MessageV1::Request(r)]) => r.clone(),
            _ => {
                self.rejections.push(Rejection::MalformedRequest);
                return Vec::new();
            }
        };
        let Ok(logical) = request.logical() else {
            self.rejections.push(Rejection::MalformedRequest);
            return Vec::new();
        };
        let Ok(command) = CommandId::derive(&request.retry_key, &logical) else {
            self.rejections.push(Rejection::MalformedRequest);
            return Vec::new();
        };
        self.admitted = Some(command);
        if self.mark_next_admitted {
            self.answer_on_release.insert(command);
        }
        if self.mark_next_relayed {
            self.relayed.insert(command);
        }
        let payload = PayloadRecordV1 {
            retry_key: request.retry_key,
            logical: request.logical.as_slice().to_vec(),
            admission,
            ack_through: request.ack_through,
        };
        // Identity: one retry key binds one payload, first presentation
        // wins. The same identity again is a duplicate only if the facts
        // beside it -- the attested admission and the acknowledged floor
        // -- are the ones this leader proposed it under; and a duplicate
        // is answered rather than ignored, because the submitter
        // presenting it may never have received this leader's reply.
        match self.bindings.get(&request.retry_key) {
            Some(bound) if *bound != command => {
                let bound = *bound;
                self.rejections.push(Rejection::RequestIdentityConflict {
                    retry_key: request.retry_key,
                    bound,
                });
                // Said only once the binding is durable (task-d22).
                if self.served_payloads.contains(&bound) {
                    return self.refuse(command, SubmissionRefusal::OtherCommand { bound });
                }
                return Vec::new();
            }
            Some(_) => {
                let accepted = self
                    .payloads
                    .get(&command)
                    .map(PayloadRecordV1::admission_digest);
                return match accepted {
                    Some(accepted) if accepted == payload.admission_digest() => {
                        self.rejections.push(Rejection::Duplicate(command));
                        self.repair_evidence(command, request.retry_key)
                    }
                    Some(accepted) => {
                        self.rejections
                            .push(Rejection::RequestFactsConflict { command, accepted });
                        self.refuse(command, SubmissionRefusal::OtherFacts { accepted })
                    }
                    None => {
                        self.rejections.push(Rejection::Forgotten(command));
                        self.refuse(command, SubmissionRefusal::Forgotten)
                    }
                };
            }
            None => {}
        }
        // Executed and retired: forgotten, its binding with it (task-d26),
        // or inside the window with its key left unbound by a restart that
        // found two executed presentations under it (task-d33). The same
        // answer as a bound command whose payload went to history; the
        // table holds no record, so initializing would take it as new.
        if self.table.retired(&command) {
            self.rejections.push(Rejection::Forgotten(command));
            return self.refuse(command, SubmissionRefusal::Forgotten);
        }
        // Atomic initialization: admission binding, conservative
        // dependencies, path evidence and index publication in one
        // transition.
        let init = match self.table.initialize(
            command,
            payload.admission_digest(),
            alloc::vec![CONSERVATIVE_KEY.to_vec()],
        ) {
            Ok(i) => i,
            Err(InitError::Backpressure) => {
                self.rejections.push(Rejection::Backpressure);
                return Vec::new();
            }
            // The table compared the digest: `AlreadyInitialized` is an
            // exact duplicate whose binding was not kept, and
            // `PayloadConflict` is not a duplicate at all.
            Err(InitError::AlreadyInitialized) => {
                self.bindings.insert(request.retry_key, command);
                if !self.payloads.contains_key(&command) {
                    self.ledger.bound_bare(command, request.retry_key);
                }
                self.rejections.push(Rejection::Duplicate(command));
                return self.repair_evidence(command, request.retry_key);
            }
            Err(InitError::PayloadConflict) => {
                self.rejections.push(Rejection::PayloadConflict(command));
                return match self.table.record(&command).and_then(|r| r.payload) {
                    Some(accepted) => {
                        self.refuse(command, SubmissionRefusal::OtherFacts { accepted })
                    }
                    None => Vec::new(),
                };
            }
        };
        self.bindings.insert(request.retry_key, command);
        self.payloads.insert(command, payload.clone());
        let ballot = self.config.quorum.ballot();
        let epoch = self.config.identity.epoch;
        let seqnum = self.seqnum;
        self.seqnum += 1;
        // Ordered as it is proposed: the leader's own logs take it as their
        // prefix, so their suffix stays empty (task-d68).
        self.table.synchronize_own(command, ballot, seqnum);
        // The durable row records the phase the command is actually in.
        // When the dependency guard already passes -- every prerequisite
        // is at least ACCEPT here, as at low load, where each one was
        // accepted long ago -- the leader accepts its own order now, and
        // the proposal batch carries the ACCEPT row, as a re-proposal's
        // does (task-d89, design Section 4.7). Its prerequisites' ACCEPT
        // rows were submitted before this batch, and a domain's journal
        // seals each record against the one before it, so this row is
        // never durable before theirs: it claims ACCEPT over no
        // prerequisite recovery could find below it, exactly as the row
        // `advance_pending` writes later would. Otherwise the row
        // is PRE-ACCEPT, and the acceptance is written once the guard
        // passes (see `advance_pending`).
        let accepts = self.table.accept(command, init.deps.clone()).is_ok();
        let record = self
            .table
            .record(&command)
            .expect("just initialized")
            .clone();
        let barrier = self.alloc.as_mut().expect("booted").allocate();
        let updates: Vec<StoreUpdate> = alloc::vec![
            payload_update(&command, &payload).expect("bounded"),
            dependency_update(epoch, &command, &record).expect("bounded"),
            proposal_update(
                epoch,
                &command,
                &ProposalRecordV1 {
                    ballot,
                    seqnum,
                    deps: init.deps.clone(),
                    path: init.path,
                },
            )
            .expect("bounded"),
        ];
        let persist = Effect::Persist(PersistBatch {
            barrier,
            base: None,
            updates: updates.clone(),
        });
        self.ledger.stage(barrier, command, record);
        let proposal = FastAck {
            replica: self.config.identity.replica,
            ballot,
            command,
            deps: init.deps.clone(),
            paths: init.paths.clone(),
            path: init.path,
            admission: init.payload,
            seqnum: Some(seqnum),
        };
        let context = self.ballots.context(boot, ballot, LocalJournalSeq::ZERO);
        // To a follower that cannot make a fast quorum from it, the
        // proposal leaves now, and its sync runs beside this leader's
        // (task-d98); every other copy waits for the batch. The leader
        // reply below always does: it is this leader's fast vote.
        let sends = self.proposal_sends(barrier);
        let early = sends.iter().any(|(_, requires)| requires.is_empty());
        let message = self.proposal_message(proposal.clone());
        if self.batch_frames {
            // Each follower's copy waits for the turn's end, to leave in
            // one frame with the turn's other proposals (task-d102).
            let payload = match message {
                ProtocolMessage::RelayedProposal { payload, .. } => Some(payload),
                _ => None,
            };
            let entry = FramedProposal::of(proposal.clone(), payload).expect("a proposal");
            for (voter, requires) in sends {
                self.queue_frame(voter, context, ballot, requires, entry.clone());
            }
        } else {
            // Encoded once for every voter it goes to (task-d61).
            let payload_bytes = message_payload_bytes(&message);
            let frame = message.encode();
            let outbox = self.outbox.as_mut().expect("booted");
            for (voter, requires) in sends {
                self.sent_bytes.proposals += 1;
                self.sent_bytes.proposal_bytes += (frame.len() - payload_bytes) as u64;
                self.sent_bytes.payload_bytes += payload_bytes as u64;
                outbox.publish(PendingSend {
                    context,
                    requires,
                    to: PeerId {
                        replica: voter,
                        incarnation: ReplicaIncarnation::ZERO,
                    },
                    frame: frame.clone(),
                });
            }
        }
        let outbox = self.outbox.as_mut().expect("booted");
        // A command its collector answers on the release alone gets no
        // reply: nothing at the collector counts it (task-d100, step A).
        if !self.answer_on_release.contains(&command) {
            let reply = ProtocolMessage::LeaderReply {
                ballot,
                command,
                seqnum,
                deps: init.deps.clone(),
                path: init.path,
            }
            .encode();
            outbox.publish(PendingSend {
                context,
                requires: alloc::vec![barrier],
                to: self.config.frontend,
                frame: reply.clone(),
            });
            // The frontend's copy is the one a submission can ask for
            // again (task-c02).
            self.replay.retain(
                command,
                crate::replay::RetainedEvidence {
                    barrier,
                    ballot,
                    context,
                    requires: alloc::vec![barrier],
                    frame: reply,
                },
                &self.table,
            );
        }
        // The leader's own acknowledgement counts toward learning later.
        let mut set = VoteSet::new(self.config.quorum.clone(), command);
        set.add(Vote::Fast(proposal)).expect("leader proposal");
        self.votes.insert(command, set);
        self.adopt_early_votes(command);
        self.put_proposal(Proposal {
            command,
            barrier,
            seqnum,
            deps: init.deps,
            path: init.path,
            paths: init.paths,
            admission: init.payload,
            durable: false,
            updates,
            attempts: 1,
            accepts,
            executed: false,
            published: true,
            sent: None,
        });
        // The row is ACCEPT at this ballot: once it is durable, it is this
        // leader's adoption of its own order.
        if accepts {
            self.adopt_own(command, barrier);
        }
        let mut effects = alloc::vec![persist];
        // An early copy waits on nothing, so it goes out with the batch's
        // submission rather than at the next event, which would most
        // often be the batch's own durability.
        if early {
            effects.extend(self.release());
        }
        effects
    }

    fn on_storage(&mut self, event: &StorageEvent) -> Vec<Effect> {
        let _timed = crate::scratch::time(crate::scratch::Region::LSettle);
        if let Some(outbox) = self.outbox.as_mut() {
            outbox.observe(event);
        }
        // A held-back re-proposal became durable: a voter with room in its
        // window takes it now (`top_up`), rather than at its next
        // adoption, which may never come if it has adopted everything
        // sent to it.
        let mut held_back_durable = false;
        self.ballots.on_storage(event);
        // Ledger bookkeeping covers every batch this machine staged, not
        // only the proposal batches: an acceptance row carries its own
        // barrier and is what a recovery report must show.
        // This leader's acceptance row is durable: it counts as its own
        // adoption, as a follower's does once its row is (task-d19). A
        // failed batch adopted nothing.
        if let Some(barrier) = event.barrier()
            && let Some(own) = self.own_adoptions.remove(&barrier)
            && matches!(event, StorageEvent::JournalDurable { .. })
            && let Some(set) = self.votes.get_mut(&own.command)
            && let Err(e) = set.add(own.vote)
        {
            self.rejections.push(Rejection::Vote(e));
        }
        if let Some(barrier) = event.barrier() {
            match event {
                StorageEvent::JournalDurable { journal_seq, .. } => {
                    self.ledger.durable(barrier, *journal_seq);
                }
                StorageEvent::Failed { .. } => {
                    self.ledger.failed(barrier);
                    // An adoption that waited on the failed batch as an
                    // earlier one was dropped with it; its own batch may
                    // still be written, so it is published again over what
                    // is in flight now.
                    let dropped: Vec<(BarrierId, CommandId)> = self
                        .own_adoptions
                        .iter()
                        .filter(|(_, own)| own.requires.contains(&barrier))
                        .map(|(b, own)| (*b, own.command))
                        .collect();
                    for (own_barrier, command) in dropped {
                        self.adopt_own(command, own_barrier);
                    }
                }
                _ => {}
            }
        }
        if let Some(barrier) = event.barrier()
            && let Some(command) = self.by_barrier.get(&barrier).copied()
        {
            match event {
                StorageEvent::JournalDurable { .. } => {
                    let calls = self.resend_calls;
                    if let Some(p) = self.proposals.get_mut(&command) {
                        p.durable = true;
                        // Its payload row is durable here now, if the
                        // batch wrote it (task-d90).
                        if writes_payload(&p.updates) {
                            self.served_payloads.insert(command);
                        }
                        // A restart finds this batch from now on, so it
                        // comes back following (task-d98).
                        self.early_ready = true;
                        // Its first send was released with the batch.
                        if p.published && p.sent.is_none() {
                            p.sent = Some(calls);
                        }
                        let key = (p.seqnum, command);
                        self.durable.insert(key);
                        held_back_durable |= self.held_back.contains(&key);
                        // An adoption counted before the proposal was
                        // durable raises the voter's highest now.
                        if let Some(set) = self.votes.get(&command) {
                            for voter in &self.config.identity.voters {
                                if set.adopted_by(voter) {
                                    raise(&mut self.adopted_through, *voter, key);
                                }
                            }
                        }
                    }
                    self.settle(command);
                }
                // Only a definite rejection proves the rows are absent; the
                // same batch is then presented again unchanged, keeping the
                // command, its identity binding and its dependents intact.
                // Any other failure may have written some or all of them, so
                // the leader stops leading instead of continuing over state
                // a later recovery cut could miss.
                StorageEvent::Failed {
                    error: StorageError::DefinitelyNotCommitted,
                    ..
                } => {
                    let mut effects = self.retry_proposal(command);
                    effects.extend(self.advance_pending());
                    effects.extend(self.release());
                    return effects;
                }
                StorageEvent::Failed { error, .. } => {
                    self.fence(FenceReason::ProposalUnresolved {
                        command,
                        error: *error,
                    });
                }
                _ => {}
            }
        }
        let mut effects = self.advance_pending();
        if held_back_durable {
            let me = self.config.identity.replica;
            let voters: Vec<ReplicaId> = self
                .config
                .identity
                .voters
                .iter()
                .copied()
                .filter(|v| *v != me)
                .collect();
            for voter in voters {
                effects.extend(self.top_up(voter));
            }
        }
        self.learn();
        effects.extend(self.deliver_due_report());
        effects.extend(self.release());
        effects
    }

    /// Send `voter` the next held-back re-proposals it has not had, oldest
    /// first, until [`RESEND_PER_VOTER`] of them are out to it unadopted
    /// (the takeover gap on #98).
    ///
    /// A new leader publishes its first `REPROPOSE_BATCH` re-proposals with
    /// their batches and holds the rest back, so its first pass does not
    /// put a long selection on every voter's control lane at once
    /// (task-d07). Sent from `resend_unvoted` alone, the rest went out 16
    /// a voter a call, a call every 250 ms: a backlog of N took about
    /// (N - 32) / 64 seconds after any leader change. Called as each of
    /// the voter's adoptions arrives, and as a held-back proposal becomes
    /// durable, this keeps the same bound on what is in flight to a voter
    /// and sends at the pace of its answers. `resend_unvoted` still sends
    /// what a lost frame or a voter's silence leaves unanswered.
    fn top_up(&mut self, voter: ReplicaId) -> Vec<Effect> {
        if self.held_back.is_empty() || !self.is_leading() {
            return Vec::new();
        }
        let Some(boot) = self.boot else {
            return Vec::new();
        };
        let mut window = self.first_sends.remove(&voter).unwrap_or_default();
        {
            let (proposals, votes) = (&self.proposals, &self.votes);
            window.out.retain(|c| {
                proposals.contains_key(c) && !votes.get(c).is_some_and(|v| v.adopted_by(&voter))
            });
        }
        let mut room = RESEND_PER_VOTER.saturating_sub(window.out.len());
        let mut chosen = Vec::new();
        let start = match window.through {
            Some(t) => Bound::Excluded((t, CommandId(Digest32([0xff; 32])))),
            None => Bound::Unbounded,
        };
        for (seqnum, command) in self.held_back.range((start, Bound::Unbounded)) {
            // Every path that drops a proposal drops it from `held_back`
            // too; one missing here is passed over, not a panic.
            let Some(p) = self.proposals.get(command) else {
                continue;
            };
            if room == 0 || !p.durable {
                break;
            }
            window.through = Some(*seqnum);
            if self.adopted(command, &voter) || window.out.contains(command) {
                continue;
            }
            chosen.push(*command);
            room -= 1;
        }
        let ballot = self.config.quorum.ballot();
        let context = self.ballots.context(boot, ballot, LocalJournalSeq::ZERO);
        let to = PeerId {
            replica: voter,
            incarnation: ReplicaIncarnation::ZERO,
        };
        let call = self.resend_calls;
        let mut sends = Vec::new();
        for command in chosen {
            window.out.insert(command);
            self.counts.topped_up += 1;
            let p = self.proposals.get_mut(&command).expect("held back");
            p.sent.get_or_insert(call);
            let p = &self.proposals[&command];
            sends.push(PendingSend {
                context,
                requires: alloc::vec![p.barrier],
                to,
                frame: self.proposal_frame(p),
            });
            sends.extend(self.own_adoption_send(command, to));
        }
        self.first_sends.insert(voter, window);
        // What every voter has had its first send of is held back no
        // longer: `resend_unvoted` re-sends it as it would any proposal.
        let me = self.config.identity.replica;
        let reached = self
            .config
            .identity
            .voters
            .iter()
            .filter(|v| **v != me)
            .map(|v| self.first_sends.get(v).and_then(|w| w.through))
            .min()
            .flatten();
        while let (Some(first), Some(reached)) = (self.held_back.first(), reached) {
            if first.0 > reached {
                break;
            }
            self.held_back.pop_first();
        }
        if sends.is_empty() {
            return Vec::new();
        }
        let outbox = self.outbox.as_mut().expect("booted");
        for send in sends {
            outbox.publish(send);
        }
        self.release()
    }

    /// Present a definitely rejected proposal batch again, unchanged, under
    /// a new barrier, and republish the sends that waited on the old one.
    /// The leader stops leading once the attempts are spent.
    fn retry_proposal(&mut self, command: CommandId) -> Vec<Effect> {
        let Some(proposal) = self.proposals.get(&command) else {
            return Vec::new();
        };
        if proposal.attempts >= MAX_PROPOSAL_ATTEMPTS {
            self.fence(FenceReason::ProposalRetriesExhausted { command });
            return Vec::new();
        }
        let (Some(boot), Some(alloc)) = (self.boot, self.alloc.as_mut()) else {
            return Vec::new();
        };
        let barrier = alloc.allocate();
        let admission = self
            .table
            .record(&command)
            .and_then(|r| r.payload)
            .unwrap_or_else(|| admission_digest(None, 0));
        let proposal = self.proposals.get_mut(&command).expect("checked above");
        self.by_barrier.remove(&proposal.barrier);
        self.by_barrier.insert(barrier, command);
        proposal.barrier = barrier;
        proposal.attempts += 1;
        // Republished below to every voter: a re-proposal held back from
        // a new leader's first batch has had its first send now, and its
        // re-sends are timed from its batch (task-d49).
        proposal.published = true;
        let republished = (proposal.seqnum, command);
        self.held_back.remove(&republished);
        let batch = PersistBatch {
            barrier,
            base: None,
            updates: proposal.updates.clone(),
        };
        let ack = FastAck {
            replica: self.config.identity.replica,
            ballot: self.config.quorum.ballot(),
            command,
            deps: proposal.deps.clone(),
            paths: proposal.paths.clone(),
            path: proposal.path,
            admission,
            seqnum: Some(proposal.seqnum),
        };
        let seqnum = proposal.seqnum;
        let deps = proposal.deps.clone();
        let path = proposal.path;
        let ballot = self.config.quorum.ballot();
        let context = self.ballots.context(boot, ballot, LocalJournalSeq::ZERO);
        // The same proposal, unchanged, under the same rule as its first
        // send (Section 4.8's third condition).
        let sends = self.proposal_sends(barrier);
        // Encoded once for every voter it goes to (task-d61).
        let frame = self.proposal_message(ack.clone()).encode();
        let outbox = self.outbox.as_mut().expect("booted");
        for (voter, requires) in sends {
            outbox.publish(PendingSend {
                context,
                requires,
                to: PeerId {
                    replica: voter,
                    incarnation: ReplicaIncarnation::ZERO,
                },
                frame: frame.clone(),
            });
        }
        if !self.answer_on_release.contains(&command) {
            let reply = ProtocolMessage::LeaderReply {
                ballot,
                command,
                seqnum,
                deps,
                path,
            }
            .encode();
            outbox.publish(PendingSend {
                context,
                requires: alloc::vec![barrier],
                to: self.config.frontend,
                frame: reply.clone(),
            });
            self.replay.retain(
                command,
                crate::replay::RetainedEvidence {
                    barrier,
                    ballot,
                    context,
                    requires: alloc::vec![barrier],
                    frame: reply,
                },
                &self.table,
            );
        }
        // A re-proposal's batch is also its acceptance: the adoption rests
        // on the new batch now, and the one on the rejected batch was
        // dropped with it.
        let accepts = self.proposals.get(&command).is_some_and(|p| p.accepts);
        self.own_adoptions.retain(|_, own| own.command != command);
        if accepts {
            self.adopt_own(command, barrier);
        }
        self.rejections.push(Rejection::ProposalRetried(command));
        alloc::vec![Effect::Persist(batch)]
    }

    /// Publish this leader's adoption of its own order for `command`, to
    /// the frontend, once `barrier` -- the batch carrying
    /// its acceptance row -- and every batch submitted before it are
    /// durable (task-d19; design Section 4.8). It is kept for re-offer
    /// beside the leader's reply, and counted in the leader's own vote set
    /// when the batch is durable.
    ///
    /// The proposal is not an acceptance: its row is PRE-ACCEPT, and a
    /// crash that loses the acceptance batch leaves a restarted leader
    /// reporting PRE-ACCEPT. Counting the leader for its proposal made a
    /// slow majority that a recovering majority could miss entirely.
    ///
    /// Nor before the payload row (task-d90): the leader's acceptance is a
    /// vote recovery may re-execute from, so it counts only where the
    /// payload row is durable here or in a proposal batch it waits on.
    fn adopt_own(&mut self, command: CommandId, barrier: BarrierId) {
        let _timed = crate::scratch::time(crate::scratch::Region::LAdoptOwn);
        if !self.served_payloads.contains(&command)
            && !self
                .proposals
                .get(&command)
                .is_some_and(|p| writes_payload(&p.updates))
        {
            self.rejections.push(Rejection::NoPayloadRow(command));
            return;
        }
        let (Some(boot), Some(admission)) = (
            self.boot,
            self.proposals
                .get(&command)
                .map(|p| p.admission)
                .or_else(|| self.table.record(&command).and_then(|r| r.payload)),
        ) else {
            return;
        };
        let ballot = self.config.quorum.ballot();
        let ack = SlowAck {
            replica: self.config.identity.replica,
            ballot,
            command,
            admission,
        };
        let context = self.ballots.context(boot, ballot, LocalJournalSeq::ZERO);
        // Batches may complete in any order: the acknowledgement waits on
        // every batch submitted before its own, not on its own alone.
        let requires: Vec<BarrierId> = if crate::scratch::bar() {
            alloc::vec![barrier]
        } else {
            self.unsettled
                .iter()
                .map(|(_, c)| &self.proposals[c])
                .filter(|p| !p.durable)
                .map(|p| p.barrier)
                .chain(self.own_adoptions.keys().copied())
                .chain(self.ledger.in_flight())
                .filter(|b| !self.outbox.as_ref().is_some_and(|o| o.is_failed(b)))
                .chain(core::iter::once(barrier))
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect()
        };
        let frame = ProtocolMessage::SlowAck(ack.clone()).encode();
        let marked = self.answer_on_release.contains(&command);
        let Some(outbox) = self.outbox.as_mut() else {
            return;
        };
        // To the frontend, which counts it, and to no other voter
        // (task-d74): a follower learns from the commit frontier. Not
        // for a command answered on the release alone (task-d100, step
        // A): its collector counts nothing.
        if !marked {
            outbox.publish(PendingSend {
                context,
                requires: requires.clone(),
                to: self.config.frontend,
                frame: frame.clone(),
            });
            self.replay.retain(
                command,
                crate::replay::RetainedEvidence {
                    barrier,
                    ballot,
                    context,
                    requires: requires.clone(),
                    frame,
                },
                &self.table,
            );
        }
        self.own_adoptions.insert(
            barrier,
            OwnAdoption {
                command,
                vote: Vote::Slow(ack),
                requires,
            },
        );
    }

    /// Adopt the leader's own order for every durable proposal whose
    /// dependencies are all at least ACCEPT; repeat while progress is made
    /// so a chain advances in one turn (bounded by the table size).
    fn advance_pending(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();
        // A leader that no longer leads (a higher promise in flight or
        // durable, or a fence) writes no new acceptance: that ballot's
        // transitions end at the recovery cut.
        if !self.is_leading() {
            return effects;
        }
        loop {
            let mut progressed = false;
            // A proposal at PRE-ACCEPT is unexecuted, so unsettled; in
            // identity order, as a pass over every proposal took them
            // (task-d46).
            let mut candidates: Vec<(CommandId, Vec<CommandId>)> = self
                .unsettled
                .iter()
                .map(|(_, c)| &self.proposals[c])
                .filter(|p| p.durable && self.table.phase_of(&p.command) == Some(Phase::PreAccept))
                .map(|p| (p.command, p.deps.clone()))
                .collect();
            candidates.sort_unstable_by_key(|(c, _)| *c);
            for (command, deps) in candidates {
                if self.table.accept(command, deps).is_ok() {
                    progressed = true;
                    // The acceptance happened: record it durably now, not
                    // when the command was merely proposed. A row claiming
                    // ACCEPT before the guard passed would let recovery
                    // restore an accepted command whose prerequisite never
                    // was accepted.
                    let epoch = self.config.identity.epoch;
                    let record = self.table.record(&command).expect("accepted").clone();
                    let Some(alloc) = self.alloc.as_mut() else {
                        continue;
                    };
                    let barrier = alloc.allocate();
                    self.ledger.stage(barrier, command, record.clone());
                    effects.push(Effect::Persist(PersistBatch {
                        barrier,
                        base: None,
                        updates: alloc::vec![
                            dependency_update(epoch, &command, &record).expect("bounded")
                        ],
                    }));
                    self.adopt_own(command, barrier);
                }
            }
            if !progressed {
                return effects;
            }
        }
    }

    fn release(&mut self) -> Vec<Effect> {
        let promised = self.ballots.promised();
        self.outbox
            .as_mut()
            .map_or_else(Vec::new, |o| o.release(&promised))
    }

    /// Tell the frontend that submitted `command` why this replica did
    /// nothing with it (task-d22): a refusal with no effect left the
    /// collector's entry pending for good. Not durable, and not needed to
    /// be: a collector that misses it solicits again.
    fn refuse(&mut self, command: CommandId, refusal: SubmissionRefusal) -> Vec<Effect> {
        let Some(boot) = self.boot else {
            return Vec::new();
        };
        let ballot = self.config.quorum.ballot();
        let context = self.ballots.context(boot, ballot, LocalJournalSeq::ZERO);
        let frame = ProtocolMessage::Refused {
            ballot,
            command,
            refusal,
        }
        .encode();
        if let Some(outbox) = self.outbox.as_mut() {
            outbox.publish(PendingSend {
                context,
                requires: Vec::new(),
                to: self.config.frontend,
                frame,
            });
        }
        self.release()
    }

    /// Offer the submitter the reply this leader already published for
    /// `command`, because a submission naming it arrived again.
    ///
    /// A leader learns of a command it never received a submission for
    /// the same way a follower does -- a peer's acknowledgement names it
    /// and the payload is transferred -- and replies to a frontend that
    /// does not yet know which collector asked. The leader's reply is
    /// the one piece of evidence every learning predicate requires, so
    /// a collector that lost it is not one voter short but unable to
    /// learn at all. The same reply is published again, to the frontend
    /// only, through the same outbox and under the same gates, and
    /// released here (task-c02). Not a proposal attempt: nothing is
    /// persisted, and the fence on repeated persistence is not touched.
    fn repair_evidence(&mut self, command: CommandId, retry_key: RetryKey) -> Vec<Effect> {
        // A command answered on the release alone (task-d100, step A):
        // what its collector may be missing is the release, which the
        // evidence store never kept. Once this leader has executed it,
        // the daemon publishes it again from its records; before then
        // the release is still coming to the origin the duplicate just
        // named, and nothing is owed.
        if self.answer_on_release.contains(&command) {
            if self.boot.is_some()
                && self.is_leading()
                && let Some(record) = self.table.record(&command)
                && record.phase >= Phase::Executed
                && let Some(admission) = record.payload
            {
                match self.replay.count_release_again(command, &self.table) {
                    Ok(()) => self.release_again.push((command, retry_key, admission)),
                    Err(why) => self
                        .rejections
                        .push(Rejection::ReplayRefused { command, why }),
                }
            }
            return Vec::new();
        }
        let why = if self.boot.is_none() || !self.is_leading() {
            crate::replay::ReplayRefusal::Fenced
        } else {
            let Some(outbox) = self.outbox.as_mut() else {
                self.rejections.push(Rejection::ReplayRefused {
                    command,
                    why: crate::replay::ReplayRefusal::Fenced,
                });
                return Vec::new();
            };
            match self.replay.plan(
                command,
                outbox,
                self.config.quorum.ballot(),
                self.config.frontend,
                &self.table,
            ) {
                Ok(sends) => {
                    for send in sends {
                        outbox.publish(send);
                    }
                    return self.release();
                }
                Err(why) => why,
            }
        };
        self.rejections
            .push(Rejection::ReplayRefused { command, why });
        Vec::new()
    }

    /// How often `command`'s reply has been published again this boot
    /// (diagnostic).
    pub fn evidence_repairs(&self, command: &CommandId) -> u32 {
        self.replay.repairs_of(command)
    }

    fn on_peer(&mut self, from: PeerId, frame: &[u8]) -> Vec<Effect> {
        let Ok(message) = ProtocolMessage::decode(frame) else {
            self.rejections.push(Rejection::MalformedPeerMessage);
            return Vec::new();
        };
        match message {
            ProtocolMessage::NewLeader { ballot, executed } => {
                let outstanding = self.cut();
                let (Some(boot), Some(alloc)) = (self.boot, self.alloc.as_mut()) else {
                    return Vec::new();
                };
                // A candidate more than a table behind this leader would
                // win and then not serve (task-d10); refused, it is told
                // how far this leader executed.
                let own = self.learner.executed_through();
                if let Some(refused) = self.ballots.refuses_behind(
                    from.replica,
                    ballot,
                    executed,
                    own,
                    self.config.capacity as u64,
                ) {
                    self.rejections.push(Rejection::Promise(refused));
                    let reply = self.ballots.refusal(from, ballot, boot, own);
                    if let Some(outbox) = self.outbox.as_mut() {
                        outbox.publish(reply);
                    }
                    return self.release();
                }
                match self
                    .ballots
                    .on_new_leader(from, ballot, boot, alloc, &outstanding)
                {
                    Ok(effects) => {
                        self.report_due = Some(ReportDue {
                            ballot,
                            to: from,
                            requires: effects.reply.requires.clone(),
                        });
                        if let Some(outbox) = self.outbox.as_mut() {
                            outbox.publish(effects.reply);
                        }
                        let mut out = alloc::vec![effects.persist];
                        out.extend(self.release());
                        out
                    }
                    Err(e) => {
                        self.rejections.push(Rejection::Promise(e));
                        Vec::new()
                    }
                }
            }
            ProtocolMessage::FastAck(ack) => self.collect(from.replica, Vote::Fast(ack)),
            ProtocolMessage::SlowAck(ack) => self.collect(from.replica, Vote::Slow(ack)),
            ProtocolMessage::PayloadRequest { commands } => {
                self.serve_payloads(from.replica, &commands)
            }
            ProtocolMessage::Sync(decision) => {
                // A deposed leader carries the Sync into its follower role.
                // So does one whose deposing promise has not arrived yet:
                // the Sync of a higher ballot is published once, and the
                // follower this leader becomes keeps it until it promises
                // that ballot (task-d05). Only a Sync from its ballot's
                // leader is kept, and only the highest: Syncs of two
                // higher ballots can arrive in either order, and the
                // lower one, kept last, would be refused once the higher
                // promise is made, with the Sync that matches it gone.
                let above = |b: &Ballot| {
                    decision.ballot.compare_same_epoch(b) == Some(core::cmp::Ordering::Greater)
                };
                let kept = self
                    .pending_sync
                    .as_ref()
                    .is_none_or(|(_, kept)| above(&kept.ballot));
                if from.replica == decision.ballot.leader
                    && kept
                    && (self.deposed() || above(&self.config.quorum.ballot()))
                {
                    self.pending_sync = Some((from.replica, decision));
                }
                Vec::new()
            }
            // Sealing the old configuration (task-55). A leader seals
            // like any other voter: leading is a role within a
            // configuration, and the fence is the configuration's.
            ProtocolMessage::SealRequest { transition } => {
                let outstanding = self.cut();
                let (Some(boot), Some(alloc)) = (self.boot, self.alloc.as_mut()) else {
                    return Vec::new();
                };
                match self
                    .ballots
                    .seal(transition, from, boot, alloc, &outstanding)
                {
                    Ok(effects) => {
                        if let Some(outbox) = self.outbox.as_mut() {
                            outbox.publish(effects.report);
                        }
                        let mut out = alloc::vec![effects.persist];
                        out.extend(self.release());
                        out
                    }
                    Err(e) => {
                        self.rejections.push(Rejection::Seal(e));
                        Vec::new()
                    }
                }
            }
            ProtocolMessage::ProposalRequest { ballot, commands } => {
                self.serve_proposals(from.replica, ballot, &commands)
            }
            ProtocolMessage::ReportPageRequest { ballot, pages } => {
                self.answer_report_pages(from.replica, ballot, &pages)
            }
            ProtocolMessage::Promise {
                ballot, replica, ..
            } => self.on_late_promise(from.replica, ballot, replica),
            ProtocolMessage::Sealed { .. }
            | ProtocolMessage::Committed { .. }
            | ProtocolMessage::Proposal(_)
            | ProtocolMessage::RelayedProposal { .. }
            | ProtocolMessage::ProposalFrame { .. }
            | ProtocolMessage::LeaderReply { .. }
            | ProtocolMessage::Refused { .. }
            | ProtocolMessage::FloorReadiness { .. }
            | ProtocolMessage::ReadConfirm { .. }
            | ProtocolMessage::ReadConfirmed { .. }
            | ProtocolMessage::FeatureSupport { .. }
            | ProtocolMessage::FeaturePrepare { .. }
            | ProtocolMessage::FeaturePrepared { .. }
            | ProtocolMessage::FeatureActivated { .. }
            | ProtocolMessage::FeatureHeld { .. }
            | ProtocolMessage::ReportPage(_)
            | ProtocolMessage::PromiseRefused { .. }
            | ProtocolMessage::CatchUpRequest { .. }
            | ProtocolMessage::CatchUpPage { .. }
            | ProtocolMessage::PayloadResponse { .. } => Vec::new(),
        }
    }

    /// Serve durable payloads to a peer; a payload whose proposal batch is
    /// not durable is not served.
    fn serve_payloads(&mut self, to: ReplicaId, commands: &[CommandId]) -> Vec<Effect> {
        let Some(boot) = self.boot else {
            return Vec::new();
        };
        // Payload transfer is not a voting transition: it is served under
        // the promised ballot so a deposed leader still supplies the
        // payloads a candidate recovers from.
        let context = self
            .ballots
            .context(boot, self.ballots.promised(), LocalJournalSeq::ZERO);
        let responses: Vec<ProtocolMessage> = commands
            .iter()
            .filter(|c| {
                self.proposals.get(*c).is_some_and(|p| p.durable)
                    || self.served_payloads.contains(c)
            })
            .filter_map(|c| {
                self.payloads
                    .get(c)
                    .map(|p| ProtocolMessage::PayloadResponse {
                        command: *c,
                        payload: p.clone(),
                    })
            })
            // A replica that fell behind asks again on a timer. If every
            // ask were answered in full, the answers alone would fill
            // the lane they share with the proposals and commits that
            // replica is waiting for, and it would fall further behind
            // for as long as it kept asking. See [`MAX_PAYLOAD_TRANSFER`].
            .take(MAX_PAYLOAD_TRANSFER)
            .collect();
        if let Some(outbox) = self.outbox.as_mut() {
            for m in responses {
                outbox.publish(PendingSend {
                    context,
                    requires: Vec::new(),
                    to: PeerId {
                        replica: to,
                        incarnation: ReplicaIncarnation::ZERO,
                    },
                    frame: m.encode(),
                });
            }
        }
        self.release()
    }

    /// Hold an acknowledgement for a command not yet proposed. Returns
    /// whether it had to be refused instead.
    fn hold_vote(&mut self, command: CommandId, vote: Vote) -> bool {
        let voters = self.config.identity.voters.len();
        if !self.early_votes.contains_key(&command) {
            // Full means the oldest goes, not that this one is refused.
            // A command this leader never proposes leaves its
            // acknowledgements here for ever otherwise, and a hold
            // full of those would quietly turn the mechanism off.
            while self.early_votes.len() >= self.config.capacity {
                match self.early_order.pop_front() {
                    Some(oldest) => {
                        self.early_votes.remove(&oldest);
                    }
                    None => return true,
                }
            }
            self.early_order.push_back(command);
        }
        let held = self.early_votes.entry(command).or_default();
        // One fast and one adoption acknowledgement per voter is all a
        // vote set will ever count, so that is all this holds.
        if held.len() >= voters.saturating_mul(2) {
            return true;
        }
        held.push(vote);
        false
    }

    /// Count the acknowledgements held for a command this leader has
    /// just proposed. Nothing is released here: the proposal's own rows
    /// are not durable yet, and learning waits for them as it always
    /// did.
    fn adopt_early_votes(&mut self, command: CommandId) {
        let Some(held) = self.early_votes.remove(&command) else {
            return;
        };
        self.early_order.retain(|c| *c != command);
        let Some(set) = self.votes.get_mut(&command) else {
            return;
        };
        let mut refused = Vec::new();
        for vote in held {
            if let Err(e) = set.add(vote) {
                refused.push(e);
            }
        }
        for e in refused {
            self.rejections.push(Rejection::Vote(e));
        }
    }

    fn collect(&mut self, from: ReplicaId, vote: Vote) -> Vec<Effect> {
        if vote.replica() != from {
            self.rejections.push(Rejection::Vote(VoteError::NotAVoter));
            return Vec::new();
        }
        // A voter votes in a ballot only once it has installed its Sync:
        // one that missed the campaign follows now (task-d33).
        if vote.ballot() == self.config.quorum.ballot() {
            self.joined.insert(from);
        }
        let command = vote.command();
        let Some(set) = self.votes.get_mut(&command) else {
            // Not "a command this leader never proposed": a command it
            // has not proposed *yet*. Every voter is sent the same
            // submission, and a voter that initializes it before this
            // leader does acknowledges it before this leader has
            // ordered it. Dropping that acknowledgement does not lose
            // the command -- the slow path still learns it -- but it
            // costs the fast path a round trip on every command whose
            // acknowledgement wins that race, which under concurrent
            // callers is most of them.
            //
            // So it is held, in the same spirit as the proposal a
            // follower holds until its payload arrives, and bounded the
            // same way: as many commands as the table has room for, and
            // per command no more acknowledgements than there are
            // voters to send them.
            if self.hold_vote(command, vote) {
                self.rejections
                    .push(Rejection::Vote(VoteError::WrongCommand));
            }
            return Vec::new();
        };
        let slow = matches!(vote, Vote::Slow(_));
        if let Err(e) = set.add(vote) {
            // Nothing was counted, so nothing can be learned or released:
            // a refusal costs the lookup that found it (task-d49).
            if e == VoteError::Duplicate {
                self.counts.duplicate_votes += 1;
                if slow && let Some((took, _)) = self.answered.remove(&(command, from)) {
                    self.counts.late += 1;
                    self.sample_latency(from, took);
                }
            }
            self.rejections.push(Rejection::Vote(e));
            return Vec::new();
        }
        if slow {
            if let Some(p) = self.proposals.get(&command)
                && p.durable
            {
                raise(&mut self.adopted_through, from, (p.seqnum, command));
            }
            if from != self.config.identity.replica {
                self.note_adoption(command, from);
                // Its adoption is the voter's answer: the next held-back
                // re-proposal goes now, not on the next timer call.
                let mut out = self.top_up(from);
                self.learn();
                out.extend(self.release_ready());
                return out;
            }
        }
        self.learn();
        self.release_ready()
    }

    /// A voter's adoption of `command` was counted: a sample of its vote
    /// latency, or the answer to a re-send (task-d49).
    ///
    /// An adoption of a proposal that was re-sent may answer either send,
    /// so it is no sample yet (Karn's rule): only a second copy, refused
    /// as a duplicate, shows that the first answered the first send.
    /// Without those, a voter slower than its interval would be re-sent
    /// to every time and its interval would never learn it is slow.
    fn note_adoption(&mut self, command: CommandId, from: ReplicaId) {
        let Some(sent) = self.proposals.get(&command).and_then(|p| p.sent) else {
            return;
        };
        let took = self.resend_calls.saturating_sub(sent);
        if self.resent.contains_key(&(command, from)) {
            self.counts.answered += 1;
            self.answered
                .insert((command, from), (took, self.resend_calls));
            return;
        }
        self.sample_latency(from, took);
    }

    /// Whether `voter` adopted `command` (task-d59).
    fn adopted(&self, command: &CommandId, voter: &ReplicaId) -> bool {
        self.votes.get(command).is_some_and(|v| v.adopted_by(voter))
    }

    /// The durable proposal with the highest sequence number `voter`
    /// adopted (task-d59). What `adopted_through` holds is never below
    /// it; once the proposal it names was swept, or its votes replaced by
    /// a new proposal of the command, the highest is looked for below.
    fn adopted_through(&mut self, voter: &ReplicaId) -> Option<(u64, CommandId)> {
        let top = *self.adopted_through.get(voter)?;
        if self.durable.contains(&top) && self.adopted(&top.1, voter) {
            return Some(top);
        }
        let found = self
            .durable
            .range(..top)
            .rev()
            .find(|(_, c)| self.adopted(c, voter))
            .copied();
        match found {
            Some(key) => self.adopted_through.insert(*voter, key),
            None => self.adopted_through.remove(voter),
        };
        found
    }

    /// What one durable proposal `voter` has not adopted is to be sent
    /// as, this call, or None if it is not sent (task-d49): `through` is
    /// the sequence number of the voter's highest adoption, `at` the call
    /// and the voter's interval, and `trickled` whether the voter's one
    /// handed-off proposal of the call was chosen.
    fn resend_kind(
        &self,
        seqnum: u64,
        command: &CommandId,
        voter: &ReplicaId,
        through: Option<u64>,
        (call, interval): (u64, u64),
        trickled: &mut bool,
    ) -> Option<Resend> {
        let decided = self
            .table
            .phase_of(command)
            .is_some_and(|phase| phase >= Phase::Commit);
        if decided && through.is_some_and(|t| seqnum <= t) {
            return None;
        }
        let resent = self.resent.get(&(*command, *voter));
        let sent = self.proposals[command].sent;
        let old = decided && sent.is_some_and(|at| call >= at.saturating_add(RESEND_HANDOFF_CALLS));
        Some(match (resent, sent) {
            (Some(r), _) if call < r.next => return None,
            _ if old && *trickled => return None,
            _ if old => {
                *trickled = true;
                Resend::HandedOff
            }
            (None, Some(sent)) if call < sent.saturating_add(interval) => return None,
            (None, None) => Resend::Deferred,
            _ if decided => Resend::Decided,
            _ if self
                .votes
                .get(command)
                .is_some_and(|v| v.voted().contains(voter)) =>
            {
                Resend::Acknowledged
            }
            _ => Resend::Unanswered,
        })
    }

    /// What `resend_unvoted` chose before task-d59, from every durable
    /// proposal sorted: checked against the indexed walk in debug builds.
    fn resend_by_sort(&self, per_voter: usize, call: u64) -> Vec<(CommandId, ReplicaId, Resend)> {
        let mut order: Vec<(u64, CommandId)> = self
            .proposals
            .values()
            .filter(|p| p.durable)
            .map(|p| (p.seqnum, p.command))
            .collect();
        order.sort();
        let mut chosen = Vec::new();
        for voter in &self.config.identity.voters {
            if *voter == self.config.identity.replica {
                continue;
            }
            let interval = self.resend_interval(voter);
            let through = order
                .iter()
                .filter(|(_, c)| self.adopted(c, voter))
                .map(|(s, _)| *s)
                .max();
            let mut taken = 0;
            let mut trickled = false;
            for (seqnum, command) in &order {
                if taken == per_voter {
                    break;
                }
                if self.adopted(command, voter) {
                    continue;
                }
                if let Some(kind) = self.resend_kind(
                    *seqnum,
                    command,
                    voter,
                    through,
                    (call, interval),
                    &mut trickled,
                ) {
                    taken += 1;
                    chosen.push((*command, *voter, kind));
                }
            }
        }
        chosen
    }

    fn sample_latency(&mut self, voter: ReplicaId, took: u64) {
        let samples = self.latency.entry(voter).or_default();
        if samples.len() == RESEND_LATENCY_SAMPLES {
            samples.pop_front();
        }
        samples.push_back(took);
    }

    /// The calls a re-send to `voter` waits after the last send
    /// (task-d49): the 99th percentile of how many its latest adoptions
    /// took, between one and [`RESEND_INTERVAL_CAP`].
    ///
    /// An adoption that arrived before the next call took none, and one
    /// that took `k` would have been re-sent by an interval below `k`. So
    /// a voter whose adoptions all arrive within a call is re-sent to on
    /// the second call after a send at the earliest, a quarter to half a
    /// second at `coordd`'s timer, which is today's floor; one that is
    /// slower is waited for as long as it usually takes.
    fn resend_interval(&self, voter: &ReplicaId) -> u64 {
        let Some(samples) = self.latency.get(voter).filter(|s| !s.is_empty()) else {
            return 1;
        };
        let mut sorted: Vec<u64> = samples.iter().copied().collect();
        sorted.sort_unstable();
        let at = (sorted.len() * 99).div_ceil(100).saturating_sub(1);
        sorted[at].clamp(1, RESEND_INTERVAL_CAP)
    }

    /// What the re-sends did since [`Leader::take_resend_counts`] was
    /// last called (task-d49).
    pub const fn resend_counts(&self) -> ResendCounts {
        self.counts
    }

    /// What the re-sends did since this was last called (task-d49).
    pub fn take_resend_counts(&mut self) -> ResendCounts {
        core::mem::take(&mut self.counts)
    }

    /// Why `command`, established on the slow path, missed the fast one,
    /// from the votes this leader counted (task-d62). None in a run
    /// forced onto the slow path, or for a command it holds no proposal
    /// of.
    fn missed_fast(&self, command: &CommandId) -> Option<MissedFast> {
        if self.learner.mode() != LearningMode::Full {
            return None;
        }
        self.votes.get(command)?.missed_fast()
    }

    /// Why `command` missed the fast path, taken once its establishment
    /// is counted (task-d62); None if it was not held.
    pub fn take_missed_fast(&mut self, command: &CommandId) -> Option<MissedFast> {
        self.missed.take(command)
    }
}

impl DeterministicMachine for Leader {
    type Event = Event;
    type Effect = Effect;

    fn step(&mut self, event: Event) -> Vec<Effect> {
        match event {
            Event::Boot {
                boot_id,
                incarnation,
            } => {
                self.boot = Some(boot_id);
                self.alloc = Some(BarrierAllocator::new(incarnation, boot_id));
                self.outbox = Some(Outbox::new(boot_id));
                Vec::new()
            }
            Event::Admitted(request) => self.on_admitted(&request.frame, request.receipt.facts()),
            // What a durable batch or a vote commits is announced in the
            // same step (task-d74), or at the end of the node's turn.
            Event::Storage(event) => {
                let mut effects = self.on_storage(&event);
                if !self.frontier_at_turn_end {
                    effects.extend(self.announce_advanced());
                }
                effects
            }
            Event::Peer(message) => {
                // The authenticated sender, at the exact incarnation the
                // transport bound; replies are addressed to it.
                let from = PeerId {
                    replica: message.provenance().from(),
                    incarnation: message.provenance().incarnation(),
                };
                let mut effects = self.on_peer(from, message.frame());
                if !self.frontier_at_turn_end {
                    effects.extend(self.announce_advanced());
                }
                effects
            }
            Event::Timer(_)
            | Event::Clock(_)
            | Event::ViewReady { .. }
            | Event::Entropy { .. }
            | Event::ConnectionClosed { .. } => Vec::new(),
        }
    }
}

/// Outstanding (not yet durable) proposal barriers, for tests and the
/// recovery cut.
impl Leader {
    /// Barriers of proposals not yet durable, in order.
    pub fn outstanding_barriers(&self) -> Vec<BarrierId> {
        self.outstanding()
    }
}

/// The command a relayed submission names, for a voter that isn't
/// leading to refuse it by (task-d102). `None` for a frame that does not
/// decode to one request.
pub(crate) fn relayed_command(frame: &[u8]) -> Option<CommandId> {
    let request = match decode_stream(frame).as_deref() {
        Ok([MessageV1::Request(r)]) => r.clone(),
        _ => return None,
    };
    let logical = request.logical().ok()?;
    CommandId::derive(&request.retry_key, &logical).ok()
}

/// Whether a proposal batch writes the command's payload row (task-d90).
fn writes_payload(updates: &[StoreUpdate]) -> bool {
    let payloads = coord_store_api::registry::Collection::PayloadV1.id();
    updates
        .iter()
        .any(|u| u.collection == payloads && u.value.is_some())
}

#[cfg(test)]
mod tests {
    //! The path a fresh proposal takes when its dependency guard fails
    //! (task-d89): a leader's own proposals are all accepted as they are
    //! proposed, so only a prerequisite this leader holds at PRE-ACCEPT
    //! from elsewhere, such as a recovered record, makes it fail. Nothing
    //! outside the crate can place one, so the test does.

    use alloc::collections::BTreeSet;
    use alloc::vec;

    use coord_types::ids::{
        ClientInstanceId, ClusterId, ConfigurationEpoch, DomainId, NamespaceId, RequestSequence,
        SessionId,
    };
    use coord_types::logical_v1::{CanonicalOperation, LogicalRequest, PutOp};
    use coord_types::wire_v1::RequestV1;

    use super::*;
    use crate::rows::decode_dependency;

    const FRONTEND: PeerId = PeerId {
        replica: ReplicaId([0xf0; 16]),
        incarnation: ReplicaIncarnation::ZERO,
    };

    fn r(i: u8) -> ReplicaId {
        ReplicaId([i; 16])
    }

    fn booted() -> Leader {
        let epoch = ConfigurationEpoch::new(1).unwrap();
        let voters: BTreeSet<ReplicaId> = (0..3).map(r).collect();
        let genesis = Ballot {
            epoch,
            number: 0,
            leader: r(0),
        };
        let config = LeaderConfig {
            identity: ConfigurationIdentity {
                cluster: ClusterId([1; 16]),
                domain: DomainId([2; 16]),
                epoch,
                voters: voters.clone(),
                replica: r(0),
                incarnation: ReplicaIncarnation::new(1).unwrap(),
                role: ReplicaRole::Voter,
            },
            quorum: BallotConfiguration::c2(epoch, genesis, voters, [r(0), r(1)].into()).unwrap(),
            genesis,
            frontend: FRONTEND,
            capacity: 8,
        };
        let mut leader = Leader::new(config, None, ExecutionPosition::ZERO);
        assert!(
            leader
                .step(Event::Boot {
                    boot_id: BootId([1; 16]),
                    incarnation: ReplicaIncarnation::new(1).unwrap(),
                })
                .is_empty()
        );
        leader
    }

    /// A put's frame and its command. The tests propose it through
    /// `propose` without admission facts: minting a receipt is the
    /// authentication boundary's (`cargo xtask check-deps`), and the
    /// guard does not read them.
    fn request(seq: u64) -> (Vec<u8>, CommandId) {
        let retry_key = RetryKey {
            cluster_id: ClusterId([1; 16]),
            domain_id: DomainId([2; 16]),
            session_id: SessionId([3; 16]),
            client_instance_id: ClientInstanceId([4; 16]),
            request_sequence: RequestSequence::new(seq).unwrap(),
        };
        let request = LogicalRequest::new(
            NamespaceId([5; 16]),
            CanonicalOperation::Put(PutOp {
                key: vec![1],
                value: vec![1],
                lease: None,
                prev_kv: false,
            }),
        );
        let command = CommandId::derive(&retry_key, &request).unwrap();
        let frame = MessageV1::Request(RequestV1::new(retry_key, &request, 0, 0).unwrap())
            .encode()
            .unwrap();
        (frame, command)
    }

    fn persisted(effects: &[Effect]) -> Vec<PersistBatch> {
        effects
            .iter()
            .filter_map(|e| match e {
                Effect::Persist(b) => Some(b.clone()),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn a_fresh_proposal_over_a_pre_accepted_prerequisite_writes_its_acceptance_later() {
        let mut leader = booted();
        // A prerequisite held at PRE-ACCEPT, the latest on the key.
        let (_, held) = request(9);
        leader
            .table
            .initialize(held, Digest32([7; 32]), vec![CONSERVATIVE_KEY.to_vec()])
            .unwrap();
        let (frame, c) = request(1);
        let proposed = leader.propose(&frame, None);
        let [batch] = persisted(&proposed).try_into().unwrap();
        let row = decode_dependency(batch.updates[1].value.as_ref().unwrap()).unwrap();
        assert_eq!(row.deps, vec![held]);
        assert_eq!(row.phase, Phase::PreAccept, "the guard failed");
        assert!(!leader.proposals[&c].accepts);
        assert_eq!(leader.table.phase_of(&c), Some(Phase::PreAccept));
        // Only the proposal and the reply wait on the batch: no adoption.
        assert_eq!(leader.pending_sends(), 3);

        // Durable, and the prerequisite still lags: nothing more.
        let after = leader.step(Event::Storage(StorageEvent::JournalDurable {
            barrier_id: batch.barrier,
            journal_seq: LocalJournalSeq::new(1).unwrap(),
        }));
        assert!(persisted(&after).is_empty());
        assert_eq!(leader.table.phase_of(&c), Some(Phase::PreAccept));

        // Once the prerequisite is accepted, the acceptance is written in
        // a batch of its own and the adoption waits on it (task-d19).
        leader.table.accept(held, Vec::new()).unwrap();
        let accepted = leader.advance_pending();
        let [accept] = persisted(&accepted).try_into().unwrap();
        assert_eq!(accept.updates.len(), 1);
        let row = decode_dependency(accept.updates[0].value.as_ref().unwrap()).unwrap();
        assert_eq!(row.phase, Phase::Accept);
        assert_eq!(leader.table.phase_of(&c), Some(Phase::Accept));
        assert!(leader.own_adoptions.contains_key(&accept.barrier));
    }

    #[test]
    fn a_fresh_proposal_whose_guard_passes_carries_its_acceptance() {
        let mut leader = booted();
        let (frame, c) = request(1);
        let proposed = leader.propose(&frame, None);
        let [batch] = persisted(&proposed).try_into().unwrap();
        let row = decode_dependency(batch.updates[1].value.as_ref().unwrap()).unwrap();
        assert_eq!(row.phase, Phase::Accept);
        assert!(leader.proposals[&c].accepts);
        assert!(leader.own_adoptions.contains_key(&batch.barrier));
        // Durable: nothing more is written for it.
        let after = leader.step(Event::Storage(StorageEvent::JournalDurable {
            barrier_id: batch.barrier,
            journal_seq: LocalJournalSeq::new(1).unwrap(),
        }));
        assert!(persisted(&after).is_empty(), "{after:?}");
        assert!(leader.votes[&c].adopted_by(&r(0)));
    }

    /// The leader chooses its next command among the ready records only
    /// (task-d107): with 2,000 commits blocked behind a command it lacks
    /// and one ready, a call examines one record.
    #[test]
    fn the_next_command_is_chosen_among_the_ready_records_only() {
        let mut leader = booted();
        let (rows, ready) = crate::commands::tests::blocked_behind_a_root(2_000);
        leader.table = CommandTable::restore(None, rows);
        leader.table.take_examined();
        assert_eq!(leader.next_executable(), Some(ready));
        assert_eq!(leader.table.take_examined(), 1);
    }

    /// task-d111's cut (e): the release gate starts where it last
    /// stopped. With the table full of proposals unexecuted and none
    /// committed, each call hands the gate one, not every one, and a
    /// proposal entered at a lower number than the frontier is walked
    /// again.
    #[test]
    fn the_release_gate_walks_from_where_it_stopped() {
        let mut leader = booted();
        let mut commands = Vec::new();
        for n in 1..=200 {
            let (frame, c) = request(n);
            let _ = leader.propose(&frame, None);
            commands.push(c);
        }
        let held = leader.unexecuted_in_order().len();
        assert!(held > 1, "{held} held");
        leader.release_walked.set(0);
        for _ in 0..10 {
            assert!(leader.release_ready().is_empty());
        }
        assert_eq!(leader.release_walked.get(), 10, "one of {held} a call");
        let first = leader.proposals[&commands[0]].seqnum;
        assert_eq!(leader.release_from, first);
        // A proposal entered below the frontier lowers it.
        leader.release_from = first + 50;
        let mut again = leader.proposals[&commands[1]].clone();
        again.seqnum = first + 1;
        leader.put_proposal(again);
        assert_eq!(leader.release_from, first + 1);
    }

    /// task-d110 at the leader: the leader's `forget_history` asks the
    /// same rule of the same ledger as a follower's (#217 review, point
    /// 3). A leader that took over a ledger it holds committed and
    /// unexecuted sweeps once, not again in the nine calls after, and
    /// again only once half of what it kept has been executed.
    #[test]
    fn a_leader_behind_sweeps_once_per_half_of_what_it_keeps() {
        let epoch = ConfigurationEpoch::new(1).unwrap();
        let voters: BTreeSet<ReplicaId> = (0..3).map(r).collect();
        let genesis = Ballot {
            epoch,
            number: 0,
            leader: r(0),
        };
        let config = crate::follower::FollowerConfig {
            identity: ConfigurationIdentity {
                cluster: ClusterId([1; 16]),
                domain: DomainId([2; 16]),
                epoch,
                voters: voters.clone(),
                replica: r(1),
                incarnation: ReplicaIncarnation::new(1).unwrap(),
                role: ReplicaRole::Voter,
            },
            quorum: BallotConfiguration::c2(epoch, genesis, voters.clone(), [r(0), r(1)].into())
                .unwrap(),
            genesis,
            frontend: FRONTEND,
            capacity: 8,
        };
        let id = |n: u64| {
            let mut d = [0u8; 32];
            d[..8].copy_from_slice(&n.to_be_bytes());
            d[8] = 7;
            CommandId(Digest32(d))
        };
        let behind = 1_000u64;
        // Committed and waiting for their payloads: nothing executes, so
        // the table forgets none of it.
        let rows: Vec<(CommandId, crate::CommandRecord)> = (0..behind)
            .map(|n| {
                let key = vec![(n % 5) as u8];
                (
                    id(n),
                    crate::CommandRecord {
                        phase: Phase::Commit,
                        deps: Vec::new(),
                        keys: vec![key.clone()],
                        payload: Some(Digest32([4; 32])),
                        paths: vec![(key, Digest32([3; 32]))],
                        synced_seq: Some(n + 1),
                        path: Digest32([3; 32]),
                    },
                )
            })
            .collect();
        let follower = crate::follower::Follower::recover_with_syncs(
            config,
            None,
            None,
            rows,
            [],
            [],
            ExecutionPosition::ZERO,
        );
        let ballot = Ballot {
            epoch,
            number: 1,
            leader: r(1),
        };
        let quorum = BallotConfiguration::c2(epoch, ballot, voters, [r(0), r(1)].into()).unwrap();
        let decision = SyncDecision {
            ballot,
            source_ballot: genesis,
            entries: BTreeMap::new(),
            reproposed: BTreeSet::new(),
        };
        let (mut leader, _) = Leader::from_recovered(follower.into_recovered(), quorum, &decision);
        assert_eq!(leader.ledger.len(), behind as usize);
        let before = leader.ledger.sweeps();
        leader.forget_history(CommandId(Digest32([0xee; 32])));
        assert_eq!(leader.ledger.sweeps(), before + 1, "swept once at first");
        for _ in 0..9 {
            leader.forget_history(CommandId(Digest32([0xee; 32])));
        }
        assert_eq!(
            leader.ledger.sweeps(),
            before + 1,
            "not again per execution"
        );
        assert_eq!(leader.ledger.len(), behind as usize, "nothing forgotten");
        for _ in 0..behind / 2 {
            leader.forget_history(CommandId(Digest32([0xee; 32])));
        }
        assert_eq!(leader.ledger.sweeps(), before + 2, "once half was executed");
    }
}

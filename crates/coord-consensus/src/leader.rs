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

use alloc::collections::BTreeMap;
use alloc::vec::Vec;

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
use crate::learner::{AppliedOutcome, LearnError, Learner, LearningMode};
use crate::messages::{PathAnchors, ProtocolMessage};
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
use crate::summary::{MAX_PAGE_ENTRIES, paginate};
use crate::vote::{FastAck, Vote, VoteError, VoteSet};

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
    /// The same command was already proposed; nothing changed.
    Duplicate(CommandId),
    /// The command table is full.
    Backpressure,
    /// A proposal batch was definitely rejected and is being presented
    /// again unchanged under a new barrier.
    ProposalRetried(CommandId),
    /// A service command presented again while still unlearned: its
    /// proposal was published to the other voters again, unchanged.
    ProposalRepublished(CommandId),
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
    /// Whether the batch is durable.
    pub durable: bool,
    /// The rows of the batch, kept so a definitely rejected write can be
    /// presented again unchanged under a new barrier.
    pub updates: Vec<StoreUpdate>,
    /// Persistence attempts made for this proposal.
    pub attempts: u32,
    /// Whether this command has executed.
    ///
    /// Recorded here rather than read from the command table, because
    /// the table is allowed to forget an executed record to make room
    /// and a forgotten record reports no phase at all. A proposal that
    /// asked the table would then read "not executed yet" for a command
    /// that executed long ago -- and would be speculated over, and its
    /// result released a second time to a caller that already has it.
    pub executed: bool,
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
    proposals: BTreeMap<CommandId, Proposal>,
    votes: BTreeMap<CommandId, VoteSet>,
    payloads: BTreeMap<CommandId, PayloadRecordV1>,
    ledger: DurableLedger,
    learner: Learner,
    seqnum: u64,
    report_due: Option<ReportDue>,
    pending_sync: Option<(ReplicaId, SyncDecision)>,
    speculation: Speculation,
    rejections: Vec<Rejection>,
    fenced: Option<FenceReason>,
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
            config,
            boot: None,
            alloc: None,
            outbox: None,
            ballots,
            table,
            bindings: BTreeMap::new(),
            proposals: BTreeMap::new(),
            votes: BTreeMap::new(),
            payloads: BTreeMap::new(),
            ledger: DurableLedger::new(),
            learner: Learner::new(executed_through),
            seqnum: 0,
            report_due: None,
            pending_sync: None,
            speculation: Speculation::new(),
            rejections: Vec::new(),
            fenced: None,
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
            identity: self.config.identity,
            ballots: self.ballots,
            table: self.table,
            ledger: self.ledger,
            payloads: self.payloads,
            bindings: self.bindings,
            served_payloads: self
                .proposals
                .values()
                .filter(|p| p.durable)
                .map(|p| p.command)
                .collect(),
            learner: self.learner,
            boot: self.boot,
            alloc: self.alloc,
            outbox: self.outbox,
            frontend: self.config.frontend,
            capacity: self.config.capacity,
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
            proposals: BTreeMap::new(),
            votes: BTreeMap::new(),
            payloads: state.payloads,
            ledger: state.ledger,
            learner: state.learner,
            seqnum: 0,
            report_due: state.report_due,
            pending_sync: None,
            speculation: Speculation::new(),
            rejections: Vec::new(),
            fenced: None,
        };
        let _ = identity;
        // Dependency order among the entries: a command follows every
        // dependency that is itself an entry.
        let mut order: Vec<CommandId> = Vec::new();
        let mut remaining: Vec<CommandId> = decision.entries.keys().copied().collect();
        while !remaining.is_empty() {
            let before = remaining.len();
            remaining.retain(|c| {
                let deps = &decision.entries[c].deps;
                if deps
                    .iter()
                    .all(|d| !decision.entries.contains_key(d) || order.contains(d))
                {
                    order.push(*c);
                    false
                } else {
                    true
                }
            });
            if remaining.len() == before {
                // A cycle among entries cannot come from one leader's order;
                // stop rather than guess.
                break;
            }
        }
        let mut effects = Vec::new();
        // The tail of the recovered order, not the largest identity: a
        // command identifier says nothing about execution order, and
        // chaining re-proposals after an arbitrary entry lets a
        // re-proposed command become executable before an earlier one.
        let mut last = order.last().copied();
        for c in order {
            let deps = decision.entries[&c].deps.clone();
            effects.extend(leader.repropose(c, deps));
        }
        for c in &decision.reproposed {
            if leader.table.phase_of(c).is_none() {
                continue;
            }
            let deps = last.map_or_else(Vec::new, |l| alloc::vec![l]);
            effects.extend(leader.repropose(*c, deps));
            last = Some(*c);
        }
        effects.extend(leader.release());
        (leader, effects)
    }

    /// Propose a known command under this ballot with `deps`.
    fn repropose(&mut self, command: CommandId, deps: Vec<CommandId>) -> Vec<Effect> {
        let Some(boot) = self.boot else {
            return Vec::new();
        };
        if self.table.phase_of(&command).is_none() {
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
        let updates = alloc::vec![
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
        self.ledger.stage(barrier, command, record.clone());
        let proposal = FastAck {
            replica: self.config.identity.replica,
            ballot,
            command,
            deps: deps.clone(),
            paths: record.paths.clone(),
            path: record.path,
            admission: record.payload.unwrap_or_else(|| admission_digest(None)),
            seqnum: Some(seqnum),
        };
        let context = self.ballots.context(boot, ballot, LocalJournalSeq::ZERO);
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
                frame: ProtocolMessage::Proposal(proposal.clone()).encode(),
            });
        }
        let mut set = VoteSet::new(self.config.quorum.clone(), command);
        set.add(Vote::Fast(proposal)).expect("leader proposal");
        self.votes.insert(command, set);
        self.proposals.insert(
            command,
            Proposal {
                command,
                barrier,
                seqnum,
                deps,
                path: record.path,
                paths: record.paths.clone(),
                durable: false,
                updates: updates.clone(),
                attempts: 1,
                executed: false,
            },
        );
        alloc::vec![Effect::Persist(PersistBatch {
            barrier,
            base: None,
            updates,
        })]
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
        let context = self
            .ballots
            .context(boot, due.ballot, LocalJournalSeq::ZERO);
        let outbox = self.outbox.as_mut().expect("booted");
        for page in paginate(&report, MAX_PAGE_ENTRIES) {
            outbox.publish(PendingSend {
                context,
                requires: Vec::new(),
                to: due.to,
                frame: ProtocolMessage::ReportPage(page).encode(),
            });
        }
        self.release()
    }

    /// The durable ledger (journal-durable records only).
    pub const fn ledger(&self) -> &DurableLedger {
        &self.ledger
    }

    /// The recovery report for `ballot` from durable state at this cut.
    pub fn report(&self, ballot: Ballot) -> RecoveryReport {
        self.ledger
            .report(self.config.identity.replica, ballot, self.ballots.synced())
    }

    /// The next command to execute through the materializer, if any.
    pub fn next_executable(&self) -> Option<CommandId> {
        self.learner
            .next_executable(&self.table, |c| self.proposals.get(c).map(|p| p.seqnum))
    }

    /// The durable payload of a proposed command.
    pub fn payload(&self, command: &CommandId) -> Option<&PayloadRecordV1> {
        self.payloads.get(command)
    }

    /// The execution frontier.
    pub const fn executed_through(&self) -> ExecutionPosition {
        self.learner.executed_through()
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
        self.learn();
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
        // less: the command is committed, its whole prefix has executed
        // before it, and its result is materialized and durable. That is
        // why it carries `speculative: false`.
        let released = self.speculation.released(&command);
        self.speculation
            .reconcile(command, outcome.position, outcome.result_digest)
            .map_err(LearnError::Speculation)?;
        if let Some(proposal) = self.proposals.get_mut(&command) {
            proposal.executed = true;
        }
        let mut effects = alloc::vec![Effect::Established(result.clone())];
        if self.is_leading() && !released {
            effects.push(Effect::Released(ReleasedResult::from_gate(
                result,
                outcome.response.clone(),
                false,
            )));
        }
        effects.extend(self.release_ready());
        Ok(effects)
    }

    fn learn(&mut self) {
        self.learner.commit_learned(&mut self.table, &self.votes);
    }

    /// Unexecuted proposals in the leader's order.
    fn unexecuted_in_order(&self) -> Vec<CommandId> {
        let mut ordered: Vec<(u64, CommandId)> = self
            .proposals
            .values()
            .filter(|p| !p.executed && self.table.phase_of(&p.command) < Some(Phase::Executed))
            .map(|p| (p.seqnum, p.command))
            .collect();
        ordered.sort();
        ordered.into_iter().map(|(_, c)| c).collect()
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
        let proposals = self.unexecuted_in_order();
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
        match gate.release(&mut self.speculation, &proposals) {
            Ok(released) => released.into_iter().map(Effect::Released).collect(),
            Err(_) => Vec::new(),
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
            && !self.ballots.is_fenced()
            && identity.role == ReplicaRole::Voter
            && identity.epoch == quorum.epoch()
            && identity.voters == *quorum.voters()
            && quorum.is_voter(&identity.replica)
            && self.ballots.promised() == quorum.ballot()
            && quorum.ballot().leader == identity.replica
            && self.ballots.in_flight().is_none()
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
        self.outbox.as_ref().map_or(0, |o| o.pending().len())
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
        self.propose(frame, Some(admission))
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
        let logical = request.logical().ok()?;
        let command = CommandId::derive(&request.retry_key, &logical).ok()?;
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
            .unwrap_or_else(|| admission_digest(None));
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
                frame: ProtocolMessage::Proposal(ack.clone()).encode(),
            });
        }
        self.rejections
            .push(Rejection::ProposalRepublished(command));
        Some(self.release())
    }

    fn propose(&mut self, frame: &[u8], admission: Option<AdmissionFacts>) -> Vec<Effect> {
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
        // Identity: one retry key binds one payload, first presentation wins.
        match self.bindings.get(&request.retry_key) {
            Some(bound) if *bound != command => {
                self.rejections.push(Rejection::RequestIdentityConflict {
                    retry_key: request.retry_key,
                    bound: *bound,
                });
                return Vec::new();
            }
            Some(_) => {
                self.rejections.push(Rejection::Duplicate(command));
                return Vec::new();
            }
            None => {}
        }
        let payload = PayloadRecordV1 {
            retry_key: request.retry_key,
            logical: request.logical.as_slice().to_vec(),
            admission,
        };
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
            Err(InitError::AlreadyInitialized | InitError::PayloadConflict) => {
                self.rejections.push(Rejection::Duplicate(command));
                return Vec::new();
            }
        };
        self.bindings.insert(request.retry_key, command);
        self.payloads.insert(command, payload.clone());
        let ballot = self.config.quorum.ballot();
        let epoch = self.config.identity.epoch;
        let seqnum = self.seqnum;
        self.seqnum += 1;
        // The durable row records the phase the command is actually in.
        // The leader's own acceptance of its order is written separately,
        // once the dependency guard passes (see `advance_pending`).
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
                frame: ProtocolMessage::Proposal(proposal.clone()).encode(),
            });
        }
        outbox.publish(PendingSend {
            context,
            requires: alloc::vec![barrier],
            to: self.config.frontend,
            frame: ProtocolMessage::LeaderReply {
                ballot,
                command,
                seqnum,
                deps: init.deps.clone(),
                path: init.path,
            }
            .encode(),
        });
        // The leader's own acknowledgement counts toward learning later.
        let mut set = VoteSet::new(self.config.quorum.clone(), command);
        set.add(Vote::Fast(proposal)).expect("leader proposal");
        self.votes.insert(command, set);
        self.proposals.insert(
            command,
            Proposal {
                command,
                barrier,
                seqnum,
                deps: init.deps,
                path: init.path,
                paths: init.paths,
                durable: false,
                updates,
                attempts: 1,
                executed: false,
            },
        );
        alloc::vec![persist]
    }

    fn on_storage(&mut self, event: &StorageEvent) -> Vec<Effect> {
        if let Some(outbox) = self.outbox.as_mut() {
            outbox.observe(event);
        }
        self.ballots.on_storage(event);
        // Ledger bookkeeping covers every batch this machine staged, not
        // only the proposal batches: an acceptance row carries its own
        // barrier and is what a recovery report must show.
        if let Some(barrier) = event.barrier() {
            match event {
                StorageEvent::JournalDurable { journal_seq, .. } => {
                    self.ledger.durable(barrier, *journal_seq);
                }
                StorageEvent::Failed { .. } => {
                    self.ledger.failed(barrier);
                }
                _ => {}
            }
        }
        if let Some(barrier) = event.barrier()
            && let Some(command) = self
                .proposals
                .values()
                .find(|p| p.barrier == barrier)
                .map(|p| p.command)
        {
            match event {
                StorageEvent::JournalDurable { .. } => {
                    if let Some(p) = self.proposals.get_mut(&command) {
                        p.durable = true;
                    }
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
        self.learn();
        effects.extend(self.deliver_due_report());
        effects.extend(self.release());
        effects
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
            .unwrap_or_else(|| admission_digest(None));
        let proposal = self.proposals.get_mut(&command).expect("checked above");
        proposal.barrier = barrier;
        proposal.attempts += 1;
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
                frame: ProtocolMessage::Proposal(ack.clone()).encode(),
            });
        }
        outbox.publish(PendingSend {
            context,
            requires: alloc::vec![barrier],
            to: self.config.frontend,
            frame: ProtocolMessage::LeaderReply {
                ballot,
                command,
                seqnum,
                deps,
                path,
            }
            .encode(),
        });
        self.rejections.push(Rejection::ProposalRetried(command));
        alloc::vec![Effect::Persist(batch)]
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
            let candidates: Vec<(CommandId, Vec<CommandId>)> = self
                .proposals
                .values()
                .filter(|p| p.durable && self.table.phase_of(&p.command) == Some(Phase::PreAccept))
                .map(|p| (p.command, p.deps.clone()))
                .collect();
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

    fn on_peer(&mut self, from: PeerId, frame: &[u8]) -> Vec<Effect> {
        let Ok(message) = ProtocolMessage::decode(frame) else {
            self.rejections.push(Rejection::MalformedPeerMessage);
            return Vec::new();
        };
        match message {
            ProtocolMessage::NewLeader { ballot } => {
                let outstanding = self.cut();
                let (Some(boot), Some(alloc)) = (self.boot, self.alloc.as_mut()) else {
                    return Vec::new();
                };
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
                if self.deposed() {
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
            ProtocolMessage::Sealed { .. }
            | ProtocolMessage::Proposal(_)
            | ProtocolMessage::Promise { .. }
            | ProtocolMessage::LeaderReply { .. }
            | ProtocolMessage::ReportPage(_)
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
            .filter(|c| self.proposals.get(c).is_some_and(|p| p.durable))
            .filter_map(|c| {
                self.payloads
                    .get(c)
                    .map(|p| ProtocolMessage::PayloadResponse {
                        command: *c,
                        payload: p.clone(),
                    })
            })
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

    fn collect(&mut self, from: ReplicaId, vote: Vote) -> Vec<Effect> {
        if vote.replica() != from {
            self.rejections.push(Rejection::Vote(VoteError::NotAVoter));
            return Vec::new();
        }
        let command = vote.command();
        let Some(set) = self.votes.get_mut(&command) else {
            // Acknowledgements for a command this leader never proposed are
            // kept out: nothing to learn from.
            self.rejections
                .push(Rejection::Vote(VoteError::WrongCommand));
            return Vec::new();
        };
        if let Err(e) = set.add(vote) {
            self.rejections.push(Rejection::Vote(e));
        }
        self.learn();
        self.release_ready()
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
            Event::Storage(event) => self.on_storage(&event),
            Event::Peer(message) => {
                // The authenticated sender, at the exact incarnation the
                // transport bound; replies are addressed to it.
                let from = PeerId {
                    replica: message.provenance().from(),
                    incarnation: message.provenance().incarnation(),
                };
                self.on_peer(from, message.frame())
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

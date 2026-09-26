//! Normal-operation follower (task-23; design Sections 4.3, 4.7, 5.1, 18;
//! prototype `handlePropose` on a follower, `fastAckFromLeader`,
//! `afterPropagate`, `handleLightSlowAck`).
//!
//! A follower is a [`DeterministicMachine`]. For an admitted request it
//! initializes the command atomically (payload binding, local
//! dependencies, path evidence, conflict index), persists payload and
//! dependency rows in one batch and, when it belongs to the ballot's
//! fast set, publishes its fast acknowledgement to every other voter and
//! the frontend requiring that batch (Section 5.1: payload, vote, path
//! evidence, prerequisite dependencies). A leader proposal that arrives
//! before the payload is held against a placeholder that no lookup or
//! guard can see; once the payload arrives, the leader's order is adopted
//! only when every direct dependency is at least ACCEPT (explicit guard,
//! not the prototype's `TODO`), the adopted order is persisted, and the
//! slow acknowledgement is published requiring that batch. Duplicate and
//! reordered messages converge. After a crash the table is rebuilt from
//! the durable rows, so a vote that was durable is a fact and one that
//! was not was never sent.
//!
//! Acknowledgements from peers are collected; learning is not decided
//! here (task-24), and equal direct dependencies prove nothing by
//! themselves.

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::vec::Vec;

use coord_core::capability::{AdmissionFacts, admission_digest};
use coord_core::effect::{BarrierId, BootId, Effect, PeerId, PersistBatch};
use coord_core::event::{Event, StorageEvent};
use coord_core::machine::DeterministicMachine;
use coord_core::outbox::{BarrierAllocator, Outbox, PendingSend};
use coord_types::ids::{Ballot, ExecutionPosition, LocalJournalSeq, ReplicaId, ReplicaIncarnation};
use coord_types::wire_v1::{MessageV1, decode_stream};
use coord_types::{CommandId, RetryKey};

use crate::ballot::{BallotState, ConfigurationIdentity, PromiseRejection, ReplicaRole};
use crate::campaign::Campaign;
use crate::commands::{CommandRecord, CommandTable, InitError};
use crate::learner::{AppliedOutcome, LearnError, Learner, LearningMode};
use crate::messages::{MAX_PAYLOAD_TRANSFER, ProtocolMessage};
use crate::phase::Phase;
use crate::quorum::BallotConfiguration;
use crate::recovery::{RecoveryError, SyncDecision, SyncEntry};
use crate::recovery::{RecoveryReport, ReportEntry};
use crate::role::RecoveredState;
use crate::rows::{PayloadRecordV1, PromiseRecordV1, dependency_update, payload_update};
use crate::rows::{SyncRecordV1, promise_update, sync_update};
use crate::summary::DurableLedger;
use crate::summary::{MAX_PAGE_ENTRIES, PageError, paginate};
use crate::vote::{FastAck, SlowAck, Vote, VoteError, VoteSet};

/// Static configuration of a follower.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FollowerConfig {
    /// Configuration identity of this replica.
    pub identity: ConfigurationIdentity,
    /// The current ballot and its fixed fast set.
    pub quorum: BallotConfiguration,
    /// Genesis ballot of the epoch.
    pub genesis: Ballot,
    /// The trusted frontend acknowledgements also go to.
    pub frontend: PeerId,
    /// Command table capacity.
    pub capacity: usize,
}

/// A refused or dropped input, recorded for the caller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FollowerRejection {
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
    /// fault: what this replica already produced for it is offered to
    /// the submitter again (task-c02).
    Duplicate(CommandId),
    /// The retry key is bound to this command, but the presentation
    /// carries other admission facts or another acknowledged floor. The
    /// identity is shared; the request is not, and nothing is replayed
    /// for it.
    RequestFactsConflict {
        /// Command.
        command: CommandId,
        /// Digest of the facts this replica accepted the command under.
        accepted: coord_types::identity::Digest32,
    },
    /// The command table holds this command under another payload
    /// digest. Nothing is replayed for it.
    PayloadConflict(CommandId),
    /// An exact duplicate could not repair delivery of this replica's
    /// evidence, and why. The command itself is unaffected.
    ReplayRefused {
        /// Command.
        command: CommandId,
        /// Why.
        why: crate::replay::ReplayRefusal,
    },
    /// The command table is full.
    Backpressure,
    /// A vote or adoption batch failed; the command stays where it was.
    BatchFailed(CommandId),
    /// A proposal for another ballot or from a non-leader.
    ForeignProposal,
    /// A Sync named a ballot at or below the synchronized one, or from
    /// another epoch: the record never regresses.
    SyncRegression(crate::ballot::SyncRejection),
    /// The synchronized-ballot row of a Sync failed; nothing was
    /// activated.
    SyncNotDurable(Ballot),
    /// A higher ballot was promised before the synchronized row became
    /// durable; that cut supersedes this Sync.
    SyncSuperseded(Ballot),
    /// A higher promise is in flight or durable, so the configured ballot
    /// no longer votes; the work belongs to the new leader.
    FencedByPromise {
        /// The ballot promised now.
        promised: Ballot,
    },
    /// A peer vote was rejected.
    Vote(VoteError),
    /// A leader proposal named this command under an admission this
    /// replica did not accept it under. The identity is shared; the
    /// command is not.
    AdmissionConflict {
        /// Command.
        command: CommandId,
        /// What this replica accepted the command under.
        accepted: coord_types::identity::Digest32,
    },
    /// A `NewLeader` was rejected.
    Promise(PromiseRejection),
    /// A `SealRequest` was rejected (task-55).
    Seal(crate::ballot::SealRejection),
    /// The configuration is sealed (or a seal row is in flight): nothing
    /// is adopted or acknowledged, because ordinary voting of this
    /// configuration is over (task-55).
    Sealed {
        /// The transition the seal is for.
        transition: crate::handoff::Transition,
    },
    /// A peer frame did not decode.
    MalformedPeerMessage,
    /// A proposal of a ballot this replica no longer votes in.
    /// A payload response that does not rehash to its identity.
    PayloadIdentityMismatch(CommandId),
    /// A report page was refused.
    Page(PageError),
    /// The campaign stopped: the selection failed with this evidence.
    Campaign(RecoveryError),
    /// A Sync for a ballot other than the promised one, or not from its
    /// leader.
    SyncRejected {
        /// Sync ballot.
        ballot: Ballot,
        /// Promised ballot.
        promised: Ballot,
    },
    /// A campaign was requested for a ballot this replica cannot lead.
    CannotLead,
    /// This replica is further behind than recovery carries anyone: a
    /// selection it was about to bind depends on `missing`, which the
    /// selection does not carry and this replica has not committed. The
    /// campaign was abandoned, and this replica does not campaign again
    /// this boot (task-d05).
    Behind {
        /// The dependency the selection leaves this replica without.
        missing: CommandId,
    },
}

/// A report owed once every batch submitted before the cut is durable.
/// A recovery report this replica owes a candidate.
type ReportDue = crate::role::PendingReport;

/// Report every command this replica executed as committed (task-d05).
///
/// A commit is not written as a row, so a durable record says ACCEPT for
/// a command long executed. Reported as ACCEPT, the selection had the new
/// leader propose it again and wait for votes -- from voters that had
/// executed and retired it and had no record left to vote from. Executing
/// a command means it was committed, under the dependencies its record
/// holds, so that is what the report says, and the selection installs it
/// as committed wherever it goes.
pub(crate) fn report_executed_as_committed(report: &mut RecoveryReport, table: &CommandTable) {
    for entry in &mut report.entries {
        if entry.phase < Phase::Commit && table.phase_of(&entry.command) == Some(Phase::Executed) {
            entry.phase = Phase::Commit;
        }
    }
}

/// How many times the command table's capacity the durable ledger may
/// hold before the records of forgotten commands are swept.
pub(crate) const HISTORY_SWEEP: usize = 4;

/// How many times the command table's own capacity of leader proposals
/// this replica will hold while it has no room to record them.
///
/// Holding is what keeps a full table from becoming a permanent one: the
/// order in a proposal is the only copy this replica is ever offered, so
/// a proposal dropped for want of a table slot is a command it can never
/// adopt, and with a conservative key making the chain total, neither is
/// anything ordered after it. The slack is generous for that reason, and
/// bounded because a peer's proposals are still a peer's input.
pub const HELD_PROPOSAL_SLACK: usize = 8;

/// A leader proposal not yet adopted: held until the payload arrives and
/// every dependency is at least ACCEPT.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeldProposal {
    /// The leader's proposal.
    pub proposal: FastAck,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Pending {
    /// Payload/dependency batch of a vote.
    Vote(CommandId),
    /// Adoption batch.
    Adoption(CommandId),
}

/// A vote this replica produced whose supporting batch is not durable
/// yet. It counts toward learning only once that batch is durable:
/// evidence this replica has not made a fact must never make a command
/// executable, because the batch may still fail.
#[derive(Clone, Debug, PartialEq, Eq)]
struct DeferredVote {
    command: CommandId,
    vote: Vote,
}

/// The follower machine of one domain.
#[derive(Debug)]
pub struct Follower {
    config: FollowerConfig,
    boot: Option<BootId>,
    alloc: Option<BarrierAllocator>,
    outbox: Option<Outbox>,
    ballots: BallotState,
    table: CommandTable,
    bindings: BTreeMap<RetryKey, CommandId>,
    held: BTreeMap<CommandId, HeldProposal>,
    /// Adopted commands: leader sequence number and whether durable.
    adopted: BTreeMap<CommandId, (u64, bool)>,
    pending: BTreeMap<BarrierId, Pending>,
    deferred: BTreeMap<BarrierId, DeferredVote>,
    votes: BTreeMap<CommandId, VoteSet>,
    payloads: BTreeMap<CommandId, PayloadRecordV1>,
    durable_payloads: BTreeMap<BarrierId, CommandId>,
    served_payloads: alloc::collections::BTreeSet<CommandId>,
    /// Where the next bounded payload ask starts in the missing set, so
    /// that a bound on how many are asked for at once does not mean the
    /// same few are asked for every time and the rest never.
    payload_cursor: usize,
    /// How many payload transfers a peer has answered this replica with.
    ///
    /// The count of *answers*, not of what is still missing. A replica
    /// catching up wants to know whether its last ask was replied to,
    /// and the missing count cannot say: under load it moves because
    /// new commands arrive by identity, whether or not anything came
    /// back. This only moves when a peer sent a payload.
    ///
    /// And only a payload that answers the outstanding ask: one named in
    /// `payloads_asked` that this replica now holds. A delayed or
    /// duplicated answer -- to an ask the retry floor already repeated,
    /// or to one a later ask superseded -- is still taken if it is new,
    /// but it does not count, so it cannot satisfy the next ask's
    /// threshold and put a second batch on the lane while one is
    /// outstanding.
    payloads_answered: u64,
    /// The commands the outstanding payload ask named that have not been
    /// answered yet. Each ask replaces it.
    payloads_asked: alloc::collections::BTreeSet<CommandId>,
    ledger: DurableLedger,
    learner: Learner,
    campaign: Option<Campaign>,
    /// Set when a selection showed this replica further behind than
    /// recovery carries anyone; it does not campaign again this boot.
    behind: Option<CommandId>,
    report_due: Option<ReportDue>,
    sync_pending: BTreeMap<CommandId, SyncEntry>,
    /// The Sync whose synchronized-ballot row is in flight: the new ballot
    /// is activated only when that row is durable.
    sync_barrier: Option<(BarrierId, SyncDecision)>,
    won: Option<SyncDecision>,
    /// Voting messages of the promised ballot that arrived before its Sync
    /// (delivery is not ordered across peers); replayed once synchronized.
    awaiting_sync: Vec<(ReplicaId, ProtocolMessage)>,
    /// A selection read back from durable state that this replica had
    /// synchronized to but not finished installing.
    resumed: Option<SyncDecision>,
    /// A Sync from its ballot's leader that arrived before this replica
    /// promised that ballot, kept until it does.
    ///
    /// The Sync is published once, to every voter, as soon as the new
    /// leader's selection is durable, and nothing publishes it again. A
    /// voter whose promise is still on its way -- the leader counted a
    /// majority without it, or this replica was answering another
    /// candidate first -- used to refuse it and was then left promised to
    /// a ballot it could never synchronize to, holding every proposal of
    /// that ballot for ever. Kept, it is installed the moment the promise
    /// is made, which is the order the protocol meant (task-d05).
    early_sync: Option<(ReplicaId, SyncDecision)>,
    rejections: Vec<FollowerRejection>,
    /// What this replica published to the frontend for each command it
    /// still remembers, kept so an exact duplicate submission can offer
    /// it to the submitter again (task-c02). Never recovered: the outbox
    /// it went through is this boot's.
    replay: crate::replay::EvidenceStore,
}

impl Follower {
    /// A follower from its configuration, the recovered promise row, the
    /// durable dependency rows of the epoch and the durable payload rows.
    /// The payload rows carry the retry key each command was derived from,
    /// so the one-key-one-payload binding survives a restart: a client
    /// reusing a durable retry key with other bytes is a conflict, never a
    /// second command.
    /// `executed_through` is the application's durable execution
    /// position: the learner resumes from it, so the next command it
    /// establishes is the one the applier will plan, not position one.
    ///
    /// Recovers *unsealed*: a replica whose configuration may have been
    /// sealed comes back through [`Follower::recover_with_syncs`], which
    /// takes the seal row. The two are separate so that recovering a
    /// sealed configuration is something a caller writes down rather
    /// than something it gets by default (task-55).
    pub fn recover(
        config: FollowerConfig,
        durable_promise: Option<PromiseRecordV1>,
        rows: impl IntoIterator<Item = (CommandId, CommandRecord)>,
        payloads: impl IntoIterator<Item = (CommandId, PayloadRecordV1)>,
        executed_through: ExecutionPosition,
    ) -> Self {
        Follower::recover_with_syncs(
            config,
            durable_promise,
            None,
            rows,
            payloads,
            core::iter::empty(),
            executed_through,
        )
    }

    /// Recover with the epoch's durable Sync rows as well. The row of the
    /// ballot this replica synchronized to is the selection it accepted:
    /// its installation resumes here, so a restart between the marker and
    /// the last installed entry cannot report a completed synchronization
    /// over an incomplete ledger.
    pub fn recover_with_syncs(
        config: FollowerConfig,
        durable_promise: Option<PromiseRecordV1>,
        durable_seal: Option<crate::rows::SealRecordV1>,
        rows: impl IntoIterator<Item = (CommandId, CommandRecord)>,
        payloads: impl IntoIterator<Item = (CommandId, PayloadRecordV1)>,
        syncs: impl IntoIterator<Item = (Ballot, SyncDecision)>,
        executed_through: ExecutionPosition,
    ) -> Self {
        let payloads: BTreeMap<CommandId, PayloadRecordV1> = payloads.into_iter().collect();
        let ballots = BallotState::recover_sealed(
            config.identity.clone(),
            config.genesis,
            durable_promise,
            durable_seal,
        );
        let resumed: Option<SyncDecision> = syncs
            .into_iter()
            .find(|(b, _)| *b == ballots.synced())
            .map(|(_, d)| d);
        let rows: Vec<(CommandId, CommandRecord)> = rows.into_iter().collect();
        let ledger = DurableLedger::restore(rows.iter().cloned());
        let table = CommandTable::restore(Some(config.capacity), rows);
        let adopted = table
            .records()
            .filter(|(_, r)| r.phase >= Phase::Accept)
            .map(|(c, _)| (*c, (u64::MAX, true)))
            .collect();
        Follower {
            replay: crate::replay::EvidenceStore::new(config.capacity),
            config,
            sync_barrier: None,
            boot: None,
            alloc: None,
            outbox: None,
            ballots,
            table,
            bindings: payloads
                .iter()
                .map(|(command, record)| (record.retry_key, *command))
                .collect(),
            held: BTreeMap::new(),
            adopted,
            pending: BTreeMap::new(),
            deferred: BTreeMap::new(),
            votes: BTreeMap::new(),
            // A payload this replica made durable stays servable after a
            // restart: a report claiming the payload is present must be one
            // this replica can honour.
            served_payloads: payloads.keys().copied().collect(),
            payload_cursor: 0,
            payloads_answered: 0,
            payloads_asked: alloc::collections::BTreeSet::new(),
            payloads,
            durable_payloads: BTreeMap::new(),
            ledger,
            learner: Learner::new(executed_through),
            campaign: None,
            behind: None,
            report_due: None,
            sync_pending: BTreeMap::new(),
            won: None,
            awaiting_sync: Vec::new(),
            rejections: Vec::new(),
            resumed,
            early_sync: None,
        }
        .resume_sync()
    }

    /// Queue the entries of a resumed selection that this replica has not
    /// installed yet. Installation itself waits for boot, like everything
    /// else that needs a barrier allocator.
    fn resume_sync(mut self) -> Self {
        let Some(decision) = self.resumed.take() else {
            return self;
        };
        for (c, e) in &decision.entries {
            if self.table.phase_of(c).is_none() {
                let _ = self.table.expect(*c);
            }
            if self.table.phase_of(c) < Some(Phase::Commit) {
                self.sync_pending.insert(*c, e.clone());
            }
        }
        self.won = None;
        self
    }

    /// Restore the durable execution frontier: the commands whose executed
    /// identity rows exist are marked executed and the learner resumes
    /// after `through`. Called before boot on a recovered follower; the
    /// rows are the materializer's, written in the same batch as the
    /// application rows (Section 6.5), so this never invents execution.
    ///
    /// The executed records are then retired, in the order they executed,
    /// and what that leaves forgotten is swept (task-d05). Every durable
    /// dependency row comes back as a record, so a voter restarted after
    /// a long history held all of it as live records, and reported all of
    /// it to the first candidate that asked -- which after a kill is
    /// straight away.
    pub fn restore_execution(
        mut self,
        through: ExecutionPosition,
        executed: impl IntoIterator<Item = CommandId>,
    ) -> Self {
        let executed: Vec<CommandId> = executed.into_iter().collect();
        for c in &executed {
            self.table.restore_executed(c);
            self.adopted.entry(*c).or_insert((u64::MAX, true));
        }
        for c in &executed {
            let _ = self.table.retire(c);
        }
        self.sweep_history();
        self.learner = Learner::with_mode(through, self.learner.mode());
        self
    }

    /// Restore the durable payloads of recovered commands (their
    /// `payload_v1` rows, written in the same batch as the dependency row)
    /// so the materializer and payload requests are served after a
    /// restart. Called before boot; unknown commands are ignored.
    pub fn restore_payloads(
        mut self,
        payloads: impl IntoIterator<Item = (CommandId, PayloadRecordV1)>,
    ) -> Self {
        for (c, p) in payloads {
            if self.table.phase_of(&c).is_none() {
                continue;
            }
            self.bindings.insert(p.retry_key, c);
            // History stays swept: `restore_execution` has just forgotten
            // it, and reloading it here would hold a payload per command
            // ever executed until the next sweep (task-d05).
            if self.table.forgotten(&c) {
                continue;
            }
            self.payloads.insert(c, p);
            self.served_payloads.insert(c);
        }
        self
    }

    /// A follower from state carried across a role change, under `quorum`.
    pub fn from_recovered(state: RecoveredState, quorum: BallotConfiguration) -> Self {
        Follower {
            config: FollowerConfig {
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
            held: BTreeMap::new(),
            adopted: BTreeMap::new(),
            pending: BTreeMap::new(),
            votes: BTreeMap::new(),
            payloads: state.payloads,
            durable_payloads: BTreeMap::new(),
            served_payloads: state.served_payloads,
            payload_cursor: 0,
            payloads_answered: 0,
            payloads_asked: alloc::collections::BTreeSet::new(),
            ledger: state.ledger,
            learner: state.learner,
            deferred: BTreeMap::new(),
            campaign: None,
            behind: None,
            // A report owed to a candidate is an obligation of the replica,
            // not of the role it held when the request arrived: dropping it
            // would stall a candidate that needs this replica's majority.
            report_due: state.report_due,
            sync_pending: BTreeMap::new(),
            sync_barrier: None,
            won: None,
            awaiting_sync: Vec::new(),
            rejections: Vec::new(),
            resumed: None,
            early_sync: None,
            replay: crate::replay::EvidenceStore::new(state.capacity),
        }
    }

    /// Give up the role: everything durable or learned, nothing
    /// ballot-scoped.
    pub fn into_recovered(self) -> RecoveredState {
        RecoveredState {
            identity: self.config.identity,
            ballots: self.ballots,
            table: self.table,
            ledger: self.ledger,
            payloads: self.payloads,
            bindings: self.bindings,
            served_payloads: self.served_payloads,
            learner: self.learner,
            boot: self.boot,
            alloc: self.alloc,
            outbox: self.outbox,
            report_due: self.report_due,
            frontend: self.config.frontend,
            capacity: self.config.capacity,
        }
    }

    /// The current ballot configuration.
    pub const fn quorum(&self) -> &BallotConfiguration {
        &self.config.quorum
    }

    /// The Sync this replica selected and activated as the new leader, if
    /// its campaign won.
    pub const fn won(&self) -> Option<&SyncDecision> {
        self.won.as_ref()
    }

    /// The campaign in progress.
    pub const fn campaign_state(&self) -> Option<&Campaign> {
        self.campaign.as_ref()
    }

    /// Start a campaign for `ballot` (this replica must be its leader):
    /// promise to itself durably, then ask every other voter.
    pub fn campaign(&mut self, ballot: Ballot) -> Vec<Effect> {
        let Some(boot) = self.boot else {
            return Vec::new();
        };
        if ballot.leader != self.config.identity.replica {
            self.rejections.push(FollowerRejection::CannotLead);
            return Vec::new();
        }
        if let Some(missing) = self.behind {
            self.rejections.push(FollowerRejection::Behind { missing });
            return Vec::new();
        }
        let Ok(config) = BallotConfiguration::c2_default(
            self.config.identity.epoch,
            ballot,
            self.config.identity.voters.clone(),
        ) else {
            self.rejections.push(FollowerRejection::CannotLead);
            return Vec::new();
        };
        let outstanding: Vec<BarrierId> = self.outstanding_for_cut();
        let alloc = self.alloc.as_mut().expect("booted");
        // A replica campaigning for itself: its own authenticated identity.
        let me = PeerId {
            replica: self.config.identity.replica,
            incarnation: self.config.identity.incarnation,
        };
        let effects = match self
            .ballots
            .on_new_leader(me, ballot, boot, alloc, &outstanding)
        {
            Ok(e) => e,
            Err(e) => {
                self.rejections.push(FollowerRejection::Promise(e));
                return Vec::new();
            }
        };
        let Effect::Persist(batch) = &effects.persist else {
            unreachable!("promise persists")
        };
        let promise_barrier = batch.barrier;
        self.report_due = Some(ReportDue {
            ballot,
            to: PeerId {
                replica: self.config.identity.replica,
                incarnation: self.config.identity.incarnation,
            },
            requires: effects.reply.requires.clone(),
        });
        let context = self.ballots.context(boot, ballot, LocalJournalSeq::ZERO);
        let outbox = self.outbox.as_mut().expect("booted");
        for voter in &self.config.identity.voters {
            if *voter == self.config.identity.replica {
                continue;
            }
            outbox.publish(PendingSend {
                context,
                requires: alloc::vec![promise_barrier],
                to: PeerId {
                    replica: *voter,
                    incarnation: ReplicaIncarnation::ZERO,
                },
                frame: ProtocolMessage::NewLeader { ballot }.encode(),
            });
        }
        self.campaign = Some(Campaign::new(config));
        let mut out = alloc::vec![effects.persist];
        out.extend(self.release());
        out
    }

    /// Resume a campaign whose selection was durably bound before a crash:
    /// the same decision is published again; nothing is reselected.
    pub fn resume_campaign(&mut self, decision: SyncDecision) -> Vec<Effect> {
        if decision.ballot.leader != self.config.identity.replica
            || self.ballots.promised() != decision.ballot
        {
            self.rejections.push(FollowerRejection::CannotLead);
            return Vec::new();
        }
        let Ok(config) = BallotConfiguration::c2_default(
            self.config.identity.epoch,
            decision.ballot,
            self.config.identity.voters.clone(),
        ) else {
            self.rejections.push(FollowerRejection::CannotLead);
            return Vec::new();
        };
        self.campaign = Some(Campaign::resumed(config, decision));
        self.advance_campaign()
    }

    /// Deliver a report once every batch before its cut is durable: pages
    /// to a candidate, or the own report to this replica's campaign.
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
        let report = self.report(due.ballot);
        if due.to.replica == self.config.identity.replica {
            if let Some(c) = self.campaign.as_mut()
                && c.ballot() == due.ballot
            {
                c.own_report(report);
            }
            return self.advance_campaign();
        }
        let Some(boot) = self.boot else {
            return Vec::new();
        };
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

    /// Select once a majority of complete reports is in, bind the result
    /// durably, then publish and activate it.
    fn advance_campaign(&mut self) -> Vec<Effect> {
        let Some(boot) = self.boot else {
            return Vec::new();
        };
        let Some(campaign) = self.campaign.as_mut() else {
            return Vec::new();
        };
        if campaign.decision().is_none() {
            match campaign.try_select() {
                Ok(None) => return Vec::new(),
                Ok(Some(_)) => {}
                Err(e) => {
                    self.rejections.push(FollowerRejection::Campaign(e));
                    self.campaign = None;
                    return Vec::new();
                }
            }
        }
        if campaign.binding().is_none() && !campaign.is_durable() {
            let decision = campaign.decision().expect("selected").clone();
            // A candidate further behind than every reporter's window
            // would win and then never execute again: the selection names
            // what it lacks only as a dependency, since every reporter
            // executed it long ago and leaves it out, and nothing it binds
            // brings it. It is not this replica's to lead; another voter
            // is, and this one waits to be brought up (task-d05).
            if let Some(missing) = Self::selection_gap(&self.table, &decision) {
                self.rejections.push(FollowerRejection::Behind { missing });
                self.behind = Some(missing);
                self.campaign = None;
                return Vec::new();
            }
            // A selected command this replica never stored (it was down
            // while the request was admitted) needs its payload before the
            // result is bound: the new leader re-proposes from payloads,
            // never from identities alone. The reporting voters hold them
            // durably (they reported the command).
            if !Self::selection_missing(&self.table, &decision).is_empty() {
                // Ask again when a voter promised that has not been asked
                // yet -- the ones asked first may be gone while a live
                // quorum holds the payload -- or when the last ask was
                // answered in full, for the next batch. An ask carries at
                // most `MAX_PAYLOAD_TRANSFER` payloads back, so a
                // candidate that asked once for everything it lacked got
                // one batch and then waited for the rest for ever.
                let me = self.config.identity.replica;
                let due = !campaign.payload_requests_due(me).is_empty();
                if !due && !self.payloads_asked.is_empty() {
                    return Vec::new();
                }
                return self.request_payloads(me);
            }
            let table = &self.table;
            campaign.commit_executed(|c| match table.phase_of(c) {
                Some(Phase::Executed) => Some(table.record(c).map(|r| r.deps.clone())),
                _ => None,
            });
            let decision = campaign.decision().expect("selected").clone();
            let barrier = self.alloc.as_mut().expect("booted").allocate();
            campaign.bound(barrier);
            let update = sync_update(self.config.identity.epoch, &SyncRecordV1 { decision })
                .expect("bounded");
            return alloc::vec![Effect::Persist(PersistBatch {
                barrier,
                base: None,
                updates: alloc::vec![update],
            })];
        }
        if campaign.is_durable() && !campaign.is_published() {
            let decision = campaign.decision().expect("selected").clone();
            campaign.mark_published();
            let context = self
                .ballots
                .context(boot, decision.ballot, LocalJournalSeq::ZERO);
            let outbox = self.outbox.as_mut().expect("booted");
            for voter in &self.config.identity.voters {
                if *voter == self.config.identity.replica {
                    continue;
                }
                outbox.publish(PendingSend {
                    context,
                    requires: Vec::new(),
                    to: PeerId {
                        replica: *voter,
                        incarnation: ReplicaIncarnation::ZERO,
                    },
                    frame: ProtocolMessage::Sync(decision.clone()).encode(),
                });
            }
            let leader = decision.ballot.leader;
            let mut out = self.on_sync(leader, decision.clone());
            if self.ballots.synced() == decision.ballot {
                // Becoming the leader is part of activating the ballot,
                // which waits for the synchronized row (see `activate`).
                // A resumed campaign whose row is already durable and whose
                // ballot is already active takes the role here instead,
                // since there is nothing left to wait for.
                if self.config.quorum.ballot() == decision.ballot {
                    self.won = Some(decision);
                }
            } else {
                // A higher ballot was promised while the row was becoming
                // durable: the bound Sync is void here and the campaign is
                // lost; the new ballot's leader recovers from the rows.
                self.campaign = None;
            }
            out.extend(self.release());
            return out;
        }
        Vec::new()
    }

    /// Whether a voting message belongs to the promised ballot whose Sync
    /// has not arrived yet: it is held (bounded by the table capacity)
    /// rather than rejected, since the leader publishes its re-proposals
    /// right after the Sync and peers may deliver them first.
    fn stash_until_sync(&self, message: &ProtocolMessage) -> bool {
        let ballot = match message {
            ProtocolMessage::Proposal(a) | ProtocolMessage::FastAck(a) => a.ballot,
            ProtocolMessage::SlowAck(a) => a.ballot,
            _ => return false,
        };
        ballot == self.ballots.promised()
            && ballot != self.config.quorum.ballot()
            && self.ballots.in_flight().is_none()
            && self.awaiting_sync.len() < self.config.capacity
    }

    /// Replay the messages held for the ballot just synchronized.
    fn replay_awaiting(&mut self) -> Vec<Effect> {
        let held = core::mem::take(&mut self.awaiting_sync);
        let mut effects = Vec::new();
        for (from, message) in held {
            effects.extend(match message {
                ProtocolMessage::Proposal(p) => self.on_proposal(from, p),
                ProtocolMessage::FastAck(a) => self.collect(from, Vote::Fast(a)),
                ProtocolMessage::SlowAck(a) => self.collect(from, Vote::Slow(a)),
                _ => Vec::new(),
            });
        }
        effects
    }

    /// Adopt a Sync: only for the ballot this replica promised and from
    /// its leader. The synchronized ballot is persisted, the entries are
    /// installed under the guards (missing payloads are fetched first),
    /// the ballot's fast set is activated and ballot-scoped votes reset.
    pub fn on_sync(&mut self, from: ReplicaId, decision: SyncDecision) -> Vec<Effect> {
        let promised = self.ballots.promised();
        if from == decision.ballot.leader
            && decision.ballot.compare_same_epoch(&promised) == Some(core::cmp::Ordering::Greater)
        {
            // Ahead of this replica's promise: kept, not refused. Only the
            // highest such Sync is worth keeping, since a promise for it
            // would void every lower one.
            let higher = self.early_sync.as_ref().is_none_or(|(_, kept)| {
                decision.ballot.compare_same_epoch(&kept.ballot)
                    == Some(core::cmp::Ordering::Greater)
            });
            if higher {
                self.early_sync = Some((from, decision));
            }
            return Vec::new();
        }
        if decision.ballot != promised || from != decision.ballot.leader {
            self.rejections.push(FollowerRejection::SyncRejected {
                ballot: decision.ballot,
                promised,
            });
            return Vec::new();
        }
        let already_synced = self.ballots.synced() == decision.ballot;
        if already_synced && self.config.quorum.ballot() == decision.ballot {
            // Duplicate Sync of the active ballot: converges without change.
            return Vec::new();
        }
        if !already_synced {
            // The synchronized ballot becomes a fact before anything of the
            // new ballot happens: activating first would let adoption rows
            // and acknowledgements of the new ballot become durable while
            // the synchronized ballot exists only in memory, so a crash in
            // that window would recover the old source state next to new
            // ballot votes.
            let record = match self.ballots.mark_synced(decision.ballot) {
                Ok(record) => record,
                Err(rejection) => {
                    self.rejections
                        .push(FollowerRejection::SyncRegression(rejection));
                    return Vec::new();
                }
            };
            let barrier = self.alloc.as_mut().expect("booted").allocate();
            // The selection goes down with the marker, in the one batch.
            // A marker without it would let this replica restart claiming
            // it had synchronized ballot B while its command ledger still
            // held the old state, and recovery selection treats the
            // highest synchronized ballot as the authoritative source of
            // accepted state. Either both rows are durable or neither is,
            // and a restart resumes the installation from the row.
            let updates = alloc::vec![
                sync_update(
                    self.config.identity.epoch,
                    &SyncRecordV1 {
                        decision: decision.clone(),
                    },
                )
                .expect("bounded"),
                promise_update(self.config.identity.epoch, &record).expect("bounded"),
            ];
            self.sync_barrier = Some((barrier, decision));
            return alloc::vec![Effect::Persist(PersistBatch {
                barrier,
                base: None,
                updates,
            })];
        }
        // The promise row already records this synchronized ballot (a
        // recovered replica, or the row just became durable): activating
        // and installing are idempotent.
        self.activate(decision)
    }

    /// Activate `decision`'s ballot: its fast set, its entries, and the
    /// messages held while the cut was open. Called only once the
    /// synchronized-ballot row is durable.
    fn activate(&mut self, decision: SyncDecision) -> Vec<Effect> {
        let mut effects = Vec::new();
        // The replica that selected this Sync leads the new ballot from
        // here: the role changes only once the ballot is this replica's.
        if decision.ballot.leader == self.config.identity.replica && self.campaign.is_some() {
            self.won = Some(decision.clone());
        }
        self.config.quorum = BallotConfiguration::c2_default(
            self.config.identity.epoch,
            decision.ballot,
            self.config.identity.voters.clone(),
        )
        .expect("valid ballot");
        self.votes.clear();
        self.held.clear();
        self.adopted.clear();
        for (c, e) in &decision.entries {
            if self.table.phase_of(c).is_none() {
                let _ = self.table.expect(*c);
            }
            self.sync_pending.insert(*c, e.clone());
        }
        effects.extend(self.advance_sync());
        effects.extend(self.replay_awaiting());
        effects
    }

    /// Install every Sync entry whose payload is known and whose
    /// dependencies are at least ACCEPT; repeat while progress is made.
    fn advance_sync(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();
        loop {
            let ready: Vec<CommandId> = self
                .sync_pending
                .iter()
                .filter(|(c, e)| {
                    self.table.phase_of(c).is_some()
                        && crate::phase::guard_accept(&e.deps, |d| self.table.phase_of(d)).is_ok()
                })
                .map(|(c, _)| *c)
                .collect();
            if ready.is_empty() {
                self.learn();
                return effects;
            }
            for command in ready {
                let entry = self.sync_pending.remove(&command).expect("ready");
                if self.table.record(&command).is_none() {
                    // Executed here and already retired: `phase_of` answers
                    // EXECUTED from the tombstone, and there is no record
                    // left to install the selection over or to write a row
                    // for. Its place in every key's order is covered by the
                    // log's prefix digest, as for any retired command, and
                    // realigning a log to it would leave an anchor that no
                    // append ever claims. A voter that led, retired what it
                    // executed, was killed and came back as a follower
                    // receives exactly these entries in the next leader's
                    // Sync, and installing them was a panic.
                    continue;
                }
                // Installing the selection means installing its whole
                // evidence, not only the combined digest: the per-key
                // logs are realigned to the selected order first, so a
                // later command derives its dependencies from the chosen
                // tail rather than this replica's pre-accept one. The
                // realignment is idempotent and monotone in the sequence
                // number, so it also runs for a record already committed
                // here, whose dependencies are then left alone.
                self.table
                    .record_leader_path(command, entry.seqnum, &entry.paths);
                // The selected order replaces a local one that is merely
                // accepted: an ACCEPT record from a lower synchronized
                // ballot may legally disagree with the chosen entry, and
                // keeping it would execute against another dependency
                // graph than the replicas that installed the selection.
                // Committed and executed records keep their dependencies.
                if self.table.phase_of(&command) < Some(Phase::Commit)
                    && self
                        .table
                        .adopt(command, entry.deps.clone(), Some(&entry.paths), entry.path)
                        .is_err()
                {
                    continue;
                }
                if entry.phase == Phase::Commit
                    && self.table.phase_of(&command) < Some(Phase::Commit)
                {
                    let _ = self.table.commit(command);
                }
                let record = self.table.record(&command).expect("installed").clone();
                let barrier = self.alloc.as_mut().expect("booted").allocate();
                effects.push(Effect::Persist(PersistBatch {
                    barrier,
                    base: None,
                    updates: alloc::vec![
                        dependency_update(self.config.identity.epoch, &command, &record)
                            .expect("bounded")
                    ],
                }));
                self.pending.insert(barrier, Pending::Adoption(command));
                self.ledger.stage(barrier, command, record);
            }
        }
    }

    /// The next command to execute through the materializer, if any.
    pub fn next_executable(&self) -> Option<CommandId> {
        self.learner
            .next_executable(&self.table, |c| self.adopted.get(c).map(|(s, _)| *s))
    }

    /// The durable ledger (journal-durable records only).
    pub const fn ledger(&self) -> &DurableLedger {
        &self.ledger
    }

    /// The recovery report for `ballot` from durable state at this cut.
    /// The barriers the report cut must wait for.
    ///
    /// The Sync persistence barrier belongs in it: the report states the
    /// synchronized ballot, which that batch is what makes durable, so a
    /// report published before it could claim a ballot this replica
    /// would not have after a restart.
    fn outstanding_for_cut(&self) -> Vec<BarrierId> {
        self.pending
            .keys()
            .copied()
            .chain(self.sync_barrier.as_ref().map(|(b, _)| *b))
            .collect()
    }

    /// This replica's recovery report for `ballot`: its durable ledger,
    /// plus any Sync selection it accepted durably and has not finished
    /// installing.
    pub fn report(&self, ballot: Ballot) -> RecoveryReport {
        let mut report =
            self.ledger
                .report(self.config.identity.replica, ballot, self.ballots.synced());
        // A Sync this replica accepted durably but has not finished
        // installing is part of what it knows, and the report is taken at
        // the synchronized ballot that Sync established. Reporting that
        // ballot while omitting its selected commands said this replica
        // had authoritative state for the ballot and that those commands
        // were never accepted: a candidate reading it as the source could
        // drop a selected command entirely. The entries are reported for
        // what they are — selected, with their order, payload still
        // outstanding — so the selection preserves them.
        let known: BTreeSet<CommandId> = report.entries.iter().map(|e| e.command).collect();
        for (command, entry) in &self.sync_pending {
            if known.contains(command) {
                continue;
            }
            report.entries.push(ReportEntry {
                command: *command,
                phase: entry.phase,
                deps: entry.deps.clone(),
                path: entry.path,
                paths: entry.paths.clone(),
                seqnum: entry.seqnum,
                // The conflict keys come from the payload, which is
                // exactly what has not arrived. The per-key digests the
                // selection installed name the keys the selected order
                // was recorded against, which is what a reader of this
                // entry can rely on.
                keys: entry.paths.iter().map(|(k, _)| k.clone()).collect(),
                payload_present: false,
            });
        }
        // History is left out: a command this replica executed and keeps
        // nothing else about. Every command a report names is one the
        // candidate may need to install or fetch, and reporting the whole
        // history made a report, and the Sync selected from reports, grow
        // without bound (task-d05).
        report.entries.retain(|e| !self.table.forgotten(&e.command));
        report_executed_as_committed(&mut report, &self.table);
        report.entries.sort_by_key(|e| e.command);
        report
    }

    /// Commands known by identity (a held proposal) without a payload.
    pub fn missing_payloads(&self) -> Vec<CommandId> {
        // Held by identity and not by content. A command this replica
        // has a record for may still have no payload: a proposal that
        // arrived before its submission leaves a placeholder, and a
        // command the leader proposed out of its own scheduler has no
        // submission coming at all.
        let mut out: Vec<CommandId> = self
            .held
            .keys()
            .chain(self.sync_pending.keys())
            .chain(self.adopted.keys())
            .filter(|c| !self.payloads.contains_key(c))
            .copied()
            .chain(self.campaign_missing())
            .collect();
        out.sort();
        out.dedup();
        out
    }

    /// The commands a selection names that this replica holds nothing
    /// for: what it needs before the selection can be bound.
    fn selection_missing(table: &CommandTable, decision: &SyncDecision) -> Vec<CommandId> {
        decision
            .entries
            .keys()
            .chain(decision.reproposed.iter())
            .filter(|c| table.phase_of(c).is_none())
            .copied()
            .collect()
    }

    /// A dependency of a selected entry that the selection does not carry
    /// and this replica has not committed (task-d05).
    ///
    /// A reporter that adopted an entry held its dependencies at ACCEPT
    /// or beyond, so a dependency no majority report names is one every
    /// such reporter executed and forgot. A replica without it committed
    /// cannot get it from this selection, and cannot execute past it.
    fn selection_gap(table: &CommandTable, decision: &SyncDecision) -> Option<CommandId> {
        decision
            .entries
            .values()
            .flat_map(|e| e.deps.iter())
            .find(|d| {
                !decision.entries.contains_key(d)
                    && !decision.reproposed.contains(d)
                    && table.phase_of(d) < Some(Phase::Commit)
            })
            .copied()
    }

    /// What this replica's own campaign is waiting for before it can bind
    /// its selection. Counted as missing like any other payload, so the
    /// runtime's paced asks cover it: an ask its peers could not answer
    /// yet is asked again rather than waited on for ever.
    fn campaign_missing(&self) -> Vec<CommandId> {
        match self.campaign.as_ref() {
            Some(c) if c.binding().is_none() && !c.is_durable() => c
                .decision()
                .map(|d| Self::selection_missing(&self.table, d))
                .unwrap_or_default(),
            _ => Vec::new(),
        }
    }

    /// The next bounded batch of missing payloads to ask for.
    ///
    /// Bounded because the ask repeats on a timer and the answer travels
    /// on a shared, bounded lane; rotating because a bound that always
    /// took the same prefix would leave the rest of the set unasked for
    /// ever. See [`MAX_PAYLOAD_TRANSFER`].
    fn payload_batch(&mut self, mut missing: Vec<CommandId>) -> Vec<CommandId> {
        if missing.len() <= MAX_PAYLOAD_TRANSFER {
            return missing;
        }
        let start = self.payload_cursor % missing.len();
        self.payload_cursor = start.wrapping_add(MAX_PAYLOAD_TRANSFER);
        missing.rotate_left(start);
        missing.truncate(MAX_PAYLOAD_TRANSFER);
        missing
    }

    /// Ask `from` for the payloads this replica lacks; the request has no
    /// durable prerequisite and is released at once.
    ///
    /// A candidate whose selection is waiting for payloads asks for
    /// those, and asks the voters that promised it instead of `from`:
    /// they reported the commands, so they hold them durably, while
    /// `from` -- the leader of the ballot this replica follows -- is
    /// usually the voter whose loss started the campaign.
    pub fn request_payloads(&mut self, from: ReplicaId) -> Vec<Effect> {
        let Some(boot) = self.boot else {
            return Vec::new();
        };
        let me = self.config.identity.replica;
        let campaign_missing = self.campaign_missing();
        let sources: Vec<ReplicaId> = match self.campaign.as_mut() {
            Some(c) if !campaign_missing.is_empty() => {
                let voters: Vec<ReplicaId> =
                    c.promised().iter().filter(|v| **v != me).copied().collect();
                c.mark_payloads_requested(&voters);
                voters
            }
            _ => alloc::vec![from],
        };
        let commands = if campaign_missing.is_empty() {
            let missing = self.missing_payloads();
            self.payload_batch(missing)
        } else {
            self.payload_batch(campaign_missing)
        };
        if commands.is_empty() || sources.is_empty() {
            return Vec::new();
        }
        self.payloads_asked = commands.iter().copied().collect();
        // Payload transfer is not a voting transition: it is published
        // under the promised ballot so a promise for a higher ballot never
        // fences it (the candidate needs payloads to recover).
        let context = self
            .ballots
            .context(boot, self.ballots.promised(), LocalJournalSeq::ZERO);
        let frame = ProtocolMessage::PayloadRequest { commands }.encode();
        if let Some(outbox) = self.outbox.as_mut() {
            for to in sources {
                outbox.publish(PendingSend {
                    context,
                    requires: Vec::new(),
                    to: PeerId {
                        replica: to,
                        incarnation: ReplicaIncarnation::ZERO,
                    },
                    frame: frame.clone(),
                });
            }
        }
        self.release()
    }

    /// How many payload transfers a peer has answered this replica with,
    /// counting only answers to the outstanding ask, each once.
    pub const fn payloads_answered(&self) -> u64 {
        self.payloads_answered
    }

    /// The durable payload of an initialized command.
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
        self.forget_history();
        Ok(alloc::vec![Effect::Established(result)])
    }

    /// Drop the durable records and payloads of commands this replica
    /// keeps nothing else about ([`CommandTable::forgotten`]), once there
    /// are enough of them to be worth a pass (task-d05).
    ///
    /// Neither is reported or served for such a command any more, so
    /// keeping them made the memory of a long-running voter grow with its
    /// history. The sweep runs when the ledger has outgrown a multiple of
    /// the table, so it is amortized over the growth that made it
    /// necessary.
    fn forget_history(&mut self) {
        if self.ledger.len() <= self.config.capacity.saturating_mul(HISTORY_SWEEP) {
            return;
        }
        self.sweep_history();
    }

    /// The sweep itself, whatever the ledger's size.
    fn sweep_history(&mut self) {
        let table = &self.table;
        self.ledger.retain(|c| !table.forgotten(c));
        self.payloads.retain(|c, _| !table.forgotten(c));
        self.served_payloads.retain(|c| !table.forgotten(c));
        self.votes.retain(|c, _| !table.forgotten(c));
        // The adoption order of a forgotten command decides nothing more,
        // and kept without its payload it would read as a payload still
        // missing.
        self.adopted.retain(|c, _| !table.forgotten(c));
    }

    fn learn(&mut self) {
        self.learner.commit_learned(&mut self.table, &self.votes);
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

    /// A fresh follower with nothing durable.
    pub fn new(config: FollowerConfig) -> Self {
        Self::recover(
            config,
            None,
            core::iter::empty(),
            core::iter::empty(),
            ExecutionPosition::ZERO,
        )
    }

    /// The command table.
    pub const fn table(&self) -> &CommandTable {
        &self.table
    }

    /// The promise state.
    pub const fn ballots(&self) -> &BallotState {
        &self.ballots
    }

    /// Proposals held for a payload or lagging dependencies.
    pub fn held(&self) -> &BTreeMap<CommandId, HeldProposal> {
        &self.held
    }

    /// Collected acknowledgements for a command.
    pub fn votes(&self, command: &CommandId) -> Option<&VoteSet> {
        self.votes.get(command)
    }

    /// Take the rejections recorded since the last call.
    pub fn take_rejections(&mut self) -> Vec<FollowerRejection> {
        core::mem::take(&mut self.rejections)
    }

    /// Sends still waiting for durability.
    pub fn pending_sends(&self) -> usize {
        self.outbox.as_ref().map_or(0, |o| o.pending().len())
    }

    /// Whether this follower may still vote in the configured ballot: it
    /// votes in this epoch at all, has promised exactly that ballot, and
    /// no higher promise is in flight. A promise for a higher ballot is
    /// the recovery cut: nothing of the old ballot may be adopted or
    /// acknowledged after it starts, whichever row becomes durable first.
    /// A seal is the same cut for every ballot of the configuration, so a
    /// durable seal, or a seal row in flight, ends voting too (task-55).
    fn may_vote(&self) -> bool {
        !self.ballots.is_fenced()
            && self.config.identity.role == ReplicaRole::Voter
            && self.ballots.promised() == self.config.quorum.ballot()
            && self.ballots.in_flight().is_none()
    }

    /// Whether this replica may send fast acknowledgements in this ballot.
    fn in_fast_set(&self) -> bool {
        self.config
            .quorum
            .fast_eligible(&self.config.identity.replica)
    }

    /// Publish an acknowledgement that waits for `barrier` and for every
    /// batch still outstanding: the payload batch of this command and the
    /// acceptance batches of its prerequisites are all part of what the
    /// acknowledgement claims, so none of them may still be volatile when
    /// it leaves. (The outbox drops a send whose barrier fails.)
    fn publish_to_voters_and_frontend(
        &mut self,
        command: CommandId,
        barrier: BarrierId,
        message: ProtocolMessage,
    ) {
        let Some(boot) = self.boot else {
            return;
        };
        let ballot = self.config.quorum.ballot();
        let context = self.ballots.context(boot, ballot, LocalJournalSeq::ZERO);
        let mut requires: Vec<BarrierId> = self.pending.keys().copied().collect();
        if !requires.contains(&barrier) {
            requires.push(barrier);
        }
        let frame = message.encode();
        let outbox = self.outbox.as_mut().expect("booted");
        for voter in &self.config.identity.voters {
            if *voter == self.config.identity.replica {
                continue;
            }
            outbox.publish(PendingSend {
                context,
                requires: requires.clone(),
                to: PeerId {
                    replica: *voter,
                    incarnation: ReplicaIncarnation::ZERO,
                },
                frame: frame.clone(),
            });
        }
        outbox.publish(PendingSend {
            context,
            requires: requires.clone(),
            to: self.config.frontend,
            frame: frame.clone(),
        });
        // The frontend's copy is the one a submission can ask for again:
        // it is the one that may have been held and let go before the
        // submitter was known (task-c02).
        self.replay.retain(
            command,
            crate::replay::RetainedEvidence {
                barrier,
                ballot,
                context,
                requires,
                frame,
            },
            &self.table,
        );
    }

    /// Offer the submitter the evidence this replica already published
    /// for `command`, because a submission naming it arrived again.
    ///
    /// The ordinary way a submission arrives after this replica has
    /// acknowledged the command: the acknowledgement went to a frontend
    /// that did not yet know which collector had asked, and that frontend
    /// has stopped holding it. The collector is one voter short; the
    /// command, ordered and perhaps executed, is unaffected. So the same
    /// acknowledgement -- the bytes that were published, under the
    /// context they were published in, requiring the barriers they
    /// required -- is published again, to the frontend only, through the
    /// same outbox. The boot fence, the durable prerequisites and the
    /// promise are checked at release exactly as they were the first
    /// time, and the release is driven here so there is no gap between
    /// the publication and the check.
    fn repair_evidence(&mut self, command: CommandId) -> Vec<Effect> {
        let why = if !self.may_vote() {
            crate::replay::ReplayRefusal::Fenced
        } else {
            let Some(outbox) = self.outbox.as_mut() else {
                self.rejections.push(FollowerRejection::ReplayRefused {
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
            .push(FollowerRejection::ReplayRefused { command, why });
        Vec::new()
    }

    /// How often `command`'s evidence has been published again this boot
    /// (diagnostic).
    pub fn evidence_repairs(&self, command: &CommandId) -> u32 {
        self.replay.repairs_of(command)
    }

    fn on_admitted(&mut self, frame: &[u8], admission: AdmissionFacts) -> Vec<Effect> {
        if self.boot.is_none() {
            return Vec::new();
        }
        if let Some(transition) = self.ballots.seal_held() {
            self.rejections
                .push(FollowerRejection::Sealed { transition });
            return Vec::new();
        }
        if !self.may_vote() {
            self.rejections.push(FollowerRejection::FencedByPromise {
                promised: self.ballots.promised(),
            });
            return Vec::new();
        }
        let request = match decode_stream(frame).as_deref() {
            Ok([MessageV1::Request(r)]) => r.clone(),
            _ => {
                self.rejections.push(FollowerRejection::MalformedRequest);
                return Vec::new();
            }
        };
        self.on_request(
            request.retry_key,
            request.logical.as_slice().to_vec(),
            Some(admission),
            request.ack_through,
        )
    }

    /// A canonical request reached this replica (from the frontend, or as
    /// a transferred payload): initialize, persist, vote.
    fn on_request(
        &mut self,
        retry_key: RetryKey,
        logical: Vec<u8>,
        admission: Option<AdmissionFacts>,
        ack_through: u64,
    ) -> Vec<Effect> {
        let Ok((request, rest)) =
            postcard::take_from_bytes::<coord_types::logical_v1::LogicalRequest>(&logical)
        else {
            self.rejections.push(FollowerRejection::MalformedRequest);
            return Vec::new();
        };
        if !rest.is_empty() {
            self.rejections.push(FollowerRejection::MalformedRequest);
            return Vec::new();
        }
        let Ok(command) = CommandId::derive(&retry_key, &request) else {
            self.rejections.push(FollowerRejection::MalformedRequest);
            return Vec::new();
        };
        let payload = PayloadRecordV1 {
            retry_key,
            logical,
            admission,
            ack_through,
        };
        match self.bindings.get(&retry_key) {
            Some(bound) if *bound != command => {
                self.rejections
                    .push(FollowerRejection::RequestIdentityConflict {
                        retry_key,
                        bound: *bound,
                    });
                return Vec::new();
            }
            // The same identity again. Whether it is the same *request*
            // is decided by everything that travelled beside the identity
            // -- the attested admission and the acknowledged floor --
            // against what this replica accepted the command under. Only
            // an exact match is a duplicate, and a duplicate is not
            // nothing: the submitter presenting it may never have
            // received what this replica already said about the command.
            Some(_) => {
                let accepted = self
                    .payloads
                    .get(&command)
                    .map(PayloadRecordV1::admission_digest);
                return match accepted {
                    Some(accepted) if accepted == payload.admission_digest() => {
                        self.rejections.push(FollowerRejection::Duplicate(command));
                        self.repair_evidence(command)
                    }
                    Some(accepted) => {
                        self.rejections
                            .push(FollowerRejection::RequestFactsConflict { command, accepted });
                        Vec::new()
                    }
                    // Bound but no payload row to compare against: nothing
                    // here can vouch for the facts, so nothing is replayed
                    // and nothing is re-initialized over the binding.
                    None => {
                        self.rejections.push(FollowerRejection::Duplicate(command));
                        Vec::new()
                    }
                };
            }
            None => {}
        }
        // Atomic initialization; the placeholder of an early proposal (if
        // any) becomes the record in this same transition. What is bound
        // beside the identity is the admission: a second presentation of
        // this command under different attested facts conflicts here
        // instead of quietly replacing what this replica accepted.
        let init = match self.table.initialize(
            command,
            payload.admission_digest(),
            alloc::vec![crate::leader::CONSERVATIVE_KEY.to_vec()],
        ) {
            Ok(i) => i,
            Err(InitError::Backpressure) => {
                // No room for this command -- and returning here would
                // also skip `advance_pending`, which is what adopts the
                // records whose turn has come and so what makes room. A
                // replica that stopped adopting while its table was full
                // would keep it full, and the submitter's retry would
                // meet the same refusal for ever.
                self.rejections.push(FollowerRejection::Backpressure);
                return self.advance_pending();
            }
            // Initialized, but the retry key was not bound: the record
            // exists without a payload row this replica can name (an
            // earlier boot's rows, or a transferred payload whose binding
            // was not kept). The table compared the digest, so
            // `AlreadyInitialized` is an exact duplicate and
            // `PayloadConflict` is not.
            Err(InitError::AlreadyInitialized) => {
                self.bindings.insert(retry_key, command);
                self.rejections.push(FollowerRejection::Duplicate(command));
                return self.repair_evidence(command);
            }
            Err(InitError::PayloadConflict) => {
                self.rejections
                    .push(FollowerRejection::PayloadConflict(command));
                return Vec::new();
            }
        };
        self.bindings.insert(retry_key, command);
        self.payloads.insert(command, payload.clone());
        let epoch = self.config.identity.epoch;
        let record = self
            .table
            .record(&command)
            .expect("just initialized")
            .clone();
        let barrier = self.alloc.as_mut().expect("booted").allocate();
        let updates = alloc::vec![
            payload_update(&command, &payload).expect("bounded"),
            dependency_update(epoch, &command, &record).expect("bounded"),
        ];
        let mut effects = alloc::vec![Effect::Persist(PersistBatch {
            barrier,
            base: None,
            updates,
        })];
        self.pending.insert(barrier, Pending::Vote(command));
        self.ledger.stage(barrier, command, record);
        self.durable_payloads.insert(barrier, command);
        // Own vote joins whatever evidence (a held proposal) already
        // arrived for this command; it never replaces it.
        self.votes
            .entry(command)
            .or_insert_with(|| VoteSet::new(self.config.quorum.clone(), command));
        if self.in_fast_set() {
            let ack = FastAck {
                replica: self.config.identity.replica,
                ballot: self.config.quorum.ballot(),
                command,
                deps: init.deps.clone(),
                paths: init.paths.clone(),
                path: init.path,
                admission: init.payload,
                seqnum: None,
            };
            // Counted once the payload and dependency rows are durable.
            self.deferred.insert(
                barrier,
                DeferredVote {
                    command,
                    vote: Vote::Fast(ack.clone()),
                },
            );
            self.publish_to_voters_and_frontend(command, barrier, ProtocolMessage::FastAck(ack));
        }
        // A proposal (or Sync entry) that arrived before the payload can now
        // be adopted (once its dependencies allow it).
        effects.extend(self.advance_sync());
        effects.extend(self.advance_pending());
        effects
    }

    fn on_proposal(&mut self, from: ReplicaId, proposal: FastAck) -> Vec<Effect> {
        if let Some(transition) = self.ballots.seal_held() {
            self.rejections
                .push(FollowerRejection::Sealed { transition });
            return Vec::new();
        }
        if !self.may_vote() {
            self.rejections.push(FollowerRejection::FencedByPromise {
                promised: self.ballots.promised(),
            });
            return Vec::new();
        }
        if from != self.config.quorum.leader()
            || proposal.replica != from
            || proposal.ballot != self.config.quorum.ballot()
            || proposal.seqnum.is_none()
        {
            self.rejections.push(FollowerRejection::ForeignProposal);
            return Vec::new();
        }
        let command = proposal.command;
        if self.held.contains_key(&command) {
            // Duplicate proposal of one still held: nothing acknowledged yet.
            return Vec::new();
        }
        if self.adopted.contains_key(&command) {
            // Duplicate proposal: converges without change -- except that
            // the leader sends a proposal again only when it has no vote
            // from this replica for it (task-d07). An acknowledgement that
            // was lost on the way is published to the leader again, as it
            // was published the first time.
            if self.kept_evidence(&command) {
                return self.reacknowledge(command, from);
            }
            // Nothing kept to publish again: adopted before a restart --
            // the evidence store is this boot's, so the acknowledgement,
            // if it was ever sent, went with it -- or kept past the
            // store's bound. Without an answer the leader re-sent it for
            // ever and, with a voter down, never learned it.
            //
            // A command decided here is answered from its durable record,
            // to the leader alone (below). One only accepted is taken as
            // the first proposal was: adopted again, which writes the
            // same row and publishes the acknowledgement once it is
            // durable.
            if self.table.phase_of(&command) >= Some(Phase::Commit) {
                return self.acknowledge_decided(&proposal, from);
            }
            self.adopted.remove(&command);
        }
        if self.table.record(&command).is_none()
            && self.table.phase_of(&command) == Some(Phase::Executed)
        {
            // Executed here and retired: there is no record to adopt the
            // order into and nothing left to decide. Held, it would wait
            // for ever for an adoption that cannot happen (task-d05).
            return self.acknowledge_decided(&proposal, from);
        }
        // A proposal asserts something about a command. Where this
        // replica already holds that command's payload and accepted it
        // under other attested facts, the leader is proposing a
        // different command under a shared identity: adopting it would
        // execute facts this replica never admitted. Nothing is held,
        // nothing is counted, and the mismatch is reported rather than
        // resolved -- whichever of the two is the real command, this
        // replica cannot tell from the proposal.
        if let Some(accepted) = self.table.record(&command).and_then(|r| r.payload)
            && accepted != proposal.admission
        {
            self.rejections
                .push(FollowerRejection::AdmissionConflict { command, accepted });
            return Vec::new();
        }
        // The leader's order is recorded into the path logs as soon as it is
        // known (prototype `recordLeaderHash`); a missing payload only makes
        // a placeholder that nothing can see.
        self.table
            .record_leader_path(command, proposal.seqnum.unwrap_or(0), &proposal.paths);
        if self.table.phase_of(&command).is_none() && self.table.expect(command).is_err() {
            // No room for a placeholder. That is a reason to wait, and it
            // is emphatically not a reason to return: what this function
            // ends with is `advance_pending`, which adopts the records
            // whose turn has come -- and adoption is what lets them
            // execute, retire, and make the room that was missing. A
            // replica that stopped adopting the moment its table filled
            // could never empty it again, and nothing would prompt it to
            // try: the leader does not re-propose, and there is no
            // message to ask it for an order it already sent.
            //
            // So the refusal is recorded and the proposal is held rather
            // than dropped. Held, because the order it carries is the
            // only copy this replica is ever offered; dropped, the
            // command would sit at PRE-ACCEPT for ever once its payload
            // did arrive, and with a conservative key making the chain
            // total, so would everything ordered after it.
            self.rejections.push(FollowerRejection::Backpressure);
            if self.held.len() >= self.config.capacity.saturating_mul(HELD_PROPOSAL_SLACK) {
                return self.advance_pending();
            }
        }
        if let Some(set) = self.votes.get_mut(&command) {
            if let Err(e) = set.add(Vote::Fast(proposal.clone())) {
                self.rejections.push(FollowerRejection::Vote(e));
            }
        } else {
            let mut set = VoteSet::new(self.config.quorum.clone(), command);
            let _ = set.add(Vote::Fast(proposal.clone()));
            self.votes.insert(command, set);
        }
        self.held.insert(command, HeldProposal { proposal });
        self.advance_pending()
    }

    /// Answer a proposal for a command decided here -- committed or
    /// executed, retired or not -- with an adoption acknowledgement, to
    /// `leader` alone (task-d07).
    ///
    /// The leader sends a proposal again only while it lacks this
    /// replica's adoption, and a replica that decided the command may be
    /// what it lacks: it learned the command from the leader's proposal
    /// and its own acknowledgement, which then went missing, and a
    /// restart took the evidence it could have published again. Decided
    /// here, the command's dependencies are those of its durable record,
    /// and a commit does not change them. The acknowledgement goes only
    /// when the proposal carries those dependencies and the admission the
    /// record holds, so it claims nothing this replica did not adopt;
    /// without a durable record (none yet, or swept as history) nothing is
    /// sent. The record is durable, so the acknowledgement waits for
    /// nothing; and it goes to the leader only, since no submitter is
    /// waiting on it.
    fn acknowledge_decided(&mut self, proposal: &FastAck, leader: ReplicaId) -> Vec<Effect> {
        let Some(boot) = self.boot else {
            return Vec::new();
        };
        let Some(record) = self.ledger.record(&proposal.command) else {
            return Vec::new();
        };
        if !crate::vote::same_set(&record.deps, &proposal.deps)
            || record.payload != Some(proposal.admission)
        {
            return Vec::new();
        }
        let ballot = self.config.quorum.ballot();
        let ack = SlowAck {
            replica: self.config.identity.replica,
            ballot,
            command: proposal.command,
            admission: proposal.admission,
        };
        let context = self.ballots.context(boot, ballot, LocalJournalSeq::ZERO);
        let Some(outbox) = self.outbox.as_mut() else {
            return Vec::new();
        };
        outbox.publish(PendingSend {
            context,
            requires: Vec::new(),
            to: PeerId {
                replica: leader,
                incarnation: ReplicaIncarnation::ZERO,
            },
            frame: ProtocolMessage::SlowAck(ack).encode(),
        });
        self.release()
    }

    /// Whether this boot kept what this replica acknowledged for `command`
    /// in the current ballot.
    fn kept_evidence(&self, command: &CommandId) -> bool {
        let ballot = self.config.quorum.ballot();
        self.replay.kept(command).iter().any(|e| e.ballot == ballot)
    }

    /// Publish to `leader` again what this replica acknowledged for
    /// `command` in the current ballot.
    ///
    /// The same frames, under the context and barriers they were first
    /// published with, so the outbox judges them exactly as it did then.
    /// Nothing is recomputed or voted again. Bounded by the leader's
    /// pacing: it re-sends a proposal only while a vote is missing.
    fn reacknowledge(&mut self, command: CommandId, leader: ReplicaId) -> Vec<Effect> {
        let ballot = self.config.quorum.ballot();
        let sends: Vec<PendingSend> = self
            .replay
            .kept(&command)
            .iter()
            .filter(|e| e.ballot == ballot)
            .map(|e| PendingSend {
                context: e.context,
                requires: e.requires.clone(),
                to: PeerId {
                    replica: leader,
                    incarnation: ReplicaIncarnation::ZERO,
                },
                frame: e.frame.clone(),
            })
            .collect();
        if sends.is_empty() {
            return Vec::new();
        }
        let Some(outbox) = self.outbox.as_mut() else {
            return Vec::new();
        };
        for send in sends {
            outbox.publish(send);
        }
        self.release()
    }

    /// Adopt every held proposal whose payload is initialized and whose
    /// dependencies are all at least ACCEPT; repeat while progress is made.
    fn advance_pending(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();
        // A proposal held before the seal (its payload had not arrived)
        // stays held: adopting it now would acknowledge after the cut the
        // seal report was built over.
        if self.ballots.is_fenced() {
            self.learn();
            return effects;
        }
        loop {
            let ready: Vec<CommandId> = self
                .held
                .iter()
                .filter(|(c, h)| {
                    // Initialized, not merely known. A proposal that
                    // arrived before the payload leaves a placeholder,
                    // and a placeholder cannot be accepted: there is
                    // nothing to accept an order *for* yet. It stays
                    // held until the payload arrives -- from the
                    // submission, or from the leader that proposed it.
                    self.table.is_initialized(c)
                        && crate::phase::guard_accept(&h.proposal.deps, |d| self.table.phase_of(d))
                            .is_ok()
                })
                .map(|(c, _)| *c)
                .collect();
            if ready.is_empty() {
                self.learn();
                return effects;
            }
            for command in ready {
                let held = self.held.remove(&command).expect("ready");
                // A command already learned or executed (installed from a
                // Sync, or durable across a restart) keeps its phase: the
                // re-proposal only supplies the new ballot's order.
                if self.table.phase_of(&command) < Some(Phase::Commit)
                    && self
                        .table
                        .adopt(
                            command,
                            held.proposal.deps.clone(),
                            Some(&held.proposal.paths),
                            held.proposal.path,
                        )
                        .is_err()
                {
                    continue;
                }
                self.adopted
                    .insert(command, (held.proposal.seqnum.unwrap_or(u64::MAX), false));
                let epoch = self.config.identity.epoch;
                let record = self.table.record(&command).expect("adopted").clone();
                let admission = record.payload.unwrap_or_else(|| admission_digest(None, 0));
                let barrier = self.alloc.as_mut().expect("booted").allocate();
                effects.push(Effect::Persist(PersistBatch {
                    barrier,
                    base: None,
                    updates: alloc::vec![
                        dependency_update(epoch, &command, &record).expect("bounded")
                    ],
                }));
                self.pending.insert(barrier, Pending::Adoption(command));
                self.ledger.stage(barrier, command, record);
                let ack = SlowAck {
                    replica: self.config.identity.replica,
                    ballot: self.config.quorum.ballot(),
                    command,
                    admission,
                };
                // Counted once the adoption row is durable, like the
                // acknowledgement itself, which waits for the same batch.
                self.deferred.insert(
                    barrier,
                    DeferredVote {
                        command,
                        vote: Vote::Slow(ack.clone()),
                    },
                );
                self.publish_to_voters_and_frontend(
                    command,
                    barrier,
                    ProtocolMessage::SlowAck(ack),
                );
            }
        }
    }

    fn on_storage(&mut self, event: &StorageEvent) -> Vec<Effect> {
        let mut out = self.on_storage_event(event);
        // A Sync that arrived ahead of this replica's promise is installed
        // once the promise is this replica's: its row is durable, so the
        // promised ballot is the Sync's.
        let promised = self.ballots.promised();
        if let Some((leader, decision)) =
            self.early_sync.take_if(|(_, kept)| kept.ballot == promised)
        {
            out.extend(self.on_sync(leader, decision));
        }
        out
    }

    fn on_storage_event(&mut self, event: &StorageEvent) -> Vec<Effect> {
        if let Some(outbox) = self.outbox.as_mut() {
            outbox.observe(event);
        }
        self.ballots.on_storage(event);
        // The synchronized-ballot row carries its own barrier, which is not
        // one of the pending vote or adoption batches: the new ballot
        // becomes this replica's only when that row is a fact.
        if let Some(barrier) = event.barrier()
            && self
                .sync_barrier
                .as_ref()
                .is_some_and(|(b, _)| *b == barrier)
        {
            let (_, decision) = self.sync_barrier.take().expect("checked");
            let mut out = match event {
                StorageEvent::JournalDurable { .. } => {
                    // Unless a higher ballot was promised while the row was
                    // becoming durable: that cut supersedes this one and its
                    // leader recovers from the rows.
                    if self.ballots.promised() == decision.ballot {
                        self.activate(decision)
                    } else {
                        self.rejections
                            .push(FollowerRejection::SyncSuperseded(decision.ballot));
                        Vec::new()
                    }
                }
                StorageEvent::Failed { .. } => {
                    self.rejections
                        .push(FollowerRejection::SyncNotDurable(decision.ballot));
                    Vec::new()
                }
                _ => {
                    self.sync_barrier = Some((barrier, decision));
                    Vec::new()
                }
            };
            out.extend(self.release());
            return out;
        }
        if let Some(barrier) = event.barrier()
            && let Some(pending) = self.pending.get(&barrier).cloned()
        {
            match event {
                StorageEvent::JournalDurable { journal_seq, .. } => {
                    self.pending.remove(&barrier);
                    self.ledger.durable(barrier, *journal_seq);
                    if let Some(c) = self.durable_payloads.remove(&barrier) {
                        self.served_payloads.insert(c);
                    }
                    if let Pending::Adoption(c) = pending
                        && let Some(entry) = self.adopted.get_mut(&c)
                    {
                        entry.1 = true;
                    }
                    // The evidence this replica produced is a fact now.
                    if let Some(deferred) = self.deferred.remove(&barrier)
                        && let Some(set) = self.votes.get_mut(&deferred.command)
                    {
                        let _ = set.add(deferred.vote);
                    }
                }
                StorageEvent::Failed { .. } => {
                    self.pending.remove(&barrier);
                    // The batch never happened: its vote is not evidence.
                    self.deferred.remove(&barrier);
                    self.ledger.failed(barrier);
                    self.durable_payloads.remove(&barrier);
                    let command = match pending {
                        Pending::Vote(c) | Pending::Adoption(c) => c,
                    };
                    self.rejections
                        .push(FollowerRejection::BatchFailed(command));
                }
                _ => {}
            }
        }
        let mut out = Vec::new();
        if let (Some(barrier), StorageEvent::JournalDurable { .. }) = (event.barrier(), event)
            && let Some(c) = self.campaign.as_mut()
            && c.binding() == Some(barrier)
        {
            c.on_durable(barrier);
            out.extend(self.advance_campaign());
        }
        out.extend(self.deliver_due_report());
        self.learn();
        out.extend(self.release());
        out
    }

    fn release(&mut self) -> Vec<Effect> {
        let promised = self.ballots.promised();
        self.outbox
            .as_mut()
            .map_or_else(Vec::new, |o| o.release(&promised))
    }

    fn on_peer(&mut self, from: PeerId, frame: &[u8]) -> Vec<Effect> {
        let Ok(message) = ProtocolMessage::decode(frame) else {
            self.rejections
                .push(FollowerRejection::MalformedPeerMessage);
            return Vec::new();
        };
        match message {
            ProtocolMessage::NewLeader { ballot } => {
                let (Some(boot), Some(alloc)) = (self.boot, self.alloc.as_mut()) else {
                    return Vec::new();
                };
                let outstanding: Vec<BarrierId> = self
                    .pending
                    .keys()
                    .copied()
                    .chain(self.sync_barrier.as_ref().map(|(b, _)| *b))
                    .collect();
                match self
                    .ballots
                    .on_new_leader(from, ballot, boot, alloc, &outstanding)
                {
                    Ok(effects) => {
                        self.awaiting_sync.clear();
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
                        self.rejections.push(FollowerRejection::Promise(e));
                        Vec::new()
                    }
                }
            }
            // Sealing the old configuration (task-55). The row and the
            // report follow the promise's rule exactly: the report is
            // published requiring the row and every batch submitted
            // before the cut, so work this replica learned immediately
            // before sealing is inside it even if the report is
            // delayed.
            ProtocolMessage::SealRequest { transition } => {
                let outstanding = self.outstanding_for_cut();
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
                        self.rejections.push(FollowerRejection::Seal(e));
                        Vec::new()
                    }
                }
            }
            // A seal report is a coordinator's to count, never a
            // voter's to act on.
            ProtocolMessage::Sealed { .. } => Vec::new(),
            ProtocolMessage::Promise {
                ballot, replica, ..
            } => {
                if let Some(c) = self.campaign.as_mut()
                    && c.ballot() == ballot
                    && replica == from.replica
                {
                    c.promise(replica);
                }
                self.advance_campaign()
            }
            ProtocolMessage::ReportPage(page) => {
                if let Some(c) = self.campaign.as_mut()
                    && let Err(e) = c.page(page)
                {
                    self.rejections.push(FollowerRejection::Page(e));
                    return Vec::new();
                }
                self.advance_campaign()
            }
            ProtocolMessage::Sync(decision) => self.on_sync(from.replica, decision),
            ProtocolMessage::Proposal(_)
            | ProtocolMessage::FastAck(_)
            | ProtocolMessage::SlowAck(_)
                if self.stash_until_sync(&message) =>
            {
                self.awaiting_sync.push((from.replica, message));
                Vec::new()
            }
            ProtocolMessage::Proposal(p) => self.on_proposal(from.replica, p),
            ProtocolMessage::FastAck(ack) => self.collect(from.replica, Vote::Fast(ack)),
            ProtocolMessage::SlowAck(ack) => self.collect(from.replica, Vote::Slow(ack)),
            ProtocolMessage::PayloadRequest { commands } => {
                self.serve_payloads(from.replica, &commands)
            }
            ProtocolMessage::PayloadResponse { command, payload } => {
                let asked = self.payloads_asked.contains(&command);
                let mut out = self.on_payload(command, payload);
                // Counted only as an answer to the outstanding ask, and
                // once: see `payloads_answered`.
                if asked && self.payloads.contains_key(&command) {
                    self.payloads_asked.remove(&command);
                    self.payloads_answered = self.payloads_answered.saturating_add(1);
                }
                if self.campaign.is_some() {
                    // A campaign waiting for this payload can bind now.
                    out.extend(self.advance_campaign());
                }
                out
            }
            ProtocolMessage::LeaderReply { .. } => Vec::new(),
        }
    }

    /// Serve durable payloads to a peer; an undurable payload is not
    /// served (it is not yet a fact).
    fn serve_payloads(&mut self, to: ReplicaId, commands: &[CommandId]) -> Vec<Effect> {
        let Some(boot) = self.boot else {
            return Vec::new();
        };
        // Served under the promised ballot: see `request_payloads`.
        let context = self
            .ballots
            .context(boot, self.ballots.promised(), LocalJournalSeq::ZERO);
        let responses: Vec<ProtocolMessage> = commands
            .iter()
            .filter(|c| self.served_payloads.contains(c))
            .filter_map(|c| {
                self.payloads
                    .get(c)
                    .map(|p| ProtocolMessage::PayloadResponse {
                        command: *c,
                        payload: p.clone(),
                    })
            })
            // A peer cannot make this replica flood its own lane, even
            // by asking for more than the protocol's own bound.
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

    /// A transferred payload: it must rehash to the identity it claims;
    /// then it is treated exactly like an admitted request.
    fn on_payload(&mut self, command: CommandId, payload: PayloadRecordV1) -> Vec<Effect> {
        let request: Result<coord_types::logical_v1::LogicalRequest, _> =
            postcard::from_bytes(&payload.logical);
        let ok = request
            .ok()
            .and_then(|r| CommandId::derive(&payload.retry_key, &r).ok())
            .is_some_and(|id| id == command);
        if !ok {
            self.rejections
                .push(FollowerRejection::PayloadIdentityMismatch(command));
            return Vec::new();
        }
        if self.payloads.contains_key(&command) {
            return Vec::new();
        }
        // The admission travels with the payload, so a replica that
        // recovered a command from a peer executes it under the same
        // attested facts as the replica that accepted it first.
        //
        // A placeholder left by an early proposal becomes the record in
        // that same transition, which is what initialization has always
        // done. Nothing has been accepted over the placeholder -- a
        // placeholder cannot be accepted -- so there is no order here to
        // overwrite.
        if !self.table.is_initialized(&command) {
            return self.on_request(
                payload.retry_key,
                payload.logical,
                payload.admission,
                payload.ack_through,
            );
        }
        // A record already exists, so this replica heard the leader's
        // evidence before the payload and holds a placeholder -- or has
        // adopted the leader's order over it. Initializing now would
        // recompute dependencies this replica has already been told, and
        // would reset an adoption it has already acknowledged, so the
        // payload is bound beside the record rather than through it.
        //
        // This is the ordinary case for the service's own commands. A
        // caller's command reaches every voter as a submission, so a
        // voter that has one has its payload; a command a leader
        // proposes out of its own scheduler reaches the others as
        // evidence alone, and the payload has to follow.
        //
        // Written as well as remembered: a payload only in memory is one
        // a restart loses, and the command would then be unexecutable
        // with every later command waiting behind it.
        if self.boot.is_none() {
            return Vec::new();
        }
        self.bindings.insert(payload.retry_key, command);
        self.payloads.insert(command, payload.clone());
        let barrier = self.alloc.as_mut().expect("booted").allocate();
        self.durable_payloads.insert(barrier, command);
        alloc::vec![Effect::Persist(PersistBatch {
            barrier,
            base: None,
            updates: alloc::vec![payload_update(&command, &payload).expect("bounded")],
        })]
    }

    fn collect(&mut self, from: ReplicaId, vote: Vote) -> Vec<Effect> {
        if vote.replica() != from {
            self.rejections
                .push(FollowerRejection::Vote(VoteError::NotAVoter));
            return Vec::new();
        }
        let command = vote.command();
        let set = self
            .votes
            .entry(command)
            .or_insert_with(|| VoteSet::new(self.config.quorum.clone(), command));
        if let Err(e) = set.add(vote) {
            self.rejections.push(FollowerRejection::Vote(e));
        }
        self.learn();
        Vec::new()
    }
}

impl DeterministicMachine for Follower {
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
                // The authenticated sender at the exact incarnation the
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

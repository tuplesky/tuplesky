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
use crate::messages::ProtocolMessage;
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
    /// Already initialized; nothing changed.
    Duplicate(CommandId),
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
}

/// A report owed once every batch submitted before the cut is durable.
/// A recovery report this replica owes a candidate.
type ReportDue = crate::role::PendingReport;

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
    ledger: DurableLedger,
    learner: Learner,
    campaign: Option<Campaign>,
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
    rejections: Vec<FollowerRejection>,
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
            payloads,
            durable_payloads: BTreeMap::new(),
            ledger,
            learner: Learner::new(executed_through),
            campaign: None,
            report_due: None,
            sync_pending: BTreeMap::new(),
            won: None,
            awaiting_sync: Vec::new(),
            rejections: Vec::new(),
            resumed,
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
    pub fn restore_execution(
        mut self,
        through: ExecutionPosition,
        executed: impl IntoIterator<Item = CommandId>,
    ) -> Self {
        for c in executed {
            self.table.restore_executed(&c);
            self.adopted.entry(c).or_insert((u64::MAX, true));
        }
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
            ledger: state.ledger,
            learner: state.learner,
            deferred: BTreeMap::new(),
            campaign: None,
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
            // A selected command this replica never stored (it was down
            // while the request was admitted) needs its payload before the
            // result is bound: the new leader re-proposes from payloads,
            // never from identities alone. The reporting voters hold them
            // durably (they reported the command).
            let missing: Vec<CommandId> = decision
                .entries
                .keys()
                .chain(decision.reproposed.iter())
                .filter(|c| self.table.phase_of(c).is_none())
                .copied()
                .collect();
            if !missing.is_empty() {
                // Ask every promised voter that has not been asked yet, so a
                // voter promising after the selection is still asked: the
                // ones asked first may be gone while a live quorum holds the
                // payload.
                let me = self.config.identity.replica;
                let voters = campaign.payload_requests_due(me);
                if voters.is_empty() {
                    return Vec::new();
                }
                campaign.mark_payloads_requested(&voters);
                let context = self
                    .ballots
                    .context(boot, decision.ballot, LocalJournalSeq::ZERO);
                let outbox = self.outbox.as_mut().expect("booted");
                for voter in voters {
                    outbox.publish(PendingSend {
                        context,
                        requires: Vec::new(),
                        to: PeerId {
                            replica: voter,
                            incarnation: ReplicaIncarnation::ZERO,
                        },
                        frame: ProtocolMessage::PayloadRequest {
                            commands: missing.clone(),
                        }
                        .encode(),
                    });
                }
                return self.release();
            }
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
        report.entries.sort_by_key(|e| e.command);
        report
    }

    /// Commands known by identity (a held proposal) without a payload.
    pub fn missing_payloads(&self) -> Vec<CommandId> {
        let mut out: Vec<CommandId> = self
            .held
            .keys()
            .chain(self.sync_pending.keys())
            .filter(|c| self.table.phase_of(c).is_none())
            .copied()
            .collect();
        out.sort();
        out.dedup();
        out
    }

    /// Ask `from` for the payloads this replica lacks; the request has no
    /// durable prerequisite and is released at once.
    pub fn request_payloads(&mut self, from: ReplicaId) -> Vec<Effect> {
        let commands = self.missing_payloads();
        let Some(boot) = self.boot else {
            return Vec::new();
        };
        if commands.is_empty() {
            return Vec::new();
        }
        // Payload transfer is not a voting transition: it is published
        // under the promised ballot so a promise for a higher ballot never
        // fences it (the candidate needs payloads to recover).
        let context = self
            .ballots
            .context(boot, self.ballots.promised(), LocalJournalSeq::ZERO);
        if let Some(outbox) = self.outbox.as_mut() {
            outbox.publish(PendingSend {
                context,
                requires: Vec::new(),
                to: PeerId {
                    replica: from,
                    incarnation: ReplicaIncarnation::ZERO,
                },
                frame: ProtocolMessage::PayloadRequest { commands }.encode(),
            });
        }
        self.release()
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
        Ok(alloc::vec![Effect::Established(result)])
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
    fn may_vote(&self) -> bool {
        self.config.identity.role == ReplicaRole::Voter
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
    fn publish_to_voters_and_frontend(&mut self, barrier: BarrierId, message: ProtocolMessage) {
        let Some(boot) = self.boot else {
            return;
        };
        let context =
            self.ballots
                .context(boot, self.config.quorum.ballot(), LocalJournalSeq::ZERO);
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
            requires,
            to: self.config.frontend,
            frame,
        });
    }

    fn on_admitted(&mut self, frame: &[u8], admission: AdmissionFacts) -> Vec<Effect> {
        if self.boot.is_none() {
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
        )
    }

    /// A canonical request reached this replica (from the frontend, or as
    /// a transferred payload): initialize, persist, vote.
    fn on_request(
        &mut self,
        retry_key: RetryKey,
        logical: Vec<u8>,
        admission: Option<AdmissionFacts>,
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
        match self.bindings.get(&retry_key) {
            Some(bound) if *bound != command => {
                self.rejections
                    .push(FollowerRejection::RequestIdentityConflict {
                        retry_key,
                        bound: *bound,
                    });
                return Vec::new();
            }
            Some(_) => {
                self.rejections.push(FollowerRejection::Duplicate(command));
                return Vec::new();
            }
            None => {}
        }
        let payload = PayloadRecordV1 {
            retry_key,
            logical,
            admission,
        };
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
                self.rejections.push(FollowerRejection::Backpressure);
                return Vec::new();
            }
            Err(InitError::AlreadyInitialized | InitError::PayloadConflict) => {
                self.rejections.push(FollowerRejection::Duplicate(command));
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
            self.publish_to_voters_and_frontend(barrier, ProtocolMessage::FastAck(ack));
        }
        // A proposal (or Sync entry) that arrived before the payload can now
        // be adopted (once its dependencies allow it).
        effects.extend(self.advance_sync());
        effects.extend(self.advance_pending());
        effects
    }

    fn on_proposal(&mut self, from: ReplicaId, proposal: FastAck) -> Vec<Effect> {
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
        if self.adopted.contains_key(&command) || self.held.contains_key(&command) {
            // Duplicate proposal: converges without change.
            return Vec::new();
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
            self.rejections.push(FollowerRejection::Backpressure);
            return Vec::new();
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

    /// Adopt every held proposal whose payload is initialized and whose
    /// dependencies are all at least ACCEPT; repeat while progress is made.
    fn advance_pending(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();
        loop {
            let ready: Vec<CommandId> = self
                .held
                .iter()
                .filter(|(c, h)| {
                    self.table.phase_of(c).is_some()
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
                let admission = record.payload.unwrap_or_else(|| admission_digest(None));
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
                self.publish_to_voters_and_frontend(barrier, ProtocolMessage::SlowAck(ack));
            }
        }
    }

    fn on_storage(&mut self, event: &StorageEvent) -> Vec<Effect> {
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
                let mut out = self.on_payload(command, payload);
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
        if self.table.phase_of(&command).is_some() {
            return Vec::new();
        }
        // The admission travels with the payload, so a replica that
        // recovered a command from a peer executes it under the same
        // attested facts as the replica that accepted it first.
        self.on_request(payload.retry_key, payload.logical, payload.admission)
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

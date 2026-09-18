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

use alloc::collections::BTreeMap;
use alloc::vec::Vec;

use coord_core::effect::{BarrierId, BootId, Effect, PeerId, PersistBatch};
use coord_core::event::{Event, StorageEvent};
use coord_core::machine::DeterministicMachine;
use coord_core::outbox::{BarrierAllocator, Outbox, PendingSend};
use coord_types::ids::{Ballot, ExecutionPosition, LocalJournalSeq, ReplicaId, ReplicaIncarnation};
use coord_types::wire_v1::{MessageV1, decode_stream};
use coord_types::{CommandId, RetryKey};

use crate::ballot::{BallotState, ConfigurationIdentity, PromiseRejection};
use crate::campaign::Campaign;
use crate::commands::{CommandRecord, CommandTable, InitError};
use crate::learner::{AppliedOutcome, LearnError, Learner};
use crate::messages::ProtocolMessage;
use crate::phase::Phase;
use crate::quorum::BallotConfiguration;
use crate::recovery::RecoveryReport;
use crate::recovery::{RecoveryError, SyncDecision, SyncEntry};
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
    /// A peer vote was rejected.
    Vote(VoteError),
    /// A `NewLeader` was rejected.
    Promise(PromiseRejection),
    /// A peer frame did not decode.
    MalformedPeerMessage,
    /// A proposal of a ballot this replica no longer votes in.
    StaleBallot,
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
#[derive(Clone, Debug, PartialEq, Eq)]
struct ReportDue {
    ballot: Ballot,
    to: ReplicaId,
    requires: Vec<BarrierId>,
}

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
    votes: BTreeMap<CommandId, VoteSet>,
    payloads: BTreeMap<CommandId, PayloadRecordV1>,
    durable_payloads: BTreeMap<BarrierId, CommandId>,
    served_payloads: alloc::collections::BTreeSet<CommandId>,
    ledger: DurableLedger,
    learner: Learner,
    campaign: Option<Campaign>,
    report_due: Option<ReportDue>,
    sync_pending: BTreeMap<CommandId, SyncEntry>,
    won: Option<SyncDecision>,
    /// Voting messages of the promised ballot that arrived before its Sync
    /// (delivery is not ordered across peers); replayed once synchronized.
    awaiting_sync: Vec<(ReplicaId, ProtocolMessage)>,
    rejections: Vec<FollowerRejection>,
}

impl Follower {
    /// A follower from its configuration, the recovered promise row and
    /// the durable dependency rows of the epoch.
    pub fn recover(
        config: FollowerConfig,
        durable_promise: Option<PromiseRecordV1>,
        rows: impl IntoIterator<Item = (CommandId, CommandRecord)>,
    ) -> Self {
        let ballots =
            BallotState::recover(config.identity.clone(), config.genesis, durable_promise);
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
            boot: None,
            alloc: None,
            outbox: None,
            ballots,
            table,
            bindings: BTreeMap::new(),
            held: BTreeMap::new(),
            adopted,
            pending: BTreeMap::new(),
            votes: BTreeMap::new(),
            payloads: BTreeMap::new(),
            durable_payloads: BTreeMap::new(),
            served_payloads: alloc::collections::BTreeSet::new(),
            ledger,
            learner: Learner::new(ExecutionPosition::ZERO),
            campaign: None,
            report_due: None,
            sync_pending: BTreeMap::new(),
            won: None,
            awaiting_sync: Vec::new(),
            rejections: Vec::new(),
        }
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
        self.learner = Learner::new(through);
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
                genesis: quorum.ballot,
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
            campaign: None,
            report_due: None,
            sync_pending: BTreeMap::new(),
            won: None,
            awaiting_sync: Vec::new(),
            rejections: Vec::new(),
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
        let outstanding: Vec<BarrierId> = self.pending.keys().copied().collect();
        let alloc = self.alloc.as_mut().expect("booted");
        let effects = match self.ballots.on_new_leader(
            self.config.identity.replica,
            ballot,
            boot,
            alloc,
            &outstanding,
        ) {
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
            to: self.config.identity.replica,
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
        if due.to == self.config.identity.replica {
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
                to: PeerId {
                    replica: due.to,
                    incarnation: ReplicaIncarnation::ZERO,
                },
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
                if campaign.payloads_requested() {
                    return Vec::new();
                }
                campaign.mark_payloads_requested();
                let me = self.config.identity.replica;
                let voters: Vec<ReplicaId> = campaign
                    .promised()
                    .iter()
                    .filter(|v| **v != me)
                    .copied()
                    .collect();
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
                self.won = Some(decision);
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
            && ballot != self.config.quorum.ballot
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
        if already_synced && self.config.quorum.ballot == decision.ballot {
            // Duplicate Sync of the active ballot: converges without change.
            return Vec::new();
        }
        let mut effects = Vec::new();
        if !already_synced {
            let record = self.ballots.mark_synced(decision.ballot);
            let barrier = self.alloc.as_mut().expect("booted").allocate();
            effects.push(Effect::Persist(PersistBatch {
                barrier,
                base: None,
                updates: alloc::vec![
                    promise_update(self.config.identity.epoch, &record).expect("bounded")
                ],
            }));
        }
        // A recovered replica whose promise row already records this
        // synchronized ballot still activates its fast set and
        // (re)installs the entries; both are idempotent.
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
                if self.table.phase_of(&command) < Some(Phase::Accept)
                    && self.table.accept(command, entry.deps.clone()).is_err()
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
    pub fn report(&self, ballot: Ballot) -> RecoveryReport {
        self.ledger
            .report(self.config.identity.replica, ballot, self.ballots.synced())
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
            self.config.quorum.ballot,
            outcome,
        )?;
        self.learn();
        Ok(alloc::vec![Effect::Established(result)])
    }

    fn learn(&mut self) {
        Learner::commit_learned(&mut self.table, &self.votes);
    }

    /// A fresh follower with nothing durable.
    pub fn new(config: FollowerConfig) -> Self {
        Self::recover(config, None, core::iter::empty())
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

    fn publish_to_voters_and_frontend(&mut self, barrier: BarrierId, message: ProtocolMessage) {
        let Some(boot) = self.boot else {
            return;
        };
        let context = self
            .ballots
            .context(boot, self.config.quorum.ballot, LocalJournalSeq::ZERO);
        let frame = message.encode();
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
        outbox.publish(PendingSend {
            context,
            requires: alloc::vec![barrier],
            to: self.config.frontend,
            frame,
        });
    }

    fn on_admitted(&mut self, frame: &[u8]) -> Vec<Effect> {
        if self.boot.is_none() {
            return Vec::new();
        }
        let request = match decode_stream(frame).as_deref() {
            Ok([MessageV1::Request(r)]) => r.clone(),
            _ => {
                self.rejections.push(FollowerRejection::MalformedRequest);
                return Vec::new();
            }
        };
        self.on_request(request.retry_key, request.logical.as_slice().to_vec())
    }

    /// A canonical request reached this replica (from the frontend, or as
    /// a transferred payload): initialize, persist, vote.
    fn on_request(&mut self, retry_key: RetryKey, logical: Vec<u8>) -> Vec<Effect> {
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
        // Atomic initialization; the placeholder of an early proposal (if
        // any) becomes the record in this same transition.
        let init = match self.table.initialize(
            command,
            command.0,
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
        self.payloads.insert(
            command,
            PayloadRecordV1 {
                retry_key,
                logical: logical.clone(),
            },
        );
        let epoch = self.config.identity.epoch;
        let record = self
            .table
            .record(&command)
            .expect("just initialized")
            .clone();
        let barrier = self.alloc.as_mut().expect("booted").allocate();
        let updates = alloc::vec![
            payload_update(&command, &PayloadRecordV1 { retry_key, logical }).expect("bounded"),
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
        let set = self
            .votes
            .entry(command)
            .or_insert_with(|| VoteSet::new(self.config.quorum.clone(), command));
        if self
            .config
            .quorum
            .fast_eligible(&self.config.identity.replica)
        {
            let ack = FastAck {
                replica: self.config.identity.replica,
                ballot: self.config.quorum.ballot,
                command,
                deps: init.deps.clone(),
                paths: init.paths.clone(),
                path: init.path,
                seqnum: None,
            };
            let _ = set.add(Vote::Fast(ack.clone()));
            self.publish_to_voters_and_frontend(barrier, ProtocolMessage::FastAck(ack));
        }
        // A proposal (or Sync entry) that arrived before the payload can now
        // be adopted (once its dependencies allow it).
        effects.extend(self.advance_sync());
        effects.extend(self.advance_pending());
        effects
    }

    fn on_proposal(&mut self, from: ReplicaId, proposal: FastAck) -> Vec<Effect> {
        if self.ballots.promised() != self.config.quorum.ballot
            || self.ballots.in_flight().is_some()
        {
            // A promise for a higher ballot fences every old-ballot voting
            // transition, including proposals arriving late.
            self.rejections.push(FollowerRejection::StaleBallot);
            return Vec::new();
        }
        if from != self.config.quorum.leader()
            || proposal.replica != from
            || proposal.ballot != self.config.quorum.ballot
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
                        .accept(command, held.proposal.deps.clone())
                        .is_err()
                {
                    continue;
                }
                self.adopted
                    .insert(command, (held.proposal.seqnum.unwrap_or(u64::MAX), false));
                let epoch = self.config.identity.epoch;
                let record = self.table.record(&command).expect("adopted").clone();
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
                    ballot: self.config.quorum.ballot,
                    command,
                };
                if let Some(set) = self.votes.get_mut(&command) {
                    let _ = set.add(Vote::Slow(ack.clone()));
                }
                self.publish_to_voters_and_frontend(barrier, ProtocolMessage::SlowAck(ack));
            }
        }
    }

    fn on_storage(&mut self, event: &StorageEvent) -> Vec<Effect> {
        if let Some(outbox) = self.outbox.as_mut() {
            outbox.observe(event);
        }
        self.ballots.on_storage(event);
        if let Some(barrier) = event.barrier()
            && let Some(pending) = self.pending.get(&barrier).cloned()
        {
            match event {
                StorageEvent::JournalDurable { .. } => {
                    self.pending.remove(&barrier);
                    self.ledger.durable(barrier);
                    if let Some(c) = self.durable_payloads.remove(&barrier) {
                        self.served_payloads.insert(c);
                    }
                    if let Pending::Adoption(c) = pending
                        && let Some(entry) = self.adopted.get_mut(&c)
                    {
                        entry.1 = true;
                    }
                }
                StorageEvent::Failed { .. } => {
                    self.pending.remove(&barrier);
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

    fn on_peer(&mut self, from: ReplicaId, frame: &[u8]) -> Vec<Effect> {
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
                let outstanding: Vec<BarrierId> = self.pending.keys().copied().collect();
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
            ProtocolMessage::Promise {
                ballot, replica, ..
            } => {
                if let Some(c) = self.campaign.as_mut()
                    && c.ballot() == ballot
                    && replica == from
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
            ProtocolMessage::Sync(decision) => self.on_sync(from, decision),
            ProtocolMessage::Proposal(_)
            | ProtocolMessage::FastAck(_)
            | ProtocolMessage::SlowAck(_)
                if self.stash_until_sync(&message) =>
            {
                self.awaiting_sync.push((from, message));
                Vec::new()
            }
            ProtocolMessage::Proposal(p) => self.on_proposal(from, p),
            ProtocolMessage::FastAck(ack) => self.collect(from, Vote::Fast(ack)),
            ProtocolMessage::SlowAck(ack) => self.collect(from, Vote::Slow(ack)),
            ProtocolMessage::PayloadRequest { commands } => self.serve_payloads(from, &commands),
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
        self.on_request(payload.retry_key, payload.logical)
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
            Event::Admitted(request) => self.on_admitted(&request.frame),
            Event::Storage(event) => self.on_storage(&event),
            Event::Peer(message) => {
                let from = message.provenance().from();
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

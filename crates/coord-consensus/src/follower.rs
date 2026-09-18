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

use crate::ballot::{BallotState, ConfigurationIdentity, PromiseRejection, ReplicaRole};
use crate::commands::{CommandRecord, CommandTable, InitError};
use crate::learner::{AppliedOutcome, LearnError, Learner};
use crate::messages::ProtocolMessage;
use crate::phase::Phase;
use crate::quorum::BallotConfiguration;
use crate::rows::{PayloadRecordV1, PromiseRecordV1, dependency_update, payload_update};
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
    /// A higher promise is in flight or durable, so the configured ballot
    /// no longer votes; the work belongs to the new leader.
    FencedByPromise {
        /// The ballot promised now.
        promised: Ballot,
    },
    /// A peer vote was rejected.
    Vote(VoteError),
    /// A `NewLeader` was rejected.
    Promise(PromiseRejection),
    /// A peer frame did not decode.
    MalformedPeerMessage,
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
    learner: Learner,
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
    pub fn recover(
        config: FollowerConfig,
        durable_promise: Option<PromiseRecordV1>,
        rows: impl IntoIterator<Item = (CommandId, CommandRecord)>,
        payloads: impl IntoIterator<Item = (CommandId, PayloadRecordV1)>,
        executed_through: ExecutionPosition,
    ) -> Self {
        let payloads: BTreeMap<CommandId, PayloadRecordV1> = payloads.into_iter().collect();
        let ballots =
            BallotState::recover(config.identity.clone(), config.genesis, durable_promise);
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
            bindings: payloads
                .iter()
                .map(|(command, record)| (record.retry_key, *command))
                .collect(),
            held: BTreeMap::new(),
            adopted,
            pending: BTreeMap::new(),
            deferred: BTreeMap::new(),
            votes: BTreeMap::new(),
            payloads,
            learner: Learner::new(executed_through),
            rejections: Vec::new(),
        }
    }

    /// The next command to execute through the materializer, if any.
    pub fn next_executable(&self) -> Option<CommandId> {
        self.learner
            .next_executable(&self.table, |c| self.adopted.get(c).map(|(s, _)| *s))
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
        Learner::commit_learned(&mut self.table, &self.votes);
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

    fn on_admitted(&mut self, frame: &[u8]) -> Vec<Effect> {
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
        let (Ok(logical), retry_key) = (request.logical(), request.retry_key) else {
            self.rejections.push(FollowerRejection::MalformedRequest);
            return Vec::new();
        };
        let Ok(command) = CommandId::derive(&retry_key, &logical) else {
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
                logical: request.logical.as_slice().to_vec(),
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
            payload_update(
                &command,
                &PayloadRecordV1 {
                    retry_key,
                    logical: request.logical.as_slice().to_vec(),
                },
            )
            .expect("bounded"),
            dependency_update(epoch, &command, &record).expect("bounded"),
        ];
        let mut effects = alloc::vec![Effect::Persist(PersistBatch {
            barrier,
            base: None,
            updates,
        })];
        self.pending.insert(barrier, Pending::Vote(command));
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
        // A proposal that arrived before the payload can now be adopted
        // (once its dependencies allow it).
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
                if self
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
                let ack = SlowAck {
                    replica: self.config.identity.replica,
                    ballot: self.config.quorum.ballot(),
                    command,
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
        if let Some(barrier) = event.barrier()
            && let Some(pending) = self.pending.get(&barrier).cloned()
        {
            match event {
                StorageEvent::JournalDurable { .. } => {
                    self.pending.remove(&barrier);
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
                    let command = match pending {
                        Pending::Vote(c) | Pending::Adoption(c) => c,
                    };
                    self.rejections
                        .push(FollowerRejection::BatchFailed(command));
                }
                _ => {}
            }
        }
        self.learn();
        self.release()
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
                let outstanding: Vec<BarrierId> = self.pending.keys().copied().collect();
                match self
                    .ballots
                    .on_new_leader(from, ballot, boot, alloc, &outstanding)
                {
                    Ok(effects) => {
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
            ProtocolMessage::Proposal(p) => self.on_proposal(from.replica, p),
            ProtocolMessage::FastAck(ack) => self.collect(from.replica, Vote::Fast(ack)),
            ProtocolMessage::SlowAck(ack) => self.collect(from.replica, Vote::Slow(ack)),
            ProtocolMessage::Promise { .. } | ProtocolMessage::LeaderReply { .. } => Vec::new(),
        }
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

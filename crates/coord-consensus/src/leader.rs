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
use crate::learner::{AppliedOutcome, LearnError, Learner};
use crate::messages::{PathAnchors, ProtocolMessage};
use crate::phase::Phase;
use crate::quorum::BallotConfiguration;
use crate::rows::{
    PayloadRecordV1, PromiseRecordV1, ProposalRecordV1, dependency_update, payload_update,
    proposal_update,
};
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
    /// The leader stopped leading; a new election recovers the role.
    Fenced(FenceReason),
    /// A peer message was rejected.
    Vote(VoteError),
    /// A `NewLeader` was rejected.
    Promise(PromiseRejection),
    /// A peer frame did not decode.
    MalformedPeerMessage,
}

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
    learner: Learner,
    seqnum: u64,
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
        let ballots =
            BallotState::recover(config.identity.clone(), config.genesis, durable_promise);
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
            learner: Learner::new(executed_through),
            seqnum: 0,
            rejections: Vec::new(),
            fenced: None,
        }
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
        Ok(alloc::vec![Effect::Established(result)])
    }

    fn learn(&mut self) {
        Learner::commit_learned(&mut self.table, &self.votes);
    }

    /// Whether this replica leads the promised ballot: it votes in this
    /// epoch (a non-voting role never proposes), its identity agrees with
    /// the ballot configuration, the promised ballot is the configured
    /// one and no promise is in flight, and it has not been fenced.
    pub fn is_leading(&self) -> bool {
        let identity = &self.config.identity;
        let quorum = &self.config.quorum;
        self.fenced.is_none()
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

    fn on_admitted(&mut self, frame: &[u8]) -> Vec<Effect> {
        let Some(boot) = self.boot else {
            return Vec::new();
        };
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
        // Atomic initialization: payload binding, conservative dependencies,
        // path evidence and index publication in one transition.
        let init =
            match self
                .table
                .initialize(command, command.0, alloc::vec![CONSERVATIVE_KEY.to_vec()])
            {
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
        self.payloads.insert(
            command,
            PayloadRecordV1 {
                retry_key: request.retry_key,
                logical: request.logical.as_slice().to_vec(),
            },
        );
        let ballot = self.config.quorum.ballot();
        let epoch = self.config.identity.epoch;
        let seqnum = self.seqnum;
        self.seqnum += 1;
        let record = self
            .table
            .record(&command)
            .expect("just initialized")
            .clone();
        let barrier = self.alloc.as_mut().expect("booted").allocate();
        let updates: Vec<StoreUpdate> = alloc::vec![
            payload_update(
                &command,
                &PayloadRecordV1 {
                    retry_key: request.retry_key,
                    logical: request.logical.as_slice().to_vec(),
                },
            )
            .expect("bounded"),
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
        let proposal = FastAck {
            replica: self.config.identity.replica,
            ballot,
            command,
            deps: init.deps.clone(),
            paths: init.paths.clone(),
            path: init.path,
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
            },
        );
        alloc::vec![persist]
    }

    fn on_storage(&mut self, event: &StorageEvent) -> Vec<Effect> {
        if let Some(outbox) = self.outbox.as_mut() {
            outbox.observe(event);
        }
        self.ballots.on_storage(event);
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
                    let retry = self.retry_proposal(command);
                    self.advance_pending();
                    let mut effects = retry;
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
        self.advance_pending();
        self.learn();
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
    fn advance_pending(&mut self) {
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
                }
            }
            if !progressed {
                return;
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
                let (Some(boot), Some(alloc)) = (self.boot, self.alloc.as_mut()) else {
                    return Vec::new();
                };
                let outstanding = self
                    .proposals
                    .values()
                    .filter(|p| !p.durable)
                    .map(|p| p.barrier)
                    .collect::<Vec<_>>();
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
                        self.rejections.push(Rejection::Promise(e));
                        Vec::new()
                    }
                }
            }
            ProtocolMessage::FastAck(ack) => self.collect(from.replica, Vote::Fast(ack)),
            ProtocolMessage::SlowAck(ack) => self.collect(from.replica, Vote::Slow(ack)),
            ProtocolMessage::Proposal(_)
            | ProtocolMessage::Promise { .. }
            | ProtocolMessage::LeaderReply { .. } => Vec::new(),
        }
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
        Vec::new()
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
            Event::Admitted(request) => self.on_admitted(&request.frame),
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

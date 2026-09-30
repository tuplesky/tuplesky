//! task-d09: the leader carries the decision.
//!
//! A follower learns a command from the acknowledgements it receives
//! itself. With five voters it needs two of its peers' as well as the
//! leader's proposal and its own adoption, and each acknowledgement is
//! published once. A follower that missed them stayed at ACCEPT, and with
//! the chain total on everything after it, until its table filled and it
//! refused new work. These tests cut followers off from each other's
//! acknowledgements under a live leader and check that the leader's
//! commit frontier (`Leader::announce_committed`, `ProtocolMessage::
//! Committed`) is enough for them to execute everything it commits, and
//! that it commits nothing a follower did not adopt.
//!
//! Three voters would not show any of this: there, the leader's proposal
//! and the follower's own adoption are already a majority.

use std::collections::VecDeque;

use coord_consensus::{
    AppliedOutcome, BallotConfiguration, ConfigurationIdentity, Follower, FollowerConfig,
    FollowerRejection, Leader, LeaderConfig, Phase, ProtocolMessage, ReplicaRole,
};
use coord_core::capability::{AdmissionReceipt, AttestedAdmission, VerifierToken};
use coord_core::effect::{BootId, Effect, PeerId};
use coord_core::event::{
    AdmittedRequest, AuthenticatedPeerMessage, Event, PeerProvenance, StorageEvent,
};
use coord_core::machine::DeterministicMachine;
use coord_sim::storage::StorageModel;
use coord_store_api::registry::Collection;
use coord_types::identity::Digest32;
use coord_types::ids::*;
use coord_types::logical_v1::{CanonicalOperation, LogicalRequest, PutOp};
use coord_types::wire_v1::{MessageV1, RequestV1};
use coord_types::{CommandId, RetryKey};

/// Voters in these tests. Five, so that a follower needs its peers.
const VOTERS: u8 = 5;

fn r(i: u8) -> ReplicaId {
    ReplicaId([i; 16])
}

fn epoch() -> ConfigurationEpoch {
    ConfigurationEpoch::new(1).unwrap()
}

fn ballot0() -> Ballot {
    Ballot {
        epoch: epoch(),
        number: 0,
        leader: r(0),
    }
}

const FRONTEND: PeerId = PeerId {
    replica: ReplicaId([0xf0; 16]),
    incarnation: ReplicaIncarnation::ZERO,
};

fn identity(me: u8) -> ConfigurationIdentity {
    ConfigurationIdentity {
        cluster: ClusterId([1; 16]),
        domain: DomainId([2; 16]),
        epoch: epoch(),
        voters: (0..VOTERS).map(r).collect(),
        replica: r(me),
        incarnation: ReplicaIncarnation::new(1).unwrap(),
        role: ReplicaRole::Voter,
    }
}

fn quorum() -> BallotConfiguration {
    BallotConfiguration::c2_default(epoch(), ballot0(), (0..VOTERS).map(r).collect()).unwrap()
}

#[allow(clippy::large_enum_variant)]
enum Role {
    Leader(Leader),
    Follower(Follower),
}

struct Node {
    role: Role,
    storage: StorageModel,
    inbox: VecDeque<(ReplicaId, Vec<u8>)>,
    executed: Vec<CommandId>,
    boot: u8,
}

impl Node {
    fn step(&mut self, e: Event) -> Vec<Effect> {
        match &mut self.role {
            Role::Leader(m) => m.step(e),
            Role::Follower(m) => m.step(e),
        }
    }

    fn leader(&self) -> &Leader {
        match &self.role {
            Role::Leader(l) => l,
            Role::Follower(_) => panic!("follower"),
        }
    }

    fn follower_mut(&mut self) -> &mut Follower {
        match &mut self.role {
            Role::Follower(f) => f,
            Role::Leader(_) => panic!("leader"),
        }
    }

    fn phase_of(&self, c: &CommandId) -> Option<Phase> {
        match &self.role {
            Role::Leader(m) => m.table().phase_of(c),
            Role::Follower(m) => m.table().phase_of(c),
        }
    }
}

struct Cluster {
    nodes: Vec<Node>,
    seed: u64,
    /// Nodes whose incoming acknowledgements from other followers are
    /// dropped. The leader's proposals still reach them.
    deaf: Vec<u8>,
    /// Nodes that receive no proposals at all.
    unproposed: Vec<u8>,
    /// Nodes that are down: they send, receive and do nothing.
    down: Vec<u8>,
    /// Every commit frontier announced: (to, through).
    announced: Vec<(u8, u64)>,
    /// Every payload ask sent: (from, commands).
    asks: Vec<(u8, Vec<CommandId>)>,
    /// The voters' table capacity.
    capacity: usize,
}

const SETTLE_STEPS: u32 = 200_000;

impl Cluster {
    fn new(seed: u64, capacity: usize) -> Self {
        let mut nodes = Vec::new();
        for me in 0..VOTERS {
            let role = if me == 0 {
                Role::Leader(Leader::new(
                    LeaderConfig {
                        identity: identity(0),
                        quorum: quorum(),
                        genesis: ballot0(),
                        frontend: FRONTEND,
                        capacity,
                    },
                    None,
                    ExecutionPosition::ZERO,
                ))
            } else {
                Role::Follower(Follower::new(FollowerConfig {
                    identity: identity(me),
                    quorum: quorum(),
                    genesis: ballot0(),
                    frontend: FRONTEND,
                    capacity,
                }))
            };
            let mut node = Node {
                role,
                storage: StorageModel::default(),
                inbox: VecDeque::new(),
                executed: Vec::new(),
                boot: 1,
            };
            node.step(Event::Boot {
                boot_id: BootId([1; 16]),
                incarnation: ReplicaIncarnation::new(1).unwrap(),
            });
            nodes.push(node);
        }
        Cluster {
            nodes,
            seed,
            deaf: Vec::new(),
            unproposed: Vec::new(),
            down: Vec::new(),
            announced: Vec::new(),
            asks: Vec::new(),
            capacity,
        }
    }

    fn rand(&mut self) -> u64 {
        self.seed ^= self.seed << 13;
        self.seed ^= self.seed >> 7;
        self.seed ^= self.seed << 17;
        self.seed
    }

    fn handle(&mut self, i: usize, effects: Vec<Effect>) {
        for e in effects {
            match e {
                Effect::Persist(batch) => {
                    let barrier = batch.barrier;
                    let node = &mut self.nodes[i];
                    node.storage.submit(batch);
                    node.storage.complete(barrier).unwrap();
                    let more = node.step(Event::Storage(StorageEvent::JournalDurable {
                        barrier_id: barrier,
                        journal_seq: LocalJournalSeq::new(1).unwrap(),
                    }));
                    self.handle(i, more);
                }
                Effect::SendWhenDurable { to, frame, .. } => {
                    if to == FRONTEND {
                        continue;
                    }
                    let dest = to.replica.0[0];
                    if self.down.contains(&dest) || self.down.contains(&(i as u8)) {
                        continue;
                    }
                    let message = ProtocolMessage::decode(&frame).unwrap();
                    match &message {
                        ProtocolMessage::FastAck(_) | ProtocolMessage::SlowAck(_)
                            if i != 0 && self.deaf.contains(&dest) =>
                        {
                            continue;
                        }
                        ProtocolMessage::Proposal(_) if self.unproposed.contains(&dest) => {
                            continue;
                        }
                        ProtocolMessage::Committed { through, .. } => {
                            self.announced.push((dest, *through));
                        }
                        ProtocolMessage::PayloadRequest { commands } => {
                            self.asks.push((i as u8, commands.clone()));
                        }
                        _ => {}
                    }
                    self.nodes[dest as usize]
                        .inbox
                        .push_back((r(i as u8), frame));
                }
                Effect::Established(_) | Effect::Released(_) => {}
                other => panic!("{other:?}"),
            }
        }
    }

    fn deliver(&mut self, to: usize, from: ReplicaId, frame: Vec<u8>) {
        let event = Event::Peer(AuthenticatedPeerMessage::new(
            PeerProvenance::from_transport(from, ReplicaIncarnation::new(1).unwrap(), 1),
            frame,
        ));
        let effects = self.nodes[to].step(event);
        self.handle(to, effects);
    }

    fn settle(&mut self) {
        // A payload ask is paced by a timer in the runtime: one per node
        // per settle here. Asked on every step, a follower whose table is
        // full refuses the answer and asks again at once, for ever.
        let mut asked: Vec<usize> = Vec::new();
        let mut steps = 0u32;
        loop {
            steps += 1;
            assert!(steps < SETTLE_STEPS, "the cluster did not settle");
            let mut progressed = false;
            let candidates: Vec<usize> = (0..self.nodes.len())
                .filter(|i| !self.nodes[*i].inbox.is_empty())
                .collect();
            if !candidates.is_empty() {
                let pick = candidates[(self.rand() % candidates.len() as u64) as usize];
                let (from, frame) = self.nodes[pick].inbox.pop_front().unwrap();
                self.deliver(pick, from, frame);
                progressed = true;
            }
            for i in 0..self.nodes.len() {
                if self.down.contains(&(i as u8)) {
                    continue;
                }
                loop {
                    let next = match &self.nodes[i].role {
                        Role::Leader(m) => m.next_executable(),
                        Role::Follower(m) => m.next_executable(),
                    };
                    let Some(c) = next else { break };
                    let position = match &self.nodes[i].role {
                        Role::Leader(m) => m.executed_through(),
                        Role::Follower(m) => m.executed_through(),
                    }
                    .checked_next()
                    .unwrap();
                    let outcome = AppliedOutcome {
                        position,
                        revision: None,
                        result_digest: Digest32(c.0.0),
                        response: c.0.0.to_vec(),
                    };
                    let effects = match &mut self.nodes[i].role {
                        Role::Leader(m) => m.applied(c, &outcome).unwrap(),
                        Role::Follower(m) => m.applied(c, &outcome).unwrap(),
                    };
                    self.nodes[i].executed.push(c);
                    self.handle(i, effects);
                    progressed = true;
                }
            }
            for i in 1..self.nodes.len() {
                if self.down.contains(&(i as u8)) || asked.contains(&i) {
                    continue;
                }
                let f = self.nodes[i].follower_mut();
                if f.missing_payloads().is_empty() {
                    continue;
                }
                let effects = f.request_payloads(r(0));
                asked.push(i);
                if !effects.is_empty() {
                    self.handle(i, effects);
                    progressed = true;
                }
            }
            if !progressed {
                return;
            }
        }
    }

    /// The re-send timer without the frontier: what the runtime did
    /// before task-d09.
    fn resend_only(&mut self) {
        let Role::Leader(l) = &mut self.nodes[0].role else {
            panic!("r0 leads")
        };
        let effects = l.resend_unvoted(coord_consensus::RESEND_PER_VOTER);
        self.handle(0, effects);
    }

    /// What the runtime's re-send timer does (task-d07, task-d09): the
    /// unvoted proposals again, then the commit frontier.
    fn tick(&mut self) {
        self.resend_only();
        let Role::Leader(l) = &mut self.nodes[0].role else {
            panic!("r0 leads")
        };
        let effects = l.announce_committed();
        self.handle(0, effects);
    }

    /// Kill node `i` and bring it back at once from its durable rows, as
    /// `coordd` restores a voter: its volatile state is gone, and its
    /// adoptions come back without the ballot's sequence numbers.
    fn restart(&mut self, i: usize) {
        let capacity = self.capacity;
        let node = &mut self.nodes[i];
        node.inbox.clear();
        node.storage.crash();
        let rows: Vec<_> = node
            .storage
            .durable_rows()
            .into_iter()
            .filter(|(c, k, _)| {
                *c == Collection::ProtocolV1.id().0 && k.len() == 41 && k[8] == 0x01
            })
            .map(|(_, k, v)| {
                (
                    CommandId(Digest32(k[9..].try_into().unwrap())),
                    coord_consensus::decode_dependency(&v).unwrap(),
                )
            })
            .collect();
        let promise = node
            .storage
            .durable_rows()
            .into_iter()
            .find(|(c, k, _)| *c == Collection::ProtocolV1.id().0 && k.len() == 9)
            .map(|(_, _, v)| coord_consensus::decode_promise(&v).unwrap());
        let payloads: Vec<_> = node
            .storage
            .durable_rows()
            .into_iter()
            .filter(|(c, _, _)| *c == Collection::PayloadV1.id().0)
            .map(|(_, k, v)| {
                (
                    CommandId(Digest32(k[..].try_into().unwrap())),
                    coord_consensus::decode_payload(&v).unwrap(),
                )
            })
            .collect();
        let mut f = Follower::recover(
            FollowerConfig {
                identity: identity(i as u8),
                quorum: quorum(),
                genesis: ballot0(),
                frontend: FRONTEND,
                capacity,
            },
            promise,
            rows,
            payloads.clone(),
            ExecutionPosition::ZERO,
        )
        .restore_execution(
            ExecutionPosition::new(node.executed.len() as u64).unwrap(),
            node.executed.iter().copied(),
        )
        .restore_payloads(payloads);
        node.boot += 1;
        f.step(Event::Boot {
            boot_id: BootId([node.boot; 16]),
            incarnation: ReplicaIncarnation::new(1).unwrap(),
        });
        node.role = Role::Follower(f);
    }

    fn settle_ticking(&mut self, rounds: usize) {
        self.settle();
        for _ in 0..rounds {
            self.tick();
            self.settle();
        }
    }

    fn admit(&mut self, seq: u64) -> CommandId {
        let all: Vec<usize> = (0..self.nodes.len()).collect();
        self.admit_to(seq, &all, 9)
    }

    /// Submit request `seq` to the voters `at` only, under the admission
    /// receipt `receipt`: the same command identity, and other attested
    /// facts when `receipt` differs.
    fn admit_to(&mut self, seq: u64, at: &[usize], receipt: u8) -> CommandId {
        let key = (seq % 250) as u8;
        let request = LogicalRequest::new(
            NamespaceId([5; 16]),
            CanonicalOperation::Put(PutOp {
                key: vec![key],
                value: vec![key],
                lease: None,
                prev_kv: false,
            }),
        );
        let rk = RetryKey {
            cluster_id: ClusterId([1; 16]),
            domain_id: DomainId([2; 16]),
            session_id: SessionId([3; 16]),
            client_instance_id: ClientInstanceId([4; 16]),
            request_sequence: RequestSequence::new(seq).unwrap(),
        };
        let command = CommandId::derive(&rk, &request).unwrap();
        let frame = MessageV1::Request(RequestV1::new(rk, &request, 0, 0).unwrap())
            .encode()
            .unwrap();
        for &i in at {
            if self.down.contains(&(i as u8)) {
                continue;
            }
            let receipt = AdmissionReceipt::submitting(
                VerifierToken::for_boundary(),
                AttestedAdmission {
                    cluster: ClusterId([1; 16]),
                    domain: DomainId([2; 16]),
                    session: SessionId([3; 16]),
                    rule_generation: 1,
                    scope_ceiling: u32::MAX,
                    receipt_id: Digest32([receipt; 32]),
                    admitted_at_ticks: 0,
                },
            );
            let effects = self.nodes[i].step(Event::Admitted(AdmittedRequest {
                receipt,
                frame: frame.clone(),
            }));
            self.handle(i, effects);
        }
        command
    }
}

/// A follower that hears no other follower executes everything the
/// leader commits, from the leader's frontier alone.
///
/// r3 is outside the fast set and receives every proposal, but none of
/// its peers' acknowledgements: it adopts each command and can learn
/// none of them. The leader commits them from the other three followers.
/// The re-send alone changes nothing for r3, since it adopted everything
/// and so lacks no proposal. The frontier commits what it adopted.
#[test]
fn a_follower_that_hears_only_the_leader_executes_what_it_commits() {
    let mut cluster = Cluster::new(3, 64);
    cluster.deaf = vec![3];
    let commands: Vec<CommandId> = (1..=10).map(|n| cluster.admit(n)).collect();
    cluster.settle();
    cluster.resend_only();
    cluster.settle();
    assert_eq!(cluster.nodes[0].executed, commands);
    assert_eq!(
        cluster.nodes[3].executed,
        Vec::<CommandId>::new(),
        "r3 learned without its peers' acknowledgements"
    );
    assert!(
        commands
            .iter()
            .all(|c| cluster.nodes[3].phase_of(c) == Some(Phase::Accept)),
        "r3 adopted every command"
    );
    cluster.settle_ticking(1);
    for i in 0..VOTERS as usize {
        assert_eq!(cluster.nodes[i].executed, commands, "node {i}");
    }
}

/// Two followers cut off from each other's acknowledgements under a live
/// leader both execute, and so does everyone else.
///
/// This is the Jepsen ring partition: r3 and r4 each see the leader but
/// no other follower's acknowledgements, so neither can make up a
/// majority of three from what it receives. The leader still commits
/// every command from all four followers.
#[test]
fn two_followers_cut_from_their_peers_both_execute() {
    let mut cluster = Cluster::new(5, 64);
    cluster.deaf = vec![3, 4];
    let commands: Vec<CommandId> = (1..=12).map(|n| cluster.admit(n)).collect();
    cluster.settle();
    assert_eq!(cluster.nodes[0].executed, commands);
    for i in [3usize, 4] {
        assert!(cluster.nodes[i].executed.is_empty(), "node {i} learned");
    }
    cluster.settle_ticking(1);
    for i in 0..VOTERS as usize {
        assert_eq!(cluster.nodes[i].executed, commands, "node {i}");
    }
}

/// Build the stall: r3 hears no peer, and the table of eight fills while
/// it can learn nothing.
///
/// Its first eight commands are adopted and wait for a commit. Every later
/// payload is refused as backpressure, and so is every later proposal's
/// placeholder: those proposals are held with nothing in the table.
fn stalled(seed: u64) -> (Cluster, Vec<CommandId>) {
    let capacity = 8;
    let mut cluster = Cluster::new(seed, capacity);
    cluster.deaf = vec![3];
    let commands: Vec<CommandId> = (1..=(capacity as u64 * 4))
        .map(|n| {
            let c = cluster.admit(n);
            cluster.settle();
            c
        })
        .collect();
    cluster.resend_only();
    cluster.settle();
    assert_eq!(cluster.nodes[0].executed, commands);
    assert!(cluster.nodes[3].executed.is_empty());
    let refused = cluster.nodes[3]
        .follower_mut()
        .take_rejections()
        .iter()
        .filter(|r| matches!(r, FollowerRejection::Backpressure))
        .count();
    assert!(refused > 0, "r3's table never filled");
    (cluster, commands)
}

/// A follower whose table filled while it could learn nothing catches up
/// once the frontier reaches it, and executes everything in order.
///
/// The frontier commits the eight it adopted, and they execute. The
/// payloads it asks for then arrive in no particular order, and the
/// table fills again with later commands, none of which can be adopted
/// before the next one in the chain; that one was refused, and held every
/// later one for ever. This is the five-node Jepsen stall, where
/// backpressure refusals on the followers reached the thousands. The
/// command whose turn has come is now let into the full table.
#[test]
fn a_follower_whose_table_filled_while_it_could_not_learn_catches_up() {
    let (mut cluster, commands) = stalled(7);
    cluster.settle_ticking(commands.len());
    assert_eq!(cluster.nodes[3].executed, commands);
}

/// A follower that lacks many payloads asks first for the one whose turn
/// has come.
///
/// r3 receives the leader's proposals of 32 commands and none of their
/// payloads. Its missing set is every command, four times what one ask
/// carries, and the ask is bounded and rotates through it in identity
/// order, so the one whose turn it is was reached once per rotation, and
/// nothing executes before it. The ask leads with
/// the held proposals whose turn has come (or that the leader's frontier
/// covers), lowest sequence number first.
#[test]
fn a_follower_asks_first_for_what_the_leader_committed_in_its_order() {
    let capacity = 32;
    let mut cluster = Cluster::new(19, capacity);
    // As many as new admission takes: the capacity less the recovery
    // reserve (task-d24).
    let admitted = capacity - capacity / coord_consensus::RECOVERY_RESERVE_PARTS;
    let commands: Vec<CommandId> = (1..=admitted as u64)
        .map(|n| cluster.admit_to(n, &[0, 1, 2, 4], 9))
        .collect();
    // Only the leader's proposals reach r3, and nothing is settled, so it
    // has asked for nothing yet.
    let frames: Vec<(ReplicaId, Vec<u8>)> = cluster.nodes[3].inbox.drain(..).collect();
    for (from, frame) in frames {
        if from == r(0)
            && matches!(
                ProtocolMessage::decode(&frame),
                Ok(ProtocolMessage::Proposal(_))
            )
        {
            cluster.deliver(3, from, frame);
        }
    }
    let f = cluster.nodes[3].follower_mut();
    assert_eq!(f.missing_payloads().len(), commands.len());
    let effects = f.request_payloads(r(0));
    let ask = effects
        .iter()
        .find_map(|e| match e {
            Effect::SendWhenDurable { frame, .. } => match ProtocolMessage::decode(frame) {
                Ok(ProtocolMessage::PayloadRequest { commands }) => Some(commands),
                _ => None,
            },
            _ => None,
        })
        .expect("r3 asked");
    assert_eq!(ask[0], commands[0], "the ask led with other commands");
    cluster.handle(3, effects);
    cluster.settle_ticking(commands.len());
    assert_eq!(cluster.nodes[0].executed, commands);
    assert_eq!(cluster.nodes[3].executed, commands);
}

/// A follower restarted under a live leader, with adoptions it could not
/// commit, executes everything the leader commits.
///
/// r3 hears no peer, so it adopts the first six commands and commits
/// none. Killed and restarted, it gets its adoptions back from the rows
/// without the ballot's sequence numbers, so the frontier cannot commit
/// them. The leader counted its acknowledgements and never re-sends
/// them, and with the chain total, nothing r3 adopts afterwards can
/// commit either. It asks the leader for those proposals, adopts them
/// again with their sequence numbers, and the frontier commits them.
#[test]
fn a_follower_restarted_with_adoptions_in_flight_executes_what_the_leader_commits() {
    let mut cluster = Cluster::new(23, 64);
    cluster.deaf = vec![3];
    let first: Vec<CommandId> = (1..=6).map(|n| cluster.admit(n)).collect();
    cluster.settle();
    assert!(
        first
            .iter()
            .all(|c| cluster.nodes[3].phase_of(c) == Some(Phase::Accept)),
        "r3 adopted every command"
    );
    cluster.restart(3);
    let late: Vec<CommandId> = (7..=9).map(|n| cluster.admit(n)).collect();
    cluster.settle();
    cluster.resend_only();
    cluster.settle();
    assert!(cluster.nodes[3].executed.is_empty());
    cluster.settle_ticking(3);
    let all: Vec<CommandId> = first.iter().chain(late.iter()).copied().collect();
    assert_eq!(cluster.nodes[0].executed, all);
    assert_eq!(cluster.nodes[3].executed, all);
}

/// The attested facts r3 bound `command` under, and the ones r1 did.
fn admissions(cluster: &Cluster, command: &CommandId) -> (Option<Digest32>, Option<Digest32>) {
    let bound = |i: usize| {
        let Role::Follower(f) = &cluster.nodes[i].role else {
            unreachable!()
        };
        f.payload(command).map(|p| p.admission_digest())
    };
    (bound(3), bound(1))
}

/// Everything after `first`, submitted to every voter.
fn and_then(cluster: &mut Cluster, first: CommandId) -> Vec<CommandId> {
    let mut all = vec![first];
    all.extend((2..=4).map(|n| cluster.admit(n)));
    all
}

/// r3 took a command under other attested facts than the leader proposed
/// it under, and executes it under the leader's, with everything after it.
fn assert_rebound(cluster: &mut Cluster, all: &[CommandId]) {
    cluster.settle_ticking(3);
    assert_eq!(cluster.nodes[0].executed, all);
    assert_eq!(cluster.nodes[3].executed, all, "r3 stalled");
    let (r3, r1) = admissions(cluster, &all[0]);
    assert_eq!(r3, r1, "r3 executed other facts");
    let rejections = cluster.nodes[3].follower_mut().take_rejections();
    assert!(
        rejections.iter().any(
            |r| matches!(r, FollowerRejection::AdmissionConflict { command, .. } if *command == all[0])
        ),
        "the conflict was not reported"
    );
}

/// A follower that received the proposal before its own submission under
/// other attested facts rebinds to the leader's, and executes.
///
/// Every presentation of a command mints its own admission receipt, so a
/// submitter that presents again after a lost link reaches the voters
/// under facts the leader never saw. The proposal reaches r3 first and is
/// held; r3's own submission of the same command then arrives under
/// another receipt. Adopted, the frontier committed it and r3 executed a
/// command under facts the quorum never admitted. Refused, r3 never
/// adopted it, and with the chain total, nothing after it either. It asks
/// the leader for its payload instead, binds it in place of its own while
/// nothing has been accepted over it, and adopts.
#[test]
fn a_follower_that_took_a_command_under_other_facts_after_its_proposal_rebinds() {
    let mut cluster = Cluster::new(29, 64);
    cluster.deaf = vec![3];
    let c = cluster.admit_to(1, &[0, 1, 2, 4], 9);
    let pending: Vec<(ReplicaId, Vec<u8>)> = cluster.nodes[3].inbox.drain(..).collect();
    for (from, frame) in pending {
        cluster.deliver(3, from, frame);
    }
    assert_eq!(cluster.admit_to(1, &[3], 8), c);
    let (r3, r1) = admissions(&cluster, &c);
    assert_ne!(r3, r1, "r3 took the same facts");
    let all = and_then(&mut cluster, c);
    assert_rebound(&mut cluster, &all);
    // Written as rebound: the durable dependency row names the leader's
    // facts, and a restart restores the leader's payload.
    let (_, r1) = admissions(&cluster, &c);
    let row = cluster.nodes[3]
        .storage
        .durable_rows()
        .into_iter()
        .find(|(col, k, _)| {
            *col == Collection::ProtocolV1.id().0
                && k.len() == 41
                && k[8] == 0x01
                && k[9..] == c.0.0
        })
        .map(|(_, _, v)| coord_consensus::decode_dependency(&v).unwrap())
        .expect("a dependency row");
    assert_eq!(row.payload, r1, "the row names other facts");
    cluster.restart(3);
    assert_eq!(
        admissions(&cluster, &c).0,
        r1,
        "the restart restored other facts"
    );
}

/// The same, with r3's own submission first: the order in which a voter
/// that missed the first presentation meets the second.
#[test]
fn a_follower_that_took_a_command_under_other_facts_before_its_proposal_rebinds() {
    let mut cluster = Cluster::new(31, 64);
    cluster.deaf = vec![3];
    let c = cluster.admit_to(1, &[3], 8);
    assert_eq!(cluster.admit_to(1, &[0, 1, 2, 4], 9), c);
    let all = and_then(&mut cluster, c);
    assert_rebound(&mut cluster, &all);
}

/// A domain whose followers' tables filled with later commands still
/// executes: a command whose turn has come is admitted without waiting
/// for the leader's frontier.
///
/// Three of the four followers receive the submissions of eight later
/// commands before the command they all depend on, and their tables of
/// eight fill. The first command reaches only the leader and r1. Its
/// proposal cannot be placed in the three full tables, and its payload
/// was admitted past capacity only once the leader had committed it --
/// which it cannot do without their votes. Nothing ever executed, on any
/// voter, with no fault active: the five-node Jepsen run's stop at 265 s.
#[test]
fn a_command_whose_turn_has_come_is_admitted_before_the_leader_commits_it() {
    let capacity = 8;
    let mut cluster = Cluster::new(53, capacity);
    // A table full for new admission: the capacity less the recovery
    // reserve (task-d24).
    let full = capacity - capacity / coord_consensus::RECOVERY_RESERVE_PARTS;
    let later: Vec<CommandId> = (2..=(full as u64 + 1))
        .map(|n| cluster.admit_to(n, &[2, 3, 4], 9))
        .collect();
    cluster.settle();
    let head = cluster.admit_to(1, &[0, 1], 9);
    cluster.settle_ticking(3);
    // The collector re-offers the later commands to the rest.
    for n in 2..=(full as u64 + 1) {
        cluster.admit_to(n, &[0, 1], 9);
    }
    cluster.settle_ticking(capacity * 2);
    let mut all = vec![head];
    all.extend(later);
    for i in 0..VOTERS as usize {
        assert_eq!(cluster.nodes[i].executed, all, "node {i}");
    }
}

/// The frontier commits only what the follower adopted from the leader.
///
/// r3 receives every payload but no proposal: it holds each command
/// initialized and nothing more. However far the frontier is, it commits
/// none of them, because committing a command the follower did not adopt
/// would take this replica's dependencies rather than the leader's. Once
/// proposals reach it, the re-send delivers them and it catches up.
#[test]
fn the_frontier_commits_nothing_the_follower_did_not_adopt() {
    let mut cluster = Cluster::new(11, 64);
    cluster.deaf = vec![3];
    cluster.unproposed = vec![3];
    let commands: Vec<CommandId> = (1..=6).map(|n| cluster.admit(n)).collect();
    cluster.settle_ticking(3);
    assert_eq!(cluster.nodes[0].executed, commands);
    assert!(
        cluster.announced.iter().any(|(to, _)| *to == 3),
        "the frontier reached r3"
    );
    assert!(cluster.nodes[3].executed.is_empty());
    assert!(
        commands
            .iter()
            .all(|c| cluster.nodes[3].phase_of(c) < Some(Phase::Accept)),
        "r3 accepted a command without its proposal"
    );
    cluster.unproposed.clear();
    cluster.settle_ticking(3);
    assert_eq!(cluster.nodes[3].executed, commands);
}

/// Only the leader of the follower's ballot can move its frontier.
#[test]
fn a_frontier_from_another_voter_is_ignored() {
    let mut cluster = Cluster::new(13, 64);
    cluster.deaf = vec![3];
    let commands: Vec<CommandId> = (1..=4).map(|n| cluster.admit(n)).collect();
    cluster.settle();
    let forged = ProtocolMessage::Committed {
        ballot: ballot0(),
        through: 100,
    }
    .encode();
    cluster.deliver(3, r(1), forged);
    cluster.settle();
    assert!(
        cluster.nodes[3].executed.is_empty(),
        "r3 took r1's word for the leader's"
    );
    cluster.settle_ticking(1);
    assert_eq!(cluster.nodes[3].executed, commands);
}

/// The leader's frontier is the committed prefix of its proposals, and
/// stops at the first one it has not committed.
///
/// With three of five voters down, the leader cannot commit anything
/// more: the frontier stays at the last command committed before, and
/// only moves once they are back.
#[test]
fn the_leaders_frontier_stops_at_the_first_command_it_has_not_committed() {
    let mut cluster = Cluster::new(17, 64);
    assert_eq!(cluster.nodes[0].leader().committed_through(), None);
    let first: Vec<CommandId> = (1..=3).map(|n| cluster.admit(n)).collect();
    cluster.settle();
    assert_eq!(cluster.nodes[0].executed, first);
    assert_eq!(cluster.nodes[0].leader().committed_through(), Some(2));
    cluster.down = vec![1, 2, 3];
    let late: Vec<CommandId> = (4..=5).map(|n| cluster.admit(n)).collect();
    cluster.settle_ticking(2);
    assert!(
        late.iter()
            .all(|c| cluster.nodes[0].phase_of(c) < Some(Phase::Commit)),
        "committed without a majority"
    );
    assert_eq!(cluster.nodes[0].leader().committed_through(), Some(2));
    cluster.down.clear();
    cluster.settle_ticking(3);
    assert_eq!(cluster.nodes[0].leader().committed_through(), Some(4));
    let all: Vec<CommandId> = first.iter().chain(late.iter()).copied().collect();
    for i in 0..VOTERS as usize {
        assert_eq!(cluster.nodes[i].executed, all, "node {i}");
    }
}

/// A proposal a voter has not adopted is re-sent to it at once, and
/// then with a gap that doubles up to `RESEND_BACKOFF_CAP` calls, not on
/// every call (task-d33).
///
/// r4 receives no proposals, so on every call of the re-send it lacks
/// all of them. Re-sent on every call, a voter that is merely behind was
/// sent the same proposals four times a second, each came back as a
/// duplicate adoption, and the leader's lane to it filled with them.
/// Once r4 can hear the leader, the next re-send due reaches it and the
/// re-sends stop.
#[test]
fn a_proposal_a_voter_lacks_is_resent_with_a_growing_gap() {
    let mut cluster = Cluster::new(11, 64);
    cluster.unproposed = vec![4];
    let commands: Vec<CommandId> = (1..=3).map(|n| cluster.admit(n)).collect();
    cluster.settle();
    assert_eq!(cluster.nodes[0].executed, commands);

    // Which calls send r4 anything, over three cap-lengths of calls.
    let calls = 3 * coord_consensus::RESEND_BACKOFF_CAP as usize;
    let mut sent_on = Vec::new();
    for call in 0..calls {
        let Role::Leader(l) = &mut cluster.nodes[0].role else {
            panic!("r0 leads")
        };
        let effects = l.resend_unvoted(coord_consensus::RESEND_PER_VOTER);
        let to_r4 = effects
            .iter()
            .filter(|e| {
                matches!(e, Effect::SendWhenDurable { to, frame, .. }
                    if to.replica == r(4)
                        && matches!(ProtocolMessage::decode(frame), Ok(ProtocolMessage::Proposal(_))))
            })
            .count();
        if to_r4 > 0 {
            assert_eq!(to_r4, commands.len(), "call {call} sent only some of them");
            sent_on.push(call);
        }
    }
    let cap = u64::from(coord_consensus::RESEND_BACKOFF_CAP);
    let mut expected = Vec::new();
    let (mut call, mut gap) = (0, 1);
    while call < calls as u64 {
        expected.push(call as usize);
        call += gap;
        gap = (gap * 2).min(cap);
    }
    assert_eq!(sent_on, expected);
    assert!(
        sent_on.len() < calls / 2,
        "re-sent on {} calls of {calls}",
        sent_on.len()
    );

    // r4 hears the leader again: the next re-send due is adopted, and
    // nothing is sent it after that.
    cluster.unproposed.clear();
    let mut delivered = false;
    for _ in 0..coord_consensus::RESEND_BACKOFF_CAP {
        let Role::Leader(l) = &mut cluster.nodes[0].role else {
            panic!("r0 leads")
        };
        let effects = l.resend_unvoted(coord_consensus::RESEND_PER_VOTER);
        delivered |= !effects.is_empty();
        cluster.handle(0, effects);
        cluster.settle();
    }
    assert!(delivered, "no re-send came due within the cap");
    assert!(
        commands.iter().all(|c| cluster.nodes[4]
            .phase_of(c)
            .is_some_and(|p| p >= Phase::Accept)),
        "r4 did not adopt what was re-sent"
    );
    let Role::Leader(l) = &mut cluster.nodes[0].role else {
        panic!("r0 leads")
    };
    assert!(
        l.resend_unvoted(coord_consensus::RESEND_PER_VOTER)
            .is_empty(),
        "a proposal r4 adopted was sent again"
    );
}

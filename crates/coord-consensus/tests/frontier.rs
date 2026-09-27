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
/// command whose turn has come, and which the leader committed, is now
/// let into the full table.
#[test]
fn a_follower_whose_table_filled_while_it_could_not_learn_catches_up() {
    let (mut cluster, commands) = stalled(7);
    cluster.settle_ticking(commands.len());
    assert_eq!(cluster.nodes[3].executed, commands);
}

/// A follower behind the frontier asks first for the payloads of the
/// commands the leader committed, in the leader's order.
///
/// Its missing set is every later command, and the ask is bounded and
/// rotates through it. The one whose turn has come was reached once per
/// rotation, and nothing executes before it.
#[test]
fn a_follower_asks_first_for_what_the_leader_committed_in_its_order() {
    let (mut cluster, commands) = stalled(19);
    // One round for the frontier to reach r3, so that its next ask knows
    // what the leader committed.
    cluster.tick();
    cluster.settle();
    let due: Vec<CommandId> = {
        let Role::Follower(f) = &cluster.nodes[3].role else {
            unreachable!()
        };
        let missing = f.missing_payloads();
        commands
            .iter()
            .filter(|c| missing.contains(c) && f.held().contains_key(c))
            .copied()
            .collect()
    };
    assert!(due.len() > 8, "only {} missing", due.len());
    cluster.asks.clear();
    cluster.tick();
    cluster.settle();
    let (_, ask) = cluster
        .asks
        .iter()
        .find(|(from, _)| *from == 3)
        .cloned()
        .expect("r3 asked");
    assert_eq!(ask[..4], due[..4], "the ask led with other commands");
    cluster.settle_ticking(commands.len());
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

/// A follower that received the proposal before a payload under other
/// attested facts adopts nothing, and the frontier executes nothing.
///
/// The proposal reaches r3 first and is held. r3's own submission of the
/// same command then arrives under another admission receipt. Adopted,
/// the frontier committed it and r3 executed a command under facts the
/// quorum never admitted; the adoption is refused instead, and reported.
#[test]
fn a_proposal_held_before_a_conflicting_payload_is_not_adopted() {
    let mut cluster = Cluster::new(29, 64);
    cluster.deaf = vec![3];
    let c = cluster.admit_to(1, &[0, 1, 2, 4], 9);
    let pending: Vec<(ReplicaId, Vec<u8>)> = cluster.nodes[3].inbox.drain(..).collect();
    for (from, frame) in pending {
        cluster.deliver(3, from, frame);
    }
    assert_eq!(cluster.admit_to(1, &[3], 8), c);
    cluster.settle_ticking(2);
    assert_eq!(cluster.nodes[0].executed, vec![c]);
    assert!(
        cluster.nodes[3].executed.is_empty(),
        "r3 executed other facts"
    );
    assert!(cluster.nodes[3].phase_of(&c) < Some(Phase::Accept));
    let rejections = cluster.nodes[3].follower_mut().take_rejections();
    assert!(
        rejections.iter().any(
            |r| matches!(r, FollowerRejection::AdmissionConflict { command, .. } if *command == c)
        ),
        "the conflict was not reported"
    );
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

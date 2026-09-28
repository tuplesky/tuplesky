//! task-d08: a voter behind by more than a table catches up from a
//! peer's executed history.
//!
//! A voter cut off while the others went on comes back to a leader that
//! retired, long ago, the commands it is missing, and re-sends only what
//! is still in its table: the voter holds proposals whose dependencies it
//! will never be offered, and fills its table with submissions it cannot
//! learn. These tests cut a voter off under a live leader, bring it back,
//! and check that pages of the leader's executed history -- served here
//! from the leader's durable rows, as `coordd` serves them -- bring it up
//! past a full table; that the table drains after; that a page from
//! another ballot is dropped; that a pulled command executed otherwise
//! than its donor stops the voter; that a restart in the middle resumes
//! without executing anything twice; and that nothing is left reported
//! as committed with other than the decided dependencies.

use std::collections::VecDeque;

use coord_consensus::{
    AppliedOutcome, BallotConfiguration, CatchUpEntry, ConfigurationIdentity, Follower,
    FollowerConfig, FollowerRejection, Leader, LeaderConfig, MAX_CATCH_UP_COMMANDS, Phase,
    ProtocolMessage, ReplicaRole,
};
use coord_core::capability::{AdmissionReceipt, AttestedAdmission, VerifierToken};
use coord_core::effect::{BootId, Effect, PeerId};
use coord_core::event::{
    AdmittedRequest, AuthenticatedPeerMessage, Event, PeerProvenance, StorageError, StorageEvent,
};
use coord_core::machine::DeterministicMachine;
use coord_sim::storage::StorageModel;
use coord_store_api::registry::Collection;
use coord_types::identity::Digest32;
use coord_types::ids::*;
use coord_types::logical_v1::{CanonicalOperation, LogicalRequest, PutOp};
use coord_types::wire_v1::{MessageV1, RequestV1};
use coord_types::{CommandId, RetryKey};

/// Voters in these tests.
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

    fn follower_mut(&mut self) -> &mut Follower {
        match &mut self.role {
            Role::Follower(f) => f,
            Role::Leader(_) => panic!("leader"),
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
    /// Followers that ask r0 to catch up, once per settle.
    catching: Vec<usize>,
    /// Whether an ask is answered from the donor's rows. Off, asks are
    /// dropped, and a test delivers pages itself.
    serve: bool,
    /// Pages served.
    pages: usize,
    /// A node whose next persisted batch fails, as a fence at a higher
    /// promise refuses one, and how many more of its batches fail.
    failing: Option<(usize, usize)>,
    /// Batches failed so far.
    failed: usize,
    /// Payload values the applier produced, by node, to compare with.
    fork: Option<(usize, CommandId)>,
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
            catching: Vec::new(),
            serve: true,
            pages: 0,
            failing: None,
            failed: 0,
            fork: None,
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
                    if let Some((node, left)) = self.failing
                        && node == i
                        && left > 0
                    {
                        self.failing = Some((node, left - 1));
                        self.failed += 1;
                        let more = self.nodes[i].step(Event::Storage(StorageEvent::Failed {
                            barrier_id: barrier,
                            error: StorageError::DefinitelyNotCommitted,
                        }));
                        self.handle(i, more);
                        continue;
                    }
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
                        // Answered by the runtime from the donor's rows,
                        // which is what this does.
                        ProtocolMessage::CatchUpRequest { ballot, after } => {
                            if self.serve {
                                let page = self.page(dest as usize, *ballot, *after);
                                self.pages += 1;
                                self.nodes[i].inbox.push_back((r(dest), page.encode()));
                            }
                            continue;
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
                    let mut outcome = AppliedOutcome {
                        position,
                        revision: None,
                        result_digest: Digest32(c.0.0),
                        response: c.0.0.to_vec(),
                    };
                    if self.fork == Some((i, c)) {
                        outcome.result_digest = Digest32([0xee; 32]);
                    }
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
            // The runtime's catch-up pacer, once per node per settle: the
            // machine itself asks again when a full page is executed.
            for i in self.catching.clone() {
                if asked.contains(&(i + 100)) {
                    continue;
                }
                let f = self.nodes[i].follower_mut();
                if f.catching_up() || !f.holds_unexecuted() {
                    continue;
                }
                let effects = f.request_catch_up(r(0));
                asked.push(i + 100);
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

    /// What `coordd` serves for an ask at `ballot` after `after`: the
    /// donor's executed commands from there, with the payload and the
    /// dependency row it holds durably for each.
    fn page(&self, donor: usize, ballot: Ballot, after: ExecutionPosition) -> ProtocolMessage {
        let node = &self.nodes[donor];
        let rows = node.storage.durable_rows();
        let entries = node
            .executed
            .iter()
            .enumerate()
            .skip(after.get() as usize)
            .take(MAX_CATCH_UP_COMMANDS)
            .map(|(i, c)| {
                let payload = rows
                    .iter()
                    .find(|(k, key, _)| *k == Collection::PayloadV1.id().0 && key == c.as_bytes())
                    .map(|(_, _, v)| coord_consensus::decode_payload(v).unwrap())
                    .expect("the donor holds the payload");
                let row = coord_consensus::dependency_key(epoch(), c);
                let decided = rows
                    .iter()
                    .find(|(k, key, _)| *k == Collection::ProtocolV1.id().0 && *key == row)
                    .map(|(_, _, v)| coord_consensus::decode_dependency(v).unwrap());
                CatchUpEntry {
                    command: *c,
                    payload,
                    decided,
                    position: ExecutionPosition::new(i as u64 + 1).unwrap(),
                    revision: None,
                    result_digest: Digest32(c.0.0),
                }
            })
            .collect();
        ProtocolMessage::CatchUpPage {
            ballot,
            after,
            through: ExecutionPosition::new(node.executed.len() as u64).unwrap(),
            entries,
        }
    }

    fn follower(&self, i: usize) -> &Follower {
        match &self.nodes[i].role {
            Role::Follower(f) => f,
            Role::Leader(_) => panic!("leader"),
        }
    }

    /// The dependency rows node `i` holds durably.
    fn dependency_rows(&self, i: usize) -> Vec<(CommandId, coord_consensus::CommandRecord)> {
        self.nodes[i]
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
            .collect()
    }

    /// Execute at most `n` of node `i`'s executable commands.
    fn execute_at_most(&mut self, i: usize, n: usize) {
        for _ in 0..n {
            let Some(c) = self.follower(i).next_executable() else {
                return;
            };
            let position = self.follower(i).executed_through().checked_next().unwrap();
            let outcome = AppliedOutcome {
                position,
                revision: None,
                result_digest: Digest32(c.0.0),
                response: c.0.0.to_vec(),
            };
            let effects = self.nodes[i].follower_mut().applied(c, &outcome).unwrap();
            self.nodes[i].executed.push(c);
            self.handle(i, effects);
        }
    }

    fn deliver_page(&mut self, to: usize, from: u8, page: &ProtocolMessage) {
        self.deliver(to, r(from), page.encode());
    }
}

/// Cut r4 off while the domain goes on for five tables' worth, then
/// bring it back under load: the leader re-sends only what its table
/// still holds, so r4 is left with proposals it cannot adopt and
/// submissions it cannot learn, and its table fills.
fn left_behind(seed: u64) -> (Cluster, usize) {
    let capacity = 8;
    let mut cluster = Cluster::new(seed, capacity);
    cluster.down = vec![4];
    for seq in 1..=40 {
        cluster.admit(seq);
        cluster.settle();
    }
    cluster.settle_ticking(2);
    cluster.down.clear();
    for seq in 41..=60 {
        cluster.admit(seq);
        cluster.settle();
    }
    cluster.settle_ticking(3);
    assert!(
        cluster.nodes[4].executed.len() < cluster.nodes[0].executed.len(),
        "r4 caught up without catch-up: the test shows nothing"
    );
    assert!(cluster.follower(4).holds_unexecuted());
    (cluster, capacity)
}

/// The n2/n4 shape (task-d08): a voter behind by more than a table, and
/// with its table full, is brought up from the leader's executed history,
/// its table drains, and it takes new work without backpressure.
#[test]
fn a_voter_past_a_full_table_catches_up_and_its_table_drains() {
    for seed in 1..=4 {
        let (mut cluster, capacity) = left_behind(seed);
        let refused = cluster.nodes[4].follower_mut().take_rejections();
        assert!(
            refused.contains(&FollowerRejection::Backpressure),
            "seed {seed}: r4's table never filled: {refused:?}"
        );
        cluster.catching = vec![4];
        cluster.settle_ticking(8);
        assert_eq!(
            cluster.nodes[4].executed, cluster.nodes[0].executed,
            "seed {seed}: r4 did not execute the leader's history in its order"
        );
        assert!(cluster.pages > 0, "seed {seed}");
        let f = cluster.follower(4);
        assert!(!f.holds_unexecuted(), "seed {seed}: r4 still holds work");
        assert!(!f.catching_up(), "seed {seed}");
        let live = f
            .table()
            .records()
            .filter(|(_, r)| r.payload.is_some() && r.phase < Phase::Executed)
            .count();
        assert_eq!(live, 0, "seed {seed}: the table did not drain");
        assert!(
            f.table().records().count() <= capacity,
            "seed {seed}: pulled commands were left in the table past its capacity: {}",
            f.table().records().count()
        );
        // And it takes new work as any voter does.
        cluster.nodes[4].follower_mut().take_rejections();
        for seq in 61..=61 + capacity as u64 * 2 {
            cluster.admit(seq);
            cluster.settle();
        }
        cluster.settle_ticking(2);
        let refused = cluster.nodes[4].follower_mut().take_rejections();
        assert!(
            !refused.contains(&FollowerRejection::Backpressure),
            "seed {seed}: r4 still refuses for backpressure: {refused:?}"
        );
        assert_eq!(
            cluster.nodes[4].executed, cluster.nodes[0].executed,
            "seed {seed}"
        );
    }
}

/// Nothing a voter caught up is left reported as committed with other
/// dependencies than the ones decided (task-d08): every dependency row it
/// holds at COMMIT or beyond names the leader's dependencies for that
/// command, and the leader holds one.
#[test]
fn nothing_caught_up_is_reported_committed_with_other_than_the_decided_deps() {
    let (mut cluster, _) = left_behind(7);
    cluster.catching = vec![4];
    cluster.settle_ticking(8);
    assert_eq!(cluster.nodes[4].executed, cluster.nodes[0].executed);
    let decided: std::collections::BTreeMap<_, _> =
        cluster.dependency_rows(0).into_iter().collect();
    let mut checked = 0;
    for (command, record) in cluster.dependency_rows(4) {
        if record.phase < Phase::Commit {
            continue;
        }
        let leader = decided
            .get(&command)
            .unwrap_or_else(|| panic!("r4 reports {command:?} committed; the leader has no row"));
        assert_eq!(record.deps, leader.deps, "{command:?}");
        checked += 1;
    }
    assert!(checked > 0);
}

/// A page answered at a ballot the voter is not synchronized at, or by a
/// peer that is not a voter of its configuration, is dropped whole
/// (task-d08).
#[test]
fn a_page_from_another_ballot_or_a_stranger_is_dropped() {
    let (mut cluster, _) = left_behind(3);
    let before = cluster.nodes[4].executed.len();
    let after = cluster.follower(4).executed_through();
    let other = Ballot {
        number: 7,
        ..ballot0()
    };
    let page = cluster.page(0, other, after);
    cluster.nodes[4].follower_mut().take_rejections();
    cluster.deliver_page(4, 0, &page);
    let refused = cluster.nodes[4].follower_mut().take_rejections();
    assert!(
        refused.contains(&FollowerRejection::CatchUpDropped {
            from: r(0),
            ballot: other
        }),
        "{refused:?}"
    );
    let page = cluster.page(0, ballot0(), after);
    cluster.deliver_page(4, 9, &page);
    let refused = cluster.nodes[4].follower_mut().take_rejections();
    assert!(
        refused.contains(&FollowerRejection::CatchUpDropped {
            from: r(9),
            ballot: ballot0()
        }),
        "{refused:?}"
    );
    cluster.settle();
    assert_eq!(cluster.nodes[4].executed.len(), before);
    assert!(!cluster.follower(4).catching_up());
    // The same page, at the right ballot from a voter, is taken.
    cluster.deliver_page(4, 0, &page);
    cluster.settle();
    assert!(cluster.nodes[4].executed.len() > before);
}

/// A pulled command this voter executes to another result than its donor
/// stops it (task-d08): it executes nothing more, and says what the two
/// executions were.
#[test]
fn a_pulled_command_executed_otherwise_stops_the_voter() {
    let (mut cluster, _) = left_behind(5);
    let after = cluster.follower(4).executed_through();
    let page = cluster.page(0, ballot0(), after);
    let ProtocolMessage::CatchUpPage { entries, .. } = &page else {
        unreachable!()
    };
    let forked = entries[1].command;
    cluster.fork = Some((4, forked));
    let before = cluster.nodes[4].executed.len();
    cluster.deliver_page(4, 0, &page);
    cluster.settle();
    let f = cluster.follower(4);
    let divergence = f.catch_up_divergence().expect("r4 stopped");
    assert_eq!(divergence.command, forked);
    assert_eq!(divergence.donor.donor, r(0));
    assert_eq!(divergence.donor.ballot, ballot0());
    assert_eq!(divergence.donor.result_digest, Digest32(forked.0.0));
    let own = divergence.own.expect("r4 executed it");
    assert_eq!(own.result_digest, Digest32([0xee; 32]));
    assert_eq!(own.position, divergence.donor.position);
    assert_eq!(f.next_executable(), None);
    assert_eq!(cluster.nodes[4].executed.len(), before + 2);
    // Nothing more is executed, whatever arrives.
    cluster.catching = vec![4];
    cluster.settle_ticking(3);
    assert_eq!(cluster.nodes[4].executed.len(), before + 2);
}

/// A voter restarted in the middle of a page resumes from its durable
/// frontier and executes nothing twice (task-d08): a pulled command whose
/// decision was durable executes as any committed command does, and the
/// next page starts where execution stands.
#[test]
fn a_restart_in_the_middle_of_a_page_resumes_without_executing_twice() {
    let (mut cluster, _) = left_behind(9);
    cluster.serve = false;
    let after = cluster.follower(4).executed_through();
    let page = cluster.page(0, ballot0(), after);
    cluster.deliver_page(4, 0, &page);
    cluster.execute_at_most(4, 3);
    cluster.restart(4);
    cluster.serve = true;
    cluster.catching = vec![4];
    cluster.settle_ticking(8);
    let executed = &cluster.nodes[4].executed;
    let unique: std::collections::BTreeSet<_> = executed.iter().collect();
    assert_eq!(unique.len(), executed.len(), "r4 executed a command twice");
    assert_eq!(*executed, cluster.nodes[0].executed);
}

/// A command the donor keeps no decided record of is executed at the
/// donor's position and goes to history: this voter keeps no dependency
/// row of it, and its table no record (task-d08).
#[test]
fn a_command_the_donor_keeps_no_row_of_goes_to_history() {
    let (mut cluster, _) = left_behind(11);
    let after = cluster.follower(4).executed_through();
    let mut page = cluster.page(0, ballot0(), after);
    let ProtocolMessage::CatchUpPage { entries, .. } = &mut page else {
        unreachable!()
    };
    let bare = entries[0].command;
    entries[0].decided = None;
    entries.truncate(1);
    cluster.deliver_page(4, 0, &page);
    cluster.settle();
    let f = cluster.follower(4);
    assert_eq!(f.catch_up_divergence(), None);
    // At the donor's position; the short page is followed by the rest.
    assert_eq!(
        cluster.nodes[4].executed.get(after.get() as usize),
        Some(&bare)
    );
    assert_eq!(f.table().phase_of(&bare), Some(Phase::Executed));
    assert!(
        f.table().record(&bare).is_none(),
        "the record went to history"
    );
    assert!(
        cluster.dependency_rows(4).iter().all(|(c, _)| *c != bare),
        "r4 kept a dependency row the donor did not have"
    );
}

/// A pulled command whose installation batch fails is installed again,
/// rather than left waiting on a batch that will never be durable: the
/// voter does not wedge, and catches up once its store takes the batch
/// (task-d08).
#[test]
fn a_failed_installation_is_installed_again() {
    let (mut cluster, _) = left_behind(13);
    let before = cluster.nodes[4].executed.len();
    let after = cluster.follower(4).executed_through();
    let page = cluster.page(0, ballot0(), after);
    cluster.failing = Some((4, 2));
    cluster.deliver_page(4, 0, &page);
    assert_eq!(cluster.failed, 2, "both failures reached r4");
    cluster.settle();
    assert!(
        cluster.nodes[4].executed.len() > before,
        "r4 wedged on a failed installation"
    );
    cluster.catching = vec![4];
    cluster.settle_ticking(8);
    assert_eq!(cluster.nodes[4].executed, cluster.nodes[0].executed);
    assert!(!cluster.follower(4).holds_unexecuted());
}

/// A page cut short of what the donor executed -- by the byte bound as
/// much as the command bound -- is followed by the next ask as soon as it
/// is executed, without waiting for the pacer (task-d08).
#[test]
fn a_page_short_of_the_donors_frontier_is_followed_at_once() {
    let (mut cluster, _) = left_behind(17);
    let after = cluster.follower(4).executed_through();
    let mut page = cluster.page(0, ballot0(), after);
    let ProtocolMessage::CatchUpPage { entries, .. } = &mut page else {
        unreachable!()
    };
    // What a donor sends when three commands reach the byte bound.
    entries.truncate(3);
    assert!(cluster.catching.is_empty(), "no pacer in this test");
    cluster.deliver_page(4, 0, &page);
    cluster.settle();
    assert_eq!(
        cluster.nodes[4].executed, cluster.nodes[0].executed,
        "r4 stopped after a short page"
    );
}

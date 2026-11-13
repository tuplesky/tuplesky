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
    AppliedOutcome, BallotConfiguration, CatchUpEntry, CatchUpPaths, ConfigurationIdentity,
    Follower, FollowerConfig, FollowerRejection, Leader, LeaderConfig, MAX_CATCH_UP_BYTES,
    MAX_CATCH_UP_COMMANDS, Phase, ProtocolMessage, ReplicaRole,
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
    /// Nodes whose batches are written and never made durable: a crash
    /// of theirs loses them (task-d25).
    held: Vec<usize>,
    /// Batches each node persisted.
    batches: Vec<usize>,
    /// Every catch-up ask sent: (from, after).
    catch_up_asks: Vec<(usize, u64)>,
    /// The most commands a served page carries: a donor whose byte bound
    /// cuts pages short.
    page_limit: usize,
    /// The length of each put's value: 1, or large values for the byte
    /// bound.
    value_len: usize,
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
            held: Vec::new(),
            batches: vec![0; VOTERS as usize],
            catch_up_asks: Vec::new(),
            page_limit: MAX_CATCH_UP_COMMANDS,
            value_len: 1,
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
                    self.batches[i] += 1;
                    let node = &mut self.nodes[i];
                    node.storage.submit(batch);
                    if self.held.contains(&i) {
                        continue;
                    }
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
                            self.catch_up_asks.push((i, after.get()));
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
            PeerProvenance::from_transport(
                from,
                ReplicaIncarnation::new(1).unwrap(),
                coord_types::wire_v1::PeerRole::Voter,
                1,
            ),
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
                value: vec![key; self.value_len],
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
            .take(self.page_limit)
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

/// Every map forgetting drops stays bounded by the table at every voter,
/// including one that falls behind by more than a table and catches up by
/// pages: before it falls behind, while the others run on without it,
/// once it has caught up, and while it takes new work after (task-d111
/// item 4 (d), #221 review). The bound is the backstop sweep's threshold,
/// four times the capacity: forgetting in order is to keep every map
/// under it without a sweep. Once every voter has executed everything,
/// no map is larger than the voter's ledger: an entry made for a
/// command after it was forgotten, as a peer's late vote made one at r4
/// before `collect` dropped those, stays over it.
#[test]
fn every_history_map_stays_bounded_at_a_voter_that_falls_behind_and_catches_up() {
    fn maps(cluster: &Cluster, i: usize) -> coord_consensus::HistoryMaps {
        match &cluster.nodes[i].role {
            Role::Leader(l) => l.history_maps(),
            Role::Follower(f) => f.history_maps(),
        }
    }
    fn check(cluster: &Cluster, seed: u64, at: &str, threshold: usize) {
        for i in 0..cluster.nodes.len() {
            let m = maps(cluster, i);
            assert!(
                m.largest() < threshold,
                "seed {seed}, {at}: r{i}'s history maps reached {m:?}"
            );
        }
    }
    // Once every voter has executed everything, an entry is one of a
    // command the ledger still records: one more, in any map, is of a
    // command it has forgotten, which nothing but a sweep would drop.
    fn settled(cluster: &Cluster, seed: u64, at: &str) {
        for i in 0..cluster.nodes.len() {
            let (ledger, m) = match &cluster.nodes[i].role {
                Role::Leader(l) => (l.ledger().len(), l.history_maps()),
                Role::Follower(f) => (f.ledger().len(), f.history_maps()),
            };
            assert!(
                m.largest() <= ledger,
                "seed {seed}, {at}: r{i} keeps more than its ledger's {ledger}: {m:?}"
            );
        }
    }
    for seed in 1..=4 {
        let (mut cluster, capacity) = left_behind(seed);
        let threshold = capacity * 4;
        check(&cluster, seed, "left behind", threshold);
        cluster.catching = vec![4];
        cluster.settle_ticking(8);
        assert_eq!(
            cluster.nodes[4].executed, cluster.nodes[0].executed,
            "seed {seed}: r4 did not catch up"
        );
        check(&cluster, seed, "caught up", threshold);
        settled(&cluster, seed, "caught up");
        for seq in 61..=60 + capacity as u64 * 16 {
            cluster.admit(seq);
            cluster.settle();
            check(&cluster, seed, "after", threshold);
        }
        cluster.settle_ticking(2);
        check(&cluster, seed, "at the end", threshold);
        settled(&cluster, seed, "at the end");
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
/// (task-d08). It is installed again at its next event, not at the
/// failure (#197 review).
#[test]
fn a_failed_installation_is_installed_again() {
    let (mut cluster, _) = left_behind(13);
    let before = cluster.nodes[4].executed.len();
    let after = cluster.follower(4).executed_through();
    let page = cluster.page(0, ballot0(), after);
    cluster.failing = Some((4, 2));
    cluster.deliver_page(4, 0, &page);
    // The window failed is installed again at the next event that is not
    // a failure, not at the failure itself (#197 review).
    assert_eq!(cluster.failed, 1, "one failure a delivery");
    cluster.tick();
    cluster.settle();
    assert_eq!(cluster.failed, 2, "both failures reached r4");
    // The next event is the leader's commit announcement at the next
    // tick. Proposals the leader resent used to reach r4 in the same
    // settle, but the leader forgets an executed command by order now and
    // resends none of its proposals (task-d111 item 4 (d)).
    cluster.tick();
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

/// A window refused by a store that refuses every batch, as a fenced one
/// does: one delivered page fails one batch, not a loop (#197 review).
/// Installed again at the failure itself, the page failed 200.
#[test]
fn a_store_that_refuses_every_batch_fails_one_window_a_page() {
    let (mut cluster, _) = left_behind(13);
    let after = cluster.follower(4).executed_through();
    let page = cluster.page(0, ballot0(), after);
    cluster.failing = Some((4, 200));
    cluster.deliver_page(4, 0, &page);
    assert_eq!(
        cluster.failed, 1,
        "the window was written again at its failure"
    );
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

impl Cluster {
    /// What the runtime's pacer does once after a boot: ask r0, whatever
    /// the node holds.
    fn ask(&mut self, i: usize) {
        let effects = self.nodes[i].follower_mut().request_catch_up(r(0));
        self.handle(i, effects);
        self.settle();
    }
}

fn entries_of(page: &ProtocolMessage) -> &[CatchUpEntry] {
    let ProtocolMessage::CatchUpPage { entries, .. } = page else {
        unreachable!()
    };
    entries
}

/// A page goes in as one window, in one batch, ahead of execution
/// (task-d25): every command of it is a durable commit before the first
/// executes, and they execute in the donor's positions.
#[test]
fn a_page_installs_as_one_window_in_one_batch() {
    for seed in [19, 23, 29] {
        let (mut cluster, _) = left_behind(seed);
        let after = cluster.follower(4).executed_through();
        let page = cluster.page(0, ballot0(), after);
        let entries = entries_of(&page).to_vec();
        assert!(
            entries.len() > 8,
            "seed {seed}: a page of {}",
            entries.len()
        );
        let batches = cluster.batches[4];
        cluster.deliver_page(4, 0, &page);
        assert_eq!(
            cluster.batches[4] - batches,
            1,
            "seed {seed}: the page did not go in as one batch"
        );
        for e in &entries {
            assert_eq!(
                cluster.follower(4).table().phase_of(&e.command),
                Some(Phase::Commit),
                "seed {seed}"
            );
        }
        cluster.settle();
        let from = after.get() as usize;
        assert_eq!(
            cluster.nodes[4].executed[from..from + entries.len()],
            cluster.nodes[0].executed[from..from + entries.len()],
            "seed {seed}: the window ran out of the donor's positions"
        );
        assert_eq!(cluster.follower(4).catch_up_divergence(), None);
    }
}

/// A command the donor kept no dependency row of has nothing to chain
/// it to the window before it, so it is a window of its own (task-d25).
#[test]
fn a_command_the_donor_kept_no_row_of_is_a_window_of_its_own() {
    let (mut cluster, _) = left_behind(31);
    let after = cluster.follower(4).executed_through();
    let mut page = cluster.page(0, ballot0(), after);
    let ProtocolMessage::CatchUpPage { entries, .. } = &mut page else {
        unreachable!()
    };
    entries[3].decided = None;
    let entries = entries.clone();
    let batches = cluster.batches[4];
    cluster.deliver_page(4, 0, &page);
    assert_eq!(cluster.batches[4] - batches, 1);
    let f = cluster.follower(4);
    for e in &entries[..3] {
        assert_eq!(f.table().phase_of(&e.command), Some(Phase::Commit));
    }
    assert!(f.table().phase_of(&entries[3].command) < Some(Phase::Commit));
    cluster.settle();
    // Three windows: the three before it, it, and the rest.
    assert_eq!(cluster.batches[4] - batches, 3);
    let from = after.get() as usize;
    assert_eq!(
        cluster.nodes[4].executed[from..from + entries.len()],
        cluster.nodes[0].executed[from..from + entries.len()]
    );
    assert_eq!(cluster.follower(4).catch_up_divergence(), None);
}

/// A crash at every point of a window -- before its batch is durable,
/// and after each of its commands executes -- resumes without executing
/// anything twice, and the first ask after the restart starts at the
/// frontier read at boot, so what the restart executed of the window is
/// compared with the donor's (task-d25).
#[test]
fn a_crash_at_every_point_of_a_window_resumes_without_executing_twice() {
    let (probe, _) = left_behind(37);
    let after = probe.follower(4).executed_through();
    let window = entries_of(&probe.page(0, ballot0(), after)).len();
    // `None`: the batch never became durable.
    let points = std::iter::once(None).chain((0..=window).map(Some));
    for point in points {
        let (mut cluster, _) = left_behind(37);
        cluster.serve = false;
        let page = cluster.page(0, ballot0(), after);
        match point {
            None => {
                cluster.held = vec![4];
                cluster.deliver_page(4, 0, &page);
                cluster.held.clear();
            }
            Some(k) => {
                cluster.deliver_page(4, 0, &page);
                cluster.execute_at_most(4, k);
            }
        }
        let boot = cluster.nodes[4].executed.len() as u64;
        cluster.restart(4);
        cluster.settle();
        cluster.serve = true;
        let asked = cluster.catch_up_asks.len();
        cluster.ask(4);
        assert_eq!(
            cluster.catch_up_asks.get(asked),
            Some(&(4, boot)),
            "{point:?}: the first ask after the boot did not start at its frontier"
        );
        cluster.catching = vec![4];
        cluster.settle_ticking(8);
        let executed = &cluster.nodes[4].executed;
        let unique: std::collections::BTreeSet<_> = executed.iter().collect();
        assert_eq!(unique.len(), executed.len(), "{point:?}: executed twice");
        assert_eq!(*executed, cluster.nodes[0].executed, "{point:?}");
        assert_eq!(cluster.follower(4).catch_up_divergence(), None, "{point:?}");
    }
}

/// What a restart executes of a window its crash left installed is
/// compared with the donor by the first page after the boot: a
/// difference stops the voter as a pulled command's does (task-d25).
#[test]
fn a_window_executed_after_a_restart_is_compared_with_the_donor() {
    let (mut cluster, _) = left_behind(41);
    cluster.serve = false;
    let after = cluster.follower(4).executed_through();
    let page = cluster.page(0, ballot0(), after);
    let forked = entries_of(&page)[3].command;
    cluster.deliver_page(4, 0, &page);
    cluster.execute_at_most(4, 1);
    cluster.restart(4);
    cluster.fork = Some((4, forked));
    let before = cluster.nodes[4].executed.len();
    cluster.settle();
    assert!(
        cluster.nodes[4].executed.len() > before + 3,
        "the restart did not execute the rest of the window"
    );
    assert_eq!(cluster.follower(4).catch_up_divergence(), None);
    cluster.serve = true;
    cluster.ask(4);
    let f = cluster.follower(4);
    let divergence = f.catch_up_divergence().expect("r4 stopped");
    assert_eq!(divergence.command, forked);
    assert_eq!(divergence.donor.result_digest, Digest32(forked.0.0));
    let own = divergence.own.expect("r4 executed it");
    assert_eq!(own.result_digest, Digest32([0xee; 32]));
    assert_eq!(own.position, divergence.donor.position);
    assert_eq!(f.next_executable(), None);
}

/// A window installed and not yet executed keeps its voter from
/// campaigning until it has run (task-d25): a won campaign would change
/// the role, and the window's comparison with the donor with it.
#[test]
fn a_voter_holding_a_window_does_not_campaign_until_it_ran() {
    let (mut cluster, _) = left_behind(43);
    let after = cluster.follower(4).executed_through();
    let page = cluster.page(0, ballot0(), after);
    cluster.deliver_page(4, 0, &page);
    let next = Ballot {
        number: 1,
        leader: r(4),
        ..ballot0()
    };
    let effects = cluster.nodes[4].follower_mut().campaign(next);
    assert!(effects.is_empty());
    let refused = cluster.nodes[4].follower_mut().take_rejections();
    assert!(
        refused.contains(&FollowerRejection::CatchingUp),
        "{refused:?}"
    );
}

/// Every execution of a commit restored at boot is compared, however many
/// pages that takes (task-d25, Codex review on #123): with the donor's
/// pages cut to three commands, the restart's execution of the eleventh
/// command of the window is still reached, and its difference stops the
/// voter.
#[test]
fn every_restored_commit_is_compared_however_many_pages_it_takes() {
    let (mut cluster, _) = left_behind(47);
    cluster.serve = false;
    let after = cluster.follower(4).executed_through();
    let page = cluster.page(0, ballot0(), after);
    let entries = entries_of(&page).to_vec();
    assert!(entries.len() > 12, "a window of {}", entries.len());
    let forked = entries[10].command;
    cluster.deliver_page(4, 0, &page);
    cluster.restart(4);
    cluster.fork = Some((4, forked));
    cluster.settle();
    assert!(cluster.nodes[4].executed.len() >= after.get() as usize + entries.len());
    assert_eq!(cluster.follower(4).catch_up_divergence(), None);
    cluster.serve = true;
    cluster.page_limit = 3;
    let asked = cluster.catch_up_asks.len();
    cluster.ask(4);
    let f = cluster.follower(4);
    let divergence = f.catch_up_divergence().expect("r4 stopped");
    assert_eq!(divergence.command, forked);
    assert_eq!(
        divergence.own.map(|o| o.result_digest),
        Some(Digest32([0xee; 32]))
    );
    assert!(
        cluster.catch_up_asks.len() - asked >= 4,
        "the comparison went on page after page: {:?}",
        &cluster.catch_up_asks[asked..]
    );
}

/// task-d33 (protocol_sim row 5, three voters, seed 39): a voter whose
/// synchronized selection held x at ACCEPT, and which then pulled x's
/// decision with other dependencies, reports the decision. With the
/// selection overlaid on it, the report presented the selection's
/// acceptance as this voter's commit, and the next selection found two
/// decisions of x.
#[test]
fn a_pulled_decision_is_reported_over_the_selection_the_voter_held() {
    let mut cluster = Cluster::new(71, 32);
    cluster.down = vec![4];
    let a = cluster.admit(1);
    let x = cluster.admit(2);
    cluster.settle();
    cluster.down.clear();
    let decided = cluster
        .dependency_rows(0)
        .into_iter()
        .find(|(c, _)| *c == x)
        .map(|(_, record)| record.deps)
        .expect("r0 decided x");
    assert!(!decided.is_empty());

    // r4 synchronizes a ballot whose selection holds x at ACCEPT with no
    // dependencies.
    let b1 = Ballot {
        number: 1,
        ..ballot0()
    };
    cluster.deliver(
        4,
        r(0),
        ProtocolMessage::NewLeader {
            ballot: b1,
            executed: ExecutionPosition::ZERO,
        }
        .encode(),
    );
    let decision = coord_consensus::SyncDecision {
        ballot: b1,
        source_ballot: ballot0(),
        entries: [(
            x,
            coord_consensus::SyncEntry {
                command: x,
                phase: Phase::Accept,
                deps: Vec::new(),
                path: Digest32([7; 32]),
                paths: Vec::new(),
                seqnum: 0,
                admission: None,
            },
        )]
        .into_iter()
        .collect(),
        reproposed: Default::default(),
    };
    cluster.deliver(4, r(0), ProtocolMessage::Sync(decision).encode());
    assert_eq!(cluster.follower(4).quorum().ballot(), b1);

    // It pulls x's decision and executes it.
    let page = cluster.page(0, b1, ExecutionPosition::ZERO);
    cluster.deliver_page(4, 0, &page);
    cluster.execute_at_most(4, 2);
    assert_eq!(cluster.nodes[4].executed, vec![a, x]);
    let report = cluster.follower(4).report(Ballot {
        number: 2,
        ..ballot0()
    });
    let entry = report
        .entries
        .iter()
        .find(|e| e.command == x)
        .expect("x is reported");
    assert!(entry.phase >= Phase::Commit);
    assert_eq!(
        entry.deps, decided,
        "the selection's acceptance was reported"
    );
}

/// A commit restored at boot that executes in a pulled window was
/// compared with the donor there, so nothing waits for it any more
/// (#213 review): once caught up, a voter that then executes on its own
/// asks from its own frontier, not from where the last page stopped.
#[test]
fn a_restored_commit_pulled_in_a_window_leaves_nothing_waiting() {
    let (mut cluster, _) = left_behind(41);
    cluster.serve = false;
    let after = cluster.follower(4).executed_through();
    let page = cluster.page(0, ballot0(), after);
    let window = entries_of(&page).len();
    // The window goes down and r4 restarts before executing any of it:
    // every command of it is a commit restored at boot.
    cluster.deliver_page(4, 0, &page);
    cluster.restart(4);
    // The same page, delivered before the restart's own execution, so
    // the restored commits execute in a pulled window.
    cluster.deliver_page(4, 0, &page);
    cluster.settle();
    assert_eq!(cluster.follower(4).catch_up_divergence(), None);
    assert!(
        cluster.follower(4).catch_up_counts().1 >= window as u64,
        "the restored commits did not execute through the pulled window: {:?}",
        cluster.follower(4).catch_up_counts()
    );
    cluster.serve = true;
    cluster.catching = vec![4];
    cluster.settle_ticking(8);
    cluster.catching.clear();
    assert_eq!(cluster.nodes[4].executed, cluster.nodes[0].executed);
    for seq in 61..=64 {
        cluster.admit(seq);
        cluster.settle();
    }
    cluster.settle_ticking(2);
    assert_eq!(
        cluster.nodes[4].executed, cluster.nodes[0].executed,
        "r4 did not execute the new work"
    );
    let frontier = cluster.follower(4).executed_through();
    let asked = cluster.catch_up_asks.len();
    cluster.ask(4);
    assert_eq!(
        cluster.catch_up_asks.get(asked),
        Some(&(4, frontier.get())),
        "the ask did not start at r4's own frontier"
    );
}

/// task-d106 step 0 (#213 review): each path that ends an ask or a page
/// without a rejection of its own is counted, by path.
#[test]
fn every_silent_end_of_an_ask_or_a_page_is_counted() {
    let paths = |cluster: &Cluster| cluster.follower(4).catch_up_paths();
    let (mut cluster, _) = left_behind(3);
    cluster.serve = false;
    let after = cluster.follower(4).executed_through();
    let start = paths(&cluster);
    assert_eq!(start, CatchUpPaths::default());

    // Asks: of itself, of a stranger, and one published.
    for donor in [4, 9] {
        let effects = cluster.nodes[4].follower_mut().request_catch_up(r(donor));
        assert!(effects.is_empty());
    }
    cluster.ask(4);
    let now = paths(&cluster);
    assert_eq!((now.declined_donor, now.asked), (2, 1), "{now:?}");

    // Pages dropped: another ballot, a stranger.
    let other = Ballot {
        number: 7,
        ..ballot0()
    };
    cluster.deliver_page(4, 0, &cluster.page(0, other, after));
    let page = cluster.page(0, ballot0(), after);
    cluster.deliver_page(4, 9, &page);
    // Ignored: not where r4 asked from.
    let mut elsewhere = page.clone();
    let ProtocolMessage::CatchUpPage { after: from, .. } = &mut elsewhere else {
        unreachable!()
    };
    *from = after.checked_next().unwrap();
    cluster.deliver_page(4, 0, &elsewhere);
    // Taken nothing: empty.
    let mut empty = page.clone();
    let ProtocolMessage::CatchUpPage { entries, .. } = &mut empty else {
        unreachable!()
    };
    entries.clear();
    cluster.deliver_page(4, 0, &empty);
    let now = paths(&cluster);
    assert_eq!(
        (
            now.dropped_ballot,
            now.dropped_sender,
            now.ignored_after,
            now.empty
        ),
        (1, 1, 1, 1),
        "{now:?}"
    );

    // Taken in part: an entry out of place ends it. With it in hand, the
    // next page is ignored and the next ask declined.
    let mut gap = page.clone();
    let ProtocolMessage::CatchUpPage { entries, .. } = &mut gap else {
        unreachable!()
    };
    assert!(entries.len() > 2);
    entries.remove(1);
    cluster.deliver_page(4, 0, &gap);
    assert!(cluster.follower(4).catching_up());
    cluster.deliver_page(4, 0, &page);
    let effects = cluster.nodes[4].follower_mut().request_catch_up(r(0));
    assert!(effects.is_empty());
    let now = paths(&cluster);
    assert_eq!(
        (now.cut_short, now.ignored_busy, now.declined_busy),
        (1, 1, 1),
        "{now:?}"
    );
    // Nothing else moved.
    assert_eq!(
        (
            now.declined_unbooted,
            now.declined_stopped,
            now.declined_unsynced,
            now.dropped_unsynced,
            now.ignored_stopped,
            now.ignored_campaign,
            now.stopped_ballot,
            now.stopped_stale
        ),
        (0, 0, 0, 0, 0, 0, 0, 0),
        "{now:?}"
    );
}

impl Cluster {
    /// A page from r0 at `after` of its first `n` commands, as if r0 had
    /// executed no further: taken whole, it is not followed by an ask.
    fn page_through(&self, after: ExecutionPosition, n: usize) -> ProtocolMessage {
        let mut page = self.page(0, ballot0(), after);
        let ProtocolMessage::CatchUpPage {
            entries, through, ..
        } = &mut page
        else {
            unreachable!()
        };
        entries.truncate(n);
        *through = ExecutionPosition::new(after.get() + n as u64).unwrap();
        page
    }
}

/// task-d25 at the bar's load (#98, 6799183006): a voter that executed
/// past where it asked from while the page was on its way -- here three
/// commands, from a page that answered the same ask first -- takes what
/// the page carries past its frontier, and the chain of asks goes on
/// from there to the donor's frontier, with no pacer. Before, the page
/// was dropped whole as `ignored_after`, and nothing asked again until
/// the pacer's next ask, a second later in `coordd`.
#[test]
fn a_page_its_voter_executed_into_while_on_its_way_is_taken_past_the_frontier() {
    let (mut cluster, _) = left_behind(17);
    assert!(cluster.catching.is_empty(), "no pacer in this test");
    cluster.serve = false;
    let start = cluster.follower(4).executed_through();
    cluster.ask(4);
    assert_eq!(cluster.catch_up_asks.last(), Some(&(4, start.get())));
    // The voter's frontier moves past the ask's start.
    let ahead = cluster.page_through(start, 3);
    cluster.deliver_page(4, 0, &ahead);
    cluster.settle();
    assert_eq!(
        cluster.follower(4).executed_through().get(),
        start.get() + 3
    );
    let asks = cluster.catch_up_asks.len();
    // The page the ask brought, from where it asked: cut short of the
    // donor's frontier, as a page at the byte bound is.
    cluster.serve = true;
    let mut stale = cluster.page(0, ballot0(), start);
    let ProtocolMessage::CatchUpPage { entries, .. } = &mut stale else {
        unreachable!()
    };
    entries.truncate(6);
    cluster.deliver_page(4, 0, &stale);
    cluster.settle();
    assert_eq!(
        cluster.catch_up_asks.get(asks),
        Some(&(4, start.get() + 6)),
        "the chain did not go on from the stale page's end"
    );
    assert_eq!(
        cluster.nodes[4].executed, cluster.nodes[0].executed,
        "r4 stopped at a stale page"
    );
    let now = paths(&cluster);
    assert_eq!(
        (
            now.ignored_after,
            now.taken_stale,
            now.stale_unasked,
            now.stopped_stale
        ),
        (0, 1, 0, 0),
        "{now:?}"
    );
}

/// One ask a page: a stale page answering an earlier ask than the last
/// is taken for what it carries past the frontier, and asks nothing; the
/// last ask's page goes on. Two chains would double the donor's reads.
#[test]
fn a_late_page_of_an_earlier_ask_starts_no_second_chain() {
    let (mut cluster, _) = left_behind(17);
    cluster.serve = false;
    let start = cluster.follower(4).executed_through();
    cluster.ask(4);
    let ahead = cluster.page_through(start, 3);
    cluster.deliver_page(4, 0, &ahead);
    cluster.settle();
    // The pacer asks again from the new frontier.
    cluster.ask(4);
    let last = cluster.follower(4).executed_through();
    assert_eq!(cluster.catch_up_asks.last(), Some(&(4, last.get())));
    let asks = cluster.catch_up_asks.len();
    // The first ask's page arrives late, cut short of the donor's
    // frontier as a page at the byte bound is: taken, and no ask
    // follows it.
    let mut late = cluster.page(0, ballot0(), start);
    let ProtocolMessage::CatchUpPage { entries, .. } = &mut late else {
        unreachable!()
    };
    entries.truncate(6);
    let carried = entries_of(&late).len() as u64;
    cluster.deliver_page(4, 0, &late);
    cluster.settle();
    assert_eq!(cluster.catch_up_asks.len(), asks, "a second chain started");
    assert_eq!(
        cluster.follower(4).executed_through().get(),
        start.get() + carried,
        "what the page carried past the frontier was taken"
    );
    let now = paths(&cluster);
    assert_eq!(
        (now.ignored_after, now.taken_stale, now.stale_unasked),
        (0, 1, 1),
        "{now:?}"
    );
}

/// After a boot the first ask still has to be answered from exactly the
/// boot frontier while restored commits wait to be compared (task-d25):
/// a page that starts below it is ignored, as before, and not taken as
/// a stale page, even after the voter executed past it.
#[test]
fn after_a_boot_a_page_below_the_boot_frontier_is_still_ignored() {
    let (mut cluster, _) = left_behind(41);
    cluster.serve = false;
    let after = cluster.follower(4).executed_through();
    let page = cluster.page(0, ballot0(), after);
    cluster.deliver_page(4, 0, &page);
    cluster.execute_at_most(4, 1);
    cluster.restart(4);
    let boot = cluster.follower(4).executed_through();
    cluster.settle();
    assert!(
        cluster.follower(4).executed_through() > boot,
        "the restart executed nothing past its frontier: the test shows nothing"
    );
    let below = ExecutionPosition::new(boot.get() - 1).unwrap();
    let before = paths(&cluster);
    cluster.deliver_page(4, 0, &cluster.page(0, ballot0(), below));
    let now = paths(&cluster);
    assert_eq!(now.ignored_after, before.ignored_after + 1, "{now:?}");
    assert_eq!(now.taken_stale, before.taken_stale, "{now:?}");
    // The ask from the boot frontier is still answered and compared.
    cluster.serve = true;
    cluster.ask(4);
    assert_eq!(cluster.follower(4).catch_up_divergence(), None);
    assert_eq!(cluster.nodes[4].executed, cluster.nodes[0].executed);
}

fn paths(cluster: &Cluster) -> coord_consensus::CatchUpPaths {
    cluster.follower(4).catch_up_paths()
}

/// The latest ask's page arrives while the window of a late page of an
/// earlier ask is still in hand: it is dropped as busy, and the chain
/// goes on once that window has executed, without the pacer (Codex on
/// #223). Before, neither page asked again and the voter stopped short.
#[test]
fn the_latest_asks_page_dropped_while_a_late_window_runs_still_continues_the_chain() {
    let (mut cluster, _) = left_behind(17);
    cluster.serve = false;
    let start = cluster.follower(4).executed_through();
    cluster.ask(4);
    let ahead = cluster.page_through(start, 3);
    cluster.deliver_page(4, 0, &ahead);
    cluster.settle();
    cluster.ask(4);
    let last = cluster.follower(4).executed_through();
    let asks = cluster.catch_up_asks.len();
    // The first ask's page, late and cut short, then the last ask's page,
    // before the first one's window has run.
    let mut late = cluster.page(0, ballot0(), start);
    let ProtocolMessage::CatchUpPage { entries, .. } = &mut late else {
        unreachable!()
    };
    entries.truncate(6);
    let latest = cluster.page(0, ballot0(), last);
    let before = paths(&cluster);
    cluster.deliver_page(4, 0, &late);
    assert!(
        cluster.follower(4).catching_up(),
        "the late window ran at once: the test shows nothing"
    );
    cluster.deliver_page(4, 0, &latest);
    let now = paths(&cluster);
    assert_eq!(now.ignored_busy, before.ignored_busy + 1, "{now:?}");
    cluster.serve = true;
    cluster.settle();
    assert_eq!(
        cluster.catch_up_asks.get(asks),
        Some(&(4, start.get() + 6)),
        "no ask followed the late window"
    );
    assert_eq!(
        cluster.nodes[4].executed, cluster.nodes[0].executed,
        "r4 stopped short with the latest ask's page dropped"
    );
}

/// Cut r4 off while the domain executes `missed` commands, then bring it
/// back with the leader's traffic flowing, as `left_behind` does.
fn left_far_behind(seed: u64, missed: u64) -> Cluster {
    let cluster = left_behind_by(seed, missed, 1);
    assert!(
        cluster.nodes[4].executed.len() + 2 * MAX_CATCH_UP_COMMANDS
            < cluster.nodes[0].executed.len(),
        "r4 is less than two pages behind: the test shows nothing"
    );
    cluster
}

/// [`left_far_behind`] with values of `value_len` bytes.
fn left_behind_by(seed: u64, missed: u64, value_len: usize) -> Cluster {
    let mut cluster = Cluster::new(seed, 8);
    cluster.value_len = value_len;
    cluster.down = vec![4];
    for seq in 1..=missed {
        cluster.admit(seq);
        cluster.settle();
    }
    cluster.settle_ticking(2);
    cluster.down.clear();
    for seq in missed + 1..=missed + 20 {
        cluster.admit(seq);
        cluster.settle();
    }
    cluster.settle_ticking(3);
    cluster
}

fn at(n: u64) -> ExecutionPosition {
    if n == 0 {
        ExecutionPosition::ZERO
    } else {
        ExecutionPosition::new(n).unwrap()
    }
}

/// One page ahead (task-d25): a page taken whole and short of the
/// donor's frontier is followed by the next ask at once, from the end of
/// what is in hand, while its window still waits to execute. That page
/// is queued after it, outside the table, and the chain goes on.
#[test]
fn the_next_page_is_asked_while_the_one_in_hand_executes() {
    let (mut cluster, _) = left_behind(17);
    cluster.serve = false;
    cluster.page_limit = 10;
    let start = cluster.follower(4).executed_through();
    cluster.ask(4);
    let asks = cluster.catch_up_asks.len();
    let first = cluster.page(0, ballot0(), start);
    cluster.deliver_page(4, 0, &first);
    assert!(
        cluster.follower(4).catching_up(),
        "the first page ran at once: the test shows nothing"
    );
    assert_eq!(
        cluster.catch_up_asks.get(asks),
        Some(&(4, start.get() + 10)),
        "no ask ahead of the page in hand"
    );
    // The page asked ahead arrives before the first window has run.
    let second = cluster.page(0, ballot0(), at(start.get() + 10));
    let before = paths(&cluster);
    cluster.serve = true;
    cluster.deliver_page(4, 0, &second);
    let now = paths(&cluster);
    assert_eq!(now.taken_ahead, before.taken_ahead + 1, "{now:?}");
    assert_eq!(now.ignored_busy, before.ignored_busy, "{now:?}");
    cluster.settle();
    assert_eq!(
        cluster.nodes[4].executed, cluster.nodes[0].executed,
        "r4 stopped short with a page asked ahead"
    );
    let now = paths(&cluster);
    assert!(now.asked_ahead >= 2, "{now:?}");
    assert_eq!(
        (now.ignored_after, now.stopped_stale, now.stale_unasked),
        (0, 0, 0),
        "{now:?}"
    );
}

/// Only the page asked ahead is taken ahead (task-d25, #236 review): a
/// page that follows exactly what is in hand but answers no ask of this
/// voter's -- here, after a page that reached its donor's frontier and so
/// asked nothing ahead -- is dropped as busy, and starts no chain. Taken,
/// it would queue a window nobody asked for and ask on from its end: a
/// second chain beside the voter's own, doubling a donor's reads.
#[test]
fn a_page_that_follows_the_one_in_hand_unasked_is_not_taken_ahead() {
    let (mut cluster, _) = left_behind(17);
    cluster.serve = false;
    let start = cluster.follower(4).executed_through();
    cluster.ask(4);
    let asks = cluster.catch_up_asks.len();
    // Taken whole and at the donor's frontier as it says: nothing is
    // asked ahead of it.
    let first = cluster.page_through(start, 3);
    cluster.deliver_page(4, 0, &first);
    assert!(
        cluster.follower(4).catching_up(),
        "the first page ran at once: the test shows nothing"
    );
    assert_eq!(cluster.catch_up_asks.len(), asks, "an ask ahead of it");
    // A page from exactly the end of what is in hand, unasked.
    let unasked = cluster.page(0, ballot0(), at(start.get() + 3));
    let before = paths(&cluster);
    cluster.deliver_page(4, 0, &unasked);
    let now = paths(&cluster);
    assert_eq!(now.taken_ahead, before.taken_ahead, "{now:?}");
    assert_eq!(now.ignored_busy, before.ignored_busy + 1, "{now:?}");
    assert_eq!(
        cluster.catch_up_asks.len(),
        asks,
        "the unasked page started a chain"
    );
}

/// The reserve holds with a page in hand and one asked ahead: a window is
/// at most one page, however much is queued behind it.
#[test]
fn a_window_is_one_page_with_two_in_hand() {
    let mut cluster = left_far_behind(5, 2 * MAX_CATCH_UP_COMMANDS as u64 + 40);
    cluster.serve = false;
    let start = cluster.follower(4).executed_through();
    cluster.ask(4);
    let first = cluster.page(0, ballot0(), start);
    assert_eq!(entries_of(&first).len(), MAX_CATCH_UP_COMMANDS);
    cluster.deliver_page(4, 0, &first);
    let second = cluster.page(0, ballot0(), at(start.get() + MAX_CATCH_UP_COMMANDS as u64));
    cluster.deliver_page(4, 0, &second);
    assert_eq!(paths(&cluster).taken_ahead, 1, "{:?}", paths(&cluster));
    let mut largest = 0;
    let mut windows = 0;
    while cluster.follower(4).catching_up() {
        let (_, window) = cluster.follower(4).table_waits();
        assert!(
            window <= MAX_CATCH_UP_COMMANDS,
            "a window of {window} with two pages in hand"
        );
        largest = largest.max(window);
        windows += usize::from(window == MAX_CATCH_UP_COMMANDS);
        cluster.execute_at_most(4, 1);
    }
    // A window shrinks as it executes: each page was one whole window.
    assert_eq!(largest, MAX_CATCH_UP_COMMANDS);
    assert_eq!(windows, 2, "the two pages were not two whole windows");
    cluster.serve = true;
    cluster.ask(4);
    assert_eq!(cluster.nodes[4].executed, cluster.nodes[0].executed);
}

/// review's ordering test on #223, with pages asked ahead: seeded random
/// orders of pacer asks, answers cut at random lengths, late answers,
/// duplicates and answers to asks never made, with the voter executing a
/// few commands at a time and moving by itself from the leader's traffic.
/// After every step r4's executed sequence is a prefix of the leader's,
/// and once served it ends level. `CATCH_UP_ORDERS` sets the seeds (400 by
/// default).
#[test]
fn two_pages_in_flight_in_any_order_never_install_twice_or_skip() {
    let seeds: u64 = std::env::var("CATCH_UP_ORDERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(400);
    let mut totals = coord_consensus::CatchUpPaths::default();
    for seed in 1..=seeds {
        let (mut cluster, _) = left_behind(1000 + seed);
        cluster.serve = false;
        cluster.page_limit = 3 + (cluster.rand() % 10) as usize;
        let mut answered: Vec<ProtocolMessage> = Vec::new();
        for _ in 0..60 {
            let pick = cluster.rand() % 8;
            match pick {
                0 => {
                    let effects = cluster.nodes[4].follower_mut().request_catch_up(r(0));
                    cluster.handle(4, effects);
                }
                1..=3 => {
                    // An answer to a recorded ask, or to one near the
                    // frontier, cut at a random length.
                    let mine: Vec<u64> = cluster
                        .catch_up_asks
                        .iter()
                        .filter(|(i, _)| *i == 4)
                        .map(|(_, a)| *a)
                        .collect();
                    let after = if pick < 3 && !mine.is_empty() {
                        mine[(cluster.rand() % mine.len() as u64) as usize]
                    } else {
                        let frontier = cluster.follower(4).executed_through().get();
                        (frontier + cluster.rand() % 5).saturating_sub(2)
                    };
                    let mut page = cluster.page(0, ballot0(), at(after));
                    if let ProtocolMessage::CatchUpPage { entries, .. } = &mut page {
                        let keep = (cluster.rand() % (entries.len() as u64 + 1)) as usize;
                        if cluster.rand() % 2 == 0 {
                            entries.truncate(keep);
                        }
                    }
                    if std::env::var_os("CATCH_UP_DEBUG").is_some() {
                        let f = cluster.follower(4);
                        eprintln!(
                            "seed {seed} after {after} busy {} last ask {:?} window {:?}",
                            f.catching_up(),
                            cluster.catch_up_asks.iter().rev().find(|(i, _)| *i == 4),
                            f.table_waits().1
                        );
                    }
                    cluster.deliver_page(4, 0, &page);
                    answered.push(page);
                }
                4 if !answered.is_empty() => {
                    // A duplicate, or a late answer.
                    let i = (cluster.rand() % answered.len() as u64) as usize;
                    let page = answered[i].clone();
                    cluster.deliver_page(4, 0, &page);
                }
                5 | 6 => {
                    let n = 1 + (cluster.rand() % 6) as usize;
                    cluster.execute_at_most(4, n);
                }
                _ => {
                    // The leader's traffic: r4 may move by itself.
                    cluster.settle();
                }
            }
            let mine = &cluster.nodes[4].executed;
            assert_eq!(
                mine[..],
                cluster.nodes[0].executed[..mine.len()],
                "seed {seed}: r4 left the leader's order"
            );
            assert_eq!(
                cluster.follower(4).catch_up_divergence(),
                None,
                "seed {seed}"
            );
            let (_, window) = cluster.follower(4).table_waits();
            assert!(
                window <= MAX_CATCH_UP_COMMANDS,
                "seed {seed}: a window of {window}"
            );
        }
        cluster.serve = true;
        cluster.ask(4);
        cluster.settle_ticking(2);
        if cluster.nodes[4].executed != cluster.nodes[0].executed {
            cluster.ask(4);
        }
        assert_eq!(
            cluster.nodes[4].executed, cluster.nodes[0].executed,
            "seed {seed}: r4 did not end level"
        );
        let p = paths(&cluster);
        totals.taken_ahead += p.taken_ahead;
        totals.asked_ahead += p.asked_ahead;
        totals.taken_stale += p.taken_stale;
        totals.stopped_stale += p.stopped_stale;
        totals.ignored_after += p.ignored_after;
        totals.ignored_busy += p.ignored_busy;
        totals.cut_short += p.cut_short;
        totals.asked += p.asked;
    }
    eprintln!("{totals:?}");
    assert!(
        totals.taken_ahead > 0,
        "no page was taken ahead: {totals:?}"
    );
    assert!(
        totals.taken_stale > 0,
        "no stale page was taken: {totals:?}"
    );
}

/// What a voter holds is two pages, however small they are (task-d25
/// review). Asked ahead while it held at most a page's worth of commands,
/// a voter served pages of one command -- a donor whose byte bound cuts
/// every page to one large command -- asked for the next on every
/// arrival, and held 126 pages with nothing executed: about 250 MiB at
/// the largest request. Now it asks ahead only while it holds one page.
/// Every ask is answered at once here, and nothing executes.
#[test]
fn what_a_voter_holds_is_two_pages_however_small_they_are() {
    for limit in [1, 10, MAX_CATCH_UP_COMMANDS] {
        let mut cluster = left_far_behind(5, 2 * MAX_CATCH_UP_COMMANDS as u64 + 40);
        cluster.serve = false;
        cluster.page_limit = limit;
        let executed = cluster.nodes[4].executed.len();
        let mut answered = cluster.catch_up_asks.len();
        cluster.ask(4);
        while answered < cluster.catch_up_asks.len() && answered < 1_000 {
            let (i, after) = cluster.catch_up_asks[answered];
            answered += 1;
            if i == 4 {
                let page = cluster.page(0, ballot0(), at(after));
                cluster.deliver_page(4, 0, &page);
            }
        }
        assert_eq!(
            cluster.nodes[4].executed.len(),
            executed,
            "pages of {limit}: r4 executed meanwhile, so the test shows nothing"
        );
        let (pages, _) = cluster.follower(4).catch_up_counts();
        assert_eq!(
            pages,
            2,
            "pages of {limit}: {pages} pages in hand with nothing executed ({:?})",
            paths(&cluster)
        );
        assert_eq!(paths(&cluster).asked_ahead, 1, "pages of {limit}");
        // Executed, the chain goes on and ends level.
        cluster.serve = true;
        cluster.settle();
        assert_eq!(
            cluster.nodes[4].executed, cluster.nodes[0].executed,
            "pages of {limit}: r4 stopped short"
        );
    }
}

/// A page is bounded in bytes where it is taken, as the donor bounds it
/// (task-d25 review), so two pages in hand are two pages' bytes whatever
/// the donor sent. A page cut at that bound, or at the command bound, and
/// nowhere else goes on at once from the cut, as a full page does: a
/// donor of a later build with a larger page is followed at full pace,
/// not at the pacer's, as an older build here follows a page of 125.
#[test]
fn a_page_past_the_bounds_is_taken_to_them_and_the_chain_goes_on() {
    for (value_len, limit) in [(64 * 1024, MAX_CATCH_UP_COMMANDS), (1, 200)] {
        let what = format!("values of {value_len}, pages of {limit}");
        let mut cluster = if value_len == 1 {
            left_far_behind(5, 2 * MAX_CATCH_UP_COMMANDS as u64 + 40)
        } else {
            left_behind_by(7, 40, value_len)
        };
        // A donor that bounds its pages by neither.
        cluster.serve = false;
        cluster.page_limit = limit;
        let start = cluster.follower(4).executed_through();
        cluster.ask(4);
        let page = cluster.page(0, ballot0(), start);
        let entries = entries_of(&page);
        let mut bytes = 0usize;
        let mut fit = 0usize;
        for e in entries {
            let len = e.encoded_len();
            if fit == MAX_CATCH_UP_COMMANDS || fit > 0 && bytes + len > MAX_CATCH_UP_BYTES {
                break;
            }
            bytes += len;
            fit += 1;
        }
        assert!(
            fit < entries.len(),
            "{what}: a page of {} is within the bounds, so the test shows nothing",
            entries.len()
        );
        let asks = cluster.catch_up_asks.len();
        let before = paths(&cluster);
        cluster.deliver_page(4, 0, &page);
        let now = paths(&cluster);
        assert_eq!(now.cut_short, before.cut_short + 1, "{what}: {now:?}");
        assert_eq!(
            cluster.catch_up_asks.get(asks),
            Some(&(4, start.get() + fit as u64)),
            "{what}: the next ask is not from where the bound cut the page"
        );
        // That ask answered, the chain runs to the end.
        cluster.serve = true;
        let next = cluster.page(0, ballot0(), at(start.get() + fit as u64));
        cluster.deliver_page(4, 0, &next);
        cluster.settle();
        assert_eq!(
            cluster.nodes[4].executed, cluster.nodes[0].executed,
            "{what}: r4 stopped short"
        );
    }
}

/// A window is at most one page when the page in hand ends a window early
/// and the page asked ahead queues behind its rest (task-d25 review's
/// case): the first page's window ends at its tenth command, which the
/// donor kept no row of. Without the limit in `install_catch_up`, the
/// third window took the first page's last 115 and the whole second page,
/// 240, past what the table's reserve is sized for
/// (`RECOVERY_RESERVE_PARTS`).
#[test]
fn a_window_is_one_page_when_the_first_ends_early() {
    let mut cluster = left_far_behind(5, 2 * MAX_CATCH_UP_COMMANDS as u64 + 40);
    cluster.serve = false;
    let start = cluster.follower(4).executed_through();
    cluster.ask(4);
    let asks = cluster.catch_up_asks.len();
    let mut first = cluster.page(0, ballot0(), start);
    let ProtocolMessage::CatchUpPage { entries, .. } = &mut first else {
        unreachable!()
    };
    assert_eq!(entries.len(), MAX_CATCH_UP_COMMANDS);
    entries[9].decided = None;
    cluster.deliver_page(4, 0, &first);
    assert_eq!(cluster.follower(4).table_waits().1, 9, "the first window");
    let next = start.get() + MAX_CATCH_UP_COMMANDS as u64;
    assert_eq!(
        cluster.catch_up_asks.get(asks),
        Some(&(4, next)),
        "no ask ahead of the page in hand"
    );
    let second = cluster.page(0, ballot0(), at(next));
    assert_eq!(entries_of(&second).len(), MAX_CATCH_UP_COMMANDS);
    cluster.deliver_page(4, 0, &second);
    assert_eq!(paths(&cluster).taken_ahead, 1, "{:?}", paths(&cluster));
    // A window is installed only once the one before has run, so a window
    // starts where the last ended.
    let mut windows = Vec::new();
    let mut last = 0;
    while cluster.follower(4).catching_up() {
        let (_, window) = cluster.follower(4).table_waits();
        if last <= 1 {
            windows.push(window);
        }
        last = window;
        cluster.execute_at_most(4, 1);
    }
    assert_eq!(
        windows,
        vec![9, 1, MAX_CATCH_UP_COMMANDS, MAX_CATCH_UP_COMMANDS - 10],
        "windows over two pages in hand"
    );
    cluster.serve = true;
    cluster.ask(4);
    assert_eq!(cluster.nodes[4].executed, cluster.nodes[0].executed);
}

/// An open chain closes once the voter executes as far as the page that
/// left it open said its donor had (task-d25 review), however it got
/// there: here from a late answer to an earlier ask, which says nothing
/// of the chain itself. Before, it stayed open until a page reached a
/// frontier, and with the pacer asking on an open chain every
/// `CHAIN_RETRY`, a voter level by itself would have asked for ever in a
/// quiet domain, where nobody answers an ask with nothing past it.
#[test]
fn an_open_chain_closes_once_the_voter_executes_as_far_as_it_was_owed() {
    let (mut cluster, _) = left_behind(17);
    cluster.serve = false;
    let start = cluster.follower(4).executed_through();
    cluster.ask(4);
    let whole = cluster.page(0, ballot0(), start);
    let mut cut = whole.clone();
    let ProtocolMessage::CatchUpPage { entries, .. } = &mut cut else {
        unreachable!()
    };
    entries.truncate(3);
    cluster.deliver_page(4, 0, &cut);
    cluster.settle();
    // The chain's next ask, from the end of the first page, is never
    // answered.
    assert_eq!(
        cluster.catch_up_asks.last(),
        Some(&(4, start.get() + 3)),
        "no ask followed the cut page"
    );
    assert!(cluster.follower(4).catch_up_chain_open());
    // The first ask's whole page arrives late, and is taken past the
    // frontier.
    cluster.deliver_page(4, 0, &whole);
    cluster.settle();
    assert_eq!(paths(&cluster).stale_unasked, 1, "{:?}", paths(&cluster));
    assert_eq!(cluster.nodes[4].executed, cluster.nodes[0].executed);
    assert!(
        !cluster.follower(4).catch_up_chain_open(),
        "a voter level with the donor's frontier still owes its chain"
    );
}

/// r4's chain left open by r0's page cut to three, its next ask taken by
/// a donor that is itself behind (r1 here), whose page reaches its own
/// frontier four commands on (#236 review). Returns where r4 stood and
/// what r0's whole first page was.
fn answered_by_a_donor_behind(cluster: &mut Cluster) -> (ExecutionPosition, ProtocolMessage) {
    cluster.serve = false;
    let start = cluster.follower(4).executed_through();
    cluster.ask(4);
    let whole = cluster.page(0, ballot0(), start);
    let mut cut = whole.clone();
    let ProtocolMessage::CatchUpPage { entries, .. } = &mut cut else {
        unreachable!()
    };
    entries.truncate(3);
    cluster.deliver_page(4, 0, &cut);
    cluster.settle();
    assert!(cluster.follower(4).catch_up_chain_open());
    // The chain's next ask goes to r1, which has executed only four more.
    let effects = cluster.nodes[4].follower_mut().request_catch_up(r(1));
    cluster.handle(4, effects);
    cluster.settle();
    let from = at(start.get() + 3);
    assert_eq!(cluster.catch_up_asks.last(), Some(&(4, from.get())));
    let mut behind = cluster.page(0, ballot0(), from);
    let ProtocolMessage::CatchUpPage {
        entries, through, ..
    } = &mut behind
    else {
        unreachable!()
    };
    entries.truncate(4);
    *through = at(from.get() + 4);
    cluster.deliver_page(4, 1, &behind);
    cluster.settle();
    assert_eq!(cluster.follower(4).executed_through(), at(from.get() + 4));
    (start, whole)
}

/// A chain answered by two donors is owed the furthest frontier either
/// said (#236 review): a donor that is itself behind answers with a page
/// that reaches its own frontier, and the chain stays open, short of what
/// the first donor said. Before, that page closed it, and in a quiet
/// domain nothing asked again: r4 stayed at 135 or 125 of 310 in review's
/// scratches. Asking on, r4 ends level, and the chain closes there.
#[test]
fn a_chain_answered_by_a_donor_behind_stays_owed_the_first_donors_frontier() {
    let (mut cluster, _) = left_behind(17);
    answered_by_a_donor_behind(&mut cluster);
    assert!(
        cluster.nodes[4].executed.len() < cluster.nodes[0].executed.len(),
        "r4 is level: the test shows nothing"
    );
    assert!(
        cluster.follower(4).catch_up_chain_open(),
        "the chain closed at the behind donor's frontier, short of what r0 said"
    );
    cluster.serve = true;
    cluster.ask(4);
    assert_eq!(cluster.nodes[4].executed, cluster.nodes[0].executed);
    assert!(!cluster.follower(4).catch_up_chain_open());
}

/// The furthest frontier said is still one the voter can reach by itself
/// (#236 review): after a donor behind has answered, a late answer to the
/// first ask brings r4 level with r0, which is what r0 said, and the chain
/// closes, with nothing left to ask for.
#[test]
fn a_chain_owed_two_donors_frontiers_closes_at_the_further_one() {
    let (mut cluster, _) = left_behind(17);
    let (_, whole) = answered_by_a_donor_behind(&mut cluster);
    assert!(cluster.follower(4).catch_up_chain_open());
    cluster.deliver_page(4, 0, &whole);
    cluster.settle();
    assert_eq!(cluster.nodes[4].executed, cluster.nodes[0].executed);
    assert!(
        !cluster.follower(4).catch_up_chain_open(),
        "a voter level with the furthest frontier said still owes its chain"
    );
}

/// A chain is open while executions since the boot are left to compare,
/// even with the voter executed as far as the donor said (#236 review):
/// that clause of `catch_up_chain_open` is what keeps a restart's
/// comparisons asked for when the donor is no further on than the voter.
/// Here the window the crash left installed runs at the restart, and a
/// donor whose frontier is exactly that answers with a page cut to three:
/// owed nothing more by position, r4 still has the rest of the window to
/// compare, and the chain stays open until it has.
#[test]
fn a_chain_with_comparisons_left_is_open_at_the_donors_frontier() {
    let (mut cluster, _) = left_behind(47);
    cluster.serve = false;
    let after = cluster.follower(4).executed_through();
    let page = cluster.page(0, ballot0(), after);
    assert!(
        entries_of(&page).len() > 6,
        "a window of {}",
        entries_of(&page).len()
    );
    cluster.deliver_page(4, 0, &page);
    cluster.restart(4);
    cluster.settle();
    let ran = cluster.follower(4).executed_through();
    assert!(ran.get() > after.get() + 6, "the restart ran the window");
    let asked = cluster.catch_up_asks.len();
    cluster.ask(4);
    let from = at(cluster.catch_up_asks[asked].1);
    let mut cut = cluster.page(0, ballot0(), from);
    let ProtocolMessage::CatchUpPage {
        entries, through, ..
    } = &mut cut
    else {
        unreachable!()
    };
    entries.truncate(3);
    *through = ran;
    cluster.deliver_page(4, 0, &cut);
    cluster.settle();
    assert_eq!(
        cluster.follower(4).executed_through(),
        ran,
        "r4 executed past what the donor said: the test shows nothing"
    );
    assert!(
        cluster.follower(4).catch_up_chain_open(),
        "executions since the boot are left to compare, and the chain closed"
    );
    cluster.serve = true;
    cluster.ask(4);
    assert_eq!(cluster.follower(4).catch_up_divergence(), None);
    assert_eq!(cluster.nodes[4].executed, cluster.nodes[0].executed);
    assert!(!cluster.follower(4).catch_up_chain_open());
}

/// A command past a page's byte bound by itself is taken alone, and the
/// chain goes on from it (#236 review): a put at the value limit is
/// larger than `MAX_CATCH_UP_BYTES` on its own, and the one-command
/// exemption on receipt is what gets a voter past it. Without it the
/// voter takes nothing from such a page and stops asking.
#[test]
fn a_command_past_the_byte_bound_alone_is_taken_by_itself() {
    let mut cluster = left_behind_by(9, 3, MAX_CATCH_UP_BYTES);
    cluster.serve = false;
    let start = cluster.follower(4).executed_through();
    cluster.ask(4);
    let page = cluster.page(0, ballot0(), start);
    let entries = entries_of(&page);
    assert!(entries.len() > 1, "a page of {}", entries.len());
    assert!(
        entries[0].encoded_len() > MAX_CATCH_UP_BYTES,
        "its first command is within the bound: the test shows nothing"
    );
    let asks = cluster.catch_up_asks.len();
    let before = paths(&cluster);
    cluster.deliver_page(4, 0, &page);
    cluster.settle();
    assert_eq!(paths(&cluster).cut_short, before.cut_short + 1);
    assert_eq!(
        cluster.follower(4).executed_through(),
        at(start.get() + 1),
        "the command past the bound was not taken by itself"
    );
    assert_eq!(
        cluster.catch_up_asks.get(asks),
        Some(&(4, start.get() + 1)),
        "the chain did not go on from the command taken"
    );
    cluster.serve = true;
    cluster.ask(4);
    assert_eq!(cluster.nodes[4].executed, cluster.nodes[0].executed);
}

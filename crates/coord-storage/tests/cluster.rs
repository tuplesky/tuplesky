//! task-24 acceptance: three- and five-voter logical histories produced by
//! the leader/follower machines with slow learning and ordered application
//! match the KV/transaction/retry/policy oracle on every node; one leader
//! response cannot establish success; no watch event precedes irrevocable
//! application; deliveries are permuted deterministically.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use coord_consensus::{
    AppliedOutcome, BallotConfiguration, ConfigurationIdentity, Follower, FollowerConfig, Leader,
    LeaderConfig, LearningMode, PayloadRecordV1, ProtocolMessage, ReplicaRole,
};
use coord_core::capability::{
    AdmissionReceipt, AttestedAdmission, EstablishedResult, VerifierToken,
};
use coord_core::effect::{BootId, Effect, PeerId, PersistBatch};
use coord_core::event::{
    AdmittedRequest, AuthenticatedPeerMessage, Event, PeerProvenance, StorageEvent,
};
use coord_core::machine::DeterministicMachine;
use coord_core::outbox::BarrierAllocator;
use coord_oracle::model::{KvModel, Outcome as OracleOutcome};
use coord_state::policy::{Action, KeyInterval, PolicyRule};
use coord_state::{Outcome, Response};
use coord_storage::policy::{bootstrap_session, rule_update};
use coord_storage::{Applier, GroupLimits, StoreWorker, WatchItem, WatchSpec, events_at};
use coord_store_api::engine::{OrderedRead, ScanRequest};
use coord_store_api::registry::Collection;
use coord_store_testkit::model::ModelEngine;
use coord_types::identity::Digest32;
use coord_types::ids::*;
use coord_types::logical_v1::*;
use coord_types::wire_v1::{MessageV1, RequestV1};
use coord_types::{CommandId, RetryKey};

const NS: NamespaceId = NamespaceId([5; 16]);
const SESSION: SessionId = SessionId([3; 16]);
const CLIENT: ClientInstanceId = ClientInstanceId([4; 16]);
const ALICE: PrincipalId = PrincipalId([0xa; 16]);
const FRONTEND: PeerId = PeerId {
    replica: ReplicaId([0xf0; 16]),
    incarnation: ReplicaIncarnation::ZERO,
};

fn r(i: u8) -> ReplicaId {
    ReplicaId([i; 16])
}

fn epoch() -> ConfigurationEpoch {
    ConfigurationEpoch::new(1).unwrap()
}

fn ballot() -> Ballot {
    Ballot {
        epoch: epoch(),
        number: 0,
        leader: r(0),
    }
}

fn identity(n: u8, me: u8) -> ConfigurationIdentity {
    ConfigurationIdentity {
        cluster: ClusterId([1; 16]),
        domain: DomainId([2; 16]),
        epoch: epoch(),
        voters: (0..n).map(r).collect(),
        replica: r(me),
        incarnation: ReplicaIncarnation::new(1).unwrap(),
        role: ReplicaRole::Voter,
    }
}

fn quorum(n: u8) -> BallotConfiguration {
    let fast: BTreeSet<ReplicaId> = (0..(n / 2 + 1)).map(r).collect();
    BallotConfiguration::c2(epoch(), ballot(), (0..n).map(r).collect(), fast).unwrap()
}

#[allow(clippy::large_enum_variant)]
enum Machine {
    Leader(Leader),
    Follower(Follower),
}

impl Machine {
    fn step(&mut self, e: Event) -> Vec<Effect> {
        match self {
            Machine::Leader(m) => m.step(e),
            Machine::Follower(m) => m.step(e),
        }
    }
    fn next_executable(&self) -> Option<CommandId> {
        match self {
            Machine::Leader(m) => m.next_executable(),
            Machine::Follower(m) => m.next_executable(),
        }
    }
    fn payload(&self, c: &CommandId) -> Option<PayloadRecordV1> {
        match self {
            Machine::Leader(m) => m.payload(c).cloned(),
            Machine::Follower(m) => m.payload(c).cloned(),
        }
    }
    fn applied(&mut self, c: CommandId, o: &AppliedOutcome) -> Vec<Effect> {
        match self {
            Machine::Leader(m) => m.applied(c, o).unwrap(),
            Machine::Follower(m) => m.applied(c, o).unwrap(),
        }
    }
}

struct Node {
    machine: Machine,
    applier: Applier<StoreWorker<ModelEngine>>,
    inbox: VecDeque<(ReplicaId, Vec<u8>)>,
    established: Vec<EstablishedResult>,
    released: Vec<coord_core::capability::ReleasedResult>,
    executed: Vec<CommandId>,
}

fn node(n: u8, me: u8) -> Node {
    let boot = BootId([me + 1; 16]);
    let inc = ReplicaIncarnation::new(1).unwrap();
    let mut worker =
        StoreWorker::open(ModelEngine::new(), boot, inc, GroupLimits::default()).unwrap();
    let mut alloc = BarrierAllocator::new(inc, boot);
    // Identical session and policy bootstrap on every node: Alice may do
    // everything on keys below "z"; nothing on "z".
    let mut updates = bootstrap_session(&SESSION, ALICE, 64, true).unwrap();
    for (i, action) in Action::ALL.iter().enumerate() {
        updates.push(
            rule_update(
                &PolicyRuleId([i as u8 + 1; 16]),
                &PolicyRule {
                    principal: ALICE,
                    action: *action,
                    namespace: NS,
                    interval: KeyInterval {
                        lower: vec![],
                        upper: Some(b"z".to_vec()),
                    },
                },
            )
            .unwrap(),
        );
    }
    let base = worker.application_base();
    worker
        .submit(PersistBatch {
            barrier: alloc.allocate(),
            base: Some(base),
            updates,
        })
        .unwrap();
    worker.flush().unwrap();
    // The bootstrap batch is ordered work, so the application already
    // stands at a position above zero: both machines resume from it.
    let executed_through = worker.application_base().execution_position;
    let applier = Applier::new(worker, alloc).unwrap();
    let mut machine = if me == 0 {
        Machine::Leader(Leader::new(
            LeaderConfig {
                identity: identity(n, me),
                quorum: quorum(n),
                genesis: ballot(),
                frontend: FRONTEND,
                capacity: 64,
            },
            None,
            executed_through,
        ))
    } else {
        Machine::Follower(Follower::recover(
            FollowerConfig {
                identity: identity(n, me),
                quorum: quorum(n),
                genesis: ballot(),
                frontend: FRONTEND,
                capacity: 64,
            },
            None,
            core::iter::empty(),
            core::iter::empty(),
            executed_through,
        ))
    };
    // task-24 qualifies the slow path; task-28's full learning is compared
    // against it in `tests/recovery.rs`.
    match &mut machine {
        Machine::Leader(m) => m.set_learning(LearningMode::SlowOnly),
        Machine::Follower(m) => m.set_learning(LearningMode::SlowOnly),
    }
    let mut node = Node {
        machine,
        applier,
        inbox: VecDeque::new(),
        established: Vec::new(),
        released: Vec::new(),
        executed: Vec::new(),
    };
    node.machine.step(Event::Boot {
        boot_id: boot,
        incarnation: inc,
    });
    node
}

struct Cluster {
    nodes: Vec<Node>,
    /// Frames the frontend received: (from, message).
    frontend: Vec<(ReplicaId, ProtocolMessage)>,
    seed: u64,
    /// Drop every replica-to-replica frame (the frontend still hears).
    drop_peers: bool,
}

impl Cluster {
    fn new(n: u8, seed: u64) -> Self {
        Cluster {
            nodes: (0..n).map(|i| node(n, i)).collect(),
            frontend: Vec::new(),
            seed,
            drop_peers: false,
        }
    }

    fn rand(&mut self) -> u64 {
        // xorshift, deterministic per seed.
        self.seed ^= self.seed << 13;
        self.seed ^= self.seed >> 7;
        self.seed ^= self.seed << 17;
        self.seed
    }

    /// Handle effects of node `i`: persist completes immediately (the
    /// model engine is durable on flush), sends go to inboxes.
    fn handle(&mut self, i: usize, effects: Vec<Effect>) {
        for e in effects {
            match e {
                Effect::Persist(batch) => {
                    let barrier = batch.barrier;
                    // Protocol rows are persisted through the node's own
                    // worker so the projection carries them.
                    let node = &mut self.nodes[i];
                    node.applier.store_mut().submit(batch).unwrap();
                    node.applier.store_mut().flush().unwrap();
                    let more = node
                        .machine
                        .step(Event::Storage(StorageEvent::JournalDurable {
                            barrier_id: barrier,
                            journal_seq: LocalJournalSeq::new(1).unwrap(),
                        }));
                    self.handle(i, more);
                }
                Effect::SendWhenDurable { to, frame, .. } => {
                    let from = r(i as u8);
                    if to == FRONTEND {
                        self.frontend
                            .push((from, ProtocolMessage::decode(&frame).unwrap()));
                        continue;
                    }
                    if self.drop_peers {
                        continue;
                    }
                    let dest = to.replica.0[0] as usize;
                    self.nodes[dest].inbox.push_back((from, frame));
                }
                Effect::Established(result) => self.nodes[i].established.push(result),
                // The leader's final-path release: a command that was
                // never speculated is disclosed once, after it has
                // executed and become durable. These tests drive no
                // speculation companion, so every release here is that
                // one, and it is the leader's alone.
                Effect::Released(released) => {
                    assert!(!released.speculative());
                    self.nodes[i].released.push(released);
                }
                other => panic!("unexpected effect {other:?}"),
            }
        }
    }

    /// Run until quiescent, delivering inbox frames in a seeded order and
    /// executing learned commands through each node's applier.
    fn settle(&mut self) {
        loop {
            let mut progressed = false;
            // Pick a node with a non-empty inbox pseudo-randomly.
            let candidates: Vec<usize> = (0..self.nodes.len())
                .filter(|i| !self.nodes[*i].inbox.is_empty())
                .collect();
            if !candidates.is_empty() {
                let pick = candidates[(self.rand() % candidates.len() as u64) as usize];
                // Deliver one frame, sometimes not the oldest.
                let len = self.nodes[pick].inbox.len();
                let idx = if self.rand().is_multiple_of(3) && len > 1 {
                    len - 1
                } else {
                    0
                };
                let (from, frame) = self.nodes[pick].inbox.remove(idx).unwrap();
                let event = Event::Peer(AuthenticatedPeerMessage::new(
                    PeerProvenance::from_transport(from, ReplicaIncarnation::new(1).unwrap(), 1),
                    frame,
                ));
                let effects = self.nodes[pick].machine.step(event);
                self.handle(pick, effects);
                progressed = true;
            }
            for i in 0..self.nodes.len() {
                while let Some(c) = self.nodes[i].machine.next_executable() {
                    let payload = self.nodes[i].machine.payload(&c).expect("payload known");
                    let outcome = self.nodes[i].applier.apply(c, &payload).unwrap();
                    let effects = self.nodes[i].machine.applied(c, &outcome);
                    self.nodes[i].executed.push(c);
                    self.handle(i, effects);
                    progressed = true;
                }
            }
            if !progressed {
                return;
            }
        }
    }

    /// The frontend admits a request to every node.
    fn admit(&mut self, seq: u64, op: CanonicalOperation) -> CommandId {
        let mut request = LogicalRequest::new(NS, op);
        request.canonicalize();
        let key = retry_key(seq);
        let command = CommandId::derive(&key, &request).unwrap();
        let frame = MessageV1::Request(RequestV1::new(key, &request, 0).unwrap())
            .encode()
            .unwrap();
        for i in 0..self.nodes.len() {
            let receipt = AdmissionReceipt::submitting(
                VerifierToken::for_boundary(),
                AttestedAdmission {
                    cluster: ClusterId([1; 16]),
                    domain: DomainId([2; 16]),
                    session: SESSION,
                    rule_generation: 1,
                    scope_ceiling: u32::MAX,
                    receipt_id: Digest32([9; 32]),
                    admitted_at_ticks: 0,
                },
            );
            let effects = self.nodes[i].machine.step(Event::Admitted(AdmittedRequest {
                receipt,
                frame: frame.clone(),
            }));
            self.handle(i, effects);
        }
        command
    }

    fn rows(&self, i: usize) -> BTreeMap<(u16, Vec<u8>), Vec<u8>> {
        let gated = self.nodes[i].applier.store().reader().snapshot().unwrap();
        let mut out = BTreeMap::new();
        for c in [
            Collection::KvCurrentV1,
            Collection::KvHistoryV1,
            Collection::EventsV1,
            Collection::RetryV1,
            Collection::ExecutedV1,
        ] {
            let page = gated
                .view()
                .scan_page(c.id(), &ScanRequest::all(10_000, 1 << 24))
                .unwrap();
            for row in page.rows {
                out.insert((c.id().0, row.key), row.value);
            }
        }
        out
    }
}

fn retry_key(seq: u64) -> RetryKey {
    RetryKey {
        cluster_id: ClusterId([1; 16]),
        domain_id: DomainId([2; 16]),
        session_id: SESSION,
        client_instance_id: CLIENT,
        request_sequence: RequestSequence::new(seq).unwrap(),
    }
}

fn put(k: &[u8], v: &[u8]) -> CanonicalOperation {
    CanonicalOperation::Put(PutOp {
        key: k.to_vec(),
        value: v.to_vec(),
        lease: None,
        prev_kv: true,
    })
}

fn get(k: &[u8]) -> CanonicalOperation {
    CanonicalOperation::Range(RangeOp {
        range: KeyRange::exact(k.to_vec()),
        revision: None,
        limit: 0,
        keys_only: false,
        count_only: false,
    })
}

fn cas(k: &[u8], expected_version: u64, v: &[u8]) -> CanonicalOperation {
    CanonicalOperation::Txn(TxnOp {
        compares: vec![Compare {
            key: k.to_vec(),
            target: CompareTarget::Version,
            result: CompareResult::Equal,
            operand: CompareOperand::Counter(expected_version),
        }],
        success: vec![BranchOp::Put(PutOp {
            key: k.to_vec(),
            value: v.to_vec(),
            lease: None,
            prev_kv: false,
        })],
        failure: vec![BranchOp::Range(RangeOp {
            range: KeyRange::exact(k.to_vec()),
            revision: None,
            limit: 0,
            keys_only: false,
            count_only: false,
        })],
    })
}

fn to_oracle(o: &Outcome) -> OracleOutcome {
    let e = |e: &coord_state::KvEntry| coord_oracle::model::KvEntry {
        value: e.value.clone(),
        create_revision: e.create_revision.get(),
        mod_revision: e.mod_revision.get(),
        version: e.version,
        lease: e.lease.map(|l| l.0),
    };
    let items = |i: &[coord_state::RangeItem]| {
        i.iter()
            .map(|x| coord_oracle::model::RangeItem {
                key: x.key.clone(),
                entry: e(&x.entry),
            })
            .collect()
    };
    match o {
        Outcome::Put { prev } => OracleOutcome::Put {
            prev: prev.as_ref().map(e),
        },
        Outcome::Range {
            items: i,
            count,
            more,
        } => OracleOutcome::Range {
            items: items(i),
            count: *count,
            more: *more,
        },
        Outcome::Txn { succeeded, results } => OracleOutcome::Txn {
            succeeded: *succeeded,
            results: results.iter().map(to_oracle).collect(),
        },
        other => panic!("{other:?}"),
    }
}

/// The retained responses of every executed command on node `i`, in
/// execution order, decoded from the retry rows.
fn responses(cluster: &Cluster, i: usize, commands: &[(u64, CommandId)]) -> Vec<Response> {
    let gated = cluster.nodes[i]
        .applier
        .store()
        .reader()
        .snapshot()
        .unwrap();
    commands
        .iter()
        .map(|(seq, _)| {
            let record = coord_storage::retry::lookup(gated.view(), &retry_key(*seq))
                .unwrap()
                .expect("executed command has a retry record");
            postcard::from_bytes(&record.response).unwrap()
        })
        .collect()
}

fn run_history(n: u8, seed: u64) {
    let mut cluster = Cluster::new(n, seed);
    let ops: Vec<(u64, CanonicalOperation)> = vec![
        (1, put(b"a", b"1")),
        (2, put(b"b", b"2")),
        (3, get(b"a")),
        (4, cas(b"a", 1, b"11")),
        (5, cas(b"a", 1, b"12")), // fails: version is now 2
        (6, put(b"a", b"13")),
        (7, get(b"b")),
        (8, put(b"z", b"denied")), // policy denies "z"
        (9, cas(b"b", 1, b"22")),
    ];
    let mut commands = Vec::new();
    for (seq, op) in &ops {
        commands.push((*seq, cluster.admit(*seq, op.clone())));
        if seq.is_multiple_of(3) {
            cluster.settle();
        }
    }
    // Retries: the same invocations again (lost responses).
    for (seq, op) in &ops[..4] {
        cluster.admit(*seq, op.clone());
    }
    cluster.settle();

    // Every node executed every command exactly once, in the leader's order.
    let order: Vec<CommandId> = cluster.nodes[0].executed.clone();
    assert_eq!(order.len(), ops.len(), "seed {seed}: leader executed all");
    for (i, node) in cluster.nodes.iter().enumerate() {
        assert_eq!(node.executed, order, "seed {seed}: node {i} order");
        assert_eq!(node.established.len(), ops.len());
        // Positions continue after the ordered bootstrap batch.
        let first = node.established[0].position().get();
        for (k, result) in node.established.iter().enumerate() {
            assert_eq!(result.position().get(), first + k as u64);
            assert_eq!(result.command(), order[k]);
            assert!(!result.fast_path());
        }
    }
    // Identical rows on every node.
    let reference = cluster.rows(0);
    for i in 1..cluster.nodes.len() {
        assert_eq!(cluster.rows(i), reference, "seed {seed}: node {i} rows");
    }
    // Responses match the independent oracle applied in execution order,
    // with policy denials and retries accounted for.
    let mut model = KvModel::default();
    let got = responses(&cluster, 0, &commands);
    for (k, c) in order.iter().enumerate() {
        let (seq, op) = ops
            .iter()
            .find(|(s, _)| commands.iter().any(|(s2, c2)| s2 == s && c2 == c))
            .unwrap();
        let response = &got[commands.iter().position(|(s, _)| s == seq).unwrap()];
        if *seq == 8 {
            assert_eq!(response.outcome, Outcome::ErrPermissionDenied);
            continue;
        }
        let expected = model.apply(op, Some(*seq));
        assert_eq!(
            to_oracle(&response.outcome),
            expected.outcome,
            "seed {seed}: op {seq}"
        );
        assert_eq!(
            response.revision.get(),
            expected.revision,
            "seed {seed}: op {seq} header"
        );
        let _ = k;
    }
    // A retry executed once: the established positions are distinct and
    // one per invocation.
    let positions: BTreeSet<u64> = cluster.nodes[0]
        .established
        .iter()
        .map(|e| e.position().get())
        .collect();
    assert_eq!(positions.len(), ops.len());
    // Watch events never precede irrevocable application: every revision
    // the leader's hub published exists in its durable engine.
    let hub = cluster.nodes[0].applier.hub();
    let published = hub.published();
    assert_eq!(published, cluster.nodes[0].applier.kv_revision().unwrap());
    let gated = cluster.nodes[0]
        .applier
        .store()
        .reader()
        .snapshot()
        .unwrap();
    for rev in 1..=published.get() {
        assert!(
            events_at(gated.view(), KvRevision::new(rev).unwrap())
                .unwrap()
                .is_some()
        );
    }
}

#[test]
fn three_voter_histories_match_the_oracle_on_every_node() {
    for seed in [1, 7, 42] {
        run_history(3, seed);
    }
}

#[test]
fn five_voter_histories_match_the_oracle_on_every_node() {
    for seed in [3, 11] {
        run_history(5, seed);
    }
}

#[test]
fn one_leader_response_cannot_establish_success() {
    let mut cluster = Cluster::new(3, 5);
    cluster.drop_peers = true;
    cluster.admit(1, put(b"a", b"1"));
    cluster.settle();
    // The frontend holds the leader's reply, but no proposal or
    // acknowledgement reached any replica: nothing is learned, applied or
    // established anywhere, and the KV revision stays at zero.
    assert!(
        cluster
            .frontend
            .iter()
            .any(|(from, m)| *from == r(0) && matches!(m, ProtocolMessage::LeaderReply { .. }))
    );
    for node in &cluster.nodes {
        assert!(node.established.is_empty());
        assert!(node.executed.is_empty());
        assert_eq!(node.applier.kv_revision().unwrap(), KvRevision::ZERO);
        assert_eq!(node.machine.next_executable(), None);
    }
    // Frames flow again for a second command, but command 1's proposal was
    // lost for good (re-proposal is recovery, task-25/26): command 2
    // depends on it and cannot execute before it, so nothing establishes.
    cluster.drop_peers = false;
    cluster.admit(2, put(b"b", b"2"));
    cluster.settle();
    for node in &cluster.nodes {
        assert!(node.established.is_empty());
    }
}

#[test]
fn watch_events_follow_irrevocable_application() {
    let mut cluster = Cluster::new(3, 9);
    let hub = cluster.nodes[1].applier.hub();
    let registration = hub
        .register(WatchSpec {
            namespace: NS,
            key: b"a".to_vec(),
            range_end: None,
            start_revision: Some(KvRevision::new(1).unwrap()),
            prev_kv: true,
            progress_notify: false,
            queue_capacity: 16,
        })
        .unwrap();
    hub.replay_complete(registration.id).unwrap();
    cluster.admit(1, put(b"a", b"1"));
    cluster.admit(2, put(b"a", b"2"));
    cluster.settle();
    let hub = cluster.nodes[1].applier.hub();
    let mut seen = Vec::new();
    while let Some(item) = hub.next(registration.id, |_| true) {
        match item {
            WatchItem::Batch(b) => seen.push(b.revision.get()),
            WatchItem::Progress(_) => {}
            WatchItem::Closed { .. } => break,
        }
    }
    assert_eq!(seen, vec![1, 2]);
    // Each delivered revision is durable on the follower that delivered it.
    let gated = cluster.nodes[1]
        .applier
        .store()
        .reader()
        .snapshot()
        .unwrap();
    for rev in seen {
        assert!(
            events_at(gated.view(), KvRevision::new(rev).unwrap())
                .unwrap()
                .is_some()
        );
    }
}

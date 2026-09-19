//! task-33 acceptance in a deterministic composition: a three-voter
//! domain (leader plus two followers over the model engine, full
//! learning, speculation at the leader) behind a trusted frontend built
//! from the admission gate, the collector and the dispatcher, joined by
//! a tick-timed wire that logs every hop.
//!
//! * the request is fanned out to every voter at once and the followers
//!   vote before the leader's proposal reaches them: the trace has no
//!   serial leader hop;
//! * a lone leader release, or lone votes, never release tentative data;
//! * cancellation detaches the caller and preserves identity and outcome
//!   resolution (resolve, retry, conflict, deadline);
//! * voter identities are counted, not connections; collection is
//!   bounded per domain;
//! * unary and finalized watch dispatch: watches carry applied revisions
//!   only, never a tentative value;
//! * the collector's golden event trace is frozen for Go reuse.

use std::collections::{BTreeMap, BTreeSet};

use coord_collector::{
    Action, Admission, AdmissionLimits, AdmissionRefusal, Caller, Collector, CollectorConfig,
    CollectorEvent, Delivery, Dispatcher, EvidenceError, HoldReason, KIND_EVIDENCE, KIND_RELEASE,
    Progress, SubmitRefusal, Submitted, admitted_from_submit, codes, decode_evidence,
    decode_release, frontend_frame,
};
use coord_consensus::{
    AppliedOutcome, BallotConfiguration, ConfigurationIdentity, FastAck, Follower, FollowerConfig,
    Leader, LeaderConfig, LearningMode, PayloadRecordV1, ProtocolMessage, ReplicaRole, SlowAck,
    VoteError,
};
use coord_core::capability::{
    AdmissionReceipt, EstablishedResult, EstablishmentEvidence, ReleasedResult, VerifierToken,
};
use coord_core::effect::{BootId, Effect, PeerId, PersistBatch};
use coord_core::event::{
    AdmittedRequest, AuthenticatedPeerMessage, Event, PeerProvenance, StorageEvent,
};
use coord_core::machine::DeterministicMachine;
use coord_core::outbox::BarrierAllocator;
use coord_state::policy::{Action as PolicyAction, KeyInterval, PolicyRule};
use coord_storage::policy::{bootstrap_session, rule_update};
use coord_storage::watch::replay_from_view;
use coord_storage::{
    Applier, GroupLimits, Overlay, SpeculationLimits, StoreWorker, WatchHub, speculate,
};
use coord_store_testkit::model::ModelEngine;
use coord_types::identity::Digest32;
use coord_types::ids::*;
use coord_types::logical_v1::*;
use coord_types::wire_v1::{
    BoundedBytes, Frame, FrameReader, MessageV1, OutcomeV1, PeerRole, RequestV1, ResolveRequestV1,
    ResponseV1, WatchOpenV1, decode_stream,
};
use coord_types::{CommandId, RetryKey};

const NS: NamespaceId = NamespaceId([5; 16]);
const SESSION: SessionId = SessionId([3; 16]);
const CLIENT: ClientInstanceId = ClientInstanceId([4; 16]);
const ALICE: PrincipalId = PrincipalId([0xa; 16]);
const CLUSTER: ClusterId = ClusterId([1; 16]);
const DOMAIN: DomainId = DomainId([2; 16]);
const FRONTEND: PeerId = PeerId {
    replica: ReplicaId([0xf0; 16]),
    incarnation: ReplicaIncarnation::ZERO,
};
const LATENCY: u64 = 1;

fn r(i: u8) -> ReplicaId {
    ReplicaId([i; 16])
}

fn inc() -> ReplicaIncarnation {
    ReplicaIncarnation::new(1).unwrap()
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
        cluster: CLUSTER,
        domain: DOMAIN,
        epoch: epoch(),
        voters: (0..n).map(r).collect(),
        replica: r(me),
        incarnation: inc(),
        role: ReplicaRole::Voter,
    }
}

fn quorum(n: u8) -> BallotConfiguration {
    let fast: BTreeSet<ReplicaId> = (0..(n / 2 + 1)).map(r).collect();
    BallotConfiguration::c2(epoch(), ballot(), (0..n).map(r).collect(), fast).unwrap()
}

fn caller() -> Caller {
    Caller {
        role: PeerRole::Client,
        session: SESSION,
        rule_generation: 1,
        scope_ceiling: u32::MAX,
    }
}

fn retry_key(seq: u64) -> RetryKey {
    RetryKey {
        cluster_id: CLUSTER,
        domain_id: DOMAIN,
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

fn request(seq: u64, op: CanonicalOperation, deadline_ms: u32) -> (CommandId, RequestV1) {
    let mut logical = LogicalRequest::new(NS, op);
    logical.canonicalize();
    let key = retry_key(seq);
    let command = CommandId::derive(&key, &logical).unwrap();
    (command, RequestV1::new(key, &logical, deadline_ms).unwrap())
}

fn frame_of(bytes: &[u8]) -> Frame {
    let mut reader = FrameReader::new();
    reader.push(bytes).expect("within the reader bound");
    let frame = reader.next_frame().unwrap().expect("one frame");
    assert!(reader.next_frame().unwrap().is_none());
    frame
}

fn response_of(delivery: &Delivery) -> ResponseV1 {
    match decode_stream(&delivery.frame).unwrap().as_slice() {
        [MessageV1::Response(r)] => r.clone(),
        other => panic!("not a response: {other:?}"),
    }
}

fn provenance(from: ReplicaId, connection: u64) -> PeerProvenance {
    PeerProvenance::from_transport(from, inc(), connection)
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
    applier: Applier<ModelEngine>,
    overlay: Overlay,
    executed: Vec<CommandId>,
}

fn node(n: u8, me: u8) -> Node {
    let boot = BootId([me + 1; 16]);
    let mut worker =
        StoreWorker::open(ModelEngine::new(), boot, inc(), GroupLimits::default()).unwrap();
    let mut alloc = BarrierAllocator::new(inc(), boot);
    let mut updates = bootstrap_session(&SESSION, ALICE, 64, true).unwrap();
    for (i, action) in PolicyAction::ALL.iter().enumerate() {
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
    // Session and policy rows are admission rows: they need an ordered
    // batch, so the bootstrap carries the application base.
    let base = worker.application_base();
    worker
        .submit(PersistBatch {
            barrier: alloc.allocate(),
            base: Some(base),
            updates,
        })
        .unwrap();
    worker.flush().unwrap();
    let applier = Applier::new(worker, alloc).unwrap();
    // Execution positions are absolute: the ordered bootstrap batch
    // already occupies the first of them, so a machine starts from the
    // frontier the store is at, not from zero.
    let bootstrapped = applier.worker().application_base().execution_position;
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
            bootstrapped,
        ))
    } else {
        Machine::Follower(
            Follower::new(FollowerConfig {
                identity: identity(n, me),
                quorum: quorum(n),
                genesis: ballot(),
                frontend: FRONTEND,
                capacity: 64,
            })
            .restore_execution(bootstrapped, []),
        )
    };
    match &mut machine {
        Machine::Leader(m) => m.set_learning(LearningMode::Full),
        Machine::Follower(m) => m.set_learning(LearningMode::Full),
    }
    let mut node = Node {
        machine,
        applier,
        overlay: Overlay::new(),
        executed: Vec::new(),
    };
    node.machine.step(Event::Boot {
        boot_id: boot,
        incarnation: inc(),
    });
    node
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Endpoint {
    Voter(u8),
    Frontend,
}

/// One logged transmission: queued at `tick`, delivered `latency` later.
#[derive(Clone, Debug)]
struct Hop {
    tick: u64,
    from: Endpoint,
    to: Endpoint,
    kind: String,
}

enum Item {
    Submit { to: u8, frame: Vec<u8> },
    Peer { from: u8, to: u8, frame: Vec<u8> },
    ToFrontend { from: u8, frame: Vec<u8> },
}

fn peer_kind(frame: &[u8]) -> String {
    match ProtocolMessage::decode(frame).unwrap() {
        ProtocolMessage::Proposal(_) => "proposal",
        ProtocolMessage::FastAck(_) => "fast-ack",
        ProtocolMessage::SlowAck(_) => "slow-ack",
        ProtocolMessage::LeaderReply { .. } => "leader-reply",
        _ => "other",
    }
    .into()
}

struct World {
    nodes: Vec<Node>,
    frontend: Dispatcher,
    tick: u64,
    seed: u64,
    queue: Vec<(u64, u64, Item)>,
    next_seq: u64,
    hops: Vec<Hop>,
    /// Ticks added to evidence and releases from these voters.
    delay_to_frontend: BTreeMap<u8, u64>,
    drop_evidence_from: BTreeSet<u8>,
    drop_releases: bool,
    deliveries: Vec<(u64, Delivery)>,
    progress: Vec<(u64, Progress)>,
    rejected: Vec<(u64, EvidenceError)>,
}

impl World {
    fn new(seed: u64) -> Self {
        Self::with_bound(seed, 64)
    }

    fn with_bound(seed: u64, max_pending: usize) -> Self {
        let collector = Collector::new(CollectorConfig {
            quorum: quorum(3),
            max_pending,
            max_resolved: 16,
        });
        let admission = Admission::new(CLUSTER, DOMAIN, AdmissionLimits::default());
        World {
            nodes: (0..3).map(|i| node(3, i)).collect(),
            frontend: Dispatcher::new(admission, collector, 16),
            tick: 0,
            seed,
            queue: Vec::new(),
            next_seq: 0,
            hops: Vec::new(),
            delay_to_frontend: BTreeMap::new(),
            drop_evidence_from: BTreeSet::new(),
            drop_releases: false,
            deliveries: Vec::new(),
            progress: Vec::new(),
            rejected: Vec::new(),
        }
    }

    fn rand(&mut self) -> u64 {
        self.seed ^= self.seed << 13;
        self.seed ^= self.seed >> 7;
        self.seed ^= self.seed << 17;
        self.seed
    }

    fn hub(&self) -> WatchHub {
        self.nodes[1].applier.hub().clone()
    }

    fn enqueue(&mut self, item: Item, extra: u64) {
        let (from, to, kind) = match &item {
            Item::Submit { to, .. } => (Endpoint::Frontend, Endpoint::Voter(*to), "submit".into()),
            Item::Peer { from, to, frame } => (
                Endpoint::Voter(*from),
                Endpoint::Voter(*to),
                peer_kind(frame),
            ),
            Item::ToFrontend { from, frame } => {
                let f = frame_of(frame);
                let kind = match f.kind {
                    KIND_EVIDENCE => peer_kind(&f.payload),
                    KIND_RELEASE => "release".into(),
                    _ => unreachable!(),
                };
                (Endpoint::Voter(*from), Endpoint::Frontend, kind)
            }
        };
        self.hops.push(Hop {
            tick: self.tick,
            from,
            to,
            kind,
        });
        let seq = self.next_seq;
        self.next_seq += 1;
        self.queue.push((self.tick + LATENCY + extra, seq, item));
    }

    /// Handle a voter's effects: persist immediately (the model engine is
    /// durable on flush), route sends.
    fn handle(&mut self, i: usize, effects: Vec<Effect>) {
        for e in effects {
            if let Some(frame) = frontend_frame(&e, FRONTEND) {
                let frame = frame.unwrap();
                let is_release = matches!(e, Effect::Released(_));
                if is_release && self.drop_releases {
                    continue;
                }
                if !is_release && self.drop_evidence_from.contains(&(i as u8)) {
                    continue;
                }
                let extra = self.delay_to_frontend.get(&(i as u8)).copied().unwrap_or(0);
                self.enqueue(
                    Item::ToFrontend {
                        from: i as u8,
                        frame,
                    },
                    extra,
                );
                continue;
            }
            match e {
                Effect::Persist(batch) => {
                    let barrier = batch.barrier;
                    let node = &mut self.nodes[i];
                    node.applier.worker_mut().submit(batch).unwrap();
                    node.applier.worker_mut().flush().unwrap();
                    let more = node
                        .machine
                        .step(Event::Storage(StorageEvent::JournalDurable {
                            barrier_id: barrier,
                            journal_seq: LocalJournalSeq::new(1).unwrap(),
                        }));
                    self.handle(i, more);
                }
                Effect::SendWhenDurable { to, frame, .. } => {
                    let dest = to.replica.0[0];
                    self.enqueue(
                        Item::Peer {
                            from: i as u8,
                            to: dest,
                            frame,
                        },
                        0,
                    );
                }
                Effect::Established(_) => {}
                other => panic!("unexpected effect {other:?}"),
            }
        }
    }

    fn deliver(&mut self, item: Item) {
        match item {
            Item::Submit { to, frame } => {
                let admitted = admitted_from_submit(PeerRole::Frontend, &frame_of(&frame)).unwrap();
                let effects = self.nodes[to as usize]
                    .machine
                    .step(Event::Admitted(admitted));
                self.handle(to as usize, effects);
            }
            Item::Peer { from, to, frame } => {
                let event =
                    Event::Peer(AuthenticatedPeerMessage::new(provenance(r(from), 1), frame));
                let effects = self.nodes[to as usize].machine.step(event);
                self.handle(to as usize, effects);
            }
            Item::ToFrontend { from, frame } => {
                let f = frame_of(&frame);
                // The voter's control connection to the frontend: one per
                // voter, its number derived from the identity.
                let prov = provenance(r(from), 100 + u64::from(from));
                let result = match f.kind {
                    KIND_EVIDENCE => self
                        .frontend
                        .collector_mut()
                        .on_evidence(prov, decode_evidence(&f).unwrap()),
                    KIND_RELEASE => self
                        .frontend
                        .collector_mut()
                        .on_release(prov, decode_release(&f).unwrap()),
                    _ => unreachable!(),
                };
                match result {
                    Ok(progress) => {
                        if let Progress::Released(release) = &progress
                            && let Some(d) = self.frontend.settle_release(release)
                        {
                            self.deliveries.push((self.tick, d));
                        }
                        self.progress.push((self.tick, progress));
                    }
                    Err(e) => self.rejected.push((self.tick, e)),
                }
            }
        }
    }

    fn execute_and_speculate(&mut self) -> bool {
        let mut progressed = false;
        for i in 0..self.nodes.len() {
            while let Some(c) = self.nodes[i].machine.next_executable() {
                let payload = self.nodes[i].machine.payload(&c).expect("payload known");
                let outcome = self.nodes[i].applier.apply(c, &payload).unwrap();
                let effects = self.nodes[i].machine.applied(c, &outcome);
                self.nodes[i].executed.push(c);
                self.handle(i, effects);
                progressed = true;
            }
            loop {
                let Machine::Leader(leader) = &self.nodes[i].machine else {
                    break;
                };
                let Some(request) = leader.next_speculable() else {
                    break;
                };
                let payload = leader.payload(&request.command).unwrap().clone();
                let node = &mut self.nodes[i];
                let outcome = speculate(
                    node.applier.worker(),
                    &mut node.overlay,
                    &SpeculationLimits::default(),
                    &request,
                    &payload,
                );
                let Machine::Leader(leader) = &mut node.machine else {
                    unreachable!()
                };
                let effects = match outcome {
                    Ok(outcome) => leader.speculated(outcome),
                    Err(_) => {
                        leader.decline_speculation(request.command);
                        Vec::new()
                    }
                };
                self.handle(i, effects);
                progressed = true;
            }
        }
        progressed
    }

    /// Advance one tick: deliver everything due, in seeded order, then
    /// execute and speculate.
    fn step(&mut self) {
        let mut due: Vec<(u64, u64, Item)> = Vec::new();
        let mut rest = Vec::new();
        for entry in self.queue.drain(..) {
            if entry.0 <= self.tick {
                due.push(entry);
            } else {
                rest.push(entry);
            }
        }
        self.queue = rest;
        while !due.is_empty() {
            let idx = (self.rand() % due.len() as u64) as usize;
            let (_, _, item) = due.remove(idx);
            self.deliver(item);
        }
        while self.execute_and_speculate() {}
        let expired = self.frontend.expire(self.tick);
        for d in expired {
            self.deliveries.push((self.tick, d));
        }
        self.tick += 1;
    }

    fn settle(&mut self) {
        let mut idle = 0;
        while idle < 4 {
            let before = self.queue.len();
            self.step();
            idle = if before == 0 && self.queue.is_empty() {
                idle + 1
            } else {
                0
            };
            assert!(self.tick < 10_000, "no quiescence");
        }
    }

    fn run_ticks(&mut self, n: u64) {
        for _ in 0..n {
            self.step();
        }
    }

    /// A client submits on `connection`; the fan-out goes out now.
    fn submit(&mut self, connection: u64, seq: u64, op: CanonicalOperation) -> (CommandId, Action) {
        self.submit_with_deadline(connection, seq, op, 0)
    }

    fn submit_with_deadline(
        &mut self,
        connection: u64,
        seq: u64,
        op: CanonicalOperation,
        deadline_ms: u32,
    ) -> (CommandId, Action) {
        let (command, request) = request(seq, op, deadline_ms);
        let frame = frame_of(&MessageV1::Request(request).encode().unwrap());
        let hub = self.hub();
        let action = self
            .frontend
            .on_frame(self.tick, connection, &caller(), &frame, &hub);
        if let Action::FanOut(fan_out) = &action {
            assert_eq!(fan_out.command, command);
            for target in &fan_out.targets {
                self.enqueue(
                    Item::Submit {
                        to: target.0[0],
                        frame: fan_out.frame.clone(),
                    },
                    0,
                );
            }
        }
        (command, action)
    }

    fn resolve(&mut self, connection: u64, seq: u64, command: CommandId) -> ResponseV1 {
        let frame = frame_of(
            &MessageV1::ResolveRequest(ResolveRequestV1 {
                retry_key: retry_key(seq),
                command_id: command,
            })
            .encode()
            .unwrap(),
        );
        let hub = self.hub();
        match self
            .frontend
            .on_frame(self.tick, connection, &caller(), &frame, &hub)
        {
            Action::Respond(d) => response_of(&d),
            other => panic!("resolve produced {other:?}"),
        }
    }

    fn delivered(&self, command: &CommandId) -> Option<(u64, ResponseV1)> {
        self.deliveries
            .iter()
            .map(|(t, d)| (*t, response_of(d)))
            .find(|(_, r)| r.command_id == *command)
    }

    fn hops(&self, from: Endpoint, to: Endpoint, kind: &str) -> Vec<u64> {
        self.hops
            .iter()
            .filter(|h| h.from == from && h.to == to && h.kind == kind)
            .map(|h| h.tick)
            .collect()
    }
}

fn ok_result(response: &ResponseV1) -> (Option<u64>, Vec<u8>) {
    match &response.outcome {
        OutcomeV1::Ok { revision, result } => {
            (revision.map(|r| r.get()), result.as_slice().to_vec())
        }
        other => panic!("not an established result: {other:?}"),
    }
}

fn err_code(response: &ResponseV1) -> u16 {
    match &response.outcome {
        OutcomeV1::Err { code, .. } => *code,
        other => panic!("not an error: {other:?}"),
    }
}

#[test]
fn fan_out_is_parallel_and_the_trace_has_no_serial_leader_hop() {
    let mut w = World::new(11);
    let t0 = w.tick;
    let (command, action) = w.submit(1, 1, put(b"a", b"1"));
    assert!(matches!(action, Action::FanOut(_)));
    w.settle();

    // Every voter received the submission from the frontend at the same
    // tick, the leader among them as one target.
    for v in 0..3 {
        assert_eq!(
            w.hops(Endpoint::Frontend, Endpoint::Voter(v), "submit"),
            vec![t0],
            "voter {v} hears the frontend directly"
        );
    }
    // No voter relayed the request.
    assert!(
        !w.hops
            .iter()
            .any(|h| matches!(h.from, Endpoint::Voter(_)) && h.kind == "submit"),
        "no voter forwards submissions"
    );
    // The fast-set follower's fast acknowledgement left before the
    // leader's proposal could have reached it: the vote did not wait on a
    // leader hop. The follower outside the fixed fast set adopts the
    // leader's order (a slow acknowledgement after the proposal), which
    // is the source rule, not a relay.
    let ack = w.hops(Endpoint::Voter(1), Endpoint::Frontend, "fast-ack");
    let proposal = w.hops(Endpoint::Voter(0), Endpoint::Voter(1), "proposal");
    assert_eq!(
        ack,
        vec![t0 + LATENCY],
        "the fast-set follower votes on arrival"
    );
    assert_eq!(proposal, vec![t0 + LATENCY]);
    assert!(
        ack[0] < proposal[0] + LATENCY,
        "vote precedes proposal arrival"
    );
    assert!(
        w.hops(Endpoint::Voter(2), Endpoint::Frontend, "fast-ack")
            .is_empty()
    );
    assert_eq!(
        w.hops(Endpoint::Voter(2), Endpoint::Frontend, "slow-ack"),
        vec![t0 + 2 * LATENCY],
        "the other follower adopts once the proposal arrives"
    );
    // The collector's own predicate held one round trip after the fan-out
    // (leader reply and both fast acks arrived together); the release
    // followed the leader's gate one hop later. Nothing was released on
    // votes alone.
    let learned_at = w
        .progress
        .iter()
        .find(|(_, p)| *p == Progress::Held(HoldReason::AwaitingRelease))
        .map(|(t, _)| *t)
        .expect("votes complete before the release");
    assert_eq!(learned_at, t0 + 2 * LATENCY);
    let (released_at, response) = w.delivered(&command).expect("released");
    assert_eq!(released_at, t0 + 3 * LATENCY);
    let (revision, _) = ok_result(&response);
    assert_eq!(revision, Some(1));
    let trace = w.frontend.collector().trace();
    assert!(trace.iter().any(|e| matches!(
        e,
        CollectorEvent::Released {
            fast: true,
            speculative: true,
            delivered: true,
            ..
        }
    )));
    // A second command on a conflicting key: same shape.
    let t1 = w.tick;
    let (c2, _) = w.submit(1, 2, put(b"a", b"2"));
    w.settle();
    let (at, r2) = w.delivered(&c2).unwrap();
    assert_eq!(at, t1 + 3 * LATENCY);
    assert_eq!(ok_result(&r2).0, Some(2));
}

#[test]
fn a_lone_leader_release_never_releases_tentative_data() {
    // Follower evidence to the frontend is late: the leader's release
    // arrives first and is held; the acknowledgements release it.
    let mut w = World::new(5);
    w.delay_to_frontend.insert(1, 20);
    w.delay_to_frontend.insert(2, 20);
    let t0 = w.tick;
    let (command, _) = w.submit(1, 1, put(b"k", b"v"));
    w.run_ticks(6);
    assert!(
        w.progress
            .iter()
            .any(|(t, p)| *t == t0 + 3 * LATENCY && *p == Progress::Held(HoldReason::AwaitingVotes)),
        "the release arrived and was held"
    );
    assert!(w.frontend.collector().trace().iter().any(|e| matches!(
        e,
        CollectorEvent::Evidence { kind, accepted: true, .. } if kind == "release"
    )));
    assert!(
        w.delivered(&command).is_none(),
        "nothing released on the leader alone"
    );
    assert!(w.frontend.collector().is_pending(&command));
    w.settle();
    let (at, _) = w.delivered(&command).expect("released once votes arrived");
    assert!(at >= t0 + 20, "released only after the acknowledgements");

    // Without the leader's release, complete votes release nothing.
    let mut w = World::new(6);
    w.drop_releases = true;
    let (command, _) = w.submit(1, 1, put(b"k", b"v"));
    w.settle();
    assert!(w.delivered(&command).is_none());
    assert!(
        w.progress
            .iter()
            .any(|(_, p)| *p == Progress::Held(HoldReason::AwaitingRelease))
    );
    assert!(w.frontend.collector().is_pending(&command));
    // The command still executed everywhere: the collector is what holds.
    assert!(w.nodes.iter().all(|n| n.executed.contains(&command)));
    let resolved = w.resolve(2, 1, command);
    assert_eq!(resolved.outcome, OutcomeV1::Pending);
}

#[test]
fn cancellation_preserves_identity_and_outcome_resolution() {
    let mut w = World::new(21);
    let hops_before = w.hops.len();
    let (command, _) = w.submit(7, 1, put(b"c", b"1"));
    // The caller goes away before any evidence.
    let hub = w.hub();
    let cancelled = w.frontend.on_connection_closed(7, &hub);
    assert_eq!(cancelled, vec![command]);
    w.settle();
    assert!(w.delivered(&command).is_none(), "no caller to deliver to");
    assert!(w.frontend.collector().trace().iter().any(|e| matches!(
        e,
        CollectorEvent::Released {
            delivered: false,
            ..
        }
    )));
    // Resolution by identity on a new connection.
    let resolved = w.resolve(8, 1, command);
    assert_eq!(resolved.command_id, command);
    assert_eq!(ok_result(&resolved).0, Some(1));
    // A different command identity under the same retry key conflicts.
    let other = CommandId(Digest32([0xee; 32]));
    assert_eq!(
        err_code(&w.resolve(8, 1, other)),
        codes::REQUEST_IDENTITY_CONFLICT
    );
    // A retry with the same payload returns the retained outcome without a
    // second fan-out.
    let fanned = w.hops.len();
    let (_, action) = w.submit(8, 1, put(b"c", b"1"));
    match action {
        Action::Respond(d) => assert_eq!(ok_result(&response_of(&d)).0, Some(1)),
        other => panic!("{other:?}"),
    }
    assert_eq!(w.hops.len(), fanned, "no second fan-out");
    // The same retry key with another payload is an identity conflict.
    let (_, action) = w.submit(8, 1, put(b"c", b"2"));
    match action {
        Action::Respond(d) => {
            assert_eq!(err_code(&response_of(&d)), codes::REQUEST_IDENTITY_CONFLICT)
        }
        other => panic!("{other:?}"),
    }
    // An unknown identity is unknown, not failed.
    assert_eq!(w.resolve(8, 99, command).outcome, OutcomeV1::Unknown);
    assert!(w.hops.len() > hops_before);

    // A client deadline: the caller hears "unknown", the command keeps
    // collecting and resolves later.
    let mut w = World::new(22);
    w.delay_to_frontend.insert(0, 30);
    w.delay_to_frontend.insert(1, 30);
    w.delay_to_frontend.insert(2, 30);
    let (command, _) = w.submit_with_deadline(9, 1, put(b"d", b"1"), 5);
    w.run_ticks(8);
    let (at, response) = w.delivered(&command).expect("deadline reported");
    assert_eq!(response.outcome, OutcomeV1::Pending, "unknown, not failed");
    assert!(at >= 5);
    assert!(w.frontend.collector().is_pending(&command));
    assert_eq!(w.resolve(9, 1, command).outcome, OutcomeV1::Pending);
    w.settle();
    assert_eq!(ok_result(&w.resolve(9, 1, command)).0, Some(1));
    // The deadline delivery was the only one: the late release found no
    // attached caller.
    assert_eq!(w.deliveries.len(), 1);
}

#[test]
fn voter_identities_are_counted_rather_than_connections() {
    let mut c = Collector::new(CollectorConfig {
        quorum: quorum(3),
        max_pending: 8,
        max_resolved: 8,
    });
    let (command, req) = request(1, put(b"x", b"1"), 0);
    let admitted = AdmittedRequest {
        receipt: AdmissionReceipt::from_verifier(
            VerifierToken::for_boundary(),
            SESSION,
            1,
            u32::MAX,
            Digest32([7; 32]),
            0,
        ),
        frame: MessageV1::Request(req).encode().unwrap(),
    };
    let Submitted::FanOut(fan_out) = c.submit(0, &admitted).unwrap() else {
        panic!()
    };
    assert_eq!(
        fan_out.targets,
        vec![r(0), r(1), r(2)],
        "every voter, at once"
    );
    let path = Digest32([1; 32]);
    let reply = ProtocolMessage::LeaderReply {
        ballot: ballot(),
        command,
        seqnum: 1,
        deps: vec![],
        path,
    };
    // The leader's release, before any vote: held.
    let released = ReleasedResult::from_gate(
        EstablishedResult::establish(EstablishmentEvidence {
            command,
            epoch: epoch(),
            ballot: ballot(),
            position: ExecutionPosition::new(1).unwrap(),
            closed_predecessors: vec![],
            result_digest: Digest32([2; 32]),
            revision: Some(KvRevision::new(1).unwrap()),
            fast_path: true,
        })
        .unwrap(),
        vec![0xaa],
        true,
    );
    assert_eq!(
        c.on_release(provenance(r(1), 5), released.clone()),
        Err(EvidenceError::NotLeader { sender: r(1) })
    );
    assert_eq!(
        c.on_release(provenance(r(0), 1), released.clone()),
        Ok(Progress::Held(HoldReason::AwaitingVotes))
    );
    // The leader's proposal plus one adopter is the slow majority of three:
    // with the release already present, the second identity releases.
    let slow = |replica: u8| {
        ProtocolMessage::SlowAck(SlowAck {
            replica: r(replica),
            ballot: ballot(),
            command,
        })
    };
    assert_eq!(
        c.on_evidence(provenance(r(0), 1), reply.clone()),
        Ok(Progress::Held(HoldReason::AwaitingVotes))
    );
    match c.on_evidence(provenance(r(2), 20), slow(2)) {
        Ok(Progress::Released(release)) => {
            assert!(!release.fast);
            assert_eq!(ok_result(&release.response), (Some(1), vec![0xaa]));
        }
        other => panic!("{other:?}"),
    }
    assert!(!c.is_pending(&command));
    for connection in [21, 22] {
        assert_eq!(
            c.on_evidence(provenance(r(2), connection), slow(2)),
            Ok(Progress::Settled),
            "late duplicates over other connections change nothing"
        );
    }
    // A fresh command: identities, not connections, and the sender bound
    // at negotiation must be the claimed replica.
    let (command, req) = request(2, put(b"y", b"1"), 0);
    let admitted = AdmittedRequest {
        receipt: admitted.receipt.clone(),
        frame: MessageV1::Request(req).encode().unwrap(),
    };
    c.submit(0, &admitted).unwrap();
    let reply = ProtocolMessage::LeaderReply {
        ballot: ballot(),
        command,
        seqnum: 2,
        deps: vec![],
        path,
    };
    let fast = |replica: u8, path: Digest32, deps: Vec<CommandId>| {
        ProtocolMessage::FastAck(FastAck {
            replica: r(replica),
            ballot: ballot(),
            command,
            deps,
            paths: vec![],
            path,
            seqnum: None,
        })
    };
    assert_eq!(
        c.on_evidence(provenance(r(2), 3), fast(1, path, vec![])),
        Err(EvidenceError::SenderMismatch {
            sender: r(2),
            claimed: r(1)
        }),
        "a connection bound to r2 cannot speak for r1"
    );
    assert_eq!(
        c.on_evidence(provenance(r(9), 4), fast(9, path, vec![])),
        Err(EvidenceError::Vote(VoteError::NotAVoter))
    );
    assert_eq!(
        c.on_evidence(provenance(r(2), 3), fast(2, path, vec![])),
        Err(EvidenceError::Vote(VoteError::NotInFastSet))
    );
    let elsewhere = CommandId(Digest32([0x33; 32]));
    assert_eq!(
        c.on_evidence(
            provenance(r(1), 5),
            fast(1, Digest32([9; 32]), vec![elsewhere])
        ),
        Ok(Progress::Held(HoldReason::AwaitingVotes)),
        "a path and order disagreement is counted but learns nothing"
    );
    assert_eq!(
        c.on_evidence(provenance(r(1), 6), fast(1, path, vec![])),
        Err(EvidenceError::Vote(VoteError::Duplicate)),
        "the same identity over another connection is a duplicate"
    );
    assert_eq!(
        c.on_evidence(provenance(r(0), 1), reply),
        Ok(Progress::Held(HoldReason::AwaitingVotes)),
        "leader plus one disagreeing fast member: neither fast nor slow"
    );
    // A release for another ballot or epoch is refused; the right one is
    // held until the slow majority adopts.
    let wrong_ballot = ReleasedResult::from_gate(
        EstablishedResult::establish(EstablishmentEvidence {
            command,
            epoch: epoch(),
            ballot: Ballot {
                number: 1,
                ..ballot()
            },
            position: ExecutionPosition::new(2).unwrap(),
            closed_predecessors: vec![],
            result_digest: Digest32([3; 32]),
            revision: Some(KvRevision::new(2).unwrap()),
            fast_path: false,
        })
        .unwrap(),
        vec![0xbb],
        false,
    );
    assert_eq!(
        c.on_release(provenance(r(0), 1), wrong_ballot),
        Err(EvidenceError::WrongBallot)
    );
    let right = ReleasedResult::from_gate(
        EstablishedResult::establish(EstablishmentEvidence {
            command,
            epoch: epoch(),
            ballot: ballot(),
            position: ExecutionPosition::new(2).unwrap(),
            closed_predecessors: vec![],
            result_digest: Digest32([3; 32]),
            revision: Some(KvRevision::new(2).unwrap()),
            fast_path: false,
        })
        .unwrap(),
        vec![0xbb],
        false,
    );
    assert_eq!(
        c.on_release(provenance(r(0), 1), right),
        Ok(Progress::Held(HoldReason::AwaitingVotes))
    );
    let slow = |replica: u8| {
        ProtocolMessage::SlowAck(SlowAck {
            replica: r(replica),
            ballot: ballot(),
            command,
        })
    };
    match c.on_evidence(provenance(r(2), 3), slow(2)) {
        Ok(Progress::Released(release)) => {
            assert!(!release.fast);
            assert_eq!(release.command, command);
            assert_eq!(ok_result(&release.response), (Some(2), vec![0xbb]));
        }
        other => panic!("{other:?}"),
    }
    // Evidence after release is harmless.
    assert_eq!(
        c.on_evidence(provenance(r(1), 5), slow(1)),
        Ok(Progress::Settled)
    );
}

#[test]
fn collection_is_bounded_per_domain_without_evicting_unresolved_work() {
    let mut w = World::with_bound(31, 2);
    w.drop_releases = true;
    let (c1, a1) = w.submit(1, 1, put(b"a", b"1"));
    let (c2, a2) = w.submit(1, 2, put(b"b", b"1"));
    assert!(matches!(a1, Action::FanOut(_)));
    assert!(matches!(a2, Action::FanOut(_)));
    let (_, a3) = w.submit(1, 3, put(b"c", b"1"));
    match a3 {
        Action::Respond(d) => assert_eq!(err_code(&response_of(&d)), codes::BACKPRESSURE),
        other => panic!("{other:?}"),
    }
    w.settle();
    assert_eq!(w.frontend.collector().pending(), 2, "nothing evicted");
    assert!(w.frontend.collector().is_pending(&c1) && w.frontend.collector().is_pending(&c2));
    // Cancellation frees no slot either: unresolved work is never dropped.
    let hub = w.hub();
    w.frontend.on_connection_closed(1, &hub);
    let (_, a4) = w.submit(2, 4, put(b"d", b"1"));
    assert!(matches!(a4, Action::Respond(_)));
    // Direct collector bound: same refusal.
    let mut c = Collector::new(CollectorConfig {
        quorum: quorum(3),
        max_pending: 1,
        max_resolved: 1,
    });
    let receipt = AdmissionReceipt::from_verifier(
        VerifierToken::for_boundary(),
        SESSION,
        1,
        u32::MAX,
        Digest32([7; 32]),
        0,
    );
    let admitted = |seq| AdmittedRequest {
        receipt: receipt.clone(),
        frame: MessageV1::Request(request(seq, get(b"q"), 0).1)
            .encode()
            .unwrap(),
    };
    assert!(matches!(
        c.submit(0, &admitted(1)),
        Ok(Submitted::FanOut(_))
    ));
    assert_eq!(
        c.submit(0, &admitted(2)),
        Err(SubmitRefusal::Backpressure { pending: 1 })
    );
    assert!(matches!(
        c.submit(0, &admitted(1)),
        Ok(Submitted::Attached { .. })
    ));
}

#[test]
fn unary_and_finalized_watch_dispatch() {
    let mut w = World::new(41);
    let hub = w.hub();
    // A client opens a watch from revision 1 on the follower's hub.
    let open = frame_of(
        &MessageV1::WatchOpen(WatchOpenV1 {
            watch_id: 9,
            namespace: NS,
            key: BoundedBytes::new(b"w".to_vec()).unwrap(),
            range_end: None,
            start_revision: Some(KvRevision::new(1).unwrap()),
            prev_kv: true,
            progress_notify: false,
        })
        .encode()
        .unwrap(),
    );
    let registration = match w.frontend.on_frame(0, 5, &caller(), &open, &hub) {
        Action::WatchOpened {
            connection: 5,
            watch_id: 9,
            registration,
        } => registration,
        other => panic!("{other:?}"),
    };
    if let Some((from, through)) = registration.replay {
        let gated = w.nodes[1].applier.worker().reader().snapshot().unwrap();
        replay_from_view(&hub, gated.view(), registration.id, NS, from, through).unwrap();
    }
    hub.replay_complete(registration.id).unwrap();
    assert!(
        w.frontend
            .pump_watch(&hub, 5, 9, usize::MAX, |_| true)
            .is_empty()
    );

    // Two writes: hold the followers' evidence and the leader's release so
    // the leader has a tentative outcome while nothing is applied yet.
    w.delay_to_frontend.insert(1, 40);
    w.delay_to_frontend.insert(2, 40);
    let (c1, _) = w.submit(1, 1, put(b"w", b"1"));
    w.run_ticks(2);
    let Machine::Leader(leader) = &w.nodes[0].machine else {
        unreachable!()
    };
    assert!(
        leader.tentative(&c1).is_some(),
        "the leader computed a tentative value"
    );
    assert!(
        w.nodes[1].executed.is_empty(),
        "nothing applied at the follower"
    );
    assert!(
        w.frontend
            .pump_watch(&hub, 5, 9, usize::MAX, |_| true)
            .is_empty(),
        "a tentative value never reaches a watch"
    );
    let (c2, _) = w.submit(1, 2, put(b"w", b"2"));
    w.settle();
    assert!(w.delivered(&c1).is_some() && w.delivered(&c2).is_some());
    let frames = w.frontend.pump_watch(&hub, 5, 9, usize::MAX, |_| true);
    let mut revisions = Vec::new();
    for f in &frames {
        match decode_stream(f).unwrap().as_slice() {
            [MessageV1::WatchEvents(e)] => {
                assert_eq!(e.watch_id, 9);
                assert!(e.complete);
                assert_eq!(e.events.as_slice().len(), 1);
                assert_eq!(e.events.as_slice()[0].key.as_slice(), b"w");
                revisions.push(e.revision.get());
            }
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(revisions, vec![1, 2]);
    // Each delivered revision is applied on the follower that served it.
    assert_eq!(w.nodes[1].executed.len(), 2);
    // Close: the client's cancel is acknowledged and the watch is gone.
    let close = frame_of(
        &MessageV1::WatchClose(coord_types::wire_v1::WatchCloseV1 {
            watch_id: 9,
            reason: coord_types::wire_v1::WatchCloseReasonV1::Cancelled,
            last_complete_revision: None,
        })
        .encode()
        .unwrap(),
    );
    match w.frontend.on_frame(w.tick, 5, &caller(), &close, &hub) {
        Action::Respond(d) => assert!(matches!(
            decode_stream(&d.frame).unwrap().as_slice(),
            [MessageV1::WatchClose(c)] if c.watch_id == 9
        )),
        other => panic!("{other:?}"),
    }
    assert!(
        w.frontend
            .pump_watch(&hub, 5, 9, usize::MAX, |_| true)
            .is_empty()
    );
    assert_eq!(hub.open_watches(), 0);

    // Unary: a read after the writes sees the applied value; a frame the
    // client may not send is a violation; a peer role cannot submit.
    let (c3, _) = w.submit(1, 3, get(b"w"));
    w.settle();
    let (_, response) = w.delivered(&c3).unwrap();
    let (_, result) = ok_result(&response);
    let decoded: coord_state::Response = postcard::from_bytes(&result).unwrap();
    assert_eq!(decoded.revision.get(), 2);
    let hello = frame_of(
        &MessageV1::Response(codes::error_response(c3, 1, "x"))
            .encode()
            .unwrap(),
    );
    assert!(matches!(
        w.frontend.on_frame(w.tick, 1, &caller(), &hello, &hub),
        Action::Violation { connection: 1, .. }
    ));
    let voter = Caller {
        role: PeerRole::Voter,
        ..caller()
    };
    let req = frame_of(
        &MessageV1::Request(request(4, get(b"w"), 0).1)
            .encode()
            .unwrap(),
    );
    match w.frontend.on_frame(w.tick, 1, &voter, &req, &hub) {
        Action::Respond(d) => assert_eq!(err_code(&response_of(&d)), codes::NOT_ADMITTED),
        other => panic!("{other:?}"),
    }
    // A voter never admits a submission from a non-collector role.
    let (_, req) = request(5, get(b"w"), 0);
    let admitted = Admission::new(CLUSTER, DOMAIN, AdmissionLimits::default())
        .admit(0, &caller(), &req)
        .unwrap();
    let Submitted::FanOut(fan_out) = w
        .frontend
        .collector_mut()
        .submit(0, &admitted.request)
        .unwrap()
    else {
        panic!()
    };
    let submit = frame_of(&fan_out.frame);
    assert!(matches!(
        admitted_from_submit(PeerRole::Client, &submit),
        Err(coord_collector::IngressError::RoleNotAuthorized(
            PeerRole::Client
        ))
    ));
    assert!(matches!(
        admitted_from_submit(PeerRole::Observer, &submit),
        Err(coord_collector::IngressError::RoleNotAuthorized(
            PeerRole::Observer
        ))
    ));
    assert!(admitted_from_submit(PeerRole::KineCollector, &submit).is_ok());
}

#[test]
fn the_collector_event_trace_is_frozen_for_go_reuse() {
    let mut w = World::new(51);
    let (c1, _) = w.submit(1, 1, put(b"t", b"1"));
    w.settle();
    let (c2, _) = w.submit(2, 2, put(b"t", b"2"));
    let hub = w.hub();
    w.frontend.on_connection_closed(2, &hub);
    w.settle();
    let _ = w.resolve(3, 2, c2);
    let _ = w.resolve(3, 1, CommandId(Digest32([0; 32])));
    let _ = w.submit(3, 1, put(b"t", b"1"));
    let _ = w.submit(3, 1, put(b"t", b"9"));
    let (c3, _) = w.submit(3, 3, put(b"t", b"1"));
    w.frontend
        .collector_mut()
        .on_evidence(
            provenance(r(9), 1),
            ProtocolMessage::SlowAck(SlowAck {
                replica: r(9),
                ballot: ballot(),
                command: c3,
            }),
        )
        .unwrap_err();
    assert_ne!(c1, c3);
    let trace = w.frontend.collector().trace().to_vec();
    let rendered = serde_json::to_string_pretty(&trace).unwrap();
    let path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/collector_trace_v1.json");
    if std::env::var_os("COORD_COLLECTOR_WRITE_FIXTURES").is_some() {
        std::fs::write(&path, format!("{rendered}\n")).unwrap();
    }
    let frozen = std::fs::read_to_string(&path).expect("fixture present");
    let frozen: Vec<CollectorEvent> = serde_json::from_str(&frozen).unwrap();
    assert_eq!(
        trace, frozen,
        "collector trace changed; regenerate deliberately"
    );
    let kinds: Vec<&str> = trace
        .iter()
        .map(|e| match e {
            CollectorEvent::Submitted { .. } => "submitted",
            CollectorEvent::Attached { .. } => "attached",
            CollectorEvent::Retained { .. } => "retained",
            CollectorEvent::Refused { .. } => "refused",
            CollectorEvent::Evidence { .. } => "evidence",
            CollectorEvent::Held { .. } => "held",
            CollectorEvent::Released { .. } => "released",
            CollectorEvent::Cancelled { .. } => "cancelled",
            CollectorEvent::Resolved { .. } => "resolved",
            CollectorEvent::TimedOut { .. } => "timed-out",
            CollectorEvent::Reconfigured { .. } => "reconfigured",
        })
        .collect();
    for k in [
        "submitted",
        "evidence",
        "held",
        "released",
        "cancelled",
        "resolved",
        "retained",
        "refused",
    ] {
        assert!(kinds.contains(&k), "trace covers {k}");
    }
    assert!(trace.iter().any(|e| matches!(
        e,
        CollectorEvent::Evidence { accepted: false, reason: Some(r), .. } if r.contains("NotAVoter")
    )));
}

#[test]
fn a_retry_of_an_unresolved_request_takes_no_second_admission_slot() {
    // The bound counts distinct unresolved requests. Counting every
    // admission instead refused a client's first retry under a bound of
    // one, and under any bound left the session permanently busy once the
    // retries outnumbered the single release that settles them.
    let mut gate = Admission::new(
        CLUSTER,
        DOMAIN,
        AdmissionLimits {
            max_pending_per_session: 1,
        },
    );
    let (_, first) = request(1, put(b"a", b"1"), 0);
    gate.admit(0, &caller(), &first).expect("admitted");
    assert_eq!(gate.pending(&SESSION), 1);
    for _ in 0..8 {
        gate.admit(0, &caller(), &first)
            .expect("a retry of the same request is the same request");
        assert_eq!(gate.pending(&SESSION), 1, "no slot was taken");
    }
    // A different request is refused while the one slot is occupied.
    let (_, other) = request(2, put(b"b", b"2"), 0);
    assert!(matches!(
        gate.admit(0, &caller(), &other),
        Err(AdmissionRefusal::SessionBusy { pending: 1 })
    ));
    // One settlement frees the request, however many times it was
    // retried, and settling it again frees nothing else.
    gate.settled(&first.retry_key);
    assert_eq!(gate.pending(&SESSION), 0);
    gate.settled(&first.retry_key);
    gate.admit(0, &caller(), &other).expect("the slot is free");
    assert_eq!(gate.pending(&SESSION), 1);
}

#[test]
fn a_request_retried_on_a_new_connection_survives_the_old_one_closing() {
    // A retry can arrive on a new connection. Leaving the key in the old
    // connection's set made that connection's close remove the new owner
    // and cancel the request, so the release never reached the connection
    // that was waiting for it.
    let mut w = World::new(23);
    let (command, first) = w.submit(7, 1, put(b"c", b"1"));
    assert!(matches!(first, Action::FanOut(_)));
    // The client reconnects and retries the same request.
    let (again, second) = w.submit(8, 1, put(b"c", b"1"));
    assert_eq!(again, command, "the same request, not a new one");
    assert!(matches!(second, Action::Pending { .. }));
    // The old connection goes away: it owns nothing now, so nothing is
    // cancelled.
    let hub = w.hub();
    assert!(
        w.frontend.on_connection_closed(7, &hub).is_empty(),
        "the reattached request is not the old connection's to cancel"
    );
    w.settle();
    let (_, response) = w
        .delivered(&command)
        .expect("the release reaches the connection now attached");
    assert_eq!(response.command_id, command);
    let to = w
        .deliveries
        .iter()
        .find(|(_, d)| response_of(d).command_id == command)
        .map(|(_, d)| d.connection)
        .expect("delivered");
    assert_eq!(to, 8, "delivered to the connection that retried");
}

#[test]
fn a_release_larger_than_an_api_frame_still_reaches_the_collector() {
    // The release used to take the API class limit while a result may be
    // as large as the storage bound, so a valid result between the two
    // could not be framed, never reached the collector, and left its
    // request pending for ever with nothing to answer.
    let established = EstablishedResult::restore(coord_core::capability::EstablishedRecord {
        command: CommandId(Digest32([1; 32])),
        epoch: epoch(),
        ballot: ballot(),
        position: ExecutionPosition::new(2).unwrap(),
        result_digest: Digest32([2; 32]),
        revision: Some(KvRevision::new(3).unwrap()),
        fast_path: false,
    })
    .expect("a valid established record");
    let big = vec![0u8; 6 * 1024 * 1024];
    let released =
        coord_core::capability::ReleasedResult::from_gate(established, big.clone(), false);
    let frame = coord_collector::release_frame(&released).expect("a large result still frames");
    let decoded = decode_release(&frame_of(&frame)).expect("round trip");
    assert_eq!(decoded.response().len(), big.len());
}

#[test]
fn a_conflicting_retry_never_frees_the_original_request_s_slot() {
    // Admission counts distinct retry keys, so a re-presentation takes no
    // slot - but refusing one released the slot the original still held.
    // With a bound of one: submit A, submit a conflicting A, and B was
    // then admitted while A was still outstanding.
    let mut gate = Admission::new(
        CLUSTER,
        DOMAIN,
        AdmissionLimits {
            max_pending_per_session: 1,
        },
    );
    let (_, first) = request(1, put(b"a", b"1"), 0);
    let admitted = gate.admit(0, &caller(), &first).expect("admitted");
    assert!(admitted.reserved, "the first presentation took the slot");
    assert_eq!(gate.pending(&SESSION), 1);

    // The same retry key with a different payload: the collector refuses
    // it, and the refusal must not release the original's reservation.
    let (_, conflicting) = request(1, put(b"a", b"different"), 0);
    assert_eq!(conflicting.retry_key, first.retry_key);
    let again = gate
        .admit(0, &caller(), &conflicting)
        .expect("admission is by identity; the conflict is the collector's to see");
    assert!(
        !again.reserved,
        "a re-presentation of an outstanding key takes no slot"
    );
    gate.settled_reservation(&conflicting.retry_key, again.reserved);
    assert_eq!(
        gate.pending(&SESSION),
        1,
        "the original is still outstanding and still holds its slot"
    );

    // So a second request is still refused.
    let (_, other) = request(2, put(b"b", b"2"), 0);
    assert!(matches!(
        gate.admit(0, &caller(), &other),
        Err(AdmissionRefusal::SessionBusy { pending: 1 })
    ));
    // Settling the original frees it, as before.
    gate.settled(&first.retry_key);
    assert_eq!(gate.pending(&SESSION), 0);
    gate.admit(0, &caller(), &other).expect("the slot is free");
}

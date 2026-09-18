//! task-27 acceptance: fixed-membership crash recovery qualified end to end
//! on the real redb engine (fidelity level B: a fault-injecting storage
//! backend with distinct volatile and durable images) under a logical
//! network (level A: seeded delivery permutations, directed cuts).
//!
//! Every replica runs the production consensus machines and the common
//! materializer over `RedbEngine`; a crash freezes the backend, derives
//! the surviving image (possibly a torn, reordered unsynced tail) and the
//! next process rebuilds its role from the projection alone through
//! `coord_storage::protocol::read_protocol`. Acknowledged outputs, retry
//! digests and lease/policy state survive a lost leader; a minority cannot
//! write; a deliberately omitted durable record is detected both by the
//! recovery consistency check and by the independent history oracle; a
//! crash after the Sync was published preserves the same-ballot choice;
//! follower restarts at every persist boundary reproduce the acknowledged
//! outcomes after recovery.
//!
//! Precise coverage: this is the reference `StoreWorker` over redb with
//! the consensus rows and the application rows in one projection. The
//! journal-first composition (task-j03/j05), quorum-certified checkpoints
//! (task-30) and lagging-replica catch-up (task-50) are not qualified here;
//! crash points cover the consensus persist batches (the materializer's
//! own crash matrix is tasks 11/12/15).

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use coord_consensus::rows::{dependency_update, payload_update};
use coord_consensus::{
    AppliedOutcome, BallotConfiguration, ConfigurationIdentity, Follower, FollowerConfig, Leader,
    LeaderConfig, PayloadRecordV1, Phase, ProtocolMessage, ReplicaRole, SyncDecision,
};
use coord_consensus::{CONSERVATIVE_KEY, CommandRecord};
use coord_core::capability::{AdmissionReceipt, EstablishedResult, VerifierToken};
use coord_core::effect::{BootId, Effect, PeerId, PersistBatch};
use coord_core::event::{
    AdmittedRequest, AuthenticatedPeerMessage, Event, PeerProvenance, StorageEvent,
};
use coord_core::machine::DeterministicMachine;
use coord_core::outbox::BarrierAllocator;
use coord_oracle::{History, ModelResponse, Outcome as OracleOutcome, check_history};
use coord_redb_faultkit::{FaultBackend, FaultPlan, Shared, Tail};
use coord_state::policy::{Action, KeyInterval, PolicyRule};
use coord_state::{LeaseStatus, Outcome, Response};
use coord_storage::codecs::RetryRecordV1;
use coord_storage::policy::{bootstrap_session, rule_update};
use coord_storage::protocol::{RecoveredProtocol, read_protocol};
use coord_storage::{Applier, GroupLimits, StoreWorker, ViewBudget, active_leases};
use coord_storage_redb::RedbEngine;
use coord_store_api::engine::{OrderedRead, ScanRequest};
use coord_store_api::registry::Collection;
use coord_types::identity::Digest32;
use coord_types::ids::*;
use coord_types::logical_v1::*;
use coord_types::wire_v1::{MessageV1, RequestV1};
use coord_types::{CommandId, RetryKey};

const NS: NamespaceId = NamespaceId([5; 16]);
const SESSION: SessionId = SessionId([3; 16]);
const CLIENT: ClientInstanceId = ClientInstanceId([4; 16]);
const ALICE: PrincipalId = PrincipalId([0xa; 16]);
const L1: LeaseId = LeaseId([7; 16]);
const FRONTEND: PeerId = PeerId {
    replica: ReplicaId([0xf0; 16]),
    incarnation: ReplicaIncarnation::ZERO,
};
const CACHE: usize = 4 << 20;

fn r(i: u8) -> ReplicaId {
    ReplicaId([i; 16])
}

fn epoch() -> ConfigurationEpoch {
    ConfigurationEpoch::new(1).unwrap()
}

fn ballot(number: u64, leader: u8) -> Ballot {
    Ballot {
        epoch: epoch(),
        number,
        leader: r(leader),
    }
}

fn inc() -> ReplicaIncarnation {
    ReplicaIncarnation::new(1).unwrap()
}

fn identity(me: u8) -> ConfigurationIdentity {
    ConfigurationIdentity {
        cluster: ClusterId([1; 16]),
        domain: DomainId([2; 16]),
        epoch: epoch(),
        voters: (0..3).map(r).collect(),
        replica: r(me),
        incarnation: inc(),
        role: ReplicaRole::Voter,
    }
}

fn quorum(b: Ballot) -> BallotConfiguration {
    BallotConfiguration::c2_default(epoch(), b, (0..3).map(r).collect()).unwrap()
}

fn boot_id(me: usize, boot: u8) -> BootId {
    let mut b = [0u8; 16];
    b[0] = me as u8 + 1;
    b[1] = boot;
    BootId(b)
}

#[allow(clippy::large_enum_variant)]
enum Role {
    Leader(Leader),
    Follower(Follower),
}

struct Node {
    role: Option<Role>,
    applier: Option<Applier<RedbEngine>>,
    shared: Arc<Shared>,
    /// The durable image while the node is down.
    image: Vec<u8>,
    inbox: VecDeque<(ReplicaId, Vec<u8>)>,
    established: Vec<EstablishedResult>,
    executed: Vec<CommandId>,
    alive: bool,
    boot: u8,
    /// Consensus persist batches this node submitted (all boots).
    batches: u64,
    /// Deliberate fault: never write dependency rows (Section 5.1 says a
    /// publication without its durable support is a bug; the oracle must
    /// see it).
    omit_dependency_rows: bool,
    /// The replica stopped executing: the materializer's outcome and the
    /// learner disagreed (a fail-closed detection, not a crash).
    halted: Option<String>,
}

impl Node {
    fn step(&mut self, e: Event) -> Vec<Effect> {
        match self.role.as_mut().unwrap() {
            Role::Leader(m) => m.step(e),
            Role::Follower(m) => m.step(e),
        }
    }
    fn follower(&self) -> &Follower {
        match self.role.as_ref().unwrap() {
            Role::Follower(f) => f,
            Role::Leader(_) => panic!("leader"),
        }
    }
    fn follower_mut(&mut self) -> &mut Follower {
        match self.role.as_mut().unwrap() {
            Role::Follower(f) => f,
            Role::Leader(_) => panic!("leader"),
        }
    }
    fn is_leader(&self) -> bool {
        matches!(self.role, Some(Role::Leader(_)))
    }
    fn next_executable(&self) -> Option<CommandId> {
        match self.role.as_ref().unwrap() {
            Role::Leader(m) => m.next_executable(),
            Role::Follower(m) => m.next_executable(),
        }
    }
    fn executed_through(&self) -> ExecutionPosition {
        match self.role.as_ref().unwrap() {
            Role::Leader(m) => m.executed_through(),
            Role::Follower(m) => m.executed_through(),
        }
    }
    fn payload(&self, c: &CommandId) -> Option<PayloadRecordV1> {
        match self.role.as_ref().unwrap() {
            Role::Leader(m) => m.payload(c).cloned(),
            Role::Follower(m) => m.payload(c).cloned(),
        }
    }
    fn applied(&mut self, c: CommandId, o: &AppliedOutcome) -> Result<Vec<Effect>, String> {
        match self.role.as_mut().unwrap() {
            Role::Leader(m) => m.applied(c, o).map_err(|e| format!("{e:?}")),
            Role::Follower(m) => m.applied(c, o).map_err(|e| format!("{e:?}")),
        }
    }
    fn applier(&mut self) -> &mut Applier<RedbEngine> {
        self.applier.as_mut().unwrap()
    }
    fn view(&self) -> coord_storage::GatedView<coord_storage_redb::RedbView> {
        self.applier
            .as_ref()
            .unwrap()
            .worker()
            .reader()
            .snapshot()
            .unwrap()
    }
    fn kv_revision(&self) -> u64 {
        self.applier.as_ref().unwrap().kv_revision().unwrap().get()
    }
}

/// Bootstrap rows every replica starts from: the session and Alice's
/// permissions (KV actions on keys below "z"; lease actions, which the
/// planner authorizes over the whole key space, unbounded).
fn bootstrap_updates() -> Vec<coord_core::effect::StoreUpdate> {
    let mut updates = bootstrap_session(&SESSION, ALICE, 64, true).unwrap();
    for (i, action) in Action::ALL.iter().enumerate() {
        let upper = match action {
            Action::LeaseGrant
            | Action::LeaseAttach
            | Action::LeaseInspect
            | Action::LeaseRenew
            | Action::LeaseRevoke => None,
            _ => Some(b"z".to_vec()),
        };
        updates.push(
            rule_update(
                &PolicyRuleId([i as u8 + 1; 16]),
                &PolicyRule {
                    principal: ALICE,
                    action: *action,
                    namespace: NS,
                    interval: KeyInterval {
                        lower: vec![],
                        upper,
                    },
                },
            )
            .unwrap(),
        );
    }
    updates
}

fn fresh_node(me: usize) -> Node {
    let (backend, shared) = FaultBackend::new(Vec::new(), FaultPlan::default());
    let engine = RedbEngine::create_on_backend(backend, CACHE).unwrap();
    let boot = boot_id(me, 1);
    let mut worker = StoreWorker::open(engine, boot, inc(), GroupLimits::default()).unwrap();
    let mut alloc = BarrierAllocator::new(inc(), boot);
    let base = worker.application_base();
    worker
        .submit(PersistBatch {
            barrier: alloc.allocate(),
            base: Some(base),
            updates: bootstrap_updates(),
        })
        .unwrap();
    worker.flush().unwrap();
    // The bootstrap batch is ordered work: both machines resume from the
    // application's durable execution position.
    let executed_through = worker.application_base().execution_position;
    let applier = Applier::new(worker, alloc).unwrap();
    let role = if me == 0 {
        Role::Leader(Leader::new(
            LeaderConfig {
                identity: identity(0),
                quorum: quorum(ballot(0, 0)),
                genesis: ballot(0, 0),
                frontend: FRONTEND,
                capacity: 64,
            },
            None,
            executed_through,
        ))
    } else {
        Role::Follower(Follower::recover(
            FollowerConfig {
                identity: identity(me as u8),
                quorum: quorum(ballot(0, 0)),
                genesis: ballot(0, 0),
                frontend: FRONTEND,
                capacity: 64,
            },
            None,
            core::iter::empty(),
            core::iter::empty(),
            executed_through,
        ))
    };
    let mut node = Node {
        role: Some(role),
        applier: Some(applier),
        shared,
        image: Vec::new(),
        inbox: VecDeque::new(),
        established: Vec::new(),
        executed: Vec::new(),
        alive: true,
        boot: 1,
        batches: 0,
        omit_dependency_rows: false,
        halted: None,
    };
    node.step(Event::Boot {
        boot_id: boot,
        incarnation: inc(),
    });
    node
}

/// Where a node's process dies relative to one of its consensus persist
/// batches.
#[derive(Clone, Copy)]
struct CrashPoint {
    node: usize,
    batch: u64,
    /// Before the batch reaches the engine (it is lost) or after it is
    /// durable but before the completion event and its publications.
    before_flush: bool,
    tail: Tail,
    /// Restart immediately (same schedule) rather than staying down.
    revive: bool,
}

struct Cluster {
    nodes: Vec<Node>,
    seed: u64,
    /// (from, to) pairs whose frames are dropped.
    cut: Vec<(u8, u8)>,
    /// Sync frames between these pairs are dropped (everything else flows).
    drop_sync: Vec<(u8, u8)>,
    frontend: Vec<(ReplicaId, ProtocolMessage)>,
    crash_point: Option<CrashPoint>,
    /// Crash points that fired.
    fired: Vec<(usize, u64, bool)>,
    /// (sequence, operation, command) of every admitted request.
    ops: Vec<(u64, CanonicalOperation, CommandId)>,
    history: History,
    tick: u64,
}

impl Cluster {
    fn new(seed: u64) -> Self {
        Cluster {
            nodes: (0..3).map(fresh_node).collect(),
            seed,
            cut: Vec::new(),
            drop_sync: Vec::new(),
            frontend: Vec::new(),
            crash_point: None,
            fired: Vec::new(),
            ops: Vec::new(),
            history: History::new(),
            tick: 0,
        }
    }

    fn rand(&mut self) -> u64 {
        self.seed ^= self.seed << 13;
        self.seed ^= self.seed >> 7;
        self.seed ^= self.seed << 17;
        self.seed
    }

    fn crash_due(&self, i: usize, before_flush: bool) -> Option<CrashPoint> {
        let cp = self.crash_point?;
        (cp.node == i && cp.batch == self.nodes[i].batches && cp.before_flush == before_flush)
            .then_some(cp)
    }

    fn fire(&mut self, i: usize, cp: CrashPoint) {
        self.crash_point = None;
        self.fired.push((i, cp.batch, cp.before_flush));
        self.crash(i, cp.tail);
        if cp.revive {
            self.revive(i);
        }
    }

    /// Handle effects of node `i`: persist through its worker (flush syncs
    /// the fault backend), release sends into inboxes.
    fn handle(&mut self, i: usize, effects: Vec<Effect>) {
        for e in effects {
            if !self.nodes[i].alive {
                return;
            }
            match e {
                Effect::Persist(mut batch) => {
                    let barrier = batch.barrier;
                    self.nodes[i].batches += 1;
                    if let Some(cp) = self.crash_due(i, true) {
                        self.fire(i, cp);
                        return;
                    }
                    if self.nodes[i].omit_dependency_rows {
                        batch.updates.retain(|u| {
                            !(u.collection == Collection::ProtocolV1.id()
                                && u.key.len() == 41
                                && u.key[8] == 0x01)
                        });
                    }
                    let node = &mut self.nodes[i];
                    node.applier().worker_mut().submit(batch).unwrap();
                    node.applier().worker_mut().flush().unwrap();
                    if let Some(cp) = self.crash_due(i, false) {
                        self.fire(i, cp);
                        return;
                    }
                    let more = self.nodes[i].step(Event::Storage(StorageEvent::JournalDurable {
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
                    let dest = to.replica.0[0];
                    if self.cut.contains(&(i as u8, dest)) || !self.nodes[dest as usize].alive {
                        continue;
                    }
                    if self.drop_sync.contains(&(i as u8, dest))
                        && matches!(
                            ProtocolMessage::decode(&frame).unwrap(),
                            ProtocolMessage::Sync(_)
                        )
                    {
                        continue;
                    }
                    self.nodes[dest as usize].inbox.push_back((from, frame));
                }
                Effect::Established(result) => self.nodes[i].established.push(result),
                other => panic!("{other:?}"),
            }
        }
    }

    /// Convert a deposed leader into a follower (carrying a pending Sync)
    /// and a follower that won its campaign into the leader.
    fn convert_roles(&mut self) {
        for i in 0..self.nodes.len() {
            if !self.nodes[i].alive {
                continue;
            }
            let deposed = matches!(&self.nodes[i].role, Some(Role::Leader(l)) if l.deposed());
            if deposed {
                let Some(Role::Leader(leader)) = self.nodes[i].role.take() else {
                    unreachable!()
                };
                let old = leader.config_quorum();
                let pending = leader.pending_sync().cloned();
                let mut follower = Follower::from_recovered(leader.into_recovered(), old);
                let effects = match pending {
                    Some((from, decision)) => follower.on_sync(from, decision),
                    None => Vec::new(),
                };
                self.nodes[i].role = Some(Role::Follower(follower));
                self.handle(i, effects);
            }
            let won = matches!(&self.nodes[i].role, Some(Role::Follower(f)) if f.won().is_some());
            if won {
                let Some(Role::Follower(follower)) = self.nodes[i].role.take() else {
                    unreachable!()
                };
                let decision = follower.won().cloned().unwrap();
                let q = follower.quorum().clone();
                let (leader, effects) =
                    Leader::from_recovered(follower.into_recovered(), q, &decision);
                self.nodes[i].role = Some(Role::Leader(leader));
                self.handle(i, effects);
            }
        }
    }

    fn settle(&mut self) {
        loop {
            let mut progressed = false;
            let candidates: Vec<usize> = (0..self.nodes.len())
                .filter(|i| self.nodes[*i].alive && !self.nodes[*i].inbox.is_empty())
                .collect();
            if !candidates.is_empty() {
                let pick = candidates[(self.rand() % candidates.len() as u64) as usize];
                let len = self.nodes[pick].inbox.len();
                let idx = if self.rand().is_multiple_of(3) && len > 1 {
                    len - 1
                } else {
                    0
                };
                let (from, frame) = self.nodes[pick].inbox.remove(idx).unwrap();
                let event = Event::Peer(AuthenticatedPeerMessage::new(
                    PeerProvenance::from_transport(from, inc(), 1),
                    frame,
                ));
                let effects = self.nodes[pick].step(event);
                self.handle(pick, effects);
                progressed = true;
            }
            self.convert_roles();
            for i in 0..self.nodes.len() {
                loop {
                    if !self.nodes[i].alive || self.nodes[i].halted.is_some() {
                        break;
                    }
                    let Some(c) = self.nodes[i].next_executable() else {
                        break;
                    };
                    let payload = self.nodes[i].payload(&c).expect("payload known");
                    let outcome = self.nodes[i].applier().apply(c, &payload).unwrap();
                    progressed = true;
                    match self.nodes[i].applied(c, &outcome) {
                        Ok(effects) => {
                            self.nodes[i].executed.push(c);
                            self.handle(i, effects);
                        }
                        Err(e) => {
                            self.nodes[i].halted = Some(e);
                            break;
                        }
                    }
                }
            }
            for i in 0..self.nodes.len() {
                if !self.nodes[i].alive {
                    continue;
                }
                if let Some(Role::Follower(f)) = self.nodes[i].role.as_mut()
                    && !f.missing_payloads().is_empty()
                {
                    let leader = f.quorum().leader();
                    let effects = f.request_payloads(leader);
                    if !effects.is_empty() {
                        self.handle(i, effects);
                        progressed = true;
                    }
                }
            }
            if !progressed {
                return;
            }
        }
    }

    /// The frontend admits a request to every live node.
    fn admit(&mut self, seq: u64, op: CanonicalOperation) -> CommandId {
        let mut request = LogicalRequest::new(NS, op.clone());
        request.canonicalize();
        let key = retry_key(seq);
        let command = CommandId::derive(&key, &request).unwrap();
        let frame = MessageV1::Request(RequestV1::new(key, &request, 0).unwrap())
            .encode()
            .unwrap();
        self.tick += 1;
        if self.ops.iter().all(|(s, _, _)| *s != seq) {
            self.history
                .invoke(seq as u32, 1, self.tick, Some(seq), op.clone());
            self.ops.push((seq, op, command));
        }
        for i in 0..self.nodes.len() {
            if !self.nodes[i].alive {
                continue;
            }
            let receipt = AdmissionReceipt::from_verifier(
                VerifierToken::for_boundary(),
                SESSION,
                1,
                u32::MAX,
                Digest32([9; 32]),
                0,
            );
            let effects = self.nodes[i].step(Event::Admitted(AdmittedRequest {
                receipt,
                frame: frame.clone(),
            }));
            self.handle(i, effects);
        }
        command
    }

    /// Crash node `i`: the backend freezes (nothing can reach the durable
    /// image any more), the surviving image is derived under `tail`, and
    /// every handle is dropped.
    fn crash(&mut self, i: usize, tail: Tail) {
        let node = &mut self.nodes[i];
        node.shared.crash();
        node.image = node.shared.crash_image(tail);
        node.role = None;
        node.applier = None;
        node.inbox.clear();
        node.alive = false;
    }

    /// Restart node `i` from its durable image: the projection is the only
    /// input; a bound Sync of a ballot this replica leads resumes.
    fn revive(&mut self, i: usize) {
        let (backend, shared) = FaultBackend::new(
            std::mem::take(&mut self.nodes[i].image),
            FaultPlan::default(),
        );
        let engine = RedbEngine::from_backend(backend, CACHE).unwrap();
        self.nodes[i].boot += 1;
        let boot = boot_id(i, self.nodes[i].boot);
        let worker = StoreWorker::open(engine, boot, inc(), GroupLimits::default()).unwrap();
        let recovered = {
            let gated = worker.reader().snapshot().unwrap();
            read_protocol(gated.view(), epoch(), ViewBudget::default()).unwrap()
        };
        let frontier = worker.meta().frontier.execution_position;
        assert!(
            recovery_consistent(&recovered, frontier),
            "node {i}: executed identities and the application frontier agree"
        );
        let synced = recovered
            .promise
            .as_ref()
            .map_or(ballot(0, 0), |p| p.synced);
        let executed: Vec<CommandId> = recovered.executed.iter().map(|(c, _)| *c).collect();
        let resume = recovered.resumable_sync(&r(i as u8)).cloned();
        let mut f = Follower::recover(
            FollowerConfig {
                identity: identity(i as u8),
                genesis: ballot(0, 0),
                quorum: quorum(synced),
                frontend: FRONTEND,
                capacity: 64,
            },
            recovered.promise.clone(),
            recovered.records.clone(),
            recovered.payloads.clone(),
            frontier,
        )
        .restore_execution(frontier, executed)
        .restore_payloads(recovered.payloads.clone());
        f.step(Event::Boot {
            boot_id: boot,
            incarnation: inc(),
        });
        let node = &mut self.nodes[i];
        node.applier = Some(Applier::new(worker, BarrierAllocator::new(inc(), boot)).unwrap());
        node.role = Some(Role::Follower(f));
        node.shared = shared;
        node.alive = true;
        if let Some(decision) = resume {
            let effects = self.nodes[i].follower_mut().resume_campaign(decision);
            self.handle(i, effects);
        }
    }

    fn recovered(&self, i: usize) -> RecoveredProtocol {
        let gated = self.nodes[i].view();
        read_protocol(gated.view(), epoch(), ViewBudget::default()).unwrap()
    }

    fn campaign(&mut self, i: usize, b: Ballot) {
        let effects = self.nodes[i].follower_mut().campaign(b);
        self.handle(i, effects);
    }

    /// Every row of the application and protocol collections of node `i`.
    fn rows(&self, i: usize) -> BTreeMap<(u16, Vec<u8>), Vec<u8>> {
        let gated = self.nodes[i].view();
        let mut out = BTreeMap::new();
        for c in [
            Collection::KvCurrentV1,
            Collection::KvHistoryV1,
            Collection::EventsV1,
            Collection::RetryV1,
            Collection::ExecutedV1,
            Collection::LeaseV1,
            Collection::LeaseKeysV1,
            Collection::SessionV1,
            Collection::PolicyV1,
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

    /// Retained retry records of node `i` by sequence.
    fn retry_records(&self, i: usize) -> BTreeMap<u64, RetryRecordV1> {
        let gated = self.nodes[i].view();
        self.ops
            .iter()
            .filter_map(|(seq, _, _)| {
                coord_storage::retry::lookup(gated.view(), &retry_key(*seq))
                    .unwrap()
                    .map(|rec| (*seq, rec))
            })
            .collect()
    }

    /// Record the responses node `i` retains as observed now (KV
    /// operations only: the oracle models KV, not leases).
    fn observe(&mut self, i: usize) {
        let records = self.retry_records(i);
        let ops = self.ops.clone();
        for (seq, op, _) in ops {
            if !is_kv(&op) || self.responded(seq) {
                continue;
            }
            if let Some(rec) = records.get(&seq) {
                let response: Response = postcard::from_bytes(&rec.response).unwrap();
                let Some(outcome) = to_oracle(&response.outcome) else {
                    continue;
                };
                self.tick += 1;
                self.history.respond(
                    seq as u32,
                    self.tick,
                    ModelResponse {
                        revision: response.revision.get(),
                        outcome,
                    },
                );
            }
        }
    }

    /// The client retries every acknowledged KV invocation against node
    /// `i` after the fault: a new invocation with the same retry identity,
    /// answered from that node's retained record. A recovered cluster must
    /// answer exactly as before (the oracle's retry rule).
    fn observe_retries(&mut self, i: usize, id_offset: u32) {
        let records = self.retry_records(i);
        let ops = self.ops.clone();
        for (seq, op, _) in ops {
            if !is_kv(&op) {
                continue;
            }
            let Some(rec) = records.get(&seq) else {
                continue;
            };
            let response: Response = postcard::from_bytes(&rec.response).unwrap();
            let Some(outcome) = to_oracle(&response.outcome) else {
                continue;
            };
            self.tick += 1;
            let id = id_offset + seq as u32;
            self.history.invoke(id, 2, self.tick, Some(seq), op.clone());
            self.tick += 1;
            self.history.respond(
                id,
                self.tick,
                ModelResponse {
                    revision: response.revision.get(),
                    outcome,
                },
            );
        }
    }

    fn responded(&self, seq: u64) -> bool {
        self.history.observations().iter().any(
            |o| matches!(o, coord_oracle::Observation::Respond { id, .. } if *id == seq as u32),
        )
    }

    fn leaders(&self) -> Vec<usize> {
        (0..self.nodes.len())
            .filter(|i| self.nodes[*i].alive && self.nodes[*i].is_leader())
            .collect()
    }
}

/// The restart check every replica performs before it votes again: the
/// executed identities recovered with the dependency rows must account
/// for the application frontier (Section 5.2: a projection commit cannot
/// compensate for a missing protocol row).
/// Execution positions the ordered bootstrap batch occupies before any
/// command runs: session and policy rows decide admission, so they are
/// ordered work and advance the frontier.
const BOOTSTRAP_POSITIONS: u64 = 1;

/// The executed protocol rows must account for the application frontier:
/// the last executed command sits exactly at it, or nothing has executed
/// yet and the frontier has moved no further than the ordered bootstrap
/// batch. A replica whose dependency rows were lost while its frontier
/// advanced fails this check.
fn recovery_consistent(recovered: &RecoveredProtocol, frontier: ExecutionPosition) -> bool {
    match recovered.executed_through().get() {
        0 => frontier.get() <= BOOTSTRAP_POSITIONS,
        last => last == frontier.get(),
    }
}

fn is_kv(op: &CanonicalOperation) -> bool {
    matches!(
        op,
        CanonicalOperation::Put(_) | CanonicalOperation::Range(_) | CanonicalOperation::Txn(_)
    )
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

fn put_leased(k: &[u8], v: &[u8], lease: LeaseId) -> CanonicalOperation {
    CanonicalOperation::Put(PutOp {
        key: k.to_vec(),
        value: v.to_vec(),
        lease: Some(lease),
        prev_kv: false,
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

fn grant(ttl: u32) -> CanonicalOperation {
    CanonicalOperation::LeaseGrant {
        lease_id: L1,
        ttl_seconds: ttl,
    }
}

/// KV outcomes as the oracle models them; a lease error (what a leased
/// write returns when the grant did not precede it) has no model, so the
/// observation is skipped and the divergence shows through the other
/// responses of the same history.
fn to_oracle(o: &Outcome) -> Option<OracleOutcome> {
    let e = |e: &coord_state::KvEntry| coord_oracle::KvEntry {
        value: e.value.clone(),
        create_revision: e.create_revision.get(),
        mod_revision: e.mod_revision.get(),
        version: e.version,
        lease: e.lease.map(|l| l.0),
    };
    let items = |i: &[coord_state::RangeItem]| {
        i.iter()
            .map(|x| coord_oracle::RangeItem {
                key: x.key.clone(),
                entry: e(&x.entry),
            })
            .collect()
    };
    Some(match o {
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
            results: results.iter().map(to_oracle).collect::<Option<Vec<_>>>()?,
        },
        _ => return None,
    })
}

/// The history every scenario starts from: KV writes, a lease with an
/// attached key, a compare-and-swap pair and a read.
fn base_history() -> Vec<(u64, CanonicalOperation)> {
    vec![
        (1, put(b"a", b"1")),
        (2, grant(30)),
        (3, put_leased(b"b", b"2", L1)),
        (4, cas(b"a", 1, b"11")),
        (5, cas(b"a", 1, b"12")),
        (6, get(b"b")),
    ]
}

/// Acknowledged outputs of node `i`: (position, command, result digest).
fn acknowledged(node: &Node) -> Vec<(u64, CommandId, Digest32)> {
    node.established
        .iter()
        .map(|e| (e.position().get(), e.command(), e.result_digest()))
        .collect()
}

#[test]
fn acknowledged_outputs_retry_digests_and_lease_state_survive_a_lost_leader() {
    let mut cluster = Cluster::new(1);
    // r2 hears no proposal: r0 and r1 form the slow quorum and apply.
    cluster.cut = vec![(0, 2), (1, 2)];
    for (seq, op) in base_history() {
        cluster.admit(seq, op);
    }
    cluster.settle();
    cluster.observe(0);
    let order = cluster.nodes[0].executed.clone();
    assert_eq!(order.len(), 6);
    assert_eq!(cluster.nodes[1].executed, order);
    assert!(cluster.nodes[2].executed.is_empty());
    let acked = acknowledged(&cluster.nodes[0]);
    let retries_before = cluster.retry_records(0);
    assert_eq!(retries_before.len(), 6);
    let rows_before = cluster.rows(0);
    assert_eq!(cluster.rows(1), rows_before);

    // The leader is lost for good. r1 crashes with a torn unsynced tail
    // and restarts: its executed identities and application frontier
    // agree, its dependency rows say ACCEPT (the COMMIT knowledge was
    // volatile), and the executed commands are not re-executed.
    cluster.crash(0, Tail::None);
    cluster.crash(1, Tail::Seeded(1));
    cluster.cut.clear();
    cluster.revive(1);
    // Six commands after the ordered bootstrap batch.
    assert_eq!(
        cluster.nodes[1].executed_through().get(),
        6 + BOOTSTRAP_POSITIONS
    );
    for c in &order {
        assert_eq!(
            cluster.nodes[1].follower().table().phase_of(c),
            Some(Phase::Executed)
        );
    }
    assert_eq!(
        cluster.rows(1),
        rows_before,
        "durable rows survive the crash"
    );

    // r2 campaigns: it never stored the payloads, so it fetches them from
    // the reporting voter before binding the selection, then leads.
    cluster.campaign(2, ballot(1, 2));
    cluster.settle();
    assert_eq!(cluster.leaders(), vec![2]);
    assert_eq!(
        cluster.nodes[2].executed, order,
        "r2 executes the acknowledged order"
    );
    assert_eq!(
        cluster.nodes[1].executed, order,
        "r1 executes nothing twice"
    );
    assert_eq!(
        acknowledged(&cluster.nodes[2]),
        acked,
        "same positions and digests"
    );
    // Retry digests and application rows are identical everywhere.
    assert_eq!(cluster.retry_records(2), retries_before);
    assert_eq!(cluster.rows(2), rows_before);
    assert_eq!(cluster.rows(1), rows_before);
    for i in [1usize, 2] {
        let gated = cluster.nodes[i].view();
        let leases = active_leases(gated.view(), ViewBudget::default()).unwrap();
        assert_eq!(leases.len(), 1, "node {i}");
        assert_eq!(leases[0].0, L1);
        assert_eq!(leases[0].1.status, LeaseStatus::Active);
        assert_eq!(leases[0].1.attached_keys, 1);
        assert_eq!(leases[0].1.owner, ALICE);
    }
    // The new ballot serves work: a retry of an acknowledged request
    // returns the retained result, new requests execute after the
    // recovered prefix, and the whole observed history is linearizable.
    cluster.admit(4, cas(b"a", 1, b"11"));
    cluster.admit(7, put(b"a", b"13"));
    cluster.admit(8, get(b"a"));
    cluster.settle();
    cluster.observe(2);
    cluster.observe_retries(2, 100);
    for i in [1usize, 2] {
        assert_eq!(cluster.nodes[i].executed.len(), 8, "node {i}");
        assert_eq!(cluster.nodes[i].kv_revision(), 4);
    }
    assert_eq!(cluster.rows(1), cluster.rows(2));
    let verdict = check_history(&cluster.history);
    assert!(verdict.ok(), "{:?}", verdict.violations);
    // The lost leader returns as a follower of nothing yet: its promise is
    // the genesis ballot, its rows are exactly what it acknowledged, and it
    // cannot serve new writes (catch-up is task-50).
    cluster.revive(0);
    assert_eq!(
        cluster.nodes[0].follower().ballots().promised(),
        ballot(0, 0)
    );
    assert_eq!(
        cluster.nodes[0].executed_through().get(),
        6 + BOOTSTRAP_POSITIONS
    );
    assert_eq!(cluster.rows(0), rows_before);
}

#[test]
fn a_minority_cannot_write() {
    let mut cluster = Cluster::new(7);
    // The leader is cut off from both followers.
    cluster.cut = vec![(0, 1), (0, 2), (1, 0), (2, 0)];
    let c1 = cluster.admit(1, put(b"a", b"1"));
    cluster.settle();
    for (i, node) in cluster.nodes.iter().enumerate() {
        assert!(node.established.is_empty(), "node {i}");
        assert_eq!(node.kv_revision(), 0, "node {i}");
    }
    assert!(
        cluster
            .frontend
            .iter()
            .any(|(from, m)| *from == r(0) && matches!(m, ProtocolMessage::LeaderReply { .. }))
    );
    // The majority recovers under r1: the command the frontend fanned out
    // was pre-accepted there and is re-proposed; it executes on the
    // majority only.
    cluster.campaign(1, ballot(1, 1));
    cluster.settle();
    // r0 never heard the new ballot: it still holds the leader role of
    // ballot 0, which no majority will ever serve again.
    assert_eq!(cluster.leaders(), vec![0, 1]);
    assert_eq!(cluster.nodes[1].executed, vec![c1]);
    assert_eq!(cluster.nodes[2].executed, vec![c1]);
    assert!(cluster.nodes[0].executed.is_empty());
    let c2 = cluster.admit(2, put(b"b", b"2"));
    cluster.settle();
    assert_eq!(cluster.nodes[1].executed, vec![c1, c2]);
    assert_eq!(cluster.nodes[2].executed, vec![c1, c2]);
    assert_eq!(cluster.rows(1), cluster.rows(2));
    // The old leader reconnects still believing in ballot 0: its proposals
    // are stale everywhere, it establishes nothing, and it holds no rows
    // the majority did not acknowledge.
    cluster.cut.clear();
    let c3 = cluster.admit(3, put(b"c", b"3"));
    cluster.settle();
    assert_eq!(cluster.nodes[1].executed, vec![c1, c2, c3]);
    assert!(cluster.nodes[0].established.is_empty());
    assert_eq!(cluster.nodes[0].kv_revision(), 0);
    cluster.observe(1);
    assert!(check_history(&cluster.history).ok());
}

#[test]
fn a_deliberately_omitted_durable_record_is_detected() {
    let mut cluster = Cluster::new(3);
    cluster.nodes[1].omit_dependency_rows = true;
    cluster.cut = vec![(0, 2), (1, 2)];
    for (seq, op) in base_history() {
        cluster.admit(seq, op);
    }
    cluster.settle();
    cluster.observe(0);
    assert_eq!(cluster.nodes[1].executed.len(), 6);
    cluster.crash(0, Tail::None);
    cluster.crash(1, Tail::All);
    cluster.cut.clear();
    // Recovery from the projection alone: executed identities exist for
    // six commands but no dependency row does. The consistency check the
    // restart performs (executed identities versus application frontier)
    // is the first detector.
    let image = cluster.nodes[1].image.clone();
    let (backend, _) = FaultBackend::new(image, FaultPlan::default());
    let engine = RedbEngine::from_backend(backend, CACHE).unwrap();
    let worker = StoreWorker::open(engine, boot_id(1, 9), inc(), GroupLimits::default()).unwrap();
    let recovered = {
        let gated = worker.reader().snapshot().unwrap();
        read_protocol(gated.view(), epoch(), ViewBudget::default()).unwrap()
    };
    assert!(recovered.records.is_empty());
    assert_eq!(recovered.executed_through(), ExecutionPosition::ZERO);
    let frontier = worker.meta().frontier.execution_position;
    assert_eq!(frontier.get(), 6 + BOOTSTRAP_POSITIONS);
    assert!(
        !recovery_consistent(&recovered, frontier),
        "the restart check fails closed"
    );
    drop(worker);
    // Proceed anyway, as a replica without that check would: the report is
    // empty, the selection keeps nothing, and the independent oracle
    // rejects the history once the recovered majority answers.
    let (backend, shared) = FaultBackend::new(
        std::mem::take(&mut cluster.nodes[1].image),
        FaultPlan::default(),
    );
    let engine = RedbEngine::from_backend(backend, CACHE).unwrap();
    let boot = boot_id(1, 3);
    let worker = StoreWorker::open(engine, boot, inc(), GroupLimits::default()).unwrap();
    // A replica trusting its application frontier while its dependency
    // rows are gone: it reports nothing and votes from position six.
    let frontier = worker.meta().frontier.execution_position;
    let mut f = Follower::recover(
        FollowerConfig {
            identity: identity(1),
            genesis: ballot(0, 0),
            quorum: quorum(ballot(0, 0)),
            frontend: FRONTEND,
            capacity: 64,
        },
        None,
        core::iter::empty(),
        core::iter::empty(),
        frontier,
    )
    .restore_execution(frontier, core::iter::empty());
    f.step(Event::Boot {
        boot_id: boot,
        incarnation: inc(),
    });
    let node = &mut cluster.nodes[1];
    node.applier = Some(Applier::new(worker, BarrierAllocator::new(inc(), boot)).unwrap());
    node.role = Some(Role::Follower(f));
    node.shared = shared;
    node.alive = true;
    cluster.campaign(2, ballot(1, 2));
    cluster.settle();
    assert_eq!(cluster.leaders(), vec![2]);
    let decision = &cluster.recovered(2).syncs[0].1;
    assert!(
        decision.entries.is_empty(),
        "the acknowledged commands are gone"
    );
    // The faulty replica itself halts: the re-proposed commands reach its
    // learner at position seven while the materializer returns the
    // retained result of position one.
    assert!(
        cluster.nodes[1]
            .halted
            .as_deref()
            .is_some_and(|h| h.contains("PositionMismatch")),
        "{:?}",
        cluster.nodes[1].halted
    );
    // The majority answers the client's retries and a fresh read from a
    // history that lost the acknowledged prefix: the oracle rejects it.
    cluster.admit(7, get(b"a"));
    cluster.settle();
    cluster.observe(2);
    cluster.observe_retries(2, 100);
    let verdict = check_history(&cluster.history);
    assert!(
        !verdict.ok(),
        "the oracle must reject the lost acknowledged writes"
    );
}

#[test]
fn a_crash_after_publishing_the_sync_preserves_the_same_ballot_choice() {
    let mut cluster = Cluster::new(5);
    cluster.cut = vec![(0, 2), (1, 2)];
    for (seq, op) in base_history().into_iter().take(3) {
        cluster.admit(seq, op);
    }
    cluster.settle();
    let order = cluster.nodes[0].executed.clone();
    cluster.crash(0, Tail::None);
    cluster.cut.clear();
    // r2's Sync is bound and published but the frame to r1 is lost; then
    // r2 crashes.
    cluster.drop_sync = vec![(2, 1)];
    cluster.campaign(2, ballot(1, 2));
    cluster.settle();
    let bound: Vec<SyncDecision> = cluster
        .recovered(2)
        .syncs
        .iter()
        .map(|(_, d)| d.clone())
        .collect();
    assert_eq!(bound.len(), 1);
    assert_eq!(bound[0].entries.len(), 3);
    cluster.crash(2, Tail::Seeded(2));
    cluster.drop_sync.clear();
    // The restart finds the promise for ballot 1 and its bound Sync:
    // exactly that decision is republished, no reselection happens, and
    // the cluster converges on the acknowledged order.
    cluster.revive(2);
    cluster.settle();
    assert_eq!(cluster.leaders(), vec![2]);
    let after: Vec<SyncDecision> = cluster
        .recovered(2)
        .syncs
        .iter()
        .map(|(_, d)| d.clone())
        .collect();
    assert_eq!(after, bound, "no second selection under ballot 1");
    assert_eq!(cluster.nodes[1].follower().ballots().synced(), ballot(1, 2));
    assert_eq!(cluster.nodes[1].executed, order);
    assert_eq!(cluster.nodes[2].executed, order);
    assert_eq!(cluster.rows(1), cluster.rows(2));

    // Variant: the crash lands before the Sync row is durable. Nothing was
    // published, the restart finds no bound selection for the promised
    // ballot, and a valid new ballot is entered instead of reusing it.
    let mut cluster = Cluster::new(6);
    cluster.cut = vec![(0, 2), (1, 2)];
    for (seq, op) in base_history().into_iter().take(3) {
        cluster.admit(seq, op);
    }
    cluster.settle();
    let order = cluster.nodes[0].executed.clone();
    cluster.crash(0, Tail::None);
    cluster.cut.clear();
    // The candidate's batches: the self-promise, then the Sync row (the
    // payload fetch writes come in between when r2 lacks payloads; here
    // it holds them from admission). Find the Sync batch by dry-running.
    let sync_batch = {
        let mut probe = Cluster::new(6);
        probe.cut = vec![(0, 2), (1, 2)];
        for (seq, op) in base_history().into_iter().take(3) {
            probe.admit(seq, op);
        }
        probe.settle();
        probe.crash(0, Tail::None);
        probe.cut.clear();
        let before = probe.nodes[2].batches;
        probe.campaign(2, ballot(1, 2));
        probe.settle();
        assert_eq!(probe.recovered(2).syncs.len(), 1);
        // The candidate's first batch is its self-promise, the second the
        // Sync row; the re-proposals follow it.
        assert!(probe.nodes[2].batches > before + 2);
        before + 2
    };
    cluster.crash_point = Some(CrashPoint {
        node: 2,
        batch: sync_batch,
        before_flush: true,
        tail: Tail::None,
        revive: false,
    });
    cluster.campaign(2, ballot(1, 2));
    cluster.settle();
    assert_eq!(cluster.fired, vec![(2, sync_batch, true)]);
    assert!(!cluster.nodes[2].alive);
    cluster.revive(2);
    let recovered = cluster.recovered(2);
    assert_eq!(recovered.promise.unwrap().promised, ballot(1, 2));
    assert!(recovered.syncs.is_empty(), "nothing was bound");
    assert!(cluster.nodes[2].follower().won().is_none());
    cluster.campaign(2, ballot(2, 2));
    cluster.settle();
    assert_eq!(cluster.leaders(), vec![2]);
    let recovered = cluster.recovered(2);
    assert_eq!(recovered.syncs.len(), 1);
    assert_eq!(recovered.syncs[0].0, ballot(2, 2));
    assert_eq!(recovered.syncs[0].1.entries.len(), 3);
    assert_eq!(cluster.nodes[1].executed, order);
    assert_eq!(cluster.nodes[2].executed, order);
}

#[test]
fn permuted_report_deliveries_select_the_same_result_on_the_real_engine() {
    let mut decisions = Vec::new();
    for seed in [3u64, 11, 19, 23] {
        let mut cluster = Cluster::new(seed);
        cluster.cut = vec![(0, 2), (1, 2)];
        for (seq, op) in base_history() {
            cluster.admit(seq, op);
        }
        cluster.settle();
        cluster.crash(0, Tail::None);
        cluster.crash(1, Tail::Seeded(seed));
        cluster.cut.clear();
        cluster.revive(1);
        cluster.campaign(2, ballot(1, 2));
        cluster.settle();
        let recovered = cluster.recovered(2);
        assert_eq!(recovered.syncs.len(), 1, "seed {seed}");
        assert_eq!(cluster.nodes[2].executed, cluster.nodes[1].executed);
        decisions.push(recovered.syncs[0].1.clone());
    }
    for d in &decisions[1..] {
        assert_eq!(d, &decisions[0]);
    }
}

/// Run the base history with one follower restart at persist batch `k`
/// (before or after it reaches the engine), then recover under a new
/// ballot led by the restarted node and compare with the acknowledged
/// outputs of the original leader.
fn follower_restart_at(k: u64, before_flush: bool) -> (Vec<(usize, u64, bool)>, bool) {
    let mut cluster = Cluster::new(40 + k);
    cluster.crash_point = Some(CrashPoint {
        node: 1,
        batch: k,
        before_flush,
        tail: Tail::Seeded(k),
        revive: true,
    });
    for (seq, op) in base_history() {
        cluster.admit(seq, op);
        cluster.settle();
    }
    cluster.settle();
    let fired = cluster.fired.clone();
    if fired.is_empty() {
        return (fired, false);
    }
    cluster.observe(0);
    let acked = acknowledged(&cluster.nodes[0]);
    let order = cluster.nodes[0].executed.clone();
    assert_eq!(
        order.len(),
        6,
        "k={k} before={before_flush}: the leader and r2 progress"
    );
    let retries = cluster.retry_records(0);
    let rows = cluster.rows(0);
    assert_eq!(cluster.rows(2), rows, "k={k} before={before_flush}: r2");
    // The restarted follower recovers the cluster under its own ballot and
    // must reproduce exactly the acknowledged prefix.
    cluster.campaign(1, ballot(1, 1));
    cluster.settle();
    assert_eq!(cluster.leaders(), vec![1], "k={k} before={before_flush}");
    for i in 0..3 {
        assert_eq!(
            cluster.nodes[i].executed, order,
            "k={k} before={before_flush}: node {i}"
        );
        assert_eq!(
            cluster.rows(i),
            rows,
            "k={k} before={before_flush}: node {i} rows"
        );
        assert_eq!(cluster.retry_records(i), retries);
    }
    assert_eq!(acknowledged(&cluster.nodes[0]), acked);
    cluster.admit(7, put(b"a", b"7"));
    cluster.settle();
    cluster.observe(1);
    assert!(
        check_history(&cluster.history).ok(),
        "k={k} before={before_flush}"
    );
    (fired, true)
}

#[test]
fn follower_restarts_at_every_persist_boundary_reproduce_the_acknowledged_outcomes() {
    // How many consensus batches the follower writes in the base history.
    let total = {
        let mut probe = Cluster::new(2);
        for (seq, op) in base_history() {
            probe.admit(seq, op);
            probe.settle();
        }
        probe.nodes[1].batches
    };
    assert!(total >= 6, "at least one batch per command: {total}");
    let mut covered = 0;
    for k in 1..=total {
        for before in [true, false] {
            let (fired, ran) = follower_restart_at(k, before);
            assert_eq!(fired, vec![(1, k, before)]);
            assert!(ran);
            covered += 1;
        }
    }
    assert_eq!(covered as u64, total * 2);
}

#[test]
fn recovery_refuses_a_payload_row_under_the_wrong_command_key() {
    // A well-formed payload stored under another command's key is
    // corruption, not a command: recovery must refuse it before the
    // replica votes, rather than binding its retry key, serving it to
    // peers and discovering the mismatch only at execution.
    let (backend, _shared) = FaultBackend::new(Vec::new(), FaultPlan::default());
    let engine = RedbEngine::create_on_backend(backend, CACHE).unwrap();
    let boot = boot_id(0, 1);
    let mut worker = StoreWorker::open(engine, boot, inc(), GroupLimits::default()).unwrap();
    let mut alloc = BarrierAllocator::new(inc(), boot);
    let base = worker.application_base();
    worker
        .submit(PersistBatch {
            barrier: alloc.allocate(),
            base: Some(base),
            updates: bootstrap_updates(),
        })
        .unwrap();
    worker.flush().unwrap();
    // A dependency row for `mine` whose payload row holds `other`'s bytes.
    let mine = command_of(1);
    let other = command_of(2);
    assert_ne!(mine, other);
    let record = CommandRecord {
        phase: Phase::PreAccept,
        deps: vec![],
        keys: vec![CONSERVATIVE_KEY.to_vec()],
        payload: Some(mine.0),
        paths: vec![],
        synced_seq: None,
        path: coord_consensus::empty_path(),
    };
    let base = worker.application_base();
    worker
        .submit(PersistBatch {
            barrier: alloc.allocate(),
            base: Some(base),
            updates: vec![
                dependency_update(epoch(), &mine, &record).unwrap(),
                payload_update(&mine, &payload_of(2)).unwrap(),
            ],
        })
        .unwrap();
    worker.flush().unwrap();
    let gated = worker.reader().snapshot().unwrap();
    let err = read_protocol(gated.view(), epoch(), ViewBudget::default())
        .expect_err("the payload does not rehash to its key");
    assert_eq!(err.class, coord_store_api::engine::ErrorClass::Corrupt);
}

#[test]
fn recovery_charges_every_row_against_one_byte_budget() {
    // The budget bounds the whole recovery: many rows that each fit a page
    // must still be refused once their total exceeds it.
    let (backend, _shared) = FaultBackend::new(Vec::new(), FaultPlan::default());
    let engine = RedbEngine::create_on_backend(backend, CACHE).unwrap();
    let boot = boot_id(0, 1);
    let mut worker = StoreWorker::open(engine, boot, inc(), GroupLimits::default()).unwrap();
    let mut alloc = BarrierAllocator::new(inc(), boot);
    let base = worker.application_base();
    worker
        .submit(PersistBatch {
            barrier: alloc.allocate(),
            base: Some(base),
            updates: bootstrap_updates(),
        })
        .unwrap();
    worker.flush().unwrap();
    let mut updates = Vec::new();
    for i in 0..32u64 {
        let command = command_of(100 + i);
        let record = CommandRecord {
            phase: Phase::PreAccept,
            deps: vec![],
            keys: vec![CONSERVATIVE_KEY.to_vec()],
            payload: Some(command.0),
            paths: vec![],
            synced_seq: None,
            path: coord_consensus::empty_path(),
        };
        updates.push(dependency_update(epoch(), &command, &record).unwrap());
        updates.push(payload_update(&command, &payload_of(100 + i)).unwrap());
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
    let gated = worker.reader().snapshot().unwrap();
    // Generous rows, tight bytes: every row fits a page, the total does not.
    let budget = ViewBudget {
        max_rows: 10_000,
        max_bytes: 1024,
    };
    let err = read_protocol(gated.view(), epoch(), budget)
        .expect_err("the cumulative bytes exceed the budget");
    assert_eq!(err.class, coord_store_api::engine::ErrorClass::Limit);
    // The same rows load under a budget that admits them.
    let recovered = read_protocol(gated.view(), epoch(), ViewBudget::default()).unwrap();
    assert_eq!(recovered.records.len(), 32);
}

/// A canonical request whose identity is derived from sequence `seq`.
fn request_of(seq: u64) -> LogicalRequest {
    let mut r = LogicalRequest::new(
        NS,
        CanonicalOperation::Put(PutOp {
            key: seq.to_be_bytes().to_vec(),
            value: vec![7],
            lease: None,
            prev_kv: false,
        }),
    );
    r.canonicalize();
    r
}

fn command_of(seq: u64) -> CommandId {
    CommandId::derive(&retry_key(seq), &request_of(seq)).unwrap()
}

fn payload_of(seq: u64) -> coord_consensus::PayloadRecordV1 {
    coord_consensus::PayloadRecordV1 {
        retry_key: retry_key(seq),
        logical: postcard::to_allocvec(&request_of(seq)).unwrap(),
    }
}

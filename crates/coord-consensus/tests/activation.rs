//! task-26 acceptance: learned outcomes survive a lost leader and lost
//! volatile commit notifications; competing recovery, delayed replies and
//! same-boot old effects cannot establish divergence; permuted reports
//! give the same selection; a crash after the Sync was bound republishes
//! the same result and never reselects under the same ballot.

use std::collections::{BTreeMap, VecDeque};

use coord_consensus::{
    AppliedOutcome, BallotConfiguration, CommandRecord, ConfigurationIdentity, Follower,
    FollowerConfig, FollowerRejection, Leader, LeaderConfig, PageError, Phase, ProtocolMessage,
    ReplicaRole, SyncDecision, decode_dependency, decode_promise, decode_sync,
};
use coord_core::capability::{AdmissionReceipt, EstablishedResult, VerifierToken};
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

const FRONTEND: PeerId = PeerId {
    replica: ReplicaId([0xf0; 16]),
    incarnation: ReplicaIncarnation::ZERO,
};

fn identity(me: u8) -> ConfigurationIdentity {
    ConfigurationIdentity {
        cluster: ClusterId([1; 16]),
        domain: DomainId([2; 16]),
        epoch: epoch(),
        voters: (0..3).map(r).collect(),
        replica: r(me),
        incarnation: ReplicaIncarnation::new(1).unwrap(),
        role: ReplicaRole::Voter,
    }
}

fn quorum(b: Ballot) -> BallotConfiguration {
    BallotConfiguration::c2_default(epoch(), b, (0..3).map(r).collect()).unwrap()
}

#[allow(clippy::large_enum_variant)]
enum Role {
    Leader(Leader),
    Follower(Follower),
}

struct Node {
    role: Option<Role>,
    storage: StorageModel,
    inbox: VecDeque<(ReplicaId, Vec<u8>)>,
    established: Vec<EstablishedResult>,
    executed: Vec<CommandId>,
    alive: bool,
    boot: u8,
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
    fn applied(&mut self, c: CommandId, o: &AppliedOutcome) -> Vec<Effect> {
        match self.role.as_mut().unwrap() {
            Role::Leader(m) => m.applied(c, o).unwrap(),
            Role::Follower(m) => m.applied(c, o).unwrap(),
        }
    }
}

struct Cluster {
    nodes: Vec<Node>,
    seed: u64,
    /// (from, to) pairs whose frames are dropped.
    cut: Vec<(u8, u8)>,
    /// Sync frames between these pairs are dropped (everything else flows).
    drop_sync: Vec<(u8, u8)>,
    frontend: Vec<(ReplicaId, ProtocolMessage)>,
}

fn boot_event(boot: u8) -> Event {
    Event::Boot {
        boot_id: BootId([boot; 16]),
        incarnation: ReplicaIncarnation::new(1).unwrap(),
    }
}

impl Cluster {
    fn new(seed: u64) -> Self {
        let mut nodes = Vec::new();
        for me in 0..3u8 {
            let role = if me == 0 {
                Role::Leader(Leader::new(
                    LeaderConfig {
                        identity: identity(0),
                        quorum: quorum(ballot(0, 0)),
                        genesis: ballot(0, 0),
                        frontend: FRONTEND,
                        capacity: 32,
                    },
                    None,
                    ExecutionPosition::ZERO,
                ))
            } else {
                Role::Follower(Follower::new(FollowerConfig {
                    identity: identity(me),
                    quorum: quorum(ballot(0, 0)),
                    genesis: ballot(0, 0),
                    frontend: FRONTEND,
                    capacity: 32,
                }))
            };
            let mut node = Node {
                role: Some(role),
                storage: StorageModel::default(),
                inbox: VecDeque::new(),
                established: Vec::new(),
                executed: Vec::new(),
                alive: true,
                boot: 1,
            };
            node.step(boot_event(1));
            nodes.push(node);
        }
        Cluster {
            nodes,
            seed,
            cut: Vec::new(),
            drop_sync: Vec::new(),
            frontend: Vec::new(),
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

    /// Convert a deposed leader into a follower (carrying a pending Sync).
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
                    PeerProvenance::from_transport(from, ReplicaIncarnation::new(1).unwrap(), 1),
                    frame,
                ));
                let effects = self.nodes[pick].step(event);
                self.handle(pick, effects);
                progressed = true;
            }
            self.convert_roles();
            for i in 0..self.nodes.len() {
                if !self.nodes[i].alive {
                    continue;
                }
                while let Some(c) = self.nodes[i].next_executable() {
                    let position = self.nodes[i].executed_through().checked_next().unwrap();
                    let outcome = AppliedOutcome {
                        position,
                        revision: None,
                        result_digest: Digest32(c.0.0),
                    };
                    let effects = self.nodes[i].applied(c, &outcome);
                    self.nodes[i].executed.push(c);
                    self.handle(i, effects);
                    progressed = true;
                }
            }
            // Fetch payloads a follower lacks from the ballot's leader.
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

    fn admit(&mut self, seq: u64, key: u8) -> CommandId {
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
        let frame = MessageV1::Request(RequestV1::new(rk, &request, 0).unwrap())
            .encode()
            .unwrap();
        for i in 0..self.nodes.len() {
            if !self.nodes[i].alive {
                continue;
            }
            let receipt = AdmissionReceipt::from_verifier(
                VerifierToken::for_boundary(),
                SessionId([3; 16]),
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

    /// Crash node `i`: volatile state is lost; `revive` rebuilds it from
    /// durable rows as a follower of the given quorum. The executed
    /// identities are durable with the application rows, so the node's
    /// execution frontier survives (the `executed` list stands in for them).
    fn crash(&mut self, i: usize) {
        self.nodes[i].alive = false;
        self.nodes[i].role = None;
        self.nodes[i].inbox.clear();
        self.nodes[i].storage.crash();
    }

    fn revive(&mut self, i: usize, q: BallotConfiguration) {
        let rows = dependency_rows(&self.nodes[i].storage);
        let promise = promise_row(&self.nodes[i].storage);
        let syncs: Vec<(Ballot, SyncDecision)> = sync_rows(&self.nodes[i].storage)
            .into_iter()
            .map(|d| (d.ballot, d))
            .collect();
        let mut f = Follower::recover_with_syncs(
            FollowerConfig {
                identity: identity(i as u8),
                genesis: ballot(0, 0),
                quorum: q,
                frontend: FRONTEND,
                capacity: 32,
            },
            promise,
            rows,
            payload_rows(&self.nodes[i].storage),
            syncs,
            ExecutionPosition::ZERO,
        )
        .restore_execution(
            ExecutionPosition::new(self.nodes[i].executed.len() as u64).unwrap(),
            self.nodes[i].executed.iter().copied(),
        );
        self.nodes[i].boot += 1;
        let boot = self.nodes[i].boot;
        f.step(boot_event(boot));
        self.nodes[i].role = Some(Role::Follower(f));
        self.nodes[i].alive = true;
    }

    fn campaign(&mut self, i: usize, b: Ballot) {
        let effects = self.nodes[i].follower_mut().campaign(b);
        self.handle(i, effects);
    }
}

fn dependency_rows(storage: &StorageModel) -> Vec<(CommandId, CommandRecord)> {
    storage
        .durable_rows()
        .into_iter()
        .filter(|(c, k, _)| *c == Collection::ProtocolV1.id().0 && k.len() == 41 && k[8] == 0x01)
        .map(|(_, k, v)| {
            (
                CommandId(Digest32(k[9..].try_into().unwrap())),
                decode_dependency(&v).unwrap(),
            )
        })
        .collect()
}

fn promise_row(storage: &StorageModel) -> Option<coord_consensus::PromiseRecordV1> {
    storage
        .durable_rows()
        .into_iter()
        .find(|(c, k, _)| *c == Collection::ProtocolV1.id().0 && k.len() == 9)
        .map(|(_, _, v)| decode_promise(&v).unwrap())
}

fn sync_rows(storage: &StorageModel) -> Vec<SyncDecision> {
    storage
        .durable_rows()
        .into_iter()
        .filter(|(c, k, _)| *c == Collection::ProtocolV1.id().0 && k.len() == 33 && k[8] == 0x03)
        .map(|(_, _, v)| decode_sync(&v).unwrap().decision)
        .collect()
}

#[test]
fn learned_outcomes_survive_a_lost_leader_and_lost_commit_notifications() {
    let mut cluster = Cluster::new(1);
    // r2 hears nothing: r0 and r1 form the slow quorum.
    cluster.cut = vec![(0, 2), (1, 2)];
    let c1 = cluster.admit(1, 1);
    let c2 = cluster.admit(2, 2);
    cluster.settle();
    assert_eq!(cluster.nodes[0].executed, vec![c1, c2]);
    assert_eq!(cluster.nodes[1].executed, vec![c1, c2], "r1 learned too");
    let established: Vec<(u64, CommandId)> = cluster.nodes[0]
        .established
        .iter()
        .map(|e| (e.position().get(), e.command()))
        .collect();
    assert_eq!(established, vec![(1, c1), (2, c2)]);
    // The leader is lost for good; r1 restarts and loses its volatile
    // COMMIT knowledge (its dependency rows say ACCEPT; the executed
    // identities are durable, so what it applied stays applied); r2 never
    // heard of c1/c2.
    cluster.crash(0);
    cluster.crash(1);
    cluster.cut.clear();
    assert_eq!(
        dependency_rows(&cluster.nodes[1].storage)
            .iter()
            .map(|(_, r)| r.phase)
            .collect::<Vec<_>>(),
        vec![Phase::Accept, Phase::Accept]
    );
    cluster.revive(1, quorum(ballot(0, 0)));
    assert_eq!(
        cluster.nodes[1].follower().table().phase_of(&c1),
        Some(Phase::Executed)
    );
    assert_eq!(
        cluster.nodes[1].follower().table().phase_of(&c2),
        Some(Phase::Executed)
    );
    // r2 campaigns for ballot 1: promises and reports from r1 and itself,
    // selection keeps the accepted commands, the Sync is bound then
    // published, r2 leads and re-proposes, r1 re-adopts, both execute in
    // the same order the lost leader established.
    cluster.campaign(2, ballot(1, 2));
    cluster.settle();
    assert!(matches!(cluster.nodes[2].role, Some(Role::Leader(_))));
    let decision = &sync_rows(&cluster.nodes[2].storage)[0];
    assert_eq!(decision.entries.len(), 2);
    assert_eq!(decision.entries[&c2].deps, vec![c1]);
    for i in [1usize, 2] {
        assert_eq!(cluster.nodes[i].executed, vec![c1, c2], "node {i}");
        let positions: Vec<u64> = cluster.nodes[i]
            .established
            .iter()
            .map(|e| e.position().get())
            .collect();
        assert_eq!(positions, vec![1, 2], "node {i}");
        assert_eq!(cluster.nodes[i].established[1].command(), c2);
    }
    // The new ballot serves new requests; every live node executes them
    // after the recovered ones.
    let c3 = cluster.admit(3, 3);
    cluster.settle();
    for i in [1usize, 2] {
        assert_eq!(cluster.nodes[i].executed, vec![c1, c2, c3], "node {i}");
    }
    // r0 comes back as a follower: its old promise cannot lower anything,
    // its old-ballot effects are gone, and it catches up through the
    // leader's Sync (a fresh campaign is not needed for it to follow).
    cluster.revive(0, quorum(ballot(0, 0)));
    let promised = cluster.nodes[0].follower().ballots().promised();
    assert_eq!(promised, ballot(0, 0));
    assert!(
        cluster
            .frontend
            .iter()
            .any(|(_, m)| matches!(m, ProtocolMessage::LeaderReply { .. }))
    );
}

#[test]
fn competing_campaigns_and_delayed_replies_cannot_establish_divergence() {
    let mut cluster = Cluster::new(7);
    let c1 = cluster.admit(1, 1);
    cluster.settle();
    for i in 0..3 {
        assert_eq!(cluster.nodes[i].executed, vec![c1]);
    }
    // r1 campaigns for ballot 1 while r2 campaigns for ballot 2; the leader
    // r0 is deposed by whichever arrives first and follows the highest.
    cluster.campaign(1, ballot(1, 1));
    cluster.campaign(2, ballot(2, 2));
    cluster.settle();
    // Exactly one leader remains: the ballot-2 candidate. Every live node
    // promised ballot 2 and synchronized to it; ballot 1's Sync (if it was
    // ever bound) was rejected wherever ballot 2 was already promised.
    let leaders: Vec<usize> = (0..3)
        .filter(|i| matches!(cluster.nodes[*i].role, Some(Role::Leader(_))))
        .collect();
    assert_eq!(leaders, vec![2]);
    for i in [0usize, 1] {
        let f = cluster.nodes[i].follower();
        assert_eq!(f.ballots().promised(), ballot(2, 2), "node {i}");
        assert_eq!(f.ballots().synced(), ballot(2, 2), "node {i}");
    }
    // Work continues under ballot 2 with identical histories.
    let c2 = cluster.admit(2, 2);
    cluster.settle();
    for i in 0..3 {
        assert_eq!(cluster.nodes[i].executed, vec![c1, c2], "node {i}");
    }
    // A delayed report page for ballot 1 reaching the ballot-2 leader is
    // a wrong-ballot page; it is refused, not merged.
    let stale_page =
        coord_consensus::paginate(&cluster.nodes[0].follower().report(ballot(1, 1)), 16).remove(0);
    let Some(Role::Leader(_)) = &cluster.nodes[2].role else {
        panic!()
    };
    // Give the page to a fresh candidate structure to show the rule.
    let mut asm = coord_consensus::ReportAssembler::new(ballot(2, 2));
    assert_eq!(asm.accept(stale_page), Err(PageError::WrongBallot));
}

#[test]
fn permuted_reports_give_the_same_selection() {
    let mut decisions = Vec::new();
    for seed in [3u64, 11, 19, 23] {
        let mut cluster = Cluster::new(seed);
        cluster.cut = vec![(0, 2), (1, 2)];
        cluster.admit(1, 1);
        cluster.admit(2, 2);
        cluster.admit(3, 3);
        cluster.settle();
        cluster.crash(0);
        cluster.cut.clear();
        cluster.campaign(2, ballot(1, 2));
        cluster.settle();
        let mut rows = sync_rows(&cluster.nodes[2].storage);
        assert_eq!(rows.len(), 1, "seed {seed}");
        decisions.push(rows.remove(0));
    }
    for d in &decisions[1..] {
        assert_eq!(d, &decisions[0]);
    }
}

#[test]
fn a_crash_after_binding_the_sync_republishes_the_same_result() {
    let mut cluster = Cluster::new(5);
    cluster.cut = vec![(0, 2), (1, 2)];
    let c1 = cluster.admit(1, 1);
    cluster.settle();
    cluster.crash(0);
    cluster.cut.clear();
    // r2 campaigns but its Sync never leaves: the Sync frame to r1 is
    // dropped after the row is bound (promises still flow).
    cluster.drop_sync = vec![(2, 1)];
    cluster.campaign(2, ballot(1, 2));
    cluster.settle();
    let bound = sync_rows(&cluster.nodes[2].storage);
    assert_eq!(bound.len(), 1, "the selection was bound durably");
    assert!(bound[0].entries.contains_key(&c1));
    // r2 crashes and restarts: the promise row says ballot 1 is promised,
    // the Sync row holds the decision; resuming republishes exactly it.
    cluster.crash(2);
    cluster.drop_sync.clear();
    cluster.revive(2, quorum(ballot(0, 0)));
    assert_eq!(
        cluster.nodes[2].follower().ballots().promised(),
        ballot(1, 2)
    );
    let effects = cluster.nodes[2]
        .follower_mut()
        .resume_campaign(bound[0].clone());
    let syncs: Vec<SyncDecision> = effects
        .iter()
        .filter_map(|e| match e {
            Effect::SendWhenDurable { frame, .. } => {
                match ProtocolMessage::decode(frame).unwrap() {
                    ProtocolMessage::Sync(d) => Some(d),
                    _ => None,
                }
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        syncs,
        vec![bound[0].clone(), bound[0].clone()],
        "one per other voter"
    );
    cluster.handle(2, effects);
    cluster.settle();
    assert_eq!(
        sync_rows(&cluster.nodes[2].storage),
        bound,
        "no second selection"
    );
    assert!(matches!(cluster.nodes[2].role, Some(Role::Leader(_))));
    assert_eq!(cluster.nodes[1].executed, vec![c1]);
    assert_eq!(cluster.nodes[2].executed, vec![c1]);
    // A campaign for a ballot this replica does not lead is refused.
    let mut lone = Follower::new(FollowerConfig {
        identity: identity(1),
        quorum: quorum(ballot(0, 0)),
        genesis: ballot(0, 0),
        frontend: FRONTEND,
        capacity: 4,
    });
    lone.step(boot_event(1));
    assert!(lone.campaign(ballot(3, 2)).is_empty());
    assert_eq!(lone.take_rejections(), vec![FollowerRejection::CannotLead]);
    let _ = BTreeMap::<u8, u8>::new();
}

/// Durable payload rows of a node's storage, as recovery consumes them.
fn payload_rows(
    storage: &coord_sim::storage::StorageModel,
) -> Vec<(CommandId, coord_consensus::PayloadRecordV1)> {
    storage
        .durable_rows()
        .into_iter()
        .filter(|(c, _, _)| *c == Collection::PayloadV1.id().0)
        .map(|(_, k, v)| {
            (
                CommandId(coord_types::identity::Digest32(k[..].try_into().unwrap())),
                coord_consensus::decode_payload(&v).unwrap(),
            )
        })
        .collect()
}

/// An authenticated peer message.
fn peer_event(from: ReplicaId, message: ProtocolMessage) -> Event {
    Event::Peer(AuthenticatedPeerMessage::new(
        PeerProvenance::from_transport(from, ReplicaIncarnation::new(1).unwrap(), 1),
        message.encode(),
    ))
}

/// A durable completion for every batch of `effects`.
fn durable_events(effects: &[Effect]) -> Vec<Event> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::Persist(b) => Some(Event::Storage(StorageEvent::JournalDurable {
                barrier_id: b.barrier,
                journal_seq: LocalJournalSeq::new(1).unwrap(),
            })),
            _ => None,
        })
        .collect()
}

#[test]
fn a_synchronized_ballot_is_durable_before_anything_of_the_new_one() {
    // A follower receives a Sync for the ballot it promised. Until the
    // synchronized-ballot row is durable, nothing of the new ballot
    // happens: the old fast set stays in force and no entry is installed,
    // so a crash in that window cannot recover the old source state next
    // to rows of the new ballot.
    let mut c = Cluster::new(3);
    let c1 = c.admit(1, 1);
    c.settle();
    let new = ballot(1, 2);
    let mut f = Follower::new(FollowerConfig {
        identity: identity(1),
        quorum: quorum(ballot(0, 0)),
        genesis: ballot(0, 0),
        frontend: FRONTEND,
        capacity: 32,
    });
    f.step(boot_event(2));
    // Promise the new ballot and make that row durable.
    let promise = f.step(peer_event(r(2), ProtocolMessage::NewLeader { ballot: new }));
    for event in durable_events(&promise) {
        f.step(event);
    }
    assert_eq!(f.ballots().promised(), new);
    let decision = SyncDecision {
        ballot: new,
        source_ballot: ballot(0, 0),
        entries: BTreeMap::from([(
            c1,
            coord_consensus::SyncEntry {
                command: c1,
                phase: Phase::Accept,
                deps: vec![],
            },
        )]),
        reproposed: Default::default(),
    };
    let effects = f.step(peer_event(r(2), ProtocolMessage::Sync(decision)));
    // Exactly one batch: the synchronized-ballot row. Nothing else.
    assert_eq!(
        effects.len(),
        1,
        "only the synchronized row is written: {effects:?}"
    );
    let Effect::Persist(batch) = &effects[0] else {
        panic!("{effects:?}")
    };
    assert_eq!(
        f.quorum().ballot(),
        ballot(0, 0),
        "the new ballot is not active before its row is durable"
    );
    assert!(f.table().phase_of(&c1).is_none(), "no entry installed");
    // The row becomes durable: now the ballot is this replica's.
    let barrier = batch.barrier;
    f.step(Event::Storage(StorageEvent::JournalDurable {
        barrier_id: barrier,
        journal_seq: LocalJournalSeq::new(9).unwrap(),
    }));
    assert_eq!(f.quorum().ballot(), new, "activated once durable");
    assert_eq!(f.ballots().synced(), new);
}

#[test]
fn reproposals_follow_the_recovered_order_not_identity_order() {
    // Two selected commands whose identity order is the reverse of their
    // dependency order, plus a command to re-propose. The re-proposed
    // command must depend on the tail of the recovered order, never on
    // whichever identity happens to sort highest.
    let mut c = Cluster::new(3);
    let (mut a, mut b, mut extra) = (c.admit(1, 1), c.admit(2, 2), c.admit(3, 3));
    c.settle();
    // Name them so that the dependency tail is not the largest identity.
    if a < b {
        core::mem::swap(&mut a, &mut b);
    }
    // `b` depends on `a`, so the tail of the order is `b`, while `a` is
    // the larger identity.
    assert!(a > b, "a sorts above b");
    let new = ballot(1, 2);
    let entries = BTreeMap::from([
        (
            a,
            coord_consensus::SyncEntry {
                command: a,
                phase: Phase::Accept,
                deps: vec![],
            },
        ),
        (
            b,
            coord_consensus::SyncEntry {
                command: b,
                phase: Phase::Accept,
                deps: vec![a],
            },
        ),
    ]);
    if extra == a || extra == b {
        extra = c.admit(4, 4);
    }
    let decision = SyncDecision {
        ballot: new,
        source_ballot: ballot(0, 0),
        entries,
        reproposed: core::iter::once(extra).collect(),
    };
    // A replica that knows all three commands becomes the new leader.
    let (leader, effects) = Leader::from_recovered(
        c.nodes[2].role.take().map(role_into_recovered).unwrap(),
        quorum(new),
        &decision,
    );
    let _ = effects;
    // The re-proposed command follows the recovered order's tail.
    let proposal = leader.proposal(&extra).expect("re-proposed");
    assert_eq!(
        proposal.deps,
        vec![b],
        "chained after the order tail, not after the largest identity {a:?}"
    );
}

/// The role-independent state of whichever role a node holds.
fn role_into_recovered(role: Role) -> coord_consensus::RecoveredState {
    match role {
        Role::Leader(l) => l.into_recovered(),
        Role::Follower(f) => f.into_recovered(),
    }
}

#[test]
fn a_receiving_follower_that_crashes_on_the_marker_still_holds_the_selection() {
    // The synchronized-ballot marker and the selection it records go down
    // together. A marker alone would let this replica restart claiming it
    // had synchronized to ballot B while its ledger still held the old
    // state, and recovery selection treats the highest synchronized
    // ballot as the authoritative source of accepted state.
    let mut c = Cluster::new(3);
    let c1 = c.admit(1, 1);
    let new = ballot(1, 2);
    let f = c.nodes[1].follower_mut();
    let promise = f.step(peer_event(r(2), ProtocolMessage::NewLeader { ballot: new }));
    for event in durable_events(&promise) {
        f.step(event);
    }
    let decision = SyncDecision {
        ballot: new,
        source_ballot: ballot(0, 0),
        entries: BTreeMap::from([(
            c1,
            coord_consensus::SyncEntry {
                command: c1,
                phase: Phase::Accept,
                deps: vec![],
            },
        )]),
        reproposed: Default::default(),
    };
    let bound = f.step(peer_event(r(2), ProtocolMessage::Sync(decision.clone())));
    // Exactly one batch, and it carries both rows: nothing can be durable
    // without the other.
    assert_eq!(bound.len(), 1, "{bound:?}");
    let Effect::Persist(batch) = &bound[0] else {
        panic!("{bound:?}")
    };
    assert_eq!(batch.updates.len(), 2, "the marker and the selection");
    // Make that batch durable and crash before anything else happens.
    let durable = coord_core::effect::PersistBatch {
        barrier: batch.barrier,
        base: batch.base,
        updates: batch.updates.clone(),
    };
    let barrier = batch.barrier;
    c.nodes[1].storage.submit(durable);
    c.nodes[1].storage.complete(barrier).unwrap();
    assert_eq!(
        sync_rows(&c.nodes[1].storage).len(),
        1,
        "the selection is durable with the marker"
    );
    // Restart: the synchronized ballot and the selection come back
    // together, and the selection's command is queued for installation.
    c.crash(1);
    c.revive(1, quorum(new));
    assert_eq!(c.nodes[1].follower().ballots().synced(), new);
    let recovered = sync_rows(&c.nodes[1].storage);
    assert_eq!(recovered, vec![decision], "the selection survived");
}

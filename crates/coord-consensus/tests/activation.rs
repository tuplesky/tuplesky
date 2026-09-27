//! task-26 acceptance: learned outcomes survive a lost leader and lost
//! volatile commit notifications; competing recovery, delayed replies and
//! same-boot old effects cannot establish divergence; permuted reports
//! give the same selection; a crash after the Sync was bound republishes
//! the same result and never reselects under the same ballot.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use coord_consensus::{
    AppliedOutcome, BallotConfiguration, CommandRecord, ConfigurationIdentity, Follower,
    FollowerConfig, FollowerRejection, Leader, LeaderConfig, PageError, Phase, ProtocolMessage,
    RecoveryError, RecoveryReport, ReplicaRole, SyncDecision, decode_dependency, decode_promise,
    decode_sync, select,
};
use coord_core::capability::{
    AdmissionReceipt, AttestedAdmission, EstablishedResult, VerifierToken,
};
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
    released: Vec<coord_core::capability::ReleasedResult>,
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
    /// Nodes that do not fetch the payloads they lack.
    no_fetch: Vec<usize>,
    /// Nodes that execute nothing while listed.
    no_execute: Vec<usize>,
    /// (from, to) pairs whose acknowledgements are dropped.
    drop_acks: Vec<(u8, u8)>,
    /// (from, to) pairs whose proposals are dropped.
    drop_proposals: Vec<(u8, u8)>,
    /// The voters' table capacity.
    capacity: usize,
    frontend: Vec<(ReplicaId, ProtocolMessage)>,
    /// Every payload ask sent: who asked, and for what.
    asks: Vec<(usize, Vec<CommandId>)>,
    /// Every proposal sent: (from, to).
    proposals_sent: Vec<(usize, u8)>,
}

/// How many rounds `Cluster::settle` runs before it calls the cluster
/// stuck. Far more than any test here needs to converge.
const SETTLE_STEPS: u32 = 100_000;

fn boot_event(boot: u8) -> Event {
    Event::Boot {
        boot_id: BootId([boot; 16]),
        incarnation: ReplicaIncarnation::new(1).unwrap(),
    }
}

impl Cluster {
    fn new(seed: u64) -> Self {
        Self::with_capacity(seed, 32)
    }

    /// A cluster whose voters' tables hold `capacity` commands.
    fn with_capacity(seed: u64, capacity: usize) -> Self {
        let mut nodes = Vec::new();
        for me in 0..3u8 {
            let role = if me == 0 {
                Role::Leader(Leader::new(
                    LeaderConfig {
                        identity: identity(0),
                        quorum: quorum(ballot(0, 0)),
                        genesis: ballot(0, 0),
                        frontend: FRONTEND,
                        capacity,
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
                    capacity,
                }))
            };
            let mut node = Node {
                role: Some(role),
                storage: StorageModel::default(),
                inbox: VecDeque::new(),
                established: Vec::new(),
                released: Vec::new(),
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
            no_fetch: Vec::new(),
            no_execute: Vec::new(),
            drop_acks: Vec::new(),
            drop_proposals: Vec::new(),
            capacity,
            frontend: Vec::new(),
            asks: Vec::new(),
            proposals_sent: Vec::new(),
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
                    if let Ok(ProtocolMessage::PayloadRequest { commands }) =
                        ProtocolMessage::decode(&frame)
                    {
                        self.asks.push((i, commands));
                    }
                    let dest = to.replica.0[0];
                    if let Ok(ProtocolMessage::Proposal(_)) = ProtocolMessage::decode(&frame) {
                        self.proposals_sent.push((i, dest));
                    }
                    if self.cut.contains(&(i as u8, dest)) || !self.nodes[dest as usize].alive {
                        continue;
                    }
                    if self.drop_proposals.contains(&(i as u8, dest))
                        && matches!(
                            ProtocolMessage::decode(&frame).unwrap(),
                            ProtocolMessage::Proposal(_)
                        )
                    {
                        continue;
                    }
                    if self.drop_acks.contains(&(i as u8, dest))
                        && matches!(
                            ProtocolMessage::decode(&frame).unwrap(),
                            ProtocolMessage::FastAck(_) | ProtocolMessage::SlowAck(_)
                        )
                    {
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
                // The leader's final-path release: a command that was
                // never speculated is disclosed once, after it has
                // executed. These tests drive no speculation companion,
                // so every release here is that one.
                Effect::Released(released) => {
                    assert!(!released.speculative());
                    self.nodes[i].released.push(released);
                }
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
        // A cluster that cannot converge -- a campaign asking for payloads
        // for ever, a follower re-asking for what nobody serves -- keeps
        // making "progress" without end. Bounded, that is a failure with
        // a message rather than a test that never returns.
        let mut steps = 0u32;
        loop {
            steps += 1;
            assert!(steps < SETTLE_STEPS, "the cluster did not settle");
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
                if !self.nodes[i].alive || self.no_execute.contains(&i) {
                    continue;
                }
                while let Some(c) = self.nodes[i].next_executable() {
                    let position = self.nodes[i].executed_through().checked_next().unwrap();
                    let outcome = AppliedOutcome {
                        position,
                        revision: None,
                        result_digest: Digest32(c.0.0),
                        response: c.0.0.to_vec(),
                    };
                    let effects = self.nodes[i].applied(c, &outcome);
                    self.nodes[i].executed.push(c);
                    self.handle(i, effects);
                    progressed = true;
                }
            }
            // Fetch payloads a follower lacks from the ballot's leader.
            for i in 0..self.nodes.len() {
                if !self.nodes[i].alive || self.no_fetch.contains(&i) {
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

    /// What the runtime's re-send timer does (task-d07): every leader
    /// sends its voters, again, the proposals they have not voted on.
    fn resend(&mut self) {
        for i in 0..self.nodes.len() {
            if !self.nodes[i].alive {
                continue;
            }
            if let Some(Role::Leader(l)) = self.nodes[i].role.as_mut() {
                let effects = l.resend_unvoted(coord_consensus::RESEND_PER_VOTER);
                self.handle(i, effects);
            }
        }
    }

    /// Settle, then re-send and settle again, `rounds` times.
    fn settle_resending(&mut self, rounds: usize) {
        self.settle();
        for _ in 0..rounds {
            self.resend();
            self.settle();
        }
    }

    fn admit(&mut self, seq: u64, key: u8) -> CommandId {
        let all: Vec<usize> = (0..self.nodes.len()).collect();
        self.admit_at(seq, key, &all)
    }

    /// The same, with the submission reaching only `at`.
    fn admit_at(&mut self, seq: u64, key: u8, at: &[usize]) -> CommandId {
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
            if !self.nodes[i].alive {
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
                    receipt_id: Digest32([9; 32]),
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
                capacity: self.capacity,
            },
            promise,
            None,
            rows,
            payload_rows(&self.nodes[i].storage),
            syncs,
            ExecutionPosition::ZERO,
        )
        .restore_execution(
            ExecutionPosition::new(self.nodes[i].executed.len() as u64).unwrap(),
            self.nodes[i].executed.iter().copied(),
        )
        // As `coordd` restores a voter: the payload rows go back in after
        // the execution frontier.
        .restore_payloads(payload_rows(&self.nodes[i].storage));
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

/// A cluster keeps serving past its command table's capacity.
///
/// The capacity bounds *unresolved* work: how many commands a replica
/// may be holding at once, none of which it may forget. It is not a
/// bound on how many commands the replica may ever execute, and a table
/// that never retired an executed record would make it one -- the
/// thirty-third command here would be refused for ever, on a cluster
/// with nothing outstanding and nothing wrong with it.
///
/// Eighty commands through a table of thirty-two, each settled before
/// the next is admitted, so nothing is ever outstanding when the next
/// arrives. Every one of them executes, in order, on every voter.
#[test]
fn a_cluster_serves_past_its_table_capacity() {
    let mut cluster = Cluster::new(9);
    let mut submitted = Vec::new();
    for n in 0..80u8 {
        submitted.push(cluster.admit(u64::from(n) + 1, n));
        cluster.settle();
    }
    for i in 0..3 {
        assert_eq!(
            cluster.nodes[i].executed, submitted,
            "replica {i} did not execute every command in order"
        );
    }
    // And the table did not grow to hold them: what it keeps is the
    // live set, not the history.
    assert!(
        cluster.nodes[1].follower().table().records().count() <= 32,
        "the table kept {} records for eighty executed commands",
        cluster.nodes[1].follower().table().records().count()
    );
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

/// A candidate missing more payloads than one answer carries still wins.
///
/// A peer answers a payload request with at most `MAX_PAYLOAD_TRANSFER`
/// payloads, however many were asked for. The campaign asked each
/// promised voter once, for everything it lacked, so a candidate behind
/// by more than that bound got the first batch and then waited for the
/// rest for ever: nobody was left to ask, and the runtime's paced asks
/// went to the ballot's old leader, which is the voter that is gone.
/// Repeated leader kills make exactly this candidate -- a voter that
/// missed a stretch of work -- and the survivors never elected anyone.
#[test]
fn a_candidate_missing_more_payloads_than_one_answer_carries_still_wins() {
    let bound = coord_consensus::MAX_PAYLOAD_TRANSFER;
    let mut cluster = Cluster::new(13);
    // r2 is down while r0 and r1 serve well past one answer's worth.
    cluster.crash(2);
    let mut submitted = Vec::new();
    for n in 0..(bound * 2 + 3) as u8 {
        submitted.push(cluster.admit(u64::from(n) + 1, n));
        cluster.settle();
    }
    assert_eq!(cluster.nodes[1].executed, submitted);
    // r2 comes back holding nothing, and the leader is lost.
    cluster.revive(2, quorum(ballot(0, 0)));
    cluster.crash(0);
    cluster.campaign(2, ballot(1, 2));
    cluster.settle();
    assert!(
        matches!(cluster.nodes[2].role, Some(Role::Leader(_))),
        "the candidate is still waiting for payloads: {:?}",
        cluster.nodes[2].follower().missing_payloads().len()
    );
    for i in [1usize, 2] {
        assert_eq!(cluster.nodes[i].executed, submitted, "node {i}");
    }
}

/// A Sync naming commands a voter already executed and retired installs
/// without them.
///
/// `phase_of` answers EXECUTED for a retired command from its tombstone,
/// so the Sync installation took such an entry as ready and then found no
/// record to install: a panic, on exactly the voter a new leader most
/// needs -- one that served past its table's capacity before the leader
/// was lost.
#[test]
fn a_sync_naming_commands_this_voter_retired_installs_without_them() {
    let mut cluster = Cluster::new(17);
    let mut submitted = Vec::new();
    for n in 0..48u8 {
        submitted.push(cluster.admit(u64::from(n) + 1, n));
        cluster.settle();
    }
    // r1 retired some of what it executed, and r2 still holds what r1
    // retired.
    let retired: Vec<CommandId> = submitted
        .iter()
        .copied()
        .filter(|c| cluster.nodes[1].follower().table().record(c).is_none())
        .collect();
    assert!(!retired.is_empty(), "r1 retired nothing");
    cluster.crash(0);
    cluster.campaign(2, ballot(1, 2));
    cluster.settle();
    let decision = sync_rows(&cluster.nodes[2].storage).remove(0);
    assert!(
        retired.iter().any(|c| decision.entries.contains_key(c)),
        "the Sync names a command r1 retired"
    );
    // What the candidate executed is selected as committed: no voter can
    // be left holding it at ACCEPT, waiting for a re-proposal that a
    // leader without the record cannot make.
    for c in &cluster.nodes[2].executed {
        if let Some(entry) = decision.entries.get(c) {
            assert_eq!(entry.phase, Phase::Commit, "{c:?}");
        }
    }
    assert!(matches!(cluster.nodes[2].role, Some(Role::Leader(_))));
    assert_eq!(cluster.nodes[1].follower().ballots().synced(), ballot(1, 2));
    // The new ballot serves; r1 executes what comes next, once.
    let next = cluster.admit(100, 100);
    cluster.settle();
    for i in [1usize, 2] {
        assert_eq!(cluster.nodes[i].executed.last(), Some(&next), "node {i}");
        assert_eq!(
            cluster.nodes[i].executed.len(),
            submitted.len() + 1,
            "node {i}"
        );
    }
}

/// A leader whose table is full still orders its next command after
/// the one before it, so a follower still behind that one cannot execute
/// the next one first (task-d06).
///
/// The table reclaims exactly when it is full, and before it computes the
/// next command's dependencies. Retirement used to clear the key's latest
/// command, so the first command a full leader proposed named no
/// dependency. Here r2 never gets command 32's payload, so it cannot
/// execute it; command 33 is the first the leader proposes after its
/// table filled. With the chain broken, r2 found 33 committed and ready,
/// with nothing ordering it after 32, and executed it first: the same
/// committed commands in two orders. Every replica must execute them in
/// one.
#[test]
fn a_follower_behind_a_full_leader_executes_in_the_leaders_order() {
    let mut cluster = Cluster::new(21);
    let capacity = 32u64;
    let mut order = Vec::new();
    for n in 1..capacity {
        order.push(cluster.admit(n, n as u8));
        cluster.settle();
    }
    // Command 32 reaches r0 and r1 only, and r2 does not fetch it.
    cluster.no_fetch = vec![2];
    order.push(cluster.admit_at(capacity, capacity as u8, &[0, 1]));
    cluster.settle();
    assert_eq!(cluster.nodes[0].executed, order, "the leader ran 32");
    // The leader's table is full; command 33 is proposed after a reclaim.
    order.push(cluster.admit(capacity + 1, (capacity + 1) as u8));
    cluster.settle();
    assert_eq!(cluster.nodes[0].executed, order);
    let behind = &cluster.nodes[2].executed;
    assert_eq!(
        behind[..],
        order[..behind.len()],
        "r2 executed out of the leader's order"
    );
    // Once r2 has the payload, it catches up in the same order.
    cluster.no_fetch.clear();
    cluster.settle();
    for i in 0..3 {
        assert_eq!(cluster.nodes[i].executed, order, "node {i}");
    }
}

/// A new leader orders its first command after the tail of the order it
/// recovered, not after the payload that reached it last (task-d06).
///
/// A follower's latest command on a key is moved by `initialize`, which
/// runs when a payload arrives, so it follows arrival order rather than
/// the leader's order. Here r2 gets C's payload first and B's (fetched)
/// second, while the leader ordered B before C. When r2 wins, it
/// re-proposes B and C in the recovered order, which moved nothing, so its
/// first fresh command D named B. r1 has C committed but no payload for
/// it, so it found D ready and executed it before C: the fork this task
/// closes for a reclaim, at an election instead. With D naming C, r1
/// waits for C's payload, and every voter executes B, C, D.
#[test]
fn a_new_leaders_first_command_follows_the_recovered_tail() {
    let mut cluster = Cluster::new(23);
    // r1 never fetches: C stays committed there without its payload.
    cluster.no_fetch = vec![1];
    let b = cluster.admit_at(1, 1, &[0, 1]);
    let c = cluster.admit_at(2, 2, &[0, 2]);
    cluster.settle();
    assert_eq!(cluster.nodes[0].executed, vec![b, c]);
    assert_eq!(cluster.nodes[2].executed, vec![b, c]);
    assert_eq!(
        cluster.nodes[2]
            .follower()
            .table()
            .record(&c)
            .map(|r| r.deps.clone()),
        Some(vec![b]),
        "the leader ordered B before C"
    );
    // r2 fetched B after C arrived: its latest on the key is B.
    assert_eq!(cluster.nodes[1].executed, vec![b]);
    cluster.crash(0);
    cluster.campaign(2, ballot(1, 2));
    cluster.settle();
    assert!(matches!(cluster.nodes[2].role, Some(Role::Leader(_))));
    let d = cluster.admit(3, 3);
    cluster.settle();
    let Some(Role::Leader(leader)) = &cluster.nodes[2].role else {
        unreachable!()
    };
    assert_eq!(
        leader.table().record(&d).map(|r| r.deps.clone()),
        Some(vec![c]),
        "the first fresh command follows the recovered tail"
    );
    // r1 holds C by identity only, so it cannot accept D after it: D
    // waits for C's payload there, and commits once it arrives.
    let behind = &cluster.nodes[1].executed;
    assert_eq!(
        behind[..],
        cluster.nodes[2].executed[..behind.len()],
        "r1 executed out of the new leader's order"
    );
    cluster.no_fetch.clear();
    cluster.settle();
    for i in [1usize, 2] {
        assert_eq!(cluster.nodes[i].executed, vec![b, c, d], "node {i}");
    }
}

/// An election after more history than the table holds completes in one
/// campaign, asks for no payload of a command the candidate executed,
/// carries a selection bounded by the table rather than the history, and
/// the new ballot serves (task-d05).
///
/// The candidate used to answer "unknown" for every command it had
/// executed and retired past its tombstone window, so a selection naming
/// the whole history left it asking for the payloads of commands it ran
/// long ago, eight to an answer, into a table that could not hold them.
/// At capacity 32 and 200 commands it waited on 160 payloads, and in
/// `coordd` the campaign timed out and restarted for ever. Every report
/// named the whole history too, so a Sync grew with it until it could no
/// longer be written as a row.
#[test]
fn an_election_after_more_history_than_the_table_holds_asks_for_nothing_executed() {
    let capacity = 32usize;
    let history = 400u64;
    let mut cluster = Cluster::new(29);
    let mut submitted = Vec::new();
    for n in 0..history {
        submitted.push(cluster.admit(n + 1, (n % 250) as u8));
        cluster.settle();
    }
    for i in 0..3 {
        assert_eq!(cluster.nodes[i].executed, submitted, "node {i}");
    }
    // What a voter reports, and keeps durable records of in memory, is
    // bounded by its table, not by the history -- a restarted voter too,
    // whose every durable row comes back.
    cluster.crash(1);
    cluster.revive(1, quorum(ballot(0, 0)));
    for i in 1..3 {
        let f = cluster.nodes[i].follower();
        let reported = f.report(ballot(1, 2)).entries.len();
        assert!(
            reported <= 2 * capacity + 1,
            "node {i} reports {reported} commands"
        );
        assert!(
            f.ledger().len() <= 4 * capacity + 8,
            "node {i} keeps {} durable records",
            f.ledger().len()
        );
        let payloads = submitted.iter().filter(|c| f.payload(c).is_some()).count();
        assert!(
            payloads <= 4 * capacity + 8,
            "node {i} holds {payloads} payloads"
        );
    }
    cluster.crash(0);
    cluster.asks.clear();
    cluster.campaign(2, ballot(1, 2));
    cluster.settle();
    assert!(
        matches!(cluster.nodes[2].role, Some(Role::Leader(_))),
        "the candidate did not win in one campaign"
    );
    let executed: BTreeSet<CommandId> = submitted.iter().copied().collect();
    for (who, commands) in &cluster.asks {
        for c in commands {
            assert!(
                !executed.contains(c),
                "node {who} asked for the payload of {c:?}, which every voter executed"
            );
        }
    }
    let decision = sync_rows(&cluster.nodes[2].storage).remove(0);
    let selected = decision.entries.len() + decision.reproposed.len();
    assert!(
        selected <= 4 * capacity,
        "the Sync names {selected} commands of a {history}-command history"
    );
    let Some(Role::Leader(leader)) = &cluster.nodes[2].role else {
        unreachable!()
    };
    assert!(
        leader.table().records().count() <= capacity,
        "the new leader holds {} records",
        leader.table().records().count()
    );
    let next = cluster.admit(1000, 7);
    cluster.settle();
    for i in [1usize, 2] {
        assert_eq!(cluster.nodes[i].executed.last(), Some(&next), "node {i}");
        assert_eq!(
            cluster.nodes[i].executed.len() as u64,
            history + 1,
            "node {i}"
        );
    }
}

/// A candidate further behind than every reporter's window does not win
/// (task-d05).
///
/// Its peers leave out of their reports what they executed long ago, so
/// the selection names what the candidate lacks only as a dependency of
/// the oldest command it does carry. Bound and won, that ballot would
/// have a leader that can never execute again. The candidate abandons
/// the campaign and stops campaigning, and a voter that is not behind
/// leads instead.
#[test]
fn a_candidate_behind_every_reporters_window_does_not_win() {
    let history = 200u64;
    let mut cluster = Cluster::new(41);
    cluster.crash(2);
    let mut submitted = Vec::new();
    for n in 0..history {
        submitted.push(cluster.admit(n + 1, 1));
        cluster.settle();
    }
    assert_eq!(cluster.nodes[1].executed, submitted);
    // r2 comes back with nothing, and the leader goes: r1 and r2 are the
    // majority left, and r2 campaigns first.
    cluster.revive(2, quorum(ballot(0, 0)));
    cluster.crash(0);
    cluster.campaign(2, ballot(1, 2));
    cluster.settle();
    assert!(
        !matches!(cluster.nodes[2].role, Some(Role::Leader(_))),
        "a candidate {history} commands behind won"
    );
    // Refused by r1 before it could select (task-d10): r1 executed a
    // whole history more than a table ahead of it. Before task-d10 it got
    // r1's promise and found itself `Behind` in its own selection.
    let rejections = cluster.nodes[2].follower_mut().take_rejections();
    assert!(
        rejections.iter().any(|x| matches!(
            x,
            FollowerRejection::CampaignRefused { by, .. } if *by == r(1)
        )),
        "{rejections:?}"
    );
    // It does not campaign again this boot.
    cluster.campaign(2, ballot(2, 2));
    assert_eq!(
        cluster.nodes[2].follower().ballots().promised(),
        ballot(1, 2)
    );
    // r0 comes back and r1, which is not behind, leads. r2 stays where
    // it is -- what brings it up is a checkpoint, not a payload -- so its
    // asks, which no voter can answer, are left out of the harness.
    cluster.no_fetch.push(2);
    cluster.revive(0, quorum(ballot(0, 0)));
    cluster.campaign(1, ballot(3, 1));
    cluster.settle();
    assert!(matches!(cluster.nodes[1].role, Some(Role::Leader(_))));
    let next = cluster.admit(1000, 1);
    cluster.settle();
    for i in [0usize, 1] {
        assert_eq!(cluster.nodes[i].executed.last(), Some(&next), "node {i}");
    }
}

/// A candidate more than a table behind the voters it asks is refused,
/// and one of them leads instead (task-d10).
///
/// rep-d-4, the reviewer's run 2 and the five-node Jepsen `n2` are one
/// shape: a voter restarts far behind, its timer fires first, it wins, and
/// it cannot serve -- a leader asks nobody for the payloads it lacks.
/// Here r2 stops at 40 while r0 and r1 go on to 80 with a table of 32,
/// then r0 dies and r2 campaigns first. r1 refuses and names its position;
/// r2 abandons, and does not campaign again until it has executed that
/// far. r1 leads above the refused ballot, which r2 promised itself, so r2
/// follows it, catches up, and may campaign again.
#[test]
fn a_candidate_a_table_behind_is_refused_and_follows_the_voter_that_refused() {
    let mut cluster = Cluster::new(47);
    for n in 0..40u64 {
        cluster.admit(n + 1, 1);
        cluster.settle();
    }
    cluster.crash(2);
    for n in 40..80u64 {
        cluster.admit(n + 1, 1);
        cluster.settle();
    }
    assert_eq!(cluster.nodes[1].executed.len(), 80);
    cluster.revive(2, quorum(ballot(0, 0)));
    assert_eq!(cluster.nodes[2].executed.len(), 40);
    cluster.crash(0);

    let refused = ballot(1, 2);
    cluster.campaign(2, refused);
    cluster.settle();
    assert!(!matches!(cluster.nodes[2].role, Some(Role::Leader(_))));
    let at = ExecutionPosition::new(80).unwrap();
    let r2 = cluster.nodes[2].follower_mut().take_rejections();
    assert!(
        r2.contains(&FollowerRejection::CampaignRefused {
            by: r(1),
            executed: at,
        }),
        "r2 is told r1's position: {r2:?}"
    );
    let r1 = cluster.nodes[1].follower_mut().take_rejections();
    assert!(
        r1.contains(&FollowerRejection::Promise(
            coord_consensus::PromiseRejection::CandidateBehind {
                candidate: ExecutionPosition::new(40).unwrap(),
                own: at,
            }
        )),
        "{r1:?}"
    );
    assert_eq!(
        cluster.nodes[1].follower().ballots().promised(),
        ballot(0, 0),
        "r1 never promised the refused ballot"
    );
    assert_eq!(
        cluster.nodes[1].follower().ballots().outranked(),
        Some(refused)
    );

    // r2 does not campaign again while it is behind r1's position.
    cluster.campaign(2, ballot(2, 2));
    assert_eq!(cluster.nodes[2].follower().ballots().promised(), refused);
    let r2 = cluster.nodes[2].follower_mut().take_rejections();
    assert!(
        r2.iter().any(|r| matches!(
            r,
            FollowerRejection::BehindVoters { needed, .. } if *needed == at
        )),
        "{r2:?}"
    );

    // r1 leads above the refused ballot, which r2 promised itself, so r2
    // promises it and follows. What r2 lacks was retired by the voters
    // that could send it, so a checkpoint brings it up (task-d08), not a
    // payload; its asks are left out of the harness, and r0 comes back
    // for a quorum that can accept what r1 proposes next.
    cluster.no_fetch.push(2);
    cluster.revive(0, quorum(ballot(0, 0)));
    cluster.campaign(1, ballot(2, 1));
    cluster.settle();
    assert!(matches!(cluster.nodes[1].role, Some(Role::Leader(_))));
    assert_eq!(
        cluster.nodes[2].follower().ballots().promised(),
        ballot(2, 1),
        "r2 follows the voter that refused it"
    );
    let next = cluster.admit(1000, 1);
    cluster.settle();
    for i in [0usize, 1] {
        assert_eq!(cluster.nodes[i].executed.last(), Some(&next), "node {i}");
    }
}

/// A replica refused at a position campaigns again once it has executed
/// that far, and not before (task-d10).
///
/// A refusal is injected here, naming a position five commands ahead of
/// r2 that r2 can reach by following, which a refusal by the table rule
/// never names (what that voter lacks is past every peer's table).
#[test]
fn a_refused_replica_campaigns_again_once_it_executed_as_far_as_the_refuser() {
    let mut cluster = Cluster::new(53);
    for n in 0..10u64 {
        cluster.admit(n + 1, 1);
        cluster.settle();
    }
    // r2 misses five commands, then campaigns, and r1 refuses it at 15.
    cluster.crash(2);
    for n in 10..15u64 {
        cluster.admit(n + 1, 1);
        cluster.settle();
    }
    cluster.revive(2, quorum(ballot(0, 0)));
    let mine = ballot(1, 2);
    cluster.campaign(2, mine);
    let at = ExecutionPosition::new(15).unwrap();
    let refusal = ProtocolMessage::PromiseRefused {
        ballot: mine,
        replica: r(1),
        executed: at,
    };
    let effects = cluster.nodes[2].step(peer_event(r(1), refusal));
    cluster.handle(2, effects);
    assert!(
        cluster.nodes[2].follower().campaign_state().is_none(),
        "abandoned on the refusal"
    );
    let r2 = cluster.nodes[2].follower_mut().take_rejections();
    assert!(
        r2.contains(&FollowerRejection::CampaignRefused {
            by: r(1),
            executed: at,
        }),
        "{r2:?}"
    );
    // The promises r0 and r1 send now reach no campaign.
    cluster.settle();
    assert!(!matches!(cluster.nodes[2].role, Some(Role::Leader(_))));
    // Behind 15, it does not campaign.
    cluster.campaign(2, ballot(2, 2));
    assert_eq!(cluster.nodes[2].follower().ballots().promised(), mine);
    let r2 = cluster.nodes[2].follower_mut().take_rejections();
    assert!(
        r2.iter().any(|x| matches!(
            x,
            FollowerRejection::BehindVoters { needed, .. } if *needed == at
        )),
        "{r2:?}"
    );
    // r1 leads above r2's ballot; r2 follows and catches up past 15.
    cluster.campaign(1, ballot(3, 1));
    cluster.settle();
    assert!(matches!(cluster.nodes[1].role, Some(Role::Leader(_))));
    let next = cluster.admit(1000, 1);
    cluster.settle();
    assert_eq!(cluster.nodes[2].executed.last(), Some(&next));
    assert!(cluster.nodes[2].executed.len() > 15);
    // Now it campaigns, and wins.
    cluster.campaign(2, ballot(4, 2));
    cluster.settle();
    assert!(matches!(cluster.nodes[2].role, Some(Role::Leader(_))));
}

/// A leader keeps the highest of the Syncs ahead of it, and only from
/// their ballots' leaders (task-d05).
///
/// Syncs of two higher ballots can reach a leader in either order before
/// the promise that deposes it. Keeping the last one kept the lower one
/// when it came second, and the follower the leader becomes refuses it
/// once it promises the higher ballot, with the one Sync that matched
/// gone.
#[test]
fn a_leader_keeps_the_highest_sync_ahead_of_it() {
    let mut cluster = Cluster::new(43);
    let sync = |b: Ballot| SyncDecision {
        ballot: b,
        source_ballot: ballot(0, 0),
        entries: BTreeMap::new(),
        reproposed: BTreeSet::new(),
    };
    for (from, b) in [
        (2u8, ballot(2, 2)),
        (1, ballot(1, 1)),
        // Relayed by a voter that does not lead it.
        (1, ballot(3, 2)),
    ] {
        let effects = cluster.nodes[0].step(peer_event(r(from), ProtocolMessage::Sync(sync(b))));
        assert!(effects.is_empty());
    }
    let Some(Role::Leader(leader)) = &cluster.nodes[0].role else {
        unreachable!()
    };
    assert_eq!(
        leader.pending_sync().map(|(from, d)| (*from, d.ballot)),
        Some((r(2), ballot(2, 2)))
    );
}

/// A Sync that reaches a voter before that voter has promised its ballot
/// is installed once the promise is made (task-d05).
///
/// The new leader publishes its Sync once, as soon as its selection is
/// durable, and it can count a majority without a slower voter. That voter
/// used to refuse a Sync for a ballot it had not promised yet and was then
/// left promised to a ballot it could never synchronize to: every proposal
/// of the ballot held, nothing executed, for good.
#[test]
fn a_sync_ahead_of_the_promise_is_installed_once_the_promise_is_made() {
    let mut cluster = Cluster::new(31);
    let c1 = cluster.admit(1, 1);
    cluster.settle();
    cluster.campaign(2, ballot(1, 2));
    // r1's promise request is held back: r0 and r2 elect r2 without it, and
    // r2's Sync reaches r1 first.
    let held: Vec<(ReplicaId, Vec<u8>)> = cluster.nodes[1].inbox.drain(..).collect();
    assert!(
        held.iter().any(|(_, f)| matches!(
            ProtocolMessage::decode(f),
            Ok(ProtocolMessage::NewLeader { .. })
        )),
        "the campaign's promise request was not in r1's inbox"
    );
    cluster.settle();
    assert!(matches!(cluster.nodes[2].role, Some(Role::Leader(_))));
    assert_eq!(cluster.nodes[1].follower().ballots().synced(), ballot(0, 0));
    cluster.nodes[1].inbox.extend(held);
    cluster.settle();
    assert_eq!(
        cluster.nodes[1].follower().ballots().synced(),
        ballot(1, 2),
        "r1 never synchronized to the ballot it promised"
    );
    let c2 = cluster.admit(2, 2);
    cluster.settle();
    for i in 0..3 {
        assert_eq!(cluster.nodes[i].executed, vec![c1, c2], "node {i}");
    }
}

/// A voter that was not linked when a proposal went out learns it from the
/// leader's re-send (task-d07).
///
/// The decisive case from the Jepsen client: two voters serve a write
/// before the third is linked, and reads through the third wait for ever.
/// The proposal was dropped on the way, nothing sent it again, and every
/// later proposal depends on it, so the third voter held everything and
/// executed nothing.
#[test]
fn a_voter_linked_after_a_proposal_went_out_learns_it_from_the_resend() {
    let mut cluster = Cluster::new(37);
    cluster.cut = vec![(0, 2), (1, 2)];
    let c1 = cluster.admit(1, 1);
    cluster.settle();
    cluster.cut.clear();
    let c2 = cluster.admit(2, 2);
    cluster.settle();
    assert_eq!(cluster.nodes[0].executed, vec![c1, c2]);
    assert_eq!(
        cluster.nodes[2].executed,
        Vec::<CommandId>::new(),
        "r2 executed without ever having c1's order"
    );
    cluster.settle_resending(2);
    for i in 0..3 {
        assert_eq!(cluster.nodes[i].executed, vec![c1, c2], "node {i}");
    }
}

/// The same for a voter in the fast set, which acknowledges a command as
/// soon as its payload arrives (task-d07).
///
/// That fast acknowledgement carries no sequence number and says nothing
/// about the proposal. Counted as a vote, it credited r1 with c1's
/// proposal, which never reached it, and with every proposal before its
/// latest acknowledgement, so the proposal r1 lacked was the one never
/// sent again. In the Jepsen shim test this stalled every read through
/// that voter: the domain's first proposal went out before the followers
/// were linked, the re-send reached only the voter outside the fast set,
/// and once its adoption let the leader learn the command, nothing sent
/// it again.
#[test]
fn a_fast_acknowledgement_does_not_stand_for_the_proposal() {
    let mut cluster = Cluster::new(59);
    // r1 receives c1's payload, and so acknowledges it fast, but not its
    // proposal.
    cluster.cut = vec![(0, 1), (2, 1)];
    let c1 = cluster.admit(1, 1);
    cluster.settle();
    cluster.cut.clear();
    let c2 = cluster.admit(2, 2);
    cluster.settle();
    assert_eq!(cluster.nodes[0].executed, vec![c1, c2]);
    assert_eq!(
        cluster.nodes[1].executed,
        Vec::<CommandId>::new(),
        "r1 executed without c1's order"
    );
    cluster.settle_resending(2);
    for i in 0..3 {
        assert_eq!(cluster.nodes[i].executed, vec![c1, c2], "node {i}");
    }
}

/// A lost acknowledgement is published again when the leader re-sends
/// the proposal it lacks a vote for (task-d07).
///
/// With r1 down, r0 needs r2's vote to commit. r2 adopted the proposal
/// and its acknowledgement was dropped; the duplicate proposal used to be
/// ignored, so the command never committed.
#[test]
fn a_lost_acknowledgement_is_published_again_on_a_resend() {
    let mut cluster = Cluster::new(41);
    let c1 = cluster.admit(1, 1);
    cluster.settle();
    cluster.crash(1);
    cluster.drop_acks = vec![(2, 0)];
    let c2 = cluster.admit(2, 2);
    cluster.settle();
    assert_eq!(
        cluster.nodes[0].executed,
        vec![c1],
        "committed without r2's vote"
    );
    cluster.drop_acks.clear();
    cluster.settle_resending(2);
    for i in [0usize, 2] {
        assert_eq!(cluster.nodes[i].executed, vec![c1, c2], "node {i}");
    }
}

/// The same, with the voter restarted before the re-send (task-d07).
///
/// What a voter published is kept for its boot, so after a restart it
/// had nothing to publish again, and with r1 down the leader re-sent for
/// ever. r2 learned c2 from the leader's proposal and its own vote and
/// executed it before the restart, so the re-sent proposal is one for a
/// command it executed and retired: it is acknowledged, since the
/// proposal carries the order of r2's durable record.
#[test]
fn a_lost_acknowledgement_is_published_again_after_the_voter_restarts() {
    let mut cluster = Cluster::new(47);
    let c1 = cluster.admit(1, 1);
    cluster.settle();
    cluster.crash(1);
    cluster.drop_acks = vec![(2, 0)];
    let c2 = cluster.admit(2, 2);
    cluster.settle();
    assert_eq!(cluster.nodes[0].executed, vec![c1]);
    cluster.drop_acks.clear();
    cluster.crash(2);
    cluster.revive(2, quorum(ballot(0, 0)));
    cluster.settle_resending(2);
    for i in [0usize, 2] {
        assert_eq!(cluster.nodes[i].executed, vec![c1, c2], "node {i}");
    }
}

/// The same, with the voter restarted before it executed the command
/// (task-d07).
///
/// Its adoption comes back from the durable row with nothing kept to
/// publish again, so the re-sent proposal is adopted again: the same row
/// is written and acknowledged as the first time.
#[test]
fn a_restored_adoption_is_acknowledged_again_on_a_resend() {
    let mut cluster = Cluster::new(53);
    let c1 = cluster.admit(1, 1);
    cluster.settle();
    cluster.crash(1);
    cluster.drop_acks = vec![(2, 0)];
    cluster.no_execute = vec![2];
    let c2 = cluster.admit(2, 2);
    cluster.settle();
    assert_eq!(cluster.nodes[0].executed, vec![c1]);
    assert_eq!(cluster.nodes[2].executed, vec![c1]);
    cluster.drop_acks.clear();
    cluster.crash(2);
    cluster.revive(2, quorum(ballot(0, 0)));
    cluster.no_execute.clear();
    cluster.settle_resending(2);
    for i in [0usize, 2] {
        assert_eq!(cluster.nodes[i].executed, vec![c1, c2], "node {i}");
    }
}

/// A proposal refused because it reached a voter before that voter
/// promised the ballot is sent again once it has (task-d07).
#[test]
fn a_proposal_refused_ahead_of_the_promise_is_sent_again() {
    let mut cluster = Cluster::new(43);
    let c1 = cluster.admit(1, 1);
    cluster.settle();
    cluster.campaign(2, ballot(1, 2));
    let held: Vec<(ReplicaId, Vec<u8>)> = cluster.nodes[1].inbox.drain(..).collect();
    cluster.settle();
    assert!(matches!(cluster.nodes[2].role, Some(Role::Leader(_))));
    // The new ballot's first command reaches r1 before r1 has promised.
    let c2 = cluster.admit(2, 2);
    cluster.settle();
    cluster.nodes[1].inbox.extend(held);
    cluster.settle();
    assert_eq!(cluster.nodes[1].follower().ballots().synced(), ballot(1, 2));
    assert_eq!(cluster.nodes[1].executed, vec![c1], "r1 had c2's order");
    cluster.settle_resending(2);
    for i in 0..3 {
        assert_eq!(cluster.nodes[i].executed, vec![c1, c2], "node {i}");
    }
}

/// A new leader publishes its re-proposals in a first batch and sends the
/// rest as votes come back, and every one of them reaches every voter
/// (task-d07).
///
/// A new leader after a long history put its whole selection on each
/// follower's control lane in one pass, and the lane refused dozens of
/// those frames; nothing sent them again.
#[test]
fn a_new_leader_publishes_its_reproposals_in_batches_and_all_arrive() {
    let batch = coord_consensus::REPROPOSE_BATCH;
    let mut cluster = Cluster::with_capacity(47, 4 * batch);
    // The old leader's proposals reach nobody: every command stays
    // pre-accepted on the followers, and the new leader re-proposes all.
    cluster.drop_proposals = vec![(0, 1), (0, 2)];
    let commands: Vec<CommandId> = (0..(batch as u64 * 2 + 5))
        .map(|n| cluster.admit(n + 1, (n % 250) as u8))
        .collect();
    cluster.settle();
    cluster.drop_proposals.clear();
    cluster.crash(0);
    cluster.proposals_sent.clear();
    cluster.campaign(2, ballot(1, 2));
    cluster.settle();
    assert!(matches!(cluster.nodes[2].role, Some(Role::Leader(_))));
    let decision = sync_rows(&cluster.nodes[2].storage).remove(0);
    let proposed_again = decision.entries.len() + decision.reproposed.len();
    assert!(
        proposed_again > batch,
        "only {proposed_again} commands to propose again"
    );
    let first = cluster
        .proposals_sent
        .iter()
        .filter(|(from, to)| *from == 2 && *to == 1)
        .count();
    assert_eq!(first, batch, "the new leader's first pass to r1");
    cluster.settle_resending(commands.len() / coord_consensus::RESEND_PER_VOTER + 2);
    for i in [1usize, 2] {
        let executed: BTreeSet<CommandId> = cluster.nodes[i].executed.iter().copied().collect();
        let wanted: BTreeSet<CommandId> = commands.iter().copied().collect();
        assert_eq!(executed, wanted, "node {i}");
    }
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
    let promise = f.step(peer_event(
        r(2),
        ProtocolMessage::NewLeader {
            ballot: new,
            executed: coord_types::ids::ExecutionPosition::ZERO,
        },
    ));
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
                path: coord_consensus::empty_path(),
                paths: Vec::new(),
                seqnum: 0,
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
    // The new leader holds both selected commands and has executed
    // neither, so the recovered order is what it follows; the command to
    // re-propose reached only it, so it is undecided there. (A command it
    // had executed would not be re-proposed at all.)
    c.no_execute = vec![2];
    let (mut a, mut b) = (c.admit(1, 1), c.admit(2, 2));
    c.settle();
    let mut extra = c.admit_at(3, 3, &[2]);
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
                path: coord_consensus::empty_path(),
                paths: Vec::new(),
                seqnum: 0,
            },
        ),
        (
            b,
            coord_consensus::SyncEntry {
                command: b,
                phase: Phase::Accept,
                deps: vec![a],
                path: coord_consensus::empty_path(),
                paths: Vec::new(),
                seqnum: 0,
            },
        ),
    ]);
    if extra == a || extra == b {
        extra = c.admit_at(4, 4, &[2]);
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
    // The re-proposed command follows the recovered order's tail. It may
    // follow the commands this leader committed as well (task-d12): here
    // those are the same two, in whichever order ballot 0 decided them.
    let proposal = leader.proposal(&extra).expect("re-proposed");
    assert_eq!(
        proposal.deps.first(),
        Some(&b),
        "chained after the order tail, not after the largest identity {a:?}"
    );
    assert!(
        proposal.deps.iter().all(|d| *d == a || *d == b),
        "{:?}",
        proposal.deps
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
    let promise = f.step(peer_event(
        r(2),
        ProtocolMessage::NewLeader {
            ballot: new,
            executed: coord_types::ids::ExecutionPosition::ZERO,
        },
    ));
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
                path: coord_consensus::empty_path(),
                paths: Vec::new(),
                seqnum: 0,
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

#[test]
fn a_sync_installs_the_selected_per_key_evidence_not_only_the_combined_digest() {
    // A replica pre-accepted a command in its own order, so its per-key
    // log carries its own digest. The selection recovery binds carries the
    // leader's per-key digests; installing it must realign the log to
    // them. Replacing only the combined digest would leave the next
    // command this replica pre-accepts derived from the local tail, and a
    // later recovery would compare fast-set evidence nobody else holds.
    let mut c = Cluster::new(3);
    let c1 = c.admit(1, 1);
    let key = coord_consensus::CONSERVATIVE_KEY.to_vec();
    let local = c.nodes[1].follower().table().path_head(&key);
    let chosen = Digest32([42; 32]);
    assert_ne!(
        local, chosen,
        "the selection disagrees with the local order"
    );
    let new = ballot(1, 2);
    let f = c.nodes[1].follower_mut();
    let promise = f.step(peer_event(
        r(2),
        ProtocolMessage::NewLeader {
            ballot: new,
            executed: coord_types::ids::ExecutionPosition::ZERO,
        },
    ));
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
                path: chosen,
                paths: vec![(key.clone(), chosen)],
                seqnum: 7,
            },
        )]),
        reproposed: Default::default(),
    };
    let bound = f.step(peer_event(r(2), ProtocolMessage::Sync(decision)));
    for event in durable_events(&bound) {
        f.step(event);
    }
    let record = f.table().record(&c1).expect("installed").clone();
    assert_eq!(record.path, chosen, "the combined evidence is the leader's");
    assert_eq!(
        record.paths,
        vec![(key.clone(), chosen)],
        "the per-key evidence is installed, not only the combined digest"
    );
    assert_eq!(record.synced_seq, Some(7));
    assert_eq!(
        f.table().path_head(&key),
        chosen,
        "the key's log resumes at the selected order, not the local one"
    );
}

#[test]
fn a_sync_row_of_the_previous_layout_still_decodes() {
    // The Sync row gained path evidence, which changed its payload
    // layout. A node restarting on a row the parent revision wrote must
    // read the selection it bound, not fail recovery as corrupt, so the
    // row carries a schema version and the old layout has a decoder.
    #[derive(serde::Serialize)]
    struct LegacyEntry {
        command: CommandId,
        phase: Phase,
        deps: Vec<CommandId>,
    }
    #[derive(serde::Serialize)]
    struct LegacyDecision {
        ballot: Ballot,
        source_ballot: Ballot,
        entries: BTreeMap<CommandId, LegacyEntry>,
        reproposed: std::collections::BTreeSet<CommandId>,
    }
    #[derive(serde::Serialize)]
    struct LegacyRecord {
        decision: LegacyDecision,
    }
    let mut c = Cluster::new(3);
    let c1 = c.admit(1, 1);
    let c2 = c.admit(2, 2);
    let legacy = LegacyRecord {
        decision: LegacyDecision {
            ballot: ballot(1, 2),
            source_ballot: ballot(0, 0),
            entries: BTreeMap::from([
                (
                    c1,
                    LegacyEntry {
                        command: c1,
                        phase: Phase::Accept,
                        deps: vec![],
                    },
                ),
                (
                    c2,
                    LegacyEntry {
                        command: c2,
                        phase: Phase::Commit,
                        deps: vec![c1],
                    },
                ),
            ]),
            reproposed: Default::default(),
        },
    };
    let row = coord_store_api::envelope::StoreEnvelopeV1 {
        record_kind: coord_consensus::SYNC_KIND,
        schema_version: 1,
        payload: postcard::to_allocvec(&legacy).unwrap(),
    }
    .encode()
    .unwrap();
    let decoded = decode_sync(&row).expect("the previous layout is still readable");
    assert_eq!(decoded.decision.ballot, ballot(1, 2));
    assert_eq!(decoded.decision.entries.len(), 2);
    assert_eq!(decoded.decision.entries[&c2].deps, vec![c1]);
    assert_eq!(decoded.decision.entries[&c2].phase, Phase::Commit);
    // What that revision did not record is empty, never invented.
    assert!(decoded.decision.entries[&c1].paths.is_empty());
    assert_eq!(decoded.decision.entries[&c1].seqnum, 0);
    assert_eq!(
        decoded.decision.entries[&c1].path,
        coord_consensus::empty_path()
    );
    // The row this revision writes carries the new version, and a version
    // neither decoder knows is refused rather than misread.
    let current = coord_consensus::encode_sync(&coord_consensus::SyncRecordV1 {
        decision: decoded.decision.clone(),
    })
    .unwrap();
    let env = coord_store_api::envelope::StoreEnvelopeV1::decode(&current).unwrap();
    assert_eq!(env.schema_version, coord_consensus::SYNC_SCHEMA_VERSION);
    assert_eq!(decode_sync(&current).unwrap().decision, decoded.decision);
    let future = coord_store_api::envelope::StoreEnvelopeV1 {
        record_kind: coord_consensus::SYNC_KIND,
        schema_version: coord_consensus::SYNC_SCHEMA_VERSION + 1,
        payload: env.payload,
    }
    .encode()
    .unwrap();
    assert!(
        decode_sync(&future).is_err(),
        "an unknown version is refused"
    );
}

#[test]
fn a_report_after_a_sync_never_omits_a_selected_command_it_still_owes() {
    // A follower can accept Sync B durably and still lack the payload of
    // a command B selected. The report came straight from the durable
    // command ledger, which does not hold that command, while its
    // `committed_ballot` announced B: a candidate reading the report as
    // the source for B would conclude the command was never accepted and
    // drop it. The selection is part of what this replica knows and the
    // report has to say so.
    let mut c = Cluster::new(3);
    let new = ballot(1, 2);
    let f = c.nodes[1].follower_mut();
    let promise = f.step(peer_event(
        r(2),
        ProtocolMessage::NewLeader {
            ballot: new,
            executed: coord_types::ids::ExecutionPosition::ZERO,
        },
    ));
    for event in durable_events(&promise) {
        f.step(event);
    }
    // A command this replica has never seen the payload of.
    let unseen = CommandId(coord_types::identity::Digest32([0x5c; 32]));
    let decision = SyncDecision {
        ballot: new,
        source_ballot: ballot(0, 0),
        entries: BTreeMap::from([(
            unseen,
            coord_consensus::SyncEntry {
                command: unseen,
                phase: Phase::Accept,
                deps: vec![],
                path: coord_consensus::empty_path(),
                paths: Vec::new(),
                seqnum: 0,
            },
        )]),
        reproposed: Default::default(),
    };
    let bound = f.step(peer_event(r(2), ProtocolMessage::Sync(decision)));
    for event in durable_events(&bound) {
        f.step(event);
    }
    assert_eq!(f.ballots().synced(), new, "Sync B is durable");
    assert!(
        f.missing_payloads().contains(&unseen),
        "the payload is still owed"
    );

    // Another election begins before the payload arrives.
    let later = ballot(2, 0);
    let _ = f.step(peer_event(
        r(0),
        ProtocolMessage::NewLeader {
            ballot: later,
            executed: coord_types::ids::ExecutionPosition::ZERO,
        },
    ));
    let report = f.report(later);
    assert_eq!(
        report.committed_ballot, new,
        "it reports the ballot it synced"
    );
    let entry = report
        .entries
        .iter()
        .find(|e| e.command == unseen)
        .expect("the selected command is in the report, not silently absent");
    assert_eq!(entry.phase, Phase::Accept);
    assert!(
        !entry.payload_present,
        "and it says plainly that the payload is still outstanding"
    );
}

#[test]
fn one_reporter_without_a_payload_does_not_stop_a_recoverable_campaign() {
    // The half-initialized guard was applied per report, so a replica
    // reporting a durable selection whose payload transfer was still in
    // flight failed the whole campaign — exactly when recovery is needed.
    // The guard's purpose is that nothing selects accepted state nobody
    // can supply, so it belongs across the report set.
    let config = quorum(ballot(1, 0));
    let command = CommandId(coord_types::identity::Digest32([0x77; 32]));
    let entry = |payload_present| coord_consensus::ReportEntry {
        command,
        phase: Phase::Accept,
        deps: vec![],
        path: coord_consensus::empty_path(),
        paths: Vec::new(),
        seqnum: 0,
        keys: Vec::new(),
        payload_present,
    };
    let report = |replica, payload_present| coord_consensus::RecoveryReport {
        replica,
        ballot: config.ballot(),
        committed_ballot: ballot(0, 0),
        entries: vec![entry(payload_present)],
    };
    // One reporter still owes the payload; another has it.
    let mixed = [report(r(0), false), report(r(1), true)];
    let decision = coord_consensus::select(&config, &mixed).expect("recoverable");
    assert!(
        decision.entries.contains_key(&command),
        "the command is selected, not dropped"
    );
    // Nobody at all: still a dead end, as before.
    let nobody = [report(r(0), false), report(r(1), false)];
    assert!(matches!(
        coord_consensus::select(&config, &nobody),
        Err(coord_consensus::RecoveryError::HalfInitialized { .. })
    ));
}

#[test]
fn a_campaign_selects_without_a_reporter_holding_a_payload_nobody_has() {
    // Five voters. r4 fell far behind and installed a Sync naming x, whose
    // payload never reached it; the others executed x and retired it, so
    // their reports leave it out and nobody can supply the payload. Every
    // campaign whose majority included r4's report failed as
    // `HalfInitialized` (the five-node Jepsen stalls on #98). A majority
    // without that report is as sound a basis as any other.
    let voters: BTreeSet<ReplicaId> = (0..5).map(r).collect();
    let config = BallotConfiguration::c2_default(epoch(), ballot(3, 0), voters).unwrap();
    let x = CommandId(Digest32([0x78; 32]));
    let dead = coord_consensus::ReportEntry {
        command: x,
        phase: Phase::Accept,
        deps: vec![],
        path: coord_consensus::empty_path(),
        paths: Vec::new(),
        seqnum: 0,
        keys: Vec::new(),
        payload_present: false,
    };
    let report = |replica, entries| RecoveryReport {
        replica,
        ballot: config.ballot(),
        committed_ballot: ballot(2, 1),
        entries,
    };
    let deliver = |c: &mut coord_consensus::Campaign, rep: &RecoveryReport| {
        c.promise(rep.replica);
        for page in coord_consensus::paginate(rep, 64) {
            c.page(page).unwrap();
        }
    };

    let mut c = coord_consensus::Campaign::new(config.clone());
    c.own_report(report(r(0), vec![]));
    deliver(&mut c, &report(r(4), vec![dead.clone()]));
    deliver(&mut c, &report(r(1), vec![]));
    assert_eq!(
        c.try_select(|_| false),
        Ok(None),
        "no majority without r4 yet: wait for the others rather than fail"
    );
    deliver(&mut c, &report(r(2), vec![]));
    let decision = c
        .try_select(|_| false)
        .expect("selected without r4's report")
        .expect("a majority without r4");
    assert!(
        !decision.entries.contains_key(&x) && !decision.reproposed.contains(&x),
        "nothing is selected that nobody can supply"
    );

    // The candidate's own report is never set aside: it waits while a
    // voter has not reported, and fails once every voter has.
    let mut own = coord_consensus::Campaign::new(config.clone());
    own.own_report(report(r(0), vec![dead]));
    for i in 1..4 {
        deliver(&mut own, &report(r(i), vec![]));
    }
    assert_eq!(own.try_select(|_| false), Ok(None));
    deliver(&mut own, &report(r(4), vec![]));
    assert!(matches!(
        own.try_select(|_| false),
        Err(RecoveryError::HalfInitialized { replica, command })
            if replica == r(0) && command == x
    ));
}

#[test]
fn a_campaign_supplies_what_its_candidate_executed() {
    // Stress run d12-15, three voters: r2 far behind (401 executed against
    // 3092) held x at ACCEPT from ballot 3's Sync without its payload. r1
    // executed x at 1185 and still held it, but a report leaves out what
    // its replica executed long ago, so r1's own report did not name it.
    // r1 reached only r2, and every campaign failed as `HalfInitialized`
    // naming r2. The candidate knows x was decided: it selects x and
    // commits it before binding.
    let config = quorum(ballot(4, 1));
    let x = CommandId(Digest32([0x79; 32]));
    let d = CommandId(Digest32([0x7a; 32]));
    let report = |replica, entries| RecoveryReport {
        replica,
        ballot: config.ballot(),
        committed_ballot: ballot(3, 0),
        entries,
    };
    let mut c = coord_consensus::Campaign::new(config.clone());
    c.own_report(report(r(1), vec![]));
    let behind = report(
        r(2),
        vec![coord_consensus::ReportEntry {
            command: x,
            phase: Phase::Accept,
            deps: vec![d],
            path: coord_consensus::empty_path(),
            paths: Vec::new(),
            seqnum: 1184,
            keys: Vec::new(),
            payload_present: false,
        }],
    );
    c.promise(r(2));
    for page in coord_consensus::paginate(&behind, 64) {
        c.page(page).unwrap();
    }
    assert_eq!(
        c.try_select(|_| false),
        Ok(None),
        "nobody supplies x and no majority remains without r2: wait"
    );
    let decision = c
        .try_select(|command| *command == x)
        .expect("the candidate supplies x")
        .expect("selected");
    assert_eq!(decision.entries[&x].deps, vec![d]);
    c.commit_executed(|command| (*command == x).then_some(Some(vec![d])));
    assert_eq!(
        c.decision().unwrap().entries[&x].phase,
        Phase::Commit,
        "executed by the candidate: committed before binding"
    );
}

/// A Sync leaves no acceptance of an earlier ballot that it does not carry
/// (task-d11).
///
/// r1 adopts x under ballot 0 and is restarted, so its table holds x at
/// ACCEPT from its rows. Ballot 1 is selected without r1's report and
/// re-proposes x; its leader and another voter would adopt x again, with
/// the dependencies ballot 1's leader orders it after. r1 then installs
/// ballot 1's Sync, which does not carry x as an entry. A report is
/// labelled with the synchronized ballot, so r1 used to report ballot 0's
/// acceptance of x as ballot 1's: beside a ballot-1 report of x under
/// other dependencies, every later selection failed as
/// `IncompatibleAccepted` (the three-voter stress stalls), and without
/// one, a selection installed dependencies no ballot-1 quorum agreed on.
/// The acceptance is demoted to PRE-ACCEPT, durably with the marker, so a
/// restart cannot bring it back.
#[test]
fn a_sync_leaves_no_acceptance_of_an_earlier_ballot() {
    let mut cluster = Cluster::new(47);
    let a = cluster.admit(1, 1);
    let x = cluster.admit(2, 1);
    cluster.no_execute = vec![1];
    cluster.settle();
    // Restarted: the in-memory commit is gone, the ACCEPT row is not.
    cluster.crash(1);
    cluster.revive(1, quorum(ballot(0, 0)));
    let before = cluster.nodes[1].follower().report(ballot(1, 0));
    let entry = before.entries.iter().find(|e| e.command == x).unwrap();
    assert_eq!((entry.phase, entry.deps.clone()), (Phase::Accept, vec![a]));

    // Ballot 1 is r0's again, so r1 is in its fast set ({r0, r1}).
    let b1 = ballot(1, 0);
    let effects = cluster.nodes[1].step(peer_event(
        r(0),
        ProtocolMessage::NewLeader {
            ballot: b1,
            executed: coord_types::ids::ExecutionPosition::ZERO,
        },
    ));
    cluster.handle(1, effects);
    let decision = SyncDecision {
        ballot: b1,
        source_ballot: ballot(0, 0),
        entries: BTreeMap::new(),
        reproposed: [x].into_iter().collect(),
    };
    let effects = cluster.nodes[1].step(peer_event(r(0), ProtocolMessage::Sync(decision)));
    cluster.handle(1, effects);
    assert_eq!(cluster.nodes[1].follower().quorum().ballot(), b1);

    // What ballot 1's leader and another voter adopted for x: the order
    // ballot 1's leader gave it, here none.
    let q = quorum(ballot(2, 0));
    let check = |cluster: &Cluster| {
        let ours = cluster.nodes[1].follower().report(ballot(2, 0));
        assert_eq!(ours.committed_ballot, b1);
        let mut theirs = ours.clone();
        theirs.replica = r(2);
        for e in &mut theirs.entries {
            if e.command == x {
                e.phase = Phase::Accept;
                e.deps = Vec::new();
            }
        }
        let selected = coord_consensus::recovery::select(&q, &[ours.clone(), theirs])
            .expect("the selection completes");
        assert_eq!(selected.entries[&x].deps, Vec::<CommandId>::new());
        let entry = ours.entries.iter().find(|e| e.command == x).unwrap();
        assert_eq!(
            entry.phase,
            Phase::PreAccept,
            "ballot 0's acceptance of x is reported as ballot 1's"
        );
        // Nor as a fast decision of ballot 1. With ballot 1's leader r0
        // down, r2 behind, and r1 the only reporter of ballot 1 and a
        // member of its fast set, the fast-path analysis used to take the
        // path x had under ballot 0 as evidence that ballot 1 decided x
        // fast, and selected it with ballot 0's dependencies.
        let q2 = quorum(ballot(2, 2));
        let ours = cluster.nodes[1].follower().report(ballot(2, 2));
        let mut older = ours.clone();
        older.replica = r(2);
        older.committed_ballot = ballot(0, 0);
        older.entries.retain(|e| e.command != x);
        let alone = coord_consensus::recovery::select(&q2, &[ours, older])
            .expect("the selection completes");
        assert!(
            !alone.entries.contains_key(&x),
            "a demoted acceptance was selected as a fast decision: {:?}",
            alone.entries.get(&x)
        );
    };
    check(&cluster);
    // Durable with the marker: a restart before the next report reports
    // the same.
    cluster.crash(1);
    cluster.revive(1, quorum(b1));
    check(&cluster);
}

/// A Sync demotes no acceptance of its own ballot (task-d11).
///
/// A duplicate Sync that arrives while the first one's marker is still
/// becoming durable activates the new ballot early, and a proposal of that
/// ballot queued before it is adopted. The marker's batch demotes x's
/// acceptance of ballot 0; when it becomes durable it must not demote the
/// acceptance of ballot 1 taken since, whose own row follows it.
#[test]
fn a_sync_demotes_no_acceptance_of_its_own_ballot() {
    let mut cluster = Cluster::new(53);
    let a = cluster.admit(1, 1);
    let x = cluster.admit(2, 1);
    cluster.no_execute = vec![1];
    cluster.settle();
    cluster.crash(1);
    cluster.revive(1, quorum(ballot(0, 0)));
    let phase = |cluster: &Cluster| {
        cluster.nodes[1]
            .follower()
            .table()
            .record(&x)
            .unwrap()
            .phase
    };
    assert_eq!(phase(&cluster), Phase::Accept);

    let b1 = ballot(1, 2);
    let effects = cluster.nodes[1].step(peer_event(
        r(2),
        ProtocolMessage::NewLeader {
            ballot: b1,
            executed: coord_types::ids::ExecutionPosition::ZERO,
        },
    ));
    cluster.handle(1, effects);
    let decision = SyncDecision {
        ballot: b1,
        source_ballot: ballot(0, 0),
        entries: BTreeMap::new(),
        reproposed: [x].into_iter().collect(),
    };
    // The marker's batch, not yet durable.
    let marker = cluster.nodes[1].step(peer_event(r(2), ProtocolMessage::Sync(decision.clone())));
    // Ballot 1's proposal of x, after a, then the duplicate Sync.
    let admission = cluster.nodes[1]
        .follower()
        .table()
        .record(&x)
        .unwrap()
        .payload
        .unwrap();
    let proposal = coord_consensus::FastAck {
        replica: r(2),
        ballot: b1,
        command: x,
        deps: vec![a],
        paths: Vec::new(),
        path: Digest32([7; 32]),
        admission,
        seqnum: Some(0),
    };
    let effects = cluster.nodes[1].step(peer_event(r(2), ProtocolMessage::Proposal(proposal)));
    cluster.handle(1, effects);
    let adoption = cluster.nodes[1].step(peer_event(r(2), ProtocolMessage::Sync(decision)));
    assert_eq!(cluster.nodes[1].follower().quorum().ballot(), b1);
    assert_eq!(phase(&cluster), Phase::Accept, "adopted under ballot 1");

    // The marker, then the adoption, become durable in that order.
    cluster.handle(1, marker);
    cluster.handle(1, adoption);
    assert_eq!(
        phase(&cluster),
        Phase::Accept,
        "the marker demoted ballot 1's own acceptance"
    );
    let report = cluster.nodes[1].follower().report(ballot(2, 0));
    let entry = report.entries.iter().find(|e| e.command == x).unwrap();
    assert_eq!((entry.phase, entry.deps.clone()), (Phase::Accept, vec![a]));
}

/// A new leader anchors fresh proposals at the key's tail, never at an
/// executed command a behind reporter put in `reproposed`.
#[test]
fn a_fresh_proposal_follows_the_tail_not_an_executed_reproposal() {
    let mut c = Cluster::new(59);
    let cmds: Vec<CommandId> = (1..=6).map(|s| c.admit(s, 1)).collect();
    c.settle();
    let tail = *cmds.last().unwrap();
    let old = cmds[1];
    for i in 0..3 {
        assert!(
            c.nodes[i].executed.contains(&tail),
            "node {i} executed the tail"
        );
    }
    // Every voter executed all six, so a selection has no entry for
    // them; a reporter behind the source ballot still holds `old`'s row,
    // so the selection re-proposes it.
    let new = ballot(1, 2);
    let decision = SyncDecision {
        ballot: new,
        source_ballot: ballot(0, 0),
        entries: BTreeMap::new(),
        reproposed: core::iter::once(old).collect(),
    };
    // Promised, as a winning candidate is.
    let effects = c.nodes[2].step(peer_event(
        r(2),
        ProtocolMessage::NewLeader {
            ballot: new,
            executed: coord_types::ids::ExecutionPosition::ZERO,
        },
    ));
    c.handle(2, effects);
    let (leader, effects) = Leader::from_recovered(
        c.nodes[2].role.take().map(role_into_recovered).unwrap(),
        quorum(new),
        &decision,
    );
    c.nodes[2].role = Some(Role::Leader(leader));
    c.handle(2, effects);
    let fresh = c.admit_at(7, 1, &[2]);
    let Some(Role::Leader(leader)) = c.nodes[2].role.as_mut() else {
        unreachable!()
    };
    let refused = leader.take_rejections();
    let Some(proposal) = leader.proposal(&fresh) else {
        panic!("not proposed: {refused:?}")
    };
    assert_eq!(
        proposal.deps,
        vec![tail],
        "a fresh command forks off the chain at the executed {old:?}"
    );
}

/// With no entry to follow, a re-proposed command still follows what the
/// new leader executed.
#[test]
fn a_reproposal_with_no_entry_to_follow_follows_the_executed_tail() {
    let mut c = Cluster::new(61);
    let cmds: Vec<CommandId> = (1..=6).map(|s| c.admit(s, 1)).collect();
    c.settle();
    let tail = *cmds.last().unwrap();
    // Reaches only r2, and ballot 0's leader never proposes it.
    let undecided = c.admit_at(7, 1, &[2]);
    let new = ballot(1, 2);
    let decision = SyncDecision {
        ballot: new,
        source_ballot: ballot(0, 0),
        entries: BTreeMap::new(),
        reproposed: core::iter::once(undecided).collect(),
    };
    let effects = c.nodes[2].step(peer_event(
        r(2),
        ProtocolMessage::NewLeader {
            ballot: new,
            executed: coord_types::ids::ExecutionPosition::ZERO,
        },
    ));
    c.handle(2, effects);
    let (leader, _) = Leader::from_recovered(
        c.nodes[2].role.take().map(role_into_recovered).unwrap(),
        quorum(new),
        &decision,
    );
    let proposal = leader.proposal(&undecided).expect("re-proposed");
    assert_eq!(
        proposal.deps,
        vec![tail],
        "the re-proposal does not follow the six commands every voter executed"
    );
}

/// A candidate restarted before it wins still chains after the last
/// command it executed: a restart replays the executed identities in the
/// order they executed, and the last one replayed is the anchor.
#[test]
fn a_restarted_candidate_with_no_entries_chains_after_what_it_executed() {
    let mut c = Cluster::new(67);
    let cmds: Vec<CommandId> = (1..=6).map(|s| c.admit(s, 1)).collect();
    c.settle();
    let tail = *cmds.last().unwrap();
    // Restarted after a trim took every executed command's dependency
    // row: only the executed identities come back, in the order they
    // executed, as `coordd` replays them. Nothing in the table says which
    // command is the key's latest.
    c.crash(2);
    let executed = c.nodes[2].executed.clone();
    let mut f = Follower::recover_with_syncs(
        FollowerConfig {
            identity: identity(2),
            genesis: ballot(0, 0),
            quorum: quorum(ballot(0, 0)),
            frontend: FRONTEND,
            capacity: c.capacity,
        },
        promise_row(&c.nodes[2].storage),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        ExecutionPosition::ZERO,
    )
    .restore_execution(
        ExecutionPosition::new(executed.len() as u64).unwrap(),
        executed.iter().copied(),
    );
    c.nodes[2].boot += 1;
    f.step(boot_event(c.nodes[2].boot));
    c.nodes[2].role = Some(Role::Follower(f));
    c.nodes[2].alive = true;
    // Every voter executed all six; a reporter behind the source ballot
    // still holds one of them.
    let new = ballot(1, 2);
    let decision = SyncDecision {
        ballot: new,
        source_ballot: ballot(0, 0),
        entries: BTreeMap::new(),
        reproposed: core::iter::once(cmds[1]).collect(),
    };
    let effects = c.nodes[2].step(peer_event(
        r(2),
        ProtocolMessage::NewLeader {
            ballot: new,
            executed: coord_types::ids::ExecutionPosition::ZERO,
        },
    ));
    c.handle(2, effects);
    let (leader, effects) = Leader::from_recovered(
        c.nodes[2].role.take().map(role_into_recovered).unwrap(),
        quorum(new),
        &decision,
    );
    c.nodes[2].role = Some(Role::Leader(leader));
    c.handle(2, effects);
    let fresh = c.admit_at(7, 1, &[2]);
    let Some(Role::Leader(leader)) = c.nodes[2].role.as_mut() else {
        unreachable!()
    };
    let refused = leader.take_rejections();
    let Some(proposal) = leader.proposal(&fresh) else {
        panic!("not proposed: {refused:?}")
    };
    assert_eq!(
        proposal.deps,
        vec![tail],
        "the restarted leader forks the chain"
    );
    assert!(
        leader.proposal(&cmds[1]).is_none(),
        "a command it executed is not proposed again"
    );
}

/// A new leader that committed commands it has not executed yet chains
/// after them, not after the last command it executed: every other voter
/// executed them, and a fresh command that followed only the executed
/// tail would run before them here and after them there. The same with
/// one of them re-proposed by a behind reporter, and with nothing
/// re-proposed.
#[test]
fn a_new_leader_chains_after_what_it_committed_and_has_not_executed() {
    for reproposed in [BTreeSet::new(), BTreeSet::from([4usize])] {
        let mut c = Cluster::new(71);
        let mut cmds: Vec<CommandId> = (1..=3).map(|s| c.admit(s, 1)).collect();
        c.settle();
        // r2 commits the next three and executes none of them.
        c.no_execute = vec![2];
        cmds.extend((4..=6).map(|s| c.admit(s, 1)));
        c.settle();
        let tail = *cmds.last().unwrap();
        assert!(c.nodes[0].executed.contains(&tail));
        assert!(c.nodes[1].executed.contains(&tail));
        assert_eq!(
            c.nodes[2].executed,
            cmds[..3],
            "r2 executed only the first three"
        );
        let new = ballot(1, 2);
        let decision = SyncDecision {
            ballot: new,
            source_ballot: ballot(0, 0),
            entries: BTreeMap::new(),
            reproposed: reproposed.iter().map(|&i| cmds[i]).collect(),
        };
        let effects = c.nodes[2].step(peer_event(
            r(2),
            ProtocolMessage::NewLeader {
                ballot: new,
                executed: coord_types::ids::ExecutionPosition::ZERO,
            },
        ));
        c.handle(2, effects);
        let (leader, effects) = Leader::from_recovered(
            c.nodes[2].role.take().map(role_into_recovered).unwrap(),
            quorum(new),
            &decision,
        );
        // A committed command is not proposed again: with chained
        // dependencies it would be decided twice, and a voter that adopted
        // the second order would report it beside this leader's commit
        // under one synchronized ballot, which fails every later selection
        // as `IncompatibleAccepted`.
        for &i in &reproposed {
            assert!(
                leader.proposal(&cmds[i]).is_none(),
                "the committed {:?} was proposed again",
                cmds[i]
            );
        }
        c.nodes[2].role = Some(Role::Leader(leader));
        c.handle(2, effects);
        let fresh = c.admit_at(7, 1, &[2]);
        let Some(Role::Leader(leader)) = c.nodes[2].role.as_mut() else {
            unreachable!()
        };
        let refused = leader.take_rejections();
        let Some(proposal) = leader.proposal(&fresh) else {
            panic!("not proposed: {refused:?}")
        };
        // After the committed tail, and after the executed one too, which
        // is already behind it.
        assert_eq!(
            proposal.deps,
            vec![tail, cmds[2]],
            "re-proposed {reproposed:?}: a fresh command forks off the committed commands"
        );
    }
}

/// A new leader whose table says an old command is unexecuted still chains
/// after the last command it executed (task-d12, stress run d12-11).
///
/// A refused command took its position and left no executed row, so a
/// restart replayed everything but it: the table held it at COMMIT, from
/// its dependency row, behind everything executed since. A selection
/// carrying it made it the last entry the leader had not executed, and a
/// chain that started there alone let a fresh command run before
/// everything executed after the refusal on a voter that had not run
/// those yet.
#[test]
fn a_command_that_comes_back_unexecuted_does_not_restart_the_chain() {
    let mut c = Cluster::new(83);
    let cmds: Vec<CommandId> = (1..=6).map(|s| c.admit(s, 1)).collect();
    c.settle();
    let tail = *cmds.last().unwrap();
    let stale = cmds[1];
    c.crash(2);
    // Every executed identity comes back but the refused one's.
    c.nodes[2].executed.retain(|x| *x != stale);
    c.no_execute = vec![2];
    c.revive(2, quorum(ballot(0, 0)));
    let deps_of_stale = dependency_rows(&c.nodes[2].storage)
        .into_iter()
        .find(|(x, _)| *x == stale)
        .map(|(_, record)| record.deps)
        .expect("its dependency row is still there");
    let new = ballot(1, 2);
    let decision = SyncDecision {
        ballot: new,
        source_ballot: ballot(0, 0),
        entries: BTreeMap::from([(
            stale,
            coord_consensus::SyncEntry {
                command: stale,
                phase: Phase::Commit,
                deps: deps_of_stale,
                path: coord_consensus::empty_path(),
                paths: Vec::new(),
                seqnum: 0,
            },
        )]),
        reproposed: BTreeSet::new(),
    };
    let effects = c.nodes[2].step(peer_event(
        r(2),
        ProtocolMessage::NewLeader {
            ballot: new,
            executed: coord_types::ids::ExecutionPosition::ZERO,
        },
    ));
    c.handle(2, effects);
    let (leader, effects) = Leader::from_recovered(
        c.nodes[2].role.take().map(role_into_recovered).unwrap(),
        quorum(new),
        &decision,
    );
    c.nodes[2].role = Some(Role::Leader(leader));
    c.handle(2, effects);
    let fresh = c.admit_at(7, 1, &[2]);
    let Some(Role::Leader(leader)) = c.nodes[2].role.as_mut() else {
        unreachable!()
    };
    let refused = leader.take_rejections();
    let Some(proposal) = leader.proposal(&fresh) else {
        panic!("not proposed: {refused:?}")
    };
    assert!(
        proposal.deps.contains(&tail),
        "a fresh command follows only {:?}, not {tail:?}",
        proposal.deps
    );
}

/// A commit in a report below the source ballot is a decision, and the
/// selection carries it (task-d12).
///
/// r1 executed x after a; r2 adopted both and, restarted before it
/// executed them, holds them at ACCEPT (a commit is not written as a
/// row). Both report below the source ballot, which only a report without x
/// holds. The selection carries x at COMMIT with r1's dependencies, and
/// r2, winning, proposes it with them: with x re-proposed instead, r2
/// would chain it after whatever it held last, and decide a second time
/// a command the domain had decided.
#[test]
fn a_commit_below_the_source_ballot_is_selected_with_its_dependencies() {
    let mut c = Cluster::new(79);
    c.no_execute = vec![2];
    let a = c.admit(1, 1);
    let x = c.admit(2, 1);
    c.settle();
    assert_eq!(c.nodes[1].executed, vec![a, x]);
    c.crash(2);
    c.revive(2, quorum(ballot(0, 0)));
    assert_eq!(
        c.nodes[2].follower().table().phase_of(&x),
        Some(Phase::Accept)
    );
    let decided = c.nodes[1]
        .follower()
        .table()
        .record(&x)
        .unwrap()
        .deps
        .clone();
    assert_eq!(decided, vec![a]);
    let new = ballot(2, 2);
    let reports = |c: &Cluster| {
        let mut reports: Vec<RecoveryReport> =
            (1..3).map(|i| c.nodes[i].follower().report(new)).collect();
        // The source: a voter synchronized to a later ballot that holds
        // neither command.
        reports.push(RecoveryReport {
            replica: r(0),
            ballot: new,
            committed_ballot: ballot(1, 0),
            entries: Vec::new(),
        });
        reports
    };
    let decision = select(&quorum(new), &reports(&c)).unwrap();
    let entry = decision.entries.get(&x).expect("x is selected");
    assert_eq!(
        (entry.phase, entry.deps.clone()),
        (Phase::Commit, decided.clone())
    );
    assert!(!decision.reproposed.contains(&x));
    // r2 wins with that selection and proposes x with its decided
    // dependencies.
    let effects = c.nodes[2].step(peer_event(
        r(2),
        ProtocolMessage::NewLeader {
            ballot: new,
            executed: coord_types::ids::ExecutionPosition::ZERO,
        },
    ));
    c.handle(2, effects);
    let (leader, _) = Leader::from_recovered(
        c.nodes[2].role.take().map(role_into_recovered).unwrap(),
        quorum(new),
        &decision,
    );
    let proposal = leader.proposal(&x).expect("x is proposed");
    assert_eq!(proposal.deps, decided);
}

/// A below-source commit that meets an at-source acceptance of the same
/// command under other dependencies is the alarm it should be (task-d12).
#[test]
fn a_below_source_commit_against_an_acceptance_of_other_dependencies_is_incompatible() {
    let mut c = Cluster::new(83);
    let a = c.admit(1, 1);
    let x = c.admit(2, 1);
    c.settle();
    assert_eq!(c.nodes[1].executed, vec![a, x]);
    let new = ballot(2, 2);
    let below = c.nodes[1].follower().report(new);
    let committed = below.entries.iter().find(|e| e.command == x).unwrap();
    assert_eq!(
        (committed.phase, committed.deps.clone()),
        (Phase::Commit, vec![a])
    );
    let mut other = committed.clone();
    other.phase = Phase::Accept;
    other.deps = Vec::new();
    let at_source = RecoveryReport {
        replica: r(0),
        ballot: new,
        committed_ballot: ballot(1, 0),
        entries: vec![other],
    };
    assert!(matches!(
        select(&quorum(new), &[below, at_source]),
        Err(RecoveryError::IncompatibleAccepted { command, .. }) if command == x
    ));
}

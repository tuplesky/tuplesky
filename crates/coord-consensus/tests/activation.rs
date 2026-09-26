//! task-26 acceptance: learned outcomes survive a lost leader and lost
//! volatile commit notifications; competing recovery, delayed replies and
//! same-boot old effects cannot establish divergence; permuted reports
//! give the same selection; a crash after the Sync was bound republishes
//! the same result and never reselects under the same ballot.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use coord_consensus::{
    AppliedOutcome, BallotConfiguration, CommandRecord, ConfigurationIdentity, Follower,
    FollowerConfig, FollowerRejection, Leader, LeaderConfig, PageError, Phase, ProtocolMessage,
    ReplicaRole, SyncDecision, decode_dependency, decode_promise, decode_sync,
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
    frontend: Vec<(ReplicaId, ProtocolMessage)>,
    /// Every payload ask sent: who asked, and for what.
    asks: Vec<(usize, Vec<CommandId>)>,
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
            frontend: Vec::new(),
            asks: Vec::new(),
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
                if !self.nodes[i].alive {
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
                capacity: 32,
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
    let promise = f.step(peer_event(r(2), ProtocolMessage::NewLeader { ballot: new }));
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
        ProtocolMessage::NewLeader { ballot: later },
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

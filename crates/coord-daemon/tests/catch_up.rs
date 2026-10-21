//! task-d08: a voter behind by more than a table is brought up from a
//! peer's executed history, end to end in one process.
//!
//! The real machines, over real (model-engine) stores, driven through the
//! real voter door: the donor answers from its durable rows
//! (`Node::serve_catch_up`), and the lagging voter asks on the real
//! pacer, with time as an input. A voter is cut off for thirty seconds of
//! twenty operations a second with a table of eight, then healed under
//! the same load; it is to be serving -- executed as far as the leader,
//! its table drained, refusing nothing for backpressure -- within ten
//! seconds, and its `executed_v1` rows are to be the leader's.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use coord_collector::{Collector, CollectorConfig, MonotonicMillis, Submitted};
use coord_consensus::{
    BallotConfiguration, ConfigurationIdentity, Follower, FollowerConfig, Leader, LeaderConfig,
    LearningMode, ReplicaRole,
};
use coord_core::capability::{AdmissionReceipt, AttestedAdmission, VerifierToken};
use coord_core::effect::{BootId, PeerId};
use coord_core::event::{AdmittedRequest, PeerProvenance};
use coord_core::outbox::BarrierAllocator;
use coord_daemon::catch_up::Pacer;
use coord_daemon::mailbox::{Ingress, IngressBudget};
use coord_daemon::node::{Machine, Node, Outbound};
use coord_daemon::voter::{Origin, Voter};
use coord_membership::genesis::{GenesisManifest, VoterSeed};
use coord_membership::membership::Membership;
use coord_state::policy::{Action as PolicyAction, KeyInterval, PolicyRule};
use coord_storage::policy::{bootstrap_session, rule_update};
use coord_storage::{Applier, GroupLimits, StoreWorker};
use coord_store_testkit::model::ModelEngine;
use coord_types::identity::Digest32;
use coord_types::ids::{
    Ballot, ClientInstanceId, ClusterId, ConfigurationEpoch, DomainId, NamespaceId, PolicyRuleId,
    PrincipalId, ReplicaId, ReplicaIncarnation, RequestSequence, SessionId,
};
use coord_types::logical_v1::{CanonicalOperation, LogicalRequest, PutOp};
use coord_types::wire_v1::{Frame, MessageV1, PeerRole, RequestV1};
use coord_types::{CommandId, RetryKey};

const NS: NamespaceId = NamespaceId([5; 16]);
const SESSION: SessionId = SessionId([3; 16]);
const ALICE: PrincipalId = PrincipalId([0xa; 16]);
const CLUSTER: ClusterId = ClusterId([1; 16]);
const DOMAIN: DomainId = DomainId([2; 16]);
const FRONTEND: PeerId = PeerId {
    replica: ReplicaId([0xf0; 16]),
    incarnation: ReplicaIncarnation::ZERO,
};

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

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn b64url(bytes: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(A[((n >> (18 - 6 * i)) & 0x3f) as usize] as char);
            }
        }
    }
    out
}

fn membership() -> Membership {
    let manifest = GenesisManifest {
        cluster: hex(&CLUSTER.0),
        domain: hex(&DOMAIN.0),
        epoch: 1,
        voters: (0u8..3)
            .map(|n| VoterSeed {
                node: hex(&[n; 16]),
                incarnation: 1,
                public_key: b64url(&[n + 1; 32]),
            })
            .collect(),
        issuer_roots: vec![b64url(&[0xca; 8])],
        wif_rules: vec![serde_json::json!({ "issuer": "test" })],
        admin: hex(&[0xa; 16]),
        protocol_version: 1,
    };
    Membership::from_genesis(&manifest).expect("membership")
}

fn identity(me: u8) -> ConfigurationIdentity {
    ConfigurationIdentity {
        cluster: CLUSTER,
        domain: DOMAIN,
        epoch: epoch(),
        voters: (0..3).map(r).collect(),
        replica: r(me),
        incarnation: inc(),
        role: ReplicaRole::Voter,
    }
}

/// Three voters, fast set {0, 1}: the fast path needs the leader and
/// follower 1, so follower 1's acknowledgement is one the caller's
/// completion depends on.
fn quorum() -> BallotConfiguration {
    let fast: BTreeSet<ReplicaId> = (0..2).map(r).collect();
    BallotConfiguration::c2(epoch(), ballot(), (0..3).map(r).collect(), fast).unwrap()
}

fn bootstrap_updates() -> Vec<coord_core::effect::StoreUpdate> {
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
    updates
}

fn store(boot: BootId) -> Applier<StoreWorker<ModelEngine>> {
    let mut worker =
        StoreWorker::open(ModelEngine::new(), boot, inc(), GroupLimits::default()).unwrap();
    let mut alloc = BarrierAllocator::new(inc(), boot);
    let base = worker.application_base();
    worker
        .submit(coord_core::effect::PersistBatch {
            barrier: alloc.allocate(),
            base: Some(base),
            updates: bootstrap_updates(),
        })
        .unwrap();
    worker.flush().unwrap();
    Applier::new(worker, alloc).unwrap()
}

/// Voter `me`, booted, with the ingress a co-located collector may
/// deliver through. Replica 0 leads the ballot.
fn voter(me: u8, capacity: usize) -> Voter<StoreWorker<ModelEngine>> {
    let boot = BootId([me + 1; 16]);
    let applier = store(boot);
    let bootstrapped = applier.store().application_base().execution_position;
    let machine = if me == 0 {
        let mut machine = Leader::new(
            LeaderConfig {
                identity: identity(0),
                quorum: quorum(),
                genesis: ballot(),
                frontend: FRONTEND,
                capacity,
            },
            None,
            bootstrapped,
        );
        machine.set_learning(LearningMode::Full);
        Machine::Leader(Box::new(machine))
    } else {
        let mut machine = Follower::new(FollowerConfig {
            identity: identity(me),
            quorum: quorum(),
            genesis: ballot(),
            frontend: FRONTEND,
            capacity,
        })
        .restore_execution(bootstrapped, []);
        machine.set_learning(LearningMode::Full);
        Machine::Follower(Box::new(machine))
    };
    let node = Node::new(machine, applier, FRONTEND);
    let ingress = Ingress::new(
        &membership(),
        r(me),
        PeerRole::Frontend,
        IngressBudget::default(),
    )
    .expect("a committed voter");
    let mut voter = Voter::new(node, ingress, (CLUSTER, DOMAIN), ballot());
    voter.boot(boot, inc()).expect("boot");
    voter
}

fn retry_key(sequence: u64) -> RetryKey {
    RetryKey {
        cluster_id: CLUSTER,
        domain_id: DOMAIN,
        session_id: SESSION,
        client_instance_id: ClientInstanceId([4; 16]),
        request_sequence: RequestSequence::new(sequence).unwrap(),
    }
}

fn logical(sequence: u64) -> LogicalRequest {
    let mut logical = LogicalRequest::new(
        NS,
        CanonicalOperation::Put(PutOp {
            key: format!("k{sequence}").into_bytes(),
            value: b"v".to_vec(),
            lease: None,
            prev_kv: false,
        }),
    );
    logical.canonicalize();
    logical
}

/// One admitted client request, as the collector's admission gate
/// hands it to the collector.
fn admitted(sequence: u64) -> AdmittedRequest {
    let request = RequestV1::new(retry_key(sequence), &logical(sequence), 0, 0).unwrap();
    AdmittedRequest {
        receipt: AdmissionReceipt::submitting(
            VerifierToken::for_boundary(),
            AttestedAdmission {
                cluster: CLUSTER,
                domain: DOMAIN,
                session: SESSION,
                rule_generation: 1,
                scope_ceiling: u32::MAX,
                receipt_id: Digest32([7; 32]),
                admitted_at_ticks: 0,
            },
        ),
        frame: MessageV1::Request(request).encode().unwrap(),
    }
}

fn frame(bytes: &[u8]) -> Frame {
    let mut reader = coord_types::wire_v1::FrameReader::new();
    reader.push(bytes).expect("bounded");
    reader.next_frame().expect("well formed").expect("complete")
}

/// The voters' table capacity: small, so thirty seconds of load is many
/// tables' worth.
const CAPACITY: usize = 8;

/// Operations a simulated second.
const RATE: u64 = 20;

struct Cluster {
    voters: Vec<Voter<StoreWorker<ModelEngine>>>,
    collector: Collector,
    peers: std::collections::VecDeque<(usize, PeerProvenance, Vec<u8>)>,
    /// Voters cut off: nothing reaches them and nothing they send leaves.
    down: Vec<usize>,
    pacers: Vec<Pacer>,
    now: Instant,
    sequence: u64,
    backpressure: Vec<usize>,
}

impl Cluster {
    fn new() -> Self {
        Cluster {
            voters: (0..3).map(|me| voter(me, CAPACITY)).collect(),
            collector: Collector::new(CollectorConfig {
                quorum: quorum(),
                max_pending: 1 << 16,
                max_resolved: 16,
                max_undelivered_bytes: usize::MAX,
            }),
            peers: std::collections::VecDeque::new(),
            down: Vec::new(),
            pacers: vec![Pacer::default(), Pacer::default(), Pacer::default()],
            now: Instant::now(),
            sequence: 0,
            backpressure: vec![0; 3],
        }
    }

    fn carry(&mut self, from: usize, out: Outbound) {
        // The collector is not what this is about: what the voters tell
        // it is dropped.
        for (to, frame) in out.peer {
            let prov = self.voters[from].provenance();
            self.peers
                .push_back((usize::from(to.replica.0[0]), prov, frame));
        }
    }

    /// One client request, submitted to every voter that is up.
    fn submit(&mut self) {
        self.sequence += 1;
        let Submitted::FanOut(fan_out) = self
            .collector
            .submit(MonotonicMillis::ZERO, &admitted(self.sequence))
            .expect("submitted")
        else {
            panic!("not new work")
        };
        let submit = frame(&fan_out.frame);
        for to in 0..3 {
            if self.down.contains(&to) {
                continue;
            }
            let out = self.voters[to]
                .on_submission(PeerRole::Frontend, &submit, Origin::Connection(7))
                .expect("driven")
                .expect("admitted");
            self.carry(to, out);
        }
    }

    /// The peer plane and execution until nothing moves.
    fn settle(&mut self) {
        loop {
            let mut moved = false;
            while let Some((to, prov, bytes)) = self.peers.pop_front() {
                let from = usize::from(prov.from().0[0]);
                if self.down.contains(&to) || self.down.contains(&from) {
                    continue;
                }
                let out = self.voters[to].on_peer(prov, bytes).expect("driven");
                self.carry(to, out);
                moved = true;
            }
            for i in 0..3 {
                if self.down.contains(&i) {
                    continue;
                }
                let out = self.voters[i].execute().expect("executed");
                if !out.is_empty() {
                    moved = true;
                }
                self.carry(i, out);
            }
            if !moved {
                break;
            }
        }
        for i in 0..3 {
            let refused = self.voters[i].take_rejections();
            self.backpressure[i] += refused
                .iter()
                .filter(|r| r.contains("Backpressure"))
                .count();
        }
    }

    /// What coordd's serve loop does on its own schedule, for one
    /// simulated second: the leader's re-send and frontier, a payload
    /// ask, and the catch-up pacer.
    fn tick(&mut self) {
        self.now += Duration::from_secs(1);
        for i in 0..3 {
            if self.down.contains(&i) {
                continue;
            }
            let out = self.voters[i].resend_proposals().expect("resent");
            self.carry(i, out);
            if self.voters[i].awaiting().is_some() || self.voters[i].missing_payloads() > 0 {
                let out = self.voters[i].request_payloads().expect("asked");
                self.carry(i, out);
            }
            let now = self.now;
            let mut pacer = std::mem::take(&mut self.pacers[i]);
            let out = self.voters[i].catch_up(&mut pacer, now).expect("asked");
            self.pacers[i] = pacer;
            self.carry(i, out);
            self.settle();
        }
    }

    /// One simulated second of load.
    fn second(&mut self) {
        for _ in 0..RATE {
            self.submit();
            self.settle();
        }
        self.tick();
    }

    fn executed_through(&self, i: usize) -> u64 {
        self.voters[i].node().machine().executed_through().get()
    }

    /// Voter `i`'s `executed_v1` rows, in position order.
    fn executed_rows(&self, i: usize) -> Vec<(u64, CommandId, Digest32)> {
        let applier = self.voters[i].node().applier();
        let gated = applier.store().reader().snapshot().expect("snapshot");
        let order = coord_storage::protocol::executed_order(
            gated.view(),
            coord_storage::views::ViewBudget::default(),
        )
        .expect("read");
        order
            .into_iter()
            .map(|(position, command)| {
                let entry =
                    coord_storage::protocol::catch_up_entry(gated.view(), epoch(), &command)
                        .expect("read")
                        .expect("executed and held");
                (position.get(), command, entry.result_digest)
            })
            .collect()
    }
}

/// A follower cut off for thirty seconds at twenty operations a second,
/// with a table of eight, is serving again within ten seconds of the
/// heal, under the same load (task-d08).
#[test]
fn a_follower_cut_off_for_thirty_seconds_serves_within_ten_of_the_heal() {
    let mut cluster = Cluster::new();
    for _ in 0..3 {
        cluster.second();
    }
    cluster.down = vec![2];
    for _ in 0..30 {
        cluster.second();
    }
    let behind = cluster.executed_through(0) - cluster.executed_through(2);
    assert!(
        behind >= 30 * RATE,
        "r2 is only {behind} behind: the partition did not hold"
    );
    cluster.down.clear();
    let mut healed_after = None;
    for second in 1..=10 {
        cluster.second();
        let caught_up = cluster.executed_through(2) == cluster.executed_through(0)
            && !cluster.voters[2].node().machine().holds_unexecuted();
        if caught_up && healed_after.is_none() {
            healed_after = Some(second);
        }
    }
    let healed_after = healed_after.unwrap_or_else(|| {
        panic!(
            "r2 did not catch up within ten seconds: at {} of {}",
            cluster.executed_through(2),
            cluster.executed_through(0)
        )
    });
    let (pages, pulled) = cluster.voters[2].node().machine().catch_up_counts();
    assert!(
        pages > 0 && pulled >= behind,
        "{pages} pages, {pulled} pulled"
    );
    assert!(cluster.voters[0].node().pages_served > 0);
    // Serving: executed as far as the leader, table drained, and new work
    // taken without backpressure once caught up.
    let refused_before = cluster.backpressure[2];
    for _ in 0..3 {
        cluster.second();
    }
    assert_eq!(cluster.executed_through(2), cluster.executed_through(0));
    assert_eq!(
        cluster.backpressure[2], refused_before,
        "r2 refused work for backpressure after it caught up (healed after {healed_after}s)"
    );
    // Its history is the leader's, row for row: the same commands at the
    // same positions with the same results.
    assert_eq!(cluster.executed_rows(2), cluster.executed_rows(0));
}

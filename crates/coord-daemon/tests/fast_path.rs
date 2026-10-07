//! A pre-acceptance the leader never ordered, at a fast-set follower
//! (task-d67), end to end in one process.
//!
//! Five voters over the reference store, driven through the real voter
//! door, with the fast set `coordd` builds (`c2_default`: the leader and
//! the next two). A client command reaches some followers and not the
//! leader, as one does while the leader's links are still coming up, and
//! its caller gives up: the leader never orders it. Run E on #98 made no
//! fast decision at all over 46,000 commands after such a start.

use coord_collector::{Collector, CollectorConfig, MonotonicMillis, Submitted};
use coord_consensus::{
    BallotConfiguration, ConfigurationIdentity, Follower, FollowerConfig, Leader, LeaderConfig,
    LearningMode, ReplicaRole,
};
use coord_core::capability::{AdmissionReceipt, AttestedAdmission, VerifierToken};
use coord_core::effect::{BootId, PeerId};
use coord_core::event::{AdmittedRequest, PeerProvenance};
use coord_core::outbox::BarrierAllocator;
use coord_daemon::mailbox::{Ingress, IngressBudget};
use coord_daemon::node::{Machine, Node, Outbound};
use coord_daemon::voter::{Origin, Voter};
use coord_membership::genesis::{GenesisManifest, VoterSeed};
use coord_membership::membership::Membership;
use coord_state::policy::{Action as PolicyAction, KeyInterval, PolicyRule};
use coord_storage::policy::{bootstrap_session, rule_update};
use coord_storage::{Applier, GroupLimits, StoreWorker};
use coord_store_testkit::model::ModelEngine;
use coord_types::RetryKey;
use coord_types::identity::Digest32;
use coord_types::ids::{
    Ballot, ClientInstanceId, ClusterId, ConfigurationEpoch, DomainId, NamespaceId, PolicyRuleId,
    PrincipalId, ReplicaId, ReplicaIncarnation, RequestSequence, SessionId,
};
use coord_types::logical_v1::{CanonicalOperation, LogicalRequest, PutOp};
use coord_types::wire_v1::{Frame, MessageV1, PeerRole, RequestV1};

const N: u8 = 5;
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
        voters: (0u8..N)
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
        voters: (0..N).map(r).collect(),
        replica: r(me),
        incarnation: inc(),
        role: ReplicaRole::Voter,
    }
}

/// Three voters, fast set {0, 1}: the fast path needs the leader and
/// follower 1, so follower 1's acknowledgement is one the caller's
/// completion depends on.
fn quorum() -> BallotConfiguration {
    BallotConfiguration::c2_default(epoch(), ballot(), (0..N).map(r).collect()).unwrap()
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
fn voter(me: u8) -> Voter<StoreWorker<ModelEngine>> {
    let capacity = 64;
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

struct Cluster {
    voters: Vec<Voter<StoreWorker<ModelEngine>>>,
    collector: Collector,
    peers: std::collections::VecDeque<(usize, PeerProvenance, Vec<u8>)>,
    sequence: u64,
}

impl Cluster {
    fn new() -> Self {
        Cluster {
            voters: (0..N).map(voter).collect(),
            collector: Collector::new(CollectorConfig {
                quorum: quorum(),
                max_pending: 1 << 16,
                max_resolved: 16,
                max_undelivered_bytes: usize::MAX,
            }),
            peers: std::collections::VecDeque::new(),
            sequence: 0,
        }
    }

    fn carry(&mut self, from: usize, out: Outbound) {
        for (to, frame) in out.peer {
            let prov = self.voters[from].provenance();
            self.peers
                .push_back((usize::from(to.replica.0[0]), prov, frame));
        }
    }

    /// One put, submitted to the voters `to`: every voter, as a
    /// collector's fan-out reaches them, or some.
    fn submit_to(&mut self, to: &[usize]) {
        self.sequence += 1;
        let Submitted::FanOut(fan_out) = self
            .collector
            .submit(MonotonicMillis::ZERO, &admitted(self.sequence))
            .expect("submitted")
        else {
            panic!("not new work")
        };
        let submit = frame(&fan_out.frame);
        for &voter in to {
            let out = self.voters[voter]
                .on_submission(PeerRole::Frontend, &submit, Origin::Connection(7))
                .expect("driven")
                .expect("admitted");
            self.carry(voter, out);
        }
    }

    /// One put to every voter, and everything it moves.
    fn command(&mut self) {
        let all: Vec<usize> = (0..usize::from(N)).collect();
        self.submit_to(&all);
        self.settle();
    }

    /// The peer plane and execution until nothing moves, then once each
    /// what `coordd` runs on its timers -- the leader's re-send and a
    /// payload ask -- and the peer plane again.
    fn settle(&mut self) {
        self.drain();
        for i in 0..usize::from(N) {
            let out = self.voters[i].resend_proposals().expect("resent");
            self.carry(i, out);
            if self.voters[i].awaiting().is_some() || self.voters[i].missing_payloads() > 0 {
                let out = self.voters[i].request_payloads().expect("asked");
                self.carry(i, out);
            }
            self.drain();
        }
    }

    fn drain(&mut self) {
        loop {
            let mut moved = false;
            while let Some((to, prov, bytes)) = self.peers.pop_front() {
                let out = self.voters[to].on_peer(prov, bytes).expect("driven");
                self.carry(to, out);
                moved = true;
            }
            for i in 0..usize::from(N) {
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
    }

    /// Commands the leader established on the fast path.
    fn fast(&self) -> u64 {
        self.voters[0].node().established.fast
    }

    /// Commands the leader established on either path.
    fn established(&self) -> u64 {
        let e = &self.voters[0].node().established;
        e.fast + e.slow
    }
}

/// Five commands, then one that reaches only the voters `orphaned_at`,
/// then `after` more: what the leader established fast of those `after`.
fn fast_after_an_orphan(orphaned_at: &[usize], after: u64) -> u64 {
    let mut cluster = Cluster::new();
    for _ in 0..5 {
        cluster.command();
    }
    assert_eq!(cluster.fast(), 5, "a quiet domain decides fast");
    cluster.submit_to(orphaned_at);
    cluster.settle();
    let before = cluster.fast();
    for _ in 0..after {
        cluster.command();
    }
    assert_eq!(
        cluster.established(),
        5 + after,
        "every command the leader was given was established"
    );
    cluster.fast() - before
}

/// A command the leader never ordered, at a follower outside the fast
/// set, changes nothing: the fast set's acknowledgements still carry the
/// leader's path.
#[test]
fn an_unordered_pre_acceptance_outside_the_fast_set_leaves_the_fast_path_alone() {
    assert_eq!(fast_after_an_orphan(&[3], 40), 40);
    assert_eq!(fast_after_an_orphan(&[3, 4], 40), 40);
}

/// A command the leader never ordered, at a fast-set follower, must not
/// take the fast path away for the rest of the ballot (task-d67).
///
/// Today it does. The follower appended the command to its path log at
/// pre-acceptance; the leader never orders it, so no synchronization
/// takes it out, and a table record leaves the log only when it executes
/// or a Sync releases it (task-d24). The next command the leader orders
/// marks it reordered, and from then on the follower's head is
/// `reordered_path`, which no leader path equals: every acknowledgement
/// it sends fails the fast predicate. With the fast set the leader and
/// two followers, one such follower is enough. Here 0 of 40 commands
/// after it are fast, at one fast-set follower or both.
#[test]
#[ignore = "task-d67: fails today, 0 of 40 fast; run with --ignored"]
fn the_fast_path_resumes_after_a_pre_acceptance_the_leader_never_ordered() {
    for orphaned_at in [&[1][..], &[1, 2]] {
        let fast = fast_after_an_orphan(orphaned_at, 40);
        assert!(
            fast > 30,
            "{fast} of 40 fast after an orphan at {orphaned_at:?}"
        );
    }
}

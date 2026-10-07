//! task-d58: the reads due in one pump are served from one snapshot, end
//! to end in one process.
//!
//! Three voters over the reference store, driven through the real voter
//! door: a put executes, two reads arrive at the leader, a round started
//! after both confirms, and one pump serves both. What is counted is the
//! snapshots the leader pinned for them.

use std::collections::BTreeSet;

use coord_collector::wire::{ReadV1, decode_read_answer, read_frame};
use coord_collector::{Collector, CollectorConfig, MonotonicMillis, ReadOutcomeV1, Submitted};
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
use coord_daemon::reads::ROUND_IN_FLIGHT_MILLIS;
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
use coord_types::logical_v1::{CanonicalOperation, KeyRange, LogicalRequest, PutOp, RangeOp};
use coord_types::wire_v1::{Frame, MessageV1, PeerRole, RequestV1};

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

/// A current read of `k1`, as a collector sends it to the leader.
fn read(sequence: u64) -> Frame {
    let mut logical = LogicalRequest::new(
        NS,
        CanonicalOperation::Range(RangeOp {
            range: KeyRange {
                key: b"k1".to_vec(),
                range_end: None,
            },
            revision: None,
            limit: 1,
            keys_only: false,
            count_only: false,
        }),
    );
    logical.canonicalize();
    let request = RequestV1::new(retry_key(sequence), &logical, 0, 0).unwrap();
    frame(
        &read_frame(&ReadV1 {
            ballot: ballot(),
            request,
        })
        .expect("a read frame"),
    )
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
            voters: (0..3).map(voter).collect(),
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

    /// One put, submitted to every voter.
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
            let out = self.voters[to]
                .on_submission(PeerRole::Frontend, &submit, Origin::Connection(7))
                .expect("driven")
                .expect("admitted");
            self.carry(to, out);
        }
    }

    /// The peer plane and execution until nothing moves. Reads are not
    /// pumped here: the test pumps them itself.
    fn settle(&mut self) {
        loop {
            let mut moved = false;
            while let Some((to, prov, bytes)) = self.peers.pop_front() {
                let out = self.voters[to].on_peer(prov, bytes).expect("driven");
                self.carry(to, out);
                moved = true;
            }
            for i in 0..3 {
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
}

/// Two reads due in one pump are served from one snapshot (task-d58).
///
/// The second read arrives once the first one's round has been in flight
/// for [`ROUND_IN_FLIGHT_MILLIS`], so it starts a round of its own, and
/// that round covers both. Once it confirms, one pump finds both due:
/// one snapshot is pinned and both are served from it, where each used to
/// pin its own.
#[test]
fn the_reads_due_in_one_pump_are_served_from_one_snapshot() {
    let mut cluster = Cluster::new();
    cluster.submit();
    cluster.settle();
    let t0 = MonotonicMillis::ZERO;
    let later = t0.plus(ROUND_IN_FLIGHT_MILLIS);

    let out = cluster.voters[0].on_read(&read(2), Origin::Connection(9), t0);
    assert!(out.reads.is_empty(), "the first read is held, not answered");
    assert!(!out.peer.is_empty(), "the first read starts a round");
    cluster.carry(0, out);
    let out = cluster.voters[0].on_read(&read(3), Origin::Connection(9), later);
    assert!(
        out.reads.is_empty(),
        "the second read is held, not answered"
    );
    assert!(!out.peer.is_empty(), "the second read starts a round");
    cluster.carry(0, out);
    cluster.settle();
    assert_eq!(cluster.voters[0].read_counts().snapshots, 0);

    let out = cluster.voters[0].pump_reads(later);
    let counts = cluster.voters[0].read_counts();
    assert_eq!(counts.rounds, 2);
    assert_eq!(counts.served, 2, "both reads are served in the one pump");
    assert_eq!(counts.snapshots, 1, "from one snapshot");
    assert_eq!(counts.behind, 0);
    assert_eq!(out.reads.len(), 2);
    for (_, bytes) in &out.reads {
        let answer = decode_read_answer(&frame(bytes)).expect("a read answer");
        assert!(
            matches!(answer.outcome, ReadOutcomeV1::Served { .. }),
            "{answer:?}"
        );
    }

    // Nothing is held, so the next pump pins nothing.
    let out = cluster.voters[0].pump_reads(later);
    assert!(out.reads.is_empty());
    assert_eq!(cluster.voters[0].read_counts().snapshots, 1);
}

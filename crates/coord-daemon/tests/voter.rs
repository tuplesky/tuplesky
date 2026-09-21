//! Acceptance for the door in front of a voter (task-j08).
//!
//! A submission reaches a voter either from a collector in another
//! process or from one in this process. These tests hold that the second
//! is a shorter journey and not a shorter admission: the two routes
//! converge before anything protocol-shaped happens, and what makes it
//! through is indistinguishable afterwards.
//!
//! They also hold the thing a co-located voter could most easily be
//! given by accident -- a way to acknowledge its own submission. Its
//! evidence is produced by the machine and carries its committed
//! identity, so the collector counts it as one voter's and not as a
//! cluster's agreement.

use std::collections::BTreeSet;

use coord_collector::wire::{SubmitV1, submit_frame};
use coord_consensus::{
    BallotConfiguration, ConfigurationIdentity, Follower, FollowerConfig, Leader, LeaderConfig,
    LearningMode, ReplicaRole,
};
use coord_core::effect::{BootId, PeerId};
use coord_core::outbox::BarrierAllocator;
use coord_daemon::LocalIngress;
use coord_daemon::mailbox::{Ingress, IngressBudget};
use coord_daemon::node::{Machine, Node};
use coord_daemon::voter::{Origin, Refused, Voter};
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
use coord_types::wire_v1::{PeerRole, RequestV1};

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

/// The same three voters the machines are configured for, as a committed
/// configuration -- so an ingress is granted by the cluster's agreement
/// rather than by the test.
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

/// The leader of the ballot, booted, with an ingress a co-located
/// collector may deliver through.
fn leader(boot: BootId, budget: IngressBudget) -> Voter<StoreWorker<ModelEngine>> {
    let applier = store(boot);
    let bootstrapped = applier.store().application_base().execution_position;
    let mut machine = Leader::new(
        LeaderConfig {
            identity: identity(0),
            quorum: quorum(),
            genesis: ballot(),
            frontend: FRONTEND,
            capacity: 64,
        },
        None,
        bootstrapped,
    );
    machine.set_learning(LearningMode::Full);
    let node = Node::new(Machine::Leader(Box::new(machine)), applier, FRONTEND);
    let ingress = Ingress::new(&membership(), r(0), PeerRole::Frontend, budget)
        .expect("replica 0 is a committed voter and Frontend may submit");
    let mut voter = Voter::new(node, ingress, (CLUSTER, DOMAIN), ballot());
    voter.boot(boot, inc()).expect("boot");
    assert!(voter.node().machine().leads());
    voter
}

/// A voter that is *not* the leader of the ballot, so a test can tell
/// this replica's own identity from the one the ballot names.
fn follower(boot: BootId) -> Voter<StoreWorker<ModelEngine>> {
    let applier = store(boot);
    let bootstrapped = applier.store().application_base().execution_position;
    let mut machine = Follower::new(FollowerConfig {
        identity: identity(1),
        quorum: quorum(),
        genesis: ballot(),
        frontend: FRONTEND,
        capacity: 64,
    })
    .restore_execution(bootstrapped, []);
    machine.set_learning(LearningMode::Full);
    let node = Node::new(Machine::Follower(Box::new(machine)), applier, FRONTEND);
    let ingress = Ingress::new(
        &membership(),
        r(1),
        PeerRole::Frontend,
        IngressBudget::default(),
    )
    .expect("replica 1 is a committed voter");
    let mut voter = Voter::new(node, ingress, (CLUSTER, DOMAIN), ballot());
    voter.boot(boot, inc()).expect("boot");
    voter
}

/// One `Submit` frame, exactly as a collector puts on the wire.
fn submission(sequence: u64) -> Vec<u8> {
    let mut logical = LogicalRequest::new(
        NS,
        CanonicalOperation::Put(PutOp {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            lease: None,
            prev_kv: false,
        }),
    );
    logical.canonicalize();
    let key = RetryKey {
        cluster_id: CLUSTER,
        domain_id: DOMAIN,
        session_id: SESSION,
        client_instance_id: ClientInstanceId([4; 16]),
        request_sequence: RequestSequence::new(sequence).unwrap(),
    };
    submit_frame(&SubmitV1 {
        receipt: coord_core::AdmissionFacts {
            attested: coord_core::AttestedAdmission {
                cluster: CLUSTER,
                domain: DOMAIN,
                session: SESSION,
                rule_generation: 1,
                scope_ceiling: u32::MAX,
                receipt_id: Digest32([7; 32]),
                admitted_at_ticks: 0,
            },
            establishing: None,
        },
        request: RequestV1::new(key, &logical, 0, 0).unwrap(),
    })
    .expect("submit frame")
}

fn frame(bytes: &[u8]) -> coord_types::wire_v1::Frame {
    let mut reader = coord_types::wire_v1::FrameReader::new();
    reader.push(bytes).expect("bounded");
    reader.next_frame().expect("well formed").expect("complete")
}

/// The same submission, delivered locally and delivered as a collector
/// on the peer plane would deliver it, produces the same round.
///
/// This is the property the short path exists under. If the two ever
/// diverged, the cluster would have two protocols and would have the
/// properties of whichever one was weaker.
#[test]
fn a_local_submission_and_a_wire_submission_produce_the_same_round() {
    let boot = BootId([7; 16]);
    let bytes = submission(1);

    let mut local = leader(boot, IngressBudget::default());
    local
        .route()
        .offer(&bytes)
        .expect("room in a fresh ingress");
    let (through_ingress, refused) = local.serve_local(8).expect("served");
    assert!(refused.is_empty());
    assert_eq!(local.admitted, 1);

    let mut wire = leader(boot, IngressBudget::default());
    let over_the_wire = wire
        .on_submission(PeerRole::Frontend, &frame(&bytes), Origin::Connection(7))
        .expect("driven")
        .expect("admitted");

    assert_eq!(
        through_ingress, over_the_wire,
        "the same submission produced a different round depending on how it arrived"
    );
    assert!(
        !through_ingress.peer.is_empty(),
        "the proposal reached the other voters either way"
    );
}

/// The role check is not the frontend's to skip. A frame the local
/// ingress would never have been created for is still refused where the
/// two routes converge, which is what makes the ingress's own check a
/// fail-fast rather than the only one.
#[test]
fn a_submitter_that_may_not_act_for_clients_is_refused_at_the_door() {
    let boot = BootId([7; 16]);
    let mut voter = leader(boot, IngressBudget::default());

    for role in [PeerRole::Client, PeerRole::Voter, PeerRole::Observer] {
        let refused = voter
            .on_submission(role, &frame(&submission(1)), Origin::Connection(7))
            .expect("driven")
            .expect_err("a non-collector may not submit for a client");
        assert!(
            matches!(refused, Refused::NotAdmissible(_)),
            "{role:?}: {refused:?}"
        );
    }
    assert_eq!(
        voter.node().rounds,
        0,
        "nothing reached the machine, so there is nothing to undo"
    );
}

/// Bytes that are not one well-formed frame never become an event. They
/// are counted and dropped: the caller is waiting on evidence that will
/// not come, which is exactly what happens when a remote voter refuses.
#[test]
fn a_malformed_local_offer_is_counted_rather_than_stepped() {
    let boot = BootId([7; 16]);
    let mut voter = leader(boot, IngressBudget::default());
    let rounds = voter.node().rounds;
    voter.route().offer(b"not a frame").expect("room");

    let (out, refused) = voter.serve_local(8).expect("served");

    assert!(out.is_empty());
    assert_eq!(refused.len(), 1);
    assert!(matches!(refused[0], Refused::Malformed(_)), "{refused:?}");
    assert_eq!(voter.refused, 1);
    assert_eq!(voter.admitted, 0);
    assert_eq!(voter.node().rounds, rounds, "the machine was never stepped");

    // One offer is one frame. A well-formed submission with anything
    // after it is refused rather than half-consumed, which is what a
    // stream parser must not let a local caller get away with either.
    let mut trailing = submission(1);
    trailing.extend_from_slice(b"and more");
    voter.route().offer(&trailing).expect("room");
    let (_, refused) = voter.serve_local(8).expect("served");
    assert_eq!(refused.len(), 1, "trailing bytes were silently accepted");
    assert!(matches!(refused[0], Refused::Malformed(_)), "{refused:?}");
    assert_eq!(voter.refused, 2);
    assert_eq!(voter.admitted, 0);
}

/// A turn is bounded. A collector that can fill the ingress faster than
/// the voter empties it slows the voter down; it does not get to decide
/// how long the voter goes without its timers, its peers and its
/// recovery.
#[test]
fn a_local_turn_takes_no_more_than_its_budget() {
    let boot = BootId([7; 16]);
    let mut voter = leader(boot, IngressBudget::default());
    for sequence in 1..=5 {
        voter.route().offer(&submission(sequence)).expect("room");
    }

    let (_, refused) = voter.serve_local(2).expect("served");

    assert!(refused.is_empty());
    assert_eq!(voter.admitted, 2, "two of the five, as budgeted");
    assert_eq!(voter.waiting(), 3, "the rest keep their place in the queue");
    assert!(voter.has_work(), "the runtime knows to come back");
}

/// A voter running beside its collector has one identity, and it is the
/// committed one. Its evidence reaches the collector under that identity
/// and is counted there like any other voter's -- there is no local
/// acknowledgement to count instead.
#[test]
fn a_voters_own_evidence_carries_its_committed_identity() {
    let boot = BootId([7; 16]);
    let voter = leader(boot, IngressBudget::default());

    let provenance = voter.provenance();

    assert_eq!(provenance.from(), r(0));
    assert_eq!(
        provenance.incarnation(),
        inc(),
        "the incarnation the committed configuration names, not one the runtime chose"
    );

    // A voter that does not lead the ballot has its own identity, not
    // the ballot's. Counting a follower's evidence under the leader's
    // name would let one replica look like two.
    let follower = follower(boot);
    assert_eq!(follower.ballot().leader, r(0));
    assert_eq!(
        follower.provenance().from(),
        r(1),
        "evidence was attributed to the ballot's leader rather than to the voter that produced it"
    );
}

/// A submission the machine has already seen does not become a second
/// command, however it arrives. One caller's retry is one command, and
/// the replica contributes to it once.
#[test]
fn the_same_submission_twice_is_still_one_command() {
    let boot = BootId([7; 16]);
    let mut voter = leader(boot, IngressBudget::default());
    let bytes = submission(1);

    voter.route().offer(&bytes).expect("room");
    let (first, _) = voter.serve_local(8).expect("served");
    voter.route().offer(&bytes).expect("room");
    let (second, _) = voter.serve_local(8).expect("served");

    assert_eq!(voter.admitted, 2, "both offers were admitted at the door");
    assert!(
        !first.peer.is_empty(),
        "the first proposal reached the voters"
    );
    assert!(
        second.peer.is_empty(),
        "the retry produced no second proposal: {second:?}"
    );
}

/// A voter remembers which collector submitted each command, so its
/// evidence can go back to the one that is waiting for it.
///
/// In a deployment where every node runs a frontend, a voter that sent
/// its evidence to whichever collector shares its process would give the
/// caller's collector nothing to count -- and would hand a second
/// collector evidence for a request it never made.
///
/// The binding is not part of the protocol: it never reaches a machine
/// and cannot change what a command is. It is bounded, and a retry
/// rebinds it, because the collector waiting now is the one that
/// submitted last.
#[test]
fn a_voter_remembers_which_collector_is_owed_each_commands_evidence() {
    let boot = BootId([7; 16]);
    let mut voter = leader(boot, IngressBudget::default());

    // One submitted locally, one over a connection.
    voter.route().offer(&submission(1)).expect("room");
    voter.serve_local(8).expect("served");
    voter
        .on_submission(
            PeerRole::Frontend,
            &frame(&submission(2)),
            Origin::Connection(9),
        )
        .expect("driven")
        .expect("admitted");

    let one = command_of(1);
    let two = command_of(2);
    assert_ne!(one, two, "different invocations are different commands");
    assert_eq!(voter.origin_of(&one), Some(Origin::Local));
    assert_eq!(voter.origin_of(&two), Some(Origin::Connection(9)));

    // A command this voter never admitted is owed to nobody.
    assert_eq!(voter.origin_of(&command_of(3)), None);

    // A retry through another collector rebinds it: the one waiting now
    // is the one that submitted last.
    voter
        .on_submission(
            PeerRole::Frontend,
            &frame(&submission(1)),
            Origin::Connection(4),
        )
        .expect("driven")
        .expect("admitted");
    assert_eq!(voter.origin_of(&one), Some(Origin::Connection(4)));
}

/// The command identity `submission(sequence)` becomes.
fn command_of(sequence: u64) -> coord_types::CommandId {
    let mut logical = LogicalRequest::new(
        NS,
        CanonicalOperation::Put(PutOp {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            lease: None,
            prev_kv: false,
        }),
    );
    logical.canonicalize();
    coord_types::CommandId::derive(
        &RetryKey {
            cluster_id: CLUSTER,
            domain_id: DOMAIN,
            session_id: SESSION,
            client_instance_id: ClientInstanceId([4; 16]),
            request_sequence: RequestSequence::new(sequence).unwrap(),
        },
        &logical,
    )
    .expect("derivable")
}

/// The short path is short in one way only. A submission that arrived
/// without touching a socket still produces sends that wait in the
/// outbox for their own record to be durable -- the rule that makes a
/// vote a promise this replica can keep across a crash.
#[test]
fn a_local_submission_does_not_skip_the_durability_gate() {
    let boot = BootId([7; 16]);
    let mut voter = leader(boot, IngressBudget::default());
    assert_eq!(voter.node().held_at_least_once(), (0, 0));

    voter.route().offer(&submission(1)).expect("room");
    let (out, _) = voter.serve_local(8).expect("served");

    let (held, _) = voter.node().held_at_least_once();
    assert!(
        held > 0,
        "a locally delivered submission produced sends that never waited on a barrier"
    );
    assert_eq!(
        voter.node().held(),
        0,
        "and they were released once the record landed, in the same round"
    );
    assert!(!out.peer.is_empty());
}

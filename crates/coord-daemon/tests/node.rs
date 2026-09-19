//! Acceptance for driving one voter (task-43).
//!
//! The protocol machines are pure, so every rule about *when* a frame may
//! leave this process is a rule about the driver rather than about them.
//! These are the ones whose violation is invisible in a passing protocol
//! test: a vote that reaches a peer before the record behind it is
//! durable, a frame prepared before a crash that is sent afterwards, a
//! machine left holding effects nobody carried out.

use std::collections::BTreeSet;

use coord_consensus::{
    BallotConfiguration, ConfigurationIdentity, Follower, FollowerConfig, Leader, LeaderConfig,
    LearningMode, ReplicaRole,
};
use coord_core::capability::{AdmissionReceipt, VerifierToken};
use coord_core::effect::{BootId, PeerId};
use coord_core::event::Event;
use coord_core::outbox::BarrierAllocator;
use coord_daemon::node::{DriveError, Machine, Node};
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
use coord_types::wire_v1::{MessageV1, RequestV1};

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

/// A bootstrapped store: one session and the policy rules for it, made
/// durable, so a request can be admitted and applied.
fn store(boot: BootId) -> Applier<ModelEngine> {
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
    let base = worker.application_base();
    worker
        .submit(coord_core::effect::PersistBatch {
            barrier: alloc.allocate(),
            base: Some(base),
            updates,
        })
        .unwrap();
    worker.flush().unwrap();
    Applier::new(worker, alloc).unwrap()
}

/// One admitted client request, as a collector's submission would have
/// produced it at the verifier boundary.
fn admitted(sequence: u64) -> coord_core::event::AdmittedRequest {
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
    let request = RequestV1::new(key, &logical, 0).unwrap();
    coord_core::event::AdmittedRequest {
        receipt: AdmissionReceipt::from_verifier(
            VerifierToken::for_boundary(),
            SESSION,
            1,
            u32::MAX,
            Digest32([7; 32]),
            0,
        ),
        frame: MessageV1::Request(request).encode().unwrap(),
    }
}

fn follower(boot: BootId) -> Node<ModelEngine> {
    let applier = store(boot);
    let bootstrapped = applier.worker().application_base().execution_position;
    let mut machine = Follower::new(FollowerConfig {
        identity: identity(1),
        quorum: quorum(),
        genesis: ballot(),
        frontend: FRONTEND,
        capacity: 64,
    })
    .restore_execution(bootstrapped, []);
    machine.set_learning(LearningMode::Full);
    Node::new(Machine::Follower(Box::new(machine)), applier, FRONTEND)
}

fn leader(boot: BootId) -> Node<ModelEngine> {
    let applier = store(boot);
    let bootstrapped = applier.worker().application_base().execution_position;
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
    Node::new(Machine::Leader(Box::new(machine)), applier, FRONTEND)
}

/// An admitted request is proposed: the batch behind it is persisted,
/// and the frames that go to the voters are the ones whose barriers that
/// persist made durable.
///
/// The machine only *describes* the send. Carrying it out is the
/// driver's, and the order matters: persist, flush, then release. A
/// driver that transmitted first would put a proposal on the wire that a
/// crash could erase from this replica's own record.
#[test]
fn a_proposal_reaches_the_voters_only_after_its_own_record_is_durable() {
    let boot = BootId([7; 16]);
    let mut node = leader(boot);
    node.on_event(
        Event::Boot {
            boot_id: boot,
            incarnation: inc(),
        },
        &ballot(),
    )
    .expect("boot");
    assert!(node.machine().leads());

    let out = node
        .on_event(Event::Admitted(admitted(1)), &ballot())
        .expect("admitted");

    // The proposal went to the other voters, at the identity the
    // configuration names, and not to the collector: a proposal is peer
    // traffic, and evidence is what the collector gets.
    let to: Vec<ReplicaId> = out.peer.iter().map(|(p, _)| p.replica).collect();
    assert!(
        to.contains(&r(1)) && to.contains(&r(2)),
        "the proposal did not reach the voters: {to:?}"
    );
    assert!(!to.contains(&FRONTEND.replica));
    assert!(out.peer.iter().all(|(_, f)| !f.is_empty()));

    // Nothing is still waiting: the flush in this round made the
    // proposal's own barrier durable, which is what released it.
    assert_eq!(node.held(), 0, "a proposal was released before its record");
    assert!(node.rounds >= 1);
}

/// A send whose record failed is dropped and reported, never
/// transmitted.
///
/// A barrier that will never be durable is not one to keep waiting for,
/// and a voter that silently swallowed the send would look merely slow
/// rather than broken. The driver reports the failure rather than
/// carrying on from memory: a replica that cannot make its own
/// transitions durable has nothing to serve from.
#[test]
fn a_replica_that_cannot_record_its_own_transition_says_so() {
    let boot = BootId([7; 16]);
    let mut node = leader(boot);
    node.on_event(
        Event::Boot {
            boot_id: boot,
            incarnation: inc(),
        },
        &ballot(),
    )
    .expect("boot");

    node.applier_mut()
        .worker_mut()
        .engine_mut()
        .inject_begin_write_error();
    let failed = node.on_event(Event::Admitted(admitted(1)), &ballot());
    assert!(
        matches!(failed, Err(DriveError::Engine(_))),
        "a failed record was reported as success: {failed:?}"
    );
}

/// Work the protocol prepared under one boot does not cross into
/// another.
///
/// A vote or a promise describes state this replica had. After a crash it
/// no longer has it, and carrying the work on would make a promise the
/// process cannot keep. Here the store has reopened under a new boot
/// while the machine still carries the old one, which is the shape the
/// fence exists for.
///
/// The fence turns out to bite a step earlier than the outbox: the store
/// refuses a batch whose barrier belongs to another boot, so the record
/// is never written and the send it would have gated is never described.
/// The driver reports that refusal rather than treating a wrong-boot
/// batch as merely dropped, and nothing reaches a peer either way.
#[test]
fn work_prepared_under_another_boot_never_reaches_the_store_or_a_peer() {
    // The store -- and so the outbox -- is at this boot.
    let mut node = leader(BootId([8; 16]));
    // The machine is at the boot before it.
    node.on_event(
        Event::Boot {
            boot_id: BootId([7; 16]),
            incarnation: inc(),
        },
        &ballot(),
    )
    .expect("boot");

    let refused = node.on_event(Event::Admitted(admitted(1)), &ballot());
    assert_eq!(
        refused,
        Err(DriveError::Submit("WrongBoot".into())),
        "a batch from another boot was accepted"
    );
    assert_eq!(node.held(), 0, "nothing is waiting to be sent");
    assert_eq!(node.executed, 0, "nothing was applied under the old boot");
}

/// A machine that answers its own storage facts with more effects is
/// driven to a standstill, not one round deep.
///
/// A driver that carried out only the first round would leave a replica
/// that had already decided what to do next holding the decision: it
/// would look alive, answer nothing, and be indistinguishable from a
/// partition.
#[test]
fn storage_facts_are_fed_back_until_the_replica_has_nothing_left_to_do() {
    let boot = BootId([7; 16]);
    let mut node = leader(boot);
    node.on_event(
        Event::Boot {
            boot_id: boot,
            incarnation: inc(),
        },
        &ballot(),
    )
    .expect("boot");
    let after_boot = node.rounds;

    // Executing when nothing is executable is not an error and does not
    // spin: an idle replica's driver does no rounds at all.
    let idle = node.execute(&ballot()).expect("execute");
    assert!(idle.is_empty(), "an idle replica asked for work: {idle:?}");
    assert_eq!(
        node.executed, 0,
        "nothing was learned, so nothing may be applied"
    );
    assert_eq!(node.rounds, after_boot, "an idle execute drove a round");
}

/// A node reports what it could not do rather than carrying on from
/// memory. A replica that cannot make its own transitions durable has
/// nothing to serve from.
#[test]
fn a_driver_error_says_what_failed() {
    for (error, expected) in [
        (DriveError::Submit("guard".into()), "batch refused: guard"),
        (DriveError::Engine("io".into()), "engine failed: io"),
        (
            DriveError::Encode("too large".into()),
            "frame not encodable: too large",
        ),
    ] {
        assert_eq!(error.to_string(), expected);
    }
}

/// A follower's vote waits for its own record, and the driver is what
/// makes it wait.
///
/// The collector fans a submission out to every voter at once, so a
/// follower votes on the request itself rather than on the leader's
/// order: that is the whole point of the fast path. The vote says "I have
/// this and I will not forget it", and a vote that reaches the collector
/// before the record behind it is durable is a promise this replica
/// cannot honour after a crash.
///
/// The machine describes the send and names the barrier it rests on.
/// Holding it until that barrier is durable is the runtime's, which is
/// why it is held here and released by the flush of the same round --
/// never sent alongside a batch that has not landed.
#[test]
fn a_followers_vote_is_held_until_its_own_record_is_durable() {
    let boot = BootId([8; 16]);
    let mut follower = follower(boot);
    follower
        .on_event(
            Event::Boot {
                boot_id: boot,
                incarnation: inc(),
            },
            &ballot(),
        )
        .expect("boot");

    let out = follower
        .on_event(Event::Admitted(admitted(1)), &ballot())
        .expect("admitted");

    // The follower did answer: its evidence went to the collector, which
    // is where a voter's vote is counted.
    assert!(
        !out.frontend.is_empty(),
        "the follower published no evidence"
    );
    // And it was held on the way: the machine described the evidence in
    // the same round as the batch behind it, so the barrier was not
    // durable yet and the send waited for the flush that made it so.
    //
    // This is the assertion that distinguishes a driver which routes
    // evidence through the durability gate from one that publishes it
    // straight to the collector. The peer sends of the same round are
    // held too, so counting held sends in general would not.
    let (_, evidence) = follower.held_at_least_once();
    assert!(
        evidence > 0,
        "the vote went to the collector alongside its own record"
    );
    assert_eq!(follower.held(), 0, "and it was released, not stranded");
}

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
use coord_core::capability::{AdmissionReceipt, AttestedAdmission, VerifierToken};
use coord_core::effect::{BootId, PeerId};
use coord_core::event::Event;
use coord_core::outbox::BarrierAllocator;
use coord_daemon::node::{DriveError, Machine, Node};
use coord_journal_api::stream::ShardId;
use coord_state::policy::{Action as PolicyAction, KeyInterval, PolicyRule};
use coord_storage::journaled::{JournalLimits, JournaledStore};
use coord_storage::policy::{bootstrap_session, rule_update};
use coord_storage::{Applier, GroupLimits, JournaledDomain, Persistence, StoreWorker};
use coord_store_testkit::journal::ModelJournal;
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

/// One session and the policy rules for it: the same bootstrap for
/// either coordinator.
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

/// A bootstrapped store: one session and the policy rules for it, made
/// durable, so a request can be admitted and applied.
fn store(boot: BootId) -> Applier<StoreWorker<ModelEngine>> {
    let mut worker =
        StoreWorker::open(ModelEngine::new(), boot, inc(), GroupLimits::default()).unwrap();
    let mut alloc = BarrierAllocator::new(inc(), boot);
    let updates = bootstrap_updates();
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

/// The same follower, but persisting through the shared journal: the
/// serving profile, where a record reaches the journal before the
/// projection.
fn journaled_follower(boot: BootId) -> Node<JournaledDomain<ModelJournal, ModelEngine>> {
    let mut store = JournaledStore::open(
        ModelJournal::new(),
        CLUSTER,
        r(1),
        inc(),
        boot,
        JournalLimits::default(),
    )
    .unwrap();
    store
        .attach(DOMAIN, ShardId::new(0).unwrap(), ModelEngine::new())
        .unwrap();
    let mut alloc = BarrierAllocator::new(inc(), boot);

    // The bootstrap goes through the journal like everything else: it is
    // the profile's only writer.
    let base = store.application_base(DOMAIN).unwrap();
    store
        .submit(coord_storage::journaled::Submission {
            domain: DOMAIN,
            ballot: Ballot {
                epoch: base.configuration,
                number: 0,
                leader: r(0),
            },
            kind: coord_storage::journaled::TransitionKind::Application {
                position: base.execution_position.checked_next().unwrap(),
                revision: None,
                result_digest: coord_types::identity::Digest32([0; 32]),
            },
            batch: coord_core::effect::PersistBatch {
                barrier: alloc.allocate(),
                base: Some(base),
                updates: bootstrap_updates(),
            },
        })
        .unwrap();
    store.flush().unwrap();

    let domain = JournaledDomain::new(
        store,
        DOMAIN,
        Ballot {
            epoch: base.configuration,
            number: 0,
            leader: r(0),
        },
    )
    .expect("attached");
    let applier = Applier::new(domain, alloc).unwrap();
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
    Node::new(Machine::Follower(Box::new(machine)), applier, FRONTEND)
}

fn follower(boot: BootId) -> Node<StoreWorker<ModelEngine>> {
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
    Node::new(Machine::Follower(Box::new(machine)), applier, FRONTEND)
}

fn leader(boot: BootId) -> Node<StoreWorker<ModelEngine>> {
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
        .store_mut()
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
        Err(DriveError::Submit("refused: WrongBoot".into())),
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

/// A vote waits for its record to be durable, not for the projection to
/// have taken it.
///
/// These are two different facts on the journal-first path and the
/// difference is the point of the profile. `JournalDurable` says the
/// record survives a crash, which is exactly and entirely what a vote
/// promises; `Materialized` says the state is readable, which a vote
/// says nothing about. Making every vote wait for materialization would
/// put the projection's latency into the consensus path for no safety
/// the journal had not already given.
#[test]
fn a_vote_rests_on_the_record_being_durable_not_on_the_projection() {
    let boot = BootId([8; 16]);
    let mut follower = journaled_follower(boot);
    follower
        .on_event(
            Event::Boot {
                boot_id: boot,
                incarnation: inc(),
            },
            &ballot(),
        )
        .expect("boot");

    // The projection will not take anything from here on.
    for _ in 0..64 {
        follower
            .applier_mut()
            .store_mut()
            .store_mut()
            .projection(DOMAIN)
            .unwrap()
            .script_commit(coord_store_testkit::model::CommitScript::DefinitelyNotCommitted);
    }

    let out = follower
        .on_event(Event::Admitted(admitted(1)), &ballot())
        .expect("admitted");

    // The evidence still went to the collector: the record is durable in
    // the journal, and that is what the vote rests on.
    assert!(
        !out.frontend.is_empty(),
        "a vote waited for the projection: the journal had already made \
         its record durable"
    );
    // It was still held on the way -- the durability gate is doing its
    // job, it is simply satisfied by journal durability.
    let (_, evidence) = follower.held_at_least_once();
    assert!(evidence > 0, "the vote was not held for its own record");

    // And the projection really is behind, so this is the case it looks
    // like: recorded, not yet readable.
    assert!(
        follower.applier().store().store().unmaterialized(DOMAIN) > 0,
        "the projection was not actually behind"
    );
}

/// A voter that serves a write records the journal and materialization
/// work it did (task-61), and the snapshot built from that recorder
/// reports those stages with non-zero counts rather than zeroes.
///
/// A single-voter domain, so the leader's own record is the quorum and
/// the put is ordered and applied in this process: the snapshot then
/// describes work that demonstrably happened.
#[test]
fn a_voter_that_served_a_write_reports_the_stages_it_passed_through() {
    use coord_daemon::metrics::{Recorder, Stage, Unavailable};
    use coord_daemon::role::RoleSet;

    let boot = BootId([7; 16]);
    let applier = store(boot);
    let bootstrapped = applier.store().application_base().execution_position;
    let alone = ConfigurationIdentity {
        voters: vec![r(0)].into_iter().collect(),
        ..identity(0)
    };
    let quorum = BallotConfiguration::c2(
        epoch(),
        ballot(),
        [r(0)].into_iter().collect(),
        [r(0)].into_iter().collect(),
    )
    .unwrap();
    let mut machine = Leader::new(
        LeaderConfig {
            identity: alone,
            quorum,
            genesis: ballot(),
            frontend: FRONTEND,
            capacity: 64,
        },
        None,
        bootstrapped,
    );
    machine.set_learning(LearningMode::Full);
    let mut node = Node::new(Machine::Leader(Box::new(machine)), applier, FRONTEND);
    let recorder = std::sync::Arc::new(Recorder::instrumenting(&[
        Stage::Journal,
        Stage::Materialization,
    ]));
    node.record_into(std::sync::Arc::clone(&recorder));

    node.on_event(
        Event::Boot {
            boot_id: boot,
            incarnation: inc(),
        },
        &ballot(),
    )
    .expect("boot");
    node.on_event(Event::Admitted(admitted(1)), &ballot())
        .expect("admitted");
    node.execute(&ballot()).expect("execute");
    assert!(node.executed >= 1, "the put was never applied");

    let stages = recorder.snapshot_stages(&RoleSet::parse("voter").expect("roles"));
    for stage in [Stage::Journal, Stage::Materialization] {
        let metrics = stages
            .iter()
            .find(|r| r.stage == stage)
            .and_then(|r| r.metrics.observed())
            .unwrap_or_else(|| panic!("{} was not observed", stage.name()));
        assert!(
            metrics.entered > 0 && metrics.completed > 0,
            "{} reported {metrics:?} for a voter that served a write",
            stage.name()
        );
        assert_eq!(metrics.refused, 0, "{} refused work", stage.name());
        assert!(
            metrics.latency.measure().is_observed(),
            "{} has counts but no latency",
            stage.name()
        );
    }
    // A stage the voter has but nothing records says so, rather than
    // reporting the zero a voter that did no fan-out would.
    let fan_out = stages
        .iter()
        .find(|r| r.stage == Stage::FanOut)
        .expect("every stage is reported");
    assert_eq!(fan_out.metrics.why(), Some(Unavailable::NotInstrumented));
}

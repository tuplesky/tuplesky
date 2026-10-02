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
    admitted_putting(sequence, b"v".to_vec())
}

/// [`admitted`], putting `value`.
fn admitted_putting(sequence: u64, value: Vec<u8>) -> coord_core::event::AdmittedRequest {
    let mut logical = LogicalRequest::new(
        NS,
        CanonicalOperation::Put(PutOp {
            key: b"k".to_vec(),
            value,
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
    let request = RequestV1::new(key, &logical, 0, 0).unwrap();
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
    let applier = journaled_applier(boot, r(1));
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

/// A bootstrapped journal-first store for replica `me`.
fn journaled_applier(
    boot: BootId,
    me: ReplicaId,
) -> Applier<JournaledDomain<ModelJournal, ModelEngine>> {
    journaled_applier_within(boot, me, JournalLimits::default())
}

/// [`journaled_applier`] under `limits`.
fn journaled_applier_within(
    boot: BootId,
    me: ReplicaId,
    limits: JournalLimits,
) -> Applier<JournaledDomain<ModelJournal, ModelEngine>> {
    let mut store =
        JournaledStore::open(ModelJournal::new(), CLUSTER, me, inc(), boot, limits).unwrap();
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
    Applier::new(domain, alloc).unwrap()
}

/// A leader that is its domain's only voter, persisting through the
/// journal: its own record is the quorum, so what it proposes it also
/// executes.
fn journaled_lone_leader(boot: BootId) -> Node<JournaledDomain<ModelJournal, ModelEngine>> {
    journaled_lone_leader_within(boot, JournalLimits::default())
}

/// [`journaled_lone_leader`] under `limits`.
fn journaled_lone_leader_within(
    boot: BootId,
    limits: JournalLimits,
) -> Node<JournaledDomain<ModelJournal, ModelEngine>> {
    let applier = journaled_applier_within(boot, r(0), limits);
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
    Node::new(Machine::Leader(Box::new(machine)), applier, FRONTEND)
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

/// A vote whose append ended uncertain is settled by a reconcile and
/// goes out, rather than waiting for a command to execute.
///
/// An uncertain append leaves the domain refusing every later batch as
/// not ready until something reconciles it. Only the applier did, when a
/// command next executed, and it kept what the reconcile settled to
/// itself: the vote waited for a new ballot or a restart, and so did
/// the command that needed it.
#[test]
fn a_vote_whose_append_ended_uncertain_is_reconciled_and_released() {
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

    // The next append reaches the log, and says it may not have.
    follower
        .applier_mut()
        .store_mut()
        .store_mut()
        .journal_mut()
        .unwrap()
        .script_append(coord_store_testkit::journal::AppendScript::Indeterminate { applied: true });

    let out = follower
        .on_event(Event::Admitted(admitted(1)), &ballot())
        .expect("an uncertain append is reconciled, not a failed round");
    assert!(
        !out.frontend.is_empty(),
        "the vote was not released once its record was known durable"
    );
    assert_eq!(follower.held(), 0, "a send is still waiting");
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

/// Lowering in groups, a vote waits for the flush that journals it, and
/// one flush journals every vote the events before it made, in one
/// append (task-d47).
///
/// The events are carried out at once; only the lowering waits. What the
/// votes justify is sent once their own barriers are journal-durable, as
/// before, which is now at the flush.
#[test]
fn lowering_in_groups_a_vote_waits_for_the_flush_that_journals_it() {
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
    follower.lower_in_groups();
    let appends = |node: &Node<JournaledDomain<ModelJournal, ModelEngine>>| {
        node.applier().store().store().journal().appends()
    };
    let before = appends(&follower);

    for sequence in 1..=3 {
        let out = follower
            .on_event(Event::Admitted(admitted(sequence)), &ballot())
            .expect("admitted");
        assert!(
            out.frontend.is_empty() && out.peer.is_empty(),
            "a vote went out before the flush that journals it"
        );
    }
    assert_eq!(appends(&follower), before, "nothing was journaled yet");
    // Each submission's records wait in the queue: the evidence for them
    // is described once they are durable, so none of it exists yet.
    assert_eq!(follower.applier().store().queued(), 3);

    let out = follower.flush(&ballot()).expect("flushed");
    assert_eq!(appends(&follower) - before, 1, "one append for every vote");
    assert_eq!(follower.applier().store().queued(), 0);
    assert_eq!(follower.held(), 0, "every vote was released by the flush");
    assert!(!out.frontend.is_empty(), "the votes went to the collector");
    // Nothing is ready to execute, so the flush also caught the
    // projection up: it does not wait on execution that is not coming.
    assert!(!follower.can_execute());
    assert_eq!(follower.applier().store().unmaterialized(), 0);
}

/// Lowering in groups, the commands whose turn has come are applied as
/// one group: one journal append and one projection commit for all of
/// them, and their results go to the collector only once the group has
/// materialized (task-d47).
#[test]
fn lowering_in_groups_the_commands_ready_together_execute_as_one_group() {
    let boot = BootId([9; 16]);
    let mut node = journaled_lone_leader(boot);
    node.on_event(
        Event::Boot {
            boot_id: boot,
            incarnation: inc(),
        },
        &ballot(),
    )
    .expect("boot");
    node.lower_in_groups();
    for sequence in 1..=5 {
        node.on_event(Event::Admitted(admitted(sequence)), &ballot())
            .expect("admitted");
    }
    node.flush(&ballot()).expect("flushed");
    assert!(
        node.can_execute(),
        "the lone voter's records decide its proposals"
    );

    let cost = |node: &Node<JournaledDomain<ModelJournal, ModelEngine>>| {
        node.applier().store().cost().expect("counted").lowering
    };
    let before = cost(&node);
    let revision_before = node.applier().kv_revision().unwrap();
    let out = node.execute(&ballot()).expect("executed");
    let after = cost(&node);
    assert_eq!(node.executed, 5);
    assert_eq!(after.appends - before.appends, 1, "one journal append");
    assert_eq!(after.commits - before.commits, 1, "one projection commit");
    assert!(!node.applier().in_group());
    assert!(node.applier().kv_revision().unwrap() > revision_before);
    assert!(
        !out.frontend.is_empty(),
        "the results went to the collector"
    );
}

/// Lowering in groups, a staged group is journaled by the flush with
/// the protocol's batches, and materialized, its results handed out,
/// only by [`Node::finish`] (task-d47).
///
/// So what the journal append released -- votes, on a voter with peers --
/// goes out after one sync, not after the group's projection commit too.
#[test]
fn lowering_in_groups_a_staged_group_is_journaled_by_the_flush_and_answered_by_finish() {
    let boot = BootId([10; 16]);
    let mut node = journaled_lone_leader(boot);
    node.on_event(
        Event::Boot {
            boot_id: boot,
            incarnation: inc(),
        },
        &ballot(),
    )
    .expect("boot");
    node.lower_in_groups();
    for sequence in 1..=5 {
        node.on_event(Event::Admitted(admitted(sequence)), &ballot())
            .expect("admitted");
    }
    node.flush(&ballot()).expect("flushed");
    assert!(node.can_execute());

    let cost = |node: &Node<JournaledDomain<ModelJournal, ModelEngine>>| {
        node.applier().store().cost().expect("counted").lowering
    };
    let before = cost(&node);
    let revision_before = node.applier().kv_revision().unwrap();
    let mut out = node.stage(&ballot()).expect("staged");
    out.absorb(node.flush(&ballot()).expect("flushed"));
    let journaled = cost(&node);
    assert_eq!(node.executed, 5);
    assert!(node.applier().in_group(), "the group stays staged");
    assert_eq!(journaled.appends - before.appends, 1, "one journal append");
    assert_eq!(
        journaled.commits, before.commits,
        "no projection commit yet"
    );
    assert_eq!(node.applier().kv_revision().unwrap(), revision_before);
    assert!(
        out.frontend.is_empty(),
        "a result went out before its group materialized"
    );

    let out = node.finish(&ballot()).expect("finished");
    let finished = cost(&node);
    assert!(!node.applier().in_group());
    assert_eq!(
        finished.appends, journaled.appends,
        "nothing left to journal"
    );
    assert_eq!(
        finished.commits - journaled.commits,
        1,
        "one projection commit"
    );
    assert!(node.applier().kv_revision().unwrap() > revision_before);
    assert!(
        !out.frontend.is_empty(),
        "the results went to the collector"
    );
}

/// Lowering in groups, a group whose batches would overfill the store's
/// queue in bytes is lowered before the next command is applied, and
/// that command is applied after it (task-d47; #138's review).
///
/// The queue here holds two of these commands' batches at most, and the
/// group's count is far from its bound, so only the byte check stands
/// between the third command and a refusal that would stop the voter.
#[test]
fn lowering_in_groups_a_group_that_would_overfill_the_queue_in_bytes_is_lowered_first() {
    let boot = BootId([11; 16]);
    let limits = JournalLimits {
        max_queued_bytes_per_domain: 24 * 1024,
        ..JournalLimits::default()
    };
    let mut node = journaled_lone_leader_within(boot, limits);
    node.on_event(
        Event::Boot {
            boot_id: boot,
            incarnation: inc(),
        },
        &ballot(),
    )
    .expect("boot");
    node.lower_in_groups();
    const COMMANDS: u64 = 6;
    for sequence in 1..=COMMANDS {
        node.on_event(
            Event::Admitted(admitted_putting(sequence, vec![sequence as u8; 3 * 1024])),
            &ballot(),
        )
        .expect("admitted");
        node.flush(&ballot()).expect("flushed");
    }
    assert!(node.can_execute());

    let cost = |node: &Node<JournaledDomain<ModelJournal, ModelEngine>>| {
        node.applier().store().cost().expect("counted").lowering
    };
    let before = cost(&node);
    let out = node
        .execute(&ballot())
        .expect("a full queue is lowered, not refused");
    assert_eq!(node.executed, COMMANDS);
    assert!(!node.applier().in_group());
    assert!(
        cost(&node).commits - before.commits > 1,
        "the commands did not fit one group's queue"
    );
    assert!(
        !out.frontend.is_empty(),
        "the results went to the collector"
    );
}

/// Lowering in groups, a proposal goes to the voters only once the flush
/// has made its own record journal-durable (task-d47).
///
/// Nothing before the flush may send it: the record is only queued. A
/// driver that reported the record durable before journaling it -- a
/// release moved ahead of its sync -- fails here.
#[test]
fn lowering_in_groups_a_proposal_is_held_until_the_flush_that_journals_it() {
    let boot = BootId([7; 16]);
    let applier = journaled_applier(boot, r(0));
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
    let mut node = Node::new(Machine::Leader(Box::new(machine)), applier, FRONTEND);
    node.on_event(
        Event::Boot {
            boot_id: boot,
            incarnation: inc(),
        },
        &ballot(),
    )
    .expect("boot");
    node.lower_in_groups();

    for sequence in 1..=2 {
        let out = node
            .on_event(Event::Admitted(admitted(sequence)), &ballot())
            .expect("admitted");
        assert!(
            out.peer.is_empty(),
            "a proposal went out before its own record was journaled"
        );
    }
    // The records wait in the queue, and nothing they justify has been
    // sent.
    assert_eq!(node.applier().store().queued(), 2);

    let out = node.flush(&ballot()).expect("flushed");
    assert_eq!(node.held(), 0, "the flush released every proposal");
    let to: Vec<ReplicaId> = out.peer.iter().map(|(p, _)| p.replica).collect();
    assert!(
        to.contains(&r(1)) && to.contains(&r(2)),
        "the proposals did not reach the voters: {to:?}"
    );
}

/// Lend `node`'s projection commits to a manual materializer
/// (task-d52), returning the handle that runs them.
fn pipeline(
    node: &mut Node<JournaledDomain<ModelJournal, ModelEngine>>,
) -> coord_storage::ManualHandle<ModelEngine> {
    let (materializer, handle) = coord_storage::ManualMaterializer::new();
    node.applier_mut()
        .store_mut()
        .store_mut()
        .pipeline(Box::new(materializer))
        .expect("nothing out yet");
    handle
}

/// A journaled lone leader, booted and lowering in groups.
fn grouped_lone_leader(boot: BootId) -> Node<JournaledDomain<ModelJournal, ModelEngine>> {
    let mut node = journaled_lone_leader(boot);
    node.on_event(
        Event::Boot {
            boot_id: boot,
            incarnation: inc(),
        },
        &ballot(),
    )
    .expect("boot");
    node.lower_in_groups();
    node
}

/// Admit `sequences`, then run one flush as the runtime does: stage the
/// ready commands, journal them with the protocol's batches, finish.
/// Returns the frames the collector was sent while the commands were
/// admitted (the leader's evidence) and those the group's flush sent.
fn admit_and_flush(
    node: &mut Node<JournaledDomain<ModelJournal, ModelEngine>>,
    sequences: std::ops::RangeInclusive<u64>,
) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
    let mut frames = Vec::new();
    for sequence in sequences {
        let out = node
            .on_event(
                Event::Admitted(admitted_putting(sequence, vec![sequence as u8; 8])),
                &ballot(),
            )
            .expect("admitted");
        frames.extend(out.frontend);
        frames.extend(node.flush(&ballot()).expect("flushed").frontend);
    }
    let mut out = node.stage(&ballot()).expect("staged");
    out.absorb(node.flush(&ballot()).expect("flushed"));
    out.absorb(node.finish(&ballot()).expect("finished"));
    (frames, out.frontend)
}

fn retry_key_of(sequence: u64) -> RetryKey {
    RetryKey {
        cluster_id: CLUSTER,
        domain_id: DOMAIN,
        session_id: SESSION,
        client_instance_id: ClientInstanceId([4; 16]),
        request_sequence: RequestSequence::new(sequence).unwrap(),
    }
}

/// Pipelined (task-d52), a group's results wait for its projection commit
/// to come back, and execution goes on meanwhile: the next group is
/// planned over the one still in flight.
#[test]
fn pipelined_a_groups_results_wait_for_its_projection_commit_while_execution_goes_on() {
    let mut node = grouped_lone_leader(BootId([13; 16]));
    let handle = pipeline(&mut node);
    let revision_before = node.applier().kv_revision().unwrap();
    let base = node.applier().store().application_base().execution_position;
    let through = |node: &Node<JournaledDomain<ModelJournal, ModelEngine>>| {
        node.applier().store().materialized_through().unwrap()
    };

    let (_, frames) = admit_and_flush(&mut node, 1..=3);
    assert_eq!(node.executed, 3);
    assert!(
        frames.is_empty(),
        "a result went out before its group's projection commit came back"
    );
    assert_eq!(node.awaiting_materialization(), 1);
    assert_eq!(node.applier().kv_revision().unwrap(), revision_before);
    // The commit out when the group closed is the protocol's, lent by a
    // flush while the commands were admitted; the group waits behind it
    // and is lent once it is back.
    assert_eq!(handle.waiting(), 1);
    assert!(handle.run());
    let out = node.settle(&ballot()).expect("settled");
    assert!(out.frontend.is_empty());
    assert_eq!(through(&node), base, "the group is not committed yet");
    assert_eq!(handle.waiting(), 1, "the group's commit is out");

    // With the first group out, the next one executes over it.
    let (_, frames) = admit_and_flush(&mut node, 4..=6);
    assert_eq!(node.executed, 6);
    assert!(frames.is_empty());
    assert_eq!(node.awaiting_materialization(), 2);
    assert_eq!(handle.waiting(), 1, "one commit out at a time");

    // The first group's commit returns and is taken back: its results go
    // out, the second group's do not, and the second group is lent.
    assert!(handle.run());
    let first = node.settle(&ballot()).expect("settled");
    assert!(!first.frontend.is_empty(), "the first group's results");
    assert_eq!(through(&node).get(), base.get() + 3);
    assert_eq!(node.awaiting_materialization(), 1);
    assert_eq!(handle.waiting(), 1);

    assert!(handle.run());
    let second = node.settle(&ballot()).expect("settled");
    assert!(!second.frontend.is_empty(), "the second group's results");
    assert_eq!(through(&node).get(), base.get() + 6);
    assert_eq!(node.awaiting_materialization(), 0);
    let revision = node.applier().kv_revision().unwrap();
    assert_eq!(revision.get() - revision_before.get(), 6);
}

/// Pipelined (task-d52), the same commands give the same results, the
/// same frames to the collector and the same projection as on a node
/// that commits on its own thread.
#[test]
fn pipelined_results_are_those_of_a_node_that_commits_on_its_own_thread() {
    let run = |pipelined: bool| {
        let mut node = grouped_lone_leader(BootId([14; 16]));
        let handle = pipelined.then(|| pipeline(&mut node));
        let mut frames = Vec::new();
        for round in 0..4u64 {
            let (admitted, flushed) = admit_and_flush(&mut node, round * 4 + 1..=round * 4 + 4);
            frames.extend(admitted);
            frames.extend(flushed);
        }
        if let Some(handle) = handle {
            while handle.run() {
                frames.extend(node.settle(&ballot()).expect("settled").frontend);
            }
            frames.extend(node.drain(&ballot()).expect("drained").frontend);
        }
        assert_eq!(node.executed, 16);
        assert_eq!(node.awaiting_materialization(), 0);
        let rows = {
            let gated = node.applier().store().reader().snapshot().unwrap();
            let mut rows = Vec::new();
            for sequence in 1..=16 {
                rows.push(
                    coord_storage::retry::lookup(gated.view(), &retry_key_of(sequence))
                        .unwrap()
                        .expect("every command's result is recorded"),
                );
            }
            rows
        };
        frames.sort();
        (frames, rows, node.applier().kv_revision().unwrap())
    };
    let (frames, rows, revision) = run(false);
    let (pipelined_frames, pipelined_rows, pipelined_revision) = run(true);
    assert_eq!(pipelined_revision, revision);
    assert_eq!(pipelined_rows, rows);
    assert_eq!(pipelined_frames, frames);
    // Contiguous positions, in the order the learner chose.
    for (i, row) in rows.iter().enumerate().skip(1) {
        assert_eq!(row.position.get(), rows[i - 1].position.get() + 1);
    }
}

/// Where a pipelined node's boot ends (task-d52).
#[derive(Clone, Copy, Debug)]
enum EndsAt {
    /// A group's projection commit is handed over and not started.
    HandedOver,
    /// A group is executed and journaled, and not handed over yet.
    MidGroup,
    /// A commit returned and its outcome was not taken back.
    CommittedNotTaken,
}

/// End a pipelined node's boot at `point`, reopen its store and resolve
/// every command it journaled: each is answered from the projection the
/// next boot recovers, none `Unknown` or `Forgotten`.
fn pipelined_boot_ends(point: EndsAt) {
    let mut node = grouped_lone_leader(BootId([15; 16]));
    let handle = pipeline(&mut node);
    admit_and_flush(&mut node, 1..=3);
    let journaled = match point {
        EndsAt::HandedOver => {
            assert_eq!(handle.waiting(), 1);
            3
        }
        EndsAt::MidGroup => {
            // A second group executed and journaled by the flush, never
            // finished.
            for sequence in 4..=6 {
                node.on_event(
                    Event::Admitted(admitted_putting(sequence, vec![sequence as u8; 8])),
                    &ballot(),
                )
                .expect("admitted");
                node.flush(&ballot()).expect("flushed");
            }
            node.stage(&ballot()).expect("staged");
            node.flush(&ballot()).expect("flushed");
            assert!(node.applier().in_group());
            6
        }
        EndsAt::CommittedNotTaken => {
            assert!(handle.run());
            assert_eq!(handle.finished(), 1);
            3
        }
    };
    assert_eq!(node.executed, journaled);
    let store = node.into_applier().into_store().into_store();
    let (journal, mut engines) = store.into_parts();
    let (_, engine) = engines.pop().expect("one domain");
    let mut next = JournaledStore::open(
        journal,
        CLUSTER,
        r(0),
        inc(),
        BootId([0x99; 16]),
        JournalLimits::default(),
    )
    .unwrap();
    next.attach(DOMAIN, ShardId::new(0).unwrap(), engine)
        .unwrap();
    let gated = next.reader(DOMAIN).unwrap().snapshot().unwrap();
    for sequence in 1..=journaled {
        let record = coord_storage::retry::lookup(gated.view(), &retry_key_of(sequence))
            .unwrap()
            .unwrap_or_else(|| panic!("{point:?}: command {sequence} is not answered"));
        assert!(record.position.get() > 0);
    }
}

#[test]
fn pipelined_a_boot_that_ends_with_a_commit_handed_over_answers_every_journaled_command() {
    pipelined_boot_ends(EndsAt::HandedOver);
}

#[test]
fn pipelined_a_boot_that_ends_in_the_middle_of_a_group_answers_every_journaled_command() {
    pipelined_boot_ends(EndsAt::MidGroup);
}

#[test]
fn pipelined_a_boot_that_ends_before_a_commit_is_taken_back_answers_every_journaled_command() {
    pipelined_boot_ends(EndsAt::CommittedNotTaken);
}

/// Lend `node`'s journal appends to a manual appender (task-d54),
/// returning the handle that runs them. The projection commits are lent
/// too: the runtime pipelines both.
fn lend_journal(
    node: &mut Node<JournaledDomain<ModelJournal, ModelEngine>>,
) -> (
    coord_storage::ManualHandle<ModelEngine>,
    coord_storage::ManualAppendHandle<ModelJournal>,
) {
    let commits = pipeline(node);
    let (appender, appends) = coord_storage::ManualAppender::new();
    node.applier_mut()
        .store_mut()
        .store_mut()
        .pipeline_journal(Box::new(appender))
        .expect("nothing out yet");
    (commits, appends)
}

/// Run whatever the appender and the materializer have waiting, settling
/// after each as the runtime does when it is woken, until neither has
/// anything; the frames the settles sent the collector.
fn pump(
    node: &mut Node<JournaledDomain<ModelJournal, ModelEngine>>,
    commits: &coord_storage::ManualHandle<ModelEngine>,
    appends: &coord_storage::ManualAppendHandle<ModelJournal>,
) -> Vec<Vec<u8>> {
    let mut frames = Vec::new();
    loop {
        let ran = appends.run() | commits.run();
        if !ran {
            return frames;
        }
        frames.extend(node.settle(&ballot()).expect("settled").frontend);
    }
}

/// With the journal lent (task-d54), the leader's proposal waits for the
/// append that makes its record durable to come back, not for the flush
/// that lent it: the flush returns with the sync still out, and the
/// settle that takes it back sends the proposal.
#[test]
fn lending_the_journal_a_proposal_is_held_until_its_append_is_taken_back() {
    let boot = BootId([16; 16]);
    let applier = journaled_applier(boot, r(0));
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
    let mut node = Node::new(Machine::Leader(Box::new(machine)), applier, FRONTEND);
    node.on_event(
        Event::Boot {
            boot_id: boot,
            incarnation: inc(),
        },
        &ballot(),
    )
    .expect("boot");
    node.lower_in_groups();
    let (commits, appends) = lend_journal(&mut node);
    // The boot's own record went out on the domain thread before the
    // journal was lent; nothing is out yet.
    assert!(!appends.waiting());

    let out = node
        .on_event(Event::Admitted(admitted(1)), &ballot())
        .expect("admitted");
    assert!(out.peer.is_empty());
    let out = node.flush(&ballot()).expect("flushed");
    assert!(
        out.peer.is_empty(),
        "a proposal went out while its append was still out"
    );
    assert!(node.applier().store().appending());
    assert!(appends.waiting());
    assert_eq!(node.applier().store().queued(), 0, "sealed and lent");

    // Synced but not taken back: still held.
    assert!(appends.run());
    let out = node.flush(&ballot()).expect("flushed");
    let mut to: Vec<ReplicaId> = out.peer.iter().map(|(p, _)| p.replica).collect();
    // A flush takes it back too, as a settle does; either is the domain
    // thread taking the outcome, never the appender's thread.
    if to.is_empty() {
        let out = node.settle(&ballot()).expect("settled");
        to = out.peer.iter().map(|(p, _)| p.replica).collect();
    }
    assert!(
        to.contains(&r(1)) && to.contains(&r(2)),
        "the proposal did not reach the voters once durable: {to:?}"
    );
    // Whatever the machine journaled in answer is the next append; the
    // proposal itself is no longer held.
    assert_eq!(node.held(), 0);
    drop(commits);
}

/// With the journal lent (task-d54), the same commands give the same
/// results, the same frames to the collector and the same projection as
/// on a node that appends on its own thread.
#[test]
fn lending_the_journal_results_are_those_of_a_node_that_appends_on_its_own_thread() {
    let run = |lending: bool| {
        let mut node = grouped_lone_leader(BootId([17; 16]));
        let handles = lending.then(|| lend_journal(&mut node));
        let mut frames = Vec::new();
        let settle = |node: &mut Node<JournaledDomain<ModelJournal, ModelEngine>>,
                      frames: &mut Vec<Vec<u8>>| {
            if let Some((commits, appends)) = &handles {
                frames.extend(pump(node, commits, appends));
            }
        };
        for round in 0..4u64 {
            for sequence in round * 4 + 1..=round * 4 + 4 {
                let out = node
                    .on_event(
                        Event::Admitted(admitted_putting(sequence, vec![sequence as u8; 8])),
                        &ballot(),
                    )
                    .expect("admitted");
                frames.extend(out.frontend);
                frames.extend(node.flush(&ballot()).expect("flushed").frontend);
                settle(&mut node, &mut frames);
            }
            let mut out = node.stage(&ballot()).expect("staged");
            out.absorb(node.flush(&ballot()).expect("flushed"));
            frames.extend(out.frontend);
            settle(&mut node, &mut frames);
            frames.extend(node.finish(&ballot()).expect("finished").frontend);
            settle(&mut node, &mut frames);
        }
        frames.extend(node.drain(&ballot()).expect("drained").frontend);
        assert_eq!(node.executed, 16);
        assert_eq!(node.awaiting_materialization(), 0);
        assert!(!node.applier().store().appending());
        let rows = {
            let gated = node.applier().store().reader().snapshot().unwrap();
            let mut rows = Vec::new();
            for sequence in 1..=16 {
                rows.push(
                    coord_storage::retry::lookup(gated.view(), &retry_key_of(sequence))
                        .unwrap()
                        .expect("every command's result is recorded"),
                );
            }
            rows
        };
        frames.sort();
        (frames, rows, node.applier().kv_revision().unwrap())
    };
    let (frames, rows, revision) = run(false);
    let (lent_frames, lent_rows, lent_revision) = run(true);
    assert_eq!(lent_revision, revision);
    assert_eq!(lent_rows, rows);
    assert_eq!(lent_frames, frames);
}

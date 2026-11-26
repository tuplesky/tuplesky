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
use coord_core::event::PeerProvenance;
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
        store: coord_types::ids::StoreInstanceId(r(me).0),
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

/// A store that remembers the ballot it was last told to record under,
/// and is otherwise the reference store.
struct Stamped {
    inner: StoreWorker<ModelEngine>,
    ballot: Option<Ballot>,
    /// The ballot each transition was submitted under, in order.
    stamps: Vec<Option<Ballot>>,
    /// Every ballot the store was fenced at, in order.
    fences: Vec<Ballot>,
    /// Refuse every submission as fenced at a newer promise, as the
    /// journaled store does for work stamped below its fence.
    fenced_out: bool,
    /// Refuse a submission stamped below the highest fence, as the
    /// journaled store does.
    below_fence: bool,
    /// Take submissions without making them durable, so a test can say
    /// later how each one ended.
    hold: bool,
    /// The barriers taken while holding, in order.
    held: Vec<coord_core::effect::BarrierId>,
    /// How many batches the queue holds, as the journaled store bounds
    /// it: past this, a submission is refused as full.
    room: Option<usize>,
}

impl coord_storage::Persistence for Stamped {
    type Reader = <StoreWorker<ModelEngine> as coord_storage::Persistence>::Reader;

    fn boot(&self) -> BootId {
        coord_storage::Persistence::boot(&self.inner)
    }
    fn application_base(&self) -> coord_core::effect::ApplyBase {
        coord_storage::Persistence::application_base(&self.inner)
    }
    fn reader(&self) -> coord_storage::GatedReader<Self::Reader> {
        coord_storage::Persistence::reader(&self.inner)
    }
    fn queued(&self) -> usize {
        coord_storage::Persistence::queued(&self.inner)
    }
    fn unmaterialized(&self) -> usize {
        coord_storage::Persistence::unmaterialized(&self.inner)
    }
    fn has_room(&self, _batch: &coord_core::effect::PersistBatch) -> bool {
        self.room
            .is_none_or(|room| coord_storage::Persistence::queued(&self.inner) < room)
    }
    fn submit(
        &mut self,
        batch: coord_core::effect::PersistBatch,
        kind: coord_storage::journaled::TransitionKind,
    ) -> Result<(), coord_storage::Refused> {
        self.stamps.push(self.ballot);
        if self.fenced_out {
            return Err(coord_storage::Refused::Fenced);
        }
        let fence = self.fences.iter().copied().reduce(|a, b| {
            if b.compare_same_epoch(&a) == Some(core::cmp::Ordering::Greater) {
                b
            } else {
                a
            }
        });
        if self.below_fence
            && let (Some(stamp), Some(fence)) = (self.ballot, fence)
            && stamp.compare_same_epoch(&fence) == Some(core::cmp::Ordering::Less)
        {
            return Err(coord_storage::Refused::Fenced);
        }
        if self
            .room
            .is_some_and(|room| coord_storage::Persistence::queued(&self.inner) >= room)
        {
            return Err(coord_storage::Refused::QueueFull);
        }
        if self.hold {
            self.held.push(batch.barrier);
            return Ok(());
        }
        coord_storage::Persistence::submit(&mut self.inner, batch, kind)
    }
    fn lower(&mut self) -> Result<coord_storage::Lowered, coord_store_api::engine::EngineError> {
        coord_storage::Persistence::lower(&mut self.inner)
    }
    fn reconcile(
        &mut self,
    ) -> Result<coord_storage::Lowered, coord_store_api::engine::EngineError> {
        coord_storage::Persistence::reconcile(&mut self.inner)
    }
    fn recovered(
        &self,
        epoch: ConfigurationEpoch,
        budget: coord_storage::ViewBudget,
    ) -> Result<coord_storage::protocol::RecoveredProtocol, coord_store_api::engine::EngineError>
    {
        coord_storage::Persistence::recovered(&self.inner, epoch, budget)
    }
    fn follow_ballot(&mut self, ballot: Ballot) {
        self.ballot = Some(ballot);
    }
    fn fence(
        &mut self,
        promised: Ballot,
    ) -> Result<Vec<coord_core::event::StorageEvent>, coord_store_api::engine::EngineError> {
        self.fences.push(promised);
        coord_storage::Persistence::fence(&mut self.inner, promised)
    }
}

/// A booted voter over a [`Stamped`] store: replica `me`, the leader of
/// the genesis ballot when `me` is 0 and a follower otherwise.
fn stamped(me: u8) -> Voter<Stamped> {
    stamped_at(me, None)
}

/// [`stamped`], started as having executed through `at`.
fn stamped_at(me: u8, at: Option<coord_types::ids::ExecutionPosition>) -> Voter<Stamped> {
    let boot = BootId([7; 16]);
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
    let applier = Applier::new(
        Stamped {
            inner: worker,
            ballot: None,
            stamps: Vec::new(),
            fences: Vec::new(),
            fenced_out: false,
            below_fence: false,
            hold: false,
            held: Vec::new(),
            room: None,
        },
        alloc,
    )
    .unwrap();
    let bootstrapped =
        coord_storage::Persistence::application_base(applier.store()).execution_position;
    let machine = if me == 0 {
        let mut leader = Leader::new(
            LeaderConfig {
                identity: identity(0),
                quorum: quorum(),
                genesis: ballot(),
                frontend: FRONTEND,
                capacity: 64,
            },
            None,
            at.unwrap_or(bootstrapped),
        );
        leader.set_learning(LearningMode::Full);
        Machine::Leader(Box::new(leader))
    } else {
        let mut follower = Follower::new(FollowerConfig {
            identity: identity(me),
            quorum: quorum(),
            genesis: ballot(),
            frontend: FRONTEND,
            capacity: 64,
        })
        .restore_execution(at.unwrap_or(bootstrapped), []);
        follower.set_learning(LearningMode::Full);
        Machine::Follower(Box::new(follower))
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

/// A `NewLeader` for `ballot`, as its candidate sends it.
fn new_leader(voter: &mut Voter<Stamped>, from: u8, ballot: Ballot) -> coord_daemon::Outbound {
    voter
        .on_peer(
            PeerProvenance::from_local_voter(r(from), inc()),
            coord_consensus::ProtocolMessage::NewLeader {
                ballot,
                executed: coord_types::ids::ExecutionPosition::ZERO,
            }
            .encode(),
        )
        .expect("stepped")
}

/// The peer messages in `out`, decoded, with who they are for.
fn messages(out: &coord_daemon::Outbound) -> Vec<(ReplicaId, coord_consensus::ProtocolMessage)> {
    out.peer
        .iter()
        .filter_map(|(to, bytes)| {
            coord_consensus::ProtocolMessage::decode(bytes)
                .ok()
                .map(|m| (to.replica, m))
        })
        .collect()
}

/// A voter's own campaign records its promise under the ballot it
/// campaigns for, fences the store there, and asks every other voter
/// (task-d01).
///
/// The order is what is under test. A promise row stamped with the
/// ballot the voter was leaving is a promise recorded under a ballot it
/// had already given up, and the fence at the new ballot then refuses it
/// as obsolete -- a campaign that can never finish.
#[test]
fn a_campaign_records_its_promise_under_the_ballot_it_campaigns_for() {
    let mut voter = stamped(1);
    let (campaigned, out) = voter
        .campaign()
        .expect("stepped")
        .expect("a follower campaigns");
    assert_eq!(campaigned.leader, r(1));
    assert!(campaigned.number > ballot().number);
    assert_eq!(voter.ballot(), campaigned);
    let store = voter.node().applier().store();
    assert_eq!(store.stamps.last(), Some(&Some(campaigned)));
    assert_eq!(store.fences, vec![campaigned]);
    let asked: Vec<ReplicaId> = messages(&out)
        .into_iter()
        .filter(|(_, m)| matches!(m, coord_consensus::ProtocolMessage::NewLeader { ballot, .. } if *ballot == campaigned))
        .map(|(to, _)| to)
        .collect();
    assert_eq!(asked, vec![r(0), r(2)]);
    // A leader does not campaign against itself.
    assert!(stamped(0).campaign().expect("stepped").is_none());
}

/// A candidate's `NewLeader` is promised under the candidate's ballot,
/// and the promise goes back to it (task-d01).
#[test]
fn a_candidates_ballot_is_adopted_before_its_promise_is_recorded() {
    let mut voter = stamped(2);
    let candidate = Ballot {
        epoch: epoch(),
        number: 1,
        leader: r(1),
    };
    let out = new_leader(&mut voter, 1, candidate);
    assert_eq!(voter.ballot(), candidate);
    let store = voter.node().applier().store();
    assert_eq!(store.stamps.last(), Some(&Some(candidate)));
    assert_eq!(store.fences, vec![candidate]);
    assert!(
        messages(&out).iter().any(|(to, m)| *to == r(1)
            && matches!(m, coord_consensus::ProtocolMessage::Promise { ballot, .. } if *ballot == candidate)),
        "no promise went back to the candidate: {:?}",
        messages(&out)
    );
}

/// A `NewLeader` the machine does not promise leaves the voter's ballot
/// where it was: adopting it early is only for the promise it is about to
/// record (task-d01).
#[test]
fn a_new_leader_the_machine_refuses_moves_nothing() {
    let mut voter = stamped(2);
    // Sent by replica 1 in replica 0's name: the machine refuses it.
    let forged = Ballot {
        epoch: epoch(),
        number: 5,
        leader: r(0),
    };
    let out = new_leader(&mut voter, 1, forged);
    assert_eq!(voter.ballot(), ballot());
    let store = voter.node().applier().store();
    assert_eq!(store.ballot, Some(ballot()));
    assert!(store.fences.is_empty());
    assert!(out.peer.is_empty());
}

/// A frame on an observer's or learner's link moves nothing, though the
/// replica it names is a voter (design Section 10.1). A higher
/// `NewLeader` doesn't raise the ballot -- `on_peer` raises it before the
/// machine sees the frame, so the role is checked before that -- nor
/// fence the store, nor draw a promise; a catch-up ask doesn't reach the
/// donor at all. The same frames on the voter's own link do each.
#[test]
fn a_frame_on_a_non_voter_link_moves_nothing_though_it_names_a_voter() {
    let candidate = Ballot {
        epoch: epoch(),
        number: 1,
        leader: r(1),
    };
    let campaign = coord_consensus::ProtocolMessage::NewLeader {
        ballot: candidate,
        executed: coord_types::ids::ExecutionPosition::ZERO,
    }
    .encode();
    let ask = coord_consensus::ProtocolMessage::CatchUpRequest {
        ballot: ballot(),
        after: coord_types::ids::ExecutionPosition::ZERO,
    }
    .encode();
    let on = |role| PeerProvenance::from_transport(r(1), inc(), role, 7);
    for role in [PeerRole::Observer, PeerRole::Learner] {
        let mut voter = stamped(2);
        let asked = voter.on_peer(on(role), ask.clone()).expect("stepped");
        assert!(asked.peer.is_empty(), "{role:?}: a page went back");
        assert_eq!(
            voter.node().unserved,
            coord_daemon::node::Unserved::default(),
            "{role:?}: the ask reached the donor"
        );
        let out = voter.on_peer(on(role), campaign.clone()).expect("stepped");
        assert_eq!(voter.ballot(), ballot(), "{role:?}: the ballot moved");
        let store = voter.node().applier().store();
        assert_eq!(store.ballot, Some(ballot()));
        assert!(store.fences.is_empty(), "{role:?}: the store was fenced");
        assert!(out.peer.is_empty(), "{role:?}: {:?}", messages(&out));

        // The control: the voter's own link. The ask reaches the donor,
        // past its sender check, and the campaign is promised.
        let asked = voter
            .on_peer(on(PeerRole::Voter), ask.clone())
            .expect("stepped");
        let unserved = voter.node().unserved;
        assert_eq!(unserved.sender, 0);
        assert!(!asked.peer.is_empty() || unserved != coord_daemon::node::Unserved::default());
        let out = voter
            .on_peer(on(PeerRole::Voter), campaign.clone())
            .expect("stepped");
        assert_eq!(voter.ballot(), candidate);
        assert_eq!(voter.node().applier().store().fences, vec![candidate]);
        assert!(
            messages(&out).iter().any(|(to, m)| *to == r(1)
                && matches!(m, coord_consensus::ProtocolMessage::Promise { ballot, .. } if *ballot == candidate)),
            "no promise went back to the candidate: {:?}",
            messages(&out)
        );
    }
}

/// A voter refuses a candidate more than a table behind it, naming its
/// position, and its own next campaign goes above the refused ballot
/// (task-d10): that candidate promised itself the ballot and hears
/// nothing below it, and it can only catch up by following.
#[test]
fn a_voter_that_refused_a_behind_candidate_campaigns_above_its_ballot() {
    let at = coord_types::ids::ExecutionPosition::new(100).unwrap();
    let mut voter = stamped_at(2, Some(at));
    let refused = Ballot {
        epoch: epoch(),
        number: 5,
        leader: r(1),
    };
    let out = new_leader(&mut voter, 1, refused);
    let sent = messages(&out);
    assert!(
        sent.iter().any(|(to, m)| *to == r(1)
            && matches!(
                m,
                coord_consensus::ProtocolMessage::PromiseRefused { ballot, executed, .. }
                    if *ballot == refused && *executed == at
            )),
        "{sent:?}"
    );
    assert!(
        !sent
            .iter()
            .any(|(_, m)| matches!(m, coord_consensus::ProtocolMessage::Promise { .. })),
        "{sent:?}"
    );
    assert_eq!(voter.node().machine().promised(), ballot());
    let (campaigned, _) = voter.campaign().expect("stepped").expect("campaigns");
    assert_eq!(
        campaigned.compare_same_epoch(&refused),
        Some(core::cmp::Ordering::Greater),
        "{campaigned:?}"
    );
}

/// Deliver every peer frame among `voters` (indexed by replica byte)
/// until none is left, starting from `out` sent by voter `from`.
fn route(voters: &mut [Voter<Stamped>], from: u8, out: coord_daemon::Outbound) {
    let mut queue: std::collections::VecDeque<(u8, u8, Vec<u8>)> = out
        .peer
        .into_iter()
        .map(|(to, frame)| (from, to.replica.0[0], frame))
        .collect();
    let mut steps = 0;
    while let Some((from, to, frame)) = queue.pop_front() {
        steps += 1;
        assert!(steps < 10_000, "the voters did not settle");
        let Some(voter) = voters.get_mut(to as usize) else {
            continue;
        };
        let out = voter
            .on_peer(PeerProvenance::from_local_voter(r(from), inc()), frame)
            .expect("stepped");
        queue.extend(
            out.peer
                .into_iter()
                .map(|(next, frame)| (to, next.replica.0[0], frame)),
        );
    }
}

/// A live leader that refuses a behind candidate leads again above the
/// refused ballot, and the behind voter follows it (task-d10).
///
/// A restarted voter's timer can fire before it hears the leader (rgb-2
/// and rgb-6 on #98). Refused by everyone, it would stay promised to its
/// own ballot, above the live one, and deaf until the next election. The
/// leader steps down and campaigns above that ballot instead.
#[test]
fn a_live_leader_that_refuses_a_behind_candidate_leads_again_above_it() {
    let ahead = coord_types::ids::ExecutionPosition::new(100).unwrap();
    let mut voters = vec![
        stamped_at(0, Some(ahead)),
        stamped(1),
        stamped_at(2, Some(ahead)),
    ];
    assert!(voters[0].leads());
    let (refused, out) = voters[1].campaign().expect("stepped").expect("campaigns");
    route(&mut voters, 1, out);

    assert!(voters[0].leads(), "the leader leads again");
    let led = voters[0].ballot();
    assert_eq!(
        led.compare_same_epoch(&refused),
        Some(core::cmp::Ordering::Greater),
        "{led:?} is above the refused {refused:?}"
    );
    for i in [1usize, 2] {
        assert_eq!(voters[i].ballot(), led, "voter {i} follows it");
        assert_eq!(voters[i].node().machine().promised(), led, "voter {i}");
    }
    assert!(!voters[1].leads());
    assert!(!voters[1].node().machine().campaigning());
}

/// Every leader a node becomes sends early as the node does (task-d98).
///
/// The setting is the node's: the domain activated the feature, not the
/// ballot. A leader that steps down and leads again above a refused
/// candidate is a new machine, made from the follower it was; given
/// nothing, it would lead without the early send until a restart.
#[test]
fn a_leader_a_node_becomes_sends_early_as_the_node_does() {
    let ahead = coord_types::ids::ExecutionPosition::new(100).unwrap();
    let mut voters = vec![
        stamped_at(0, Some(ahead)),
        stamped(1),
        stamped_at(2, Some(ahead)),
    ];
    for voter in &mut voters {
        voter.node_mut().set_early_proposal(true);
    }
    let Machine::Leader(first) = voters[0].node().machine() else {
        panic!("voter 0 leads the genesis ballot")
    };
    assert!(first.early_proposal(), "the leader it holds now");
    let (refused, out) = voters[1].campaign().expect("stepped").expect("campaigns");
    route(&mut voters, 1, out);

    assert!(voters[0].leads(), "the leader leads again");
    assert_eq!(
        voters[0].ballot().compare_same_epoch(&refused),
        Some(core::cmp::Ordering::Greater),
        "at a ballot of its own, as a new machine"
    );
    let Machine::Leader(again) = voters[0].node().machine() else {
        panic!("voter 0 leads")
    };
    assert!(again.early_proposal(), "and the leader it became");
    assert!(voters[1].node().early_proposal());
}

/// A transition the store refuses as fenced fails its barrier and the
/// voter goes on serving (task-d01).
///
/// A voter whose promise did not become durable goes back to the ballot
/// it had, and the store's fence stays where it was, so what it then asks
/// to persist is stamped below the fence and refused. That refusal is
/// expected, and a driver that took it as a fault would end the serve
/// loop. Here the store refuses the promise row itself: the machine hears
/// that its barrier definitely did not commit, sends no promise, and the
/// voter follows it back.
#[test]
fn a_transition_refused_as_fenced_fails_its_barrier_and_the_voter_serves_on() {
    let mut voter = stamped(2);
    voter.node_mut().applier_mut().store_mut().fenced_out = true;
    let candidate = Ballot {
        epoch: epoch(),
        number: 1,
        leader: r(1),
    };
    let out = voter
        .on_peer(
            PeerProvenance::from_local_voter(r(1), inc()),
            coord_consensus::ProtocolMessage::NewLeader {
                ballot: candidate,
                executed: coord_types::ids::ExecutionPosition::ZERO,
            }
            .encode(),
        )
        .expect("a refusal the voter expects does not end its turn");
    assert_eq!(voter.node().fenced, 1);
    assert!(
        !messages(&out)
            .iter()
            .any(|(_, m)| matches!(m, coord_consensus::ProtocolMessage::Promise { .. })),
        "a promise went out that is not durable: {:?}",
        messages(&out)
    );
    assert_eq!(voter.node().machine().promised(), ballot());
    assert_eq!(voter.ballot(), ballot());
}

/// A candidate's promise that is fenced and then fails, as a group
/// failure would fail it, leaving the voter back at the ballot it had and
/// below the ballot its store is fenced at (task-d01).
fn promise_that_fails(voter: &mut Voter<Stamped>, from: u8, candidate: Ballot) {
    voter.node_mut().applier_mut().store_mut().hold = true;
    new_leader(voter, from, candidate);
    let store = voter.node_mut().applier_mut().store_mut();
    assert_eq!(
        store.fences.last(),
        Some(&candidate),
        "the promise was fenced"
    );
    store.hold = false;
    store.below_fence = true;
    let barrier = *store.held.last().expect("the promise row was taken");
    let at = voter.ballot();
    voter
        .node_mut()
        .on_storage(
            coord_core::event::StorageEvent::Failed {
                barrier_id: barrier,
                error: coord_core::event::StorageError::DefinitelyNotCommitted,
            },
            &at,
        )
        .expect("a failed promise row is a storage fact");
    voter.follow_machine().expect("followed");
    assert_eq!(
        voter.ballot(),
        ballot(),
        "the voter went back to its ballot"
    );
}

/// A leader whose work is refused by its own fence stops, rather than
/// leading a domain it can no longer propose in (task-d01).
///
/// The leader promised a candidate's ballot and fenced its store there,
/// and the promise row then failed: the machine still leads the old
/// ballot, and every batch it makes is stamped below the fence. Serving
/// on, it would exhaust its retries and stop proposing while still the
/// leader, so neither it nor the voters holding its links would campaign.
/// Stopped, it restarts as a follower of its ballot and campaigns.
#[test]
fn a_leader_whose_work_is_refused_by_its_own_fence_stops() {
    let mut voter = stamped(0);
    promise_that_fails(
        &mut voter,
        2,
        Ballot {
            epoch: epoch(),
            number: 1,
            leader: r(2),
        },
    );
    assert!(voter.leads(), "a promise that failed deposes nobody");
    let stopped = voter.on_submission(
        PeerRole::Frontend,
        &frame(&submission(1)),
        Origin::Connection(7),
    );
    assert!(
        matches!(stopped, Err(coord_daemon::DriveError::Fenced(_))),
        "a leader refused by its fence served on: {stopped:?}"
    );
}

/// A follower back below its own fence campaigns above it (task-d01).
///
/// After a candidate's promise fails, the successor of what the voter
/// holds and has promised is the old number plus one under its own id.
/// With the candidate's id higher at that number, that ballot is below
/// the fence and its own store refuses the campaign's promise every
/// time. The campaign is for the successor of the fence instead.
#[test]
fn a_follower_below_its_own_fence_campaigns_above_it() {
    let mut voter = stamped(1);
    let candidate = Ballot {
        epoch: epoch(),
        number: 1,
        leader: r(2),
    };
    promise_that_fails(&mut voter, 2, candidate);
    let (campaigned, _) = voter
        .campaign()
        .expect("stepped")
        .expect("a campaign its own store does not refuse");
    assert_eq!(campaigned.leader, r(1));
    assert_eq!(
        campaigned.compare_same_epoch(&candidate),
        Some(core::cmp::Ordering::Greater),
        "{campaigned:?} is not above the fence at {candidate:?}"
    );
    assert_eq!(voter.node().machine().promised(), campaigned);
    assert_eq!(
        voter.node().fenced,
        0,
        "nothing of the campaign was refused"
    );
}

/// A leader that promises a higher ballot steps down in place: the same
/// voter, store and outbox, following the candidate (task-d01).
#[test]
fn a_leader_that_promises_a_higher_ballot_follows_it() {
    let mut voter = stamped(0);
    assert!(voter.leads());
    let candidate = Ballot {
        epoch: epoch(),
        number: 1,
        leader: r(2),
    };
    let out = new_leader(&mut voter, 2, candidate);
    assert!(!voter.leads(), "the deposed leader still leads");
    assert!(matches!(voter.node().machine(), Machine::Follower(_)));
    assert_eq!(voter.ballot(), candidate);
    assert!(
        messages(&out)
            .iter()
            .any(|(to, m)| *to == r(2)
                && matches!(m, coord_consensus::ProtocolMessage::Promise { .. })),
        "{:?}",
        messages(&out)
    );
    // And it welcomes nobody: it leads no ballot.
    assert!(voter.welcome(r(1)).peer.is_empty());
}

/// What the store stamps on a transition is the ballot the machine is
/// stepped at, from the first round and across every ballot the voter
/// moves to.
///
/// The two are one value held in two places. A store opened with a
/// ballot of its own and never told otherwise records a follower's
/// promises and votes under a ballot it never entered, and a fence set
/// at the ballot actually promised would refuse them as obsolete.
#[test]
fn the_store_records_under_the_ballot_the_voter_is_at() {
    let boot = BootId([7; 16]);
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
    let applier = Applier::new(
        Stamped {
            inner: worker,
            ballot: None,
            stamps: Vec::new(),
            fences: Vec::new(),
            fenced_out: false,
            below_fence: false,
            hold: false,
            held: Vec::new(),
            room: None,
        },
        alloc,
    )
    .unwrap();
    let bootstrapped =
        coord_storage::Persistence::application_base(applier.store()).execution_position;
    let machine = Follower::new(FollowerConfig {
        identity: identity(1),
        quorum: quorum(),
        genesis: ballot(),
        frontend: FRONTEND,
        capacity: 64,
    })
    .restore_execution(bootstrapped, []);
    let node = Node::new(Machine::Follower(Box::new(machine)), applier, FRONTEND);
    let ingress = Ingress::new(
        &membership(),
        r(1),
        PeerRole::Frontend,
        IngressBudget::default(),
    )
    .expect("replica 1 is a committed voter");
    let mut voter = Voter::new(node, ingress, (CLUSTER, DOMAIN), ballot());
    assert_eq!(voter.node().applier().store().ballot, Some(ballot()));

    let adopted = Ballot {
        epoch: epoch(),
        number: 3,
        leader: r(2),
    };
    voter.set_ballot(adopted);
    assert_eq!(voter.ballot(), adopted);
    assert_eq!(voter.node().applier().store().ballot, Some(adopted));
}

/// A step that asks for more batches than the store's queue holds is
/// carried out, not refused.
///
/// Installing a Sync writes a row per selected command, and a selection
/// names what every reporter still holds -- on a domain that has served
/// for a while, far more than the journaled store's queue depth. The
/// round submitted them all before lowering any, the queue refused the
/// rest as full, and the voter stopped: the follower installing the new
/// leader's Sync, exactly when the domain was electing one, and it met
/// the same Sync again when it came back. Here the queue holds two, and
/// the Sync names six commands this follower holds.
#[test]
fn a_step_that_asks_for_more_than_the_queue_holds_is_carried_out() {
    let mut voter = stamped(2);
    let held: Vec<coord_types::CommandId> = (1..=6).map(command_of).collect();
    for sequence in 1..=6 {
        voter.route().offer(&submission(sequence)).expect("room");
        voter.serve_local(1).expect("served");
    }
    let candidate = Ballot {
        epoch: epoch(),
        number: 1,
        leader: r(1),
    };
    new_leader(&mut voter, 1, candidate);
    voter.node_mut().applier_mut().store_mut().room = Some(2);
    let before = voter.node().applier().store().stamps.len();
    let decision = coord_consensus::SyncDecision {
        ballot: candidate,
        source_ballot: ballot(),
        entries: held
            .iter()
            .map(|c| {
                (
                    *c,
                    coord_consensus::SyncEntry {
                        command: *c,
                        phase: coord_consensus::Phase::Accept,
                        deps: Vec::new(),
                        path: coord_consensus::empty_path(),
                        paths: Vec::new(),
                        seqnum: 0,
                        admission: None,
                    },
                )
            })
            .collect(),
        reproposed: BTreeSet::new(),
        counted: Default::default(),
    };
    voter
        .on_peer(
            PeerProvenance::from_local_voter(r(1), inc()),
            coord_consensus::ProtocolMessage::Sync(decision).encode(),
        )
        .expect("the Sync is installed, not refused as full");
    let store = voter.node().applier().store();
    assert!(
        store.stamps.len() - before > 2,
        "more batches than the queue holds were submitted: {}",
        store.stamps.len() - before
    );
    assert_eq!(coord_storage::Persistence::queued(store), 0);
    assert_eq!(voter.node().held(), 0, "every send was released");
}

/// A follower below its own fence takes a request and serves on (#197
/// review): the store refuses its vote batch as fenced, and nothing is
/// written again at that failure. A write there was refused the same way
/// and written again until `carry_out` gave up and the node stopped; an
/// election race puts a follower in this state, as Jepsen's repeated
/// leader kills found (task-d01).
#[test]
fn a_fenced_follower_takes_a_request_and_serves_on() {
    let mut follower = stamped(1);
    promise_that_fails(
        &mut follower,
        2,
        Ballot {
            epoch: epoch(),
            number: 1,
            leader: r(2),
        },
    );
    let _ = follower
        .on_submission(
            PeerRole::Frontend,
            &frame(&submission(1)),
            Origin::Connection(8),
        )
        .expect("a follower below its fence serves on");
}

/// A command's vote batch fails and then its adoption batch, in the order
/// a definite journal failure fails a group and the queue behind it (#197
/// review). A later command on the same key names it: the batch that
/// names it carries its rows, so no durable record here names a command
/// with no row here.
#[test]
fn both_of_a_commands_batches_fail_then_a_later_vote_names_it() {
    let mut leader = stamped(0);
    let mut follower = stamped(1);
    follower.node_mut().applier_mut().store_mut().hold = true;
    let out = leader
        .on_submission(
            PeerRole::Frontend,
            &frame(&submission(1)),
            Origin::Connection(7),
        )
        .expect("serves")
        .expect("admitted");
    for (_, f) in out.peer.into_iter().filter(|(to, _)| to.replica == r(1)) {
        follower
            .on_peer(PeerProvenance::from_local_voter(r(0), inc()), f)
            .expect("stepped");
    }
    follower
        .on_submission(
            PeerRole::Frontend,
            &frame(&submission(1)),
            Origin::Connection(8),
        )
        .expect("stepped")
        .expect("admitted");
    // Command A's vote batch and adoption batch, failed in submission
    // order, as a definite journal failure fails a group and its queue.
    let held = core::mem::take(&mut follower.node_mut().applier_mut().store_mut().held);
    assert!(held.len() >= 2, "both of A's batches were held: {held:?}");
    follower.node_mut().applier_mut().store_mut().hold = false;
    let at = follower.ballot();
    for barrier in held {
        follower
            .node_mut()
            .on_storage(
                coord_core::event::StorageEvent::Failed {
                    barrier_id: barrier,
                    error: coord_core::event::StorageError::DefinitelyNotCommitted,
                },
                &at,
            )
            .expect("a storage fact");
    }
    // Command B, same key: the follower acknowledges it to the leader.
    let out = leader
        .on_submission(
            PeerRole::Frontend,
            &frame(&submission(2)),
            Origin::Connection(7),
        )
        .expect("serves")
        .expect("admitted");
    for (_, f) in out.peer.into_iter().filter(|(to, _)| to.replica == r(1)) {
        follower
            .on_peer(PeerProvenance::from_local_voter(r(0), inc()), f)
            .expect("stepped");
    }
    follower
        .on_submission(
            PeerRole::Frontend,
            &frame(&submission(2)),
            Origin::Connection(8),
        )
        .expect("stepped")
        .expect("admitted");
    let Machine::Follower(f) = follower.node().machine() else {
        panic!("a follower")
    };
    let report = f.report(ballot());
    let durable: BTreeSet<_> = report.entries.iter().map(|e| e.command).collect();
    assert!(report.entries.len() >= 2, "{report:?}");
    for e in &report.entries {
        assert!(
            e.deps.iter().all(|d| durable.contains(d)),
            "{:?} names {:?}",
            e.command,
            e.deps
        );
    }
}

/// Give `voter` the peer store check (task-d121, C1).
fn keep_peer_stores(voter: &mut Voter<Stamped>) {
    voter
        .node_mut()
        .keep_peer_stores(coord_daemon::peer_store::PeerStores::new(
            inc(),
            BootId([7; 16]),
        ));
}

/// The store `voter`'s store durably records for `about` under
/// `incarnation`, read by key as a restart reads it.
fn durable_row(
    voter: &Voter<Stamped>,
    about: ReplicaId,
    incarnation: ReplicaIncarnation,
) -> Option<coord_types::ids::StoreInstanceId> {
    let gated = coord_storage::Persistence::reader(voter.node().applier().store())
        .snapshot()
        .expect("a view");
    coord_checkpoint::peer_store::read_peer_store(gated.view(), &about, incarnation)
        .expect("the row reads")
        .map(|row| row.store)
}

/// The frames in `out` for `to`, undecoded, as the link carries them.
fn frames_to(out: &coord_daemon::Outbound, to: ReplicaId) -> Vec<Vec<u8>> {
    out.peer
        .iter()
        .filter(|(peer, _)| peer.replica == to)
        .map(|(_, bytes)| bytes.clone())
        .collect()
}

/// A follower's answer to `candidate`'s `NewLeader`: its promise and its
/// report pages, as its own machine sends them.
fn answer(candidate: Ballot) -> Vec<Vec<u8>> {
    let mut peer = stamped(2);
    let frames = frames_to(&new_leader(&mut peer, 1, candidate), r(1));
    let decoded: Vec<_> = frames
        .iter()
        .filter_map(|f| coord_consensus::ProtocolMessage::decode(f).ok())
        .collect();
    assert!(
        decoded
            .iter()
            .any(|m| matches!(m, coord_consensus::ProtocolMessage::Promise { .. })),
        "the peer promised: {decoded:?}"
    );
    assert!(
        decoded
            .iter()
            .any(|m| matches!(m, coord_consensus::ProtocolMessage::ReportPage(_))),
        "the peer reported: {decoded:?}"
    );
    frames
}

fn is_sync(m: &coord_consensus::ProtocolMessage) -> bool {
    matches!(m, coord_consensus::ProtocolMessage::Sync(_))
}

/// A peer's first promise and report under an incarnation wait for the
/// row recording its store to be durable, and only then reach the
/// candidate's machine (task-d121, C1). The row is what a restart reads.
#[test]
fn a_peers_first_promise_and_report_wait_for_its_store_row() {
    let mut candidate = stamped(1);
    keep_peer_stores(&mut candidate);
    let (campaigned, _) = candidate
        .campaign()
        .expect("stepped")
        .expect("a follower campaigns");
    let frames = answer(campaigned);
    // From here a batch waits for the turn's flush, as in `coordd`.
    candidate.node_mut().lower_in_groups();
    let mut sent = Vec::new();
    for frame in frames.clone() {
        let out = candidate
            .on_peer(PeerProvenance::from_local_voter(r(2), inc()), frame)
            .expect("stepped");
        sent.extend(messages(&out));
    }
    assert_eq!(
        candidate.held_peers(),
        frames.len(),
        "all held behind the row"
    );
    let counts = candidate.node().peer_stores().expect("kept").counts;
    assert_eq!(counts.recorded, 1, "one row for the peer");
    assert_eq!(counts.held, frames.len() as u64 - 1);
    assert_eq!(counts.refused, 0);
    assert!(
        !sent.iter().any(|(_, m)| is_sync(m)),
        "won on a row not durable"
    );
    for _ in 0..4 {
        sent.extend(messages(&candidate.flush().expect("flushed")));
        sent.extend(messages(&candidate.finish().expect("finished")));
    }
    assert_eq!(
        candidate.held_peers(),
        0,
        "handed on once the row is durable"
    );
    let counted = sent
        .iter()
        .find_map(|(_, m)| match m {
            coord_consensus::ProtocolMessage::Sync(d) => Some(d.counted.clone()),
            _ => None,
        })
        .expect("the campaign counted the peer");
    // The Sync names the store of every voter it counted, the
    // candidate's own among them.
    assert_eq!(
        counted,
        [1, 2]
            .into_iter()
            .map(|i| (
                r(i),
                coord_consensus::CountedStore {
                    incarnation: inc(),
                    store: coord_types::ids::StoreInstanceId(r(i).0),
                }
            ))
            .collect(),
    );
    assert_eq!(
        durable_row(&candidate, r(2), inc()),
        Some(coord_types::ids::StoreInstanceId(r(2).0)),
        "a restart reads the peer's store"
    );
}

/// A promise and report from another store than the one recorded for the
/// peer are refused, logged and counted, and the campaign never counts
/// them: a voter wiped and initialized again under its identity (task-d121,
/// C1). The same voter back under a new incarnation is not refused.
#[test]
fn a_promise_and_report_from_another_store_are_refused() {
    let mut candidate = stamped(1);
    // The peer's store as this voter first heard it, before the wipe.
    {
        let row = coord_checkpoint::peer_store::PeerStoreV1 {
            voter: r(2),
            incarnation: inc(),
            store: coord_types::ids::StoreInstanceId([0xee; 16]),
        };
        let store = &mut candidate.node_mut().applier_mut().store_mut().inner;
        store
            .submit(coord_core::effect::PersistBatch {
                barrier: BarrierAllocator::new(inc(), BootId([7; 16]))
                    .for_runtime()
                    .allocate(),
                base: None,
                updates: vec![row.update().expect("encoded")],
            })
            .expect("taken");
        store.flush().expect("flushed");
    }
    keep_peer_stores(&mut candidate);
    let (campaigned, _) = candidate
        .campaign()
        .expect("stepped")
        .expect("a follower campaigns");
    let frames = answer(campaigned);
    let mut sent = Vec::new();
    for frame in frames.clone() {
        let out = candidate
            .on_peer(PeerProvenance::from_local_voter(r(2), inc()), frame)
            .expect("stepped");
        sent.extend(messages(&out));
    }
    sent.extend(messages(&candidate.flush().expect("flushed")));
    assert_eq!(candidate.held_peers(), 0);
    let counts = candidate.node().peer_stores().expect("kept").counts;
    assert_eq!(counts.refused, frames.len() as u64, "every frame refused");
    assert_eq!(counts.recorded, 0);
    assert!(
        !sent.iter().any(|(_, m)| is_sync(m)),
        "counted a wiped voter"
    );
    let rejections = candidate.take_rejections();
    assert!(
        rejections.iter().any(|r| r.contains("wiped")),
        "the refusal is logged: {rejections:?}"
    );
    // Back through membership as a new generation: recorded, not refused.
    let next = inc().checked_next().expect("an incarnation");
    candidate
        .on_peer(
            PeerProvenance::from_local_voter(r(2), next),
            frames[0].clone(),
        )
        .expect("stepped");
    let counts = candidate.node().peer_stores().expect("kept").counts;
    assert_eq!(counts.recorded, 1, "a new generation is recorded");
    assert_eq!(counts.refused, frames.len() as u64);
}

/// A row whose batch fails is forgotten: what it held is dropped, and the
/// peer's next promise is recorded again (task-d121, C1).
#[test]
fn a_store_row_that_fails_is_recorded_again() {
    let mut candidate = stamped(1);
    keep_peer_stores(&mut candidate);
    let (campaigned, _) = candidate
        .campaign()
        .expect("stepped")
        .expect("a follower campaigns");
    let frames = answer(campaigned);
    candidate.node_mut().applier_mut().store_mut().hold = true;
    candidate
        .on_peer(
            PeerProvenance::from_local_voter(r(2), inc()),
            frames[0].clone(),
        )
        .expect("stepped");
    assert_eq!(candidate.held_peers(), 1);
    let store = candidate.node_mut().applier_mut().store_mut();
    store.hold = false;
    let barrier = *store.held.last().expect("the row was taken");
    assert!(barrier.sequence >= coord_core::outbox::PEER_STORE_BARRIERS);
    let at = candidate.ballot();
    candidate
        .node_mut()
        .on_storage(
            coord_core::event::StorageEvent::Failed {
                barrier_id: barrier,
                error: coord_core::event::StorageError::DefinitelyNotCommitted,
            },
            &at,
        )
        .expect("a failed row is a storage fact");
    candidate.flush().expect("flushed");
    assert_eq!(
        candidate.held_peers(),
        0,
        "what the failed row held is dropped"
    );
    let counts = candidate.node().peer_stores().expect("kept").counts;
    assert_eq!(counts.failed, 1);
    assert_eq!(durable_row(&candidate, r(2), inc()), None);
    let mut sent = Vec::new();
    for frame in frames {
        let out = candidate
            .on_peer(PeerProvenance::from_local_voter(r(2), inc()), frame)
            .expect("stepped");
        sent.extend(messages(&out));
    }
    sent.extend(messages(&candidate.flush().expect("flushed")));
    let counts = candidate.node().peer_stores().expect("kept").counts;
    assert_eq!(counts.recorded, 2, "recorded again");
    assert_eq!(candidate.held_peers(), 0);
    assert!(
        sent.iter().any(|(_, m)| is_sync(m)),
        "the campaign counted the peer: {sent:?}"
    );
}

/// Write a row recording `store` for voter `about` into `voter`'s store,
/// as an earlier promise would have.
fn write_store_row(voter: &mut Voter<Stamped>, about: ReplicaId, store: [u8; 16]) {
    write_rows(
        voter,
        vec![coord_checkpoint::peer_store::PeerStoreV1 {
            voter: about,
            incarnation: inc(),
            store: coord_types::ids::StoreInstanceId(store),
        }],
    );
}

/// Write `rows` into `voter`'s store, a hundred to a batch.
fn write_rows(voter: &mut Voter<Stamped>, rows: Vec<coord_checkpoint::peer_store::PeerStoreV1>) {
    let mut barriers = BarrierAllocator::new(inc(), BootId([7; 16])).for_runtime();
    let inner = &mut voter.node_mut().applier_mut().store_mut().inner;
    for chunk in rows.chunks(100) {
        inner
            .submit(coord_core::effect::PersistBatch {
                barrier: barriers.allocate(),
                base: None,
                updates: chunk
                    .iter()
                    .map(|row| row.update().expect("encoded"))
                    .collect(),
            })
            .expect("taken");
    }
    inner.flush().expect("flushed");
}

/// Candidate 1 campaigns and `peer` (replica 2) promises; the Sync frames
/// the candidate sends the peer once it wins, and the ballot.
fn won_with(peer: &mut Voter<Stamped>) -> (Ballot, Vec<Vec<u8>>) {
    let mut candidate = stamped(1);
    keep_peer_stores(&mut candidate);
    let (campaigned, _) = candidate
        .campaign()
        .expect("stepped")
        .expect("a follower campaigns");
    let mut to_peer = Vec::new();
    for frame in frames_to(&new_leader(peer, 1, campaigned), r(1)) {
        let out = candidate
            .on_peer(PeerProvenance::from_local_voter(r(2), inc()), frame)
            .expect("stepped");
        to_peer.extend(frames_to(&out, r(2)));
    }
    for _ in 0..4 {
        to_peer.extend(frames_to(&candidate.flush().expect("flushed"), r(2)));
        to_peer.extend(frames_to(&candidate.finish().expect("finished"), r(2)));
    }
    let syncs: Vec<_> = to_peer
        .into_iter()
        .filter(|f| matches!(coord_consensus::ProtocolMessage::decode(f), Ok(m) if is_sync(&m)))
        .collect();
    assert!(!syncs.is_empty(), "the candidate won");
    (campaigned, syncs)
}

/// Deliver the Sync `frames` from candidate 1 to `voter`, and settle.
fn install(voter: &mut Voter<Stamped>, frames: Vec<Vec<u8>>) {
    for frame in frames {
        voter
            .on_peer(PeerProvenance::from_local_voter(r(1), inc()), frame)
            .expect("stepped");
    }
    for _ in 0..4 {
        voter.flush().expect("flushed");
        voter.finish().expect("finished");
    }
}

/// A voter that installs a Sync records the store of each voter it
/// counted that it has no row for (task-d121, C1): here the candidate's,
/// whose promise it never received.
#[test]
fn a_voter_that_installs_a_sync_learns_the_stores_it_counted() {
    let mut peer = stamped(2);
    keep_peer_stores(&mut peer);
    let (campaigned, syncs) = won_with(&mut peer);
    install(&mut peer, syncs);
    assert!(peer.node().machine().synchronized_at(&campaigned));
    let counts = peer.node().peer_stores().expect("kept").counts;
    assert_eq!(counts.learned, 1, "the candidate's store, not its own");
    assert_eq!(counts.disagreed, 0);
    assert_eq!(
        durable_row(&peer, r(1), inc()),
        Some(coord_types::ids::StoreInstanceId(r(1).0)),
        "a restart reads the candidate's store"
    );
    assert_eq!(durable_row(&peer, r(2), inc()), None, "no row for itself");
}

/// A voter that lowers in groups, as `coordd` does, installs a Sync only
/// once a later flush makes its batch durable. It still learns the stores
/// the Sync counted, then (Codex on #326).
#[test]
fn a_voter_that_installs_a_sync_in_groups_learns_the_stores_it_counted() {
    let mut peer = stamped(2);
    peer.node_mut().lower_in_groups();
    keep_peer_stores(&mut peer);
    let (campaigned, syncs) = won_with(&mut peer);
    for frame in syncs {
        peer.on_peer(PeerProvenance::from_local_voter(r(1), inc()), frame)
            .expect("stepped");
    }
    assert!(
        !peer.node().machine().synchronized_at(&campaigned),
        "the install waits for a flush"
    );
    for _ in 0..4 {
        peer.flush().expect("flushed");
        peer.finish().expect("finished");
    }
    assert!(peer.node().machine().synchronized_at(&campaigned));
    let counts = peer.node().peer_stores().expect("kept").counts;
    assert_eq!(counts.learned, 1, "learned once the install completed");
    assert_eq!(
        durable_row(&peer, r(1), inc()),
        Some(coord_types::ids::StoreInstanceId(r(1).0))
    );
}

/// Syncs of higher ballots that arrive while one of a lower ballot waits
/// for its flush, ahead of their NewLeader and never installed, don't
/// cost the lower one its counted stores, however many arrive (task-d121,
/// C1; Codex on #326).
#[test]
fn higher_syncs_never_installed_leave_the_installed_one_learned() {
    let mut peer = stamped(2);
    peer.node_mut().lower_in_groups();
    keep_peer_stores(&mut peer);
    let (campaigned, syncs) = won_with(&mut peer);
    let decision = syncs
        .iter()
        .find_map(|f| match coord_consensus::ProtocolMessage::decode(f) {
            Ok(coord_consensus::ProtocolMessage::Sync(d)) => Some(d),
            _ => None,
        })
        .expect("a Sync");
    for frame in syncs {
        peer.on_peer(PeerProvenance::from_local_voter(r(1), inc()), frame)
            .expect("stepped");
    }
    assert!(!peer.node().machine().synchronized_at(&campaigned));
    // Five higher ballots' Syncs from r0, each counting another store for
    // r0, none of whose NewLeader ever arrives.
    for n in 1..=5 {
        let mut higher = decision.clone();
        higher.ballot = Ballot {
            number: campaigned.number + n,
            leader: r(0),
            ..campaigned
        };
        higher.counted.insert(
            r(0),
            coord_consensus::CountedStore {
                incarnation: inc(),
                store: coord_types::ids::StoreInstanceId([0x0f; 16]),
            },
        );
        peer.on_peer(
            PeerProvenance::from_local_voter(r(0), inc()),
            coord_consensus::ProtocolMessage::Sync(higher).encode(),
        )
        .expect("stepped");
    }
    for _ in 0..4 {
        peer.flush().expect("flushed");
        peer.finish().expect("finished");
    }
    assert!(peer.node().machine().synchronized_at(&campaigned));
    assert_eq!(peer.node().peer_stores().expect("kept").counts.learned, 1);
    assert_eq!(
        durable_row(&peer, r(1), inc()),
        Some(coord_types::ids::StoreInstanceId(r(1).0))
    );
    assert_eq!(durable_row(&peer, r(0), inc()), None, "never installed");
}

/// A voter that installed a Sync before it kept its peer stores, as a
/// voter does that stopped between the install and the rows' batch,
/// learns the stores it counted from the Sync's durable row at its next
/// step (task-d121, C1; Codex on #326).
#[test]
fn a_voter_resuming_an_installed_sync_learns_the_stores_it_counted() {
    let mut peer = stamped(2);
    let (campaigned, syncs) = won_with(&mut peer);
    install(&mut peer, syncs);
    assert!(peer.node().machine().synchronized_at(&campaigned));
    assert_eq!(durable_row(&peer, r(1), inc()), None, "nothing learned");
    keep_peer_stores(&mut peer);
    peer.flush().expect("flushed");
    peer.finish().expect("finished");
    assert_eq!(peer.node().peer_stores().expect("kept").counts.learned, 1);
    assert_eq!(
        durable_row(&peer, r(1), inc()),
        Some(coord_types::ids::StoreInstanceId(r(1).0))
    );
}

/// A Sync that counted another store for a voter than the one this voter
/// recorded is installed all the same, and the row is kept; the
/// disagreement is logged and counted (task-d121, C1).
#[test]
fn a_sync_that_counted_another_store_is_installed_and_the_row_kept() {
    let mut peer = stamped(2);
    write_store_row(&mut peer, r(1), [0xee; 16]);
    keep_peer_stores(&mut peer);
    let (campaigned, syncs) = won_with(&mut peer);
    install(&mut peer, syncs);
    assert!(
        peer.node().machine().synchronized_at(&campaigned),
        "installed"
    );
    let counts = peer.node().peer_stores().expect("kept").counts;
    assert_eq!(counts.disagreed, 1);
    assert_eq!(counts.learned, 0);
    let rejections = peer.take_rejections();
    assert!(
        rejections
            .iter()
            .any(|r| r.contains("counted store") && r.contains("installed")),
        "the disagreement is logged: {rejections:?}"
    );
    assert_eq!(
        durable_row(&peer, r(1), inc()),
        Some(coord_types::ids::StoreInstanceId([0xee; 16])),
        "the row is never overwritten"
    );
}

/// A peer whose history holds more rows than any whole read of them
/// allowed is served: each row is read by its key (from Astra's review of
/// C1, where 1,025 rows kept a voter from reopening).
#[test]
fn a_peer_history_of_any_length_is_served() {
    let mut candidate = stamped(1);
    let last = 1_100u64;
    write_rows(
        &mut candidate,
        (1..=last)
            .map(|i| coord_checkpoint::peer_store::PeerStoreV1 {
                voter: r(2),
                incarnation: ReplicaIncarnation::new(i).unwrap(),
                store: coord_types::ids::StoreInstanceId(r(2).0),
            })
            .collect(),
    );
    keep_peer_stores(&mut candidate);
    let (campaigned, _) = candidate
        .campaign()
        .expect("stepped")
        .expect("a follower campaigns");
    let frames = answer(campaigned);
    let mut sent = Vec::new();
    for frame in frames.clone() {
        let out = candidate
            .on_peer(
                PeerProvenance::from_local_voter(r(2), ReplicaIncarnation::new(last).unwrap()),
                frame,
            )
            .expect("stepped");
        sent.extend(messages(&out));
    }
    let counts = candidate.node().peer_stores().expect("kept").counts;
    assert_eq!(counts.matched, frames.len() as u64, "the row read by key");
    assert_eq!(counts.recorded, 0);
    assert!(
        sent.iter().any(|(_, m)| is_sync(m)),
        "the campaign counted the peer: {sent:?}"
    );
}

/// A peer's retries while its row is in the journal are held once each,
/// not once per retry (from Astra's review of C1: three copies of one page
/// were held).
#[test]
fn a_retried_page_waits_once_behind_its_store_row() {
    let mut candidate = stamped(1);
    keep_peer_stores(&mut candidate);
    let (campaigned, _) = candidate
        .campaign()
        .expect("stepped")
        .expect("a follower campaigns");
    let page = answer(campaigned)
        .into_iter()
        .find(|f| {
            matches!(
                coord_consensus::ProtocolMessage::decode(f),
                Ok(coord_consensus::ProtocolMessage::ReportPage(_))
            )
        })
        .expect("a page");
    candidate.node_mut().applier_mut().store_mut().hold = true;
    for _ in 0..3 {
        candidate
            .on_peer(PeerProvenance::from_local_voter(r(2), inc()), page.clone())
            .expect("stepped");
    }
    assert_eq!(candidate.held_peers(), 1, "one copy held");
    let counts = candidate.node().peer_stores().expect("kept").counts;
    assert_eq!(counts.recorded, 1);
    assert_eq!(counts.duplicates, 2);
}

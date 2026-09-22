//! Duplicate evidence repair, end to end and in one process (task-c02).
//!
//! The failure this covers: a voter learns a command from its peers
//! before the collector's submission reaches it, acknowledges it, and
//! holds the acknowledgement for a submitter it does not know yet. The
//! hold is bounded -- in time and in depth -- and what it lets go was the
//! caller's only copy of that voter's vote. Before task-c02 the late
//! submission was an exact duplicate with no effects, and the caller
//! waited until its deadline.
//!
//! These tests run the real machines over real (model-engine) stores,
//! the real voter door, the real parked-evidence bookkeeping and the real
//! collector, with time as an input rather than something waited for.
//! They hold that:
//!
//! * the hold expiring, or the depth crowding out, loses the evidence
//!   and the collector holds the release for want of votes;
//! * the duplicate submission afterwards has each voter publish its
//!   acknowledgement again -- to the collector alone, with no peer
//!   retransmission and no second execution -- and the collector then
//!   learns and releases;
//! * that repaired evidence is *necessary*: the same schedule with the
//!   repair's output discarded leaves the caller waiting, which is what
//!   the tests above would show if the repair were reverted;
//! * when the voter's retained evidence cannot be replayed (the bound
//!   on repairs is spent), the refusal is explicit and the collector's
//!   fallback -- this node's own durable record of the execution --
//!   completes the caller's request with the same outcome the leader
//!   released; and the same fallback closes the other half, a lost
//!   release, from the votes the collector did count.

use std::collections::{BTreeSet, VecDeque};
use std::time::{Duration, Instant};

use coord_collector::{
    Collector, CollectorConfig, CollectorEvent, EvidenceError, HoldReason, KIND_EVIDENCE,
    KIND_RELEASE, MonotonicMillis, Progress, Release, SettleError, Submitted, decode_evidence,
    decode_release,
};
use coord_consensus::{
    BallotConfiguration, ConfigurationIdentity, Follower, FollowerConfig, Leader, LeaderConfig,
    LearningMode, MAX_EVIDENCE_REPAIRS, ProtocolMessage, ReplicaRole,
};
use coord_core::capability::{AdmissionReceipt, AttestedAdmission, VerifierToken};
use coord_core::effect::{BootId, PeerId};
use coord_core::event::{AdmittedRequest, PeerProvenance};
use coord_core::outbox::BarrierAllocator;
use coord_daemon::mailbox::{Ingress, IngressBudget};
use coord_daemon::node::{Machine, Node, Outbound};
use coord_daemon::parked::Parked;
use coord_daemon::settle::records_for;
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
/// The API-class connection the collector submits over.
const COLLECTOR: Origin = Origin::Connection(7);
/// The hold on parked evidence: what coordd uses, so the schedule is
/// the daemon's and not a shorter one.
const HOLD: Duration = Duration::from_secs(1);

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
                capacity: 64,
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
            capacity: 64,
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

/// The command a frontend frame is about, read from the frame itself --
/// exactly as coordd reads it.
fn command_of_frame(frame: &Frame) -> Option<CommandId> {
    match frame.kind {
        KIND_EVIDENCE => ProtocolMessage::decode(&frame.payload).ok()?.command(),
        KIND_RELEASE => decode_release(frame)
            .ok()
            .map(|r| r.established().command()),
        _ => None,
    }
}

/// Which of a voter's outbound frames to lose on the way to the
/// collector, standing in for the frontend that stopped holding them.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Lose {
    Nothing,
    /// Every release the leader publishes.
    Releases,
}

/// Three voters, the collector that submitted to them, and the frontend
/// bookkeeping in between: each voter's parked evidence, routed the way
/// coordd routes it.
struct Cluster {
    voters: Vec<Voter<StoreWorker<ModelEngine>>>,
    parked: Vec<Parked>,
    collector: Collector,
    now: Instant,
    peers: VecDeque<(usize, PeerProvenance, Vec<u8>)>,
    lose: Lose,
    /// What the collector said to each frame it was handed.
    progress: Vec<(u8, Progress)>,
    rejected: Vec<(u8, EvidenceError)>,
    releases: Vec<Release>,
    /// Frames that reached the collector, per voter, by kind.
    counted: Vec<(u8, u16)>,
    /// How many frames each voter parked since the count was last
    /// reset -- what it published for a submitter it did not know, and
    /// so what a repair has to publish again.
    published: Vec<usize>,
}

impl Cluster {
    fn new(depth: usize) -> Self {
        Cluster {
            voters: (0..3).map(voter).collect(),
            parked: (0..3).map(|_| Parked::new(HOLD, depth)).collect(),
            collector: Collector::new(CollectorConfig {
                quorum: quorum(),
                max_pending: 16,
                max_resolved: 16,
                max_undelivered_bytes: usize::MAX,
            }),
            now: Instant::now(),
            peers: VecDeque::new(),
            lose: Lose::Nothing,
            progress: Vec::new(),
            rejected: Vec::new(),
            releases: Vec::new(),
            counted: Vec::new(),
            published: vec![0; 3],
        }
    }

    fn command(&self, sequence: u64) -> CommandId {
        CommandId::derive(&retry_key(sequence), &logical(sequence)).unwrap()
    }

    /// The collector submits `sequence`; returns the `Submit` frame it
    /// fans out, which the test delivers on its own schedule.
    fn submit(&mut self, sequence: u64) -> Vec<u8> {
        match self
            .collector
            .submit(MonotonicMillis::ZERO, &admitted(sequence))
            .expect("submitted")
        {
            Submitted::FanOut(fan_out) => {
                assert_eq!(fan_out.command, self.command(sequence));
                assert_eq!(fan_out.targets, (0..3).map(r).collect::<Vec<_>>());
                fan_out.frame.to_vec()
            }
            other => panic!("not new work: {other:?}"),
        }
    }

    /// The submission reaches voter `to`, over the collector's
    /// connection. Returns the round it produced, after routing it.
    fn deliver_submission(&mut self, to: usize, submit: &[u8]) -> RoundShape {
        let out = self.voters[to]
            .on_submission(PeerRole::Frontend, &frame(submit), COLLECTOR)
            .expect("driven")
            .expect("admitted");
        let shape = RoundShape {
            peer: out.peer.len(),
            frontend: out.frontend.len(),
        };
        self.carry(to, out);
        shape
    }

    /// Everything a voter's round asked the runtime to do: frontend
    /// frames to the collector that asked (or parked, when this voter
    /// cannot say which), peer frames to the peer plane.
    fn carry(&mut self, from: usize, out: Outbound) {
        for bytes in out.frontend {
            self.route_frontend(from, bytes);
        }
        for (to, frame) in out.peer {
            let prov = self.voters[from].provenance();
            self.peers
                .push_back((usize::from(to.replica.0[0]), prov, frame));
        }
    }

    /// coordd's `hand_to_collector`: to the collector the submission
    /// came from, where this voter knows it, and parked otherwise.
    fn route_frontend(&mut self, from: usize, bytes: Vec<u8>) {
        let f = frame(&bytes);
        let command = command_of_frame(&f).expect("a frame about a command");
        match self.voters[from].origin_of(&command) {
            Some(COLLECTOR) => self.hand_to_collector(from, &f),
            Some(other) => panic!("owed to a collector this test never used: {other:?}"),
            None => {
                let prov = self.voters[from].provenance();
                self.parked[from].park(command, prov, bytes, self.now);
                self.published[from] += 1;
            }
        }
    }

    /// One frame reaches the collector under the voter's committed
    /// identity.
    fn hand_to_collector(&mut self, from: usize, f: &Frame) {
        let prov = self.voters[from].provenance();
        let result = match f.kind {
            KIND_EVIDENCE => self
                .collector
                .on_evidence(prov, decode_evidence(f).expect("evidence")),
            KIND_RELEASE => {
                if self.lose == Lose::Releases {
                    return;
                }
                self.collector
                    .on_release(prov, decode_release(f).expect("release"))
            }
            other => panic!("not a frontend frame kind: {other}"),
        };
        self.counted.push((from as u8, f.kind));
        match result {
            Ok(progress) => {
                if let Progress::Released(release) = &progress {
                    self.releases.push(release.clone());
                }
                self.progress.push((from as u8, progress));
            }
            Err(e) => self.rejected.push((from as u8, e)),
        }
    }

    /// coordd's `route_parked`, at the cluster's current instant.
    fn route_parked(&mut self) -> usize {
        let mut unclaimed = 0;
        for i in 0..3 {
            let routed = self.parked[i].route(self.now, |c| self.voters[i].origin_of(c));
            unclaimed += routed.unclaimed;
            for (origin, _, bytes) in routed.ready {
                assert_eq!(origin, COLLECTOR);
                self.hand_to_collector(i, &frame(&bytes));
            }
        }
        unclaimed
    }

    /// Run the peer plane and execution until nothing moves: every peer
    /// frame delivered, every executable command applied, every missing
    /// payload asked for and answered. Time does not pass.
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
                if self.voters[i].awaiting().is_some() || self.voters[i].missing_payloads() > 0 {
                    let out = self.voters[i].request_payloads().expect("asked");
                    if !out.is_empty() {
                        moved = true;
                    }
                    self.carry(i, out);
                }
            }
            self.route_parked();
            if !moved {
                return;
            }
        }
    }

    fn executed(&self) -> Vec<u64> {
        self.voters.iter().map(|v| v.node().executed).collect()
    }

    fn rejections(&mut self) -> Vec<String> {
        self.voters
            .iter_mut()
            .flat_map(Voter::take_rejections)
            .collect()
    }

    /// The failure schedule these tests share: the collector submits,
    /// the submission reaches the leader at once and the followers not
    /// yet; the followers learn the command from the leader, acknowledge
    /// it, and park the acknowledgement for a submitter they do not
    /// know. The leader learns from the peer plane, executes and
    /// releases; the collector holds the release for want of votes.
    fn run_to_half_held(&mut self, sequence: u64) -> Vec<u8> {
        self.published = vec![0; 3];
        let submit = self.submit(sequence);
        let command = self.command(sequence);
        let shape = self.deliver_submission(0, &submit);
        assert!(shape.peer > 0, "the leader proposed to its voters");
        self.settle();

        assert_eq!(self.executed(), [1, 1, 1], "every voter executed it once");
        assert!(
            self.collector.is_pending(&command),
            "the collector is still collecting"
        );
        assert!(self.releases.is_empty());
        assert_eq!(
            self.progress
                .iter()
                .filter(|(from, _)| *from == 0)
                .map(|(_, p)| p.clone())
                .collect::<Vec<_>>(),
            vec![
                Progress::Held(HoldReason::AwaitingVotes),
                Progress::Held(HoldReason::AwaitingVotes),
            ],
            "the leader's reply and its release both reached the collector, and it holds them: {:?}",
            self.progress
        );
        for i in 1..3 {
            assert!(
                self.parked[i].commands().all(|c| *c == command),
                "follower {i} parked evidence for another command"
            );
            // A follower that held the proposal until the payload came
            // adopts it on the slow path as well: what it parked, and
            // what a repair must publish again, is every frame it
            // published for the command, one or two.
            assert!(
                (1..=2).contains(&self.published[i]),
                "follower {i} parked {} frames",
                self.published[i]
            );
        }
        submit
    }
}

#[derive(Debug)]
struct RoundShape {
    peer: usize,
    frontend: usize,
}

/// The core regression. The parked evidence expires, the duplicate
/// arrives afterwards, and the collector learns and releases from the
/// evidence the duplicate had the voters publish again -- with no peer
/// retransmission and no second execution.
#[test]
fn a_duplicate_after_the_hold_expired_repairs_the_callers_evidence() {
    let mut w = Cluster::new(256);
    let submit = w.run_to_half_held(1);
    let command = w.command(1);

    // The hold runs out with no submission to say where the evidence
    // goes: the followers let it go. Nothing reaches the collector.
    w.now += HOLD;
    let published = w.published.clone();
    assert_eq!(
        w.route_parked(),
        published[1] + published[2],
        "both followers let all their evidence go"
    );
    assert!(w.parked.iter().all(Parked::is_empty));
    assert!(w.collector.is_pending(&command));
    assert!(w.releases.is_empty());
    let counted_before = w.counted.len();

    // The submission finally reaches the followers. It is an exact
    // duplicate of what they learned from the leader, and each one
    // publishes its acknowledgement again: to the collector alone.
    for (i, &parked) in published.iter().enumerate().skip(1) {
        let shape = w.deliver_submission(i, &submit);
        assert_eq!(
            shape.peer, 0,
            "follower {i} retransmitted to its peers on a duplicate"
        );
        assert_eq!(
            shape.frontend, parked,
            "follower {i} published exactly what it had parked, once more"
        );
        assert_eq!(w.voters[i].origin_of(&command), Some(COLLECTOR));
    }
    w.settle();

    assert_eq!(w.executed(), [1, 1, 1], "nothing executed a second time");
    assert!(w.parked.iter().all(Parked::is_empty));
    assert_eq!(
        w.counted.len() - counted_before,
        published[1] + published[2],
        "exactly the repaired acknowledgements reached the collector"
    );
    assert!(w.rejected.is_empty(), "{:?}", w.rejected);
    assert_eq!(w.releases.len(), 1, "{:?}", w.progress);
    let release = &w.releases[0];
    assert_eq!(release.command, command);
    assert!(release.fast, "the fast set {{0, 1}} was complete");
    assert!(!release.speculative);
    assert!(!w.collector.is_pending(&command));
    assert!(
        w.rejections()
            .iter()
            .all(|why| !why.contains("ReplayRefused")),
        "a repair was refused"
    );
    assert_eq!(
        w.voters[1].node().held(),
        0,
        "nothing left waiting in the outbox"
    );
}

/// The same, with the evidence crowded out by depth rather than let go
/// by time: a second command's acknowledgement pushes the first's out
/// with no time having passed.
#[test]
fn a_duplicate_after_the_depth_crowded_it_out_repairs_the_callers_evidence() {
    // Room for one frame per follower: the second command's evidence
    // takes the first's place.
    let mut w = Cluster::new(1);
    let first = w.run_to_half_held(1);
    let c1 = w.command(1);
    let published = w.published.clone();

    let second = w.submit(2);
    w.deliver_submission(0, &second);
    w.settle();
    let c2 = w.command(2);
    for i in 1..3 {
        assert!(
            w.parked[i].commands().all(|c| *c == c2) && !w.parked[i].is_empty(),
            "follower {i} holds the second command's evidence and crowded out the first's"
        );
    }
    assert!(w.collector.is_pending(&c1));
    assert!(w.collector.is_pending(&c2));
    assert!(w.releases.is_empty());
    assert_eq!(
        w.route_parked(),
        0,
        "no time passed: nothing expired, the first's evidence was crowded out"
    );

    // The first's submission arrives too late for the park and repairs
    // instead; the second is still waiting for its own.
    for (i, &parked) in published.iter().enumerate().skip(1) {
        let shape = w.deliver_submission(i, &first);
        assert_eq!((shape.peer, shape.frontend), (0, parked));
    }
    w.settle();
    assert_eq!(w.releases.len(), 1, "{:?}", w.progress);
    assert_eq!(w.releases[0].command, c1);
    assert!(w.collector.is_pending(&c2));
    assert_eq!(w.executed(), [2, 2, 2]);
    assert!(w.rejected.is_empty(), "{:?}", w.rejected);
}

/// The negative control. The same schedule with the duplicate's output
/// thrown away is exactly what the code before task-c02 did -- a
/// duplicate had no effects -- and the caller never completes. This is
/// what the tests above turn into if the repair is reverted.
#[test]
fn without_the_repaired_evidence_the_caller_waits_for_ever() {
    let mut w = Cluster::new(256);
    let submit = w.run_to_half_held(1);
    let command = w.command(1);
    w.now += HOLD;
    w.route_parked();

    for i in 1..3 {
        let out = w.voters[i]
            .on_submission(PeerRole::Frontend, &frame(&submit), COLLECTOR)
            .expect("driven")
            .expect("admitted");
        // What a no-effects duplicate would have produced.
        drop(out);
    }
    w.now += HOLD * 10;
    w.settle();

    assert!(
        w.collector.is_pending(&command),
        "the collector released without the followers' evidence"
    );
    assert!(w.releases.is_empty());
    assert!(
        w.collector
            .half_established()
            .iter()
            .any(|(c, _)| *c == command),
        "the collector holds the release and not the votes"
    );
}

/// When the voter's retained evidence cannot be replayed, the refusal is
/// explicit and the fallback is this node's durable record: the
/// collector completes the request from what the node itself executed,
/// corroborated by the release it holds. The bound on repairs is the
/// cheapest way to reach a refusal on a real cluster; `Forgotten` and
/// `NothingToReplay` take the same path.
#[test]
fn a_refused_repair_falls_back_to_the_durable_record() {
    let mut w = Cluster::new(256);
    let submit = w.run_to_half_held(1);
    let command = w.command(1);
    w.now += HOLD;
    w.route_parked();

    // Nothing to settle from before the record exists is not this
    // test's case -- every voter executed -- but a key nobody has a
    // record for is left out rather than invented.
    let none = records_for(w.voters[0].node().applier(), [(command, retry_key(9))]);
    assert!(
        none.is_empty(),
        "a record was invented for a key never executed"
    );

    // The repairs are spent: each duplicate publishes the evidence
    // again and each time it is lost on the way (thrown away here).
    for _ in 0..MAX_EVIDENCE_REPAIRS {
        let out = w.voters[1]
            .on_submission(PeerRole::Frontend, &frame(&submit), COLLECTOR)
            .expect("driven")
            .expect("admitted");
        assert_eq!(out.frontend.len(), w.published[1]);
        assert!(out.peer.is_empty());
    }
    let shape = w.deliver_submission(1, &submit);
    assert_eq!(
        (shape.peer, shape.frontend),
        (0, 0),
        "past the bound the duplicate publishes nothing"
    );
    let refusals: Vec<String> = w
        .rejections()
        .into_iter()
        .filter(|why| why.contains("ReplayRefused"))
        .collect();
    assert_eq!(refusals.len(), 1, "{refusals:?}");
    assert!(refusals[0].contains("TooMany"), "{refusals:?}");
    assert!(w.collector.is_pending(&command));

    // The consumer of that refusal: the collector's half-held commands,
    // looked up in this node's own store. The record is under the
    // command's identity, and it says what the leader released.
    let half = w.collector.half_established();
    assert_eq!(half, vec![(command, retry_key(1))]);
    let records = records_for(w.voters[0].node().applier(), half.clone());
    assert_eq!(records.len(), 1);
    let (found, record) = &records[0];
    assert_eq!(*found, command);
    // A follower that executed the same command holds the same record:
    // whichever node the collector runs on answers the same way.
    let on_follower = records_for(w.voters[2].node().applier(), half);
    assert_eq!(on_follower[0].1.result_digest, record.result_digest);
    assert_eq!(on_follower[0].1.response, record.response);

    let progress = w
        .collector
        .settle_from_record(
            command,
            record.result_digest,
            record.revision,
            &record.response,
        )
        .expect("settled");
    let Progress::Released(release) = progress else {
        panic!("not released: {progress:?}");
    };
    assert_eq!(release.command, command);
    assert!(!release.fast, "no fast quorum was ever counted");
    assert!(!release.speculative);
    assert!(!w.collector.is_pending(&command));
    assert!(w.collector.trace().iter().any(|e| matches!(
        e,
        CollectorEvent::SettledFromRecord { corroborated, .. } if corroborated == "release"
    )));
    // Settled once; a second look finds nothing half held.
    assert!(w.collector.half_established().is_empty());
    assert_eq!(
        w.collector.settle_from_record(
            command,
            record.result_digest,
            record.revision,
            &record.response
        ),
        Ok(Progress::Settled)
    );
    assert_eq!(w.executed(), [1, 1, 1]);
}

/// The other half. The votes arrived and the release did not: the
/// collector has learned and cannot answer, and no voter's duplicate
/// path can help, because a release is not evidence and is not
/// replayed. The durable record closes this half too, corroborated by
/// the votes.
#[test]
fn a_lost_release_is_settled_from_the_durable_record() {
    let mut w = Cluster::new(256);
    w.lose = Lose::Releases;
    let submit = w.submit(1);
    let command = w.command(1);
    for i in 0..3 {
        w.deliver_submission(i, &submit);
    }
    w.settle();

    assert_eq!(w.executed(), [1, 1, 1]);
    assert!(w.releases.is_empty());
    assert!(
        w.progress
            .iter()
            .any(|(_, p)| matches!(p, Progress::Held(HoldReason::AwaitingRelease)))
    );
    let half = w.collector.half_established();
    assert_eq!(half, vec![(command, retry_key(1))]);

    let records = records_for(w.voters[0].node().applier(), half);
    let (_, record) = &records[0];
    let progress = w
        .collector
        .settle_from_record(
            command,
            record.result_digest,
            record.revision,
            &record.response,
        )
        .expect("settled");
    let Progress::Released(release) = progress else {
        panic!("not released: {progress:?}");
    };
    assert_eq!(release.command, command);
    assert!(
        release.fast,
        "the fast set was counted before the release was lost"
    );
    assert!(w.collector.trace().iter().any(|e| matches!(
        e,
        CollectorEvent::SettledFromRecord { corroborated, .. } if corroborated == "votes"
    )));
}

/// A command the collector counted nothing of its own for is not
/// settled from the record: that is a caller's retry, answered before
/// anything is submitted, and not this path.
#[test]
fn the_record_alone_settles_nothing() {
    let mut w = Cluster::new(256);
    w.lose = Lose::Releases;
    let submit = w.submit(1);
    let command = w.command(1);
    // Only the leader hears of it; it cannot learn alone, so the
    // collector holds nothing of its own.
    w.deliver_submission(0, &submit);
    // No settle: the followers never answer, the leader never executes.
    assert!(w.collector.half_established().is_empty());
    assert_eq!(
        w.collector
            .settle_from_record(command, Digest32([0; 32]), None, b""),
        Err(SettleError::Uncorroborated)
    );
    assert!(w.collector.is_pending(&command));
}

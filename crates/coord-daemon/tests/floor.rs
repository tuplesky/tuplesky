//! task-d27: three voters agree a forgetting floor, end to end in one
//! process.
//!
//! The real machines over real (model-engine) stores, driven through the
//! real voter door, with the floor on every voter and a boundary every
//! four executed positions. Each voter exports at the boundary, keeps the
//! image, journals its promise and sends it; its peers record it; a
//! majority promising one checkpoint activates the floor on each.

use std::collections::BTreeSet;

use coord_checkpoint::{CheckpointOrigin, CheckpointReadinessV1};
use coord_collector::{Collector, CollectorConfig, MonotonicMillis, Submitted};
use coord_consensus::{
    BallotConfiguration, ConfigurationIdentity, Follower, FollowerConfig, Leader, LeaderConfig,
    LearningMode, ReplicaRole,
};
use coord_core::capability::{AdmissionReceipt, AttestedAdmission, VerifierToken};
use coord_core::effect::{BootId, PeerId};
use coord_core::event::{AdmittedRequest, PeerProvenance};
use coord_core::outbox::BarrierAllocator;
use coord_daemon::floor::{Floor, FloorRefusal, FloorSettings};
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

const CAPACITY: usize = 64;

/// A boundary every four executed positions.
const INTERVAL: u64 = 4;

fn settings(me: u8, images: &std::path::Path, headroom_bytes: u64) -> FloorSettings {
    FloorSettings {
        interval: INTERVAL,
        images: images.join(format!("voter-{me}")),
        headroom_bytes,
        origin: CheckpointOrigin {
            cluster: CLUSTER,
            domain: DOMAIN,
        },
        voters: (0..3).map(r).collect(),
        me: r(me),
    }
}

fn open_floor(voter: &Voter<StoreWorker<ModelEngine>>, settings: FloorSettings) -> Floor {
    let applier = voter.node().applier();
    let gated = applier.store().reader().snapshot().expect("snapshot");
    Floor::open(settings, gated.view(), inc(), applier.store().boot()).expect("floor")
}

struct Cluster {
    voters: Vec<Voter<StoreWorker<ModelEngine>>>,
    collector: Collector,
    peers: std::collections::VecDeque<(usize, PeerProvenance, Vec<u8>)>,
    sequence: u64,
    _images: tempfile::TempDir,
}

impl Cluster {
    /// Three voters taking part, each with `headroom[i]` bytes to keep.
    fn new(headroom: [u64; 3]) -> Self {
        let images = tempfile::tempdir().expect("tempdir");
        let voters = (0..3u8)
            .map(|me| {
                let mut voter = voter(me, CAPACITY);
                let floor = open_floor(
                    &voter,
                    settings(me, images.path(), headroom[usize::from(me)]),
                );
                voter.node_mut().keep_floor(floor);
                voter
            })
            .collect();
        Cluster {
            voters,
            collector: Collector::new(CollectorConfig {
                quorum: quorum(),
                max_pending: 1 << 16,
                max_resolved: 16,
                max_undelivered_bytes: usize::MAX,
            }),
            peers: std::collections::VecDeque::new(),
            sequence: 0,
            _images: images,
        }
    }

    fn images(&self) -> &std::path::Path {
        self._images.path()
    }

    fn carry(&mut self, from: usize, out: Outbound) {
        for (to, frame) in out.peer {
            let prov = self.voters[from].provenance();
            self.peers
                .push_back((usize::from(to.replica.0[0]), prov, frame));
        }
    }

    /// One client request, submitted to every voter.
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

    /// The peer plane and execution until nothing moves.
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

    fn run(&mut self, commands: u64) {
        for _ in 0..commands {
            self.submit();
            self.settle();
        }
    }

    fn floor(&self, i: usize) -> &Floor {
        self.voters[i].node().floor().expect("taking part")
    }

    fn executed_through(&self, i: usize) -> u64 {
        self.voters[i].node().machine().executed_through().get()
    }

    /// The certificate voter `i` journaled, read back from its store.
    fn published(&self, i: usize) -> Option<coord_checkpoint::ActivatedFloorV1> {
        let applier = self.voters[i].node().applier();
        let gated = applier.store().reader().snapshot().expect("snapshot");
        coord_checkpoint::floor::published_activation(gated.view()).expect("read")
    }
}

/// The bootstrap took a position: the first command a voter executes
/// here is at the second.
fn boundaries_through(position: u64) -> u64 {
    position / INTERVAL * INTERVAL
}

/// Three voters that executed the same commands promise the same
/// checkpoint at each boundary, and each activates the floor from a
/// majority of the promises, journaled where a restart reads it.
#[test]
fn three_voters_agree_a_floor_at_each_boundary() {
    let mut cluster = Cluster::new([0; 3]);
    cluster.run(9);
    let through = cluster.executed_through(0);
    assert!(through >= 2 * INTERVAL, "executed through {through}");
    let boundary = boundaries_through(through);
    for i in 0..3 {
        assert_eq!(cluster.executed_through(i), through, "voter {i} is behind");
        let floor = cluster.floor(i);
        assert_eq!(floor.last_refusal(), None, "voter {i} refused a boundary");
        assert_eq!(
            floor.promised().map(|p| p.get()),
            Some(boundary),
            "voter {i} did not promise the last boundary"
        );
        let activated = floor.activated().expect("activated");
        assert_eq!(activated.boundary.execution_position.get(), boundary);
        assert!(activated.signers.len() >= 2, "{:?}", activated.signers);
        // Journaled, not only held.
        assert_eq!(cluster.published(i).as_ref(), Some(activated));
        // Every peer's promise at every boundary was heard, none refused.
        assert_eq!(floor.counts.heard, 2 * (boundary / INTERVAL), "voter {i}");
        assert_eq!(floor.counts.rejected, 0, "voter {i}");
    }
    // One checkpoint: the same root everywhere, and every voter keeps
    // the image it promised about.
    let root = cluster.floor(0).activated().unwrap().root;
    for i in 0..3 {
        assert_eq!(cluster.floor(i).activated().unwrap().root, root);
        let kept =
            coord_checkpoint::SharedImageStore::open(&cluster.images().join(format!("voter-{i}")))
                .expect("images");
        assert!(kept.holds(&root), "voter {i} does not keep the image");
        // And only what a standing promise names: the first boundary's
        // image went when the second was promised.
        let images = std::fs::read_dir(kept.root()).expect("listed").count();
        assert_eq!(images, 1, "voter {i} keeps superseded images");
    }
}

/// A restarted voter reads back what it promised and the floor it
/// activated, so it neither promises below them nor forgets the floor.
#[test]
fn a_restarted_voter_reads_back_its_promise_and_its_floor() {
    let mut cluster = Cluster::new([0; 3]);
    cluster.run(5);
    let before = cluster.floor(1);
    let (promised, activated) = (before.promised(), before.activated().cloned());
    assert!(promised.is_some() && activated.is_some());
    let reopened = open_floor(&cluster.voters[1], settings(1, cluster.images(), 0));
    assert_eq!(reopened.promised(), promised);
    assert_eq!(reopened.activated().cloned(), activated);
    assert!(
        !reopened.due(promised.unwrap(), promised.unwrap()),
        "it would promise again"
    );
}

/// A voter short of disk headroom promises nothing, and says why; the
/// other two are a majority and activate without it.
#[test]
fn a_voter_short_of_headroom_promises_nothing_and_two_still_activate() {
    let mut cluster = Cluster::new([0, 0, u64::MAX]);
    cluster.run(5);
    let short = cluster.floor(2);
    assert_eq!(short.promised(), None);
    assert!(
        matches!(short.last_refusal(), Some(FloorRefusal::Headroom { .. })),
        "{:?}",
        short.last_refusal()
    );
    for i in 0..3 {
        let activated = cluster.floor(i).activated().expect("activated");
        assert_eq!(
            activated.signers,
            [r(0), r(1)].into_iter().collect(),
            "voter {i}"
        );
    }
}

/// A promise is taken only from the voter it names: one relayed over
/// another voter's link is refused, so no voter signs for another.
#[test]
fn a_promise_is_taken_only_from_the_voter_it_names() {
    let mut cluster = Cluster::new([0; 3]);
    cluster.run(5);
    let held = cluster.floor(0).counts;
    let theirs = CheckpointReadinessV1 {
        voter: r(1),
        boundary: coord_checkpoint::CheckpointBoundary {
            execution_position: coord_types::ids::ExecutionPosition::new(4 * INTERVAL).unwrap(),
            ..cluster.floor(0).activated().unwrap().boundary
        },
        cluster: CLUSTER,
        domain: DOMAIN,
        configuration: cluster.floor(0).activated().unwrap().configuration,
        root: Digest32([9; 32]),
    };
    let frame = coord_consensus::ProtocolMessage::FloorReadiness {
        readiness: theirs.encode().unwrap(),
    }
    .encode();
    // Over voter 2's link.
    let from_two = cluster.voters[2].provenance();
    cluster.voters[0].on_peer(from_two, frame).expect("driven");
    let after = cluster.floor(0).counts;
    assert_eq!(after.rejected, held.rejected + 1);
    assert_eq!(after.heard, held.heard);
}

/// Copy an images directory, image by image.
fn copy_images(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to).expect("created");
    for entry in std::fs::read_dir(from).expect("listed") {
        let entry = entry.expect("entry");
        let path = entry.path();
        if path.is_dir() {
            copy_images(&path, &to.join(entry.file_name()));
        } else {
            std::fs::copy(&path, to.join(entry.file_name())).expect("copied");
        }
    }
}

/// The images kept in `dir`.
fn kept(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir).expect("listed").count()
}

/// A superseded image stays until the batch that superseded it is
/// durable: until then, the rows naming it are what a crash leaves, and
/// the promise they record is about those bytes.
#[test]
fn a_superseded_image_is_kept_until_the_batch_that_supersedes_it_is_durable() {
    let mut cluster = Cluster::new([0; 3]);
    cluster.run(3);
    assert_eq!(cluster.executed_through(0), INTERVAL);
    // A floor of voter 0's store beside its own, with its own images.
    let alone = cluster.images().join("alone");
    copy_images(&cluster.images().join("voter-0"), &alone);
    let mut floor = open_floor(&cluster.voters[0], {
        let mut s = settings(0, cluster.images(), 0);
        s.images = alone.clone();
        s
    });
    let first = floor.activated().expect("activated").root;
    assert_eq!(floor.promised().map(|p| p.get()), Some(INTERVAL));

    cluster.run(4);
    assert_eq!(cluster.executed_through(0), 2 * INTERVAL);
    let applier = cluster.voters[0].node().applier();
    let gated = applier.store().reader().snapshot().expect("snapshot");
    let view = gated.view();
    let position = coord_types::ids::ExecutionPosition::new(2 * INTERVAL).unwrap();
    assert!(floor.due(position, position));
    floor.boundary(view, position).expect("promised");
    let promised = floor.barrier();
    // Voter 1's promise at the same boundary activates the floor there.
    let theirs = coord_checkpoint::read_readiness(view, &coord_checkpoint::TrimLimits::default())
        .expect("read")
        .into_iter()
        .find(|x| x.voter == r(1) && x.boundary.execution_position == position)
        .expect("voter 1 promised");
    // Heard while this voter's own promise is in flight, it activates
    // nothing yet: the own promise's row may never exist.
    floor
        .hear(view, r(1), &theirs.encode().unwrap())
        .expect("recorded");
    let heard = floor.barrier();
    assert_eq!(floor.activated().expect("activated").root, first);

    // Neither batch durable: nothing moves, both images stay.
    assert!(floor.settle(|_| false, |_| false).is_none());
    assert_eq!(kept(&alone), 2);
    // The promise durable: with voter 1's it activates the floor at the
    // boundary, in a batch of its own. The rows still name the first
    // image as the floor until that batch is durable.
    let updates = floor
        .settle(|b| *b == promised || *b == heard, |_| false)
        .expect("the floor activates");
    assert_eq!(updates.len(), 1);
    let activated = floor.barrier();
    let second = floor.activated().expect("activated").root;
    assert_ne!(first, second);
    assert_eq!(kept(&alone), 2);
    // All durable: nothing names the first image any more.
    assert!(
        floor
            .settle(
                |b| *b == promised || *b == heard || *b == activated,
                |_| false
            )
            .is_none()
    );
    assert_eq!(kept(&alone), 1);
    let store = coord_checkpoint::SharedImageStore::open(&alone).expect("images");
    assert!(store.holds(&second) && !store.holds(&first));
}

/// A batch that fails reclaims nothing, and neither does a later one
/// that is durable: the rows the failed batch would have written are not
/// there, so the old ones may still name the old image.
#[test]
fn a_failed_batch_reclaims_nothing() {
    let mut cluster = Cluster::new([0; 3]);
    cluster.run(3);
    let alone = cluster.images().join("alone");
    copy_images(&cluster.images().join("voter-0"), &alone);
    let mut floor = open_floor(&cluster.voters[0], {
        let mut s = settings(0, cluster.images(), 0);
        s.images = alone.clone();
        s
    });
    cluster.run(4);
    let applier = cluster.voters[0].node().applier();
    let gated = applier.store().reader().snapshot().expect("snapshot");
    let position = coord_types::ids::ExecutionPosition::new(2 * INTERVAL).unwrap();
    floor.boundary(gated.view(), position).expect("promised");
    let promised = floor.barrier();
    floor.settle(|_| false, |b| *b == promised);
    assert_eq!(kept(&alone), 2);
    // A later batch durable: the own promise its images name was never
    // written, so the first image is still the promise the rows hold.
    let later = floor.barrier();
    floor.settle(|b| *b == later, |_| false);
    assert_eq!(kept(&alone), 2);
}

/// A promise whose batch failed was never written: it activates nothing
/// with the promises heard afterwards, and the voter promises again at
/// its next boundary.
#[test]
fn an_own_promise_whose_batch_failed_counts_for_nothing() {
    let mut cluster = Cluster::new([0; 3]);
    cluster.run(3);
    let alone = cluster.images().join("alone");
    copy_images(&cluster.images().join("voter-0"), &alone);
    let mut floor = open_floor(&cluster.voters[0], {
        let mut s = settings(0, cluster.images(), 0);
        s.images = alone.clone();
        s
    });
    let first = floor.activated().expect("activated").boundary;
    cluster.run(4);
    let applier = cluster.voters[0].node().applier();
    let gated = applier.store().reader().snapshot().expect("snapshot");
    let view = gated.view();
    let position = coord_types::ids::ExecutionPosition::new(2 * INTERVAL).unwrap();
    floor.boundary(view, position).expect("promised");
    let promised = floor.barrier();
    assert!(floor.settle(|_| false, |b| *b == promised).is_none());
    // Rolled back to the promise the rows hold, and due again.
    assert_eq!(floor.promised().map(|p| p.get()), Some(INTERVAL));
    assert!(floor.due(position, position), "the failed promise stands");

    // Voter 1's promise at that boundary now activates nothing: with the
    // failed promise counted it would have made a majority.
    let theirs = coord_checkpoint::read_readiness(view, &coord_checkpoint::TrimLimits::default())
        .expect("read")
        .into_iter()
        .find(|x| x.voter == r(1) && x.boundary.execution_position == position)
        .expect("voter 1 promised");
    let rows = floor
        .hear(view, r(1), &theirs.encode().unwrap())
        .expect("recorded");
    assert_eq!(rows.len(), 1, "an activation counting the failed promise");
    assert_eq!(floor.activated().expect("activated").boundary, first);
}

/// A retry answered from the record reports its original position, which
/// the frontier is past: it is no boundary, and nothing is exported.
#[test]
fn a_position_behind_the_frontier_is_no_boundary() {
    let cluster = Cluster::new([0; 3]);
    let floor = cluster.floor(0);
    let boundary = coord_types::ids::ExecutionPosition::new(INTERVAL).unwrap();
    let past = coord_types::ids::ExecutionPosition::new(INTERVAL + 1).unwrap();
    assert!(floor.due(boundary, boundary));
    assert!(!floor.due(boundary, past));
}

/// A voter whose promised image is gone or damaged does not reopen its
/// floor: it would serve on advertising a promise it cannot keep.
#[test]
fn a_voter_whose_promised_image_is_lost_does_not_reopen_its_floor() {
    let mut cluster = Cluster::new([0; 3]);
    cluster.run(5);
    let promised = cluster.floor(1).promised().expect("promised");

    // Gone: an empty images directory.
    let applier = cluster.voters[1].node().applier();
    let gated = applier.store().reader().snapshot().expect("snapshot");
    let mut empty = settings(1, cluster.images(), 0);
    empty.images = cluster.images().join("empty");
    let refused = Floor::open(empty, gated.view(), inc(), applier.store().boot())
        .expect_err("opened without its image");
    assert!(refused.contains("cannot be read back"), "{refused}");
    assert!(refused.contains(&promised.get().to_string()), "{refused}");
    // The operator's way back: the refusal names the image, and a peer
    // that promised the same boundary keeps it under the same name.
    let root = refused
        .strip_prefix("the image ")
        .and_then(|rest| rest.split(' ').next())
        .expect("the refusal names the image");
    assert_eq!(cluster.floor(0).promised(), Some(promised));
    let restored = cluster.images().join("restored");
    std::fs::create_dir_all(&restored).expect("created");
    copy_images(
        &cluster.images().join("voter-0").join(root),
        &restored.join(root),
    );
    let mut from_peer = settings(1, cluster.images(), 0);
    from_peer.images = restored;
    Floor::open(from_peer, gated.view(), inc(), applier.store().boot())
        .expect("reopened with the peer's copy");

    // Damaged: a chunk cut short.
    let damaged = cluster.images().join("damaged");
    copy_images(&cluster.images().join("voter-1"), &damaged);
    let image = std::fs::read_dir(&damaged)
        .expect("listed")
        .map(|e| e.expect("entry").path())
        .find(|p| p.is_dir())
        .expect("an image");
    let chunk = std::fs::read_dir(&image)
        .expect("listed")
        .map(|e| e.expect("entry").path())
        .find(|p| {
            p.file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("chunk-")
        })
        .expect("a chunk");
    let bytes = std::fs::read(&chunk).expect("read");
    std::fs::write(&chunk, &bytes[..bytes.len() - 1]).expect("cut");
    let mut cut = settings(1, cluster.images(), 0);
    cut.images = damaged;
    let refused = Floor::open(cut, gated.view(), inc(), applier.store().boot())
        .expect_err("opened with a damaged image");
    assert!(refused.contains("cannot be read back"), "{refused}");

    // Intact, it reopens.
    open_floor(&cluster.voters[1], settings(1, cluster.images(), 0));
}

#[test]
fn common_state_over_the_inline_cap_refuses_the_floor_at_start() {
    use coord_daemon::floor::INLINE_EXPORT_CAP_BYTES;
    use coord_store_api::engine::{LocalEngine, SnapshotSource, WriteTxn};
    use coord_store_api::registry::Collection;

    let mut engine = ModelEngine::new();
    let value = vec![b'v'; 64 * 1024];
    let rows = INLINE_EXPORT_CAP_BYTES as usize / value.len() + 1;
    let mut tx = engine.begin_write().unwrap();
    for i in 0..rows {
        tx.put(
            Collection::KvCurrentV1.id(),
            format!("key-{i:06}").as_bytes(),
            &value,
        )
        .unwrap();
    }
    tx.commit_durable().unwrap();
    let images = tempfile::tempdir().unwrap();
    let view = engine.reader().snapshot().unwrap();
    let refused = Floor::open(settings(0, images.path(), 0), &view, inc(), BootId([9; 16]))
        .expect_err("refused");
    assert!(refused.contains("[floor] enabled = false"), "{refused}");
    // Nothing was created for a floor that does not start.
    assert!(!images.path().join("voter-0").exists());
}

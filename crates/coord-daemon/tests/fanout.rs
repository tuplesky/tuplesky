//! Acceptance for offering a submission to the voters (task-43,
//! task-j08).
//!
//! The collector plans which voters a submission must reach; the
//! committed configuration decides which generation of each of them may
//! receive it, and -- for a voter running in this very process --
//! whether the short path applies at all. These tests hold that binding,
//! hold that a voter this process cannot reach is a voter that
//! contributes nothing rather than a failed submission, and hold that
//! the local route is a capability the configuration validates rather
//! than a name a request can claim.

use std::cell::RefCell;

use coord_collector::FanOut;
use coord_daemon::fanout::{LocalIngress, NotQueued, PeerFanOut, Route, Saturated, dispatch};
use coord_daemon::mailbox::{Ingress, IngressBudget};
use coord_membership::genesis::{GenesisManifest, VoterSeed};
use coord_membership::membership::Membership;
use coord_transport::SendError;
use coord_types::CommandId;
use coord_types::RetryKey;
use coord_types::identity::Digest32;
use coord_types::ids::{
    ClientInstanceId, ClusterId, DomainId, ReplicaId, ReplicaIncarnation, RequestSequence,
    SessionId,
};

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

const DOMAIN: DomainId = DomainId([0x22; 16]);

fn replica(n: u8) -> ReplicaId {
    ReplicaId([n; 16])
}

fn inc(n: u64) -> ReplicaIncarnation {
    ReplicaIncarnation::new(n).unwrap()
}

/// Three committed voters, each at a different incarnation so a test can
/// tell a looked-up incarnation from a guessed one.
fn membership() -> Membership {
    let manifest = GenesisManifest {
        cluster: hex(&[0x11; 16]),
        domain: hex(&DOMAIN.0),
        epoch: 1,
        voters: (1u8..=3)
            .map(|n| VoterSeed {
                node: hex(&[n; 16]),
                incarnation: u64::from(n) * 7,
                public_key: b64url(&[n; 32]),
            })
            .collect(),
        issuer_roots: vec![b64url(&[0xca; 8])],
        wif_rules: vec![serde_json::json!({ "issuer": "test" })],
        admin: hex(&[0xa; 16]),
        protocol_version: 1,
    };
    Membership::from_genesis(&manifest).expect("membership")
}

fn plan(targets: Vec<ReplicaId>) -> FanOut {
    FanOut {
        command: CommandId(Digest32([0xc; 32])),
        retry_key: RetryKey {
            cluster_id: ClusterId([0x11; 16]),
            domain_id: DOMAIN,
            session_id: SessionId([5; 16]),
            client_instance_id: ClientInstanceId([6; 16]),
            request_sequence: RequestSequence::new(1).unwrap(),
        },
        targets,
        frame: b"submit-frame".to_vec(),
    }
}

/// One call the daemon made: the targets it resolved, the domain it
/// named, and the exact bytes it handed over.
type Call = (Vec<(ReplicaId, ReplicaIncarnation)>, DomainId, Vec<u8>);

/// Records what it was asked to send, and fails whichever replicas it
/// was told to fail.
struct Peers {
    unreachable: Vec<ReplicaId>,
    seen: RefCell<Vec<Call>>,
}

impl Peers {
    fn new(unreachable: Vec<ReplicaId>) -> Self {
        Peers {
            unreachable,
            seen: RefCell::new(Vec::new()),
        }
    }
}

impl PeerFanOut for Peers {
    fn fan_out(
        &self,
        targets: &[(ReplicaId, ReplicaIncarnation)],
        group: DomainId,
        frame: &[u8],
    ) -> Vec<Result<(), SendError>> {
        self.seen
            .borrow_mut()
            .push((targets.to_vec(), group, frame.to_vec()));
        targets
            .iter()
            .map(|(r, _)| {
                if self.unreachable.contains(r) {
                    Err(SendError::NotConnected)
                } else {
                    Ok(())
                }
            })
            .collect()
    }
}

/// A `LocalIngress` the real constructor would never produce: it names
/// whatever replica, incarnation and domain a test tells it to.
///
/// Its existence is the point. `Ingress::new` takes its incarnation from
/// the committed membership and exists only for a committed voter, so a
/// stale or foreign local route cannot be built through it; this is how
/// a test asks what `dispatch` would do if one somehow were.
struct Claimed {
    replica: ReplicaId,
    incarnation: ReplicaIncarnation,
    domain: DomainId,
    taken: RefCell<Vec<Vec<u8>>>,
}

impl Claimed {
    fn new(replica: ReplicaId, incarnation: ReplicaIncarnation, domain: DomainId) -> Self {
        Claimed {
            replica,
            incarnation,
            domain,
            taken: RefCell::new(Vec::new()),
        }
    }
}

impl LocalIngress for Claimed {
    fn replica(&self) -> ReplicaId {
        self.replica
    }
    fn incarnation(&self) -> ReplicaIncarnation {
        self.incarnation
    }
    fn domain(&self) -> DomainId {
        self.domain
    }
    fn offer(&self, frame: &[u8]) -> Result<(), Saturated> {
        self.taken.borrow_mut().push(frame.to_vec());
        Ok(())
    }
}

/// Every committed voter is offered the frame, at the incarnation the
/// configuration names, with the collector's exact bytes.
#[test]
fn every_committed_voter_receives_the_frame_at_its_committed_incarnation() {
    let membership = membership();
    let peers = Peers::new(Vec::new());
    let plan = plan(vec![replica(1), replica(2), replica(3)]);

    let out = dispatch(&membership, &peers, None, &plan);

    assert_eq!(
        out.queued
            .iter()
            .map(|q| (q.replica, q.incarnation, q.route))
            .collect::<Vec<_>>(),
        vec![
            (replica(1), inc(7), Route::Remote),
            (replica(2), inc(14), Route::Remote),
            (replica(3), inc(21), Route::Remote),
        ],
        "each voter at the incarnation the configuration names"
    );
    assert_eq!(out.queued_remote(), 3);
    assert_eq!(out.queued_local(), 0, "this process runs no voter");
    assert!(out.rejected.is_empty());

    // One call, every target at once: a fan-out is parallel, not a
    // sequence of sends that could serialize behind a slow peer.
    let seen = peers.seen.borrow();
    assert_eq!(seen.len(), 1, "one fan-out, not one call per voter");
    assert_eq!(seen[0].1, DOMAIN);
    assert_eq!(seen[0].2, b"submit-frame", "the collector's exact frame");
}

/// A target the committed configuration does not name as a voter is not
/// offered the frame at all. The incarnation is looked up rather than
/// carried, so there is no generation of an unknown node to fall back to.
#[test]
fn a_target_that_is_not_a_committed_voter_is_never_offered_the_frame() {
    let membership = membership();
    let peers = Peers::new(Vec::new());
    let stranger = replica(9);
    let plan = plan(vec![replica(1), stranger, replica(2)]);

    let out = dispatch(&membership, &peers, None, &plan);

    assert_eq!(
        out.rejected,
        vec![(stranger, NotQueued::NotACommittedVoter)],
        "a disagreement between the plan and the configuration, \
         and not confused with a full or unreachable destination"
    );
    assert_eq!(out.not_a_committed_voter(), 1);
    assert_eq!(out.saturated(), 0);
    assert_eq!(out.unavailable(), 0);

    let seen = peers.seen.borrow();
    assert!(
        seen[0].0.iter().all(|(r, _)| *r != stranger),
        "the stranger was not handed to the transport at all: {:?}",
        seen[0].0
    );
    // The voters that are committed still got it: one bad target in a
    // plan does not stop the submission.
    assert_eq!(out.queued_remote(), 2);
}

/// A voter this process cannot reach contributes no evidence and nothing
/// more. The others still receive the frame, and the submission is not
/// failed: only the quorum rule decides whether enough answered.
#[test]
fn an_unreachable_voter_does_not_fail_the_submission() {
    let membership = membership();
    let peers = Peers::new(vec![replica(2)]);
    let plan = plan(vec![replica(1), replica(2), replica(3)]);

    let out = dispatch(&membership, &peers, None, &plan);

    assert_eq!(
        out.queued.iter().map(|q| q.replica).collect::<Vec<_>>(),
        vec![replica(1), replica(3)]
    );
    assert_eq!(
        out.rejected,
        vec![(replica(2), NotQueued::Unavailable(SendError::NotConnected))]
    );
    assert_eq!(out.unavailable(), 1);
    assert_eq!(
        out.not_a_committed_voter(),
        0,
        "a network fact, not a configuration disagreement"
    );

    // Every target was still offered to the transport: unreachability is
    // the transport's answer, not a decision taken before asking.
    let seen = peers.seen.borrow();
    assert_eq!(seen[0].0.len(), 3);
}

/// A plan whose targets are all strangers offers nothing, and says so
/// rather than reporting a successful fan-out to nobody.
#[test]
fn a_plan_of_strangers_offers_nothing() {
    let membership = membership();
    let peers = Peers::new(Vec::new());
    let plan = plan(vec![replica(8), replica(9)]);

    let out = dispatch(&membership, &peers, None, &plan);

    assert!(out.queued.is_empty());
    assert_eq!(out.not_a_committed_voter(), 2);
    assert!(peers.seen.borrow()[0].0.is_empty());
}

/// A voter running in this process is delivered to through its own
/// ingress, and is not dialled: the transport is never asked for it.
///
/// The other voters are unaffected -- the frame they get is the same
/// bytes the local voter got, because the local route skips the wire and
/// not the content.
#[test]
fn a_co_located_voter_is_delivered_to_without_a_network_hop() {
    let membership = membership();
    let peers = Peers::new(Vec::new());
    let mine = Ingress::new(&membership, replica(1), IngressBudget::default())
        .expect("replica 1 is a committed voter");
    let route = mine.route();
    let plan = plan(vec![replica(1), replica(2), replica(3)]);

    let out = dispatch(&membership, &peers, Some(&route), &plan);

    assert_eq!(out.queued_local(), 1);
    assert_eq!(out.queued_remote(), 2);
    assert_eq!(
        out.queued[0],
        coord_daemon::fanout::Queued {
            replica: replica(1),
            incarnation: inc(7),
            route: Route::Local,
        },
        "the local voter still at the incarnation the configuration names"
    );

    let seen = peers.seen.borrow();
    assert_eq!(
        seen[0].0,
        vec![(replica(2), inc(14)), (replica(3), inc(21))],
        "this process did not dial itself"
    );
    assert_eq!(
        mine.take(8),
        vec![b"submit-frame".to_vec()],
        "the voter's ingress holds the collector's exact frame, \
         waiting for the voter's own turn"
    );
}

/// The local route is not the frontend's opinion of who the voter is.
/// A runtime whose incarnation the configuration has superseded is not
/// this domain's voter any more, and the frame goes over the wire to
/// whoever is.
#[test]
fn a_local_route_at_a_superseded_incarnation_is_not_used() {
    let membership = membership();
    let peers = Peers::new(Vec::new());
    // The configuration says replica 1 votes at incarnation 7.
    let stale = Claimed::new(replica(1), inc(6), DOMAIN);
    let plan = plan(vec![replica(1)]);

    let out = dispatch(&membership, &peers, Some(&stale), &plan);

    assert_eq!(out.queued_local(), 0);
    assert_eq!(out.queued_remote(), 1);
    assert_eq!(out.queued[0].incarnation, inc(7));
    assert!(
        stale.taken.borrow().is_empty(),
        "a superseded runtime was not handed the frame"
    );
    assert_eq!(peers.seen.borrow()[0].0, vec![(replica(1), inc(7))]);
}

/// A local route belongs to one voter. Naming another replica does not
/// reach it, however co-located the process happens to be.
#[test]
fn a_local_route_never_takes_another_voters_frame() {
    let membership = membership();
    let peers = Peers::new(Vec::new());
    let mine = Ingress::new(&membership, replica(1), IngressBudget::default()).expect("voter");
    let route = mine.route();
    let plan = plan(vec![replica(2), replica(3)]);

    let out = dispatch(&membership, &peers, Some(&route), &plan);

    assert_eq!(out.queued_local(), 0);
    assert_eq!(out.queued_remote(), 2);
    assert_eq!(
        mine.depth(),
        0,
        "the local voter was not named and got nothing"
    );
}

/// A local route into another domain's runtime is not a route at all.
/// The membership that decides the target is this domain's, so a
/// capability from elsewhere is ignored rather than trusted for its
/// replica id.
#[test]
fn a_local_route_in_another_domain_is_not_used() {
    let membership = membership();
    let peers = Peers::new(Vec::new());
    let elsewhere = Claimed::new(replica(1), inc(7), DomainId([0x33; 16]));
    let plan = plan(vec![replica(1)]);

    let out = dispatch(&membership, &peers, Some(&elsewhere), &plan);

    assert_eq!(out.queued_remote(), 1);
    assert!(elsewhere.taken.borrow().is_empty());
}

/// A plan naming a replica the configuration does not know is refused
/// before the local route is even considered. A process cannot deliver
/// to its own voter a frame the configuration says that voter may not
/// have.
#[test]
fn a_local_route_does_not_rescue_a_target_the_configuration_rejects() {
    let membership = membership();
    let peers = Peers::new(Vec::new());
    let stranger = replica(9);
    let claimed = Claimed::new(stranger, inc(1), DOMAIN);
    let plan = plan(vec![stranger]);

    let out = dispatch(&membership, &peers, Some(&claimed), &plan);

    assert!(out.queued.is_empty());
    assert_eq!(
        out.rejected,
        vec![(stranger, NotQueued::NotACommittedVoter)]
    );
    assert!(claimed.taken.borrow().is_empty());
}

/// A full local ingress refuses, and that refusal is its own reason --
/// a live voter under load, not an absent one. Crucially it does not
/// stop the voters that could have taken the frame from getting it.
#[test]
fn a_full_local_ingress_does_not_hold_up_the_remote_voters() {
    let membership = membership();
    let peers = Peers::new(Vec::new());
    let mine = Ingress::new(
        &membership,
        replica(1),
        IngressBudget {
            frames: 1,
            bytes: 1 << 20,
        },
    )
    .expect("voter");
    let route = mine.route();
    // The voter has not had its turn yet, and its one slot is taken.
    assert!(LocalIngress::offer(&route, b"earlier").is_ok());
    let plan = plan(vec![replica(1), replica(2), replica(3)]);

    let out = dispatch(&membership, &peers, Some(&route), &plan);

    assert_eq!(out.rejected, vec![(replica(1), NotQueued::Saturated)]);
    assert_eq!(out.saturated(), 1);
    assert_eq!(out.unavailable(), 0, "a full queue is not an absent peer");
    assert_eq!(out.not_a_committed_voter(), 0);
    assert_eq!(
        out.queued_remote(),
        2,
        "local backpressure did not stop the remote fan-out"
    );
    assert_eq!(
        peers.seen.borrow()[0].0,
        vec![(replica(2), inc(14)), (replica(3), inc(21))]
    );
}

/// A transport that answered fewer targets than it was handed has said
/// nothing about the rest. They are reported as unreached rather than
/// quietly counted as queued, which would inflate the fan-out against a
/// quorum rule that trusts it.
#[test]
fn a_transport_that_answers_short_leaves_no_target_counted_as_queued() {
    struct Silent;
    impl PeerFanOut for Silent {
        fn fan_out(
            &self,
            _targets: &[(ReplicaId, ReplicaIncarnation)],
            _group: DomainId,
            _frame: &[u8],
        ) -> Vec<Result<(), SendError>> {
            vec![Ok(())]
        }
    }
    let membership = membership();
    let plan = plan(vec![replica(1), replica(2), replica(3)]);

    let out = dispatch(&membership, &Silent, None, &plan);

    assert_eq!(out.queued_remote(), 1);
    assert_eq!(out.unavailable(), 2);
}

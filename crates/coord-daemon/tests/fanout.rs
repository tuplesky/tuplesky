//! Acceptance for sending a submission to the voters (task-43).
//!
//! The collector plans which voters a submission must reach; the
//! committed configuration decides which generation of each of them may
//! receive it. These tests hold that binding, and hold that a voter this
//! process cannot reach is a voter that contributes nothing -- not a
//! failed submission.

use std::cell::RefCell;

use coord_collector::FanOut;
use coord_daemon::fanout::{PeerFanOut, dispatch};
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

/// Every committed voter is sent to, at the incarnation the
/// configuration names, with the collector's exact frame.
#[test]
fn every_committed_voter_receives_the_frame_at_its_committed_incarnation() {
    let membership = membership();
    let peers = Peers::new(Vec::new());
    let plan = plan(vec![replica(1), replica(2), replica(3)]);

    let out = dispatch(&membership, &peers, &plan);

    assert_eq!(
        out.sent,
        vec![
            (replica(1), inc(7)),
            (replica(2), inc(14)),
            (replica(3), inc(21))
        ],
        "each voter at the incarnation the configuration names"
    );
    assert_eq!(out.reached(), 3);
    assert!(out.unreachable.is_empty());
    assert!(out.not_a_voter.is_empty());

    // One call, every target at once: a fan-out is parallel, not a
    // sequence of sends that could serialize behind a slow peer.
    let seen = peers.seen.borrow();
    assert_eq!(seen.len(), 1, "one fan-out, not one call per voter");
    assert_eq!(seen[0].1, DOMAIN);
    assert_eq!(seen[0].2, b"submit-frame", "the collector's exact frame");
}

/// A target the committed configuration does not name as a voter is not
/// sent to at all. The incarnation is looked up rather than carried, so
/// there is no generation of an unknown node to fall back to.
#[test]
fn a_target_that_is_not_a_committed_voter_is_never_sent_to() {
    let membership = membership();
    let peers = Peers::new(Vec::new());
    let stranger = replica(9);
    let plan = plan(vec![replica(1), stranger, replica(2)]);

    let out = dispatch(&membership, &peers, &plan);

    assert_eq!(out.not_a_voter, vec![stranger]);
    assert_eq!(out.sent, vec![(replica(1), inc(7)), (replica(2), inc(14))]);

    let seen = peers.seen.borrow();
    assert!(
        seen[0].0.iter().all(|(r, _)| *r != stranger),
        "the stranger was not handed to the transport at all: {:?}",
        seen[0].0
    );
    // The voters that are committed still got it: one bad target in a
    // plan does not stop the submission.
    assert_eq!(out.reached(), 2);
}

/// A voter this process cannot reach contributes no evidence and nothing
/// more. The others still receive the frame, and the submission is not
/// failed: only the quorum rule decides whether enough answered.
#[test]
fn an_unreachable_voter_does_not_fail_the_submission() {
    let membership = membership();
    let peers = Peers::new(vec![replica(2)]);
    let plan = plan(vec![replica(1), replica(2), replica(3)]);

    let out = dispatch(&membership, &peers, &plan);

    assert_eq!(out.sent, vec![(replica(1), inc(7)), (replica(3), inc(21))]);
    assert_eq!(out.unreachable, vec![(replica(2), SendError::NotConnected)]);
    assert_eq!(out.reached(), 2, "the reachable voters still got it");
    assert!(out.not_a_voter.is_empty());

    // Every target was still offered to the transport: unreachability is
    // the transport's answer, not a decision taken before asking.
    let seen = peers.seen.borrow();
    assert_eq!(seen[0].0.len(), 3);
}

/// A plan whose targets are all strangers sends nothing, and says so
/// rather than reporting a successful fan-out to nobody.
#[test]
fn a_plan_of_strangers_sends_nothing() {
    let membership = membership();
    let peers = Peers::new(Vec::new());
    let plan = plan(vec![replica(8), replica(9)]);

    let out = dispatch(&membership, &peers, &plan);

    assert_eq!(out.reached(), 0);
    assert_eq!(out.not_a_voter, vec![replica(8), replica(9)]);
    assert!(out.sent.is_empty());
    assert!(peers.seen.borrow()[0].0.is_empty());
}

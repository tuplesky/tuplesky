//! Acceptance for binding an admission to the command that was accepted
//! (task-j09; design Sections 4.1, 9.3).
//!
//! A command's identity is its retry key and its canonical request, and
//! deliberately not the credential that admitted it: a retry under a
//! rotated credential has to stay the same command. That leaves the
//! attested facts -- who was authenticated, under which trust rule,
//! within what limits -- outside the identity although they decide what
//! execution does with the command. So they are bound to the
//! acknowledgement instead, and these are the three ways that has to
//! hold:
//!
//! * a second presentation of one command under other facts does not
//!   replace what this replica accepted;
//! * a leader proposal that disagrees with a payload this replica holds
//!   is refused rather than held;
//! * a quorum cannot form across senders that accepted one identity as
//!   different facts.

use std::collections::BTreeSet;

use coord_consensus::{
    BallotConfiguration, ConfigurationIdentity, FastAck, Follower, FollowerConfig,
    FollowerRejection, ProtocolMessage, ReplicaRole, SlowAck, Vote, VoteError, VoteSet,
};
use coord_core::capability::{
    AdmissionFacts, AdmissionReceipt, AttestedAdmission, AttestedEstablishment, CredentialDeadline,
    VerifierToken, admission_digest,
};
use coord_core::effect::{BootId, Effect, PeerId};
use coord_core::event::{AdmittedRequest, AuthenticatedPeerMessage, Event, PeerProvenance};
use coord_core::machine::DeterministicMachine;
use coord_types::identity::Digest32;
use coord_types::ids::*;
use coord_types::logical_v1::{CanonicalOperation, LogicalRequest, PutOp};
use coord_types::wire_v1::{MessageV1, RequestV1};
use coord_types::{CommandId, RetryKey};

fn r(i: u8) -> ReplicaId {
    ReplicaId([i; 16])
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

const FRONTEND: PeerId = PeerId {
    replica: ReplicaId([0xf0; 16]),
    incarnation: ReplicaIncarnation::ZERO,
};

fn voters() -> BTreeSet<ReplicaId> {
    (0..3).map(r).collect()
}

fn quorum() -> BallotConfiguration {
    BallotConfiguration::c2(epoch(), ballot(), voters(), [r(0), r(1)].into()).unwrap()
}

fn booted(me: u8) -> Follower {
    let mut f = Follower::new(FollowerConfig {
        identity: ConfigurationIdentity {
            cluster: ClusterId([1; 16]),
            domain: DomainId([2; 16]),
            epoch: epoch(),
            voters: voters(),
            replica: r(me),
            incarnation: ReplicaIncarnation::new(1).unwrap(),
            role: ReplicaRole::Voter,
        },
        quorum: quorum(),
        genesis: ballot(),
        frontend: FRONTEND,
        capacity: 8,
    });
    assert!(
        f.step(Event::Boot {
            boot_id: BootId([1; 16]),
            incarnation: ReplicaIncarnation::new(1).unwrap(),
        })
        .is_empty()
    );
    f
}

fn retry_key() -> RetryKey {
    RetryKey {
        cluster_id: ClusterId([1; 16]),
        domain_id: DomainId([2; 16]),
        session_id: SessionId([3; 16]),
        client_instance_id: ClientInstanceId([4; 16]),
        request_sequence: RequestSequence::new(1).unwrap(),
    }
}

fn logical() -> LogicalRequest {
    LogicalRequest::new(
        NamespaceId([5; 16]),
        CanonicalOperation::Put(PutOp {
            key: vec![1],
            value: vec![2],
            lease: None,
            prev_kv: false,
        }),
    )
}

fn command() -> CommandId {
    CommandId::derive(&retry_key(), &logical()).unwrap()
}

/// What a verifier attested, as a submission receipt with its own
/// identity and tick.
fn attested(receipt: u8, ticks: u64) -> AttestedAdmission {
    AttestedAdmission {
        cluster: ClusterId([1; 16]),
        domain: DomainId([2; 16]),
        session: SessionId([3; 16]),
        rule_generation: 1,
        scope_ceiling: u32::MAX,
        receipt_id: Digest32([receipt; 32]),
        admitted_at_ticks: ticks,
    }
}

/// The request as it reaches a voter, admitted under `receipt`.
fn admitted(receipt: u8, ticks: u64) -> Event {
    acking(receipt, ticks, 0)
}

/// The same, acknowledging a floor.
fn acking(receipt: u8, ticks: u64, ack_through: u64) -> Event {
    Event::Admitted(AdmittedRequest {
        receipt: AdmissionReceipt::submitting(
            VerifierToken::for_boundary(),
            attested(receipt, ticks),
        ),
        frame: MessageV1::Request(RequestV1::new(retry_key(), &logical(), 0, ack_through).unwrap())
            .encode()
            .unwrap(),
    })
}

fn peer(from: u8, message: ProtocolMessage) -> Event {
    Event::Peer(AuthenticatedPeerMessage::new(
        PeerProvenance::from_transport(r(from), ReplicaIncarnation::new(1).unwrap(), 1),
        message.encode(),
    ))
}

/// A first presentation binds the facts; a second under other facts
/// changes nothing.
///
/// A fresh receipt on a retry is exactly this: same caller, same
/// request, same identity, a newly minted admission. What the cluster
/// agreed to is what was accepted first, and the retry does not rewrite
/// it.
#[test]
fn a_fresh_receipt_on_a_retry_does_not_replace_what_was_accepted() {
    let mut f = booted(1);
    let effects = f.step(admitted(9, 17));
    assert!(!effects.is_empty(), "the first presentation was accepted");
    let bound = f.payload(&command()).expect("the payload is held").clone();
    assert_eq!(
        bound.admission.unwrap().attested.receipt_id,
        Digest32([9; 32])
    );

    // A second presentation of the same command with another receipt.
    assert!(
        f.step(admitted(4, 99)).is_empty(),
        "nothing new is proposed for a command already accepted"
    );
    assert_eq!(
        f.take_rejections(),
        vec![FollowerRejection::Duplicate(command())],
        "the retry is a duplicate, not an update"
    );
    assert_eq!(
        f.payload(&command()).expect("still held").admission,
        bound.admission,
        "the accepted command still means what it meant"
    );
}

/// A leader proposal that names other facts for a command this replica
/// holds is refused, not held.
///
/// Adopting it would execute facts this replica never admitted. Which
/// of the two is the real command is not something the proposal can
/// settle, so nothing is adopted and the disagreement is reported.
#[test]
fn a_proposal_that_disagrees_with_the_payload_is_refused() {
    let mut f = booted(1);
    let effects = f.step(admitted(9, 17));
    let accepted = admission_digest(
        Some(&AdmissionFacts {
            attested: attested(9, 17),
            establishing: None,
        }),
        0,
    );
    // Make this replica's own vote durable first, so the only thing
    // under test is the proposal.
    for event in durable(&effects) {
        f.step(event);
    }
    f.take_rejections();

    let elsewhere = admission_digest(
        Some(&AdmissionFacts {
            attested: attested(9, 17),
            establishing: Some(AttestedEstablishment {
                principal: PrincipalId([0xbd; 16]),
                trust_rule: TrustRuleId([0x7c; 16]),
                credential_valid_until: CredentialDeadline(u64::MAX),
            }),
        }),
        0,
    );
    let proposal = FastAck {
        replica: r(0),
        ballot: ballot(),
        command: command(),
        deps: Vec::new(),
        paths: Vec::new(),
        path: Digest32([7; 32]),
        admission: elsewhere,
        seqnum: Some(1),
    };
    assert!(
        f.step(peer(0, ProtocolMessage::Proposal(proposal)))
            .is_empty()
    );
    assert_eq!(
        f.take_rejections(),
        vec![FollowerRejection::AdmissionConflict {
            command: command(),
            accepted,
        }]
    );
}

/// A quorum cannot form across senders that accepted one identity as
/// different facts.
#[test]
fn evidence_under_other_facts_is_not_counted() {
    let ours = admission_digest(
        Some(&AdmissionFacts {
            attested: attested(9, 17),
            establishing: None,
        }),
        0,
    );
    let theirs = admission_digest(
        Some(&AdmissionFacts {
            attested: attested(4, 99),
            establishing: None,
        }),
        0,
    );
    let mut votes = VoteSet::new(quorum(), command());
    let proposal = FastAck {
        replica: r(0),
        ballot: ballot(),
        command: command(),
        deps: Vec::new(),
        paths: Vec::new(),
        path: Digest32([7; 32]),
        admission: ours,
        seqnum: Some(1),
    };
    votes.add(Vote::Fast(proposal.clone())).unwrap();
    assert_eq!(votes.admission(), Some(ours));

    // The fast-set member that would complete the fast quorum, under
    // other facts.
    let mut disagreeing = proposal.clone();
    disagreeing.replica = r(1);
    disagreeing.seqnum = None;
    disagreeing.admission = theirs;
    assert_eq!(
        votes.add(Vote::Fast(disagreeing.clone())),
        Err(VoteError::AdmissionConflict { counted: ours })
    );
    assert!(
        votes.learned().is_none(),
        "one sender is not a fast quorum of three"
    );

    // Adoption is accepting a command too, so a slow acknowledgement
    // under other facts is no more countable.
    assert_eq!(
        votes.add(Vote::Slow(SlowAck {
            replica: r(2),
            ballot: ballot(),
            command: command(),
            admission: theirs,
        })),
        Err(VoteError::AdmissionConflict { counted: ours })
    );
    assert!(votes.learned().is_none());

    // The same senders agreeing on what was accepted do learn it.
    disagreeing.admission = ours;
    votes.add(Vote::Fast(disagreeing)).unwrap();
    assert!(votes.learned().is_some());
}

fn durable(effects: &[Effect]) -> Vec<Event> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::Persist(b) => Some(Event::Storage(
                coord_core::event::StorageEvent::JournalDurable {
                    barrier_id: b.barrier,
                    journal_seq: LocalJournalSeq::new(1).unwrap(),
                },
            )),
            _ => None,
        })
        .collect()
}

/// A command's acknowledged floor is part of what the command is.
///
/// Executing a command retires the invocation identities at or below the
/// floor its frame acknowledged, and retirement is state. Two replicas
/// that accepted one command under different acknowledged floors would
/// retire different prefixes of the same client's sequences, and a later
/// invocation would then be `TooOld` on one and new work on another. So
/// the floor is bound into the digest every acknowledgement carries,
/// exactly as the admission is, and a proposal naming a different one is
/// a conflict rather than an update.
#[test]
fn a_proposal_that_acknowledges_a_different_floor_is_refused() {
    let mut f = booted(1);
    let effects = f.step(acking(9, 17, 4));
    let accepted = admission_digest(
        Some(&AdmissionFacts {
            attested: attested(9, 17),
            establishing: None,
        }),
        4,
    );
    for event in durable(&effects) {
        f.step(event);
    }
    f.take_rejections();

    // Same admission, same identity, another floor.
    let elsewhere = admission_digest(
        Some(&AdmissionFacts {
            attested: attested(9, 17),
            establishing: None,
        }),
        9,
    );
    assert_ne!(accepted, elsewhere, "the floor reaches the digest");
    let proposal = FastAck {
        replica: r(0),
        ballot: ballot(),
        command: command(),
        deps: Vec::new(),
        paths: Vec::new(),
        path: Digest32([7; 32]),
        admission: elsewhere,
        seqnum: Some(1),
    };
    assert!(
        f.step(peer(0, ProtocolMessage::Proposal(proposal)))
            .is_empty()
    );
    assert_eq!(
        f.take_rejections(),
        vec![FollowerRejection::AdmissionConflict {
            command: command(),
            accepted,
        }]
    );
}

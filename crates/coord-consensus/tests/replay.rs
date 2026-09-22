//! task-c02 acceptance: an exact duplicate submission repairs delivery of
//! the evidence a replica already produced, to the frontend only, under
//! the same gates as the first publication; and every way the repair is
//! refused leaves the command exactly where it was.
//!
//! Nothing here uses a clock. The follower and the leader are
//! deterministic machines, and what they publish, refuse and persist is
//! read straight off their effects.

use std::collections::BTreeSet;

use coord_consensus::{
    BallotConfiguration, ConfigurationIdentity, Follower, FollowerConfig, FollowerRejection,
    Leader, LeaderConfig, MAX_EVIDENCE_REPAIRS, ProtocolMessage, Rejection, ReplayRefusal,
    ReplicaRole,
};
use coord_core::capability::{AdmissionReceipt, AttestedAdmission, VerifierToken};
use coord_core::effect::{BootId, Effect, PeerId};
use coord_core::event::{
    AdmittedRequest, AuthenticatedPeerMessage, Event, PeerProvenance, StorageError, StorageEvent,
};
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

fn ballot(number: u64, leader: u8) -> Ballot {
    Ballot {
        epoch: epoch(),
        number,
        leader: r(leader),
    }
}

const FRONTEND: PeerId = PeerId {
    replica: ReplicaId([0xf0; 16]),
    incarnation: ReplicaIncarnation::ZERO,
};

fn identity(me: u8) -> ConfigurationIdentity {
    ConfigurationIdentity {
        cluster: ClusterId([1; 16]),
        domain: DomainId([2; 16]),
        epoch: epoch(),
        voters: (0..3).map(r).collect(),
        replica: r(me),
        incarnation: ReplicaIncarnation::new(1).unwrap(),
        role: ReplicaRole::Voter,
    }
}

fn quorum() -> BallotConfiguration {
    let voters: BTreeSet<ReplicaId> = (0..3).map(r).collect();
    BallotConfiguration::c2(epoch(), ballot(0, 0), voters, [r(0), r(1)].into()).unwrap()
}

fn boot() -> Event {
    Event::Boot {
        boot_id: BootId([1; 16]),
        incarnation: ReplicaIncarnation::new(1).unwrap(),
    }
}

/// Follower `me` of a 3-voter epoch led by r0 with fast set {r0, r1}.
fn follower(me: u8) -> Follower {
    let mut f = Follower::new(FollowerConfig {
        identity: identity(me),
        quorum: quorum(),
        genesis: ballot(0, 0),
        frontend: FRONTEND,
        capacity: 8,
    });
    assert!(f.step(boot()).is_empty());
    f
}

fn leader() -> Leader {
    let mut l = Leader::new(
        LeaderConfig {
            identity: identity(0),
            quorum: quorum(),
            genesis: ballot(0, 0),
            frontend: FRONTEND,
            capacity: 8,
        },
        None,
        ExecutionPosition::ZERO,
    );
    assert!(l.step(boot()).is_empty());
    l
}

fn retry_key(seq: u64) -> RetryKey {
    RetryKey {
        cluster_id: ClusterId([1; 16]),
        domain_id: DomainId([2; 16]),
        session_id: SessionId([3; 16]),
        client_instance_id: ClientInstanceId([4; 16]),
        request_sequence: RequestSequence::new(seq).unwrap(),
    }
}

/// A submission of `Put(key, value)` under retry key `seq`, admitted
/// under a receipt identified by `receipt`.
fn presented(seq: u64, key: u8, value: u8, receipt: u8) -> (Event, CommandId) {
    let request = LogicalRequest::new(
        NamespaceId([5; 16]),
        CanonicalOperation::Put(PutOp {
            key: vec![key],
            value: vec![value],
            lease: None,
            prev_kv: false,
        }),
    );
    let command = CommandId::derive(&retry_key(seq), &request).unwrap();
    let frame = MessageV1::Request(RequestV1::new(retry_key(seq), &request, 0, 0).unwrap())
        .encode()
        .unwrap();
    let receipt = AdmissionReceipt::submitting(
        VerifierToken::for_boundary(),
        AttestedAdmission {
            cluster: ClusterId([1; 16]),
            domain: DomainId([2; 16]),
            session: SessionId([3; 16]),
            rule_generation: 1,
            scope_ceiling: u32::MAX,
            receipt_id: Digest32([receipt; 32]),
            admitted_at_ticks: 0,
        },
    );
    (Event::Admitted(AdmittedRequest { receipt, frame }), command)
}

fn admitted(seq: u64, key: u8, value: u8) -> (Event, CommandId) {
    presented(seq, key, value, 9)
}

fn peer(from: u8, message: ProtocolMessage) -> Event {
    Event::Peer(AuthenticatedPeerMessage::new(
        PeerProvenance::from_transport(r(from), ReplicaIncarnation::new(1).unwrap(), 1),
        message.encode(),
    ))
}

fn durable_of(effects: &[Effect], seq: u64) -> Vec<Event> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::Persist(b) => Some(Event::Storage(StorageEvent::JournalDurable {
                barrier_id: b.barrier,
                journal_seq: LocalJournalSeq::new(seq).unwrap(),
            })),
            _ => None,
        })
        .collect()
}

fn failed_of(effects: &[Effect]) -> Vec<Event> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::Persist(b) => Some(Event::Storage(StorageEvent::Failed {
                barrier_id: b.barrier,
                error: StorageError::NoSpace,
            })),
            _ => None,
        })
        .collect()
}

fn sends(effects: &[Effect]) -> Vec<(ReplicaId, ProtocolMessage)> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::SendWhenDurable { to, frame, .. } => {
                Some((to.replica, ProtocolMessage::decode(frame).unwrap()))
            }
            _ => None,
        })
        .collect()
}

/// The sends that went to the frontend, and a check that nothing went
/// anywhere else.
fn frontend_only(effects: &[Effect]) -> Vec<ProtocolMessage> {
    let sent = sends(effects);
    for (to, m) in &sent {
        assert_eq!(*to, FRONTEND.replica, "a repair reaches peers: {m:?}");
    }
    sent.into_iter().map(|(_, m)| m).collect()
}

/// A fast-set follower that has acknowledged `seq` durably. Returns the
/// command and the acknowledgement as first published.
fn acknowledged(f: &mut Follower, seq: u64) -> (CommandId, ProtocolMessage) {
    let (e, c) = admitted(seq, seq as u8, 1);
    let effects = f.step(e);
    let mut released = Vec::new();
    for d in durable_of(&effects, seq) {
        released.extend(f.step(d));
    }
    let sent = sends(&released);
    assert_eq!(sent.len(), 3, "two peers and the frontend");
    let (_, ack) = sent
        .into_iter()
        .find(|(to, _)| *to == FRONTEND.replica)
        .expect("the frontend's copy");
    assert!(f.take_rejections().is_empty());
    (c, ack)
}

// --- the repair -----------------------------------------------------------

/// The same submission again publishes the same acknowledgement again,
/// to the frontend and to nobody else.
#[test]
fn a_duplicate_publishes_the_acknowledgement_again_to_the_frontend_only() {
    let mut f = follower(1);
    let (c, ack) = acknowledged(&mut f, 1);

    let again = frontend_only(&f.step(admitted(1, 1, 1).0));
    assert_eq!(again, vec![ack], "the same bytes, not a recomputed vote");
    assert_eq!(f.take_rejections(), vec![FollowerRejection::Duplicate(c)]);
    assert_eq!(f.evidence_repairs(&c), 1);
    assert_eq!(
        f.pending_sends(),
        0,
        "released in the same step, not queued"
    );
}

/// A repair is not a new proposal and not a new vote: nothing is
/// persisted for it and nothing about the command changes.
#[test]
fn a_repair_persists_nothing() {
    let mut f = follower(1);
    let (c, _) = acknowledged(&mut f, 1);
    let phase = f.table().phase_of(&c);

    let effects = f.step(admitted(1, 1, 1).0);
    assert!(
        !effects.iter().any(|e| matches!(e, Effect::Persist(_))),
        "a repair wrote something: {effects:?}"
    );
    assert_eq!(f.table().phase_of(&c), phase);
}

/// The leader's reply is the one piece of evidence every predicate
/// needs, and it is repaired the same way.
#[test]
fn a_leader_publishes_its_reply_again_to_the_frontend_only() {
    let mut l = leader();
    let (e, c) = admitted(1, 1, 1);
    let effects = l.step(e);
    let mut released = Vec::new();
    for d in durable_of(&effects, 1) {
        released.extend(l.step(d));
    }
    let first = sends(&released);
    assert_eq!(first.len(), 3, "two proposals and the reply");
    let reply = first
        .iter()
        .find(|(to, _)| *to == FRONTEND.replica)
        .map(|(_, m)| m.clone())
        .expect("the reply");
    assert!(matches!(reply, ProtocolMessage::LeaderReply { command, .. } if command == c));

    let again = frontend_only(&l.step(admitted(1, 1, 1).0));
    assert_eq!(again, vec![reply]);
    assert_eq!(l.take_rejections(), vec![Rejection::Duplicate(c)]);
    assert_eq!(l.evidence_repairs(&c), 1);
    assert!(
        !frontend_only(&l.step(admitted(1, 1, 1).0)).is_empty(),
        "and again, while the command is unresolved"
    );
}

/// A leader whose proposal batch was definitely rejected presents the
/// same rows again under a fresh barrier and republishes the identical
/// reply behind it. What a later duplicate replays is that retry, once
/// the retry is durable -- not the first publication, whose batch
/// failed. The bytes are the same; the barrier they rest on is not.
#[test]
fn a_retried_proposal_is_replayed_under_its_retrys_barrier() {
    let mut l = leader();
    let (e, c) = admitted(1, 1, 1);
    let effects = l.step(e);
    let Effect::Persist(first) = &effects[0] else {
        panic!("a proposal persists its batch: {effects:?}");
    };
    // Definitely rejected: the batch is presented again, unchanged,
    // under a new barrier, and the reply is published again behind it.
    let retried = l.step(Event::Storage(StorageEvent::Failed {
        barrier_id: first.barrier,
        error: StorageError::DefinitelyNotCommitted,
    }));
    let Effect::Persist(again) = &retried[0] else {
        panic!("the rejected batch is presented again: {retried:?}");
    };
    assert_ne!(again.barrier, first.barrier);
    assert_eq!(l.take_rejections(), vec![Rejection::ProposalRetried(c)]);

    // Before the retry is durable there is nothing to replay yet: the
    // retry's own send is still queued, and the failed original is not
    // what is retained.
    assert!(frontend_only(&l.step(admitted(1, 1, 1).0)).is_empty());
    assert_eq!(
        l.take_rejections(),
        vec![
            Rejection::Duplicate(c),
            Rejection::ReplayRefused {
                command: c,
                why: ReplayRefusal::NotYetDurable
            }
        ]
    );

    // The retry lands. Its reply goes, and a duplicate now replays it.
    let mut released = Vec::new();
    for d in durable_of(&retried, 2) {
        released.extend(l.step(d));
    }
    let reply = sends(&released)
        .into_iter()
        .find(|(to, _)| *to == FRONTEND.replica)
        .map(|(_, m)| m)
        .expect("the retry's reply reached the frontend");
    assert_eq!(frontend_only(&l.step(admitted(1, 1, 1).0)), vec![reply]);
    assert_eq!(l.take_rejections(), vec![Rejection::Duplicate(c)]);
    assert_eq!(l.evidence_repairs(&c), 1);
}

// --- what is not a duplicate -----------------------------------------------

/// The same identity under other admission facts is a conflict. Nothing
/// is replayed for it: what this replica acknowledged was the first
/// presentation, and handing that acknowledgement to a presentation of
/// other facts would attest facts it never accepted.
#[test]
fn other_facts_under_the_same_identity_are_a_conflict_and_replay_nothing() {
    let mut f = follower(1);
    let (c, _) = acknowledged(&mut f, 1);
    let accepted = f.payload(&c).expect("held").admission_digest();

    let effects = f.step(presented(1, 1, 1, 8).0);
    assert!(effects.is_empty(), "{effects:?}");
    assert_eq!(
        f.take_rejections(),
        vec![FollowerRejection::RequestFactsConflict {
            command: c,
            accepted
        }]
    );
    assert_eq!(f.evidence_repairs(&c), 0);

    let mut l = leader();
    let (e, c) = admitted(1, 1, 1);
    let effects = l.step(e);
    for d in durable_of(&effects, 1) {
        l.step(d);
    }
    l.take_rejections();
    assert!(l.step(presented(1, 1, 1, 8).0).is_empty());
    assert!(matches!(
        l.take_rejections().as_slice(),
        [Rejection::RequestFactsConflict { command, .. }] if *command == c
    ));
}

/// Another payload under the same retry key was never a duplicate and
/// still is not.
#[test]
fn another_payload_under_the_same_key_is_still_an_identity_conflict() {
    let mut f = follower(1);
    let (c, _) = acknowledged(&mut f, 1);
    assert!(f.step(admitted(1, 1, 7).0).is_empty());
    assert!(matches!(
        f.take_rejections().as_slice(),
        [FollowerRejection::RequestIdentityConflict { bound, .. }] if *bound == c
    ));
}

// --- the gates ----------------------------------------------------------------

/// Before the batch behind the acknowledgement is durable, the original
/// publication is still queued. A duplicate adds no second copy, and the
/// original goes by itself when the batch is durable.
#[test]
fn before_durability_nothing_is_added_and_the_original_still_goes() {
    let mut f = follower(1);
    let (e, c) = admitted(1, 1, 1);
    let effects = f.step(e);

    assert!(sends(&f.step(admitted(1, 1, 1).0)).is_empty());
    assert_eq!(
        f.take_rejections(),
        vec![
            FollowerRejection::Duplicate(c),
            FollowerRejection::ReplayRefused {
                command: c,
                why: ReplayRefusal::NotYetDurable
            }
        ]
    );
    assert_eq!(f.evidence_repairs(&c), 0);

    let mut released = Vec::new();
    for d in durable_of(&effects, 1) {
        released.extend(f.step(d));
    }
    let to_frontend: Vec<_> = sends(&released)
        .into_iter()
        .filter(|(to, _)| *to == FRONTEND.replica)
        .collect();
    assert_eq!(to_frontend.len(), 1, "the original, once");
}

/// A batch that failed never produced evidence; the original send was
/// dropped for that reason and a duplicate must not produce the copy
/// that was refused.
#[test]
fn a_failed_batch_is_never_replayed() {
    let mut f = follower(1);
    let (e, c) = admitted(1, 1, 1);
    let effects = f.step(e);
    for x in failed_of(&effects) {
        assert!(sends(&f.step(x)).is_empty());
    }
    assert!(matches!(
        f.take_rejections().as_slice(),
        [FollowerRejection::BatchFailed(x)] if *x == c
    ));

    assert!(sends(&f.step(admitted(1, 1, 1).0)).is_empty());
    assert_eq!(
        f.take_rejections(),
        vec![
            FollowerRejection::Duplicate(c),
            FollowerRejection::ReplayRefused {
                command: c,
                why: ReplayRefusal::BatchFailed
            }
        ]
    );
}

/// A replica fenced by a higher promise votes in nothing, and replays
/// nothing. The evidence it holds is the old ballot's; the new ballot
/// collects its own.
///
/// The fence that fires is the machine's own eligibility gate, which
/// refuses the presentation before the duplicate path is reached: a
/// follower says `FencedByPromise`, a leader `NotLeading`. The repair
/// path applies the same fence again where it decides, which is why
/// `ReplayRefusal::Fenced` exists, but a presentation should never get
/// that far.
#[test]
fn a_fenced_replica_replays_nothing() {
    let mut f = follower(1);
    let (c, _) = acknowledged(&mut f, 1);
    // A promise in flight is enough: this replica may not vote until it
    // knows how that ends.
    let _ = f.step(peer(
        2,
        ProtocolMessage::NewLeader {
            ballot: ballot(1, 2),
        },
    ));
    f.take_rejections();

    assert!(sends(&f.step(admitted(1, 1, 1).0)).is_empty());
    let why = f.take_rejections();
    assert!(
        why.iter().any(|x| matches!(
            x,
            FollowerRejection::FencedByPromise { .. }
                | FollowerRejection::ReplayRefused {
                    why: ReplayRefusal::Fenced,
                    ..
                }
        )),
        "{why:?}"
    );
    assert_eq!(f.evidence_repairs(&c), 0);

    let mut l = leader();
    let (e, c) = admitted(1, 1, 1);
    let effects = l.step(e);
    for d in durable_of(&effects, 1) {
        l.step(d);
    }
    let _ = l.step(peer(
        2,
        ProtocolMessage::NewLeader {
            ballot: ballot(1, 2),
        },
    ));
    l.take_rejections();
    assert!(sends(&l.step(admitted(1, 1, 1).0)).is_empty());
    let why = l.take_rejections();
    assert!(
        why.iter().any(|x| matches!(
            x,
            Rejection::NotLeading { .. }
                | Rejection::ReplayRefused {
                    why: ReplayRefusal::Fenced,
                    ..
                }
        )),
        "{why:?}"
    );
    assert_eq!(l.evidence_repairs(&c), 0);
}

/// A replica outside the fast set has nothing to hand over until it has
/// adopted an order, and says so rather than inventing an
/// acknowledgement from the record it holds.
#[test]
fn a_replica_that_has_not_acknowledged_yet_has_nothing_to_replay() {
    let mut f = follower(2);
    let (e, c) = admitted(1, 1, 1);
    let effects = f.step(e);
    for d in durable_of(&effects, 1) {
        assert!(
            sends(&f.step(d)).is_empty(),
            "outside the fast set, no fast vote"
        );
    }
    assert!(sends(&f.step(admitted(1, 1, 1).0)).is_empty());
    assert_eq!(
        f.take_rejections(),
        vec![
            FollowerRejection::Duplicate(c),
            FollowerRejection::ReplayRefused {
                command: c,
                why: ReplayRefusal::NothingToReplay
            }
        ]
    );
}

// --- the bound -------------------------------------------------------------

/// One boot repairs one command a bounded number of times. Past it the
/// command is still whatever it was; only the shortcut closes.
#[test]
fn repairs_of_one_command_are_bounded_per_boot() {
    let mut f = follower(1);
    let (c, ack) = acknowledged(&mut f, 1);
    for n in 1..=MAX_EVIDENCE_REPAIRS {
        assert_eq!(
            frontend_only(&f.step(admitted(1, 1, 1).0)),
            vec![ack.clone()]
        );
        assert_eq!(f.evidence_repairs(&c), n);
        assert_eq!(f.take_rejections(), vec![FollowerRejection::Duplicate(c)]);
    }
    assert!(sends(&f.step(admitted(1, 1, 1).0)).is_empty());
    assert_eq!(
        f.take_rejections(),
        vec![
            FollowerRejection::Duplicate(c),
            FollowerRejection::ReplayRefused {
                command: c,
                why: ReplayRefusal::TooMany
            }
        ]
    );
    assert_eq!(f.evidence_repairs(&c), MAX_EVIDENCE_REPAIRS);
    assert!(f.table().phase_of(&c).is_some(), "the command is untouched");
}

/// Repairing one command spends nothing of another's.
#[test]
fn the_bound_is_per_command() {
    let mut f = follower(1);
    let (c1, _) = acknowledged(&mut f, 1);
    let (c2, ack2) = acknowledged(&mut f, 2);
    for _ in 0..MAX_EVIDENCE_REPAIRS {
        f.step(admitted(1, 1, 1).0);
    }
    f.take_rejections();
    assert_eq!(f.evidence_repairs(&c1), MAX_EVIDENCE_REPAIRS);
    assert_eq!(frontend_only(&f.step(admitted(2, 2, 1).0)), vec![ack2]);
    assert_eq!(f.evidence_repairs(&c2), 1);
}

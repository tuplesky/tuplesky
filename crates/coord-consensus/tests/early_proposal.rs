//! task-d98: the leader's proposal before its batch is durable (design
//! Sections 4.3 and 4.8).
//!
//! A fresh proposal leaves before its batch is durable only while the
//! domain has activated `EarlyProposal`, only once a proposal batch of
//! the leadership is durable, and only to a follower that cannot make a
//! fast quorum from it. Every other copy, and the leader reply, wait for
//! the batch; and nothing is committed over the adoptions an early copy
//! brings back until the batch is durable.

use std::collections::BTreeSet;

use coord_consensus::{
    BallotConfiguration, ConfigurationIdentity, Leader, LeaderConfig, Phase, ProtocolMessage,
    ReplicaRole, SlowAck,
};
use coord_core::capability::{AdmissionReceipt, AttestedAdmission, VerifierToken};
use coord_core::effect::{BarrierId, BootId, Effect, PeerId};
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

fn genesis() -> Ballot {
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

/// `n` voters, r0 leading, under C2's default fast set: a majority
/// starting at the leader, so {r0, r1} at three and {r0, r1, r2} at five.
fn config(n: u8) -> LeaderConfig {
    let voters: BTreeSet<ReplicaId> = (0..n).map(r).collect();
    LeaderConfig {
        identity: ConfigurationIdentity {
            cluster: ClusterId([1; 16]),
            domain: DomainId([2; 16]),
            epoch: epoch(),
            voters: voters.clone(),
            replica: r(0),
            incarnation: ReplicaIncarnation::new(1).unwrap(),
            role: ReplicaRole::Voter,
        },
        quorum: BallotConfiguration::c2_default(epoch(), genesis(), voters).unwrap(),
        genesis: genesis(),
        frontend: FRONTEND,
        capacity: 16,
    }
}

fn booted(n: u8, early: bool) -> Leader {
    let mut leader = Leader::new(config(n), None, ExecutionPosition::ZERO);
    leader.set_early_proposal(early);
    assert!(
        leader
            .step(Event::Boot {
                boot_id: BootId([1; 16]),
                incarnation: ReplicaIncarnation::new(1).unwrap()
            })
            .is_empty()
    );
    leader
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

fn admitted(seq: u64) -> (Event, CommandId) {
    let request = LogicalRequest::new(
        NamespaceId([5; 16]),
        CanonicalOperation::Put(PutOp {
            key: vec![seq as u8],
            value: vec![1],
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
            receipt_id: Digest32([9; 32]),
            admitted_at_ticks: 0,
        },
    );
    (Event::Admitted(AdmittedRequest { receipt, frame }), command)
}

/// The one batch `effects` submit.
fn batch(effects: &[Effect]) -> BarrierId {
    let barriers: Vec<BarrierId> = effects
        .iter()
        .filter_map(|e| match e {
            Effect::Persist(b) => Some(b.barrier),
            _ => None,
        })
        .collect();
    assert_eq!(barriers.len(), 1, "one batch: {effects:?}");
    barriers[0]
}

fn durable(barrier: BarrierId, seq: u64) -> Event {
    Event::Storage(StorageEvent::JournalDurable {
        barrier_id: barrier,
        journal_seq: LocalJournalSeq::new(seq).unwrap(),
    })
}

/// The proposals among `effects`: the voter each went to, whether it
/// left with nothing to wait on, and its frame.
fn proposals(effects: &[Effect]) -> Vec<(u8, bool, Vec<u8>)> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::SendWhenDurable {
                requires,
                to,
                frame,
                ..
            } if matches!(
                ProtocolMessage::decode(frame),
                Ok(ProtocolMessage::Proposal(_))
            ) =>
            {
                Some((to.replica.0[0], requires.is_empty(), frame.clone()))
            }
            _ => None,
        })
        .collect()
}

fn replies(effects: &[Effect]) -> usize {
    effects
        .iter()
        .filter(|e| match e {
            Effect::SendWhenDurable { frame, .. } => matches!(
                ProtocolMessage::decode(frame),
                Ok(ProtocolMessage::LeaderReply { .. })
            ),
            _ => false,
        })
        .count()
}

/// A leader past its first durable batch: the guard that holds a
/// leadership's first proposals is behind it.
fn ready(n: u8, early: bool) -> (Leader, u64) {
    let mut leader = booted(n, early);
    let (event, _) = admitted(1);
    let effects = leader.step(event);
    leader.step(durable(batch(&effects), 1));
    (leader, 2)
}

/// With the feature off, which is every domain that has not activated it,
/// no proposal leaves before its batch, at either size, and nothing
/// counts as sent early: the mixed-build case.
#[test]
fn with_early_proposals_off_no_proposal_leaves_before_its_batch() {
    for n in [3u8, 5] {
        let (mut leader, mut seq) = ready(n, false);
        for _ in 0..3 {
            let (event, _) = admitted(seq);
            let effects = leader.step(event);
            assert!(
                proposals(&effects).is_empty(),
                "{n} voters: a proposal left before its batch: {effects:?}"
            );
            assert_eq!(replies(&effects), 0);
            let released = leader.step(durable(batch(&effects), seq));
            let sent = proposals(&released);
            assert_eq!(sent.len(), usize::from(n) - 1, "one to each follower");
            assert!(sent.iter().all(|(_, early, _)| !early));
            assert_eq!(replies(&released), 1);
            seq += 1;
        }
        assert_eq!(leader.take_resend_counts().early, 0);
    }
}

/// At three voters under C2 the fast set is the leader and r1: r1's own
/// fast acknowledgement and the proposal are a fast quorum, so r1 gets the
/// proposal once the batch is durable. r2 cannot make one, and gets it at
/// once. The leader reply waits for the batch either way.
#[test]
fn at_three_voters_only_the_follower_outside_the_fast_set_gets_it_early() {
    let (mut leader, seq) = ready(3, true);
    let (event, command) = admitted(seq);
    let effects = leader.step(event);
    let early = proposals(&effects);
    assert_eq!(early.len(), 1, "{effects:?}");
    let (to, nothing_to_wait_on, frame) = &early[0];
    assert_eq!(*to, 2, "the follower outside the fast set");
    assert!(nothing_to_wait_on);
    assert_eq!(replies(&effects), 0, "the leader reply is its fast vote");
    assert_eq!(leader.take_resend_counts().early, 1);

    let released = leader.step(durable(batch(&effects), seq));
    let late = proposals(&released);
    assert_eq!(late.len(), 1, "{released:?}");
    assert_eq!(late[0].0, 1, "the fast-set follower, once durable");
    assert!(!late[0].1);
    assert_eq!(&late[0].2, frame, "one proposal, the same to both");
    assert_eq!(replies(&released), 1);
    assert_eq!(leader.table().phase_of(&command), Some(Phase::Accept));
}

/// At five voters under C2 a follower holds the proposal and at most its
/// own fast acknowledgement, two of the three a fast quorum needs: every
/// follower gets it at once.
#[test]
fn at_five_voters_every_follower_gets_it_early() {
    let (mut leader, seq) = ready(5, true);
    let (event, _) = admitted(seq);
    let effects = leader.step(event);
    let mut early: Vec<u8> = proposals(&effects)
        .into_iter()
        .map(|(to, nothing_to_wait_on, _)| {
            assert!(nothing_to_wait_on);
            to
        })
        .collect();
    early.sort_unstable();
    assert_eq!(early, vec![1, 2, 3, 4]);
    assert_eq!(replies(&effects), 0);
    assert_eq!(leader.take_resend_counts().early, 4);
    let released = leader.step(durable(batch(&effects), seq));
    assert!(proposals(&released).is_empty(), "nothing left to send");
    assert_eq!(replies(&released), 1);
}

/// No proposal leaves early before a batch of the leadership is durable:
/// a genesis leader that restarted on a store holding none would come
/// back leading (`coordd`'s `fresh`) and number its proposals from zero
/// again. A second batch already in flight when the first completes keeps
/// its own barrier; the ones after it go early.
#[test]
fn the_first_batch_holds_every_proposal_and_one_in_flight_keeps_its_barrier() {
    let mut leader = booted(3, true);
    let (e1, _) = admitted(1);
    let (e2, _) = admitted(2);
    let effects1 = leader.step(e1);
    let effects2 = leader.step(e2);
    assert!(proposals(&effects1).is_empty(), "{effects1:?}");
    assert!(proposals(&effects2).is_empty(), "{effects2:?}");

    let released1 = leader.step(durable(batch(&effects1), 1));
    let sent1 = proposals(&released1);
    assert_eq!(sent1.len(), 2, "the first command's, to both followers");
    assert!(sent1.iter().all(|(_, early, _)| !early));

    let (e3, _) = admitted(3);
    let effects3 = leader.step(e3);
    let sent3 = proposals(&effects3);
    assert_eq!(sent3.len(), 1, "the third goes early: {effects3:?}");
    assert_eq!((sent3[0].0, sent3[0].1), (2, true));

    // The second was batched before the first was durable: it waits for
    // its own batch, to both followers.
    let released2 = leader.step(durable(batch(&effects2), 2));
    let sent2 = proposals(&released2);
    assert_eq!(sent2.len(), 2, "{released2:?}");
    assert!(sent2.iter().all(|(_, early, _)| !early));
    assert_eq!(leader.take_resend_counts().early, 1);
}

/// The adoptions an early proposal brings back can make a slow quorum
/// before the leader's own batch is durable. Nothing is committed over
/// them until it is (task-d89's gate): the frontier this leader announces
/// never covers a proposal it may lose.
/// Adoptions of `command` from `followers`.
fn adopt(leader: &mut Leader, command: CommandId, followers: &[u8]) {
    let admission = leader
        .table()
        .record(&command)
        .and_then(|r| r.payload)
        .expect("initialized");
    for follower in followers {
        leader.step(Event::Peer(AuthenticatedPeerMessage::new(
            PeerProvenance::from_transport(r(*follower), ReplicaIncarnation::new(1).unwrap(), 1),
            ProtocolMessage::SlowAck(SlowAck {
                replica: r(*follower),
                ballot: genesis(),
                command,
                admission,
            })
            .encode(),
        )));
    }
}

#[test]
fn adoptions_of_an_early_proposal_commit_nothing_before_the_batch() {
    // Every command depends on the one before it: the first is committed
    // here, so only the leader's batch stands between the next and its
    // commit.
    let mut leader = booted(5, true);
    let (e1, c1) = admitted(1);
    let effects1 = leader.step(e1);
    leader.step(durable(batch(&effects1), 1));
    adopt(&mut leader, c1, &[1, 2]);
    assert_eq!(leader.table().phase_of(&c1), Some(Phase::Commit));

    let (event, command) = admitted(2);
    let effects = leader.step(event);
    assert_eq!(proposals(&effects).len(), 4);
    adopt(&mut leader, command, &[1, 2, 3]);
    assert!(
        leader.votes(&command).unwrap().learned_slow().is_some(),
        "three followers' adoptions are a slow quorum at five voters"
    );
    assert_eq!(
        leader.table().phase_of(&command),
        Some(Phase::Accept),
        "not committed: the leader's batch is not durable"
    );
    leader.step(durable(batch(&effects), 2));
    assert_eq!(leader.table().phase_of(&command), Some(Phase::Commit));
}

/// A batch definitely not written is presented again unchanged, and its
/// proposal goes out again by the same rule: at once to the follower that
/// cannot make a fast quorum from it, the same frame as before, and to
/// the fast-set follower once the new batch is durable.
#[test]
fn a_retried_batch_goes_early_by_the_same_rule() {
    let (mut leader, seq) = ready(3, true);
    let (event, _) = admitted(seq);
    let effects = leader.step(event);
    let first = proposals(&effects);
    assert_eq!(first.len(), 1);
    let retried = leader.step(Event::Storage(StorageEvent::Failed {
        barrier_id: batch(&effects),
        error: StorageError::DefinitelyNotCommitted,
    }));
    let again = proposals(&retried);
    assert_eq!(again.len(), 1, "{retried:?}");
    assert_eq!((again[0].0, again[0].1), (2, true));
    assert_eq!(again[0].2, first[0].2, "unchanged");
    assert_eq!(replies(&retried), 0);
    let released = leader.step(durable(batch(&retried), seq));
    let late = proposals(&released);
    assert_eq!(late.len(), 1);
    assert_eq!((late[0].0, late[0].1), (1, false));
    assert_eq!(replies(&released), 1);
}

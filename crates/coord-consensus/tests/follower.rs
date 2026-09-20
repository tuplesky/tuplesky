//! task-23 acceptance: leader/follower message races, conflict arrival
//! permutations, duplicate identities and crashes between state and vote
//! preserve learning obligations; half-initialized dependencies never
//! appear; dependency-phase guards are explicit; equal direct dependencies
//! are not a learning proof.

use std::collections::BTreeSet;

use coord_consensus::{
    BallotConfiguration, CONSERVATIVE_KEY, CommandRecord, ConfigurationIdentity, FastAck, Follower,
    FollowerConfig, FollowerRejection, PayloadRecordV1, Phase, ProtocolMessage, ReplicaRole,
    decode_dependency, decode_payload, dependency_key,
};
use coord_core::capability::{AdmissionReceipt, AttestedAdmission, VerifierToken};
use coord_core::effect::{BootId, Effect, PeerId};
use coord_core::event::{
    AdmittedRequest, AuthenticatedPeerMessage, Event, PeerProvenance, StorageError, StorageEvent,
};
use coord_core::machine::DeterministicMachine;
use coord_sim::storage::StorageModel;
use coord_store_api::registry::Collection;
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

/// Follower `me` in a 3-voter epoch led by r0 with fast set {r0, r1}.
fn config(me: u8) -> FollowerConfig {
    let voters: BTreeSet<ReplicaId> = (0..3).map(r).collect();
    FollowerConfig {
        identity: ConfigurationIdentity {
            cluster: ClusterId([1; 16]),
            domain: DomainId([2; 16]),
            epoch: epoch(),
            voters: voters.clone(),
            replica: r(me),
            incarnation: ReplicaIncarnation::new(1).unwrap(),
            role: ReplicaRole::Voter,
        },
        quorum: BallotConfiguration::c2(epoch(), ballot(0, 0), voters, [r(0), r(1)].into())
            .unwrap(),
        genesis: ballot(0, 0),
        frontend: FRONTEND,
        capacity: 8,
    }
}

fn boot_event() -> Event {
    Event::Boot {
        boot_id: BootId([1; 16]),
        incarnation: ReplicaIncarnation::new(1).unwrap(),
    }
}

fn booted(me: u8) -> Follower {
    let mut f = Follower::new(config(me));
    assert!(f.step(boot_event()).is_empty());
    f
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

fn admitted(seq: u64, key: u8, value: u8) -> (Event, CommandId) {
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
    let frame = MessageV1::Request(RequestV1::new(retry_key(seq), &request, 0).unwrap())
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

fn peer(from: u8, message: ProtocolMessage) -> Event {
    Event::Peer(AuthenticatedPeerMessage::new(
        PeerProvenance::from_transport(r(from), ReplicaIncarnation::new(1).unwrap(), 1),
        message.encode(),
    ))
}

/// The leader's proposal for `command` with `deps`, computed by a leader
/// table that saw the given order.
fn proposal(
    command: CommandId,
    deps: Vec<CommandId>,
    seqnum: u64,
    paths: Vec<(Vec<u8>, Digest32)>,
    path: Digest32,
) -> ProtocolMessage {
    ProtocolMessage::Proposal(FastAck {
        replica: r(0),
        ballot: ballot(0, 0),
        command,
        deps,
        paths,
        path,
        admission: admitted_under(),
        seqnum: Some(seqnum),
    })
}

/// The admission every request these tests admit is submitted under.
/// A proposal that named another one would be a proposal about another
/// command, whatever identity it shares.
fn admitted_under() -> Digest32 {
    coord_core::capability::admission_digest(Some(&coord_core::capability::AdmissionFacts {
        attested: AttestedAdmission {
            cluster: ClusterId([1; 16]),
            domain: DomainId([2; 16]),
            session: SessionId([3; 16]),
            rule_generation: 1,
            scope_ceiling: u32::MAX,
            receipt_id: Digest32([9; 32]),
            admitted_at_ticks: 0,
        },
        establishing: None,
    }))
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

#[test]
fn fast_votes_wait_for_durable_payload_dependencies_and_path() {
    let mut fast = booted(1);
    let (e1, c1) = admitted(1, 1, 1);
    let effects = fast.step(e1);
    assert_eq!(effects.len(), 1, "persist first, send nothing");
    let Effect::Persist(batch) = &effects[0] else {
        panic!()
    };
    let collections: Vec<Collection> = batch
        .updates
        .iter()
        .map(|u| Collection::from_id(u.collection).unwrap())
        .collect();
    assert_eq!(
        collections,
        vec![Collection::PayloadV1, Collection::ProtocolV1]
    );
    assert_eq!(batch.updates[1].key, dependency_key(epoch(), &c1));
    let row = decode_dependency(batch.updates[1].value.as_ref().unwrap()).unwrap();
    assert_eq!(row.phase, Phase::PreAccept);
    assert_eq!(fast.pending_sends(), 3, "two voters and the frontend");
    let released = fast.step(durable_of(&effects, 1).remove(0));
    let sent = sends(&released);
    assert_eq!(sent.len(), 3);
    for (to, m) in &sent {
        assert!(*to == r(0) || *to == r(2) || *to == FRONTEND.replica);
        let ProtocolMessage::FastAck(ack) = m else {
            panic!("{m:?}")
        };
        assert_eq!(ack.replica, r(1));
        assert_eq!(
            ack.seqnum, None,
            "a follower never carries a sequence number"
        );
        assert_eq!(
            ack.path, row.path,
            "the vote carries the durable path evidence"
        );
        assert_eq!(ack.deps, row.deps);
    }
    // A voter outside the fast set persists and votes nothing fast.
    let mut slow = booted(2);
    let effects = slow.step(admitted(1, 1, 1).0);
    assert_eq!(effects.len(), 1);
    assert_eq!(slow.pending_sends(), 0);
    assert!(slow.step(durable_of(&effects, 1).remove(0)).is_empty());
    assert_eq!(slow.table().phase_of(&c1), Some(Phase::PreAccept));
    // Duplicate identities: the same request again changes nothing; a
    // different payload under the same retry key is refused.
    assert!(fast.step(admitted(1, 1, 1).0).is_empty());
    assert!(fast.step(admitted(1, 1, 7).0).is_empty());
    let rejections = fast.take_rejections();
    assert_eq!(rejections[0], FollowerRejection::Duplicate(c1));
    assert!(matches!(
        rejections[1],
        FollowerRejection::RequestIdentityConflict { bound, .. } if bound == c1
    ));
}

#[test]
fn a_proposal_before_the_payload_is_held_against_an_invisible_placeholder() {
    let mut f = booted(1);
    let (e1, c1) = admitted(1, 1, 1);
    let (e2, c2) = admitted(2, 2, 2);
    // The leader saw c1 then c2. Its proposal for c2 (deps [c1]) reaches
    // this follower before either payload.
    let mut leader_view = coord_consensus::CommandTable::new();
    let l1 = leader_view
        .initialize(c1, c1.0, vec![CONSERVATIVE_KEY.to_vec()])
        .unwrap();
    let l2 = leader_view
        .initialize(c2, c2.0, vec![CONSERVATIVE_KEY.to_vec()])
        .unwrap();
    let p2 = proposal(c2, vec![c1], 1, l2.paths.clone(), l2.path);
    assert!(f.step(peer(0, p2.clone())).is_empty());
    assert_eq!(f.held().len(), 1);
    assert_eq!(
        f.table().phase_of(&c2),
        None,
        "placeholder reports no phase"
    );
    assert!(
        f.table().conflicts(&[CONSERVATIVE_KEY.to_vec()]).is_empty(),
        "a conflicting command initialized now cannot see the placeholder"
    );
    // A duplicate proposal converges without change.
    assert!(f.step(peer(0, p2.clone())).is_empty());
    assert_eq!(f.held().len(), 1);
    // c2's payload arrives: initialized (deps computed without the
    // placeholder), voted, but the leader's order needs c1 at ACCEPT.
    let effects = f.step(e2);
    assert_eq!(effects.len(), 1, "vote batch only; adoption waits for c1");
    assert_eq!(
        f.table().record(&c2).unwrap().deps,
        vec![],
        "local deps: nothing visible"
    );
    assert_eq!(f.held().len(), 1, "held: dependency c1 lags");
    let released = f.step(durable_of(&effects, 1).remove(0));
    assert!(matches!(sends(&released)[0].1, ProtocolMessage::FastAck(_)));
    // c1 arrives with its proposal (deps []): c1 adopts, then c2 adopts in
    // the same turn (chain), each adoption persisted before its slow ack.
    let effects1 = f.step(e1);
    assert_eq!(effects1.len(), 1);
    f.step(durable_of(&effects1, 2).remove(0));
    let p1 = proposal(c1, vec![], 0, l1.paths.clone(), l1.path);
    let adoptions = f.step(peer(0, p1));
    assert_eq!(
        adoptions.len(),
        2,
        "two adoption batches (c1 then c2), no send before durability"
    );
    assert!(f.held().is_empty());
    assert_eq!(
        f.table().phase_of(&c1),
        Some(Phase::Accept),
        "adopted, but this replica's own vote is not durable yet"
    );
    assert_eq!(f.table().phase_of(&c2), Some(Phase::Accept));
    assert_eq!(
        f.table().record(&c2).unwrap().deps,
        vec![c1],
        "leader order adopted"
    );
    for (i, e) in adoptions.iter().enumerate() {
        let Effect::Persist(b) = e else { panic!() };
        let row = decode_dependency(b.updates[0].value.as_ref().unwrap()).unwrap();
        assert_eq!(row.phase, Phase::Accept, "adoption {i} persisted");
    }
    // The adoption rows become durable: only now does this replica's own
    // slow vote count, and the commands are learned.
    let mut released = Vec::new();
    for event in durable_of(&adoptions, 3) {
        released.extend(f.step(event));
    }
    assert_eq!(f.table().phase_of(&c1), Some(Phase::Commit));
    assert_eq!(f.table().phase_of(&c2), Some(Phase::Commit));
    let slow: Vec<_> = sends(&released)
        .into_iter()
        .filter(|(_, m)| matches!(m, ProtocolMessage::SlowAck(_)))
        .collect();
    assert_eq!(
        slow.len(),
        6,
        "two slow acks to two voters and the frontend"
    );
    assert!(f.take_rejections().is_empty());
}

#[test]
fn conflict_arrival_permutations_converge_on_the_leader_order() {
    // Two followers see c1/c2 in opposite orders; the leader saw c1, c2.
    let (e1, c1) = admitted(1, 1, 1);
    let (e2, c2) = admitted(2, 2, 2);
    let mut leader_view = coord_consensus::CommandTable::new();
    let l1 = leader_view
        .initialize(c1, c1.0, vec![CONSERVATIVE_KEY.to_vec()])
        .unwrap();
    let l2 = leader_view
        .initialize(c2, c2.0, vec![CONSERVATIVE_KEY.to_vec()])
        .unwrap();
    let mut a = booted(1);
    let mut b = booted(1);
    for (f, order) in [(&mut a, [e1.clone(), e2.clone()]), (&mut b, [e2, e1])] {
        for e in order {
            let effects = f.step(e);
            f.step(durable_of(&effects, 1).remove(0));
        }
    }
    assert_eq!(a.table().record(&c2).unwrap().deps, vec![c1]);
    assert_eq!(
        b.table().record(&c2).unwrap().deps,
        vec![],
        "b saw c2 first"
    );
    assert_eq!(b.table().record(&c1).unwrap().deps, vec![c2]);
    assert_ne!(
        a.table().record(&c2).unwrap().path,
        b.table().record(&c2).unwrap().path,
        "different paths: b cannot support a fast decision for c2"
    );
    // Both adopt the leader's order; afterwards their dependency state is
    // identical and each published a slow ack for both commands.
    for f in [&mut a, &mut b] {
        let p1 = proposal(c1, vec![], 0, l1.paths.clone(), l1.path);
        let p2 = proposal(c2, vec![c1], 1, l2.paths.clone(), l2.path);
        let mut effects = f.step(peer(0, p2));
        effects.extend(f.step(peer(0, p1)));
        let mut acks = 0;
        for d in durable_of(&effects, 5) {
            acks += sends(&f.step(d)).len();
        }
        assert_eq!(acks, 6);
        assert_eq!(f.table().record(&c1).unwrap().deps, vec![]);
        assert_eq!(f.table().record(&c2).unwrap().deps, vec![c1]);
        assert_eq!(f.table().phase_of(&c1), Some(Phase::Commit));
        assert_eq!(f.table().phase_of(&c2), Some(Phase::Commit));
        assert!(f.held().is_empty());
    }
    // A proposal from a non-leader, or for another ballot, is foreign.
    let mut stray = FastAck {
        replica: r(2),
        ballot: ballot(0, 0),
        command: c1,
        deps: vec![],
        paths: vec![],
        path: Digest32([0; 32]),
        admission: admitted_under(),
        seqnum: Some(9),
    };
    assert!(
        a.step(peer(2, ProtocolMessage::Proposal(stray.clone())))
            .is_empty()
    );
    stray.replica = r(0);
    stray.ballot = ballot(1, 0);
    assert!(a.step(peer(0, ProtocolMessage::Proposal(stray))).is_empty());
    assert_eq!(
        a.take_rejections(),
        vec![
            FollowerRejection::ForeignProposal,
            FollowerRejection::ForeignProposal
        ]
    );
}

#[test]
fn a_crash_between_state_and_vote_preserves_the_learning_obligation() {
    let (e1, c1) = admitted(1, 1, 1);
    // Crash before the batch is durable: nothing was sent, and the
    // recovered follower knows nothing, so it votes afresh from the same
    // request (same deps, same path).
    let mut f = booted(1);
    let mut storage = StorageModel::default();
    let effects = f.step(e1.clone());
    let Effect::Persist(batch) = effects[0].clone() else {
        panic!()
    };
    storage.submit(batch.clone());
    assert_eq!(f.pending_sends(), 3, "the vote waits");
    assert_eq!(storage.crash(), 1);
    let rows = restore_rows(&storage);
    assert!(rows.is_empty());
    let mut recovered = Follower::recover(
        config(1),
        None,
        rows,
        restore_payloads(&storage),
        ExecutionPosition::ZERO,
    );
    recovered.step(boot_event());
    assert_eq!(
        recovered.pending_sends(),
        0,
        "nothing to replay: no vote existed"
    );
    let again = recovered.step(e1.clone());
    let Effect::Persist(batch2) = &again[0] else {
        panic!()
    };
    assert_eq!(
        batch2.updates, batch.updates,
        "the same durable state is proposed"
    );

    // Durable, then crash: the vote is a fact. The recovered table carries
    // the record with its phase, dependencies and path, the conflict index
    // is rebuilt from it, and a repeated request is a duplicate.
    let mut f = booted(1);
    let mut storage = StorageModel::default();
    let effects = f.step(e1.clone());
    let Effect::Persist(batch) = effects[0].clone() else {
        panic!()
    };
    storage.submit(batch);
    storage.complete(effects_barrier(&effects)).unwrap();
    let released = f.step(durable_of(&effects, 1).remove(0));
    assert_eq!(sends(&released).len(), 3);
    storage.crash();
    let rows = restore_rows(&storage);
    assert_eq!(rows.len(), 1);
    let payloads = restore_payloads(&storage);
    assert_eq!(payloads.len(), 1, "the payload row carries the retry key");
    let mut recovered = Follower::recover(config(1), None, rows, payloads, ExecutionPosition::ZERO);
    recovered.step(boot_event());
    assert_eq!(recovered.table().phase_of(&c1), Some(Phase::PreAccept));
    assert_eq!(
        recovered.table().record(&c1).unwrap().path,
        f.table().record(&c1).unwrap().path
    );
    assert_eq!(
        recovered.table().conflicts(&[CONSERVATIVE_KEY.to_vec()]),
        vec![c1],
        "index rebuilt from the record"
    );
    assert_eq!(
        recovered.table().path_head(CONSERVATIVE_KEY),
        f.table().path_head(CONSERVATIVE_KEY)
    );
    // A later command depends on the recovered one, as it would have
    // before the crash.
    let (e2, c2) = admitted(2, 2, 2);
    let effects = recovered.step(e2.clone());
    assert_eq!(recovered.table().record(&c2).unwrap().deps, vec![c1]);
    let _ = effects;
    let mut fresh = booted(1);
    fresh.step(e1);
    fresh.step(e2);
    assert_eq!(
        fresh.table().record(&c2).unwrap().path,
        recovered.table().record(&c2).unwrap().path,
        "identical path evidence with and without the crash"
    );
    // A failed batch is reported and leaves the command where it was.
    let mut g = booted(1);
    let (e3, c3) = admitted(3, 3, 3);
    let effects = g.step(e3);
    let Effect::Persist(b) = &effects[0] else {
        panic!()
    };
    assert!(
        g.step(Event::Storage(StorageEvent::Failed {
            barrier_id: b.barrier,
            error: StorageError::NoSpace
        }))
        .is_empty()
    );
    assert_eq!(
        g.take_rejections(),
        vec![FollowerRejection::BatchFailed(c3)]
    );
    assert_eq!(g.pending_sends(), 0, "the vote is dropped with its batch");
}

fn effects_barrier(effects: &[Effect]) -> coord_core::effect::BarrierId {
    match &effects[0] {
        Effect::Persist(b) => b.barrier,
        _ => panic!(),
    }
}

/// The durable payload rows: what recovery rebuilds retry-key bindings
/// from.
fn restore_payloads(storage: &StorageModel) -> Vec<(CommandId, PayloadRecordV1)> {
    storage
        .durable_rows()
        .into_iter()
        .filter(|(c, _, _)| *c == Collection::PayloadV1.id().0)
        .map(|(_, k, v)| {
            (
                CommandId(Digest32(k[..].try_into().unwrap())),
                decode_payload(&v).unwrap(),
            )
        })
        .collect()
}

fn restore_rows(storage: &StorageModel) -> Vec<(CommandId, CommandRecord)> {
    storage
        .durable_rows()
        .into_iter()
        .filter(|(c, k, _)| *c == Collection::ProtocolV1.id().0 && k.len() == 41 && k[8] == 0x01)
        .map(|(_, k, v)| {
            (
                CommandId(Digest32(k[9..].try_into().unwrap())),
                decode_dependency(&v).unwrap(),
            )
        })
        .collect()
}

#[test]
fn equal_direct_dependencies_are_not_learning_and_guards_are_explicit() {
    let mut f = booted(1);
    let (e1, c1) = admitted(1, 1, 1);
    let effects = f.step(e1);
    f.step(durable_of(&effects, 1).remove(0));
    let record = f.table().record(&c1).unwrap().clone();
    // A peer fast ack with the same direct deps but a different path, and
    // the leader's proposal with the same path: collected, never acted on.
    let other_path = FastAck {
        replica: r(2),
        ballot: ballot(0, 0),
        command: c1,
        deps: record.deps.clone(),
        paths: vec![(CONSERVATIVE_KEY.to_vec(), Digest32([7; 32]))],
        path: Digest32([7; 32]),
        admission: record.payload.expect("initialized"),
        seqnum: None,
    };
    assert!(
        f.step(peer(2, ProtocolMessage::FastAck(other_path)))
            .is_empty()
    );
    assert_eq!(
        f.take_rejections(),
        vec![FollowerRejection::Vote(
            coord_consensus::VoteError::NotInFastSet
        )],
        "r2 is outside the fast set: its fast ack never counts"
    );
    let p1 = proposal(c1, vec![], 0, record.paths.clone(), record.path);
    let adoption = f.step(peer(0, p1));
    assert_eq!(adoption.len(), 1);
    f.step(durable_of(&adoption, 2).remove(0));
    let votes = f.votes(&c1).unwrap();
    assert!(votes.learned_slow().is_some(), "the slow predicate holds");
    assert_eq!(
        f.table().phase_of(&c1),
        Some(Phase::Commit),
        "learned slowly: committed, executed only once applied"
    );
    assert_eq!(f.next_executable(), Some(c1));
    // The guard is explicit: a proposal whose dependency is unknown here
    // stays held, and the table refuses the transition outright.
    let (_, c9) = admitted(9, 9, 9);
    let (e2, c2) = admitted(2, 2, 2);
    let effects = f.step(e2);
    f.step(durable_of(&effects, 3).remove(0));
    let p2 = proposal(c2, vec![c9], 1, record.paths.clone(), record.path);
    assert!(f.step(peer(0, p2)).is_empty());
    assert_eq!(f.held().len(), 1);
    assert_eq!(f.table().phase_of(&c2), Some(Phase::PreAccept));
    assert_eq!(
        f.table().clone().accept(c2, vec![c9]),
        Err(coord_consensus::GuardViolation::DependencyUnknown { dep: c9 })
    );
}

#[test]
fn an_adoption_acknowledgement_waits_for_the_payload_batch_too() {
    // A proposal arrives before the payload. When the payload is admitted,
    // the payload batch and the adoption batch are emitted together; the
    // slow acknowledgement claims both, so completing the adoption batch
    // alone must not release it.
    let mut f = booted(2); // r2 is outside the fast set: it adopts
    let (e1, c1) = admitted(1, 1, 1);
    assert!(
        f.step(peer(
            0,
            proposal(c1, vec![], 0, vec![], coord_consensus::empty_path())
        ))
        .is_empty(),
        "held until the payload arrives"
    );
    let effects = f.step(e1);
    let batches: Vec<_> = effects
        .iter()
        .filter_map(|e| match e {
            Effect::Persist(b) => Some(b.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(batches.len(), 2, "payload batch and adoption batch");
    let (payload, adoption) = (batches[0].clone(), batches[1].clone());
    assert_eq!(f.pending_sends(), 3, "the adoption acknowledgement waits");
    // The adoption batch alone is not enough: the payload row is still
    // volatile, so a crash now would leave an accepted dependency row
    // without the payload it accepted.
    let released = f.step(Event::Storage(StorageEvent::JournalDurable {
        barrier_id: adoption.barrier,
        journal_seq: LocalJournalSeq::new(1).unwrap(),
    }));
    assert!(
        sends(&released).is_empty(),
        "nothing leaves while the payload batch is volatile: {released:?}"
    );
    assert_eq!(f.pending_sends(), 3);
    let released = f.step(Event::Storage(StorageEvent::JournalDurable {
        barrier_id: payload.barrier,
        journal_seq: LocalJournalSeq::new(2).unwrap(),
    }));
    assert_eq!(
        sends(&released).len(),
        3,
        "both batches durable: the acknowledgement goes out"
    );
    assert_eq!(f.pending_sends(), 0);
}

#[test]
fn a_higher_promise_in_flight_fences_the_configured_ballot() {
    let mut f = booted(1);
    let (e1, c1) = admitted(1, 1, 1);
    // r2 campaigns for a higher ballot: the promise row is persisted and
    // is still in flight.
    let effects = f.step(peer(
        2,
        ProtocolMessage::NewLeader {
            ballot: ballot(1, 2),
        },
    ));
    assert!(matches!(effects.as_slice(), [Effect::Persist(_), ..]));
    assert!(f.ballots().in_flight().is_some());
    assert_eq!(
        f.ballots().promised(),
        ballot(0, 0),
        "the old promise is still the durable one"
    );
    // Nothing of the old ballot is adopted or acknowledged after the cut,
    // although the promise row is not durable yet and the old promise is
    // what `release` still compares against.
    assert!(f.step(e1).is_empty(), "no admission under the old ballot");
    assert!(
        f.step(peer(
            0,
            proposal(c1, vec![], 0, vec![], coord_consensus::empty_path())
        ))
        .is_empty(),
        "no adoption of an old-ballot proposal"
    );
    assert_eq!(
        f.take_rejections(),
        vec![
            FollowerRejection::FencedByPromise {
                promised: ballot(0, 0)
            },
            FollowerRejection::FencedByPromise {
                promised: ballot(0, 0)
            }
        ]
    );
    assert_eq!(f.table().phase_of(&c1), None, "nothing was initialized");
    assert_eq!(f.pending_sends(), 1, "only the promise reply waits");
}

#[test]
fn recovery_rebuilds_retry_key_bindings_from_durable_payloads() {
    let mut f = booted(1);
    let mut storage = StorageModel::default();
    let (e1, c1) = admitted(1, 1, 1);
    let effects = f.step(e1);
    let Effect::Persist(batch) = effects[0].clone() else {
        panic!()
    };
    storage.submit(batch);
    storage.complete(effects_barrier(&effects)).unwrap();
    f.step(durable_of(&effects, 1).remove(0));
    storage.crash();
    let payloads = restore_payloads(&storage);
    assert_eq!(payloads.len(), 1);
    assert_eq!(payloads[0].0, c1);
    let mut recovered = Follower::recover(
        config(1),
        None,
        restore_rows(&storage),
        payloads,
        ExecutionPosition::ZERO,
    );
    recovered.step(boot_event());
    // The same retry key with other bytes derives another command; without
    // the rebuilt binding it would be admitted as a second command.
    let other = admitted_with_key(1, 9, 9);
    assert!(recovered.step(other.0).is_empty());
    assert_eq!(
        recovered.take_rejections(),
        vec![FollowerRejection::RequestIdentityConflict {
            retry_key: retry_key(1),
            bound: c1
        }]
    );
    assert_eq!(recovered.table().phase_of(&other.1), None);
    // The same bytes are the same command: a duplicate, not new work.
    let (again, _) = admitted(1, 1, 1);
    assert!(recovered.step(again).is_empty());
    assert_eq!(
        recovered.take_rejections(),
        vec![FollowerRejection::Duplicate(c1)]
    );
}

/// An admitted request reusing `seq`'s retry key with other payload bytes.
fn admitted_with_key(seq: u64, key: u8, value: u8) -> (Event, CommandId) {
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
    let frame = MessageV1::Request(RequestV1::new(retry_key(seq), &request, 0).unwrap())
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

#[test]
fn the_synchronized_path_anchor_survives_a_crash() {
    // The follower initializes c1 locally, then the leader's order arrives
    // with a different anchor. A restored follower must resume its log at
    // the leader's anchor, not the stale local digest, so the next command
    // carries the same evidence as on the live follower.
    let mut f = booted(1);
    let mut storage = StorageModel::default();
    let (e1, c1) = admitted(1, 1, 1);
    let mut batches = persisted(&f.step(e1));
    let leader_anchor = Digest32([0x5a; 32]);
    // The leader's order arrives and is adopted at once: its batch carries
    // the record with the synchronized anchor.
    batches.extend(persisted(&f.step(peer(
        0,
        proposal(
            c1,
            vec![],
            7,
            vec![(CONSERVATIVE_KEY.to_vec(), leader_anchor)],
            leader_anchor,
        ),
    ))));
    assert_eq!(f.table().path_head(CONSERVATIVE_KEY), leader_anchor);
    let record = f.table().record(&c1).unwrap();
    assert_eq!(record.synced_seq, Some(7));
    assert_eq!(
        record.paths,
        vec![(CONSERVATIVE_KEY.to_vec(), leader_anchor)]
    );
    assert_eq!(batches.len(), 2, "payload batch and adoption batch");
    for batch in batches {
        let barrier = batch.barrier;
        storage.submit(batch);
        storage.complete(barrier).unwrap();
    }
    storage.crash();
    let recovered = Follower::recover(
        config(1),
        None,
        restore_rows(&storage),
        restore_payloads(&storage),
        ExecutionPosition::ZERO,
    );
    assert_eq!(
        recovered.table().record(&c1).unwrap().synced_seq,
        Some(7),
        "the synchronized sequence is durable"
    );
    assert_eq!(
        recovered.table().path_head(CONSERVATIVE_KEY),
        f.table().path_head(CONSERVATIVE_KEY),
        "the restored log resumes at the leader's anchor"
    );
}

/// The batches of a step, in order.
fn persisted(effects: &[Effect]) -> Vec<coord_core::effect::PersistBatch> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::Persist(b) => Some(b.clone()),
            _ => None,
        })
        .collect()
}

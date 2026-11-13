//! task-23 acceptance: leader/follower message races, conflict arrival
//! permutations, duplicate identities and crashes between state and vote
//! preserve learning obligations; half-initialized dependencies never
//! appear; dependency-phase guards are explicit; equal direct dependencies
//! are not a learning proof.

use std::collections::BTreeSet;

use coord_consensus::{
    BallotConfiguration, CONSERVATIVE_KEY, CommandRecord, ConfigurationIdentity, FastAck, Follower,
    FollowerConfig, FollowerRejection, FramedProposal, PayloadRecordV1, Phase, ProtocolMessage,
    RelayTaken, ReplayRefusal, ReplicaRole, SlowAck, decode_dependency, decode_payload,
    dependency_key,
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
use coord_types::wire_v1::{BoundedBytes, MessageV1, RequestV1};
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

fn peer(from: u8, message: ProtocolMessage) -> Event {
    Event::Peer(AuthenticatedPeerMessage::new(
        PeerProvenance::from_transport(
            r(from),
            ReplicaIncarnation::new(1).unwrap(),
            coord_types::wire_v1::PeerRole::Voter,
            1,
        ),
        message.encode(),
    ))
}

/// The leader's own adoption of `command`, sent once its acceptance row is
/// durable (task-d19): the leader's copy of the slow majority.
fn leader_adoption(f: &Follower, command: CommandId) -> Event {
    peer(
        0,
        ProtocolMessage::SlowAck(SlowAck {
            replica: r(0),
            ballot: ballot(0, 0),
            command,
            admission: f
                .table()
                .record(&command)
                .and_then(|r| r.payload)
                .expect("admitted"),
        }),
    )
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
    coord_core::capability::admission_digest(
        Some(&coord_core::capability::AdmissionFacts {
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
        }),
        0,
    )
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
    assert_eq!(
        fast.pending_sends(),
        2,
        "the leader and the frontend, not the other voter (task-d74)"
    );
    let released = fast.step(durable_of(&effects, 1).remove(0));
    let sent = sends(&released);
    assert_eq!(sent.len(), 2);
    for (to, m) in &sent {
        assert!(*to == r(0) || *to == FRONTEND.replica);
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
    // Duplicate identities: the same request again is answered with the
    // same acknowledgement, to the frontend only (task-c02); a different
    // payload under the same retry key is refused.
    let again = sends(&fast.step(admitted(1, 1, 1).0));
    assert_eq!(again.len(), 1, "one send, and only to the frontend");
    assert_eq!(again[0].0, FRONTEND.replica);
    assert!(
        matches!(&again[0].1, ProtocolMessage::FastAck(a) if a.replica == r(1) && a.path == row.path),
        "the same acknowledgement, not a new one: {:?}",
        again[0].1
    );
    // Answered, to the frontend only, with why (task-d22).
    let refused = sends(&fast.step(admitted(1, 1, 7).0));
    assert!(
        matches!(refused.as_slice(), [(to, ProtocolMessage::Refused {
            refusal: coord_consensus::SubmissionRefusal::OtherCommand { bound }, ..
        })] if *to == FRONTEND.replica && *bound == c1),
        "{refused:?}"
    );
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
    // slow vote count, and with the leader's adoption the commands are
    // learned. The leader's proposal alone is not its adoption (task-d19).
    let mut released = Vec::new();
    for event in durable_of(&adoptions, 3) {
        released.extend(f.step(event));
    }
    assert_eq!(
        f.table().phase_of(&c1),
        Some(Phase::Accept),
        "the leader's acceptance is not known durable yet"
    );
    for c in [c1, c2] {
        let adoption = leader_adoption(&f, c);
        released.extend(f.step(adoption));
    }
    assert_eq!(f.table().phase_of(&c1), Some(Phase::Commit));
    assert_eq!(f.table().phase_of(&c2), Some(Phase::Commit));
    let slow: Vec<_> = sends(&released)
        .into_iter()
        .filter(|(_, m)| matches!(m, ProtocolMessage::SlowAck(_)))
        .collect();
    assert_eq!(
        slow.len(),
        4,
        "two slow acks to the leader and the frontend (task-d74)"
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
        assert_eq!(acks, 4, "to the leader and the frontend (task-d74)");
        for c in [c1, c2] {
            let adoption = leader_adoption(f, c);
            f.step(adoption);
        }
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
    assert_eq!(f.pending_sends(), 2, "the vote waits");
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
    assert_eq!(sends(&released).len(), 2, "the leader and the frontend");
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

/// task-d98: a follower outside the fast set that gets the leader's
/// proposal before the leader's batch is durable must not complete a fast
/// quorum with a fast-set member's acknowledgement. Only a build from
/// before task-d74 sends one to a follower, and counted with an early
/// proposal it would decide over a vote the leader may lose. The follower
/// takes the decision from the leader's commit frontier instead.
#[test]
fn a_follower_never_counts_another_voters_fast_ack_toward_a_fast_decision() {
    // r2 is outside the fast set {r0, r1} at three voters.
    let mut f = booted(2);
    let (e1, c1) = admitted(1, 1, 1);
    let effects = f.step(e1);
    f.step(durable_of(&effects, 1).remove(0));
    let record = f.table().record(&c1).unwrap().clone();
    // r1's fast ack, equal to the leader's proposal in path and deps, as a
    // pre-task-d74 build would send it to every voter.
    let r1_ack = FastAck {
        replica: r(1),
        ballot: ballot(0, 0),
        command: c1,
        deps: record.deps.clone(),
        paths: record.paths.clone(),
        path: record.path,
        admission: record.payload.expect("initialized"),
        seqnum: None,
    };
    assert!(f.step(peer(1, ProtocolMessage::FastAck(r1_ack))).is_empty());
    let adoption = f.step(peer(
        0,
        proposal(
            c1,
            record.deps.clone(),
            0,
            record.paths.clone(),
            record.path,
        ),
    ));
    f.step(durable_of(&adoption, 2).remove(0));
    assert!(
        f.votes(&c1).unwrap().learned().is_none(),
        "the leader's proposal and nothing else: no fast quorum"
    );
    assert_ne!(
        f.table().phase_of(&c1),
        Some(Phase::Commit),
        "not committed until the leader's frontier says so"
    );
    assert!(
        f.take_rejections()
            .contains(&FollowerRejection::PeerFastAck),
        "r1's fast ack was refused, not counted"
    );
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
        vec![FollowerRejection::PeerFastAck],
        "a follower never counts another voter's fast ack (task-d98)"
    );
    let p1 = proposal(c1, vec![], 0, record.paths.clone(), record.path);
    let adoption = f.step(peer(0, p1));
    assert_eq!(adoption.len(), 1);
    f.step(durable_of(&adoption, 2).remove(0));
    assert!(
        f.votes(&c1).unwrap().learned_slow().is_none(),
        "the leader's proposal is not its adoption"
    );
    let leader = leader_adoption(&f, c1);
    f.step(leader);
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
    assert_eq!(f.pending_sends(), 2, "the adoption acknowledgement waits");
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
    assert_eq!(f.pending_sends(), 2);
    let released = f.step(Event::Storage(StorageEvent::JournalDurable {
        barrier_id: payload.barrier,
        journal_seq: LocalJournalSeq::new(2).unwrap(),
    }));
    assert_eq!(
        sends(&released).len(),
        2,
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
            executed: coord_types::ids::ExecutionPosition::ZERO,
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
    // Refused with the reason, since the binding is durable (task-d22).
    let refused = recovered.step(other.0);
    assert!(
        refused.iter().all(|e| matches!(e, Effect::SendWhenDurable { to, frame, .. }
            if *to == FRONTEND && matches!(ProtocolMessage::decode(frame),
                Ok(ProtocolMessage::Refused { refusal: coord_consensus::SubmissionRefusal::OtherCommand { bound }, .. }) if bound == c1)))
            && refused.len() == 1,
        "{refused:?}"
    );
    assert_eq!(
        recovered.take_rejections(),
        vec![FollowerRejection::RequestIdentityConflict {
            retry_key: retry_key(1),
            bound: c1
        }]
    );
    assert_eq!(recovered.table().phase_of(&other.1), None);
    // The same bytes are the same command: a duplicate, not new work.
    // Nothing is replayed for it either: the acknowledgement was the
    // previous boot's, published through an outbox this boot does not
    // have, and the durable record of the execution is the way back
    // (task-c02).
    let (again, _) = admitted(1, 1, 1);
    assert!(recovered.step(again).is_empty());
    assert_eq!(
        recovered.take_rejections(),
        vec![
            FollowerRejection::Duplicate(c1),
            FollowerRejection::ReplayRefused {
                command: c1,
                why: ReplayRefusal::NothingToReplay
            }
        ]
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

// ---------------------------------------------------------------------
// task-55: a seal fences the follower's ordinary voting.
// ---------------------------------------------------------------------

fn transition() -> coord_consensus::handoff::Transition {
    coord_consensus::handoff::Transition {
        from: epoch(),
        to: ConfigurationEpoch::new(2).unwrap(),
        subject: Digest32([0xa1; 32]),
    }
}

/// Once sealed, a follower adopts and acknowledges nothing of the old
/// ballot: not a request, not a proposal, and not a proposal it held
/// before the seal whose payload arrives after it. A restart that reads
/// the seal row back is just as fenced.
#[test]
fn a_seal_stops_the_follower_voting_live_and_after_a_restart() {
    let mut f = booted(1);
    let (e1, c1) = admitted(1, 1, 1);
    let (e2, c2) = admitted(2, 2, 2);
    // c1's proposal arrives before its payload and is held.
    assert!(
        f.step(peer(
            0,
            proposal(c1, vec![], 0, vec![], coord_consensus::empty_path())
        ))
        .is_empty()
    );
    let seal = f.step(peer(
        0,
        ProtocolMessage::SealRequest {
            transition: transition(),
        },
    ));
    let [Effect::Persist(row)] = seal.as_slice() else {
        panic!("a seal persists its row: {seal:?}")
    };
    f.step(Event::Storage(StorageEvent::JournalDurable {
        barrier_id: row.barrier,
        journal_seq: LocalJournalSeq::new(1).unwrap(),
    }));
    assert!(f.ballots().is_sealed());

    assert!(f.step(e1).is_empty(), "no fast vote for the held proposal");
    assert!(f.step(e2).is_empty(), "no fast vote for a new request");
    assert!(
        f.step(peer(
            0,
            proposal(c2, vec![], 1, vec![], coord_consensus::empty_path())
        ))
        .is_empty(),
        "no adoption of a proposal"
    );
    let sealed = FollowerRejection::Sealed {
        transition: transition(),
    };
    assert_eq!(f.take_rejections(), vec![sealed.clone(); 3]);
    assert_eq!(f.table().phase_of(&c2), None, "nothing was initialized");

    // The restart reads the row back and votes no more.
    let mut restarted = Follower::recover_with_syncs(
        config(1),
        None,
        Some(coord_consensus::SealRecordV1 {
            transition: transition(),
            at: ballot(0, 0),
        }),
        std::iter::empty(),
        std::iter::empty(),
        std::iter::empty(),
        ExecutionPosition::ZERO,
    );
    assert!(restarted.step(boot_event()).is_empty());
    let (e3, _) = admitted(3, 3, 3);
    assert!(restarted.step(e3).is_empty());
    assert!(
        restarted
            .step(peer(
                0,
                proposal(c2, vec![], 1, vec![], coord_consensus::empty_path())
            ))
            .is_empty()
    );
    assert_eq!(restarted.take_rejections(), vec![sealed; 2]);
}

/// A full table stops this replica taking new work. It must not stop it
/// finishing the work it has.
///
/// Both doors into the machine used to give up the moment the table was
/// full: a submission it had no room for, and a proposal it had no room
/// to place a marker for, each recorded the refusal and returned. What
/// they returned from is the function that ends by adopting the records
/// whose turn has come -- and adoption is what lets a command execute,
/// be retired, and free the slot that was missing. So a table that
/// filled stayed full, and stayed full for ever: the leader does not
/// re-propose, there is no message to ask it for an order it already
/// sent, and nothing else would prompt this replica to look again.
///
/// That is not a hypothetical. One voter of three did exactly this under
/// a benchmark: sixty-four records at PRE-ACCEPT, every later proposal
/// refused, its projection frozen, and -- because a frontend reads
/// replicated policy from that projection -- every read its own callers
/// asked for refused as unauthorized for the rest of the run.
#[test]
fn a_full_table_still_adopts_and_still_keeps_the_order_it_was_sent() {
    // Full for new admission: the capacity less the share kept for
    // recovery and catch-up (task-d24).
    let capacity = config(1).capacity;
    let capacity = capacity - capacity / coord_consensus::RECOVERY_RESERVE_PARTS;
    let mut f = booted(1);

    // The leader's view, so the proposals carry real path evidence.
    let mut leader_view = coord_consensus::CommandTable::new();
    let mut commands = Vec::new();
    let mut orders = Vec::new();
    for seq in 1..=(capacity as u64 + 2) {
        let (event, command) = admitted(seq, 1, seq as u8);
        let order = leader_view
            .initialize(command, command.0, vec![CONSERVATIVE_KEY.to_vec()])
            .unwrap();
        commands.push((seq, event, command));
        orders.push(order);
    }

    // Fill the table exactly, leaving nothing adopted: the proposals
    // arrive in reverse, so every one of them is held behind a
    // dependency that is still at PRE-ACCEPT.
    for (seq, event, _) in commands.iter().take(capacity) {
        let effects = f.step(event.clone());
        for durable in durable_of(&effects, *seq) {
            f.step(durable);
        }
    }
    assert_eq!(f.table().len(), capacity, "the table is exactly full");
    for index in (1..capacity).rev() {
        let (_, _, command) = commands[index];
        let deps = vec![commands[index - 1].2];
        let order = &orders[index];
        f.step(peer(
            0,
            proposal(command, deps, index as u64, order.paths.clone(), order.path),
        ));
    }
    assert_eq!(
        f.held().len(),
        capacity - 1,
        "every proposal but the first is held behind its dependency"
    );

    // Now the two commands there is no room for. Both doors are tried:
    // the submission and the proposal.
    let (overflow_seq, overflow_event, overflow) = commands[capacity].clone();
    let refused = f.step(overflow_event);
    assert!(
        f.take_rejections()
            .contains(&FollowerRejection::Backpressure),
        "a submission with no room is refused, and says so"
    );
    assert!(refused.is_empty(), "nothing was admitted");
    assert_eq!(f.table().phase_of(&overflow), None, "and nothing recorded");
    let _ = overflow_seq;

    let (_, _, beyond) = commands[capacity + 1];
    let order = &orders[capacity + 1];
    f.step(peer(
        0,
        proposal(
            beyond,
            vec![overflow],
            (capacity + 1) as u64,
            order.paths.clone(),
            order.path,
        ),
    ));
    assert!(
        f.take_rejections()
            .contains(&FollowerRejection::Backpressure),
        "a proposal with no room for a marker is refused, and says so"
    );
    assert!(
        f.held().contains_key(&beyond),
        "but the order it carries is kept: it is the only copy this \
         replica is ever offered, and the leader does not re-propose"
    );

    // And the refusals did not stop the machine finishing what it had.
    // The first command's proposal arrives; every held proposal behind
    // it adopts in the same turn, exactly as it would on an empty table.
    let order = &orders[0];
    let adoptions = f.step(peer(
        0,
        proposal(commands[0].2, vec![], 0, order.paths.clone(), order.path),
    ));
    assert_eq!(
        adoptions.len(),
        capacity,
        "a full table adopts every record whose turn has come"
    );
    for (_, _, command) in commands.iter().take(capacity) {
        assert!(
            f.table().phase_of(command) >= Some(Phase::Accept),
            "adopted under the leader's order, not left at {:?}",
            f.table().phase_of(command)
        );
    }
    assert_eq!(
        f.held().len(),
        1,
        "only the proposal whose command has no record is still held"
    );
}

/// A proposal held when this replica promises a higher ballot is not
/// adopted, and a payload that arrives after the promise is not voted on
/// (task-d12).
///
/// The report the candidate selects from is taken at the promise, so it
/// cannot name what this replica accepts after it. Adopting the held
/// proposal then let this replica's own acceptance and the old leader's
/// proposal make a quorum here: it committed and executed a command the
/// new ballot's selection never saw, and the new leader chained its first
/// command after the same predecessor (stress runs d12c-1, -7, -9, -11).
#[test]
fn a_proposal_held_across_a_higher_promise_is_not_accepted_after_it() {
    let mut f = booted(1);
    let (e1, c1) = admitted(1, 1, 1);
    let (_, c2) = admitted(2, 2, 2);
    let mut leader_view = coord_consensus::CommandTable::new();
    let l1 = leader_view
        .initialize(c1, c1.0, vec![CONSERVATIVE_KEY.to_vec()])
        .unwrap();
    let l2 = leader_view
        .initialize(c2, c2.0, vec![CONSERVATIVE_KEY.to_vec()])
        .unwrap();
    // c1 is accepted (and, with the fast votes, committed) in ballot 0.
    let effects = f.step(e1);
    for event in durable_of(&effects, 1) {
        f.step(event);
    }
    let adopted = f.step(peer(0, proposal(c1, vec![], 0, l1.paths.clone(), l1.path)));
    for event in durable_of(&adopted, 2) {
        f.step(event);
    }
    assert!(f.table().phase_of(&c1) >= Some(Phase::Accept));
    // c2's proposal arrives before its payload: held.
    assert!(
        f.step(peer(
            0,
            proposal(c2, vec![c1], 1, l2.paths.clone(), l2.path)
        ))
        .is_empty()
    );
    assert!(f.held().contains_key(&c2));
    // r2 campaigns; the promise becomes durable, and the report it answers
    // with names c1 and not c2.
    let promised = f.step(peer(
        2,
        ProtocolMessage::NewLeader {
            ballot: ballot(1, 2),
            executed: coord_types::ids::ExecutionPosition::ZERO,
        },
    ));
    for event in durable_of(&promised, 3) {
        f.step(event);
    }
    assert_eq!(f.ballots().promised(), ballot(1, 2));
    let report = f.report(ballot(1, 2));
    assert!(report.entries.iter().all(|e| e.command != c2));
    // c2's payload arrives now, as a fetched payload does.
    let request = LogicalRequest::new(
        NamespaceId([5; 16]),
        CanonicalOperation::Put(PutOp {
            key: vec![2],
            value: vec![2],
            lease: None,
            prev_kv: false,
        }),
    );
    let payload = PayloadRecordV1 {
        retry_key: retry_key(2),
        logical: postcard::to_allocvec(&request).unwrap(),
        admission: Some(coord_core::capability::AdmissionFacts {
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
        }),
        ack_through: 0,
    };
    assert_eq!(payload.admission_digest(), admitted_under());
    let mut effects = f.step(peer(
        0,
        ProtocolMessage::PayloadResponse {
            command: c2,
            payload,
        },
    ));
    let mut sent = Vec::new();
    let mut seq = 4;
    while !effects.is_empty() {
        sent.extend(sends(&effects));
        let mut next = Vec::new();
        for event in durable_of(&effects, seq) {
            next.extend(f.step(event));
        }
        seq += 1;
        effects = next;
    }
    assert!(
        f.table().phase_of(&c2) < Some(Phase::Accept),
        "c2 accepted after the promise: {:?}",
        f.table().phase_of(&c2)
    );
    assert!(
        sent.iter().all(|(_, m)| !matches!(
            m,
            ProtocolMessage::FastAck(a) if a.command == c2
        ) && !matches!(
            m,
            ProtocolMessage::SlowAck(a) if a.command == c2
        )),
        "acknowledged c2 in the ballot it promised away: {sent:?}"
    );
}

/// A proposal's arrival does not align this replica's path logs; only
/// adopting it does (task-d34, F7; vicaya on #118).
///
/// The leader ordered cz, c0, c1. r1 never received cz. It pre-accepted
/// c0 with its own dependencies (none it can see), then took c0's
/// proposal, which it cannot adopt while cz is unknown. Aligned on
/// arrival, its log took the leader's anchor for c0, and c1 pre-accepted
/// next carried the leader's path over r1's own history: an equal path
/// without cz behind it. With the leader absent, a recovery could not
/// rebuild the fast decision's ancestors from r1's records, and refused
/// the decided command. r1's path for c1 is now the one a replica that
/// never received the proposal computes.
#[test]
fn a_proposal_that_is_not_adopted_leaves_the_path_log_alone() {
    let (_, cz) = admitted(1, 1, 1);
    let (e0, c0) = admitted(2, 2, 2);
    let (e1, c1) = admitted(3, 3, 3);
    let mut leader_view = coord_consensus::CommandTable::new();
    for c in [cz, c0, c1] {
        leader_view
            .initialize(c, c.0, vec![CONSERVATIVE_KEY.to_vec()])
            .unwrap();
    }
    let l0 = leader_view.record(&c0).unwrap().clone();
    let l1 = leader_view.record(&c1).unwrap().clone();
    assert_eq!(l0.deps, vec![cz]);
    let mut aligned = booted(1);
    let mut plain = booted(1);
    for f in [&mut aligned, &mut plain] {
        let effects = f.step(e0.clone());
        f.step(durable_of(&effects, 1).remove(0));
    }
    // c0's proposal reaches `aligned`: held, since cz is unknown here.
    let p0 = proposal(c0, vec![cz], 1, l0.paths.clone(), l0.path);
    aligned.step(peer(0, p0));
    assert!(aligned.held().contains_key(&c0));
    assert_eq!(aligned.table().phase_of(&c0), Some(Phase::PreAccept));
    for f in [&mut aligned, &mut plain] {
        let effects = f.step(e1.clone());
        f.step(durable_of(&effects, 2).remove(0));
    }
    let a = aligned.table().record(&c1).unwrap();
    let p = plain.table().record(&c1).unwrap();
    assert_eq!(a.deps, vec![c0]);
    assert_eq!(
        a.path, p.path,
        "the path is this replica's own history, not the leader's"
    );
    assert_ne!(
        a.path, l1.path,
        "the leader's path for c1 runs through cz, which this replica never held"
    );
}

// ---------------------------------------------------------------------
// task-d90 (point 4): a vote rests on the payload row, not its digest.
// ---------------------------------------------------------------------

/// Durable dependency rows and durable payload rows, as recovery reads them.
type DurableRows = (
    Vec<(CommandId, CommandRecord)>,
    Vec<(CommandId, PayloadRecordV1)>,
);

/// A follower outside the fast set that admitted `c1` and made its batch
/// durable, then crashed: its durable rows, with the dependency row and
/// the payload row in hand separately.
fn durable_admission() -> (CommandId, DurableRows) {
    let mut f = booted(2);
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
    (c1, (restore_rows(&storage), restore_payloads(&storage)))
}

/// A record whose dependency row is durable and whose payload row is not
/// -- restored from the one without the other, as after a Sync installed
/// it or a payload batch was lost -- is initialized, with its admission
/// digest, and holds no payload. It is reported without one, a proposal
/// for it is adopted only once the payload is back, and the adoption's
/// acknowledgement waits for the batch that writes it (task-d90, design
/// Section 4.5, point 4). Adopted over the digest, its slow vote counted
/// toward a decision no recovery could re-execute from this voter.
#[test]
fn an_adoption_waits_for_the_payload_row_not_only_its_digest() {
    let (c1, (rows, mut payloads)) = durable_admission();
    let payload = payloads.remove(0).1;
    let mut g = Follower::recover(config(2), None, rows, Vec::new(), ExecutionPosition::ZERO);
    g.step(boot_event());
    assert!(g.table().is_initialized(&c1));
    assert!(g.payload(&c1).is_none());
    let reported = |g: &Follower| {
        g.report(ballot(1, 1))
            .entries
            .into_iter()
            .find(|e| e.command == c1)
            .expect("reported")
            .payload_present
    };
    assert!(!reported(&g), "no payload row: none reported present");
    // The leader's proposal is held for the payload, which is asked for.
    let record = g.table().record(&c1).unwrap().clone();
    let held = g.step(peer(
        0,
        proposal(c1, vec![], 0, record.paths.clone(), record.path),
    ));
    assert!(
        persisted(&held).is_empty(),
        "adopted over the digest alone: {held:?}"
    );
    assert_eq!(g.held().len(), 1);
    assert!(g.missing_payloads().contains(&c1));
    // The payload arrives: its batch and the adoption's, and the
    // acknowledgement waits for both.
    let effects = g.step(peer(
        0,
        ProtocolMessage::PayloadResponse {
            command: c1,
            payload,
        },
    ));
    let batches = persisted(&effects);
    assert_eq!(batches.len(), 2, "the payload batch and the adoption");
    let (payload_batch, adoption) = (batches[0].barrier, batches[1].barrier);
    let released = g.step(Event::Storage(StorageEvent::JournalDurable {
        barrier_id: adoption,
        journal_seq: LocalJournalSeq::new(1).unwrap(),
    }));
    assert!(
        sends(&released).is_empty(),
        "the payload row is still volatile: {released:?}"
    );
    assert!(!reported(&g));
    let released = g.step(Event::Storage(StorageEvent::JournalDurable {
        barrier_id: payload_batch,
        journal_seq: LocalJournalSeq::new(2).unwrap(),
    }));
    assert_eq!(sends(&released).len(), 2, "the leader and the frontend");
    assert!(reported(&g), "durable now, and reported present");
}

/// A decided record is answered from its durable record when the leader
/// proposes it again (task-d07), and only with its payload row durable
/// here too (task-d90, point 4): without one the answer was a vote no
/// recovery could re-execute from.
#[test]
fn a_decided_record_is_acknowledged_again_only_with_its_payload_row() {
    let (c1, (mut rows, payloads)) = durable_admission();
    rows[0].1.phase = Phase::Commit;
    let record = rows[0].1.clone();
    let again = || {
        peer(
            0,
            proposal(c1, vec![], 0, record.paths.clone(), record.path),
        )
    };
    let mut without = Follower::recover(
        config(2),
        None,
        rows.clone(),
        Vec::new(),
        ExecutionPosition::ZERO,
    );
    without.step(boot_event());
    let out = without.step(again());
    assert!(sends(&out).is_empty(), "acknowledged without a payload row");
    assert_eq!(without.pending_sends(), 0);
    let mut with = Follower::recover(config(2), None, rows, payloads, ExecutionPosition::ZERO);
    with.step(boot_event());
    let out = with.step(again());
    assert!(
        matches!(&sends(&out)[..], [(to, ProtocolMessage::SlowAck(_))] if *to == r(0)),
        "{out:?}"
    );
}

/// The payload batch an adoption waited on fails (#195 review): the
/// acknowledgement it held was dropped, and kept evidence that names the
/// failed batch would be dropped again each time it was published. It is
/// forgotten. The acceptance is durable without its payload row, so the
/// next event that is not a failure writes the payload row again (#197
/// review), and the leader's next proposal is adopted again, its
/// acknowledgement waiting on that row.
#[test]
fn a_failed_payload_batch_is_written_again_by_the_next_adoption() {
    let (c1, (rows, mut payloads)) = durable_admission();
    let payload = payloads.remove(0).1;
    let mut g = Follower::recover(config(2), None, rows, Vec::new(), ExecutionPosition::ZERO);
    g.step(boot_event());
    let record = g.table().record(&c1).unwrap().clone();
    let again = || {
        peer(
            0,
            proposal(c1, vec![], 0, record.paths.clone(), record.path),
        )
    };
    g.step(again());
    let effects = g.step(peer(
        0,
        ProtocolMessage::PayloadResponse {
            command: c1,
            payload,
        },
    ));
    let batches = persisted(&effects);
    let (payload_batch, adoption) = (batches[0].barrier, batches[1].barrier);
    g.step(Event::Storage(StorageEvent::Failed {
        barrier_id: payload_batch,
        error: StorageError::NoSpace,
    }));
    let released = g.step(Event::Storage(StorageEvent::JournalDurable {
        barrier_id: adoption,
        journal_seq: LocalJournalSeq::new(1).unwrap(),
    }));
    assert!(
        sends(&released).is_empty(),
        "the payload row was never written"
    );
    let repair = persisted(&released);
    assert_eq!(
        repair.len(),
        1,
        "the payload row written again: {released:?}"
    );
    assert!(
        repair[0]
            .updates
            .iter()
            .all(|u| u.collection == Collection::PayloadV1.id())
    );
    // The leader proposes again, lacking this voter's vote.
    let effects = g.step(again());
    let batches = persisted(&effects);
    assert_eq!(batches.len(), 1, "adopted again: {effects:?}");
    let mut sent = Vec::new();
    for (seq, barrier) in [(2, repair[0].barrier), (3, batches[0].barrier)] {
        sent.extend(sends(&g.step(Event::Storage(
            StorageEvent::JournalDurable {
                barrier_id: barrier,
                journal_seq: LocalJournalSeq::new(seq).unwrap(),
            },
        ))));
    }
    assert!(
        sent.iter()
            .any(|(to, m)| *to == r(0) && matches!(m, ProtocolMessage::SlowAck(_))),
        "{sent:?}"
    );
}

/// A submission's own batch, which writes its payload row and its
/// dependency row, fails (task-d90, point 4, the fault of protocol_sim's
/// row 21): the record stays initialized in memory with its payload, and
/// no row holds it. The adoption of the leader's proposal for it writes
/// the payload row again, which the vote waits on.
#[test]
fn a_vote_after_its_submissions_batch_failed_writes_the_payload_row_again() {
    let mut f = booted(1);
    let (e1, c1) = admitted(1, 1, 1);
    let effects = f.step(e1);
    f.step(Event::Storage(StorageEvent::Failed {
        barrier_id: effects_barrier(&effects),
        error: StorageError::DefinitelyNotCommitted,
    }));
    let record = f
        .table()
        .record(&c1)
        .expect("initialized in memory")
        .clone();
    let effects = f.step(peer(
        0,
        proposal(c1, vec![], 0, record.paths.clone(), record.path),
    ));
    let batches = persisted(&effects);
    assert!(
        batches.iter().any(|b| b
            .updates
            .iter()
            .any(|u| u.collection == Collection::PayloadV1.id() && u.value.is_some())),
        "the vote's batch writes the payload row again: {batches:?}"
    );
}

/// A vote's batch fails (task-d90): the record stays in the table, so the
/// next vote on its keys names it as a dependency. Nothing is written at
/// the failure; the next batch that names it carries its payload and
/// dependency rows, so they are durable with that vote or fail with it.
/// Before, nothing wrote the record again, the later vote's row named a
/// command a restart lost, and a recovery read the gap as another order
/// than the leader's and re-proposed a command a fast quorum had decided
/// (protocol_sim row 21, five voters, seed 608). A write at the failure
/// looped on a fenced queue, which refuses every batch (#197 review).
#[test]
fn a_vote_carries_the_rows_of_a_dependency_whose_batch_failed() {
    let mut g = booted(1);
    let (e1, c1) = admitted(1, 1, 1);
    let first = effects_barrier(&g.step(e1));
    assert!(
        g.step(Event::Storage(StorageEvent::Failed {
            barrier_id: first,
            error: StorageError::NoSpace,
        }))
        .is_empty(),
        "nothing is written at the failure"
    );
    let (e2, c2) = admitted(2, 2, 2);
    let second = persisted(&g.step(e2));
    assert_eq!(second.len(), 1);
    assert_eq!(g.table().record(&c2).unwrap().deps, vec![c1]);
    let rows: Vec<(Collection, Vec<u8>)> = second[0]
        .updates
        .iter()
        .map(|u| (Collection::from_id(u.collection).unwrap(), u.key.clone()))
        .collect();
    assert!(
        rows.contains(&(Collection::ProtocolV1, dependency_key(epoch(), &c1))),
        "the vote carries the dependency's row: {rows:?}"
    );
    assert_eq!(
        rows.iter()
            .filter(|(c, _)| *c == Collection::PayloadV1)
            .count(),
        2,
        "and its payload row, beside the vote's own"
    );
    let released = g.step(Event::Storage(StorageEvent::JournalDurable {
        barrier_id: second[0].barrier,
        journal_seq: LocalJournalSeq::new(1).unwrap(),
    }));
    let sent = sends(&released);
    assert_eq!(sent.len(), 2, "the leader and the frontend: {sent:?}");
    assert!(
        sent.iter().all(|(_, m)| matches!(m, ProtocolMessage::FastAck(a) if a.command == c2 && a.deps == vec![c1])),
        "{sent:?}"
    );
    assert_eq!(
        g.ledger().record(&c1).map(|r| r.phase),
        Some(Phase::PreAccept)
    );
    // A third vote names `c2`, now written: nothing more is carried.
    let (e3, _) = admitted(3, 3, 3);
    let third = persisted(&g.step(e3));
    assert_eq!(third[0].updates.len(), 2, "its own rows only");
    // Once fenced, the queue refuses every batch, and a vote is refused
    // once: nothing writes again at a failure.
    let mut h = booted(1);
    for seq in 1..=4 {
        let (e, _) = admitted(seq, seq as u8, seq as u8);
        let batches = persisted(&h.step(e));
        assert_eq!(batches.len(), 1);
        assert!(
            h.step(Event::Storage(StorageEvent::Failed {
                barrier_id: batches[0].barrier,
                error: StorageError::NoSpace,
            }))
            .is_empty()
        );
    }
}

/// The record the newest batch in flight writes for a command (task-d90):
/// what a report gives for a command executed and retired while that row
/// is not yet durable, in place of the older durable row (protocol_sim row
/// 22 with early proposals, five voters, seed 120, without catch-up).
#[test]
fn the_ledger_names_the_newest_record_in_flight() {
    use coord_consensus::DurableLedger;
    use coord_core::effect::BarrierId;
    let barrier = |sequence| BarrierId {
        node_generation: ReplicaIncarnation::new(1).unwrap(),
        boot_id: BootId([1; 16]),
        sequence,
    };
    let (_, c) = admitted(1, 1, 1);
    let (_, d) = admitted(2, 2, 2);
    let record = |phase, deps: Vec<CommandId>| CommandRecord {
        phase,
        deps,
        keys: vec![CONSERVATIVE_KEY.to_vec()],
        payload: Some(Digest32([7; 32])),
        paths: Vec::new(),
        synced_seq: None,
        path: Digest32([0; 32]),
    };
    let mut ledger = DurableLedger::new();
    ledger.stage(barrier(1), c, record(Phase::PreAccept, Vec::new()));
    ledger.durable(barrier(1), LocalJournalSeq::new(1).unwrap());
    assert!(ledger.in_flight_records().is_empty());
    ledger.stage(barrier(2), c, record(Phase::Accept, vec![d]));
    ledger.stage(barrier(3), c, record(Phase::Commit, vec![d]));
    let in_flight = ledger.in_flight_records();
    assert_eq!(in_flight.len(), 1);
    assert_eq!(in_flight[&c].phase, Phase::Commit, "the newest batch's");
    assert_eq!(in_flight[&c].deps, vec![d]);
    assert_eq!(ledger.record(&c).map(|r| r.phase), Some(Phase::PreAccept));
    ledger.durable(barrier(2), LocalJournalSeq::new(2).unwrap());
    ledger.durable(barrier(3), LocalJournalSeq::new(3).unwrap());
    assert!(ledger.in_flight_records().is_empty());
    assert_eq!(ledger.record(&c).map(|r| r.phase), Some(Phase::Commit));
}

/// A commit over an adoption whose batch failed (#197 review): the record
/// is committed in memory under the leader's order while the row here is
/// still the pre-acceptance, under other dependencies. Executed over that
/// row, a report, and a restart, gave the command committed under
/// dependencies it was never decided with (protocol_sim row 21 with its
/// queue failed, three voters, seed 39). It executes only once its rows
/// are written again, in a batch of the next event that is not a failure,
/// and a failure of that batch holds it back again without a write.
#[test]
fn a_commit_whose_adoption_failed_executes_only_over_its_rows_written_again() {
    let (e1, c1) = admitted(1, 1, 1);
    let (e2, c2) = admitted(2, 2, 2);
    let mut leader_view = coord_consensus::CommandTable::new();
    let l1 = leader_view
        .initialize(c1, c1.0, vec![CONSERVATIVE_KEY.to_vec()])
        .unwrap();
    let mut f = booted(1);
    for e in [e2, e1] {
        let effects = f.step(e);
        f.step(durable_of(&effects, 1).remove(0));
    }
    assert_eq!(f.ledger().record(&c1).unwrap().deps, vec![c2]);
    let adoption = persisted(&f.step(peer(0, proposal(c1, vec![], 0, l1.paths.clone(), l1.path))));
    assert_eq!(adoption.len(), 1);
    assert!(
        persisted(&f.step(Event::Storage(StorageEvent::Failed {
            barrier_id: adoption[0].barrier,
            error: StorageError::DefinitelyNotCommitted,
        })))
        .is_empty(),
        "nothing is written at the failure"
    );
    let ack = |from: u8, f: &Follower| {
        peer(
            from,
            ProtocolMessage::SlowAck(SlowAck {
                replica: r(from),
                ballot: ballot(0, 0),
                command: c1,
                admission: f.table().record(&c1).and_then(|r| r.payload).unwrap(),
            }),
        )
    };
    let e = ack(0, &f);
    f.step(e);
    let e = ack(2, &f);
    let carried = persisted(&f.step(e));
    assert_eq!(f.table().phase_of(&c1), Some(Phase::Commit));
    assert_eq!(f.table().record(&c1).unwrap().deps, vec![]);
    let rows: Vec<CommandRecord> = carried
        .iter()
        .flat_map(|b| b.updates.iter())
        .filter(|u| u.collection == Collection::ProtocolV1.id())
        .filter(|u| u.key == dependency_key(epoch(), &c1))
        .map(|u| decode_dependency(u.value.as_deref().unwrap()).unwrap())
        .collect();
    assert_eq!(
        rows.len(),
        1,
        "the commit's rows go out with the event that committed it"
    );
    assert_eq!(rows[0].deps, vec![]);
    assert_eq!(f.next_executable(), Some(c1), "over its row in flight");
    // That batch fails too: held back, and nothing is written at the
    // failure.
    let failed = f.step(Event::Storage(StorageEvent::Failed {
        barrier_id: carried[0].barrier,
        error: StorageError::DefinitelyNotCommitted,
    }));
    assert!(persisted(&failed).is_empty());
    assert_eq!(
        f.next_executable(),
        None,
        "not over the pre-acceptance's row"
    );
    // The next event that is not a failure writes it again.
    let e = ack(0, &f);
    let again = persisted(&f.step(e));
    assert_eq!(again.len(), 1);
    assert_eq!(f.next_executable(), Some(c1));
    f.step(Event::Storage(StorageEvent::JournalDurable {
        barrier_id: again[0].barrier,
        journal_seq: LocalJournalSeq::new(2).unwrap(),
    }));
    assert_eq!(f.ledger().record(&c1).unwrap().deps, vec![]);
    assert_eq!(f.next_executable(), Some(c1));
}

/// A decided command whose payload's batch failed (#243 review, rule (b)):
/// its dependency row is durable and its payload is in memory, with no
/// payload row durable or in flight. Executed then, the store's lowering
/// refuses the execution (rule (b): no pending payload to move), and the
/// voter is lost until its state is cleared. So it executes only over a
/// payload row, written again by the next event that is not a failure.
#[test]
fn a_decided_command_whose_payload_batch_failed_executes_only_over_its_row_written_again() {
    // Durable: c1's accepted dependency row, and no payload row.
    let (e1, c1) = admitted(1, 1, 1);
    let mut f = booted(1);
    let mut storage = StorageModel::default();
    let mut seq = 0;
    let mut durable = |f: &mut Follower, effects: &[Effect], storage: &mut StorageModel| {
        for b in persisted(effects) {
            storage.submit(b.clone());
            storage.complete(b.barrier).unwrap();
            seq += 1;
            f.step(Event::Storage(StorageEvent::JournalDurable {
                barrier_id: b.barrier,
                journal_seq: LocalJournalSeq::new(seq).unwrap(),
            }));
        }
    };
    let effects = f.step(e1);
    durable(&mut f, &effects, &mut storage);
    let record = f.table().record(&c1).unwrap().clone();
    let effects = f.step(peer(
        0,
        proposal(c1, vec![], 0, record.paths.clone(), record.path),
    ));
    durable(&mut f, &effects, &mut storage);
    storage.crash();
    let (rows, mut payloads) = (restore_rows(&storage), restore_payloads(&storage));
    let payload = payloads.remove(0).1;
    let mut g = Follower::recover(config(1), None, rows, Vec::new(), ExecutionPosition::ZERO);
    g.step(boot_event());
    assert_eq!(g.table().phase_of(&c1), Some(Phase::Accept));
    assert!(g.payload(&c1).is_none());
    // The leader proposes it again, the payload arrives, and the adoption
    // turns durable while the payload's row is still in flight.
    let record = g.table().record(&c1).unwrap().clone();
    g.step(peer(
        0,
        proposal(c1, vec![], 0, record.paths.clone(), record.path),
    ));
    let batches = persisted(&g.step(peer(
        0,
        ProtocolMessage::PayloadResponse {
            command: c1,
            payload,
        },
    )));
    assert_eq!(batches.len(), 2, "the payload row's batch and the adoption");
    let (written, adoption) = (&batches[0], &batches[1]);
    assert!(
        written
            .updates
            .iter()
            .all(|u| u.collection == Collection::PayloadV1.id())
    );
    g.step(Event::Storage(StorageEvent::JournalDurable {
        barrier_id: adoption.barrier,
        journal_seq: LocalJournalSeq::new(1).unwrap(),
    }));
    // The leader's frontier commits it.
    assert!(
        persisted(&g.step(peer(
            0,
            ProtocolMessage::Committed {
                ballot: ballot(0, 0),
                through: 0,
            },
        )))
        .is_empty()
    );
    assert_eq!(g.table().phase_of(&c1), Some(Phase::Commit));
    assert_eq!(
        g.next_executable(),
        Some(c1),
        "over its payload row in flight"
    );
    // That batch fails: nothing is written at the failure, and the
    // payload stays in memory with no row of it durable or in flight.
    let failed = g.step(Event::Storage(StorageEvent::Failed {
        barrier_id: written.barrier,
        error: StorageError::NoSpace,
    }));
    assert!(
        persisted(&failed).is_empty(),
        "nothing is written at the failure"
    );
    assert!(g.payload(&c1).is_some(), "the payload stays in memory");
    assert_eq!(
        g.next_executable(),
        None,
        "not with its payload in no row, durable or in flight"
    );
    // The next event that is not a failure writes the payload row again.
    let again = persisted(&g.step(peer(
        0,
        ProtocolMessage::Committed {
            ballot: ballot(0, 0),
            through: 0,
        },
    )));
    assert_eq!(again.len(), 1, "the payload row written again: {again:?}");
    assert!(
        again[0]
            .updates
            .iter()
            .any(|u| u.collection == Collection::PayloadV1.id() && u.value.is_some())
    );
    assert_eq!(g.next_executable(), Some(c1));
}

/// The payload record a relayed proposal of request `seq` carries
/// (task-d102): what the collector submitted, under the admission
/// [`admitted_under`] names.
fn relayed_payload(seq: u64, key: u8, value: u8) -> (PayloadRecordV1, CommandId) {
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
    let logical = RequestV1::new(retry_key(seq), &request, 0, 0)
        .unwrap()
        .logical
        .as_slice()
        .to_vec();
    let payload = PayloadRecordV1 {
        retry_key: retry_key(seq),
        logical,
        admission: Some(coord_core::capability::AdmissionFacts {
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
        }),
        ack_through: 0,
    };
    assert_eq!(payload.admission_digest(), admitted_under());
    (payload, command)
}

fn relayed(proposal: ProtocolMessage, payload: PayloadRecordV1) -> ProtocolMessage {
    let ProtocolMessage::Proposal(vote) = proposal else {
        panic!("not a proposal");
    };
    ProtocolMessage::RelayedProposal { vote, payload }
}

/// task-d102: a follower that never got the submission takes the payload
/// from the leader's relayed proposal and adopts the command at once: one
/// batch holds the payload row and the ACCEPT row, nothing is
/// pre-accepted or fast-acknowledged, and the one acknowledgement, a slow
/// one, goes to the leader alone once that batch is durable.
#[test]
fn a_relayed_proposal_is_adopted_in_one_batch_without_pre_accepting() {
    let mut f = booted(1);
    let (payload, c1) = relayed_payload(1, 1, 1);
    let mut leader_view = coord_consensus::CommandTable::new();
    let l1 = leader_view
        .initialize(c1, c1.0, vec![CONSERVATIVE_KEY.to_vec()])
        .unwrap();
    let p1 = relayed(proposal(c1, vec![], 0, l1.paths, l1.path), payload);
    let effects = f.step(peer(0, p1));
    assert_eq!(f.relayed_taken(), 1);
    assert!(f.answers_on_release(&c1));
    assert!(f.held().is_empty(), "adopted at once");
    assert_eq!(f.table().phase_of(&c1), Some(Phase::Accept));
    let batches: Vec<_> = effects
        .iter()
        .filter_map(|e| match e {
            Effect::Persist(b) => Some(b),
            _ => None,
        })
        .collect();
    assert_eq!(batches.len(), 1, "one batch: {effects:?}");
    let payloads = Collection::PayloadV1.id();
    assert!(
        batches[0]
            .updates
            .iter()
            .any(|u| u.collection == payloads && u.value.is_some()),
        "the payload row rides the adoption batch"
    );
    let row = decode_dependency(batches[0].updates[0].value.as_ref().unwrap()).unwrap();
    assert_eq!(row.phase, Phase::Accept);
    assert!(
        sends(&effects).is_empty(),
        "nothing leaves before the batch"
    );
    let mut released = Vec::new();
    for event in durable_of(&effects, 1) {
        released.extend(f.step(event));
    }
    let out = sends(&released);
    assert_eq!(out.len(), 1, "{out:?}");
    assert_eq!(out[0].0, r(0), "to the leader alone");
    assert!(matches!(out[0].1, ProtocolMessage::SlowAck(_)));
    assert!(f.take_rejections().is_empty());
}

/// task-d102: a payload that does not match its relayed proposal -- under
/// other admission facts -- is not taken. The proposal is held as one
/// without its payload, and nothing is written or acknowledged.
#[test]
fn a_relayed_payload_that_does_not_match_its_proposal_is_not_taken() {
    let mut f = booted(1);
    let (mut payload, c1) = relayed_payload(1, 1, 1);
    payload.ack_through = 7;
    let mut leader_view = coord_consensus::CommandTable::new();
    let l1 = leader_view
        .initialize(c1, c1.0, vec![CONSERVATIVE_KEY.to_vec()])
        .unwrap();
    let effects = f.step(peer(
        0,
        relayed(proposal(c1, vec![], 0, l1.paths, l1.path), payload),
    ));
    assert_eq!(f.relayed_taken(), 0);
    assert_eq!(
        f.relay_taken(),
        RelayTaken {
            not_taken: 1,
            ..RelayTaken::default()
        },
        "counted as not taken (#224 review)"
    );
    assert!(
        !effects.iter().any(|e| matches!(e, Effect::Persist(_))),
        "nothing written"
    );
    assert_eq!(f.held().len(), 1, "held for its payload");
    assert!(f.table().record(&c1).is_none_or(|r| r.payload.is_none()));
    assert!(
        f.take_rejections()
            .contains(&FollowerRejection::PayloadIdentityMismatch(c1))
    );
}

/// Cost pass row 16: a relayed payload's identity is the digest of its
/// logical bytes as they came. Bytes that decode to the command's request
/// but aren't the encoding its identity was derived from (here, one byte
/// more) are refused. The check this replaces decoded them, encoded the
/// request again and took them.
#[test]
fn a_relayed_payload_is_taken_only_as_the_bytes_its_command_names() {
    let mut f = booted(1);
    let (mut payload, c1) = relayed_payload(1, 1, 1);
    payload.logical.push(0);
    assert_eq!(
        postcard::from_bytes::<LogicalRequest>(&payload.logical)
            .ok()
            .and_then(|r| CommandId::derive(&payload.retry_key, &r).ok()),
        Some(c1),
        "a decode and an encoding made again find the command in these bytes"
    );
    let mut leader_view = coord_consensus::CommandTable::new();
    let l1 = leader_view
        .initialize(c1, c1.0, vec![CONSERVATIVE_KEY.to_vec()])
        .unwrap();
    let effects = f.step(peer(
        0,
        relayed(proposal(c1, vec![], 0, l1.paths.clone(), l1.path), payload),
    ));
    assert_eq!(f.relayed_taken(), 0);
    assert!(
        !effects.iter().any(|e| matches!(e, Effect::Persist(_))),
        "nothing written"
    );
    assert_eq!(f.held().len(), 1, "held for its payload");
    assert!(
        f.take_rejections()
            .contains(&FollowerRejection::PayloadIdentityMismatch(c1))
    );

    // The bytes the command was derived from are taken.
    let mut f = booted(1);
    let (payload, c1) = relayed_payload(1, 1, 1);
    f.step(peer(
        0,
        relayed(proposal(c1, vec![], 0, l1.paths, l1.path), payload),
    ));
    assert_eq!(f.relayed_taken(), 1);
}

/// Review on #277: a follower's own intake takes a request's logical
/// bytes only as the canonical encoding of the request they decode to, as
/// the leader's `propose` does. Bytes that decode to the request with
/// nothing left over but aren't its encoding (here, the schema version's
/// varint written in two bytes) are refused, with nothing written, bound
/// or sent. Taken, they would be held and, if this replica led, relayed
/// as bytes that the identity check above refuses.
#[test]
fn an_admitted_request_is_taken_only_as_its_canonical_encoding() {
    let (event, c1) = admitted(1, 1, 1);
    let request = LogicalRequest::new(
        NamespaceId([5; 16]),
        CanonicalOperation::Put(PutOp {
            key: vec![1],
            value: vec![1],
            lease: None,
            prev_kv: false,
        }),
    );
    let canonical = request.canonical_bytes().unwrap();
    assert!(canonical[0] < 0x80, "the schema version is one byte");
    let mut long = vec![canonical[0] | 0x80, 0];
    long.extend_from_slice(&canonical[1..]);
    // What the intake checked before: a decode with nothing left over,
    // and the command derived from the request decoded.
    let (decoded, rest) = postcard::take_from_bytes::<LogicalRequest>(&long).unwrap();
    assert!(rest.is_empty());
    assert_eq!(CommandId::derive(&retry_key(1), &decoded).unwrap(), c1);
    let mut wire = RequestV1::new(retry_key(1), &request, 0, 0).unwrap();
    wire.logical = BoundedBytes::new(long).unwrap();
    assert!(wire.logical().is_err(), "the bytes are not canonical");
    let Event::Admitted(mut long_form) = event.clone() else {
        unreachable!("an admitted request")
    };
    long_form.frame = MessageV1::Request(wire).encode().unwrap();

    let mut f = booted(1);
    let effects = f.step(Event::Admitted(long_form));
    assert!(
        !effects.iter().any(|e| matches!(e, Effect::Persist(_))),
        "nothing written"
    );
    assert_eq!(
        f.take_rejections(),
        vec![FollowerRejection::MalformedRequest]
    );
    assert_eq!(f.admitted(), None);

    // The canonical bytes are taken, and written.
    let effects = f.step(event);
    assert!(effects.iter().any(|e| matches!(e, Effect::Persist(_))));
    assert_eq!(f.admitted(), Some(c1));
}

/// task-d102: a follower that already took the submission keeps what it
/// accepted; the payload a relayed proposal carries is left aside and the
/// proposal is adopted as any.
#[test]
fn a_relayed_proposal_of_a_command_held_already_adopts_as_any() {
    let mut f = booted(1);
    let (e1, c1) = admitted(1, 1, 1);
    let votes = f.step(e1);
    for event in durable_of(&votes, 1) {
        f.step(event);
    }
    let (payload, same) = relayed_payload(1, 1, 1);
    assert_eq!(same, c1);
    let mut leader_view = coord_consensus::CommandTable::new();
    let l1 = leader_view
        .initialize(c1, c1.0, vec![CONSERVATIVE_KEY.to_vec()])
        .unwrap();
    let adoption = f.step(peer(
        0,
        relayed(proposal(c1, vec![], 0, l1.paths, l1.path), payload),
    ));
    assert_eq!(f.relayed_taken(), 0);
    assert_eq!(
        f.relay_taken(),
        RelayTaken {
            known: 1,
            ..RelayTaken::default()
        },
        "counted as known (#224 review)"
    );
    assert!(!f.answers_on_release(&c1), "not marked by the relay");
    // Its own durable fast vote and the leader's proposal are a fast
    // quorum here: decided, as a plain proposal would leave it.
    assert!(f.table().phase_of(&c1) >= Some(Phase::Accept));
    assert!(f.held().is_empty());
    assert!(adoption.len() <= 1, "at most the adoption batch");
    assert!(f.take_rejections().is_empty());
}

fn framed(messages: Vec<ProtocolMessage>) -> ProtocolMessage {
    let proposals = messages
        .into_iter()
        .map(|m| match m {
            ProtocolMessage::Proposal(vote) => FramedProposal::of(vote, None).unwrap(),
            ProtocolMessage::RelayedProposal { vote, payload } => {
                FramedProposal::of(vote, Some(payload)).unwrap()
            }
            other => panic!("not a proposal: {other:?}"),
        })
        .collect();
    ProtocolMessage::ProposalFrame {
        replica: r(0),
        ballot: ballot(0, 0),
        proposals,
    }
}

/// task-d102: a frame's proposals are taken in one step, each as the
/// relayed proposal it stands for: both commands are adopted at once,
/// their batches come out of the one step, and each gets one slow
/// acknowledgement, to the leader alone.
#[test]
fn a_frames_proposals_are_adopted_in_one_step() {
    let mut f = booted(1);
    let (p1, c1) = relayed_payload(1, 1, 1);
    let (p2, c2) = relayed_payload(2, 2, 2);
    let mut leader_view = coord_consensus::CommandTable::new();
    let l1 = leader_view
        .initialize(c1, c1.0, vec![CONSERVATIVE_KEY.to_vec()])
        .unwrap();
    let l2 = leader_view
        .initialize(c2, c2.0, vec![CONSERVATIVE_KEY.to_vec()])
        .unwrap();
    let frame = framed(vec![
        relayed(proposal(c1, vec![], 0, l1.paths, l1.path), p1),
        relayed(proposal(c2, vec![c1], 1, l2.paths, l2.path), p2),
    ]);
    let effects = f.step(peer(0, frame));
    assert_eq!(f.frames_taken(), 1);
    assert_eq!(f.relayed_taken(), 2);
    assert_eq!(
        f.relay_taken(),
        RelayTaken {
            frames: 1,
            taken: 2,
            ..RelayTaken::default()
        }
    );
    assert!(f.held().is_empty(), "adopted at once");
    assert_eq!(f.table().phase_of(&c1), Some(Phase::Accept));
    assert_eq!(f.table().phase_of(&c2), Some(Phase::Accept));
    assert!(
        sends(&effects).is_empty(),
        "nothing leaves before the batches"
    );
    let mut released = Vec::new();
    for event in durable_of(&effects, 1) {
        released.extend(f.step(event));
    }
    let out = sends(&released);
    assert_eq!(out.len(), 2, "{out:?}");
    for (to, message) in &out {
        assert_eq!(*to, r(0), "to the leader alone");
        assert!(matches!(message, ProtocolMessage::SlowAck(_)));
    }
    assert!(f.take_rejections().is_empty());
}

/// task-d102: an entry a follower refuses leaves the frame's others as
/// they are: a mismatched payload is not taken, and the next entry is
/// adopted all the same.
#[test]
fn a_refused_entry_leaves_the_frames_others_as_they_are() {
    let mut f = booted(1);
    let (mut bad, c1) = relayed_payload(1, 1, 1);
    bad.ack_through = 7;
    let (p2, c2) = relayed_payload(2, 2, 2);
    let mut leader_view = coord_consensus::CommandTable::new();
    let l1 = leader_view
        .initialize(c1, c1.0, vec![CONSERVATIVE_KEY.to_vec()])
        .unwrap();
    let l2 = leader_view
        .initialize(c2, c2.0, vec![CONSERVATIVE_KEY.to_vec()])
        .unwrap();
    let frame = framed(vec![
        relayed(proposal(c1, vec![], 0, l1.paths, l1.path), bad),
        relayed(proposal(c2, vec![], 1, l2.paths, l2.path), p2),
    ]);
    f.step(peer(0, frame));
    assert_eq!(f.relayed_taken(), 1, "only the matching payload");
    assert_eq!(
        f.relay_taken(),
        RelayTaken {
            frames: 1,
            taken: 1,
            known: 0,
            not_taken: 1,
        }
    );
    assert_ne!(f.table().phase_of(&c1), Some(Phase::Accept));
    assert_eq!(f.table().phase_of(&c2), Some(Phase::Accept));
}

/// task-d102: a proposal held for want of its payload -- a plain copy
/// arrived first -- is adopted when a relayed copy brings the payload,
/// though that copy is a duplicate of one held. Found by the frames sweep
/// (row 1 at 3 voters, seed 11): left, the hold waited on itself for ever.
#[test]
fn a_held_proposal_is_adopted_when_a_relayed_copy_brings_its_payload() {
    let mut f = booted(1);
    let (payload, c1) = relayed_payload(1, 1, 1);
    let mut leader_view = coord_consensus::CommandTable::new();
    let l1 = leader_view
        .initialize(c1, c1.0, vec![CONSERVATIVE_KEY.to_vec()])
        .unwrap();
    let plain = proposal(c1, vec![], 0, l1.paths.clone(), l1.path);
    f.step(peer(0, plain.clone()));
    assert!(f.held().contains_key(&c1), "held without its payload");
    let effects = f.step(peer(0, relayed(plain, payload)));
    assert_eq!(f.relayed_taken(), 1);
    assert!(f.held().is_empty(), "adopted once the payload came");
    assert_eq!(f.table().phase_of(&c1), Some(Phase::Accept));
    assert!(
        effects.iter().any(|e| matches!(e, Effect::Persist(_))),
        "the adoption batch: {effects:?}"
    );
}

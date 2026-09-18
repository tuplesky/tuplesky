//! task-22 acceptance: golden traces of the leader machine match the
//! command-table model; every leader reply has exact durable support;
//! reordered/duplicate client requests cannot bind conflicting payload;
//! premature accept/commit/execution is blocked while dependencies lag;
//! a higher promise stops proposing; votes are collected, never learned.

use std::collections::BTreeSet;
use std::path::PathBuf;

use coord_consensus::{
    BallotConfiguration, CONSERVATIVE_KEY, CommandTable, ConfigurationIdentity, FastAck,
    GuardViolation, Leader, LeaderConfig, Phase, ProtocolMessage, Rejection, ReplicaRole, SlowAck,
    VoteError, decode_dependency, decode_payload, decode_proposal, dependency_key, payload_key,
    proposal_key,
};
use coord_core::capability::{AdmissionReceipt, VerifierToken};
use coord_core::effect::{BootId, Effect, PeerId};
use coord_core::event::{
    AdmittedRequest, AuthenticatedPeerMessage, Event, PeerProvenance, StorageError, StorageEvent,
};
use coord_core::machine::DeterministicMachine;
use coord_store_api::registry::Collection;
use coord_types::identity::Digest32;
use coord_types::ids::*;
use coord_types::logical_v1::{CanonicalOperation, LogicalRequest, PutOp};
use coord_types::wire_v1::{MessageV1, RequestV1};
use coord_types::{CommandId, RetryKey};
use serde::Serialize;

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

fn config() -> LeaderConfig {
    let voters: BTreeSet<ReplicaId> = (0..3).map(r).collect();
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
        quorum: BallotConfiguration::c2(epoch(), ballot(0, 0), voters, [r(0), r(1)].into())
            .unwrap(),
        genesis: ballot(0, 0),
        frontend: FRONTEND,
        capacity: 8,
    }
}

fn booted() -> Leader {
    let mut leader = Leader::new(config(), None);
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

fn logical(key: u8, value: u8) -> LogicalRequest {
    LogicalRequest::new(
        NamespaceId([5; 16]),
        CanonicalOperation::Put(PutOp {
            key: vec![key],
            value: vec![value],
            lease: None,
            prev_kv: false,
        }),
    )
}

fn admitted(seq: u64, key: u8, value: u8) -> (Event, CommandId) {
    let request = logical(key, value);
    let command = CommandId::derive(&retry_key(seq), &request).unwrap();
    let frame = MessageV1::Request(RequestV1::new(retry_key(seq), &request, 0).unwrap())
        .encode()
        .unwrap();
    let receipt = AdmissionReceipt::from_verifier(
        VerifierToken::for_boundary(),
        SessionId([3; 16]),
        1,
        u32::MAX,
        Digest32([9; 32]),
        0,
    );
    (Event::Admitted(AdmittedRequest { receipt, frame }), command)
}

fn peer(from: u8, message: ProtocolMessage) -> Event {
    Event::Peer(AuthenticatedPeerMessage::new(
        PeerProvenance::from_transport(r(from), ReplicaIncarnation::new(1).unwrap(), 1),
        message.encode(),
    ))
}

fn durable(effects: &[Effect], seq: u64) -> Vec<Event> {
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

/// A readable, order-stable summary of an effect for golden traces.
fn summarize(effect: &Effect) -> String {
    match effect {
        Effect::Persist(b) => {
            let rows: Vec<String> = b
                .updates
                .iter()
                .map(|u| {
                    let c = coord_store_api::registry::Collection::from_id(u.collection)
                        .map_or("?", |c| c.name());
                    format!("{c}:{}", hex(&u.key))
                })
                .collect();
            format!(
                "persist b{} base={:?} [{}]",
                b.barrier.sequence,
                b.base,
                rows.join(", ")
            )
        }
        Effect::SendWhenDurable {
            context,
            requires,
            to,
            frame,
        } => {
            let requires: Vec<String> = requires
                .iter()
                .map(|b| format!("b{}", b.sequence))
                .collect();
            let message = ProtocolMessage::decode(frame).unwrap();
            let kind = match &message {
                ProtocolMessage::Proposal(p) => {
                    format!("proposal seq={} deps={}", p.seqnum.unwrap(), p.deps.len())
                }
                ProtocolMessage::LeaderReply { seqnum, deps, .. } => {
                    format!("leader-reply seq={seqnum} deps={}", deps.len())
                }
                ProtocolMessage::Promise { ballot, .. } => format!("promise {}", ballot.number),
                other => format!("{other:?}"),
            };
            format!(
                "send {kind} to {} requires [{}] ballot={}",
                hex(&to.replica.0[..2]),
                requires.join(","),
                context.ballot.number
            )
        }
        other => format!("{other:?}"),
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

#[derive(Serialize)]
struct Trace {
    steps: Vec<(String, Vec<String>)>,
}

fn golden(name: &str, trace: &Trace) {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures/golden")
        .join(name);
    let mut json = serde_json::to_string_pretty(trace).unwrap();
    json.push('\n');
    if std::env::var_os("COORD_CONSENSUS_WRITE_FIXTURES").is_some() {
        std::fs::write(&path, &json).unwrap();
    }
    let frozen = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("missing golden trace {}: {e}", path.display()));
    assert_eq!(frozen, json, "golden trace {} differs", path.display());
}

#[test]
fn proposals_are_published_with_exact_durable_support_and_match_the_model() {
    let mut leader = booted();
    let mut trace = Trace { steps: Vec::new() };
    let (e1, c1) = admitted(1, 1, 1);
    let (e2, c2) = admitted(2, 2, 2);
    let mut model = CommandTable::new();
    let m1 = model
        .initialize(c1, c1.0, vec![CONSERVATIVE_KEY.to_vec()])
        .unwrap();
    let m2 = model
        .initialize(c2, c2.0, vec![CONSERVATIVE_KEY.to_vec()])
        .unwrap();

    // Admit c1: one batch with payload, dependency and proposal rows; no
    // send yet.
    let effects = leader.step(e1);
    trace
        .steps
        .push(("admit c1".into(), effects.iter().map(summarize).collect()));
    assert_eq!(effects.len(), 1);
    let Effect::Persist(batch) = &effects[0] else {
        panic!()
    };
    assert_eq!(batch.base, None);
    let keys: Vec<(Collection, Vec<u8>)> = batch
        .updates
        .iter()
        .map(|u| (Collection::from_id(u.collection).unwrap(), u.key.clone()))
        .collect();
    assert_eq!(
        keys,
        vec![
            (Collection::PayloadV1, payload_key(&c1)),
            (Collection::ProtocolV1, dependency_key(epoch(), &c1)),
            (Collection::ProtocolV1, proposal_key(epoch(), &c1)),
        ]
    );
    let payload = decode_payload(batch.updates[0].value.as_ref().unwrap()).unwrap();
    assert_eq!(payload.retry_key, retry_key(1));
    assert_eq!(
        CommandId::derive(
            &payload.retry_key,
            &postcard::from_bytes(&payload.logical).unwrap()
        ),
        Ok(c1),
        "payload rehashes to its identity"
    );
    let dependency = decode_dependency(batch.updates[1].value.as_ref().unwrap()).unwrap();
    assert_eq!(dependency.deps, m1.deps);
    assert_eq!(dependency.path, m1.path, "path evidence matches the model");
    let proposal = decode_proposal(batch.updates[2].value.as_ref().unwrap()).unwrap();
    assert_eq!(proposal.seqnum, 0);
    assert_eq!(proposal.ballot, ballot(0, 0));
    assert_eq!(
        leader.pending_sends(),
        3,
        "two voters and the frontend wait"
    );
    assert_eq!(leader.table().phase_of(&c1), Some(Phase::PreAccept));

    // Admit c2 before c1 is durable: it depends on c1 (conservative
    // conflicts) and its sends wait on its own batch.
    let effects2 = leader.step(e2);
    trace
        .steps
        .push(("admit c2".into(), effects2.iter().map(summarize).collect()));
    let Effect::Persist(batch2) = &effects2[0] else {
        panic!()
    };
    let proposal2 = decode_proposal(batch2.updates[2].value.as_ref().unwrap()).unwrap();
    assert_eq!(proposal2.seqnum, 1);
    assert_eq!(proposal2.deps, vec![c1]);
    assert_eq!(proposal2.path, m2.path);
    assert_eq!(leader.pending_sends(), 6);

    // c2's batch becomes durable first: its sends are released (exact
    // support is its own batch), but c2 cannot enter ACCEPT while c1 lags.
    let released = leader.step(durable(&effects2, 1).remove(0));
    trace.steps.push((
        "c2 durable".into(),
        released.iter().map(summarize).collect(),
    ));
    assert_eq!(released.len(), 3);
    for e in &released {
        let Effect::SendWhenDurable { requires, to, .. } = e else {
            panic!()
        };
        assert_eq!(requires, &vec![batch2.barrier]);
        assert!(to.replica == r(1) || to.replica == r(2) || *to == FRONTEND);
    }
    let reply = released
        .iter()
        .find_map(|e| match e {
            Effect::SendWhenDurable { to, frame, .. } if *to == FRONTEND => Some(frame),
            _ => None,
        })
        .unwrap();
    assert!(matches!(
        ProtocolMessage::decode(reply).unwrap(),
        ProtocolMessage::LeaderReply { command, seqnum: 1, .. } if command == c2
    ));
    assert_eq!(
        leader.table().phase_of(&c2),
        Some(Phase::PreAccept),
        "c1 lags"
    );
    assert_eq!(leader.table().phase_of(&c1), Some(Phase::PreAccept));

    // c1 durable: c1 accepts, then c2 (chain advances in one turn).
    let released = leader.step(durable(&effects, 2).remove(0));
    trace.steps.push((
        "c1 durable".into(),
        released.iter().map(summarize).collect(),
    ));
    assert_eq!(released.len(), 3);
    assert_eq!(leader.table().phase_of(&c1), Some(Phase::Accept));
    assert_eq!(leader.table().phase_of(&c2), Some(Phase::Accept));
    assert_eq!(leader.pending_sends(), 0);
    assert!(leader.take_rejections().is_empty());
    // No learning here: the leader's own vote is collected, nothing more.
    assert!(leader.votes(&c1).unwrap().learned().is_none());
    golden("leader_normal.json", &trace);
}

#[test]
fn reordered_and_duplicate_requests_cannot_bind_conflicting_payload() {
    let mut leader = booted();
    let (e1, c1) = admitted(1, 1, 1);
    let effects = leader.step(e1.clone());
    assert_eq!(effects.len(), 1);
    // Same retry key, different payload: refused, nothing persisted.
    let (conflict, other) = admitted(1, 1, 9);
    assert_ne!(other, c1);
    assert!(leader.step(conflict).is_empty());
    assert_eq!(
        leader.take_rejections(),
        vec![Rejection::RequestIdentityConflict {
            retry_key: retry_key(1),
            bound: c1
        }]
    );
    assert!(leader.proposal(&other).is_none());
    // The same request again (a retry, reordered after the conflict): no
    // second proposal, no new sequence number.
    assert!(leader.step(e1).is_empty());
    assert_eq!(leader.take_rejections(), vec![Rejection::Duplicate(c1)]);
    assert_eq!(leader.proposal(&c1).unwrap().seqnum, 0);
    // A different retry key with the same logical payload is a distinct
    // command with its own proposal.
    let (e2, c2) = admitted(2, 1, 1);
    assert_ne!(c2, c1);
    assert_eq!(leader.step(e2).len(), 1);
    assert_eq!(leader.proposal(&c2).unwrap().seqnum, 1);
    assert_eq!(leader.proposal(&c2).unwrap().deps, vec![c1]);
    // Backpressure at capacity refuses new work; nothing is evicted.
    let mut small = Leader::new(
        LeaderConfig {
            capacity: 1,
            ..config()
        },
        None,
    );
    small.step(Event::Boot {
        boot_id: BootId([1; 16]),
        incarnation: ReplicaIncarnation::new(1).unwrap(),
    });
    assert_eq!(small.step(admitted(1, 1, 1).0).len(), 1);
    assert!(small.step(admitted(2, 2, 2).0).is_empty());
    assert_eq!(small.take_rejections(), vec![Rejection::Backpressure]);
    assert!(small.proposal(&admitted(1, 1, 1).1).is_some());
}

#[test]
fn premature_phases_are_blocked_while_dependencies_lag() {
    let mut leader = booted();
    let (e1, c1) = admitted(1, 1, 1);
    let (e2, c2) = admitted(2, 2, 2);
    let effects1 = leader.step(e1);
    let effects2 = leader.step(e2);
    // c1's batch fails: c1 is not proposed; c2 (durable) still cannot
    // accept because its dependency lags, and the table refuses commit and
    // execution for both.
    let Effect::Persist(b1) = &effects1[0] else {
        panic!()
    };
    let dropped = leader.step(Event::Storage(StorageEvent::Failed {
        barrier_id: b1.barrier,
        error: StorageError::DefinitelyNotCommitted,
    }));
    assert!(dropped.is_empty(), "no send is released by a failed batch");
    assert_eq!(
        leader.take_rejections(),
        vec![Rejection::ProposalFailed(c1)]
    );
    assert!(leader.proposal(&c1).is_none());
    let released = leader.step(durable(&effects2, 1).remove(0));
    assert_eq!(released.len(), 3, "c2's own evidence is published");
    assert_eq!(
        leader.table().phase_of(&c2),
        Some(Phase::PreAccept),
        "c1 lags"
    );
    let mut table = leader.table().clone();
    assert_eq!(
        table.accept(c2, vec![c1]),
        Err(GuardViolation::DependencyNotAccepted { dep: c1 })
    );
    assert_eq!(
        table.commit(c2),
        Err(GuardViolation::DependencyNotCommitted { dep: c1 })
    );
    assert_eq!(
        table.execute(c2),
        Err(GuardViolation::DependencyNotExecuted { dep: c1 })
    );
    // The dropped sends of c1 are gone: its proposal never reaches anyone.
    assert_eq!(leader.pending_sends(), 0);
}

#[test]
fn a_higher_promise_stops_proposing_and_fences_unreleased_proposals() {
    let mut leader = booted();
    let (e1, c1) = admitted(1, 1, 1);
    let effects1 = leader.step(e1);
    assert!(leader.is_leading());
    // r2 asks for ballot 1: the promise is persisted and its reply waits
    // for c1's outstanding batch too.
    let effects = leader.step(peer(
        2,
        ProtocolMessage::NewLeader {
            ballot: ballot(1, 2),
        },
    ));
    assert_eq!(effects.len(), 1);
    let Effect::Persist(promise) = &effects[0] else {
        panic!()
    };
    assert!(!leader.is_leading());
    let (e2, _) = admitted(2, 2, 2);
    assert!(leader.step(e2).is_empty());
    assert_eq!(
        leader.take_rejections(),
        vec![Rejection::NotLeading {
            promised: ballot(0, 0)
        }]
    );
    // The promise becomes durable: c1's unreleased proposal is obsolete and
    // dropped; the promise reply still waits for c1's batch (the cut).
    let released = leader.step(Event::Storage(StorageEvent::JournalDurable {
        barrier_id: promise.barrier,
        journal_seq: LocalJournalSeq::new(1).unwrap(),
    }));
    assert!(released.is_empty());
    assert_eq!(leader.ballots().promised(), ballot(1, 2));
    let released = leader.step(durable(&effects1, 2).remove(0));
    assert_eq!(released.len(), 1, "only the promise reply");
    assert!(matches!(
        &released[0],
        Effect::SendWhenDurable { to, frame, .. }
            if to.replica == r(2)
                && matches!(ProtocolMessage::decode(frame).unwrap(), ProtocolMessage::Promise { .. })
    ));
    assert_eq!(leader.pending_sends(), 0);
    assert_eq!(
        leader.table().phase_of(&c1),
        Some(Phase::Accept),
        "durable state is kept"
    );
}

#[test]
fn votes_are_collected_but_never_learned_here() {
    let mut leader = booted();
    let (e1, c1) = admitted(1, 1, 1);
    let effects = leader.step(e1);
    leader.step(durable(&effects, 1).remove(0));
    let path = leader.proposal(&c1).unwrap().path;
    let ack = |replica: u8| FastAck {
        replica: r(replica),
        ballot: ballot(0, 0),
        command: c1,
        deps: vec![],
        path,
        seqnum: None,
    };
    // r1 (fast set) agrees on the path; r2 adopts; an observer is refused;
    // a vote whose sender does not match its provenance is refused.
    assert!(
        leader
            .step(peer(1, ProtocolMessage::FastAck(ack(1))))
            .is_empty()
    );
    assert!(
        leader
            .step(peer(
                2,
                ProtocolMessage::SlowAck(SlowAck {
                    replica: r(2),
                    ballot: ballot(0, 0),
                    command: c1
                })
            ))
            .is_empty()
    );
    assert!(
        leader
            .step(peer(9, ProtocolMessage::FastAck(ack(9))))
            .is_empty()
    );
    assert!(
        leader
            .step(peer(2, ProtocolMessage::FastAck(ack(1))))
            .is_empty()
    );
    assert_eq!(
        leader.take_rejections(),
        vec![
            Rejection::Vote(VoteError::NotAVoter),
            Rejection::Vote(VoteError::NotAVoter)
        ]
    );
    let votes = leader.votes(&c1).unwrap();
    assert_eq!(votes.voted(), [r(0), r(1), r(2)].into());
    // The predicate would hold, but the leader emits nothing for it: no
    // Established effect, no COMMIT (task-24 decides learning).
    assert!(votes.learned().is_some());
    assert_eq!(leader.table().phase_of(&c1), Some(Phase::Accept));
    // Acknowledgements for an unknown command are refused.
    let (_, unknown) = admitted(7, 7, 7);
    let stray = FastAck {
        command: unknown,
        ..ack(1)
    };
    assert!(
        leader
            .step(peer(1, ProtocolMessage::FastAck(stray)))
            .is_empty()
    );
    assert_eq!(
        leader.take_rejections(),
        vec![Rejection::Vote(VoteError::WrongCommand)]
    );
}

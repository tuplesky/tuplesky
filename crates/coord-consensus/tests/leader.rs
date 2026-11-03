//! task-22 acceptance: golden traces of the leader machine match the
//! command-table model; every leader reply has exact durable support;
//! reordered/duplicate client requests cannot bind conflicting payload;
//! premature commit/execution is blocked while dependencies lag (the
//! acceptance guard's own case is in `leader.rs`'s unit tests);
//! a higher promise stops proposing; votes are collected, never learned.

use std::collections::BTreeSet;
use std::path::PathBuf;

use coord_consensus::{
    BallotConfiguration, CONSERVATIVE_KEY, CommandTable, ConfigurationIdentity, FastAck,
    FenceReason, GuardViolation, Leader, LeaderConfig, MAX_PROPOSAL_ATTEMPTS, Phase,
    ProtocolMessage, Rejection, ReplayRefusal, ReplicaRole, SlowAck, VoteError, decode_dependency,
    decode_payload, decode_proposal, dependency_key, payload_key, proposal_key,
};
use coord_core::capability::{AdmissionReceipt, AttestedAdmission, VerifierToken};
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

fn leader_with_role(role: ReplicaRole) -> Leader {
    let mut config = config();
    config.identity.role = role;
    Leader::new(config, None, ExecutionPosition::ZERO)
}

fn booted() -> Leader {
    let mut leader = Leader::new(config(), None, ExecutionPosition::ZERO);
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
                ProtocolMessage::Proposal(ack) => {
                    format!(
                        "proposal seq={} deps={} anchors={}",
                        ack.seqnum.unwrap(),
                        ack.deps.len(),
                        ack.paths.len()
                    )
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

fn sends_of(effects: &[Effect]) -> Vec<ProtocolMessage> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::SendWhenDurable { frame, .. } => Some(ProtocolMessage::decode(frame).unwrap()),
            _ => None,
        })
        .collect()
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
    // send yet. Nothing precedes c1, so the dependency guard passes when
    // it is proposed and the dependency row is already ACCEPT: the
    // leader's acceptance rides in the proposal batch (task-d89).
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
    assert_eq!(
        dependency.phase,
        Phase::Accept,
        "the guard passed: the row is the leader's acceptance"
    );
    let proposal = decode_proposal(batch.updates[2].value.as_ref().unwrap()).unwrap();
    assert_eq!(proposal.seqnum, 0);
    assert_eq!(proposal.ballot, ballot(0, 0));
    assert_eq!(
        leader.pending_sends(),
        6,
        "the proposal to two voters, the reply, and the leader's adoption \
         to two voters and the frontend wait on the batch"
    );
    assert_eq!(leader.table().phase_of(&c1), Some(Phase::Accept));
    assert!(leader.proposal(&c1).unwrap().accepts);

    // Admit c2 before c1 is durable: it depends on c1 (conservative
    // conflicts), which is already ACCEPT here, so c2's guard passes too
    // and its batch carries its acceptance.
    let effects2 = leader.step(e2);
    trace
        .steps
        .push(("admit c2".into(), effects2.iter().map(summarize).collect()));
    let Effect::Persist(batch2) = &effects2[0] else {
        panic!()
    };
    let dependency2 = decode_dependency(batch2.updates[1].value.as_ref().unwrap()).unwrap();
    assert_eq!(dependency2.phase, Phase::Accept);
    assert_eq!(dependency2.deps, vec![c1]);
    let proposal2 = decode_proposal(batch2.updates[2].value.as_ref().unwrap()).unwrap();
    assert_eq!(proposal2.seqnum, 1);
    assert_eq!(proposal2.deps, vec![c1]);
    assert_eq!(proposal2.path, m2.path);
    assert_eq!(leader.pending_sends(), 12);
    assert_eq!(leader.table().phase_of(&c2), Some(Phase::Accept));

    // c1's batch becomes durable, as the journal makes a domain's batches
    // durable, in submission order: its proposal, its reply and its
    // adoption are released, and nothing more is written.
    let released = leader.step(durable(&effects, 1).remove(0));
    trace.steps.push((
        "c1 durable".into(),
        released.iter().map(summarize).collect(),
    ));
    assert_eq!(released.len(), 6, "{released:?}");
    for e in &released {
        let Effect::SendWhenDurable { requires, to, .. } = e else {
            panic!("only sends: no acceptance row is written later: {e:?}")
        };
        assert_eq!(requires, &vec![batch.barrier]);
        assert!(to.replica == r(1) || to.replica == r(2) || *to == FRONTEND);
    }
    let reply = released
        .iter()
        .find_map(|e| match e {
            Effect::SendWhenDurable { to, frame, .. }
                if *to == FRONTEND
                    && matches!(
                        ProtocolMessage::decode(frame).unwrap(),
                        ProtocolMessage::LeaderReply { .. }
                    ) =>
            {
                Some(frame)
            }
            _ => None,
        })
        .unwrap();
    assert!(matches!(
        ProtocolMessage::decode(reply).unwrap(),
        ProtocolMessage::LeaderReply { command, seqnum: 0, .. } if command == c1
    ));
    assert!(leader.votes(&c1).unwrap().adopted_by(&r(0)));
    // c2's adoption waits on c1's batch as well as its own (task-d19).
    assert_eq!(leader.pending_sends(), 6);
    assert!(!leader.votes(&c2).unwrap().adopted_by(&r(0)));

    // c2's batch: its sends and its adoption go.
    let released = leader.step(durable(&effects2, 2).remove(0));
    trace.steps.push((
        "c2 durable".into(),
        released.iter().map(summarize).collect(),
    ));
    assert_eq!(released.len(), 6, "{released:?}");
    assert!(
        released
            .iter()
            .all(|e| matches!(e, Effect::SendWhenDurable { .. }))
    );
    assert_eq!(leader.pending_sends(), 0);
    assert!(leader.take_rejections().is_empty());
    let slow: Vec<_> = sends_of(&released)
        .into_iter()
        .filter(
            |m| matches!(m, ProtocolMessage::SlowAck(a) if a.replica == r(0) && a.command == c2),
        )
        .collect();
    assert_eq!(slow.len(), 3, "c2 adopted by the leader, to all three");
    assert!(leader.votes(&c2).unwrap().adopted_by(&r(0)));
    // The leader's own vote alone learns nothing.
    assert!(leader.votes(&c1).unwrap().learned_slow().is_none());
    assert_eq!(leader.next_executable(), None);
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
    // second proposal, no new sequence number. The reply is not repeated
    // either, because its batch is not durable yet and the original is
    // still queued (task-c02).
    assert!(leader.step(e1).is_empty());
    assert_eq!(
        leader.take_rejections(),
        vec![
            Rejection::Duplicate(c1),
            Rejection::ReplayRefused {
                command: c1,
                why: ReplayRefusal::NotYetDurable
            }
        ]
    );
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
        ExecutionPosition::ZERO,
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
fn a_rejected_batch_is_presented_again_ahead_of_its_dependents() {
    let mut leader = booted();
    let (e1, c1) = admitted(1, 1, 1);
    let (e2, c2) = admitted(2, 2, 2);
    let effects1 = leader.step(e1);
    let effects2 = leader.step(e2);
    // Both are accepted with their proposals (task-d89), c2 over c1.
    assert_eq!(leader.table().phase_of(&c1), Some(Phase::Accept));
    assert_eq!(leader.table().phase_of(&c2), Some(Phase::Accept));
    // c1's batch is definitely rejected. The journal then drops every
    // batch queued behind it as definitely not committed too, so c2's
    // ACCEPT row is never durable without c1's. Each is presented again,
    // unchanged and in order, so c1 keeps its identity and its
    // dependents.
    let Effect::Persist(b1) = &effects1[0] else {
        panic!()
    };
    let Effect::Persist(b2) = &effects2[0] else {
        panic!()
    };
    let mut again = Vec::new();
    for barrier in [b1.barrier, b2.barrier] {
        let retried = leader.step(Event::Storage(StorageEvent::Failed {
            barrier_id: barrier,
            error: StorageError::DefinitelyNotCommitted,
        }));
        let [Effect::Persist(batch)] = retried.as_slice() else {
            panic!("the rejected batch is presented again: {retried:?}")
        };
        again.push(batch.clone());
    }
    assert_ne!(again[0].barrier, b1.barrier, "a fresh barrier");
    assert_eq!(again[0].updates, b1.updates, "the same rows, unchanged");
    assert_eq!(again[1].updates, b2.updates);
    assert!(again[0].barrier < again[1].barrier, "c1's again first");
    assert_eq!(
        leader.take_rejections(),
        vec![
            Rejection::ProposalRetried(c1),
            Rejection::ProposalRetried(c2)
        ]
    );
    let proposal = leader.proposal(&c1).expect("still proposed");
    assert_eq!(proposal.attempts, 2);
    assert!(!proposal.durable);
    assert!(leader.is_leading(), "a definite rejection does not fence");
    // Accepted is not committed or executed: the table refuses both for
    // c2 while c1 is neither.
    let mut table = leader.table().clone();
    assert_eq!(
        table.commit(c2),
        Err(GuardViolation::DependencyNotCommitted { dep: c1 })
    );
    assert_eq!(
        table.execute(c2),
        Err(GuardViolation::DependencyNotExecuted { dep: c1 })
    );
    // Each one's proposal, reply and adoption wait on its new barrier.
    assert_eq!(leader.pending_sends(), 12);
    let released = leader.step(Event::Storage(StorageEvent::JournalDurable {
        barrier_id: again[0].barrier,
        journal_seq: LocalJournalSeq::new(1).unwrap(),
    }));
    assert_eq!(
        released.len(),
        6,
        "c1's evidence is published: {released:?}"
    );
    assert!(leader.votes(&c1).unwrap().adopted_by(&r(0)));
}

#[test]
fn an_unresolved_proposal_write_stops_the_leader() {
    for error in [
        StorageError::Indeterminate,
        StorageError::Quarantine,
        StorageError::NoSpace,
    ] {
        let mut leader = booted();
        let (e1, c1) = admitted(1, 1, 1);
        let effects = leader.step(e1);
        let Effect::Persist(batch) = &effects[0] else {
            panic!()
        };
        assert!(leader.is_leading());
        // The rows may or may not be durable: the leader stops instead of
        // admitting more work over state a recovery cut could miss.
        let out = leader.step(Event::Storage(StorageEvent::Failed {
            barrier_id: batch.barrier,
            error,
        }));
        assert!(out.is_empty(), "nothing is persisted or released: {out:?}");
        assert_eq!(
            leader.fenced(),
            Some(FenceReason::ProposalUnresolved { command: c1, error })
        );
        assert!(!leader.is_leading());
        assert_eq!(
            leader.take_rejections(),
            vec![Rejection::Fenced(FenceReason::ProposalUnresolved {
                command: c1,
                error
            })]
        );
        // The command stays in the table: its rows may exist. Its
        // acceptance was in the same batch (task-d89), and its adoption
        // waits on it, so nothing claims it was durable.
        assert!(leader.proposal(&c1).is_some());
        assert_eq!(leader.table().phase_of(&c1), Some(Phase::Accept));
        assert!(!leader.votes(&c1).unwrap().adopted_by(&r(0)));
        // A further request is refused, naming the promised ballot.
        let (e2, _) = admitted(2, 2, 2);
        assert!(leader.step(e2).is_empty());
        assert!(matches!(
            leader.take_rejections().as_slice(),
            [Rejection::NotLeading { .. }]
        ));
    }
}

#[test]
fn repeated_rejection_stops_the_leader_instead_of_retrying_forever() {
    let mut leader = booted();
    let (e1, c1) = admitted(1, 1, 1);
    let mut effects = leader.step(e1);
    for attempt in 1..MAX_PROPOSAL_ATTEMPTS {
        let Effect::Persist(batch) = &effects[0] else {
            panic!()
        };
        effects = leader.step(Event::Storage(StorageEvent::Failed {
            barrier_id: batch.barrier,
            error: StorageError::DefinitelyNotCommitted,
        }));
        assert_eq!(leader.proposal(&c1).unwrap().attempts, attempt + 1);
        assert!(leader.is_leading());
        leader.take_rejections();
    }
    let Effect::Persist(batch) = &effects[0] else {
        panic!()
    };
    let out = leader.step(Event::Storage(StorageEvent::Failed {
        barrier_id: batch.barrier,
        error: StorageError::DefinitelyNotCommitted,
    }));
    assert!(out.is_empty());
    assert_eq!(
        leader.fenced(),
        Some(FenceReason::ProposalRetriesExhausted { command: c1 })
    );
    assert!(!leader.is_leading());
}

#[test]
fn a_non_voting_role_never_leads() {
    for role in [ReplicaRole::Observer, ReplicaRole::Learner] {
        let mut leader = leader_with_role(role);
        leader.step(Event::Boot {
            boot_id: BootId([1; 16]),
            incarnation: ReplicaIncarnation::new(1).unwrap(),
        });
        assert!(
            !leader.is_leading(),
            "{role:?} names the ballot's leader but never votes"
        );
        let (e1, _) = admitted(1, 1, 1);
        assert!(leader.step(e1).is_empty(), "{role:?} persists nothing");
        assert!(matches!(
            leader.take_rejections().as_slice(),
            [Rejection::NotLeading { .. }]
        ));
    }
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
            executed: coord_types::ids::ExecutionPosition::ZERO,
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
    assert_eq!(
        released.len(),
        2,
        "the promise reply and the report owed to the candidate"
    );
    assert!(matches!(
        &released[0],
        Effect::SendWhenDurable { to, frame, .. }
            if to.replica == r(2)
                && matches!(ProtocolMessage::decode(frame).unwrap(), ProtocolMessage::Promise { .. })
    ));
    assert!(matches!(
        &released[1],
        Effect::SendWhenDurable { to, frame, .. }
            if to.replica == r(2)
                && matches!(ProtocolMessage::decode(frame).unwrap(), ProtocolMessage::ReportPage(_))
    ));
    assert_eq!(leader.pending_sends(), 0);
    // c1 was accepted with its proposal (task-d89). Its adoption's sends,
    // like the proposal's, were obsolete once the promise was durable,
    // and were dropped (nothing is pending). The row itself was submitted
    // before the promise and is in the cut the report is taken at, so the
    // leader counts it in its own vote set, as it counted an acceptance
    // batch already in flight when a promise arrived.
    assert_eq!(leader.table().phase_of(&c1), Some(Phase::Accept));
    assert!(leader.votes(&c1).unwrap().adopted_by(&r(0)));
    assert!(
        leader
            .report(ballot(1, 2))
            .entries
            .iter()
            .any(|e| e.command == c1 && e.phase == Phase::Accept),
        "the report carries the acceptance the vote counts"
    );
}

#[test]
fn votes_are_collected_but_never_learned_here() {
    let mut leader = booted();
    let (e1, c1) = admitted(1, 1, 1);
    let effects = leader.step(e1);
    let path = leader.proposal(&c1).unwrap().path;
    let paths = leader.proposal(&c1).unwrap().paths.clone();
    let admitted_under = leader
        .table()
        .record(&c1)
        .and_then(|r| r.payload)
        .expect("initialized");
    let ack = |replica: u8| FastAck {
        replica: r(replica),
        ballot: ballot(0, 0),
        command: c1,
        deps: vec![],
        paths: paths.clone(),
        path,
        admission: admitted_under,
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
                    command: c1,
                    admission: admitted_under,
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
    assert!(
        leader.votes(&c1).unwrap().learned_slow().is_none(),
        "r2's adoption and the leader's proposal: the leader's acceptance \
         row is not durable yet (task-d19)"
    );
    // The proposal batch carries the acceptance row (task-d89).
    leader.step(durable(&effects, 1).remove(0));
    let votes = leader.votes(&c1).unwrap();
    assert_eq!(votes.voted(), [r(0), r(1), r(2)].into());
    // The slow predicate holds: the command is committed (task-24), but
    // nothing is established until the materializer applies it.
    assert!(votes.learned_slow().is_some());
    assert_eq!(leader.table().phase_of(&c1), Some(Phase::Commit));
    assert_eq!(leader.next_executable(), Some(c1));
    // An acknowledgement for a command this leader has not proposed is
    // held rather than refused, and counts for nothing until it does.
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
    assert_eq!(leader.take_rejections(), vec![]);
    assert!(
        leader.votes(&unknown).is_none(),
        "a held acknowledgement is not a vote set"
    );
}

/// A fast quorum that forms before the leader's proposal batch is durable
/// commits nothing until it is (task-d89). The leader's fast vote is its
/// proposal, and its acceptance now rides in the same batch, so the table
/// holds the command at ACCEPT from the start: what keeps the leader from
/// committing over a volatile proposal is the batch's durability, as it
/// was when the acceptance waited for it.
#[test]
fn an_early_fast_quorum_commits_nothing_before_the_proposal_is_durable() {
    let mut leader = booted();
    let (e1, c1) = admitted(1, 1, 1);
    let effects = leader.step(e1);
    assert_eq!(leader.table().phase_of(&c1), Some(Phase::Accept));
    let proposal = leader.proposal(&c1).unwrap();
    let ack = FastAck {
        replica: r(1),
        ballot: ballot(0, 0),
        command: c1,
        deps: vec![],
        paths: proposal.paths.clone(),
        path: proposal.path,
        admission: leader.table().record(&c1).and_then(|r| r.payload).unwrap(),
        seqnum: None,
    };
    // r1, the other member of the fast set, saw the submission and agrees.
    leader.step(peer(1, ProtocolMessage::FastAck(ack)));
    assert!(
        leader.votes(&c1).unwrap().learned().is_some(),
        "the votes alone make a fast quorum"
    );
    assert_eq!(
        leader.table().phase_of(&c1),
        Some(Phase::Accept),
        "not committed: the leader's vote is a proposal not yet durable"
    );
    assert_eq!(leader.next_executable(), None);
    leader.step(durable(&effects, 1).remove(0));
    assert_eq!(leader.table().phase_of(&c1), Some(Phase::Commit));
    assert_eq!(leader.next_executable(), Some(c1));
}

/// An acknowledgement that outruns the leader's own proposal still
/// counts toward the fast path.
///
/// Every voter is sent the same submission, so a voter that initializes
/// it first acknowledges it before this leader has ordered it. Refusing
/// that acknowledgement does not lose the command -- the slow path
/// learns it -- but it costs a round trip on every command whose
/// acknowledgement wins the race, and under concurrent callers that is
/// most of them. The benchmark saw a thousand of these on the leader in
/// one matrix.
#[test]
fn an_acknowledgement_that_arrives_before_the_proposal_still_counts() {
    let mut leader = booted();
    // The command's identity, without offering it to the leader yet.
    let (e1, c1) = admitted(1, 1, 1);
    let mut ahead = booted();
    let effects = ahead.step(e1.clone());
    ahead.step(durable(&effects, 1).remove(0));
    let proposal = ahead.proposal(&c1).unwrap();
    let (path, paths) = (proposal.path, proposal.paths.clone());
    let admitted_under = ahead
        .table()
        .record(&c1)
        .and_then(|r| r.payload)
        .expect("initialized");

    // r1 acknowledges first. The leader has proposed nothing.
    let early = FastAck {
        replica: r(1),
        ballot: ballot(0, 0),
        command: c1,
        deps: vec![],
        paths,
        path,
        admission: admitted_under,
        seqnum: None,
    };
    assert!(
        leader
            .step(peer(1, ProtocolMessage::FastAck(early)))
            .is_empty()
    );

    // Now the submission reaches it. The held acknowledgement joins the
    // leader's own, so the fast set is complete without another round.
    let effects = leader.step(e1);
    leader.step(durable(&effects, 1).remove(0));
    let votes = leader.votes(&c1).expect("proposed");
    assert_eq!(
        votes.voted(),
        [r(0), r(1)].into(),
        "the acknowledgement that arrived first was not counted"
    );
    assert_eq!(
        leader.take_rejections(),
        vec![],
        "holding an early acknowledgement is not a refusal"
    );
}

// ---------------------------------------------------------------------
// task-55: a seal fences the leader's own ballot.
// ---------------------------------------------------------------------

fn transition() -> coord_consensus::handoff::Transition {
    coord_consensus::handoff::Transition {
        from: epoch(),
        to: ConfigurationEpoch::new(2).unwrap(),
        subject: Digest32([0xa1; 32]),
    }
}

fn is_seal_report(effect: &Effect) -> bool {
    matches!(
        effect,
        Effect::SendWhenDurable { frame, .. }
            if matches!(ProtocolMessage::decode(frame).unwrap(), ProtocolMessage::Sealed { .. })
    )
}

/// A seal ends the leader's service of its ballot from the cut on: while
/// the row is in flight, once it is durable, and after a restart that
/// reads it back. A failed row fenced nothing, so service resumes.
#[test]
fn a_seal_stops_the_leader_proposing_live_and_after_a_restart() {
    let mut leader = booted();
    let seal = leader.step(peer(
        2,
        ProtocolMessage::SealRequest {
            transition: transition(),
        },
    ));
    let [Effect::Persist(row)] = seal.as_slice() else {
        panic!("a seal persists its row: {seal:?}")
    };
    assert!(!leader.is_leading(), "the cut is taken at the request");
    let (e1, _) = admitted(1, 1, 1);
    assert!(leader.step(e1).is_empty(), "no proposal while sealing");
    let released = leader.step(Event::Storage(StorageEvent::JournalDurable {
        barrier_id: row.barrier,
        journal_seq: LocalJournalSeq::new(1).unwrap(),
    }));
    assert!(released.iter().any(is_seal_report));
    let (e2, _) = admitted(2, 2, 2);
    assert!(leader.step(e2).is_empty(), "no proposal once sealed");
    assert!(!leader.is_leading());
    assert_eq!(
        leader.take_rejections(),
        vec![
            Rejection::Sealed {
                transition: transition()
            };
            2
        ]
    );

    // The restart reads the row and leads nothing.
    let mut restarted = Leader::new_sealed(
        config(),
        None,
        Some(coord_consensus::SealRecordV1 {
            transition: transition(),
            at: ballot(0, 0),
        }),
        ExecutionPosition::ZERO,
    );
    restarted.step(Event::Boot {
        boot_id: BootId([2; 16]),
        incarnation: ReplicaIncarnation::new(1).unwrap(),
    });
    assert!(!restarted.is_leading());
    let (e3, _) = admitted(3, 3, 3);
    assert!(restarted.step(e3).is_empty(), "no proposal after restart");
    assert_eq!(
        restarted.take_rejections(),
        vec![Rejection::Sealed {
            transition: transition()
        }]
    );

    // A failed seal row fenced nothing: the leader leads again.
    let mut leader = booted();
    let seal = leader.step(peer(
        2,
        ProtocolMessage::SealRequest {
            transition: transition(),
        },
    ));
    let [Effect::Persist(row)] = seal.as_slice() else {
        panic!()
    };
    leader.step(Event::Storage(StorageEvent::Failed {
        barrier_id: row.barrier,
        error: StorageError::DefinitelyNotCommitted,
    }));
    assert!(leader.is_leading());
    let (e4, _) = admitted(4, 4, 4);
    assert_eq!(leader.step(e4).len(), 1, "the proposal batch");
}

/// The seal cut includes the acceptance batch of a proposal that is
/// already durable.
///
/// A proposal's own batch becoming durable is the turn in which its
/// acceptance row is written, under a barrier of its own. A cut taken
/// over proposal batches alone would release the seal report while that
/// ACCEPT row was still volatile, and a terminal state built from the
/// report would show the command below ACCEPT although the replica holds
/// ACCEPT once the write lands.
#[test]
fn the_seal_report_waits_for_an_acceptance_batch_still_in_flight() {
    let mut leader = booted();
    let (e1, c1) = admitted(1, 1, 1);
    let proposed = leader.step(e1);
    // The proposal batch carries the ACCEPT row (task-d89).
    let [Effect::Persist(accept)] = proposed.as_slice() else {
        panic!("the proposal batch is written: {proposed:?}")
    };
    assert_eq!(leader.table().phase_of(&c1), Some(Phase::Accept));
    assert!(leader.proposal(&c1).unwrap().accepts);

    let seal = leader.step(peer(
        2,
        ProtocolMessage::SealRequest {
            transition: transition(),
        },
    ));
    let [Effect::Persist(row)] = seal.as_slice() else {
        panic!("a seal persists its row: {seal:?}")
    };
    let released = leader.step(Event::Storage(StorageEvent::JournalDurable {
        barrier_id: row.barrier,
        journal_seq: LocalJournalSeq::new(2).unwrap(),
    }));
    assert!(
        !released.iter().any(is_seal_report),
        "the ACCEPT row is still volatile: {released:?}"
    );
    let released = leader.step(Event::Storage(StorageEvent::JournalDurable {
        barrier_id: accept.barrier,
        journal_seq: LocalJournalSeq::new(3).unwrap(),
    }));
    assert_eq!(released.iter().filter(|e| is_seal_report(e)).count(), 1);
}

/// A service command whose proposal never reached the other voters is
/// proposed to them again when its scheduler presents it again.
///
/// A service command -- an authority epoch, an expiry candidate -- has no
/// caller and no collector to retry it, so a proposal whose peer sends
/// were lost would otherwise sit bound at the leader, heard by nobody,
/// and every later presentation would be refused as a duplicate. Here
/// the first proposal's sends are released and dropped, as a failed
/// send drops them; presenting the same frame again publishes the same
/// proposal to every other voter and writes nothing again.
#[test]
fn a_service_command_presented_again_is_proposed_to_the_voters_again() {
    let service = |operation: CanonicalOperation, seq: u64| {
        let mut logical = LogicalRequest::new(NamespaceId([5; 16]), operation);
        logical.canonicalize();
        let key = RetryKey {
            session_id: SessionId([0; 16]),
            client_instance_id: ClientInstanceId([0; 16]),
            ..retry_key(seq)
        };
        let command = CommandId::derive(&key, &logical).unwrap();
        let frame = MessageV1::Request(RequestV1::new(key, &logical, 0, 0).unwrap())
            .encode()
            .unwrap();
        (frame, command)
    };
    let proposals_to_peers = |effects: &[Effect], command: CommandId| -> BTreeSet<ReplicaId> {
        effects
            .iter()
            .filter_map(|e| match e {
                Effect::SendWhenDurable { to, frame, .. } => match ProtocolMessage::decode(frame) {
                    Ok(ProtocolMessage::Proposal(ack)) if ack.command == command => {
                        Some(to.replica)
                    }
                    _ => None,
                },
                _ => None,
            })
            .collect()
    };
    let peers: BTreeSet<ReplicaId> = [r(1), r(2)].into();

    for (operation, seq) in [
        (
            CanonicalOperation::EstablishLeaseAuthority {
                epoch: LeaseAuthorityEpoch::new(1).unwrap(),
            },
            1,
        ),
        (
            CanonicalOperation::ExpireLease {
                lease_id: LeaseId([7; 16]),
                generation: LeaseGeneration::new(1).unwrap(),
                expected_renewal_sequence: 3,
                authority_epoch: LeaseAuthorityEpoch::new(1).unwrap(),
            },
            2,
        ),
    ] {
        let mut leader = booted();
        let (frame, command) = service(operation, seq);
        let first = leader.propose_service(&frame);
        assert!(
            matches!(first.as_slice(), [Effect::Persist(_)]),
            "the first presentation did not persist the proposal: {first:?}"
        );
        // Durable, so the proposal is released to the voters -- and lost
        // on the way: nothing is done with these sends.
        let mut released = Vec::new();
        for event in durable(&first, 1) {
            released.extend(leader.step(event));
        }
        assert_eq!(proposals_to_peers(&released, command), peers);

        // Presented again: the same proposal goes to every other voter
        // again, and no row is written a second time.
        let again = leader.propose_service(&frame);
        assert!(
            !again.iter().any(|e| matches!(e, Effect::Persist(_))),
            "a second presentation wrote the proposal again: {again:?}"
        );
        assert_eq!(
            proposals_to_peers(&again, command),
            peers,
            "the proposal was not published to the voters again: {again:?}"
        );
        assert!(
            leader
                .take_rejections()
                .contains(&Rejection::ProposalRepublished(command)),
            "the republication was not reported"
        );
        assert_eq!(leader.proposal(&command).unwrap().seqnum, 0);
    }
}

//! task-25 acceptance: reports come from durable state at the cut, not
//! from in-memory phases or a lagging projection; incomplete or corrupt
//! pages never count; a missing payload is fetched and rehashed, never
//! fabricated; old-ballot work is held across recovery and required state
//! survives a crash; legal phase differences select, incompatible
//! candidates are diagnosed from real reports.

use std::collections::BTreeSet;

use coord_consensus::{
    BallotConfiguration, CONSERVATIVE_KEY, CommandRecord, CommandTable, ConfigurationIdentity,
    FastAck, Follower, FollowerConfig, FollowerRejection, Leader, LeaderConfig, PageError, Phase,
    ProtocolMessage, RecoveryError, ReplicaRole, ReportAssembler, ReportPage, decode_dependency,
    paginate, select,
};
use coord_core::capability::{AdmissionReceipt, AttestedAdmission, VerifierToken};
use coord_core::effect::{BootId, Effect, PeerId};
use coord_core::event::{
    AdmittedRequest, AuthenticatedPeerMessage, Event, PeerProvenance, StorageEvent,
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
    BallotConfiguration::c2(
        epoch(),
        ballot(0, 0),
        (0..3).map(r).collect(),
        [r(0), r(1)].into(),
    )
    .unwrap()
}

fn boot_event() -> Event {
    Event::Boot {
        boot_id: BootId([1; 16]),
        incarnation: ReplicaIncarnation::new(1).unwrap(),
    }
}

fn follower(me: u8) -> Follower {
    let mut f = Follower::new(FollowerConfig {
        identity: identity(me),
        quorum: quorum(),
        genesis: ballot(0, 0),
        frontend: FRONTEND,
        capacity: 16,
    });
    f.step(boot_event());
    f
}

fn leader() -> Leader {
    let mut l = Leader::new(
        LeaderConfig {
            identity: identity(0),
            quorum: quorum(),
            genesis: ballot(0, 0),
            frontend: FRONTEND,
            capacity: 16,
        },
        None,
        ExecutionPosition::ZERO,
    );
    l.step(boot_event());
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

fn admitted(seq: u64, key: u8) -> (Event, CommandId) {
    let request = LogicalRequest::new(
        NamespaceId([5; 16]),
        CanonicalOperation::Put(PutOp {
            key: vec![key],
            value: vec![key],
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

fn durable_of(effects: &[Effect]) -> Vec<Event> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::Persist(b) => Some(Event::Storage(StorageEvent::JournalDurable {
                barrier_id: b.barrier,
                journal_seq: LocalJournalSeq::new(1).unwrap(),
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

fn proposal_from(table: &CommandTable, c: CommandId, seqnum: u64) -> ProtocolMessage {
    let rec = table.record(&c).unwrap();
    ProtocolMessage::Proposal(FastAck {
        replica: r(0),
        ballot: ballot(0, 0),
        command: c,
        deps: rec.deps.clone(),
        paths: rec.paths.clone(),
        path: rec.path,
        seqnum: Some(seqnum),
    })
}

#[test]
fn reports_come_from_durable_state_at_the_cut_not_in_memory_phases() {
    let mut f = follower(1);
    let (e1, c1) = admitted(1, 1);
    let (e2, c2) = admitted(2, 2);
    let (e3, c3) = admitted(3, 3);
    // The leader saw c3 first: its proposal for c3 has no dependencies.
    let mut leader_view = CommandTable::new();
    leader_view
        .initialize(c3, c3.0, vec![CONSERVATIVE_KEY.to_vec()])
        .unwrap();
    // c1: vote durable. c2: vote batch pending. c3: vote durable, adoption
    // of the leader order pending.
    let eff1 = f.step(e1);
    f.step(durable_of(&eff1).remove(0));
    let _eff2 = f.step(e2);
    let eff3 = f.step(e3);
    f.step(durable_of(&eff3).remove(0));
    let adopt = f.step(peer(0, proposal_from(&leader_view, c3, 0)));
    assert_eq!(
        adopt.len(),
        1,
        "adoption batch submitted, not yet durable: {:?}",
        f.take_rejections()
    );
    assert!(
        f.table().phase_of(&c3) >= Some(Phase::Accept),
        "in memory the adoption (and slow learning) already happened"
    );
    let report = f.report(ballot(1, 2));
    let entries: Vec<(CommandId, Phase)> = report
        .entries
        .iter()
        .map(|e| (e.command, e.phase))
        .collect();
    assert!(entries.contains(&(c1, Phase::PreAccept)));
    assert!(
        !entries.iter().any(|(c, _)| *c == c2),
        "an undurable vote is not stable"
    );
    assert!(
        entries.contains(&(c3, Phase::PreAccept)),
        "the durable phase, not the pending adoption"
    );
    assert!(report.entries.iter().all(|e| e.payload_present));
    assert_eq!(report.committed_ballot, ballot(0, 0));
    // Once the adoption is durable the report reflects it.
    f.step(durable_of(&adopt).remove(0));
    let report = f.report(ballot(1, 2));
    assert!(
        report
            .entries
            .iter()
            .any(|e| e.command == c3 && e.phase == Phase::Accept)
    );
    // The leader's report likewise comes from its durable proposals.
    let mut l = leader();
    let (e1, c1) = admitted(1, 1);
    let eff = l.step(e1);
    assert!(
        l.report(ballot(1, 2)).entries.is_empty(),
        "pending proposal is not reported"
    );
    let accepted = l.step(durable_of(&eff).remove(0));
    // The proposal row is durable; the report says PRE-ACCEPT until the
    // acceptance the guard allowed is durable too.
    let entries = l.report(ballot(1, 2)).entries;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].command, c1);
    assert_eq!(entries[0].phase, Phase::PreAccept);
    for event in durable_of(&accepted) {
        l.step(event);
    }
    let entries = l.report(ballot(1, 2)).entries;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].command, c1);
    assert_eq!(entries[0].phase, Phase::Accept);
}

#[test]
fn pages_assemble_only_when_complete_and_verified() {
    let mut f = follower(1);
    let mut commands = Vec::new();
    for i in 1..=7u8 {
        let (e, c) = admitted(u64::from(i), i);
        let eff = f.step(e);
        f.step(durable_of(&eff).remove(0));
        commands.push(c);
    }
    let report = f.report(ballot(1, 2));
    let pages = paginate(&report, 3);
    assert_eq!(pages.len(), 3);
    assert!(pages.iter().all(|p| p.total == 3));
    // Delivered out of order, with a duplicate: assembled exactly once.
    let mut asm = ReportAssembler::new(ballot(1, 2));
    asm.accept(pages[2].clone()).unwrap();
    assert!(asm.complete().is_empty());
    assert_eq!(asm.incomplete(), vec![r(1)]);
    asm.accept(pages[0].clone()).unwrap();
    asm.accept(pages[0].clone()).unwrap();
    assert!(
        asm.complete().is_empty(),
        "one page still missing never counts"
    );
    asm.accept(pages[1].clone()).unwrap();
    let complete = asm.complete();
    assert_eq!(complete, vec![report.clone()]);
    assert!(asm.incomplete().is_empty());
    // A corrupt page is refused; a page beyond the bound is refused; a page
    // of another ballot is refused; inconsistent totals are refused.
    let mut corrupt = pages[1].clone();
    corrupt.entries.pop();
    assert_eq!(asm.accept(corrupt), Err(PageError::Corrupt));
    let mut huge = pages[0].clone();
    huge.total = coord_consensus::MAX_REPORT_PAGES + 1;
    assert_eq!(asm.accept(huge), Err(PageError::OutOfBounds));
    let mut other = pages[0].clone();
    other.ballot = ballot(2, 2);
    assert_eq!(asm.accept(other), Err(PageError::WrongBallot));
    let mut asm2 = ReportAssembler::new(ballot(1, 2));
    asm2.accept(pages[0].clone()).unwrap();
    let mut inconsistent = paginate(&report, 2)[1].clone();
    inconsistent.page = 1;
    assert_eq!(asm2.accept(inconsistent), Err(PageError::Inconsistent));
    // Selection never sees an incomplete report: with only one complete
    // report of three voters there are not enough.
    let cfg = BallotConfiguration::c2(
        epoch(),
        ballot(1, 2),
        (0..3).map(r).collect(),
        [r(2), r(0)].into(),
    )
    .unwrap();
    assert_eq!(
        select(&cfg, &asm.complete()),
        Err(RecoveryError::InsufficientReports { have: 1, need: 2 })
    );
    let _ = ReportPage {
        replica: r(9),
        ballot: ballot(1, 2),
        committed_ballot: ballot(0, 0),
        page: 0,
        total: 1,
        entries: vec![],
        snapshot: Digest32([0; 32]),
        digest: Digest32([0; 32]),
    };
}

#[test]
fn a_missing_payload_is_fetched_and_rehashed_never_fabricated() {
    let mut l = leader();
    let mut f = follower(2);
    let (e1, c1) = admitted(1, 1);
    let eff = l.step(e1);
    let released = l.step(durable_of(&eff).remove(0));
    let (_, proposal) = sends(&released)
        .into_iter()
        .find(|(to, m)| *to == r(2) && matches!(m, ProtocolMessage::Proposal(_)))
        .unwrap();
    // The proposal reaches r2 but the client's request never does.
    assert!(f.step(peer(0, proposal)).is_empty());
    assert_eq!(f.missing_payloads(), vec![c1]);
    assert!(
        f.report(ballot(1, 2)).entries.is_empty(),
        "no fabricated entry"
    );
    // r2 fetches; the request needs no durable prerequisite.
    let req = f.request_payloads(r(0));
    let sent = sends(&req);
    assert_eq!(sent.len(), 1);
    assert!(
        matches!(&sent[0].1, ProtocolMessage::PayloadRequest { commands } if commands == &vec![c1])
    );
    let response = l.step(peer(2, sent[0].1.clone()));
    let responses = sends(&response);
    assert_eq!(responses.len(), 1);
    let ProtocolMessage::PayloadResponse { command, payload } = responses[0].1.clone() else {
        panic!()
    };
    assert_eq!(command, c1);
    // A tampered payload does not rehash to the identity: refused, still
    // missing.
    let mut tampered = payload.clone();
    tampered.retry_key = retry_key(9);
    assert!(
        f.step(peer(
            0,
            ProtocolMessage::PayloadResponse {
                command,
                payload: tampered
            }
        ))
        .is_empty()
    );
    assert_eq!(
        f.take_rejections(),
        vec![FollowerRejection::PayloadIdentityMismatch(c1)]
    );
    assert_eq!(f.missing_payloads(), vec![c1]);
    // The genuine payload initializes the command like an admitted
    // request; the held proposal is adopted after the batch, and the
    // report now carries it.
    let effects = f.step(peer(
        0,
        ProtocolMessage::PayloadResponse { command, payload },
    ));
    assert!(f.missing_payloads().is_empty());
    assert!(!effects.is_empty());
    let mut all = effects;
    loop {
        let d = durable_of(&all);
        if d.is_empty() {
            break;
        }
        all = Vec::new();
        for e in d {
            all.extend(f.step(e));
        }
    }
    assert_eq!(
        f.table().phase_of(&c1).map(|p| p >= Phase::Accept),
        Some(true)
    );
    assert!(
        f.report(ballot(1, 2))
            .entries
            .iter()
            .any(|e| e.command == c1)
    );
    // An undurable payload is never served: a fresh leader with a pending
    // proposal answers nothing.
    let mut l2 = leader();
    let (e2, c2) = admitted(2, 2);
    l2.step(e2);
    assert!(
        sends(&l2.step(peer(
            2,
            ProtocolMessage::PayloadRequest { commands: vec![c2] }
        )))
        .is_empty()
    );
}

#[test]
fn old_ballot_work_is_held_across_recovery_and_required_state_survives_a_crash() {
    let mut f = follower(1);
    let mut storage = StorageModel::default();
    let (e1, c1) = admitted(1, 1);
    let eff = f.step(e1);
    let Effect::Persist(batch) = eff[0].clone() else {
        panic!()
    };
    storage.submit(batch);
    storage
        .complete(match &eff[0] {
            Effect::Persist(b) => b.barrier,
            _ => panic!(),
        })
        .unwrap();
    f.step(durable_of(&eff).remove(0));
    // A promise for ballot 1 (r2) is made and persisted.
    let promise = f.step(peer(
        2,
        ProtocolMessage::NewLeader {
            ballot: ballot(1, 2),
        },
    ));
    let Effect::Persist(pb) = promise[0].clone() else {
        panic!()
    };
    storage.submit(pb.clone());
    storage.complete(pb.barrier).unwrap();
    f.step(durable_of(&promise).remove(0));
    assert_eq!(f.ballots().promised(), ballot(1, 2));
    // A late proposal of ballot 0 is refused: no adoption, no slow ack.
    let mut leader_view = CommandTable::new();
    leader_view
        .initialize(c1, c1.0, vec![CONSERVATIVE_KEY.to_vec()])
        .unwrap();
    assert!(
        f.step(peer(0, proposal_from(&leader_view, c1, 0)))
            .is_empty()
    );
    assert_eq!(
        f.take_rejections(),
        vec![FollowerRejection::FencedByPromise {
            promised: ballot(1, 2)
        }]
    );
    assert_eq!(f.table().phase_of(&c1), Some(Phase::PreAccept));
    let before = f.report(ballot(1, 2));
    // Crash: recover from the durable rows; the report is identical and
    // the promise still bounds old messages.
    storage.crash();
    let rows: Vec<(CommandId, CommandRecord)> = storage
        .durable_rows()
        .into_iter()
        .filter(|(c, k, _)| *c == Collection::ProtocolV1.id().0 && k.len() == 41 && k[8] == 0x01)
        .map(|(_, k, v)| {
            (
                CommandId(Digest32(k[9..].try_into().unwrap())),
                decode_dependency(&v).unwrap(),
            )
        })
        .collect();
    let promise_row = storage
        .durable_rows()
        .into_iter()
        .find(|(c, k, _)| *c == Collection::ProtocolV1.id().0 && k.len() == 9)
        .map(|(_, _, v)| coord_consensus::decode_promise(&v).unwrap());
    let mut recovered = Follower::recover(
        FollowerConfig {
            identity: identity(1),
            quorum: quorum(),
            genesis: ballot(0, 0),
            frontend: FRONTEND,
            capacity: 16,
        },
        promise_row,
        rows,
        // The durable payload rows this replica may still serve.
        payload_rows(&storage),
        ExecutionPosition::ZERO,
    );
    recovered.step(Event::Boot {
        boot_id: BootId([2; 16]),
        incarnation: ReplicaIncarnation::new(1).unwrap(),
    });
    assert_eq!(recovered.report(ballot(1, 2)), before);
    assert_eq!(recovered.ballots().promised(), ballot(1, 2));
    assert!(
        recovered
            .step(peer(
                0,
                ProtocolMessage::NewLeader {
                    ballot: ballot(0, 0)
                }
            ))
            .is_empty()
    );
    assert!(
        recovered
            .step(peer(0, proposal_from(&leader_view, c1, 0)))
            .is_empty()
    );
    assert_eq!(
        recovered.take_rejections(),
        vec![
            FollowerRejection::Promise(coord_consensus::PromiseRejection::NotHigher {
                promised: ballot(1, 2)
            }),
            FollowerRejection::FencedByPromise {
                promised: ballot(1, 2)
            }
        ]
    );
}

#[test]
fn legal_phase_differences_select_and_incompatible_candidates_are_diagnosed() {
    let (e1, c1) = admitted(1, 1);
    let mut leader_view = CommandTable::new();
    leader_view
        .initialize(c1, c1.0, vec![CONSERVATIVE_KEY.to_vec()])
        .unwrap();
    // r1 adopted the leader's order for c1; r2 only voted (PreAccept).
    let mut f1 = follower(1);
    let mut f2 = follower(2);
    for f in [&mut f1, &mut f2] {
        let eff = f.step(e1.clone());
        f.step(durable_of(&eff).remove(0));
    }
    let adopt = f1.step(peer(0, proposal_from(&leader_view, c1, 0)));
    assert_eq!(adopt.len(), 1, "{:?}", f1.take_rejections());
    f1.step(durable_of(&adopt).remove(0));
    let cfg = BallotConfiguration::c2(
        epoch(),
        ballot(1, 2),
        (0..3).map(r).collect(),
        [r(2), r(0)].into(),
    )
    .unwrap();
    let reports = vec![f1.report(ballot(1, 2)), f2.report(ballot(1, 2))];
    let decision = select(&cfg, &reports).unwrap();
    assert_eq!(decision.entries[&c1].phase, Phase::Accept);
    assert_eq!(decision.entries[&c1].deps, vec![]);
    assert!(decision.reproposed.is_empty());
    // Two replicas that adopted different orders for the same command
    // under the same synchronized ballot: diagnosed, never merged.
    let (e2, c2) = admitted(2, 2);
    let mut g1 = follower(1);
    let mut g2 = follower(2);
    for g in [&mut g1, &mut g2] {
        for e in [e1.clone(), e2.clone()] {
            let eff = g.step(e);
            g.step(durable_of(&eff).remove(0));
        }
        // Both adopt c1 first, so c2's proposals can be adopted.
        let a = g.step(peer(0, proposal_from(&leader_view, c1, 0)));
        g.step(durable_of(&a).remove(0));
    }
    let honest = FastAck {
        replica: r(0),
        ballot: ballot(0, 0),
        command: c2,
        deps: vec![c1],
        paths: vec![],
        path: Digest32([1; 32]),
        seqnum: Some(1),
    };
    let mut conflicting = honest.clone();
    conflicting.deps = vec![];
    let a1 = g1.step(peer(0, ProtocolMessage::Proposal(honest)));
    g1.step(durable_of(&a1).remove(0));
    let a2 = g2.step(peer(0, ProtocolMessage::Proposal(conflicting)));
    g2.step(durable_of(&a2).remove(0));
    let reports = vec![g1.report(ballot(1, 2)), g2.report(ballot(1, 2))];
    assert!(matches!(
        select(&cfg, &reports),
        Err(RecoveryError::IncompatibleAccepted { command, .. }) if command == c2
    ));
    let _ = BTreeSet::<u8>::new();
}

/// Durable payload rows of a storage model, as recovery consumes them.
fn payload_rows(storage: &StorageModel) -> Vec<(CommandId, coord_consensus::PayloadRecordV1)> {
    storage
        .durable_rows()
        .into_iter()
        .filter(|(c, _, _)| *c == Collection::PayloadV1.id().0)
        .map(|(_, k, v)| {
            (
                CommandId(coord_types::identity::Digest32(k[..].try_into().unwrap())),
                coord_consensus::decode_payload(&v).unwrap(),
            )
        })
        .collect()
}

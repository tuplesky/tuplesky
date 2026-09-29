//! task-d22: every collector entry the voters refuse ends.
//!
//! A voter answered three refusals with no effect at all -- a request
//! under other admission facts, another payload under a bound retry key,
//! a duplicate whose payload it had forgotten -- and the collector
//! removed an entry only when it settled, so each such entry held one of
//! the domain's pending slots for good. A voter now says why it refused,
//! and the collector settles on it: a retry key bound to another request
//! is a conflict, and the other two are answered from the durable record.
//! An entry holding neither half of a release is asked for again, and so
//! is every entry after a ballot change.

use std::collections::BTreeSet;

use coord_collector::{
    Collector, CollectorConfig, CollectorEvent, EvidenceError, HoldReason, MonotonicMillis,
    Progress, SOLICIT_AFTER_MILLIS, SettleError, SubmitRefusal, Submitted,
};
use coord_consensus::{BallotConfiguration, ProtocolMessage, SubmissionRefusal};
use coord_core::capability::{AdmissionReceipt, AttestedAdmission, VerifierToken};
use coord_core::event::{AdmittedRequest, PeerProvenance};
use coord_types::identity::Digest32;
use coord_types::ids::*;
use coord_types::logical_v1::*;
use coord_types::wire_v1::{MessageV1, OutcomeV1, RequestV1, codes};
use coord_types::{CommandId, RetryKey};

const NS: NamespaceId = NamespaceId([5; 16]);
const SESSION: SessionId = SessionId([3; 16]);
const CLUSTER: ClusterId = ClusterId([1; 16]);
const DOMAIN: DomainId = DomainId([2; 16]);
const CLIENT: ClientInstanceId = ClientInstanceId([4; 16]);

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

fn quorum(b: Ballot) -> BallotConfiguration {
    let fast: BTreeSet<ReplicaId> = (0..2).map(r).collect();
    BallotConfiguration::c2(epoch(), b, (0..3).map(r).collect(), fast).unwrap()
}

fn admitted(seq: u64) -> (CommandId, AdmittedRequest) {
    let mut logical = LogicalRequest::new(
        NS,
        CanonicalOperation::Put(PutOp {
            key: b"k".to_vec(),
            value: seq.to_be_bytes().to_vec(),
            lease: None,
            prev_kv: false,
        }),
    );
    logical.canonicalize();
    let key = RetryKey {
        cluster_id: CLUSTER,
        domain_id: DOMAIN,
        session_id: SESSION,
        client_instance_id: CLIENT,
        request_sequence: RequestSequence::new(seq).unwrap(),
    };
    let command = CommandId::derive(&key, &logical).unwrap();
    (
        command,
        AdmittedRequest {
            receipt: AdmissionReceipt::submitting(
                VerifierToken::for_boundary(),
                AttestedAdmission {
                    cluster: CLUSTER,
                    domain: DOMAIN,
                    session: SESSION,
                    rule_generation: 1,
                    scope_ceiling: u32::MAX,
                    receipt_id: Digest32([7; 32]),
                    admitted_at_ticks: 0,
                },
            ),
            frame: MessageV1::Request(RequestV1::new(key, &logical, 0, 0).unwrap())
                .encode()
                .unwrap(),
        },
    )
}

fn collector(max_pending: usize) -> Collector {
    Collector::new(CollectorConfig {
        quorum: quorum(ballot(0, 0)),
        max_pending,
        max_resolved: max_pending,
        max_undelivered_bytes: usize::MAX,
    })
}

fn submit(c: &mut Collector, seq: u64) -> CommandId {
    let (command, request) = admitted(seq);
    assert!(matches!(
        c.submit(MonotonicMillis::ZERO, &request),
        Ok(Submitted::FanOut(_))
    ));
    command
}

fn from(i: u8) -> PeerProvenance {
    PeerProvenance::from_transport(r(i), ReplicaIncarnation::new(1).unwrap(), 1)
}

fn refused(command: CommandId, refusal: SubmissionRefusal) -> ProtocolMessage {
    ProtocolMessage::Refused {
        ballot: ballot(0, 0),
        command,
        refusal,
    }
}

/// A session sends 256 retries the voters refuse, one per pending slot:
/// the domain is full, each refusal ends its entry, and the domain serves
/// new work again.
#[test]
fn refused_retries_free_every_pending_slot() {
    let mut c = collector(256);
    let commands: Vec<CommandId> = (1..=256).map(|seq| submit(&mut c, seq)).collect();
    let (_, next) = admitted(1000);
    assert!(
        matches!(
            c.submit(MonotonicMillis::ZERO, &next),
            Err(SubmitRefusal::Backpressure { pending: 256 })
        ),
        "every slot is held"
    );
    for (i, command) in commands.iter().enumerate() {
        let refusal = if i % 2 == 0 {
            SubmissionRefusal::OtherCommand {
                bound: CommandId(Digest32([9; 32])),
            }
        } else {
            SubmissionRefusal::OtherFacts {
                accepted: Digest32([8; 32]),
            }
        };
        let progress = c.on_evidence(from(1), refused(*command, refusal)).unwrap();
        assert_eq!(progress, Progress::Held(HoldReason::AwaitingRecord));
        // This node's record of the key answers it: the other command it
        // binds, or this command's execution.
        let settled = match refusal {
            SubmissionRefusal::OtherCommand { bound } => {
                c.settle_conflict_from_record(*command, bound).unwrap()
            }
            _ => c
                .settle_from_record(
                    *command,
                    Digest32([1; 32]),
                    ExecutionPosition::new(i as u64 + 1).unwrap(),
                    None,
                    b"done",
                )
                .unwrap(),
        };
        assert!(matches!(settled, Progress::Released(_)), "{settled:?}");
    }
    assert_eq!(c.pending(), 0);
    assert!(matches!(
        c.submit(MonotonicMillis::ZERO, &next),
        Ok(Submitted::FanOut(_))
    ));
}

/// A retry key a voter holds bound to another request holds the entry
/// for the record; this node's record of the key, bound to the other
/// request, ends it with a conflict. The answer is not kept for the key:
/// a later release of the command is not compared with it.
#[test]
fn a_key_bound_to_another_request_ends_the_entry_with_a_conflict() {
    let mut c = collector(4);
    let command = submit(&mut c, 1);
    let bound = CommandId(Digest32([9; 32]));
    let progress = c
        .on_evidence(
            from(2),
            refused(command, SubmissionRefusal::OtherCommand { bound }),
        )
        .unwrap();
    assert_eq!(progress, Progress::Held(HoldReason::AwaitingRecord));
    // A record naming the command itself is not a conflict.
    assert!(c.settle_conflict_from_record(command, command).is_err());
    let progress = c.settle_conflict_from_record(command, bound).unwrap();
    let Progress::Released(release) = progress else {
        panic!("{progress:?}");
    };
    assert_eq!(release.command, command);
    assert!(!release.speculative && !release.fast);
    assert!(matches!(
        release.response.outcome,
        OutcomeV1::Err { code, .. } if code == codes::REQUEST_IDENTITY_CONFLICT
    ));
    assert!(!c.is_pending(&command));
    // Another voter's refusal of the same command changes nothing.
    assert_eq!(
        c.on_evidence(
            from(1),
            refused(command, SubmissionRefusal::OtherCommand { bound })
        ),
        Err(EvidenceError::UnknownCommand)
    );
    assert!(c.trace().iter().any(|e| matches!(
        e,
        CollectorEvent::VoterRefused { reason, .. } if reason == "other-command"
    )));
}

/// One voter's refusal is not the domain's decision (Codex review): a
/// minority voter that saw another presentation of the key first refuses
/// the command, while a quorum accepts and executes it. The entry is held,
/// and settles from the votes and the leader's release as any other.
#[test]
fn a_minority_voter_bound_to_another_request_does_not_end_the_entry() {
    let mut c = collector(4);
    let command = submit(&mut c, 1);
    let bound = CommandId(Digest32([9; 32]));
    assert_eq!(
        c.on_evidence(
            from(2),
            refused(command, SubmissionRefusal::OtherCommand { bound })
        ),
        Ok(Progress::Held(HoldReason::AwaitingRecord))
    );
    assert!(
        c.is_pending(&command),
        "the entry was ended on one voter's word"
    );
    // Nothing without a record settles it either.
    assert!(c.half_established().iter().any(|(k, _)| *k == command));
}

/// A command a voter holds under other admission facts, or executed so
/// long ago it forgot the payload, is answered from this node's durable
/// record: with nothing of the collector's own to corroborate it.
#[test]
fn other_facts_and_a_forgotten_duplicate_are_answered_from_the_record() {
    for refusal in [
        SubmissionRefusal::OtherFacts {
            accepted: Digest32([8; 32]),
        },
        SubmissionRefusal::Forgotten,
    ] {
        let mut c = collector(4);
        let command = submit(&mut c, 1);
        // Before the refusal the record alone settles nothing.
        assert_eq!(
            c.settle_from_record(
                command,
                Digest32([1; 32]),
                ExecutionPosition::new(1).unwrap(),
                None,
                b"done"
            ),
            Err(SettleError::Uncorroborated)
        );
        assert!(c.half_established().is_empty());
        assert_eq!(
            c.on_evidence(from(0), refused(command, refusal)).unwrap(),
            Progress::Held(HoldReason::AwaitingRecord)
        );
        assert_eq!(c.half_established().len(), 1);
        let Progress::Released(release) = c
            .settle_from_record(
                command,
                Digest32([1; 32]),
                ExecutionPosition::new(1).unwrap(),
                None,
                b"done",
            )
            .unwrap()
        else {
            panic!("not released");
        };
        assert!(matches!(release.response.outcome, OutcomeV1::Ok { .. }));
        assert!(c.trace().iter().any(|e| matches!(
            e,
            CollectorEvent::SettledFromRecord { corroborated, .. } if corroborated == "record"
        )));
    }
}

/// A refusal from a replica that is not a voter is not counted.
#[test]
fn a_refusal_from_a_stranger_is_not_counted() {
    let mut c = collector(4);
    let command = submit(&mut c, 1);
    assert_eq!(
        c.on_evidence(from(9), refused(command, SubmissionRefusal::Forgotten)),
        Err(EvidenceError::NotAVoter { sender: r(9) })
    );
    assert!(c.half_established().is_empty());
}

/// An entry that holds neither half of a release is submitted to every
/// voter again after `SOLICIT_AFTER_MILLIS`, and from the second time on
/// the record may settle it. One that holds a half is not asked for.
#[test]
fn an_entry_holding_nothing_is_asked_for_again_then_settled_from_the_record() {
    let mut c = collector(4);
    let command = submit(&mut c, 1);
    assert_eq!(
        c.next_solicit(),
        Some(MonotonicMillis::new(SOLICIT_AFTER_MILLIS))
    );
    assert!(
        c.due_solicits(MonotonicMillis::new(SOLICIT_AFTER_MILLIS - 1), 16)
            .is_empty()
    );
    let first = c.due_solicits(MonotonicMillis::new(SOLICIT_AFTER_MILLIS), 16);
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].command, command);
    assert_eq!(first[0].targets, (0..3).map(r).collect::<Vec<_>>());
    assert!(
        c.half_established().is_empty(),
        "asked for once, not yet the record's"
    );
    let second = c.due_solicits(MonotonicMillis::new(2 * SOLICIT_AFTER_MILLIS), 16);
    assert_eq!(second.len(), 1);
    assert_eq!(c.half_established(), vec![(command, first[0].retry_key)]);
    assert!(
        c.trace()
            .iter()
            .any(|e| matches!(e, CollectorEvent::Solicited { times: 2, .. }))
    );
}

/// A ballot change voids what an entry held and nothing sends it again:
/// every entry is asked for again at once and may be settled from the
/// record.
#[test]
fn a_ballot_change_asks_for_every_entry_again() {
    let mut c = collector(4);
    let a = submit(&mut c, 1);
    let b = submit(&mut c, 2);
    c.reconfigure(quorum(ballot(1, 1)));
    let asked = c.due_solicits(MonotonicMillis::new(1), 16);
    let commands: BTreeSet<CommandId> = asked.iter().map(|f| f.command).collect();
    assert_eq!(commands, BTreeSet::from([a, b]));
    assert_eq!(c.half_established().len(), 2);
}

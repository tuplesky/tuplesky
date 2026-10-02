//! Current reads served by the leader read barrier, as the frontend's
//! dispatcher plans them (task-d50).
//!
//! A read waits on its leader's answer outside the collector, so each
//! rule the collector keeps for an ordered command -- one payload per
//! retry key, a retry attaching to what is already running, a client
//! deadline measured from the presentation -- has to hold for it too,
//! including after it falls back to the ordered path.

use std::collections::BTreeSet;

use coord_collector::{
    Action, Admission, AdmissionLimits, Caller, Collector, CollectorConfig, Delivery, Dispatcher,
    MonotonicMillis, ReadAnswerV1, ReadOutcomeV1, ReadRefusal, ReadResolution, codes,
};
use coord_consensus::BallotConfiguration;
use coord_storage::WatchHub;
use coord_types::ids::{
    Ballot, ClientInstanceId, ClusterId, ConfigurationEpoch, DomainId, KvRevision, NamespaceId,
    ReplicaId, RequestSequence, SessionId,
};
use coord_types::logical_v1::{CanonicalOperation, KeyRange, LogicalRequest, PutOp, RangeOp};
use coord_types::wire_v1::{
    Frame, FrameReader, MessageV1, OutcomeV1, PeerRole, RequestV1, ResponseV1, decode_stream,
};
use coord_types::{CommandId, RetryKey};

const NS: NamespaceId = NamespaceId([5; 16]);
const SESSION: SessionId = SessionId([3; 16]);
const CLIENT: ClientInstanceId = ClientInstanceId([4; 16]);
const CLUSTER: ClusterId = ClusterId([1; 16]);
const DOMAIN: DomainId = DomainId([2; 16]);
/// A wall-clock reading, Unix seconds: what the admission gate takes.
const WALL: u64 = 1_700_000_000;

fn r(i: u8) -> ReplicaId {
    ReplicaId([i; 16])
}

fn quorum() -> BallotConfiguration {
    let epoch = ConfigurationEpoch::new(1).unwrap();
    let fast: BTreeSet<ReplicaId> = (0..2).map(r).collect();
    BallotConfiguration::c2(
        epoch,
        Ballot {
            epoch,
            number: 0,
            leader: r(0),
        },
        (0..3).map(r).collect(),
        fast,
    )
    .unwrap()
}

fn collector() -> Collector {
    Collector::new(CollectorConfig {
        quorum: quorum(),
        max_pending: 16,
        max_resolved: 16,
        max_undelivered_bytes: usize::MAX,
    })
}

/// A dispatcher sending current reads to the leader of ballot 0, r(0).
fn dispatcher() -> Dispatcher {
    let mut d = Dispatcher::new(
        Admission::new(CLUSTER, DOMAIN, AdmissionLimits::default()),
        collector(),
        16,
    );
    d.set_leader_reads(true);
    d
}

fn caller() -> Caller {
    Caller {
        role: PeerRole::Client,
        session: SESSION,
        rule_generation: 1,
        scope_ceiling: u32::MAX,
    }
}

fn retry_key(seq: u64) -> RetryKey {
    RetryKey {
        cluster_id: CLUSTER,
        domain_id: DOMAIN,
        session_id: SESSION,
        client_instance_id: CLIENT,
        request_sequence: RequestSequence::new(seq).unwrap(),
    }
}

/// A put of `k{seq}` under retry key `seq`.
fn put(seq: u64, deadline_ms: u32) -> (CommandId, RequestV1) {
    encoded(
        seq,
        deadline_ms,
        CanonicalOperation::Put(PutOp {
            key: format!("k{seq}").into_bytes(),
            value: b"v".to_vec(),
            lease: None,
            prev_kv: false,
        }),
    )
}

/// A current read of `key` under retry key `seq`: one the barrier serves.
fn get(seq: u64, key: &str, deadline_ms: u32) -> (CommandId, RequestV1) {
    encoded(
        seq,
        deadline_ms,
        CanonicalOperation::Range(RangeOp {
            range: KeyRange::exact(key),
            revision: None,
            limit: 0,
            keys_only: false,
            count_only: false,
        }),
    )
}

fn encoded(seq: u64, deadline_ms: u32, operation: CanonicalOperation) -> (CommandId, RequestV1) {
    let mut logical = LogicalRequest::new(NS, operation);
    logical.canonicalize();
    let key = retry_key(seq);
    let command = CommandId::derive(&key, &logical).unwrap();
    (
        command,
        RequestV1::new(key, &logical, deadline_ms, 0).unwrap(),
    )
}

fn frame_of(request: &RequestV1) -> Frame {
    let bytes = MessageV1::Request(request.clone()).encode().unwrap();
    let mut reader = FrameReader::new();
    reader.push(&bytes).expect("within the reader bound");
    reader.next_frame().unwrap().expect("one frame")
}

fn hub() -> WatchHub {
    WatchHub::new(KvRevision::ZERO, KvRevision::ZERO)
}

fn ms(millis: u64) -> MonotonicMillis {
    MonotonicMillis::new(millis)
}

fn response(frame: &[u8]) -> ResponseV1 {
    match decode_stream(frame).unwrap().as_slice() {
        [MessageV1::Response(r)] => r.clone(),
        other => panic!("not a response: {other:?}"),
    }
}

fn answer(seq: u64, outcome: ReadOutcomeV1) -> ReadAnswerV1 {
    ReadAnswerV1 {
        retry_key: retry_key(seq),
        ballot: quorum().ballot(),
        outcome,
    }
}

fn refused(seq: u64) -> ReadAnswerV1 {
    answer(
        seq,
        ReadOutcomeV1::Refused {
            reason: ReadRefusal::Expired,
        },
    )
}

/// Present `request` on `connection` at `t`.
fn present(d: &mut Dispatcher, t: u64, connection: u64, request: &RequestV1) -> Action {
    d.on_frame(
        WALL,
        ms(t),
        connection,
        &caller(),
        &frame_of(request),
        &hub(),
    )
}

/// The plain case: a current read goes to the leader, and its served
/// answer is the caller's, carrying the leader's bytes.
#[test]
fn a_current_read_goes_to_the_leader_and_its_answer_is_delivered() {
    let mut d = dispatcher();
    let (command, read) = get(1, "a", 0);
    let Action::Read(plan) = present(&mut d, 0, 1, &read) else {
        panic!("not sent to the leader");
    };
    assert_eq!(plan.leader, r(0));
    assert_eq!(plan.command, command);
    assert_eq!(d.reads_waiting(), 1);
    assert!(!d.collector().is_pending(&command));

    let Some(ReadResolution::Answer(Delivery {
        connection, frame, ..
    })) = d.on_read_answer(
        ms(3),
        r(0),
        answer(
            1,
            ReadOutcomeV1::Served {
                response: b"rows".to_vec(),
            },
        ),
    )
    else {
        panic!("not answered");
    };
    assert_eq!(connection, 1);
    let response = response(&frame);
    assert_eq!(response.command_id, command);
    let OutcomeV1::Ok { result, .. } = response.outcome else {
        panic!("not served: {:?}", response.outcome);
    };
    assert_eq!(result.as_slice(), b"rows");
    assert_eq!(d.reads_waiting(), 0);
}

/// A read that fell back to the ordered path is a command the collector
/// holds, whether or not a caller is still attached to it. Its caller's
/// connection closes; the retry, on another connection, attaches to that
/// command. Sending it to the leader again would answer the same
/// invocation twice, once from a state the ordered command may already
/// have moved past.
#[test]
fn a_retry_of_a_read_ordered_and_then_abandoned_attaches_to_the_command() {
    let mut d = dispatcher();
    let (command, read) = get(1, "a", 0);
    let Action::Read(_) = present(&mut d, 0, 1, &read) else {
        panic!("not sent to the leader");
    };
    let Some(ReadResolution::Ordered(Action::FanOut(_))) =
        d.on_read_answer(ms(2), r(0), refused(1))
    else {
        panic!("a refusal did not order the read");
    };
    assert!(d.collector().is_pending(&command));

    d.on_connection_closed(1, &hub());
    assert!(d.collector().is_pending(&command), "cancelling dropped it");

    match present(&mut d, 5, 2, &read) {
        Action::Pending { command: attached } => assert_eq!(attached, command),
        other => panic!("the retry was not attached to the ordered read: {other:?}"),
    }
    assert_eq!(d.reads_waiting(), 0);
}

/// The same, when the read was ordered because its leader did not
/// answer in time.
#[test]
fn a_retry_of_a_read_ordered_at_its_fallback_attaches_to_the_command() {
    let mut d = dispatcher();
    let (command, read) = get(1, "a", 0);
    let Action::Read(_) = present(&mut d, 0, 1, &read) else {
        panic!("not sent to the leader");
    };
    let ordered = d.expire_reads(ms(coord_collector::READ_FALLBACK_MILLIS));
    assert!(matches!(ordered.as_slice(), [Action::FanOut(_)]));
    d.on_connection_closed(1, &hub());

    match present(&mut d, 2_000, 2, &read) {
        Action::Pending { command: attached } => assert_eq!(attached, command),
        other => panic!("the retry was not attached to the ordered read: {other:?}"),
    }
}

/// A retry key whose read is waiting on its leader is bound to that
/// read: another payload under it is refused as a conflict, as it would
/// be under an ordered command, and the read keeps waiting.
#[test]
fn another_payload_under_a_waiting_reads_retry_key_is_a_conflict() {
    let mut d = dispatcher();
    let (command, read) = get(1, "a", 0);
    let Action::Read(_) = present(&mut d, 0, 1, &read) else {
        panic!("not sent to the leader");
    };

    // A write under the same key would have been ordered as a command of
    // its own.
    let (_, write) = put(1, 0);
    let Action::Respond(delivery) = present(&mut d, 1, 2, &write) else {
        panic!("a write under the read's retry key was not refused");
    };
    let OutcomeV1::Err { code, .. } = response(&delivery.frame).outcome else {
        panic!("not an error");
    };
    assert_eq!(code, codes::REQUEST_IDENTITY_CONFLICT);

    // A read of another key would have been sent to the leader again,
    // replacing the first one's caller.
    let (_, other) = get(1, "b", 0);
    let Action::Respond(delivery) = present(&mut d, 2, 3, &other) else {
        panic!("another read under the read's retry key was not refused");
    };
    let OutcomeV1::Err { code, .. } = response(&delivery.frame).outcome else {
        panic!("not an error");
    };
    assert_eq!(code, codes::REQUEST_IDENTITY_CONFLICT);

    // The first read still waits, for its first caller.
    assert_eq!(d.reads_waiting(), 1);
    assert!(
        !d.collector().is_bound(&retry_key(1)),
        "a conflicting payload was bound"
    );
    assert!(!d.collector().is_pending(&command));
    let Some(ReadResolution::Answer(Delivery { connection, .. })) = d.on_read_answer(
        ms(3),
        r(0),
        answer(
            1,
            ReadOutcomeV1::Served {
                response: Vec::new(),
            },
        ),
    ) else {
        panic!("not answered");
    };
    assert_eq!(connection, 1);
}

/// A client deadline shorter than the fallback is the read's deadline:
/// at it, the caller is told the outcome is pending, as an ordered
/// command's caller is. Nothing was ordered, so nothing is left behind.
#[test]
fn a_client_deadline_before_the_fallback_answers_pending() {
    let mut d = dispatcher();
    let (command, read) = get(1, "a", 500);
    let Action::Read(_) = present(&mut d, 0, 1, &read) else {
        panic!("not sent to the leader");
    };
    assert_eq!(d.next_read_deadline(), Some(ms(500)));
    assert!(d.expire_reads(ms(499)).is_empty());

    let expired = d.expire_reads(ms(500));
    let [Action::Respond(delivery)] = expired.as_slice() else {
        panic!("not answered at its deadline: {expired:?}");
    };
    assert_eq!(delivery.connection, 1);
    let response = response(&delivery.frame);
    assert_eq!(response.command_id, command);
    assert_eq!(response.outcome, OutcomeV1::Pending);
    assert_eq!(d.reads_waiting(), 0);
    assert!(!d.collector().is_pending(&command));
    assert_eq!(d.next_read_deadline(), None);
}

/// A read ordered at its fallback keeps the deadline it was presented
/// with, measured from its presentation: 10 s from 0 ms is 10 s, not
/// 10 s from the fallback.
#[test]
fn a_read_ordered_at_its_fallback_keeps_its_presented_deadline() {
    let mut d = dispatcher();
    let (command, read) = get(1, "a", 10_000);
    let Action::Read(_) = present(&mut d, 0, 1, &read) else {
        panic!("not sent to the leader");
    };
    assert_eq!(
        d.next_read_deadline(),
        Some(ms(coord_collector::READ_FALLBACK_MILLIS))
    );
    let ordered = d.expire_reads(ms(coord_collector::READ_FALLBACK_MILLIS));
    assert!(matches!(ordered.as_slice(), [Action::FanOut(_)]));
    assert!(d.collector().is_pending(&command));

    assert!(d.expire(ms(9_999)).is_empty());
    let expired = d.expire(ms(10_000));
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].connection, 1);
    assert_eq!(response(&expired[0].frame).outcome, OutcomeV1::Pending);
}

/// A retry of a waiting read re-attaches its caller under the deadline
/// the retry presented, from the retry's own presentation.
#[test]
fn a_retry_of_a_waiting_read_restarts_its_deadline() {
    let mut d = dispatcher();
    let (command, read) = get(1, "a", 500);
    let Action::Read(_) = present(&mut d, 0, 1, &read) else {
        panic!("not sent to the leader");
    };
    match present(&mut d, 400, 2, &read) {
        Action::Pending { command: attached } => assert_eq!(attached, command),
        other => panic!("the retry was not attached to the waiting read: {other:?}"),
    }
    assert!(d.expire_reads(ms(500)).is_empty());
    let expired = d.expire_reads(ms(900));
    let [Action::Respond(delivery)] = expired.as_slice() else {
        panic!("not answered at the retry's deadline: {expired:?}");
    };
    assert_eq!(delivery.connection, 2);
}

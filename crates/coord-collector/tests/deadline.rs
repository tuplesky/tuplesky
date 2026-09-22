//! Request deadlines on the collector boundary's own clock (task-c03).
//!
//! `ClockHealth::now` is Unix seconds and authenticates tokens; a
//! request's `deadline_ms` is milliseconds of local time. Until task-c03
//! the two were one `u64` and a nominal 1,500 ms deadline was 1,500
//! seconds. These tests hold that a deadline is measured on the
//! monotonic reading the caller supplies ([`MonotonicMillis`]), at
//! millisecond resolution, and that the wall clock the admission gate
//! stamps a receipt with has no say in it.

use std::collections::BTreeSet;

use coord_collector::{
    Action, Admission, AdmissionLimits, Caller, Collector, CollectorConfig, Dispatcher,
    MonotonicMillis,
};
use coord_consensus::BallotConfiguration;
use coord_core::capability::{AdmissionReceipt, AttestedAdmission, VerifierToken};
use coord_core::event::AdmittedRequest;
use coord_storage::WatchHub;
use coord_types::identity::Digest32;
use coord_types::ids::{
    Ballot, ClientInstanceId, ClusterId, ConfigurationEpoch, DomainId, KvRevision, NamespaceId,
    ReplicaId, RequestSequence, SessionId,
};
use coord_types::logical_v1::{CanonicalOperation, LogicalRequest, PutOp};
use coord_types::wire_v1::{Frame, FrameReader, MessageV1, PeerRole, RequestV1, decode_stream};
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

fn dispatcher() -> Dispatcher {
    Dispatcher::new(
        Admission::new(CLUSTER, DOMAIN, AdmissionLimits::default()),
        collector(),
        16,
    )
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

fn request(seq: u64, deadline_ms: u32) -> (CommandId, RequestV1) {
    let mut logical = LogicalRequest::new(
        NS,
        CanonicalOperation::Put(PutOp {
            key: format!("k{seq}").into_bytes(),
            value: b"v".to_vec(),
            lease: None,
            prev_kv: false,
        }),
    );
    logical.canonicalize();
    let key = retry_key(seq);
    let command = CommandId::derive(&key, &logical).unwrap();
    (
        command,
        RequestV1::new(key, &logical, deadline_ms, 0).unwrap(),
    )
}

/// The request as the admission gate hands it to the collector.
fn admitted(seq: u64, deadline_ms: u32) -> (CommandId, AdmittedRequest) {
    let (command, request) = request(seq, deadline_ms);
    let admitted = AdmittedRequest {
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
        frame: MessageV1::Request(request).encode().unwrap(),
    };
    (command, admitted)
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

/// A 1,500 ms deadline expires at 1,500 ms of local time, and not one
/// millisecond before. This is the case that was 1,500 *seconds*.
#[test]
fn a_1500_ms_deadline_expires_at_1500_ms_and_not_before() {
    let mut c = collector();
    let (command, request) = admitted(1, 1_500);
    c.submit(ms(0), &request).expect("admitted");

    assert!(
        c.expire(ms(1_499)).is_empty(),
        "expired before its deadline"
    );
    let expired = c.expire(ms(1_500));
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].command, command);
    assert_eq!(expired[0].retry_key, retry_key(1));
    // Reported once; still pending, resolvable by identity.
    assert!(c.expire(ms(10_000)).is_empty());
    assert!(c.is_pending(&command));
}

/// A deadline is measured from when the request was presented, not from
/// the origin.
#[test]
fn a_deadline_is_measured_from_the_presentation() {
    let mut c = collector();
    let (_, request) = admitted(1, 1_500);
    c.submit(ms(40_000), &request).expect("admitted");

    assert!(c.expire(ms(41_499)).is_empty());
    assert_eq!(c.expire(ms(41_500)).len(), 1);
}

/// Subsecond deadlines are deadlines: a 250 ms one passes at 250 ms.
/// On a seconds clock it would have been the whole of the next four
/// minutes.
#[test]
fn a_subsecond_deadline_works_at_millisecond_resolution() {
    let mut c = collector();
    let (_, request) = admitted(1, 250);
    c.submit(ms(0), &request).expect("admitted");

    assert!(c.expire(ms(249)).is_empty());
    assert_eq!(c.expire(ms(250)).len(), 1);
}

/// A deadline of zero is no deadline, as before: the request never
/// expires on the collector's account.
#[test]
fn a_zero_deadline_never_expires() {
    let mut c = collector();
    let (command, request) = admitted(1, 0);
    c.submit(ms(0), &request).expect("admitted");

    assert!(c.expire(ms(u64::MAX)).is_empty());
    assert!(c.is_pending(&command));
}

/// A retry re-attaches the caller and restarts the deadline from the
/// retry's own presentation, on the same clock.
#[test]
fn a_retry_restarts_the_deadline_from_its_own_presentation() {
    let mut c = collector();
    let (_, request) = admitted(1, 1_000);
    c.submit(ms(0), &request).expect("admitted");
    // Presented again at 800 ms with the same deadline: due at 1,800.
    c.submit(ms(800), &request).expect("attached");

    assert!(
        c.expire(ms(1_000)).is_empty(),
        "the first presentation's deadline still counted"
    );
    assert!(c.expire(ms(1_799)).is_empty());
    assert_eq!(c.expire(ms(1_800)).len(), 1);
}

/// The boundary takes both clocks and keeps them apart. The wall clock
/// jumps a day between two frames with ten milliseconds of local time
/// passing: nothing expires, because a deadline is local time. And a
/// wall clock that stands still does not hold a deadline open: local
/// time passing is what expires it.
#[test]
fn a_wall_clock_jump_with_no_monotonic_progress_expires_nothing() {
    let mut d = dispatcher();
    let hub = hub();
    let (c1, first) = request(1, 1_500);
    let Action::FanOut(_) = d.on_frame(WALL, ms(0), 1, &caller(), &frame_of(&first), &hub) else {
        panic!("not fanned out");
    };

    // A day later by the wall, ten milliseconds later by the loop: a
    // second request is admitted (admission takes the wall clock), and
    // the first is nowhere near its deadline.
    let (c2, second) = request(2, 1_500);
    let Action::FanOut(_) = d.on_frame(
        WALL + 86_400,
        ms(10),
        1,
        &caller(),
        &frame_of(&second),
        &hub,
    ) else {
        panic!("not fanned out");
    };
    assert!(
        d.expire(ms(10)).is_empty(),
        "a wall-clock step expired a request whose local time had not run"
    );
    assert!(d.collector().is_pending(&c1));
    assert!(d.collector().is_pending(&c2));

    // Local time runs while the wall clock, for all the collector
    // knows, has not moved at all: the first expires at its 1,500 ms,
    // the second at its own.
    let expired = d.expire(ms(1_500));
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].retry_key, retry_key(1));
    let response = match decode_stream(&expired[0].frame).unwrap().as_slice() {
        [MessageV1::Response(r)] => r.clone(),
        other => panic!("not a response: {other:?}"),
    };
    assert_eq!(response.command_id, c1);
    assert_eq!(d.expire(ms(1_509)).len(), 0);
    assert_eq!(d.expire(ms(1_510)).len(), 1);
    // Expiration reports and retains: the commands stay pending.
    assert!(d.collector().is_pending(&c1));
    assert!(d.collector().is_pending(&c2));
}

/// A repeat's schedule and a deadline share the one time base, so a
/// destination's next offer and a caller's deadline are compared
/// against the same reading -- there is no second clock to disagree.
#[test]
fn repeats_and_deadlines_share_one_time_base() {
    let mut c = collector();
    let (command, request) = admitted(1, 30);
    let coord_collector::Submitted::FanOut(plan) = c.submit(ms(0), &request).expect("admitted")
    else {
        panic!("not fanned out");
    };
    c.offered(
        ms(0),
        &coord_collector::Offered {
            command,
            outcomes: plan
                .targets
                .iter()
                .map(|t| (*t, coord_collector::OfferOutcome::Saturated))
                .collect(),
        },
    );
    // The first repeat waits the 25 ms floor; the deadline is 30 ms.
    assert!(c.due_offers(ms(24), 16).is_empty());
    assert!(c.expire(ms(24)).is_empty());
    assert_eq!(c.due_offers(ms(25), 16).len(), 1);
    assert!(c.expire(ms(29)).is_empty());
    assert_eq!(c.expire(ms(30)).len(), 1);
    // The obligation to offer outlives the caller's deadline.
    assert!(c.is_pending(&command));
}

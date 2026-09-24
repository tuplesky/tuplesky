//! Acceptance for the serving decision (task-43).
//!
//! Every property here is about one stream and whether it stays open.
//! They are worth stating because each way of getting it wrong is silent:
//! the caller sees an empty answer, or waits out a deadline, or keeps a
//! connection it should have lost. None of those is a type error.

use coord_collector::{Action, Delivery, FanOut};
use coord_daemon::serve::{Step, step};
use coord_session::Ingress;
use coord_session::binding::BindError;
use coord_storage::watch::{Registration, WatchId};
use coord_transport::CloseCode;
use coord_types::identity::Digest32;
use coord_types::ids::{
    ClientInstanceId, ClusterId, DomainId, KvRevision, RequestSequence, SessionId,
};
use coord_types::{CommandId, RetryKey};

fn key(sequence: u64) -> RetryKey {
    RetryKey {
        cluster_id: ClusterId([1; 16]),
        domain_id: DomainId([2; 16]),
        session_id: SessionId([3; 16]),
        client_instance_id: ClientInstanceId([4; 16]),
        request_sequence: RequestSequence::new(sequence).unwrap(),
    }
}

fn plan(sequence: u64) -> FanOut {
    FanOut {
        command: CommandId(Digest32([0xc; 32])),
        retry_key: key(sequence),
        targets: Vec::new(),
        frame: b"submit".to_vec(),
    }
}

/// A binding is answered on its own stream and that stream ends: the
/// acknowledgement is the whole exchange, and the connection's later work
/// opens its own streams. Holding it would leave one stream per
/// connection open for nothing.
#[test]
fn a_binding_is_acknowledged_and_its_stream_ends() {
    let decided = step(Ingress::Bound(b"bind-ack".to_vec()), None);
    assert_eq!(decided, Step::Answer(b"bind-ack".to_vec()));
    assert!(!decided.keeps_the_stream());
    assert_eq!(decided.held(), None);
}

/// A connection with no session, an ended one, or one whose binding was
/// refused stops being served. Ignoring the frame is not the same as
/// closing: the same frame can simply be sent again.
///
/// All three close as `Rejected` and say no more. Whether a token was
/// wrong, absent or merely late is not something an unbound caller is
/// entitled to distinguish, and the reason crosses the wire.
#[test]
fn a_connection_without_a_live_binding_stops_being_served() {
    for (ingress, reason) in [
        (Ingress::NotBound, "not bound"),
        (Ingress::Expired, "binding expired"),
        (
            Ingress::Rejected(BindError::SessionMismatch {
                bound: SessionId([9; 16]),
            }),
            "bind refused",
        ),
    ] {
        let decided = step(ingress, Some(key(1)));
        assert_eq!(
            decided,
            Step::Close {
                code: CloseCode::Rejected,
                reason,
            }
        );
        // Even where the frame named an invocation, nothing is held: an
        // unbound connection's stream is not a stream to answer later.
        assert!(!decided.keeps_the_stream());
        assert_eq!(decided.held(), None);
    }
}

/// A result that already exists is written and the stream ends. A refusal
/// is a result like any other: it is the answer, not a reason to hold.
#[test]
fn an_established_result_is_written_and_the_stream_ends() {
    let decided = step(
        Ingress::Action(Action::Respond(Delivery {
            connection: 7,
            retry_key: key(1),
            frame: b"response".to_vec(),
        })),
        Some(key(1)),
    );
    assert_eq!(decided, Step::Answer(b"response".to_vec()));
    assert!(!decided.keeps_the_stream());
}

/// A submission holds its stream under the plan's own invocation, not
/// under whatever the frame appeared to name. The plan is what the
/// collector admitted, and its retry key is what a later delivery will be
/// addressed to.
#[test]
fn a_submission_holds_its_stream_under_the_plans_own_invocation() {
    let decided = step(Ingress::Action(Action::FanOut(plan(5))), Some(key(99)));
    let Step::Submit(sent) = &decided else {
        panic!("{decided:?}");
    };
    assert_eq!(sent.retry_key, key(5));
    assert!(decided.keeps_the_stream());
    assert_eq!(
        decided.held(),
        Some(key(5)),
        "held under the plan's invocation, not the frame's"
    );
}

/// A request that is already running holds its stream under the
/// invocation the frame named. A held stream and a submitted one are held
/// the same way: the difference is only whether the frame still has to
/// reach the voters.
#[test]
fn a_request_already_running_holds_its_stream() {
    let decided = step(
        Ingress::Action(Action::Pending {
            command: CommandId(Digest32([0xc; 32])),
        }),
        Some(key(3)),
    );
    assert_eq!(decided, Step::Hold(key(3)));
    assert!(decided.keeps_the_stream());
    assert_eq!(decided.held(), Some(key(3)));
}

/// A pending request whose invocation cannot be named is refused rather
/// than held.
///
/// The dispatcher reports a pending request by its command, and a command
/// is derived from the request: it is not what a delivery is addressed
/// to. Holding such a stream under a guessed key would leave a caller
/// waiting for an answer that can never be matched to it, and -- worse --
/// under a key some other caller could hold.
#[test]
fn a_pending_request_that_names_no_invocation_is_refused_not_held() {
    let decided = step(
        Ingress::Action(Action::Pending {
            command: CommandId(Digest32([0xc; 32])),
        }),
        None,
    );
    assert_eq!(
        decided,
        Step::Close {
            code: CloseCode::Protocol,
            reason: "pending without an invocation",
        }
    );
    assert!(!decided.keeps_the_stream());
}

/// A watch keeps its stream, because a watch is not a request with a long
/// answer: the frontend writes events, progress and finally a close onto
/// that one stream over the life of the subscription.
#[test]
fn a_watch_keeps_the_stream_it_was_opened_on() {
    let registration = Registration {
        id: WatchId(11),
        replay: Some((KvRevision::new(4).unwrap(), KvRevision::new(9).unwrap())),
    };
    let decided = step(
        Ingress::Action(Action::WatchOpened {
            connection: 7,
            watch_id: 42,
            registration: registration.clone(),
        }),
        None,
    );
    assert_eq!(
        decided,
        Step::Watch {
            watch_id: 42,
            registration,
        }
    );
    assert!(decided.keeps_the_stream());
    // A watch is not an invocation: nothing is held for it under a retry
    // key, and a unary delivery must never find it.
    assert_eq!(decided.held(), None);
}

/// A frame the connection may not send closes the connection, not just
/// the stream. Refusing one stream and leaving the connection open lets
/// the same frame be sent again on the next one.
#[test]
fn a_frame_that_may_not_be_sent_closes_the_connection() {
    let decided = step(
        Ingress::Action(Action::Violation {
            connection: 7,
            kind: 0x0999,
        }),
        Some(key(1)),
    );
    assert_eq!(
        decided,
        Step::Close {
            code: CloseCode::Protocol,
            reason: "frame not permitted",
        }
    );
    // The kind the peer sent is not echoed back: the reason crosses the
    // wire and is a fixed string.
    let Step::Close { reason, .. } = decided else {
        unreachable!()
    };
    assert!(!reason.contains("0999") && !reason.contains("2457"));
}

//! Acceptance for the held request streams (task-43).
//!
//! A response the collector establishes later has to reach the stream
//! that asked for it, exactly once, and no other. These are the
//! properties an event loop would otherwise get subtly wrong: answering
//! the wrong caller, answering twice, or dropping a stream on the floor
//! so its caller waits out a deadline for a result this process already
//! decided not to send.

use coord_daemon::pending::{Pending, Undeliverable};
use coord_types::RetryKey;
use coord_types::ids::{ClientInstanceId, ClusterId, DomainId, RequestSequence, SessionId};

fn key(session: u8, client: u8, sequence: u64) -> RetryKey {
    RetryKey {
        cluster_id: ClusterId([1; 16]),
        domain_id: DomainId([2; 16]),
        session_id: SessionId([session; 16]),
        client_instance_id: ClientInstanceId([client; 16]),
        request_sequence: RequestSequence::new(sequence).unwrap(),
    }
}

/// A stand-in for the means of writing one response. Dropping it without
/// answering is exactly what must not happen silently, so it is counted.
#[derive(Debug, PartialEq, Eq)]
struct Stream(&'static str);

/// A held stream is answered once, and the entry is gone afterwards.
#[test]
fn a_held_stream_is_answered_exactly_once() {
    let mut pending: Pending<Stream> = Pending::new();
    assert!(pending.is_empty());

    assert!(pending.hold(7, key(1, 1, 1), Stream("first")).is_none());
    assert_eq!(pending.len(), 1);

    assert_eq!(pending.take(7, &key(1, 1, 1)), Ok(Stream("first")));
    assert!(pending.is_empty(), "the entry is taken, not copied");

    // A second delivery for the same invocation finds nothing: it cannot
    // answer a stream that was already answered (or closed).
    assert_eq!(pending.take(7, &key(1, 1, 1)), Err(Undeliverable::Unknown));
}

/// A result goes to the connection that asked, never to whichever one
/// happens to name the key. A retry key is an invocation identity, not a
/// capability to read its result.
#[test]
fn a_result_is_never_written_to_a_connection_that_did_not_ask() {
    let mut pending: Pending<Stream> = Pending::new();
    pending.hold(7, key(1, 1, 1), Stream("alice"));

    assert_eq!(
        pending.take(9, &key(1, 1, 1)),
        Err(Undeliverable::OtherConnection { held_by: 7 }),
        "another connection may not collect this result"
    );
    // And the refusal did not consume it: the caller that asked is still
    // waiting and is still answerable.
    assert_eq!(pending.len(), 1);
    assert_eq!(pending.take(7, &key(1, 1, 1)), Ok(Stream("alice")));
}

/// A retry of the same invocation on a new connection is answered on the
/// new stream, and the stream it displaced is handed back rather than
/// dropped, so its caller can be closed instead of left hanging.
#[test]
fn a_retry_on_a_new_connection_displaces_the_old_stream_without_losing_it() {
    let mut pending: Pending<Stream> = Pending::new();
    pending.hold(7, key(1, 1, 1), Stream("first attempt"));

    let displaced = pending.hold(9, key(1, 1, 1), Stream("retry"));
    assert_eq!(
        displaced,
        Some(Stream("first attempt")),
        "the displaced stream comes back to be closed"
    );
    assert_eq!(pending.len(), 1, "one invocation, one waiting stream");

    // The answer goes to the connection that retried.
    assert_eq!(
        pending.take(7, &key(1, 1, 1)),
        Err(Undeliverable::OtherConnection { held_by: 9 })
    );
    assert_eq!(pending.take(9, &key(1, 1, 1)), Ok(Stream("retry")));

    // The old connection is forgotten with it: closing it releases
    // nothing, rather than releasing a stream that moved on.
    assert!(pending.close(7).is_empty());
}

/// Closing a connection releases every stream it held, and forgets them,
/// so a delivery that arrives afterwards is unknown rather than a write
/// to a stream that is already gone.
#[test]
fn closing_a_connection_releases_its_streams_and_forgets_them() {
    let mut pending: Pending<Stream> = Pending::new();
    pending.hold(7, key(1, 1, 1), Stream("a"));
    pending.hold(7, key(1, 1, 2), Stream("b"));
    pending.hold(9, key(2, 1, 1), Stream("other connection"));

    let mut released = pending.close(7);
    released.sort_by_key(|s| s.0);
    assert_eq!(released, vec![Stream("a"), Stream("b")]);

    assert_eq!(pending.take(7, &key(1, 1, 1)), Err(Undeliverable::Unknown));
    assert_eq!(pending.take(7, &key(1, 1, 2)), Err(Undeliverable::Unknown));

    // Another connection's stream is untouched.
    assert_eq!(pending.len(), 1);
    assert_eq!(
        pending.take(9, &key(2, 1, 1)),
        Ok(Stream("other connection"))
    );

    // Closing a connection that holds nothing is not an error.
    assert!(pending.close(7).is_empty());
    assert!(pending.close(1234).is_empty());
}

/// The per-connection index does not accumulate. Every path that removes
/// the last key of a connection removes the connection with it, so a
/// process that serves many short connections does not grow a map of
/// empty sets.
#[test]
fn a_connection_is_forgotten_once_it_holds_nothing() {
    let mut pending: Pending<Stream> = Pending::new();
    for round in 1..=50u64 {
        pending.hold(round, key(1, 1, round), Stream("x"));
        assert!(pending.take(round, &key(1, 1, round)).is_ok());
    }
    assert!(pending.is_empty());
    // Nothing is left behind for any of them.
    for round in 1..=50u64 {
        assert!(pending.close(round).is_empty(), "connection {round}");
    }
    assert_eq!(
        format!("{pending:?}"),
        "Pending { waiting: 0, connections: 0 }"
    );
}

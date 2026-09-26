//! The bounded, rotating look at the collector's half-held commands
//! (task-c02).
//!
//! The bound is on one turn's reads. What these tests hold is that it is
//! not also a bound on which commands are ever read: a command that can
//! never settle here does not keep the ones behind it from being looked
//! at, however many turns it stays pending.

use coord_collector::SettleError;
use coord_daemon::settle::{Settled, offer, window};
use coord_storage::codecs::RetryRecordV1;
use coord_types::CommandId;
use coord_types::identity::Digest32;
use coord_types::ids::ExecutionPosition;

#[test]
fn a_turn_takes_at_most_its_bound_and_the_next_turn_continues() {
    let half: Vec<u32> = (0..10).collect();
    let (first, cursor) = window(half.clone(), 0, 4);
    assert_eq!(first, [0, 1, 2, 3]);
    let (second, cursor) = window(half.clone(), cursor, 4);
    assert_eq!(second, [4, 5, 6, 7]);
    let (third, cursor) = window(half.clone(), cursor, 4);
    assert_eq!(third, [8, 9, 0, 1], "wraps rather than stopping short");
    let (fourth, _) = window(half, cursor, 4);
    assert_eq!(fourth, [2, 3, 4, 5]);
}

/// The case the rotation exists for: entries that never settle sit at
/// the front for ever, and everything behind them is still reached.
#[test]
fn what_sits_in_front_does_not_hide_what_is_behind() {
    // Twenty pending; the first sixteen never settle here.
    let half: Vec<u32> = (0..20).collect();
    let mut cursor = 0;
    let mut seen = std::collections::BTreeSet::new();
    for _ in 0..3 {
        let (this_turn, next) = window(half.clone(), cursor, 16);
        assert_eq!(this_turn.len(), 16);
        seen.extend(this_turn);
        cursor = next;
    }
    assert!(
        (16..20).all(|c| seen.contains(&c)),
        "the commands behind the stuck prefix were never looked at: {seen:?}"
    );
}

#[test]
fn a_list_shorter_than_the_bound_is_taken_whole_and_once() {
    let half: Vec<u32> = vec![7, 8, 9];
    let (taken, cursor) = window(half.clone(), 5, 16);
    assert_eq!(taken.len(), 3, "nothing is looked at twice in one turn");
    assert_eq!(
        taken
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        3
    );
    let (again, _) = window(half, cursor, 16);
    assert_eq!(again.len(), 3);
}

#[test]
fn nothing_half_held_or_no_budget_is_nothing() {
    assert_eq!(window(Vec::<u32>::new(), 9, 16), (Vec::new(), 0));
    assert_eq!(window(vec![1u32, 2], 0, 0), (Vec::new(), 0));
}

fn record(n: u8) -> (CommandId, RetryRecordV1) {
    let command = CommandId(Digest32([n; 32]));
    (
        command,
        RetryRecordV1 {
            command_id: command,
            position: ExecutionPosition::new(u64::from(n)).unwrap(),
            revision: None,
            response: vec![n],
            result_digest: Digest32([n; 32]),
        },
    )
}

/// What the collector passes over is passed over; what it settles goes
/// out, and is counted whether or not it has a delivery.
#[test]
fn a_turn_without_a_mismatch_sends_what_settled() {
    let turn = offer((1..=4).map(record), |c, _| match c.as_bytes()[0] {
        1 => Ok(Some(1)),
        2 => Err(SettleError::Uncorroborated),
        3 => Ok(None),
        _ => Err(SettleError::NotPending),
    });
    assert_eq!(
        turn,
        Settled::Offered {
            settled: 2,
            deliveries: vec![1],
        }
    );
}

/// A mismatch ends the turn: what was settled before it does not go out,
/// and nothing after it is offered (task-d06).
#[test]
fn a_mismatch_ends_the_turn_and_sends_nothing() {
    let mut offered = Vec::new();
    let turn = offer((1..=3).map(record), |c, _| {
        offered.push(c.as_bytes()[0]);
        match c.as_bytes()[0] {
            2 => Err(SettleError::Mismatch),
            n => Ok(Some(n)),
        }
    });
    assert_eq!(turn, Settled::Diverged(record(2).0));
    assert_eq!(offered, vec![1, 2]);
}

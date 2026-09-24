//! The bounded, rotating look at the collector's half-held commands
//! (task-c02).
//!
//! The bound is on one turn's reads. What these tests hold is that it is
//! not also a bound on which commands are ever read: a command that can
//! never settle here does not keep the ones behind it from being looked
//! at, however many turns it stays pending.

use coord_daemon::settle::window;

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

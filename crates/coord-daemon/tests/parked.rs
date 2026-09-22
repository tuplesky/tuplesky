//! The bounds on a voter's held evidence, with time as an input
//! (task-c02).
//!
//! Both bounds are performance controls: they decide how long the
//! ordinary race is served from the hold rather than from a repair, and
//! how much a loaded voter carries. Neither is a place where a caller's
//! evidence is lost for good, so what these tests hold is that the bounds
//! do exactly what they say -- the hold expires at the hold and not
//! before, the depth crowds out the oldest and only the oldest -- and
//! that a submission arriving inside them routes what was held to the
//! collector that asked.

use std::time::{Duration, Instant};

use coord_core::event::PeerProvenance;
use coord_daemon::parked::Parked;
use coord_daemon::voter::Origin;
use coord_types::CommandId;
use coord_types::identity::Digest32;
use coord_types::ids::{ReplicaId, ReplicaIncarnation};

const HOLD: Duration = Duration::from_millis(1_000);

fn command(n: u8) -> CommandId {
    CommandId(Digest32([n; 32]))
}

fn provenance() -> PeerProvenance {
    PeerProvenance::from_local_voter(ReplicaId([1; 16]), ReplicaIncarnation::new(1).unwrap())
}

fn known(command: &CommandId) -> Option<Origin> {
    Some(Origin::Connection(u64::from(command.0.0[0])))
}

fn unknown(_: &CommandId) -> Option<Origin> {
    None
}

/// Evidence whose submitter arrives inside the hold goes to that
/// submitter, in the order it was parked.
#[test]
fn held_evidence_is_routed_to_the_submitter_once_it_is_known() {
    let t0 = Instant::now();
    let mut parked = Parked::new(HOLD, 8);
    parked.park(command(1), provenance(), b"one".to_vec(), t0);
    parked.park(command(2), provenance(), b"two".to_vec(), t0);

    // Nobody has submitted yet: everything waits.
    let routed = parked.route(t0 + HOLD / 2, unknown);
    assert!(routed.ready.is_empty());
    assert_eq!(routed.unclaimed, 0);
    assert_eq!(parked.len(), 2);

    // The submission for command 2 arrives; command 1 keeps waiting.
    let routed = parked.route(t0 + HOLD / 2, |c| {
        (*c == command(2)).then(|| known(c)).flatten()
    });
    assert_eq!(routed.ready.len(), 1);
    assert_eq!(routed.ready[0].0, Origin::Connection(2));
    assert_eq!(routed.ready[0].2, b"two");
    assert_eq!(routed.unclaimed, 0);
    assert_eq!(parked.commands().copied().collect::<Vec<_>>(), [command(1)]);
}

/// The hold expires at the hold, not before: a frame parked at `t0` is
/// still there one tick short of it and gone at it.
#[test]
fn the_hold_expires_at_the_hold_and_not_before() {
    let t0 = Instant::now();
    let mut parked = Parked::new(HOLD, 8);
    parked.park(command(1), provenance(), b"one".to_vec(), t0);

    let routed = parked.route(t0 + HOLD - Duration::from_millis(1), unknown);
    assert_eq!(routed.unclaimed, 0, "let go before the hold had run");
    assert_eq!(parked.len(), 1);

    let routed = parked.route(t0 + HOLD, unknown);
    assert_eq!(routed.unclaimed, 1);
    assert!(routed.ready.is_empty());
    assert!(parked.is_empty());
}

/// A frame whose submitter arrives at the very instant the hold expires
/// is routed, not let go: knowing where it belongs wins.
#[test]
fn a_submitter_known_at_expiry_still_wins() {
    let t0 = Instant::now();
    let mut parked = Parked::new(HOLD, 8);
    parked.park(command(1), provenance(), b"one".to_vec(), t0);

    let routed = parked.route(t0 + HOLD, known);
    assert_eq!(routed.ready.len(), 1);
    assert_eq!(routed.unclaimed, 0);
}

/// Past the depth the oldest goes, and only the oldest -- and it goes
/// with no time having passed at all, which is why the depth is a bound
/// of its own and not the hold seen from another side.
#[test]
fn the_depth_crowds_out_the_oldest_before_the_hold_expires() {
    let t0 = Instant::now();
    let mut parked = Parked::new(HOLD, 3);
    for n in 1..=3 {
        assert_eq!(parked.park(command(n), provenance(), vec![n], t0), 0);
    }
    assert_eq!(
        parked.park(command(4), provenance(), vec![4], t0),
        1,
        "the fourth frame crowded one out"
    );
    assert_eq!(
        parked.commands().copied().collect::<Vec<_>>(),
        [command(2), command(3), command(4)],
        "the oldest went, the rest kept their order"
    );

    // Nothing has expired: what was crowded out is not counted as
    // unclaimed, and what remains is still waiting.
    let routed = parked.route(t0, unknown);
    assert_eq!(routed.unclaimed, 0);
    assert_eq!(parked.len(), 3);

    // The submission for the crowded-out command finds nothing here:
    // this is the case the voter's own repair exists for.
    let routed = parked.route(t0, |c| (*c == command(1)).then(|| known(c)).flatten());
    assert!(routed.ready.is_empty());
}

/// Every frame let go is counted exactly once, whichever bound let it
/// go, so the two counters an operator reads add up to what was held.
#[test]
fn what_is_let_go_is_counted_once_under_the_bound_that_let_it_go() {
    let t0 = Instant::now();
    let mut parked = Parked::new(HOLD, 2);
    let mut crowded_out = 0;
    for n in 1..=5 {
        crowded_out += parked.park(command(n), provenance(), vec![n], t0);
    }
    assert_eq!(crowded_out, 3);
    assert_eq!(parked.len(), 2);

    let routed = parked.route(t0 + HOLD, unknown);
    assert_eq!(routed.unclaimed, 2);
    assert_eq!(crowded_out + routed.unclaimed, 5);
    assert!(parked.is_empty());
}

/// What is held says when it next has something to let go of on its
/// own: the oldest frame's hold, which moves on once that frame goes.
///
/// The runtime wakes on it; without the wake, a voter nobody submits to
/// runs no turn and the hold is never applied.
#[test]
fn the_next_expiry_is_the_oldest_hold_and_moves_on_when_it_goes() {
    let t0 = Instant::now();
    let mut parked = Parked::new(HOLD, 8);
    assert_eq!(parked.next_expiry(), None);
    parked.park(command(1), provenance(), b"one".to_vec(), t0);
    parked.park(command(2), provenance(), b"two".to_vec(), t0 + HOLD / 4);
    assert_eq!(parked.next_expiry(), Some(t0 + HOLD));

    assert_eq!(parked.route(t0 + HOLD, unknown).unclaimed, 1);
    assert_eq!(parked.next_expiry(), Some(t0 + HOLD / 4 + HOLD));

    // Routed rather than let go, it moves on just the same.
    let routed = parked.route(t0 + HOLD, known);
    assert_eq!(routed.ready.len(), 1);
    assert_eq!(parked.next_expiry(), None);
}

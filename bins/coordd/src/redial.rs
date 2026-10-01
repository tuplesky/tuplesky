//! When to dial a voter again (task-d03).
//!
//! Both planes are dialled when the serving loop starts, and a connection
//! does not last for ever: the transport ends every one at its age cap
//! (twelve hours by default) or when it has been idle, and a voter that
//! stops takes its links with it. Dialled once, a mesh heals only when
//! nodes are restarted -- a dropped link stays down for the survivors, a
//! restarted voter is re-dialled by nobody, and half a day after start
//! the whole domain goes quiet.
//!
//! This is the schedule that keeps dialling, one entry per voter of one
//! plane. It decides and does nothing: the serving loop tells it which
//! voters a connection is holding, starts the dials it says are due, and
//! reports how each one ended. Time is handed in, so the schedule is
//! tested without a clock.
//!
//! The rules are three. A voter whose link is held costs nothing. A voter
//! whose link was just lost is dialled again soon, at the floor. A voter
//! that stays unreachable is dialled at a rate that halves with every
//! failure down to one attempt per ceiling, and never stops: a voter
//! permanently absent costs a bounded rate, and one that comes back is
//! found. Every wait is jittered per voter, so the survivors of one event
//! do not all dial at the same instant, and a full mesh -- where both ends
//! of every pair dial and the transport closes one of the two
//! connections when they meet -- does not become a storm.

use std::time::{Duration, Instant};

/// The shortest wait before dialling a voter again: after a link is lost,
/// and after the first failure.
pub const FLOOR: Duration = Duration::from_millis(250);

/// The longest: what a voter that never comes back costs, one attempt
/// per this.
pub const CEILING: Duration = Duration::from_secs(10);

/// What happened to one voter's link since the schedule last looked.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Change {
    /// Nothing new.
    Same,
    /// The link is held again after it was not. Work that was waiting for
    /// this voter can go now rather than at its own next attempt.
    Returned,
    /// The link was held and is not any more.
    Lost,
}

/// One voter's entry.
#[derive(Clone, Debug)]
struct Slot {
    /// Whether a connection is holding this voter's link, as last seen.
    held: bool,
    /// Whether a dial is out for this voter now. Nothing else is started
    /// for it until that one ends.
    in_flight: bool,
    /// The wait before the next attempt, before jitter.
    backoff: Duration,
    /// When the next attempt is due; `None` while held or in flight.
    next: Option<Instant>,
    /// Attempts made, which with the salt picks the jitter.
    attempts: u64,
    /// Per-voter, so two voters' jitter differs.
    salt: u64,
}

/// The re-dial schedule for one plane's voters.
#[derive(Clone, Debug)]
pub struct Redial {
    slots: Vec<Slot>,
    floor: Duration,
    ceiling: Duration,
}

impl Redial {
    /// A schedule for voters with these salts, none of them held yet and
    /// none due until first observed.
    pub fn new(salts: impl IntoIterator<Item = u64>) -> Self {
        Self::with_bounds(salts, FLOOR, CEILING)
    }

    /// The same with other bounds (for tests).
    pub fn with_bounds(
        salts: impl IntoIterator<Item = u64>,
        floor: Duration,
        ceiling: Duration,
    ) -> Self {
        let ceiling = ceiling.max(floor);
        Redial {
            slots: salts
                .into_iter()
                .map(|salt| Slot {
                    held: false,
                    in_flight: false,
                    backoff: floor,
                    next: None,
                    attempts: 0,
                    salt,
                })
                .collect(),
            floor,
            ceiling,
        }
    }

    /// Make every voter due at `now`: the first dials, when the serving
    /// loop starts.
    pub fn start(&mut self, now: Instant) {
        for slot in &mut self.slots {
            if !slot.held && !slot.in_flight {
                slot.next = Some(now);
            }
        }
    }

    /// Record whether voter `i`'s link is held at `now`.
    ///
    /// A voter that is not held and has nothing scheduled is scheduled
    /// here, at its current backoff, so a voter can never be left
    /// unreachable with no attempt coming.
    pub fn observe(&mut self, i: usize, held: bool, now: Instant) -> Change {
        let (floor, ceiling) = (self.floor, self.ceiling);
        let Some(slot) = self.slots.get_mut(i) else {
            return Change::Same;
        };
        if held {
            let was = slot.held;
            slot.held = true;
            slot.backoff = floor;
            slot.next = None;
            return if was { Change::Same } else { Change::Returned };
        }
        if slot.held {
            // Lost: dial again soon, whatever the backoff was before the
            // link was last up.
            slot.held = false;
            slot.backoff = floor;
            if !slot.in_flight {
                slot.next = Some(now + jitter(floor, slot.salt, slot.attempts, ceiling));
            }
            return Change::Lost;
        }
        if slot.next.is_none() && !slot.in_flight {
            slot.next = Some(now + jitter(slot.backoff, slot.salt, slot.attempts, ceiling));
        }
        Change::Same
    }

    /// The voters due a dial at `now`, each marked in flight.
    pub fn due(&mut self, now: Instant) -> Vec<usize> {
        let mut out = Vec::new();
        for (i, slot) in self.slots.iter_mut().enumerate() {
            if slot.held || slot.in_flight {
                continue;
            }
            if slot.next.is_some_and(|at| at <= now) {
                slot.in_flight = true;
                slot.next = None;
                slot.attempts = slot.attempts.saturating_add(1);
                out.push(i);
            }
        }
        out
    }

    /// Voter `i`'s dial ended at `now`, having reached it or not.
    ///
    /// Reaching it resets the backoff; whether the link is actually held
    /// is still what [`Redial::observe`] is told next, because a dial that
    /// reached the voter may have lost the collision with the voter's own
    /// dial and been closed, and one that failed may have failed for that
    /// same reason while the voter's dial is holding the link.
    pub fn finished(&mut self, i: usize, reached: bool, now: Instant) {
        let (floor, ceiling) = (self.floor, self.ceiling);
        let Some(slot) = self.slots.get_mut(i) else {
            return;
        };
        slot.in_flight = false;
        slot.backoff = if reached {
            floor
        } else {
            slot.backoff.saturating_mul(2).min(ceiling)
        };
        if !slot.held {
            slot.next = Some(now + jitter(slot.backoff, slot.salt, slot.attempts, ceiling));
        }
    }

    /// When the next dial falls due, for the loop to wake on.
    pub fn next_deadline(&self) -> Option<Instant> {
        self.slots
            .iter()
            .filter(|s| !s.held && !s.in_flight)
            .filter_map(|s| s.next)
            .min()
    }

    /// Attempts made across every voter.
    #[cfg(test)]
    fn attempts(&self) -> u64 {
        self.slots.iter().map(|s| s.attempts).sum()
    }
}

/// `wait` scaled into `[3/4, 5/4)` of itself by a value drawn from the
/// voter's salt and attempt count, and kept at most `ceiling * 5/4`.
///
/// Deterministic, so a schedule can be tested, and different per voter
/// and per attempt, which is all jitter has to be.
fn jitter(wait: Duration, salt: u64, attempts: u64, ceiling: Duration) -> Duration {
    let draw = splitmix(salt ^ attempts.wrapping_mul(0x9e37_79b9_7f4a_7c15));
    // A fraction in [0, 1) with 16 bits of resolution.
    let fraction = (draw >> 48) as u32;
    let scaled = wait.saturating_mul(3) / 4 + wait.saturating_mul(fraction) / (2 * 65_536);
    scaled.min(ceiling.saturating_mul(5) / 4)
}

fn splitmix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

/// A salt for a voter, from its identity.
pub fn salt(replica: &[u8]) -> u64 {
    replica.iter().fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
        (h ^ u64::from(*b)).wrapping_mul(0x0100_0000_01b3)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const F: Duration = Duration::from_millis(100);
    const C: Duration = Duration::from_millis(1600);

    fn schedule(n: u64) -> Redial {
        Redial::with_bounds(0..n, F, C)
    }

    #[test]
    fn every_voter_is_due_at_the_start() {
        let t = Instant::now();
        let mut r = schedule(3);
        r.start(t);
        assert_eq!(r.next_deadline(), Some(t));
        assert_eq!(r.due(t), vec![0, 1, 2]);
    }

    #[test]
    fn a_held_voter_costs_nothing() {
        let t = Instant::now();
        let mut r = schedule(2);
        assert_eq!(r.observe(0, true, t), Change::Returned);
        assert_eq!(r.observe(1, true, t), Change::Returned);
        assert_eq!(r.next_deadline(), None);
        assert!(r.due(t + C * 100).is_empty());
        assert_eq!(r.attempts(), 0);
        assert_eq!(r.observe(0, true, t), Change::Same);
    }

    #[test]
    fn a_lost_link_is_dialled_again_at_the_floor() {
        let t = Instant::now();
        let mut r = schedule(1);
        r.observe(0, true, t);
        assert_eq!(r.observe(0, false, t), Change::Lost);
        let at = r.next_deadline().expect("scheduled");
        assert!(at >= t + F * 3 / 4 && at < t + F * 5 / 4, "{:?}", at - t);
        assert!(r.due(at - Duration::from_millis(1)).is_empty());
        assert_eq!(r.due(at), vec![0]);
        // In flight: nothing else is started for it, and nothing is due.
        assert!(r.due(at + C).is_empty());
        assert_eq!(r.next_deadline(), None);
        // Reached, and the link is held: the voter costs nothing again.
        r.finished(0, true, at);
        assert_eq!(r.observe(0, true, at), Change::Returned);
        assert_eq!(r.next_deadline(), None);
    }

    /// A voter that never comes back is dialled at a rate that falls to
    /// one attempt per ceiling and never to none.
    #[test]
    fn an_absent_voter_backs_off_to_the_ceiling_and_is_never_given_up() {
        let mut now = Instant::now();
        let mut r = schedule(1);
        r.observe(0, false, now);
        let mut waits = Vec::new();
        for _ in 0..20 {
            let at = r
                .next_deadline()
                .expect("an absent voter is always scheduled");
            waits.push(at - now);
            now = at;
            assert_eq!(r.due(now), vec![0]);
            r.finished(0, false, now);
            r.observe(0, false, now);
        }
        assert_eq!(r.attempts(), 20);
        // Growing, then bounded by the jittered ceiling.
        assert!(waits[1] > waits[0] && waits[3] > waits[1], "{waits:?}");
        for wait in &waits {
            assert!(*wait < C * 5 / 4 + Duration::from_millis(1), "{waits:?}");
        }
        for wait in &waits[8..] {
            assert!(
                *wait >= C * 3 / 4,
                "the rate did not settle at the ceiling: {waits:?}"
            );
        }
        // And when it does come back, it is found and costs nothing.
        assert_eq!(r.observe(0, true, now), Change::Returned);
        assert_eq!(r.next_deadline(), None);
    }

    /// A dial that lost the collision with the voter's own dial is not a
    /// reason to keep dialling: the link is held, and held is what counts.
    #[test]
    fn a_failed_dial_to_a_held_voter_schedules_nothing() {
        let t = Instant::now();
        let mut r = schedule(1);
        r.observe(0, false, t);
        let at = r.next_deadline().unwrap();
        assert_eq!(r.due(at), vec![0]);
        r.observe(0, true, at);
        r.finished(0, false, at);
        assert_eq!(r.next_deadline(), None);
    }

    /// Voters of one event are not all dialled at the same instant.
    #[test]
    fn two_voters_lost_together_are_dialled_apart() {
        let t = Instant::now();
        let mut r = Redial::with_bounds([salt(&[1; 16]), salt(&[2; 16])], F, C);
        r.observe(0, true, t);
        r.observe(1, true, t);
        r.observe(0, false, t);
        r.observe(1, false, t);
        let first = r.due(t + F * 5 / 4);
        assert_eq!(first.len(), 2);
        let mut r2 = Redial::with_bounds([salt(&[1; 16]), salt(&[2; 16])], F, C);
        r2.observe(0, false, t);
        r2.observe(1, false, t);
        let a = r2.slots[0].next.unwrap();
        let b = r2.slots[1].next.unwrap();
        assert_ne!(a, b, "two voters were scheduled for the same instant");
    }
}

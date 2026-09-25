//! When to renew this node's leaf, and when to stop serving on it
//! (task-d02; design Sections 10.4, 20.4).
//!
//! task-58 left the arithmetic -- [`RenewalPolicy::decide`] and its due
//! point -- reachable only through `coordd inspect`. This is the part of
//! the serving loop that acts on it: sleep until the leaf is due, start
//! one enrollment at a time once it is, back off while the issuer does
//! not answer, and stop serving when the leaf reaches its `notAfter`.
//! Like [`crate::redial`] it decides and does nothing; the loop starts
//! the enrollment it says to, and reports how each one ended. Time is
//! handed in, in unix milliseconds, so the schedule is tested without a
//! clock.
//!
//! Three rules are the ones that matter.
//!
//! * **The deadline is the leaf's, and it does not move.** Nothing here
//!   can extend it: an outage is survived by turning due at two thirds of
//!   the lifetime, and past `notAfter` the answer is [`Step::Expired`]
//!   however the attempts went -- including while one is still in flight,
//!   whose answer is then never used.
//! * **Retries are bounded, and never scheduled past the deadline.** A
//!   failure waits a jittered backoff that doubles up to a ceiling, so an
//!   issuer that is down costs a bounded attempt rate and a fleet that lost
//!   it together does not retry in step. A wait that would run past the
//!   deadline is cut to one last attempt a floor before it, and after that
//!   to the deadline itself, where the leaf expires.
//! * **Expired is final.** The loop ends when this says so. An issuer that
//!   answers afterwards is not asked, and a restart on the expired leaf is
//!   refused at startup, where the credential is verified before anything
//!   is opened; the node comes back only on a leaf renewed out of band.

use std::time::Duration;

use coord_node_issuer::{Leaf, Renewal, RenewalPolicy};

/// The first wait after a failed attempt.
pub const FLOOR: Duration = Duration::from_secs(1);

/// The longest wait between attempts while the issuer stays away: what an
/// outage costs the issuer, one attempt per this per node.
pub const CEILING: Duration = Duration::from_secs(300);

/// What the serving loop does about its leaf now.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Step {
    /// Nothing before this instant (unix milliseconds).
    Wait {
        /// When to look again.
        until_ms: u64,
    },
    /// Start one enrollment now. The schedule counts it in flight, and
    /// starts nothing else until [`Schedule::renewed`] or
    /// [`Schedule::failed`] says how it ended.
    Enroll,
    /// An enrollment is in flight; look again at the deadline whatever
    /// happens to it, since the leaf expires then either way.
    InFlight {
        /// The leaf's `notAfter` (unix milliseconds).
        until_ms: u64,
    },
    /// The leaf is no longer valid. Stop serving on it.
    Expired,
}

/// One node's renewal schedule.
#[derive(Clone, Debug)]
pub struct Schedule {
    policy: RenewalPolicy,
    /// Per-node, from the node's identity: the same due point on every
    /// start, and a different one from its neighbours'.
    seed: u64,
    /// The leaf being served on.
    leaf: Leaf,
    in_flight: bool,
    /// When the next attempt may start, after a failure.
    retry_at_ms: Option<u64>,
    /// The next failure's wait, before jitter.
    backoff: Duration,
    floor: Duration,
    ceiling: Duration,
    /// Enrollments started, failed, and leaves renewed (diagnostic).
    pub attempts: u64,
    /// Attempts that ended without a leaf put into service.
    pub failures: u64,
    /// Leaves put into service.
    pub renewals: u64,
}

impl Schedule {
    /// A schedule for `leaf` under `policy`, with this node's `seed`.
    pub fn new(policy: RenewalPolicy, leaf: Leaf, seed: u64) -> Self {
        Self::with_bounds(policy, leaf, seed, FLOOR, CEILING)
    }

    /// The same with other retry bounds (for tests).
    pub fn with_bounds(
        policy: RenewalPolicy,
        leaf: Leaf,
        seed: u64,
        floor: Duration,
        ceiling: Duration,
    ) -> Self {
        Schedule {
            policy,
            seed,
            leaf,
            in_flight: false,
            retry_at_ms: None,
            backoff: floor,
            floor,
            ceiling: ceiling.max(floor),
            attempts: 0,
            failures: 0,
            renewals: 0,
        }
    }

    /// The leaf being served on.
    pub const fn leaf(&self) -> Leaf {
        self.leaf
    }

    /// When the leaf turns due (unix seconds), for a report.
    pub const fn due_at(&self) -> u64 {
        bounded(self.policy, &self.leaf).due_at(&self.leaf, self.seed)
    }

    /// The policy's answer for the leaf at `now_ms`, for a report.
    pub const fn decide(&self, now_ms: u64) -> Renewal {
        bounded(self.policy, &self.leaf).decide(&self.leaf, now_ms / 1000, self.seed)
    }

    /// The leaf's `notAfter` in milliseconds.
    const fn deadline_ms(&self) -> u64 {
        self.leaf.expires_at.saturating_mul(1000)
    }

    /// What to do at `now_ms`. An [`Step::Enroll`] is counted as started.
    pub fn step(&mut self, now_ms: u64) -> Step {
        let step = self.peek(now_ms);
        if step == Step::Enroll {
            self.in_flight = true;
            self.retry_at_ms = None;
            self.attempts = self.attempts.saturating_add(1);
        }
        step
    }

    /// What [`Schedule::step`] would say at `now_ms`, changing nothing.
    pub fn peek(&self, now_ms: u64) -> Step {
        match self.decide(now_ms) {
            // First, and whatever is in flight: the deadline is the
            // leaf's, and an answer that arrives after it changes nothing.
            Renewal::Expired => Step::Expired,
            Renewal::Wait { until } => Step::Wait {
                until_ms: until.saturating_mul(1000),
            },
            Renewal::Due if self.in_flight => Step::InFlight {
                until_ms: self.deadline_ms(),
            },
            Renewal::Due => match self.retry_at_ms {
                Some(at) if at > now_ms => Step::Wait { until_ms: at },
                _ => Step::Enroll,
            },
        }
    }

    /// When the loop has to look again, from `now_ms`.
    pub fn wake_at(&self, now_ms: u64) -> u64 {
        match self.peek(now_ms) {
            Step::Wait { until_ms } | Step::InFlight { until_ms } => until_ms,
            Step::Enroll | Step::Expired => now_ms,
        }
    }

    /// The enrollment in flight ended at `now_ms` without a leaf put into
    /// service: the issuer did not answer, refused, or answered with a
    /// leaf this node would not present.
    pub fn failed(&mut self, now_ms: u64) {
        self.in_flight = false;
        self.failures = self.failures.saturating_add(1);
        let wait = crate::redial::jitter(self.backoff, self.seed, self.attempts, self.ceiling);
        self.backoff = self.backoff.saturating_mul(2).min(self.ceiling);
        let wait_ms = u64::try_from(wait.as_millis()).unwrap_or(u64::MAX);
        let candidate = now_ms.saturating_add(wait_ms);
        let deadline = self.deadline_ms();
        let floor_ms = u64::try_from(self.floor.as_millis()).unwrap_or(u64::MAX);
        let last = deadline.saturating_sub(floor_ms);
        // Never past the deadline. A wait that would cross it becomes one
        // last attempt a floor before it; once that is behind, the next
        // look is the deadline, where the leaf expires.
        self.retry_at_ms = Some(if candidate < deadline {
            candidate
        } else if last > now_ms {
            last
        } else {
            deadline
        });
    }

    /// The enrollment in flight put `leaf` into service. The next due
    /// point is the new leaf's, and a failure after it starts again from
    /// the floor.
    pub fn renewed(&mut self, leaf: Leaf) {
        self.in_flight = false;
        self.retry_at_ms = None;
        self.backoff = self.floor;
        self.renewals = self.renewals.saturating_add(1);
        self.leaf = leaf;
    }
}

/// This node's jitter seed: the first eight bytes of its identity, as
/// `coordd inspect` has always derived it, so the report and the running
/// node name the same due point.
pub fn seed(node: &coord_types::ids::ReplicaId) -> u64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&node.0[..8]);
    u64::from_be_bytes(bytes)
}

/// `policy` for `leaf`, with its jitter kept inside the first half of
/// the window between the leaf's due point and its `notAfter`.
///
/// The jitter is a spread over a fleet, in seconds, and a leaf's
/// lifetime is whatever the issuer granted. Unbounded, an hour of
/// jitter on an hour-long leaf -- due at forty minutes -- pushes most
/// nodes' due point past the expiry, where the policy clamps it to a
/// second before it: one attempt, shorter than a request's own timeout,
/// and then the node stops. Half the remaining window keeps the spread
/// and leaves the other half for retries.
pub const fn bounded(policy: RenewalPolicy, leaf: &Leaf) -> RenewalPolicy {
    let lifetime = leaf.lifetime();
    let den = if policy.renew_at_den == 0 {
        1
    } else {
        policy.renew_at_den
    };
    let spent = lifetime / den * policy.renew_at_num;
    let room = lifetime.saturating_sub(spent) / 2;
    RenewalPolicy {
        jitter_secs: if policy.jitter_secs < room {
            policy.jitter_secs
        } else {
            room
        },
        ..policy
    }
}

/// The renewal policy a configuration asks for: the default arithmetic,
/// with the configured spread where there is one.
pub fn policy(config: Option<&coord_daemon::RenewalConfig>) -> RenewalPolicy {
    let mut policy = RenewalPolicy::default();
    if let Some(config) = config {
        policy.jitter_secs = config.jitter_secs;
    }
    policy
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A thirty-second leaf, due at twenty seconds with no spread.
    const LEAF: Leaf = Leaf {
        issued_at: 1_000,
        expires_at: 1_030,
    };
    const F: Duration = Duration::from_secs(1);
    const C: Duration = Duration::from_secs(8);

    fn schedule() -> Schedule {
        let policy = RenewalPolicy {
            jitter_secs: 0,
            ..RenewalPolicy::default()
        };
        Schedule::with_bounds(policy, LEAF, 7, F, C)
    }

    fn ms(secs: u64) -> u64 {
        secs * 1000
    }

    #[test]
    fn a_leaf_is_left_alone_until_it_is_due_and_then_enrolled_once() {
        let mut s = schedule();
        assert_eq!(
            s.step(ms(1_005)),
            Step::Wait {
                until_ms: ms(1_020)
            }
        );
        assert_eq!(s.wake_at(ms(1_005)), ms(1_020));
        assert_eq!(s.attempts, 0);
        assert_eq!(s.step(ms(1_020)), Step::Enroll);
        // One at a time: in flight, nothing else starts, and the loop is
        // woken at the deadline whatever becomes of it.
        assert_eq!(
            s.step(ms(1_021)),
            Step::InFlight {
                until_ms: ms(1_030)
            }
        );
        assert_eq!(s.attempts, 1);
    }

    #[test]
    fn a_renewal_moves_the_due_point_to_the_new_leaf() {
        let mut s = schedule();
        assert_eq!(s.step(ms(1_020)), Step::Enroll);
        let next = Leaf {
            issued_at: 1_020,
            expires_at: 1_140,
        };
        s.renewed(next);
        assert_eq!(s.leaf(), next);
        assert_eq!(
            s.step(ms(1_021)),
            Step::Wait {
                until_ms: ms(1_100)
            }
        );
        // And the old deadline is no longer this node's.
        assert_eq!(
            s.step(ms(1_031)),
            Step::Wait {
                until_ms: ms(1_100)
            }
        );
        assert_eq!((s.attempts, s.failures, s.renewals), (1, 0, 1));
    }

    /// The issuer is down from the due point to the deadline: attempts
    /// back off, never land past the deadline, and the leaf expires on
    /// time with no attempt able to move it.
    #[test]
    fn an_outage_is_retried_with_backoff_up_to_the_deadline_and_never_past_it() {
        let mut s = schedule();
        let mut now = ms(1_020);
        let mut starts = Vec::new();
        loop {
            match s.step(now) {
                Step::Enroll => {
                    starts.push(now);
                    s.failed(now);
                }
                Step::Wait { until_ms } => {
                    assert!(until_ms > now, "a wait that does not move");
                    assert!(
                        until_ms <= ms(1_030),
                        "a retry scheduled past the deadline: {until_ms}"
                    );
                    now = until_ms;
                }
                Step::Expired => break,
                Step::InFlight { .. } => unreachable!("every attempt ended"),
            }
        }
        assert_eq!(now, ms(1_030), "expired at the deadline and not after");
        // A handful of attempts, spreading out, the last a floor before
        // the deadline.
        assert!(starts.len() >= 3 && starts.len() <= 6, "{starts:?}");
        let gaps: Vec<u64> = starts.windows(2).map(|w| w[1] - w[0]).collect();
        assert!(gaps[1] > gaps[0], "no backoff: {gaps:?}");
        assert_eq!(*starts.last().unwrap(), ms(1_029), "{starts:?}");
        assert_eq!(s.failures, starts.len() as u64);
        // Expired is final: the issuer answering now changes nothing the
        // schedule can say.
        assert_eq!(s.step(ms(1_031)), Step::Expired);
    }

    /// An enrollment still in flight when the leaf expires does not hold
    /// the leaf open: the loop is woken at the deadline and told Expired.
    #[test]
    fn an_answer_still_outstanding_at_the_deadline_does_not_extend_it() {
        let mut s = schedule();
        assert_eq!(s.step(ms(1_029)), Step::Enroll);
        assert_eq!(s.wake_at(ms(1_029)), ms(1_030));
        assert_eq!(s.step(ms(1_030)), Step::Expired);
    }

    /// The issuer returning before the deadline renews: the backoff was a
    /// wait, not a verdict.
    #[test]
    fn an_issuer_that_returns_before_the_deadline_renews() {
        let mut s = schedule();
        assert_eq!(s.step(ms(1_020)), Step::Enroll);
        s.failed(ms(1_020));
        let Step::Wait { until_ms } = s.step(ms(1_020)) else {
            panic!("no retry scheduled")
        };
        assert_eq!(s.step(until_ms), Step::Enroll);
        s.renewed(Leaf {
            issued_at: 1_021,
            expires_at: 1_081,
        });
        assert!(matches!(s.step(until_ms), Step::Wait { .. }));
        assert_eq!((s.attempts, s.failures, s.renewals), (2, 1, 1));
    }

    #[test]
    fn the_backoff_is_bounded_by_its_ceiling() {
        let policy = RenewalPolicy {
            jitter_secs: 0,
            ..RenewalPolicy::default()
        };
        // A long leaf, so the deadline is not what bounds the waits.
        let mut s = Schedule::with_bounds(
            policy,
            Leaf {
                issued_at: 0,
                expires_at: 300_000,
            },
            3,
            F,
            C,
        );
        let mut now = ms(200_001);
        for _ in 0..20 {
            assert_eq!(s.step(now), Step::Enroll);
            s.failed(now);
            let Step::Wait { until_ms } = s.step(now) else {
                panic!("no retry")
            };
            let wait = Duration::from_millis(until_ms - now);
            assert!(wait < C * 5 / 4 + Duration::from_millis(1), "{wait:?}");
            now = until_ms;
        }
    }

    #[test]
    fn the_default_jitter_leaves_a_short_leaf_half_its_window_for_retries() {
        // An hour-long leaf is due at forty minutes. The default hour of
        // jitter would put most nodes past its end, at a second before it.
        let leaf = Leaf {
            issued_at: 1_000,
            expires_at: 1_000 + 3_600,
        };
        let mut latest = 0;
        for seed in 0..10_000u64 {
            let s = Schedule::new(RenewalPolicy::default(), leaf, seed.wrapping_mul(0x9e37));
            let due = s.due_at();
            assert!(due >= 1_000 + 2_400, "seed {seed}: due at {due}");
            assert!(due < 1_000 + 3_000, "seed {seed}: due at {due}");
            latest = latest.max(due);
        }
        // Still a spread, not a single point.
        assert!(latest > 1_000 + 2_400 + 300, "{latest}");
        // And a long leaf keeps the whole configured hour.
        let long = Leaf {
            issued_at: 0,
            expires_at: 90 * 86_400,
        };
        assert_eq!(bounded(RenewalPolicy::default(), &long).jitter_secs, 3_600);
    }
}

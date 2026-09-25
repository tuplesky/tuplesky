//! When a voter campaigns for leadership (task-d01).
//!
//! The protocol machines know how to campaign, how to win, and how a
//! leader that promised a higher ballot steps down. What they do not
//! know is *when*: they ignore timers, there is no heartbeat, and a
//! follower goes on following a leader that stopped an hour ago. This is
//! that decision, and nothing else: the serving loop tells it whether the
//! voter currently has a leader and whether it can reach a majority, and
//! it says when to campaign. Time is handed in, so the schedule is tested
//! without a clock.
//!
//! A voter is without a leader when it does not lead and holds no link to
//! the voter its ballot names -- or its ballot names itself and it does
//! not lead, which is a restarted leader, or a campaign of its own that
//! has not been won. No heartbeat is needed for that: the transport keeps
//! a link to every voter up, re-dials it when it drops (task-d03), and
//! closes one whose peer stopped answering at its idle timeout. So the
//! absence of the link *is* the silence of the leader, measured with the
//! transport's own clock.
//!
//! The rules are four. A voter that has a leader never campaigns on its
//! own. One without a leader waits a jittered patience first, so the
//! survivors of one failure do not all campaign at the same instant --
//! the first to be promised is the others' leader and they stop waiting.
//! It campaigns only while it can reach a majority, since a campaign
//! nobody can answer only raises the ballot the rest must then outbid.
//! And one whose campaign did not produce a leader waits twice as long
//! before the next, up to a ceiling, so two candidates that keep
//! outbidding each other spread apart rather than duel for ever.
//!
//! An operator may also ask for a campaign (`SIGUSR1`): it goes on the
//! next pass that can reach a majority, whether or not the voter has a
//! leader, which is how leadership is moved on purpose.

use std::time::{Duration, Instant};

/// How long a voter without a leader waits before its first campaign,
/// before jitter.
pub const PATIENCE: Duration = Duration::from_secs(1);

/// The longest wait between campaigns that did not produce a leader.
pub const CEILING: Duration = Duration::from_secs(16);

/// Why a campaign is due.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Why {
    /// The voter has had no leader for its patience.
    Leaderless,
    /// An operator asked for one.
    Requested,
}

/// The campaign schedule for one voter.
#[derive(Clone, Debug)]
pub struct Election {
    /// When the next campaign is due, while the voter has no leader.
    next: Option<Instant>,
    /// The wait before the next campaign, before jitter.
    backoff: Duration,
    /// Campaigns made, which with the salt picks the jitter.
    attempts: u64,
    /// Per-voter, so two voters' jitter differs.
    salt: u64,
    /// An operator's request, not yet acted on.
    requested: bool,
    patience: Duration,
    ceiling: Duration,
}

impl Election {
    /// A schedule for the voter with this salt, which has a leader until
    /// first observed otherwise.
    pub const fn new(salt: u64) -> Self {
        Self::with_bounds(salt, PATIENCE, CEILING)
    }

    /// The same, with its own patience and ceiling.
    pub const fn with_bounds(salt: u64, patience: Duration, ceiling: Duration) -> Self {
        Election {
            next: None,
            backoff: patience,
            attempts: 0,
            salt,
            requested: false,
            patience,
            ceiling,
        }
    }

    /// An operator asked this voter to campaign.
    pub const fn request(&mut self) {
        self.requested = true;
    }

    /// Look at the voter: whether it has a leader (it leads, or holds a
    /// link to the voter its ballot names) and whether it can reach a
    /// majority of the voters, itself included. Says when a campaign is
    /// due now, and why.
    pub fn observe(&mut self, led: bool, majority: bool, now: Instant) -> Option<Why> {
        if led {
            // A leader again: the next time it has none, it starts from
            // its patience, not from where a past duel left the backoff.
            self.next = None;
            self.backoff = self.patience;
        } else if self.next.is_none() {
            self.next = Some(now + self.wait());
        }
        if !majority {
            return None;
        }
        if self.requested {
            self.requested = false;
            self.campaigned(now);
            return Some(Why::Requested);
        }
        if self.next.is_some_and(|at| at <= now) {
            self.campaigned(now);
            return Some(Why::Leaderless);
        }
        None
    }

    /// When the schedule next wants to be looked at: the next campaign,
    /// if one is scheduled. A request is acted on at the next pass, which
    /// the signal that carried it has already woken.
    pub const fn next_deadline(&self) -> Option<Instant> {
        self.next
    }

    /// A campaign went out now: the next, if this one produces no leader,
    /// waits twice as long.
    fn campaigned(&mut self, now: Instant) {
        self.attempts = self.attempts.saturating_add(1);
        self.backoff = self.backoff.saturating_mul(2).min(self.ceiling);
        self.next = Some(now + self.wait());
    }

    fn wait(&self) -> Duration {
        crate::redial::jitter(self.backoff, self.salt, self.attempts, self.ceiling)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const P: Duration = Duration::from_millis(100);
    const C: Duration = Duration::from_millis(800);

    fn schedule(salt: u64) -> Election {
        Election::with_bounds(salt, P, C)
    }

    /// Step `e` in 10 ms ticks from `start` until it campaigns or `limit`
    /// passes; the time it campaigned at.
    fn first_campaign(
        e: &mut Election,
        start: Instant,
        limit: Duration,
        majority: bool,
    ) -> Option<Instant> {
        let mut now = start;
        while now <= start + limit {
            if e.observe(false, majority, now).is_some() {
                return Some(now);
            }
            now += Duration::from_millis(10);
        }
        None
    }

    #[test]
    fn a_voter_with_a_leader_never_campaigns_on_its_own() {
        let mut e = schedule(1);
        let t = Instant::now();
        for i in 0..1000 {
            assert_eq!(e.observe(true, true, t + P * i), None);
        }
        assert_eq!(e.next_deadline(), None);
    }

    #[test]
    fn a_voter_without_a_leader_waits_its_jittered_patience_first() {
        let mut e = schedule(1);
        let t = Instant::now();
        let at = first_campaign(&mut e, t, C * 2, true).expect("campaigns");
        let waited = at - t;
        assert!(waited >= P * 3 / 4, "{waited:?}");
        assert!(
            waited <= P * 5 / 4 + Duration::from_millis(10),
            "{waited:?}"
        );
    }

    #[test]
    fn two_survivors_of_one_failure_do_not_campaign_at_the_same_instant() {
        let t = Instant::now();
        let mut differ = 0;
        for salt in 0..32u64 {
            let a = first_campaign(&mut schedule(salt), t, C, true);
            let b = first_campaign(&mut schedule(salt + 1000), t, C, true);
            if a != b {
                differ += 1;
            }
        }
        assert!(differ >= 24, "{differ} of 32 pairs differ");
    }

    #[test]
    fn a_voter_that_cannot_reach_a_majority_does_not_campaign() {
        let mut e = schedule(1);
        let t = Instant::now();
        assert_eq!(first_campaign(&mut e, t, C * 4, false), None);
        // Once it can, it goes at once: its patience ran while it could not.
        assert_eq!(e.observe(false, true, t + C * 4), Some(Why::Leaderless));
    }

    #[test]
    fn campaigns_that_produce_no_leader_back_off_to_the_ceiling() {
        let mut e = schedule(7);
        let mut now = Instant::now();
        let mut gaps = Vec::new();
        let mut last = None;
        while gaps.len() < 8 {
            if e.observe(false, true, now).is_some() {
                if let Some(l) = last {
                    gaps.push(now - l);
                }
                last = Some(now);
            }
            now += Duration::from_millis(5);
        }
        assert!(gaps[1] > gaps[0], "{gaps:?}");
        for g in &gaps {
            assert!(*g <= C * 5 / 4 + Duration::from_millis(5), "{gaps:?}");
        }
        assert!(gaps[7] >= C * 3 / 4, "{gaps:?}");
    }

    #[test]
    fn a_leader_again_starts_the_next_wait_from_its_patience() {
        let mut e = schedule(3);
        let t = Instant::now();
        let mut now = t;
        let mut campaigns = 0;
        while campaigns < 4 {
            if e.observe(false, true, now).is_some() {
                campaigns += 1;
            }
            now += Duration::from_millis(5);
        }
        assert_eq!(e.observe(true, true, now), None);
        let at = first_campaign(&mut e, now, C * 2, true).expect("campaigns");
        assert!(at - now <= P * 5 / 4 + Duration::from_millis(10));
    }

    #[test]
    fn an_operator_request_goes_on_the_next_pass_with_a_majority() {
        let mut e = schedule(1);
        let t = Instant::now();
        e.request();
        assert_eq!(e.observe(true, false, t), None, "no majority: held");
        assert_eq!(e.observe(true, true, t), Some(Why::Requested));
        assert_eq!(e.observe(true, true, t), None, "acted on once");
    }
}

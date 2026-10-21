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

use coord_types::ids::{Ballot, ReplicaId};

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
    /// When this voter last campaigned.
    last: Option<Instant>,
    /// The promised ballot that has not synchronized here yet, and since
    /// when (task-d10).
    unsynced: Option<(Ballot, Instant)>,
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
            last: None,
            unsynced: None,
            patience,
            ceiling,
        }
    }

    /// Whether the voter has a leader, for [`Election::observe`].
    ///
    /// It does when it leads, or when the ballot it promised names another
    /// voter it holds a link to -- as long as that ballot has synchronized
    /// here (`active`, the ballot whose Sync it adopted, is `promised`),
    /// or has had the ceiling to do so (task-d10).
    ///
    /// A promise is not yet a leader. A candidate collects its promises
    /// before it knows whether it can bind a selection, and one that finds
    /// itself behind every reporter stands down without ever sending a
    /// Sync (task-d05). Counted as a leader because its link held, it held
    /// every voter that promised it: in the five-node Jepsen run none of
    /// them campaigned again, and the domain served nothing.
    ///
    /// Nor is the Sync a round trip away: the candidate collects a report
    /// from a majority, paged, and binds its selection durably first,
    /// which on a busy domain takes longer than the patience. A voter that
    /// campaigned over it then voided a campaign that would have
    /// succeeded, and in the stress runs the ballots duelled into the
    /// forties. So a promise gets what a candidate gives its own campaign
    /// in `observe`: the ceiling, from when this voter first saw it.
    pub fn led(
        &mut self,
        leads: bool,
        me: ReplicaId,
        promised: Ballot,
        active: Ballot,
        linked: bool,
        now: Instant,
    ) -> bool {
        if leads {
            self.unsynced = None;
            return true;
        }
        if promised.leader == me || !linked {
            return false;
        }
        if active == promised {
            self.unsynced = None;
            return true;
        }
        let since = match self.unsynced {
            Some((ballot, since)) if ballot == promised => since,
            _ => {
                self.unsynced = Some((promised, now));
                now
            }
        };
        now < since + self.ceiling
    }

    /// An operator asked this voter to campaign.
    pub const fn request(&mut self) {
        self.requested = true;
    }

    /// Look at the voter: whether it has a leader (it leads, or holds a
    /// link to the voter its ballot names) and whether it can reach a
    /// majority of the voters, itself included. Says when a campaign is
    /// due now, and why.
    ///
    /// `campaigning` is whether a campaign of this voter's own is still
    /// under way. While it is, and for up to the ceiling after it
    /// started, a leaderless voter waits for it rather than replacing it.
    /// A campaign collects a report from a majority, and a report carries
    /// what its voter holds, so on a busy domain the collection can take
    /// longer than the doubled patience; a new campaign would discard it,
    /// raise the ballot every voter must promise again, and start the
    /// collection over. One that is stuck, whose reports will never come,
    /// is replaced after the ceiling. An operator's request is not held
    /// back by it.
    pub fn observe(
        &mut self,
        led: bool,
        majority: bool,
        campaigning: bool,
        now: Instant,
    ) -> Option<Why> {
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
        let finishing = campaigning && self.last.is_some_and(|at| now < at + self.ceiling);
        if self.next.is_some_and(|at| at <= now) && !finishing {
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
        self.last = Some(now);
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
            if e.observe(false, majority, false, now).is_some() {
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
            assert_eq!(e.observe(true, true, false, t + P * i), None);
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
        assert_eq!(
            e.observe(false, true, false, t + C * 4),
            Some(Why::Leaderless)
        );
    }

    #[test]
    fn campaigns_that_produce_no_leader_back_off_to_the_ceiling() {
        let mut e = schedule(7);
        let mut now = Instant::now();
        let mut gaps = Vec::new();
        let mut last = None;
        while gaps.len() < 8 {
            if e.observe(false, true, false, now).is_some() {
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
            if e.observe(false, true, false, now).is_some() {
                campaigns += 1;
            }
            now += Duration::from_millis(5);
        }
        assert_eq!(e.observe(true, true, false, now), None);
        let at = first_campaign(&mut e, now, C * 2, true).expect("campaigns");
        assert!(at - now <= P * 5 / 4 + Duration::from_millis(10));
    }

    #[test]
    fn an_operator_request_goes_on_the_next_pass_with_a_majority() {
        let mut e = schedule(1);
        let t = Instant::now();
        e.request();
        assert_eq!(e.observe(true, false, false, t), None, "no majority: held");
        assert_eq!(e.observe(true, true, false, t), Some(Why::Requested));
        assert_eq!(e.observe(true, true, false, t), None, "acted on once");
    }

    #[test]
    fn a_campaign_still_under_way_is_left_to_finish_up_to_the_ceiling() {
        let mut e = schedule(5);
        let t = Instant::now();
        let started = first_campaign(&mut e, t, C * 2, true).expect("campaigns");
        // Still collecting: past the doubled patience, nothing new.
        let mut now = started;
        while now < started + C {
            assert_eq!(
                e.observe(false, true, true, now),
                None,
                "{:?}",
                now - started
            );
            now += Duration::from_millis(10);
        }
        // Stuck past the ceiling: replaced.
        let again = (0..200)
            .map(|i| started + C + Duration::from_millis(10 * i))
            .find(|at| e.observe(false, true, true, *at).is_some())
            .expect("a stuck campaign is replaced");
        assert!(again >= started + C);
        // And an operator is never held back by one.
        e.request();
        assert_eq!(e.observe(false, true, true, again), Some(Why::Requested));
    }

    fn ballot(number: u64, leader: u8) -> Ballot {
        Ballot {
            epoch: coord_types::ids::ConfigurationEpoch::new(1).unwrap(),
            number,
            leader: ReplicaId([leader; 16]),
        }
    }

    /// A voter following a synchronized ballot whose leader it reaches
    /// has a leader. One whose promised ballot has not synchronized has
    /// one for the ceiling and not after, however well linked its
    /// candidate is (task-d10).
    #[test]
    fn a_promise_that_never_synchronizes_is_a_leader_only_for_the_ceiling() {
        let me = ReplicaId([3; 16]);
        let t = Instant::now();
        let mut e = schedule(1);
        // Following ballot 2, synchronized, its leader linked.
        assert!(e.led(false, me, ballot(2, 5), ballot(2, 5), true, t));
        // Promised ballot 3, still voting in ballot 2: a campaign under
        // way, given the ceiling from when this voter first saw it.
        assert!(e.led(false, me, ballot(3, 5), ballot(2, 1), true, t));
        assert!(e.led(false, me, ballot(3, 5), ballot(2, 1), true, t + C / 2));
        // Past it, the candidate stood down or is stuck: leaderless.
        assert!(!e.led(false, me, ballot(3, 5), ballot(2, 1), true, t + C));
        // A new promise starts its own allowance.
        assert!(e.led(false, me, ballot(4, 1), ballot(2, 1), true, t + C));
        // The link still matters, synchronized or not.
        assert!(!e.led(false, me, ballot(2, 5), ballot(2, 5), false, t));
        // A ballot that names this voter is its own to lead.
        assert!(!e.led(false, me, ballot(5, 3), ballot(5, 3), true, t));
        assert!(e.led(true, me, ballot(5, 3), ballot(5, 3), true, t));
    }

    /// A voter held by a promise that never synchronizes campaigns once the
    /// ceiling and its patience have passed; one whose Sync comes within
    /// the ceiling, however slowly, never does.
    #[test]
    fn a_voter_whose_promise_does_not_synchronize_campaigns_after_the_ceiling() {
        let me = ReplicaId([3; 16]);
        let t = Instant::now();
        let step = Duration::from_millis(10);
        let mut stuck = schedule(7);
        let mut campaigned = None;
        for i in 0..400u32 {
            let now = t + step * i;
            let led = stuck.led(false, me, ballot(3, 5), ballot(2, 1), true, now);
            if stuck.observe(led, true, false, now).is_some() {
                campaigned = Some(now);
                break;
            }
        }
        let at = campaigned.expect("it waited on the promise for ever");
        assert!(at >= t + C, "it campaigned over a live candidate");

        let mut live = schedule(7);
        for i in 0..400u32 {
            let now = t + step * i;
            // The Sync arrives just inside the ceiling.
            let active = if now < t + C - step {
                ballot(2, 1)
            } else {
                ballot(3, 5)
            };
            let led = live.led(false, me, ballot(3, 5), active, true, now);
            assert_eq!(live.observe(led, true, false, now), None, "at {i}");
        }
    }
}

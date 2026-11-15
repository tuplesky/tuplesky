//! BBR with a guard on the least round trip it sizes its window from.
//!
//! quinn's BBR sizes its window as a gain times the largest bandwidth it
//! has measured times the least round trip of quinn's estimator, which
//! takes every raw sample and never rises. A sample is the time between a
//! packet's send and the receipt stamp of the datagram whose ACK covers
//! it. quinn's endpoint driver stamps every datagram of a receive pass
//! with one clock read taken when the pass starts. If the driver's thread
//! is descheduled after that read, datagrams that arrive meanwhile carry
//! the stale stamp, and a packet sent shortly before it reads as acked
//! within a fraction of the path's round trip. On hosts with no idle
//! core, such samples read 0.01 to 0.6 ms on a 2 ms path (#98, the census
//! of 6889872235).
//!
//! The short least does nothing while BBR is in Startup, where the window
//! only grows. BBR leaves Startup after three round starts at which the
//! sender was not limited by the application and the bandwidth it
//! measures grew by less than a quarter, or one such round start during
//! loss recovery. quinn counts the sender as limited by the application
//! unless its last poll ran out of window or stopped at its budget of 20
//! datagrams, so a sender leaves Startup once its bursts outrun a poll
//! while ACKs arrive. From then on the window is the target, as soon as
//! the next ACK arrives. Both conditions are needed (#98, K6): with a
//! least of 0.62 ms on the 2.06 ms path a window of 9.6 MB fell to 14 KB,
//! while in a run that never left Startup a least of 0.28 ms left the
//! window to grow to 27 MB. `coord-transport-sim`'s `bbr_stamps` runs
//! show both conditions, alone and together.
//!
//! [`GuardedBbr`] runs quinn's BBR unchanged and hands it, in place of
//! quinn's estimator, the last copy of that estimator whose least came
//! from a sample whose stamp was on time. The controller reads the clock
//! itself when an ACK's packets have been counted, once an ACK, so it
//! knows how far the stamp lags behind the time the ACK is handled. The
//! true round trip lies between the sample and the sample plus that lag.
//! A sample counts as on time when the lag is at most a quarter of the
//! sample, so a least it adopts is at least four fifths of a round trip
//! the path really took.
//!
//! What it keeps from quinn: the estimator is quinn's own, so a least
//! never rises within a path, and a new path's estimator is adopted as
//! it comes. What it gives up: once a late sample has set quinn's least,
//! an on-time sample between that least and the adopted one cannot be
//! adopted, because quinn's estimator no longer moves there. The adopted
//! least then stays where it was, which can be above the path's own.

use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use quinn::congestion::{Bbr, BbrConfig, Controller, ControllerFactory, ControllerMetrics};
use quinn_proto::RttEstimator;

/// A sample counts as on time when its stamp lags the handling of its ACK
/// by at most this share of the sample.
pub const ON_TIME_LAG_SHARE: u32 = 4;

/// The clock a controller reads to judge a stamp's lag.
pub type Clock = Arc<dyn Fn() -> Instant + Send + Sync>;

/// The factory for [`GuardedBbr`].
#[derive(Clone)]
pub struct GuardedBbrConfig {
    bbr: Arc<BbrConfig>,
    clock: Clock,
}

impl GuardedBbrConfig {
    /// quinn's BBR with its default configuration, judged against `clock`.
    /// Production reads [`Instant::now`]; a simulation passes its own.
    pub fn with_clock(clock: Clock) -> Self {
        GuardedBbrConfig {
            bbr: Arc::new(BbrConfig::default()),
            clock,
        }
    }
}

impl Default for GuardedBbrConfig {
    fn default() -> Self {
        Self::with_clock(Arc::new(Instant::now))
    }
}

impl fmt::Debug for GuardedBbrConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GuardedBbrConfig")
            .field("bbr", &self.bbr)
            .finish_non_exhaustive()
    }
}

impl ControllerFactory for GuardedBbrConfig {
    fn build(self: Arc<Self>, _now: Instant, current_mtu: u16) -> Box<dyn Controller> {
        Box::new(GuardedBbr {
            bbr: Bbr::new(self.bbr.clone(), current_mtu),
            clock: self.clock.clone(),
            guard: Guard::default(),
            adopted: None,
        })
    }
}

/// quinn's BBR, handed only a least round trip from an on-time sample.
#[derive(Clone)]
pub struct GuardedBbr {
    bbr: Bbr,
    clock: Clock,
    guard: Guard,
    /// The estimator BBR is handed: the last copy of quinn's whose least
    /// came from an on-time sample, or the first one seen.
    adopted: Option<RttEstimator>,
}

impl GuardedBbr {
    /// The least round trip BBR is handed, once a packet was acknowledged.
    pub fn adopted_min_rtt(&self) -> Option<Duration> {
        self.adopted.as_ref().map(RttEstimator::min)
    }

    /// How many times quinn's least fell on a late sample and was not
    /// adopted.
    pub fn late_minimums(&self) -> u64 {
        self.guard.late_minimums
    }
}

impl fmt::Debug for GuardedBbr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GuardedBbr")
            .field("bbr", &self.bbr)
            .field("guard", &self.guard)
            .field("adopted_min", &self.adopted_min_rtt())
            .finish_non_exhaustive()
    }
}

/// What the guard knows of the samples quinn's estimator has taken.
#[derive(Clone, Debug, Default)]
struct Guard {
    /// quinn's least the last time a packet was acknowledged.
    seen_min: Option<Duration>,
    /// Whether the ACK being handled has acknowledged a packet that
    /// elicits an ACK. quinn takes a sample from an ACK only then (while
    /// it validates a new path it takes one without telling the
    /// controller of any packet, which this guard does not see).
    eliciting: bool,
    /// How far the stamp of the last ACK that acknowledged such a packet
    /// lagged behind the time it was handled.
    last_lag: Option<Duration>,
    /// How many times quinn's least fell on a late sample.
    late_minimums: u64,
}

impl Guard {
    /// A packet was acknowledged while quinn's least is `min`; whether to
    /// adopt quinn's estimator as it stands. quinn takes an ACK's sample
    /// after the controller has seen the ACK's end, so a least that fell
    /// since the last packet acknowledged is the sample of the last ACK
    /// that acknowledged one, and that ACK's lag says whether it was on
    /// time.
    fn acknowledged(&mut self, min: Duration) -> bool {
        self.eliciting = true;
        let adopt = match self.seen_min {
            // The first estimator, and a new path's, which starts again
            // from the configured initial round trip.
            None => true,
            Some(seen) if min > seen => true,
            Some(seen) if min < seen => {
                let on_time = self.last_lag.is_some_and(|lag| on_time(min, lag));
                self.late_minimums += u64::from(!on_time);
                on_time
            }
            Some(_) => false,
        };
        self.seen_min = Some(min);
        adopt
    }

    /// An ACK stamped `stamp` was handled at `handled`.
    fn ended(&mut self, stamp: Instant, handled: Instant) {
        if std::mem::take(&mut self.eliciting) {
            self.last_lag = Some(handled.saturating_duration_since(stamp));
        }
    }
}

/// Whether a raw sample is on time when its ACK's stamp lagged behind the
/// ACK's handling by `lag`. The datagram arrived between its stamp and
/// its handling, so the round trip the path took lies between `sample`
/// and `sample + lag`.
fn on_time(sample: Duration, lag: Duration) -> bool {
    lag <= sample / ON_TIME_LAG_SHARE
}

impl Controller for GuardedBbr {
    fn on_sent(&mut self, now: Instant, bytes: u64, last_packet_number: u64) {
        self.bbr.on_sent(now, bytes, last_packet_number);
    }

    fn on_ack(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        app_limited: bool,
        rtt: &RttEstimator,
    ) {
        if self.guard.acknowledged(rtt.min()) {
            self.adopted = Some(*rtt);
        }
        let rtt = self.adopted.as_ref().unwrap_or(rtt);
        self.bbr.on_ack(now, sent, bytes, app_limited, rtt);
    }

    fn on_end_acks(
        &mut self,
        now: Instant,
        in_flight: u64,
        app_limited: bool,
        largest_packet_num_acked: Option<u64>,
    ) {
        self.guard.ended(now, (self.clock)());
        self.bbr
            .on_end_acks(now, in_flight, app_limited, largest_packet_num_acked);
    }

    fn on_congestion_event(
        &mut self,
        now: Instant,
        sent: Instant,
        is_persistent_congestion: bool,
        lost_bytes: u64,
    ) {
        self.bbr
            .on_congestion_event(now, sent, is_persistent_congestion, lost_bytes);
    }

    fn on_mtu_update(&mut self, new_mtu: u16) {
        self.bbr.on_mtu_update(new_mtu);
    }

    fn window(&self) -> u64 {
        self.bbr.window()
    }

    fn metrics(&self) -> ControllerMetrics {
        self.bbr.metrics()
    }

    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(self.clone())
    }

    fn initial_window(&self) -> u64 {
        self.bbr.initial_window()
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: Duration = Duration::from_millis(1);

    /// One ACK: its packets acknowledged at quinn's least `min`, then its
    /// end, handled `lag` after its stamp. Returns whether the guard
    /// adopted quinn's estimator at the ACK's first packet.
    fn ack(guard: &mut Guard, min: Duration, lag: Duration) -> bool {
        let adopt = guard.acknowledged(min);
        assert!(!guard.acknowledged(min), "a second packet changes nothing");
        let stamp = Instant::now();
        guard.ended(stamp, stamp + lag);
        adopt
    }

    #[test]
    fn the_first_estimator_is_adopted_and_an_unchanged_least_adopts_nothing() {
        let mut guard = Guard::default();
        assert!(ack(&mut guard, 333 * MS, Duration::ZERO));
        assert!(!ack(&mut guard, 333 * MS, 10 * MS));
        assert_eq!(guard.late_minimums, 0);
    }

    #[test]
    fn a_least_that_fell_on_an_on_time_sample_is_adopted() {
        let mut guard = Guard::default();
        assert!(ack(&mut guard, 333 * MS, MS / 4));
        // The previous ACK's lag was a quarter of 1 ms: on time for a
        // sample of 1 ms or more.
        assert!(ack(&mut guard, MS, MS / 4));
        assert_eq!(guard.late_minimums, 0);
    }

    #[test]
    fn a_least_that_fell_on_a_late_sample_is_not_adopted_and_is_counted() {
        let mut guard = Guard::default();
        assert!(ack(&mut guard, 2 * MS, 2 * MS));
        // 0.3 ms read on a stamp handled 2 ms late.
        assert!(!ack(&mut guard, MS * 3 / 10, Duration::ZERO));
        assert_eq!(guard.late_minimums, 1);
        // quinn's least stays where the late sample put it: nothing new
        // to adopt, however on time the later ACKs.
        assert!(!ack(&mut guard, MS * 3 / 10, Duration::ZERO));
        assert_eq!(guard.late_minimums, 1);
        // An on-time sample below it is adopted.
        assert!(ack(&mut guard, MS / 5, Duration::ZERO));
    }

    #[test]
    fn a_least_that_fell_before_any_ack_ended_is_not_adopted() {
        let mut guard = Guard::default();
        assert!(guard.acknowledged(333 * MS));
        assert!(!guard.acknowledged(MS));
        assert_eq!(guard.late_minimums, 1);
    }

    #[test]
    fn a_new_paths_estimator_is_adopted() {
        let mut guard = Guard::default();
        assert!(ack(&mut guard, 333 * MS, Duration::ZERO));
        assert!(ack(&mut guard, 2 * MS, Duration::ZERO));
        // quinn starts a new path's estimator again from the initial
        // round trip, whatever the lag.
        assert!(ack(&mut guard, 333 * MS, 100 * MS));
    }

    #[test]
    fn an_ack_that_acknowledged_no_eliciting_packet_keeps_the_last_lag() {
        let mut guard = Guard::default();
        assert!(ack(&mut guard, 333 * MS, Duration::ZERO));
        // An on-time ACK whose sample quinn folds in after its end.
        assert!(!guard.acknowledged(333 * MS));
        let stamp = Instant::now();
        guard.ended(stamp, stamp);
        // Then an ACK of ACK-only packets, late: quinn takes no sample
        // from it, and its lag is not the one that judges.
        guard.ended(stamp, stamp + 10 * MS);
        assert!(guard.acknowledged(2 * MS));
    }

    #[test]
    fn a_sample_is_on_time_when_its_lag_is_a_quarter_of_it_or_less() {
        assert!(on_time(2 * MS, Duration::ZERO));
        assert!(on_time(2 * MS, MS / 2));
        assert!(!on_time(2 * MS, MS / 2 + Duration::from_nanos(1)));
        // A stamp older than the packet's send reads zero, and any lag is
        // late.
        assert!(on_time(Duration::ZERO, Duration::ZERO));
        assert!(!on_time(Duration::ZERO, Duration::from_nanos(1)));
    }
}

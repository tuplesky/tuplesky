//! The collector boundary's own reading of elapsed time (task-c03).
//!
//! Two clocks reach this boundary and they answer different questions.
//! Authentication wall time -- `ClockHealth::now`, Unix seconds with an
//! uncertainty beside it -- is what a token's `exp`, `nbf` and `iat` are
//! judged against, and its units are load-bearing there. A request's
//! deadline and a destination's repeat schedule ask something else: how
//! much *local* time has passed since a point this process chose. That
//! is a monotonic reading, in milliseconds, and it must not step when
//! the wall clock is corrected: a deadline on a clock that can jump is a
//! deadline that can fire a day early or never, and a retry schedule on
//! one can stall or stampede.
//!
//! Before this type the two were one `u64` parameter, and a nominal
//! 1,500 ms deadline added to Unix seconds was 1,500 seconds. Giving the
//! reading a type of its own makes the two inputs distinct at the
//! signature, so a caller cannot hand the wall clock to the deadline
//! path by mistake. The crate holds no clock: the runtime reads its
//! monotonic source and passes the value in, which is also what lets a
//! test drive expiry without waiting for it.

/// A monotonic reading, in milliseconds since an origin the caller
/// chose -- the start of the serving loop, or zero in a test.
///
/// Only differences between readings mean anything; the origin does
/// not. Comparisons are what the collector does with it: a deadline is
/// a reading, and it has passed when the current reading is at or past
/// it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MonotonicMillis(u64);

impl MonotonicMillis {
    /// The origin.
    pub const ZERO: Self = MonotonicMillis(0);

    /// A reading of `millis` since the origin.
    pub const fn new(millis: u64) -> Self {
        MonotonicMillis(millis)
    }

    /// The reading, in milliseconds since the origin.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// The reading `millis` later, saturating rather than wrapping: a
    /// deadline past the end of time never passes, which is the right
    /// failure.
    pub const fn plus(self, millis: u64) -> Self {
        MonotonicMillis(self.0.saturating_add(millis))
    }
}

impl core::fmt::Display for MonotonicMillis {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{} ms", self.0)
    }
}

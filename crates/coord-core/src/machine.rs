//! The deterministic machine contract and injected clock.

use alloc::vec::Vec;

use serde::{Deserialize, Serialize};

/// Small synchronous state machine with owned inputs and effects.
///
/// Effect vector order does not imply asynchronous completion order; the
/// world (production runtime or simulator) completes them and feeds results
/// back as events. Implementations must not read ambient time, randomness
/// or I/O and must iterate deterministically.
pub trait DeterministicMachine {
    /// Owned input.
    type Event;
    /// Owned output.
    type Effect;

    /// Process one event.
    fn step(&mut self, event: Self::Event) -> Vec<Self::Effect>;
}

/// Injected clock reading (design Section 18.2). Monotonic ticks drive
/// timers; wall-time bounds and health feed admission-time policy only.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClockSnapshot {
    /// Monotonic logical ticks since boot; never decreases within a boot.
    pub monotonic_ticks: u64,
    /// Lower bound of wall time in milliseconds since the Unix epoch.
    pub wall_lower_ms: u64,
    /// Upper bound of wall time in milliseconds since the Unix epoch.
    pub wall_upper_ms: u64,
    /// Whether the time source is believed healthy; unhealthy clocks deny
    /// validity-dependent admission and suspend expiration.
    pub healthy: bool,
}

impl ClockSnapshot {
    /// Width of the wall-time uncertainty interval.
    pub const fn uncertainty_ms(&self) -> u64 {
        self.wall_upper_ms.saturating_sub(self.wall_lower_ms)
    }

    /// A snapshot is usable for validity decisions only when healthy and
    /// its interval is well formed.
    pub const fn usable(&self) -> bool {
        self.healthy && self.wall_lower_ms <= self.wall_upper_ms
    }
}

//! Deterministic test ports for the `coord-core` clock and entropy traits.
//! They are predictable on purpose and exist only in this test-only crate;
//! production ports (OS entropy, system clock) live in the runtime crate.

use coord_core::machine::ClockSnapshot;
use coord_core::ports::{ClockSource, EntropySource};

/// Deterministic counter-based entropy for tests. Not random; never linked
/// into production (this crate is test-only).
#[derive(Debug, Default)]
pub struct CountingEntropy {
    counter: u64,
}

impl EntropySource for CountingEntropy {
    fn fill(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            self.counter = self.counter.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let bytes = self.counter.to_be_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
    }
}

/// Manually advanced clock for tests.
#[derive(Debug, Clone, Copy)]
pub struct ManualClock {
    snapshot: ClockSnapshot,
}

impl ManualClock {
    /// Start at tick zero with the given wall time and healthy status.
    pub const fn new(wall_ms: u64) -> Self {
        ManualClock {
            snapshot: ClockSnapshot {
                monotonic_ticks: 0,
                wall_lower_ms: wall_ms,
                wall_upper_ms: wall_ms,
                healthy: true,
            },
        }
    }

    /// Advance monotonic ticks and wall time together.
    pub fn advance(&mut self, ticks: u64, wall_ms: u64) {
        self.snapshot.monotonic_ticks += ticks;
        self.snapshot.wall_lower_ms += wall_ms;
        self.snapshot.wall_upper_ms += wall_ms;
    }

    /// Mark the clock unhealthy (e.g. detected step or rate anomaly).
    pub fn set_healthy(&mut self, healthy: bool) {
        self.snapshot.healthy = healthy;
    }
}

impl ClockSource for ManualClock {
    fn now(&self) -> ClockSnapshot {
        self.snapshot
    }
}

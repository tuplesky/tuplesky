//! Test-only ports. Production ports (OS entropy, system clock) are supplied
//! by the runtime crate; this crate offers deterministic stand-ins so
//! machines can be driven in unit tests without a simulator.

use crate::machine::ClockSnapshot;

/// Source of entropy for the world, consumed only at trusted boundaries.
pub trait EntropySource {
    /// Fill `buf` with bytes.
    fn fill(&mut self, buf: &mut [u8]);
}

/// Source of clock snapshots for the world.
pub trait ClockSource {
    /// Current snapshot.
    fn now(&self) -> ClockSnapshot;
}

/// Deterministic counter-based entropy for tests. Not random; never linked
/// into production (the runtime crate provides OS entropy).
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

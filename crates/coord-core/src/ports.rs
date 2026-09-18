//! Clock and entropy ports. Production ports (OS entropy, system clock) are
//! supplied by the runtime crate; deterministic test stand-ins live in the
//! test-only `coord-sim` crate so they are never linked into production
//! artifacts (the dependency-policy check rejects that edge).

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

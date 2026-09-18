//! Seeded ChaCha12 generator with named substreams (design Section 21.1).
//!
//! Each named stream is seeded from `BLAKE3(master_seed || name)` and is
//! independent of every other stream, so adding draws in one consumer
//! (for example the workload) does not change another's schedule (for
//! example crash timing). The generator family and seeding rule are pinned
//! by [`GENERATOR`]; a bundle records it and refuses to replay under any
//! other.

use std::collections::BTreeMap;

use rand_chacha::ChaCha12Rng;
use rand_core::{Rng, SeedableRng};

/// Pinned generator description recorded in bundles.
pub const GENERATOR: &str = "chacha12/rand_chacha-0.10/blake3-substream-v1";

/// Named substreams over one master seed.
#[derive(Debug)]
pub struct NamedStreams {
    master: [u8; 32],
    streams: BTreeMap<String, ChaCha12Rng>,
    draws: BTreeMap<String, u64>,
}

impl NamedStreams {
    /// Create from a master seed.
    pub fn new(master: [u8; 32]) -> Self {
        NamedStreams {
            master,
            streams: BTreeMap::new(),
            draws: BTreeMap::new(),
        }
    }

    /// Master seed.
    pub const fn master(&self) -> [u8; 32] {
        self.master
    }

    fn stream(&mut self, name: &str) -> &mut ChaCha12Rng {
        if !self.streams.contains_key(name) {
            let mut hasher = blake3::Hasher::new_derive_key("tuplesky coord-sim substream v1");
            hasher.update(&self.master);
            hasher.update(name.as_bytes());
            let seed = *hasher.finalize().as_bytes();
            self.streams
                .insert(name.to_owned(), ChaCha12Rng::from_seed(seed));
        }
        *self.draws.entry(name.to_owned()).or_insert(0) += 1;
        self.streams.get_mut(name).expect("inserted above")
    }

    /// Next 64-bit value from `name`.
    pub fn next_u64(&mut self, name: &str) -> u64 {
        self.stream(name).next_u64()
    }

    /// Uniform value in `0..bound` (unbiased via rejection; `bound > 0`).
    pub fn below(&mut self, name: &str, bound: u64) -> u64 {
        assert!(bound > 0, "bound must be positive");
        let zone = u64::MAX - (u64::MAX % bound);
        loop {
            let v = self.next_u64(name);
            if v < zone {
                return v % bound;
            }
        }
    }

    /// Uniform value in `lo..=hi`.
    pub fn range(&mut self, name: &str, lo: u64, hi: u64) -> u64 {
        assert!(lo <= hi);
        lo + self.below(name, hi - lo + 1)
    }

    /// Bernoulli trial with probability `per_million / 1_000_000`.
    pub fn chance(&mut self, name: &str, per_million: u32) -> bool {
        self.below(name, 1_000_000) < u64::from(per_million)
    }

    /// Sixteen bytes from `name`.
    pub fn bytes16(&mut self, name: &str) -> [u8; 16] {
        let mut out = [0u8; 16];
        self.stream(name).fill_bytes(&mut out);
        out
    }

    /// Number of draws made per stream (recorded in run reports).
    pub fn draws(&self) -> &BTreeMap<String, u64> {
        &self.draws
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn streams_are_independent_and_reproducible() {
        let mut a = NamedStreams::new([7; 32]);
        let mut b = NamedStreams::new([7; 32]);
        let a1 = a.next_u64("net");
        // Extra draws on an unrelated stream do not perturb "net".
        for _ in 0..100 {
            b.next_u64("workload");
        }
        let b1 = b.next_u64("net");
        assert_eq!(a1, b1);
        assert_ne!(a.next_u64("net"), a.next_u64("clock"));
        let mut c = NamedStreams::new([8; 32]);
        assert_ne!(c.next_u64("net"), a1);
    }

    #[test]
    fn below_is_in_range() {
        let mut s = NamedStreams::new([1; 32]);
        for _ in 0..1000 {
            assert!(s.below("x", 7) < 7);
            let r = s.range("x", 3, 5);
            assert!((3..=5).contains(&r));
        }
        assert!(!s.chance("x", 0));
        assert!(s.chance("x", 1_000_000));
    }
}

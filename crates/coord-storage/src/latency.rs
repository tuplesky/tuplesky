//! Log-linear latency buckets (task-d97): the one bucketing the metrics
//! line's distributions use, kept here so the pipeline threads, which
//! time their jobs below the daemon, count in the same buckets.
//!
//! Values are microseconds. Those below [`SUB_BUCKETS`] get a bucket
//! each; above, every power of two is split into [`SUB_BUCKETS`], so a
//! value is placed within about a fifth of where it fell, up to 2^26 µs
//! (about 67 s). A longer value falls in the last bucket.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Sub-buckets a power of two of microseconds is split into.
pub const SUB_BUCKETS: usize = 4;

/// Buckets in all.
pub const BUCKETS: usize = SUB_BUCKETS + (26 - 2 + 1) * SUB_BUCKETS;

/// The bucket `micros` falls in.
pub fn bucket(micros: u64) -> usize {
    let sub = SUB_BUCKETS as u64;
    if micros < sub {
        return micros as usize;
    }
    let octave = 63 - u64::from(micros.leading_zeros());
    let within = (micros >> (octave - 2)) & (sub - 1);
    usize::try_from(sub + (octave - 2) * sub + within)
        .unwrap_or(usize::MAX)
        .min(BUCKETS - 1)
}

/// The least value, in microseconds, bucket `index` holds.
pub fn lower(index: usize) -> u64 {
    if index < SUB_BUCKETS {
        return index as u64;
    }
    let octave = ((index - SUB_BUCKETS) / SUB_BUCKETS) as u64 + 2;
    let within = ((index - SUB_BUCKETS) % SUB_BUCKETS) as u64;
    (SUB_BUCKETS as u64 + within) << (octave - 2)
}

/// The bucket a wait falls in.
pub fn bucket_of(wait: Duration) -> usize {
    bucket(u64::try_from(wait.as_micros()).unwrap_or(u64::MAX))
}

/// Bucket counts and the longest value, kept by one thread and read by
/// another: what a pipeline thread records each job's times into.
#[derive(Debug)]
pub struct SharedBuckets {
    counts: [AtomicU64; BUCKETS],
    max_nanos: AtomicU64,
}

impl Default for SharedBuckets {
    fn default() -> Self {
        SharedBuckets {
            counts: std::array::from_fn(|_| AtomicU64::new(0)),
            max_nanos: AtomicU64::new(0),
        }
    }
}

impl SharedBuckets {
    /// Count `wait`.
    pub fn record(&self, wait: Duration) {
        self.counts[bucket_of(wait)].fetch_add(1, Ordering::Relaxed);
        let nanos = u64::try_from(wait.as_nanos()).unwrap_or(u64::MAX);
        self.max_nanos.fetch_max(nanos, Ordering::Relaxed);
    }

    /// The counts so far and the longest value.
    pub fn read(&self) -> Counts {
        Counts {
            buckets: std::array::from_fn(|i| self.counts[i].load(Ordering::Relaxed)),
            max: Duration::from_nanos(self.max_nanos.load(Ordering::Relaxed)),
        }
    }
}

/// A reading of [`SharedBuckets`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Counts {
    /// Values counted in each bucket.
    pub buckets: [u64; BUCKETS],
    /// The longest value counted.
    pub max: Duration,
}

impl Default for Counts {
    fn default() -> Self {
        Counts {
            buckets: [0; BUCKETS],
            max: Duration::ZERO,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_buckets_count_where_the_bucketing_says() {
        let shared = SharedBuckets::default();
        shared.record(Duration::from_micros(3));
        shared.record(Duration::from_millis(2));
        shared.record(Duration::from_millis(2));
        let read = shared.read();
        assert_eq!(read.buckets[3], 1);
        assert_eq!(read.buckets[bucket(2_000)], 2);
        assert_eq!(read.buckets.iter().sum::<u64>(), 3);
        assert_eq!(read.max, Duration::from_millis(2));
    }
}

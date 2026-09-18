//! Samples, percentiles, counters and process resources (design Sections
//! 17.14 and 22.3).
//!
//! Every metric this module cannot obtain is `None` and is named in
//! [`Resources::unavailable`]; an unavailable metric is never reported as
//! zero. Percentiles stop at p99.9: a run reports the samples it has and
//! never an extrapolated tail.

use std::path::Path;

use serde::{Deserialize, Serialize};

/// Raw latency samples in nanoseconds.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Samples {
    values: Vec<u64>,
}

impl Samples {
    /// Empty.
    pub const fn new() -> Self {
        Samples { values: Vec::new() }
    }

    /// Record one observation.
    pub fn push(&mut self, nanos: u64) {
        self.values.push(nanos);
    }

    /// Number of observations.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Whether nothing was observed.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// The raw observations in arrival order; a run preserves them.
    pub fn raw(&self) -> &[u64] {
        &self.values
    }

    /// Sum of all observations.
    pub fn total_ns(&self) -> u64 {
        self.values.iter().sum()
    }

    /// Nearest-rank percentile (`q` in `0.0..=1.0`); `None` when empty.
    pub fn percentile(&self, q: f64) -> Option<u64> {
        if self.values.is_empty() {
            return None;
        }
        let mut sorted = self.values.clone();
        sorted.sort_unstable();
        let rank = (q * sorted.len() as f64).ceil() as usize;
        Some(sorted[rank.clamp(1, sorted.len()) - 1])
    }

    /// The reported summary. A percentile with fewer samples than its rank
    /// needs is still the nearest rank of what was measured; `count` is
    /// reported beside it so no reader mistakes it for a qualified tail.
    pub fn summary(&self) -> Percentiles {
        Percentiles {
            count: self.values.len() as u64,
            min_ns: self.percentile(0.0),
            p50_ns: self.percentile(0.50),
            p95_ns: self.percentile(0.95),
            p99_ns: self.percentile(0.99),
            p999_ns: self.percentile(0.999),
            max_ns: self.percentile(1.0),
            mean_ns: if self.values.is_empty() {
                None
            } else {
                Some(self.total_ns() / self.values.len() as u64)
            },
        }
    }
}

/// Reported distribution of one measured phase.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Percentiles {
    /// Number of samples behind the percentiles.
    pub count: u64,
    /// Smallest sample.
    pub min_ns: Option<u64>,
    /// Median.
    pub p50_ns: Option<u64>,
    /// 95th percentile.
    pub p95_ns: Option<u64>,
    /// 99th percentile.
    pub p99_ns: Option<u64>,
    /// 99.9th percentile.
    pub p999_ns: Option<u64>,
    /// Largest sample.
    pub max_ns: Option<u64>,
    /// Arithmetic mean.
    pub mean_ns: Option<u64>,
}

/// Spread of one statistic over repeated trials.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Variation {
    /// Number of repetitions behind the spread.
    pub repetitions: u64,
    /// Smallest repetition value.
    pub min_ns: Option<u64>,
    /// Median repetition value.
    pub median_ns: Option<u64>,
    /// Largest repetition value.
    pub max_ns: Option<u64>,
    /// `(max - min) / median` in percent; `None` with fewer than two
    /// repetitions or a zero median.
    pub spread_percent: Option<u64>,
}

impl Variation {
    /// Spread of one statistic across repetitions.
    pub fn of(values: &[u64]) -> Variation {
        if values.is_empty() {
            return Variation::default();
        }
        let mut sorted = values.to_vec();
        sorted.sort_unstable();
        let median = sorted[sorted.len() / 2];
        let (min, max) = (sorted[0], sorted[sorted.len() - 1]);
        Variation {
            repetitions: values.len() as u64,
            min_ns: Some(min),
            median_ns: Some(median),
            max_ns: Some(max),
            spread_percent: if values.len() < 2 || median == 0 {
                None
            } else {
                Some((max - min) * 100 / median)
            },
        }
    }
}

/// Everything a run counts beside latency: offered work, what was refused
/// and what the harness itself did. Overload is visible here, never hidden
/// by observing only admitted requests.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Counters {
    /// Operations the arrival schedule offered.
    pub offered: u64,
    /// Operations that reached the engine as new work.
    pub admitted: u64,
    /// Operations a retained retry result answered without executing.
    pub retained_retries: u64,
    /// Operations the common layers refused (denied, not admitted, guard).
    pub rejected: u64,
    /// Operations that failed with an engine or apply error.
    pub errors: u64,
    /// Revisions published to the watch hub.
    pub published_revisions: u64,
    /// Events delivered to the subscribed watch.
    pub published_events: u64,
    /// Maintenance (garbage-collection) steps executed with the workload.
    pub maintenance_steps: u64,
    /// Maintenance steps that still had work left when the phase ended.
    pub maintenance_debt_steps: u64,
    /// Largest number of operations past their scheduled arrival at once.
    pub max_backlog: u64,
    /// Pinned-snapshot stability checks performed.
    pub pinned_checks: u64,
}

/// Process and device counters. Each field is `None` when this platform
/// does not expose it, and the reason is listed in `unavailable`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Resources {
    /// CPU time on processor, nanoseconds (`/proc/self/schedstat`).
    pub cpu_ns: Option<u64>,
    /// Peak resident set size in bytes (`VmHWM`).
    pub peak_rss_bytes: Option<u64>,
    /// Logical key plus value bytes the workload handed to the engine.
    pub logical_bytes_written: u64,
    /// Bytes the process actually sent to the storage layer
    /// (`/proc/self/io` `write_bytes`).
    pub physical_write_bytes: Option<u64>,
    /// Bytes the engine's directory occupies after the run.
    pub engine_bytes: Option<u64>,
    /// Metrics this platform did not expose.
    pub unavailable: Vec<String>,
}

fn read_first_field_u64(path: &str) -> Option<u64> {
    std::fs::read_to_string(path)
        .ok()?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

fn read_kv_kb(path: &str, field: &str) -> Option<u64> {
    let text = std::fs::read_to_string(path).ok()?;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix(field) {
            let value: u64 = rest
                .trim_start_matches(':')
                .split_whitespace()
                .next()?
                .parse()
                .ok()?;
            return Some(value * 1024);
        }
    }
    None
}

fn read_proc_io(field: &str) -> Option<u64> {
    let text = std::fs::read_to_string("/proc/self/io").ok()?;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix(field) {
            return rest.trim_start_matches(':').trim().parse().ok();
        }
    }
    None
}

/// Recursive size of a directory in bytes.
pub fn directory_bytes(path: &Path) -> Option<u64> {
    let mut total = 0;
    let mut stack = vec![path.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).ok()? {
            let entry = entry.ok()?;
            let meta = entry.metadata().ok()?;
            if meta.is_dir() {
                stack.push(entry.path());
            } else {
                total += meta.len();
            }
        }
    }
    Some(total)
}

/// A process-counter reading taken at a phase boundary.
#[derive(Clone, Copy, Debug, Default)]
pub struct ProcessCounters {
    cpu_ns: Option<u64>,
    write_bytes: Option<u64>,
}

impl ProcessCounters {
    /// Read the counters now.
    pub fn read() -> ProcessCounters {
        ProcessCounters {
            cpu_ns: read_first_field_u64("/proc/self/schedstat"),
            write_bytes: read_proc_io("write_bytes"),
        }
    }

    /// Resources consumed between `self` and a later reading.
    pub fn since(&self, later: &ProcessCounters, logical_bytes: u64) -> Resources {
        let mut unavailable = Vec::new();
        let cpu_ns = match (self.cpu_ns, later.cpu_ns) {
            (Some(a), Some(b)) => Some(b.saturating_sub(a)),
            _ => {
                unavailable.push("cpu_ns (/proc/self/schedstat)".to_owned());
                None
            }
        };
        let physical_write_bytes = match (self.write_bytes, later.write_bytes) {
            (Some(a), Some(b)) => Some(b.saturating_sub(a)),
            _ => {
                unavailable.push("physical_write_bytes (/proc/self/io)".to_owned());
                None
            }
        };
        let peak_rss_bytes = read_kv_kb("/proc/self/status", "VmHWM");
        if peak_rss_bytes.is_none() {
            unavailable.push("peak_rss_bytes (/proc/self/status VmHWM)".to_owned());
        }
        // No free-space syscall is linked here; an experiment records the
        // engine's own footprint and leaves device free space unavailable
        // rather than guessing it.
        unavailable.push("free_disk_bytes (no statvfs binding linked)".to_owned());
        unavailable.push("device write counters (/proc/diskstats not attributed)".to_owned());
        Resources {
            cpu_ns,
            peak_rss_bytes,
            logical_bytes_written: logical_bytes,
            physical_write_bytes,
            engine_bytes: None,
            unavailable,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_are_nearest_rank_and_absent_when_unmeasured() {
        let empty = Samples::new();
        assert_eq!(empty.summary().p50_ns, None);
        assert_eq!(empty.summary().count, 0);
        let mut s = Samples::new();
        for v in 1..=100u64 {
            s.push(v * 10);
        }
        let p = s.summary();
        assert_eq!(p.count, 100);
        assert_eq!(p.min_ns, Some(10));
        assert_eq!(p.p50_ns, Some(500));
        assert_eq!(p.p95_ns, Some(950));
        assert_eq!(p.p99_ns, Some(990));
        // Nearest rank of 100 samples cannot exceed the largest sample; the
        // count beside it says the 99.9th percentile is not qualified.
        assert_eq!(p.p999_ns, Some(1000));
        assert_eq!(p.max_ns, Some(1000));
    }

    #[test]
    fn variation_needs_two_repetitions() {
        assert_eq!(Variation::of(&[]).repetitions, 0);
        assert_eq!(Variation::of(&[5]).spread_percent, None);
        let v = Variation::of(&[10, 12, 14]);
        assert_eq!(v.median_ns, Some(12));
        assert_eq!(v.spread_percent, Some(33));
    }

    #[test]
    fn unavailable_counters_are_none_and_named() {
        let before = ProcessCounters::default();
        let after = ProcessCounters::default();
        let r = before.since(&after, 42);
        assert_eq!(r.cpu_ns, None);
        assert_eq!(r.physical_write_bytes, None);
        assert_eq!(r.logical_bytes_written, 42);
        assert!(r.unavailable.iter().any(|u| u.starts_with("cpu_ns")));
        assert!(
            r.unavailable
                .iter()
                .any(|u| u.starts_with("physical_write_bytes"))
        );
    }
}

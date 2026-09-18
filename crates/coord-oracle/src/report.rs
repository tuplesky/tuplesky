//! Verdicts and, separately, latency statistics.

use serde::{Deserialize, Serialize};

use crate::history::OpId;

/// A detected violation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Violation {
    /// A response arrived for an unknown or already-responded invocation.
    MalformedHistory {
        /// Explanation.
        detail: String,
    },
    /// Two distinct acknowledged mutations report the same revision.
    SharedRevision {
        /// First operation.
        first: OpId,
        /// Second operation.
        second: OpId,
        /// Revision.
        revision: u64,
    },
    /// The same retry identity produced two different acknowledged results
    /// (a duplicate execution or a changed payload accepted).
    RetryInconsistent {
        /// First operation.
        first: OpId,
        /// Second operation.
        second: OpId,
    },
    /// Watch batches for one revision disagree with each other.
    ConflictingWatchBatches {
        /// Revision.
        revision: u64,
    },
    /// Watch delivery order is not increasing by revision.
    WatchOrder {
        /// Earlier revision delivered after a later one.
        revision: u64,
    },
    /// No sequential execution of the reference model matches the
    /// responses, real-time order and watch batches.
    NotLinearizable {
        /// Number of completed operations the best partial order explained.
        explained: usize,
        /// Total completed operations.
        completed: usize,
        /// Operation that could not be placed in the best attempt, if any.
        stuck_at: Option<OpId>,
    },
}

/// Correctness verdict. Contains no performance data.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Verdict {
    /// Violations found; empty means the history is accepted.
    pub violations: Vec<Violation>,
    /// Search nodes explored (diagnostic, not a correctness signal).
    pub search_nodes: u64,
    /// A witness sequential order when accepted.
    pub witness: Vec<OpId>,
}

impl Verdict {
    /// Whether the history is accepted.
    pub fn ok(&self) -> bool {
        self.violations.is_empty()
    }
}

/// Latency statistics derived from invocation/response ticks. Reported
/// separately from the verdict so a fast wrong history never looks good.
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct LatencyReport {
    /// Completed operations.
    pub completed: usize,
    /// Pending operations (no response).
    pub pending: usize,
    /// Minimum latency in ticks.
    pub min: Option<u64>,
    /// Median latency in ticks.
    pub p50: Option<u64>,
    /// 99th percentile latency in ticks.
    pub p99: Option<u64>,
    /// Maximum latency in ticks.
    pub max: Option<u64>,
}

impl LatencyReport {
    /// Compute from `(invoke_tick, response_tick)` pairs and a pending count.
    pub fn from_samples(mut samples: Vec<u64>, pending: usize) -> Self {
        samples.sort_unstable();
        let pick = |q: f64| -> Option<u64> {
            if samples.is_empty() {
                return None;
            }
            let idx = ((samples.len() as f64 - 1.0) * q).round() as usize;
            Some(samples[idx.min(samples.len() - 1)])
        };
        LatencyReport {
            completed: samples.len(),
            pending,
            min: samples.first().copied(),
            p50: pick(0.5),
            p99: pick(0.99),
            max: samples.last().copied(),
        }
    }
}

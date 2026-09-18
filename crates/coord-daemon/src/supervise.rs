//! Bounded supervised workers (design Section 22.1): a crashing worker is
//! retried within a restart budget over a window; exceeding the budget
//! quarantines the process rather than spinning.

use std::collections::BTreeMap;
use std::collections::VecDeque;

/// A worker identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct WorkerId(pub u32);

/// Why a worker stopped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerError {
    /// A transient failure: retry within the budget.
    Transient(String),
    /// A fatal failure: quarantine now, whatever the budget.
    Fatal(String),
}

/// A restart budget over a window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RestartBudget {
    /// Restarts allowed in the window.
    pub max_restarts: u32,
    /// The window, in ticks (milliseconds).
    pub window_ticks: u64,
}

impl Default for RestartBudget {
    fn default() -> Self {
        RestartBudget {
            max_restarts: 5,
            window_ticks: 30_000,
        }
    }
}

/// What the supervisor decided after a worker stopped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Decision {
    /// Restart the worker after `after_ticks`.
    Restart {
        /// Backoff.
        after_ticks: u64,
    },
    /// The budget is spent (or the failure was fatal): quarantine.
    Quarantine {
        /// Reason.
        reason: String,
    },
}

struct WorkerState {
    restarts: VecDeque<u64>,
    backoff: u64,
}

/// The supervisor of one process's workers.
pub struct Supervisor {
    budget: RestartBudget,
    base_backoff: u64,
    workers: BTreeMap<WorkerId, WorkerState>,
}

impl Supervisor {
    /// A supervisor with `budget` and a base backoff.
    pub fn new(budget: RestartBudget, base_backoff_ticks: u64) -> Self {
        Supervisor {
            budget,
            base_backoff: base_backoff_ticks.max(1),
            workers: BTreeMap::new(),
        }
    }

    /// Register a worker.
    pub fn register(&mut self, id: WorkerId) {
        self.workers.entry(id).or_insert(WorkerState {
            restarts: VecDeque::new(),
            backoff: self.base_backoff,
        });
    }

    /// A worker stopped at `now` with `error`; decide what to do.
    pub fn on_stop(&mut self, id: WorkerId, now: u64, error: &WorkerError) -> Decision {
        let budget = self.budget;
        let base = self.base_backoff;
        let state = self.workers.entry(id).or_insert(WorkerState {
            restarts: VecDeque::new(),
            backoff: base,
        });
        if let WorkerError::Fatal(reason) = error {
            return Decision::Quarantine {
                reason: reason.clone(),
            };
        }
        while let Some(&front) = state.restarts.front() {
            if now.saturating_sub(front) > budget.window_ticks {
                state.restarts.pop_front();
            } else {
                break;
            }
        }
        if state.restarts.len() as u32 >= budget.max_restarts {
            return Decision::Quarantine {
                reason: format!("worker {} exceeded {} restarts", id.0, budget.max_restarts),
            };
        }
        state.restarts.push_back(now);
        let after = state.backoff;
        state.backoff = (state.backoff.saturating_mul(2)).min(budget.window_ticks.max(base));
        Decision::Restart { after_ticks: after }
    }

    /// A worker ran cleanly for long enough: reset its backoff.
    pub fn on_stable(&mut self, id: WorkerId) {
        if let Some(state) = self.workers.get_mut(&id) {
            state.backoff = self.base_backoff;
            state.restarts.clear();
        }
    }

    /// Restarts recorded for a worker in the current window.
    pub fn restarts(&self, id: WorkerId) -> usize {
        self.workers.get(&id).map_or(0, |s| s.restarts.len())
    }
}

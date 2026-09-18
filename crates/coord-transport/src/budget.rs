//! Shared destination and node budgets (task-31; design Sections 3.3,
//! 11.3): bytes handed to QUIC and unacknowledged, plus stream opens in
//! flight, are bounded per destination across every lane and connection
//! to it, and per node across every destination, so more connections
//! never multiply the allowed window. A reserve of each budget is usable
//! only by the control lane: a saturated bulk or watch lane cannot take
//! the bytes recovery and votes need.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::lane::Lane;

/// Budget configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BudgetLimits {
    /// Bytes in flight per destination (all lanes and connections).
    pub destination_bytes: usize,
    /// Bytes in flight across every destination.
    pub node_bytes: usize,
    /// Part of each budget only the control lane may use.
    pub control_reserve: usize,
    /// Stream opens in flight per destination.
    pub max_opens: usize,
}

impl Default for BudgetLimits {
    fn default() -> Self {
        BudgetLimits {
            destination_bytes: 16 * 1024 * 1024,
            node_bytes: 64 * 1024 * 1024,
            control_reserve: 2 * 1024 * 1024,
            max_opens: 512,
        }
    }
}

/// Why a frame cannot be admitted at all (it would never fit).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BudgetError {
    /// Larger than what the lane may ever hold in flight.
    TooLarge {
        /// Requested bytes.
        bytes: usize,
        /// Bytes the lane may hold at most.
        limit: usize,
    },
}

#[derive(Debug, Default)]
struct Counters {
    in_flight: AtomicUsize,
    peak: AtomicUsize,
}

/// A byte budget with a control-only reserve.
#[derive(Debug)]
pub struct Budget {
    /// Every byte in flight (capacity).
    all: Arc<Semaphore>,
    /// Bytes non-control lanes may hold (capacity minus the reserve).
    shared: Arc<Semaphore>,
    capacity: usize,
    reserve: usize,
    counters: Arc<Counters>,
}

impl Budget {
    /// A budget of `capacity` bytes with `reserve` for control.
    pub fn new(capacity: usize, reserve: usize) -> Self {
        let capacity = capacity.max(1);
        let reserve = reserve.min(capacity);
        Budget {
            all: Arc::new(Semaphore::new(capacity)),
            shared: Arc::new(Semaphore::new(capacity - reserve)),
            capacity,
            reserve,
            counters: Arc::new(Counters::default()),
        }
    }

    /// Most bytes a lane may hold in flight at once.
    pub const fn limit(&self, lane: Lane) -> usize {
        match lane {
            Lane::Control => self.capacity,
            _ => self.capacity - self.reserve,
        }
    }

    /// Bytes in flight now.
    pub fn in_flight(&self) -> usize {
        self.counters.in_flight.load(Ordering::SeqCst)
    }

    /// Most bytes ever in flight.
    pub fn peak(&self) -> usize {
        self.counters.peak.load(Ordering::SeqCst)
    }

    /// Reject up front what could never be admitted.
    pub fn check(&self, lane: Lane, bytes: usize) -> Result<(), BudgetError> {
        let limit = self.limit(lane);
        if bytes > limit || u32::try_from(bytes).is_err() {
            return Err(BudgetError::TooLarge { bytes, limit });
        }
        Ok(())
    }

    /// Wait for `bytes` of budget for `lane`.
    pub async fn acquire(&self, lane: Lane, bytes: usize) -> Result<BytesPermit, BudgetError> {
        self.check(lane, bytes)?;
        let n = bytes as u32;
        let shared = match lane {
            Lane::Control => None,
            _ => Some(
                self.shared
                    .clone()
                    .acquire_many_owned(n)
                    .await
                    .expect("budget semaphore never closes"),
            ),
        };
        let all = self
            .all
            .clone()
            .acquire_many_owned(n)
            .await
            .expect("budget semaphore never closes");
        let now = self.counters.in_flight.fetch_add(bytes, Ordering::SeqCst) + bytes;
        self.counters.peak.fetch_max(now, Ordering::SeqCst);
        Ok(BytesPermit {
            _all: all,
            _shared: shared,
            bytes,
            counters: self.counters.clone(),
        })
    }
}

/// Bytes held in flight; released when dropped.
#[derive(Debug)]
pub struct BytesPermit {
    _all: OwnedSemaphorePermit,
    _shared: Option<OwnedSemaphorePermit>,
    bytes: usize,
    counters: Arc<Counters>,
}

impl Drop for BytesPermit {
    fn drop(&mut self) {
        self.counters
            .in_flight
            .fetch_sub(self.bytes, Ordering::SeqCst);
    }
}

/// Stream opens in flight per destination.
#[derive(Debug)]
pub struct Opens {
    permits: Arc<Semaphore>,
}

impl Opens {
    /// Up to `max` opens.
    pub fn new(max: usize) -> Self {
        Opens {
            permits: Arc::new(Semaphore::new(max.max(1))),
        }
    }

    /// Wait for an open slot.
    pub async fn acquire(&self) -> OwnedSemaphorePermit {
        self.permits
            .clone()
            .acquire_owned()
            .await
            .expect("opens semaphore never closes")
    }
}

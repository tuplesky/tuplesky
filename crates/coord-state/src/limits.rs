//! Semantic work and response limits (design Section 19.3).

/// Limits applied by the planner. Replicated semantic limits, not local
/// scheduling budgets: lowering a local budget backpressures but cannot
/// change a chosen result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlanLimits {
    /// Maximum encoded response size estimate in bytes.
    pub max_response_bytes: usize,
    /// Maximum events one revision may carry.
    pub max_events_per_revision: usize,
    /// Maximum keys a single range delete may remove.
    pub max_delete_keys: usize,
}

impl Default for PlanLimits {
    fn default() -> Self {
        PlanLimits {
            max_response_bytes: 8 * 1024 * 1024,
            max_events_per_revision: 4096,
            max_delete_keys: 4096,
        }
    }
}

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
    /// Maximum keys attached to one lease.
    pub max_lease_attachments: u32,
    /// Maximum worst-case deletion/event bytes of one lease's attachments
    /// (see [`attachment_cost`]), rechecked on every write of an attached
    /// key. Revoking a lease writes all of it in one batch, so this must
    /// stay below the storage worker's single-batch limit with headroom
    /// for the batch's fixed rows (lease record, revision counter and retry
    /// binding).
    ///
    /// [`attachment_cost`]: crate::attachment_cost
    pub max_lease_bytes: u64,
}

impl Default for PlanLimits {
    fn default() -> Self {
        PlanLimits {
            max_response_bytes: 8 * 1024 * 1024,
            max_events_per_revision: 4096,
            max_delete_keys: 4096,
            max_lease_attachments: 128,
            // One MiB under the worker's 8 MiB single-batch default.
            max_lease_bytes: 7 * 1024 * 1024,
        }
    }
}

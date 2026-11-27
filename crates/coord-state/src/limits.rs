//! Semantic work and response limits (design Section 19.3).

/// The largest response that can be retained for a retry.
///
/// A retained result is stored whole inside one durable envelope, so
/// this cannot exceed what that envelope holds. Nothing bounded it
/// before: a response the planner accepted could be impossible to
/// persist as its retry record, and the chosen command then failed after
/// planning with a storage error rather than an ordered outcome,
/// leaving it unresolved with every successor behind it. Exceeding it is
/// an ordinary terminal rejection, decided before anything is committed.
///
/// This bounds the response alone. The events a mutation produces are
/// stored in their own rows and are covered by `max_response_bytes`
/// together with the response, which is the planner's work budget and
/// not a durable representation.
///
/// `coord-storage` checks this against the envelope's own limit, which
/// it can see and this crate cannot.
pub const MAX_RETAINED_RESPONSE_BYTES: usize = 2 * 1024 * 1024 + 8 * 1024;

/// The most worst-case revocation bytes one lease may hold
/// ([`PlanLimits::max_lease_bytes`]): one MiB under a journal record's
/// 4 MiB, so revoking or expiring a lease at its quota is always one
/// record.
///
/// It was 7 MiB, one MiB under the projection worker's 8 MiB single
/// batch. But a node journals every command as one record, and a record
/// holds 4 MiB, so a revoke or an expiry of a lease past about 4 MiB
/// could never be recorded: every voter stopped on it, and an expiry is
/// the domain's own command, so it can't be refused away. `coord-storage`
/// checks this against the record's bound, which it can see and this
/// crate cannot.
pub const MAX_LEASE_BYTES: u64 = 3 * 1024 * 1024;

/// Limits applied by the planner. Replicated semantic limits, not local
/// scheduling budgets: lowering a local budget backpressures but cannot
/// change a chosen result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlanLimits {
    /// Maximum encoded response size estimate in bytes, response and the
    /// events it produces together: the planner's work budget.
    pub max_response_bytes: usize,
    /// Maximum encoded response that can be retained for a retry. The
    /// response is stored whole in one durable envelope, so a larger one
    /// could never be persisted.
    pub max_retained_response_bytes: usize,
    /// Maximum events one revision may carry.
    pub max_events_per_revision: usize,
    /// Maximum keys a single range delete may remove.
    pub max_delete_keys: usize,
    /// Maximum keys attached to one lease.
    pub max_lease_attachments: u32,
    /// Maximum worst-case deletion/event bytes of one lease's attachments
    /// (see [`attachment_cost`]), rechecked on every write of an attached
    /// key. Revoking a lease writes all of it in one batch, which is one
    /// journal record, so this must stay below the record's bound with
    /// headroom for the batch's fixed rows (lease record, revision counter
    /// and retry binding). See [`MAX_LEASE_BYTES`].
    ///
    /// [`attachment_cost`]: crate::attachment_cost
    pub max_lease_bytes: u64,
}

impl Default for PlanLimits {
    fn default() -> Self {
        PlanLimits {
            max_response_bytes: 8 * 1024 * 1024,
            max_retained_response_bytes: MAX_RETAINED_RESPONSE_BYTES,
            max_events_per_revision: 4096,
            max_delete_keys: 4096,
            max_lease_attachments: 128,
            max_lease_bytes: MAX_LEASE_BYTES,
        }
    }
}

/// Outstanding retry window a session established from an admission
/// receipt is created with.
///
/// Replicated, not configured: the window is written into the session
/// row by the command that creates it, and every replica plans that
/// command from the same receipt. A per-node setting here would make
/// the row -- and therefore every later admission decision against it --
/// depend on which node happened to plan the establishment.
///
/// It matches `coord-storage`'s default window, which is what a session
/// that never named one is admitted under, so establishing a session
/// explicitly does not silently change what its clients may have
/// outstanding.
pub const SESSION_RETRY_WINDOW: u32 = 1024;

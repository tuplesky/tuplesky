//! Native lease records (design Sections 7.1, 17.1).
//!
//! A lease is owned by a principal, never by a session or connection. Its
//! identity is derived at the trusted boundary from the stable request, so
//! retries reproduce it; a revoked lease keeps a tombstone record so its
//! identity and generation are never reused. Attachment accounting (count
//! and worst-case deletion/event bytes) lives in the record and is
//! rechecked on every write of an attached key, not only on attachment, so
//! a lease can never grow beyond what one atomic revocation may delete.

use coord_types::ids::{LeaseGeneration, NamespaceId, PrincipalId};
use serde::{Deserialize, Serialize};

/// Fixed per-event overhead charged to a lease's deletion budget besides
/// key and value bytes (metadata of the previous entry in the event).
pub const ATTACHMENT_OVERHEAD: u64 = 48;

/// What a lease record is for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LeasePurpose {
    /// A native lease granted through the public API.
    Native,
    /// A hidden Kine per-key TTL binding (task-17). Invisible to native
    /// lease operations: it cannot be attached to, inspected or revoked.
    KinePrivate,
}

/// Lifecycle state of a lease (Section 7.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LeaseStatus {
    /// Granted and not yet ended.
    Active,
    /// Explicitly revoked; the record is a tombstone.
    Revoked,
    /// Expired by a matching conditional expiration (task-16).
    Expired,
    /// A Kine private binding superseded by a later write of its key
    /// (task-17); the record is a tombstone.
    Replaced,
}

/// The replicated lease record stored in `lease_v1`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LeaseRecord {
    /// Namespace the lease was granted in; attachments never cross it.
    pub namespace: NamespaceId,
    /// Ownership generation; never reused for the same lease identity.
    pub generation: LeaseGeneration,
    /// Owning principal.
    pub owner: PrincipalId,
    /// Granted TTL in seconds.
    pub ttl_seconds: u32,
    /// Committed renewal count (task-16 increments it).
    pub renewal_sequence: u64,
    /// Purpose.
    pub purpose: LeasePurpose,
    /// Lifecycle state.
    pub status: LeaseStatus,
    /// Number of currently attached keys.
    pub attached_keys: u32,
    /// Worst-case bytes one revocation must delete/emit for the attached keys.
    pub attached_bytes: u64,
}

impl LeaseRecord {
    /// Whether native operations may see this record.
    pub const fn is_native_active(&self) -> bool {
        matches!(self.purpose, LeasePurpose::Native) && matches!(self.status, LeaseStatus::Active)
    }
}

/// Bytes charged to a lease for one attached key with `value`.
pub fn attachment_cost(key: &[u8], value: &[u8]) -> u64 {
    key.len() as u64 + value.len() as u64 + ATTACHMENT_OVERHEAD
}

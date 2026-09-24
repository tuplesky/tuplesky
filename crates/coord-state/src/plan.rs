//! Plans, logical mutations, events and responses.

use alloc::vec::Vec;

use coord_core::effect::ApplyBase;
use coord_types::ids::{
    ExecutionPosition, KvRevision, LeaseAuthorityEpoch, LeaseGeneration, LeaseId,
};
use serde::{Deserialize, Serialize};

use crate::lease::LeaseRecord;
use crate::view::KvEntry;

/// One item of a range result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeItem {
    /// Key.
    pub key: Vec<u8>,
    /// Entry (value empty when keys-only).
    pub entry: KvEntry,
}

/// Operation outcome.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Outcome {
    /// Put applied.
    Put {
        /// Previous entry when requested and present.
        prev: Option<KvEntry>,
    },
    /// Delete applied (possibly to zero keys).
    Delete {
        /// Keys deleted.
        deleted: u64,
        /// Previous entries when requested.
        prev: Vec<RangeItem>,
    },
    /// Read.
    Range {
        /// Items (empty when count-only).
        items: Vec<RangeItem>,
        /// Total matching keys.
        count: u64,
        /// More items exist beyond the limit.
        more: bool,
    },
    /// Transaction.
    Txn {
        /// Whether the success branch ran.
        succeeded: bool,
        /// Outcomes of the executed branch, in order.
        results: Vec<Outcome>,
    },
    /// Compaction floor advanced (or already at/above the target).
    Compacted,
    /// Requested revision is below the compaction floor.
    ErrCompacted,
    /// Requested revision is above the current revision.
    ErrFutureRevision,
    /// A native lease was granted.
    LeaseGranted {
        /// Lease identity (derived from the stable request).
        lease_id: LeaseId,
        /// Ownership generation.
        generation: LeaseGeneration,
        /// Granted TTL in seconds.
        ttl_seconds: u32,
    },
    /// A lease was revoked and its attached keys deleted atomically.
    LeaseRevoked {
        /// Keys deleted.
        deleted: u64,
    },
    /// Authoritative lease existence, generation and granted TTL. The
    /// remaining time is a separately labelled scheduler estimate supplied
    /// outside deterministic application (task-16), never part of the plan.
    LeaseTimeToLive {
        /// Lease identity.
        lease_id: LeaseId,
        /// Ownership generation.
        generation: LeaseGeneration,
        /// Granted TTL in seconds.
        granted_ttl_seconds: u32,
        /// Committed renewals.
        renewal_sequence: u64,
        /// Attached keys in order, when requested.
        keys: Option<Vec<Vec<u8>>>,
    },
    /// The named lease does not exist, is not active or is not visible in
    /// this namespace.
    ErrLeaseNotFound,
    /// A grant named an identity that already has a record (live or tombstone).
    ErrLeaseExists,
    /// The principal does not own the lease.
    ErrLeasePermission,
    /// Attachment count or deletion/event byte quota exceeded.
    ErrLeaseQuota,
    /// A replicated renewal committed (Section 7.1): the new renewal
    /// sequence is the anchor a scheduler rearms from.
    LeaseKeptAlive {
        /// Lease identity.
        lease_id: LeaseId,
        /// Ownership generation.
        generation: LeaseGeneration,
        /// Renewal sequence after this renewal.
        renewal_sequence: u64,
        /// Granted TTL in seconds.
        ttl_seconds: u32,
    },
    /// A conditional expiration matched and deleted the attachments.
    LeaseExpired {
        /// Keys deleted.
        deleted: u64,
    },
    /// A conditional expiration did not match (the lease was renewed,
    /// revoked, already ended or never existed): nothing changed.
    ExpireStale,
    /// A new lease expiry authority epoch was established.
    LeaseAuthorityEstablished {
        /// The epoch now in force.
        epoch: LeaseAuthorityEpoch,
    },
    /// The command carried an authority epoch that is not the current one
    /// (a former leader's expiration, or a stale establishment).
    ErrStaleAuthority,
}

/// Response to the client: header revision plus outcome.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Response {
    /// Domain revision after the command.
    pub revision: KvRevision,
    /// Outcome.
    pub outcome: Outcome,
}

/// Event kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum KvEventKind {
    /// Put.
    Put,
    /// Delete.
    Delete,
}

/// One event of the plan's revision.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvEvent {
    /// Kind.
    pub kind: KvEventKind,
    /// Key.
    pub key: Vec<u8>,
    /// New entry for puts.
    pub entry: Option<KvEntry>,
    /// Previous entry, if the key existed.
    pub prev: Option<KvEntry>,
}

/// A logical mutation for common storage to lower into rows.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Mutation {
    /// Write the current entry and a history version.
    Write {
        /// Key.
        key: Vec<u8>,
        /// New entry.
        entry: KvEntry,
    },
    /// Remove the current entry and record a tombstone version.
    Delete {
        /// Key.
        key: Vec<u8>,
        /// Entry removed.
        prev: KvEntry,
    },
    /// Attach the key to a lease, or refresh the binding of an already
    /// attached key (reverse index maintenance).
    LeaseAttach {
        /// Lease.
        lease: LeaseId,
        /// Key.
        key: Vec<u8>,
        /// Lease generation bound.
        generation: LeaseGeneration,
        /// Modification revision of the bound entry.
        mod_revision: KvRevision,
    },
    /// Detach the key from a lease.
    LeaseDetach {
        /// Lease.
        lease: LeaseId,
        /// Key.
        key: Vec<u8>,
    },
    /// Write (create or replace) a lease record.
    LeaseWrite {
        /// Lease.
        lease: LeaseId,
        /// Complete new record.
        record: LeaseRecord,
    },
    /// Establish the lease expiry authority epoch.
    LeaseAuthority {
        /// New epoch.
        epoch: LeaseAuthorityEpoch,
    },
    /// Advance the MVCC retention floor.
    CompactTo {
        /// New floor.
        revision: KvRevision,
    },
}

/// A deterministic application plan.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplyPlan {
    /// Base the plan was computed against; materialization rechecks it.
    pub base: ApplyBase,
    /// Execution position this command occupies.
    pub position: ExecutionPosition,
    /// New KV revision when the plan mutates KV.
    pub revision: Option<KvRevision>,
    /// Logical mutations, in order.
    pub mutations: Vec<Mutation>,
    /// Complete event set of `revision` (empty for non-mutations).
    pub events: Vec<KvEvent>,
    /// Response.
    pub response: Response,
}

impl ApplyPlan {
    /// Whether the plan changes KV.
    pub const fn mutates(&self) -> bool {
        self.revision.is_some()
    }
}

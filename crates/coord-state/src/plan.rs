//! Plans, logical mutations, events and responses.

use alloc::vec::Vec;

use coord_core::effect::ApplyBase;
use coord_types::ids::{ExecutionPosition, KvRevision, LeaseId};
use serde::{Deserialize, Serialize};

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
    /// Attach the key to a lease (reverse index maintenance).
    LeaseAttach {
        /// Lease.
        lease: LeaseId,
        /// Key.
        key: Vec<u8>,
    },
    /// Detach the key from a lease.
    LeaseDetach {
        /// Lease.
        lease: LeaseId,
        /// Key.
        key: Vec<u8>,
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

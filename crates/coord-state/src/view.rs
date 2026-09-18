//! Owned read views.

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::vec::Vec;

use coord_core::effect::ApplyBase;
use coord_types::ids::{KvRevision, LeaseGeneration, LeaseId, NamespaceId};
use serde::{Deserialize, Serialize};

/// A stored key's entry.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct KvEntry {
    /// Value.
    pub value: Vec<u8>,
    /// Creation revision.
    pub create_revision: KvRevision,
    /// Last modification revision.
    pub mod_revision: KvRevision,
    /// Version counter (1 at creation).
    pub version: u64,
    /// Attached lease, if any.
    pub lease: Option<LeaseId>,
    /// Generation of the attached lease.
    pub lease_generation: Option<LeaseGeneration>,
}

/// A historical snapshot of a key interval at one revision: greatest version
/// of each key at or below the revision, tombstones excluded. Built by
/// common storage (Section 17.2); the planner only paginates it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalView {
    /// Revision of the snapshot.
    pub revision: KvRevision,
    /// Entries in key order.
    pub entries: BTreeMap<Vec<u8>, KvEntry>,
}

/// Owned, bounded view at an established predecessor.
///
/// `current` must contain every entry the request can touch (the exact key
/// or the whole interval, and every compared key); the builder is
/// responsible for that coverage.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadView {
    /// Base this view reflects.
    pub base: ApplyBase,
    /// Namespace of the request.
    pub namespace: NamespaceId,
    /// Current domain revision.
    pub kv_revision: KvRevision,
    /// Compaction floor: history strictly below it is unavailable.
    pub compact_floor: KvRevision,
    /// Current entries covering the request's keys.
    pub current: BTreeMap<Vec<u8>, KvEntry>,
    /// Historical snapshot when the request reads an explicit revision.
    pub historical: Option<HistoricalView>,
    /// Leases known to exist (for attachment checks).
    pub leases: BTreeSet<LeaseId>,
}

impl ReadView {
    /// Empty view at a base.
    pub fn empty(base: ApplyBase, namespace: NamespaceId, kv_revision: KvRevision) -> Self {
        ReadView {
            base,
            namespace,
            kv_revision,
            compact_floor: KvRevision::ZERO,
            current: BTreeMap::new(),
            historical: None,
            leases: BTreeSet::new(),
        }
    }
}

//! Owned read views.

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::vec::Vec;

use coord_core::effect::ApplyBase;
use coord_types::identity::Digest32;
use coord_types::ids::{
    KvRevision, LeaseAuthorityEpoch, LeaseGeneration, LeaseId, NamespaceId, PrincipalId, SessionId,
    TrustRuleId,
};

use crate::lease::LeaseRecord;
use crate::policy::{Authorization, GrantRecord, SessionRecord, TrustRule};
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
    /// Principal the request executes as (from its admitted session).
    pub principal: PrincipalId,
    /// Current domain revision.
    pub kv_revision: KvRevision,
    /// Compaction floor: history strictly below it is unavailable.
    pub compact_floor: KvRevision,
    /// Replicated lease expiry authority epoch (`ZERO`: none established).
    pub lease_authority: LeaseAuthorityEpoch,
    /// Current entries covering the request's keys.
    pub current: BTreeMap<Vec<u8>, KvEntry>,
    /// Historical snapshot when the request reads an explicit revision.
    pub historical: Option<HistoricalView>,
    /// Lease records the request may touch: every lease named by the
    /// request plus every lease attached to a loaded current entry.
    /// Absent means the lease does not exist.
    pub leases: BTreeMap<LeaseId, LeaseRecord>,
    /// Attached keys of the leases the request revokes or inspects, whose
    /// current entries are also in `current`.
    pub lease_keys: BTreeMap<LeaseId, BTreeSet<Vec<u8>>>,
    /// Authorization context: `Some` for a client request executing under a
    /// session (deny by default), `None` for a trusted internal view.
    pub authorization: Option<Authorization>,
    /// Session records an internal command touches.
    pub sessions: BTreeMap<SessionId, SessionRecord>,
    /// Grant commitments an internal command touches.
    pub grants: BTreeMap<Digest32, GrantRecord>,
    /// Trust rules an internal command touches.
    pub trust_rules: BTreeMap<TrustRuleId, TrustRule>,
}

impl ReadView {
    /// Empty view at a base.
    pub fn empty(
        base: ApplyBase,
        namespace: NamespaceId,
        principal: PrincipalId,
        kv_revision: KvRevision,
    ) -> Self {
        ReadView {
            base,
            namespace,
            principal,
            kv_revision,
            compact_floor: KvRevision::ZERO,
            lease_authority: LeaseAuthorityEpoch::ZERO,
            current: BTreeMap::new(),
            historical: None,
            leases: BTreeMap::new(),
            lease_keys: BTreeMap::new(),
            authorization: None,
            sessions: BTreeMap::new(),
            grants: BTreeMap::new(),
            trust_rules: BTreeMap::new(),
        }
    }
}

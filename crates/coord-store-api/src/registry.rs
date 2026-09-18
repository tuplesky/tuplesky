//! Frozen logical collection registry (design Sections 17.1 and 17.10).
//!
//! Identifiers are assigned explicitly and never recycled. New collections
//! are appended with new numbers in a reviewed change; enum order is not
//! the identifier.

use coord_core::effect::CollectionId;

/// A registered logical collection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Collection {
    /// Origin/incarnation/genesis, formats, frontiers, applied stamp.
    MetaV1,
    /// Configuration epochs and certificates.
    ConfigV1,
    /// Immutable canonical commands by command identity.
    PayloadV1,
    /// Source protocol state by epoch, ballot, command.
    ProtocolV1,
    /// Established execution positions.
    ExecutionV1,
    /// Applied command identity/position/result.
    ExecutedV1,
    /// Current KV rows.
    KvCurrentV1,
    /// Historical KV versions.
    KvHistoryV1,
    /// Complete ordered revision events.
    EventsV1,
    /// Leases and private bindings.
    LeaseV1,
    /// Reverse lease-to-key index.
    LeaseKeysV1,
    /// Sessions.
    SessionV1,
    /// Policy.
    PolicyV1,
    /// Auth grant commitments.
    AuthGrantV1,
    /// Retained retry results.
    RetryV1,
    /// Retry floors and windows.
    RetryFloorV1,
    /// Checkpoint metadata.
    CheckpointV1,
}

impl Collection {
    /// Every registered collection, in identifier order.
    pub const ALL: [Collection; 17] = [
        Collection::MetaV1,
        Collection::ConfigV1,
        Collection::PayloadV1,
        Collection::ProtocolV1,
        Collection::ExecutionV1,
        Collection::ExecutedV1,
        Collection::KvCurrentV1,
        Collection::KvHistoryV1,
        Collection::EventsV1,
        Collection::LeaseV1,
        Collection::LeaseKeysV1,
        Collection::SessionV1,
        Collection::PolicyV1,
        Collection::AuthGrantV1,
        Collection::RetryV1,
        Collection::RetryFloorV1,
        Collection::CheckpointV1,
    ];

    /// Frozen identifier.
    pub const fn id(self) -> CollectionId {
        CollectionId(match self {
            Collection::MetaV1 => 0x0001,
            Collection::ConfigV1 => 0x0002,
            Collection::PayloadV1 => 0x0003,
            Collection::ProtocolV1 => 0x0004,
            Collection::ExecutionV1 => 0x0005,
            Collection::ExecutedV1 => 0x0006,
            Collection::KvCurrentV1 => 0x0007,
            Collection::KvHistoryV1 => 0x0008,
            Collection::EventsV1 => 0x0009,
            Collection::LeaseV1 => 0x000a,
            Collection::LeaseKeysV1 => 0x000b,
            Collection::SessionV1 => 0x000c,
            Collection::PolicyV1 => 0x000d,
            Collection::AuthGrantV1 => 0x000e,
            Collection::RetryV1 => 0x000f,
            Collection::RetryFloorV1 => 0x0010,
            Collection::CheckpointV1 => 0x0011,
        })
    }

    /// Stable name.
    pub const fn name(self) -> &'static str {
        match self {
            Collection::MetaV1 => "meta_v1",
            Collection::ConfigV1 => "config_v1",
            Collection::PayloadV1 => "payload_v1",
            Collection::ProtocolV1 => "protocol_v1",
            Collection::ExecutionV1 => "execution_v1",
            Collection::ExecutedV1 => "executed_v1",
            Collection::KvCurrentV1 => "kv_current_v1",
            Collection::KvHistoryV1 => "kv_history_v1",
            Collection::EventsV1 => "events_v1",
            Collection::LeaseV1 => "lease_v1",
            Collection::LeaseKeysV1 => "lease_keys_v1",
            Collection::SessionV1 => "session_v1",
            Collection::PolicyV1 => "policy_v1",
            Collection::AuthGrantV1 => "auth_grant_v1",
            Collection::RetryV1 => "retry_v1",
            Collection::RetryFloorV1 => "retry_floor_v1",
            Collection::CheckpointV1 => "checkpoint_v1",
        }
    }

    /// Whether the collection participates in the common (cross-replica)
    /// state hash. Node-private collections are excluded from common
    /// digests (Section 17.6).
    pub const fn in_common_hash(self) -> bool {
        !matches!(
            self,
            Collection::MetaV1 | Collection::ProtocolV1 | Collection::CheckpointV1
        )
    }

    /// Look up by identifier.
    pub fn from_id(id: CollectionId) -> Option<Collection> {
        Collection::ALL.iter().copied().find(|c| c.id() == id)
    }
}

/// ASCII field names used as keys inside `meta_v1`.
pub mod meta_fields {
    /// Cluster/restore identity.
    pub const CLUSTER_ID: &[u8] = b"cluster_id";
    /// Domain identity.
    pub const DOMAIN_ID: &[u8] = b"domain_id";
    /// Replica identity.
    pub const REPLICA_ID: &[u8] = b"replica_id";
    /// Replica incarnation.
    pub const INCARNATION: &[u8] = b"incarnation";
    /// Store format version.
    pub const FORMAT_VERSION: &[u8] = b"format_version";
    /// Selected engine name.
    pub const ENGINE: &[u8] = b"engine";
    /// Selected durability profile.
    pub const PROFILE: &[u8] = b"profile";
    /// Applied stamp (`AppliedStamp`).
    pub const APPLIED_STAMP: &[u8] = b"applied_stamp";
    /// Execution frontier.
    pub const EXECUTION_FRONTIER: &[u8] = b"execution_frontier";
    /// KV revision frontier.
    pub const KV_REVISION: &[u8] = b"kv_revision";
    /// MVCC retention floor.
    pub const RETENTION_FLOOR: &[u8] = b"retention_floor";
    /// Replicated lease expiry authority epoch (Section 7.2).
    pub const LEASE_AUTHORITY: &[u8] = b"lease_authority";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_unique_dense_and_frozen() {
        for (i, c) in Collection::ALL.iter().enumerate() {
            assert_eq!(c.id().0 as usize, i + 1, "{c:?}");
            assert_eq!(Collection::from_id(c.id()), Some(*c));
        }
        assert_eq!(Collection::from_id(CollectionId(0)), None);
        assert_eq!(Collection::from_id(CollectionId(0x0012)), None);
        assert!(!Collection::MetaV1.in_common_hash());
        assert!(Collection::KvCurrentV1.in_common_hash());
    }
}

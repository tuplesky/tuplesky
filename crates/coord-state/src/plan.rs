//! Plans, logical mutations, events and responses.

use alloc::vec::Vec;

use coord_core::effect::ApplyBase;
use coord_types::identity::Digest32;
use coord_types::ids::{
    ExecutionPosition, KvRevision, LeaseAuthorityEpoch, LeaseGeneration, LeaseId, SessionId,
};
use serde::{Deserialize, Serialize};

use crate::lease::LeaseRecord;
use crate::policy::{GrantRecord, PolicyRule, SessionRecord, TrustRule};
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
    /// Kine create succeeded; the header revision is the creation revision.
    KineCreated,
    /// Kine create found the key present (exact duplicate-key result).
    ErrKeyExists,
    /// Kine compare-and-update result from one execution point.
    KineUpdated {
        /// Whether the update applied.
        updated: bool,
        /// The current entry after the operation (`None`: key absent).
        current: Option<KineKv>,
    },
    /// Kine conditional delete result from one execution point.
    KineDeleted {
        /// Whether the key is gone (absent keys report `true`, as the
        /// reference bridge does).
        deleted: bool,
        /// The entry the operation saw (`None`: key absent).
        prev: Option<KineKv>,
    },
    /// The session cannot execute: unknown, retired, or its trust rule is
    /// disabled or regenerated.
    ErrSessionInvalid,
    /// Current policy does not permit the operation (or the selected
    /// branch, or a comparison) at this execution point.
    ErrPermissionDenied,
    /// A session was created from a consumed receipt.
    SessionCreated {
        /// The session.
        session: SessionId,
    },
    /// A session was retired.
    SessionRetired,
    /// The receipt was already consumed or the session identity exists.
    ErrReceiptConsumed,
    /// The trust rule named by a receipt is missing, disabled or at another
    /// generation; or a trust-rule write would move its generation backward.
    ErrTrustRuleInvalid,
    /// A grant commitment was ordered.
    GrantCommitted,
    /// The commitment already exists.
    ErrGrantExists,
    /// The named grant is not pending (unknown, consumed or revoked).
    ErrGrantUnavailable,
    /// A refresh family rotated to a new secret commitment.
    RefreshAdvanced {
        /// Generation after rotation.
        generation: u64,
    },
    /// A retired refresh secret was presented: the family and its session
    /// are revoked.
    ErrRefreshReuse,
    /// A policy or trust rule was written or removed.
    PolicyUpdated,
    /// The command was rejected at execution by a deterministic property
    /// of the request and the state it executes against. It occupied its
    /// execution position and produced this result; it changed nothing,
    /// and executing it again can only reject it again.
    ErrRejected {
        /// What was rejected.
        reason: RejectionReason,
    },
}

/// Why a chosen command was rejected at execution. Every case is a
/// deterministic function of the request and the state, so the rejection
/// is the command's result rather than something to retry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RejectionReason {
    /// The request violates the frozen schema. The violation is a
    /// property of the request bytes, so it is diagnosable from the
    /// durable payload without carrying the detail in the replicated
    /// result.
    Invalid,
    /// The request's namespace is not the one it executes in.
    NamespaceMismatch,
    /// The response would exceed the semantic response budget.
    ResponseTooLarge,
    /// The mutation would exceed the events-per-revision budget.
    TooManyEvents,
    /// The range delete would remove more keys than allowed.
    TooManyDeletes,
    /// The domain revision or execution position cannot advance.
    CounterOverflow,
    /// The operation is not planned by this planner.
    Unsupported,
    /// The state the request would have to read to be planned exceeds the
    /// schema's view budget. The budget is a replicated constant, not a
    /// local setting, so every replica reaches this rejection for the
    /// same command against the same state.
    ViewTooLarge,
    /// The retry key was already bound to a different request. The
    /// original binding stands; this command executes as a rejection and
    /// never as the bound request.
    RetryConflict,
    /// The request's sequence is at or below its session's retired floor.
    RetryTooOld,
    /// The request's sequence is beyond the session's outstanding window.
    RetryOutOfWindow,
    /// The session is unknown or retired, so nothing executes under it.
    SessionInvalid,
    /// Current authorization no longer permits handing out the retained
    /// result, and nothing re-executes.
    RetryUnauthorized,
}

/// A Kine-facing entry: the entry's value and revisions plus the TTL of
/// its private binding. It deliberately carries no lease identity: the
/// hidden binding (and any native lease) never reaches a Kine caller.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KineKv {
    /// Key.
    pub key: Vec<u8>,
    /// Value.
    pub value: Vec<u8>,
    /// Revision at which the key was created.
    pub create_revision: KvRevision,
    /// Revision of the last modification.
    pub mod_revision: KvRevision,
    /// Version (number of modifications since creation).
    pub version: u64,
    /// Kine-facing TTL in seconds (`0`: no private binding).
    pub ttl_seconds: u32,
}

impl KineKv {
    /// The Kine-facing projection of `entry` at `key` with `ttl_seconds`.
    pub fn project(key: &[u8], entry: &KvEntry, ttl_seconds: u32) -> Self {
        KineKv {
            key: key.to_vec(),
            value: entry.value.clone(),
            create_revision: entry.create_revision,
            mod_revision: entry.mod_revision,
            version: entry.version,
            ttl_seconds,
        }
    }
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
    /// Write or remove a session record.
    SessionWrite {
        /// Session.
        session: SessionId,
        /// New record (`None` removes; retirement keeps the record).
        record: Option<SessionRecord>,
    },
    /// Write a grant commitment row.
    GrantWrite {
        /// Commitment digest (the row key).
        commitment: Digest32,
        /// Record.
        record: GrantRecord,
    },
    /// Write or remove a permission rule.
    PolicyRuleWrite {
        /// Principal the rule belongs to (part of the row key).
        principal: coord_types::ids::PrincipalId,
        /// Rule identity.
        rule: coord_types::ids::PolicyRuleId,
        /// New rule (`None` removes).
        record: Option<PolicyRule>,
    },
    /// Write a trust rule.
    TrustRuleWrite {
        /// Rule identity.
        rule: coord_types::ids::TrustRuleId,
        /// New state.
        record: TrustRule,
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

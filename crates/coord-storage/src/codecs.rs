//! Common row schemas (design Section 17.1). Keys use the reviewed ordered
//! encoders; values are `StoreEnvelopeV1` with frozen record kinds.

use coord_state::lease::LeaseRecord;
use coord_state::policy::{GrantRecord, PolicyRule, SessionRecord, TrustRule};
use coord_state::view::KvEntry;
use coord_state::{KvEvent, KvEventKind};
use coord_store_api::engine::{EngineError, ErrorClass, OrderedRead};
use coord_store_api::envelope::StoreEnvelopeV1;
use coord_store_api::registry::{Collection, meta_fields};
use coord_types::ids::{
    KvRevision, LeaseAuthorityEpoch, LeaseGeneration, LeaseId, NamespaceId, PolicyRuleId,
    PrincipalId, TrustRuleId,
};
use coord_types::ordered_key;
use serde::{Deserialize, Serialize};

/// Record kind of a current entry in `kv_current_v1`.
pub const KV_CURRENT_KIND: u16 = 0x0001;
/// Record kind of a history version in `kv_history_v1`.
pub const KV_HISTORY_KIND: u16 = 0x0001;
/// Record kind of an event in `events_v1`.
pub const EVENT_KIND: u16 = 0x0001;
/// Record kind of a `u64` frontier row in `meta_v1`.
pub const COUNTER_KIND: u16 = 0x0003;
/// Record kind of a lease reverse-index row in `lease_keys_v1`.
pub const LEASE_KEY_KIND: u16 = 0x0001;
/// Record kind of a lease record in `lease_v1`.
pub const LEASE_KIND: u16 = 0x0001;

fn corrupt(what: &'static str) -> EngineError {
    EngineError::new(ErrorClass::Corrupt, what)
}

fn encode<T: Serialize>(kind: u16, value: &T) -> Result<Vec<u8>, EngineError> {
    let payload = postcard::to_allocvec(value)
        .map_err(|_| EngineError::new(ErrorClass::Limit, "record encode"))?;
    StoreEnvelopeV1 {
        record_kind: kind,
        schema_version: 1,
        payload,
    }
    .encode()
}

fn decode<T: for<'de> Deserialize<'de>>(
    kind: u16,
    bytes: &[u8],
    what: &'static str,
) -> Result<T, EngineError> {
    let env = StoreEnvelopeV1::decode(bytes)?;
    if env.record_kind != kind || env.schema_version != 1 {
        return Err(corrupt(what));
    }
    let (value, rest): (T, &[u8]) =
        postcard::take_from_bytes(&env.payload).map_err(|_| corrupt(what))?;
    if !rest.is_empty() {
        return Err(corrupt(what));
    }
    Ok(value)
}

/// A history version: an entry or a tombstone.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryRecordV1 {
    /// The entry as of this revision, `None` for a deletion.
    pub entry: Option<KvEntry>,
}

/// An event row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventRecordV1 {
    /// Kind.
    pub kind: KvEventKind,
    /// Namespace.
    pub namespace: NamespaceId,
    /// Key.
    pub key: Vec<u8>,
    /// New entry for puts.
    pub entry: Option<KvEntry>,
    /// Previous entry when the key existed.
    pub prev: Option<KvEntry>,
}

/// Current-row key.
pub fn current_key(namespace: &NamespaceId, key: &[u8]) -> Vec<u8> {
    ordered_key::encode_current(namespace, key)
}

/// History-row key.
pub fn history_key(namespace: &NamespaceId, key: &[u8], revision: KvRevision) -> Vec<u8> {
    ordered_key::encode_history(namespace, key, revision)
}

/// Event-row key: revision then ordinal, both big-endian.
pub fn event_key(revision: KvRevision, ordinal: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(12);
    out.extend_from_slice(&revision.to_be_bytes());
    out.extend_from_slice(&ordinal.to_be_bytes());
    out
}

/// Decode an event-row key.
pub fn decode_event_key(key: &[u8]) -> Result<(KvRevision, u32), EngineError> {
    if key.len() != 12 {
        return Err(corrupt("event key length"));
    }
    let revision =
        KvRevision::from_be_slice(&key[..8]).map_err(|_| corrupt("event key revision"))?;
    let ordinal = u32::from_be_bytes([key[8], key[9], key[10], key[11]]);
    Ok((revision, ordinal))
}

/// Lease reverse-index key: lease id then the current-row key.
pub fn lease_key(lease: &LeaseId, namespace: &NamespaceId, key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + 18 + key.len());
    out.extend_from_slice(lease.as_bytes());
    out.extend_from_slice(&current_key(namespace, key));
    out
}

/// Prefix of every reverse-index key of `lease` in `namespace`.
pub fn lease_key_prefix(lease: &LeaseId, namespace: &NamespaceId) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    out.extend_from_slice(lease.as_bytes());
    out.extend_from_slice(namespace.as_bytes());
    out
}

/// Decode a reverse-index key into its lease, namespace and user key.
pub fn decode_lease_key_row(key: &[u8]) -> Result<(LeaseId, NamespaceId, Vec<u8>), EngineError> {
    if key.len() < 16 {
        return Err(corrupt("lease_keys key length"));
    }
    let lease = LeaseId::from_slice(&key[..16]).map_err(|_| corrupt("lease_keys lease id"))?;
    let decoded =
        ordered_key::decode_current(&key[16..]).map_err(|_| corrupt("lease_keys current key"))?;
    Ok((lease, decoded.namespace, decoded.key))
}

/// Lease record row key: the lease identity.
pub fn lease_row_key(lease: &LeaseId) -> Vec<u8> {
    lease.as_bytes().to_vec()
}

/// A reverse-index row: the binding's generation and the bound entry's
/// modification revision (what a conditional expiration matches).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseKeyRecordV1 {
    /// Lease generation bound.
    pub generation: LeaseGeneration,
    /// Modification revision of the bound entry.
    pub mod_revision: KvRevision,
}

/// Encode a current entry.
pub fn encode_current(entry: &KvEntry) -> Result<Vec<u8>, EngineError> {
    encode(KV_CURRENT_KIND, entry)
}

/// Decode a current entry.
pub fn decode_current(bytes: &[u8]) -> Result<KvEntry, EngineError> {
    decode(KV_CURRENT_KIND, bytes, "kv_current record")
}

/// Encode a history version.
pub fn encode_history(record: &HistoryRecordV1) -> Result<Vec<u8>, EngineError> {
    encode(KV_HISTORY_KIND, record)
}

/// Decode a history version.
pub fn decode_history(bytes: &[u8]) -> Result<HistoryRecordV1, EngineError> {
    decode(KV_HISTORY_KIND, bytes, "kv_history record")
}

/// Encode an event.
pub fn encode_event(record: &EventRecordV1) -> Result<Vec<u8>, EngineError> {
    encode(EVENT_KIND, record)
}

/// Decode an event.
pub fn decode_event(bytes: &[u8]) -> Result<EventRecordV1, EngineError> {
    decode(EVENT_KIND, bytes, "event record")
}

/// Encode a `u64` frontier row.
pub fn encode_counter(value: u64) -> Result<Vec<u8>, EngineError> {
    encode(COUNTER_KIND, &value)
}

/// Decode a `u64` frontier row.
pub fn decode_counter(bytes: &[u8]) -> Result<u64, EngineError> {
    decode(COUNTER_KIND, bytes, "counter record")
}

/// Encode a lease reverse-index row.
pub fn encode_lease_key(record: &LeaseKeyRecordV1) -> Result<Vec<u8>, EngineError> {
    encode(LEASE_KEY_KIND, record)
}

/// Decode a lease reverse-index row.
pub fn decode_lease_key(bytes: &[u8]) -> Result<LeaseKeyRecordV1, EngineError> {
    decode(LEASE_KEY_KIND, bytes, "lease_keys record")
}

/// Encode a lease record.
pub fn encode_lease(record: &LeaseRecord) -> Result<Vec<u8>, EngineError> {
    encode(LEASE_KIND, record)
}

/// Decode a lease record.
pub fn decode_lease(bytes: &[u8]) -> Result<LeaseRecord, EngineError> {
    decode(LEASE_KIND, bytes, "lease record")
}

/// Convert a planner event into its row record.
pub fn event_record(namespace: NamespaceId, event: &KvEvent) -> EventRecordV1 {
    EventRecordV1 {
        kind: event.kind,
        namespace,
        key: event.key.clone(),
        entry: event.entry.clone(),
        prev: event.prev.clone(),
    }
}

/// Meta field names used by materialization.
pub mod fields {
    pub use coord_store_api::registry::meta_fields::{KV_REVISION, RETENTION_FLOOR};
}

/// Read the KV revision frontier from a view (zero when absent).
pub fn read_kv_revision<V: coord_store_api::engine::OrderedRead>(
    view: &V,
) -> Result<KvRevision, EngineError> {
    match view.get(
        coord_store_api::registry::Collection::MetaV1.id(),
        meta_fields::KV_REVISION,
    )? {
        Some(bytes) => KvRevision::new(decode_counter(&bytes)?)
            .map_err(|_| corrupt("kv revision out of range")),
        None => Ok(KvRevision::ZERO),
    }
}

/// Read the retention floor from a view (zero when absent).
pub fn read_retention_floor<V: coord_store_api::engine::OrderedRead>(
    view: &V,
) -> Result<KvRevision, EngineError> {
    match view.get(
        coord_store_api::registry::Collection::MetaV1.id(),
        meta_fields::RETENTION_FLOOR,
    )? {
        Some(bytes) => KvRevision::new(decode_counter(&bytes)?)
            .map_err(|_| corrupt("retention floor out of range")),
        None => Ok(KvRevision::ZERO),
    }
}

/// Read the lease expiry authority epoch (`ZERO` when never established).
pub fn read_lease_authority<V: OrderedRead>(view: &V) -> Result<LeaseAuthorityEpoch, EngineError> {
    match view.get(Collection::MetaV1.id(), meta_fields::LEASE_AUTHORITY)? {
        None => Ok(LeaseAuthorityEpoch::ZERO),
        Some(bytes) => LeaseAuthorityEpoch::new(decode_counter(&bytes)?)
            .map_err(|_| corrupt("lease authority out of range")),
    }
}

// ---- task-12: retry, floor, executed and minimal session records ----

use coord_types::identity::{CommandId, Digest32, RetryKey};
use coord_types::ids::{ClientInstanceId, ExecutionPosition, RequestSequence, SessionId};

/// Record kind of a retained result in `retry_v1`.
pub const RETRY_KIND: u16 = 0x0001;
/// Record kind of a floor in `retry_floor_v1`.
pub const RETRY_FLOOR_KIND: u16 = 0x0001;
/// Record kind of an executed identity, named by command in `executed_v1`
/// (written by position in `execution_v1` since task-d113's step 2).
pub const EXECUTED_KIND: u16 = 0x0001;
/// Record kind of an executed row in `execution_v1` (task-d113, step 2).
pub const EXECUTION_KIND: u16 = 0x0001;
/// Record kind of the minimal session state in `session_v1` (task-18
/// extends the record with principal, ceiling and generations).
pub const SESSION_KIND: u16 = 0x0001;

/// Retained result of one invocation (design Section 6.5).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryRecordV1 {
    /// Command identity bound to the retry key (payload conflicts are
    /// detected against it).
    pub command_id: CommandId,
    /// Execution position the command occupied.
    pub position: ExecutionPosition,
    /// KV revision it produced, if any.
    pub revision: Option<KvRevision>,
    /// Exact encoded response.
    pub response: Vec<u8>,
    /// Digest of the response.
    pub result_digest: Digest32,
}

/// Retirement floor and outstanding window of one client instance.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryFloorV1 {
    /// Highest retired sequence; at or below is never new work.
    pub floor: RequestSequence,
    /// Maximum outstanding sequences above the floor.
    pub width: u32,
}

/// Applied identity of a command: the update a node's execution names by
/// command in `executed_v1`, and the part of its `execution_v1` row
/// (task-d113, step 2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutedRecordV1 {
    /// Execution position.
    pub position: ExecutionPosition,
    /// KV revision, if any.
    pub revision: Option<KvRevision>,
    /// Result digest.
    pub result_digest: Digest32,
}

/// An executed row as `execution_v1` holds it, by its position (task-d113,
/// step 2): the executed identity, with the command it names, since the
/// key no longer does.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionRecordV1 {
    /// The command executed at the position.
    pub command: CommandId,
    /// Its executed identity.
    pub executed: ExecutedRecordV1,
}

/// Record kind of a grant commitment row in `auth_grant_v1`.
pub const GRANT_KIND: u16 = 0x0001;
/// Record kind of a trust rule row in `policy_v1`.
pub const TRUST_RULE_KIND: u16 = 0x0001;
/// Record kind of a permission rule row in `policy_v1`.
pub const POLICY_RULE_KIND: u16 = 0x0002;
/// Key prefix byte of trust rules in `policy_v1`.
pub const TRUST_RULE_PREFIX: u8 = 0x01;
/// Key prefix byte of permission rules in `policy_v1`.
pub const POLICY_RULE_PREFIX: u8 = 0x02;

/// `auth_grant_v1` key: the commitment digest.
pub fn grant_key(commitment: &Digest32) -> Vec<u8> {
    commitment.0.to_vec()
}

/// `policy_v1` key of a trust rule.
pub fn trust_rule_key(rule: &TrustRuleId) -> Vec<u8> {
    let mut out = Vec::with_capacity(17);
    out.push(TRUST_RULE_PREFIX);
    out.extend_from_slice(rule.as_bytes());
    out
}

/// `policy_v1` key prefix of every permission rule of `principal`.
pub fn policy_rule_prefix(principal: &PrincipalId) -> Vec<u8> {
    let mut out = Vec::with_capacity(17);
    out.push(POLICY_RULE_PREFIX);
    out.extend_from_slice(principal.as_bytes());
    out
}

/// `policy_v1` key of a permission rule.
pub fn policy_rule_key(principal: &PrincipalId, rule: &PolicyRuleId) -> Vec<u8> {
    let mut out = policy_rule_prefix(principal);
    out.extend_from_slice(rule.as_bytes());
    out
}

/// Encode a grant commitment row.
pub fn encode_grant(record: &GrantRecord) -> Result<Vec<u8>, EngineError> {
    encode(GRANT_KIND, record)
}

/// Decode a grant commitment row.
pub fn decode_grant(bytes: &[u8]) -> Result<GrantRecord, EngineError> {
    decode(GRANT_KIND, bytes, "grant record")
}

/// Encode a trust rule row.
pub fn encode_trust_rule(record: &TrustRule) -> Result<Vec<u8>, EngineError> {
    encode(TRUST_RULE_KIND, record)
}

/// Decode a trust rule row.
pub fn decode_trust_rule(bytes: &[u8]) -> Result<TrustRule, EngineError> {
    decode(TRUST_RULE_KIND, bytes, "trust rule record")
}

/// Encode a permission rule row.
pub fn encode_policy_rule(record: &PolicyRule) -> Result<Vec<u8>, EngineError> {
    encode(POLICY_RULE_KIND, record)
}

/// Decode a permission rule row.
pub fn decode_policy_rule(bytes: &[u8]) -> Result<PolicyRule, EngineError> {
    decode(POLICY_RULE_KIND, bytes, "policy rule record")
}

/// `retry_v1` key: session, client instance, big-endian sequence.
pub fn retry_key(key: &RetryKey) -> Vec<u8> {
    let mut out = Vec::with_capacity(40);
    out.extend_from_slice(key.session_id.as_bytes());
    out.extend_from_slice(key.client_instance_id.as_bytes());
    out.extend_from_slice(&key.request_sequence.to_be_bytes());
    out
}

/// `retry_floor_v1` key: session then client instance.
pub fn retry_floor_key(session: &SessionId, client: &ClientInstanceId) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    out.extend_from_slice(session.as_bytes());
    out.extend_from_slice(client.as_bytes());
    out
}

/// `executed_v1` key: the command identity bytes. Only names the update;
/// the lowering writes it by position (task-d113, step 2).
pub fn executed_key(command: &CommandId) -> Vec<u8> {
    command.as_bytes().to_vec()
}

/// `execution_v1` key, and an executed command's `payload_v1` key: the
/// execution position, big-endian (task-d113, step 2).
pub fn position_key(position: ExecutionPosition) -> [u8; 8] {
    position.to_be_bytes()
}

/// The position a `position_key` names.
pub fn position_of(key: &[u8]) -> Result<ExecutionPosition, EngineError> {
    ExecutionPosition::from_be_slice(key)
        .map_err(|_| EngineError::new(ErrorClass::Corrupt, "a position key of the wrong length"))
}

/// A `decided_v1` value (task-d113, step 2): the configuration epoch the
/// record was decided in, big-endian, then the dependency row's bytes as
/// `protocol_v1` held them. Its key is the command's execution position.
pub fn decided_value(epoch: [u8; 8], record: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + record.len());
    out.extend_from_slice(&epoch);
    out.extend_from_slice(record);
    out
}

/// The epoch and the dependency row's bytes a `decided_v1` value holds.
pub fn split_decided(bytes: &[u8]) -> Result<([u8; 8], &[u8]), EngineError> {
    match bytes.split_first_chunk::<8>() {
        Some((epoch, record)) => Ok((*epoch, record)),
        None => Err(EngineError::new(
            ErrorClass::Corrupt,
            "a decided record shorter than its epoch",
        )),
    }
}

/// `session_v1` key: the session identity bytes.
pub fn session_key(session: &SessionId) -> Vec<u8> {
    session.as_bytes().to_vec()
}

/// Encode a retry record.
pub fn encode_retry(record: &RetryRecordV1) -> Result<Vec<u8>, EngineError> {
    encode(RETRY_KIND, record)
}
/// Decode a retry record.
pub fn decode_retry(bytes: &[u8]) -> Result<RetryRecordV1, EngineError> {
    decode(RETRY_KIND, bytes, "retry record")
}
/// Encode a floor.
pub fn encode_retry_floor(record: &RetryFloorV1) -> Result<Vec<u8>, EngineError> {
    encode(RETRY_FLOOR_KIND, record)
}
/// Decode a floor.
pub fn decode_retry_floor(bytes: &[u8]) -> Result<RetryFloorV1, EngineError> {
    decode(RETRY_FLOOR_KIND, bytes, "retry floor record")
}
/// Encode an executed identity.
pub fn encode_executed(record: &ExecutedRecordV1) -> Result<Vec<u8>, EngineError> {
    encode(EXECUTED_KIND, record)
}
/// Decode an executed identity.
pub fn decode_executed(bytes: &[u8]) -> Result<ExecutedRecordV1, EngineError> {
    decode(EXECUTED_KIND, bytes, "executed record")
}
/// Encode an executed row of `execution_v1`.
pub fn encode_execution(record: &ExecutionRecordV1) -> Result<Vec<u8>, EngineError> {
    encode(EXECUTION_KIND, record)
}
/// Decode an executed row of `execution_v1`.
pub fn decode_execution(bytes: &[u8]) -> Result<ExecutionRecordV1, EngineError> {
    decode(EXECUTION_KIND, bytes, "execution record")
}
/// Encode a session state.
pub fn encode_session(record: &SessionRecord) -> Result<Vec<u8>, EngineError> {
    encode(SESSION_KIND, record)
}
/// Decode a session state.
pub fn decode_session(bytes: &[u8]) -> Result<SessionRecord, EngineError> {
    decode(SESSION_KIND, bytes, "session record")
}

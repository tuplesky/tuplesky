//! Common row schemas (design Section 17.1). Keys use the reviewed ordered
//! encoders; values are `StoreEnvelopeV1` with frozen record kinds.

use coord_state::view::KvEntry;
use coord_state::{KvEvent, KvEventKind};
use coord_store_api::engine::{EngineError, ErrorClass};
use coord_store_api::envelope::StoreEnvelopeV1;
use coord_store_api::registry::meta_fields;
use coord_types::ids::{KvRevision, LeaseId, NamespaceId};
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

/// Encode a lease reverse-index row (empty payload for now; task-15 adds
/// generation and expected mod revision).
pub fn encode_lease_key() -> Result<Vec<u8>, EngineError> {
    encode(LEASE_KEY_KIND, &())
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

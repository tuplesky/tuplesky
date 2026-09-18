//! Source-required protocol rows of `protocol_v1` (design Sections 5.2,
//! 17.1): the promise record of an epoch. Values are `StoreEnvelopeV1`
//! payloads; keys are epoch-prefixed so an epoch's rows are contiguous.

use alloc::vec::Vec;

use coord_core::effect::StoreUpdate;
use coord_store_api::engine::{EngineError, ErrorClass};
use coord_store_api::envelope::StoreEnvelopeV1;
use coord_store_api::registry::Collection;
use coord_types::identity::Digest32;
use coord_types::ids::{Ballot, ConfigurationEpoch};
use coord_types::{CommandId, RetryKey};

use crate::commands::CommandRecord;
use serde::{Deserialize, Serialize};

/// Record kind of the promise row.
pub const PROMISE_KIND: u16 = 0x0001;
/// Record kind of a command dependency row.
pub const DEPENDENCY_KIND: u16 = 0x0002;
/// Key tag of the promise row within an epoch.
const PROMISE_TAG: u8 = 0x00;
/// Key tag of command dependency rows within an epoch.
const DEPENDENCY_TAG: u8 = 0x01;
/// Record kind of a leader proposal row.
pub const PROPOSAL_KIND: u16 = 0x0003;
/// Key tag of leader proposal rows within an epoch.
const PROPOSAL_TAG: u8 = 0x02;
/// Record kind of a payload row in `payload_v1`.
pub const PAYLOAD_KIND: u16 = 0x0001;

/// The durable promise of one replica in one epoch: the highest ballot it
/// promised (no lower ballot is voted after it) and the ballot it last
/// synchronized (`cballot`, the source of state it reports in recovery).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PromiseRecordV1 {
    /// Highest promised ballot.
    pub promised: Ballot,
    /// Highest synchronized ballot.
    pub synced: Ballot,
}

/// `protocol_v1` key of the epoch's promise row.
pub fn promise_key(epoch: ConfigurationEpoch) -> Vec<u8> {
    let mut out = Vec::with_capacity(9);
    out.extend_from_slice(&epoch.to_be_bytes());
    out.push(PROMISE_TAG);
    out
}

/// Encode the promise row.
pub fn encode_promise(record: &PromiseRecordV1) -> Result<Vec<u8>, EngineError> {
    let payload = postcard::to_allocvec(record)
        .map_err(|_| EngineError::new(ErrorClass::Limit, "promise encode"))?;
    StoreEnvelopeV1 {
        record_kind: PROMISE_KIND,
        schema_version: 1,
        payload,
    }
    .encode()
}

/// Decode the promise row.
pub fn decode_promise(bytes: &[u8]) -> Result<PromiseRecordV1, EngineError> {
    let env = StoreEnvelopeV1::decode(bytes)?;
    if env.record_kind != PROMISE_KIND || env.schema_version != 1 {
        return Err(EngineError::new(ErrorClass::Corrupt, "promise record"));
    }
    let (record, rest): (PromiseRecordV1, &[u8]) = postcard::take_from_bytes(&env.payload)
        .map_err(|_| EngineError::new(ErrorClass::Corrupt, "promise record"))?;
    if !rest.is_empty() {
        return Err(EngineError::new(ErrorClass::Corrupt, "promise record"));
    }
    Ok(record)
}

/// The update writing the epoch's promise row.
pub fn promise_update(
    epoch: ConfigurationEpoch,
    record: &PromiseRecordV1,
) -> Result<StoreUpdate, EngineError> {
    Ok(StoreUpdate {
        collection: Collection::ProtocolV1.id(),
        key: promise_key(epoch),
        value: Some(encode_promise(record)?),
    })
}

/// `protocol_v1` key of a command's dependency row in an epoch.
pub fn dependency_key(epoch: ConfigurationEpoch, command: &CommandId) -> Vec<u8> {
    let mut out = Vec::with_capacity(41);
    out.extend_from_slice(&epoch.to_be_bytes());
    out.push(DEPENDENCY_TAG);
    out.extend_from_slice(command.as_bytes());
    out
}

/// Encode a command's required dependency state (phase, dependencies,
/// payload binding and path evidence).
pub fn encode_dependency(record: &CommandRecord) -> Result<Vec<u8>, EngineError> {
    let payload = postcard::to_allocvec(record)
        .map_err(|_| EngineError::new(ErrorClass::Limit, "dependency encode"))?;
    StoreEnvelopeV1 {
        record_kind: DEPENDENCY_KIND,
        schema_version: 1,
        payload,
    }
    .encode()
}

/// Decode a dependency row.
pub fn decode_dependency(bytes: &[u8]) -> Result<CommandRecord, EngineError> {
    let env = StoreEnvelopeV1::decode(bytes)?;
    if env.record_kind != DEPENDENCY_KIND || env.schema_version != 1 {
        return Err(EngineError::new(ErrorClass::Corrupt, "dependency record"));
    }
    let (record, rest): (CommandRecord, &[u8]) = postcard::take_from_bytes(&env.payload)
        .map_err(|_| EngineError::new(ErrorClass::Corrupt, "dependency record"))?;
    if !rest.is_empty() {
        return Err(EngineError::new(ErrorClass::Corrupt, "dependency record"));
    }
    Ok(record)
}

/// The update persisting a command's dependency row.
pub fn dependency_update(
    epoch: ConfigurationEpoch,
    command: &CommandId,
    record: &CommandRecord,
) -> Result<StoreUpdate, EngineError> {
    Ok(StoreUpdate {
        collection: Collection::ProtocolV1.id(),
        key: dependency_key(epoch, command),
        value: Some(encode_dependency(record)?),
    })
}

/// The immutable canonical command in `payload_v1`, keyed by command
/// identity: retry key and canonical logical bytes (rehashed on read).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PayloadRecordV1 {
    /// Stable invocation identity.
    pub retry_key: RetryKey,
    /// Canonical `LogicalRequest` encoding.
    pub logical: Vec<u8>,
}

/// `payload_v1` key: the command identity.
pub fn payload_key(command: &CommandId) -> Vec<u8> {
    command.as_bytes().to_vec()
}

fn envelope<T: Serialize>(
    kind: u16,
    value: &T,
    what: &'static str,
) -> Result<Vec<u8>, EngineError> {
    let payload =
        postcard::to_allocvec(value).map_err(|_| EngineError::new(ErrorClass::Limit, what))?;
    StoreEnvelopeV1 {
        record_kind: kind,
        schema_version: 1,
        payload,
    }
    .encode()
}

fn unwrap<T: for<'de> Deserialize<'de>>(
    kind: u16,
    bytes: &[u8],
    what: &'static str,
) -> Result<T, EngineError> {
    let env = StoreEnvelopeV1::decode(bytes)?;
    if env.record_kind != kind || env.schema_version != 1 {
        return Err(EngineError::new(ErrorClass::Corrupt, what));
    }
    let (value, rest): (T, &[u8]) = postcard::take_from_bytes(&env.payload)
        .map_err(|_| EngineError::new(ErrorClass::Corrupt, what))?;
    if !rest.is_empty() {
        return Err(EngineError::new(ErrorClass::Corrupt, what));
    }
    Ok(value)
}

/// Encode a payload row.
pub fn encode_payload(record: &PayloadRecordV1) -> Result<Vec<u8>, EngineError> {
    envelope(PAYLOAD_KIND, record, "payload encode")
}

/// Decode a payload row.
pub fn decode_payload(bytes: &[u8]) -> Result<PayloadRecordV1, EngineError> {
    unwrap(PAYLOAD_KIND, bytes, "payload record")
}

/// The update persisting a command's payload.
pub fn payload_update(
    command: &CommandId,
    record: &PayloadRecordV1,
) -> Result<StoreUpdate, EngineError> {
    Ok(StoreUpdate {
        collection: Collection::PayloadV1.id(),
        key: payload_key(command),
        value: Some(encode_payload(record)?),
    })
}

/// The leader's recoverable proposal state for a command.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProposalRecordV1 {
    /// Ballot the proposal was made under.
    pub ballot: Ballot,
    /// Leader sequence number.
    pub seqnum: u64,
    /// Ordered dependencies.
    pub deps: Vec<CommandId>,
    /// Dependency-path evidence.
    pub path: Digest32,
}

/// `protocol_v1` key of a command's proposal row in an epoch.
pub fn proposal_key(epoch: ConfigurationEpoch, command: &CommandId) -> Vec<u8> {
    let mut out = Vec::with_capacity(41);
    out.extend_from_slice(&epoch.to_be_bytes());
    out.push(PROPOSAL_TAG);
    out.extend_from_slice(command.as_bytes());
    out
}

/// Encode a proposal row.
pub fn encode_proposal(record: &ProposalRecordV1) -> Result<Vec<u8>, EngineError> {
    envelope(PROPOSAL_KIND, record, "proposal encode")
}

/// Decode a proposal row.
pub fn decode_proposal(bytes: &[u8]) -> Result<ProposalRecordV1, EngineError> {
    unwrap(PROPOSAL_KIND, bytes, "proposal record")
}

/// The update persisting a leader proposal.
pub fn proposal_update(
    epoch: ConfigurationEpoch,
    command: &CommandId,
    record: &ProposalRecordV1,
) -> Result<StoreUpdate, EngineError> {
    Ok(StoreUpdate {
        collection: Collection::ProtocolV1.id(),
        key: proposal_key(epoch, command),
        value: Some(encode_proposal(record)?),
    })
}

//! Source-required protocol rows of `protocol_v1` (design Sections 5.2,
//! 17.1): the promise record of an epoch. Values are `StoreEnvelopeV1`
//! payloads; keys are epoch-prefixed so an epoch's rows are contiguous.

use alloc::vec::Vec;

use coord_core::effect::StoreUpdate;
use coord_store_api::engine::{EngineError, ErrorClass};
use coord_store_api::envelope::StoreEnvelopeV1;
use coord_store_api::registry::Collection;
use coord_types::ids::{Ballot, ConfigurationEpoch};
use serde::{Deserialize, Serialize};

/// Record kind of the promise row.
pub const PROMISE_KIND: u16 = 0x0001;
/// Key tag of the promise row within an epoch.
const PROMISE_TAG: u8 = 0x00;

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

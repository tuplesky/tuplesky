//! Protocol rows in the projection (task-20): reading the recovered promise
//! of an epoch so the ballot state is wired from durable state, never from
//! a message.

use coord_consensus::rows::{PromiseRecordV1, decode_promise, promise_key};
use coord_store_api::engine::{EngineError, OrderedRead};
use coord_store_api::registry::Collection;
use coord_types::ids::ConfigurationEpoch;

/// The epoch's durable promise row, if any.
pub fn read_promise<V: OrderedRead>(
    view: &V,
    epoch: ConfigurationEpoch,
) -> Result<Option<PromiseRecordV1>, EngineError> {
    match view.get(Collection::ProtocolV1.id(), &promise_key(epoch))? {
        Some(bytes) => Ok(Some(decode_promise(&bytes)?)),
        None => Ok(None),
    }
}

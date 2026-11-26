//! The store each peer voter's promises and reports came from
//! (task-d121, C1).
//!
//! A voter wiped and initialized again under its own identity comes back
//! with a new [`StoreInstanceId`], minted at its `init`. What tells it
//! apart from the voter it was is this row: one per peer and
//! incarnation, holding the store the first promise or report from that
//! peer came from. The runtime writes it before it hands the message on,
//! and refuses a later one from another store (`coord_daemon`'s
//! `peer_store`).
//!
//! Keyed by incarnation, so a voter that comes back through membership as
//! a new generation (task-d41) has no row yet, and is not refused. The
//! row lives in `checkpoint_v1`, which is node-private: no shared
//! checkpoint carries it to another node.

use coord_core::effect::StoreUpdate;
use coord_store_api::engine::{EngineError, OrderedRead};
use coord_store_api::registry::Collection;
use coord_types::ids::{ReplicaId, ReplicaIncarnation, StoreInstanceId};
use serde::{Deserialize, Serialize};

use crate::trim::{corrupt, decode_record, encode_record};

/// Record kind of a peer's store row in `checkpoint_v1`.
pub const PEER_STORE_RECORD_KIND: u16 = 0x000c;
/// Key prefix of the peer store rows; the voter and its incarnation
/// follow.
pub const PEER_STORE_KEY_PREFIX: &[u8] = b"store_instance_v1/";

/// The store a peer's first promise or report came from, under one
/// incarnation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerStoreV1 {
    /// The peer.
    pub voter: ReplicaId,
    /// Its incarnation.
    pub incarnation: ReplicaIncarnation,
    /// The store it spoke from.
    pub store: StoreInstanceId,
}

impl PeerStoreV1 {
    /// Encode as a `checkpoint_v1` row value.
    pub fn encode(&self) -> Result<Vec<u8>, EngineError> {
        encode_record(PEER_STORE_RECORD_KIND, self, "peer store encode")
    }

    /// Decode a `checkpoint_v1` row value.
    pub fn decode(bytes: &[u8]) -> Result<Self, EngineError> {
        decode_record(PEER_STORE_RECORD_KIND, bytes, "peer store record")
    }

    /// The update that writes this row.
    pub fn update(&self) -> Result<StoreUpdate, EngineError> {
        Ok(StoreUpdate {
            collection: Collection::CheckpointV1.id(),
            key: peer_store_key(&self.voter, self.incarnation),
            value: Some(self.encode()?),
        })
    }
}

/// The key of `voter`'s row under `incarnation`.
pub fn peer_store_key(voter: &ReplicaId, incarnation: ReplicaIncarnation) -> Vec<u8> {
    let mut key = PEER_STORE_KEY_PREFIX.to_vec();
    key.extend_from_slice(voter.as_bytes());
    key.extend_from_slice(&incarnation.to_be_bytes());
    key
}

/// The row this node durably holds for `voter` under `incarnation`, read
/// by its key: a node never reads the rows whole, so however many
/// incarnations a peer has had, every set of rows it wrote reopens.
pub fn read_peer_store<V: OrderedRead>(
    view: &V,
    voter: &ReplicaId,
    incarnation: ReplicaIncarnation,
) -> Result<Option<PeerStoreV1>, EngineError> {
    let Some(value) = view.get(
        Collection::CheckpointV1.id(),
        &peer_store_key(voter, incarnation),
    )?
    else {
        return Ok(None);
    };
    let record = PeerStoreV1::decode(&value)?;
    if record.voter != *voter || record.incarnation != incarnation {
        return Err(corrupt("peer store row disagrees with its key"));
    }
    Ok(Some(record))
}

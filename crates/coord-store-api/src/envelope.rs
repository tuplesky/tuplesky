//! `StoreEnvelopeV1`: the bounded value envelope of every row, and the
//! applied stamp persisted in `meta_v1` (design Sections 17.1 and 17.10).
//!
//! Row values are `record_kind || schema_version || payload` in postcard;
//! ordered keys never use postcard (see `coord_types::ordered_key`).

use alloc::vec::Vec;

use coord_types::identity::Digest32;
use coord_types::ids::LocalJournalSeq;
use serde::{Deserialize, Serialize};

use crate::engine::{EngineError, ErrorClass};
use crate::seq::StoreSeq;

/// Maximum payload accepted inside an envelope (largest admitted value plus
/// metadata headroom).
pub const MAX_ENVELOPE_PAYLOAD: usize = 1024 * 1024 + 4096;

/// Bounded value envelope.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreEnvelopeV1 {
    /// Record kind within the collection (frozen per collection).
    pub record_kind: u16,
    /// Schema version of the payload.
    pub schema_version: u16,
    /// Payload bytes.
    pub payload: Vec<u8>,
}

impl StoreEnvelopeV1 {
    /// Encode; fails on oversized payloads.
    pub fn encode(&self) -> Result<Vec<u8>, EngineError> {
        if self.payload.len() > MAX_ENVELOPE_PAYLOAD {
            return Err(EngineError::new(
                ErrorClass::Limit,
                "envelope payload exceeds limit",
            ));
        }
        postcard::to_allocvec(self)
            .map_err(|_| EngineError::new(ErrorClass::Limit, "envelope encode failed"))
    }

    /// Decode exactly; trailing bytes and oversized payloads are corruption.
    pub fn decode(bytes: &[u8]) -> Result<Self, EngineError> {
        let (env, rest): (StoreEnvelopeV1, &[u8]) = postcard::take_from_bytes(bytes)
            .map_err(|_| EngineError::new(ErrorClass::Corrupt, "envelope decode failed"))?;
        if !rest.is_empty() {
            return Err(EngineError::new(
                ErrorClass::Corrupt,
                "trailing bytes after envelope",
            ));
        }
        if env.payload.len() > MAX_ENVELOPE_PAYLOAD {
            return Err(EngineError::new(
                ErrorClass::Corrupt,
                "envelope payload exceeds limit",
            ));
        }
        Ok(env)
    }
}

/// The applied stamp persisted atomically with every projection update.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedStamp {
    /// Last materialized store sequence.
    pub store_seq: StoreSeq,
    /// Local journal sequence it represents (one-to-one with `store_seq`).
    pub journal_seq: LocalJournalSeq,
    /// Digest binding the last immutable batch and its guard context.
    pub last_batch_digest: Digest32,
}

/// Record kind of the applied stamp inside `meta_v1`.
pub const STAMP_RECORD_KIND: u16 = 0x0001;
/// Schema version of the applied stamp.
pub const STAMP_SCHEMA_VERSION: u16 = 1;

impl AppliedStamp {
    /// Encode as an envelope value.
    pub fn to_envelope(&self) -> Result<Vec<u8>, EngineError> {
        let payload = postcard::to_allocvec(self)
            .map_err(|_| EngineError::new(ErrorClass::Limit, "stamp encode failed"))?;
        StoreEnvelopeV1 {
            record_kind: STAMP_RECORD_KIND,
            schema_version: STAMP_SCHEMA_VERSION,
            payload,
        }
        .encode()
    }

    /// Decode from an envelope value; wrong kind/version is corruption.
    pub fn from_envelope(bytes: &[u8]) -> Result<Self, EngineError> {
        let env = StoreEnvelopeV1::decode(bytes)?;
        if env.record_kind != STAMP_RECORD_KIND || env.schema_version != STAMP_SCHEMA_VERSION {
            return Err(EngineError::new(
                ErrorClass::Corrupt,
                "unexpected stamp record kind or version",
            ));
        }
        let (stamp, rest): (AppliedStamp, &[u8]) = postcard::take_from_bytes(&env.payload)
            .map_err(|_| EngineError::new(ErrorClass::Corrupt, "stamp decode failed"))?;
        if !rest.is_empty() {
            return Err(EngineError::new(
                ErrorClass::Corrupt,
                "trailing bytes after stamp",
            ));
        }
        if stamp.store_seq.journal_seq() != stamp.journal_seq {
            return Err(EngineError::new(
                ErrorClass::Corrupt,
                "stamp sequence mapping mismatch",
            ));
        }
        Ok(stamp)
    }
}

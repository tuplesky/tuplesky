//! Lowering of immutable batches into engine rows, stamps and digests.

use coord_core::effect::{ApplyBase, PersistBatch, StoreUpdate};
use coord_store_api::engine::{EngineError, WriteTxn};
use coord_store_api::envelope::{AppliedStamp, StoreEnvelopeV1};
use coord_store_api::registry::{Collection, meta_fields};
use coord_store_api::seq::StoreSeq;
use coord_types::identity::{Digest32, HashDomain};
use coord_types::ids::{ConfigurationEpoch, ExecutionPosition, LocalJournalSeq};
use serde::{Deserialize, Serialize};

/// Digest of one batch bound to its guard context.
pub type GroupDigest = Digest32;

/// Digest binding a batch's barrier, base and every update, chained onto
/// the previous stamp digest so the stamp identifies the exact history.
pub fn batch_digest(previous: &Digest32, batch: &PersistBatch) -> Digest32 {
    let mut parts: Vec<Vec<u8>> = Vec::with_capacity(4 + batch.updates.len() * 3);
    parts.push(previous.0.to_vec());
    let mut barrier = Vec::with_capacity(32);
    barrier.extend_from_slice(&batch.barrier.node_generation.to_be_bytes());
    barrier.extend_from_slice(&batch.barrier.boot_id.0);
    barrier.extend_from_slice(&batch.barrier.sequence.to_be_bytes());
    parts.push(barrier);
    let mut base = Vec::with_capacity(17);
    match batch.base {
        Some(b) => {
            base.push(1);
            base.extend_from_slice(&b.configuration.to_be_bytes());
            base.extend_from_slice(&b.execution_position.to_be_bytes());
        }
        None => base.push(0),
    }
    parts.push(base);
    for u in &batch.updates {
        parts.push(u.collection.0.to_be_bytes().to_vec());
        parts.push(u.key.clone());
        match &u.value {
            Some(v) => {
                let mut tagged = Vec::with_capacity(v.len() + 1);
                tagged.push(1);
                tagged.extend_from_slice(v);
                parts.push(tagged);
            }
            None => parts.push(vec![0]),
        }
    }
    let refs: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
    HashDomain::JournalBatch.digest(&refs)
}

/// Application frontier stored in `meta_v1`: the base the next application
/// batch must extend.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionFrontier {
    /// Configuration epoch.
    pub configuration: ConfigurationEpoch,
    /// Highest established execution position applied.
    pub execution_position: ExecutionPosition,
}

impl ExecutionFrontier {
    /// The frontier of an empty projection.
    pub const INITIAL: ExecutionFrontier = ExecutionFrontier {
        configuration: ConfigurationEpoch::ZERO,
        execution_position: ExecutionPosition::ZERO,
    };

    /// The base an application batch must carry to extend this frontier.
    pub const fn as_base(self) -> ApplyBase {
        ApplyBase {
            configuration: self.configuration,
            execution_position: self.execution_position,
        }
    }

    fn encode(&self) -> Result<Vec<u8>, EngineError> {
        let payload = postcard::to_allocvec(self).map_err(|_| {
            EngineError::new(
                coord_store_api::engine::ErrorClass::Limit,
                "frontier encode",
            )
        })?;
        StoreEnvelopeV1 {
            record_kind: FRONTIER_RECORD_KIND,
            schema_version: 1,
            payload,
        }
        .encode()
    }

    fn decode(bytes: &[u8]) -> Result<Self, EngineError> {
        let env = StoreEnvelopeV1::decode(bytes)?;
        if env.record_kind != FRONTIER_RECORD_KIND || env.schema_version != 1 {
            return Err(EngineError::new(
                coord_store_api::engine::ErrorClass::Corrupt,
                "unexpected frontier record",
            ));
        }
        let (f, rest): (ExecutionFrontier, &[u8]) = postcard::take_from_bytes(&env.payload)
            .map_err(|_| {
                EngineError::new(
                    coord_store_api::engine::ErrorClass::Corrupt,
                    "frontier decode",
                )
            })?;
        if !rest.is_empty() {
            return Err(EngineError::new(
                coord_store_api::engine::ErrorClass::Corrupt,
                "trailing frontier bytes",
            ));
        }
        Ok(f)
    }
}

/// Record kind of the execution frontier inside `meta_v1`.
pub const FRONTIER_RECORD_KIND: u16 = 0x0002;

/// Durable metadata read at boot and after every flush.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DurableMeta {
    /// Applied stamp, or the initial stamp when the projection is empty.
    pub stamp: AppliedStamp,
    /// Application frontier.
    pub frontier: ExecutionFrontier,
}

impl DurableMeta {
    /// The metadata of an empty projection.
    pub fn initial() -> Self {
        DurableMeta {
            stamp: AppliedStamp {
                store_seq: StoreSeq::INITIAL,
                journal_seq: LocalJournalSeq::ZERO,
                last_batch_digest: Digest32([0; 32]),
            },
            frontier: ExecutionFrontier::INITIAL,
        }
    }

    /// Read from any ordered view.
    pub fn read<V: coord_store_api::engine::OrderedRead>(view: &V) -> Result<Self, EngineError> {
        let meta = Collection::MetaV1.id();
        let stamp = match view.get(meta, meta_fields::APPLIED_STAMP)? {
            Some(bytes) => AppliedStamp::from_envelope(&bytes)?,
            None => DurableMeta::initial().stamp,
        };
        let frontier = match view.get(meta, meta_fields::EXECUTION_FRONTIER)? {
            Some(bytes) => ExecutionFrontier::decode(&bytes)?,
            None => ExecutionFrontier::INITIAL,
        };
        Ok(DurableMeta { stamp, frontier })
    }

    /// Write both rows inside the transaction.
    pub fn write<W: WriteTxn>(&self, tx: &mut W) -> Result<(), EngineError> {
        let meta = Collection::MetaV1.id();
        tx.put(meta, meta_fields::APPLIED_STAMP, &self.stamp.to_envelope()?)?;
        tx.put(
            meta,
            meta_fields::EXECUTION_FRONTIER,
            &self.frontier.encode()?,
        )?;
        Ok(())
    }
}

/// Lower one update into the transaction.
pub fn lower_update<W: WriteTxn>(tx: &mut W, update: &StoreUpdate) -> Result<(), EngineError> {
    match &update.value {
        Some(v) => tx.put(update.collection, &update.key, v),
        None => tx.delete(update.collection, &update.key),
    }
}

/// Byte cost of a batch for grouping budgets.
pub fn batch_bytes(batch: &PersistBatch) -> usize {
    batch
        .updates
        .iter()
        .map(|u| u.key.len() + u.value.as_ref().map_or(0, Vec::len) + 16)
        .sum::<usize>()
        + 64
}

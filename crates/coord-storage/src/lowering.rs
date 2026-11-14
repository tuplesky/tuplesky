//! Lowering of immutable batches into engine rows, stamps and digests.

use std::collections::HashMap;

use coord_core::effect::{ApplyBase, CollectionId, PersistBatch, StoreUpdate};
use coord_store_api::engine::{EngineError, ErrorClass, OrderedRead, WriteTxn};
use coord_store_api::envelope::{AppliedStamp, StoreEnvelopeV1};
use coord_store_api::registry::{Collection, meta_fields};
use coord_store_api::seq::StoreSeq;
use coord_types::identity::{Digest32, HashDomain};
use coord_types::ids::{ConfigurationEpoch, ExecutionPosition};

use crate::intake::{Assigned, Assigner};
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
        StoreEnvelopeV1::encode_value(FRONTIER_RECORD_KIND, 1, self, "frontier encode")
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
            stamp: AppliedStamp::new(StoreSeq::INITIAL, Digest32([0; 32])),
            frontier: ExecutionFrontier::INITIAL,
        }
    }

    /// Read from any ordered view.
    ///
    /// The stamp and the frontier are written atomically, so after the first
    /// flush exactly one missing row is corruption, never an empty
    /// projection: only the both-missing case is the initial state.
    pub fn read<V: coord_store_api::engine::OrderedRead>(view: &V) -> Result<Self, EngineError> {
        let meta = Collection::MetaV1.id();
        let stamp = view.get(meta, meta_fields::APPLIED_STAMP)?;
        let frontier = view.get(meta, meta_fields::EXECUTION_FRONTIER)?;
        match (stamp, frontier) {
            (None, None) => Ok(DurableMeta::initial()),
            (Some(stamp), Some(frontier)) => Ok(DurableMeta {
                stamp: AppliedStamp::from_envelope(&stamp)?,
                frontier: ExecutionFrontier::decode(&frontier)?,
            }),
            (Some(_), None) => Err(EngineError::new(
                ErrorClass::Corrupt,
                "applied stamp present but execution frontier missing",
            )),
            (None, Some(_)) => Err(EngineError::new(
                ErrorClass::Corrupt,
                "execution frontier present but applied stamp missing",
            )),
        }
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

/// Lower one record's updates, the record at `seq`, into the transaction,
/// through the key assignment (task-d113): a dependency or proposal row
/// named by command is written under its intake key, a payload to the
/// pending collection by intake order, and an executed row by its
/// position with its payload moved (step 2).
///
/// Each update is written before the next is assigned, so a later update
/// of the same record finds an earlier one's row.
pub fn lower_record<W: WriteTxn>(
    tx: &mut W,
    assigner: &mut Assigner<'_>,
    seq: u64,
    updates: &[StoreUpdate],
) -> Result<(), EngineError> {
    for (i, update) in updates.iter().enumerate() {
        let ordinal = u32::try_from(i)
            .map_err(|_| EngineError::new(ErrorClass::Limit, "a record of too many updates"))?;
        assigner.observe_kv(update, |key| tx.get(update.collection, key))?;
        let assigned = assigner.assign(seq, ordinal, update, |collection, key| {
            tx.get(collection, key)
        })?;
        match assigned {
            Assigned::AsIs => lower_update(tx, update)?,
            Assigned::Key(key) => match &update.value {
                Some(v) => tx.put(update.collection, &key, v)?,
                None => tx.delete(update.collection, &key)?,
            },
            Assigned::Skip => {}
            Assigned::Writes(writes) => {
                for write in &writes {
                    lower_update(tx, write)?;
                }
            }
        }
    }
    Ok(())
}

/// A commit's rows, lowered through the key assignment ahead of writing
/// them, so the engine can take them as one batch
/// ([`WriteTxn::apply_updates`]: each table opened once a commit, not once
/// a row, task-d86) while each update is still assigned against the rows
/// the updates before it leave (task-d113).
///
/// A row this commit has written is read from here; any other from the
/// transaction, which holds none of this commit's rows yet.
#[derive(Debug, Default)]
pub struct Lowered {
    writes: Vec<StoreUpdate>,
    /// Each collection's rows by key, to their place in `writes`. Looked
    /// up by a borrowed key, so a read through the overlay allocates
    /// nothing; a commit touches a handful of collections, found by a
    /// scan.
    at: Vec<(CollectionId, HashMap<Vec<u8>, usize>)>,
}

/// Rows a collection's index is sized for when a commit first writes to
/// it: about what a commit of a dozen commands writes to its busiest
/// collection, so a typical commit's index never rehashes.
const ROWS_A_COLLECTION: usize = 32;

/// Where `key` in `collection` stands in `writes`, if this commit wrote it.
fn written(
    at: &[(CollectionId, HashMap<Vec<u8>, usize>)],
    collection: CollectionId,
    key: &[u8],
) -> Option<usize> {
    at.iter()
        .find(|(c, _)| *c == collection)
        .and_then(|(_, rows)| rows.get(key).copied())
}

impl Lowered {
    /// Nothing lowered yet.
    pub fn new() -> Self {
        Lowered::default()
    }

    /// Lower the record at `seq` as [`lower_record`] does, reading `base`
    /// under the rows lowered so far, and keep its rows.
    pub fn lower_record<R: OrderedRead>(
        &mut self,
        base: &R,
        assigner: &mut Assigner<'_>,
        seq: u64,
        updates: &[StoreUpdate],
    ) -> Result<(), EngineError> {
        for (i, update) in updates.iter().enumerate() {
            let ordinal = u32::try_from(i)
                .map_err(|_| EngineError::new(ErrorClass::Limit, "a record of too many updates"))?;
            let (writes, at) = (&self.writes, &self.at);
            // A row this commit has written is read from here, any other
            // from `base`: the key index's old value (task-d105) and the
            // assignment's reads alike.
            let overlay = |collection: CollectionId, key: &[u8]| match written(at, collection, key)
            {
                Some(i) => Ok(writes[i].value.clone()),
                None => base.get(collection, key),
            };
            assigner.observe_kv(update, |key| overlay(update.collection, key))?;
            let assigned = assigner.assign(seq, ordinal, update, overlay)?;
            match assigned {
                Assigned::AsIs => self.keep(update.clone()),
                Assigned::Key(key) => self.keep(StoreUpdate {
                    collection: update.collection,
                    key,
                    value: update.value.clone(),
                }),
                Assigned::Skip => {}
                Assigned::Writes(writes) => {
                    for write in writes {
                        self.keep(write);
                    }
                }
            }
        }
        Ok(())
    }

    /// Keep one row; a later row for the same key is the one read.
    ///
    /// A `meta_v1` row this commit has already written takes the later
    /// value in its place, so each meta key is written once a commit, not
    /// once a command (task-d117 step 1, design Section 17.17 point 5).
    /// Application puts `kv_revision`, `retention_floor` and
    /// `lease_authority` as ordinary updates, about one meta row a command.
    /// Rows under other keys never depend on where a meta row stands, so
    /// the commit leaves the same rows; the key index saw every one of
    /// them as it was assigned, before it got here.
    fn keep(&mut self, update: StoreUpdate) {
        let place = self.writes.len();
        let rows = match self.at.iter().position(|(c, _)| *c == update.collection) {
            Some(i) => &mut self.at[i].1,
            None => {
                self.at
                    .push((update.collection, HashMap::with_capacity(ROWS_A_COLLECTION)));
                &mut self.at.last_mut().expect("just pushed").1
            }
        };
        if update.collection == Collection::MetaV1.id()
            && let Some(&earlier) = rows.get(&update.key)
        {
            self.writes[earlier].value = update.value;
            return;
        }
        rows.insert(update.key.clone(), place);
        self.writes.push(update);
    }

    /// The rows, in the order lowered: a key written twice keeps its last
    /// value when they are applied in order, and a `meta_v1` key is here
    /// once, with its last value, where it was first written.
    pub fn updates(&self) -> &[StoreUpdate] {
        &self.writes
    }
}

/// Lower one update into the transaction, under its own key.
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

#[cfg(test)]
mod tests {
    use super::*;

    fn row(collection: u16, key: &[u8], value: Option<&[u8]>) -> StoreUpdate {
        StoreUpdate {
            collection: CollectionId(collection),
            key: key.to_vec(),
            value: value.map(<[u8]>::to_vec),
        }
    }

    /// The commit's own rows are found by collection and key: one key
    /// under two collections is two rows, and a key written twice is read
    /// as its later write, a delete included.
    #[test]
    fn a_commit_finds_its_own_rows_by_collection_and_key() {
        let mut lowered = Lowered::new();
        lowered.keep(row(7, b"k", Some(b"one")));
        lowered.keep(row(8, b"k", Some(b"two")));
        lowered.keep(row(7, b"other", Some(b"three")));
        lowered.keep(row(7, b"k", None));
        let at = &lowered.at;
        assert_eq!(written(at, CollectionId(7), b"k"), Some(3));
        assert_eq!(lowered.updates()[3].value, None);
        assert_eq!(written(at, CollectionId(8), b"k"), Some(1));
        assert_eq!(written(at, CollectionId(7), b"other"), Some(2));
        assert_eq!(written(at, CollectionId(8), b"other"), None);
        assert_eq!(written(at, CollectionId(9), b"k"), None);
        // Every row stays in `writes`, in the order lowered.
        assert_eq!(lowered.updates().len(), 4);
    }

    /// A `meta_v1` key written by every command of a commit is one row,
    /// where it was first written, with the last value, a delete included;
    /// the overlay reads that value. Any other collection keeps every row.
    #[test]
    fn a_meta_key_is_one_row_a_commit_with_its_last_value() {
        let meta = Collection::MetaV1.id().0;
        let revision = meta_fields::KV_REVISION;
        let floor = meta_fields::RETENTION_FLOOR;
        let mut lowered = Lowered::new();
        lowered.keep(row(meta, revision, Some(b"1")));
        lowered.keep(row(7, b"k", Some(b"one")));
        lowered.keep(row(meta, revision, Some(b"2")));
        lowered.keep(row(meta, floor, Some(b"f")));
        lowered.keep(row(7, b"k", Some(b"two")));
        lowered.keep(row(meta, revision, Some(b"3")));
        lowered.keep(row(meta, floor, None));
        let rows: Vec<_> = lowered
            .updates()
            .iter()
            .map(|u| (u.collection.0, u.key.as_slice(), u.value.as_deref()))
            .collect();
        assert_eq!(
            rows,
            vec![
                (meta, revision, Some(&b"3"[..])),
                (7, &b"k"[..], Some(&b"one"[..])),
                (meta, floor, None),
                (7, &b"k"[..], Some(&b"two"[..])),
            ]
        );
        let at = &lowered.at;
        assert_eq!(written(at, CollectionId(meta), revision), Some(0));
        assert_eq!(written(at, CollectionId(meta), floor), Some(2));
        assert_eq!(written(at, CollectionId(7), b"k"), Some(3));
    }

    /// More rows than an index is sized for: it grows and still finds
    /// every row.
    #[test]
    fn a_collection_past_its_sized_rows_still_finds_each_one() {
        let mut lowered = Lowered::new();
        for i in 0..(ROWS_A_COLLECTION * 3) {
            lowered.keep(row(7, &i.to_be_bytes(), Some(&[1])));
        }
        for i in 0..(ROWS_A_COLLECTION * 3) {
            assert_eq!(
                written(&lowered.at, CollectionId(7), &i.to_be_bytes()),
                Some(i)
            );
        }
    }
}

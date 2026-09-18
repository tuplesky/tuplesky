//! Per-node logical storage model: volatile writes become durable after a
//! controlled delay unless a fault fails them; a crash discards everything
//! not yet durable. This is fidelity level A (design Section 21.2): it
//! models the contract, not engine bytes.

use std::collections::BTreeMap;

use coord_core::effect::{BarrierId, PersistBatch};
use coord_core::event::StorageError;
use coord_types::ids::LocalJournalSeq;

/// Durable and volatile images of one node.
#[derive(Debug, Default)]
pub struct StorageModel {
    durable_rows: BTreeMap<(u16, Vec<u8>), Vec<u8>>,
    durable_seq: u64,
    volatile: BTreeMap<BarrierId, PersistBatch>,
    /// Batches whose completion was already reported; a duplicate completion
    /// is a modeling bug and is rejected.
    completed: BTreeMap<BarrierId, Result<LocalJournalSeq, StorageError>>,
}

impl StorageModel {
    /// Accept a batch into the volatile image.
    pub fn submit(&mut self, batch: PersistBatch) {
        self.volatile.insert(batch.barrier, batch);
    }

    /// Make a volatile batch durable; returns its local sequence.
    pub fn complete(&mut self, barrier: BarrierId) -> Option<LocalJournalSeq> {
        let batch = self.volatile.remove(&barrier)?;
        self.durable_seq += 1;
        for update in batch.updates {
            match update.value {
                Some(v) => {
                    self.durable_rows
                        .insert((update.collection.0, update.key), v);
                }
                None => {
                    self.durable_rows.remove(&(update.collection.0, update.key));
                }
            }
        }
        let seq = LocalJournalSeq::new(self.durable_seq).expect("bounded");
        self.completed.insert(barrier, Ok(seq));
        Some(seq)
    }

    /// Fail a volatile batch with `error`; it never becomes durable.
    pub fn fail(&mut self, barrier: BarrierId, error: StorageError) -> bool {
        if self.volatile.remove(&barrier).is_none() {
            return false;
        }
        self.completed.insert(barrier, Err(error));
        true
    }

    /// Crash: the volatile image is lost; the durable image survives.
    pub fn crash(&mut self) -> usize {
        let lost = self.volatile.len();
        self.volatile.clear();
        lost
    }

    /// Durable rows as `(collection, key, value)`.
    pub fn durable_rows(&self) -> Vec<(u16, Vec<u8>, Vec<u8>)> {
        self.durable_rows
            .iter()
            .map(|((c, k), v)| (*c, k.clone(), v.clone()))
            .collect()
    }

    /// Whether a barrier completed durably.
    pub fn is_durable(&self, barrier: &BarrierId) -> bool {
        matches!(self.completed.get(barrier), Some(Ok(_)))
    }

    /// Durable sequence head.
    pub const fn durable_seq(&self) -> u64 {
        self.durable_seq
    }

    /// Number of volatile batches.
    pub fn volatile_len(&self) -> usize {
        self.volatile.len()
    }
}

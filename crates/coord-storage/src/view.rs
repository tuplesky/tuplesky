//! The durable-view gate.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use coord_store_api::engine::{EngineError, OrderedRead, SnapshotSource};
use coord_store_api::seq::StoreSeq;

use crate::lowering::DurableMeta;

/// Why a view was not handed out.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ViewError {
    /// Engine failure.
    Engine(EngineError),
    /// The snapshot's stamp is ahead of what this boot has completed
    /// (for example an unreconciled indeterminate commit); hold it.
    AheadOfCompletion {
        /// Stamp the snapshot carries.
        snapshot: StoreSeq,
        /// Sequence completed by the worker.
        completed: StoreSeq,
    },
    /// The worker quarantined the projection.
    Quarantined,
}

impl From<EngineError> for ViewError {
    fn from(e: EngineError) -> Self {
        ViewError::Engine(e)
    }
}

/// Shared frontier: completed store sequence and quarantine flag.
#[derive(Debug, Default)]
pub struct Frontier {
    completed: AtomicU64,
    quarantined: std::sync::atomic::AtomicBool,
}

impl Frontier {
    pub(crate) fn set_completed(&self, seq: StoreSeq) {
        self.completed
            .store(seq.journal_seq().get(), Ordering::Release);
    }

    pub(crate) fn quarantine(&self) {
        self.quarantined.store(true, Ordering::Release);
    }

    /// Completed sequence.
    pub fn completed(&self) -> u64 {
        self.completed.load(Ordering::Acquire)
    }

    /// Whether the projection is quarantined.
    pub fn is_quarantined(&self) -> bool {
        self.quarantined.load(Ordering::Acquire)
    }
}

/// A snapshot together with the stamp it proves.
pub struct GatedView<V> {
    view: V,
    meta: DurableMeta,
}

impl<V: OrderedRead> GatedView<V> {
    /// The snapshot.
    pub fn view(&self) -> &V {
        &self.view
    }

    /// Durable metadata the snapshot reflects.
    pub fn meta(&self) -> &DurableMeta {
        &self.meta
    }

    /// Store sequence the snapshot covers.
    pub fn store_seq(&self) -> StoreSeq {
        self.meta.stamp.store_seq
    }
}

/// Reader that only hands out snapshots covered by the completed frontier.
#[derive(Clone)]
pub struct GatedReader<R> {
    reader: R,
    frontier: Arc<Frontier>,
}

impl<R: SnapshotSource> GatedReader<R> {
    pub(crate) fn new(reader: R, frontier: Arc<Frontier>) -> Self {
        GatedReader { reader, frontier }
    }

    /// Pin a snapshot and verify its stamp against the completed frontier.
    pub fn snapshot(&self) -> Result<GatedView<R::View>, ViewError> {
        if self.frontier.is_quarantined() {
            return Err(ViewError::Quarantined);
        }
        let view = self.reader.snapshot()?;
        let meta = DurableMeta::read(&view)?;
        let completed = self.frontier.completed();
        if meta.stamp.store_seq.journal_seq().get() > completed {
            return Err(ViewError::AheadOfCompletion {
                snapshot: meta.stamp.store_seq,
                completed: StoreSeq::from_journal(
                    coord_types::ids::LocalJournalSeq::new(completed).expect("bounded"),
                ),
            });
        }
        Ok(GatedView { view, meta })
    }
}

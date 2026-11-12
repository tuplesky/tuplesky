//! The durable-view gate.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard};

use coord_store_api::engine::{EngineError, OrderedRead, SnapshotSource};
use coord_store_api::seq::StoreSeq;

use crate::intake::{IndexDelta, IntakeIndex};
use crate::kv_index::KvIndex;
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

/// Shared frontier: completed store sequence and quarantine flag, and the
/// projection's index by command of its intake-keyed rows (task-d113).
///
/// The index lives here because it moves with the gate: a commit's
/// changes to it are published before the gate moves to the commit's
/// stamp, so a snapshot the gate hands out is never ahead of the index.
#[derive(Debug, Default)]
pub struct Frontier {
    completed: AtomicU64,
    /// The stamp of a projection commit that is running, or zero.
    committing: AtomicU64,
    quarantined: std::sync::atomic::AtomicBool,
    index: RwLock<IntakeIndex>,
    /// The key index of the retained event rows (task-d105). It moves
    /// with the intake index: one delta carries both.
    kv: RwLock<KvIndex>,
}

impl Frontier {
    pub(crate) fn set_completed(&self, seq: StoreSeq) {
        self.completed
            .store(seq.journal_seq().get(), Ordering::Release);
    }

    /// A projection commit stamped `seq` is about to run (task-d52).
    ///
    /// Its rows can become visible to a snapshot an instant before the
    /// commit returns and [`Frontier::set_completed`] moves the gate. A
    /// reader that pins one in that instant waits for the commit to end
    /// rather than being refused: once the materializer commits on its
    /// own thread, that instant is no longer one no reader can see.
    pub(crate) fn begin_commit(&self, seq: StoreSeq) {
        self.committing
            .store(seq.journal_seq().get(), Ordering::Release);
    }

    /// The running commit has ended, whatever its outcome; the gate has
    /// moved already if it returned.
    pub(crate) fn end_commit(&self) {
        self.committing.store(0, Ordering::Release);
    }

    fn committing(&self) -> u64 {
        self.committing.load(Ordering::Acquire)
    }

    /// A quarantined store drops its index with it (rule (d)); the next
    /// open rebuilds it from the rows.
    pub(crate) fn quarantine(&self) {
        self.quarantined.store(true, Ordering::Release);
        self.index_mut().clear();
        self.kv_mut().clear();
    }

    /// Install the index an open built from the projection.
    pub(crate) fn install_index(&self, index: IntakeIndex) {
        *self.index_mut() = index;
    }

    /// Publish a commit's changes to the index. Called before the gate
    /// moves to `stamp`.
    pub(crate) fn publish_index(&self, mut delta: IndexDelta, stamp: StoreSeq) {
        let kv = std::mem::take(&mut delta.kv);
        self.kv_mut().publish(kv);
        self.index_mut().publish(delta, stamp.journal_seq().get());
    }

    /// Install the key index an open built from the projection
    /// (task-d105).
    pub(crate) fn install_kv_index(&self, index: KvIndex) {
        *self.kv_mut() = index;
    }

    /// The key index as published (task-d105).
    pub fn kv_index(&self) -> RwLockReadGuard<'_, KvIndex> {
        self.kv
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn kv_mut(&self) -> std::sync::RwLockWriteGuard<'_, KvIndex> {
        self.kv
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The index as published.
    pub(crate) fn index(&self) -> RwLockReadGuard<'_, IntakeIndex> {
        // A panic while the index was written leaves it as it was then;
        // its readers settle every answer against the rows.
        self.index
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn index_mut(&self) -> std::sync::RwLockWriteGuard<'_, IntakeIndex> {
        self.index
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Entries the index holds (diagnostic).
    pub fn index_len(&self) -> usize {
        self.index().len()
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
    /// The gate it came through, whose index by command it may ask about
    /// executed commands (rule (c)); `None` for a view made without one.
    gate: Option<Arc<Frontier>>,
}

impl<V: OrderedRead> GatedView<V> {
    /// Bind a snapshot to the metadata it proves, with no gate behind it.
    /// Only the crate's own tests make one.
    #[cfg(test)]
    pub(crate) const fn new(view: V, meta: DurableMeta) -> Self {
        GatedView {
            view,
            meta,
            gate: None,
        }
    }

    /// Bind a snapshot to the metadata it proves, and to the gate whose
    /// index answers for it. Only the gate (and the crate's own tests) may
    /// claim that pairing.
    pub(crate) fn with_gate(view: V, meta: DurableMeta, gate: Option<Arc<Frontier>>) -> Self {
        GatedView { view, meta, gate }
    }

    /// The gate this view came through.
    pub(crate) fn gate(&self) -> Option<&Arc<Frontier>> {
        self.gate.as_ref()
    }

    /// The snapshot.
    pub fn view(&self) -> &V {
        &self.view
    }

    /// Run `f` over the key index as published (task-d105), or over one
    /// built from this snapshot when the view has no gate.
    pub fn with_kv_index<T>(&self, f: impl FnOnce(&KvIndex) -> T) -> Result<T, EngineError>
    where
        V: OrderedRead,
    {
        match &self.gate {
            Some(gate) => Ok(f(&gate.kv_index())),
            None => Ok(f(&KvIndex::build(&self.view)?)),
        }
    }

    /// Durable metadata the snapshot reflects.
    pub fn meta(&self) -> &DurableMeta {
        &self.meta
    }

    /// Store sequence the snapshot covers.
    pub fn store_seq(&self) -> StoreSeq {
        self.meta.stamp.store_seq()
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
        let stamp = meta.stamp.store_seq().journal_seq().get();
        let mut completed = self.frontier.completed();
        // A snapshot of a commit that is still returning: its rows are
        // visible and its gate is about to move. The commit is waited
        // out, not refused; the moment is the commit's own return, and a
        // commit that fails leaves `committing` without moving the gate.
        while stamp > completed && self.frontier.committing() == stamp {
            std::thread::yield_now();
            completed = self.frontier.completed();
        }
        // `end_commit` follows `set_completed`, so a commit that ended
        // between the two loads above has moved the gate by now.
        completed = self.frontier.completed();
        if stamp > completed {
            return Err(ViewError::AheadOfCompletion {
                snapshot: meta.stamp.store_seq(),
                completed: StoreSeq::from_journal(
                    coord_types::ids::LocalJournalSeq::new(completed).expect("bounded"),
                ),
            });
        }
        Ok(GatedView::with_gate(
            view,
            meta,
            Some(self.frontier.clone()),
        ))
    }

    /// A copy of the index by command as published (task-d113): for
    /// checking it against the rows ([`IntakeIndex::build`]), and for
    /// diagnostics. Readers ask it through the view, never through this.
    pub fn intake_index(&self) -> IntakeIndex {
        self.frontier.index().clone()
    }

    /// The journal sequence the completed frontier has reached: a
    /// snapshot pinned now carries a stamp at or below it. One atomic
    /// load, no snapshot.
    ///
    /// Only a completed projection commit moves it, so while it reads
    /// what it read before a snapshot was pinned, a new snapshot would
    /// show what that one did (task-d58).
    pub fn completed(&self) -> u64 {
        self.frontier.completed()
    }
}

#[cfg(test)]
mod committing {
    //! The moment between a projection commit making its rows visible and
    //! the gate moving (task-d52). Once the materializer commits on a
    //! thread of its own, a reader on the domain thread can land in it.

    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use coord_store_api::engine::{LocalEngine, WriteTxn};
    use coord_store_api::envelope::AppliedStamp;
    use coord_store_api::seq::StoreSeq;
    use coord_store_testkit::model::ModelEngine;
    use coord_types::identity::Digest32;
    use coord_types::ids::LocalJournalSeq;

    use super::{Frontier, GatedReader, ViewError};
    use crate::lowering::{DurableMeta, ExecutionFrontier};

    fn stamp(n: u64) -> StoreSeq {
        StoreSeq::from_journal(LocalJournalSeq::new(n).unwrap())
    }

    /// A projection whose committed stamp is `n`, as the materializer's
    /// commit would have left it.
    fn committed_at(n: u64) -> ModelEngine {
        let mut engine = ModelEngine::new();
        let mut tx = engine.begin_write().unwrap();
        DurableMeta {
            stamp: AppliedStamp::new(stamp(n), Digest32([n as u8; 32])),
            frontier: ExecutionFrontier::INITIAL,
        }
        .write(&mut tx)
        .unwrap();
        tx.commit_durable().unwrap();
        engine
    }

    #[test]
    fn a_snapshot_of_a_commit_still_returning_waits_for_the_gate() {
        let engine = committed_at(5);
        let frontier = Arc::new(Frontier::default());
        frontier.set_completed(stamp(4));
        frontier.begin_commit(stamp(5));
        let reader = GatedReader::new(engine.reader(), frontier.clone());
        let started = Instant::now();
        let materializer = std::thread::spawn({
            let frontier = frontier.clone();
            move || {
                std::thread::sleep(Duration::from_millis(50));
                frontier.set_completed(stamp(5));
                frontier.end_commit();
            }
        });
        let view = reader.snapshot().expect("waited for the commit to return");
        assert_eq!(view.store_seq(), stamp(5));
        assert!(started.elapsed() >= Duration::from_millis(40));
        materializer.join().unwrap();
    }

    #[test]
    fn a_snapshot_of_a_commit_that_failed_is_refused() {
        let engine = committed_at(5);
        let frontier = Arc::new(Frontier::default());
        frontier.set_completed(stamp(4));
        frontier.begin_commit(stamp(5));
        let reader = GatedReader::new(engine.reader(), frontier.clone());
        let materializer = std::thread::spawn({
            let frontier = frontier.clone();
            move || {
                std::thread::sleep(Duration::from_millis(20));
                // An indeterminate commit: the gate does not move.
                frontier.end_commit();
            }
        });
        assert!(matches!(
            reader.snapshot(),
            Err(ViewError::AheadOfCompletion { .. })
        ));
        materializer.join().unwrap();
    }

    #[test]
    fn a_snapshot_ahead_of_the_gate_with_no_commit_running_is_refused_at_once() {
        let engine = committed_at(5);
        let frontier = Arc::new(Frontier::default());
        frontier.set_completed(stamp(4));
        let reader = GatedReader::new(engine.reader(), frontier);
        assert!(matches!(
            reader.snapshot(),
            Err(ViewError::AheadOfCompletion { .. })
        ));
    }
}

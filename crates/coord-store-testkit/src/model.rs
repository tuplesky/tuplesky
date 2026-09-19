//! The deterministic model engine.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};

use coord_core::effect::CollectionId;
use coord_store_api::engine::{
    CommitFailure, Direction, EngineError, ErrorClass, LocalEngine, OrderedRead, Row, RowPage,
    ScanRequest, SnapshotSource, WriteTxn,
};

type Rows = BTreeMap<(u16, Vec<u8>), Vec<u8>>;
type Pending = BTreeMap<(u16, Vec<u8>), Option<Vec<u8>>>;
type RowIter<'a> = Box<dyn Iterator<Item = (&'a (u16, Vec<u8>), &'a Vec<u8>)> + 'a>;

/// Deliberate misbehaviors the conformance suite must detect. The honest
/// model enables none of them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Misbehavior {
    /// Commit applies only the first half of the updates.
    TornWrites,
    /// Snapshots observe later commits instead of a pinned image.
    MixedSnapshots,
    /// Scans ignore the upper bound.
    ReversedBounds,
    /// An injected iterator error is reported as end of data.
    SwallowedIteratorErrors,
    /// `commit_durable` succeeds but the batch is lost on crash.
    FalseDurability,
    /// Pending writes of an open transaction are visible to snapshots.
    EarlyVisibility,
}

/// Scripted outcome of the next commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitScript {
    /// Commit durably.
    Durable,
    /// Fail with definite noncommit evidence.
    DefinitelyNotCommitted,
    /// Fail with an indeterminate error; the batch is durably applied when
    /// `applied` is true, otherwise absent.
    Indeterminate {
        /// Whether the batch actually reached durable storage.
        applied: bool,
    },
}

/// Distinct model events (none is protocol establishment).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ModelEvent {
    /// A transaction's rows became visible to new snapshots.
    Visible {
        /// Transaction number.
        txn: u64,
    },
    /// A transaction's rows became durable.
    Durable {
        /// Transaction number.
        txn: u64,
    },
    /// A checkpoint was published (reserved capability; unused here).
    CheckpointPublished,
    /// The engine was reopened after a crash; lost transactions listed.
    Reopened {
        /// Transactions that were visible but not durable.
        lost: Vec<u64>,
    },
}

#[derive(Default)]
struct Shared {
    /// Visible image.
    visible: Rows,
    /// Durable image (what survives a crash).
    durable: Rows,
    /// Per-transaction durable flag for lost-on-crash accounting.
    volatile_txns: BTreeSet<u64>,
    /// Pending rows of the open transaction (for the early-visibility misbehavior).
    open_pending: Option<Pending>,
    events: Vec<ModelEvent>,
    misbehaviors: BTreeSet<Misbehavior>,
    scripts: VecDeque<CommitScript>,
    /// Inject an iterator error after this many rows in the next scan.
    iterator_error_after: Option<usize>,
    /// Fail the next `begin_write`.
    fail_begin_write: bool,
    /// Fail a write transaction's Nth `put`/`delete` (1 = the first).
    fail_write_at: Option<usize>,
    /// Writes seen since the current transaction opened.
    writes_in_txn: usize,
    txn_counter: u64,
}

/// The model engine.
pub struct ModelEngine {
    shared: Arc<Mutex<Shared>>,
    writer_open: bool,
}

impl Default for ModelEngine {
    fn default() -> Self {
        ModelEngine::new()
    }
}

impl ModelEngine {
    /// Honest model.
    pub fn new() -> Self {
        ModelEngine {
            shared: Arc::new(Mutex::new(Shared::default())),
            writer_open: false,
        }
    }

    /// Model with the given misbehaviors enabled.
    pub fn misbehaving(misbehaviors: &[Misbehavior]) -> Self {
        let engine = ModelEngine::new();
        engine.shared.lock().unwrap().misbehaviors = misbehaviors.iter().copied().collect();
        engine
    }

    /// Script the outcome of upcoming commits (consumed in order).
    pub fn script_commit(&self, script: CommitScript) {
        self.shared.lock().unwrap().scripts.push_back(script);
    }

    /// Inject an iterator error after `rows` rows in the next scan.
    pub fn inject_iterator_error(&self, rows: usize) {
        self.shared.lock().unwrap().iterator_error_after = Some(rows);
    }

    /// Fail the next attempt to open a write transaction.
    pub fn inject_begin_write_error(&self) {
        self.shared.lock().unwrap().fail_begin_write = true;
    }

    /// Fail the `nth` write (put or delete) of the next transaction,
    /// counting from one. Lowering an update and writing the projection
    /// metadata are both writes, so this reaches every pre-commit step.
    pub fn inject_write_error_at(&self, nth: usize) {
        let mut s = self.shared.lock().unwrap();
        s.fail_write_at = Some(nth);
        s.writes_in_txn = 0;
    }

    /// Stop injecting projection faults.
    pub fn clear_injected_faults(&self) {
        let mut s = self.shared.lock().unwrap();
        s.fail_begin_write = false;
        s.fail_write_at = None;
        s.writes_in_txn = 0;
    }

    /// Crash and reopen: everything not durable is lost.
    pub fn crash_and_reopen(&mut self) {
        let mut s = self.shared.lock().unwrap();
        let lost: Vec<u64> = s.volatile_txns.iter().copied().collect();
        s.volatile_txns.clear();
        s.visible = s.durable.clone();
        s.open_pending = None;
        s.events.push(ModelEvent::Reopened { lost });
        self.writer_open = false;
    }

    /// Recorded events.
    pub fn events(&self) -> Vec<ModelEvent> {
        self.shared.lock().unwrap().events.clone()
    }

    /// Durable rows as `(collection, key, value)`.
    pub fn durable_rows(&self) -> Vec<(u16, Vec<u8>, Vec<u8>)> {
        self.shared
            .lock()
            .unwrap()
            .durable
            .iter()
            .map(|((c, k), v)| (*c, k.clone(), v.clone()))
            .collect()
    }
}

fn scan_rows(
    rows: &Rows,
    collection: CollectionId,
    request: &ScanRequest,
    misbehaviors: &BTreeSet<Misbehavior>,
    error_after: Option<usize>,
) -> Result<RowPage, EngineError> {
    let ignore_upper = misbehaviors.contains(&Misbehavior::ReversedBounds);
    let swallow = misbehaviors.contains(&Misbehavior::SwallowedIteratorErrors);
    let in_upper = |k: &[u8]| -> bool {
        if ignore_upper {
            return true;
        }
        match &request.upper {
            std::ops::Bound::Unbounded => true,
            std::ops::Bound::Included(u) => k <= u.as_slice(),
            std::ops::Bound::Excluded(u) => k < u.as_slice(),
        }
    };
    let in_lower = |k: &[u8]| -> bool {
        match &request.lower {
            std::ops::Bound::Unbounded => true,
            std::ops::Bound::Included(l) => k >= l.as_slice(),
            std::ops::Bound::Excluded(l) => k > l.as_slice(),
        }
    };
    let iter: RowIter<'_> = match request.direction {
        Direction::Forward => Box::new(rows.iter()),
        Direction::Reverse => Box::new(rows.iter().rev()),
    };
    let mut out = Vec::new();
    let mut bytes = 0usize;
    let mut visited = 0usize;
    for ((c, k), v) in iter {
        if *c != collection.0 || !in_lower(k) || !in_upper(k) || !request.past_cursor(k) {
            continue;
        }
        if let Some(n) = error_after
            && visited >= n
        {
            if swallow {
                return Ok(RowPage {
                    rows: out,
                    exhausted: true,
                });
            }
            return Err(EngineError::new(ErrorClass::Io, "injected iterator error"));
        }
        visited += 1;
        if out.len() as u32 >= request.max_rows.get() {
            return Ok(RowPage {
                rows: out,
                exhausted: false,
            });
        }
        let cost = k.len() + v.len();
        if bytes + cost > request.max_bytes.get() as usize {
            if out.is_empty() {
                return Err(EngineError::new(
                    ErrorClass::Limit,
                    "next row exceeds page byte budget",
                ));
            }
            return Ok(RowPage {
                rows: out,
                exhausted: false,
            });
        }
        bytes += cost;
        out.push(Row {
            key: k.clone(),
            value: v.clone(),
        });
    }
    Ok(RowPage {
        rows: out,
        exhausted: true,
    })
}

/// Reader handle.
#[derive(Clone)]
pub struct ModelReader(Arc<Mutex<Shared>>);

/// A pinned view (or, under `MixedSnapshots`, a live one).
pub struct ModelView {
    pinned: Option<Rows>,
    shared: Arc<Mutex<Shared>>,
}

impl OrderedRead for ModelView {
    fn get(&self, collection: CollectionId, key: &[u8]) -> Result<Option<Vec<u8>>, EngineError> {
        let k = (collection.0, key.to_vec());
        match &self.pinned {
            Some(rows) => Ok(rows.get(&k).cloned()),
            None => Ok(self.shared.lock().unwrap().visible.get(&k).cloned()),
        }
    }

    fn scan_page(
        &self,
        collection: CollectionId,
        request: &ScanRequest,
    ) -> Result<RowPage, EngineError> {
        let mut s = self.shared.lock().unwrap();
        let error_after = s.iterator_error_after.take();
        let misbehaviors = s.misbehaviors.clone();
        match &self.pinned {
            Some(rows) => scan_rows(rows, collection, request, &misbehaviors, error_after),
            None => scan_rows(&s.visible, collection, request, &misbehaviors, error_after),
        }
    }
}

impl SnapshotSource for ModelReader {
    type View = ModelView;

    fn snapshot(&self) -> Result<ModelView, EngineError> {
        let s = self.0.lock().unwrap();
        if s.misbehaviors.contains(&Misbehavior::MixedSnapshots) {
            return Ok(ModelView {
                pinned: None,
                shared: self.0.clone(),
            });
        }
        let mut rows = s.visible.clone();
        if s.misbehaviors.contains(&Misbehavior::EarlyVisibility)
            && let Some(pending) = &s.open_pending
        {
            for (k, v) in pending {
                match v {
                    Some(v) => {
                        rows.insert(k.clone(), v.clone());
                    }
                    None => {
                        rows.remove(k);
                    }
                }
            }
        }
        Ok(ModelView {
            pinned: Some(rows),
            shared: self.0.clone(),
        })
    }
}

/// The unique write transaction (worker-local).
pub struct ModelWrite<'a> {
    engine: &'a mut ModelEngine,
    pending: Pending,
    finished: bool,
}

impl ModelWrite<'_> {
    /// Whether this write is the one a test asked to fail.
    fn injected_write_failure(&self) -> Result<(), EngineError> {
        let mut s = self.engine.shared.lock().unwrap();
        s.writes_in_txn += 1;
        if s.fail_write_at == Some(s.writes_in_txn) {
            s.fail_write_at = None;
            return Err(EngineError::new(ErrorClass::Io, "injected: write"));
        }
        Ok(())
    }

    fn merged(&self) -> Rows {
        let mut rows = self.engine.shared.lock().unwrap().visible.clone();
        for (k, v) in &self.pending {
            match v {
                Some(v) => {
                    rows.insert(k.clone(), v.clone());
                }
                None => {
                    rows.remove(k);
                }
            }
        }
        rows
    }

    fn sync_pending(&self) {
        let mut s = self.engine.shared.lock().unwrap();
        if s.misbehaviors.contains(&Misbehavior::EarlyVisibility) {
            s.open_pending = Some(self.pending.clone());
        }
    }
}

impl OrderedRead for ModelWrite<'_> {
    fn get(&self, collection: CollectionId, key: &[u8]) -> Result<Option<Vec<u8>>, EngineError> {
        Ok(self.merged().get(&(collection.0, key.to_vec())).cloned())
    }

    fn scan_page(
        &self,
        collection: CollectionId,
        request: &ScanRequest,
    ) -> Result<RowPage, EngineError> {
        let rows = self.merged();
        let mut s = self.engine.shared.lock().unwrap();
        let error_after = s.iterator_error_after.take();
        let misbehaviors = s.misbehaviors.clone();
        scan_rows(&rows, collection, request, &misbehaviors, error_after)
    }
}

impl WriteTxn for ModelWrite<'_> {
    fn put(
        &mut self,
        collection: CollectionId,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), EngineError> {
        self.injected_write_failure()?;
        self.pending
            .insert((collection.0, key.to_vec()), Some(value.to_vec()));
        self.sync_pending();
        Ok(())
    }

    fn delete(&mut self, collection: CollectionId, key: &[u8]) -> Result<(), EngineError> {
        self.injected_write_failure()?;
        self.pending.insert((collection.0, key.to_vec()), None);
        self.sync_pending();
        Ok(())
    }

    fn abort(mut self) -> Result<(), EngineError> {
        self.finished = true;
        Ok(())
    }

    fn commit_durable(mut self) -> Result<(), CommitFailure> {
        self.finished = true;
        let mut s = self.engine.shared.lock().unwrap();
        let script = s.scripts.pop_front().unwrap_or(CommitScript::Durable);
        let torn = s.misbehaviors.contains(&Misbehavior::TornWrites);
        let false_durability = s.misbehaviors.contains(&Misbehavior::FalseDurability);
        s.txn_counter += 1;
        let txn = s.txn_counter;
        let apply =
            |rows: &mut Rows, pending: &BTreeMap<(u16, Vec<u8>), Option<Vec<u8>>>, limit: usize| {
                for (k, v) in pending.iter().take(limit) {
                    match v {
                        Some(v) => {
                            rows.insert(k.clone(), v.clone());
                        }
                        None => {
                            rows.remove(k);
                        }
                    }
                }
            };
        let limit = if torn {
            self.pending.len().div_ceil(2)
        } else {
            self.pending.len()
        };
        match script {
            CommitScript::Durable => {
                apply(&mut s.visible, &self.pending, limit);
                s.events.push(ModelEvent::Visible { txn });
                if false_durability {
                    s.volatile_txns.insert(txn);
                } else {
                    apply(&mut s.durable, &self.pending, limit);
                    s.events.push(ModelEvent::Durable { txn });
                }
                Ok(())
            }
            CommitScript::DefinitelyNotCommitted => Err(CommitFailure::DefinitelyNotCommitted(
                EngineError::new(ErrorClass::Busy, "scripted definite noncommit"),
            )),
            CommitScript::Indeterminate { applied } => {
                if applied {
                    apply(&mut s.visible, &self.pending, limit);
                    apply(&mut s.durable, &self.pending, limit);
                    s.events.push(ModelEvent::Visible { txn });
                    s.events.push(ModelEvent::Durable { txn });
                }
                Err(CommitFailure::Indeterminate(EngineError::new(
                    ErrorClass::Io,
                    "scripted indeterminate commit",
                )))
            }
        }
    }
}

impl Drop for ModelWrite<'_> {
    fn drop(&mut self) {
        // Dropping without commit discards everything, like abort.
        self.engine.writer_open = false;
        self.engine.shared.lock().unwrap().open_pending = None;
    }
}

impl LocalEngine for ModelEngine {
    type Reader = ModelReader;
    type Write<'a> = ModelWrite<'a>;

    fn reader(&self) -> ModelReader {
        ModelReader(self.shared.clone())
    }

    fn begin_write(&mut self) -> Result<ModelWrite<'_>, EngineError> {
        if self.writer_open {
            return Err(EngineError::new(ErrorClass::Busy, "writer already open"));
        }
        {
            let mut s = self.shared.lock().unwrap();
            if s.fail_begin_write {
                s.fail_begin_write = false;
                return Err(EngineError::new(ErrorClass::Io, "injected: begin_write"));
            }
            s.writes_in_txn = 0;
        }
        self.writer_open = true;
        if self
            .shared
            .lock()
            .unwrap()
            .misbehaviors
            .contains(&Misbehavior::EarlyVisibility)
        {
            self.shared.lock().unwrap().open_pending = Some(BTreeMap::new());
        }
        Ok(ModelWrite {
            engine: self,
            pending: BTreeMap::new(),
            finished: false,
        })
    }
}

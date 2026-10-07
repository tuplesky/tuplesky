//! The redb adapter for the portable engine contract.

use std::ops::Bound;
use std::sync::Arc;

use coord_core::effect::CollectionId;
use coord_store_api::engine::{
    CommitFailure, Direction, EngineError, ErrorClass, LocalEngine, OrderedRead, Row, RowPage,
    ScanRequest, SnapshotSource, WriteTxn,
};
use coord_store_api::registry::Collection;
use redb::{ReadableDatabase, ReadableTable, TableDefinition};

/// Table definition of a registered collection.
pub fn table_definition(c: Collection) -> TableDefinition<'static, &'static [u8], &'static [u8]> {
    TableDefinition::new(c.name())
}

fn collection(id: CollectionId) -> Result<Collection, EngineError> {
    Collection::from_id(id)
        .ok_or_else(|| EngineError::new(ErrorClass::Unsupported, "unregistered collection"))
}

fn classify(e: &redb::StorageError) -> ErrorClass {
    match e {
        redb::StorageError::Corrupted(_) => ErrorClass::Corrupt,
        redb::StorageError::ValueTooLarge(_) => ErrorClass::Limit,
        redb::StorageError::Io(io) if io.kind() == std::io::ErrorKind::StorageFull => {
            ErrorClass::NoSpace
        }
        redb::StorageError::Io(_) | redb::StorageError::PreviousIo => ErrorClass::Io,
        redb::StorageError::DatabaseClosed | redb::StorageError::LockPoisoned(_) => {
            ErrorClass::Corrupt
        }
        _ => ErrorClass::Io,
    }
}

fn storage_error(e: redb::StorageError) -> EngineError {
    EngineError::new(classify(&e), redact(&e))
}

fn table_error(e: redb::TableError) -> EngineError {
    match e {
        redb::TableError::Storage(s) => storage_error(s),
        redb::TableError::TableDoesNotExist(_) => {
            EngineError::new(ErrorClass::Corrupt, "registered table missing")
        }
        other => EngineError::new(ErrorClass::Corrupt, redact(&other)),
    }
}

/// Diagnostics never carry key or value bytes; redb messages are type-level.
fn redact(e: &dyn std::fmt::Display) -> String {
    let s = e.to_string();
    if s.len() > 120 {
        s[..120].to_owned()
    } else {
        s
    }
}

/// One raw row from a redb range iterator.
type RawItem<'a> = Result<
    (
        redb::AccessGuard<'a, &'static [u8]>,
        redb::AccessGuard<'a, &'static [u8]>,
    ),
    redb::StorageError,
>;

fn scan<T: ReadableTable<&'static [u8], &'static [u8]>>(
    table: &T,
    request: &ScanRequest,
) -> Result<RowPage, EngineError> {
    let mut lower: Bound<&[u8]> = match &request.lower {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(b) => Bound::Included(b.as_slice()),
        Bound::Excluded(b) => Bound::Excluded(b.as_slice()),
    };
    let mut upper: Bound<&[u8]> = match &request.upper {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(b) => Bound::Included(b.as_slice()),
        Bound::Excluded(b) => Bound::Excluded(b.as_slice()),
    };
    // The cursor narrows the range itself: starting every page at the
    // interval's edge and skipping to the cursor walks the rows before it
    // again, which makes a whole scan quadratic in its rows.
    if let Some(cursor) = request.resume_after.as_deref() {
        let (near, far) = match request.direction {
            Direction::Forward => (&mut lower, upper),
            Direction::Reverse => (&mut upper, lower),
        };
        let inside = match *near {
            Bound::Unbounded => true,
            Bound::Included(b) | Bound::Excluded(b) => match request.direction {
                Direction::Forward => cursor >= b,
                Direction::Reverse => cursor <= b,
            },
        };
        if inside {
            *near = Bound::Excluded(cursor);
        }
        let past_far = match far {
            Bound::Unbounded => false,
            Bound::Included(b) | Bound::Excluded(b) => match request.direction {
                Direction::Forward => cursor >= b,
                Direction::Reverse => cursor <= b,
            },
        };
        if past_far {
            return Ok(RowPage {
                rows: Vec::new(),
                exhausted: true,
            });
        }
    }
    let iter = table
        .range::<&[u8]>((lower, upper))
        .map_err(storage_error)?;
    let mut rows = Vec::new();
    let mut bytes = 0usize;
    let mut process = |item: RawItem<'_>| -> Result<Option<RowPage>, EngineError> {
        let (k, v) = item.map_err(storage_error)?;
        let (key, value) = (k.value(), v.value());
        if !request.past_cursor(key) {
            return Ok(None);
        }
        if rows.len() as u32 >= request.max_rows.get() {
            return Ok(Some(RowPage {
                rows: std::mem::take(&mut rows),
                exhausted: false,
            }));
        }
        let cost = key.len() + value.len();
        if bytes + cost > request.max_bytes.get() as usize {
            if rows.is_empty() {
                return Err(EngineError::new(
                    ErrorClass::Limit,
                    "next row exceeds page byte budget",
                ));
            }
            return Ok(Some(RowPage {
                rows: std::mem::take(&mut rows),
                exhausted: false,
            }));
        }
        bytes += cost;
        rows.push(Row {
            key: key.to_vec(),
            value: value.to_vec(),
        });
        Ok(None)
    };
    match request.direction {
        Direction::Forward => {
            for item in iter {
                if let Some(page) = process(item)? {
                    return Ok(page);
                }
            }
        }
        Direction::Reverse => {
            for item in iter.rev() {
                if let Some(page) = process(item)? {
                    return Ok(page);
                }
            }
        }
    }
    Ok(RowPage {
        rows,
        exhausted: true,
    })
}

/// Where the database bytes live.
enum Source {
    /// A verified file under a generation directory.
    File {
        path: std::path::PathBuf,
        cache_bytes: usize,
    },
    /// A caller-supplied storage backend (fault harnesses, in-memory tests).
    Backend,
}

/// The engine: one redb database.
pub struct RedbEngine {
    db: Arc<redb::Database>,
    source: Source,
    writer_open: bool,
    /// Set when a reopen failed: the file could not be verified again, so
    /// nothing is served or accepted until a later reopen succeeds. The
    /// placeholder database installed during reopen is never exposed.
    quarantined: bool,
    /// Commit in one phase, the torn commit detected by redb's checksums
    /// (task-d48): set once a journal is underneath. Two-phase otherwise.
    one_phase: bool,
}

impl RedbEngine {
    /// Wrap an opened database (the lifecycle module verifies it first).
    pub(crate) fn from_database(
        db: redb::Database,
        path: std::path::PathBuf,
        cache_bytes: usize,
    ) -> Self {
        RedbEngine {
            db: Arc::new(db),
            source: Source::File { path, cache_bytes },
            writer_open: false,
            quarantined: false,
            one_phase: false,
        }
    }

    /// Whether a failed reopen left the engine unavailable.
    pub fn is_quarantined(&self) -> bool {
        self.quarantined
    }

    /// Whether commits are one-phase (task-d48): the journal is
    /// underneath, see [`LocalEngine::commit_under_journal`].
    pub fn commits_in_one_phase(&self) -> bool {
        self.one_phase
    }

    fn quarantine_error() -> EngineError {
        EngineError::new(
            ErrorClass::Corrupt,
            "engine quarantined after a failed reopen",
        )
    }

    /// Open a database over a caller-supplied storage backend. This bypasses
    /// the generation lifecycle (no manifest, lock or identity checks) and
    /// exists for fault harnesses and in-memory tests; production roots go
    /// through [`crate::Generation`]. An empty backend is refused: opening
    /// never initializes.
    pub fn from_backend(
        backend: impl redb::StorageBackend,
        cache_bytes: usize,
    ) -> Result<Self, EngineError> {
        let len = backend
            .len()
            .map_err(|e| EngineError::new(ErrorClass::Io, redact(&e)))?;
        if len == 0 {
            return Err(EngineError::new(
                ErrorClass::Corrupt,
                "empty backend; opening never initializes",
            ));
        }
        let db = redb::Database::builder()
            .set_cache_size(cache_bytes)
            .create_with_backend(backend)
            .map_err(|e| EngineError::new(ErrorClass::Corrupt, redact(&e)))?;
        Ok(RedbEngine {
            db: Arc::new(db),
            source: Source::Backend,
            writer_open: false,
            quarantined: false,
            one_phase: false,
        })
    }

    /// Initialize a brand-new database over a backend and create every
    /// registered table. Test-composition counterpart of `Generation::create`.
    pub fn create_on_backend(
        backend: impl redb::StorageBackend,
        cache_bytes: usize,
    ) -> Result<Self, EngineError> {
        let len = backend
            .len()
            .map_err(|e| EngineError::new(ErrorClass::Io, redact(&e)))?;
        if len != 0 {
            return Err(EngineError::new(
                ErrorClass::Corrupt,
                "backend not empty; create never reinitializes",
            ));
        }
        let db = redb::Database::builder()
            .set_cache_size(cache_bytes)
            .create_with_backend(backend)
            .map_err(|e| EngineError::new(ErrorClass::Corrupt, redact(&e)))?;
        {
            let mut txn = db
                .begin_write()
                .map_err(|e| EngineError::new(ErrorClass::Io, redact(&e)))?;
            txn.set_durability(redb::Durability::Immediate)
                .map_err(|e| EngineError::new(ErrorClass::Unsupported, redact(&e)))?;
            txn.set_two_phase_commit(true);
            for c in Collection::ALL {
                txn.open_table(table_definition(c)).map_err(table_error)?;
            }
            txn.commit()
                .map_err(|e| EngineError::new(ErrorClass::Io, redact(&e)))?;
        }
        Ok(RedbEngine {
            db: Arc::new(db),
            source: Source::Backend,
            writer_open: false,
            quarantined: false,
            one_phase: false,
        })
    }

    /// Verify every table's checksums. Used by the lifecycle after an open
    /// that redb reports as not cleanly shut down; failure is quarantine.
    pub fn verify_integrity(&mut self) -> Result<(), EngineError> {
        if Arc::strong_count(&self.db) > 1 || self.writer_open {
            return Err(EngineError::new(ErrorClass::Busy, "handles outstanding"));
        }
        let db = Arc::get_mut(&mut self.db).expect("sole owner");
        check_integrity(db)
    }

    /// Process-model crash: drop every handle to the database file and
    /// reopen the same, already verified file. Fails closed while readers or
    /// a writer are outstanding, because a live handle would keep the file
    /// open. Backend-sourced engines cannot reopen this way (the harness
    /// rebuilds them from its durable image). Real disk-level faults are
    /// task-09.
    pub fn reopen(&mut self) -> Result<(), EngineError> {
        if self.writer_open {
            return Err(EngineError::new(ErrorClass::Busy, "writer outstanding"));
        }
        if Arc::strong_count(&self.db) > 1 {
            return Err(EngineError::new(ErrorClass::Busy, "readers outstanding"));
        }
        let Source::File { path, cache_bytes } = &self.source else {
            return Err(EngineError::new(
                ErrorClass::Unsupported,
                "backend-sourced engine cannot reopen",
            ));
        };
        let (path, cache_bytes) = (path.clone(), *cache_bytes);
        let closed = std::mem::replace(&mut self.db, Arc::new(placeholder_database()?));
        drop(closed);
        // Until the file is open again the engine is quarantined: a failed
        // open must not leave the in-memory placeholder serving reads or
        // accepting "durable" commits that vanish with the process.
        self.quarantined = true;
        let mut db = redb::Database::builder()
            .set_cache_size(cache_bytes)
            .open(&path)
            .map_err(|e| EngineError::new(ErrorClass::Corrupt, redact(&e)))?;
        // Corruption outside the meta pages does not stop `open`; every
        // generation reopen verifies the tables before serving anything.
        check_integrity(&mut db)?;
        self.db = Arc::new(db);
        self.quarantined = false;
        Ok(())
    }

    /// Path of the database file, when file-sourced.
    pub fn path(&self) -> Option<&std::path::Path> {
        match &self.source {
            Source::File { path, .. } => Some(path),
            Source::Backend => None,
        }
    }
}

/// Verify every table's checksums; a repair or an inconsistency is
/// corruption (nothing is repaired into a different state silently).
pub(crate) fn check_integrity(db: &mut redb::Database) -> Result<(), EngineError> {
    match db.check_integrity() {
        Ok(true) => Ok(()),
        Ok(false) => Err(EngineError::new(
            ErrorClass::Corrupt,
            "integrity check repaired or found inconsistencies",
        )),
        Err(e) => Err(EngineError::new(ErrorClass::Corrupt, redact(&e))),
    }
}

/// An in-memory database used only as a swap placeholder during reopen.
fn placeholder_database() -> Result<redb::Database, EngineError> {
    redb::Database::builder()
        .create_with_backend(redb::backends::InMemoryBackend::new())
        .map_err(|e| EngineError::new(ErrorClass::Io, redact(&e)))
}

/// Reader handle.
#[derive(Clone)]
pub struct RedbReader {
    db: Arc<redb::Database>,
    quarantined: bool,
}

/// A table of a read transaction, opened.
type ReadTable = redb::ReadOnlyTable<&'static [u8], &'static [u8]>;

/// A pinned cross-table snapshot.
///
/// Each table is opened on its first read and kept for the snapshot's
/// life (task-d58). Opening one walks the transaction's table tree, and a
/// snapshot that serves several reads -- a pump's held reads, a view
/// built from sessions, policy and grants -- used to walk it again for
/// every point read: 11% of the leader's domain thread in the five-voter
/// profile. A table opened in a read transaction is that transaction's
/// state of it, so keeping it changes nothing a read can see.
pub struct RedbView {
    txn: redb::ReadTransaction,
    tables: [std::cell::OnceCell<ReadTable>; Collection::ALL.len()],
}

impl RedbView {
    fn new(txn: redb::ReadTransaction) -> Self {
        RedbView {
            txn,
            tables: std::array::from_fn(|_| std::cell::OnceCell::new()),
        }
    }

    /// `c`'s table, opened once. A failed open is not kept: the next
    /// read tries again and fails the same way.
    fn table(&self, c: CollectionId) -> Result<&ReadTable, EngineError> {
        let collection = collection(c)?;
        let slot = Collection::ALL
            .iter()
            .position(|known| *known == collection)
            .expect("a registered collection is in the registry");
        if let Some(table) = self.tables[slot].get() {
            return Ok(table);
        }
        let table = self
            .txn
            .open_table(table_definition(collection))
            .map_err(table_error)?;
        Ok(self.tables[slot].get_or_init(|| table))
    }
}

impl OrderedRead for RedbView {
    fn get(&self, c: CollectionId, key: &[u8]) -> Result<Option<Vec<u8>>, EngineError> {
        Ok(self
            .table(c)?
            .get(key)
            .map_err(storage_error)?
            .map(|g| g.value().to_vec()))
    }

    fn scan_page(&self, c: CollectionId, request: &ScanRequest) -> Result<RowPage, EngineError> {
        scan(self.table(c)?, request)
    }
}

impl SnapshotSource for RedbReader {
    type View = RedbView;

    fn snapshot(&self) -> Result<RedbView, EngineError> {
        if self.quarantined {
            return Err(RedbEngine::quarantine_error());
        }
        let txn = self.db.begin_read().map_err(|e| match e {
            redb::TransactionError::Storage(s) => storage_error(s),
            other => EngineError::new(ErrorClass::Busy, redact(&other)),
        })?;
        Ok(RedbView::new(txn))
    }
}

/// The unique write transaction.
pub struct RedbWrite<'a> {
    engine: &'a mut RedbEngine,
    txn: Option<redb::WriteTransaction>,
}

impl RedbWrite<'_> {
    fn txn(&self) -> &redb::WriteTransaction {
        self.txn.as_ref().expect("transaction open")
    }
}

impl OrderedRead for RedbWrite<'_> {
    fn get(&self, c: CollectionId, key: &[u8]) -> Result<Option<Vec<u8>>, EngineError> {
        let table = self
            .txn()
            .open_table(table_definition(collection(c)?))
            .map_err(table_error)?;
        Ok(table
            .get(key)
            .map_err(storage_error)?
            .map(|g| g.value().to_vec()))
    }

    fn scan_page(&self, c: CollectionId, request: &ScanRequest) -> Result<RowPage, EngineError> {
        let table = self
            .txn()
            .open_table(table_definition(collection(c)?))
            .map_err(table_error)?;
        scan(&table, request)
    }
}

impl WriteTxn for RedbWrite<'_> {
    fn put(&mut self, c: CollectionId, key: &[u8], value: &[u8]) -> Result<(), EngineError> {
        let mut table = self
            .txn()
            .open_table(table_definition(collection(c)?))
            .map_err(table_error)?;
        table.insert(key, value).map_err(storage_error)?;
        Ok(())
    }

    fn delete(&mut self, c: CollectionId, key: &[u8]) -> Result<(), EngineError> {
        let mut table = self
            .txn()
            .open_table(table_definition(collection(c)?))
            .map_err(table_error)?;
        table.remove(key).map_err(storage_error)?;
        Ok(())
    }

    fn abort(mut self) -> Result<(), EngineError> {
        let txn = self.txn.take().expect("transaction open");
        txn.abort().map_err(storage_error)
    }

    fn commit_durable(mut self) -> Result<(), CommitFailure> {
        let txn = self.txn.take().expect("transaction open");
        commit(txn)
    }

    /// One `Durability::None` commit (task-j06): visible at once, made
    /// durable by the next Immediate commit, rolled back by a crash
    /// before one. redb never leaves part of a commit: the header names
    /// the last durable commit until a durable one replaces it.
    fn commit_working(mut self) -> Result<(), CommitFailure> {
        let mut txn = self.txn.take().expect("transaction open");
        if let Err(e) = txn.set_durability(redb::Durability::None) {
            // Nothing was written: the transaction is dropped unapplied.
            return Err(CommitFailure::DefinitelyNotCommitted(EngineError::new(
                ErrorClass::Unsupported,
                redact(&e),
            )));
        }
        commit(txn)
    }
}

/// Commit `txn`, sorting a failure into what is known of its outcome.
fn commit(txn: redb::WriteTransaction) -> Result<(), CommitFailure> {
    match txn.commit() {
        Ok(()) => Ok(()),
        // A poisoned transaction never reached the commit path.
        Err(redb::CommitError::TransactionPoisoned) => Err(CommitFailure::DefinitelyNotCommitted(
            EngineError::new(ErrorClass::Io, "transaction poisoned before commit"),
        )),
        // Anything during commit/sync is indeterminate: redb may have
        // written the commit record before reporting the failure.
        Err(redb::CommitError::Storage(s)) => Err(CommitFailure::Indeterminate(storage_error(s))),
        Err(other) => Err(CommitFailure::Indeterminate(EngineError::new(
            ErrorClass::Io,
            redact(&other),
        ))),
    }
}

impl Drop for RedbWrite<'_> {
    fn drop(&mut self) {
        // Dropping an uncommitted redb transaction aborts it.
        self.txn.take();
        self.engine.writer_open = false;
    }
}

impl LocalEngine for RedbEngine {
    type Reader = RedbReader;
    type Write<'a> = RedbWrite<'a>;

    fn reader(&self) -> RedbReader {
        RedbReader {
            db: self.db.clone(),
            quarantined: self.quarantined,
        }
    }

    fn begin_write(&mut self) -> Result<RedbWrite<'_>, EngineError> {
        if self.quarantined {
            return Err(RedbEngine::quarantine_error());
        }
        if self.writer_open {
            return Err(EngineError::new(ErrorClass::Busy, "writer already open"));
        }
        let mut txn = self.db.begin_write().map_err(|e| match e {
            redb::TransactionError::Storage(s) => storage_error(s),
            other => EngineError::new(ErrorClass::Busy, redact(&other)),
        })?;
        // Strict profile: Immediate durability, every commit synced; quick
        // repair stays off (Section 17.3.4). Two-phase commit, unless a
        // journal is underneath (task-d48): then one phase, one sync, and
        // a commit a crash tore fails redb's checksums at the next open
        // and rolls back to the one before it, which the journal replay
        // carries forward again.
        txn.set_durability(redb::Durability::Immediate)
            .map_err(|e| EngineError::new(ErrorClass::Unsupported, redact(&e)))?;
        txn.set_two_phase_commit(!self.one_phase);
        txn.set_quick_repair(false);
        self.writer_open = true;
        Ok(RedbWrite {
            engine: self,
            txn: Some(txn),
        })
    }

    fn commit_under_journal(&mut self) {
        self.one_phase = true;
    }

    const WORKING_STATE: bool = true;

    /// An empty Immediate commit: redb persists every `Durability::None`
    /// commit before it with it (task-j06).
    fn sync_working(&mut self) -> Result<(), CommitFailure> {
        let txn = self
            .begin_write()
            .map_err(CommitFailure::DefinitelyNotCommitted)?;
        txn.commit_durable()
    }
}

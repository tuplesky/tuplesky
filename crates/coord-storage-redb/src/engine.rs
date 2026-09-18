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
    let lower: Bound<&[u8]> = match &request.lower {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(b) => Bound::Included(b.as_slice()),
        Bound::Excluded(b) => Bound::Excluded(b.as_slice()),
    };
    let upper: Bound<&[u8]> = match &request.upper {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(b) => Bound::Included(b.as_slice()),
        Bound::Excluded(b) => Bound::Excluded(b.as_slice()),
    };
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

/// The engine: one redb database.
pub struct RedbEngine {
    db: Arc<redb::Database>,
    path: std::path::PathBuf,
    cache_bytes: usize,
    writer_open: bool,
    /// Set when a reopen failed: the file could not be verified again, so
    /// nothing is served or accepted until a later reopen succeeds. The
    /// placeholder database installed during reopen is never exposed.
    quarantined: bool,
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
            path,
            cache_bytes,
            writer_open: false,
            quarantined: false,
        }
    }

    /// Whether a failed reopen left the engine unavailable.
    pub fn is_quarantined(&self) -> bool {
        self.quarantined
    }

    fn quarantine_error() -> EngineError {
        EngineError::new(
            ErrorClass::Corrupt,
            "engine quarantined after a failed reopen",
        )
    }

    /// Process-model crash: drop every handle to the database file and
    /// reopen the same, already verified file. Fails closed while readers or
    /// a writer are outstanding, because a live handle would keep the file
    /// open. Real disk-level faults are task-09.
    pub fn reopen(&mut self) -> Result<(), EngineError> {
        if self.writer_open {
            return Err(EngineError::new(ErrorClass::Busy, "writer outstanding"));
        }
        if Arc::strong_count(&self.db) > 1 {
            return Err(EngineError::new(ErrorClass::Busy, "readers outstanding"));
        }
        let closed = std::mem::replace(&mut self.db, Arc::new(placeholder_database()?));
        drop(closed);
        // Until the file is open again the engine is quarantined: a failed
        // open must not leave the in-memory placeholder serving reads or
        // accepting "durable" commits that vanish with the process.
        self.quarantined = true;
        let db = redb::Database::builder()
            .set_cache_size(self.cache_bytes)
            .open(&self.path)
            .map_err(|e| EngineError::new(ErrorClass::Corrupt, redact(&e)))?;
        self.db = Arc::new(db);
        self.quarantined = false;
        Ok(())
    }

    /// Path of the database file.
    pub fn path(&self) -> &std::path::Path {
        &self.path
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

/// A pinned cross-table snapshot.
pub struct RedbView {
    txn: redb::ReadTransaction,
}

impl OrderedRead for RedbView {
    fn get(&self, c: CollectionId, key: &[u8]) -> Result<Option<Vec<u8>>, EngineError> {
        let table = self
            .txn
            .open_table(table_definition(collection(c)?))
            .map_err(table_error)?;
        Ok(table
            .get(key)
            .map_err(storage_error)?
            .map(|g| g.value().to_vec()))
    }

    fn scan_page(&self, c: CollectionId, request: &ScanRequest) -> Result<RowPage, EngineError> {
        let table = self
            .txn
            .open_table(table_definition(collection(c)?))
            .map_err(table_error)?;
        scan(&table, request)
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
        Ok(RedbView { txn })
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
        match txn.commit() {
            Ok(()) => Ok(()),
            // A poisoned transaction never reached the commit path.
            Err(redb::CommitError::TransactionPoisoned) => {
                Err(CommitFailure::DefinitelyNotCommitted(EngineError::new(
                    ErrorClass::Io,
                    "transaction poisoned before commit",
                )))
            }
            // Anything during commit/sync is indeterminate: redb may have
            // written the commit record before reporting the failure.
            Err(redb::CommitError::Storage(s)) => {
                Err(CommitFailure::Indeterminate(storage_error(s)))
            }
            Err(other) => Err(CommitFailure::Indeterminate(EngineError::new(
                ErrorClass::Io,
                redact(&other),
            ))),
        }
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
        // Strict profile: Immediate durability plus two-phase commit; quick
        // repair stays off (Section 17.3.4).
        txn.set_durability(redb::Durability::Immediate)
            .map_err(|e| EngineError::new(ErrorClass::Unsupported, redact(&e)))?;
        txn.set_two_phase_commit(true);
        txn.set_quick_repair(false);
        self.writer_open = true;
        Ok(RedbWrite {
            engine: self,
            txn: Some(txn),
        })
    }
}

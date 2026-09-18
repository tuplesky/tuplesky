//! The fjall adapter for the portable engine contract.

use std::cell::Cell;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use coord_core::effect::CollectionId;
use coord_store_api::engine::{
    CommitFailure, Direction, EngineError, ErrorClass, LocalEngine, OrderedRead, Row, RowPage,
    ScanRequest, SnapshotSource, WriteTxn,
};
use coord_store_api::registry::Collection;
use fjall::{
    KeyspaceCreateOptions, PersistMode, Readable, SingleWriterTxDatabase, SingleWriterTxKeyspace,
    SingleWriterWriteTx,
};

/// Physical grouping (Section 17.11): fewer keyspaces than collections,
/// chosen by access pattern, never one keyspace per logical table. Recorded
/// in the in-database identity record as [`LAYOUT_NAME`].
pub const GROUPS: [(&str, &[Collection]); 4] = [
    (
        "meta",
        &[
            Collection::MetaV1,
            Collection::ConfigV1,
            Collection::CheckpointV1,
        ],
    ),
    (
        "protocol",
        &[
            Collection::PayloadV1,
            Collection::ProtocolV1,
            Collection::ExecutionV1,
            Collection::ExecutedV1,
            Collection::RetryV1,
            Collection::RetryFloorV1,
        ],
    ),
    (
        "kv",
        &[
            Collection::KvCurrentV1,
            Collection::KvHistoryV1,
            Collection::EventsV1,
        ],
    ),
    (
        "auth",
        &[
            Collection::LeaseV1,
            Collection::LeaseKeysV1,
            Collection::SessionV1,
            Collection::PolicyV1,
            Collection::AuthGrantV1,
        ],
    ),
];

/// Name of the physical layout above.
pub const LAYOUT_NAME: &str = "fjall-groups-v1";

/// The explicit fjall feature set this adapter is built with (audited by
/// `cargo xtask check-deps`).
pub const FEATURES: &str = "lz4";

/// Physical prefix bytes per key.
const PREFIX_LEN: usize = 2;

/// fjall keys are limited to 65536 bytes; the logical key keeps room for the
/// physical prefix (Section 17.9 rule 4: semantic limits include prefix
/// overhead).
const MAX_LOGICAL_KEY: usize = 65536 - PREFIX_LEN;

/// The physical group of a collection.
pub fn group_of(c: Collection) -> &'static str {
    GROUPS
        .iter()
        .find(|(_, members)| members.contains(&c))
        .map(|(name, _)| *name)
        .expect("every registered collection is grouped")
}

fn collection(id: CollectionId) -> Result<Collection, EngineError> {
    Collection::from_id(id)
        .ok_or_else(|| EngineError::new(ErrorClass::Unsupported, "unregistered collection"))
}

fn prefix(c: Collection) -> [u8; PREFIX_LEN] {
    c.id().0.to_be_bytes()
}

fn physical_key(c: Collection, key: &[u8]) -> Result<Vec<u8>, EngineError> {
    if key.len() > MAX_LOGICAL_KEY {
        return Err(EngineError::new(
            ErrorClass::Limit,
            "key exceeds the engine key limit including the collection prefix",
        ));
    }
    let mut out = Vec::with_capacity(PREFIX_LEN + key.len());
    out.extend_from_slice(&prefix(c));
    out.extend_from_slice(key);
    Ok(out)
}

/// Diagnostics never carry key or value bytes; fjall messages are
/// type-level and truncated.
fn redact(e: &dyn std::fmt::Display) -> String {
    let s = e.to_string();
    if s.len() > 120 {
        s[..120].to_owned()
    } else {
        s
    }
}

fn classify(e: &fjall::Error) -> ErrorClass {
    match e {
        fjall::Error::Io(io) if io.kind() == std::io::ErrorKind::StorageFull => ErrorClass::NoSpace,
        fjall::Error::Io(_) => ErrorClass::Io,
        fjall::Error::Storage(inner) => match inner {
            fjall::LsmError::Io(io) if io.kind() == std::io::ErrorKind::StorageFull => {
                ErrorClass::NoSpace
            }
            fjall::LsmError::Io(_) => ErrorClass::Io,
            _ => ErrorClass::Corrupt,
        },
        fjall::Error::Locked => ErrorClass::Busy,
        // A poisoned database, unrecoverable state, journal recovery
        // failures, bad versions, trailers, tags or decompression are
        // corruption to quarantine.
        _ => ErrorClass::Corrupt,
    }
}

fn engine_error(e: fjall::Error) -> EngineError {
    EngineError::new(classify(&e), redact(&e))
}

/// The opened database plus its physical keyspaces.
struct Inner {
    db: SingleWriterTxDatabase,
    keyspaces: Vec<(&'static str, SingleWriterTxKeyspace)>,
}

impl Inner {
    fn keyspace(&self, c: Collection) -> &SingleWriterTxKeyspace {
        let group = group_of(c);
        &self
            .keyspaces
            .iter()
            .find(|(name, _)| *name == group)
            .expect("every group is opened")
            .1
    }

    /// Open the database at `path`. With `create`, the directory must not
    /// hold a database yet and every group keyspace is created; otherwise
    /// the database and every group keyspace must already exist (opening
    /// never creates or reinitializes).
    fn open(path: &Path, cache_bytes: u64, create: bool) -> Result<Inner, EngineError> {
        let present = path.join(fjall_marker()).is_file();
        if create && present {
            return Err(EngineError::new(
                ErrorClass::Corrupt,
                "database present; create never reinitializes",
            ));
        }
        if !create && !present {
            return Err(EngineError::new(
                ErrorClass::Corrupt,
                "no database at path; opening never initializes",
            ));
        }
        // Explicit configuration: the transaction chooses its persist mode
        // (never the builder's buffered default); cache and worker budget
        // are recorded tunables.
        let db = SingleWriterTxDatabase::builder(path)
            .manual_journal_persist(true)
            .cache_size(cache_bytes)
            .open()
            .map_err(engine_error)?;
        let mut keyspaces = Vec::with_capacity(GROUPS.len());
        for (name, _) in GROUPS {
            if !create && !db.keyspace_exists(name) {
                return Err(EngineError::new(
                    ErrorClass::Corrupt,
                    "registered keyspace group missing",
                ));
            }
            let ks = db
                .keyspace(name, KeyspaceCreateOptions::default)
                .map_err(engine_error)?;
            keyspaces.push((name, ks));
        }
        Ok(Inner { db, keyspaces })
    }
}

/// The file fjall writes first into a database directory.
fn fjall_marker() -> &'static str {
    "version"
}

/// One page over a readable (a pinned snapshot or the write transaction).
fn scan<R: Readable>(
    readable: &R,
    keyspace: &SingleWriterTxKeyspace,
    c: Collection,
    request: &ScanRequest,
) -> Result<RowPage, EngineError> {
    let p = prefix(c);
    let with_prefix = |k: &[u8]| -> Result<Vec<u8>, EngineError> { physical_key(c, k) };
    // Clamp the logical interval to the collection's physical prefix.
    let mut lower: Bound<Vec<u8>> = match &request.lower {
        Bound::Unbounded => Bound::Included(p.to_vec()),
        Bound::Included(k) => Bound::Included(with_prefix(k)?),
        Bound::Excluded(k) => Bound::Excluded(with_prefix(k)?),
    };
    let mut upper: Bound<Vec<u8>> = match &request.upper {
        // The next prefix is exclusive; registered ids never reach u16::MAX.
        Bound::Unbounded => Bound::Excluded((c.id().0 + 1).to_be_bytes().to_vec()),
        Bound::Included(k) => Bound::Included(with_prefix(k)?),
        Bound::Excluded(k) => Bound::Excluded(with_prefix(k)?),
    };
    // Fold the exclusive cursor into the bound on the scan side.
    if let Some(cursor) = &request.resume_after {
        let cursor = with_prefix(cursor)?;
        match request.direction {
            Direction::Forward => {
                let tighter = match &lower {
                    Bound::Included(l) | Bound::Excluded(l) => cursor >= *l,
                    Bound::Unbounded => true,
                };
                if tighter {
                    lower = Bound::Excluded(cursor);
                }
            }
            Direction::Reverse => {
                let tighter = match &upper {
                    Bound::Included(u) | Bound::Excluded(u) => cursor <= *u,
                    Bound::Unbounded => true,
                };
                if tighter {
                    upper = Bound::Excluded(cursor);
                }
            }
        }
    }
    let iter = readable.range(keyspace, (lower, upper));
    let mut rows = Vec::new();
    let mut bytes = 0usize;
    let mut process = |guard: fjall::Guard| -> Result<Option<RowPage>, EngineError> {
        // An iterator failure is an error, never the end of the interval.
        let (k, v) = guard.into_inner().map_err(engine_error)?;
        let Some(key) = k.strip_prefix(p.as_slice()) else {
            return Err(EngineError::new(
                ErrorClass::Corrupt,
                "physical key outside its collection prefix",
            ));
        };
        if !request.past_cursor(key) {
            return Ok(None);
        }
        if rows.len() as u32 >= request.max_rows.get() {
            return Ok(Some(RowPage {
                rows: std::mem::take(&mut rows),
                exhausted: false,
            }));
        }
        let cost = key.len() + v.len();
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
            value: v.to_vec(),
        });
        Ok(None)
    };
    match request.direction {
        Direction::Forward => {
            for guard in iter {
                if let Some(page) = process(guard)? {
                    return Ok(page);
                }
            }
        }
        Direction::Reverse => {
            for guard in iter.rev() {
                if let Some(page) = process(guard)? {
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

fn get<R: Readable>(
    readable: &R,
    keyspace: &SingleWriterTxKeyspace,
    c: Collection,
    key: &[u8],
) -> Result<Option<Vec<u8>>, EngineError> {
    let physical = physical_key(c, key)?;
    Ok(readable
        .get(keyspace, physical)
        .map_err(engine_error)?
        .map(|v| v.to_vec()))
}

/// The engine: one fjall single-writer database.
pub struct FjallEngine {
    inner: Option<Arc<Inner>>,
    path: PathBuf,
    cache_bytes: u64,
    writer_open: Cell<bool>,
}

impl FjallEngine {
    /// Create a fresh database under `path` (which must not hold one).
    pub(crate) fn create(path: &Path, cache_bytes: u64) -> Result<Self, EngineError> {
        let inner = Inner::open(path, cache_bytes, true)?;
        Ok(FjallEngine {
            inner: Some(Arc::new(inner)),
            path: path.to_path_buf(),
            cache_bytes,
            writer_open: Cell::new(false),
        })
    }

    /// Open an existing database under `path`; never initializes.
    pub(crate) fn open(path: &Path, cache_bytes: u64) -> Result<Self, EngineError> {
        let inner = Inner::open(path, cache_bytes, false)?;
        Ok(FjallEngine {
            inner: Some(Arc::new(inner)),
            path: path.to_path_buf(),
            cache_bytes,
            writer_open: Cell::new(false),
        })
    }

    fn inner(&self) -> &Arc<Inner> {
        self.inner.as_ref().expect("engine open")
    }

    /// Process-model crash: drop every handle to the database and reopen the
    /// same directory (journal recovery decides what survived). Fails closed
    /// while readers or a writer are outstanding, because a live handle
    /// would keep the database open.
    pub fn reopen(&mut self) -> Result<(), EngineError> {
        if self.writer_open.get() {
            return Err(EngineError::new(ErrorClass::Busy, "writer outstanding"));
        }
        if Arc::strong_count(self.inner()) > 1 {
            return Err(EngineError::new(ErrorClass::Busy, "readers outstanding"));
        }
        let closed = self.inner.take();
        drop(closed);
        let inner = Inner::open(&self.path, self.cache_bytes, false)?;
        self.inner = Some(Arc::new(inner));
        Ok(())
    }

    /// Database directory.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Bytes on disk as the engine accounts them (an experiment metric; not
    /// zero when unavailable but an error).
    pub fn disk_space(&self) -> Result<u64, EngineError> {
        self.inner().db.disk_space().map_err(engine_error)
    }
}

/// Reader handle.
#[derive(Clone)]
pub struct FjallReader {
    inner: Arc<Inner>,
}

/// A pinned cross-keyspace snapshot.
pub struct FjallView {
    inner: Arc<Inner>,
    snapshot: fjall::Snapshot,
}

impl OrderedRead for FjallView {
    fn get(&self, c: CollectionId, key: &[u8]) -> Result<Option<Vec<u8>>, EngineError> {
        let c = collection(c)?;
        get(&self.snapshot, self.inner.keyspace(c), c, key)
    }

    fn scan_page(&self, c: CollectionId, request: &ScanRequest) -> Result<RowPage, EngineError> {
        let c = collection(c)?;
        scan(&self.snapshot, self.inner.keyspace(c), c, request)
    }
}

impl SnapshotSource for FjallReader {
    type View = FjallView;

    fn snapshot(&self) -> Result<FjallView, EngineError> {
        Ok(FjallView {
            inner: self.inner.clone(),
            snapshot: self.inner.db.read_tx(),
        })
    }
}

/// The unique write transaction.
pub struct FjallWrite<'a> {
    engine: &'a FjallEngine,
    tx: Option<SingleWriterWriteTx<'a>>,
}

impl FjallWrite<'_> {
    fn tx(&self) -> &SingleWriterWriteTx<'_> {
        self.tx.as_ref().expect("transaction open")
    }
}

impl OrderedRead for FjallWrite<'_> {
    fn get(&self, c: CollectionId, key: &[u8]) -> Result<Option<Vec<u8>>, EngineError> {
        let c = collection(c)?;
        get(self.tx(), self.engine.inner().keyspace(c), c, key)
    }

    fn scan_page(&self, c: CollectionId, request: &ScanRequest) -> Result<RowPage, EngineError> {
        let c = collection(c)?;
        scan(self.tx(), self.engine.inner().keyspace(c), c, request)
    }
}

impl WriteTxn for FjallWrite<'_> {
    fn put(&mut self, c: CollectionId, key: &[u8], value: &[u8]) -> Result<(), EngineError> {
        let c = collection(c)?;
        let physical = physical_key(c, key)?;
        if value.len() > u32::MAX as usize {
            return Err(EngineError::new(
                ErrorClass::Limit,
                "value exceeds the engine value limit",
            ));
        }
        let keyspace = self.engine.inner().keyspace(c);
        self.tx
            .as_mut()
            .expect("transaction open")
            .insert(keyspace, physical, value);
        Ok(())
    }

    fn delete(&mut self, c: CollectionId, key: &[u8]) -> Result<(), EngineError> {
        let c = collection(c)?;
        let physical = physical_key(c, key)?;
        let keyspace = self.engine.inner().keyspace(c);
        self.tx
            .as_mut()
            .expect("transaction open")
            .remove(keyspace, physical);
        Ok(())
    }

    fn abort(mut self) -> Result<(), EngineError> {
        let tx = self.tx.take().expect("transaction open");
        tx.rollback();
        Ok(())
    }

    fn commit_durable(mut self) -> Result<(), CommitFailure> {
        let tx = self.tx.take().expect("transaction open");
        // The batch is journaled and synced (SyncAll) before it becomes
        // visible; a failure anywhere in that path may have written part of
        // the journal record and poisons the database, so it is
        // indeterminate. No specific noncommit evidence is available.
        tx.commit()
            .map_err(|e| CommitFailure::Indeterminate(engine_error(e)))
    }
}

impl Drop for FjallWrite<'_> {
    fn drop(&mut self) {
        // Dropping an uncommitted transaction rolls it back.
        self.tx.take();
        self.engine.writer_open.set(false);
    }
}

impl LocalEngine for FjallEngine {
    type Reader = FjallReader;
    type Write<'a> = FjallWrite<'a>;

    fn reader(&self) -> FjallReader {
        FjallReader {
            inner: self.inner().clone(),
        }
    }

    fn begin_write(&mut self) -> Result<FjallWrite<'_>, EngineError> {
        if self.writer_open.get() {
            return Err(EngineError::new(ErrorClass::Busy, "writer already open"));
        }
        let this: &FjallEngine = &*self;
        // Explicit strict durability: fsync of data and metadata for the
        // whole cross-keyspace batch, never the buffered default.
        let tx = this
            .inner()
            .db
            .write_tx()
            .durability(Some(PersistMode::SyncAll));
        this.writer_open.set(true);
        Ok(FjallWrite {
            engine: this,
            tx: Some(tx),
        })
    }
}

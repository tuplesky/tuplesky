//! The narrow strict state-adapter contract (design Section 17.9).
//!
//! Normative semantics an adapter must provide:
//!
//! 1. Atomic updates across all logical collections with point/scan
//!    read-your-writes inside the transaction.
//! 2. One snapshot across every collection and page; never reopened between
//!    pages.
//! 3. Unsigned lexicographic key order, identical endpoints, exclusive
//!    cursor; reverse scans resume below the prior key; physical prefixes
//!    never leak and unbounded ranges stay inside the logical collection.
//! 4. Rows, bytes and allocation overhead are accounted. An oversized next
//!    row in an empty page is a typed `Limit` error, never an endless
//!    non-exhausted empty page. Iterator failure is not EOF.
//! 5. `commit_durable` success meets the whole transaction's qualified
//!    durability. OS-buffer flush, visibility, later periodic sync or clean
//!    shutdown do not qualify.
//! 6. Commit/sync failures are `Indeterminate` unless specific noncommit
//!    evidence exists. Corruption and uncertain I/O quarantine.
//! 7. No actor-facing weak durability, savepoint rollback, TTL/merge
//!    callback, native transaction identity or public engine sequence.

use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;
use core::num::NonZeroU32;
use core::ops::Bound;

pub use coord_core::effect::CollectionId;

/// Scan direction.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Direction {
    /// Ascending key order.
    Forward,
    /// Descending key order.
    Reverse,
}

/// A bounded page request inside one logical collection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanRequest {
    /// Lower bound of the logical interval.
    pub lower: Bound<Vec<u8>>,
    /// Upper bound of the logical interval.
    pub upper: Bound<Vec<u8>>,
    /// Direction.
    pub direction: Direction,
    /// Exclusive cursor: forward scans resume above it, reverse scans below.
    pub resume_after: Option<Vec<u8>>,
    /// Maximum rows in the page.
    pub max_rows: NonZeroU32,
    /// Maximum key plus value bytes in the page.
    pub max_bytes: NonZeroU32,
}

impl ScanRequest {
    /// Whole collection, ascending, with the given budgets.
    pub fn all(max_rows: u32, max_bytes: u32) -> Self {
        ScanRequest {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
            direction: Direction::Forward,
            resume_after: None,
            max_rows: NonZeroU32::new(max_rows.max(1)).expect("non-zero"),
            max_bytes: NonZeroU32::new(max_bytes.max(1)).expect("non-zero"),
        }
    }

    /// Whether `key` lies inside the logical interval.
    pub fn contains(&self, key: &[u8]) -> bool {
        let above_lower = match &self.lower {
            Bound::Unbounded => true,
            Bound::Included(l) => key >= l.as_slice(),
            Bound::Excluded(l) => key > l.as_slice(),
        };
        let below_upper = match &self.upper {
            Bound::Unbounded => true,
            Bound::Included(u) => key <= u.as_slice(),
            Bound::Excluded(u) => key < u.as_slice(),
        };
        above_lower && below_upper
    }

    /// Whether `key` is past the cursor in scan direction.
    pub fn past_cursor(&self, key: &[u8]) -> bool {
        match (&self.resume_after, self.direction) {
            (None, _) => true,
            (Some(c), Direction::Forward) => key > c.as_slice(),
            (Some(c), Direction::Reverse) => key < c.as_slice(),
        }
    }
}

/// One row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Row {
    /// Key.
    pub key: Vec<u8>,
    /// Value.
    pub value: Vec<u8>,
}

/// One page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowPage {
    /// Rows in scan order.
    pub rows: Vec<Row>,
    /// Whether the interval is exhausted after these rows.
    pub exhausted: bool,
}

/// Engine failure class.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ErrorClass {
    /// I/O error of uncertain effect.
    Io,
    /// Detected corruption; quarantine.
    Corrupt,
    /// Out of space.
    NoSpace,
    /// Capability or operation unsupported by the engine.
    Unsupported,
    /// A caller budget (rows, bytes, request size) was exceeded.
    Limit,
    /// Engine busy; retry later without semantic change.
    Busy,
}

/// Engine error with redacted diagnostic.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineError {
    /// Class.
    pub class: ErrorClass,
    /// Redacted diagnostic (no keys or values).
    pub diagnostic: String,
}

impl EngineError {
    /// Construct.
    pub fn new(class: ErrorClass, diagnostic: impl Into<String>) -> Self {
        EngineError {
            class,
            diagnostic: diagnostic.into(),
        }
    }
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.class, self.diagnostic)
    }
}

impl core::error::Error for EngineError {}

/// How a commit failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommitFailure {
    /// Specific evidence that nothing was committed; the caller may replan.
    DefinitelyNotCommitted(EngineError),
    /// Outcome unknown; the caller must reconcile from semantic records,
    /// never blindly retry the byte batch.
    Indeterminate(EngineError),
}

impl CommitFailure {
    /// The underlying error.
    pub const fn error(&self) -> &EngineError {
        match self {
            CommitFailure::DefinitelyNotCommitted(e) | CommitFailure::Indeterminate(e) => e,
        }
    }
}

impl fmt::Display for CommitFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommitFailure::DefinitelyNotCommitted(e) => write!(f, "definitely not committed: {e}"),
            CommitFailure::Indeterminate(e) => write!(f, "indeterminate commit: {e}"),
        }
    }
}

impl core::error::Error for CommitFailure {}

/// Bounded ordered reads over one consistent view.
pub trait OrderedRead {
    /// Point read.
    fn get(&self, collection: CollectionId, key: &[u8]) -> Result<Option<Vec<u8>>, EngineError>;

    /// One bounded page. An oversized next row with no rows yet in the page
    /// is `ErrorClass::Limit`.
    fn scan_page(
        &self,
        collection: CollectionId,
        request: &ScanRequest,
    ) -> Result<RowPage, EngineError>;
}

/// The unique write transaction. Not `Send`: it lives inside one worker job
/// and never crosses an await.
pub trait WriteTxn: OrderedRead + Sized {
    /// Put (visible to later reads in this transaction).
    fn put(
        &mut self,
        collection: CollectionId,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), EngineError>;

    /// Delete.
    fn delete(&mut self, collection: CollectionId, key: &[u8]) -> Result<(), EngineError>;

    /// Abort; nothing becomes visible.
    fn abort(self) -> Result<(), EngineError>;

    /// Commit with the whole transaction's qualified durability. This is the
    /// only success an actor may rely on.
    fn commit_durable(self) -> Result<(), CommitFailure>;
}

/// Source of pinned cross-collection snapshots.
pub trait SnapshotSource: Clone + Send + Sync + 'static {
    /// The pinned view type.
    type View: OrderedRead;

    /// Pin a snapshot spanning every collection.
    fn snapshot(&self) -> Result<Self::View, EngineError>;
}

/// A local state engine: one unique writer, many readers.
pub trait LocalEngine: Send + 'static {
    /// Reader handle type.
    type Reader: SnapshotSource;
    /// Write transaction type (worker-local, need not be `Send`).
    type Write<'a>: WriteTxn
    where
        Self: 'a;

    /// Obtain a reader handle.
    fn reader(&self) -> Self::Reader;

    /// Begin the unique write transaction.
    fn begin_write(&mut self) -> Result<Self::Write<'_>, EngineError>;
}

/// Reserved capability (task-j06): atomic working-state application without
/// per-transaction sync, valid only under a published durable checkpoint
/// plus durable redo. Deliberately distinct from [`WriteTxn::commit_durable`];
/// no engine implements it until that task's qualification.
pub trait AtomicWorkingState: LocalEngine {
    /// Apply atomically with visibility but without qualified durability.
    fn commit_working(
        &mut self,
        updates: &[coord_core::effect::StoreUpdate],
    ) -> Result<(), CommitFailure>;
}

/// Reserved capability (task-j04): publish a durable local checkpoint of the
/// engine at a represented sequence. Distinct from commit and from the
/// common snapshot export.
pub trait DurableCheckpointPublisher: LocalEngine {
    /// Opaque checkpoint reference.
    type CheckpointRef;

    /// Publish a complete, synced checkpoint.
    fn publish_checkpoint(
        &mut self,
        represented: crate::seq::StoreSeq,
    ) -> Result<Self::CheckpointRef, EngineError>;
}

//! Offline same-engine schema migration (task-60; design Sections 17.7,
//! 17.10, 17.13, 17.16).
//!
//! A schema change is a generation replacement, done offline, with the
//! node stopped. It is not an in-place startup rewrite and it is not an
//! engine savepoint rollback of live promises: the first would make a
//! failure indistinguishable from a partially converted store, and the
//! second would take back votes this replica has already cast.
//!
//! The shape is the one the install lifecycle already has. Stage a new
//! `gen-<n>` of the *same* engine and profile, rewrite every row into
//! it, then activate -- and activation is the only step that changes
//! `CURRENT`, so a failure or a crash at any earlier point leaves the
//! previously selected generation exactly as it was and the staging
//! unreferenced. The prior generation is not deleted here; reclaiming
//! it is [`Generation::prune_unselected`], deliberately separate, so an
//! operator can still fall back by hand before the old directory goes.
//!
//! What a migration carries that an install does not: this node's own
//! `protocol_v1` obligations. An install replaces a learner's state and
//! must never touch a promise; a migration rewrites this node's own
//! state, and dropping its promises would be the amnesia every other
//! rule here exists to prevent.
//!
//! What it never does:
//!
//! * **No cross-engine conversion.** The selected generation's engine
//!   and profile must be this build's, and a mismatch fails closed. An
//!   end-to-end engine comparison is two fresh clusters (Section
//!   17.13), never a rolling conversion.
//! * **No downgrade.** A store already at this build's schema is
//!   [`Outcome::AlreadyCurrent`] and nothing is written; one *above* it
//!   was written by a newer build and is refused. There is no operation
//!   that lowers a schema version.
//! * **No identity change.** The migration keeps the cluster, domain,
//!   replica and incarnation it found, so a migration can never be a
//!   quiet re-enrolment.
//! * **No collection-registry change.** The source is opened through
//!   [`Generation::open_existing`] and the staging checks its
//!   predecessor the same way, and both require the selected manifest's
//!   collection registry to equal this build's. A schema change that
//!   adds, removes or renumbers a collection is therefore refused before
//!   any [`SchemaMigration::rewrite`] runs. That is safe today because
//!   the `StoreSchema` window is one version wide, so no admitted store
//!   can carry another registry; the release that first changes the
//!   registry has to verify the source against the registry its own
//!   manifest records, not this build's, before this can migrate it.
//! * **No store larger than memory.** Every row of the source is read
//!   and held before the staging is created, because the source and the
//!   staging are two generations of one root under one lock and the
//!   source's engine is closed before the root is extended.
//!   [`MigrateLimits`] shapes each page read and each write
//!   transaction; it does not bound what is retained, so peak memory is
//!   the size of the whole store. Streaming the source into the staging
//!   would need the lifecycle to hold both engines open under the one
//!   lock, which it deliberately does not.

use std::path::Path;

use coord_store_api::engine::{Direction, LocalEngine, OrderedRead, ScanRequest, WriteTxn};
use coord_store_api::registry::Collection;
use coord_types::formats::{Format, FormatError, admit};

use crate::lifecycle::{Generation, InactiveGeneration, OpenError, OpenOptions, StoreIdentity};
use crate::manifest::{ENGINE_NAME, MANIFEST_FORMAT, PROFILE_NAME, StoreManifestV1};

/// One row offered to a migration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Row<'a> {
    /// Collection the row belongs to.
    pub collection: Collection,
    /// Key.
    pub key: &'a [u8],
    /// Value.
    pub value: &'a [u8],
}

/// A rewritten row: its key and its value.
pub type Rewritten = (Vec<u8>, Vec<u8>);

/// A declared same-engine schema migration.
///
/// The implementation is per release and lives beside the change that
/// needed it. It is a pure function of one row, deliberately: a
/// migration that could see the whole store could depend on order, and
/// a rewrite that depends on order is one nobody can reason about after
/// an interruption.
pub trait SchemaMigration {
    /// The schema version this migration reads.
    fn from(&self) -> u32;
    /// The schema version it writes. Must be greater than [`Self::from`].
    fn to(&self) -> u32;
    /// Rewrite one row, or drop it by returning `None`.
    ///
    /// Returning the row unchanged is the common case: most rows of
    /// most collections are unaffected by any one schema change.
    fn rewrite(&self, row: &Row<'_>) -> Result<Option<Rewritten>, MigrateError>;
}

/// The identity migration: every row unchanged.
///
/// What a release with no row-level change still runs, so that the
/// generation replacement and its interruption behaviour are exercised
/// by the same code path a real migration takes rather than by a
/// special case nobody tests.
#[derive(Clone, Copy, Debug)]
pub struct Unchanged {
    /// Version read.
    pub from: u32,
    /// Version written.
    pub to: u32,
}

impl SchemaMigration for Unchanged {
    fn from(&self) -> u32 {
        self.from
    }
    fn to(&self) -> u32 {
        self.to
    }
    fn rewrite(&self, row: &Row<'_>) -> Result<Option<Rewritten>, MigrateError> {
        Ok(Some((row.key.to_vec(), row.value.to_vec())))
    }
}

/// How many rows a migration writes before a durable commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MigrateLimits {
    /// Rows read per engine page.
    pub page_rows: core::num::NonZeroU32,
    /// Bytes read per engine page.
    pub page_bytes: core::num::NonZeroU32,
    /// Rows written before a durable commit.
    pub rows_per_commit: u32,
}

impl Default for MigrateLimits {
    fn default() -> Self {
        MigrateLimits {
            page_rows: core::num::NonZeroU32::new(1024).expect("positive"),
            page_bytes: core::num::NonZeroU32::new(4 * 1024 * 1024).expect("positive"),
            rows_per_commit: 4096,
        }
    }
}

/// Why a migration was refused or stopped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MigrateError {
    /// The selected generation was written by another engine or
    /// durability profile. There is no cross-engine migration.
    EngineMismatch {
        /// What was found.
        found: String,
    },
    /// The selected generation's schema is outside this build's window.
    Format(FormatError),
    /// The migration does not read the schema the store is at.
    WrongMigration {
        /// Schema found in the store.
        found: u32,
        /// Schema the migration reads.
        reads: u32,
    },
    /// The migration does not write this build's schema, or writes one
    /// at or below what it reads.
    NotAnUpgrade {
        /// Schema the migration reads.
        from: u32,
        /// Schema it writes.
        to: u32,
    },
    /// A row could not be rewritten.
    Row {
        /// Why.
        reason: String,
    },
    /// The store lifecycle failed.
    Open(String),
}

impl core::fmt::Display for MigrateError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            MigrateError::EngineMismatch { found } => write!(
                f,
                "this store was written by {found}, and there is no cross-engine migration"
            ),
            MigrateError::Format(e) => write!(f, "{e}"),
            MigrateError::WrongMigration { found, reads } => write!(
                f,
                "this store is at schema {found} and the migration offered reads {reads}"
            ),
            MigrateError::NotAnUpgrade { from, to } => {
                write!(f, "a migration from {from} to {to} is not an upgrade")
            }
            MigrateError::Row { reason } => write!(f, "a row could not be migrated: {reason}"),
            MigrateError::Open(reason) => write!(f, "{reason}"),
        }
    }
}

impl core::error::Error for MigrateError {}

impl From<OpenError> for MigrateError {
    fn from(e: OpenError) -> Self {
        MigrateError::Open(format!("{e:?}"))
    }
}

impl From<coord_store_api::engine::EngineError> for MigrateError {
    fn from(e: coord_store_api::engine::EngineError) -> Self {
        MigrateError::Row {
            reason: e.to_string(),
        }
    }
}

/// What a migration did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The store was already at this build's schema; nothing was
    /// written.
    AlreadyCurrent {
        /// The schema it is at.
        schema: u32,
    },
    /// A replacement generation was staged, filled and selected.
    Migrated {
        /// Generation number now selected.
        generation: u64,
        /// Rows written.
        rows: u64,
        /// Rows the migration dropped.
        dropped: u64,
        /// Durable commits used.
        commits: u32,
    },
}

/// Read the selected generation's manifest without opening its engine.
///
/// What an operator asks before deciding anything, and what a daemon
/// reports at startup: which schema this store is at, and whether this
/// build can read it at all.
pub fn selected_schema(root: &Path) -> Result<StoreManifestV1, MigrateError> {
    let manifest = crate::lifecycle::selected_manifest(root)?;
    if manifest.engine != ENGINE_NAME || manifest.profile != PROFILE_NAME {
        return Err(MigrateError::EngineMismatch {
            found: format!("{}/{}", manifest.engine, manifest.profile),
        });
    }
    Ok(manifest)
}

/// Migrate `root`'s selected generation to this build's schema, offline.
///
/// The node must be stopped: the root lock is held for the whole
/// operation, so a running daemon makes this fail rather than race.
pub fn migrate(
    root: &Path,
    options: OpenOptions,
    step: &dyn SchemaMigration,
    limits: &MigrateLimits,
) -> Result<Outcome, MigrateError> {
    let manifest = selected_schema(root)?;
    admit(Format::StoreSchema, manifest.format).map_err(MigrateError::Format)?;
    if manifest.format == MANIFEST_FORMAT {
        return Ok(Outcome::AlreadyCurrent {
            schema: manifest.format,
        });
    }
    if step.from() != manifest.format {
        return Err(MigrateError::WrongMigration {
            found: manifest.format,
            reads: step.from(),
        });
    }
    if step.to() <= step.from() || step.to() != MANIFEST_FORMAT {
        return Err(MigrateError::NotAnUpgrade {
            from: step.from(),
            to: step.to(),
        });
    }
    let identity = StoreIdentity {
        cluster_id: manifest.cluster_id,
        domain_id: manifest.domain_id,
        replica_id: manifest.replica_id,
        incarnation: manifest.incarnation,
    };
    rewrite_into_new_generation(root, identity, options, step, limits)
}

/// Stage a replacement generation, rewrite every row into it through
/// `step`, and select it.
///
/// The mechanism [`migrate`] runs once its preconditions hold, exposed
/// because those preconditions are about *versions* and this is about
/// rows: until a second schema version exists there is no version pair
/// for `migrate` to accept, and a rewrite that only ran in a future
/// release would be a rewrite nobody had ever executed. Callers that
/// are not `migrate` are responsible for the version rules themselves;
/// `migrate` is the only caller that has them.
pub fn rewrite_into_new_generation(
    root: &Path,
    identity: StoreIdentity,
    options: OpenOptions,
    step: &dyn SchemaMigration,
    limits: &MigrateLimits,
) -> Result<Outcome, MigrateError> {
    // The source is opened through its own generation, and the root lock
    // it took is carried straight into the staging: were it released in
    // between, a writer could open the selected generation, commit and
    // close in the gap, and the staging would proceed from rows read
    // before those writes and activate without them. Only the source
    // engine is closed first, because a root has one open database per
    // process.
    let source = Generation::open_existing(root, identity, options)?;
    let rows = collect(&source, limits)?;
    let (engine, lock, _) = source.into_parts();
    drop(engine);

    let mut staged = InactiveGeneration::stage_migration_with_lock(root, lock, identity, options)?;
    let mut written = 0u64;
    let mut dropped = 0u64;
    let mut commits = 0u32;
    let mut pending = 0u32;
    let mut txn = staged.engine().begin_write()?;
    for (collection, key, value) in &rows {
        let rewritten = step.rewrite(&Row {
            collection: *collection,
            key,
            value,
        })?;
        let Some((key, value)) = rewritten else {
            dropped += 1;
            continue;
        };
        txn.put(collection.id(), &key, &value)?;
        written += 1;
        pending += 1;
        if pending >= limits.rows_per_commit.max(1) {
            txn.commit_durable().map_err(|e| MigrateError::Row {
                reason: format!("{e:?}"),
            })?;
            commits += 1;
            pending = 0;
            txn = staged.engine().begin_write()?;
        }
    }
    txn.commit_durable().map_err(|e| MigrateError::Row {
        reason: format!("{e:?}"),
    })?;
    commits += 1;
    let generation = staged.generation();
    // The only step that changes `CURRENT`. Everything fallible is above
    // it, so an interruption leaves the previous selection in force.
    staged.activate()?;
    Ok(Outcome::Migrated {
        generation,
        rows: written,
        dropped,
        commits,
    })
}

type Held = (Collection, Vec<u8>, Vec<u8>);

/// Every row of the source generation, in registry and key order.
///
/// Read whole rather than streamed alongside the write, because the
/// source and the staging are two generations of one root and one lock:
/// holding a read transaction open across the staging would keep the
/// source engine open while the root is being extended.
fn collect(source: &Generation, limits: &MigrateLimits) -> Result<Vec<Held>, MigrateError> {
    let mut out: Vec<Held> = Vec::new();
    let view = source.reader_snapshot()?;
    for collection in Collection::ALL {
        let mut resume: Option<Vec<u8>> = None;
        loop {
            let page = view.scan_page(
                collection.id(),
                &ScanRequest {
                    lower: std::ops::Bound::Unbounded,
                    upper: std::ops::Bound::Unbounded,
                    direction: Direction::Forward,
                    resume_after: resume.clone(),
                    max_rows: limits.page_rows,
                    max_bytes: limits.page_bytes,
                },
            )?;
            for row in &page.rows {
                out.push((collection, row.key.clone(), row.value.clone()));
            }
            match page.rows.last() {
                Some(last) if !page.exhausted => resume = Some(last.key.clone()),
                _ => break,
            }
        }
    }
    Ok(out)
}

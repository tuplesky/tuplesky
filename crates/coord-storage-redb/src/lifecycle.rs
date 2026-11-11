//! Fail-closed generation lifecycle.
//!
//! Layout under a root directory:
//!
//! ```text
//! <root>/lock                 exclusive advisory lock while open
//! <root>/CURRENT              name of the selected generation directory
//! <root>/gen-<n>/manifest.v1  checksummed StoreManifestV1
//! <root>/gen-<n>/domain.redb  the database
//! ```
//!
//! Creation is explicit and refuses an initialized root. Opening never
//! creates: a missing `CURRENT`, manifest or database, an empty or corrupt
//! database, an identity, engine or generation mismatch, or a second opener
//! all fail closed. The identity is also stored inside `meta_v1` and must
//! agree with the manifest, so a copied manifest cannot lend identity to a
//! foreign database.
//!
//! An [`InactiveGeneration`] (task-50) is the install path: it is created
//! under the root as the next `gen-<n>` with the caller's own identity,
//! filled and verified through its engine while `CURRENT` still names the
//! previous generation (or nothing), and only [`InactiveGeneration::activate`]
//! syncs the data, writes the manifest and then the pointer. A crash before
//! the pointer leaves the previous selection; a crash after leaves a
//! complete new one; nothing ever reverts a written pointer. A root whose
//! selected generation holds protocol obligations is never replaced this
//! way.

use std::fs::File;
use std::path::{Path, PathBuf};

use coord_store_api::registry::{Collection, meta_fields};
use coord_types::ids::{ClusterId, DomainId, ReplicaId, ReplicaIncarnation};
use redb::{ReadableDatabase, ReadableTableMetadata, TableDefinition};

use crate::engine::{RedbEngine, table_definition};
use crate::manifest::{ENGINE_NAME, ManifestError, PROFILE_NAME, StoreManifestV1};

/// Identity a generation must carry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoreIdentity {
    /// Cluster/restore identity.
    pub cluster_id: ClusterId,
    /// Domain.
    pub domain_id: DomainId,
    /// Replica.
    pub replica_id: ReplicaId,
    /// Incarnation.
    pub incarnation: ReplicaIncarnation,
}

/// Engine tuning recorded by the caller.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OpenOptions {
    /// redb page cache bytes (Section 19.3 starts at 256 MiB per active allocation).
    pub cache_bytes: usize,
}

impl Default for OpenOptions {
    fn default() -> Self {
        OpenOptions {
            cache_bytes: 256 * 1024 * 1024,
        }
    }
}

/// Why a generation could not be created or opened. Every variant is a
/// hard stop; none invites creation or reinitialization.
#[derive(Debug)]
pub enum OpenError {
    /// The root is already held by another opener.
    Busy,
    /// Creation on an already initialized root.
    AlreadyInitialized,
    /// Open on a root without `CURRENT`.
    NotInitialized,
    /// `CURRENT` names a generation directory that does not exist.
    MissingGeneration(String),
    /// Manifest problem.
    Manifest(ManifestError),
    /// Manifest identity field differs from the expectation.
    IdentityMismatch(&'static str),
    /// Manifest engine/profile differs.
    EngineMismatch(String),
    /// Manifest generation differs from `CURRENT`.
    GenerationMismatch,
    /// Database file absent.
    MissingDatabase,
    /// Database file empty (never a fresh database on open).
    EmptyDatabase,
    /// The store root's own records (`CURRENT`, a generation directory,
    /// the manifest's registry) are malformed or disagree with the request.
    Corrupt(String),
    /// The database file's bytes are wrong: redb reports it corrupted, it
    /// is not a redb database, a registered table is missing, or the
    /// integrity check fails (task-j05). Only this, an absent file and an
    /// empty one say the file itself cannot be used.
    DatabaseUnreadable(String),
    /// redb could not open or read the database for a reason that says
    /// nothing about its bytes: another opener holds it, it needs an
    /// upgrade, a repair was aborted, an I/O error. Refused under every
    /// profile; the file may be whole.
    Database(String),
    /// In-database identity record disagrees with the manifest.
    IdentityRecordMismatch(&'static str),
    /// The selected generation holds protocol obligations (promises,
    /// votes); a snapshot install never replaces a voter's state.
    ExistingObligations,
    /// The staged generation's cluster, domain or replica differs from the
    /// root's selected generation.
    RootIdentityMismatch(&'static str),
    /// I/O failure.
    Io(std::io::Error),
}

impl std::fmt::Display for OpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OpenError::Busy => f.write_str("store root is locked by another opener"),
            OpenError::AlreadyInitialized => f.write_str("store root already initialized"),
            OpenError::NotInitialized => f.write_str("store root not initialized (no CURRENT)"),
            OpenError::MissingGeneration(g) => write!(f, "CURRENT names missing generation {g}"),
            OpenError::Manifest(e) => write!(f, "{e}"),
            OpenError::IdentityMismatch(field) => {
                write!(f, "manifest {field} does not match expected identity")
            }
            OpenError::EngineMismatch(e) => write!(f, "manifest engine/profile mismatch: {e}"),
            OpenError::GenerationMismatch => {
                f.write_str("manifest generation differs from CURRENT")
            }
            OpenError::MissingDatabase => f.write_str("database file missing"),
            OpenError::EmptyDatabase => f.write_str("database file empty"),
            OpenError::Corrupt(e) => write!(f, "store root inconsistent: {e}"),
            OpenError::DatabaseUnreadable(e) => write!(f, "database unreadable: {e}"),
            OpenError::Database(e) => write!(f, "database could not be opened: {e}"),
            OpenError::IdentityRecordMismatch(field) => {
                write!(f, "in-database {field} differs from manifest")
            }
            OpenError::ExistingObligations => {
                f.write_str("selected generation holds protocol obligations")
            }
            OpenError::RootIdentityMismatch(field) => {
                write!(
                    f,
                    "staged {field} differs from the root's selected generation"
                )
            }
            OpenError::Io(e) => write!(f, "io: {e}"),
        }
    }
}

impl std::error::Error for OpenError {}

impl OpenError {
    /// Whether this is how a selected generation's database fails when
    /// the file itself cannot be used: absent, empty, or bytes redb proves
    /// wrong (task-j05). Not an identity record that disagrees with the
    /// manifest (a database that belongs to someone else), not the root's
    /// own records, and not an error that says nothing about the bytes
    /// ([`OpenError::Database`]: a lock, a format upgrade, I/O): those stay
    /// refused.
    ///
    /// The staging that acts on it, [`Generation::stage_reinstall_unreadable`],
    /// verifies the root and the manifest again first.
    pub const fn is_unreadable_database(&self) -> bool {
        matches!(
            self,
            OpenError::MissingDatabase
                | OpenError::EmptyDatabase
                | OpenError::DatabaseUnreadable(_)
        )
    }

    /// Classify an error from opening a database file.
    fn from_database(e: redb::DatabaseError) -> Self {
        match e {
            redb::DatabaseError::Storage(e) => Self::from_storage(e),
            other => OpenError::Database(other.to_string()),
        }
    }

    /// Classify an error from reading a database: only redb's own
    /// corruption, or a file that is not a redb database (which redb
    /// reports as I/O of kind `InvalidData`), proves the bytes wrong.
    fn from_storage(e: redb::StorageError) -> Self {
        match e {
            redb::StorageError::Corrupted(m) => OpenError::DatabaseUnreadable(m),
            redb::StorageError::Io(io) if io.kind() == std::io::ErrorKind::InvalidData => {
                OpenError::DatabaseUnreadable(io.to_string())
            }
            other => OpenError::Database(other.to_string()),
        }
    }

    fn from_transaction(e: redb::TransactionError) -> Self {
        match e {
            redb::TransactionError::Storage(e) => Self::from_storage(e),
            other => OpenError::Database(other.to_string()),
        }
    }

    /// A registered table that is absent, or present under another type,
    /// is the file's fault.
    fn from_table(table: &str, e: redb::TableError) -> Self {
        match e {
            redb::TableError::Storage(e) => match Self::from_storage(e) {
                OpenError::DatabaseUnreadable(m) => {
                    OpenError::DatabaseUnreadable(format!("table {table}: {m}"))
                }
                other => other,
            },
            e @ (redb::TableError::TableDoesNotExist(_)
            | redb::TableError::TableTypeMismatch { .. }
            | redb::TableError::TableIsMultimap(_)
            | redb::TableError::TypeDefinitionChanged { .. }) => {
                OpenError::DatabaseUnreadable(format!("table {table}: {e}"))
            }
            other => OpenError::Database(format!("table {table}: {other}")),
        }
    }
}

impl From<std::io::Error> for OpenError {
    fn from(e: std::io::Error) -> Self {
        OpenError::Io(e)
    }
}

impl From<ManifestError> for OpenError {
    fn from(e: ManifestError) -> Self {
        OpenError::Manifest(e)
    }
}

/// Exclusive root lock held for the lifetime of an open generation.
#[derive(Debug)]
pub struct RootLock {
    _file: File,
    path: PathBuf,
}

impl RootLock {
    /// Acquire the root lock. `create` is only for [`Generation::create`]:
    /// opening an existing root must not create the directory or the lock
    /// file, so a misspelled or absent root reports `NotInitialized` and
    /// leaves no artifacts behind.
    fn acquire(root: &Path, create: bool) -> Result<RootLock, OpenError> {
        if create {
            std::fs::create_dir_all(root)?;
        } else if !root.is_dir() {
            return Err(OpenError::NotInitialized);
        }
        // An initialized root is one that carries `CURRENT`; its lock file
        // may be (re)created because it is a per-root artifact that a copy
        // or restore can lose. An uninitialized root gets no lock file.
        let create_lock = create || root.join(CURRENT).is_file();
        let path = root.join("lock");
        let file = match File::options()
            .read(true)
            .write(true)
            .create(create_lock)
            .truncate(false)
            .open(&path)
        {
            Ok(file) => file,
            Err(e) if !create_lock && e.kind() == std::io::ErrorKind::NotFound => {
                return Err(OpenError::NotInitialized);
            }
            Err(e) => return Err(OpenError::Io(e)),
        };
        match file.try_lock() {
            Ok(()) => Ok(RootLock { _file: file, path }),
            Err(std::fs::TryLockError::WouldBlock) => Err(OpenError::Busy),
            Err(std::fs::TryLockError::Error(e)) => Err(OpenError::Io(e)),
        }
    }

    /// Lock file path.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// An open generation: the engine plus the lock and manifest.
pub struct Generation {
    engine: RedbEngine,
    manifest: StoreManifestV1,
    _lock: RootLock,
    directory: PathBuf,
}

const CURRENT: &str = "CURRENT";
const MANIFEST: &str = "manifest.v1";
const DATABASE: &str = "domain.redb";

fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = File::create(&tmp)?;
        use std::io::Write as _;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    if let Some(dir) = path.parent() {
        File::open(dir)?.sync_all()?;
    }
    Ok(())
}

impl Generation {
    /// Create generation 1 under an uninitialized root.
    pub fn create(
        root: &Path,
        identity: StoreIdentity,
        options: OpenOptions,
    ) -> Result<Generation, OpenError> {
        let lock = RootLock::acquire(root, true)?;
        if root.join(CURRENT).exists() {
            return Err(OpenError::AlreadyInitialized);
        }
        let generation = 1u64;
        let directory = root.join(format!("gen-{generation:06}"));
        if directory.exists() {
            return Err(OpenError::AlreadyInitialized);
        }
        let (db, db_path) = initialize_database(&directory, &identity, &options)?;
        let manifest = manifest_for(&identity, generation);
        manifest.write(&directory.join(MANIFEST))?;
        // The durable pointer selects the generation last.
        write_atomic(
            &root.join(CURRENT),
            directory.file_name().unwrap().as_encoded_bytes(),
        )?;
        Ok(Generation {
            engine: RedbEngine::from_database(db, db_path, options.cache_bytes),
            manifest,
            _lock: lock,
            directory,
        })
    }

    /// Open the selected generation of an initialized root, verifying it
    /// against `expected`.
    pub fn open_existing(
        root: &Path,
        expected: StoreIdentity,
        options: OpenOptions,
    ) -> Result<Generation, OpenError> {
        let lock = RootLock::acquire(root, false)?;
        let current = match std::fs::read(root.join(CURRENT)) {
            Ok(bytes) => String::from_utf8(bytes)
                .map_err(|_| OpenError::Corrupt("CURRENT is not UTF-8".into()))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(OpenError::NotInitialized);
            }
            Err(e) => return Err(OpenError::Io(e)),
        };
        let current = current.trim();
        if current.is_empty() || current.contains('/') || current.contains("..") {
            return Err(OpenError::Corrupt("CURRENT content invalid".into()));
        }
        let directory = root.join(current);
        if !directory.is_dir() {
            return Err(OpenError::MissingGeneration(current.to_owned()));
        }
        let manifest = StoreManifestV1::read(&directory.join(MANIFEST))?;
        if manifest.engine != ENGINE_NAME || manifest.profile != PROFILE_NAME {
            return Err(OpenError::EngineMismatch(format!(
                "{}/{}",
                manifest.engine, manifest.profile
            )));
        }
        if manifest.cluster_id != expected.cluster_id {
            return Err(OpenError::IdentityMismatch("cluster_id"));
        }
        if manifest.domain_id != expected.domain_id {
            return Err(OpenError::IdentityMismatch("domain_id"));
        }
        if manifest.replica_id != expected.replica_id {
            return Err(OpenError::IdentityMismatch("replica_id"));
        }
        if manifest.incarnation != expected.incarnation {
            return Err(OpenError::IdentityMismatch("incarnation"));
        }
        let expected_dir = format!("gen-{:06}", manifest.generation);
        if expected_dir != current {
            return Err(OpenError::GenerationMismatch);
        }
        if manifest.collections != StoreManifestV1::registry_snapshot() {
            return Err(OpenError::Corrupt(
                "manifest collection registry differs from this build".into(),
            ));
        }
        let db_path = directory.join(DATABASE);
        let db = open_verified_database(&db_path, &manifest, &options)?;
        let engine = RedbEngine::from_database(db, db_path, options.cache_bytes);
        Ok(Generation {
            engine,
            manifest,
            _lock: lock,
            directory,
        })
    }

    /// Adopt the selected generation under a newly committed incarnation
    /// of the same replica (task-58; design Section 20.4).
    ///
    /// A node whose voting key is replaced keeps its durable state. The
    /// replacement is a committed configuration transition, not a new
    /// replica: the journal, its checkpoints and the epoch metadata are
    /// this node's own and are exactly what the new generation has to
    /// come back on. Refusing to open them would make an authorized
    /// replacement a restore from nothing.
    ///
    /// Only forwards. A manifest *ahead* of `identity` is the fencing
    /// case this stamp exists for -- a disk cloned or restored from
    /// before a replacement -- and stays refused, so an old credential
    /// can never re-adopt a root that has moved past it. The caller must
    /// already have established that `identity.incarnation` is the
    /// *committed* one; this decides nothing about membership and is not
    /// a way to acquire a generation by asserting a number.
    ///
    /// Only the manifest is written, and it is written atomically, so
    /// this half of adoption is one step with nothing to interrupt
    /// halfway. It is not the whole of a node's adoption: the journal's
    /// stream is the other half, and it has to be carried forward
    /// *before* this runs (see [`Generation::adoption_pending`]), since
    /// this overwrites the generation it would be carried from. The
    /// projection database is deliberately untouched: a write here
    /// would commit whatever the last run left uncommitted in it, and
    /// the journal-first invariant is that the materialized frontier
    /// never runs ahead of the journal's durable head. The identity
    /// record inside the database therefore keeps naming the generation
    /// the database was created under, and [`Generation::open_existing`]
    /// accepts a record that lags the manifest -- never one that leads
    /// it.
    ///
    /// Returns the generation the root was stamped with, where it was
    /// behind -- which is what the caller needs to carry this node's
    /// journal stream forward with it.
    pub fn adopt(
        root: &Path,
        identity: StoreIdentity,
    ) -> Result<Option<ReplicaIncarnation>, OpenError> {
        let _lock = RootLock::acquire(root, false)?;
        let (directory, manifest) = Self::stamped(root, &identity)?;
        if manifest.incarnation == identity.incarnation {
            return Ok(None);
        }
        let previous = manifest.incarnation;
        let adopted = StoreManifestV1 {
            incarnation: identity.incarnation,
            ..manifest
        };
        adopted.write(&directory.join(MANIFEST))?;
        Ok(Some(previous))
    }

    /// What [`Generation::adopt`] would adopt from, without adopting it:
    /// the generation the root is stamped with where it is behind
    /// `identity`, `None` where it is already `identity`'s, and the same
    /// refusals.
    ///
    /// Adoption is two durable steps in two places -- this manifest and
    /// the journal's stream mapping -- and the order is what makes a
    /// crash between them recoverable. The stream has to be carried
    /// forward first, and carrying it needs the generation it is carried
    /// *from*, which only the manifest records and which advancing the
    /// manifest overwrites. So the caller reads it here, carries the
    /// stream (idempotent once done), and only then adopts: a crash in
    /// between leaves the manifest behind, and the next start reads the
    /// same answer and finishes.
    pub fn adoption_pending(
        root: &Path,
        identity: StoreIdentity,
    ) -> Result<Option<ReplicaIncarnation>, OpenError> {
        let _lock = RootLock::acquire(root, false)?;
        let (_, manifest) = Self::stamped(root, &identity)?;
        Ok((manifest.incarnation < identity.incarnation).then_some(manifest.incarnation))
    }

    /// The selected generation's directory and manifest, checked as
    /// belonging to `identity`'s replica at a generation no later than
    /// its own. The caller holds the root lock.
    fn stamped(
        root: &Path,
        identity: &StoreIdentity,
    ) -> Result<(PathBuf, StoreManifestV1), OpenError> {
        let current = match std::fs::read(root.join(CURRENT)) {
            Ok(bytes) => String::from_utf8(bytes)
                .map_err(|_| OpenError::Corrupt("CURRENT is not UTF-8".into()))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(OpenError::NotInitialized);
            }
            Err(e) => return Err(OpenError::Io(e)),
        };
        let current = current.trim();
        if current.is_empty() || current.contains('/') || current.contains("..") {
            return Err(OpenError::Corrupt("CURRENT content invalid".into()));
        }
        let directory = root.join(current);
        if !directory.is_dir() {
            return Err(OpenError::MissingGeneration(current.to_owned()));
        }
        let manifest = StoreManifestV1::read(&directory.join(MANIFEST))?;
        // The replica and its origin are not what a replacement changes,
        // so nothing here may paper over a different one.
        if manifest.cluster_id != identity.cluster_id {
            return Err(OpenError::IdentityMismatch("cluster_id"));
        }
        if manifest.domain_id != identity.domain_id {
            return Err(OpenError::IdentityMismatch("domain_id"));
        }
        if manifest.replica_id != identity.replica_id {
            return Err(OpenError::IdentityMismatch("replica_id"));
        }
        if manifest.incarnation > identity.incarnation {
            return Err(OpenError::IdentityMismatch("incarnation"));
        }
        Ok((directory, manifest))
    }

    /// The engine.
    pub fn engine(&mut self) -> &mut RedbEngine {
        &mut self.engine
    }

    /// A pinned read view of this generation.
    pub fn reader_snapshot(
        &self,
    ) -> Result<<<RedbEngine as coord_store_api::engine::LocalEngine>::Reader as coord_store_api::engine::SnapshotSource>::View, coord_store_api::engine::EngineError>{
        use coord_store_api::engine::{LocalEngine, SnapshotSource};
        self.engine.reader().snapshot()
    }

    /// The verified manifest.
    pub fn manifest(&self) -> &StoreManifestV1 {
        &self.manifest
    }

    /// Generation directory.
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// Split into the engine and the root lock. The lock must outlive every
    /// use of the engine; dropping it lets another opener in.
    pub fn into_parts(self) -> (RedbEngine, RootLock, StoreManifestV1) {
        (self.engine, self._lock, self.manifest)
    }

    /// Stage the next generation of this root while keeping the root lock
    /// this generation already holds, so no other opener can slip in between
    /// closing the selected generation and staging its replacement. The
    /// selected generation's engine is closed first (one open database per
    /// root per process); its directory, manifest and the active pointer stay
    /// exactly as they are until the staging is activated.
    pub fn stage_next(
        self,
        identity: StoreIdentity,
        options: OpenOptions,
    ) -> Result<InactiveGeneration, OpenError> {
        let root = self
            .directory
            .parent()
            .ok_or_else(|| OpenError::Corrupt("generation directory has no root".into()))?
            .to_path_buf();
        let (engine, lock, _) = self.into_parts();
        drop(engine);
        stage_with_lock(&root, lock, identity, options)
    }

    /// Stage the next generation of this root to reinstall this node's own
    /// local recovery image into (task-j06, Section 17.16.4), keeping the
    /// root lock as [`Generation::stage_next`] does.
    ///
    /// Unlike an install, the selected generation may hold protocol
    /// obligations: the image is this node's own state at the baseline,
    /// promises and votes included, so they are carried, not replaced.
    /// The selected generation is kept until the staging is activated;
    /// a crash before that boots it again.
    pub fn stage_reinstall(
        self,
        identity: StoreIdentity,
        options: OpenOptions,
    ) -> Result<InactiveGeneration, OpenError> {
        let root = self
            .directory
            .parent()
            .ok_or_else(|| OpenError::Corrupt("generation directory has no root".into()))?
            .to_path_buf();
        let (engine, lock, _) = self.into_parts();
        drop(engine);
        InactiveGeneration::stage_migration_with_lock(&root, lock, identity, options)
    }

    /// Stage the next generation of `root` to reinstall this node's own
    /// local recovery image into, when the selected generation's database
    /// cannot be opened at all (task-j05 step 3; Section 17.16.4). Under
    /// the replay-backed profile the projection is derived state: the
    /// journal and the image of a published baseline rebuild it, so a
    /// file redb cannot open is discarded and reinstalled like a
    /// projection that does not continue the journal.
    ///
    /// Everything about the root is verified as any staging verifies it:
    /// `CURRENT`, the selected manifest, its engine, profile and registry,
    /// and that `identity` is the same replica of the same domain, no
    /// earlier than the manifest. Only the database is not opened, and it
    /// must fail as [`OpenError::is_unreadable_database`] says: a database
    /// that opens is not discarded, and one whose identity record names
    /// another node is refused. The selected generation stays selected
    /// until the staging is activated, so a crash before that finds the
    /// same unreadable file and reinstalls again.
    pub fn stage_reinstall_unreadable(
        root: &Path,
        identity: StoreIdentity,
        options: OpenOptions,
    ) -> Result<InactiveGeneration, OpenError> {
        let lock = RootLock::acquire(root, false)?;
        if !root.join(CURRENT).is_file() {
            return Err(OpenError::NotInitialized);
        }
        stage_with_lock_for(root, lock, identity, options, Obligations::Unreadable)
    }

    /// Remove every generation directory of this root except the selected
    /// one, reclaiming replaced and abandoned stagings. Only safe once the
    /// selection is durable, which it is for an open generation: this reads
    /// the selected directory it was opened with and never touches `CURRENT`.
    /// Returns the generation numbers removed.
    pub fn prune_unselected(&self) -> std::io::Result<Vec<u64>> {
        self.prune_unselected_except(None)
    }

    /// [`Generation::prune_unselected`], keeping generation `keep` as well:
    /// the one a reinstall just replaced, kept as evidence of what was
    /// discarded, while every earlier one goes (#212 review).
    pub fn prune_unselected_except(&self, keep: Option<u64>) -> std::io::Result<Vec<u64>> {
        self.prune_where(|number| Some(number) != keep)
    }

    /// Remove the stagings a stopped attempt left above the selected
    /// generation, on an ordinary start (#212 re-review). A staging takes
    /// the next number and `CURRENT` never named one that is above the
    /// selection, so none of them holds anything a start reads. The
    /// generations below the selection, a reinstall's discarded one among
    /// them, are kept: they are evidence, and the reinstall prunes them.
    pub fn prune_abandoned_stagings(&self) -> std::io::Result<Vec<u64>> {
        let selected = self.manifest.generation;
        self.prune_where(|number| number > selected)
    }

    /// Remove every generation other than the selected one that `remove`
    /// names, syncing the root once if any went.
    fn prune_where(&self, remove: impl Fn(u64) -> bool) -> std::io::Result<Vec<u64>> {
        let (Some(root), Some(selected)) = (self.directory.parent(), self.directory.file_name())
        else {
            return Ok(Vec::new());
        };
        let mut removed = Vec::new();
        for number in generation_numbers(root)? {
            let name = format!("gen-{number:06}");
            if name.as_str() == selected || !remove(number) {
                continue;
            }
            std::fs::remove_dir_all(root.join(&name))?;
            removed.push(number);
        }
        if !removed.is_empty() {
            File::open(root)?.sync_all()?;
        }
        removed.sort_unstable();
        Ok(removed)
    }
}

/// Table definition helper re-exported for callers that inspect raw tables
/// in tests.
pub fn raw_table(name: &'static str) -> TableDefinition<'static, &'static [u8], &'static [u8]> {
    TableDefinition::new(name)
}

fn manifest_for(identity: &StoreIdentity, generation: u64) -> StoreManifestV1 {
    StoreManifestV1 {
        format: crate::manifest::MANIFEST_FORMAT,
        engine: ENGINE_NAME.to_owned(),
        profile: PROFILE_NAME.to_owned(),
        cluster_id: identity.cluster_id,
        domain_id: identity.domain_id,
        replica_id: identity.replica_id,
        incarnation: identity.incarnation,
        generation,
        collections: StoreManifestV1::registry_snapshot(),
    }
}

/// Create the database of a generation directory with every registered
/// table and the identity record in one durable transaction.
fn initialize_database(
    directory: &Path,
    identity: &StoreIdentity,
    options: &OpenOptions,
) -> Result<(redb::Database, PathBuf), OpenError> {
    std::fs::create_dir_all(directory)?;
    let db_path = directory.join(DATABASE);
    if db_path.exists() {
        return Err(OpenError::AlreadyInitialized);
    }
    let db = redb::Database::builder()
        .set_cache_size(options.cache_bytes)
        .create(&db_path)
        .map_err(|e| OpenError::Database(e.to_string()))?;
    {
        let mut txn = db
            .begin_write()
            .map_err(|e| OpenError::Database(e.to_string()))?;
        txn.set_durability(redb::Durability::Immediate)
            .map_err(|e| OpenError::Database(e.to_string()))?;
        txn.set_two_phase_commit(true);
        for c in Collection::ALL {
            txn.open_table(table_definition(c))
                .map_err(|e| OpenError::Database(e.to_string()))?;
        }
        {
            let mut meta = txn
                .open_table(table_definition(Collection::MetaV1))
                .map_err(|e| OpenError::Database(e.to_string()))?;
            let rows: [(&[u8], Vec<u8>); 6] = [
                (meta_fields::CLUSTER_ID, identity.cluster_id.0.to_vec()),
                (meta_fields::DOMAIN_ID, identity.domain_id.0.to_vec()),
                (meta_fields::REPLICA_ID, identity.replica_id.0.to_vec()),
                (
                    meta_fields::INCARNATION,
                    identity.incarnation.to_be_bytes().to_vec(),
                ),
                (meta_fields::ENGINE, ENGINE_NAME.as_bytes().to_vec()),
                (meta_fields::PROFILE, PROFILE_NAME.as_bytes().to_vec()),
            ];
            for (k, v) in rows {
                meta.insert(k, v.as_slice())
                    .map_err(|e| OpenError::Database(e.to_string()))?;
            }
        }
        txn.commit()
            .map_err(|e| OpenError::Database(e.to_string()))?;
    }
    Ok((db, db_path))
}

/// Open the database of a generation whose manifest has already been
/// verified, and apply the in-database checks every selected generation
/// must pass before anything reads or replaces it, in this order:
///
/// 1. the file exists and is not empty;
/// 2. redb opens it with its integrity check on open
///    (`set_check_integrity_on_open`, redb 4.5): every page of the
///    committed trees is verified and the allocator state rebuilt, so
///    damage anywhere in the file is found before any of its rows is
///    read, and is redb's `Corrupted` rather than a panic (#212 review);
/// 3. the `meta_v1` identity rows agree with the manifest, so a copied
///    manifest cannot lend identity to a foreign database;
/// 4. every registered table exists.
///
/// Damage is [`OpenError::DatabaseUnreadable`]: redb's `Corrupted`, a file
/// that isn't redb's, a missing table, and a panic inside redb, which redb
/// 4.2 raised on some single-page damage and which is still caught as a
/// backstop. Identity is read only from a file that verified, so a
/// damaged identity leaf is unreadable, never another node's database,
/// and a foreign database is refused for its identity whatever tables it
/// lacks. A file the check repaired (its allocator rebuilt, or a last
/// commit rolled back) verified after the repair and is used: its stamp
/// is checked against the journal like any other projection's. redb does
/// not say whether the open repaired anything, so no line is written for
/// it; a second `check_integrity()` would say, at the cost of reading the
/// whole file again at every start.
fn open_verified_database(
    db_path: &Path,
    manifest: &StoreManifestV1,
    options: &OpenOptions,
) -> Result<redb::Database, OpenError> {
    let meta = match std::fs::metadata(db_path) {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(OpenError::MissingDatabase);
        }
        Err(e) => return Err(OpenError::Io(e)),
    };
    if meta.len() == 0 {
        return Err(OpenError::EmptyDatabase);
    }
    let db = unwinding("open", || {
        redb::Database::builder()
            .set_cache_size(options.cache_bytes)
            .set_check_integrity_on_open(true)
            .open(db_path)
            .map_err(|e| match OpenError::from_database(e) {
                OpenError::DatabaseUnreadable(m) => {
                    OpenError::DatabaseUnreadable(format!("integrity: {m}"))
                }
                other => other,
            })
    })?;
    let read = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let txn = db.begin_read().map_err(OpenError::from_transaction)?;
        let meta_table = txn
            .open_table(table_definition(Collection::MetaV1))
            .map_err(|e| OpenError::from_table(Collection::MetaV1.name(), e))?;
        let check =
            |field: &'static str, key: &[u8], expected_bytes: &[u8]| -> Result<(), OpenError> {
                let value = meta_table.get(key).map_err(OpenError::from_storage)?;
                match value {
                    Some(v) if v.value() == expected_bytes => Ok(()),
                    _ => Err(OpenError::IdentityRecordMismatch(field)),
                }
            };
        check(
            "cluster_id",
            meta_fields::CLUSTER_ID,
            &manifest.cluster_id.0,
        )?;
        check("domain_id", meta_fields::DOMAIN_ID, &manifest.domain_id.0)?;
        check(
            "replica_id",
            meta_fields::REPLICA_ID,
            &manifest.replica_id.0,
        )?;
        // The record's generation may lag the manifest's, and only
        // that way round (task-58). The record says which key
        // generation this database was created under; the manifest
        // says which one the node is committed at now, and an
        // authorized replacement advances the manifest alone --
        // writing to the projection outside the journal-first path
        // would make its last non-durable commits durable and push
        // the materialized frontier past the journal's head. A
        // record *ahead* of the manifest is still a database that
        // does not belong under it.
        {
            let value = meta_table
                .get(meta_fields::INCARNATION)
                .map_err(OpenError::from_storage)?;
            let recorded = value
                .as_ref()
                .map(|v| ReplicaIncarnation::from_be_slice(v.value()))
                .and_then(Result::ok)
                .ok_or(OpenError::IdentityRecordMismatch("incarnation"))?;
            if recorded > manifest.incarnation {
                return Err(OpenError::IdentityRecordMismatch("incarnation"));
            }
        }
        check("engine", meta_fields::ENGINE, ENGINE_NAME.as_bytes())?;
        check("profile", meta_fields::PROFILE, PROFILE_NAME.as_bytes())?;
        for c in Collection::ALL {
            txn.open_table(table_definition(c))
                .map_err(|e| OpenError::from_table(c.name(), e))?;
        }
        Ok(())
    }));
    match read {
        Ok(Ok(())) => Ok(db),
        Ok(Err(e)) => Err(e),
        Err(payload) => {
            // Not closed: redb's close writes its allocator state and
            // clears its recovery flag, and the file is left as it was
            // found, as evidence (#212 review). The handle's file lock
            // stays held until the process exits; nothing in it opens
            // this file again, since `replay` reinstalls into a new
            // generation and `strict` stops.
            std::mem::forget(db);
            Err(panicked("identity and tables", &*payload))
        }
    }
}

/// `f`, with a panic inside redb taken as proof the file is malformed:
/// redb 4.2 panicked rather than erred on some damaged pages (an
/// allocator or tree page whose bytes pass its checks badly), and did so
/// on every open of the same file. 4.5's integrity check on open reports
/// them as `Corrupted`; this stays as the backstop.
fn unwinding<T>(
    what: &'static str,
    f: impl FnOnce() -> Result<T, OpenError>,
) -> Result<T, OpenError> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(result) => result,
        Err(payload) => Err(panicked(what, &*payload)),
    }
}

/// A panic inside redb, as the unreadable database it shows.
fn panicked(what: &'static str, payload: &(dyn std::any::Any + Send)) -> OpenError {
    let message = payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "no message".to_owned());
    OpenError::DatabaseUnreadable(format!("redb panicked in its {what}: {message}"))
}

/// The generation directories under a root, by number.
fn generation_numbers(root: &Path) -> std::io::Result<Vec<u64>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(root)? {
        let name = entry?.file_name();
        let Some(name) = name.to_str() else { continue };
        if let Some(n) = name.strip_prefix("gen-")
            && let Ok(n) = n.parse::<u64>()
        {
            out.push(n);
        }
    }
    Ok(out)
}

/// One durable step of activation, in the order they happen. A crash may
/// fall between any two of them, and
/// [`InactiveGeneration::activate_interrupted`] stops after exactly one, so
/// interrupted-generation tests (design Section 17.7) exercise the real
/// sequence rather than a reconstruction of it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ActivateStep {
    /// The database file and the generation directory are synced.
    SyncData,
    /// The checksummed manifest is written and synced.
    WriteManifest,
    /// The active pointer is replaced atomically and synced. After this the
    /// new generation is the selected one; nothing ever reverts it.
    WritePointer,
}

/// A generation created under a root but not selected: the install target of
/// a shared checkpoint (task-50; design Sections 17.6 and 17.13).
///
/// Staging holds the root lock, so the selected generation cannot be served
/// or replaced by anyone else meanwhile, and the previously selected
/// generation is left entirely untouched: it keeps its directory, manifest
/// and the active pointer until [`InactiveGeneration::activate`] has made
/// the replacement durable. Nothing here promotes an epoch, resets a local
/// promise or converts between engines: staging only ever creates a new
/// `gen-<n>` of this same engine and profile, with the caller's own
/// identity.
pub struct InactiveGeneration {
    engine: RedbEngine,
    identity: StoreIdentity,
    generation: u64,
    lock: RootLock,
    root: PathBuf,
    directory: PathBuf,
    previous: Option<StoreManifestV1>,
}

impl InactiveGeneration {
    /// Stage a replacement generation for an offline schema migration
    /// (task-60), carrying this node's protocol obligations forward.
    ///
    /// Deliberately not [`InactiveGeneration::stage`]: an install
    /// refuses a selected generation that holds promises, and a
    /// migration rewrites this node's own state and must keep them.
    /// Nothing else differs -- same engine, same profile, same
    /// identity, and `CURRENT` untouched until activation.
    pub fn stage_migration(
        root: &Path,
        identity: StoreIdentity,
        options: OpenOptions,
    ) -> Result<InactiveGeneration, OpenError> {
        let lock = RootLock::acquire(root, false)?;
        stage_with_lock_for(root, lock, identity, options, Obligations::Carried)
    }

    /// [`InactiveGeneration::stage_migration`] under a root lock the
    /// caller already holds -- the one the source generation was read
    /// under -- so nothing can open, write and close the selected
    /// generation between the rows being read and the replacement being
    /// staged. The source engine must already be closed.
    pub(crate) fn stage_migration_with_lock(
        root: &Path,
        lock: RootLock,
        identity: StoreIdentity,
        options: OpenOptions,
    ) -> Result<InactiveGeneration, OpenError> {
        stage_with_lock_for(root, lock, identity, options, Obligations::Carried)
    }

    /// Stage the next generation of `root` for `identity`, acquiring the
    /// root lock.
    ///
    /// The root may be uninitialized (a node with no state yet) or hold a
    /// selected generation of the same engine, profile, collection registry,
    /// cluster, domain and replica whose `protocol_v1` is empty (a learner or
    /// observer catching up again). A selected generation that holds protocol
    /// obligations, belongs to another identity, was written by another
    /// engine or schema, or cannot be read is refused: an install never
    /// replaces a voter's state and never converts anything.
    pub fn stage(
        root: &Path,
        identity: StoreIdentity,
        options: OpenOptions,
    ) -> Result<InactiveGeneration, OpenError> {
        // Staging may be a node's first state, so the root is created if
        // it is not there: the contract above admits an uninitialized
        // root, unlike opening an existing one.
        let lock = RootLock::acquire(root, true)?;
        stage_with_lock(root, lock, identity, options)
    }

    /// The engine of the staged generation. It is not selected: nothing
    /// serves from it, and a crash while it is being filled loses only the
    /// staging.
    pub fn engine(&mut self) -> &mut RedbEngine {
        &mut self.engine
    }

    /// Generation number the staging will select.
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Manifest of the generation `CURRENT` selects right now, if any. It
    /// stays selected until activation succeeds.
    pub const fn previous(&self) -> Option<&StoreManifestV1> {
        self.previous.as_ref()
    }

    /// Staged directory.
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// Make the staged generation the selected one: sync the data, write the
    /// checksummed manifest, then replace the active pointer.
    ///
    /// A failure or crash before the pointer leaves the previous selection
    /// in force and the staging unreferenced. Once the pointer is written
    /// the new generation is selected for good: no later error falls back to
    /// the old generation, which is why every fallible install step happens
    /// before this is called.
    pub fn activate(self) -> Result<Generation, OpenError> {
        let InactiveGeneration {
            engine,
            identity,
            generation,
            lock,
            root,
            directory,
            previous: _,
        } = self;
        let manifest = manifest_for(&identity, generation);
        activate_steps(&root, &directory, &manifest, ActivateStep::WritePointer)?;
        Ok(Generation {
            engine,
            manifest,
            _lock: lock,
            directory,
        })
    }

    /// Perform activation up to and including `step` and then stop, as a
    /// crash at that point would: the engine and the root lock are dropped
    /// and nothing further is written. Support for interrupted-generation
    /// tests; production code calls [`InactiveGeneration::activate`].
    pub fn activate_interrupted(self, step: ActivateStep) -> Result<(), OpenError> {
        let manifest = manifest_for(&self.identity, self.generation);
        activate_steps(&self.root, &self.directory, &manifest, step)
    }

    /// Discard the staged generation. The selection is untouched, so a
    /// failed install is always retryable.
    pub fn abandon(self) -> std::io::Result<()> {
        let InactiveGeneration {
            engine,
            lock,
            directory,
            ..
        } = self;
        drop(engine);
        std::fs::remove_dir_all(&directory)?;
        if let Some(parent) = directory.parent() {
            File::open(parent)?.sync_all()?;
        }
        drop(lock);
        Ok(())
    }
}

/// The durable steps of activation, in order, stopping after `last`.
fn activate_steps(
    root: &Path,
    directory: &Path,
    manifest: &StoreManifestV1,
    last: ActivateStep,
) -> Result<(), OpenError> {
    // Committed transactions were synced by redb; this syncs the file and
    // the directory entries that name it, so the data is complete on disk
    // before anything points at it.
    File::open(directory.join(DATABASE))?.sync_all()?;
    File::open(directory)?.sync_all()?;
    if last == ActivateStep::SyncData {
        return Ok(());
    }
    // The manifest is written atomically and synced, with its directory.
    manifest.write(&directory.join(MANIFEST))?;
    if last == ActivateStep::WriteManifest {
        return Ok(());
    }
    // The durable pointer selects the generation last.
    let name = directory
        .file_name()
        .ok_or_else(|| OpenError::Corrupt("generation directory has no name".into()))?;
    write_atomic(&root.join(CURRENT), name.as_encoded_bytes())?;
    Ok(())
}

/// Stage the next generation under an already held root lock.
/// The manifest of the generation `CURRENT` selects, without opening
/// its engine (task-60).
pub fn selected_manifest(root: &Path) -> Result<StoreManifestV1, OpenError> {
    let bytes = std::fs::read(root.join(CURRENT)).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            OpenError::NotInitialized
        } else {
            OpenError::Io(e)
        }
    })?;
    let current = String::from_utf8(bytes)
        .map_err(|_| OpenError::Corrupt("CURRENT is not UTF-8".into()))?
        .trim()
        .to_owned();
    if current.is_empty() || current.contains('/') || current.contains("..") {
        return Err(OpenError::Corrupt("CURRENT content invalid".into()));
    }
    let directory = root.join(&current);
    if !directory.is_dir() {
        return Err(OpenError::MissingGeneration(current));
    }
    Ok(StoreManifestV1::read(&directory.join(MANIFEST))?)
}

fn stage_with_lock(
    root: &Path,
    lock: RootLock,
    identity: StoreIdentity,
    options: OpenOptions,
) -> Result<InactiveGeneration, OpenError> {
    stage_with_lock_for(root, lock, identity, options, Obligations::Refused)
}

/// Whether a staging may replace a selected generation that holds
/// protocol obligations.
///
/// An install never may: it replaces a learner's or observer's state,
/// and a promise is not something to be replaced. An offline schema
/// migration must, because it rewrites this node's *own* state and
/// dropping its promises would be exactly the amnesia every other rule
/// here exists to prevent (task-60).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Obligations {
    /// A selected generation holding obligations is refused.
    Refused,
    /// Obligations are carried forward by the caller.
    Carried,
    /// The selected database cannot be opened, so its obligations cannot
    /// be read; the caller carries them forward from this node's own
    /// recovery image (task-j05). A database that opens is refused.
    Unreadable,
}

fn stage_with_lock_for(
    root: &Path,
    lock: RootLock,
    identity: StoreIdentity,
    options: OpenOptions,
    obligations: Obligations,
) -> Result<InactiveGeneration, OpenError> {
    let previous = read_selected_for(root, &identity, &options, obligations)?;
    let generation = generation_numbers(root)?
        .into_iter()
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| OpenError::Corrupt("generation number exhausted".into()))?;
    let directory = root.join(format!("gen-{generation:06}"));
    if directory.exists() {
        return Err(OpenError::AlreadyInitialized);
    }
    let (db, db_path) = initialize_database(&directory, &identity, &options)?;
    Ok(InactiveGeneration {
        engine: RedbEngine::from_database(db, db_path, options.cache_bytes),
        identity,
        generation,
        lock,
        root: root.to_path_buf(),
        directory,
        previous,
    })
}

/// The manifest of the generation `CURRENT` selects, verified as a legal
/// predecessor of a staging for `identity`. `None` when the root holds no
/// selection yet.
fn read_selected_for(
    root: &Path,
    identity: &StoreIdentity,
    options: &OpenOptions,
    obligations: Obligations,
) -> Result<Option<StoreManifestV1>, OpenError> {
    let bytes = match std::fs::read(root.join(CURRENT)) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(OpenError::Io(e)),
    };
    let current = String::from_utf8(bytes)
        .map_err(|_| OpenError::Corrupt("CURRENT is not UTF-8".into()))?
        .trim()
        .to_owned();
    if current.is_empty() || current.contains('/') || current.contains("..") {
        return Err(OpenError::Corrupt("CURRENT content invalid".into()));
    }
    let directory = root.join(&current);
    if !directory.is_dir() {
        return Err(OpenError::MissingGeneration(current));
    }
    let manifest = StoreManifestV1::read(&directory.join(MANIFEST))?;
    // No engine, profile or schema conversion: a staging only ever extends a
    // root this build already understands.
    if manifest.engine != ENGINE_NAME || manifest.profile != PROFILE_NAME {
        return Err(OpenError::EngineMismatch(format!(
            "{}/{}",
            manifest.engine, manifest.profile
        )));
    }
    if manifest.collections != StoreManifestV1::registry_snapshot() {
        return Err(OpenError::Corrupt(
            "manifest collection registry differs from this build".into(),
        ));
    }
    if format!("gen-{:06}", manifest.generation) != current {
        return Err(OpenError::GenerationMismatch);
    }
    // The staging node is the same replica of the same domain, and time only
    // moves forward.
    if manifest.cluster_id != identity.cluster_id {
        return Err(OpenError::RootIdentityMismatch("cluster_id"));
    }
    if manifest.domain_id != identity.domain_id {
        return Err(OpenError::RootIdentityMismatch("domain_id"));
    }
    if manifest.replica_id != identity.replica_id {
        return Err(OpenError::RootIdentityMismatch("replica_id"));
    }
    if manifest.incarnation > identity.incarnation {
        return Err(OpenError::RootIdentityMismatch("incarnation"));
    }
    // The database must pass the same checks an open applies: a selected
    // generation that open would refuse (a swapped or foreign database, a
    // missing table, damaged pages) fails closed here too, rather than being
    // silently replaced and later pruned.
    let opened = open_verified_database(&directory.join(DATABASE), &manifest, options);
    if obligations == Obligations::Unreadable {
        return match opened {
            Ok(_) => Err(OpenError::Corrupt(
                "the selected database opens; only an unreadable one is discarded".into(),
            )),
            Err(e) if e.is_unreadable_database() => Ok(Some(manifest)),
            Err(e) => Err(e),
        };
    }
    let db = opened?;
    // The selected generation must hold no protocol obligations: promises and
    // votes are never replaced or reset by an install. An offline migration
    // says `Carried` and copies them forward itself (task-60).
    if obligations == Obligations::Carried {
        return Ok(Some(manifest));
    }
    let txn = db.begin_read().map_err(OpenError::from_transaction)?;
    let protocol = txn
        .open_table(table_definition(Collection::ProtocolV1))
        .map_err(|e| OpenError::from_table(Collection::ProtocolV1.name(), e))?;
    let obligations = protocol.len().map_err(OpenError::from_storage)?;
    if obligations > 0 {
        return Err(OpenError::ExistingObligations);
    }
    Ok(Some(manifest))
}

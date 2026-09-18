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

use std::fs::File;
use std::path::{Path, PathBuf};

use coord_store_api::registry::{Collection, meta_fields};
use coord_types::ids::{ClusterId, DomainId, ReplicaId, ReplicaIncarnation};
use redb::{ReadableDatabase, TableDefinition};

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
    /// redb refused the database or a required table is absent.
    Corrupt(String),
    /// In-database identity record disagrees with the manifest.
    IdentityRecordMismatch(&'static str),
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
            OpenError::Corrupt(e) => write!(f, "database corrupt or unusable: {e}"),
            OpenError::IdentityRecordMismatch(field) => {
                write!(f, "in-database {field} differs from manifest")
            }
            OpenError::Io(e) => write!(f, "io: {e}"),
        }
    }
}

impl std::error::Error for OpenError {}

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
        let path = root.join("lock");
        let file = match File::options()
            .read(true)
            .write(true)
            .create(create)
            .truncate(false)
            .open(&path)
        {
            Ok(file) => file,
            Err(e) if !create && e.kind() == std::io::ErrorKind::NotFound => {
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
        std::fs::create_dir_all(&directory)?;
        let db_path = directory.join(DATABASE);
        if db_path.exists() {
            return Err(OpenError::AlreadyInitialized);
        }
        let db = redb::Database::builder()
            .set_cache_size(options.cache_bytes)
            .create(&db_path)
            .map_err(|e| OpenError::Corrupt(e.to_string()))?;
        // Create every registered table and the identity record in one
        // durable transaction.
        {
            let mut txn = db
                .begin_write()
                .map_err(|e| OpenError::Corrupt(e.to_string()))?;
            txn.set_durability(redb::Durability::Immediate)
                .map_err(|e| OpenError::Corrupt(e.to_string()))?;
            txn.set_two_phase_commit(true);
            for c in Collection::ALL {
                txn.open_table(table_definition(c))
                    .map_err(|e| OpenError::Corrupt(e.to_string()))?;
            }
            {
                let mut meta = txn
                    .open_table(table_definition(Collection::MetaV1))
                    .map_err(|e| OpenError::Corrupt(e.to_string()))?;
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
                        .map_err(|e| OpenError::Corrupt(e.to_string()))?;
                }
            }
            txn.commit()
                .map_err(|e| OpenError::Corrupt(e.to_string()))?;
        }
        let manifest = StoreManifestV1 {
            format: crate::manifest::MANIFEST_FORMAT,
            engine: ENGINE_NAME.to_owned(),
            profile: PROFILE_NAME.to_owned(),
            cluster_id: identity.cluster_id,
            domain_id: identity.domain_id,
            replica_id: identity.replica_id,
            incarnation: identity.incarnation,
            generation,
            collections: StoreManifestV1::registry_snapshot(),
        };
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
        let meta = match std::fs::metadata(&db_path) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(OpenError::MissingDatabase);
            }
            Err(e) => return Err(OpenError::Io(e)),
        };
        if meta.len() == 0 {
            return Err(OpenError::EmptyDatabase);
        }
        let db = redb::Database::builder()
            .set_cache_size(options.cache_bytes)
            .open(&db_path)
            .map_err(|e| OpenError::Corrupt(e.to_string()))?;
        // Every registered table must exist, and the identity record must
        // agree with the manifest.
        {
            let txn = db
                .begin_read()
                .map_err(|e| OpenError::Corrupt(e.to_string()))?;
            for c in Collection::ALL {
                txn.open_table(table_definition(c))
                    .map_err(|e| OpenError::Corrupt(format!("table {}: {e}", c.name())))?;
            }
            let meta_table = txn
                .open_table(table_definition(Collection::MetaV1))
                .map_err(|e| OpenError::Corrupt(e.to_string()))?;
            let check =
                |field: &'static str, key: &[u8], expected_bytes: &[u8]| -> Result<(), OpenError> {
                    let value = meta_table
                        .get(key)
                        .map_err(|e| OpenError::Corrupt(e.to_string()))?;
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
            check(
                "incarnation",
                meta_fields::INCARNATION,
                &manifest.incarnation.to_be_bytes(),
            )?;
            check("engine", meta_fields::ENGINE, ENGINE_NAME.as_bytes())?;
            check("profile", meta_fields::PROFILE, PROFILE_NAME.as_bytes())?;
        }
        Ok(Generation {
            engine: RedbEngine::from_database(db, db_path, options.cache_bytes),
            manifest,
            _lock: lock,
            directory,
        })
    }

    /// The engine.
    pub fn engine(&mut self) -> &mut RedbEngine {
        &mut self.engine
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
}

/// Table definition helper re-exported for callers that inspect raw tables
/// in tests.
pub fn raw_table(name: &'static str) -> TableDefinition<'static, &'static [u8], &'static [u8]> {
    TableDefinition::new(name)
}

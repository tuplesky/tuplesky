//! Fail-closed generation lifecycle for the fjall experiment, mirroring the
//! redb reference:
//!
//! ```text
//! <root>/lock                 exclusive advisory lock while open
//! <root>/CURRENT              name of the selected generation directory
//! <root>/gen-<n>/manifest.v1  checksummed StoreManifestV1 (engine "fjall")
//! <root>/gen-<n>/fjall/       the database directory
//! ```
//!
//! Creation is explicit and refuses an initialized root. Opening never
//! creates: a missing `CURRENT`, manifest or database directory, an empty
//! or unrecoverable database, an identity, engine, layout or generation
//! mismatch, or a second opener all fail closed. The manifest type, identity
//! and error enumeration are the redb crate's, so a root of either engine
//! opened by the other adapter is an `EngineMismatch`.

use std::fs::File;
use std::path::{Path, PathBuf};

use coord_storage_redb::manifest::{MANIFEST_FORMAT, StoreManifestV1};
use coord_storage_redb::{OpenError, StoreIdentity};
use coord_store_api::engine::{LocalEngine, OrderedRead, SnapshotSource, WriteTxn};
use coord_store_api::registry::{Collection, meta_fields};

use crate::engine::{FEATURES, FjallEngine, LAYOUT_NAME};

/// Engine name recorded for this adapter.
pub const ENGINE_NAME: &str = "fjall";
/// Durability profile of this experiment.
pub const PROFILE_NAME: &str = "experimental-single-writer-sync-all-v1";

/// Identity-record keys private to this adapter (alongside the shared
/// `meta_fields`).
mod fjall_fields {
    /// Physical layout name.
    pub const LAYOUT: &[u8] = b"fjall_layout";
    /// Feature set the database was created with.
    pub const FEATURES: &[u8] = b"fjall_features";
}

/// Engine tuning recorded by the caller.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FjallOpenOptions {
    /// fjall block cache bytes.
    pub cache_bytes: u64,
}

impl Default for FjallOpenOptions {
    fn default() -> Self {
        FjallOpenOptions {
            cache_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Exclusive root lock held for the lifetime of an open generation.
#[derive(Debug)]
pub struct RootLock {
    _file: File,
}

impl RootLock {
    fn acquire(root: &Path) -> Result<RootLock, OpenError> {
        std::fs::create_dir_all(root)?;
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join("lock"))?;
        match file.try_lock() {
            Ok(()) => Ok(RootLock { _file: file }),
            Err(std::fs::TryLockError::WouldBlock) => Err(OpenError::Busy),
            Err(std::fs::TryLockError::Error(e)) => Err(OpenError::Io(e)),
        }
    }
}

/// An open generation: the engine plus the lock and manifest.
pub struct FjallGeneration {
    engine: FjallEngine,
    manifest: StoreManifestV1,
    _lock: RootLock,
    directory: PathBuf,
}

const CURRENT: &str = "CURRENT";
const MANIFEST: &str = "manifest.v1";
const DATABASE: &str = "fjall";

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

fn corrupt(e: impl std::fmt::Display) -> OpenError {
    OpenError::Corrupt(e.to_string())
}

impl FjallGeneration {
    /// Create generation 1 under an uninitialized root.
    pub fn create(
        root: &Path,
        identity: StoreIdentity,
        options: FjallOpenOptions,
    ) -> Result<FjallGeneration, OpenError> {
        let lock = RootLock::acquire(root)?;
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
        let mut engine = FjallEngine::create(&db_path, options.cache_bytes).map_err(corrupt)?;
        // The identity record is written in one durable transaction.
        {
            let mut tx = engine.begin_write().map_err(corrupt)?;
            let meta = Collection::MetaV1.id();
            let rows: [(&[u8], Vec<u8>); 8] = [
                (meta_fields::CLUSTER_ID, identity.cluster_id.0.to_vec()),
                (meta_fields::DOMAIN_ID, identity.domain_id.0.to_vec()),
                (meta_fields::REPLICA_ID, identity.replica_id.0.to_vec()),
                (
                    meta_fields::INCARNATION,
                    identity.incarnation.to_be_bytes().to_vec(),
                ),
                (meta_fields::ENGINE, ENGINE_NAME.as_bytes().to_vec()),
                (meta_fields::PROFILE, PROFILE_NAME.as_bytes().to_vec()),
                (fjall_fields::LAYOUT, LAYOUT_NAME.as_bytes().to_vec()),
                (fjall_fields::FEATURES, FEATURES.as_bytes().to_vec()),
            ];
            for (k, v) in rows {
                tx.put(meta, k, &v).map_err(corrupt)?;
            }
            tx.commit_durable().map_err(corrupt)?;
        }
        let manifest = StoreManifestV1 {
            format: MANIFEST_FORMAT,
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
        Ok(FjallGeneration {
            engine,
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
        options: FjallOpenOptions,
    ) -> Result<FjallGeneration, OpenError> {
        let lock = RootLock::acquire(root)?;
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
        match std::fs::read_dir(&db_path) {
            Ok(mut entries) => {
                if entries.next().is_none() {
                    return Err(OpenError::EmptyDatabase);
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(OpenError::MissingDatabase);
            }
            Err(e) => return Err(OpenError::Io(e)),
        }
        let engine = FjallEngine::open(&db_path, options.cache_bytes).map_err(corrupt)?;
        // The identity record must agree with the manifest, and the physical
        // layout and feature set with this build.
        {
            let view = engine.reader().snapshot().map_err(corrupt)?;
            let meta = Collection::MetaV1.id();
            let check =
                |field: &'static str, key: &[u8], expected_bytes: &[u8]| -> Result<(), OpenError> {
                    match view.get(meta, key).map_err(corrupt)? {
                        Some(v) if v == expected_bytes => Ok(()),
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
            match view.get(meta, fjall_fields::LAYOUT).map_err(corrupt)? {
                Some(v) if v == LAYOUT_NAME.as_bytes() => {}
                Some(_) => {
                    return Err(OpenError::EngineMismatch(
                        "physical layout differs from this build".into(),
                    ));
                }
                None => return Err(OpenError::IdentityRecordMismatch("fjall_layout")),
            }
            match view.get(meta, fjall_fields::FEATURES).map_err(corrupt)? {
                Some(v) if v == FEATURES.as_bytes() => {}
                Some(_) => {
                    return Err(OpenError::EngineMismatch(
                        "feature set differs from this build".into(),
                    ));
                }
                None => return Err(OpenError::IdentityRecordMismatch("fjall_features")),
            }
        }
        Ok(FjallGeneration {
            engine,
            manifest,
            _lock: lock,
            directory,
        })
    }

    /// The engine.
    pub fn engine(&mut self) -> &mut FjallEngine {
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

    /// Split into the engine and the root lock, mirroring the redb
    /// reference: a caller that moves the engine into the common worker
    /// must keep the lock alive for as long as it uses the engine, because
    /// dropping it lets another opener in.
    pub fn into_parts(self) -> (FjallEngine, RootLock, StoreManifestV1) {
        (self.engine, self._lock, self.manifest)
    }
}

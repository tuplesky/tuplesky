//! Taking a backup of this node's selected generation, verifying one,
//! and restoring a new cluster from one (task-59; design Sections 5.4,
//! 7.4, 17.16, 22.2).
//!
//! Every rule that matters lives in `coord_checkpoint::restore`; this is
//! the file layout and the operator's side of it. The procedure itself
//! is `docs/operations/disaster-recovery.md`, and this module refuses
//! exactly what that page says it refuses.

use std::path::{Path, PathBuf};

use coord_checkpoint::export::{CheckpointOrigin, ExportLimits, export_shared};
use coord_checkpoint::install::{ChunkSet, InstallLimits};
use coord_checkpoint::manifest::{ChunkV1, SharedManifestV1};
use coord_checkpoint::restore::{
    Artifact, BackupManifestV1, FencingAttestationV1, RestorePlan, Restored, plan_restore,
    restore_shared,
};
use coord_store_api::engine::{LocalEngine, SnapshotSource};
use coord_types::ids::{ClusterId, ConfigurationEpoch};

/// Name of the backup manifest inside a backup directory. JSON, because
/// an operator reads it and hands parts of it to the attestation.
pub const BACKUP_MANIFEST: &str = "backup.json";
/// Name of the artifact's own manifest. Postcard: it is the artifact.
pub const ARTIFACT_MANIFEST: &str = "artifact.manifest";

/// Why a backup or restore could not be carried out here.
#[derive(Debug)]
pub enum BackupError {
    /// A file could not be read or written.
    File {
        /// Where.
        path: String,
        /// Why.
        reason: String,
    },
    /// The backup directory already holds a backup.
    Exists {
        /// Where.
        path: String,
    },
    /// The export failed.
    Export(String),
    /// The backup or the artifact does not verify, or the restore was
    /// refused.
    Refused(String),
}

impl core::fmt::Display for BackupError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            BackupError::File { path, reason } => write!(f, "cannot use {path}: {reason}"),
            BackupError::Exists { path } => {
                write!(
                    f,
                    "{path} already holds a backup; backups are never overwritten"
                )
            }
            BackupError::Export(reason) => {
                write!(f, "this node could not export a backup: {reason}")
            }
            BackupError::Refused(reason) => write!(f, "{reason}"),
        }
    }
}

impl core::error::Error for BackupError {}

fn file(path: &Path, e: &dyn core::fmt::Display) -> BackupError {
    BackupError::File {
        path: path.display().to_string(),
        reason: e.to_string(),
    }
}

fn chunk_name(ordinal: u32) -> String {
    format!("chunk-{ordinal:06}")
}

/// Export `engine`'s common state and write a backup into `out`.
///
/// The backup is written into a directory that does not already hold
/// one. Overwriting a backup in place would leave, for the duration of
/// the write, neither the old one nor a complete new one -- and a
/// backup is exactly the thing that must not be unavailable at the
/// moment it is wanted.
pub fn take<E: LocalEngine>(
    engine: &E,
    origin: CheckpointOrigin,
    out: &Path,
    taken_at: u64,
) -> Result<BackupManifestV1, BackupError> {
    if out.join(BACKUP_MANIFEST).exists() {
        return Err(BackupError::Exists {
            path: out.display().to_string(),
        });
    }
    let checkpoint = {
        let view = engine
            .reader()
            .snapshot()
            .map_err(|e| BackupError::Export(e.to_string()))?;
        export_shared(&view, origin, &ExportLimits::default())
            .map_err(|e| BackupError::Export(format!("{e}")))?
    };
    std::fs::create_dir_all(out).map_err(|e| file(out, &e))?;
    for chunk in &checkpoint.chunks {
        let path = out.join(chunk_name(chunk.ordinal));
        let bytes = chunk
            .encode()
            .map_err(|e| BackupError::Export(e.to_string()))?;
        std::fs::write(&path, &bytes).map_err(|e| file(&path, &e))?;
    }
    let path = out.join(ARTIFACT_MANIFEST);
    let bytes = postcard::to_allocvec(&checkpoint.manifest)
        .map_err(|e| BackupError::Export(e.to_string()))?;
    std::fs::write(&path, &bytes).map_err(|e| file(&path, &e))?;
    // The backup manifest last: it is what names the backup, so a
    // directory that carries one carries a complete backup.
    let manifest = BackupManifestV1::of(&checkpoint.manifest, taken_at);
    let path = out.join(BACKUP_MANIFEST);
    let json = serde_json::to_vec_pretty(&manifest).map_err(|e| file(&path, &e))?;
    std::fs::write(&path, &json).map_err(|e| file(&path, &e))?;
    Ok(manifest)
}

/// A backup read back off disk, with its artifact.
pub struct Backup {
    /// The backup manifest.
    pub manifest: BackupManifestV1,
    /// The artifact's own manifest.
    pub artifact: SharedManifestV1,
    /// Encoded chunks in ordinal order.
    pub chunks: Vec<Vec<u8>>,
}

impl Backup {
    /// A chunk set holding every chunk, checked against the artifact.
    pub fn chunk_set(&self) -> Result<ChunkSet, BackupError> {
        let mut set = ChunkSet::for_manifest(&self.artifact)
            .map_err(|e| BackupError::Refused(format!("{e}")))?;
        for bytes in &self.chunks {
            set.accept(&self.artifact, bytes.clone())
                .map_err(|e| BackupError::Refused(format!("{e}")))?;
        }
        Ok(set)
    }
}

/// Read a backup directory and verify it against itself.
///
/// Every chunk digest and the artifact root are recomputed, and the
/// backup manifest must bind that exact root: a backup index repointed
/// at different bytes fails here rather than at the restore.
pub fn read(dir: &Path) -> Result<Backup, BackupError> {
    let path = dir.join(BACKUP_MANIFEST);
    let bytes = std::fs::read(&path).map_err(|e| file(&path, &e))?;
    let manifest: BackupManifestV1 = serde_json::from_slice(&bytes).map_err(|e| file(&path, &e))?;
    manifest
        .verify()
        .map_err(|e| BackupError::Refused(format!("this backup manifest is not usable: {e}")))?;

    let path = dir.join(ARTIFACT_MANIFEST);
    let bytes = std::fs::read(&path).map_err(|e| file(&path, &e))?;
    let artifact: SharedManifestV1 = postcard::from_bytes(&bytes).map_err(|e| file(&path, &e))?;

    let mut chunks = Vec::with_capacity(artifact.chunks.len());
    for descriptor in &artifact.chunks {
        let path = dir.join(chunk_name(descriptor.ordinal));
        chunks.push(std::fs::read(&path).map_err(|e| file(&path, &e))?);
    }
    // The artifact against itself, then the backup manifest against the
    // artifact. Both, because either alone leaves a way to hand over
    // bytes that hash correctly but are not the backup that was taken.
    let root = coord_checkpoint::verify_shared(&artifact, &chunks)
        .map_err(|e| BackupError::Refused(format!("this backup does not verify: {e}")))?;
    if root != manifest.captured {
        return Err(BackupError::Refused(
            "this backup manifest names a different artifact than the one beside it".into(),
        ));
    }
    // Decoding every chunk here rather than at the restore, so a
    // verification says the backup is usable and means it.
    for bytes in &chunks {
        ChunkV1::decode(bytes)
            .map_err(|e| BackupError::Refused(format!("a chunk does not decode: {e}")))?;
    }
    Ok(Backup {
        manifest,
        artifact,
        chunks,
    })
}

/// Read an operator's fencing attestation.
pub fn fencing(path: &Path) -> Result<FencingAttestationV1, BackupError> {
    let bytes = std::fs::read(path).map_err(|e| file(path, &e))?;
    serde_json::from_slice(&bytes).map_err(|e| file(path, &e))
}

/// Admit a restore of `backup` into `successor`, or say why not.
pub fn plan(
    backup: &Backup,
    successor: ClusterId,
    configuration: ConfigurationEpoch,
    attestation: &FencingAttestationV1,
) -> Result<RestorePlan, BackupError> {
    plan_restore(
        &backup.manifest,
        Artifact::Shared,
        successor,
        configuration,
        Some(attestation),
    )
    .map_err(|e| BackupError::Refused(format!("this restore is refused: {e}")))
}

/// Carry out an admitted restore into a fresh generation's engine.
pub fn carry_out<E: LocalEngine>(
    engine: &mut E,
    backup: &Backup,
    plan: &RestorePlan,
    attestation: &FencingAttestationV1,
) -> Result<Restored, BackupError> {
    restore_shared(
        engine,
        &backup.artifact,
        backup.chunk_set()?,
        plan,
        attestation,
        &InstallLimits::default(),
    )
    .map_err(|e| BackupError::Refused(format!("this restore could not be carried out: {e}")))
}

/// Where a backup's files are, for messages.
pub fn describe(dir: &Path) -> PathBuf {
    dir.join(BACKUP_MANIFEST)
}

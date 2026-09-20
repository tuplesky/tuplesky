//! Where a node keeps its local recovery checkpoints, and the
//! crash-safe order it writes, selects and reclaims them in (task-j04;
//! design Section 17.16.3).
//!
//! The publication order of Section 17.16.3 is five steps, and only the
//! second and fifth are here; the others belong to the journal, because
//! it is the journal that makes a pointer durable and the journal whose
//! prefix is reclaimed:
//!
//! 1. pin a verified snapshot at `C` (the caller's; [`crate::local`]
//!    reads it);
//! 2. **build a complete inactive image and sync it**, keeping the prior
//!    image and the journal;
//! 3. append and sync `PublishLocalCheckpoint`;
//! 4. only then retire journal entries through `C`, synced;
//! 5. **reclaim superseded images**, retryable and idempotent.
//!
//! Two properties carry the crash matrix, and both are about *what a
//! crash can leave behind*:
//!
//! **An image directory exists only when it is complete.** Contents go
//! to a hidden pending directory, every file is synced, the directory
//! is synced, and only then is it renamed into place and the root
//! synced. A crash before the rename leaves a pending directory, which
//! nothing selects and [`LocalCheckpointStore::reclaim`] removes; a
//! crash after it leaves an image that loads.
//!
//! **A directory is never a selection.** What chooses recovery state is
//! the newest durable pointer in the journal, and this store answers
//! only about the image that pointer names. So a complete image whose
//! pointer never became durable is simply unselected -- the prior
//! publication stays authoritative -- and the newest directory by name,
//! by mtime or by sequence means nothing at all.
//!
//! An image the selected pointer names and that is missing, short, long
//! or corrupt is [`StoreError::Quarantine`]. It is never an empty
//! projection and never permission to initialize: the node knows it
//! published that image, so not finding it is damage, not absence.

use std::fmt;
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use coord_journal_api::frontier::{CheckpointPointerV1, LOCAL_CHECKPOINT_FORMAT_V1};
use coord_journal_api::record::RecordOrigin;
use coord_types::identity::Digest32;

use crate::local::{LocalCheckpointV1, LocalError, LocalManifestV1, verify_local};
use crate::manifest::{ArtifactError, ChunkV1, MAX_CHUNK_BYTES, MAX_MANIFEST_BYTES};

/// Name of the manifest inside an image directory.
const MANIFEST: &str = "manifest.bin";
/// Prefix of a chunk file inside an image directory.
const CHUNK: &str = "chunk-";
/// Prefix of a directory that is being written and is not an image.
const PENDING: &str = ".pending-";

/// Why a store operation stopped.
#[derive(Debug)]
pub enum StoreError {
    /// The filesystem failed. Nothing is assumed about what became
    /// durable.
    Io(io::Error),
    /// The artifact could not be encoded or decoded.
    Artifact(ArtifactError),
    /// The image the selected pointer names is damaged: missing, short,
    /// long, or not what the pointer says it is.
    ///
    /// This quarantines the scope. A node that published a pointer
    /// knows the image existed, so its absence is the loss of a durable
    /// prefix, not an empty store.
    Quarantine {
        /// What is wrong, in fixed words.
        reason: &'static str,
    },
    /// The image is present and does not verify.
    Invalid(LocalError),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreError::Io(e) => write!(f, "checkpoint store: {e}"),
            StoreError::Artifact(e) => write!(f, "checkpoint artifact: {e}"),
            StoreError::Quarantine { reason } => {
                write!(f, "the selected local checkpoint is damaged: {reason}")
            }
            StoreError::Invalid(e) => write!(f, "the selected local checkpoint is invalid: {e}"),
        }
    }
}

impl std::error::Error for StoreError {}

impl From<io::Error> for StoreError {
    fn from(e: io::Error) -> Self {
        StoreError::Io(e)
    }
}

impl From<ArtifactError> for StoreError {
    fn from(e: ArtifactError) -> Self {
        StoreError::Artifact(e)
    }
}

/// Sync one directory's own metadata, so a rename or creation inside it
/// survives. Opening a directory for reading is enough on the platforms
/// this build supports.
fn sync_dir(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

/// Write `bytes` to `path` and sync the file itself.
fn write_synced(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = File::create(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

fn read_bounded(path: &Path, bound: usize) -> Result<Vec<u8>, StoreError> {
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Err(StoreError::Quarantine {
                reason: "a file the manifest names is missing",
            });
        }
        Err(e) => return Err(StoreError::Io(e)),
    };
    let mut bytes = Vec::new();
    // One byte over the bound is read deliberately: a file that is too
    // long is damage to report, not a prefix to accept.
    Read::by_ref(&mut file)
        .take(bound as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > bound {
        return Err(StoreError::Quarantine {
            reason: "a file the manifest names is longer than the artifact allows",
        });
    }
    Ok(bytes)
}

fn chunk_name(ordinal: u32) -> String {
    format!("{CHUNK}{ordinal:05}.bin")
}

/// A node's local checkpoint directory.
#[derive(Clone, Debug)]
pub struct LocalCheckpointStore {
    root: PathBuf,
}

impl LocalCheckpointStore {
    /// Open (creating) the directory `root`, syncing its creation.
    pub fn open(root: &Path) -> Result<Self, StoreError> {
        fs::create_dir_all(root)?;
        if let Some(parent) = root.parent() {
            sync_dir(parent)?;
        }
        Ok(LocalCheckpointStore {
            root: root.to_path_buf(),
        })
    }

    /// Where the images live.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The directory one image lives in. Named by its own identity, so
    /// writing the same image twice writes the same place and a name
    /// can never stand for different bytes.
    fn image_dir(&self, checkpoint_id: &Digest32) -> PathBuf {
        self.root.join(hex(checkpoint_id))
    }

    /// Step 2: write `checkpoint` as a complete inactive image and
    /// return the pointer that would select it.
    ///
    /// Nothing is selected by this. The returned pointer is what the
    /// caller appends to the journal next; until that record is durable
    /// the image is bytes on disk that recovery ignores.
    ///
    /// Idempotent: writing an image that is already present rewrites it
    /// under the same name, because the name is the image's own
    /// identity.
    pub fn write(&self, checkpoint: &LocalCheckpointV1) -> Result<CheckpointPointerV1, StoreError> {
        verify_local(checkpoint).map_err(StoreError::Invalid)?;
        let manifest = checkpoint.manifest.encode()?;
        let checkpoint_id = checkpoint.manifest.root;
        let pending = self.root.join(format!("{PENDING}{}", hex(&checkpoint_id)));
        // A leftover pending directory of an interrupted attempt is not
        // evidence of anything; it is replaced.
        if pending.exists() {
            fs::remove_dir_all(&pending)?;
        }
        fs::create_dir(&pending)?;
        for chunk in &checkpoint.chunks {
            let encoded = chunk.encode()?;
            write_synced(&pending.join(chunk_name(chunk.ordinal)), &encoded)?;
        }
        write_synced(&pending.join(MANIFEST), &manifest)?;
        // Every file is durable before the directory is: a directory
        // that appears with a file whose bytes are not there yet is
        // exactly the "complete image" that is not one.
        sync_dir(&pending)?;
        let image = self.image_dir(&checkpoint_id);
        if image.exists() {
            fs::remove_dir_all(&image)?;
        }
        fs::rename(&pending, &image)?;
        sync_dir(&self.root)?;
        Ok(CheckpointPointerV1 {
            origin: checkpoint.manifest.origin,
            represented: checkpoint.manifest.represented,
            format: LOCAL_CHECKPOINT_FORMAT_V1,
            manifest_digest: checkpoint.manifest.digest()?,
            checkpoint_id,
        })
    }

    /// Load and verify the image `pointer` selects.
    ///
    /// Everything the pointer says is checked against the image: the
    /// manifest's digest, the root, the origin and the represented
    /// sequence. An image that is missing or disagrees quarantines; it
    /// never degrades to "start from nothing".
    pub fn load(&self, pointer: &CheckpointPointerV1) -> Result<LocalCheckpointV1, StoreError> {
        if pointer.format != LOCAL_CHECKPOINT_FORMAT_V1 {
            return Err(StoreError::Quarantine {
                reason: "the selected pointer names a checkpoint format this build does not read",
            });
        }
        let dir = self.image_dir(&pointer.checkpoint_id);
        if !dir.is_dir() {
            return Err(StoreError::Quarantine {
                reason: "the image the selected pointer names is not present",
            });
        }
        let encoded = read_bounded(&dir.join(MANIFEST), MAX_MANIFEST_BYTES)?;
        let digest = crate::local::manifest_digest(&encoded);
        if digest != pointer.manifest_digest {
            return Err(StoreError::Quarantine {
                reason: "the manifest is not the one the selected pointer names",
            });
        }
        let manifest = LocalManifestV1::decode(&encoded)?;
        if manifest.root != pointer.checkpoint_id
            || manifest.origin != pointer.origin
            || manifest.represented != pointer.represented
        {
            return Err(StoreError::Quarantine {
                reason: "the manifest disagrees with the selected pointer",
            });
        }
        let mut chunks = Vec::with_capacity(manifest.chunks.len());
        for descriptor in &manifest.chunks {
            let bytes = read_bounded(&dir.join(chunk_name(descriptor.ordinal)), MAX_CHUNK_BYTES)?;
            chunks.push(ChunkV1::decode(&bytes)?);
        }
        let checkpoint = LocalCheckpointV1 { manifest, chunks };
        verify_local(&checkpoint).map_err(StoreError::Invalid)?;
        Ok(checkpoint)
    }

    /// Step 5: remove every image but the one `keep` names, and every
    /// pending directory.
    ///
    /// Retryable and idempotent, and deliberately unable to remove the
    /// published baseline: the one thing it never deletes is the image
    /// the selected pointer names. Returns how many directories were
    /// removed.
    pub fn reclaim(&self, keep: &CheckpointPointerV1) -> Result<usize, StoreError> {
        let survivor = hex(&keep.checkpoint_id);
        let mut removed = 0;
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if name == survivor || !entry.file_type()?.is_dir() {
                continue;
            }
            if !name.starts_with(PENDING) && !is_image_name(name) {
                // Not this store's to delete.
                continue;
            }
            fs::remove_dir_all(entry.path())?;
            removed += 1;
        }
        if removed > 0 {
            sync_dir(&self.root)?;
        }
        Ok(removed)
    }

    /// Image identities present on disk, for diagnostics.
    ///
    /// Never an input to recovery: the durable pointer selects, and a
    /// listing cannot. It exists so an operator can see what is taking
    /// space, and so a test can say which directories a crash left.
    pub fn images(&self) -> Result<Vec<Digest32>, StoreError> {
        let mut out = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if !entry.file_type()?.is_dir() || !is_image_name(name) {
                continue;
            }
            if let Some(id) = unhex(name) {
                out.push(id);
            }
        }
        out.sort_by_key(|d| d.0);
        Ok(out)
    }
}

/// Whether `name` is an image directory's name: 64 lowercase hex digits.
fn is_image_name(name: &str) -> bool {
    name.len() == 64
        && name
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn hex(digest: &Digest32) -> String {
    let mut out = String::with_capacity(64);
    for byte in digest.0 {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn unhex(name: &str) -> Option<Digest32> {
    if !is_image_name(name) {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, chunk) in name.as_bytes().chunks(2).enumerate() {
        out[i] = u8::from_str_radix(std::str::from_utf8(chunk).ok()?, 16).ok()?;
    }
    Some(Digest32(out))
}

/// The origin a store belongs to, for callers that keep one directory
/// per incarnation.
pub fn image_belongs_to(manifest: &LocalManifestV1, origin: &RecordOrigin) -> bool {
    manifest.origin == *origin
}

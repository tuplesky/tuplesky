//! Generation manifest: identity and format, checksummed.

use std::path::Path;

use coord_store_api::registry::Collection;
use coord_types::ids::{ClusterId, DomainId, ReplicaId, ReplicaIncarnation};
use serde::{Deserialize, Serialize};

/// Current manifest format.
pub const MANIFEST_FORMAT: u32 = 1;
/// Engine name recorded for this adapter.
pub const ENGINE_NAME: &str = "redb";
/// Durability profile of this reference adapter.
pub const PROFILE_NAME: &str = "strict-single-store-v1";

/// Manifest of one generation directory.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreManifestV1 {
    /// Manifest format version.
    pub format: u32,
    /// Engine name.
    pub engine: String,
    /// Durability profile.
    pub profile: String,
    /// Cluster/restore identity.
    pub cluster_id: ClusterId,
    /// Domain.
    pub domain_id: DomainId,
    /// Replica.
    pub replica_id: ReplicaId,
    /// Replica incarnation.
    pub incarnation: ReplicaIncarnation,
    /// Generation number within the root.
    pub generation: u64,
    /// Registered collections `(id, name)` at creation.
    pub collections: Vec<(u16, String)>,
}

impl StoreManifestV1 {
    /// Collections snapshot from the frozen registry.
    pub fn registry_snapshot() -> Vec<(u16, String)> {
        Collection::ALL
            .iter()
            .map(|c| (c.id().0, c.name().to_owned()))
            .collect()
    }

    /// Encode as postcard followed by a 32-byte BLAKE3 digest of the body.
    pub fn encode(&self) -> Vec<u8> {
        let mut body = postcard::to_allocvec(self).expect("manifest encodes");
        let digest = blake3::keyed_hash(&MANIFEST_KEY, &body);
        body.extend_from_slice(digest.as_bytes());
        body
    }

    /// Decode and verify the digest; any deviation is corruption.
    pub fn decode(bytes: &[u8]) -> Result<Self, ManifestError> {
        if bytes.len() < 32 {
            return Err(ManifestError::Corrupt("manifest shorter than its digest"));
        }
        let (body, digest) = bytes.split_at(bytes.len() - 32);
        if blake3::keyed_hash(&MANIFEST_KEY, body).as_bytes() != digest {
            return Err(ManifestError::Corrupt("manifest digest mismatch"));
        }
        let (manifest, rest): (StoreManifestV1, &[u8]) = postcard::take_from_bytes(body)
            .map_err(|_| ManifestError::Corrupt("manifest body undecodable"))?;
        if !rest.is_empty() {
            return Err(ManifestError::Corrupt("trailing bytes in manifest body"));
        }
        if manifest.format != MANIFEST_FORMAT {
            return Err(ManifestError::UnsupportedFormat(manifest.format));
        }
        Ok(manifest)
    }

    /// Write atomically: temp file, fsync, rename, directory fsync.
    pub fn write(&self, path: &Path) -> std::io::Result<()> {
        let tmp = path.with_extension("tmp");
        {
            let mut f = std::fs::File::create(&tmp)?;
            use std::io::Write as _;
            f.write_all(&self.encode())?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, path)?;
        if let Some(dir) = path.parent() {
            std::fs::File::open(dir)?.sync_all()?;
        }
        Ok(())
    }

    /// Read and verify.
    pub fn read(path: &Path) -> Result<Self, ManifestError> {
        let bytes = std::fs::read(path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                ManifestError::Missing
            } else {
                ManifestError::Io(e.to_string())
            }
        })?;
        Self::decode(&bytes)
    }
}

/// Domain-separation key for the manifest digest (not a secret).
const MANIFEST_KEY: [u8; 32] = *b"tuplesky store manifest v1 key!!";

/// Manifest failures.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ManifestError {
    /// File absent.
    Missing,
    /// Undecodable or digest mismatch.
    Corrupt(&'static str),
    /// Newer or unknown format.
    UnsupportedFormat(u32),
    /// I/O failure.
    Io(String),
}

impl std::fmt::Display for ManifestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ManifestError::Missing => f.write_str("manifest missing"),
            ManifestError::Corrupt(why) => write!(f, "manifest corrupt: {why}"),
            ManifestError::UnsupportedFormat(v) => write!(f, "unsupported manifest format {v}"),
            ManifestError::Io(e) => write!(f, "manifest io: {e}"),
        }
    }
}

impl std::error::Error for ManifestError {}

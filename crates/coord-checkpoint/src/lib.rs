//! `SharedCheckpointV1`: the canonical, portable snapshot of a domain's
//! common logical state (task-49; design Sections 5.3, 17.6, 17.12,
//! 17.16.1).
//!
//! * [`manifest`]: the frozen artifact. A [`manifest::SharedManifestV1`]
//!   binds cluster, domain, configuration epoch and the closed boundary
//!   (execution position, KV revision, retention floor, lease authority),
//!   one summary per common collection and one descriptor per chunk; its
//!   root is a domain-separated BLAKE3 digest of exactly those fields.
//!   Chunks ([`manifest::ChunkV1`]) hold rows in canonical order and are
//!   hashed by their encoded bytes. Two raw kinds of the snapshot range
//!   carry manifest and chunks; the manifest is carried whole in one
//!   frame, so its encoded size is bounded
//!   ([`manifest::MAX_MANIFEST_BYTES`]) and export and verification both
//!   hold it to that bound.
//! * [`export`]: [`export::export_shared`] traverses one pinned
//!   multi-collection view (any `OrderedRead`) through bounded pages, in
//!   registry order and unsigned key order, and includes only common state
//!   at the boundary: node-private collections (`meta_v1`, `protocol_v1`,
//!   `checkpoint_v1`) are never read into the digest, payloads of
//!   unexecuted commands are skipped, history and events are normalized
//!   to the replicated retention floor so local garbage-collection
//!   progress cannot change the digest, and rows beyond the boundary are
//!   an error. The applied stamp is re-read between collections so a view
//!   that changes under the export is refused rather than mixed.
//! * [`verify`]: [`verify::verify_shared`] recomputes every chunk digest and
//!   the root and checks order, uniqueness, counts, bounds (the manifest's
//!   own encoded size among them) and descriptors, so an importer
//!   (task-50) trusts bytes only after this.
//! * [`install`] (task-50; Sections 10.3, 17.6, 17.13): the importing half.
//!   A [`install::ChunkSet`] collects chunks against their descriptors so a
//!   missing or corrupt one blocks the install, and
//!   [`install::install_shared`] writes the verified rows into the engine of
//!   an inactive generation after origin, configuration, schema, identity
//!   and emptiness checks, closing with the boundary of `meta_v1` and the
//!   [`install::InstalledCheckpointV1`] receipt. It writes no identity and
//!   no `protocol_v1` row, so a learner inherits neither the donor's
//!   identity nor any vote, and it never selects anything: the physical
//!   generation lifecycle (`coord-storage-redb`) does that afterwards.
//!
//! This is not `LocalRecoveryCheckpointV1` (task-j04): it carries no
//! promises, votes, stamps, journal sequences or physical files, gives a
//! learner no identity or authority, and authorizes no trimming.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod export;
pub mod install;
pub mod manifest;
pub mod verify;

pub use export::{CheckpointOrigin, ExportError, ExportLimits, export_shared};
pub use install::{
    ChunkSet, InstallError, InstallLimits, InstallRequirements, Installed, InstalledCheckpointV1,
    install_shared, installed_baseline,
};
pub use manifest::{
    CheckpointBoundary, ChunkDescriptorV1, ChunkV1, CollectionSummaryV1, MAX_CHUNKS,
    MAX_MANIFEST_BYTES, RowV1, SHARED_CHECKPOINT_FORMAT_V1, SharedCheckpointV1, SharedManifestV1,
};
pub use verify::{VerifyError, verify_shared};

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "production";

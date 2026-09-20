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
//! * [`trim`] (task-51; Sections 5.3, 17.5-17.6, 17.16.5): the conservative
//!   reference increment of semantic forgetting. A [`trim::CheckpointAckV1`]
//!   per voter records durable possession of one checkpoint;
//!   [`trim::establish_floor`] turns those into a [`trim::TrimFloor`] only
//!   when every configured voter acknowledged the identical
//!   `(configuration, boundary, root)`, and an acknowledgement from a
//!   replica outside the voter set is refused, so observers supply no trim
//!   votes. [`trim::publish_floor_in`] makes the floor durable, re-checked
//!   inside the write so it never lowers, [`trim::TrimFence`] answers
//!   delayed below-floor traffic from retained common state instead of
//!   re-creating protocol rows, and [`trim::plan_trim`] refuses to plan
//!   until that floor is durable and then emits bounded batches of
//!   `protocol_v1` deletions for commands executed at or below the floor,
//!   never a promise, a bound Sync, an unresolved obligation or a row a
//!   retained command depends on. A missing voter stops trimming and
//!   [`trim::trim_backpressure`] reports the retained rows against the bound
//!   the caller refuses new work at; nothing is ever evicted.
//!
//! This is not `LocalRecoveryCheckpointV1` (task-j04): it carries no
//! promises, votes, stamps, journal sequences or physical files, gives a
//! learner no identity or authority, and by itself authorizes no trimming:
//! only the all-voter floor of [`trim`] does that.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod export;
pub mod floor;
pub mod handoff;
pub mod install;
pub mod local;
pub mod maintain;
pub mod manifest;
pub mod store;
pub mod trim;
pub mod verify;

pub use export::{CheckpointOrigin, ExportError, ExportLimits, export_shared};
pub use local::{
    InstallLocalError, InstallLocalLimits, LocalCheckpointV1, LocalError, LocalLimits,
    LocalManifestV1, LocalPin, export_local, install_local, verify_local,
};

pub use floor::{
    ActivatedFloorV1, CheckpointReadinessV1, RecoveryObligation, RecoveryReport, activate_floor,
    publish_activation, read_readiness, record_readiness, recovery_obligation,
};
pub use handoff::{
    TerminalCertificateV1, TerminalStateV1, publish_certificate, published_certificate,
    select_certificate,
};
pub use maintain::{BaselineError, LocalBaseline, Publication};
pub use store::{LocalCheckpointStore, StoreError};

pub use install::{
    ChunkSet, InstallError, InstallLimits, InstallRequirements, Installed, InstalledCheckpointV1,
    SelectError, install_shared, installed_baseline, select_installed,
};
pub use manifest::{
    CheckpointBoundary, ChunkDescriptorV1, ChunkV1, CollectionSummaryV1, MAX_CHUNKS,
    MAX_MANIFEST_BYTES, RowV1, SHARED_CHECKPOINT_FORMAT_V1, SharedCheckpointV1, SharedManifestV1,
};
pub use trim::{
    CheckpointAckV1, FenceDecision, TrimBackpressure, TrimError, TrimFence, TrimFloor, TrimLimits,
    TrimPlan, TrimmedFloorV1, ack_key, ack_update, establish_floor, plan_trim, publish_floor,
    publish_floor_in, published_floor, read_acks, trim_backpressure,
};
pub use verify::{VerifyError, verify_shared};

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "production";

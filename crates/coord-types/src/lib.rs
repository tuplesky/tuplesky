//! Shared identities, canonical commands and ordered-key encoding (task-02).
//!
//! This crate is the frozen vocabulary every other TupleSky crate speaks:
//!
//! * [`ids`]: fixed 16-byte identifiers and checked, non-recycling counters.
//!   Each counter is a distinct type, so a local journal sequence can never be
//!   passed where a KV revision, execution position, epoch or ballot belongs
//!   (design Sections 2.3 and 10.5.1).
//! * [`logical_v1`]: the versioned canonical operation schema whose postcard
//!   encoding is hashed into command identity (Section 4.4). Field and variant
//!   order are frozen; see `spec/logical-v1.md`.
//! * [`identity`]: the stable invocation identity (`RetryKey`), the
//!   domain-separated BLAKE3 `CommandId`, admission context that is explicitly
//!   *excluded* from identity, and the conflict rule for payload changes under
//!   one retry key.
//! * [`ordered_key`]: the reviewed ordered-key encoder for namespace/key/
//!   revision rows (Section 17.2) with property-tested byte ordering.
//! * [`wire_v1`]: bounded frames and stable transport DTOs (task-03,
//!   Sections 11.2 and 19.1), decoded only within declared limits.
//! * [`config_v1`]: the authoritative configuration records
//!   (`GroupConfigurationV1`, `BallotConfigurationV1`), authenticated hints,
//!   endpoint and observer catalogs and the bootstrap, subscription and
//!   observer-discovery frames of the configuration kind range (task-m01,
//!   Section 10.5). Verification of the chain lives in `coord-membership`.
//!
//! The crate performs no I/O, reads no clocks and generates no randomness. It
//! is `no_std` + `alloc` so the compile boundary in `cargo xtask check-deps`
//! can keep runtimes and engines out of core crates.
#![forbid(unsafe_code)]
#![no_std]
#![warn(missing_docs)]
extern crate alloc;

pub mod config_v1;
pub mod error;
pub mod identity;
pub mod ids;
pub mod logical_v1;
pub mod ordered_key;
pub mod wire_v1;

pub use error::{CounterOverflow, DecodeError, IdentityError, ValidationError};
pub use identity::{AdmissionContext, CommandId, Digest32, HashDomain, RetryKey};
pub use ids::{
    Ballot, CatalogGeneration, ClientInstanceId, ClusterId, ConfigurationEpoch, DomainId,
    EndpointGeneration, ExecutionPosition, FinalizedFrameSeq, KvRevision, LeaseGeneration, LeaseId,
    LocalJournalSeq, NamespaceId, ReadFenceId, ReplicaId, ReplicaIncarnation, RequestSequence,
    SessionId,
};

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "core";

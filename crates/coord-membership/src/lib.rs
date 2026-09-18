//! Signed genesis, durable initialization and the peer TLS identity
//! binder (task-42; design Sections 10, 10.5, 17.1, 20.4).
//!
//! * [`genesis`]: the signed, immutable genesis manifest binding cluster
//!   and domain, the exact initial voters (node identity, key generation
//!   and role), the issuer trust anchors, the admin principal and the
//!   protocol version. It is delivered through deployment trust and
//!   verified against a pinned signing key; no open enrollment, no
//!   first-request admin, no trust-on-first-use peer discovery.
//! * [`membership`]: the committed configuration the manifest initializes
//!   and later handoffs (task-54+) extend: for one epoch, the exact voter
//!   incarnations and roles. It answers whether a bound peer is a voter
//!   at the current generation.
//! * [`init`]: durable initialization against a [`init::GenesisStore`].
//!   A first boot pins the manifest digest and the committed config; a
//!   later boot requires the stored digest to match. A missing, empty or
//!   rolled-back store is quarantined and requires a new generation
//!   through the handoff lifecycle, never a silent create-or-open of an
//!   empty voter.
//! * [`binder`]: the production [`coord_transport::IdentityBinder`]. After
//!   ordinary TLS validation, it reads the node-identity URI SAN from the
//!   peer's certificate (issued by task-41), checks cluster, domain and
//!   role, and for a voter requires the exact committed incarnation: a
//!   stale generation, a wrong origin, or a frontend, observer or learner
//!   never binds as a voter. Cloned identities carry the same
//!   incarnation and are deduplicated by consensus, never counted twice.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod binder;
pub mod genesis;
pub mod init;
pub mod membership;

pub use binder::PeerBinder;
pub use genesis::{
    GenesisError, GenesisManifest, SignedGenesis, VoterSeed, sign_genesis, verify_genesis,
};
pub use init::{GenesisStore, InitError, Initialized, initialize};
pub use membership::{Membership, MembershipError, VoterEntry};

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "production";

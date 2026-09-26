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
//! * [`configuration`] (task-m01): verification of the configuration
//!   chain (`GroupConfigurationV1` records from the trusted genesis, each
//!   epoch approved by a majority of the previous epoch's exact voters
//!   under the keys that epoch recorded), ballot configurations under the
//!   source quorum rules, voter-attested endpoint and observer catalogs,
//!   and the monotonically installed client view driven by authenticated
//!   hints and bootstrap responses. No directory, controller or larger
//!   epoch number advances the chain; historical epochs verify without a
//!   live issuer.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod binder;
pub mod configuration;
pub mod genesis;
pub mod init;
pub mod membership;

pub use binder::PeerBinder;
pub use configuration::{
    BallotError, BootstrapOutcome, CatalogError, ChainError, ClientConfiguration,
    ConfigurationChain, EvidenceError, GenesisAnchor, HintDecision, Installed, SignError,
    VerifiedConfiguration, VoterAuthority, sign_message, verify_endpoint_catalog, verify_signature,
};
pub use genesis::{
    GenesisError, GenesisManifest, PROTOCOL_VERSION, SignedGenesis, VoterSeed, admin_key_from_pem,
    sign_genesis, sign_genesis_pem, verify_genesis,
};
pub use init::{GenesisStore, InitError, Initialized, initialize};
pub use membership::{CredentialChange, Membership, MembershipError, VoterEntry};

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "production";

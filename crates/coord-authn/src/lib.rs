//! Hardened external JWT and issuer verification (task-35; design
//! Sections 9.1-9.4, 20.1-20.2).
//!
//! Everything that decides admission is deterministic and sans-I/O: the
//! verifiers take a token, the injected clock health and the key cache
//! state, and answer with a verified identity, a denial, or a request to
//! fetch a *configured* key set. Only [`http::HardenedFetcher`] performs
//! I/O, against configured JWKS endpoints alone.
//!
//! * [`config`]: issuer configuration: exact issuer string, one JWKS
//!   endpoint, an explicit set of asymmetric algorithms, exact
//!   audiences and a maximum token age. Symmetric algorithms are refused
//!   at configuration time.
//! * [`clock`]: clock health and uncertainty intervals; validity is
//!   judged conservatively and an unhealthy clock denies admission.
//! * [`jwks`]: bounded key caches per issuer with a freshness limit, a
//!   staleness limit after which cached keys deny, and a refresh budget
//!   so unknown key identifiers cannot flood the issuer.
//! * [`verifier`]: two distinct verifier types over one registry:
//!   [`verifier::OidcVerifier`] for human ID tokens (client audience,
//!   `azp`, nonce) and [`verifier::WifVerifier`] for workload assertions
//!   (Kubernetes projected service-account tokens in offline or explicit
//!   TokenReview mode; GitHub Actions with immutable repository and owner
//!   identifiers). Token-directed key locations (`jku`, `x5u`, `jwk`,
//!   `x5c`) are never dereferenced; an unverified `iss` only selects a
//!   configured namespace.
//! * [`receipt`]: trust rules map a verified identity to a principal and
//!   scope ceiling, deny by default, and mint the canonical admission
//!   receipt; claims never grant permission directly and the raw token is
//!   never recorded.
//! * [`log`]: the bounded log of admitted receipts.
//!
//! Nothing here signs tokens (task-36) or accepts arbitrary cloud identity
//! formats: unsupported assertions are refused, not approximated.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod clock;
pub mod config;
pub mod http;
pub mod jwks;
pub mod log;
pub mod receipt;
pub mod verifier;

pub use clock::{ClockHealth, TimeError};
pub use config::{ConfigError, IssuerConfig};
pub use http::{FetchConfig, FetchError, HardenedFetcher};
pub use jwks::{JwksError, JwksLimits, KeyCache, KeyLookup};
pub use log::{AdmissionLog, AdmittedRecord};
pub use receipt::{Admitted, MintError, SubjectKind, TrustRuleConfig, mint};
pub use verifier::{
    Decision, KubernetesMode, OidcVerifier, Registry, TokenReview, VerifiedIdentity, VerifyError,
    WifVerifier, Workload, WorkloadKind,
};

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "production";

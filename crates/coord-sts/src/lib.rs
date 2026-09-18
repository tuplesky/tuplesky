//! RFC 8693 token exchange and service credential signing (task-36;
//! design Sections 9.1-9.4, 20.1-20.2).
//!
//! A workload presents a short-lived external assertion; the STS verifies
//! it with the hardened verifiers of `coord-authn`, maps it through trust
//! rules to a canonical admission receipt, submits the receipt as the
//! replicated `ConsumeAdmission` command (which rechecks current policy
//! and consumes the receipt exactly once), and only then signs a
//! cluster-specific ES256 service token bound to the created session.
//!
//! * [`keys`]: the process-local key ring: PKCS#8 signing keys loaded
//!   from outside replicated state, rotation with overlap, and the public
//!   JWKS. Private material is never serialized or printed.
//! * [`token`]: service-token claims, signing and verification, and the
//!   scope vocabulary.
//! * [`exchange`]: the exchange itself, sans-I/O: request validation,
//!   verification, receipt minting, session creation through the
//!   [`exchange::SessionCreator`] port, lifetime and scope ceilings, and
//!   RFC 6749 error mapping.
//! * [`http`]: the bounded Axum handlers (`POST /token`,
//!   `GET /.well-known/jwks.json`): body, concurrency and time bounds,
//!   key refresh through the hardened fetcher, injected clock and
//!   entropy.
//!
//! Workloads get no refresh secret: they exchange again with a fresh
//! assertion. No IdP is called per operation: keys are cached and the
//! service token is verified locally by API endpoints (task-37).
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod exchange;
pub mod http;
pub mod keys;
pub mod token;

pub use exchange::{
    CreatorError, ExchangeError, ExchangeForm, ExchangeResponse, GRANT_TYPE, SessionCreator, Sts,
    StsConfig, TOKEN_TYPE_ACCESS, TOKEN_TYPE_ID, TOKEN_TYPE_JWT,
};
pub use http::{
    AppState, ClockSource, EntropySource, HttpLimits, ProviderEntropy, SystemClock, TokenReviewer,
    router,
};
pub use keys::{KeyError, KeyRing, SigningKey};
pub use token::{
    ServiceClaims, TokenError, action_bit, scope_bits, scope_string, verify_service_token,
};

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "production";

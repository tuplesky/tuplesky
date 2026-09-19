//! OIDC browser login and the service code flow (task-38; design
//! Sections 8.1, 8.2, 20.1, 20.3).
//!
//! The broker is an upstream OpenID Connect relying party and a narrow
//! service authorization server for public clients (the CLI). Two flows
//! with distinct identities run back to back:
//!
//! * the **service** leg between the CLI and the broker: the CLI starts a
//!   login with its own PKCE S256 challenge, an exact registered
//!   loopback redirect and a state; the broker later issues a one-time
//!   *service code* that only that verifier, client and redirect can
//!   redeem, and the replicated state consumes the code's commitment
//!   exactly once when it creates the session;
//! * the **upstream** leg between the broker and the configured IdP:
//!   openidconnect performs discovery, the authorization request with
//!   the broker's own PKCE, state and nonce, the code exchange and the
//!   ID token verification; the application checks `azp` and the
//!   multi-audience rule and keeps the principal as `(issuer, subject)`,
//!   never an email.
//!
//! Device authorization ([`device`]) reuses the same upstream login from
//! the verification page: the CLI polls with a secret device code within
//! bounded intervals and attempts, the user code alone grants nothing,
//! and an approved grant is taken by exactly one poll.
//!
//! Sessions from these logins carry a refresh family ([`refresh`]):
//! rotating secret commitments in replicated state, reuse of a retired
//! secret revoking the family and retiring the session, and no recovery
//! of a lost rotation response other than a fresh login.
//!
//! Upstream codes are never reused as service codes; pending logins are
//! bounded broker-local state (a restart forces a fresh login); every
//! secret is redacted from diagnostics.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod device;
pub mod http;
pub mod refresh;
pub mod service;
pub mod upstream;

pub use device::{
    DeviceAuthorization, DeviceDisplay, DeviceError, DeviceLimits, DeviceLogin, Poll,
    normalize_user_code, user_code,
};
pub use refresh::{
    RefreshError, SessionBackend, SessionReader, logout, new_family, parse_refresh_token, refresh,
    refresh_token, secret_commitment,
};
pub use service::{
    Approved, LoginError, LoginLimits, RedeemRequest, Redeemed, Registration, ServiceLogin,
    StartRequest, Started, UpstreamIdentity, azp_policy,
};
pub use upstream::{
    BoundedHttpClient, HttpError, MAX_UPSTREAM_BODY_BYTES, Upstream, UpstreamConfig, UpstreamError,
    hardened_http_client,
};

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "production";

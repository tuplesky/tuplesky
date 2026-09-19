//! `coordctl`: the operator CLI's login lifecycle (task-40; design
//! Sections 8.1, 8.2, 20.3).
//!
//! * [`credentials`]: what a login yields (session, short-lived access
//!   token, refresh token), redacted from every diagnostic.
//! * [`store`]: explicit secure stores only: the platform keyring
//!   (Apple keychain behind the `keychain` feature, Secret Service
//!   behind `secret-service`) or memory for one process. There is no
//!   plaintext file store and no fallback to one. Updates of a shared
//!   credential are serialized through a process lock and an advisory
//!   file lock so concurrent CLI processes never race a rotation.
//! * [`client`]: browser login through a loopback listener bound to
//!   127.0.0.1 that accepts only the expected callback, device login
//!   with bounded polling honouring `slow_down`, refresh, and logout.
//!   A refresh refused as reuse means the family was revoked: the CLI
//!   reports that a fresh login is required and never invents recovery.
//!
//! Tokens are never accepted as arguments or written to logs. Headless
//! automation uses workload identity federation, not desktop refresh.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod client;
pub mod credentials;
pub mod store;

pub use client::{BrokerClient, CliError, DeviceStart, PollOutcome, TokenResponse};
pub use credentials::Credentials;
pub use store::{
    CredentialStore, KeyringStore, MemoryStore, StoreError, StoreKind, UpdateGuard, UpdateLock,
    begin_update, open_store, update,
};

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "production";

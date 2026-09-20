//! Provision and run a real TupleSky domain outside `cargo test`
//! (task-48, task-62; design Sections 3.1, 6.8.4, 22.3, 23 G4).
//!
//! The integration tests build their fixtures inside the test binary,
//! which is right for them and useless to anything else: a Kubernetes
//! certification run needs a domain that outlives one test process, that
//! an API server and a Go Kine build can reach, and that a benchmark can
//! drive for minutes. This crate is that domain.
//!
//! * [`pki`] issues the fixture authorities and the credentials under
//!   them -- node, collector, issuer and the two separate authorities of
//!   the Kubernetes storage edge.
//! * [`domain`] writes one provisioned domain into a directory: genesis,
//!   signed endpoint catalog, published issuer keys and a strict
//!   `coordd.toml` per node, described by a single `harness.json`.
//! * [`issuer`] answers the credential exchange a workload performs. It
//!   is explicitly not an identity provider; its own documentation says
//!   what it does instead and why that is the right boundary for what
//!   these runs measure.
//! * [`run`] initializes each node's first generation and starts every
//!   committed voter, waiting until each one actually serves.
//!
//! Nothing here weakens what it starts. The daemons run the production
//! startup checks against this material and refuse it the moment it
//! stops agreeing with itself, which is how a harness bug shows up as a
//! harness bug instead of as a result.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod domain;
pub mod issuer;
pub mod pki;
pub mod run;

pub use domain::{Edge, Issuer, Node, Plan, Provisioned, provision};
pub use run::{Daemon, RunError, initialize, start_all};

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "test-only";

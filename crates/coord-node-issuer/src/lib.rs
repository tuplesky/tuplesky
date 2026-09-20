//! The independent WIF node issuer (task-41; design Sections 10.1, 10.2,
//! 20.4).
//!
//! The issuer is deployable before any voter exists: a workload proves
//! its platform identity with a WIF assertion (verified by the same
//! `coord-authn` verifiers), proves possession of a freshly generated
//! node key with a CSR, and receives a short-lived certificate whose
//! subject, SANs, extended key usage and constraints are built from the
//! matched role policy, never copied from the CSR. Issuing a certificate
//! is not enrolling a voter: the certificate binds cluster, node,
//! generation and role, but committed membership and the current key
//! generation decide votes (task-42).
//!
//! * [`ca`]: the protected reference CA: a PKCS#8 signing key and its
//!   certificate, validated at startup (the certificate is a CA that can
//!   sign, its key matches, its constraints are sane).
//! * [`policy`]: role policies that map a verified workload identity to a
//!   node identity and the certificate fields it may hold.
//! * [`issuer`]: CSR possession, request validation (no CA requests, no
//!   unauthorized names, bounded lifetime, allowed algorithms) and
//!   policy-built issuance.
//! * [`identity`]: the node identity SAN encoding (`tuplesky:` URI) that
//!   the transport binder reads.
//! * [`http`]: the bounded signer endpoint on its own narrow port.
//! * [`lifecycle`] (task-58): when a holder renews, how long a replaced
//!   leaf stays accepted, and what a warm session's deadline is. An
//!   expired leaf has no "serve anyway" outcome, the overlap during a
//!   rotation is bounded at both ends, and a renewal never extends a
//!   session bound under the leaf it replaced.
//!
//! Root credentials never enter quorum data; an HSM or KMS signer can
//! replace the reference CA without membership changes.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod ca;
pub mod http;
pub mod identity;
pub mod issuer;
pub mod lifecycle;
pub mod policy;

pub use ca::{Ca, CaError};
pub use http::{IssuerState, SignClock, SystemSignClock, router};
pub use identity::{NodeIdentity, node_uri, parse_node_uri};
pub use issuer::{IssueError, Issued, NodeIssuer, NodeRequest};
pub use lifecycle::{Leaf, Renewal, RenewalPolicy};
pub use policy::{NodePolicy, PolicyError, RolePolicy};

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "production";

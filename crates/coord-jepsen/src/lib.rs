//! A Jepsen client for TupleSky: one bound native session, driven by
//! JSON lines (design Sections 12.3, 21.5).
//!
//! Jepsen's own etcd test cannot be pointed at TupleSky: the Kubernetes
//! storage edge serves only the transaction shapes Kubernetes sends, so
//! every workload but a watch is refused there. This drives the native
//! API instead, through the SDK, the way any Rust client would, and
//! reports each operation in the only terms a Jepsen checker trusts:
//! `ok`, `fail` (it had no effect) or `info` (it may have).
//!
//! * [`ops`]: requests built from operations, and answers read back as
//!   verdicts. Pure.
//! * [`session`]: the connection, the binding and the SDK, resolving an
//!   answer that did not arrive before calling it unknown.
//! * [`protocol`]: the line protocol the Clojure client speaks.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod ops;
pub mod protocol;
pub mod session;

pub use ops::{Answer, Codec, Verdict};
pub use session::{OpenError, Session, Timing};

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "test-only";

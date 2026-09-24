//! Native Rust SDK request lifecycle (task-34; design Sections 4.4, 6.5,
//! 11.5, 19.4).
//!
//! The SDK is a sans-I/O state machine: the application (or the transport
//! composition of task-43) opens connections, writes the frames the SDK
//! hands out and feeds back what arrives. Everything that matters for
//! correctness is decided here, deterministically:
//!
//! * [`credential`]: credential providers behind a cache with expiry. A
//!   credential is presented once per connection binding; ordinary
//!   operations never trigger another exchange.
//! * [`identity`]: the stable invocation identity. One client instance
//!   identity per process, a monotonic request sequence, and the retry
//!   key plus canonical payload allocated once per invocation and reused
//!   verbatim on every retry. A retry with another payload under the same
//!   sequence is refused locally as a payload conflict; there is no
//!   implicit fresh identity.
//! * [`pool`]: a bounded warm pool: connections and streams per connection
//!   are capped; beyond the cap a request is refused with a typed error
//!   rather than opening more.
//! * [`lifecycle`]: the request state machine. A reset or reconnect
//!   re-sends the same invocation on another bound connection; a deadline
//!   makes the outcome *unknown*, never failed, with a `ResolveRequest`
//!   the application can send later; responses map to typed results and
//!   retry errors.
//!
//! The SDK understands only the typed API kinds of `wire_v1`. Protocol
//! evidence and collector frames are never decoded or exposed: an
//! evidence frame arriving at a client is a protocol violation.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod credential;
pub mod identity;
pub mod lifecycle;
pub mod pool;

pub use credential::{
    CachedProvider, Credential, CredentialError, CredentialProvider, StaticProvider,
};
pub use identity::{ClientInstance, InstanceState, Invocation};
pub use lifecycle::{
    Client, ClientConfig, ClientError, Completion, ConnectionId, Outcome, RequestId, RequestState,
    RetryError, SdkAction,
};
pub use pool::{Pool, PoolError, PoolLimits, StreamPermit};

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "production";

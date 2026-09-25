//! Quinn transport adapter (task-30; design Sections 11.1-11.5, 19.2,
//! 19.4): QUIC over TLS 1.3 with the explicit AWS-LC provider, bounded
//! reliable streams, first-frame role negotiation and owned dispatch.
//!
//! The adapter owns sockets, handshakes and stream lifecycles and hands
//! the rest of the system *owned events*: an authenticated peer frame with
//! its [`coord_core::event::PeerProvenance`], an API request with a
//! responder, a connection opened or closed. Nothing here is protocol
//! state: a completed write, a QUIC ACK or a finished stream is transport
//! bookkeeping only, never durability, learning or establishment (there
//! is no event of those kinds to emit).
//!
//! Negotiation is fail-closed. Two ALPNs separate the native API
//! (`coord-api/1`: clients, trusted frontends, Kine collectors) from the
//! internal peer plane (`coord-peer/1`: voters, observers, learners). The
//! first frame on the control stream is `Hello`; it must name this cluster
//! and domain, a role admitted by the ALPN class, an incarnation for peer
//! roles, and it must be bound to the TLS peer certificate by the
//! [`IdentityBinder`] the runtime supplies. Malformed frames, unsupported
//! versions, mismatches and unbound certificates close the connection
//! with a code; nothing is dispatched before `HelloAck`.
//!
//! Application 0-RTT is disabled on both sides, active migration is
//! refused for the server, mutual TLS is required, and every bound is
//! explicit: connections, streams per connection, frame class limits (the
//! `wire_v1` reader), handshake and read timeouts, event queue depth and
//! a shutdown deadline. Connections are opened only when the runtime asks
//! for one: nothing dials a mesh.
//!
//! Traffic isolation (task-31): a peer link has one connection per
//! [`lane::Lane`] (control, unary, watch, bulk) with its own stream
//! limits, windows and fair per-group queue, explicit CUBIC on all; bytes
//! handed to QUIC are bounded per destination across lanes and per node
//! across destinations with a control-only reserve; events are delivered
//! per lane so a stalled bulk or watch consumer never blocks control; and
//! queue wait, credit wait and RTT are measured as three quantities.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod budget;
pub mod config;
pub mod endpoint;
pub mod frames;
pub mod identity;
pub mod lane;
pub mod sched;

pub use budget::{Budget, BudgetError, BudgetLimits};
pub use config::{ALPN_API, ALPN_PEER, Class, ClientIdentity, Limits, LocalIdentity, TlsProfile};
pub use endpoint::{
    CloseCode, CloseReason, ConnectionId, Destination, Dialer, RequestError, Responder, SendError,
    Transport, TransportError, TransportEvent,
};
pub use frames::{FrameError, KIND_PEER_EVIDENCE, evidence_frame};
pub use identity::{BindError, BoundIdentity, IdentityBinder, role_class};
pub use lane::{Lane, LaneError, LaneLimits, lane_of_hello, role_lanes};
pub use sched::{FairQueue, LaneStats, QueueError, Queued, WaitStats};

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "production";

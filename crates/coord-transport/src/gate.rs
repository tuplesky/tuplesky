//! The pre-bind gate (task-d122; design Section 11.2): what a connection
//! without a client certificate may do before it binds a session.
//!
//! The native API plane does not require a client certificate. A caller
//! without one is authorized by the session binding it presents above
//! this layer, so until it binds it is an unauthenticated party, and the
//! owner's rule is that an unauthenticated party gets no influence on
//! resources or availability. So such a connection, until its
//! `BindAck`, holds:
//! - one place under [`Limits::max_unbound`], apart from the connection
//!   cap every certificate holder shares, for at most
//!   [`Limits::bind_deadline`] from the end of its TLS handshake;
//! - the stream limits and windows every accepted connection starts at
//!   (QUIC credit cannot be taken back once advertised), never its
//!   lane's;
//! - one stream, carrying one `Bind` frame of at most
//!   [`SESSION_BIND_MAX_FRAME`] bytes, refused from its header otherwise
//!   and read under no shared budget.
//!
//! The bind is then checked here, before the runtime sees anything: first
//! whether the node takes binds at all ([`Transport::publish_fresh_until`]),
//! then the token, by the [`BindGate`] the runtime installs. A refusal
//! closes the connection with [`CloseCode::Rejected`] and one of two fixed
//! reasons, [`BIND_REFUSED`] or [`BIND_UNAVAILABLE`], which clients match
//! exactly. A second stream before the `BindAck` closes the connection.
//! Nothing of a connection refused after its `Hello` reaches the
//! runtime: no `Connected`, no request and no `Closed`. One refused
//! before then, at the cap, the handshake or its `Hello`, is reported as
//! every refused handshake is, by a `Closed` with no lane, sent without
//! waiting for room in the event queue.
//!
//! The runtime still decides the binding: the gate only keeps what an
//! unauthenticated party can make this node do to the cost of one
//! handshake and one token check.
//!
//! [`Limits::max_unbound`]: crate::Limits::max_unbound
//! [`Limits::bind_deadline`]: crate::Limits::bind_deadline
//! [`SESSION_BIND_MAX_FRAME`]: coord_types::wire_v1::SESSION_BIND_MAX_FRAME
//! [`Transport::publish_fresh_until`]: crate::Transport::publish_fresh_until
//! [`CloseCode::Rejected`]: crate::CloseCode::Rejected

use coord_types::wire_v1::Frame;

/// The close reason of a bind refused for its credential: the caller's
/// token will not do here, now or later.
pub const BIND_REFUSED: &str = "bind refused";

/// The close reason of a bind refused because this node cannot decide
/// binds now: the caller keeps its token and retries, here or at another
/// node.
pub const BIND_UNAVAILABLE: &str = "bind unavailable";

/// What the gate says of a bind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BindCheck {
    /// The token verifies: the bind goes to the runtime, which decides it.
    Pass,
    /// Refused for its credential ([`BIND_REFUSED`]).
    Refused,
    /// This node cannot check it now ([`BIND_UNAVAILABLE`]).
    Unavailable,
}

/// Checks the bind of a connection without a client certificate before
/// the runtime sees it (task-d122), the way an [`IdentityBinder`] checks
/// a certificate: the runtime supplies it, and this crate learns nothing
/// about sessions or tokens.
///
/// It runs on the connection's task, once per connection at most. It
/// must decide from what it holds, without waiting on the runtime.
///
/// [`IdentityBinder`]: crate::IdentityBinder
pub trait BindGate: Send + Sync {
    /// Check `frame`, a `Bind` of the version this build reads.
    fn check(&self, frame: &Frame) -> BindCheck;
}

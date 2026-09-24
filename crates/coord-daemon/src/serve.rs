//! What the serving loop does with one ingress (design Sections 3.2,
//! 4.3, 22.1).
//!
//! The pieces of an API connection were built separately and deliberately
//! hold nothing of each other: `coord-transport` negotiates and frames but
//! does not know what a session is; `coord-session` decides what a frame
//! of a bound connection means but holds no sockets; `coord-collector`
//! plans a submission but does not send it. Joining them is the daemon's
//! own work, and the join has exactly one decision in it -- what happens
//! to the stream the frame arrived on.
//!
//! That decision is this module, as a value rather than as I/O, because
//! getting it wrong is silent. A stream that should have been held is
//! instead finished, and the caller sees an empty answer to a request that
//! is still running. A stream that should have been finished is held, and
//! the caller waits out its deadline. A connection whose binding was
//! refused keeps being served because ignoring its frames is not the same
//! as closing it. None of those fail a type check, and all of them are
//! decided here, where a test can state them without a socket.

use coord_collector::{Action, FanOut};
use coord_session::Ingress;
use coord_storage::watch::Registration;
use coord_transport::CloseCode;
use coord_types::RetryKey;

/// What the loop does with the stream a frame arrived on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step {
    /// Write this frame on that stream and finish it. The invocation is
    /// over: either it was answered from retained state, or it was
    /// refused, or it was a binding that is now acknowledged.
    Answer(Vec<u8>),
    /// Keep the stream open under this invocation. The result does not
    /// exist yet; a later delivery finds this stream by its retry key and
    /// by the connection that asked.
    Hold(RetryKey),
    /// Keep the stream open under the plan's invocation, and send the
    /// plan to the voters first.
    Submit(Box<FanOut>),
    /// Keep the stream open as this watch's output stream: replay the
    /// registration's revisions onto it, then pump it for as long as the
    /// subscription lasts.
    Watch {
        /// The client's identifier for the subscription.
        watch_id: u64,
        /// What the hub registered, including the revisions to replay.
        registration: Registration,
    },
    /// Stop serving this connection.
    ///
    /// Not merely this stream: a frame that may not be sent at all is a
    /// statement about the connection, and leaving it open after refusing
    /// its frame lets the same frame be sent again.
    Close {
        /// The code the peer is closed with.
        code: CloseCode,
        /// A short fixed reason. It crosses the wire, so nothing derived
        /// from a caller's data appears in it.
        reason: &'static str,
    },
}

/// Decide what to do with `ingress`, which arrived on a stream of
/// `connection`.
///
/// `retry_key` is the invocation the frame named, where it named one. The
/// dispatcher reports a pending request by its command, but a command is
/// derived from the request and is not what a later delivery is addressed
/// to; a request whose stream must be held and whose invocation cannot be
/// named is a request this process cannot answer, and it is refused here
/// rather than held under a key guessed from the wrong identity.
pub fn step(ingress: Ingress, retry_key: Option<RetryKey>) -> Step {
    match ingress {
        // A binding is answered on its own stream and that stream ends:
        // the acknowledgement is the whole of the exchange, and the
        // connection's later work opens its own streams.
        Ingress::Bound(ack) => Step::Answer(ack),
        // A binding whose session the cluster has still to agree on.
        // The stream stays open under the establishment's own
        // invocation, exactly as a request's does, and the
        // acknowledgement is written when that command's outcome comes
        // back: nothing the caller could do with the session precedes
        // the session.
        Ingress::Establishing(plan) => Step::Submit(plan),
        // A refused, absent or ended binding are three ways of saying the
        // same thing -- this connection has no session -- and the peer
        // learns which only as `Rejected`, because whether a token was
        // wrong or merely late is not something an unbound caller is
        // entitled to distinguish.
        Ingress::Rejected(_) => Step::Close {
            code: CloseCode::Rejected,
            reason: "bind refused",
        },
        Ingress::NotBound => Step::Close {
            code: CloseCode::Rejected,
            reason: "not bound",
        },
        Ingress::Expired => Step::Close {
            code: CloseCode::Rejected,
            reason: "binding expired",
        },
        Ingress::Action(action) => match action {
            Action::Respond(delivery) => Step::Answer(delivery.frame),
            Action::FanOut(plan) => Step::Submit(Box::new(plan)),
            Action::Pending { .. } => match retry_key {
                Some(key) => Step::Hold(key),
                None => Step::Close {
                    code: CloseCode::Protocol,
                    reason: "pending without an invocation",
                },
            },
            Action::WatchOpened {
                watch_id,
                registration,
                ..
            } => Step::Watch {
                watch_id,
                registration,
            },
            Action::Violation { .. } => Step::Close {
                code: CloseCode::Protocol,
                reason: "frame not permitted",
            },
        },
    }
}

impl Step {
    /// The invocation whose stream this step keeps open, if any.
    ///
    /// A held stream and a submitted one are held the same way and under
    /// the same key; the difference between them is only whether the
    /// frame still has to reach the voters.
    pub fn held(&self) -> Option<RetryKey> {
        match self {
            Step::Hold(key) => Some(*key),
            Step::Submit(plan) => Some(plan.retry_key),
            _ => None,
        }
    }

    /// Whether the stream this step names stays open after it.
    pub const fn keeps_the_stream(&self) -> bool {
        matches!(self, Step::Hold(_) | Step::Submit(_) | Step::Watch { .. })
    }
}

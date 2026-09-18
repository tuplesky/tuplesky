//! The voter side of the collector contract (design Sections 3.2, 3.3,
//! 19.4): what a voter accepts from a collector and what it sends back.
//!
//! A `Submit` becomes an admitted request only when the connection's
//! bound role is a collector role (`Frontend`, `KineCollector`). A native
//! client, an observer or another voter presenting the same frame is
//! refused: role-scoped collector access never becomes general request
//! or voting access, and nothing that arrives on an API-class connection
//! is ever treated as a vote (votes only exist on the peer plane).

use coord_core::capability::ReleasedResult;
use coord_core::effect::{Effect, PeerId};
use coord_core::event::AdmittedRequest;
use coord_types::wire_v1::{Frame, MessageV1, PeerRole};

use crate::wire::{CollectorWireError, decode_submit, evidence_frame_from_bytes, release_frame};

/// Why a frame from an API-class connection was not admitted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IngressError {
    /// The role may not submit on behalf of clients.
    RoleNotAuthorized(PeerRole),
    /// Not a collector submission.
    Wire(CollectorWireError),
}

/// Whether `role` is an authorized collector.
pub const fn is_collector(role: PeerRole) -> bool {
    matches!(role, PeerRole::Frontend | PeerRole::KineCollector)
}

/// Turn a collector's `Submit` into the admitted request the consensus
/// machines consume. The receipt travels sealed; the request frame is
/// re-encoded from the exact request so identity is bit-preserved.
pub fn admitted_from_submit(
    role: PeerRole,
    frame: &Frame,
) -> Result<AdmittedRequest, IngressError> {
    if !is_collector(role) {
        return Err(IngressError::RoleNotAuthorized(role));
    }
    let submit = decode_submit(frame).map_err(IngressError::Wire)?;
    let frame = MessageV1::Request(submit.request)
        .encode()
        .map_err(|_| IngressError::Wire(CollectorWireError::TooLarge))?;
    Ok(AdmittedRequest {
        receipt: submit.receipt,
        frame,
    })
}

/// The frame a voter sends to its collector for `effect`, when the effect
/// addresses the collector: evidence published to the frontend peer
/// identity, or a released result. Everything else is not the
/// collector's.
pub fn frontend_frame(
    effect: &Effect,
    frontend: PeerId,
) -> Option<Result<Vec<u8>, CollectorWireError>> {
    match effect {
        Effect::SendWhenDurable { to, frame, .. } if *to == frontend => {
            Some(evidence_frame_from_bytes(frame))
        }
        Effect::Released(released) => Some(release_frame(released)),
        _ => None,
    }
}

/// Encode a released result for the collector (the leader's release gate
/// output).
pub fn released_frame(released: &ReleasedResult) -> Result<Vec<u8>, CollectorWireError> {
    release_frame(released)
}

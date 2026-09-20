//! The voter side of the collector contract (design Sections 3.2, 3.3,
//! 19.4): what a voter accepts from a collector and what it sends back.
//!
//! A `Submit` becomes an admitted request only when the connection's
//! bound role is a collector role (`Frontend`, `KineCollector`). A native
//! client, an observer or another voter presenting the same frame is
//! refused: role-scoped collector access never becomes general request
//! or voting access, and nothing that arrives on an API-class connection
//! is ever treated as a vote (votes only exist on the peer plane).

use coord_core::capability::{AdmissionPurpose, AdmissionReceipt, ReleasedResult, VerifierToken};
use coord_core::effect::{Effect, PeerId};
use coord_core::event::AdmittedRequest;
use coord_types::ids::{ClusterId, DomainId};
use coord_types::wire_v1::{Frame, MessageV1, PeerRole};

use crate::wire::{CollectorWireError, decode_submit, evidence_frame_from_bytes, release_frame};

/// Why a frame from an API-class connection was not admitted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IngressError {
    /// The role may not submit on behalf of clients.
    RoleNotAuthorized(PeerRole),
    /// The role may relay a client's work but may not say who a
    /// credential belongs to, and these claims establish a session.
    NotASessionIssuer(PeerRole),
    /// The claims were attested by a verifier of another cluster or
    /// another domain. They admit nothing here whatever they say.
    WrongOrigin {
        /// Cluster the claims name.
        cluster: ClusterId,
        /// Domain the claims name.
        domain: DomainId,
    },
    /// Not a collector submission.
    Wire(CollectorWireError),
}

/// Whether `role` is an authorized collector.
///
/// The role vocabulary's own answer, not a second copy of it: the
/// transport admits a submission stream on exactly this rule, and a
/// rule that was written down twice would eventually be two rules.
pub const fn is_collector(role: PeerRole) -> bool {
    role.may_submit_for_clients()
}

/// Turn a collector's `Submit` into the admitted request the consensus
/// machines consume.
///
/// This is the verifier boundary for a collector submission: the claims
/// travel as a record, and the receipt -- which nothing that merely
/// decodes a frame can produce -- is minted here, after the checks
/// below. The request frame is re-encoded from the exact request so
/// identity is bit-preserved.
///
/// # What is checked, and why it is not one check
///
/// Deserializing a record is not proof of where it came from. So the
/// capability is reconstructed only after the ingress it arrived on is
/// shown to have the authority the claims call for:
///
/// * **Origin.** The claims name the cluster and domain their verifier
///   belongs to, and they must be `cluster` and `domain` -- this
///   voter's own. A receipt minted elsewhere admits nothing here.
/// * **Purpose against authority.** A submission under an existing
///   session needs a role that may relay a client's work. Establishing
///   a session needs a role that may say who a credential belongs to,
///   which is strictly narrower: being able to send a `Submit` is not
///   authority to originate an identity.
///
/// Nothing here decides whether the session exists, whether the rule is
/// still enabled, or whether the receipt was already consumed. Those
/// are the replicated state machine's, at the command's own position.
pub fn admitted_from_submit(
    role: PeerRole,
    frame: &Frame,
    cluster: ClusterId,
    domain: DomainId,
) -> Result<AdmittedRequest, IngressError> {
    let submit = decode_submit(frame).map_err(IngressError::Wire)?;
    let facts = submit.receipt;
    if facts.attested.cluster != cluster || facts.attested.domain != domain {
        return Err(IngressError::WrongOrigin {
            cluster: facts.attested.cluster,
            domain: facts.attested.domain,
        });
    }
    let authorized = match facts.purpose() {
        AdmissionPurpose::Submit => role.may_submit_for_clients(),
        AdmissionPurpose::Establish => role.may_establish_sessions(),
    };
    if !authorized {
        return Err(match facts.purpose() {
            AdmissionPurpose::Submit => IngressError::RoleNotAuthorized(role),
            AdmissionPurpose::Establish => IngressError::NotASessionIssuer(role),
        });
    }
    // Only now: the ingress has the authority these facts call for, so
    // they may become a capability.
    let receipt = AdmissionReceipt::attesting(VerifierToken::for_boundary(), facts);
    let frame = MessageV1::Request(submit.request)
        .encode()
        .map_err(|_| IngressError::Wire(CollectorWireError::TooLarge))?;
    Ok(AdmittedRequest { receipt, frame })
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

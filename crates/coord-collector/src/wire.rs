//! Collector frames (registered in `spec/wire-v1.md`). The submission
//! travels in the API kind range because it carries a client request and
//! takes its class limit; the release and the evidence frame are in the
//! collector-evidence range, which is sized for an established result.
//!
//! | Kind | Direction | Payload |
//! |---|---|---|
//! | `Submit` `0x0103` | collector to every voter | [`SubmitV1`] |
//! | `Release` `0x0701` | leader to collector | `ReleasedResult` (postcard) |
//! | `Evidence` `0x0700` | voter to collector | `ProtocolMessage` (postcard) |
//!
//! None of these is decodable by the typed `wire_v1` decoder: they are
//! dispatched by raw kind at the collector boundary, the way peer
//! evidence is on the peer plane.

use coord_consensus::ProtocolMessage;
use coord_core::capability::{
    AdmissionReceipt, AttestedAdmission, CredentialDeadline, ReleasedResult,
};
use coord_types::identity::Digest32;
use coord_types::ids::{ClusterId, DomainId, PrincipalId, SessionId, TrustRuleId};
use coord_types::wire_v1::{Frame, RequestV1, WireError, encode_frame};
use serde::{Deserialize, Serialize};

/// A collector submits an admitted, canonical request to a voter.
///
/// The registry's number, not a second one: the transport admits
/// exactly this kind on a request stream, and this is the boundary that
/// mints a receipt from what arrives.
pub use coord_types::wire_v1::KIND_COLLECTOR_SUBMIT as KIND_SUBMIT;
/// The leader publishes a released result to the collector. It is in the
/// collector-evidence range, not the API range: an established result may
/// be as large as `MAX_RESULT_BYTES`, which the API class cannot carry,
/// and a release that cannot be framed would leave its request pending
/// with nothing to convert into a `RESULT_TOO_LARGE` answer.
pub const KIND_RELEASE: u16 = 0x0701;
/// A voter's protocol evidence (leader reply, fast or slow ack).
pub const KIND_EVIDENCE: u16 = 0x0700;
/// Schema version of every collector frame.
pub const VERSION: u16 = 1;

/// What a verifier attested when a credential is to establish the
/// session it names, as it travels between a collector and a voter.
///
/// Its presence is what says the receipt is for establishment: there is
/// no separate purpose field to disagree with it, and a submission's
/// claims cannot carry a principal at all.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EstablishmentClaimsV1 {
    /// Principal the verified credential maps to under configured trust.
    pub principal: PrincipalId,
    /// The trust rule actually used.
    pub trust_rule: TrustRuleId,
    /// When the verified credential stops admitting new work.
    pub credential_valid_until: CredentialDeadline,
}

/// The claims of an admission receipt, as they travel between a
/// collector and a voter.
///
/// An [`AdmissionReceipt`] is deliberately not deserializable: it is
/// minted by a verifier and never restored from bytes, so nothing that
/// merely decodes a frame can produce one. The claims therefore cross
/// the wire as their own record, and the voter's collector boundary
/// mints the receipt again ([`crate::admitted_from_submit`]) only after
/// it has checked that the connection's bound role may do what the
/// claims say they are for, and that they are this domain's.
///
/// Transporting claims is not authority to originate them. A role that
/// may relay a client's work is not thereby a role that may say who a
/// credential belongs to, which is why the two are different checks
/// against different questions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdmissionClaimsV1 {
    /// Cluster the verifier belongs to.
    pub cluster: ClusterId,
    /// Domain the session lives in.
    pub domain: DomainId,
    /// Session the admission was for.
    pub session: SessionId,
    /// Rule/issuer generation the admission relied on.
    pub rule_generation: u64,
    /// Scope ceiling; policy at execution can only narrow it.
    pub scope_ceiling: u32,
    /// Unique receipt identity (single use).
    pub receipt_id: Digest32,
    /// Admission tick.
    pub admitted_at_ticks: u64,
    /// What was attested for establishing a session, where that is what
    /// this receipt is for. `None` is a submission under a session that
    /// already exists.
    pub establishing: Option<EstablishmentClaimsV1>,
}

impl AdmissionClaimsV1 {
    /// The claims a minted receipt carries.
    pub fn of(receipt: &AdmissionReceipt) -> Self {
        let a = receipt.attested();
        AdmissionClaimsV1 {
            cluster: a.cluster,
            domain: a.domain,
            session: a.session,
            rule_generation: a.rule_generation,
            scope_ceiling: a.scope_ceiling,
            receipt_id: a.receipt_id,
            admitted_at_ticks: a.admitted_at_ticks,
            establishing: receipt.establishment().map(|e| EstablishmentClaimsV1 {
                principal: e.principal,
                trust_rule: e.trust_rule,
                credential_valid_until: e.credential_valid_until,
            }),
        }
    }

    /// What every receipt attests, as the capability's own type.
    pub const fn attested(&self) -> AttestedAdmission {
        AttestedAdmission {
            cluster: self.cluster,
            domain: self.domain,
            session: self.session,
            rule_generation: self.rule_generation,
            scope_ceiling: self.scope_ceiling,
            receipt_id: self.receipt_id,
            admitted_at_ticks: self.admitted_at_ticks,
        }
    }
}

/// The submission: the admission claims minted at the collector and the
/// exact request the client sent (its logical bytes are what identity
/// binds).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmitV1 {
    /// Admission claims.
    pub receipt: AdmissionClaimsV1,
    /// The client request.
    pub request: RequestV1,
}

/// Why a collector frame was not accepted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CollectorWireError {
    /// Not the kind expected here.
    UnexpectedKind {
        /// Kind seen.
        kind: u16,
    },
    /// Not a supported schema version.
    UnsupportedVersion {
        /// Version seen.
        version: u16,
    },
    /// The payload did not decode.
    Malformed,
    /// Bytes followed the payload.
    Trailing,
    /// The frame exceeds its class limit.
    TooLarge,
}

fn check(frame: &Frame, kind: u16) -> Result<(), CollectorWireError> {
    if frame.kind != kind {
        return Err(CollectorWireError::UnexpectedKind { kind: frame.kind });
    }
    if frame.version != VERSION {
        return Err(CollectorWireError::UnsupportedVersion {
            version: frame.version,
        });
    }
    Ok(())
}

fn exact<'a, T: Deserialize<'a>>(payload: &'a [u8]) -> Result<T, CollectorWireError> {
    let (value, rest): (T, &[u8]) =
        postcard::take_from_bytes(payload).map_err(|_| CollectorWireError::Malformed)?;
    if !rest.is_empty() {
        return Err(CollectorWireError::Trailing);
    }
    Ok(value)
}

fn frame(kind: u16, payload: &[u8]) -> Result<Vec<u8>, CollectorWireError> {
    encode_frame(kind, VERSION, payload).map_err(|e| match e {
        WireError::PayloadTooLarge | WireError::LengthAboveClassLimit { .. } => {
            CollectorWireError::TooLarge
        }
        _ => CollectorWireError::Malformed,
    })
}

/// Encode a submission.
pub fn submit_frame(submit: &SubmitV1) -> Result<Vec<u8>, CollectorWireError> {
    let payload = postcard::to_allocvec(submit).map_err(|_| CollectorWireError::TooLarge)?;
    frame(KIND_SUBMIT, &payload)
}

/// Decode a submission.
pub fn decode_submit(frame: &Frame) -> Result<SubmitV1, CollectorWireError> {
    check(frame, KIND_SUBMIT)?;
    exact(&frame.payload)
}

/// Encode protocol evidence for the collector.
pub fn evidence_frame(message: &ProtocolMessage) -> Result<Vec<u8>, CollectorWireError> {
    evidence_frame_from_bytes(&message.encode())
}

/// Encode already-encoded protocol evidence (the bytes of a machine's
/// send effect) for the collector.
pub fn evidence_frame_from_bytes(message: &[u8]) -> Result<Vec<u8>, CollectorWireError> {
    frame(KIND_EVIDENCE, message)
}

/// Decode protocol evidence.
pub fn decode_evidence(frame: &Frame) -> Result<ProtocolMessage, CollectorWireError> {
    check(frame, KIND_EVIDENCE)?;
    ProtocolMessage::decode(&frame.payload).map_err(|_| CollectorWireError::Malformed)
}

/// Encode a released result.
pub fn release_frame(released: &ReleasedResult) -> Result<Vec<u8>, CollectorWireError> {
    let payload = postcard::to_allocvec(released).map_err(|_| CollectorWireError::TooLarge)?;
    frame(KIND_RELEASE, &payload)
}

/// Decode a released result.
pub fn decode_release(frame: &Frame) -> Result<ReleasedResult, CollectorWireError> {
    check(frame, KIND_RELEASE)?;
    exact(&frame.payload)
}

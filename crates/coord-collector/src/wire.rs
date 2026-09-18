//! Collector frames (registered in `spec/wire-v1.md`). Two travel in the
//! API kind range because they carry or answer a client request and take
//! its class limit; the evidence frame is in the collector-evidence range.
//!
//! | Kind | Direction | Payload |
//! |---|---|---|
//! | `Submit` `0x0103` | collector to every voter | [`SubmitV1`] |
//! | `Release` `0x0104` | leader to collector | `ReleasedResult` (postcard) |
//! | `Evidence` `0x0700` | voter to collector | `ProtocolMessage` (postcard) |
//!
//! None of these is decodable by the typed `wire_v1` decoder: they are
//! dispatched by raw kind at the collector boundary, the way peer
//! evidence is on the peer plane.

use coord_consensus::ProtocolMessage;
use coord_core::capability::{AdmissionReceipt, ReleasedResult};
use coord_types::wire_v1::{Frame, RequestV1, WireError, encode_frame};
use serde::{Deserialize, Serialize};

/// A collector submits an admitted, canonical request to a voter.
pub const KIND_SUBMIT: u16 = 0x0103;
/// The leader publishes a released result to the collector.
pub const KIND_RELEASE: u16 = 0x0104;
/// A voter's protocol evidence (leader reply, fast or slow ack).
pub const KIND_EVIDENCE: u16 = 0x0700;
/// Schema version of every collector frame.
pub const VERSION: u16 = 1;

/// The submission: the sealed receipt minted at admission and the exact
/// request the client sent (its logical bytes are what identity binds).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmitV1 {
    /// Admission receipt.
    pub receipt: AdmissionReceipt,
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

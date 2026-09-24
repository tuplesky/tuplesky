//! Binding frames (registered in `spec/wire-v1.md`): `Bind` carries the
//! service token once per connection; `BindAck` answers with what was
//! bound. Both are raw kinds in the API range.

use coord_types::ids::SessionId;
use coord_types::wire_v1::{BoundedBytes, Frame, WireError, encode_frame};
use serde::{Deserialize, Serialize};

/// A client binds a session. The number belongs to the shared wire
/// vocabulary, so that the transport boundary that admits the frame and
/// this module that answers it cannot drift apart.
pub const KIND_BIND: u16 = coord_types::wire_v1::KIND_SESSION_BIND;
/// The frontend acknowledges a binding.
pub const KIND_BIND_ACK: u16 = coord_types::wire_v1::KIND_SESSION_BIND_ACK;
/// Largest accepted token.
pub const MAX_TOKEN_BYTES: usize = 8 * 1024;
const VERSION: u16 = coord_types::wire_v1::SESSION_BIND_VERSION;

/// The binding request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BindV1 {
    /// The service token (redacted from any diagnostic by the frontend).
    pub token: BoundedBytes<MAX_TOKEN_BYTES>,
}

/// The binding acknowledgement.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BindAckV1 {
    /// Session bound.
    pub session: SessionId,
    /// Conservative validity end (unix seconds).
    pub expires_at: u64,
    /// Scope bits of the token.
    pub scope: u32,
    /// Trust rule generation the session was admitted under.
    pub rule_generation: u64,
}

/// Why a binding frame was not accepted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionWireError {
    /// Not the expected kind or version.
    Unexpected,
    /// The payload did not decode exactly.
    Malformed,
    /// Over the bound.
    TooLarge,
}

fn exact<'a, T: Deserialize<'a>>(frame: &'a Frame, kind: u16) -> Result<T, SessionWireError> {
    if frame.kind != kind || frame.version != VERSION {
        return Err(SessionWireError::Unexpected);
    }
    let (value, rest): (T, &[u8]) =
        postcard::take_from_bytes(&frame.payload).map_err(|_| SessionWireError::Malformed)?;
    if !rest.is_empty() {
        return Err(SessionWireError::Malformed);
    }
    Ok(value)
}

fn frame<T: Serialize>(kind: u16, value: &T) -> Result<Vec<u8>, SessionWireError> {
    let payload = postcard::to_allocvec(value).map_err(|_| SessionWireError::TooLarge)?;
    encode_frame(kind, VERSION, &payload).map_err(|e| match e {
        WireError::PayloadTooLarge | WireError::LengthAboveClassLimit { .. } => {
            SessionWireError::TooLarge
        }
        _ => SessionWireError::Malformed,
    })
}

/// Encode a binding request.
pub fn bind_frame(token: &[u8]) -> Result<Vec<u8>, SessionWireError> {
    let token = BoundedBytes::new(token.to_vec()).map_err(|_| SessionWireError::TooLarge)?;
    frame(KIND_BIND, &BindV1 { token })
}

/// Decode a binding request.
pub fn decode_bind(frame: &Frame) -> Result<BindV1, SessionWireError> {
    exact(frame, KIND_BIND)
}

/// Encode an acknowledgement.
pub fn bind_ack_frame(ack: &BindAckV1) -> Result<Vec<u8>, SessionWireError> {
    frame(KIND_BIND_ACK, ack)
}

/// Decode an acknowledgement.
pub fn decode_bind_ack(frame: &Frame) -> Result<BindAckV1, SessionWireError> {
    exact(frame, KIND_BIND_ACK)
}

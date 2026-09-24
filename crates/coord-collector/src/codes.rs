//! Error responses of the frontend. The frozen codes live in
//! `coord_types::wire_v1::codes` (the SDK maps them to typed retry
//! errors); pending and unknown outcomes are not errors but the `Pending`
//! and `Unknown` outcomes of the wire.

use coord_types::CommandId;
use coord_types::wire_v1::{BoundedBytes, OutcomeV1, ResponseV1};

pub use coord_types::wire_v1::codes::{
    BACKPRESSURE, MALFORMED_REQUEST, NOT_ADMITTED, REQUEST_IDENTITY_CONFLICT, RESULT_TOO_LARGE,
};

/// A response whose outcome is not established yet (`ResolveRequest`
/// later). Used for resolution of pending work and for a client
/// deadline: the outcome is unknown, never failed.
pub const fn pending_response(command_id: CommandId) -> ResponseV1 {
    ResponseV1 {
        command_id,
        outcome: OutcomeV1::Pending,
    }
}

/// A response for an identity this endpoint does not know.
pub const fn unknown_response(command_id: CommandId) -> ResponseV1 {
    ResponseV1 {
        command_id,
        outcome: OutcomeV1::Unknown,
    }
}

/// An error response with a bounded, redacted detail.
pub fn error_response(command_id: CommandId, code: u16, detail: &str) -> ResponseV1 {
    let mut bytes = detail.as_bytes().to_vec();
    bytes.truncate(coord_types::wire_v1::MAX_REASON_BYTES);
    ResponseV1 {
        command_id,
        outcome: OutcomeV1::Err {
            code,
            detail: BoundedBytes::new(bytes).expect("truncated to the bound"),
        },
    }
}

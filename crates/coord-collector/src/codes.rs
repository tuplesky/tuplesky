//! Frozen error codes of `ResponseV1::Err` produced at the frontend
//! (design Section 4.4; the SDK's typed retry errors of task-34 map from
//! these). Codes are append-only. Pending and unknown outcomes are not
//! errors: they are the `Pending` and `Unknown` outcomes of the wire.

use coord_types::CommandId;
use coord_types::wire_v1::{BoundedBytes, OutcomeV1, ResponseV1};

/// The retry key is bound to another payload (`RequestIdentityConflict`).
pub const REQUEST_IDENTITY_CONFLICT: u16 = 0x0001;
/// The frontend or the domain is at its collection bound; retry later
/// with the same identity.
pub const BACKPRESSURE: u16 = 0x0002;
/// The request did not decode as a canonical request.
pub const MALFORMED_REQUEST: u16 = 0x0003;
/// The caller may not submit this request (role, cluster, domain or
/// session mismatch).
pub const NOT_ADMITTED: u16 = 0x0004;
/// The established result does not fit the response bound.
pub const RESULT_TOO_LARGE: u16 = 0x0005;

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

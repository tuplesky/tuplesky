//! The bounded signer endpoint (design Sections 10.2, 20.4): a narrow
//! port that enrolls nodes. Body, concurrency and time are bounded; the
//! clock is injected; the CA certificate (the trust anchor peers pin) is
//! served for bootstrap. This binary is deployable before any voter.

use std::sync::Arc;

use axum::extract::{DefaultBodyLimit, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use coord_authn::ClockHealth;
use tokio::sync::{Mutex, Semaphore};

use crate::issuer::{IssueError, NodeIssuer, NodeRequest};

/// A clock the endpoint reads.
pub trait SignClock: Send + Sync {
    /// The current reading.
    fn read(&self) -> ClockHealth;
}

/// The system clock with a configured uncertainty.
#[derive(Debug)]
pub struct SystemSignClock {
    /// Uncertainty in seconds.
    pub uncertainty: u64,
}

impl SignClock for SystemSignClock {
    fn read(&self) -> ClockHealth {
        match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
            Ok(d) => ClockHealth::healthy(d.as_secs(), self.uncertainty),
            Err(_) => ClockHealth {
                now: 0,
                uncertainty: self.uncertainty,
                healthy: false,
            },
        }
    }
}

/// Shared endpoint state.
pub struct IssuerState {
    /// The issuer.
    pub issuer: Mutex<NodeIssuer>,
    /// Clock.
    pub clock: Box<dyn SignClock>,
    /// Concurrent enrollments at most.
    pub permits: Semaphore,
    /// Request body bound.
    pub max_body_bytes: usize,
}

impl IssuerState {
    /// Assemble the state.
    pub fn new(
        issuer: NodeIssuer,
        clock: Box<dyn SignClock>,
        max_in_flight: usize,
        max_body_bytes: usize,
    ) -> Self {
        IssuerState {
            issuer: Mutex::new(issuer),
            clock,
            permits: Semaphore::new(max_in_flight),
            max_body_bytes,
        }
    }
}

/// The router.
pub fn router(state: Arc<IssuerState>) -> Router {
    let limit = state.max_body_bytes;
    Router::new()
        .route("/enroll", post(enroll))
        .route("/ca", get(ca))
        .layer(DefaultBodyLimit::max(limit))
        .with_state(state)
}

#[derive(serde::Deserialize)]
struct EnrollBody {
    assertion: String,
    /// Base64url of the CSR DER.
    csr: String,
    /// Hex of the node identity.
    node: String,
    incarnation: u64,
    lifetime_secs: u64,
}

fn b64url_decode(s: &str) -> Option<Vec<u8>> {
    const REV: fn(u8) -> Option<u8> = |c| match c {
        b'A'..=b'Z' => Some(c - b'A'),
        b'a'..=b'z' => Some(c - b'a' + 26),
        b'0'..=b'9' => Some(c - b'0' + 52),
        b'-' => Some(62),
        b'_' => Some(63),
        _ => None,
    };
    let mut out = Vec::new();
    let mut acc = 0u32;
    let mut bits = 0;
    for &c in s.as_bytes() {
        let v = REV(c)?;
        acc = (acc << 6) | u32::from(v);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    Some(out)
}

fn unhex16(s: &str) -> Option<[u8; 16]> {
    if s.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
        out[i] = u8::from_str_radix(std::str::from_utf8(chunk).ok()?, 16).ok()?;
    }
    Some(out)
}

fn b64url_encode(bytes: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(T[(n >> 18) as usize & 63] as char);
        out.push(T[(n >> 12) as usize & 63] as char);
        if chunk.len() > 1 {
            out.push(T[(n >> 6) as usize & 63] as char);
        }
        if chunk.len() > 2 {
            out.push(T[n as usize & 63] as char);
        }
    }
    out
}

fn error(status: StatusCode, code: &str) -> Response {
    (status, Json(serde_json::json!({ "error": code }))).into_response()
}

async fn enroll(State(state): State<Arc<IssuerState>>, Json(body): Json<EnrollBody>) -> Response {
    let Ok(_permit) = state.permits.try_acquire() else {
        return error(StatusCode::SERVICE_UNAVAILABLE, "busy");
    };
    let (Some(csr_der), Some(node)) = (b64url_decode(&body.csr), unhex16(&body.node)) else {
        return error(StatusCode::BAD_REQUEST, "malformed");
    };
    let request = NodeRequest {
        assertion: body.assertion,
        csr_der,
        node,
        incarnation: body.incarnation,
        lifetime_secs: body.lifetime_secs,
    };
    let clock = state.clock.read();
    match state.issuer.lock().await.enroll(&request, &clock) {
        Ok(issued) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "certificate": b64url_encode(&issued.certificate),
                "node_uri": issued.node_uri,
                "expires_at": issued.expires_at,
            })),
        )
            .into_response(),
        Err(e) => {
            let (status, code) = match e {
                IssueError::KeysUnavailable { .. } | IssueError::ClockUnhealthy => {
                    (StatusCode::SERVICE_UNAVAILABLE, "issuer_unavailable")
                }
                IssueError::Assertion => (StatusCode::UNAUTHORIZED, "assertion_rejected"),
                IssueError::Signing => (StatusCode::INTERNAL_SERVER_ERROR, "signing"),
                _ => (StatusCode::BAD_REQUEST, "request_rejected"),
            };
            error(status, code)
        }
    }
}

async fn ca(State(state): State<Arc<IssuerState>>) -> Response {
    let der = state.issuer.lock().await.ca().certificate().to_vec();
    (
        StatusCode::OK,
        Json(serde_json::json!({ "ca_certificate": b64url_encode(&der) })),
    )
        .into_response()
}

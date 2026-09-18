//! The persistent binding of one API connection (design Sections 9.3,
//! 11.5, 19.4): what the service token established, its validity, and
//! the rule that a rebind refreshes validity but never identity.

use coord_authn::ClockHealth;
use coord_collector::Caller;
use coord_sts::{TokenError, verify_service_token};
use coord_types::ids::SessionId;
use coord_types::wire_v1::PeerRole;
use serde_json::Value;

/// What the frontend verifies tokens against.
#[derive(Clone, Debug)]
pub struct BindingConfig {
    /// STS issuer.
    pub issuer: String,
    /// This cluster's resource (token audience).
    pub resource: String,
    /// The STS's published keys (refreshed out of band).
    pub jwks: Value,
}

/// One connection's binding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Binding {
    /// Session.
    pub session: SessionId,
    /// Principal (lowercase hex, as the token carries it).
    pub principal: String,
    /// Scope ceiling of the token (never wider than the session's).
    pub scope_ceiling: u32,
    /// Trust rule generation the session was admitted under.
    pub rule_generation: u64,
    /// Token expiry (unix seconds).
    pub expires_at: u64,
    /// When the binding was made or last refreshed.
    pub bound_at: u64,
    /// Rebinds so far.
    pub rebinds: u32,
}

impl Binding {
    /// The caller the dispatcher sees.
    pub const fn caller(&self) -> Caller {
        Caller {
            role: PeerRole::Client,
            session: self.session,
            rule_generation: self.rule_generation,
            scope_ceiling: self.scope_ceiling,
        }
    }

    /// Whether the binding admits work at `clock` (conservatively: the
    /// expiry must be beyond the latest instant the clock may denote).
    pub const fn active(&self, clock: &ClockHealth) -> bool {
        clock.healthy && clock.latest() < self.expires_at
    }
}

/// Why a binding was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BindError {
    /// The token is not valid here.
    Token(TokenError),
    /// The session claim is not a session identity.
    Malformed,
    /// A rebind named another session.
    SessionMismatch {
        /// The session already bound.
        bound: SessionId,
    },
    /// A rebind carried different immutable claims: a rebind refreshes
    /// validity and never changes the authorization context.
    ClaimsChanged,
}

fn hex_bytes<const N: usize>(hex: &str) -> Option<[u8; N]> {
    if hex.len() != 2 * N {
        return None;
    }
    let mut out = [0u8; N];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let s = std::str::from_utf8(chunk).ok()?;
        out[i] = u8::from_str_radix(s, 16).ok()?;
    }
    Some(out)
}

/// Verify `token` at `clock` and produce the binding; with `existing`, a
/// rebind that must keep the same session.
pub fn verify_bind(
    config: &BindingConfig,
    token: &[u8],
    clock: &ClockHealth,
    existing: Option<&Binding>,
) -> Result<Binding, BindError> {
    let token = std::str::from_utf8(token).map_err(|_| BindError::Malformed)?;
    let claims = verify_service_token(token, &config.jwks, &config.issuer, &config.resource, clock)
        .map_err(BindError::Token)?;
    let session = SessionId(hex_bytes::<16>(&claims.sid).ok_or(BindError::Malformed)?);
    if let Some(e) = existing {
        if e.session != session {
            return Err(BindError::SessionMismatch { bound: e.session });
        }
        // A rebind refreshes validity, nothing else. The claims that
        // decide what the connection may do are part of its authorization
        // context, so a same-session token carrying different ones would
        // change that context rather than extend it, and a wider scope
        // would widen every later admission.
        if e.principal != claims.sub
            || e.scope_ceiling != claims.scope
            || e.rule_generation != claims.generation
        {
            return Err(BindError::ClaimsChanged);
        }
    }
    Ok(Binding {
        session,
        principal: claims.sub,
        scope_ceiling: claims.scope,
        rule_generation: claims.generation,
        expires_at: claims.exp,
        bound_at: clock.now,
        rebinds: existing.map_or(0, |e| e.rebinds + 1),
    })
}

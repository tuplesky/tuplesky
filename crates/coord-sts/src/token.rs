//! Service-token claims (design Sections 9.1, 9.3): a cluster-specific
//! ES256 token bound to one replicated session with its immutable
//! principal and a scope no wider than the session's ceiling. API
//! endpoints verify it locally (task-37) against the published JWKS.

use coord_authn::ClockHealth;
use coord_state::policy::Action;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The claims of a service token.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceClaims {
    /// STS issuer.
    pub iss: String,
    /// Principal (lowercase hex).
    pub sub: String,
    /// Cluster resource.
    pub aud: String,
    /// Session (lowercase hex).
    pub sid: String,
    /// Scope bits (subset of the session ceiling).
    pub scope: u32,
    /// Trust rule (lowercase hex).
    pub rule: String,
    /// Trust rule generation.
    pub generation: u64,
    /// Receipt identity (lowercase hex): the token identifier.
    pub jti: String,
    /// Issued at.
    pub iat: u64,
    /// Expiry.
    pub exp: u64,
}

/// Why a service token was not accepted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TokenError {
    /// Not a token of this issuer's kind.
    Malformed,
    /// Not ES256, or a key-location header.
    Algorithm,
    /// Unknown key identifier.
    UnknownKey,
    /// Signature or claim failure.
    Invalid,
    /// Time claims under the clock's uncertainty.
    Time(coord_authn::TimeError),
}

const NAMES: [(&str, Action); 9] = [
    ("read", Action::Read),
    ("write", Action::Write),
    ("delete", Action::Delete),
    ("lease-grant", Action::LeaseGrant),
    ("lease-attach", Action::LeaseAttach),
    ("lease-inspect", Action::LeaseInspect),
    ("lease-renew", Action::LeaseRenew),
    ("lease-revoke", Action::LeaseRevoke),
    ("compact", Action::Compact),
];

/// The bit of a scope name.
pub fn action_bit(name: &str) -> Option<u32> {
    NAMES.iter().find(|(n, _)| *n == name).map(|(_, a)| a.bit())
}

/// Parse a space-separated scope string; unknown names are `None`.
pub fn scope_bits(scope: &str) -> Option<u32> {
    let mut bits = 0;
    for name in scope.split_whitespace() {
        bits |= action_bit(name)?;
    }
    Some(bits)
}

/// The scope string of bits.
pub fn scope_string(bits: u32) -> String {
    NAMES
        .iter()
        .filter(|(_, a)| bits & a.bit() != 0)
        .map(|(n, _)| *n)
        .collect::<Vec<_>>()
        .join(" ")
}

/// How many keys in `jwks` a token could actually be verified with.
///
/// [`verify_service_token`] needs a `keys` array whose entries carry a
/// `kid` and an EC public point this build can decode. A document that
/// is valid JSON and has none of those refuses every caller, so a
/// process that treated "the file parsed" as "the verifier is
/// configured" would come up, announce itself, and look -- from
/// outside -- exactly like a client problem. This is the question worth
/// asking at startup, and it is asked here so it stays the same
/// question the verifier asks.
pub fn usable_verification_keys(jwks: &Value) -> usize {
    let Some(keys) = jwks.get("keys").and_then(Value::as_array) else {
        return 0;
    };
    keys.iter()
        .filter(|k| {
            k.get("kid").and_then(Value::as_str).is_some()
                && match (
                    k.get("x").and_then(Value::as_str),
                    k.get("y").and_then(Value::as_str),
                ) {
                    (Some(x), Some(y)) => DecodingKey::from_ec_components(x, y).is_ok(),
                    _ => false,
                }
        })
        .count()
}

/// Verify a service token against a published JWKS document, exact
/// issuer and audience, at `clock`.
pub fn verify_service_token(
    token: &str,
    jwks: &Value,
    issuer: &str,
    audience: &str,
    clock: &ClockHealth,
) -> Result<ServiceClaims, TokenError> {
    let header = decode_header(token).map_err(|_| TokenError::Malformed)?;
    if header.alg != Algorithm::ES256
        || header.jku.is_some()
        || header.x5u.is_some()
        || header.jwk.is_some()
    {
        return Err(TokenError::Algorithm);
    }
    let kid = header.kid.ok_or(TokenError::UnknownKey)?;
    let keys = jwks
        .get("keys")
        .and_then(Value::as_array)
        .ok_or(TokenError::UnknownKey)?;
    let jwk = keys
        .iter()
        .find(|k| k.get("kid").and_then(Value::as_str) == Some(kid.as_str()))
        .ok_or(TokenError::UnknownKey)?;
    let (Some(x), Some(y)) = (
        jwk.get("x").and_then(Value::as_str),
        jwk.get("y").and_then(Value::as_str),
    ) else {
        return Err(TokenError::UnknownKey);
    };
    let key = DecodingKey::from_ec_components(x, y).map_err(|_| TokenError::UnknownKey)?;
    let mut validation = Validation::new(Algorithm::ES256);
    validation.validate_exp = false;
    validation.validate_nbf = false;
    validation.set_audience(&[audience]);
    validation.set_issuer(&[issuer]);
    validation.set_required_spec_claims(&["exp", "iss", "sub", "aud"]);
    let data =
        decode::<ServiceClaims>(token, &key, &validation).map_err(|_| TokenError::Invalid)?;
    let claims = data.claims;
    clock
        .check(claims.exp, None, Some(claims.iat), None)
        .map_err(TokenError::Time)?;
    Ok(claims)
}

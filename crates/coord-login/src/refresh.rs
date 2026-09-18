//! Refresh families (design Sections 8.2, 20.3): a session created by a
//! browser or device login is bound to a replicated refresh family whose
//! current secret commitment rotates on every accepted refresh. A
//! presented secret that is not the current one (a retired secret being
//! reused, or a lost rotation response followed by a retry) revokes the
//! family and retires its session at that position: the only recovery
//! is a fresh interactive login. Secrets never persist here; the
//! replicated rows hold commitments only.

use coord_authn::ClockHealth;
use coord_state::plan::Outcome;
use coord_state::policy::{GrantRecord, SessionRecord};
use coord_state::{InternalCommand, Response};
use coord_sts::{CreatorError, ExchangeError, ExchangeResponse, SessionCreator, Sts};
use coord_types::identity::{Digest32, HashDomain};
use coord_types::ids::{NamespaceId, SessionId};

/// Reads of replicated session and grant rows the broker needs.
pub trait SessionReader {
    /// The session record, if any.
    fn session(&self, session: &SessionId) -> Result<Option<SessionRecord>, CreatorError>;
    /// The grant record of a family, if any.
    fn grant(&self, commitment: &Digest32) -> Result<Option<GrantRecord>, CreatorError>;
}

/// What the broker needs from replicated state: commands and reads.
pub trait SessionBackend: SessionCreator + SessionReader {}

impl<T: SessionCreator + SessionReader> SessionBackend for T {}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
        out[i] = u8::from_str_radix(std::str::from_utf8(chunk).ok()?, 16).ok()?;
    }
    Some(out)
}

/// The commitment of a refresh secret.
pub fn secret_commitment(secret: &str) -> Digest32 {
    HashDomain::AuthGrantCommitment.digest(&[b"refresh-secret", secret.as_bytes()])
}

/// A fresh family: its first secret and the family commitment (the row
/// key, which is the first secret's commitment).
pub fn new_family(entropy: &[u8; 32]) -> (String, Digest32) {
    let secret = hex(entropy);
    let commitment = secret_commitment(&secret);
    (secret, commitment)
}

/// The refresh token a client holds: family and current secret.
pub fn refresh_token(family: &Digest32, secret: &str) -> String {
    format!("{}.{}", hex(&family.0), secret)
}

/// Parse a refresh token.
pub fn parse_refresh_token(token: &str) -> Option<(Digest32, String)> {
    let (family, secret) = token.split_once('.')?;
    let family = Digest32(unhex(family)?);
    if secret.len() != 64 || !secret.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    Some((family, secret.to_string()))
}

/// Why a refresh failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RefreshError {
    /// Not a refresh token.
    Malformed,
    /// No such family (or revoked).
    UnknownFamily,
    /// A retired secret was presented: the family is revoked and its
    /// session retired; log in again.
    FamilyRevoked,
    /// The session is gone.
    SessionRetired,
    /// Replicated state cannot be reached.
    Unavailable,
    /// Signing.
    Exchange(ExchangeError),
}

/// Rotate the family and sign a new service token for its session.
pub fn refresh(
    token: &str,
    namespace: NamespaceId,
    clock: &ClockHealth,
    entropy: &[u8; 32],
    sts: &mut Sts,
    backend: &mut dyn SessionBackend,
) -> Result<ExchangeResponse, RefreshError> {
    let (family, secret) = parse_refresh_token(token).ok_or(RefreshError::Malformed)?;
    let next_secret = hex(entropy);
    let response: Response = backend
        .create(InternalCommand::AdvanceRefresh {
            namespace,
            family,
            presented: secret_commitment(&secret),
            next: secret_commitment(&next_secret),
        })
        .map_err(|_| RefreshError::Unavailable)?;
    match response.outcome {
        Outcome::RefreshAdvanced { .. } => {}
        Outcome::ErrRefreshReuse => return Err(RefreshError::FamilyRevoked),
        _ => return Err(RefreshError::UnknownFamily),
    }
    let grant = backend
        .grant(&family)
        .map_err(|_| RefreshError::Unavailable)?
        .ok_or(RefreshError::UnknownFamily)?;
    let session = grant.session.ok_or(RefreshError::UnknownFamily)?;
    let record = backend
        .session(&session)
        .map_err(|_| RefreshError::Unavailable)?
        .ok_or(RefreshError::SessionRetired)?;
    let mut issued = sts
        .renew(session, &record, clock)
        .map_err(RefreshError::Exchange)?;
    issued.refresh_token = Some(refresh_token(&family, &next_secret));
    Ok(issued)
}

/// Log out: retire the family's session (the family stays bound to the
/// retired session and can never rotate again).
pub fn logout(
    token: &str,
    namespace: NamespaceId,
    backend: &mut dyn SessionBackend,
) -> Result<(), RefreshError> {
    let (family, _) = parse_refresh_token(token).ok_or(RefreshError::Malformed)?;
    let grant = backend
        .grant(&family)
        .map_err(|_| RefreshError::Unavailable)?
        .ok_or(RefreshError::UnknownFamily)?;
    let session = grant.session.ok_or(RefreshError::UnknownFamily)?;
    let response = backend
        .create(InternalCommand::RetireSession { namespace, session })
        .map_err(|_| RefreshError::Unavailable)?;
    match response.outcome {
        Outcome::SessionRetired => Ok(()),
        _ => Err(RefreshError::SessionRetired),
    }
}

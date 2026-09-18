//! The service code flow (design Sections 8.1, 8.2, 20.3), sans-I/O.
//!
//! ```text
//! CLI ──start(client, redirect, S256 challenge, state)──▶ broker: pending txn
//!                                                          (its own upstream state, nonce, PKCE)
//! browser ◀── upstream authorization ──▶ IdP ──callback(code, state)──▶ broker
//! broker: exchange upstream code (openidconnect) ──approve(identity)──▶ service code
//!         commitment = H(code) ordered as a pending grant
//! CLI ◀── redirect(code, state) ── browser
//! CLI ──redeem(code, verifier, client, redirect)──▶ broker: consume locally,
//!         then the receipt with the commitment: one session at most
//! ```

use std::collections::{BTreeMap, VecDeque};
use std::fmt;

use coord_types::identity::{Digest32, HashDomain};
use oauth2::{PkceCodeChallenge, PkceCodeVerifier};

/// A registered public client.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Registration {
    /// Client identifier.
    pub client_id: String,
    /// Exact redirect URIs (loopback listeners of the CLI).
    pub redirect_uris: Vec<String>,
    /// Upstream configuration name the client logs in through.
    pub upstream: String,
}

/// Bounds of pending logins.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LoginLimits {
    /// Pending transactions at once.
    pub max_pending: usize,
    /// Seconds a started login may wait for the upstream callback.
    pub pending_ttl_secs: u64,
    /// Seconds a service code may wait for redemption.
    pub code_ttl_secs: u64,
}

impl Default for LoginLimits {
    fn default() -> Self {
        LoginLimits {
            max_pending: 1024,
            pending_ttl_secs: 600,
            code_ttl_secs: 120,
        }
    }
}

/// What the CLI sends to start a login.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StartRequest {
    /// Client identifier.
    pub client_id: String,
    /// The exact redirect URI of this login.
    pub redirect_uri: String,
    /// PKCE code challenge.
    pub code_challenge: String,
    /// Must be `S256`.
    pub code_challenge_method: String,
    /// Client state, echoed on the redirect.
    pub state: String,
}

/// A started login: the upstream authorization parameters the broker
/// uses (never shown to the CLI).
#[derive(Clone, PartialEq, Eq)]
pub struct Started {
    /// Transaction identity.
    pub txn: Digest32,
    /// Upstream configuration name.
    pub upstream: String,
    /// Upstream `state`.
    pub upstream_state: String,
    /// Upstream `nonce`.
    pub upstream_nonce: String,
    /// The broker's PKCE challenge for the upstream request.
    pub upstream_challenge: PkceCodeChallenge,
}

impl fmt::Debug for Started {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Started")
            .field("upstream", &self.upstream)
            .field("secrets", &"<redacted>")
            .finish()
    }
}

/// What the upstream leg verified.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpstreamIdentity {
    /// Exact issuer.
    pub issuer: String,
    /// Subject.
    pub subject: String,
    /// Audiences of the ID token.
    pub audiences: Vec<String>,
    /// `azp`, when present.
    pub authorized_party: Option<String>,
    /// ID token expiry (unix seconds).
    pub expires_at: u64,
}

/// An approved login: where the browser goes and what to order.
#[derive(Clone, PartialEq, Eq)]
pub struct Approved {
    /// Transaction.
    pub txn: Digest32,
    /// The client's redirect with `code` and `state`.
    pub redirect: String,
    /// Commitment of the service code (order it as a pending grant).
    pub commitment: Digest32,
}

impl fmt::Debug for Approved {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Approved")
            .field("commitment", &self.commitment)
            .field("redirect", &"<redacted>")
            .finish()
    }
}

/// What the CLI sends to redeem a service code.
#[derive(Clone, PartialEq, Eq)]
pub struct RedeemRequest {
    /// The service code.
    pub code: String,
    /// PKCE verifier.
    pub code_verifier: String,
    /// Client identifier.
    pub client_id: String,
    /// The redirect URI the login started with.
    pub redirect_uri: String,
}

impl fmt::Debug for RedeemRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedeemRequest")
            .field("client_id", &self.client_id)
            .field("redirect_uri", &self.redirect_uri)
            .field("secrets", &"<redacted>")
            .finish()
    }
}

/// A redeemed code: the identity to admit and the commitment to consume
/// with the receipt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Redeemed {
    /// Transaction.
    pub txn: Digest32,
    /// Grant commitment.
    pub commitment: Digest32,
    /// Verified upstream identity.
    pub identity: UpstreamIdentity,
    /// Upstream configuration name.
    pub upstream: String,
}

/// Why a step was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LoginError {
    /// Unknown client.
    UnknownClient,
    /// The redirect is not registered for the client.
    RedirectNotRegistered,
    /// Only `S256` is accepted.
    UnsupportedChallengeMethod,
    /// The challenge or verifier is malformed.
    MalformedPkce,
    /// The pending bound is reached.
    TooManyPending,
    /// No transaction for that upstream state (a stale, foreign or
    /// replayed callback, or a broker restart).
    UnknownTransaction,
    /// The transaction waited too long.
    TransactionExpired,
    /// The upstream identity's issuer is not the transaction's.
    IssuerMismatch,
    /// `azp` missing with several audiences, or not the upstream client.
    AzpMismatch,
    /// The audience does not include the upstream client.
    AudienceMismatch,
    /// No such code, or already redeemed.
    UnknownCode,
    /// The code waited too long.
    CodeExpired,
    /// The verifier does not match the challenge.
    VerifierMismatch,
    /// The client is not the one that started the login.
    ClientMismatch,
    /// The redirect is not the one that started the login.
    RedirectMismatch,
}

/// Application-level authorized-party policy: with several audiences
/// `azp` is required; when present it must be the upstream client.
pub fn azp_policy(identity: &UpstreamIdentity, client_id: &str) -> Result<(), LoginError> {
    if !identity.audiences.iter().any(|a| a == client_id) {
        return Err(LoginError::AudienceMismatch);
    }
    match (&identity.authorized_party, identity.audiences.len()) {
        (Some(azp), _) if azp != client_id => Err(LoginError::AzpMismatch),
        (None, n) if n > 1 => Err(LoginError::AzpMismatch),
        _ => Ok(()),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    Started,
    Approved,
}

struct Txn {
    client_id: String,
    redirect_uri: String,
    challenge: String,
    state: String,
    upstream: String,
    upstream_state: String,
    upstream_nonce: String,
    upstream_verifier: String,
    created_at: u64,
    stage: Stage,
    identity: Option<UpstreamIdentity>,
    commitment: Option<Digest32>,
    approved_at: u64,
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The broker's service-login state.
pub struct ServiceLogin {
    limits: LoginLimits,
    registrations: BTreeMap<String, Registration>,
    /// Upstream configuration name to its client identifier.
    upstream_clients: BTreeMap<String, String>,
    pending: BTreeMap<Digest32, Txn>,
    order: VecDeque<Digest32>,
    by_upstream_state: BTreeMap<String, Digest32>,
    codes: BTreeMap<Digest32, Digest32>,
    /// Logins started.
    pub started: u64,
    /// Logins approved.
    pub approved: u64,
    /// Codes redeemed.
    pub redeemed: u64,
}

impl ServiceLogin {
    /// A broker over `registrations` and the upstream client identifiers
    /// by configuration name.
    pub fn new(
        limits: LoginLimits,
        registrations: Vec<Registration>,
        upstream_clients: BTreeMap<String, String>,
    ) -> Self {
        ServiceLogin {
            limits,
            registrations: registrations
                .into_iter()
                .map(|r| (r.client_id.clone(), r))
                .collect(),
            upstream_clients,
            pending: BTreeMap::new(),
            order: VecDeque::new(),
            by_upstream_state: BTreeMap::new(),
            codes: BTreeMap::new(),
            started: 0,
            approved: 0,
            redeemed: 0,
        }
    }

    /// Pending transactions.
    pub fn pending(&self) -> usize {
        self.pending.len()
    }

    /// Start a login at `now` with `entropy` from the world.
    pub fn start(
        &mut self,
        now: u64,
        request: &StartRequest,
        entropy: &[u8; 32],
    ) -> Result<Started, LoginError> {
        let registration = self
            .registrations
            .get(&request.client_id)
            .cloned()
            .ok_or(LoginError::UnknownClient)?;
        if !registration.redirect_uris.contains(&request.redirect_uri) {
            return Err(LoginError::RedirectNotRegistered);
        }
        if request.code_challenge_method != "S256" {
            return Err(LoginError::UnsupportedChallengeMethod);
        }
        if !(43..=128).contains(&request.code_challenge.len())
            || !request
                .code_challenge
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return Err(LoginError::MalformedPkce);
        }
        self.prune(now);
        if self.pending.len() >= self.limits.max_pending {
            return Err(LoginError::TooManyPending);
        }
        let txn = HashDomain::AuthGrantCommitment.digest(&[b"login-txn", entropy]);
        let upstream_state = hex(&entropy[..16]);
        let upstream_nonce = hex(&entropy[16..]);
        let (challenge, verifier) = PkceCodeChallenge::new_random_sha256();
        self.pending.insert(
            txn,
            Txn {
                client_id: request.client_id.clone(),
                redirect_uri: request.redirect_uri.clone(),
                challenge: request.code_challenge.clone(),
                state: request.state.clone(),
                upstream: registration.upstream.clone(),
                upstream_state: upstream_state.clone(),
                upstream_nonce: upstream_nonce.clone(),
                upstream_verifier: verifier.secret().clone(),
                created_at: now,
                stage: Stage::Started,
                identity: None,
                commitment: None,
                approved_at: 0,
            },
        );
        self.order.push_back(txn);
        self.by_upstream_state.insert(upstream_state.clone(), txn);
        self.started += 1;
        Ok(Started {
            txn,
            upstream: registration.upstream.clone(),
            upstream_state,
            upstream_nonce,
            upstream_challenge: challenge,
        })
    }

    /// The transaction an upstream callback state names, with the
    /// broker's PKCE verifier and nonce for the exchange.
    pub fn upstream_exchange(
        &self,
        upstream_state: &str,
    ) -> Result<(Digest32, String, String), LoginError> {
        let txn = *self
            .by_upstream_state
            .get(upstream_state)
            .ok_or(LoginError::UnknownTransaction)?;
        let t = self
            .pending
            .get(&txn)
            .ok_or(LoginError::UnknownTransaction)?;
        if t.stage != Stage::Started {
            return Err(LoginError::UnknownTransaction);
        }
        Ok((txn, t.upstream_verifier.clone(), t.upstream.clone()))
    }

    /// The upstream nonce of the transaction a callback state names.
    pub fn upstream_nonce(&self, upstream_state: &str) -> Option<String> {
        let txn = self.by_upstream_state.get(upstream_state)?;
        self.pending.get(txn).map(|t| t.upstream_nonce.clone())
    }

    /// The upstream leg verified `identity` for the callback carrying
    /// `upstream_state`: approve, mint the service code.
    pub fn approve(
        &mut self,
        now: u64,
        upstream_state: &str,
        identity: UpstreamIdentity,
        expected_issuer: &str,
        entropy: &[u8; 32],
    ) -> Result<Approved, LoginError> {
        self.prune(now);
        let txn = *self
            .by_upstream_state
            .get(upstream_state)
            .ok_or(LoginError::UnknownTransaction)?;
        let t = self
            .pending
            .get_mut(&txn)
            .ok_or(LoginError::UnknownTransaction)?;
        if t.stage != Stage::Started {
            return Err(LoginError::UnknownTransaction);
        }
        if identity.issuer != expected_issuer {
            return Err(LoginError::IssuerMismatch);
        }
        let upstream_client = self
            .upstream_clients
            .get(&t.upstream)
            .ok_or(LoginError::UnknownTransaction)?;
        azp_policy(&identity, upstream_client)?;
        let code = hex(entropy);
        let commitment = HashDomain::AuthGrantCommitment.digest(&[code.as_bytes()]);
        t.stage = Stage::Approved;
        t.identity = Some(identity);
        t.commitment = Some(commitment);
        t.approved_at = now;
        // The upstream state is single use.
        let redirect = format!(
            "{}?code={}&state={}",
            t.redirect_uri,
            code,
            urlencode(&t.state)
        );
        self.by_upstream_state.remove(upstream_state);
        self.codes.insert(commitment, txn);
        self.approved += 1;
        Ok(Approved {
            txn,
            redirect,
            commitment,
        })
    }

    /// Redeem a service code: the verifier, client and redirect must be
    /// the login's; the code is consumed here and its commitment is what
    /// replicated state consumes with the receipt.
    pub fn redeem(&mut self, now: u64, request: &RedeemRequest) -> Result<Redeemed, LoginError> {
        let commitment = HashDomain::AuthGrantCommitment.digest(&[request.code.as_bytes()]);
        let txn = *self.codes.get(&commitment).ok_or(LoginError::UnknownCode)?;
        let t = self.pending.get(&txn).ok_or(LoginError::UnknownCode)?;
        if t.stage != Stage::Approved {
            return Err(LoginError::UnknownCode);
        }
        if now.saturating_sub(t.approved_at) > self.limits.code_ttl_secs {
            self.remove(txn);
            return Err(LoginError::CodeExpired);
        }
        self.prune(now);
        let t = self.pending.get(&txn).ok_or(LoginError::UnknownCode)?;
        if !(43..=128).contains(&request.code_verifier.len()) {
            return Err(LoginError::MalformedPkce);
        }
        let derived = PkceCodeChallenge::from_code_verifier_sha256(&PkceCodeVerifier::new(
            request.code_verifier.clone(),
        ));
        if derived.as_str() != t.challenge {
            return Err(LoginError::VerifierMismatch);
        }
        if request.client_id != t.client_id {
            return Err(LoginError::ClientMismatch);
        }
        if request.redirect_uri != t.redirect_uri {
            return Err(LoginError::RedirectMismatch);
        }
        let t = self.remove(txn).expect("present");
        self.redeemed += 1;
        Ok(Redeemed {
            txn,
            commitment,
            identity: t.identity.expect("approved"),
            upstream: t.upstream,
        })
    }

    fn remove(&mut self, txn: Digest32) -> Option<Txn> {
        let t = self.pending.remove(&txn)?;
        self.by_upstream_state.remove(&t.upstream_state);
        if let Some(c) = t.commitment {
            self.codes.remove(&c);
        }
        self.order.retain(|x| *x != txn);
        Some(t)
    }

    /// Drop transactions past their bounds; returns how many.
    pub fn prune(&mut self, now: u64) -> usize {
        let expired: Vec<Digest32> = self
            .pending
            .iter()
            .filter(|(_, t)| match t.stage {
                Stage::Started => now.saturating_sub(t.created_at) > self.limits.pending_ttl_secs,
                Stage::Approved => now.saturating_sub(t.approved_at) > self.limits.code_ttl_secs,
            })
            .map(|(id, _)| *id)
            .collect();
        for id in &expired {
            self.remove(*id);
        }
        expired.len()
    }
}

fn urlencode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.' || b == b'~' {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

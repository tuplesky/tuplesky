//! Device authorization (RFC 8628; design Sections 8.1, 8.2, 20.3): the
//! CLI receives a secret device code and a short user code; the user
//! enters the user code on the verification page and authenticates
//! through the existing upstream browser login; the CLI polls with the
//! device code, honouring `interval` and `slow_down`, until the grant is
//! approved, denied or expired. The user code alone grants nothing and
//! is not a secret; the device code never appears in a URL; a grant is
//! taken by exactly one poll and its commitment is consumed exactly
//! once by replicated state. No upstream device endpoint is assumed.

use std::collections::BTreeMap;
use std::fmt;

use coord_types::identity::{Digest32, HashDomain};
use oauth2::PkceCodeChallenge;

use crate::service::{Redeemed, Registration, Started, UpstreamIdentity, azp_policy};

/// Bounds of device grants.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeviceLimits {
    /// Grants pending at once.
    pub max_pending: usize,
    /// Seconds a grant lives.
    pub code_ttl_secs: u64,
    /// Initial polling interval.
    pub interval_secs: u64,
    /// Polls per grant at most.
    pub max_polls: u32,
    /// User-code lookups per window (flood bound).
    pub max_attempts_per_window: u32,
    /// The lookup window.
    pub attempt_window_secs: u64,
}

impl Default for DeviceLimits {
    fn default() -> Self {
        DeviceLimits {
            max_pending: 1024,
            code_ttl_secs: 600,
            interval_secs: 5,
            max_polls: 200,
            max_attempts_per_window: 30,
            attempt_window_secs: 60,
        }
    }
}

/// What the CLI receives (RFC 8628 Section 3.2).
#[derive(Clone, PartialEq, Eq)]
pub struct DeviceAuthorization {
    /// The secret the CLI polls with.
    pub device_code: String,
    /// The short code the user types.
    pub user_code: String,
    /// Where the user goes.
    pub verification_uri: String,
    /// The same, with the user code filled in (no secret).
    pub verification_uri_complete: String,
    /// Seconds until the grant expires.
    pub expires_in: u64,
    /// Minimum seconds between polls.
    pub interval: u64,
}

impl fmt::Debug for DeviceAuthorization {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeviceAuthorization")
            .field("device_code", &"<redacted>")
            .field("user_code", &self.user_code)
            .field("verification_uri", &self.verification_uri)
            .finish()
    }
}

/// What the verification page shows before approval.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeviceDisplay {
    /// Client identifier.
    pub client_id: String,
    /// Cluster the session would be for.
    pub cluster: String,
    /// Seconds before the grant expires.
    pub expires_in: u64,
}

/// A poll's answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Poll {
    /// Not decided yet.
    Pending,
    /// Polled too soon; the interval grew.
    SlowDown,
    /// The user denied.
    Denied,
    /// The grant expired or is spent.
    Expired,
    /// Approved and taken by this poll.
    Approved(Redeemed),
}

/// Why a step was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeviceError {
    /// Unknown client.
    UnknownClient,
    /// The pending bound is reached (or the user code collided).
    TooManyPending,
    /// No pending grant has that user code.
    UnknownUserCode,
    /// Too many user-code lookups in the window.
    TooManyAttempts,
    /// The grant was already approved or denied.
    AlreadyDecided,
    /// No browser leg for that upstream state.
    UnknownTransaction,
    /// The upstream identity's issuer is not the expected one.
    IssuerMismatch,
    /// Authorized-party or audience policy.
    Policy(crate::service::LoginError),
    /// No grant for that device code.
    UnknownDeviceCode,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum State {
    Pending,
    Browser {
        upstream_state: String,
        upstream_nonce: String,
        upstream_verifier: String,
    },
    /// The upstream leg verified an identity, but the grant is not
    /// ordered yet. A poll sees this as pending: publishing approval
    /// before the grant is committed would hand out a session for a
    /// grant that replicated state never accepted.
    Verified(UpstreamIdentity),
    Approved(UpstreamIdentity),
    Denied,
    Taken,
}

struct Grant {
    client_id: String,
    upstream: String,
    user_code: String,
    created_at: u64,
    last_poll: Option<u64>,
    interval: u64,
    polls: u32,
    state: State,
}

const ALPHABET: &[u8; 32] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789";

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The user code of `entropy`: eight unambiguous characters in two
/// groups, derived separately from the device code.
pub fn user_code(entropy: &[u8; 32]) -> String {
    let digest = HashDomain::AuthGrantCommitment.digest(&[b"user-code", entropy]);
    let mut out = String::new();
    for (i, b) in digest.0.iter().take(8).enumerate() {
        if i == 4 {
            out.push('-');
        }
        out.push(ALPHABET[(*b as usize) % 32] as char);
    }
    out
}

/// Normalize what a user typed.
pub fn normalize_user_code(input: &str) -> String {
    let mut out = String::new();
    for c in input.chars().filter(|c| c.is_ascii_alphanumeric()) {
        if out.len() == 4 {
            out.push('-');
        }
        out.push(c.to_ascii_uppercase());
    }
    out
}

/// The device-grant state of the broker.
pub struct DeviceLogin {
    limits: DeviceLimits,
    registrations: BTreeMap<String, Registration>,
    upstream_clients: BTreeMap<String, String>,
    verification_uri: String,
    cluster: String,
    grants: BTreeMap<Digest32, Grant>,
    by_user_code: BTreeMap<String, Digest32>,
    by_upstream_state: BTreeMap<String, Digest32>,
    window_start: u64,
    attempts: u32,
    /// Grants issued.
    pub issued: u64,
    /// Grants taken by a poll.
    pub taken: u64,
}

impl DeviceLogin {
    /// A device-grant broker for `cluster` whose verification page is
    /// `verification_uri`.
    pub fn new(
        limits: DeviceLimits,
        registrations: Vec<Registration>,
        upstream_clients: BTreeMap<String, String>,
        verification_uri: String,
        cluster: String,
    ) -> Self {
        DeviceLogin {
            limits,
            registrations: registrations
                .into_iter()
                .map(|r| (r.client_id.clone(), r))
                .collect(),
            upstream_clients,
            verification_uri,
            cluster,
            grants: BTreeMap::new(),
            by_user_code: BTreeMap::new(),
            by_upstream_state: BTreeMap::new(),
            window_start: 0,
            attempts: 0,
            issued: 0,
            taken: 0,
        }
    }

    /// Pending grants.
    pub fn pending(&self) -> usize {
        self.grants.len()
    }

    /// Issue a device and user code for `client_id` at `now`.
    pub fn authorize(
        &mut self,
        now: u64,
        client_id: &str,
        entropy: &[u8; 32],
    ) -> Result<DeviceAuthorization, DeviceError> {
        let registration = self
            .registrations
            .get(client_id)
            .cloned()
            .ok_or(DeviceError::UnknownClient)?;
        self.prune(now);
        if self.grants.len() >= self.limits.max_pending {
            return Err(DeviceError::TooManyPending);
        }
        let device_code = hex(entropy);
        let commitment = HashDomain::AuthGrantCommitment.digest(&[device_code.as_bytes()]);
        let user_code = user_code(entropy);
        if self.by_user_code.contains_key(&user_code) || self.grants.contains_key(&commitment) {
            return Err(DeviceError::TooManyPending);
        }
        self.grants.insert(
            commitment,
            Grant {
                client_id: client_id.to_string(),
                upstream: registration.upstream,
                user_code: user_code.clone(),
                created_at: now,
                last_poll: None,
                interval: self.limits.interval_secs,
                polls: 0,
                state: State::Pending,
            },
        );
        self.by_user_code.insert(user_code.clone(), commitment);
        self.issued += 1;
        Ok(DeviceAuthorization {
            device_code,
            verification_uri: self.verification_uri.clone(),
            verification_uri_complete: format!("{}?user_code={}", self.verification_uri, user_code),
            user_code,
            expires_in: self.limits.code_ttl_secs,
            interval: self.limits.interval_secs,
        })
    }

    fn attempt(&mut self, now: u64) -> Result<(), DeviceError> {
        if now.saturating_sub(self.window_start) >= self.limits.attempt_window_secs {
            self.window_start = now;
            self.attempts = 0;
        }
        if self.attempts >= self.limits.max_attempts_per_window {
            return Err(DeviceError::TooManyAttempts);
        }
        self.attempts += 1;
        Ok(())
    }

    /// What the verification page shows for a typed user code (rate
    /// limited: guessing is bounded).
    pub fn lookup(&mut self, now: u64, typed: &str) -> Result<DeviceDisplay, DeviceError> {
        self.attempt(now)?;
        self.prune(now);
        let code = normalize_user_code(typed);
        let id = *self
            .by_user_code
            .get(&code)
            .ok_or(DeviceError::UnknownUserCode)?;
        let g = &self.grants[&id];
        if !matches!(g.state, State::Pending | State::Browser { .. }) {
            return Err(DeviceError::AlreadyDecided);
        }
        Ok(DeviceDisplay {
            client_id: g.client_id.clone(),
            cluster: self.cluster.clone(),
            expires_in: (g.created_at + self.limits.code_ttl_secs).saturating_sub(now),
        })
    }

    /// The user starts the browser leg for a typed user code: the same
    /// upstream login, with this grant's own state, nonce and PKCE.
    pub fn begin_browser(
        &mut self,
        now: u64,
        typed: &str,
        entropy: &[u8; 32],
    ) -> Result<Started, DeviceError> {
        self.attempt(now)?;
        self.prune(now);
        let code = normalize_user_code(typed);
        let id = *self
            .by_user_code
            .get(&code)
            .ok_or(DeviceError::UnknownUserCode)?;
        let g = self.grants.get_mut(&id).expect("indexed");
        if !matches!(g.state, State::Pending | State::Browser { .. }) {
            return Err(DeviceError::AlreadyDecided);
        }
        if let State::Browser { upstream_state, .. } = &g.state {
            self.by_upstream_state.remove(upstream_state);
        }
        let upstream_state = hex(&entropy[..16]);
        let upstream_nonce = hex(&entropy[16..]);
        let (challenge, verifier) = PkceCodeChallenge::new_random_sha256();
        g.state = State::Browser {
            upstream_state: upstream_state.clone(),
            upstream_nonce: upstream_nonce.clone(),
            upstream_verifier: verifier.secret().clone(),
        };
        let upstream = g.upstream.clone();
        self.by_upstream_state.insert(upstream_state.clone(), id);
        Ok(Started {
            txn: id,
            upstream,
            upstream_state,
            upstream_nonce,
            upstream_challenge: challenge,
        })
    }

    /// The exchange parameters for a callback state: the broker's PKCE
    /// verifier, the upstream name and the nonce.
    pub fn upstream_exchange(
        &self,
        upstream_state: &str,
    ) -> Result<(String, String, String), DeviceError> {
        let id = self
            .by_upstream_state
            .get(upstream_state)
            .ok_or(DeviceError::UnknownTransaction)?;
        match &self
            .grants
            .get(id)
            .ok_or(DeviceError::UnknownTransaction)?
            .state
        {
            State::Browser {
                upstream_verifier,
                upstream_nonce,
                ..
            } => Ok((
                upstream_verifier.clone(),
                self.grants[id].upstream.clone(),
                upstream_nonce.clone(),
            )),
            _ => Err(DeviceError::UnknownTransaction),
        }
    }

    /// The upstream leg verified `identity`: approve the grant; returns
    /// the commitment to order as a pending grant.
    pub fn complete_browser(
        &mut self,
        now: u64,
        upstream_state: &str,
        identity: UpstreamIdentity,
        expected_issuer: &str,
    ) -> Result<Digest32, DeviceError> {
        self.prune(now);
        let id = *self
            .by_upstream_state
            .get(upstream_state)
            .ok_or(DeviceError::UnknownTransaction)?;
        let g = self
            .grants
            .get_mut(&id)
            .ok_or(DeviceError::UnknownTransaction)?;
        if !matches!(g.state, State::Browser { .. }) {
            return Err(DeviceError::UnknownTransaction);
        }
        if identity.issuer != expected_issuer {
            return Err(DeviceError::IssuerMismatch);
        }
        let client = self
            .upstream_clients
            .get(&g.upstream)
            .ok_or(DeviceError::UnknownTransaction)?;
        azp_policy(&identity, client).map_err(DeviceError::Policy)?;
        // Verified, not approved: the poll keeps waiting until the grant
        // is committed, so a failed commit leaves nothing to redeem.
        g.state = State::Verified(identity);
        self.by_upstream_state.remove(upstream_state);
        Ok(id)
    }

    /// The grant `id` is committed in replicated state: publish the
    /// approval so the device's next poll takes it.
    pub fn publish_browser(&mut self, now: u64, id: Digest32) -> Result<(), DeviceError> {
        self.prune(now);
        let g = self
            .grants
            .get_mut(&id)
            .ok_or(DeviceError::UnknownTransaction)?;
        match std::mem::replace(&mut g.state, State::Denied) {
            State::Verified(identity) => {
                g.state = State::Approved(identity);
                Ok(())
            }
            other => {
                g.state = other;
                Err(DeviceError::UnknownTransaction)
            }
        }
    }

    /// The upstream leg refused the login for `upstream_state`: the
    /// grant is denied, so the device's poll learns of it instead of
    /// waiting out the code's lifetime.
    pub fn deny_upstream(&mut self, now: u64, upstream_state: &str) -> Result<(), DeviceError> {
        self.prune(now);
        let id = *self
            .by_upstream_state
            .get(upstream_state)
            .ok_or(DeviceError::UnknownTransaction)?;
        let g = self
            .grants
            .get_mut(&id)
            .ok_or(DeviceError::UnknownTransaction)?;
        if !matches!(g.state, State::Browser { .. }) {
            return Err(DeviceError::UnknownTransaction);
        }
        g.state = State::Denied;
        self.by_upstream_state.remove(upstream_state);
        Ok(())
    }

    /// The user denied a typed user code.
    pub fn deny(&mut self, now: u64, typed: &str) -> Result<(), DeviceError> {
        self.attempt(now)?;
        let code = normalize_user_code(typed);
        let id = *self
            .by_user_code
            .get(&code)
            .ok_or(DeviceError::UnknownUserCode)?;
        let g = self.grants.get_mut(&id).expect("indexed");
        match &g.state {
            State::Pending | State::Browser { .. } => {
                if let State::Browser { upstream_state, .. } = &g.state {
                    self.by_upstream_state.remove(upstream_state);
                }
                g.state = State::Denied;
                Ok(())
            }
            _ => Err(DeviceError::AlreadyDecided),
        }
    }

    /// The CLI polls with its device code.
    pub fn poll(&mut self, now: u64, device_code: &str) -> Result<Poll, DeviceError> {
        let id = HashDomain::AuthGrantCommitment.digest(&[device_code.as_bytes()]);
        let Some(g) = self.grants.get_mut(&id) else {
            return Err(DeviceError::UnknownDeviceCode);
        };
        if now.saturating_sub(g.created_at) > self.limits.code_ttl_secs {
            self.remove(id);
            return Ok(Poll::Expired);
        }
        g.polls += 1;
        if g.polls > self.limits.max_polls {
            self.remove(id);
            return Ok(Poll::Expired);
        }
        if let Some(last) = g.last_poll
            && now.saturating_sub(last) < g.interval
        {
            g.last_poll = Some(now);
            g.interval = g.interval.saturating_add(5);
            return Ok(Poll::SlowDown);
        }
        g.last_poll = Some(now);
        match &g.state {
            // Verified but not yet committed is still pending: the
            // device learns of approval only once the grant is ordered.
            State::Pending | State::Browser { .. } | State::Verified(_) => Ok(Poll::Pending),
            State::Denied => {
                self.remove(id);
                Ok(Poll::Denied)
            }
            State::Taken => {
                self.remove(id);
                Ok(Poll::Expired)
            }
            State::Approved(identity) => {
                let redeemed = Redeemed {
                    txn: id,
                    commitment: id,
                    identity: identity.clone(),
                    upstream: g.upstream.clone(),
                };
                g.state = State::Taken;
                self.taken += 1;
                Ok(Poll::Approved(redeemed))
            }
        }
    }

    fn remove(&mut self, id: Digest32) {
        if let Some(g) = self.grants.remove(&id) {
            self.by_user_code.remove(&g.user_code);
            if let State::Browser { upstream_state, .. } = &g.state {
                self.by_upstream_state.remove(upstream_state);
            }
        }
    }

    /// Drop expired grants; returns how many.
    pub fn prune(&mut self, now: u64) -> usize {
        let expired: Vec<Digest32> = self
            .grants
            .iter()
            .filter(|(_, g)| now.saturating_sub(g.created_at) > self.limits.code_ttl_secs)
            .map(|(id, _)| *id)
            .collect();
        for id in &expired {
            self.remove(*id);
        }
        expired.len()
    }
}

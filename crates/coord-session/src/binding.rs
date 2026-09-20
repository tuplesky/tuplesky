//! The persistent binding of one API connection (design Sections 9.3,
//! 11.5, 19.4): what the service token established, its validity, and
//! the rule that a rebind refreshes validity but never identity.

use coord_authn::ClockHealth;
use coord_collector::Caller;
use coord_core::capability::{
    AdmissionReceipt, AttestedAdmission, AttestedEstablishment, CredentialDeadline, VerifierToken,
};
use coord_state::policy::SessionRecord;
use coord_sts::{TokenError, verify_service_token};
use coord_types::RetryKey;
use coord_types::identity::Digest32;
use coord_types::ids::{
    ClientInstanceId, ClusterId, DomainId, NamespaceId, PrincipalId, RequestSequence, SessionId,
    TrustRuleId,
};
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
    /// Cluster this verifier belongs to. A receipt minted here names it,
    /// and a voter of another cluster admits nothing from it.
    pub cluster: ClusterId,
    /// Domain the sessions it establishes live in.
    pub domain: DomainId,
}

impl BindingConfig {
    /// The namespace a session establishment is planned in: the
    /// domain's own.
    ///
    /// Session and grant rows are domain-wide, so this names only the
    /// view the command is planned against, and it is derived rather
    /// than configured: every replica plans the same command, and a
    /// per-node setting here would be a per-node command.
    pub const fn namespace(&self) -> NamespaceId {
        NamespaceId(self.domain.0)
    }
}

/// One connection's binding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Binding {
    /// Session.
    pub session: SessionId,
    /// Principal (lowercase hex, as the token carries it).
    pub principal: String,
    /// The same principal as an identity. Parsed once, here, so that
    /// nothing downstream has to decide whether a hex string is one.
    pub principal_id: PrincipalId,
    /// Scope ceiling of the token (never wider than the session's).
    pub scope_ceiling: u32,
    /// Trust rule the credential was mapped under.
    pub trust_rule: TrustRuleId,
    /// Trust rule generation the session was admitted under.
    pub rule_generation: u64,
    /// The token's own identifier: the single-use receipt identity of
    /// the admission this binding attests.
    pub receipt_id: Digest32,
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

    /// The receipt this verified binding attests, for establishing the
    /// session the credential names.
    ///
    /// This is the authentication boundary: the credential was checked
    /// against the configured issuer, audience and keys, and what comes
    /// out says who was authenticated, under which trust rule at which
    /// generation, within what ceiling, and until when the credential
    /// admits new work. Every field is the verified credential's or the
    /// configuration's; none is a caller's.
    ///
    /// It does not say the session exists. Replicated execution rechecks
    /// the rule and its generation against current policy, refuses a
    /// receipt already consumed, and refuses a session identity that
    /// already exists -- so a second binding of the same session
    /// converges on the row the first wrote rather than replacing it.
    ///
    /// `admitted_at_ticks` is this boundary's own monotonic tick, not a
    /// time: it is not comparable across processes and no replica reads
    /// it. The credential's deadline is the time, and it is checked
    /// here, once.
    pub fn establishment(
        &self,
        config: &BindingConfig,
        admitted_at_ticks: u64,
    ) -> AdmissionReceipt {
        AdmissionReceipt::establishing(
            VerifierToken::for_boundary(),
            AttestedAdmission {
                cluster: config.cluster,
                domain: config.domain,
                session: self.session,
                rule_generation: self.rule_generation,
                scope_ceiling: self.scope_ceiling,
                receipt_id: self.receipt_id,
                admitted_at_ticks,
            },
            AttestedEstablishment {
                principal: self.principal_id,
                trust_rule: self.trust_rule,
                credential_valid_until: CredentialDeadline(self.expires_at),
            },
        )
    }

    /// The invocation identity of this binding's establishment.
    ///
    /// Derived from the credential, not chosen: the session it creates,
    /// and the credential's own identifier as the client instance. So
    /// a retry of the same bind is the same command and recovers its
    /// outcome, while a fresh credential for the same session is a
    /// different command -- which finds the session already there and
    /// converges on it, rather than replacing what was accepted.
    ///
    /// Sequence one is the session's first invocation, which is the one
    /// that creates it. A client instance of its own choosing that
    /// collided with this identity would find the retry key bound to
    /// another payload and be refused; it could not take its place.
    pub fn establishment_key(&self, config: &BindingConfig) -> RetryKey {
        let mut instance = [0u8; 16];
        instance.copy_from_slice(&self.receipt_id.0[..16]);
        RetryKey {
            cluster_id: config.cluster,
            domain_id: config.domain,
            session_id: self.session,
            client_instance_id: ClientInstanceId(instance),
            request_sequence: RequestSequence::new(1).expect("one is a sequence"),
        }
    }

    /// Whether `record` is a session this binding may be admitted under.
    ///
    /// A session's principal and its ceiling are immutable and its
    /// retirement is permanent, so this is the whole question: the
    /// credential describes the session that exists, or it describes
    /// another one and admits nothing here. A token may name a narrower
    /// scope than the session's ceiling -- that is narrowing, which is
    /// always allowed -- but never a wider one.
    pub fn agrees_with(&self, record: &SessionRecord) -> bool {
        record.active
            && record.principal == self.principal_id
            && record.trust_rule == self.trust_rule
            && record.rule_generation == self.rule_generation
            && self.scope_ceiling <= record.scope_ceiling
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
    /// The session the credential names exists and is not the session
    /// the credential describes -- another principal, another rule, a
    /// wider scope than its ceiling, or retired. A session's identity is
    /// immutable and its retirement permanent, so this binding admits
    /// nothing.
    SessionDisagrees,
    /// Replicated policy could not be read, or the establishment this
    /// binding needed did not happen. Nothing is bound; the caller may
    /// present the credential again.
    Unavailable,
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
        principal_id: PrincipalId(hex_bytes::<16>(&claims.sub).ok_or(BindError::Malformed)?),
        principal: claims.sub,
        scope_ceiling: claims.scope,
        trust_rule: TrustRuleId(hex_bytes::<16>(&claims.rule).ok_or(BindError::Malformed)?),
        rule_generation: claims.generation,
        receipt_id: Digest32(hex_bytes::<32>(&claims.jti).ok_or(BindError::Malformed)?),
        expires_at: claims.exp,
        bound_at: clock.now,
        rebinds: existing.map_or(0, |e| e.rebinds + 1),
    })
}

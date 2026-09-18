//! The exchange (design Sections 9.1-9.4, 20.2), sans-I/O.
//!
//! ```text
//! form ──validate──▶ verify assertion ──rules──▶ receipt ──ConsumeAdmission──▶ session ──sign──▶ token
//!                    (keys cached;              (deny by default,   (replicated: rechecks   (exp ≤ assertion,
//!                     NeedKeys is a             single use)          current policy,          ≤ rule, ≤ STS;
//!                     fetch, never a            never the token      consumes once)           scope ⊆ ceiling)
//!                     per-request IdP call)
//! ```
//!
//! Failure anywhere issues nothing. The receipt is the only thing that
//! leaves this module toward replicated state; the assertion and the
//! signing keys never do.

use coord_authn::{
    AdmissionLog, ClockHealth, Decision, TokenReview, TrustRuleConfig, VerifiedIdentity,
    VerifyError, WifVerifier, mint,
};
use coord_state::plan::Outcome;
use coord_state::policy::SessionRecord;
use coord_state::{InternalCommand, Response};
use coord_types::identity::{Digest32, HashDomain};
use coord_types::ids::NamespaceId;
use coord_types::ids::SessionId;
use serde::{Deserialize, Serialize};

use crate::keys::KeyRing;
use crate::token::{ServiceClaims, scope_bits, scope_string};

/// RFC 8693 grant type.
pub const GRANT_TYPE: &str = "urn:ietf:params:oauth:grant-type:token-exchange";
/// Subject token type: a JWT assertion.
pub const TOKEN_TYPE_JWT: &str = "urn:ietf:params:oauth:token-type:jwt";
/// Subject token type: an OIDC ID token.
pub const TOKEN_TYPE_ID: &str = "urn:ietf:params:oauth:token-type:id_token";
/// Issued token type.
pub const TOKEN_TYPE_ACCESS: &str = "urn:ietf:params:oauth:token-type:access_token";

/// The exchange request form (RFC 8693 Section 2.1).
#[derive(Clone, Deserialize)]
pub struct ExchangeForm {
    /// Must be [`GRANT_TYPE`].
    pub grant_type: String,
    /// The external assertion.
    pub subject_token: String,
    /// [`TOKEN_TYPE_JWT`] or [`TOKEN_TYPE_ID`].
    pub subject_token_type: String,
    /// Target audience (the cluster resource).
    pub audience: Option<String>,
    /// Target resource (the cluster resource).
    pub resource: Option<String>,
    /// Requested scope (space-separated action names).
    pub scope: Option<String>,
    /// Requested token type, when any (must be [`TOKEN_TYPE_ACCESS`]).
    pub requested_token_type: Option<String>,
}

impl std::fmt::Debug for ExchangeForm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExchangeForm")
            .field("grant_type", &self.grant_type)
            .field("subject_token", &"<redacted>")
            .field("subject_token_type", &self.subject_token_type)
            .field("audience", &self.audience)
            .field("resource", &self.resource)
            .field("scope", &self.scope)
            .finish()
    }
}

/// The successful response (RFC 8693 Section 2.2.1).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExchangeResponse {
    /// The service token.
    pub access_token: String,
    /// [`TOKEN_TYPE_ACCESS`].
    pub issued_token_type: String,
    /// `Bearer`.
    pub token_type: String,
    /// Seconds until expiry.
    pub expires_in: u64,
    /// Granted scope.
    pub scope: String,
    /// A refresh token, when a refresh family was bound to the session
    /// (browser and device logins; never for workload exchanges).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,
}

/// Why an exchange failed (RFC 6749 Section 5.2 vocabulary).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExchangeError {
    /// Malformed or unsupported request.
    InvalidRequest(&'static str),
    /// The assertion was not accepted, or policy denied it.
    InvalidGrant(&'static str),
    /// Not this cluster's resource.
    InvalidTarget,
    /// Requested scope outside the ceiling or unknown.
    InvalidScope,
    /// The issuer's configured keys must be fetched first (the HTTP layer
    /// does this once, then retries).
    KeysUnavailable {
        /// Issuer configuration name.
        name: String,
        /// Configured endpoint.
        jwks_url: String,
    },
    /// A dependency is down (issuer keys, replicated state): fail closed.
    Unavailable(&'static str),
    /// An internal failure.
    Server(&'static str),
}

impl ExchangeError {
    /// The RFC 6749 error code.
    pub const fn code(&self) -> &'static str {
        match self {
            ExchangeError::InvalidRequest(_) => "invalid_request",
            ExchangeError::InvalidGrant(_) => "invalid_grant",
            ExchangeError::InvalidTarget => "invalid_target",
            ExchangeError::InvalidScope => "invalid_scope",
            ExchangeError::KeysUnavailable { .. } | ExchangeError::Unavailable(_) => {
                "temporarily_unavailable"
            }
            ExchangeError::Server(_) => "server_error",
        }
    }

    /// The HTTP status.
    pub const fn status(&self) -> u16 {
        match self {
            ExchangeError::InvalidRequest(_)
            | ExchangeError::InvalidGrant(_)
            | ExchangeError::InvalidTarget
            | ExchangeError::InvalidScope => 400,
            ExchangeError::KeysUnavailable { .. } | ExchangeError::Unavailable(_) => 503,
            ExchangeError::Server(_) => 500,
        }
    }

    /// A bounded description (never token material).
    pub fn description(&self) -> &'static str {
        match self {
            ExchangeError::InvalidRequest(d)
            | ExchangeError::InvalidGrant(d)
            | ExchangeError::Unavailable(d)
            | ExchangeError::Server(d) => d,
            ExchangeError::InvalidTarget => "not this cluster's resource",
            ExchangeError::InvalidScope => "scope outside the ceiling",
            ExchangeError::KeysUnavailable { .. } => "issuer keys unavailable",
        }
    }
}

/// Why the replicated session command could not be applied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CreatorError {
    /// Replicated state cannot be reached now.
    Unavailable,
    /// The command was rejected before execution (bounded reason).
    Rejected(String),
}

/// The port to replicated state: submits the `ConsumeAdmission` command
/// and returns its established response.
pub trait SessionCreator {
    /// Submit and wait for the outcome.
    fn create(&mut self, command: InternalCommand) -> Result<Response, CreatorError>;
}

/// STS configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StsConfig {
    /// Issuer of service tokens.
    pub issuer: String,
    /// The cluster resource (audience of service tokens; the only
    /// accepted target).
    pub resource: String,
    /// Namespace session commands are planned in.
    pub namespace: NamespaceId,
    /// Longest service-token lifetime, whatever the rule allows.
    pub max_token_lifetime_secs: u64,
    /// Outstanding retry window of the created session's clients.
    pub session_window: u32,
    /// Largest accepted assertion.
    pub max_subject_token_bytes: usize,
}

/// The STS core.
pub struct Sts {
    config: StsConfig,
    verifier: WifVerifier,
    rules: Vec<TrustRuleConfig>,
    ring: KeyRing,
    log: AdmissionLog,
    /// Exchanges that issued a token.
    pub issued: u64,
}

impl Sts {
    /// An STS over `verifier`, `rules` and `ring`.
    pub fn new(
        config: StsConfig,
        verifier: WifVerifier,
        rules: Vec<TrustRuleConfig>,
        ring: KeyRing,
    ) -> Self {
        Sts {
            config,
            verifier,
            rules,
            ring,
            log: AdmissionLog::new(1024),
            issued: 0,
        }
    }

    /// Configuration.
    pub const fn config(&self) -> &StsConfig {
        &self.config
    }

    /// The verifier (key installation).
    pub const fn verifier_mut(&mut self) -> &mut WifVerifier {
        &mut self.verifier
    }

    /// The key ring.
    pub const fn ring(&self) -> &KeyRing {
        &self.ring
    }

    /// The key ring (rotation).
    pub const fn ring_mut(&mut self) -> &mut KeyRing {
        &mut self.ring
    }

    /// Replace the trust rules (configuration reload).
    pub fn set_rules(&mut self, rules: Vec<TrustRuleConfig>) {
        self.rules = rules;
    }

    /// The admission log.
    pub const fn log(&self) -> &AdmissionLog {
        &self.log
    }

    /// Perform one exchange at `clock` with `entropy` from the world.
    pub fn exchange(
        &mut self,
        form: &ExchangeForm,
        clock: &ClockHealth,
        entropy: &[u8; 32],
        review: Option<&TokenReview>,
        creator: &mut dyn SessionCreator,
    ) -> Result<ExchangeResponse, ExchangeError> {
        if form.grant_type != GRANT_TYPE {
            return Err(ExchangeError::InvalidRequest("unsupported grant_type"));
        }
        if form.subject_token_type != TOKEN_TYPE_JWT && form.subject_token_type != TOKEN_TYPE_ID {
            return Err(ExchangeError::InvalidRequest(
                "unsupported subject_token_type",
            ));
        }
        if let Some(t) = &form.requested_token_type
            && t != TOKEN_TYPE_ACCESS
        {
            return Err(ExchangeError::InvalidRequest(
                "unsupported requested_token_type",
            ));
        }
        if form.subject_token.is_empty()
            || form.subject_token.len() > self.config.max_subject_token_bytes
        {
            return Err(ExchangeError::InvalidRequest("subject_token size"));
        }
        let target = form.resource.as_deref().or(form.audience.as_deref());
        if target != Some(self.config.resource.as_str()) {
            return Err(ExchangeError::InvalidTarget);
        }
        let requested = match &form.scope {
            None => None,
            Some(s) => Some(scope_bits(s).ok_or(ExchangeError::InvalidScope)?),
        };
        if !clock.healthy {
            return Err(ExchangeError::Unavailable("clock health"));
        }
        let identity = match self.verifier.verify(&form.subject_token, clock, review) {
            Decision::Verified(i) => *i,
            Decision::NeedKeys { name, jwks_url } => {
                return Err(ExchangeError::KeysUnavailable { name, jwks_url });
            }
            Decision::Denied(VerifyError::KeysStale) => {
                return Err(ExchangeError::Unavailable("issuer keys stale"));
            }
            Decision::Denied(VerifyError::TokenReviewUnavailable) => {
                return Err(ExchangeError::Unavailable("token review"));
            }
            Decision::Denied(VerifyError::Time(coord_authn::TimeError::ClockUnhealthy)) => {
                return Err(ExchangeError::Unavailable("clock health"));
            }
            Decision::Denied(_) => return Err(ExchangeError::InvalidGrant("assertion rejected")),
        };
        self.issue(&identity, None, None, requested, clock, entropy, creator)
    }

    /// Admit a verified identity through the trust rules and issue a
    /// service token once replicated state created the session: the
    /// receipt is consumed together with `code` (a browser or device
    /// login's grant commitment) when one is given.
    #[allow(clippy::too_many_arguments)]
    pub fn issue(
        &mut self,
        identity: &VerifiedIdentity,
        code: Option<Digest32>,
        refresh_family: Option<Digest32>,
        requested: Option<u32>,
        clock: &ClockHealth,
        entropy: &[u8; 32],
        creator: &mut dyn SessionCreator,
    ) -> Result<ExchangeResponse, ExchangeError> {
        let admitted = mint(identity, &self.rules, clock, entropy)
            .map_err(|_| ExchangeError::InvalidGrant("no trust rule admits the identity"))?;
        let ceiling = admitted.receipt.scope_ceiling;
        let scope = match requested {
            Some(bits) if bits & !ceiling != 0 => return Err(ExchangeError::InvalidScope),
            Some(bits) => bits,
            None => ceiling,
        };
        // The session is created at the scope actually granted, not at the
        // rule's ceiling. Signing the narrower scope into the token while
        // creating the session at the broader one left the narrowing
        // nowhere but the token: execution reconstructs authorization
        // from the replicated session, so an operation presented through
        // a read-only token would have run under a read/write session.
        let mut granted = admitted.receipt.clone();
        granted.scope_ceiling = scope;
        // Replicated state decides: current policy and single use.
        let response = creator
            .create(InternalCommand::ConsumeAdmission {
                namespace: self.config.namespace,
                receipt: granted,
                code,
                refresh_family,
                window: self.config.session_window,
            })
            .map_err(|e| match e {
                CreatorError::Unavailable => ExchangeError::Unavailable("replicated state"),
                CreatorError::Rejected(_) => ExchangeError::Server("session command rejected"),
            })?;
        match response.outcome {
            Outcome::SessionCreated { session } if session == admitted.session => {}
            Outcome::ErrTrustRuleInvalid => {
                return Err(ExchangeError::InvalidGrant(
                    "policy changed since verification",
                ));
            }
            Outcome::ErrReceiptConsumed => {
                return Err(ExchangeError::InvalidGrant("receipt already consumed"));
            }
            Outcome::ErrGrantUnavailable => {
                return Err(ExchangeError::InvalidGrant("grant already consumed"));
            }
            _ => return Err(ExchangeError::Server("unexpected session outcome")),
        }
        let exp = admitted.valid_until.min(
            clock
                .now
                .saturating_add(self.config.max_token_lifetime_secs),
        );
        if exp <= clock.now {
            return Err(ExchangeError::InvalidGrant("no validity remains"));
        }
        let claims = ServiceClaims {
            iss: self.config.issuer.clone(),
            sub: hex(&admitted.receipt.principal.0),
            aud: self.config.resource.clone(),
            sid: hex(&admitted.session.0),
            scope,
            rule: hex(&admitted.rule.0),
            generation: admitted.receipt.rule_generation,
            jti: hex(&admitted.receipt.receipt_id.0),
            iat: clock.now,
            exp,
        };
        let access_token = self
            .ring
            .sign(&claims)
            .map_err(|_| ExchangeError::Server("signing"))?;
        self.log.record(identity, &admitted, clock.now);
        self.issued += 1;
        Ok(ExchangeResponse {
            access_token,
            issued_token_type: TOKEN_TYPE_ACCESS.into(),
            token_type: "Bearer".into(),
            expires_in: exp - clock.now,
            scope: scope_string(scope),
            refresh_token: None,
        })
    }

    /// Sign a fresh service token for an existing, active session (a
    /// refresh): the session's immutable principal, ceiling and rule
    /// binding, with the STS lifetime bound.
    pub fn renew(
        &mut self,
        session: SessionId,
        record: &SessionRecord,
        clock: &ClockHealth,
    ) -> Result<ExchangeResponse, ExchangeError> {
        if !record.active {
            return Err(ExchangeError::InvalidGrant("session retired"));
        }
        if !clock.healthy {
            return Err(ExchangeError::Unavailable("clock health"));
        }
        let exp = clock
            .now
            .saturating_add(self.config.max_token_lifetime_secs);
        let claims = ServiceClaims {
            iss: self.config.issuer.clone(),
            sub: hex(&record.principal.0),
            aud: self.config.resource.clone(),
            sid: hex(&session.0),
            scope: record.scope_ceiling,
            rule: hex(&record.trust_rule.0),
            generation: record.rule_generation,
            jti: hex(&HashDomain::AdmissionReceipt
                .digest(&[&session.0, &clock.now.to_be_bytes()])
                .0),
            iat: clock.now,
            exp,
        };
        let access_token = self
            .ring
            .sign(&claims)
            .map_err(|_| ExchangeError::Server("signing"))?;
        self.issued += 1;
        Ok(ExchangeResponse {
            access_token,
            issued_token_type: TOKEN_TYPE_ACCESS.into(),
            token_type: "Bearer".into(),
            expires_in: exp - clock.now,
            scope: scope_string(record.scope_ceiling),
            refresh_token: None,
        })
    }
}

/// Lowercase hex.
pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

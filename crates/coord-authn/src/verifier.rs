//! Distinct verifier types over one issuer registry (design Sections
//! 9.1, 9.4, 20.1, 20.2).
//!
//! The registry never lets a token choose its keys: an unverified `iss`
//! only selects one *configured* issuer namespace, key-location headers
//! (`jku`, `x5u`, `jwk`, `x5c`) are refused outright, the header
//! algorithm must be in the issuer's explicit asymmetric set and must
//! match the family (and published `alg`) of the cached key, and time
//! claims are judged against the injected clock health rather than the
//! library's wall clock. A missing key yields a request to fetch the
//! configured endpoint, subject to the cache's refresh budget.

use std::collections::BTreeMap;

use jsonwebtoken::dangerous::insecure_decode;
use jsonwebtoken::errors::ErrorKind;
use jsonwebtoken::{Algorithm, Validation, decode, decode_header};
use serde::Deserialize;
use serde_json::Value;

use crate::clock::{ClockHealth, TimeError};
use crate::config::{ConfigError, IssuerConfig};
use crate::jwks::{JwksError, JwksLimits, KeyCache, KeyLookup};

/// Why a token was denied. Nothing about the token's secret content is
/// carried; only the failing check.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VerifyError {
    /// Not a JWT, or claims that do not decode.
    Malformed,
    /// `iss` names no configured issuer.
    UnknownIssuer,
    /// The header algorithm is not in the issuer's explicit set.
    AlgorithmNotAllowed(Algorithm),
    /// The header names where its keys live; never followed.
    TokenDirectedKeys,
    /// No `kid`, or a `kid` the cache does not hold and cannot refresh now.
    UnknownKey,
    /// The cached keys are beyond their staleness limit.
    KeysStale,
    /// The cached key's family or published algorithm differs from the
    /// header algorithm.
    KeyAlgorithmMismatch,
    /// Signature verification failed.
    InvalidSignature,
    /// No configured audience present.
    InvalidAudience,
    /// The verified `iss` differs from the configured one.
    InvalidIssuer,
    /// A required claim is missing.
    MissingClaim(String),
    /// Time claims.
    Time(TimeError),
    /// `azp` is required (several audiences) or differs from the client.
    AzpMismatch,
    /// The nonce differs from the expected one.
    NonceMismatch,
    /// A workload claim is missing or inconsistent.
    ClaimMismatch(String),
    /// TokenReview is required and unavailable (fail closed).
    TokenReviewUnavailable,
    /// TokenReview did not authenticate the token for this subject and
    /// audience.
    TokenReviewDenied,
    /// The verifier has no workload kind for this issuer.
    NoWorkloadKind,
}

/// Standard claims as this crate reads them.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum Audience {
    One(String),
    Many(Vec<String>),
}

#[derive(Debug, Deserialize)]
struct RawClaims {
    iss: Option<String>,
    sub: Option<String>,
    aud: Option<Audience>,
    exp: Option<u64>,
    nbf: Option<u64>,
    iat: Option<u64>,
    azp: Option<String>,
    nonce: Option<String>,
    #[serde(flatten)]
    rest: BTreeMap<String, Value>,
}

#[derive(Debug, Deserialize)]
struct UnverifiedIssuer {
    iss: String,
}

/// A verified identity: what the signature and configured checks
/// established, never the token.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedIdentity {
    /// Configured issuer name.
    pub name: String,
    /// Exact issuer.
    pub issuer: String,
    /// Subject.
    pub subject: String,
    /// Audiences the token carried (at least one configured).
    pub audiences: Vec<String>,
    /// `exp`.
    pub expires_at: u64,
    /// `iat`.
    pub issued_at: Option<u64>,
    /// `azp`.
    pub azp: Option<String>,
    /// `nonce`.
    pub nonce: Option<String>,
    /// Non-standard claims, as data for trust rules; never permission.
    pub claims: BTreeMap<String, Value>,
    /// Workload claims, for WIF verification.
    pub workload: Option<Workload>,
}

/// What a verifier decided.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Decision {
    /// Verified.
    Verified(Box<VerifiedIdentity>),
    /// The configured key set of `name` must be fetched from `jwks_url`
    /// and installed; then verify again.
    NeedKeys {
        /// Issuer configuration name.
        name: String,
        /// The configured endpoint (never one the token named).
        jwks_url: String,
    },
    /// Denied.
    Denied(VerifyError),
}

/// The configured issuers and their key caches.
pub struct Registry {
    by_issuer: BTreeMap<String, IssuerConfig>,
    caches: BTreeMap<String, KeyCache>,
    limits: JwksLimits,
}

impl Registry {
    /// A registry over validated configurations.
    pub fn new(configs: Vec<IssuerConfig>, limits: JwksLimits) -> Result<Self, ConfigError> {
        let mut by_issuer = BTreeMap::new();
        let mut caches = BTreeMap::new();
        for c in configs {
            c.validate()?;
            caches.insert(c.name.clone(), KeyCache::new(limits));
            by_issuer.insert(c.issuer.clone(), c);
        }
        Ok(Registry {
            by_issuer,
            caches,
            limits,
        })
    }

    /// The configuration named `name`.
    pub fn config(&self, name: &str) -> Option<&IssuerConfig> {
        self.by_issuer.values().find(|c| c.name == name)
    }

    /// The key cache of `name`.
    pub fn cache(&self, name: &str) -> Option<&KeyCache> {
        self.caches.get(name)
    }

    /// Cache bounds.
    pub const fn limits(&self) -> JwksLimits {
        self.limits
    }

    /// Install a fetched key document for `name` at `now`.
    pub fn install_keys(
        &mut self,
        name: &str,
        document: &[u8],
        now: u64,
    ) -> Result<usize, JwksError> {
        self.caches
            .get_mut(name)
            .ok_or(JwksError::Malformed)?
            .install(document, now)
    }

    /// A fetch for `name` failed.
    pub fn refresh_failed(&mut self, name: &str) {
        if let Some(c) = self.caches.get_mut(name) {
            c.refresh_failed();
        }
    }

    /// Verify `token` at `clock` against the configured issuer its `iss`
    /// selects.
    pub fn verify(&mut self, token: &str, clock: &ClockHealth) -> Decision {
        let header = match decode_header(token) {
            Ok(h) => h,
            Err(_) => return Decision::Denied(VerifyError::Malformed),
        };
        if header.jku.is_some()
            || header.x5u.is_some()
            || header.jwk.is_some()
            || header.x5c.is_some()
        {
            return Decision::Denied(VerifyError::TokenDirectedKeys);
        }
        // Unverified: it only selects a configured namespace, nothing else.
        let unverified = match insecure_decode::<UnverifiedIssuer>(token) {
            Ok(t) => t.claims.iss,
            Err(_) => return Decision::Denied(VerifyError::Malformed),
        };
        let Some(config) = self.by_issuer.get(&unverified).cloned() else {
            return Decision::Denied(VerifyError::UnknownIssuer);
        };
        if !config.algorithms.contains(&header.alg) {
            return Decision::Denied(VerifyError::AlgorithmNotAllowed(header.alg));
        }
        let Some(kid) = header.kid.as_deref() else {
            return Decision::Denied(VerifyError::UnknownKey);
        };
        let cache = self.caches.get_mut(&config.name).expect("cache per issuer");
        let key = match cache.lookup(kid, clock.now) {
            KeyLookup::Found(k) => k.clone(),
            KeyLookup::NeedRefresh => {
                return Decision::NeedKeys {
                    name: config.name.clone(),
                    jwks_url: config.jwks_url.clone(),
                };
            }
            KeyLookup::Unknown => return Decision::Denied(VerifyError::UnknownKey),
            KeyLookup::Stale => return Decision::Denied(VerifyError::KeysStale),
        };
        if key.key.family() != header.alg.family() || key.algorithm.is_some_and(|a| a != header.alg)
        {
            return Decision::Denied(VerifyError::KeyAlgorithmMismatch);
        }
        let mut validation = Validation::new(header.alg);
        validation.algorithms = vec![header.alg];
        validation.validate_exp = false;
        validation.validate_nbf = false;
        validation.validate_aud = true;
        validation.set_audience(&config.audiences);
        validation.set_issuer(&[config.issuer.as_str()]);
        validation.set_required_spec_claims(&["exp", "iss", "sub", "aud"]);
        let data = match decode::<RawClaims>(token, &key.key, &validation) {
            Ok(d) => d,
            Err(e) => {
                return Decision::Denied(match e.kind() {
                    ErrorKind::InvalidSignature => VerifyError::InvalidSignature,
                    ErrorKind::InvalidAudience => VerifyError::InvalidAudience,
                    ErrorKind::InvalidIssuer => VerifyError::InvalidIssuer,
                    ErrorKind::MissingRequiredClaim(c) => VerifyError::MissingClaim(c.clone()),
                    ErrorKind::InvalidAlgorithm => VerifyError::AlgorithmNotAllowed(header.alg),
                    _ => VerifyError::Malformed,
                });
            }
        };
        let claims = data.claims;
        // The library checked presence; these are the typed views.
        let (Some(iss), Some(sub), Some(aud), Some(exp)) =
            (claims.iss, claims.sub, claims.aud, claims.exp)
        else {
            return Decision::Denied(VerifyError::MissingClaim("exp".into()));
        };
        if iss != config.issuer {
            return Decision::Denied(VerifyError::InvalidIssuer);
        }
        if let Err(e) = clock.check(exp, claims.nbf, claims.iat, config.max_age_secs) {
            return Decision::Denied(VerifyError::Time(e));
        }
        let audiences = match aud {
            Audience::One(a) => vec![a],
            Audience::Many(v) => v,
        };
        if !audiences.iter().any(|a| config.audiences.contains(a)) {
            return Decision::Denied(VerifyError::InvalidAudience);
        }
        Decision::Verified(Box::new(VerifiedIdentity {
            name: config.name,
            issuer: iss,
            subject: sub,
            audiences,
            expires_at: exp,
            issued_at: claims.iat,
            azp: claims.azp,
            nonce: claims.nonce,
            claims: claims.rest,
            workload: None,
        }))
    }
}

/// The verifier of human OIDC ID tokens for one relying-party client.
pub struct OidcVerifier {
    registry: Registry,
    client_id: String,
}

impl OidcVerifier {
    /// A verifier for `client_id`.
    pub const fn new(registry: Registry, client_id: String) -> Self {
        OidcVerifier {
            registry,
            client_id,
        }
    }

    /// The registry (key installation).
    pub const fn registry_mut(&mut self) -> &mut Registry {
        &mut self.registry
    }

    /// The registry.
    pub const fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Verify an ID token: the client must be an audience; with several
    /// audiences `azp` is required and must be the client; when present
    /// `azp` must be the client; an expected nonce must match.
    pub fn verify(&mut self, token: &str, clock: &ClockHealth, nonce: Option<&str>) -> Decision {
        let identity = match self.registry.verify(token, clock) {
            Decision::Verified(i) => i,
            other => return other,
        };
        if !identity.audiences.contains(&self.client_id) {
            return Decision::Denied(VerifyError::InvalidAudience);
        }
        match (&identity.azp, identity.audiences.len()) {
            (Some(azp), _) if *azp != self.client_id => {
                return Decision::Denied(VerifyError::AzpMismatch);
            }
            (None, n) if n > 1 => return Decision::Denied(VerifyError::AzpMismatch),
            _ => {}
        }
        if let Some(expected) = nonce
            && identity.nonce.as_deref() != Some(expected)
        {
            return Decision::Denied(VerifyError::NonceMismatch);
        }
        Decision::Verified(identity)
    }
}

/// How Kubernetes service-account tokens are checked.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KubernetesMode {
    /// Offline JWT verification only: bound-object existence is not
    /// established.
    Offline,
    /// The caller performs a TokenReview and the verifier requires its
    /// result; an unavailable review denies.
    TokenReview,
}

/// The result of a TokenReview the caller performed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TokenReview {
    /// The API server could not be asked.
    Unavailable,
    /// The review answered.
    Reviewed {
        /// Whether the token is authenticated now.
        authenticated: bool,
        /// The authenticated username.
        username: Option<String>,
        /// Audiences the review confirmed.
        audiences: Vec<String>,
    },
}

/// Which workload assertion an issuer provides.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkloadKind {
    /// Kubernetes projected service-account tokens.
    Kubernetes(KubernetesMode),
    /// GitHub Actions OIDC assertions.
    GithubActions,
}

/// Workload claims, parsed into stable platform identities.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Workload {
    /// A Kubernetes service account.
    KubernetesServiceAccount {
        /// Namespace.
        namespace: String,
        /// Service-account name.
        name: String,
        /// Service-account UID when the token carries it.
        uid: Option<String>,
        /// Bound pod name when the token carries it.
        pod: Option<String>,
    },
    /// A GitHub Actions workflow run.
    GithubActions {
        /// Immutable repository identifier.
        repository_id: String,
        /// Immutable owner identifier.
        repository_owner_id: String,
        /// Repository name (mutable; informational).
        repository: String,
        /// Workflow reference.
        workflow_ref: String,
        /// Deployment environment, when any.
        environment: Option<String>,
    },
}

/// The verifier of workload assertions.
pub struct WifVerifier {
    registry: Registry,
    kinds: BTreeMap<String, WorkloadKind>,
}

impl WifVerifier {
    /// A verifier whose issuers (by configuration name) provide `kinds`.
    pub const fn new(registry: Registry, kinds: BTreeMap<String, WorkloadKind>) -> Self {
        WifVerifier { registry, kinds }
    }

    /// The registry (key installation).
    pub const fn registry_mut(&mut self) -> &mut Registry {
        &mut self.registry
    }

    /// The registry.
    pub const fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Verify a workload assertion; `review` is the TokenReview result the
    /// caller obtained when the issuer's mode requires one.
    pub fn verify(
        &mut self,
        token: &str,
        clock: &ClockHealth,
        review: Option<&TokenReview>,
    ) -> Decision {
        let mut identity = match self.registry.verify(token, clock) {
            Decision::Verified(i) => i,
            other => return other,
        };
        let Some(kind) = self.kinds.get(&identity.name).copied() else {
            return Decision::Denied(VerifyError::NoWorkloadKind);
        };
        let workload = match kind {
            WorkloadKind::Kubernetes(mode) => {
                let w = match kubernetes(&identity) {
                    Ok(w) => w,
                    Err(e) => return Decision::Denied(e),
                };
                if mode == KubernetesMode::TokenReview {
                    let config = self.registry.config(&identity.name).expect("configured");
                    match review {
                        None | Some(TokenReview::Unavailable) => {
                            return Decision::Denied(VerifyError::TokenReviewUnavailable);
                        }
                        Some(TokenReview::Reviewed {
                            authenticated,
                            username,
                            audiences,
                        }) => {
                            let ok = *authenticated
                                && username.as_deref() == Some(identity.subject.as_str())
                                && audiences.iter().any(|a| config.audiences.contains(a));
                            if !ok {
                                return Decision::Denied(VerifyError::TokenReviewDenied);
                            }
                        }
                    }
                }
                w
            }
            WorkloadKind::GithubActions => match github(&identity) {
                Ok(w) => w,
                Err(e) => return Decision::Denied(e),
            },
        };
        identity.workload = Some(workload);
        Decision::Verified(identity)
    }
}

fn string_claim<'a>(
    claims: &'a BTreeMap<String, Value>,
    name: &str,
) -> Result<&'a str, VerifyError> {
    claims
        .get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| VerifyError::ClaimMismatch(name.into()))
}

fn kubernetes(identity: &VerifiedIdentity) -> Result<Workload, VerifyError> {
    let mut parts = identity.subject.splitn(4, ':');
    let (Some("system"), Some("serviceaccount"), Some(namespace), Some(name)) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(VerifyError::ClaimMismatch("sub".into()));
    };
    if namespace.is_empty() || name.is_empty() || name.contains(':') {
        return Err(VerifyError::ClaimMismatch("sub".into()));
    }
    let mut uid = None;
    let mut pod = None;
    if let Some(k8s) = identity.claims.get("kubernetes.io") {
        let ns = k8s.get("namespace").and_then(Value::as_str);
        if ns.is_some_and(|n| n != namespace) {
            return Err(VerifyError::ClaimMismatch("kubernetes.io.namespace".into()));
        }
        if let Some(sa) = k8s.get("serviceaccount") {
            if sa
                .get("name")
                .and_then(Value::as_str)
                .is_some_and(|n| n != name)
            {
                return Err(VerifyError::ClaimMismatch(
                    "kubernetes.io.serviceaccount.name".into(),
                ));
            }
            uid = sa.get("uid").and_then(Value::as_str).map(str::to_string);
        }
        pod = k8s
            .get("pod")
            .and_then(|p| p.get("name"))
            .and_then(Value::as_str)
            .map(str::to_string);
    }
    Ok(Workload::KubernetesServiceAccount {
        namespace: namespace.to_string(),
        name: name.to_string(),
        uid,
        pod,
    })
}

fn github(identity: &VerifiedIdentity) -> Result<Workload, VerifyError> {
    let c = &identity.claims;
    Ok(Workload::GithubActions {
        repository_id: string_claim(c, "repository_id")?.to_string(),
        repository_owner_id: string_claim(c, "repository_owner_id")?.to_string(),
        repository: string_claim(c, "repository")?.to_string(),
        workflow_ref: string_claim(c, "workflow_ref")?.to_string(),
        environment: c
            .get("environment")
            .and_then(Value::as_str)
            .map(str::to_string),
    })
}

//! Issuer configuration (design Sections 9.2, 9.4, 20.2): exact issuer,
//! one configured JWKS endpoint, explicit asymmetric algorithms and
//! exact audiences. Nothing is discovered from a token.

use jsonwebtoken::Algorithm;

/// Why a configuration is refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConfigError {
    /// A symmetric or unsupported algorithm was configured.
    AlgorithmNotPermitted(Algorithm),
    /// No algorithm.
    NoAlgorithm,
    /// No audience.
    NoAudience,
    /// The JWKS endpoint is not an HTTPS URL (or loopback HTTP when
    /// explicitly allowed).
    InsecureEndpoint(String),
    /// Empty issuer or name.
    Empty,
}

/// One configured issuer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IssuerConfig {
    /// Configuration name (selects the key namespace).
    pub name: String,
    /// Exact `iss` value.
    pub issuer: String,
    /// The configured JWKS endpoint; the only place keys come from.
    pub jwks_url: String,
    /// Algorithms accepted from this issuer (asymmetric only).
    pub algorithms: Vec<Algorithm>,
    /// Exact audiences accepted.
    pub audiences: Vec<String>,
    /// Maximum age of a token by `iat`, in seconds.
    pub max_age_secs: Option<u64>,
    /// Allow a loopback HTTP endpoint (tests and local development).
    pub allow_insecure_loopback: bool,
}

/// Whether `alg` is one of the asymmetric algorithms this crate accepts.
pub const fn permitted(alg: Algorithm) -> bool {
    matches!(
        alg,
        Algorithm::RS256
            | Algorithm::RS384
            | Algorithm::RS512
            | Algorithm::ES256
            | Algorithm::ES384
    )
}

/// Whether `url` is an acceptable key endpoint.
pub fn secure_endpoint(url: &str, allow_insecure_loopback: bool) -> bool {
    if url.starts_with("https://") {
        return true;
    }
    allow_insecure_loopback
        && (url.starts_with("http://127.0.0.1") || url.starts_with("http://localhost"))
}

impl IssuerConfig {
    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.name.is_empty() || self.issuer.is_empty() {
            return Err(ConfigError::Empty);
        }
        if self.algorithms.is_empty() {
            return Err(ConfigError::NoAlgorithm);
        }
        for alg in &self.algorithms {
            if !permitted(*alg) {
                return Err(ConfigError::AlgorithmNotPermitted(*alg));
            }
        }
        if self.audiences.is_empty() {
            return Err(ConfigError::NoAudience);
        }
        if !secure_endpoint(&self.jwks_url, self.allow_insecure_loopback) {
            return Err(ConfigError::InsecureEndpoint(self.jwks_url.clone()));
        }
        Ok(())
    }
}

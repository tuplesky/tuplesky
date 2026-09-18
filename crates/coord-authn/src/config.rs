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
    /// Two configurations share a name, and the name selects the key
    /// cache: they would share keys across issuer boundaries.
    DuplicateName(String),
    /// Two configurations claim the same `iss`; the second would silently
    /// replace the first.
    DuplicateIssuer(String),
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
///
/// Plaintext is admitted only for a genuine loopback host, and only when
/// the configuration asks for it. A prefix test is not enough: the
/// authority of `http://localhost@evil.example/keys` is `evil.example`,
/// and `http://localhost.evil.example/keys` is a different host again,
/// so both would have passed one and let an attacker serve JWKS over
/// plaintext.
pub fn secure_endpoint(url: &str, allow_insecure_loopback: bool) -> bool {
    if url.starts_with("https://") {
        return true;
    }
    if !allow_insecure_loopback {
        return false;
    }
    let Some(rest) = url.strip_prefix("http://") else {
        return false;
    };
    // The authority ends at the first `/`, `?` or `#`; anything before an
    // `@` inside it is userinfo, not the host.
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    let host_port = match authority.rsplit_once('@') {
        Some((_userinfo, host)) => host,
        None => authority,
    };
    let host = match host_port.strip_prefix('[') {
        // An IPv6 literal: everything up to the closing bracket.
        Some(rest) => match rest.split_once(']') {
            Some((inside, after)) => {
                if !after.is_empty() && !after.starts_with(':') {
                    return false;
                }
                inside
            }
            None => return false,
        },
        None => host_port.split(':').next().unwrap_or_default(),
    };
    loopback_host(host)
}

/// Whether `host` names the loopback interface exactly.
fn loopback_host(host: &str) -> bool {
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    match host.parse::<core::net::IpAddr>() {
        Ok(ip) => ip.is_loopback(),
        Err(_) => false,
    }
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

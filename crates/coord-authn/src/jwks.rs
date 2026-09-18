//! Bounded key caches (design Section 9.4): keys come only from the
//! configured endpoint; the cache has a freshness limit after which a
//! refresh is requested, a staleness limit after which cached keys deny,
//! a bound on stored keys and document size, and a refresh budget (plus
//! a minimum interval after a fetch) so a flood of unknown key
//! identifiers cannot turn into a flood of fetches.

use std::collections::BTreeMap;

use jsonwebtoken::jwk::{AlgorithmParameters, JwkSet, KeyAlgorithm};
use jsonwebtoken::{Algorithm, DecodingKey};

/// Cache bounds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JwksLimits {
    /// Keys stored per issuer at most.
    pub max_keys: usize,
    /// Bytes a key document may have at most.
    pub max_document_bytes: usize,
    /// Seconds after a fetch during which the keys are fresh.
    pub ttl_secs: u64,
    /// Seconds after a fetch after which cached keys may no longer be
    /// used at all (fail closed).
    pub stale_limit_secs: u64,
    /// Refreshes triggered by unknown key identifiers per window.
    pub refreshes_per_window: u32,
    /// The window, in seconds.
    pub window_secs: u64,
    /// Seconds after a successful fetch during which an unknown key
    /// identifier is simply unknown (the issuer was just asked).
    pub min_refresh_interval_secs: u64,
}

impl Default for JwksLimits {
    fn default() -> Self {
        JwksLimits {
            max_keys: 32,
            max_document_bytes: 64 * 1024,
            ttl_secs: 600,
            stale_limit_secs: 6 * 3600,
            refreshes_per_window: 2,
            window_secs: 60,
            min_refresh_interval_secs: 5,
        }
    }
}

/// Why a key document was not installed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JwksError {
    /// Larger than the bound.
    TooLarge {
        /// Bytes.
        bytes: usize,
    },
    /// Not a JWK set.
    Malformed,
    /// More keys than the bound.
    TooManyKeys {
        /// Keys.
        keys: usize,
    },
    /// A key without a `kid`, or a duplicate.
    UnusableKey,
}

/// A cached key with the algorithm it was published for, if any.
#[derive(Clone)]
pub struct CachedKey {
    /// The verification key.
    pub key: DecodingKey,
    /// The published `alg`, if the key set named one.
    pub algorithm: Option<Algorithm>,
}

/// What a lookup found.
pub enum KeyLookup<'a> {
    /// A usable key.
    Found(&'a CachedKey),
    /// Unknown here (or the keys are past their freshness limit) and the
    /// refresh budget allows a fetch of the configured endpoint. The
    /// caller installs the document and looks up again.
    NeedRefresh,
    /// Unknown here and no refresh is allowed now.
    Unknown,
    /// The cached keys are beyond the staleness limit: nothing verifies
    /// until a refresh succeeds.
    Stale,
}

/// The key cache of one issuer.
pub struct KeyCache {
    limits: JwksLimits,
    keys: BTreeMap<String, CachedKey>,
    fetched_at: Option<u64>,
    window_start: u64,
    refreshes_in_window: u32,
    /// Refresh requests handed out.
    pub refresh_requests: u64,
    /// Lookups refused for lack of budget.
    pub refused: u64,
}

impl KeyCache {
    /// An empty cache.
    pub const fn new(limits: JwksLimits) -> Self {
        KeyCache {
            limits,
            keys: BTreeMap::new(),
            fetched_at: None,
            window_start: 0,
            refreshes_in_window: 0,
            refresh_requests: 0,
            refused: 0,
        }
    }

    /// Keys stored.
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Whether no key is stored.
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Whether the keys are within their freshness limit at `now`.
    pub fn is_fresh(&self, now: u64) -> bool {
        self.fetched_at
            .is_some_and(|t| now.saturating_sub(t) <= self.limits.ttl_secs)
    }

    /// Whether the keys are beyond the staleness limit at `now`.
    pub fn is_stale(&self, now: u64) -> bool {
        self.fetched_at
            .is_some_and(|t| now.saturating_sub(t) > self.limits.stale_limit_secs)
    }

    fn take_budget(&mut self, now: u64) -> bool {
        if now.saturating_sub(self.window_start) >= self.limits.window_secs {
            self.window_start = now;
            self.refreshes_in_window = 0;
        }
        if self.refreshes_in_window >= self.limits.refreshes_per_window {
            self.refused += 1;
            return false;
        }
        self.refreshes_in_window += 1;
        self.refresh_requests += 1;
        true
    }

    fn just_fetched(&self, now: u64) -> bool {
        self.fetched_at
            .is_some_and(|t| now.saturating_sub(t) < self.limits.min_refresh_interval_secs)
    }

    /// Look up `kid` at `now`.
    pub fn lookup(&mut self, kid: &str, now: u64) -> KeyLookup<'_> {
        if self.is_stale(now) {
            // Stale keys never verify; a refresh is requested within the
            // budget, otherwise the answer is simply stale.
            if self.take_budget(now) {
                return KeyLookup::NeedRefresh;
            }
            return KeyLookup::Stale;
        }
        if self.keys.contains_key(kid) && self.is_fresh(now) {
            return KeyLookup::Found(&self.keys[kid]);
        }
        if !self.keys.contains_key(kid) && self.just_fetched(now) {
            // The issuer was just asked and did not publish it.
            return KeyLookup::Unknown;
        }
        if self.take_budget(now) {
            return KeyLookup::NeedRefresh;
        }
        if self.keys.contains_key(kid) {
            // Past the freshness limit but within the staleness limit and
            // out of refresh budget: still usable.
            return KeyLookup::Found(&self.keys[kid]);
        }
        KeyLookup::Unknown
    }

    /// Install a fetched key document at `now`, replacing the keys.
    pub fn install(&mut self, document: &[u8], now: u64) -> Result<usize, JwksError> {
        if document.len() > self.limits.max_document_bytes {
            return Err(JwksError::TooLarge {
                bytes: document.len(),
            });
        }
        let set: JwkSet = serde_json::from_slice(document).map_err(|_| JwksError::Malformed)?;
        if set.keys.len() > self.limits.max_keys {
            return Err(JwksError::TooManyKeys {
                keys: set.keys.len(),
            });
        }
        let mut keys = BTreeMap::new();
        for jwk in &set.keys {
            let Some(kid) = jwk.common.key_id.clone() else {
                return Err(JwksError::UnusableKey);
            };
            if !matches!(
                jwk.algorithm,
                AlgorithmParameters::EllipticCurve(_) | AlgorithmParameters::RSA(_)
            ) {
                // Symmetric and other key types never enter the cache.
                return Err(JwksError::UnusableKey);
            }
            let algorithm = match jwk.common.key_algorithm {
                None => None,
                Some(KeyAlgorithm::RS256) => Some(Algorithm::RS256),
                Some(KeyAlgorithm::RS384) => Some(Algorithm::RS384),
                Some(KeyAlgorithm::RS512) => Some(Algorithm::RS512),
                Some(KeyAlgorithm::ES256) => Some(Algorithm::ES256),
                Some(KeyAlgorithm::ES384) => Some(Algorithm::ES384),
                Some(_) => return Err(JwksError::UnusableKey),
            };
            let key = DecodingKey::from_jwk(jwk).map_err(|_| JwksError::UnusableKey)?;
            if keys.insert(kid, CachedKey { key, algorithm }).is_some() {
                return Err(JwksError::UnusableKey);
            }
        }
        self.keys = keys;
        self.fetched_at = Some(now);
        Ok(self.keys.len())
    }

    /// A refresh failed (issuer outage): cached keys stay usable until the
    /// staleness limit, nothing else changes.
    pub const fn refresh_failed(&mut self) {}
}

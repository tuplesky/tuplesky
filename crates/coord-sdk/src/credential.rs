//! Credential providers (design Sections 9, 11.5): the SDK obtains a
//! credential from a provider, presents it once when a connection is
//! bound, and never again per operation. Providers are cached with expiry
//! and a single in-flight refresh; the SDK never logs or exposes the
//! secret bytes.

use std::fmt;

/// A credential the SDK may present: opaque bytes and their expiry.
#[derive(Clone, PartialEq, Eq)]
pub struct Credential {
    secret: Vec<u8>,
    /// Expiry in ticks (milliseconds of the caller's clock).
    pub expires_at: u64,
}

impl Credential {
    /// A credential valid until `expires_at`.
    pub fn new(secret: Vec<u8>, expires_at: u64) -> Self {
        Credential { secret, expires_at }
    }

    /// The bytes to present at binding time. Only the binding code calls
    /// this; nothing else ever sees them.
    pub fn present(&self) -> &[u8] {
        &self.secret
    }

    /// Whether it is still usable at `now` with `margin` ticks to spare.
    pub const fn usable_at(&self, now: u64, margin: u64) -> bool {
        now.saturating_add(margin) < self.expires_at
    }
}

impl fmt::Debug for Credential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credential")
            .field("secret", &"<redacted>")
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

/// Why no credential could be produced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CredentialError {
    /// The source refused or failed (redacted reason).
    Unavailable,
    /// A refresh is already in flight; try again when it completes.
    RefreshInFlight,
}

/// Something that produces credentials (a file, a workload exchange, a
/// login flow). Implementations are called only by the cache.
pub trait CredentialProvider {
    /// Produce a credential at `now`.
    fn acquire(&mut self, now: u64) -> Result<Credential, CredentialError>;
}

/// A fixed credential (tests, static service credentials).
#[derive(Clone, Debug)]
pub struct StaticProvider {
    credential: Credential,
    /// How many times it was acquired.
    pub acquisitions: u64,
}

impl StaticProvider {
    /// A provider always returning `credential`.
    pub const fn new(credential: Credential) -> Self {
        StaticProvider {
            credential,
            acquisitions: 0,
        }
    }
}

impl CredentialProvider for StaticProvider {
    fn acquire(&mut self, _now: u64) -> Result<Credential, CredentialError> {
        self.acquisitions += 1;
        Ok(self.credential.clone())
    }
}

/// A cache in front of a provider: the cached credential is reused until
/// its expiry margin, and a refresh happens at most once at a time.
#[derive(Debug)]
pub struct CachedProvider<P> {
    source: P,
    cached: Option<Credential>,
    margin: u64,
    refreshing: bool,
    /// Exchanges performed against the source.
    pub exchanges: u64,
}

impl<P: CredentialProvider> CachedProvider<P> {
    /// Cache `source`, refreshing `margin` ticks before expiry.
    pub const fn new(source: P, margin: u64) -> Self {
        CachedProvider {
            source,
            cached: None,
            margin,
            refreshing: false,
            exchanges: 0,
        }
    }

    /// The credential to present at `now`: cached when usable, otherwise
    /// one exchange with the source.
    pub fn credential(&mut self, now: u64) -> Result<Credential, CredentialError> {
        if let Some(c) = &self.cached
            && c.usable_at(now, self.margin)
        {
            return Ok(c.clone());
        }
        if self.refreshing {
            return Err(CredentialError::RefreshInFlight);
        }
        self.refreshing = true;
        let result = self.source.acquire(now);
        self.refreshing = false;
        self.exchanges += 1;
        let credential = result?;
        self.cached = Some(credential.clone());
        Ok(credential)
    }

    /// Drop the cached credential (a binding was rejected as expired or
    /// revoked): the next call exchanges again.
    pub fn invalidate(&mut self) {
        self.cached = None;
    }

    /// The source.
    pub const fn source(&self) -> &P {
        &self.source
    }
}

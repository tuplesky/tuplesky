//! Hardened key fetching (design Sections 9.4, 20.1): only configured
//! endpoints are ever requested, over HTTPS (loopback HTTP only when
//! explicitly allowed), with redirects refused, no implicit environment
//! proxy, bounded time and bounded body. This is the only I/O of the
//! crate.

use std::collections::BTreeSet;
use std::time::Duration;

use crate::config::secure_endpoint;

/// Fetch bounds and allow-list.
#[derive(Clone, Debug)]
pub struct FetchConfig {
    /// The only URLs that may be requested.
    pub allowed_urls: BTreeSet<String>,
    /// Whole-request timeout.
    pub timeout: Duration,
    /// Connect timeout.
    pub connect_timeout: Duration,
    /// Body bound.
    pub max_body_bytes: usize,
    /// Allow loopback HTTP endpoints.
    pub allow_insecure_loopback: bool,
    /// An explicitly configured proxy URL; environment proxies are never
    /// used.
    pub proxy: Option<String>,
}

impl Default for FetchConfig {
    fn default() -> Self {
        FetchConfig {
            allowed_urls: BTreeSet::new(),
            timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(2),
            max_body_bytes: 64 * 1024,
            allow_insecure_loopback: false,
            proxy: None,
        }
    }
}

/// Why a fetch failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FetchError {
    /// The URL is not a configured endpoint.
    NotConfigured,
    /// The URL is not an acceptable endpoint.
    InsecureEndpoint,
    /// The server answered with a redirect; never followed.
    Redirected {
        /// Status.
        status: u16,
    },
    /// A non-success status.
    Status {
        /// Status.
        status: u16,
    },
    /// The body exceeds the bound.
    BodyTooLarge {
        /// Bound.
        limit: usize,
    },
    /// The request timed out.
    Timeout,
    /// A transport failure (redacted class).
    Transport,
    /// The client could not be built.
    Build(String),
}

/// The fetcher.
pub struct HardenedFetcher {
    client: reqwest::Client,
    config: FetchConfig,
}

impl HardenedFetcher {
    /// Build a client with the hardening applied.
    pub fn new(config: FetchConfig) -> Result<Self, FetchError> {
        // The TLS stack is the explicit AWS-LC provider; installing twice
        // is harmless.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let mut builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(config.timeout)
            .connect_timeout(config.connect_timeout)
            .https_only(!config.allow_insecure_loopback)
            .user_agent("coord-authn/1");
        builder = match &config.proxy {
            Some(url) => builder
                .proxy(reqwest::Proxy::all(url).map_err(|e| FetchError::Build(e.to_string()))?),
            None => builder.no_proxy(),
        };
        let client = builder
            .build()
            .map_err(|e| FetchError::Build(e.to_string()))?;
        Ok(HardenedFetcher { client, config })
    }

    /// Fetch a configured key document.
    pub async fn fetch_jwks(&self, url: &str) -> Result<Vec<u8>, FetchError> {
        if !self.config.allowed_urls.contains(url) {
            return Err(FetchError::NotConfigured);
        }
        if !secure_endpoint(url, self.config.allow_insecure_loopback) {
            return Err(FetchError::InsecureEndpoint);
        }
        let response = self.client.get(url).send().await.map_err(classify)?;
        let status = response.status();
        if status.is_redirection() {
            return Err(FetchError::Redirected {
                status: status.as_u16(),
            });
        }
        if !status.is_success() {
            return Err(FetchError::Status {
                status: status.as_u16(),
            });
        }
        let limit = self.config.max_body_bytes;
        if response.content_length().is_some_and(|n| n > limit as u64) {
            return Err(FetchError::BodyTooLarge { limit });
        }
        let mut body = Vec::new();
        let mut response = response;
        while let Some(chunk) = response.chunk().await.map_err(classify)? {
            if body.len() + chunk.len() > limit {
                return Err(FetchError::BodyTooLarge { limit });
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }
}

fn classify(e: reqwest::Error) -> FetchError {
    if e.is_timeout() {
        FetchError::Timeout
    } else {
        FetchError::Transport
    }
}

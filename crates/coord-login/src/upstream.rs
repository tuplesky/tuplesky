//! The upstream relying-party leg through openidconnect (design Sections
//! 8.2, 20.1): discovery of a pinned issuer, the authorization request
//! with the broker's PKCE, state and nonce, the code exchange and the ID
//! token verification with explicit algorithms, followed by the
//! application's authorized-party policy. The HTTP client never follows
//! redirects and uses no environment proxy.

use std::time::Duration;

use openidconnect::core::{
    CoreAuthenticationFlow, CoreClient, CoreJwsSigningAlgorithm, CoreProviderMetadata,
};
use openidconnect::{
    AuthorizationCode, ClientId, CsrfToken, EndpointMaybeSet, EndpointNotSet, EndpointSet,
    HttpRequest, HttpResponse, IssuerUrl, Nonce, OAuth2TokenResponse, PkceCodeChallenge,
    PkceCodeVerifier, RedirectUrl, TokenResponse,
};

use crate::service::{LoginError, UpstreamIdentity, azp_policy};

/// One configured upstream provider.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpstreamConfig {
    /// Configuration name.
    pub name: String,
    /// Exact issuer URL.
    pub issuer: String,
    /// The broker's client identifier at the provider.
    pub client_id: String,
    /// The broker's callback (exact).
    pub redirect_uri: String,
    /// Allow a loopback HTTP issuer (tests).
    pub allow_insecure_loopback: bool,
}

/// Why the upstream leg failed (bounded; never token material).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UpstreamError {
    /// Bad configuration.
    Config(String),
    /// Discovery failed or named another issuer.
    Discovery,
    /// The code exchange failed.
    Exchange,
    /// No ID token in the response.
    NoIdToken,
    /// The ID token did not verify (signature, issuer, audience, expiry,
    /// nonce or algorithm).
    IdToken,
    /// The application's audience or authorized-party policy.
    Policy(LoginError),
}

type Client = CoreClient<
    EndpointSet,
    EndpointNotSet,
    EndpointNotSet,
    EndpointNotSet,
    EndpointMaybeSet,
    EndpointMaybeSet,
>;

/// A hardened HTTP client for the upstream leg.
pub fn hardened_http_client(timeout: Duration) -> Result<reqwest::Client, UpstreamError> {
    // The TLS stack is the explicit AWS-LC provider; installing twice is
    // harmless.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .timeout(timeout)
        .connect_timeout(timeout)
        .user_agent("coord-login/1")
        .build()
        .map_err(|e| UpstreamError::Config(e.to_string()))
}

/// A discovered upstream client.
pub struct Upstream {
    config: UpstreamConfig,
    client: Client,
}

impl Upstream {
    /// Discover `config.issuer` and build the client; discovery must
    /// return exactly that issuer.
    pub async fn discover(
        config: UpstreamConfig,
        http: &reqwest::Client,
    ) -> Result<Self, UpstreamError> {
        let issuer = IssuerUrl::new(config.issuer.clone())
            .map_err(|e| UpstreamError::Config(e.to_string()))?;
        // The scheme and host are decided by parsing, never by a prefix
        // of the string: `http://127.0.0.1.evil.example` starts with the
        // loopback prefix and is not loopback at all.
        let parsed = issuer.url();
        let loopback = matches!(parsed.host_str(), Some("127.0.0.1" | "::1" | "localhost"));
        let permitted = match parsed.scheme() {
            "https" => true,
            "http" => config.allow_insecure_loopback && loopback,
            _ => false,
        };
        if !permitted {
            return Err(UpstreamError::Config("issuer must be https".into()));
        }
        // Every upstream read is bounded: the provider decides the body
        // and a hostile or broken one must not decide our memory.
        let bounded = BoundedHttpClient::with_default_bound(http);
        let metadata = CoreProviderMetadata::discover_async(issuer.clone(), &bounded)
            .await
            .map_err(|_| UpstreamError::Discovery)?;
        if metadata.issuer() != &issuer {
            return Err(UpstreamError::Discovery);
        }
        let redirect = RedirectUrl::new(config.redirect_uri.clone())
            .map_err(|e| UpstreamError::Config(e.to_string()))?;
        let client = CoreClient::from_provider_metadata(
            metadata,
            ClientId::new(config.client_id.clone()),
            None,
        )
        .set_redirect_uri(redirect);
        Ok(Upstream { config, client })
    }

    /// Configuration.
    pub const fn config(&self) -> &UpstreamConfig {
        &self.config
    }

    /// The authorization URL for a started login.
    pub fn authorize_url(&self, state: &str, nonce: &str, challenge: PkceCodeChallenge) -> String {
        let state = state.to_string();
        let nonce = nonce.to_string();
        let (url, _, _) = self
            .client
            .authorize_url(
                CoreAuthenticationFlow::AuthorizationCode,
                move || CsrfToken::new(state),
                move || Nonce::new(nonce),
            )
            .set_pkce_challenge(challenge)
            .url();
        url.to_string()
    }

    /// Exchange the upstream `code` with the broker's PKCE `verifier`,
    /// verify the ID token against `nonce` with explicit algorithms, and
    /// apply the authorized-party policy.
    pub async fn exchange(
        &self,
        code: String,
        verifier: String,
        nonce: &str,
        http: &reqwest::Client,
    ) -> Result<UpstreamIdentity, UpstreamError> {
        let response = self
            .client
            .exchange_code(AuthorizationCode::new(code))
            .map_err(|_| UpstreamError::Exchange)?
            .set_pkce_verifier(PkceCodeVerifier::new(verifier))
            .request_async(&BoundedHttpClient::with_default_bound(http))
            .await
            .map_err(|_| UpstreamError::Exchange)?;
        let _ = response.access_token();
        let id_token = response.id_token().ok_or(UpstreamError::NoIdToken)?;
        // Additional audiences are the application's decision: the
        // client must be an audience (library) and `azp` must name it
        // when there are several (policy below).
        let verifier = self
            .client
            .id_token_verifier()
            .set_allowed_algs([
                CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256,
                CoreJwsSigningAlgorithm::EcdsaP256Sha256,
            ])
            .set_other_audience_verifier_fn(|_| true);
        let claims = id_token
            .claims(&verifier, &Nonce::new(nonce.to_string()))
            .map_err(|_| UpstreamError::IdToken)?;
        let identity = UpstreamIdentity {
            issuer: claims.issuer().to_string(),
            subject: claims.subject().to_string(),
            audiences: claims.audiences().iter().map(|a| a.to_string()).collect(),
            authorized_party: claims.authorized_party().map(|a| a.to_string()),
            expires_at: u64::try_from(claims.expiration().timestamp()).unwrap_or(0),
        };
        azp_policy(&identity, &self.config.client_id).map_err(UpstreamError::Policy)?;
        Ok(identity)
    }
}

/// How much of an upstream response is read before it is refused.
///
/// Discovery documents and token responses are small; a provider that
/// is hostile, compromised or simply broken is not, and the client used
/// to read whatever it sent into memory.
pub const MAX_UPSTREAM_BODY_BYTES: usize = 256 * 1024;

/// A `reqwest` client that reads at most `max_body_bytes` of a response.
pub struct BoundedHttpClient<'a> {
    inner: &'a reqwest::Client,
    max_body_bytes: usize,
}

impl<'a> BoundedHttpClient<'a> {
    /// Wrap `inner`, reading at most `max_body_bytes` per response.
    pub const fn new(inner: &'a reqwest::Client, max_body_bytes: usize) -> Self {
        BoundedHttpClient {
            inner,
            max_body_bytes,
        }
    }

    /// Wrap `inner` with the default bound.
    pub const fn with_default_bound(inner: &'a reqwest::Client) -> Self {
        Self::new(inner, MAX_UPSTREAM_BODY_BYTES)
    }
}

/// Why an upstream request produced no usable response.
#[derive(Debug)]
pub enum HttpError {
    /// The request could not be made or the response could not be read.
    Transport,
    /// The response body passed the bound and was refused unread.
    TooLarge {
        /// The bound it passed.
        limit: usize,
    },
    /// The request or response could not be represented.
    Malformed,
}

impl std::fmt::Display for HttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HttpError::Transport => write!(f, "upstream request failed"),
            HttpError::TooLarge { limit } => {
                write!(f, "upstream response exceeds {limit} bytes")
            }
            HttpError::Malformed => write!(f, "upstream exchange malformed"),
        }
    }
}

impl std::error::Error for HttpError {}

impl<'c> openidconnect::AsyncHttpClient<'c> for BoundedHttpClient<'_> {
    type Error = HttpError;
    type Future =
        std::pin::Pin<Box<dyn Future<Output = Result<HttpResponse, Self::Error>> + Send + 'c>>;

    fn call(&'c self, request: HttpRequest) -> Self::Future {
        Box::pin(async move {
            let (parts, body) = request.into_parts();
            let url =
                reqwest::Url::parse(&parts.uri.to_string()).map_err(|_| HttpError::Malformed)?;
            let mut builder = self.inner.request(parts.method, url);
            for (name, value) in parts.headers.iter() {
                builder = builder.header(name, value);
            }
            let mut response = builder
                .body(body)
                .send()
                .await
                .map_err(|_| HttpError::Transport)?;
            let status = response.status();
            let headers = response.headers().clone();
            // Read in chunks so an unbounded body is refused as it
            // arrives rather than after it has been buffered.
            let mut collected: Vec<u8> = Vec::new();
            while let Some(chunk) = response.chunk().await.map_err(|_| HttpError::Transport)? {
                if collected.len() + chunk.len() > self.max_body_bytes {
                    return Err(HttpError::TooLarge {
                        limit: self.max_body_bytes,
                    });
                }
                collected.extend_from_slice(&chunk);
            }
            let mut out = openidconnect::http::Response::builder().status(status);
            for (name, value) in headers.iter() {
                out = out.header(name, value);
            }
            out.body(collected).map_err(|_| HttpError::Malformed)
        })
    }
}

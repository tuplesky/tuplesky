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
    IssuerUrl, Nonce, OAuth2TokenResponse, PkceCodeChallenge, PkceCodeVerifier, RedirectUrl,
    TokenResponse,
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
        if !config.issuer.starts_with("https://")
            && !(config.allow_insecure_loopback
                && (config.issuer.starts_with("http://127.0.0.1")
                    || config.issuer.starts_with("http://localhost")))
        {
            return Err(UpstreamError::Config("issuer must be https".into()));
        }
        let issuer = IssuerUrl::new(config.issuer.clone())
            .map_err(|e| UpstreamError::Config(e.to_string()))?;
        let metadata = CoreProviderMetadata::discover_async(issuer.clone(), http)
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
            .request_async(http)
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

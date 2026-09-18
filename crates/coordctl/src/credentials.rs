//! Stored credentials, redacted from diagnostics.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::client::TokenResponse;

/// What the CLI holds after a login.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Credentials {
    /// Broker base URL.
    pub broker: String,
    /// Client identifier.
    pub client_id: String,
    /// Short-lived service token.
    pub access_token: String,
    /// Unix seconds the access token expires at.
    pub expires_at: u64,
    /// Granted scope.
    pub scope: String,
    /// Refresh token (family and current secret), when any.
    pub refresh_token: Option<String>,
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credentials")
            .field("broker", &self.broker)
            .field("client_id", &self.client_id)
            .field("expires_at", &self.expires_at)
            .field("scope", &self.scope)
            .field("access_token", &"<redacted>")
            .field(
                "refresh_token",
                &self.refresh_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

impl Credentials {
    /// Credentials from a token response at `now`.
    pub fn from_response(broker: &str, client_id: &str, now: u64, r: &TokenResponse) -> Self {
        Credentials {
            broker: broker.to_string(),
            client_id: client_id.to_string(),
            access_token: r.access_token.clone(),
            expires_at: now.saturating_add(r.expires_in),
            scope: r.scope.clone(),
            refresh_token: r.refresh_token.clone(),
        }
    }

    /// Whether the access token is usable at `now` with `margin` to spare.
    pub const fn fresh(&self, now: u64, margin: u64) -> bool {
        now.saturating_add(margin) < self.expires_at
    }

    /// What `status` may print: never a secret.
    pub fn summary(&self) -> String {
        format!(
            "broker={} client={} expires_at={} scope=[{}] refresh={}",
            self.broker,
            self.client_id,
            self.expires_at,
            self.scope,
            if self.refresh_token.is_some() {
                "yes"
            } else {
                "no"
            }
        )
    }
}

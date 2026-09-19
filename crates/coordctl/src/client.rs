//! The CLI's broker client: browser login through a loopback callback,
//! device login with bounded polling, refresh and logout. The HTTP
//! client never follows redirects and uses no environment proxy.

use std::fmt;
use std::io::Write;
use std::time::Duration;

use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::store::StoreError;

/// A token response of the broker.
#[derive(Clone, PartialEq, Eq, Deserialize)]
pub struct TokenResponse {
    /// Service token.
    pub access_token: String,
    /// Seconds until expiry.
    pub expires_in: u64,
    /// Scope.
    #[serde(default)]
    pub scope: String,
    /// Refresh token, when a family was bound.
    #[serde(default)]
    pub refresh_token: Option<String>,
}

impl fmt::Debug for TokenResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenResponse")
            .field("expires_in", &self.expires_in)
            .field("scope", &self.scope)
            .field("secrets", &"<redacted>")
            .finish()
    }
}

/// The device authorization the broker issued.
#[derive(Clone, Deserialize)]
pub struct DeviceStart {
    /// Secret the CLI polls with.
    pub device_code: String,
    /// What the user types.
    pub user_code: String,
    /// Where the user goes.
    pub verification_uri: String,
    /// The same with the user code filled in.
    pub verification_uri_complete: String,
    /// Seconds until expiry.
    pub expires_in: u64,
    /// Polling interval.
    pub interval: u64,
}

impl fmt::Debug for DeviceStart {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeviceStart")
            .field("user_code", &self.user_code)
            .field("device_code", &"<redacted>")
            .finish()
    }
}

/// A poll's answer.
#[derive(Debug)]
pub enum PollOutcome {
    /// A token.
    Token(TokenResponse),
    /// Not decided.
    Pending,
    /// Polled too fast: wait longer.
    SlowDown,
    /// The user denied.
    Denied,
    /// The grant expired.
    Expired,
}

/// Why a CLI operation failed (never carries a secret).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CliError {
    /// Transport.
    Http(String),
    /// The broker refused.
    Broker {
        /// Status.
        status: u16,
        /// RFC 6749 code.
        code: String,
        /// Bounded description.
        description: String,
    },
    /// The refresh family was revoked or the session retired: a fresh
    /// interactive login is required. Nothing is recovered silently.
    FreshLoginRequired,
    /// The user denied the device grant.
    Denied,
    /// The grant or login window expired.
    Expired,
    /// The browser callback was not the expected one.
    Callback(&'static str),
    /// Store.
    Store(StoreError),
    /// No refresh token is stored.
    NoRefreshToken,
}

#[derive(Deserialize)]
struct BrokerError {
    error: String,
    #[serde(default)]
    error_description: String,
}

/// The broker client of one CLI invocation.
pub struct BrokerClient {
    http: reqwest::Client,
    broker: String,
    client_id: String,
}

fn random_hex() -> String {
    let mut out = [0u8; 16];
    rustls::crypto::aws_lc_rs::default_provider()
        .secure_random
        .fill(&mut out)
        .expect("secure random");
    out.iter().map(|b| format!("{b:02x}")).collect()
}

fn urlencode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.' || b == b'~' {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn urldecode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16)
        {
            out.push(v);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

impl BrokerClient {
    /// A client for `broker` as `client_id`.
    pub fn new(broker: &str, client_id: &str, timeout: Duration) -> Result<Self, CliError> {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .timeout(timeout)
            .user_agent("coordctl/1")
            .build()
            .map_err(|e| CliError::Http(e.to_string()))?;
        Ok(BrokerClient {
            http,
            broker: broker.trim_end_matches('/').to_string(),
            client_id: client_id.to_string(),
        })
    }

    async fn post_form<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        form: &[(&str, &str)],
    ) -> Result<Result<T, BrokerError>, CliError> {
        let response = self
            .http
            .post(format!("{}{path}", self.broker))
            .form(form)
            .send()
            .await
            .map_err(|e| CliError::Http(classify(&e)))?;
        let status = response.status().as_u16();
        let body = response
            .bytes()
            .await
            .map_err(|e| CliError::Http(classify(&e)))?;
        if status == 200 {
            return serde_json::from_slice::<T>(&body)
                .map(Ok)
                .map_err(|_| CliError::Http("malformed broker response".into()));
        }
        match serde_json::from_slice::<BrokerError>(&body) {
            Ok(e) => Ok(Err(e)),
            Err(_) => Err(CliError::Broker {
                status,
                code: "unknown".into(),
                description: String::new(),
            }),
        }
    }

    /// Start a device login.
    pub async fn device_authorize(&self) -> Result<DeviceStart, CliError> {
        match self
            .post_form::<DeviceStart>("/device/authorize", &[("client_id", &self.client_id)])
            .await?
        {
            Ok(d) => Ok(d),
            Err(e) => Err(broker_error(400, e)),
        }
    }

    /// Poll a device grant once.
    pub async fn device_poll(&self, device_code: &str) -> Result<PollOutcome, CliError> {
        match self
            .post_form::<TokenResponse>(
                "/device/token",
                &[
                    ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
                    ("device_code", device_code),
                    ("client_id", &self.client_id),
                ],
            )
            .await?
        {
            Ok(t) => Ok(PollOutcome::Token(t)),
            Err(e) => Ok(match e.error.as_str() {
                "authorization_pending" => PollOutcome::Pending,
                "slow_down" => PollOutcome::SlowDown,
                "access_denied" => PollOutcome::Denied,
                "expired_token" => PollOutcome::Expired,
                _ => return Err(broker_error(400, e)),
            }),
        }
    }

    /// Device login end to end: show the user code, poll within the
    /// interval and the grant's lifetime, honouring `slow_down`.
    pub async fn device_login(&self, ui: &mut dyn Write) -> Result<TokenResponse, CliError> {
        let start = self.device_authorize().await?;
        let _ = writeln!(
            ui,
            "Open {} and enter the code {} (or open {})",
            start.verification_uri, start.user_code, start.verification_uri_complete
        );
        let mut interval = start.interval.max(1);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(start.expires_in);
        loop {
            tokio::time::sleep(Duration::from_secs(interval)).await;
            if tokio::time::Instant::now() > deadline {
                return Err(CliError::Expired);
            }
            match self.device_poll(&start.device_code).await? {
                PollOutcome::Token(t) => return Ok(t),
                PollOutcome::Pending => {}
                PollOutcome::SlowDown => interval += 5,
                PollOutcome::Denied => return Err(CliError::Denied),
                PollOutcome::Expired => return Err(CliError::Expired),
            }
        }
    }

    /// Browser login: a loopback listener receives the one-time service
    /// code on the expected path with the expected state; the code is
    /// redeemed with the PKCE verifier. `wait` bounds the whole login.
    pub async fn browser_login(
        &self,
        ui: &mut dyn Write,
        wait: Duration,
    ) -> Result<TokenResponse, CliError> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| CliError::Http(e.to_string()))?;
        let port = listener
            .local_addr()
            .map_err(|e| CliError::Http(e.to_string()))?
            .port();
        let redirect_uri = format!("http://127.0.0.1:{port}/callback");
        let (challenge, verifier) = oauth2::PkceCodeChallenge::new_random_sha256();
        let state = random_hex();
        let url = format!(
            "{}/login/start?client_id={}&redirect_uri={}&code_challenge={}&code_challenge_method=S256&state={}",
            self.broker,
            urlencode(&self.client_id),
            urlencode(&redirect_uri),
            challenge.as_str(),
            state
        );
        let _ = writeln!(ui, "Open this URL in your browser to log in:\n{url}");
        let callback = tokio::time::timeout(wait, accept_callback(&listener, &state))
            .await
            .map_err(|_| CliError::Expired)??;
        match self
            .post_form::<TokenResponse>(
                "/login/redeem",
                &[
                    ("code", &callback),
                    ("code_verifier", verifier.secret()),
                    ("client_id", &self.client_id),
                    ("redirect_uri", &redirect_uri),
                ],
            )
            .await?
        {
            Ok(t) => Ok(t),
            Err(e) => Err(broker_error(400, e)),
        }
    }

    /// Rotate the refresh family and obtain a new token.
    pub async fn refresh(&self, refresh_token: &str) -> Result<TokenResponse, CliError> {
        match self
            .post_form::<TokenResponse>("/login/refresh", &[("refresh_token", refresh_token)])
            .await?
        {
            Ok(t) => Ok(t),
            Err(e) if e.error == "invalid_grant" => Err(CliError::FreshLoginRequired),
            Err(e) => Err(broker_error(400, e)),
        }
    }

    /// Retire the session.
    pub async fn logout(&self, refresh_token: &str) -> Result<(), CliError> {
        match self
            .post_form::<serde_json::Value>("/login/logout", &[("refresh_token", refresh_token)])
            .await?
        {
            Ok(_) => Ok(()),
            Err(e) if e.error == "invalid_grant" => Ok(()),
            Err(e) => Err(broker_error(400, e)),
        }
    }
}

fn classify(e: &reqwest::Error) -> String {
    if e.is_timeout() {
        "timeout".into()
    } else if e.is_connect() {
        "connect".into()
    } else {
        "transport".into()
    }
}

fn broker_error(status: u16, e: BrokerError) -> CliError {
    CliError::Broker {
        status,
        code: e.error,
        description: e.error_description,
    }
}

/// Test hook: [`accept_callback`] on a caller-provided listener.
pub async fn accept_callback_for_test(
    listener: &TcpListener,
    state: &str,
) -> Result<String, CliError> {
    accept_callback(listener, state).await
}

/// Accept exactly one callback on the listener: the path must be
/// `/callback` and the state must match; anything else is answered with
/// an error and the wait continues.
async fn accept_callback(listener: &TcpListener, state: &str) -> Result<String, CliError> {
    loop {
        let (mut socket, peer) = listener
            .accept()
            .await
            .map_err(|e| CliError::Http(e.to_string()))?;
        if !peer.ip().is_loopback() {
            continue;
        }
        let mut buf = vec![0u8; 8192];
        let n = socket.read(&mut buf).await.unwrap_or(0);
        let request = String::from_utf8_lossy(&buf[..n]).into_owned();
        let line = request.lines().next().unwrap_or("");
        let target = line.split_whitespace().nth(1).unwrap_or("");
        let (path, query) = target.split_once('?').unwrap_or((target, ""));
        let mut code = None;
        let mut got_state = None;
        for kv in query.split('&') {
            if let Some(v) = kv.strip_prefix("code=") {
                code = Some(urldecode(v));
            } else if let Some(v) = kv.strip_prefix("state=") {
                got_state = Some(urldecode(v));
            }
        }
        let ok = path == "/callback" && got_state.as_deref() == Some(state) && code.is_some();
        let body = if ok {
            "Login complete. You may close this window."
        } else {
            "Unexpected callback."
        };
        let response = format!(
            "HTTP/1.1 {} OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            if ok { 200 } else { 400 },
            body.len()
        );
        let _ = socket.write_all(response.as_bytes()).await;
        let _ = socket.shutdown().await;
        if ok {
            return Ok(code.expect("checked"));
        }
    }
}

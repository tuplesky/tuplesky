//! Bounded Axum handlers of the browser login (design Sections 8.1,
//! 20.1): `GET /login/start` redirects the browser to the upstream
//! authorization request, `GET /login/callback` completes the upstream
//! leg and redirects the browser to the CLI's loopback with the one-time
//! service code, and `POST /login/redeem` turns the code, verifier,
//! client and redirect into a session and service token. Every step
//! is bounded in body, concurrency and time; failures issue nothing.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::extract::{DefaultBodyLimit, Form, Query, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use coord_authn::VerifiedIdentity;
use coord_state::InternalCommand;
use coord_state::plan::Outcome;
use coord_state::policy::GrantKind;
use coord_sts::{ClockSource, EntropySource, ExchangeError, HttpLimits, Sts};
use coord_types::ids::NamespaceId;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::{Mutex, Semaphore};

use crate::device::{DeviceError, DeviceLogin, Poll};
use crate::refresh::{RefreshError, SessionBackend, new_family, refresh_token};
use crate::service::{LoginError, RedeemRequest, ServiceLogin, StartRequest, UpstreamIdentity};
use crate::upstream::{Upstream, UpstreamError};

/// Shared handler state.
pub struct LoginState {
    /// The service-login core.
    pub login: Mutex<ServiceLogin>,
    /// The device-grant core.
    pub device: Mutex<DeviceLogin>,
    /// Discovered upstream clients by configuration name.
    pub upstreams: BTreeMap<String, Upstream>,
    /// The hardened HTTP client for the upstream leg.
    pub http: reqwest::Client,
    /// The STS (trust rules, receipts, signing).
    pub sts: Mutex<Sts>,
    /// The port to replicated state.
    pub creator: Mutex<Box<dyn SessionBackend + Send>>,
    /// Clock.
    pub clock: Box<dyn ClockSource>,
    /// Entropy.
    pub entropy: Box<dyn EntropySource>,
    /// Namespace grant commands are planned in.
    pub namespace: NamespaceId,
    /// Bounds.
    pub limits: HttpLimits,
    permits: Semaphore,
}

impl LoginState {
    /// Assemble the state.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        login: ServiceLogin,
        device: DeviceLogin,
        upstreams: BTreeMap<String, Upstream>,
        http: reqwest::Client,
        sts: Sts,
        creator: Box<dyn SessionBackend + Send>,
        clock: Box<dyn ClockSource>,
        entropy: Box<dyn EntropySource>,
        namespace: NamespaceId,
        limits: HttpLimits,
    ) -> Self {
        LoginState {
            login: Mutex::new(login),
            device: Mutex::new(device),
            upstreams,
            http,
            sts: Mutex::new(sts),
            creator: Mutex::new(creator),
            clock,
            entropy,
            namespace,
            limits,
            permits: Semaphore::new(limits.max_in_flight),
        }
    }
}

/// The router.
pub fn router(state: Arc<LoginState>) -> Router {
    let limit = state.limits.max_body_bytes;
    Router::new()
        .route("/login/start", get(start))
        .route("/login/callback", get(callback))
        .route("/login/redeem", post(redeem))
        .route("/device/authorize", post(device_authorize))
        .route("/device/verify", get(device_verify))
        .route("/device/callback", get(device_callback))
        .route("/device/deny", post(device_deny))
        .route("/device/token", post(device_token))
        .route("/login/refresh", post(refresh_handler))
        .route("/login/logout", post(logout_handler))
        .layer(DefaultBodyLimit::max(limit))
        .with_state(state)
}

fn error(status: StatusCode, code: &str, description: &str) -> Response {
    (
        status,
        Json(json!({ "error": code, "error_description": description })),
    )
        .into_response()
}

fn login_error(e: &LoginError) -> Response {
    let code = match e {
        LoginError::TooManyPending => "temporarily_unavailable",
        LoginError::UnknownClient | LoginError::ClientMismatch => "invalid_client",
        LoginError::UnknownCode | LoginError::CodeExpired | LoginError::VerifierMismatch => {
            "invalid_grant"
        }
        _ => "invalid_request",
    };
    let status = if matches!(e, LoginError::TooManyPending) {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::BAD_REQUEST
    };
    error(status, code, &format!("{e:?}"))
}

fn redirect(location: &str) -> Response {
    let mut r = StatusCode::FOUND.into_response();
    if let Ok(v) = HeaderValue::from_str(location) {
        r.headers_mut().insert(header::LOCATION, v);
    }
    r.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    r
}

#[derive(Deserialize)]
struct StartQuery {
    client_id: String,
    redirect_uri: String,
    code_challenge: String,
    code_challenge_method: String,
    state: String,
}

async fn start(State(state): State<Arc<LoginState>>, Query(q): Query<StartQuery>) -> Response {
    let clock = state.clock.read();
    let entropy = state.entropy.fill();
    let request = StartRequest {
        client_id: q.client_id,
        redirect_uri: q.redirect_uri,
        code_challenge: q.code_challenge,
        code_challenge_method: q.code_challenge_method,
        state: q.state,
    };
    let started = match state
        .login
        .lock()
        .await
        .start(clock.now, &request, &entropy)
    {
        Ok(s) => s,
        Err(e) => return login_error(&e),
    };
    let Some(upstream) = state.upstreams.get(&started.upstream) else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "temporarily_unavailable",
            "upstream",
        );
    };
    let url = upstream.authorize_url(
        &started.upstream_state,
        &started.upstream_nonce,
        started.upstream_challenge,
    );
    redirect(&url)
}

#[derive(Deserialize)]
struct CallbackQuery {
    code: Option<String>,
    state: String,
    error: Option<String>,
}

async fn callback(
    State(state): State<Arc<LoginState>>,
    Query(q): Query<CallbackQuery>,
) -> Response {
    let Ok(_permit) = state.permits.try_acquire() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "temporarily_unavailable",
            "busy",
        );
    };
    if q.error.is_some() {
        return error(StatusCode::BAD_REQUEST, "access_denied", "upstream denied");
    }
    let Some(code) = q.code else {
        return error(StatusCode::BAD_REQUEST, "invalid_request", "missing code");
    };
    let clock = state.clock.read();
    let (_, verifier, upstream_name, nonce) = {
        let login = state.login.lock().await;
        match login.upstream_exchange(&q.state) {
            Ok((txn, verifier, name)) => {
                let nonce = login.upstream_nonce(&q.state).unwrap_or_default();
                (txn, verifier, name, nonce)
            }
            Err(e) => return login_error(&e),
        }
    };
    let Some(upstream) = state.upstreams.get(&upstream_name) else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "temporarily_unavailable",
            "upstream",
        );
    };
    let identity = match tokio::time::timeout(
        state.limits.timeout,
        upstream.exchange(code, verifier, &nonce, &state.http),
    )
    .await
    {
        Ok(Ok(i)) => i,
        Ok(Err(UpstreamError::Policy(e))) => return login_error(&e),
        Ok(Err(_)) => {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid_grant",
                "upstream exchange",
            );
        }
        Err(_) => {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "temporarily_unavailable",
                "timeout",
            );
        }
    };
    let entropy = state.entropy.fill();
    let approved = match state.login.lock().await.approve(
        clock.now,
        &q.state,
        identity,
        &upstream.config().issuer,
        &entropy,
    ) {
        Ok(a) => a,
        Err(e) => return login_error(&e),
    };
    // The grant is ordered before the browser learns the code.
    let committed = state
        .creator
        .lock()
        .await
        .create(InternalCommand::CommitGrant {
            namespace: state.namespace,
            commitment: approved.commitment,
            kind: GrantKind::Code,
        });
    match committed {
        Ok(r) if r.outcome == Outcome::GrantCommitted => redirect(&approved.redirect),
        _ => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "temporarily_unavailable",
            "grant",
        ),
    }
}

#[derive(Deserialize)]
struct RedeemForm {
    code: String,
    code_verifier: String,
    client_id: String,
    redirect_uri: String,
}

async fn redeem(State(state): State<Arc<LoginState>>, Form(form): Form<RedeemForm>) -> Response {
    let Ok(_permit) = state.permits.try_acquire() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "temporarily_unavailable",
            "busy",
        );
    };
    let clock = state.clock.read();
    let request = RedeemRequest {
        code: form.code,
        code_verifier: form.code_verifier,
        client_id: form.client_id,
        redirect_uri: form.redirect_uri,
    };
    let redeemed = match state.login.lock().await.redeem(clock.now, &request) {
        Ok(r) => r,
        Err(e) => return login_error(&e),
    };
    issue_with_family(
        &state,
        &redeemed.identity,
        &redeemed.upstream,
        redeemed.commitment,
        &clock,
    )
    .await
}

/// Order a refresh family, create the session bound to it and the login
/// grant, and answer with the service token and the refresh token.
async fn issue_with_family(
    state: &LoginState,
    identity: &UpstreamIdentity,
    upstream: &str,
    code: coord_types::identity::Digest32,
    clock: &coord_authn::ClockHealth,
) -> Response {
    let identity = verified(identity, upstream);
    let (secret, family) = new_family(&state.entropy.fill());
    let entropy = state.entropy.fill();
    let result = {
        let mut sts = state.sts.lock().await;
        let mut creator = state.creator.lock().await;
        match creator.create(InternalCommand::CommitGrant {
            namespace: state.namespace,
            commitment: family,
            kind: GrantKind::RefreshFamily,
        }) {
            Ok(r) if r.outcome == Outcome::GrantCommitted => {}
            _ => {
                return error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "temporarily_unavailable",
                    "grant",
                );
            }
        }
        sts.issue(
            &identity,
            Some(code),
            Some(family),
            None,
            clock,
            &entropy,
            creator.as_mut(),
        )
    };
    match result {
        Ok(mut response) => {
            response.refresh_token = Some(refresh_token(&family, &secret));
            (
                StatusCode::OK,
                Json(serde_json::to_value(response).expect("serializable")),
            )
                .into_response()
        }
        Err(e) => exchange_error(&e),
    }
}

#[derive(Deserialize)]
struct RefreshForm {
    refresh_token: String,
}

async fn refresh_handler(
    State(state): State<Arc<LoginState>>,
    Form(form): Form<RefreshForm>,
) -> Response {
    let Ok(_permit) = state.permits.try_acquire() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "temporarily_unavailable",
            "busy",
        );
    };
    let clock = state.clock.read();
    let entropy = state.entropy.fill();
    let result = {
        let mut sts = state.sts.lock().await;
        let mut creator = state.creator.lock().await;
        crate::refresh::refresh(
            &form.refresh_token,
            state.namespace,
            &clock,
            &entropy,
            &mut sts,
            creator.as_mut(),
        )
    };
    match result {
        Ok(response) => (
            StatusCode::OK,
            Json(serde_json::to_value(response).expect("serializable")),
        )
            .into_response(),
        Err(e) => refresh_error(&e),
    }
}

async fn logout_handler(
    State(state): State<Arc<LoginState>>,
    Form(form): Form<RefreshForm>,
) -> Response {
    let result = {
        let mut creator = state.creator.lock().await;
        crate::refresh::logout(&form.refresh_token, state.namespace, creator.as_mut())
    };
    match result {
        Ok(()) => (StatusCode::OK, Json(json!({ "logged_out": true }))).into_response(),
        Err(e) => refresh_error(&e),
    }
}

fn refresh_error(e: &RefreshError) -> Response {
    match e {
        RefreshError::Malformed => {
            error(StatusCode::BAD_REQUEST, "invalid_request", "refresh_token")
        }
        RefreshError::UnknownFamily | RefreshError::SessionRetired => error(
            StatusCode::BAD_REQUEST,
            "invalid_grant",
            "no such family or session",
        ),
        RefreshError::FamilyRevoked => error(
            StatusCode::BAD_REQUEST,
            "invalid_grant",
            "refresh family revoked: a retired secret was presented; log in again",
        ),
        RefreshError::Unavailable => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "temporarily_unavailable",
            "replicated state",
        ),
        RefreshError::Exchange(x) => exchange_error(x),
    }
}

fn exchange_error(e: &ExchangeError) -> Response {
    error(
        StatusCode::from_u16(e.status()).expect("valid"),
        e.code(),
        e.description(),
    )
}

fn device_error(e: &DeviceError) -> Response {
    let (status, code) = match e {
        DeviceError::TooManyPending | DeviceError::TooManyAttempts => {
            (StatusCode::TOO_MANY_REQUESTS, "slow_down")
        }
        DeviceError::UnknownClient => (StatusCode::BAD_REQUEST, "invalid_client"),
        DeviceError::UnknownDeviceCode => (StatusCode::BAD_REQUEST, "invalid_grant"),
        _ => (StatusCode::BAD_REQUEST, "invalid_request"),
    };
    error(status, code, &format!("{e:?}"))
}

#[derive(Deserialize)]
struct DeviceAuthorizeForm {
    client_id: String,
}

async fn device_authorize(
    State(state): State<Arc<LoginState>>,
    Form(form): Form<DeviceAuthorizeForm>,
) -> Response {
    let clock = state.clock.read();
    let entropy = state.entropy.fill();
    match state
        .device
        .lock()
        .await
        .authorize(clock.now, &form.client_id, &entropy)
    {
        Ok(a) => (
            StatusCode::OK,
            Json(json!({
                "device_code": a.device_code,
                "user_code": a.user_code,
                "verification_uri": a.verification_uri,
                "verification_uri_complete": a.verification_uri_complete,
                "expires_in": a.expires_in,
                "interval": a.interval,
            })),
        )
            .into_response(),
        Err(e) => device_error(&e),
    }
}

#[derive(Deserialize)]
struct UserCodeQuery {
    user_code: String,
}

async fn device_verify(
    State(state): State<Arc<LoginState>>,
    Query(q): Query<UserCodeQuery>,
) -> Response {
    let clock = state.clock.read();
    let entropy = state.entropy.fill();
    let started = match state
        .device
        .lock()
        .await
        .begin_browser(clock.now, &q.user_code, &entropy)
    {
        Ok(s) => s,
        Err(e) => return device_error(&e),
    };
    let Some(upstream) = state.upstreams.get(&started.upstream) else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "temporarily_unavailable",
            "upstream",
        );
    };
    redirect(&upstream.authorize_url(
        &started.upstream_state,
        &started.upstream_nonce,
        started.upstream_challenge,
    ))
}

async fn device_callback(
    State(state): State<Arc<LoginState>>,
    Query(q): Query<CallbackQuery>,
) -> Response {
    let Ok(_permit) = state.permits.try_acquire() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "temporarily_unavailable",
            "busy",
        );
    };
    let Some(code) = q.code else {
        return error(StatusCode::BAD_REQUEST, "access_denied", "upstream denied");
    };
    let clock = state.clock.read();
    let (verifier, upstream_name, nonce) =
        match state.device.lock().await.upstream_exchange(&q.state) {
            Ok(v) => v,
            Err(e) => return device_error(&e),
        };
    let Some(upstream) = state.upstreams.get(&upstream_name) else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "temporarily_unavailable",
            "upstream",
        );
    };
    let identity = match tokio::time::timeout(
        state.limits.timeout,
        upstream.exchange(code, verifier, &nonce, &state.http),
    )
    .await
    {
        Ok(Ok(i)) => i,
        Ok(Err(_)) => {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid_grant",
                "upstream exchange",
            );
        }
        Err(_) => {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "temporarily_unavailable",
                "timeout",
            );
        }
    };
    let commitment = match state.device.lock().await.complete_browser(
        clock.now,
        &q.state,
        identity,
        &upstream.config().issuer,
    ) {
        Ok(c) => c,
        Err(e) => return device_error(&e),
    };
    let committed = state
        .creator
        .lock()
        .await
        .create(InternalCommand::CommitGrant {
            namespace: state.namespace,
            commitment,
            kind: GrantKind::Code,
        });
    match committed {
        Ok(r) if r.outcome == Outcome::GrantCommitted => {
            (StatusCode::OK, Json(json!({ "approved": true }))).into_response()
        }
        _ => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "temporarily_unavailable",
            "grant",
        ),
    }
}

async fn device_deny(
    State(state): State<Arc<LoginState>>,
    Form(form): Form<UserCodeQuery>,
) -> Response {
    let clock = state.clock.read();
    match state.device.lock().await.deny(clock.now, &form.user_code) {
        Ok(()) => (StatusCode::OK, Json(json!({ "denied": true }))).into_response(),
        Err(e) => device_error(&e),
    }
}

#[derive(Deserialize)]
struct DeviceTokenForm {
    grant_type: String,
    device_code: String,
}

async fn device_token(
    State(state): State<Arc<LoginState>>,
    Form(form): Form<DeviceTokenForm>,
) -> Response {
    if form.grant_type != "urn:ietf:params:oauth:grant-type:device_code" {
        return error(
            StatusCode::BAD_REQUEST,
            "unsupported_grant_type",
            "grant_type",
        );
    }
    let Ok(_permit) = state.permits.try_acquire() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "temporarily_unavailable",
            "busy",
        );
    };
    let clock = state.clock.read();
    let redeemed = match state.device.lock().await.poll(clock.now, &form.device_code) {
        Ok(Poll::Approved(r)) => r,
        Ok(Poll::Pending) => {
            return error(StatusCode::BAD_REQUEST, "authorization_pending", "pending");
        }
        Ok(Poll::SlowDown) => return error(StatusCode::BAD_REQUEST, "slow_down", "interval"),
        Ok(Poll::Denied) => return error(StatusCode::BAD_REQUEST, "access_denied", "denied"),
        Ok(Poll::Expired) => return error(StatusCode::BAD_REQUEST, "expired_token", "expired"),
        Err(e) => return device_error(&e),
    };
    issue_with_family(
        &state,
        &redeemed.identity,
        &redeemed.upstream,
        redeemed.commitment,
        &clock,
    )
    .await
}

/// The verified identity of a redeemed login, as trust rules see it:
/// `(issuer, subject)`, never an email.
pub fn verified(identity: &UpstreamIdentity, upstream: &str) -> VerifiedIdentity {
    VerifiedIdentity {
        name: upstream.to_string(),
        issuer: identity.issuer.clone(),
        subject: identity.subject.clone(),
        audiences: identity.audiences.clone(),
        expires_at: identity.expires_at,
        issued_at: None,
        azp: identity.authorized_party.clone(),
        nonce: None,
        claims: BTreeMap::new(),
        workload: None,
    }
}

/// Convenience for handlers that only need a value.
pub fn value(v: Value) -> Json<Value> {
    Json(v)
}

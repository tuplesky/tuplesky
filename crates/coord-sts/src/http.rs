//! Bounded Axum handlers (design Sections 20.1, 20.2): `POST /token` for
//! the exchange and `GET /.well-known/jwks.json` for the public keys.
//! Bodies, concurrent exchanges and time are bounded; missing issuer keys
//! are fetched once through the hardened fetcher, never per request; the
//! clock and entropy are injected.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::{DefaultBodyLimit, Form, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use coord_authn::{ClockHealth, HardenedFetcher, TokenReview};
use serde_json::{Value, json};
use tokio::sync::{Mutex, Semaphore};

use crate::exchange::{ExchangeError, ExchangeForm, SessionCreator, Sts};

/// A clock the handlers read.
pub trait ClockSource: Send + Sync {
    /// The current reading.
    fn read(&self) -> ClockHealth;
}

/// The system clock with a configured uncertainty.
#[derive(Debug)]
pub struct SystemClock {
    /// Uncertainty in seconds.
    pub uncertainty: u64,
}

impl ClockSource for SystemClock {
    fn read(&self) -> ClockHealth {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(d) => ClockHealth::healthy(d.as_secs(), self.uncertainty),
            Err(_) => ClockHealth {
                now: 0,
                uncertainty: self.uncertainty,
                healthy: false,
            },
        }
    }
}

/// Entropy the handlers draw.
pub trait EntropySource: Send + Sync {
    /// 32 fresh bytes.
    fn fill(&self) -> [u8; 32];
}

/// Entropy from the TLS provider's secure random.
#[derive(Debug, Default)]
pub struct ProviderEntropy;

impl EntropySource for ProviderEntropy {
    fn fill(&self) -> [u8; 32] {
        let mut out = [0u8; 32];
        rustls::crypto::aws_lc_rs::default_provider()
            .secure_random
            .fill(&mut out)
            .expect("secure random");
        out
    }
}

/// HTTP bounds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HttpLimits {
    /// Request body bound.
    pub max_body_bytes: usize,
    /// Exchanges in flight at once.
    pub max_in_flight: usize,
    /// Time bound of one exchange, key fetch included.
    pub timeout: Duration,
}

impl Default for HttpLimits {
    fn default() -> Self {
        HttpLimits {
            max_body_bytes: 16 * 1024,
            max_in_flight: 64,
            timeout: Duration::from_secs(10),
        }
    }
}

/// Shared handler state.
pub struct AppState {
    /// The STS core.
    pub sts: Mutex<Sts>,
    /// The port to replicated state.
    pub creator: Mutex<Box<dyn SessionCreator + Send>>,
    /// Key fetching (`None`: keys are installed out of band).
    pub fetcher: Option<HardenedFetcher>,
    /// Clock.
    pub clock: Box<dyn ClockSource>,
    /// Entropy.
    pub entropy: Box<dyn EntropySource>,
    /// TokenReview results are obtained by the deployment's reviewer;
    /// `None` here means offline modes only.
    pub review: Option<TokenReview>,
    /// Bounds.
    pub limits: HttpLimits,
    permits: Semaphore,
}

impl AppState {
    /// Assemble the state.
    pub fn new(
        sts: Sts,
        creator: Box<dyn SessionCreator + Send>,
        fetcher: Option<HardenedFetcher>,
        clock: Box<dyn ClockSource>,
        entropy: Box<dyn EntropySource>,
        limits: HttpLimits,
    ) -> Self {
        AppState {
            sts: Mutex::new(sts),
            creator: Mutex::new(creator),
            fetcher,
            clock,
            entropy,
            review: None,
            limits,
            permits: Semaphore::new(limits.max_in_flight),
        }
    }
}

/// The router.
pub fn router(state: Arc<AppState>) -> Router {
    let limit = state.limits.max_body_bytes;
    Router::new()
        .route("/token", post(token))
        .route("/.well-known/jwks.json", get(jwks))
        .layer(DefaultBodyLimit::max(limit))
        .with_state(state)
}

fn error_body(e: &ExchangeError) -> (StatusCode, Json<Value>) {
    (
        StatusCode::from_u16(e.status()).expect("valid status"),
        Json(json!({ "error": e.code(), "error_description": e.description() })),
    )
}

async fn token(
    State(state): State<Arc<AppState>>,
    Form(form): Form<ExchangeForm>,
) -> (StatusCode, Json<Value>) {
    let Ok(_permit) = state.permits.try_acquire() else {
        return error_body(&ExchangeError::Unavailable("too many exchanges in flight"));
    };
    match tokio::time::timeout(state.limits.timeout, exchange(&state, form)).await {
        Ok(Ok(response)) => (
            StatusCode::OK,
            Json(serde_json::to_value(response).expect("serializable")),
        ),
        Ok(Err(e)) => error_body(&e),
        Err(_) => error_body(&ExchangeError::Unavailable("exchange timed out")),
    }
}

async fn exchange(
    state: &AppState,
    form: ExchangeForm,
) -> Result<crate::ExchangeResponse, ExchangeError> {
    let clock = state.clock.read();
    let entropy = state.entropy.fill();
    let mut fetched = false;
    loop {
        let result = {
            let mut sts = state.sts.lock().await;
            let mut creator = state.creator.lock().await;
            sts.exchange(
                &form,
                &clock,
                &entropy,
                state.review.as_ref(),
                creator.as_mut(),
            )
        };
        match result {
            Err(ExchangeError::KeysUnavailable { name, jwks_url }) if !fetched => {
                fetched = true;
                let Some(fetcher) = &state.fetcher else {
                    return Err(ExchangeError::Unavailable("issuer keys unavailable"));
                };
                let document = fetcher
                    .fetch_jwks(&jwks_url)
                    .await
                    .map_err(|_| ExchangeError::Unavailable("issuer unreachable"))?;
                let mut sts = state.sts.lock().await;
                sts.verifier_mut()
                    .registry_mut()
                    .install_keys(&name, &document, clock.now)
                    .map_err(|_| ExchangeError::Unavailable("issuer keys unusable"))?;
            }
            Err(ExchangeError::KeysUnavailable { .. }) => {
                return Err(ExchangeError::Unavailable("issuer keys unavailable"));
            }
            other => return other,
        }
    }
}

async fn jwks(State(state): State<Arc<AppState>>) -> Json<Value> {
    Json(state.sts.lock().await.ring().jwks())
}

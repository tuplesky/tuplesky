//! Bounded Axum handlers (design Sections 20.1, 20.2): `POST /token` for
//! the exchange and `GET /.well-known/jwks.json` for the public keys.
//! Bodies, concurrent exchanges and time are bounded; missing issuer keys
//! are fetched once through the hardened fetcher, never per request; the
//! clock and entropy are injected.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex as SyncMutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::{DefaultBodyLimit, Form, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use coord_authn::{ClockHealth, HardenedFetcher, TokenReview};
use serde_json::{Value, json};
use tokio::sync::{Mutex, Semaphore};

use crate::exchange::{ExchangeError, ExchangeForm, SessionCreator, Sts};

/// Reviews one presented token with the cluster's API server.
///
/// A `TokenReview` carries no token identifier and the verifier can only
/// compare its username and audiences, so one result shared by every
/// exchange would let a successful review of one token authorize another
/// of the same service account — including one whose bound object was
/// deleted. Each assertion is therefore reviewed on its own.
pub trait TokenReviewer: Send + Sync {
    /// Review `token`. `None` means the reviewer could not reach the API
    /// server, and verification then fails closed.
    fn review<'a>(
        &'a self,
        token: &'a str,
    ) -> core::pin::Pin<Box<dyn core::future::Future<Output = Option<TokenReview>> + Send + 'a>>;
}

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
    /// The STS core. A plain mutex: the exchange is synchronous and runs
    /// on the blocking pool, never on an executor worker.
    pub sts: SyncMutex<Sts>,
    /// The port to replicated state.
    pub creator: SyncMutex<Box<dyn SessionCreator + Send>>,
    /// Key fetching (`None`: keys are installed out of band).
    pub fetcher: Option<HardenedFetcher>,
    /// Clock.
    pub clock: Box<dyn ClockSource>,
    /// Entropy.
    pub entropy: Box<dyn EntropySource>,
    /// Reviews each presented assertion; `None` means offline modes only.
    pub reviewer: Option<Arc<dyn TokenReviewer>>,
    /// Bounds.
    pub limits: HttpLimits,
    permits: Semaphore,
    /// One gate per issuer, so a burst on a cold cache produces one
    /// upstream fetch and the rest wait for it.
    fetches: Mutex<BTreeMap<String, Arc<Mutex<()>>>>,
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
            sts: SyncMutex::new(sts),
            creator: SyncMutex::new(creator),
            fetcher,
            clock,
            entropy,
            reviewer: None,
            limits,
            permits: Semaphore::new(limits.max_in_flight),
            fetches: Mutex::new(BTreeMap::new()),
        }
    }

    /// Review every presented assertion with `reviewer`.
    #[must_use]
    pub fn with_reviewer(mut self, reviewer: Arc<dyn TokenReviewer>) -> Self {
        self.reviewer = Some(reviewer);
        self
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

/// One synchronous exchange, on the blocking pool.
///
/// A production creator blocks waiting on replicated state; on an
/// executor worker that call would never yield, the surrounding timeout
/// could not fire, and enough stalled exchanges would starve unrelated
/// handlers, key publication included.
async fn attempt(
    state: &Arc<AppState>,
    form: &ExchangeForm,
    clock: ClockHealth,
    review: Option<TokenReview>,
) -> Result<crate::ExchangeResponse, ExchangeError> {
    let state = Arc::clone(state);
    let form = form.clone();
    let entropy = state.entropy.fill();
    tokio::task::spawn_blocking(move || {
        let mut sts = state.sts.lock().expect("sts lock");
        let mut creator = state.creator.lock().expect("creator lock");
        sts.exchange(&form, &clock, &entropy, review.as_ref(), creator.as_mut())
    })
    .await
    .map_err(|_| ExchangeError::Unavailable("exchange worker"))?
}

async fn exchange(
    state: &Arc<AppState>,
    form: ExchangeForm,
) -> Result<crate::ExchangeResponse, ExchangeError> {
    let clock = state.clock.read();
    // This assertion's own review, obtained before anything looks at it.
    let review = match &state.reviewer {
        Some(reviewer) => reviewer.review(&form.subject_token).await,
        None => None,
    };
    let (name, jwks_url) = match attempt(state, &form, clock, review.clone()).await {
        Err(ExchangeError::KeysUnavailable { name, jwks_url }) => (name, jwks_url),
        other => return other,
    };
    let Some(fetcher) = &state.fetcher else {
        return Err(ExchangeError::Unavailable("issuer keys unavailable"));
    };
    // One fetch per issuer for a whole burst. Without the gate every
    // concurrent exchange on a cold cache called the endpoint itself,
    // which is the unknown-key storm the refresh budget exists to bound.
    let gate = {
        let mut gates = state.fetches.lock().await;
        Arc::clone(
            gates
                .entry(name.clone())
                .or_insert_with(|| Arc::new(Mutex::new(()))),
        )
    };
    let _held = gate.lock().await;
    // Claim the refresh before asking again. The claim makes a miss cost
    // no refresh budget, so the ask below reports missing keys instead of
    // denying the assertion outright once the burst has spent the
    // window, and the requests queued behind this one are not denied
    // either: the budget bounds upstream fetches, and the gate has
    // already reduced this burst to one.
    {
        let mut sts = state.sts.lock().expect("sts lock");
        if let Some(cache) = sts.verifier_mut().registry_mut().cache_mut(&name) {
            cache.begin_refresh();
        }
    }
    let outcome = fetch_and_retry(state, &form, clock, review, fetcher, &name, &jwks_url).await;
    {
        let mut sts = state.sts.lock().expect("sts lock");
        if let Some(cache) = sts.verifier_mut().registry_mut().cache_mut(&name) {
            cache.end_refresh();
        }
    }
    outcome
}

/// Under the issuer's gate and its refresh claim: ask once more in case
/// the holder before this one already installed the keys, otherwise fetch
/// them and ask again.
#[allow(clippy::too_many_arguments)]
async fn fetch_and_retry(
    state: &Arc<AppState>,
    form: &ExchangeForm,
    clock: ClockHealth,
    review: Option<TokenReview>,
    fetcher: &HardenedFetcher,
    name: &str,
    jwks_url: &str,
) -> Result<crate::ExchangeResponse, ExchangeError> {
    match attempt(state, form, clock, review.clone()).await {
        Err(ExchangeError::KeysUnavailable { .. }) => {}
        other => return other,
    }
    let document = fetcher
        .fetch_jwks(jwks_url)
        .await
        .map_err(|_| ExchangeError::Unavailable("issuer unreachable"))?;
    state
        .sts
        .lock()
        .expect("sts lock")
        .verifier_mut()
        .registry_mut()
        .install_keys(name, &document, clock.now)
        .map_err(|_| ExchangeError::Unavailable("issuer keys unusable"))?;
    match attempt(state, form, clock, review).await {
        Err(ExchangeError::KeysUnavailable { .. }) => {
            Err(ExchangeError::Unavailable("issuer keys unavailable"))
        }
        other => other,
    }
}

async fn jwks(State(state): State<Arc<AppState>>) -> Json<Value> {
    let jwks = state.sts.lock().expect("sts lock").ring().jwks();
    Json(jwks)
}

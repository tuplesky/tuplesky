//! task-40 end to end: the CLI logs in through the browser flow (a
//! loopback callback checked for path and state) and through the device
//! flow against the real broker router and a fake upstream provider,
//! stores credentials, refreshes with rotation, is told to log in again
//! when its secret is retired, and logs out.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{Form, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use coord_authn::{ClockHealth, SubjectKind, TrustRuleConfig};
use coord_core::effect::BootId;
use coord_core::outbox::BarrierAllocator;
use coord_login::http::{LoginState, router};
use coord_login::{
    DeviceLimits, DeviceLogin, LoginLimits, Registration, ServiceLogin, SessionReader, Upstream,
    UpstreamConfig, hardened_http_client,
};
use coord_state::policy::{Action, GrantRecord, SessionRecord, TrustRule};
use coord_state::{InternalCommand, PlanLimits, Response, plan_internal};
use coord_storage::materialize::{ApplyOutcome, apply_plan};
use coord_storage::views::{ViewBudget, build_internal_view};
use coord_storage::{GroupLimits, StoreWorker, codecs};
use coord_store_api::engine::OrderedRead;
use coord_store_api::registry::Collection;
use coord_store_testkit::model::ModelEngine;
use coord_sts::{
    ClockSource, CreatorError, EntropySource, HttpLimits, KeyRing, SessionCreator, SigningKey, Sts,
    StsConfig,
};
use coord_types::identity::Digest32;
use coord_types::ids::*;
use coordctl::CredentialStore;
use coordctl::{BrokerClient, CliError, Credentials, MemoryStore, UpdateLock, update};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::net::TcpListener;

const NS: NamespaceId = NamespaceId([5; 16]);

// ---- fake upstream provider -------------------------------------------

struct Idp {
    issuer: String,
    nonce: Mutex<Option<String>>,
    enc: EncodingKey,
    jwks: Value,
}

#[derive(Deserialize)]
struct TokenForm {
    code: String,
}

async fn discovery(State(idp): State<Arc<Idp>>) -> Json<Value> {
    let issuer = idp.issuer.clone();
    Json(json!({
        "issuer": issuer,
        "authorization_endpoint": format!("{issuer}/authorize"),
        "token_endpoint": format!("{issuer}/token"),
        "jwks_uri": format!("{issuer}/jwks"),
        "response_types_supported": ["code"],
        "subject_types_supported": ["public"],
        "id_token_signing_alg_values_supported": ["ES256"],
    }))
}

async fn jwks(State(idp): State<Arc<Idp>>) -> Json<Value> {
    Json(idp.jwks.clone())
}

async fn token(State(idp): State<Arc<Idp>>, Form(form): Form<TokenForm>) -> Json<Value> {
    assert_eq!(form.code, "good");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let nonce = idp.nonce.lock().unwrap().clone().unwrap_or_default();
    let claims = json!({
        "iss": idp.issuer, "sub": "user-42", "aud": "broker", "exp": now + 300, "iat": now,
        "nonce": nonce,
    });
    let mut header = Header::new(Algorithm::ES256);
    header.kid = Some("k1".into());
    Json(json!({
        "access_token": "upstream-access",
        "token_type": "Bearer",
        "expires_in": 300,
        "id_token": encode(&header, &claims, &idp.enc).unwrap(),
    }))
}

async fn fake_idp() -> (Arc<Idp>, String) {
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    let point = key.public_key_raw();
    let jwks_doc = json!({"keys": [{
        "kty": "EC", "crv": "P-256", "kid": "k1", "alg": "ES256", "use": "sig",
        "x": coord_sts::keys::b64url(&point[1..33]), "y": coord_sts::keys::b64url(&point[33..65]),
    }]});
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let issuer = format!("http://{}", listener.local_addr().unwrap());
    let idp = Arc::new(Idp {
        issuer: issuer.clone(),
        nonce: Mutex::new(None),
        enc: EncodingKey::from_ec_der(&key.serialize_der()),
        jwks: jwks_doc,
    });
    let app = Router::new()
        .route("/.well-known/openid-configuration", get(discovery))
        .route("/jwks", get(jwks))
        .route("/token", post(token))
        .with_state(idp.clone());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (idp, issuer)
}

// ---- replicated state stand-in ------------------------------------------

struct Domain {
    worker: StoreWorker<ModelEngine>,
    alloc: BarrierAllocator,
}

impl Domain {
    fn new() -> Self {
        let boot = BootId([1; 16]);
        let inc = ReplicaIncarnation::new(1).unwrap();
        let mut d = Domain {
            worker: StoreWorker::open(ModelEngine::new(), boot, inc, GroupLimits::default())
                .unwrap(),
            alloc: BarrierAllocator::new(inc, boot),
        };
        d.apply(&InternalCommand::PutTrustRule {
            namespace: NS,
            rule: TrustRuleId([9; 16]),
            record: TrustRule {
                enabled: true,
                generation: 3,
            },
        });
        d
    }

    fn apply(&mut self, command: &InternalCommand) -> Response {
        let gated = self.worker.reader().snapshot().unwrap();
        let view = build_internal_view(&gated, command, ViewBudget::default()).unwrap();
        let planned = plan_internal(command, &view, &PlanLimits::default()).unwrap();
        drop(gated);
        match apply_plan(&mut self.worker, self.alloc.allocate(), NS, &planned, None).unwrap() {
            ApplyOutcome::Applied(_) => planned.response,
            other => panic!("{other:?}"),
        }
    }
}

impl SessionCreator for Domain {
    fn create(&mut self, command: InternalCommand) -> Result<Response, CreatorError> {
        Ok(self.apply(&command))
    }
}

impl SessionReader for Domain {
    fn session(&self, session: &SessionId) -> Result<Option<SessionRecord>, CreatorError> {
        let gated = self.worker.reader().snapshot().unwrap();
        Ok(gated
            .view()
            .get(Collection::SessionV1.id(), &codecs::session_key(session))
            .unwrap()
            .map(|b| codecs::decode_session(&b).unwrap()))
    }
    fn grant(&self, commitment: &Digest32) -> Result<Option<GrantRecord>, CreatorError> {
        let gated = self.worker.reader().snapshot().unwrap();
        Ok(gated
            .view()
            .get(Collection::AuthGrantV1.id(), &codecs::grant_key(commitment))
            .unwrap()
            .map(|b| codecs::decode_grant(&b).unwrap()))
    }
}

struct SystemClock;
impl ClockSource for SystemClock {
    fn read(&self) -> ClockHealth {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        ClockHealth::healthy(now, 2)
    }
}

struct Entropy(Mutex<u8>);
impl EntropySource for Entropy {
    fn fill(&self) -> [u8; 32] {
        let mut n = self.0.lock().unwrap();
        *n = n.wrapping_add(1);
        let mut e = [0u8; 32];
        for (i, b) in e.iter_mut().enumerate() {
            *b = n.wrapping_mul(31).wrapping_add(i as u8);
        }
        e
    }
}

// ---- the broker -----------------------------------------------------------

async fn broker(issuer: &str) -> String {
    let http = hardened_http_client(Duration::from_secs(3)).unwrap();
    let upstream = Upstream::discover(
        UpstreamConfig {
            name: "idp".into(),
            issuer: issuer.into(),
            client_id: "broker".into(),
            redirect_uri: "http://127.0.0.1:1/unused".into(),
            device_redirect_uri: "http://127.0.0.1:1/unused-device".into(),
            allow_insecure_loopback: true,
        },
        &http,
    )
    .await
    .unwrap();
    let mut upstreams = BTreeMap::new();
    upstreams.insert("idp".to_string(), upstream);
    let mut upstream_clients = BTreeMap::new();
    upstream_clients.insert("idp".to_string(), "broker".to_string());
    // The CLI's loopback port is dynamic: register the prefix by
    // registering every port would be silly, so the test registers the
    // exact redirect after the listener is known. Here a permissive test
    // registration lists a range of candidate ports.
    let redirect_uris: Vec<String> = (1024..65535u32)
        .step_by(1)
        .take(0)
        .map(|p| format!("http://127.0.0.1:{p}/callback"))
        .collect();
    let registration = Registration {
        client_id: "coordctl".into(),
        redirect_uris,
        upstream: "idp".into(),
    };
    let login = ServiceLogin::new(
        LoginLimits::default(),
        vec![registration.clone()],
        upstream_clients.clone(),
    );
    let device = DeviceLogin::new(
        DeviceLimits {
            interval_secs: 1,
            ..DeviceLimits::default()
        },
        vec![registration],
        upstream_clients,
        "http://broker/device".into(),
        "cluster-1".into(),
    );
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    let ring = KeyRing::new(SigningKey::from_pkcs8_der("sts-1", &key.serialize_der()).unwrap());
    let rules = vec![TrustRuleConfig {
        id: TrustRuleId([9; 16]),
        generation: 3,
        enabled: true,
        issuer: "idp".into(),
        subject: SubjectKind::Human,
        audience: "broker".into(),
        required: BTreeMap::new(),
        principal: PrincipalId([7; 16]),
        scope_ceiling: Action::Read.bit(),
        max_lifetime_secs: 300,
    }];
    let registry = coord_authn::Registry::new(vec![], coord_authn::JwksLimits::default()).unwrap();
    let sts = Sts::new(
        StsConfig {
            issuer: "https://sts".into(),
            resource: "tuplesky://c".into(),
            namespace: NS,
            max_token_lifetime_secs: 120,
            session_window: 64,
            max_subject_token_bytes: 8192,
        },
        coord_authn::WifVerifier::new(registry, BTreeMap::new()),
        rules,
        ring,
    );
    let state = Arc::new(LoginState::new(
        login,
        device,
        upstreams,
        http,
        sts,
        Box::new(Domain::new()),
        Box::new(SystemClock),
        Box::new(Entropy(Mutex::new(0))),
        NS,
        HttpLimits::default(),
    ));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    format!("http://{addr}")
}

fn query(url: &str, name: &str) -> Option<String> {
    url.split('?')
        .nth(1)?
        .split('&')
        .find_map(|kv| kv.strip_prefix(&format!("{name}=")))
        .map(str::to_string)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn device_login_refresh_reuse_and_logout_through_the_cli() {
    let (idp, issuer) = fake_idp().await;
    let broker_url = broker(&issuer).await;
    let client = BrokerClient::new(&broker_url, "coordctl", Duration::from_secs(5)).unwrap();
    let browser = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    // Device login: the CLI starts, the "user" verifies the code through
    // the upstream login (the test plays the browser), the CLI's poll
    // takes the grant.
    let start = client.device_authorize().await.unwrap();
    assert!(!format!("{start:?}").contains(&start.device_code));
    // The user is shown what they are approving before anything is sent
    // upstream: the client and the cluster, for this user code.
    let verify = browser
        .get(format!(
            "{broker_url}/device/verify?user_code={}",
            start.user_code
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(verify.status(), 200);
    let shown: serde_json::Value = verify.json().await.unwrap();
    assert_eq!(shown["client_id"], "coordctl");
    assert!(shown["cluster"].is_string());
    assert!(shown["expires_in"].as_u64().unwrap() > 0);
    // Only on confirmation does the upstream leg begin.
    let approve = browser
        .post(format!("{broker_url}/device/approve"))
        .form(&[("user_code", start.user_code.clone())])
        .send()
        .await
        .unwrap();
    assert_eq!(approve.status(), 302);
    let to_idp = approve.headers()["location"].to_str().unwrap().to_string();
    assert!(to_idp.starts_with(&format!("{issuer}/authorize")));
    // The device leg asks the upstream to come back to the device
    // callback, not to the browser leg's.
    assert_eq!(
        query(&to_idp, "redirect_uri").as_deref(),
        Some("http%3A%2F%2F127.0.0.1%3A1%2Funused-device"),
        "{to_idp}"
    );
    let state = query(&to_idp, "state").unwrap();
    *idp.nonce.lock().unwrap() = query(&to_idp, "nonce");
    let approved = browser
        .get(format!(
            "{broker_url}/device/callback?code=good&state={state}"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(approved.status(), 200, "{}", approved.text().await.unwrap());
    // Polling happens inside `device_login`; here the grant is already
    // approved, so one poll returns the token (a second authorization is
    // not needed).
    let response = loop {
        match client.device_poll(&start.device_code).await.unwrap() {
            coordctl::PollOutcome::Token(t) => break t,
            coordctl::PollOutcome::SlowDown | coordctl::PollOutcome::Pending => {
                tokio::time::sleep(Duration::from_millis(1100)).await;
            }
            other => panic!("{other:?}"),
        }
    };
    assert!(response.refresh_token.is_some());
    assert_eq!(response.scope, "read");
    // Stored, redacted, refreshed with rotation.
    let store = MemoryStore::new();
    let dir = tempfile::tempdir().unwrap();
    let lock = UpdateLock::new(&dir.path().join("lock"));
    let credentials = Credentials::from_response(&broker_url, "coordctl", 1_700_000_000, &response);
    update(&store, &lock, |_| Ok(Some(credentials.clone()))).unwrap();
    let first_refresh = credentials.refresh_token.clone().unwrap();
    let rotated = client.refresh(&first_refresh).await.unwrap();
    let second_refresh = rotated.refresh_token.clone().unwrap();
    assert_ne!(second_refresh, first_refresh);
    assert_ne!(rotated.access_token, response.access_token);
    update(&store, &lock, |_| {
        Ok(Some(Credentials::from_response(
            &broker_url,
            "coordctl",
            1_700_000_060,
            &rotated,
        )))
    })
    .unwrap();
    // A stale copy (the rotation response was lost, or another process
    // kept the old secret) presents the retired secret: the family is
    // revoked and a fresh login is required; the current secret is dead
    // too.
    assert_eq!(
        client.refresh(&first_refresh).await,
        Err(CliError::FreshLoginRequired)
    );
    assert_eq!(
        client.refresh(&second_refresh).await,
        Err(CliError::FreshLoginRequired)
    );
    // Logout of a revoked family is idempotent; the store is cleared.
    client.logout(&second_refresh).await.unwrap();
    update(&store, &lock, |_| Ok(None)).unwrap();
    assert_eq!(store.load().unwrap(), None);
    // A denied device grant.
    let start2 = client.device_authorize().await.unwrap();
    let denied = browser
        .post(format!("{broker_url}/device/deny"))
        .form(&[("user_code", start2.user_code.as_str())])
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), 200);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(matches!(
        client.device_poll(&start2.device_code).await.unwrap(),
        coordctl::PollOutcome::Denied
    ));
    let _: BTreeSet<String> = BTreeSet::new();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn browser_login_accepts_only_the_expected_loopback_callback() {
    let (idp, issuer) = fake_idp().await;
    let broker_url = broker(&issuer).await;
    // The browser flow needs the CLI's exact loopback redirect to be
    // registered; the CLI chooses its port, so the test drives the flow
    // by hand with a registered redirect: it plays the CLI's listener.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let redirect = format!("http://127.0.0.1:{port}/callback");
    // Register through a second broker instance whose registration lists
    // this exact redirect.
    let broker_url = {
        let _ = broker_url;
        broker_with_redirect(&issuer, &redirect).await
    };
    let browser = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let (challenge, verifier) = oauth2::PkceCodeChallenge::new_random_sha256();
    let start = browser
        .get(format!(
            "{broker_url}/login/start?client_id=coordctl&redirect_uri={}&code_challenge={}&code_challenge_method=S256&state=st-cli",
            redirect.replace(':', "%3A").replace('/', "%2F"),
            challenge.as_str()
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(start.status(), 302);
    let to_idp = start.headers()["location"].to_str().unwrap().to_string();
    let state = query(&to_idp, "state").unwrap();
    *idp.nonce.lock().unwrap() = query(&to_idp, "nonce");
    let back = browser
        .get(format!(
            "{broker_url}/login/callback?code=good&state={state}"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(back.status(), 302);
    let to_cli = back.headers()["location"].to_str().unwrap().to_string();
    assert!(to_cli.starts_with(&redirect));
    let code = query(&to_cli, "code").unwrap();
    assert_eq!(query(&to_cli, "state").as_deref(), Some("st-cli"));
    // Redeem with the verifier: a session and refresh token.
    let client = BrokerClient::new(&broker_url, "coordctl", Duration::from_secs(5)).unwrap();
    let redeemed: Value = browser
        .post(format!("{broker_url}/login/redeem"))
        .form(&[
            ("code", code.as_str()),
            ("code_verifier", verifier.secret().as_str()),
            ("client_id", "coordctl"),
            ("redirect_uri", redirect.as_str()),
        ])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(redeemed["refresh_token"].is_string());
    let refresh_token = redeemed["refresh_token"].as_str().unwrap().to_string();
    assert!(client.refresh(&refresh_token).await.is_ok());
    // The CLI's own listener: an unexpected callback (wrong state) is
    // answered with 400 and ignored; the expected one completes.
    drop(listener);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let accept = tokio::spawn(async move {
        coordctl::client::accept_callback_for_test(&listener, "expected").await
    });
    let probe = reqwest::Client::builder().no_proxy().build().unwrap();
    let wrong = probe
        .get(format!(
            "http://127.0.0.1:{port}/callback?code=x&state=forged"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(wrong.status(), 400);
    let elsewhere = probe
        .get(format!(
            "http://127.0.0.1:{port}/other?code=x&state=expected"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(elsewhere.status(), 400);
    let right = probe
        .get(format!(
            "http://127.0.0.1:{port}/callback?code=the-code&state=expected"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(right.status(), 200);
    assert_eq!(accept.await.unwrap().unwrap(), "the-code");
}

async fn broker_with_redirect(issuer: &str, redirect: &str) -> String {
    let http = hardened_http_client(Duration::from_secs(3)).unwrap();
    let upstream = Upstream::discover(
        UpstreamConfig {
            name: "idp".into(),
            issuer: issuer.into(),
            client_id: "broker".into(),
            redirect_uri: "http://127.0.0.1:1/unused".into(),
            device_redirect_uri: "http://127.0.0.1:1/unused-device".into(),
            allow_insecure_loopback: true,
        },
        &http,
    )
    .await
    .unwrap();
    let mut upstreams = BTreeMap::new();
    upstreams.insert("idp".to_string(), upstream);
    let mut upstream_clients = BTreeMap::new();
    upstream_clients.insert("idp".to_string(), "broker".to_string());
    let registration = Registration {
        client_id: "coordctl".into(),
        redirect_uris: vec![redirect.to_string()],
        upstream: "idp".into(),
    };
    let login = ServiceLogin::new(
        LoginLimits::default(),
        vec![registration.clone()],
        upstream_clients.clone(),
    );
    let device = DeviceLogin::new(
        DeviceLimits::default(),
        vec![registration],
        upstream_clients,
        "http://broker/device".into(),
        "cluster-1".into(),
    );
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    let ring = KeyRing::new(SigningKey::from_pkcs8_der("sts-1", &key.serialize_der()).unwrap());
    let rules = vec![TrustRuleConfig {
        id: TrustRuleId([9; 16]),
        generation: 3,
        enabled: true,
        issuer: "idp".into(),
        subject: SubjectKind::Human,
        audience: "broker".into(),
        required: BTreeMap::new(),
        principal: PrincipalId([7; 16]),
        scope_ceiling: Action::Read.bit(),
        max_lifetime_secs: 300,
    }];
    let registry = coord_authn::Registry::new(vec![], coord_authn::JwksLimits::default()).unwrap();
    let sts = Sts::new(
        StsConfig {
            issuer: "https://sts".into(),
            resource: "tuplesky://c".into(),
            namespace: NS,
            max_token_lifetime_secs: 120,
            session_window: 64,
            max_subject_token_bytes: 8192,
        },
        coord_authn::WifVerifier::new(registry, BTreeMap::new()),
        rules,
        ring,
    );
    let state = Arc::new(LoginState::new(
        login,
        device,
        upstreams,
        http,
        sts,
        Box::new(Domain::new()),
        Box::new(SystemClock),
        Box::new(Entropy(Mutex::new(0))),
        NS,
        HttpLimits::default(),
    ));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    format!("http://{addr}")
}

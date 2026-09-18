//! task-38 acceptance for the upstream leg: the real openidconnect client
//! against a fake provider: discovery pinned to the issuer, an
//! authorization request carrying the broker's state, nonce and PKCE, a
//! code exchange whose ID token is verified for signature, issuer,
//! audience, nonce and algorithm, and the application's authorized-party
//! rule on top.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{Form, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use coord_login::{LoginError, Upstream, UpstreamConfig, UpstreamError, hardened_http_client};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::net::TcpListener;

struct Idp {
    issuer: Mutex<String>,
    nonce: Mutex<Option<String>>,
    enc: EncodingKey,
    jwks: Value,
    /// Tokens issued.
    issued: Mutex<u64>,
    /// The verifier the token endpoint received.
    verifier: Mutex<Option<String>>,
}

#[derive(Deserialize)]
struct TokenForm {
    grant_type: String,
    code: String,
    code_verifier: Option<String>,
}

async fn discovery(State(idp): State<Arc<Idp>>) -> Json<Value> {
    let issuer = idp.issuer.lock().unwrap().clone();
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
    assert_eq!(form.grant_type, "authorization_code");
    *idp.verifier.lock().unwrap() = form.code_verifier;
    let issuer = idp.issuer.lock().unwrap().clone();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let nonce = idp.nonce.lock().unwrap().clone().unwrap_or_default();
    let mut claims = json!({
        "iss": issuer, "sub": "user-42", "aud": "broker", "exp": now + 300, "iat": now,
        "nonce": nonce, "email": "someone@example",
    });
    let mut alg = Algorithm::ES256;
    match form.code.as_str() {
        "good" => {}
        "multi-noazp" => claims["aud"] = json!(["broker", "other"]),
        "multi-azp" => {
            claims["aud"] = json!(["broker", "other"]);
            claims["azp"] = json!("broker");
        }
        "azp-wrong" => claims["azp"] = json!("other"),
        "aud-wrong" => claims["aud"] = json!("other"),
        "nonce-wrong" => claims["nonce"] = json!("not-the-nonce"),
        "iss-wrong" => claims["iss"] = json!("https://other.example"),
        "alg-hs256" => alg = Algorithm::HS256,
        other => panic!("code {other}"),
    }
    let mut header = Header::new(alg);
    header.kid = Some("k1".into());
    let id_token = if alg == Algorithm::HS256 {
        encode(&header, &claims, &EncodingKey::from_secret(b"secret")).unwrap()
    } else {
        encode(&header, &claims, &idp.enc).unwrap()
    };
    *idp.issued.lock().unwrap() += 1;
    Json(json!({
        "access_token": "opaque-upstream-access-token",
        "token_type": "Bearer",
        "expires_in": 300,
        "id_token": id_token,
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
    let addr = listener.local_addr().unwrap();
    let issuer = format!("http://{addr}");
    let idp = Arc::new(Idp {
        issuer: Mutex::new(issuer.clone()),
        nonce: Mutex::new(None),
        enc: EncodingKey::from_ec_der(&key.serialize_der()),
        jwks: jwks_doc,
        issued: Mutex::new(0),
        verifier: Mutex::new(None),
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

fn config(issuer: &str) -> UpstreamConfig {
    UpstreamConfig {
        name: "idp".into(),
        issuer: issuer.into(),
        client_id: "broker".into(),
        redirect_uri: "https://broker.example/login/callback".into(),
        allow_insecure_loopback: true,
    }
}

fn query(url: &str, name: &str) -> Option<String> {
    url.split('?')
        .nth(1)?
        .split('&')
        .find_map(|kv| kv.strip_prefix(&format!("{name}=")))
        .map(str::to_string)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_upstream_leg_verifies_tokens_and_applies_the_azp_policy() {
    let (idp, issuer) = fake_idp().await;
    let http = hardened_http_client(Duration::from_secs(3)).unwrap();
    let upstream = Upstream::discover(config(&issuer), &http).await.unwrap();
    // The authorization request carries the broker's state, nonce and
    // PKCE challenge and the exact redirect.
    let (challenge, verifier) = oauth2::PkceCodeChallenge::new_random_sha256();
    let url = upstream.authorize_url("st-1", "nn-1", challenge.clone());
    assert!(url.starts_with(&format!("{issuer}/authorize?")));
    assert_eq!(query(&url, "state").as_deref(), Some("st-1"));
    assert_eq!(query(&url, "nonce").as_deref(), Some("nn-1"));
    assert_eq!(
        query(&url, "code_challenge").as_deref(),
        Some(challenge.as_str())
    );
    assert_eq!(
        query(&url, "code_challenge_method").as_deref(),
        Some("S256")
    );
    assert_eq!(query(&url, "client_id").as_deref(), Some("broker"));
    assert_eq!(
        query(&url, "redirect_uri").as_deref(),
        Some("https%3A%2F%2Fbroker.example%2Flogin%2Fcallback")
    );
    assert!(query(&url, "scope").unwrap().contains("openid"));
    *idp.nonce.lock().unwrap() = Some("nn-1".into());
    // The exchange presents the broker's verifier and verifies the token.
    let identity = upstream
        .exchange("good".into(), verifier.secret().clone(), "nn-1", &http)
        .await
        .unwrap();
    assert_eq!(identity.issuer, issuer);
    assert_eq!(identity.subject, "user-42");
    assert_eq!(identity.audiences, vec!["broker"]);
    assert_eq!(
        idp.verifier.lock().unwrap().as_deref(),
        Some(verifier.secret().as_str())
    );
    assert!(
        !format!("{identity:?}").contains("someone@example"),
        "email is not identity"
    );
    // Authorized-party policy and the crate's own checks.
    let exchange = |code: &str, nonce: &str| {
        let http = http.clone();
        let upstream = &upstream;
        let code = code.to_string();
        let nonce = nonce.to_string();
        let v = verifier.secret().clone();
        async move { upstream.exchange(code, v, &nonce, &http).await }
    };
    // The library rejects several audiences without azp and a foreign
    // azp itself; the application's policy (`azp_policy`, exercised on
    // the service leg) stands behind it either way.
    assert!(matches!(
        exchange("multi-noazp", "nn-1").await,
        Err(UpstreamError::IdToken | UpstreamError::Policy(LoginError::AzpMismatch))
    ));
    assert!(exchange("multi-azp", "nn-1").await.is_ok());
    assert!(matches!(
        exchange("azp-wrong", "nn-1").await,
        Err(UpstreamError::IdToken | UpstreamError::Policy(LoginError::AzpMismatch))
    ));
    assert_eq!(
        exchange("aud-wrong", "nn-1").await,
        Err(UpstreamError::IdToken)
    );
    assert_eq!(
        exchange("nonce-wrong", "nn-1").await,
        Err(UpstreamError::IdToken)
    );
    assert_eq!(exchange("good", "nn-2").await, Err(UpstreamError::IdToken));
    assert_eq!(
        exchange("iss-wrong", "nn-1").await,
        Err(UpstreamError::IdToken)
    );
    assert_eq!(
        exchange("alg-hs256", "nn-1").await,
        Err(UpstreamError::IdToken)
    );
    assert!(*idp.issued.lock().unwrap() >= 8);
    // Discovery that names another issuer is a mix-up and is refused;
    // a non-HTTPS issuer outside loopback is refused before any request.
    *idp.issuer.lock().unwrap() = "https://other.example".into();
    assert_eq!(
        Upstream::discover(config(&issuer), &http).await.err(),
        Some(UpstreamError::Discovery)
    );
    let mut insecure = config("http://idp.example");
    insecure.allow_insecure_loopback = false;
    assert!(matches!(
        Upstream::discover(insecure, &http).await.err(),
        Some(UpstreamError::Config(_))
    ));
    let _: BTreeMap<String, String> = BTreeMap::new();
}

//! task-36 acceptance over HTTP: the bounded Axum exchange issues tokens,
//! publishes public keys, maps errors to RFC 6749 codes, bounds body and
//! concurrency, and fetches issuer keys once for many exchanges rather
//! than per operation.

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::*;
use coord_authn::{ClockHealth, FetchConfig, HardenedFetcher, KubernetesMode};
use coord_state::{InternalCommand, Response};
use coord_sts::{
    AppState, ClockSource, EntropySource, GRANT_TYPE, HttpLimits, TOKEN_TYPE_JWT, router,
    verify_service_token,
};
use coord_sts::{CreatorError, SessionCreator};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const NOW: u64 = 1_700_000_000;

struct FixedClock;
impl ClockSource for FixedClock {
    fn read(&self) -> ClockHealth {
        ClockHealth::healthy(NOW, 5)
    }
}

struct Counter(AtomicUsize);
impl EntropySource for Counter {
    fn fill(&self) -> [u8; 32] {
        let n = self.0.fetch_add(1, Ordering::SeqCst) as u8 + 1;
        [n; 32]
    }
}

/// A creator that takes a while, to hold exchanges in flight.
struct Slow {
    inner: Domain,
    delay: Duration,
}
impl SessionCreator for Slow {
    fn create(&mut self, command: InternalCommand) -> Result<Response, CreatorError> {
        std::thread::sleep(self.delay);
        self.inner.create(command)
    }
}

/// Serve the issuer's JWKS, counting requests.
async fn jwks_server(jwks: Vec<u8>) -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = hits.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            counter.fetch_add(1, Ordering::SeqCst);
            let mut buf = vec![0u8; 4096];
            let _ = socket.read(&mut buf).await;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                jwks.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
            let _ = socket.write_all(&jwks).await;
            let _ = socket.shutdown().await;
        }
    });
    (format!("http://{addr}/keys"), hits)
}

async fn serve(state: Arc<AppState>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    format!("http://{addr}")
}

fn form(token: &str) -> Vec<(&'static str, String)> {
    vec![
        ("grant_type", GRANT_TYPE.into()),
        ("subject_token", token.into()),
        ("subject_token_type", TOKEN_TYPE_JWT.into()),
        ("resource", RESOURCE.into()),
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_bounded_exchange_issues_tokens_and_fetches_keys_once() {
    let k8s = k8s_issuer();
    let (jwks_url, hits) = jwks_server(k8s.jwks.clone()).await;
    let sts = sts(&k8s, &jwks_url, KubernetesMode::Offline, 3600);
    let fetcher = HardenedFetcher::new(FetchConfig {
        allowed_urls: [jwks_url.clone()].into_iter().collect::<BTreeSet<_>>(),
        allow_insecure_loopback: true,
        ..FetchConfig::default()
    })
    .unwrap();
    let state = Arc::new(AppState::new(
        sts,
        Box::new(Domain::new()),
        Some(fetcher),
        Box::new(FixedClock),
        Box::new(Counter(AtomicUsize::new(0))),
        HttpLimits {
            max_body_bytes: 4096,
            max_in_flight: 8,
            timeout: Duration::from_secs(5),
        },
    ));
    let base = serve(state.clone()).await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let token = assertion(&k8s, NOW, NOW + 600);
    for i in 0..5 {
        let r = client
            .post(format!("{base}/token"))
            .form(&form(&token))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200, "exchange {i}");
        let body: Value = r.json().await.unwrap();
        assert_eq!(body["token_type"], "Bearer");
        assert_eq!(body["expires_in"], 300);
        let jwks: Value = client
            .get(format!("{base}/.well-known/jwks.json"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        verify_service_token(
            body["access_token"].as_str().unwrap(),
            &jwks,
            "https://sts.cluster-1",
            RESOURCE,
            &ClockHealth::healthy(NOW, 5),
        )
        .unwrap();
        assert!(
            serde_json::to_string(&jwks)
                .unwrap()
                .contains("\"kid\":\"sts-1\"")
        );
    }
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "one key fetch for five exchanges"
    );
    // RFC 6749 errors.
    let r = client
        .post(format!("{base}/token"))
        .form(&[
            ("grant_type", "password"),
            ("subject_token", "x"),
            ("subject_token_type", TOKEN_TYPE_JWT),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    let body: Value = r.json().await.unwrap();
    assert_eq!(body["error"], "invalid_request");
    let r = client
        .post(format!("{base}/token"))
        .form(&form("not.a.jwt"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    assert_eq!(r.json::<Value>().await.unwrap()["error"], "invalid_grant");
    // Body bound.
    let big = "x".repeat(5000);
    let r = client
        .post(format!("{base}/token"))
        .form(&form(&big))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 413);
    // A malformed form is a client error, not a crash.
    let r = client
        .post(format!("{base}/token"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body("grant_type=")
        .send()
        .await
        .unwrap();
    assert!(r.status().is_client_error());
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exchanges_in_flight_are_bounded() {
    // The TLS stack's provider (the fetcher installs it in the other test).
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let k8s = k8s_issuer();
    let mut sts = sts(&k8s, "https://k8s/keys", KubernetesMode::Offline, 3600);
    sts.verifier_mut()
        .registry_mut()
        .install_keys("k8s", &k8s.jwks, NOW)
        .unwrap();
    let state = Arc::new(AppState::new(
        sts,
        Box::new(Slow {
            inner: Domain::new(),
            delay: Duration::from_millis(400),
        }),
        None,
        Box::new(FixedClock),
        Box::new(Counter(AtomicUsize::new(0))),
        HttpLimits {
            max_body_bytes: 4096,
            max_in_flight: 1,
            timeout: Duration::from_secs(5),
        },
    ));
    let base = serve(state).await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let token = assertion(&k8s, NOW, NOW + 600);
    let mut handles = Vec::new();
    for _ in 0..3 {
        let client = client.clone();
        let url = format!("{base}/token");
        let f = form(&token);
        handles.push(tokio::spawn(async move {
            client
                .post(url)
                .form(&f)
                .send()
                .await
                .unwrap()
                .status()
                .as_u16()
        }));
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let mut statuses: Vec<u16> = Vec::new();
    for h in handles {
        statuses.push(h.await.unwrap());
    }
    statuses.sort_unstable();
    assert_eq!(
        statuses,
        vec![200, 503, 503],
        "one in flight, the rest refused"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cold_cache_burst_fetches_the_issuer_keys_once() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    // `fetched` was local to one request, so every exchange of a burst on
    // a cold cache observed missing keys before any of them installed the
    // document and each called the endpoint itself. That is the
    // unknown-key storm the refresh budget exists to bound.
    let k8s = k8s_issuer();
    let (jwks_url, hits) = jwks_server(k8s.jwks.clone()).await;
    let sts = sts(&k8s, &jwks_url, KubernetesMode::Offline, 3600);
    let fetcher = HardenedFetcher::new(FetchConfig {
        allowed_urls: [jwks_url.clone()].into_iter().collect::<BTreeSet<_>>(),
        allow_insecure_loopback: true,
        ..FetchConfig::default()
    })
    .unwrap();
    let state = Arc::new(AppState::new(
        sts,
        Box::new(Domain::new()),
        Some(fetcher),
        Box::new(FixedClock),
        Box::new(Counter(AtomicUsize::new(0))),
        HttpLimits {
            max_body_bytes: 4096,
            max_in_flight: 8,
            timeout: Duration::from_secs(10),
        },
    ));
    let base = serve(state).await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let token = assertion(&k8s, NOW, NOW + 600);
    let mut handles = Vec::new();
    for _ in 0..6 {
        let client = client.clone();
        let url = format!("{base}/token");
        let f = form(&token);
        handles.push(tokio::spawn(async move {
            let r = client.post(url).form(&f).send().await.unwrap();
            let status = r.status().as_u16();
            let body: Value = r.json().await.unwrap();
            (status, body.to_string())
        }));
    }
    let mut results = Vec::new();
    for h in handles {
        results.push(h.await.unwrap());
    }
    let statuses: Vec<u16> = results.iter().map(|(status, _)| *status).collect();
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "one upstream fetch served the whole burst, not one per exchange"
    );
    assert!(
        statuses.contains(&200),
        "the burst produced tokens: {results:?}"
    );
    // The issuer's refresh budget still bounds floods, so an exchange
    // that raced ahead of the refresh claim may be refused within it;
    // what must not happen is a fetch per exchange.
    assert!(
        statuses.iter().all(|s| *s == 200 || *s == 400),
        "{results:?}"
    );
    // Once the keys are installed, every later exchange succeeds without
    // touching the issuer again.
    for _ in 0..3 {
        let r = client
            .post(format!("{base}/token"))
            .form(&form(&token))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
    }
    assert_eq!(hits.load(Ordering::SeqCst), 1, "still one fetch");
}

/// A reviewer that records every token it was asked about and authorizes
/// only the ones it was told to.
struct Reviewer {
    allowed: std::collections::BTreeSet<String>,
    result: coord_authn::TokenReview,
    seen: Arc<std::sync::Mutex<Vec<String>>>,
}

impl coord_sts::TokenReviewer for Reviewer {
    fn review<'a>(
        &'a self,
        token: &'a str,
    ) -> core::pin::Pin<
        Box<dyn core::future::Future<Output = Option<coord_authn::TokenReview>> + Send + 'a>,
    > {
        Box::pin(async move {
            self.seen.lock().unwrap().push(token.to_string());
            // A token the API server rejects is reviewed and denied, not
            // simply unreviewable.
            Some(if self.allowed.contains(token) {
                self.result.clone()
            } else {
                coord_authn::TokenReview::Reviewed {
                    authenticated: false,
                    username: None,
                    audiences: Vec::new(),
                }
            })
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_assertion_is_reviewed_on_its_own() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    // One application-wide result authorized any token of the same
    // service account, because a TokenReview carries no token identifier
    // and the verifier can only compare its username and audiences. A
    // token Kubernetes would reject — a deleted bound object, say — rode
    // in on another token's review.
    let k8s = k8s_issuer();
    let mut sts = sts(&k8s, "https://k8s/keys", KubernetesMode::TokenReview, 3600);
    sts.verifier_mut()
        .registry_mut()
        .install_keys("k8s", &k8s.jwks, NOW)
        .unwrap();
    let good = assertion(&k8s, NOW, NOW + 600);
    let revoked = assertion(&k8s, NOW, NOW + 601);
    assert_ne!(good, revoked, "two tokens of the same service account");
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let reviewer = Arc::new(Reviewer {
        allowed: [good.clone()].into_iter().collect(),
        result: coord_authn::TokenReview::Reviewed {
            authenticated: true,
            username: Some("system:serviceaccount:prod:kine".into()),
            audiences: vec![AUD.into()],
        },
        seen: seen.clone(),
    });
    let state = Arc::new(
        AppState::new(
            sts,
            Box::new(Domain::new()),
            None,
            Box::new(FixedClock),
            Box::new(Counter(AtomicUsize::new(0))),
            HttpLimits {
                max_body_bytes: 4096,
                max_in_flight: 8,
                timeout: Duration::from_secs(5),
            },
        )
        .with_reviewer(reviewer),
    );
    let base = serve(state).await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let ok = client
        .post(format!("{base}/token"))
        .form(&form(&good))
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), 200);
    let denied = client
        .post(format!("{base}/token"))
        .form(&form(&revoked))
        .send()
        .await
        .unwrap();
    assert_eq!(
        denied.status(),
        400,
        "the second token has no review of its own"
    );
    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen, vec![good, revoked], "each assertion was reviewed");
}

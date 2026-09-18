//! task-35 acceptance for the hardened fetcher: only configured endpoints
//! are requested, redirects are never followed, bodies and time are
//! bounded, and an issuer outage is a clean failure.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use coord_authn::{FetchConfig, FetchError, HardenedFetcher};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Serve canned HTTP/1.1 responses, one per connection; returns the base
/// URL and the number of requests received.
async fn server(responses: Vec<Vec<u8>>, hang: bool) -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = hits.clone();
    tokio::spawn(async move {
        let mut responses = responses.into_iter();
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            counter.fetch_add(1, Ordering::SeqCst);
            let mut buf = vec![0u8; 4096];
            let mut seen = Vec::new();
            loop {
                let n = match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => n,
                };
                seen.extend_from_slice(&buf[..n]);
                if seen.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            if hang {
                tokio::time::sleep(Duration::from_secs(30)).await;
                continue;
            }
            if let Some(r) = responses.next() {
                let _ = socket.write_all(&r).await;
                let _ = socket.shutdown().await;
            }
        }
    });
    (format!("http://{addr}"), hits)
}

fn response(status: &str, headers: &str, body: &[u8]) -> Vec<u8> {
    let mut out = format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\n{headers}Connection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    out.extend_from_slice(body);
    out
}

fn fetcher(urls: &[&str], max_body: usize, timeout: Duration) -> HardenedFetcher {
    HardenedFetcher::new(FetchConfig {
        allowed_urls: urls.iter().map(|u| u.to_string()).collect::<BTreeSet<_>>(),
        timeout,
        connect_timeout: Duration::from_millis(500),
        max_body_bytes: max_body,
        allow_insecure_loopback: true,
        proxy: None,
    })
    .unwrap()
}

#[tokio::test]
async fn only_configured_endpoints_are_fetched_and_redirects_are_never_followed() {
    let body = br#"{"keys":[]}"#;
    let (base, hits) = server(
        vec![
            response("200 OK", "Content-Type: application/json\r\n", body),
            response(
                "302 Found",
                &format!("Location: {}/elsewhere\r\n", "http://127.0.0.1:1"),
                b"",
            ),
            response("503 Service Unavailable", "", b"down"),
        ],
        false,
    )
    .await;
    let keys = format!("{base}/keys");
    let f = fetcher(&[&keys], 1024, Duration::from_secs(2));
    assert_eq!(f.fetch_jwks(&keys).await.unwrap(), body.to_vec());
    // A token-directed or otherwise unconfigured URL is never requested,
    // even on the same host.
    assert_eq!(
        f.fetch_jwks(&format!("{base}/other")).await,
        Err(FetchError::NotConfigured)
    );
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    // A redirect is an error, not a hop.
    assert_eq!(
        f.fetch_jwks(&keys).await,
        Err(FetchError::Redirected { status: 302 })
    );
    assert_eq!(
        hits.load(Ordering::SeqCst),
        2,
        "the redirect target was not contacted"
    );
    // An outage is a status error.
    assert_eq!(
        f.fetch_jwks(&keys).await,
        Err(FetchError::Status { status: 503 })
    );
    // Loopback HTTP needs the explicit allowance; anything else needs TLS.
    let strict = HardenedFetcher::new(FetchConfig {
        allowed_urls: [keys.clone(), "http://issuer.example/keys".to_string()]
            .into_iter()
            .collect(),
        allow_insecure_loopback: false,
        ..FetchConfig::default()
    })
    .unwrap();
    assert_eq!(
        strict.fetch_jwks(&keys).await,
        Err(FetchError::InsecureEndpoint)
    );
    assert_eq!(
        strict.fetch_jwks("http://issuer.example/keys").await,
        Err(FetchError::InsecureEndpoint)
    );
    assert_eq!(hits.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn bodies_and_time_are_bounded() {
    let big = vec![b'x'; 4096];
    let (base, _) = server(
        vec![
            response("200 OK", "", &big),
            // Unknown length, streamed past the bound.
            {
                let mut r = b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n".to_vec();
                r.extend_from_slice(&big);
                r
            },
        ],
        false,
    )
    .await;
    let keys = format!("{base}/keys");
    let f = fetcher(&[&keys], 1024, Duration::from_secs(2));
    assert_eq!(
        f.fetch_jwks(&keys).await,
        Err(FetchError::BodyTooLarge { limit: 1024 })
    );
    assert_eq!(
        f.fetch_jwks(&keys).await,
        Err(FetchError::BodyTooLarge { limit: 1024 })
    );
    let (base, _) = server(Vec::new(), true).await;
    let keys = format!("{base}/keys");
    let f = fetcher(&[&keys], 1024, Duration::from_millis(300));
    assert_eq!(f.fetch_jwks(&keys).await, Err(FetchError::Timeout));
    // Nothing listening: a transport failure, never a hang.
    let f = fetcher(
        &["http://127.0.0.1:1/keys"],
        1024,
        Duration::from_millis(500),
    );
    assert_eq!(
        f.fetch_jwks("http://127.0.0.1:1/keys").await,
        Err(FetchError::Transport)
    );
}

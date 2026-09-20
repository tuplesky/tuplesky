//! Cross-language acceptance of the native API authentication contract
//! (task-46): the Kine adapter's registered Go driver, unchanged, against
//! a real [`coord_transport::Transport`] and the real session binding.
//!
//! The Go driver presents no client certificate: on the API plane its
//! authority is the service token it binds with, and the peer plane's
//! mutual TLS is untouched (`coord-transport/tests/transport.rs` holds
//! that side). This test binds an endpoint, verifies `Bind` frames with
//! `verify_bind` against a real STS key ring, answers requests, and then
//! runs the Go test that drives Kine's driver registry against it. A
//! missing Go toolchain skips it; a Go failure fails it with the Go
//! output.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use coord_authn::ClockHealth;
use coord_session::{BindingConfig, verify_bind};
use coord_state::plan::{Outcome, Response};
use coord_sts::{KeyRing, ServiceClaims, SigningKey};
use coord_transport::{Limits, Transport, TransportEvent};
use coord_transport_testkit::{TestBinder, TestCa};
use coord_types::CommandId;
use coord_types::ids::{
    ClusterId, DomainId, KvRevision, NamespaceId, ReplicaId, ReplicaIncarnation, SessionId,
};
use coord_types::logical_v1::CanonicalOperation;
use coord_types::wire_v1::{MessageV1, OutcomeV1, PeerRole, ResponseV1, decode};

const CLUSTER: ClusterId = ClusterId([0x11; 16]);
const DOMAIN: DomainId = DomainId([0x22; 16]);
const NAMESPACE: NamespaceId = NamespaceId([0x33; 16]);
const SESSION: SessionId = SessionId([0x44; 16]);
const PRINCIPAL: [u8; 16] = [0x55; 16];
const ISSUER: &str = "https://sts.interop";
const RESOURCE: &str = "interop";
const SERVER_NAME: &str = "frontend.local";
/// Revision the stand-in domain answers with.
const REVISION: u64 = 7;

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_secs()
}

/// A service token for `session`, valid for an hour unless `issuer` is
/// another one (the invalid case: a token this frontend cannot verify).
fn service_token(ring: &KeyRing, issuer: &str, session: SessionId) -> String {
    let issued = now();
    ring.sign(&ServiceClaims {
        iss: issuer.into(),
        sub: hex(&PRINCIPAL),
        aud: RESOURCE.into(),
        sid: hex(&session.0),
        scope: 0xffff,
        rule: hex(&[1u8; 16]),
        generation: 1,
        jti: hex(&[2u8; 32]),
        iat: issued,
        exp: issued + 3600,
    })
    .expect("signing")
}

/// The answer the stand-in domain gives one canonical operation. Only
/// what the Kine backend's start-up and health read need is modelled;
/// the domain's own semantics are tested in `coord-state`.
fn answer(operation: &CanonicalOperation) -> Option<Outcome> {
    match operation {
        CanonicalOperation::KineCreate(_) => Some(Outcome::KineCreated),
        CanonicalOperation::Range(_) => Some(Outcome::Range {
            items: Vec::new(),
            count: 0,
            more: false,
        }),
        _ => None,
    }
}

/// The frontend of the test: session binding, then one canned answer per
/// request. It runs until the Go process is done.
async fn serve(mut transport: Transport, config: BindingConfig) {
    let mut bound = false;
    while let Some(event) = transport.next_event().await {
        let TransportEvent::ApiRequest {
            frame, responder, ..
        } = event
        else {
            continue;
        };
        let clock = ClockHealth::healthy(now(), 5);
        if frame.kind == coord_session::KIND_BIND {
            let reply = match coord_session::decode_bind(&frame)
                .ok()
                .and_then(|bind| verify_bind(&config, bind.token.as_slice(), &clock, None).ok())
            {
                Some(binding) => {
                    bound = true;
                    coord_session::bind_ack_frame(&coord_session::BindAckV1 {
                        session: binding.session,
                        expires_at: binding.expires_at,
                        scope: binding.scope_ceiling,
                        rule_generation: binding.rule_generation,
                    })
                    .expect("bounded")
                }
                None => MessageV1::Close(coord_types::wire_v1::CloseV1 {
                    code: 2,
                    reason: coord_types::wire_v1::BoundedBytes::new(b"bind refused".to_vec())
                        .expect("bounded"),
                })
                .encode()
                .expect("bounded"),
            };
            let _ = responder.respond(reply).await;
            continue;
        }
        // Nothing but a binding may precede work: an unbound connection's
        // request is refused, exactly as the bound frontend does.
        let Ok(MessageV1::Request(request)) = decode(&frame) else {
            continue;
        };
        let Ok(logical) = request.logical() else {
            continue;
        };
        let command = CommandId::derive(&request.retry_key, &logical).expect("canonical");
        let outcome = match (bound, answer(&logical.operation)) {
            (true, Some(outcome)) => outcome,
            _ => {
                let _ = responder
                    .respond(
                        MessageV1::Response(ResponseV1 {
                            command_id: command,
                            outcome: OutcomeV1::Unknown,
                        })
                        .encode()
                        .expect("bounded"),
                    )
                    .await;
                continue;
            }
        };
        let revision = KvRevision::new(REVISION).expect("positive");
        let result = postcard::to_allocvec(&Response { revision, outcome }).expect("encodable");
        let reply = MessageV1::Response(ResponseV1 {
            command_id: command,
            outcome: OutcomeV1::Ok {
                revision: Some(revision),
                result: coord_types::wire_v1::BoundedBytes::new(result).expect("bounded"),
            },
        })
        .encode()
        .expect("bounded");
        let _ = responder.respond(reply).await;
    }
}

/// The Go module of the Kine adapter.
fn go_module() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .join("adapters")
        .join("kine")
}

#[tokio::test(flavor = "multi_thread")]
async fn the_registered_go_driver_negotiates_binds_and_requests_against_the_native_endpoint() {
    if Command::new("go").arg("version").output().is_err() {
        eprintln!("skipped: no Go toolchain");
        return;
    }
    let ca = TestCa::new();
    let frontend = ca.issue(
        SERVER_NAME,
        ReplicaId([1; 16]),
        ReplicaIncarnation::new(1).expect("positive"),
        PeerRole::Frontend,
    );
    let mut binder = TestBinder::new(CLUSTER, DOMAIN);
    binder.register(&frontend);
    let limits = Limits {
        handshake_timeout: Duration::from_secs(10),
        frame_timeout: Duration::from_secs(10),
        idle_timeout: Duration::from_secs(60),
        ..Limits::default()
    };
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("loopback");
    let transport = Transport::bind(
        addr,
        frontend.local(&ca, CLUSTER, DOMAIN, vec![]),
        Arc::new(binder),
        limits,
    )
    .expect("endpoint");
    let endpoint = transport.local_addr().expect("bound address");

    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).expect("sts key");
    let ring = KeyRing::new(
        SigningKey::from_pkcs8_der("interop-1", &key.serialize_der()).expect("signing key"),
    );
    let good = service_token(&ring, ISSUER, SESSION);
    // Signed by the same key but issued by another STS: verification
    // refuses it, so the binding is refused and no work is admitted.
    let bad = service_token(&ring, "https://sts.elsewhere", SESSION);
    let config = BindingConfig {
        issuer: ISSUER.into(),
        resource: RESOURCE.into(),
        jwks: ring.jwks(),
        cluster: CLUSTER,
        domain: DOMAIN,
    };
    let frontend_task = tokio::spawn(serve(transport, config));

    let dir = tempdir();
    let ca_pem = dir.join("frontend-ca.pem");
    std::fs::write(&ca_pem, pem(ca.certificate_der().as_ref())).expect("write CA");

    let output = tokio::task::spawn_blocking(move || {
        Command::new("go")
            .args([
                "test",
                "-v",
                "-count=1",
                "-run",
                "TestTheRegisteredDriverBindsAndRequestsAgainstTheNativeEndpoint|TestAnInvalidServiceTokenIsRefusedByTheNativeEndpoint",
                "./driver/...",
            ])
            .current_dir(go_module())
            .env("COORD_INTEROP_ENDPOINT", endpoint.to_string())
            .env("COORD_INTEROP_CA", &ca_pem)
            .env("COORD_INTEROP_SERVER_NAME", SERVER_NAME)
            .env("COORD_INTEROP_CLUSTER", hex(&CLUSTER.0))
            .env("COORD_INTEROP_DOMAIN", hex(&DOMAIN.0))
            .env("COORD_INTEROP_NAMESPACE", hex(&NAMESPACE.0))
            .env("COORD_INTEROP_TOKEN", good)
            .env("COORD_INTEROP_BAD_TOKEN", bad)
            .output()
    })
    .await
    .expect("go test joined")
    .expect("go test ran");
    frontend_task.abort();
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(
        output.status.success(),
        "the Go driver could not complete the native handshake:\n{stdout}\n{stderr}",
    );
    // A skipped Go test would pass silently and prove nothing.
    assert!(
        !stdout.contains("SKIP") && !stdout.contains("no test files"),
        "the Go regression did not run:\n{stdout}\n{stderr}",
    );
    assert!(
        stdout.contains("PASS: TestTheRegisteredDriverBindsAndRequestsAgainstTheNativeEndpoint")
            && stdout.contains("PASS: TestAnInvalidServiceTokenIsRefusedByTheNativeEndpoint"),
        "the Go regression did not report both cases:\n{stdout}\n{stderr}",
    );
}

/// A private directory for this test's files.
fn tempdir() -> PathBuf {
    let base = std::env::temp_dir().join(format!("kine-interop-{}", std::process::id()));
    std::fs::create_dir_all(&base).expect("temp dir");
    base
}

/// DER to PEM (the Go side reads a certificate file).
fn pem(der: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut body = String::new();
    for chunk in der.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                body.push(ALPHABET[((n >> (18 - 6 * i)) & 0x3f) as usize] as char);
            } else {
                body.push('=');
            }
        }
    }
    let mut out = String::from("-----BEGIN CERTIFICATE-----\n");
    for line in body.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(line).expect("ascii"));
        out.push('\n');
    }
    out.push_str("-----END CERTIFICATE-----\n");
    out
}

//! End-to-end smoke test of the integrated stack: the Kubernetes storage
//! edge down to the real domain.
//!
//! `kine_interop` proves the handshake with a stand-in that answers canned
//! outcomes. This test replaces that stand-in with the real applying path
//! -- `coord_storage::Applier` over a store, the real KV planner, the real
//! retained results -- and drives it through the pinned Kine server bridge
//! with a real etcd client. Every seam between the tracks is on the path:
//! the wire codec, the session binding at the transport boundary, the
//! Kine-facing entry projection, the planner and the store.
//!
//! What it is not: a Kubernetes conformance run (task-48 owns that, with
//! a real API server) and not a replication test (one replica, no
//! consensus round). It is the smoke test that says the composed stack
//! answers real requests with real state.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use coord_authn::ClockHealth;
use coord_consensus::PayloadRecordV1;
use coord_core::outbox::BarrierAllocator;
use coord_session::{BindingConfig, verify_bind};
use coord_state::policy::{Action, KeyInterval, PolicyRule};
use coord_storage::policy::{bootstrap_session, rule_update};
use coord_storage::{Applier, GroupLimits, StoreWorker};
use coord_store_testkit::model::ModelEngine;
use coord_sts::{KeyRing, ServiceClaims, SigningKey};
use coord_transport::{Limits, Transport, TransportEvent};
use coord_transport_testkit::{TestBinder, TestCa};
use coord_types::CommandId;
use coord_types::ids::{
    ClusterId, DomainId, NamespaceId, PolicyRuleId, PrincipalId, ReplicaId, ReplicaIncarnation,
    SessionId,
};
use coord_types::wire_v1::{MessageV1, OutcomeV1, PeerRole, ResponseV1, decode};

const CLUSTER: ClusterId = ClusterId([0x11; 16]);
const DOMAIN: DomainId = DomainId([0x22; 16]);
const NAMESPACE: NamespaceId = NamespaceId([0x33; 16]);
const SESSION: SessionId = SessionId([0x44; 16]);
const PRINCIPAL: PrincipalId = PrincipalId([0x55; 16]);
const ISSUER: &str = "https://sts.e2e";
const RESOURCE: &str = "e2e";
const SERVER_NAME: &str = "frontend.local";

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_secs()
}

fn service_token(ring: &KeyRing, session: SessionId) -> String {
    let issued = now();
    ring.sign(&ServiceClaims {
        iss: ISSUER.into(),
        sub: hex(PRINCIPAL.0.as_slice()),
        aud: RESOURCE.into(),
        sid: hex(&session.0),
        scope: 0xffff,
        rule: hex(session.0.as_slice()),
        generation: 1,
        jti: hex(&[2u8; 32]),
        iat: issued,
        exp: issued + 3600,
    })
    .expect("signing")
}

/// The real domain: a store with this session admitted and permitted
/// every action in the namespace, behind the real applying path.
fn domain() -> Applier<StoreWorker<ModelEngine>> {
    let boot = coord_core::effect::BootId([1; 16]);
    let inc = ReplicaIncarnation::new(1).expect("positive");
    let mut worker =
        StoreWorker::open(ModelEngine::new(), boot, inc, GroupLimits::default()).expect("open");
    let mut alloc = BarrierAllocator::new(inc, boot);
    // The Kine backend spends one sequence per request and never rewinds,
    // so the window only has to outlast this test.
    let mut updates = bootstrap_session(&SESSION, PRINCIPAL, 1 << 16, true).expect("session");
    for (i, action) in Action::ALL.iter().enumerate() {
        updates.push(
            rule_update(
                &PolicyRuleId([i as u8 + 1; 16]),
                &PolicyRule {
                    principal: PRINCIPAL,
                    action: *action,
                    namespace: NAMESPACE,
                    interval: KeyInterval {
                        lower: Vec::new(),
                        upper: None,
                    },
                },
            )
            .expect("rule"),
        );
    }
    worker
        .submit(coord_core::effect::PersistBatch {
            barrier: alloc.allocate(),
            base: Some(worker.application_base()),
            updates,
        })
        .expect("bootstrap");
    worker.flush().expect("bootstrap durable");
    Applier::new(worker, alloc).expect("applier")
}

/// The frontend: the real session binding, then the real applying path
/// for every request. It runs until the Go process is done.
async fn serve(mut transport: Transport, config: BindingConfig) {
    let mut applier = domain();
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
        let Ok(MessageV1::Request(request)) = decode(&frame) else {
            continue;
        };
        let Ok(logical) = request.logical() else {
            continue;
        };
        let command = CommandId::derive(&request.retry_key, &logical).expect("canonical");
        // Nothing but a binding may precede work.
        let retained = if bound {
            let payload = PayloadRecordV1 {
                ack_through: 0,
                retry_key: request.retry_key,
                logical: request.logical.as_slice().to_vec(),
                admission: None,
            };
            applier
                .apply(command, &payload)
                .ok()
                .and_then(|_| {
                    let gated = applier.store().reader().snapshot().ok()?;
                    coord_storage::retry::lookup(gated.view(), &request.retry_key).ok()?
                })
                .map(|record| (record.revision, record.response))
        } else {
            None
        };
        let reply = match retained {
            Some((revision, response)) => MessageV1::Response(ResponseV1 {
                command_id: command,
                outcome: OutcomeV1::Ok {
                    revision,
                    result: coord_types::wire_v1::BoundedBytes::new(response).expect("bounded"),
                },
            }),
            None => MessageV1::Response(ResponseV1 {
                command_id: command,
                outcome: OutcomeV1::Unknown,
            }),
        };
        let _ = responder.respond(reply.encode().expect("bounded")).await;
    }
}

fn go_module() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .join("adapters")
        .join("kine")
}

/// The Kubernetes storage edge, end to end, against the real domain: an
/// etcd client through the pinned Kine bridge, the registered `coord://`
/// driver, QUIC and the session binding, into the real planner and store.
#[tokio::test(flavor = "multi_thread")]
async fn the_kubernetes_storage_edge_runs_against_the_real_domain() {
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
        idle_timeout: Duration::from_secs(120),
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
        SigningKey::from_pkcs8_der("e2e-1", &key.serialize_der()).expect("signing key"),
    );
    let token = service_token(&ring, SESSION);
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
                "TestTheKubernetesStorageEdgeRunsAgainstTheRealDomain",
                "./driver/...",
            ])
            .current_dir(go_module())
            .env("COORD_INTEROP_ENDPOINT", endpoint.to_string())
            .env("COORD_INTEROP_CA", &ca_pem)
            .env("COORD_INTEROP_SERVER_NAME", SERVER_NAME)
            .env("COORD_INTEROP_CLUSTER", hex(&CLUSTER.0))
            .env("COORD_INTEROP_DOMAIN", hex(&DOMAIN.0))
            .env("COORD_INTEROP_NAMESPACE", hex(&NAMESPACE.0))
            .env("COORD_INTEROP_TOKEN", token)
            .env("COORD_INTEROP_BAD_TOKEN", "not-a-token")
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
        "the storage edge did not run against the real domain:\n{stdout}\n{stderr}",
    );
    // A skipped Go test would pass silently and prove nothing.
    assert!(
        stdout.contains("PASS: TestTheKubernetesStorageEdgeRunsAgainstTheRealDomain"),
        "the end-to-end case did not run:\n{stdout}\n{stderr}",
    );
}

/// A private directory for this test's files.
fn tempdir() -> PathBuf {
    let base = std::env::temp_dir().join(format!("kine-e2e-{}", std::process::id()));
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

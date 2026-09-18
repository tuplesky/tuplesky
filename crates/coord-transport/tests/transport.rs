//! task-30 acceptance on real loopback endpoints: role negotiation and
//! peer frames with bound provenance; malformed frames and origin, role,
//! version and identity mismatches fail closed; no application 0-RTT; a
//! transport completion is never durability or establishment; bounded
//! shutdown and stream limits; sparse connections only where asked.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use coord_transport::{
    ALPN_API, ALPN_PEER, BoundIdentity, Class, CloseReason, Limits, Transport, TransportEvent,
    evidence_frame,
};
use coord_transport_testkit::{TestBinder, TestCa, TestIdentity};
use coord_types::ids::{ClusterId, DomainId, ReplicaId, ReplicaIncarnation};
use coord_types::wire_v1::{
    BoundedVec, CloseV1, HEADER_LEN, HelloV1, MessageV1, PeerRole, encode_frame,
};
use quinn::crypto::rustls::QuicClientConfig;
use tokio::time::timeout;

const CLUSTER: ClusterId = ClusterId([1; 16]);
const DOMAIN: DomainId = DomainId([2; 16]);

fn r(i: u8) -> ReplicaId {
    ReplicaId([i; 16])
}

fn inc(n: u64) -> ReplicaIncarnation {
    ReplicaIncarnation::new(n).unwrap()
}

fn limits() -> Limits {
    Limits {
        handshake_timeout: Duration::from_secs(3),
        frame_timeout: Duration::from_secs(3),
        idle_timeout: Duration::from_secs(5),
        keep_alive: Duration::from_millis(500),
        ..Limits::default()
    }
}

struct Fixture {
    ca: TestCa,
    binder: Arc<TestBinder>,
    ids: Vec<TestIdentity>,
}

fn fixture(roles: &[PeerRole]) -> Fixture {
    let ca = TestCa::new();
    let mut binder = TestBinder::new(CLUSTER, DOMAIN);
    let ids: Vec<TestIdentity> = roles
        .iter()
        .enumerate()
        .map(|(i, role)| ca.issue(&format!("node-{i}"), r(i as u8), inc(1), *role))
        .collect();
    for id in &ids {
        binder.register(id);
    }
    Fixture {
        ca,
        binder: Arc::new(binder),
        ids,
    }
}

fn bind(f: &Fixture, i: usize) -> Transport {
    let local = f.ids[i].local(&f.ca, CLUSTER, DOMAIN, vec![1, 2]);
    Transport::bind(
        "127.0.0.1:0".parse().unwrap(),
        local,
        f.binder.clone(),
        limits(),
    )
    .unwrap()
}

async fn event(t: &mut Transport) -> TransportEvent {
    timeout(Duration::from_secs(5), t.next_event())
        .await
        .expect("event within deadline")
        .expect("endpoint alive")
}

async fn connect_peer(from: &Transport, to: &Transport, to_id: &TestIdentity) -> SocketAddr {
    let addr = to.local_addr().unwrap();
    from.connect(
        addr,
        &to_id.name,
        PeerRole::Voter,
        Some(inc(1)),
        to_id.expected(),
    )
    .await
    .unwrap();
    addr
}

#[tokio::test(flavor = "multi_thread")]
async fn voters_negotiate_roles_and_exchange_frames_with_bound_provenance() {
    let f = fixture(&[PeerRole::Voter, PeerRole::Voter, PeerRole::Voter]);
    let mut a = bind(&f, 0);
    let mut b = bind(&f, 1);
    let c = bind(&f, 2);
    connect_peer(&a, &b, &f.ids[1]).await;
    match event(&mut a).await {
        TransportEvent::Connected {
            class, identity, ..
        } => {
            assert_eq!(class, Class::Peer);
            assert_eq!(identity.replica, Some(r(1)));
            assert_eq!(identity.incarnation, Some(inc(1)));
            assert_eq!(identity.role, PeerRole::Voter);
            assert_eq!(identity.capabilities, vec![1, 2], "granted intersection");
        }
        other => panic!("{other:?}"),
    }
    match event(&mut b).await {
        TransportEvent::Connected { identity, .. } => {
            assert_eq!(identity.replica, Some(r(0)));
        }
        other => panic!("{other:?}"),
    }
    // A frame each way over the one connection; provenance is what
    // negotiation bound, not what the frame claims.
    a.send_peer(r(1), inc(1), evidence_frame(b"vote-from-a").unwrap())
        .await
        .unwrap();
    match event(&mut b).await {
        TransportEvent::PeerFrame {
            provenance,
            kind,
            payload,
            ..
        } => {
            assert_eq!(provenance.from(), r(0));
            assert_eq!(provenance.incarnation(), inc(1));
            assert_eq!(kind, coord_transport::KIND_PEER_EVIDENCE);
            assert_eq!(payload, b"vote-from-a");
        }
        other => panic!("{other:?}"),
    }
    b.send_peer(r(0), inc(1), evidence_frame(b"ack-from-b").unwrap())
        .await
        .unwrap();
    match event(&mut a).await {
        TransportEvent::PeerFrame {
            provenance,
            payload,
            ..
        } => {
            assert_eq!(provenance.from(), r(1));
            assert_eq!(payload, b"ack-from-b");
        }
        other => panic!("{other:?}"),
    }
    // Sparse: nothing dialed the third voter, and it dialed nobody.
    assert_eq!(a.connections(), 1);
    assert_eq!(b.connections(), 1);
    assert_eq!(c.connections(), 0);
    assert!(matches!(
        a.send_peer(r(2), inc(1), evidence_frame(b"x").unwrap())
            .await,
        Err(coord_transport::SendError::NotConnected)
    ));
}

/// A raw client endpoint presenting `id`'s certificate, for driving the
/// acceptor with hand-built frames.
fn raw_client(f: &Fixture, id: &TestIdentity, alpn: &[u8]) -> quinn::Endpoint {
    raw_client_with(f, id, alpn, Arc::new(quinn::TransportConfig::default()))
}

fn raw_client_with(
    f: &Fixture,
    id: &TestIdentity,
    alpn: &[u8],
    transport: Arc<quinn::TransportConfig>,
) -> quinn::Endpoint {
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let mut client = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(f.ca.roots())
        .with_client_auth_cert(id.chain.clone(), id.key.clone_key())
        .unwrap();
    client.alpn_protocols = vec![alpn.to_vec()];
    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    let mut config =
        quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(client).unwrap()));
    config.transport_config(transport);
    endpoint.set_default_client_config(config);
    endpoint
}

fn hello(role: PeerRole, cluster: ClusterId, incarnation: Option<ReplicaIncarnation>) -> Vec<u8> {
    MessageV1::Hello(HelloV1 {
        role,
        cluster_id: cluster,
        domain_id: DOMAIN,
        incarnation,
        capabilities: BoundedVec::new(vec![1]).unwrap(),
    })
    .encode()
    .unwrap()
}

/// Send `first` as the first control frame and return how the acceptor
/// closed the connection.
async fn first_frame(
    f: &Fixture,
    id: &TestIdentity,
    alpn: &[u8],
    acceptor: &mut Transport,
    first: Vec<u8>,
) -> CloseReason {
    first_frame_ending(f, id, alpn, acceptor, first, false).await
}

async fn first_frame_ending(
    f: &Fixture,
    id: &TestIdentity,
    alpn: &[u8],
    acceptor: &mut Transport,
    first: Vec<u8>,
    finish: bool,
) -> CloseReason {
    let client = raw_client(f, id, alpn);
    let conn = client
        .connect(acceptor.local_addr().unwrap(), &f.ids[0].name)
        .unwrap()
        .await
        .unwrap();
    let (mut send, _recv) = conn.open_bi().await.unwrap();
    send.write_all(&first).await.unwrap();
    if finish {
        send.finish().unwrap();
    }
    loop {
        match event(acceptor).await {
            TransportEvent::Closed { reason, .. } => return reason,
            TransportEvent::Connected { .. } => panic!("must not connect"),
            _ => {}
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn malformed_frames_and_mismatches_fail_closed() {
    let f = fixture(&[PeerRole::Voter, PeerRole::Voter, PeerRole::Frontend]);
    let mut acceptor = bind(&f, 0);
    let voter = &f.ids[1];
    // A stream that ends inside the header is truncated; one that stalls
    // there hits the negotiation deadline. Neither dispatches anything.
    let reason = first_frame_ending(&f, voter, ALPN_PEER, &mut acceptor, vec![0xff; 3], true).await;
    assert!(
        matches!(reason, CloseReason::Malformed(ref m) if m.contains("Truncated")),
        "{reason:?}"
    );
    let reason = first_frame(&f, voter, ALPN_PEER, &mut acceptor, vec![0xff; 3]).await;
    assert_eq!(reason, CloseReason::Timeout);
    // A header whose length is below the minimum is malformed before any
    // payload is read.
    let mut short = Vec::new();
    short.extend_from_slice(&2u32.to_be_bytes());
    short.extend_from_slice(&0x0001u16.to_be_bytes());
    short.extend_from_slice(&1u16.to_be_bytes());
    let reason = first_frame(&f, voter, ALPN_PEER, &mut acceptor, short).await;
    assert!(
        matches!(reason, CloseReason::Malformed(ref m) if m.contains("LengthBelowMinimum")),
        "{reason:?}"
    );
    // A frame above the negotiation class limit is refused before any
    // payload is read.
    let mut oversize = Vec::new();
    oversize.extend_from_slice(&(64 * 1024 + 1u32).to_be_bytes());
    oversize.extend_from_slice(&0x0001u16.to_be_bytes());
    oversize.extend_from_slice(&1u16.to_be_bytes());
    let reason = first_frame(&f, voter, ALPN_PEER, &mut acceptor, oversize).await;
    assert!(
        matches!(reason, CloseReason::Malformed(ref m) if m.contains("LengthAboveClassLimit")),
        "{reason:?}"
    );
    // A well-formed frame of another kind first.
    let not_hello = encode_frame(0x0102, 1, &[0u8; 4]).unwrap();
    let reason = first_frame(&f, voter, ALPN_PEER, &mut acceptor, not_hello).await;
    assert!(matches!(reason, CloseReason::Malformed(_)), "{reason:?}");
    // Unsupported schema version of Hello.
    let mut wrong_version = hello(PeerRole::Voter, CLUSTER, Some(inc(1)));
    wrong_version[HEADER_LEN - 2..HEADER_LEN].copy_from_slice(&2u16.to_be_bytes());
    let reason = first_frame(&f, voter, ALPN_PEER, &mut acceptor, wrong_version).await;
    assert!(
        matches!(reason, CloseReason::Malformed(ref m) if m.contains("UnsupportedVersion")),
        "{reason:?}"
    );
    // Wrong cluster.
    let reason = first_frame(
        &f,
        voter,
        ALPN_PEER,
        &mut acceptor,
        hello(PeerRole::Voter, ClusterId([9; 16]), Some(inc(1))),
    )
    .await;
    assert_eq!(reason, CloseReason::Rejected("cluster".into()));
    // A peer role on the API ALPN, and a missing incarnation.
    let reason = first_frame(
        &f,
        voter,
        ALPN_API,
        &mut acceptor,
        hello(PeerRole::Voter, CLUSTER, Some(inc(1))),
    )
    .await;
    assert_eq!(reason, CloseReason::Rejected("role class".into()));
    let reason = first_frame(
        &f,
        voter,
        ALPN_PEER,
        &mut acceptor,
        hello(PeerRole::Voter, CLUSTER, None),
    )
    .await;
    assert_eq!(reason, CloseReason::Rejected("incarnation".into()));
    // A certificate entitled to Frontend claiming Voter, and a certificate
    // the binder never issued.
    let frontend = &f.ids[2];
    let reason = first_frame(
        &f,
        frontend,
        ALPN_PEER,
        &mut acceptor,
        hello(PeerRole::Voter, CLUSTER, Some(inc(1))),
    )
    .await;
    assert!(
        matches!(reason, CloseReason::Rejected(ref m) if m.contains("RoleNotAuthorized")),
        "{reason:?}"
    );
    let stranger = f.ca.issue("stranger", r(7), inc(1), PeerRole::Voter);
    let reason = first_frame(
        &f,
        &stranger,
        ALPN_PEER,
        &mut acceptor,
        hello(PeerRole::Voter, CLUSTER, Some(inc(1))),
    )
    .await;
    assert!(
        matches!(reason, CloseReason::Rejected(ref m) if m.contains("UnknownCertificate")),
        "{reason:?}"
    );
    // An unknown ALPN never reaches negotiation.
    let client = raw_client(&f, voter, b"h3");
    let err = client
        .connect(acceptor.local_addr().unwrap(), &f.ids[0].name)
        .unwrap()
        .await;
    assert!(err.is_err(), "alpn mismatch fails the handshake");
    assert_eq!(acceptor.connections(), 0);
    // The outgoing side binds the server's certificate to the expected
    // identity before sending anything: the wrong node is a rejection.
    let dialer = bind(&f, 1);
    let mut wrong = f.ids[0].expected();
    wrong.replica = Some(r(5));
    let err = dialer
        .connect(
            acceptor.local_addr().unwrap(),
            &f.ids[0].name,
            PeerRole::Voter,
            Some(inc(1)),
            wrong,
        )
        .await;
    assert!(
        matches!(err, Err(coord_transport::TransportError::Rejected(_))),
        "{err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn no_application_zero_rtt() {
    assert_eq!(Transport::tls_profile().max_early_data_size, 0);
    assert!(!Transport::tls_profile().client_early_data);
    assert!(Transport::tls_profile().tls13_only);
    assert!(Transport::tls_profile().client_certificate_required);
    let f = fixture(&[PeerRole::Voter, PeerRole::Voter]);
    let mut acceptor = bind(&f, 0);
    // A raw client that would use early data if the server allowed it.
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let mut client = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(f.ca.roots())
        .with_client_auth_cert(f.ids[1].chain.clone(), f.ids[1].key.clone_key())
        .unwrap();
    client.alpn_protocols = vec![ALPN_PEER.to_vec()];
    client.enable_early_data = true;
    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(
        QuicClientConfig::try_from(client).unwrap(),
    )));
    let addr = acceptor.local_addr().unwrap();
    // First connection: full handshake, negotiate, then close orderly so a
    // session ticket may be cached.
    let conn = endpoint
        .connect(addr, &f.ids[0].name)
        .unwrap()
        .await
        .unwrap();
    let (mut send, _recv) = conn.open_bi().await.unwrap();
    send.write_all(&hello(PeerRole::Voter, CLUSTER, Some(inc(1))))
        .await
        .unwrap();
    assert!(matches!(
        event(&mut acceptor).await,
        TransportEvent::Connected { .. }
    ));
    conn.close(0u32.into(), b"");
    assert!(matches!(
        event(&mut acceptor).await,
        TransportEvent::Closed { .. }
    ));
    // Second connection: 0-RTT is either impossible or rejected by the
    // server; application data never rides early data.
    let connecting = endpoint.connect(addr, &f.ids[0].name).unwrap();
    match connecting.into_0rtt() {
        Err(connecting) => {
            let _ = connecting.await.unwrap();
        }
        Ok((_conn, accepted)) => {
            assert!(!accepted.await, "the server must not accept early data");
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn transport_completion_is_not_durability_or_establishment() {
    let f = fixture(&[PeerRole::Voter, PeerRole::Voter]);
    let a = bind(&f, 0);
    let mut b = bind(&f, 1);
    connect_peer(&a, &b, &f.ids[1]).await;
    // The send completes while the receiver has not looked at anything.
    a.send_peer(r(1), inc(1), evidence_frame(b"proposal").unwrap())
        .await
        .unwrap();
    let mut kinds = Vec::new();
    for _ in 0..2 {
        kinds.push(match event(&mut b).await {
            // Exhaustive: the event type has no variant a state machine
            // could mistake for a storage completion or an established
            // result; only these exist.
            TransportEvent::Connected { .. } => "connected",
            TransportEvent::PeerFrame { .. } => "frame",
            TransportEvent::ApiRequest { .. } => "request",
            TransportEvent::Closed { .. } => "closed",
        });
    }
    assert_eq!(kinds, vec!["connected", "frame"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_is_bounded_and_stream_bounds_hold() {
    let f = fixture(&[PeerRole::Voter, PeerRole::Voter]);
    let mut a = bind(&f, 0);
    let mut b = bind(&f, 1);
    connect_peer(&a, &b, &f.ids[1]).await;
    assert!(matches!(
        event(&mut a).await,
        TransportEvent::Connected { .. }
    ));
    assert!(matches!(
        event(&mut b).await,
        TransportEvent::Connected { .. }
    ));
    // A one-frame stream with trailing bytes closes the connection.
    let raw = raw_client(&f, &f.ids[1], ALPN_PEER);
    let conn = raw
        .connect(a.local_addr().unwrap(), &f.ids[0].name)
        .unwrap()
        .await
        .unwrap();
    let (mut send, _recv) = conn.open_bi().await.unwrap();
    send.write_all(&hello(PeerRole::Voter, CLUSTER, Some(inc(1))))
        .await
        .unwrap();
    assert!(matches!(
        event(&mut a).await,
        TransportEvent::Connected { .. }
    ));
    let mut uni = conn.open_uni().await.unwrap();
    let mut frame = evidence_frame(b"x").unwrap();
    frame.extend_from_slice(b"trailing");
    uni.write_all(&frame).await.unwrap();
    uni.finish().unwrap();
    match event(&mut a).await {
        TransportEvent::Closed { reason, .. } => {
            assert!(
                matches!(reason, CloseReason::Malformed(ref m) if m.contains("Trailing")),
                "{reason:?}"
            )
        }
        other => panic!("{other:?}"),
    }
    // Shutdown returns within its deadline even though the peer holds the
    // connection open; the peer observes the close.
    let started = std::time::Instant::now();
    let drained = b.shutdown(Duration::from_secs(2)).await;
    assert!(started.elapsed() < Duration::from_secs(5));
    let _ = drained;
    match event(&mut a).await {
        TransportEvent::Closed { reason, .. } => {
            assert!(
                matches!(
                    reason,
                    CloseReason::PeerClosed { .. } | CloseReason::Transport(_)
                ),
                "{reason:?}"
            )
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(a.connections(), 0);
    let _ = (
        ALPN_API,
        BoundIdentity {
            role: PeerRole::Voter,
            replica: None,
            incarnation: None,
            capabilities: vec![],
        },
    );
}

/// Negotiate a raw client by hand and return the live connection. Bytes
/// in `pipelined` follow the hello in the same write, so they arrive in
/// the same read the hello does.
async fn negotiated(
    f: &Fixture,
    id: &TestIdentity,
    alpn: &[u8],
    acceptor: &mut Transport,
    role: PeerRole,
    pipelined: &[u8],
) -> (quinn::Endpoint, quinn::Connection, quinn::SendStream) {
    let client = raw_client(f, id, alpn);
    let conn = client
        .connect(acceptor.local_addr().unwrap(), &f.ids[0].name)
        .unwrap()
        .await
        .unwrap();
    let (mut send, _recv) = conn.open_bi().await.unwrap();
    let mut first = hello(role, CLUSTER, Some(inc(1)));
    first.extend_from_slice(pipelined);
    send.write_all(&first).await.unwrap();
    match event(acceptor).await {
        TransportEvent::Connected { .. } => {}
        other => panic!("{other:?}"),
    }
    (client, conn, send)
}

/// A peer-evidence frame with its kind or version overwritten.
fn evidence_with(kind: u16, version: u16, payload: &[u8]) -> Vec<u8> {
    let mut frame = evidence_frame(payload).unwrap();
    frame[4..6].copy_from_slice(&kind.to_be_bytes());
    frame[6..8].copy_from_slice(&version.to_be_bytes());
    frame
}

#[tokio::test(flavor = "multi_thread")]
async fn peer_streams_carry_only_supported_peer_evidence() {
    // The frame reader checks lengths and class limits, not what a frame
    // is. A negotiated peer sending an unsupported evidence version, or
    // another kind entirely, must not have it emitted as authenticated
    // consensus input.
    for (kind, version) in [
        (coord_transport::KIND_PEER_EVIDENCE, 2u16),
        (0x0100u16, 1u16),
    ] {
        let f = fixture(&[PeerRole::Voter, PeerRole::Voter]);
        let mut acceptor = bind(&f, 0);
        let (_client, conn, _control) = negotiated(
            &f,
            &f.ids[1],
            ALPN_PEER,
            &mut acceptor,
            PeerRole::Voter,
            &[],
        )
        .await;
        let mut uni = conn.open_uni().await.unwrap();
        uni.write_all(&evidence_with(kind, version, b"not-evidence"))
            .await
            .unwrap();
        uni.finish().unwrap();
        loop {
            match event(&mut acceptor).await {
                TransportEvent::PeerFrame { .. } => {
                    panic!("{kind:#06x} v{version} reached consensus")
                }
                TransportEvent::Closed { reason, .. } => {
                    assert!(
                        matches!(reason, CloseReason::Malformed(ref m) if m.contains("peer frame")),
                        "{reason:?}"
                    );
                    break;
                }
                _ => {}
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn api_streams_carry_only_client_originated_requests() {
    // A reply, a handshake message or an undecodable payload on an API
    // request stream is a protocol violation at the boundary, not
    // something every consumer downstream has to re-check.
    let f = fixture(&[PeerRole::Voter, PeerRole::Frontend]);
    let mut acceptor = bind(&f, 0);
    let (_client, conn, _control) = negotiated(
        &f,
        &f.ids[1],
        ALPN_API,
        &mut acceptor,
        PeerRole::Frontend,
        &[],
    )
    .await;
    let (mut send, _recv) = conn.open_bi().await.unwrap();
    send.write_all(&hello(PeerRole::Frontend, CLUSTER, None))
        .await
        .unwrap();
    send.finish().unwrap();
    loop {
        match event(&mut acceptor).await {
            TransportEvent::ApiRequest { .. } => panic!("a hello was dispatched as a request"),
            TransportEvent::Closed { reason, .. } => {
                assert!(
                    matches!(reason, CloseReason::Malformed(ref m) if m.contains("not a request")),
                    "{reason:?}"
                );
                break;
            }
            _ => {}
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_close_pipelined_behind_the_hello_is_not_lost() {
    // One QUIC read can carry the hello and the close that follows it.
    // The reader holding those extra bytes lives with the control stream,
    // so the close is honored rather than discarded with a per-read
    // reader.
    let f = fixture(&[PeerRole::Voter, PeerRole::Voter]);
    let mut acceptor = bind(&f, 0);
    let close = MessageV1::Close(CloseV1 {
        code: 0x1234,
        reason: coord_types::wire_v1::BoundedBytes::new(b"bye".to_vec()).unwrap(),
    })
    .encode()
    .unwrap();
    let (_client, _conn, _control) = negotiated(
        &f,
        &f.ids[1],
        ALPN_PEER,
        &mut acceptor,
        PeerRole::Voter,
        &close,
    )
    .await;
    match event(&mut acceptor).await {
        TransportEvent::Closed { reason, .. } => {
            assert_eq!(reason, CloseReason::PeerClosed { code: 0x1234 })
        }
        other => panic!("{other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_frame_deadline_bounds_the_whole_peer_send() {
    // A peer that grants no more flow-control credit lets the stream open
    // complete and then leaves the write pending. The deadline covers the
    // complete frame, so the sender gives up instead of hanging.
    let f = fixture(&[PeerRole::Voter, PeerRole::Voter]);
    let mut acceptor = bind(&f, 0);
    let mut transport = quinn::TransportConfig::default();
    transport.receive_window(quinn::VarInt::from_u32(4096));
    transport.stream_receive_window(quinn::VarInt::from_u32(4096));
    let client = raw_client_with(&f, &f.ids[1], ALPN_PEER, Arc::new(transport));
    let conn = client
        .connect(acceptor.local_addr().unwrap(), &f.ids[0].name)
        .unwrap()
        .await
        .unwrap();
    let (mut send, _recv) = conn.open_bi().await.unwrap();
    send.write_all(&hello(PeerRole::Voter, CLUSTER, Some(inc(1))))
        .await
        .unwrap();
    match event(&mut acceptor).await {
        TransportEvent::Connected { .. } => {}
        other => panic!("{other:?}"),
    }
    // Far more than the peer's window, and it never reads a byte.
    let big = evidence_frame(&vec![7u8; 256 * 1024]).unwrap();
    let started = std::time::Instant::now();
    let sent = timeout(
        Duration::from_secs(20),
        acceptor.send_peer(r(1), inc(1), big),
    )
    .await
    .expect("the send returns rather than hanging");
    assert!(
        matches!(sent, Err(coord_transport::SendError::Timeout)),
        "{sent:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "it gave up at the frame deadline: {:?}",
        started.elapsed()
    );
}

//! task-30 and task-31 acceptance on real loopback endpoints: role and
//! lane negotiation with bound provenance; malformed frames and origin,
//! role, lane, version and identity mismatches fail closed; no
//! application 0-RTT; a transport completion is never durability or
//! establishment; bounded shutdown and stream limits; sparse connections
//! only where asked; a stalled bulk consumer cannot starve control; the
//! destination budget is shared across lanes with a control reserve;
//! oversize frames are refused before buffering; fan-out admits each
//! destination on its own; queue wait, credit wait and RTT are distinct.

use std::sync::Arc;
use std::time::Duration;

use coord_transport::{
    ALPN_API, ALPN_PEER, BudgetLimits, Class, CloseCode, CloseReason, Destination, Lane,
    LaneLimits, Limits, SendError, Transport, TransportError, TransportEvent, evidence_frame,
};
use coord_transport_testkit::{TestBinder, TestCa, TestIdentity};
use coord_types::ids::{ClusterId, DomainId, ReplicaId, ReplicaIncarnation};
use coord_types::wire_v1::{
    BoundedVec, CloseV1, HEADER_LEN, HelloV1, KIND_SESSION_BIND, KIND_SESSION_BIND_ACK, MessageV1,
    PeerRole, SESSION_BIND_VERSION, encode_frame,
};
use quinn::crypto::rustls::QuicClientConfig;
use tokio::time::timeout;

const CLUSTER: ClusterId = ClusterId([1; 16]);
const DOMAIN: DomainId = DomainId([2; 16]);
const OTHER_DOMAIN: DomainId = DomainId([3; 16]);

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

fn bind_with(f: &Fixture, i: usize, limits: Limits) -> Transport {
    let local = f.ids[i].local(&f.ca, CLUSTER, DOMAIN, vec![1, 2]);
    Transport::bind(
        "127.0.0.1:0".parse().unwrap(),
        local,
        f.binder.clone(),
        limits,
    )
    .unwrap()
}

fn bind(f: &Fixture, i: usize) -> Transport {
    bind_with(f, i, limits())
}

async fn event(t: &mut Transport) -> TransportEvent {
    timeout(Duration::from_secs(5), t.next_event())
        .await
        .expect("event within deadline")
        .expect("endpoint alive")
}

async fn event_in(t: &mut Transport, lane: Lane) -> TransportEvent {
    timeout(Duration::from_secs(5), t.next_event_in(lane))
        .await
        .expect("event within deadline")
        .expect("endpoint alive")
}

async fn connect_lane(from: &Transport, to: &Transport, to_id: &TestIdentity, lane: Lane) {
    let addr = to.local_addr().unwrap();
    from.connect(
        addr,
        &to_id.name,
        PeerRole::Voter,
        Some(inc(1)),
        lane,
        to_id.expected(),
    )
    .await
    .unwrap();
}

fn dest(i: u8, lane: Lane) -> Destination {
    Destination::Replica {
        replica: r(i),
        incarnation: inc(1),
        lane,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn voters_negotiate_roles_and_exchange_frames_with_bound_provenance() {
    let f = fixture(&[PeerRole::Voter, PeerRole::Voter, PeerRole::Voter]);
    let mut a = bind(&f, 0);
    let mut b = bind(&f, 1);
    let c = bind(&f, 2);
    connect_lane(&a, &b, &f.ids[1], Lane::Control).await;
    match event(&mut a).await {
        TransportEvent::Connected {
            class,
            lane,
            identity,
            ..
        } => {
            assert_eq!(class, Class::Peer);
            assert_eq!(lane, Lane::Control);
            assert_eq!(identity.replica, Some(r(1)));
            assert_eq!(identity.incarnation, Some(inc(1)));
            assert_eq!(identity.role, PeerRole::Voter);
            assert_eq!(
                identity.capabilities,
                vec![1, 2, Lane::Control.capability()],
                "granted intersection plus the lane"
            );
        }
        other => panic!("{other:?}"),
    }
    match event(&mut b).await {
        TransportEvent::Connected { identity, lane, .. } => {
            assert_eq!(identity.replica, Some(r(0)));
            assert_eq!(lane, Lane::Control);
        }
        other => panic!("{other:?}"),
    }
    // A frame each way over the one connection; provenance is what
    // negotiation bound, not what the frame claims.
    a.send(
        dest(1, Lane::Control),
        DOMAIN,
        evidence_frame(b"vote-from-a").unwrap(),
    )
    .unwrap();
    match event(&mut b).await {
        TransportEvent::PeerFrame {
            provenance,
            lane,
            kind,
            payload,
            ..
        } => {
            assert_eq!(provenance.from(), r(0));
            assert_eq!(provenance.incarnation(), inc(1));
            assert_eq!(lane, Lane::Control);
            assert_eq!(kind, coord_transport::KIND_PEER_EVIDENCE);
            assert_eq!(payload, b"vote-from-a");
        }
        other => panic!("{other:?}"),
    }
    b.send(
        dest(0, Lane::Control),
        DOMAIN,
        evidence_frame(b"ack-from-b").unwrap(),
    )
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
    // Sparse: nothing dialed the third voter, and it dialed nobody; the
    // bulk lane to b was never opened either.
    assert_eq!(a.connections(), 1);
    assert_eq!(b.connections(), 1);
    assert_eq!(c.connections(), 0);
    assert_eq!(
        a.send(
            dest(2, Lane::Control),
            DOMAIN,
            evidence_frame(b"x").unwrap()
        ),
        Err(SendError::NotConnected)
    );
    assert_eq!(
        a.send(dest(1, Lane::Bulk), DOMAIN, evidence_frame(b"x").unwrap()),
        Err(SendError::NotConnected)
    );
    // A voter may not open a unary lane.
    let err = a
        .connect(
            b.local_addr().unwrap(),
            &f.ids[1].name,
            PeerRole::Voter,
            Some(inc(1)),
            Lane::Unary,
            f.ids[1].expected(),
        )
        .await;
    assert!(matches!(
        err,
        Err(TransportError::LaneNotAdmitted(Lane::Unary))
    ));
    let stats = a.stats(r(1), inc(1), Lane::Control).unwrap();
    assert_eq!(stats.frames, 1);
    assert_eq!(stats.queue_wait.count, 1);
    assert_eq!(stats.credit_wait.count, 1);
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

fn hello_with(
    role: PeerRole,
    cluster: ClusterId,
    incarnation: Option<ReplicaIncarnation>,
    capabilities: Vec<u16>,
) -> Vec<u8> {
    MessageV1::Hello(HelloV1 {
        role,
        cluster_id: cluster,
        domain_id: DOMAIN,
        incarnation,
        capabilities: BoundedVec::new(capabilities).unwrap(),
    })
    .encode()
    .unwrap()
}

fn hello(role: PeerRole, cluster: ClusterId, incarnation: Option<ReplicaIncarnation>) -> Vec<u8> {
    hello_with(
        role,
        cluster,
        incarnation,
        vec![1, Lane::Control.capability()],
    )
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
    // payload is read; one above the class limit likewise.
    let mut short = Vec::new();
    short.extend_from_slice(&2u32.to_be_bytes());
    short.extend_from_slice(&0x0001u16.to_be_bytes());
    short.extend_from_slice(&1u16.to_be_bytes());
    let reason = first_frame(&f, voter, ALPN_PEER, &mut acceptor, short).await;
    assert!(
        matches!(reason, CloseReason::Malformed(ref m) if m.contains("LengthBelowMinimum")),
        "{reason:?}"
    );
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
    // No lane, two lanes, or a lane the role may not open.
    for caps in [
        vec![1],
        vec![Lane::Control.capability(), Lane::Bulk.capability()],
        vec![Lane::Unary.capability()],
    ] {
        let reason = first_frame(
            &f,
            voter,
            ALPN_PEER,
            &mut acceptor,
            hello_with(PeerRole::Voter, CLUSTER, Some(inc(1)), caps),
        )
        .await;
        assert!(
            matches!(reason, CloseReason::Rejected(ref m) if m.starts_with("lane")),
            "{reason:?}"
        );
    }
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
            Lane::Control,
            wrong,
        )
        .await;
    assert!(matches!(err, Err(TransportError::Rejected(_))), "{err:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn no_application_zero_rtt() {
    assert_eq!(Transport::tls_profile().max_early_data_size, 0);
    assert!(!Transport::tls_profile().client_early_data);
    assert!(Transport::tls_profile().tls13_only);
    assert!(Transport::tls_profile().peer_mutual_tls);
    assert!(Transport::tls_profile().api_mutual_tls_for_trusted_roles);
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
    connect_lane(&a, &b, &f.ids[1], Lane::Control).await;
    // The send is admitted while the receiver has not looked at anything.
    a.send(
        dest(1, Lane::Control),
        DOMAIN,
        evidence_frame(b"proposal").unwrap(),
    )
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
            TransportEvent::ApiDelivery { .. } => "delivery",
            TransportEvent::Closed { .. } => "closed",
        });
    }
    assert_eq!(kinds, vec!["connected", "frame"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_is_bounded_and_stream_bounds_hold() {
    // A third identity for the raw client: one connection per lane is a
    // rule now, so a raw dial under r(1)'s identity would displace b's
    // connection instead of standing beside it.
    let f = fixture(&[PeerRole::Voter, PeerRole::Voter, PeerRole::Voter]);
    let mut a = bind(&f, 0);
    let mut b = bind(&f, 1);
    connect_lane(&a, &b, &f.ids[1], Lane::Control).await;
    assert!(matches!(
        event(&mut a).await,
        TransportEvent::Connected { .. }
    ));
    assert!(matches!(
        event(&mut b).await,
        TransportEvent::Connected { .. }
    ));
    // A one-frame stream with trailing bytes closes the connection.
    let raw = raw_client(&f, &f.ids[2], ALPN_PEER);
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
}

/// Limits that make bulk stall quickly: tiny queues, one bulk stream and
/// a small destination budget with a control reserve.
fn tight_limits() -> Limits {
    let mut l = limits();
    l.event_queue = 2;
    l.lanes[Lane::Bulk.index()] = LaneLimits {
        max_uni_streams: 1,
        queue_depth: 2,
        max_groups: 2,
        ..LaneLimits::BULK
    };
    l.budget = BudgetLimits {
        destination_bytes: 48 * 1024,
        node_bytes: 96 * 1024,
        control_reserve: 16 * 1024,
        max_opens: 8,
    };
    l
}

fn bulk_frame(fill: u8) -> Vec<u8> {
    evidence_frame(&vec![fill; 20 * 1024]).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stalled_bulk_consumer_cannot_starve_control_and_the_budget_is_shared() {
    let f = fixture(&[PeerRole::Voter, PeerRole::Voter]);
    let a = bind_with(&f, 0, tight_limits());
    let mut b = bind_with(&f, 1, tight_limits());
    connect_lane(&a, &b, &f.ids[1], Lane::Control).await;
    connect_lane(&a, &b, &f.ids[1], Lane::Bulk).await;
    assert_eq!(a.connections(), 2, "one connection per lane");
    assert!(matches!(
        event_in(&mut b, Lane::Control).await,
        TransportEvent::Connected {
            lane: Lane::Control,
            ..
        }
    ));
    assert!(matches!(
        event_in(&mut b, Lane::Bulk).await,
        TransportEvent::Connected {
            lane: Lane::Bulk,
            ..
        }
    ));
    // b never reads its bulk lane again. a pushes bulk until every bound
    // pushes back: the bulk queue refuses, the sender waits on credit and
    // the destination budget never exceeds its shared part.
    let mut refused = 0;
    let mut admitted = 0;
    let deadline = std::time::Instant::now() + Duration::from_secs(4);
    while std::time::Instant::now() < deadline {
        match a.send(dest(1, Lane::Bulk), DOMAIN, bulk_frame(0xbb)) {
            Ok(()) => admitted += 1,
            Err(SendError::QueueFull { lane: Lane::Bulk }) => {
                refused += 1;
                if refused > 20 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => panic!("{e:?}"),
        }
    }
    assert!(
        refused > 0,
        "the bulk queue refused after {admitted} frames"
    );
    let (_, peak) = a.budget_of(r(1), inc(1)).unwrap();
    assert!(peak > 0);
    assert!(
        peak <= 48 * 1024 - 16 * 1024,
        "bulk never took the control reserve: peak {peak}"
    );
    let bulk = a.stats(r(1), inc(1), Lane::Bulk).unwrap();
    assert!(bulk.refused > 0);
    assert!(bulk.queued > 0);
    // Control still flows, promptly, over its own connection and queue,
    // using the reserve the bulk lane cannot touch.
    let started = std::time::Instant::now();
    a.send(
        dest(1, Lane::Control),
        DOMAIN,
        evidence_frame(b"vote-while-bulk-stalls").unwrap(),
    )
    .unwrap();
    match event_in(&mut b, Lane::Control).await {
        TransportEvent::PeerFrame { payload, lane, .. } => {
            assert_eq!(lane, Lane::Control);
            assert_eq!(payload, b"vote-while-bulk-stalls");
        }
        other => panic!("{other:?}"),
    }
    assert!(started.elapsed() < Duration::from_secs(2));
    let control = a.stats(r(1), inc(1), Lane::Control).unwrap();
    assert_eq!(control.refused, 0);
    assert_eq!(control.frames, 1);
    // Waits are measured apart from the RTT: bulk waited for credit while
    // the loopback RTT stayed small.
    assert!(bulk.credit_wait.count >= 1);
    assert!(bulk.queue_wait.count >= 1);
    assert!(control.rtt < Duration::from_secs(1));
    // A frame the bulk lane could never hold is refused up front.
    let huge = evidence_frame(&vec![0u8; 40 * 1024]).unwrap();
    assert!(matches!(
        a.send(dest(1, Lane::Bulk), DOMAIN, huge),
        Err(SendError::TooLarge { .. })
    ));
    // Draining bulk at b lets the backlog through.
    let mut drained = 0;
    while drained < admitted {
        match timeout(Duration::from_secs(5), b.next_event_in(Lane::Bulk)).await {
            Ok(Some(TransportEvent::PeerFrame {
                lane: Lane::Bulk, ..
            })) => drained += 1,
            Ok(Some(_)) => {}
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(drained, admitted);
}

#[tokio::test(flavor = "multi_thread")]
async fn fan_out_admits_each_destination_on_its_own_and_groups_share_fairly() {
    let f = fixture(&[PeerRole::Voter, PeerRole::Voter, PeerRole::Voter]);
    let a = bind_with(&f, 0, tight_limits());
    let mut b = bind_with(&f, 1, tight_limits());
    let mut c = bind_with(&f, 2, tight_limits());
    connect_lane(&a, &b, &f.ids[1], Lane::Bulk).await;
    connect_lane(&a, &c, &f.ids[2], Lane::Bulk).await;
    let _ = event_in(&mut b, Lane::Bulk).await;
    let _ = event_in(&mut c, Lane::Bulk).await;
    // Saturate b's bulk lane (b does not read it); c stays idle.
    let deadline = std::time::Instant::now() + Duration::from_secs(4);
    let mut saturated = false;
    while std::time::Instant::now() < deadline {
        if a.send(dest(1, Lane::Bulk), DOMAIN, bulk_frame(0x11))
            .is_err()
        {
            saturated = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(saturated);
    // Fan-out: b is refused, c is admitted and delivered; the same frame
    // in another group is admitted for b on its own queue (fair share), and
    // a third group is beyond the group bound.
    let out = a.fan_out(
        &[(r(1), inc(1)), (r(2), inc(1))],
        Lane::Bulk,
        DOMAIN,
        &bulk_frame(0x22),
    );
    assert_eq!(out[0], Err(SendError::QueueFull { lane: Lane::Bulk }));
    assert_eq!(out[1], Ok(()));
    match event_in(&mut c, Lane::Bulk).await {
        TransportEvent::PeerFrame { payload, .. } => assert_eq!(payload[0], 0x22),
        other => panic!("{other:?}"),
    }
    assert_eq!(
        a.send(dest(1, Lane::Bulk), OTHER_DOMAIN, bulk_frame(0x33)),
        Ok(())
    );
    assert_eq!(
        a.send(dest(1, Lane::Bulk), DomainId([4; 16]), bulk_frame(0x44)),
        Err(SendError::TooManyGroups { lane: Lane::Bulk })
    );
    // Node-wide accounting covers both destinations.
    let (_, node_peak) = a.node_budget();
    assert!(node_peak > 0 && node_peak <= 96 * 1024);
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
async fn the_session_binding_is_the_only_undecodable_kind_an_api_stream_may_carry() {
    // The binding frame's payload belongs to `coord-session`, so this
    // crate admits it by kind. That exception is enumerated: it covers
    // exactly this kind at exactly this version, and no other kind the
    // typed decoder does not know reaches a consumer.
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

    // The binding arrives as a request frame, undecoded.
    let (mut send, _recv) = conn.open_bi().await.unwrap();
    send.write_all(
        &encode_frame(KIND_SESSION_BIND, SESSION_BIND_VERSION, &[0x02, 0xaa, 0xbb]).unwrap(),
    )
    .await
    .unwrap();
    send.finish().unwrap();
    loop {
        match event(&mut acceptor).await {
            TransportEvent::ApiRequest { frame, .. } => {
                assert_eq!(frame.kind, KIND_SESSION_BIND);
                assert_eq!(frame.version, SESSION_BIND_VERSION);
                break;
            }
            TransportEvent::Closed { reason, .. } => panic!("binding refused: {reason:?}"),
            _ => {}
        }
    }

    // A binding of another version, the acknowledgement kind (which only
    // the frontend may send), and a neighbouring unregistered kind are
    // each refused. A refusal closes the connection, so each case gets
    // its own.
    for (kind, version) in [
        (KIND_SESSION_BIND, SESSION_BIND_VERSION + 1),
        (KIND_SESSION_BIND_ACK, SESSION_BIND_VERSION),
        (KIND_SESSION_BIND + 0x10, SESSION_BIND_VERSION),
    ] {
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
        send.write_all(&encode_frame(kind, version, &[0x02, 0xaa, 0xbb]).unwrap())
            .await
            .unwrap();
        send.finish().unwrap();
        loop {
            match event(&mut acceptor).await {
                TransportEvent::ApiRequest { .. } => {
                    panic!("{kind:#06x} v{version} was dispatched as a request")
                }
                TransportEvent::Closed { reason, .. } => {
                    assert!(
                        matches!(reason, CloseReason::Malformed(_)),
                        "{kind:#06x} v{version}: {reason:?}"
                    );
                    break;
                }
                _ => {}
            }
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
    // Far more than the peer's window, and it never reads a byte. The
    // queue accepts it; the lane's sender opens the stream and then
    // waits on credit that never comes.
    let big = evidence_frame(&vec![7u8; 256 * 1024]).unwrap();
    acceptor
        .send(
            coord_transport::Destination::Replica {
                replica: r(1),
                incarnation: inc(1),
                lane: Lane::Control,
            },
            DOMAIN,
            big,
        )
        .expect("queued");
    // With one deadline over the whole frame the sender gives up and
    // counts the loss; without it the write stays pending forever and
    // this lane never sends again.
    let started = std::time::Instant::now();
    loop {
        let stats = acceptor
            .stats(r(1), inc(1), Lane::Control)
            .expect("the lane exists");
        if stats.refused > 0 {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "the sender never gave up: {stats:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_second_connection_on_one_lane_replaces_and_closes_the_first() {
    // One connection per lane. A second dial of the same lane by the same
    // replica takes the slot; leaving the displaced connection open would
    // keep its receive loop, streams and windows alive, so repeated dials
    // would multiply exactly the capacity the lane bounds.
    let f = fixture(&[PeerRole::Voter, PeerRole::Voter]);
    let mut a = bind(&f, 0);
    let mut b = bind(&f, 1);
    connect_lane(&b, &a, &f.ids[0], Lane::Control).await;
    assert!(matches!(
        event(&mut a).await,
        TransportEvent::Connected { .. }
    ));
    assert!(matches!(
        event(&mut b).await,
        TransportEvent::Connected { .. }
    ));
    assert_eq!(a.connections(), 1);
    connect_lane(&b, &a, &f.ids[0], Lane::Control).await;
    // The acceptor sees the new connection and the old one ending; the
    // order of the two is not fixed.
    let mut seen = (false, false);
    while !(seen.0 && seen.1) {
        match event(&mut a).await {
            TransportEvent::Connected { .. } => seen.0 = true,
            TransportEvent::Closed { .. } => seen.1 = true,
            other => panic!("{other:?}"),
        }
    }
    // Settle: exactly one connection for the lane, not two.
    for _ in 0..50 {
        if a.connections() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(a.connections(), 1, "the displaced connection is closed");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_idle_link_is_not_kept_for_the_process_lifetime() {
    // Every API connection and every replica incarnation is its own link
    // key. Keeping an emptied link would retain its queues, semaphores
    // and counters for as long as the endpoint lives, so ordinary
    // connect/disconnect churn would grow without bound.
    let f = fixture(&[PeerRole::Voter, PeerRole::Voter]);
    let mut a = bind(&f, 0);
    assert_eq!(a.links(), 0);
    for _ in 0..4 {
        let b = bind(&f, 1);
        connect_lane(&b, &a, &f.ids[0], Lane::Control).await;
        assert!(matches!(
            event(&mut a).await,
            TransportEvent::Connected { .. }
        ));
        assert_eq!(a.links(), 1);
        b.shutdown(Duration::from_secs(2)).await;
        match event(&mut a).await {
            TransportEvent::Closed { .. } => {}
            other => panic!("{other:?}"),
        }
        for _ in 0..50 {
            if a.links() == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(a.links(), 0, "the emptied link is released");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reply_is_admitted_under_the_same_budgets_as_any_other_traffic() {
    // A reply is traffic. Writing it straight to the stream would let
    // concurrent unary requests, or many API connections, hand window
    // after window to QUIC outside the node and destination caps the
    // lanes exist to enforce.
    let f = fixture(&[PeerRole::Voter, PeerRole::Frontend]);
    let mut acceptor = bind(&f, 0);
    assert_eq!(acceptor.node_budget(), (0, 0));
    let client = raw_client(&f, &f.ids[1], ALPN_API);
    let conn = client
        .connect(acceptor.local_addr().unwrap(), &f.ids[0].name)
        .unwrap()
        .await
        .unwrap();
    let (mut control, _control_recv) = conn.open_bi().await.unwrap();
    control
        .write_all(&hello_with(
            PeerRole::Frontend,
            CLUSTER,
            None,
            vec![Lane::Unary.capability()],
        ))
        .await
        .unwrap();
    match event(&mut acceptor).await {
        TransportEvent::Connected { .. } => {}
        other => panic!("{other:?}"),
    }
    let (mut send, mut recv) = conn.open_bi().await.unwrap();
    send.write_all(&request_frame()).await.unwrap();
    send.finish().unwrap();
    let responder = match event(&mut acceptor).await {
        TransportEvent::ApiRequest { responder, .. } => responder,
        other => panic!("{other:?}"),
    };
    let reply = evidence_frame(&vec![3u8; 64 * 1024]).unwrap();
    let size = reply.len();
    responder.respond(reply).await.unwrap();
    let mut got = Vec::new();
    let mut buf = [0u8; 4096];
    while let Some(n) = recv.read(&mut buf).await.unwrap() {
        got.extend_from_slice(&buf[..n]);
    }
    assert_eq!(got.len(), size, "the reply arrived whole");
    // The reply passed through the node budget on its way out.
    let (_, peak) = acceptor.node_budget();
    assert!(peak >= size, "the reply was never charged: peak {peak}");
}

/// A minimal well-formed unary request frame.
fn request_frame() -> Vec<u8> {
    let mut request = coord_types::logical_v1::LogicalRequest::new(
        coord_types::ids::NamespaceId([5; 16]),
        coord_types::logical_v1::CanonicalOperation::Put(coord_types::logical_v1::PutOp {
            key: vec![1],
            value: vec![2],
            lease: None,
            prev_kv: false,
        }),
    );
    request.canonicalize();
    let retry_key = coord_types::RetryKey {
        cluster_id: CLUSTER,
        domain_id: DOMAIN,
        session_id: coord_types::ids::SessionId([3; 16]),
        client_instance_id: coord_types::ids::ClientInstanceId([4; 16]),
        request_sequence: coord_types::ids::RequestSequence::new(1).unwrap(),
    };
    MessageV1::Request(coord_types::wire_v1::RequestV1::new(retry_key, &request, 0).unwrap())
        .encode()
        .unwrap()
}

/// Open a raw QUIC connection to `acceptor` offering `alpn`, with a
/// client certificate only when `identity` is given, and send `first` as
/// the first control frame. Returns how the acceptor ended it.
async fn raw_first_frame(
    f: &Fixture,
    acceptor: &mut Transport,
    server: &TestIdentity,
    alpn: &[u8],
    identity: Option<&TestIdentity>,
    first: Vec<u8>,
) -> (Option<CloseReason>, bool) {
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(f.ca.roots());
    let mut client = match identity {
        Some(id) => builder
            .with_client_auth_cert(id.chain.clone(), id.key.clone_key())
            .unwrap(),
        None => builder.with_no_client_auth(),
    };
    client.alpn_protocols = vec![alpn.to_vec()];
    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(
        QuicClientConfig::try_from(client).unwrap(),
    )));
    let addr = acceptor.local_addr().unwrap();
    let Ok(conn) = endpoint.connect(addr, &server.name).unwrap().await else {
        // The handshake itself failed: nothing reaches negotiation.
        return (None, false);
    };
    // A rejected certificate surfaces as a connection error here rather
    // than at `connect`, because QUIC carries the alert asynchronously.
    let Ok((mut send, _recv)) = conn.open_bi().await else {
        return (None, false);
    };
    if send.write_all(&first).await.is_err() {
        return (None, false);
    }
    let mut connected = false;
    let reason = loop {
        match event(acceptor).await {
            TransportEvent::Connected { .. } => connected = true,
            TransportEvent::Closed { reason, .. } => break Some(reason),
            _ => {}
        }
        if connected {
            break None;
        }
    };
    conn.close(0u32.into(), b"");
    (reason, connected)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_client_reaches_the_api_plane_without_a_client_certificate() {
    let f = fixture(&[PeerRole::Voter]);
    let mut acceptor = bind(&f, 0);
    let (reason, connected) = raw_first_frame(
        &f,
        &mut acceptor,
        &f.ids[0],
        ALPN_API,
        None,
        hello_with(
            PeerRole::Client,
            CLUSTER,
            None,
            vec![Lane::Unary.capability()],
        ),
    )
    .await;
    assert!(connected, "anonymous client refused: {reason:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_api_role_that_acts_for_others_is_refused_without_a_client_certificate() {
    let f = fixture(&[PeerRole::Voter]);
    let mut acceptor = bind(&f, 0);
    for role in [PeerRole::Frontend, PeerRole::KineCollector] {
        let (reason, connected) = raw_first_frame(
            &f,
            &mut acceptor,
            &f.ids[0],
            ALPN_API,
            None,
            hello_with(role, CLUSTER, None, vec![Lane::Unary.capability()]),
        )
        .await;
        assert!(!connected, "{role:?} negotiated anonymously");
        assert!(
            matches!(reason, Some(CloseReason::Rejected(_))),
            "{role:?}: {reason:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_peer_plane_still_requires_a_client_certificate() {
    let f = fixture(&[PeerRole::Voter]);
    let mut acceptor = bind(&f, 0);
    let (reason, connected) = raw_first_frame(
        &f,
        &mut acceptor,
        &f.ids[0],
        ALPN_PEER,
        None,
        hello(PeerRole::Voter, CLUSTER, Some(inc(1))),
    )
    .await;
    assert!(!connected, "a voter negotiated without a certificate");
    assert!(
        reason.is_none() || matches!(reason, Some(CloseReason::Rejected(_))),
        "{reason:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_client_certificate_outside_the_trust_anchors_is_refused_on_both_planes() {
    let f = fixture(&[PeerRole::Voter]);
    let other = TestCa::new();
    let stranger = other.issue("stranger", r(9), inc(1), PeerRole::Client);
    let mut acceptor = bind(&f, 0);
    for (alpn, first) in [
        (
            ALPN_API,
            hello_with(
                PeerRole::Client,
                CLUSTER,
                None,
                vec![Lane::Unary.capability()],
            ),
        ),
        (ALPN_PEER, hello(PeerRole::Voter, CLUSTER, Some(inc(1)))),
    ] {
        let (reason, connected) =
            raw_first_frame(&f, &mut acceptor, &f.ids[0], alpn, Some(&stranger), first).await;
        assert!(
            !connected,
            "an untrusted certificate negotiated: {reason:?}"
        );
    }
}

/// A watch is not a request with a long answer: the caller opens one
/// stream and the frontend writes events, progress and finally a close
/// onto it over the life of the subscription. A responder that could only
/// write once and finish could not serve that shape at all.
///
/// Each frame is still admitted under both budgets. What differs from a
/// unary reply is only how long the admitted bytes are held: a
/// subscription outlives any one frame, so holding each frame's budget
/// until the stream ends would let one slow consumer take the lane's
/// whole allowance and never give it back.
#[tokio::test(flavor = "multi_thread")]
async fn a_watch_stream_carries_many_frames_and_ends_when_the_frontend_says_so() {
    let f = fixture(&[PeerRole::Voter, PeerRole::Frontend]);
    let mut acceptor = bind(&f, 0);
    let client = raw_client(&f, &f.ids[1], ALPN_API);
    let conn = client
        .connect(acceptor.local_addr().unwrap(), &f.ids[0].name)
        .unwrap()
        .await
        .unwrap();
    let (mut control, _control_recv) = conn.open_bi().await.unwrap();
    control
        .write_all(&hello_with(
            PeerRole::Frontend,
            CLUSTER,
            None,
            vec![Lane::Watch.capability()],
        ))
        .await
        .unwrap();
    match event(&mut acceptor).await {
        TransportEvent::Connected { .. } => {}
        other => panic!("{other:?}"),
    }
    let (mut send, mut recv) = conn.open_bi().await.unwrap();
    send.write_all(&request_frame()).await.unwrap();
    send.finish().unwrap();
    let mut responder = match event(&mut acceptor).await {
        TransportEvent::ApiRequest { responder, .. } => responder,
        other => panic!("{other:?}"),
    };

    let mut expected = Vec::new();
    for round in 0..4u8 {
        let frame = evidence_frame(&vec![round; 1024]).unwrap();
        expected.extend_from_slice(&frame);
        responder.push(&frame).await.unwrap();
    }
    responder.finish().unwrap();

    let mut got = Vec::new();
    let mut buf = [0u8; 4096];
    while let Some(n) = recv.read(&mut buf).await.unwrap() {
        got.extend_from_slice(&buf[..n]);
    }
    assert_eq!(
        got, expected,
        "every frame arrived, in order, and the stream ended"
    );
    // Each pushed frame was charged on its way out, like any other
    // traffic: the peak covers at least one whole frame.
    let (_, peak) = acceptor.node_budget();
    assert!(peak >= 1024, "watch frames were never charged: peak {peak}");
}

/// A connection the runtime above refuses stops being served.
///
/// The transport refuses what it can judge alone -- framing, negotiation,
/// lane -- but whether a bound session may still send is not one of those
/// things. Without this the daemon could only ignore such a caller's
/// frames, which is not the same as closing: the same frame can be sent
/// again on the next stream.
#[tokio::test(flavor = "multi_thread")]
async fn the_runtime_can_close_a_connection_the_transport_would_have_allowed() {
    let f = fixture(&[PeerRole::Voter, PeerRole::Frontend]);
    let mut acceptor = bind(&f, 0);
    let client = raw_client(&f, &f.ids[1], ALPN_API);
    let conn = client
        .connect(acceptor.local_addr().unwrap(), &f.ids[0].name)
        .unwrap()
        .await
        .unwrap();
    let (mut control, _control_recv) = conn.open_bi().await.unwrap();
    control
        .write_all(&hello_with(
            PeerRole::Frontend,
            CLUSTER,
            None,
            vec![Lane::Unary.capability()],
        ))
        .await
        .unwrap();
    let connection = match event(&mut acceptor).await {
        TransportEvent::Connected { connection, .. } => connection,
        other => panic!("{other:?}"),
    };

    assert!(acceptor.disconnect(connection, CloseCode::Rejected, "not bound"));
    // The close this endpoint took is distinguishable afterwards from one
    // the peer took: it carries this side's own reason.
    match event(&mut acceptor).await {
        TransportEvent::Closed { reason, .. } => {
            assert_eq!(reason, CloseReason::Rejected("not bound".into()));
        }
        other => panic!("{other:?}"),
    }
    // And the peer really is gone, not merely unsubscribed: the client
    // itself observes the close, carrying the code this side sent.
    //
    // This waits for the close rather than probing with `open_bi`.
    // Opening a stream is a local act -- QUIC hands out a stream id
    // without a round trip -- so it can still succeed for as long as the
    // CONNECTION_CLOSE is in flight, which made that check fail about
    // one run in three.
    let ended = timeout(Duration::from_secs(5), conn.closed())
        .await
        .expect("the client observed the close within the bound");
    assert!(
        matches!(
            ended,
            quinn::ConnectionError::ApplicationClosed(ref c)
                if c.error_code == quinn::VarInt::from_u32(CloseCode::Rejected as u32)
        ),
        "the client saw a different ending: {ended:?}"
    );
    for _ in 0..50 {
        if acceptor.connections() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(acceptor.connections(), 0);

    // A connection that is not open is reported as such rather than
    // silently succeeding: the caller learns its close did nothing.
    assert!(!acceptor.disconnect(connection, CloseCode::Rejected, "again"));
}

/// An endpoint that grants several lanes still opens exactly one.
///
/// A node's endpoint grants the lanes its role may use -- a voter grants
/// control and bulk, a collector grants three -- and a `Hello` declares
/// the lane *being opened*. Announcing the endpoint's whole set would
/// declare several lanes on one connection, which the acceptor refuses
/// outright: a peer that saw two would have no way to tell which stream
/// is which.
///
/// The transport's own tests granted capabilities that are not lanes, so
/// nothing here dialled with more than one until a daemon did.
#[tokio::test(flavor = "multi_thread")]
async fn an_endpoint_granting_several_lanes_declares_only_the_one_it_opens() {
    let f = fixture(&[PeerRole::Voter, PeerRole::Voter]);
    let granted: Vec<u16> = coord_transport::role_lanes(PeerRole::Voter)
        .iter()
        .map(|lane| lane.capability())
        .collect();
    assert!(granted.len() > 1, "a voter grants more than one lane");

    let a = Transport::bind(
        "127.0.0.1:0".parse().unwrap(),
        f.ids[0].local(&f.ca, CLUSTER, DOMAIN, granted.clone()),
        f.binder.clone(),
        limits(),
    )
    .unwrap();
    let mut b = Transport::bind(
        "127.0.0.1:0".parse().unwrap(),
        f.ids[1].local(&f.ca, CLUSTER, DOMAIN, granted),
        f.binder.clone(),
        limits(),
    )
    .unwrap();

    connect_lane(&a, &b, &f.ids[1], Lane::Control).await;

    let TransportEvent::Connected { lane, identity, .. } = event(&mut b).await else {
        panic!("the acceptor refused a connection from an endpoint granting several lanes");
    };
    assert_eq!(lane, Lane::Control, "the lane the dialer opened");
    assert_eq!(identity.replica, Some(r(0)));
}

//! task-d122 acceptance on real loopback endpoints: the pre-bind gate.
//!
//! An API connection without a client certificate is nobody until it
//! binds. Until its `BindAck` it holds a place under the unbound cap for
//! at most the bind deadline, stays at the floor every accepted
//! connection starts at, and may open one stream carrying one `Bind` no
//! longer than a bind can be, refused from its header otherwise. The
//! bind is refused before its token is checked where the node takes no
//! binds, and then by the gate the runtime installs; either refusal
//! closes with the exact reason clients match. Nothing a refused
//! connection does reaches the runtime, and certificate holders are not
//! made to wait on any of it.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use coord_transport::frames::ControlStream;
use coord_transport::{
    ALPN_API, BIND_REFUSED, BIND_UNAVAILABLE, BindCheck, BindGate, CloseCode, Lane, Limits,
    Transport, TransportEvent,
};
use coord_transport_testkit::{TestBinder, TestCa, TestIdentity};
use coord_types::ids::{ClusterId, DomainId, ReplicaId, ReplicaIncarnation};
use coord_types::wire_v1::{
    BoundedVec, COLLECTOR_FRESHNESS_VERSION, COLLECTOR_SUBMIT_VERSION, CloseV1, Frame, HelloV1,
    KIND_COLLECTOR_FRESHNESS, KIND_COLLECTOR_SUBMIT, KIND_SESSION_BIND, KIND_SESSION_BIND_ACK,
    MessageV1, PeerRole, SESSION_BIND_MAX_FRAME, SESSION_BIND_VERSION, encode_frame,
};
use quinn::crypto::rustls::QuicClientConfig;
use tokio::time::timeout;

const CLUSTER: ClusterId = ClusterId([1; 16]);
const DOMAIN: DomainId = DomainId([2; 16]);

/// The payload the test gate passes; any other it refuses.
const GOOD: &[u8] = b"good token";

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

/// Node 0 serves; the others are certificate holders that dial it.
fn fixture(roles: &[PeerRole]) -> Fixture {
    let ca = TestCa::new();
    let mut binder = TestBinder::new(CLUSTER, DOMAIN);
    let ids: Vec<TestIdentity> = roles
        .iter()
        .enumerate()
        .map(|(i, role)| {
            ca.issue(
                &format!("node-{i}"),
                ReplicaId([i as u8; 16]),
                ReplicaIncarnation::new(1).unwrap(),
                *role,
            )
        })
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

fn acceptor(f: &Fixture, limits: Limits) -> Transport {
    let local = f.ids[0].local(&f.ca, CLUSTER, DOMAIN, vec![1, 2]);
    Transport::bind(
        "127.0.0.1:0".parse().unwrap(),
        local,
        f.binder.clone(),
        limits,
    )
    .unwrap()
}

/// A gate that passes [`GOOD`] and refuses anything else, counting what
/// it is asked.
#[derive(Default)]
struct CountingGate {
    checks: AtomicUsize,
}

impl BindGate for CountingGate {
    fn check(&self, frame: &Frame) -> BindCheck {
        self.checks.fetch_add(1, Ordering::SeqCst);
        if frame.payload == GOOD {
            BindCheck::Pass
        } else {
            BindCheck::Refused
        }
    }
}

impl CountingGate {
    fn install(t: &Transport) -> Arc<CountingGate> {
        let gate = Arc::new(CountingGate::default());
        t.set_bind_gate(gate.clone());
        gate
    }

    fn checks(&self) -> usize {
        self.checks.load(Ordering::SeqCst)
    }
}

/// A client endpoint offering the API plane, with a certificate only
/// where `identity` is given.
fn client(f: &Fixture, identity: Option<&TestIdentity>) -> quinn::Endpoint {
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(f.ca.roots());
    let mut config = match identity {
        Some(id) => builder
            .with_client_auth_cert(id.chain.clone(), id.key.clone_key())
            .unwrap(),
        None => builder.with_no_client_auth(),
    };
    config.alpn_protocols = vec![ALPN_API.to_vec()];
    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(
        QuicClientConfig::try_from(config).unwrap(),
    )));
    endpoint
}

fn hello(role: PeerRole, lane: Lane) -> Vec<u8> {
    MessageV1::Hello(HelloV1 {
        role,
        cluster_id: CLUSTER,
        domain_id: DOMAIN,
        incarnation: None,
        capabilities: BoundedVec::new(vec![lane.capability()]).unwrap(),
    })
    .encode()
    .unwrap()
}

/// A connection that has completed TLS, with or without a certificate.
async fn tls(f: &Fixture, to: &Transport, identity: Option<&TestIdentity>) -> Dialed {
    let endpoint = client(f, identity);
    let conn = endpoint
        .connect(to.local_addr().unwrap(), &f.ids[0].name)
        .unwrap()
        .await
        .unwrap();
    Dialed {
        _endpoint: endpoint,
        conn,
        control: None,
    }
}

struct Dialed {
    _endpoint: quinn::Endpoint,
    conn: quinn::Connection,
    /// The control stream's send half, kept so the stream stays open.
    control: Option<quinn::SendStream>,
}

impl Dialed {
    /// Send `Hello` as `role` on `lane` and read the `HelloAck`.
    async fn negotiate(&mut self, role: PeerRole, lane: Lane) {
        let (mut send, recv) = self.conn.open_bi().await.unwrap();
        send.write_all(&hello(role, lane)).await.unwrap();
        let mut control = ControlStream::new(recv);
        let ack = control.next_frame(Duration::from_secs(3)).await.unwrap();
        assert!(
            matches!(
                coord_types::wire_v1::decode(&ack),
                Ok(MessageV1::HelloAck(_))
            ),
            "{ack:?}"
        );
        self.control = Some(send);
    }

    /// Open a stream, write `bytes` and finish it.
    async fn send(&self, bytes: &[u8]) -> quinn::RecvStream {
        let (mut send, recv) = self.conn.open_bi().await.unwrap();
        send.write_all(bytes).await.unwrap();
        send.finish().unwrap();
        recv
    }

    /// How the acceptor closed this connection, within `within`: the
    /// code and the reason text.
    async fn closed(&self, within: Duration) -> (u64, Vec<u8>) {
        match timeout(within, self.conn.closed())
            .await
            .expect("closed within the deadline")
        {
            quinn::ConnectionError::ApplicationClosed(close) => {
                (close.error_code.into_inner(), close.reason.to_vec())
            }
            other => panic!("not closed by the application: {other:?}"),
        }
    }

    fn open(&self) -> bool {
        self.conn.close_reason().is_none()
    }
}

/// An anonymous `Client` past `HelloAck`.
async fn anonymous(f: &Fixture, to: &Transport) -> Dialed {
    let mut dialed = tls(f, to, None).await;
    dialed.negotiate(PeerRole::Client, Lane::Unary).await;
    dialed
}

fn bind_frame(token: &[u8]) -> Vec<u8> {
    encode_frame(KIND_SESSION_BIND, SESSION_BIND_VERSION, token).unwrap()
}

/// Nothing reaches the runtime for `quiet`.
async fn nothing_emitted(t: &mut Transport, quiet: Duration) {
    if let Ok(event) = timeout(quiet, t.next_event()).await {
        panic!("the runtime heard of it: {event:?}");
    }
}

/// The next event that is not about a connection refused before
/// negotiation.
async fn next_negotiated(t: &mut Transport) -> TransportEvent {
    loop {
        let event = timeout(Duration::from_secs(5), t.next_event())
            .await
            .expect("event within deadline")
            .expect("endpoint alive");
        if !matches!(event, TransportEvent::Closed { lane: None, .. }) {
            return event;
        }
    }
}

const REJECTED: u64 = CloseCode::Rejected as u64;
const PROTOCOL: u64 = CloseCode::Protocol as u64;
const TIMEOUT: u64 = CloseCode::Timeout as u64;

#[tokio::test(flavor = "multi_thread")]
async fn a_bind_the_gate_passes_reaches_the_runtime_and_its_ack_raises_the_connection() {
    let f = fixture(&[PeerRole::Voter]);
    let mut t = acceptor(&f, limits());
    let gate = CountingGate::install(&t);
    let dialed = anonymous(&f, &t).await;
    // Negotiated, but nobody yet: the runtime has heard nothing.
    nothing_emitted(&mut t, Duration::from_millis(300)).await;

    // At the floor: four bidirectional streams, the control stream one of
    // them, so a fourth request stream cannot be opened.
    let (mut bind_send, mut bind_recv) = dialed.conn.open_bi().await.unwrap();
    let _second = dialed.conn.open_bi().await.unwrap();
    let _third = dialed.conn.open_bi().await.unwrap();
    assert!(
        timeout(Duration::from_millis(300), dialed.conn.open_bi())
            .await
            .is_err(),
        "an unbound connection was given more than the floor's streams"
    );

    bind_send.write_all(&bind_frame(GOOD)).await.unwrap();
    bind_send.finish().unwrap();
    let connection = match next_negotiated(&mut t).await {
        TransportEvent::Connected {
            connection,
            identity,
            ..
        } => {
            assert_eq!(identity.role, PeerRole::Client);
            assert_eq!(identity.replica, None);
            connection
        }
        other => panic!("{other:?}"),
    };
    let responder = match next_negotiated(&mut t).await {
        TransportEvent::ApiRequest {
            connection: c,
            frame,
            responder,
            ..
        } => {
            assert_eq!(c, connection);
            assert_eq!(frame.kind, KIND_SESSION_BIND);
            assert_eq!(frame.payload, GOOD);
            responder
        }
        other => panic!("{other:?}"),
    };
    assert_eq!(gate.checks(), 1);

    // Still at the floor while the runtime decides.
    assert!(
        timeout(Duration::from_millis(300), dialed.conn.open_bi())
            .await
            .is_err(),
        "raised before the binding was acknowledged"
    );
    responder
        .respond(encode_frame(KIND_SESSION_BIND_ACK, SESSION_BIND_VERSION, b"ack").unwrap())
        .await
        .unwrap();
    let ack = bind_recv.read_to_end(64 * 1024).await.unwrap();
    assert_eq!(&ack[4..6], &KIND_SESSION_BIND_ACK.to_be_bytes());

    // Bound: the lane's limits, and requests reach the runtime as any
    // other client's do.
    let (mut send, _recv) = timeout(Duration::from_secs(2), dialed.conn.open_bi())
        .await
        .expect("raised to the lane once acknowledged")
        .unwrap();
    send.write_all(&bind_frame(b"a rebind goes to the runtime unchecked"))
        .await
        .unwrap();
    send.finish().unwrap();
    match next_negotiated(&mut t).await {
        TransportEvent::ApiRequest { connection: c, .. } => assert_eq!(c, connection),
        other => panic!("{other:?}"),
    }
    assert_eq!(
        gate.checks(),
        1,
        "a bound connection's binds are the runtime's"
    );
    assert!(dialed.open());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bad_token_is_refused_in_the_transport_with_the_exact_reason() {
    let f = fixture(&[PeerRole::Voter]);
    let mut t = acceptor(&f, limits());
    let gate = CountingGate::install(&t);
    let dialed = anonymous(&f, &t).await;
    dialed.send(&bind_frame(b"expired or forged")).await;
    assert_eq!(
        dialed.closed(Duration::from_secs(3)).await,
        (REJECTED, BIND_REFUSED.as_bytes().to_vec())
    );
    assert_eq!(gate.checks(), 1);
    nothing_emitted(&mut t, Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stale_node_answers_bind_unavailable_to_any_token_without_checking_it() {
    let f = fixture(&[PeerRole::Voter]);
    let mut t = acceptor(&f, limits());
    let gate = CountingGate::install(&t);

    // Not fresh, and fresh only until an instant already past: both are
    // a node that takes no binds now.
    for until in [None, Some(Instant::now())] {
        t.publish_fresh_until(until);
        for token in [GOOD, b"forged".as_slice()] {
            let dialed = anonymous(&f, &t).await;
            dialed.send(&bind_frame(token)).await;
            assert_eq!(
                dialed.closed(Duration::from_secs(3)).await,
                (REJECTED, BIND_UNAVAILABLE.as_bytes().to_vec()),
                "{until:?}, {:?}",
                String::from_utf8_lossy(token)
            );
        }
    }
    assert_eq!(gate.checks(), 0, "a stale node checked a token");
    nothing_emitted(&mut t, Duration::from_millis(300)).await;

    // Fresh again: the token is checked, and a good one goes through.
    t.publish_fresh_until(Some(Instant::now() + Duration::from_secs(60)));
    let dialed = anonymous(&f, &t).await;
    dialed.send(&bind_frame(GOOD)).await;
    assert!(matches!(
        next_negotiated(&mut t).await,
        TransportEvent::Connected { .. }
    ));
    assert_eq!(gate.checks(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stream_other_than_a_bind_before_binding_closes_the_connection_with_no_event() {
    let f = fixture(&[PeerRole::Voter]);
    let mut t = acceptor(&f, limits());
    let gate = CountingGate::install(&t);
    // A request, a bind of another version, and the acknowledgement kind.
    for frame in [
        MessageV1::Close(CloseV1 {
            code: 1,
            reason: coord_types::wire_v1::BoundedBytes::new(Vec::new()).unwrap(),
        })
        .encode()
        .unwrap(),
        encode_frame(KIND_SESSION_BIND, SESSION_BIND_VERSION + 1, GOOD).unwrap(),
        encode_frame(KIND_SESSION_BIND_ACK, SESSION_BIND_VERSION, GOOD).unwrap(),
        encode_frame(0x0101, 1, b"a request").unwrap(),
    ] {
        let dialed = anonymous(&f, &t).await;
        dialed.send(&frame).await;
        assert_eq!(
            dialed.closed(Duration::from_secs(3)).await.0,
            PROTOCOL,
            "{:#06x}",
            u16::from_be_bytes([frame[4], frame[5]])
        );
    }
    assert_eq!(gate.checks(), 0);
    nothing_emitted(&mut t, Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_oversized_bind_is_refused_from_its_header() {
    let f = fixture(&[PeerRole::Voter]);
    let mut t = acceptor(&f, limits());
    let gate = CountingGate::install(&t);
    let dialed = anonymous(&f, &t).await;
    // One byte over: only the header is sent, and the refusal comes from
    // it, long before the bind deadline or the frame timeout.
    let length = u32::try_from(SESSION_BIND_MAX_FRAME - 4 + 1).unwrap();
    let mut header = length.to_be_bytes().to_vec();
    header.extend_from_slice(&KIND_SESSION_BIND.to_be_bytes());
    header.extend_from_slice(&SESSION_BIND_VERSION.to_be_bytes());
    let (mut send, _recv) = dialed.conn.open_bi().await.unwrap();
    send.write_all(&header).await.unwrap();
    let started = Instant::now();
    assert_eq!(dialed.closed(Duration::from_secs(3)).await.0, PROTOCOL);
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(gate.checks(), 0);
    nothing_emitted(&mut t, Duration::from_millis(300)).await;

    // The largest bind there can be is admitted.
    let dialed = anonymous(&f, &t).await;
    let largest = vec![0x5a; SESSION_BIND_MAX_FRAME - 8];
    dialed.send(&bind_frame(&largest)).await;
    assert_eq!(
        dialed.closed(Duration::from_secs(3)).await,
        (REJECTED, BIND_REFUSED.as_bytes().to_vec())
    );
    assert_eq!(gate.checks(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn header_only_large_class_streams_take_no_receive_budget() {
    let f = fixture(&[PeerRole::Voter]);
    let mut t = acceptor(&f, limits());
    CountingGate::install(&t);
    // Eight connections, each naming a frame of the largest class in a
    // header and sending nothing more: before the gate these held the
    // API plane's whole receive budget for the frame timeout.
    let mut dialed = Vec::new();
    for _ in 0..8 {
        let d = anonymous(&f, &t).await;
        let length = u32::try_from(8 * 1024 * 1024).unwrap();
        let mut header = length.to_be_bytes().to_vec();
        header.extend_from_slice(&0x0201u16.to_be_bytes());
        header.extend_from_slice(&1u16.to_be_bytes());
        let (mut send, _recv) = d.conn.open_bi().await.unwrap();
        send.write_all(&header).await.unwrap();
        dialed.push((d, send));
    }
    for (d, _) in &dialed {
        assert_eq!(d.closed(Duration::from_secs(3)).await.0, PROTOCOL);
    }
    assert_eq!(t.receive_budget(), (0, 0), "a header took receive budget");
    nothing_emitted(&mut t, Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn binds_pipelined_on_one_unbound_connection_cost_at_most_one_check() {
    let f = fixture(&[PeerRole::Voter]);
    let mut t = acceptor(&f, limits());
    let gate = CountingGate::install(&t);

    // Refused: the first bind is checked and the connection closed; the
    // others are never read.
    let dialed = anonymous(&f, &t).await;
    for _ in 0..3 {
        // The acceptor can refuse the first and close the connection
        // before a later stream opens (aarch64 CI): then none is opened.
        let Ok((mut send, _recv)) = dialed.conn.open_bi().await else {
            break;
        };
        let _ = send.write_all(&bind_frame(b"forged")).await;
        let _ = send.finish();
    }
    assert_eq!(dialed.closed(Duration::from_secs(3)).await.0, REJECTED);
    assert_eq!(gate.checks(), 1);
    nothing_emitted(&mut t, Duration::from_millis(300)).await;

    // Passed: the first goes to the runtime; a second stream before its
    // acknowledgement closes the connection without being read.
    let dialed = anonymous(&f, &t).await;
    dialed.send(&bind_frame(GOOD)).await;
    assert!(matches!(
        next_negotiated(&mut t).await,
        TransportEvent::Connected { .. }
    ));
    let _held = match next_negotiated(&mut t).await {
        TransportEvent::ApiRequest { responder, .. } => responder,
        other => panic!("{other:?}"),
    };
    dialed.send(&bind_frame(GOOD)).await;
    assert_eq!(dialed.closed(Duration::from_secs(3)).await.0, PROTOCOL);
    assert_eq!(gate.checks(), 2, "one check for each connection");
    match next_negotiated(&mut t).await {
        TransportEvent::Closed { .. } => {}
        other => panic!("the second bind reached the runtime: {other:?}"),
    }

    // Two binds on one stream: the stream carries one frame, so the
    // second is a violation found before anything is checked.
    let dialed = anonymous(&f, &t).await;
    let mut both = bind_frame(GOOD);
    both.extend_from_slice(&bind_frame(GOOD));
    dialed.send(&both).await;
    assert_eq!(dialed.closed(Duration::from_secs(3)).await.0, PROTOCOL);
    assert_eq!(gate.checks(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unbound_connection_is_closed_at_its_deadline_and_its_place_freed() {
    let f = fixture(&[PeerRole::Voter, PeerRole::Frontend]);
    let mut t = acceptor(
        &f,
        Limits {
            max_unbound: 1,
            bind_deadline: Duration::from_millis(800),
            ..limits()
        },
    );
    CountingGate::install(&t);
    let idle = anonymous(&f, &t).await;

    // The one place is taken: another anonymous caller is closed as soon
    // as its handshake shows it has no certificate ...
    let refused = tls(&f, &t, None).await;
    assert_eq!(refused.closed(Duration::from_secs(3)).await.0, REJECTED);
    // ... while a certificate holder still connects.
    let mut frontend = tls(&f, &t, Some(&f.ids[1])).await;
    frontend.negotiate(PeerRole::Frontend, Lane::Unary).await;
    match next_negotiated(&mut t).await {
        TransportEvent::Connected { identity, .. } => {
            assert_eq!(identity.role, PeerRole::Frontend);
        }
        other => panic!("{other:?}"),
    }

    // The idle one is closed at its deadline, with no event ...
    assert_eq!(idle.closed(Duration::from_secs(3)).await.0, TIMEOUT);
    nothing_emitted(&mut t, Duration::from_millis(300)).await;
    // ... and its place is free.
    let next = anonymous(&f, &t).await;
    next.send(&bind_frame(GOOD)).await;
    assert!(matches!(
        next_negotiated(&mut t).await,
        TransportEvent::Connected { .. }
    ));
    assert!(frontend.open());
}

#[tokio::test(flavor = "multi_thread")]
async fn an_oversized_first_control_frame_closes_the_connection() {
    let f = fixture(&[PeerRole::Voter, PeerRole::Frontend]);
    let mut t = acceptor(&f, limits());
    // A first frame that is not `Hello`, from its header: a large class
    // and a negotiation kind that is not `Hello`, from a caller with no
    // certificate and from one with.
    for (identity, kind, length) in [
        (None, 0x0201u16, 8 * 1024 * 1024u32),
        (Some(&f.ids[1]), 0x0201u16, 8 * 1024 * 1024u32),
        (None, 0x0003u16, 1024u32),
    ] {
        let dialed = tls(&f, &t, identity).await;
        let (mut send, _recv) = dialed.conn.open_bi().await.unwrap();
        let mut header = length.to_be_bytes().to_vec();
        header.extend_from_slice(&kind.to_be_bytes());
        header.extend_from_slice(&1u16.to_be_bytes());
        send.write_all(&header).await.unwrap();
        let started = Instant::now();
        let (code, _) = dialed.closed(Duration::from_secs(3)).await;
        assert_eq!(code, PROTOCOL, "{kind:#06x}");
        assert!(started.elapsed() < Duration::from_secs(1), "{kind:#06x}");
        // Refused before negotiation, so reported as every refused
        // handshake is: one `Closed` with no lane.
        let event = timeout(Duration::from_secs(3), t.next_event())
            .await
            .expect("event within deadline")
            .expect("endpoint alive");
        assert!(
            matches!(event, TransportEvent::Closed { lane: None, .. }),
            "{event:?}"
        );
    }
    // After `Hello` only a `Close` may follow, and no longer than one.
    let mut dialed = anonymous(&f, &t).await;
    let mut header = 1024u32.to_be_bytes().to_vec();
    header.extend_from_slice(&0x0003u16.to_be_bytes());
    header.extend_from_slice(&1u16.to_be_bytes());
    dialed
        .control
        .as_mut()
        .unwrap()
        .write_all(&header)
        .await
        .unwrap();
    assert_eq!(dialed.closed(Duration::from_secs(3)).await.0, PROTOCOL);
    nothing_emitted(&mut t, Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn collector_traffic_makes_progress_under_an_anonymous_flood() {
    // The node a frontend submits to, and asks freshness of, is flooded
    // by anonymous callers: every unbound place held idle, more refused
    // at the cap, and header-only streams of the largest class on each
    // held place. The frontend's submission and its freshness ask still
    // arrive, promptly.
    let f = fixture(&[PeerRole::Voter, PeerRole::Frontend]);
    let mut t = acceptor(
        &f,
        Limits {
            max_unbound: 16,
            bind_deadline: Duration::from_secs(30),
            ..limits()
        },
    );
    CountingGate::install(&t);
    let mut frontend = tls(&f, &t, Some(&f.ids[1])).await;
    frontend.negotiate(PeerRole::Frontend, Lane::Unary).await;
    assert!(matches!(
        next_negotiated(&mut t).await,
        TransportEvent::Connected { .. }
    ));

    let mut flood = Vec::new();
    for _ in 0..16 {
        flood.push(anonymous(&f, &t).await);
    }
    for _ in 0..8 {
        let refused = tls(&f, &t, None).await;
        assert_eq!(refused.closed(Duration::from_secs(3)).await.0, REJECTED);
    }
    // A header on each held place's one stream: refused, and the place
    // with it; then hold the places again, idle.
    for d in &flood {
        let mut header = (8u32 * 1024 * 1024).to_be_bytes().to_vec();
        header.extend_from_slice(&0x0201u16.to_be_bytes());
        header.extend_from_slice(&1u16.to_be_bytes());
        let (mut send, _recv) = d.conn.open_bi().await.unwrap();
        send.write_all(&header).await.unwrap();
    }
    for d in &flood {
        assert_eq!(d.closed(Duration::from_secs(3)).await.0, PROTOCOL);
    }
    let mut held = Vec::new();
    for _ in 0..16 {
        held.push(anonymous(&f, &t).await);
    }

    let started = Instant::now();
    let submission = encode_frame(
        KIND_COLLECTOR_SUBMIT,
        COLLECTOR_SUBMIT_VERSION,
        &vec![7u8; 1024 * 1024],
    )
    .unwrap();
    frontend.send(&submission).await;
    let freshness = encode_frame(
        KIND_COLLECTOR_FRESHNESS,
        COLLECTOR_FRESHNESS_VERSION,
        b"ask",
    )
    .unwrap();
    frontend.send(&freshness).await;
    let mut kinds = Vec::new();
    while kinds.len() < 2 {
        match next_negotiated(&mut t).await {
            TransportEvent::ApiRequest { frame, .. } => kinds.push(frame.kind),
            other => panic!("{other:?}"),
        }
    }
    kinds.sort_unstable();
    let mut expected = vec![KIND_COLLECTOR_SUBMIT, KIND_COLLECTOR_FRESHNESS];
    expected.sort_unstable();
    assert_eq!(kinds, expected);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "collector traffic waited {:?} under the flood",
        started.elapsed()
    );
    assert!(frontend.open());
    assert!(held.iter().all(|d| d.open()));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bind_the_runtime_lets_go_of_unacknowledged_is_refused_and_its_place_freed() {
    let f = fixture(&[PeerRole::Voter]);
    let mut t = acceptor(
        &f,
        Limits {
            max_unbound: 1,
            bind_deadline: Duration::from_millis(800),
            ..limits()
        },
    );
    CountingGate::install(&t);

    // The runtime answers a first bind with something other than an
    // acknowledgement: the connection does not bind, and is closed once
    // the answer is written.
    let answered = anonymous(&f, &t).await;
    let mut recv = answered.send(&bind_frame(GOOD)).await;
    assert!(matches!(
        next_negotiated(&mut t).await,
        TransportEvent::Connected { .. }
    ));
    let responder = match next_negotiated(&mut t).await {
        TransportEvent::ApiRequest { responder, .. } => responder,
        other => panic!("{other:?}"),
    };
    responder
        .respond(encode_frame(KIND_SESSION_BIND, SESSION_BIND_VERSION, b"not an ack").unwrap())
        .await
        .unwrap();
    let answer = recv.read_to_end(64 * 1024).await.unwrap();
    assert_eq!(&answer[4..6], &KIND_SESSION_BIND.to_be_bytes());
    assert_eq!(
        answered.closed(Duration::from_secs(3)).await,
        (REJECTED, BIND_REFUSED.as_bytes().to_vec())
    );
    assert!(matches!(
        next_negotiated(&mut t).await,
        TransportEvent::Closed { .. }
    ));

    // The runtime drops a first bind's stream and closes nothing: the
    // connection is closed after the grace, and its place is free.
    let dropped = anonymous(&f, &t).await;
    dropped.send(&bind_frame(GOOD)).await;
    assert!(matches!(
        next_negotiated(&mut t).await,
        TransportEvent::Connected { .. }
    ));
    match next_negotiated(&mut t).await {
        TransportEvent::ApiRequest { responder, .. } => drop(responder),
        other => panic!("{other:?}"),
    }
    assert!(dropped.open(), "the runtime is given its grace to close");
    assert_eq!(
        dropped.closed(Duration::from_secs(3)).await,
        (REJECTED, BIND_REFUSED.as_bytes().to_vec())
    );
    assert!(matches!(
        next_negotiated(&mut t).await,
        TransportEvent::Closed { .. }
    ));
    let next = anonymous(&f, &t).await;
    next.send(&bind_frame(GOOD)).await;
    assert!(matches!(
        next_negotiated(&mut t).await,
        TransportEvent::Connected { .. }
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_acknowledgement_not_written_whole_closes_the_connection() {
    let f = fixture(&[PeerRole::Voter]);
    let mut t = acceptor(&f, limits());
    CountingGate::install(&t);

    // The caller stops the bind's answer stream before the runtime
    // acknowledges the bind. The acknowledgement is not written, and the
    // connection, raised to its lane for it, is closed rather than left
    // open at the lane's limits without one (#334 review).
    let dialed = anonymous(&f, &t).await;
    let mut recv = dialed.send(&bind_frame(GOOD)).await;
    recv.stop(quinn::VarInt::from_u32(0)).unwrap();
    assert!(matches!(
        next_negotiated(&mut t).await,
        TransportEvent::Connected { .. }
    ));
    let responder = match next_negotiated(&mut t).await {
        TransportEvent::ApiRequest { responder, .. } => responder,
        other => panic!("{other:?}"),
    };
    // The stop reaches the acceptor before the answer is written.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        responder
            .respond(encode_frame(KIND_SESSION_BIND_ACK, SESSION_BIND_VERSION, b"ack").unwrap())
            .await
            .is_err(),
        "the acknowledgement was written to a stopped stream"
    );
    assert_eq!(
        dialed.closed(Duration::from_secs(3)).await,
        (REJECTED, BIND_UNAVAILABLE.as_bytes().to_vec())
    );
    assert!(matches!(
        next_negotiated(&mut t).await,
        TransportEvent::Closed { .. }
    ));
}

/// A node budget that holds one 2 MiB reply and nothing beside it, and a
/// frontend reply that holds it: written to a caller that never reads it,
/// the reply stops at the caller's stream window with its budget admitted,
/// and its future, left unpolled, never times out. A `BindAck` admitted
/// after it waits for room (#334 review).
async fn hold_the_node_budget(
    f: &Fixture,
    t: &mut Transport,
) -> (
    Dialed,
    quinn::RecvStream,
    std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), coord_transport::SendError>> + Send>,
    >,
) {
    const HELD: usize = 2 * 1024 * 1024;
    let mut frontend = tls(f, t, Some(&f.ids[1])).await;
    frontend.negotiate(PeerRole::Frontend, Lane::Unary).await;
    assert!(matches!(
        next_negotiated(t).await,
        TransportEvent::Connected { .. }
    ));
    let freshness = encode_frame(
        KIND_COLLECTOR_FRESHNESS,
        COLLECTOR_FRESHNESS_VERSION,
        b"ask",
    )
    .unwrap();
    let unread = frontend.send(&freshness).await;
    let responder = match next_negotiated(t).await {
        TransportEvent::ApiRequest { responder, .. } => responder,
        other => panic!("{other:?}"),
    };
    let mut reply = Box::pin(responder.respond(vec![0u8; HELD]));
    tokio::select! {
        r = &mut reply => panic!("the reply was written whole: {r:?}"),
        () = tokio::time::sleep(Duration::from_millis(300)) => {}
    }
    assert!(t.node_budget().0 >= HELD, "{:?}", t.node_budget());
    (frontend, unread, reply)
}

fn held_budget_limits() -> Limits {
    let mut l = limits();
    l.budget.node_bytes = 2 * 1024 * 1024 + 8;
    l.budget.control_reserve = 0;
    l
}

#[tokio::test(flavor = "multi_thread")]
async fn an_acknowledgement_waiting_for_room_ends_within_its_deadline_and_closes_the_connection() {
    let f = fixture(&[PeerRole::Voter, PeerRole::Frontend]);
    let mut t = acceptor(&f, held_budget_limits());
    CountingGate::install(&t);
    let (_frontend, _unread, _reply) = hold_the_node_budget(&f, &mut t).await;

    // The bind's acknowledgement finds no room. One deadline covers its
    // admission and its write, so it ends within the frame deadline, and
    // the connection, never raised, is closed as unavailable (#334 review).
    let dialed = anonymous(&f, &t).await;
    let _recv = dialed.send(&bind_frame(GOOD)).await;
    assert!(matches!(
        next_negotiated(&mut t).await,
        TransportEvent::Connected { .. }
    ));
    let responder = match next_negotiated(&mut t).await {
        TransportEvent::ApiRequest { responder, .. } => responder,
        other => panic!("{other:?}"),
    };
    let started = Instant::now();
    let answered = timeout(
        Duration::from_secs(10),
        responder
            .respond(encode_frame(KIND_SESSION_BIND_ACK, SESSION_BIND_VERSION, b"ack").unwrap()),
    )
    .await
    .expect("the acknowledgement ended within its deadline");
    assert!(
        matches!(answered, Err(coord_transport::SendError::Timeout)),
        "{answered:?}"
    );
    assert!(
        started.elapsed() < limits().frame_timeout + Duration::from_secs(1),
        "the acknowledgement waited {:?}",
        started.elapsed()
    );
    assert_eq!(
        dialed.closed(Duration::from_secs(3)).await,
        (REJECTED, BIND_UNAVAILABLE.as_bytes().to_vec())
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_acknowledgement_dropped_while_it_waits_closes_the_connection() {
    let f = fixture(&[PeerRole::Voter, PeerRole::Frontend]);
    let mut t = acceptor(&f, held_budget_limits());
    CountingGate::install(&t);
    let (_frontend, _unread, _reply) = hold_the_node_budget(&f, &mut t).await;

    // The runtime gives up on the acknowledgement while it waits for room:
    // its future is dropped, and the connection is closed as unavailable,
    // as a returned error closes it (#334 review).
    let dialed = anonymous(&f, &t).await;
    let _recv = dialed.send(&bind_frame(GOOD)).await;
    assert!(matches!(
        next_negotiated(&mut t).await,
        TransportEvent::Connected { .. }
    ));
    let responder = match next_negotiated(&mut t).await {
        TransportEvent::ApiRequest { responder, .. } => responder,
        other => panic!("{other:?}"),
    };
    assert!(
        timeout(
            Duration::from_millis(300),
            responder.respond(
                encode_frame(KIND_SESSION_BIND_ACK, SESSION_BIND_VERSION, b"ack").unwrap()
            ),
        )
        .await
        .is_err(),
        "the acknowledgement found room"
    );
    assert_eq!(
        dialed.closed(Duration::from_secs(3)).await,
        (REJECTED, BIND_UNAVAILABLE.as_bytes().to_vec())
    );
}

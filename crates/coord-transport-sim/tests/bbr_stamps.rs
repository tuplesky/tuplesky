//! BBR's window on a 2 ms path when the sender's receipt stamps run late
//! (#98: the Control window from n1 to n4 that collapsed at 480).
//!
//! Two quinn-proto endpoints over one link, in virtual time counted in
//! microseconds: a constant delay each way on a link far faster than the
//! load. The sender writes bursts of frames, each on a stream of its own,
//! and its connection driver sends at most quinn's budget of datagrams a
//! poll, polling again a little later, as on a host with no idle core.
//! The sender's endpoint can stall, as quinn's endpoint driver does when
//! its thread is descheduled after it read the clock for a receive pass:
//! datagrams that arrive during the stall are handled when it ends,
//! stamped with the time it began. The sender's connection keeps sending
//! at the true time meanwhile, as quinn's connection driver does on
//! another thread. Each run uses the Control lane's transport
//! configuration, with the controller under test, and is deterministic.
//!
//! The runs show the two conditions behind the collapse, and the guard
//! ([`coord_transport::congestion`]) removing the first:
//! - a short least round trip alone leaves the window alone while BBR is
//!   in Startup, where it only grows;
//! - bursts that outrun a poll take BBR out of Startup, and without
//!   stalls it keeps a window that carries them;
//! - with both, quinn's BBR sizes its window from the short least, below
//!   what the bursts need in flight, and they back up;
//! - behind the guard, the same run keeps the window it has without
//!   stalls, while quinn's own least still reads short.

use std::collections::VecDeque;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::BytesMut;
use coord_transport::congestion::{Clock, GuardedBbrConfig};
use coord_transport::{LaneLimits, Limits};
use coord_transport_testkit::TestCa;
use coord_types::ids::{ReplicaId, ReplicaIncarnation};
use coord_types::wire_v1::PeerRole;
use quinn_proto::congestion::{BbrConfig, ControllerFactory};
use quinn_proto::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn_proto::{
    ClientConfig, Connection, ConnectionHandle, DatagramEvent, Dir, Endpoint, EndpointConfig,
    Event, ServerConfig, StreamEvent, StreamId,
};

/// One-way delay: a 2 ms round trip, the same-region path of #98.
const DELAY_US: u64 = 1_000;
/// The link's rate, in bytes a millisecond: 125 MB/s, far more than the
/// load, as on #98's emulated regions, which delay but do not limit.
const RATE_PER_MS: u64 = 125_000;
/// quinn's connection driver sends at most this many datagrams a poll,
/// then polls again (quinn's `MAX_TRANSMIT_DATAGRAMS`), here after this
/// many microseconds, as on a host with no idle core. What the last
/// `poll_transmit` of a poll left in quinn's `app_limited` is what BBR
/// reads at the next ACK: a poll cut short by the budget leaves it false,
/// so a sender whose bursts outrun one poll is not limited by the
/// application whenever an ACK lands between its polls.
const POLL_BUDGET: usize = 20;
const REPOLL_US: u64 = 100;
/// How long a run lasts, and the last stretch it is judged over.
const RUN_US: u64 = 3_000_000;
const JUDGED_US: u64 = 1_000_000;

/// The sender's load: a burst of this many frames of [`FRAME`] bytes, as a
/// leader's turn sends a follower, every 0.8 to 1.2 ms (a millisecond on
/// average, the gap moving from burst to burst as turns do). Each frame
/// is on a unidirectional stream of its own as the peer plane sends
/// evidence, so the connection's receive window bounds what is in flight
/// rather than one stream's.
#[derive(Clone, Copy, Debug)]
struct Load(u64);

impl Load {
    /// The gap after the `k`th burst.
    fn gap_us(k: u64) -> u64 {
        800 + k * 389 % 400
    }
}

const FRAME: usize = 4 * 1024;
/// One frame a burst: four datagrams, never a poll's budget.
const LIGHT: Load = Load(1);
/// Six a burst: 24 KB, 21 datagrams, a poll's budget and more.
const BURSTS: Load = Load(6);
/// About what [`BURSTS`] needs in flight on the path: its
/// bandwidth-delay product.
const BURSTS_BDP: u64 = 6 * FRAME as u64 * 2 * DELAY_US / 1_000;

/// The sender's endpoint stalls once every `every_us`, starting at
/// `from_us`, at a phase within the first millisecond of each period and
/// for 1.40 to 1.98 ms, both moving from period to period: short of the
/// path's round trip, so a sample can read from a few hundredths of a
/// millisecond to most of it, as #98's census read 0.01 to 0.6 ms on the
/// 2 ms path.
#[derive(Clone, Copy, Debug)]
struct Stalls {
    from_us: u64,
    every_us: u64,
}

impl Stalls {
    /// The `period`th stall: when it begins and how long it lasts.
    fn nth(&self, period: u64) -> (u64, u64) {
        let began = self.from_us + period * self.every_us + period * 397 % 1_000;
        (began, 1_400 + period * 613 % 580)
    }
}

const STALLS: Stalls = Stalls {
    from_us: 500_000,
    every_us: 25_000,
};

/// What a run measured over its judged stretch.
#[derive(Debug)]
struct Outcome {
    /// Bytes the sender was asked to send, and the receiver read, a
    /// millisecond.
    offered_per_ms: u64,
    delivered_per_ms: u64,
    /// The most bytes asked for and not yet read, at a millisecond.
    most_waiting: u64,
    /// Datagrams the sender handled late, stamped when a stall began.
    late_datagrams: u64,
    /// The smallest and the last congestion window the sender had.
    least_window: u64,
    last_window: u64,
    /// quinn's least round trip at the end, raw samples.
    quinn_min_rtt: Duration,
    /// The smoothed round trip at the end.
    rtt: Duration,
}

struct Side {
    endpoint: Endpoint,
    conn: Option<(ConnectionHandle, Connection)>,
    /// Datagrams to handle: when, with what stamp, from where.
    inbound: VecDeque<(u64, u64, SocketAddr, BytesMut)>,
}

struct Run {
    base: Instant,
    now: u64,
    clock: Arc<AtomicU64>,
    sides: [Side; 2],
    addrs: [SocketAddr; 2],
    /// When the bottleneck is next free, per direction.
    free_at: [u64; 2],
    stalls: Option<Stalls>,
    load: Load,
    connected: bool,
    /// Frames asked for so far, and when the next burst is asked.
    due: u64,
    bursts: u64,
    next_burst: u64,
    frames: u64,
    /// The frame being written: its stream and how much of it was taken.
    writing: Option<(StreamId, usize)>,
    accepted: Vec<StreamId>,
    late: u64,
    /// Whether a side's last poll ran out of its budget.
    more: [bool; 2],
    written: u64,
    read: u64,
}

fn addr(i: u8) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, i)), 4433)
}

impl Run {
    fn new(
        congestion: impl FnOnce(Clock) -> Arc<dyn ControllerFactory + Send + Sync>,
        load: Load,
        stalls: Option<Stalls>,
    ) -> Self {
        let base = Instant::now();
        let clock = Arc::new(AtomicU64::new(0));
        let reader: Clock = {
            let clock = clock.clone();
            Arc::new(move || base + Duration::from_micros(clock.load(Ordering::Relaxed)))
        };
        let limits = Limits::node();
        let transport = coord_transport::lane::transport_config_with(
            &LaneLimits::CONTROL,
            &limits,
            congestion(reader),
        )
        .expect("transport config");

        let ca = TestCa::new();
        let server_id = ca.issue(
            "receiver",
            ReplicaId([2; 16]),
            ReplicaIncarnation::new(1).expect("incarnation"),
            PeerRole::Voter,
        );
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let mut server = rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .expect("tls13")
            .with_no_client_auth()
            .with_single_cert(server_id.chain.clone(), server_id.key.clone_key())
            .expect("server cert");
        server.alpn_protocols = vec![b"bbr".to_vec()];
        server.max_early_data_size = 0;
        let mut server_config = ServerConfig::with_crypto(Arc::new(
            QuicServerConfig::try_from(server).expect("quic server"),
        ));
        server_config.transport_config(transport.clone());
        let mut client = rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .expect("tls13")
            .with_root_certificates(ca.roots())
            .with_no_client_auth();
        client.alpn_protocols = vec![b"bbr".to_vec()];
        let mut client_config = ClientConfig::new(Arc::new(
            QuicClientConfig::try_from(client).expect("quic client"),
        ));
        client_config.transport_config(transport);

        let endpoint = |seed: u8, server: Option<Arc<ServerConfig>>| {
            let mut config = EndpointConfig::default();
            config.rng_seed(Some([seed; 32]));
            Endpoint::new(Arc::new(config), server, false, None)
        };
        let mut sender = endpoint(1, None);
        let receiver = endpoint(2, Some(Arc::new(server_config)));
        let addrs = [addr(1), addr(2)];
        let conn = sender
            .connect(base, client_config, addrs[1], "receiver")
            .expect("connect");
        Run {
            base,
            now: 0,
            clock,
            sides: [
                Side {
                    endpoint: sender,
                    conn: Some(conn),
                    inbound: VecDeque::new(),
                },
                Side {
                    endpoint: receiver,
                    conn: None,
                    inbound: VecDeque::new(),
                },
            ],
            addrs,
            free_at: [0, 0],
            stalls,
            load,
            connected: false,
            due: 0,
            bursts: 0,
            next_burst: 0,
            frames: 0,
            writing: None,
            accepted: Vec::new(),
            late: 0,
            more: [false, false],
            written: 0,
            read: 0,
        }
    }

    fn at(&self, us: u64) -> Instant {
        self.base + Duration::from_micros(us)
    }

    /// When and with what stamp side `to` handles a datagram that arrives
    /// at `arrival`.
    fn handling(&self, to: usize, arrival: u64) -> (u64, u64) {
        match self.stalls {
            Some(s) if to == 0 && arrival >= s.from_us => {
                let (began, length) = s.nth((arrival - s.from_us) / s.every_us);
                if (began..began + length).contains(&arrival) {
                    (began + length, began)
                } else {
                    (arrival, arrival)
                }
            }
            _ => (arrival, arrival),
        }
    }

    fn send(&mut self, from: usize, data: &[u8], segment: Option<usize>) {
        let to = 1 - from;
        let size = segment.unwrap_or(data.len());
        for chunk in data.chunks(size) {
            let start = self.free_at[from].max(self.now);
            let done = start + (chunk.len() as u64 * 1_000).div_ceil(RATE_PER_MS);
            self.free_at[from] = done;
            let (handled, stamp) = self.handling(to, done + DELAY_US);
            self.sides[to].inbound.push_back((
                handled,
                stamp,
                self.addrs[from],
                BytesMut::from(chunk),
            ));
        }
    }

    fn handle_inbound(&mut self, i: usize) {
        let mut response = Vec::new();
        while self.sides[i]
            .inbound
            .front()
            .is_some_and(|d| d.0 <= self.now)
        {
            let (handled, stamp, remote, data) = self.sides[i].inbound.pop_front().unwrap();
            self.late += u64::from(handled != stamp);
            let stamp = self.at(stamp);
            let side = &mut self.sides[i];
            match side
                .endpoint
                .handle(stamp, remote, None, None, data, &mut response)
            {
                Some(DatagramEvent::NewConnection(incoming)) => {
                    let (handle, conn) = side
                        .endpoint
                        .accept(incoming, stamp, &mut response, None)
                        .expect("accept");
                    side.conn = Some((handle, conn));
                }
                Some(DatagramEvent::ConnectionEvent(handle, event)) => {
                    if let Some((h, conn)) = side.conn.as_mut()
                        && *h == handle
                    {
                        conn.handle_event(event);
                    }
                }
                Some(DatagramEvent::Response(transmit)) => {
                    let bytes = response[..transmit.size].to_vec();
                    self.send(i, &bytes, transmit.segment_size);
                }
                None => {}
            }
            response.clear();
        }
    }

    fn drive(&mut self, i: usize) {
        let now = self.at(self.now);
        let Some((handle, conn)) = self.sides[i].conn.as_mut() else {
            return;
        };
        if conn.poll_timeout().is_some_and(|t| t <= now) {
            conn.handle_timeout(now);
        }
        while let Some(event) = conn.poll_endpoint_events() {
            if let Some(event) = self.sides[i].endpoint.handle_event(*handle, event) {
                conn.handle_event(event);
            }
        }
        while let Some(event) = conn.poll() {
            match event {
                Event::Connected if i == 0 => self.connected = true,
                Event::Stream(StreamEvent::Opened { dir: Dir::Uni }) => {
                    while let Some(id) = conn.streams().accept(Dir::Uni) {
                        self.accepted.push(id);
                    }
                }
                Event::ConnectionLost { reason } => panic!("connection lost: {reason}"),
                _ => {}
            }
        }
        if i == 0 && self.connected {
            while self.next_burst <= self.now {
                self.due += self.load.0;
                self.next_burst += Load::gap_us(self.bursts);
                self.bursts += 1;
            }
            let due = self.due;
            let frame = [7u8; FRAME];
            loop {
                let (id, done) = match self.writing {
                    Some(writing) => writing,
                    None if self.frames < due => match conn.streams().open(Dir::Uni) {
                        Some(id) => (id, 0),
                        None => break,
                    },
                    None => break,
                };
                let mut stream = conn.send_stream(id);
                let n = stream.write(&frame[done..]).unwrap_or(0);
                if done + n < FRAME {
                    self.writing = Some((id, done + n));
                    break;
                }
                stream.finish().expect("finish");
                self.writing = None;
                self.frames += 1;
                self.written += FRAME as u64;
            }
        } else {
            // Read every stream to the end of what has arrived, and forget
            // the ones that ended.
            let mut open = Vec::with_capacity(self.accepted.len());
            for &id in &self.accepted {
                let mut stream = conn.recv_stream(id);
                let Ok(mut chunks) = stream.read(true) else {
                    continue;
                };
                let mut ended = false;
                loop {
                    match chunks.next(usize::MAX) {
                        Ok(Some(chunk)) => self.read += chunk.bytes.len() as u64,
                        Ok(None) => {
                            ended = true;
                            break;
                        }
                        Err(_) => break,
                    }
                }
                let _ = chunks.finalize();
                if !ended {
                    open.push(id);
                }
            }
            self.accepted = open;
        }
        let mut buf = Vec::with_capacity(64 * 1024);
        let mut out = Vec::new();
        let mut datagrams = 0;
        while datagrams < POLL_BUDGET {
            let Some(transmit) = conn.poll_transmit(now, 10, &mut buf) else {
                break;
            };
            datagrams += transmit
                .size
                .div_ceil(transmit.segment_size.unwrap_or(transmit.size));
            out.push((buf[..transmit.size].to_vec(), transmit.segment_size));
            buf.clear();
        }
        self.more[i] = datagrams >= POLL_BUDGET;
        for (bytes, segment) in out {
            self.send(i, &bytes, segment);
        }
    }

    fn next_event(&mut self) -> u64 {
        let (now, base) = (self.now, self.base);
        let mut next = (now + 1_000 - now % 1_000).min(self.next_burst.max(now + 1));
        if self.more.contains(&true) {
            next = next.min(now + REPOLL_US);
        }
        for side in &mut self.sides {
            if let Some(d) = side.inbound.front() {
                next = next.min(d.0);
            }
            if let Some(t) = side.conn.as_mut().and_then(|(_, c)| c.poll_timeout()) {
                let t = t.saturating_duration_since(base).as_micros() as u64;
                next = next.min(t.max(now + 1));
            }
        }
        next
    }

    fn run(mut self) -> Outcome {
        let judged_from = RUN_US - JUDGED_US;
        let mut read_at_judged = 0;
        let mut due_at_judged = 0;
        let mut least_window = u64::MAX;
        let mut most_waiting = 0;
        while self.now < RUN_US {
            self.clock.store(self.now, Ordering::Relaxed);
            for i in 0..2 {
                self.handle_inbound(i);
                self.drive(i);
            }
            if self.now.is_multiple_of(1_000) && self.now >= judged_from {
                if self.now == judged_from {
                    read_at_judged = self.read;
                    due_at_judged = self.due;
                }
                let (_, conn) = self.sides[0].conn.as_ref().expect("sender");
                least_window = least_window.min(conn.stats().path.cwnd);
                most_waiting = most_waiting.max(self.due * FRAME as u64 - self.read);
            }
            self.now = self.next_event();
        }
        let (_, conn) = self.sides[0].conn.as_ref().expect("sender");
        let path = conn.stats().path;
        Outcome {
            most_waiting,
            late_datagrams: self.late,
            offered_per_ms: (self.due - due_at_judged) * FRAME as u64 / (JUDGED_US / 1_000),
            delivered_per_ms: (self.read - read_at_judged) / (JUDGED_US / 1_000),
            least_window,
            last_window: path.cwnd,
            quinn_min_rtt: path.min_rtt,
            rtt: path.rtt,
        }
    }
}

fn quinn_bbr(_clock: Clock) -> Arc<dyn ControllerFactory + Send + Sync> {
    Arc::new(BbrConfig::default())
}

fn guarded_bbr(clock: Clock) -> Arc<dyn ControllerFactory + Send + Sync> {
    Arc::new(GuardedBbrConfig::with_clock(clock))
}

const PATH_RTT: Duration = Duration::from_micros(2 * DELAY_US);

/// Condition one alone: stalls make quinn's least short, but a sender
/// whose sends never outrun one poll is limited by the application at
/// every ACK, so BBR stays in Startup and the window stays large (K6's
/// first run: a least of 0.28 ms, a window of 27 MB).
#[test]
fn a_short_least_alone_leaves_the_window_of_a_sender_in_startup() {
    let out = Run::new(quinn_bbr, LIGHT, Some(STALLS)).run();
    eprintln!("{out:?}");
    assert!(out.late_datagrams > 0, "{out:?}");
    assert!(
        out.quinn_min_rtt < PATH_RTT / 3,
        "the least reads short: {out:?}"
    );
    assert!(
        out.least_window >= 1 << 20,
        "and the window stays large: {out:?}"
    );
    assert!(
        out.delivered_per_ms >= out.offered_per_ms * 99 / 100,
        "the load is carried: {out:?}"
    );
}

/// Condition two alone: a sender whose bursts outrun a poll leaves
/// Startup, and with on-time stamps keeps a window that carries them: no
/// more than two bursts ever wait.
#[test]
fn bursts_without_late_stamps_leave_startup_and_are_carried() {
    let out = Run::new(quinn_bbr, BURSTS, None).run();
    eprintln!("{out:?}");
    assert!(out.quinn_min_rtt >= PATH_RTT, "{out:?}");
    assert!(out.last_window < 1 << 20, "out of Startup: {out:?}");
    assert!(out.least_window >= 4 * BURSTS_BDP, "{out:?}");
    assert!(out.most_waiting <= BURSTS_BDP, "{out:?}");
    assert!(out.rtt < PATH_RTT * 3 / 2, "no queue: {out:?}");
}

/// Both: quinn's BBR, out of Startup with a short least, sizes its window
/// from it, below what the bursts need in flight, and they back up (K6's
/// second run: 9.6 MB to 14 KB, and the follower fell behind). How far
/// the window falls depends on the bandwidth BBR has measured; here it
/// falls to a sixth of the window without late stamps.
#[test]
fn quinn_bbr_out_of_startup_with_a_short_least_collapses_its_window() {
    let out = Run::new(quinn_bbr, BURSTS, Some(STALLS)).run();
    eprintln!("{out:?}");
    assert!(out.quinn_min_rtt < PATH_RTT / 3, "{out:?}");
    assert!(
        out.least_window < BURSTS_BDP,
        "the window collapses: {out:?}"
    );
    assert!(
        out.most_waiting > 4 * BURSTS_BDP,
        "and the bursts back up: {out:?}"
    );
}

/// Both, behind the guard: quinn's least still reads short, but BBR is
/// never handed it, and the window carries the bursts as it does without
/// stalls.
#[test]
fn the_guard_keeps_the_window_of_bursts_through_late_stamps() {
    let out = Run::new(guarded_bbr, BURSTS, Some(STALLS)).run();
    eprintln!("{out:?}");
    assert!(
        out.quinn_min_rtt < PATH_RTT / 3,
        "quinn's least reads short: {out:?}"
    );
    assert!(out.least_window >= 4 * BURSTS_BDP, "{out:?}");
    assert!(out.most_waiting <= BURSTS_BDP, "{out:?}");
    assert!(out.rtt < PATH_RTT * 3 / 2, "no queue: {out:?}");
}

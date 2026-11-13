#![allow(missing_docs)]
//! SCRATCH, TEST-ONLY, MUST NEVER MERGE: the transport's switches for the
//! tokio round (#185, lead 08:00 (3)). Each bounds what a stream a buffer,
//! or no stream at all, would save; none of them is correct to ship.
//!
//! - `COORDD_X_APIB`: the API plane carries several frames a stream, as the
//!   peer plane does (one stream a buffer, not one a frame), on submissions
//!   to the leader and deliveries back to collectors.
//! - `COORDD_X_PSTR`: the peer plane keeps one stream a lane open for the
//!   connection's life, with no open, finish or acknowledgement task a buffer.
//! - `COORDD_X_DGRAM`: the peer plane sends every frame that fits as a QUIC
//!   datagram (unreliable), packing a buffer's frames into as few as fit.
//! - `COORDD_X_CSTR`: a client keeps one request stream for its
//!   connection's life, and the frontend answers on it without finishing.
//! - `COORDD_X_AES128` (structural floors, lead 10:43 (4)): the TLS 1.3
//!   suites offered and accepted are TLS_AES_128_GCM_SHA256 alone, not
//!   rustls's default order, which puts AES-256-GCM first.
//! - `COORDD_X_QSPLIT` (10:43 (1), (3)): every ten seconds, each
//!   endpoint prints QUIC's own counts summed by connection kind (peer,
//!   api to a voter, api to a client): datagrams, send calls, bytes, and
//!   the frames sent by type, so ACK-only packets can be bounded.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

fn on(name: &str, cell: &OnceLock<bool>) -> bool {
    *cell.get_or_init(|| {
        let on = std::env::var(name).is_ok_and(|v| !v.is_empty() && v != "0");
        if on {
            eprintln!("SCRATCH switch {name} is on");
        }
        on
    })
}

/// `COORDD_X_APIB`.
pub fn apib() -> bool {
    static C: OnceLock<bool> = OnceLock::new();
    on("COORDD_X_APIB", &C)
}

/// `COORDD_X_PSTR`.
pub fn pstr() -> bool {
    static C: OnceLock<bool> = OnceLock::new();
    on("COORDD_X_PSTR", &C)
}

/// `COORDD_X_DGRAM`.
pub fn dgram() -> bool {
    static C: OnceLock<bool> = OnceLock::new();
    on("COORDD_X_DGRAM", &C)
}

/// `COORDD_X_CSTR`.
pub fn cstr() -> bool {
    static C: OnceLock<bool> = OnceLock::new();
    on("COORDD_X_CSTR", &C)
}

/// `COORDD_X_AES128`.
pub fn aes128() -> bool {
    static C: OnceLock<bool> = OnceLock::new();
    on("COORDD_X_AES128", &C)
}

/// `COORDD_X_QSPLIT`.
pub fn qsplit() -> bool {
    static C: OnceLock<bool> = OnceLock::new();
    on("COORDD_X_QSPLIT", &C)
}

/// One kind's QUIC counts, summed over its live connections.
#[derive(Default)]
pub struct QuicSum {
    pub conns: u64,
    pub tx_datagrams: u64,
    pub tx_ios: u64,
    pub tx_bytes: u64,
    pub rx_datagrams: u64,
    pub rx_ios: u64,
    pub tx_acks: u64,
    pub tx_stream: u64,
    pub tx_datagram_frames: u64,
    pub tx_ping: u64,
    pub tx_flow: u64,
    pub tx_crypto: u64,
    pub tx_other: u64,
    pub rx_acks: u64,
    pub sent_packets: u64,
    pub lost_packets: u64,
}

impl QuicSum {
    pub fn add(&mut self, s: &quinn::ConnectionStats) {
        let f = &s.frame_tx;
        self.conns += 1;
        self.tx_datagrams += s.udp_tx.datagrams;
        self.tx_ios += s.udp_tx.ios;
        self.tx_bytes += s.udp_tx.bytes;
        self.rx_datagrams += s.udp_rx.datagrams;
        self.rx_ios += s.udp_rx.ios;
        self.tx_acks += f.acks;
        self.tx_stream += f.stream;
        self.tx_datagram_frames += f.datagram;
        self.tx_ping += f.ping;
        self.tx_flow += f.max_data
            + f.max_stream_data
            + f.max_streams_bidi
            + f.max_streams_uni
            + f.data_blocked
            + f.stream_data_blocked
            + f.streams_blocked_bidi
            + f.streams_blocked_uni;
        self.tx_crypto += f.crypto;
        self.tx_other += f.reset_stream
            + f.stop_sending
            + f.new_connection_id
            + f.retire_connection_id
            + f.path_challenge
            + f.path_response
            + f.new_token
            + u64::from(f.handshake_done)
            + f.immediate_ack
            + f.ack_frequency
            + f.connection_close;
        self.rx_acks += s.frame_rx.acks;
        self.sent_packets += s.path.sent_packets;
        self.lost_packets += s.path.lost_packets;
    }

    pub fn line(&self, kind: &str) -> String {
        format!(
            " {kind}: conns={} tx_dgrams={} tx_ios={} tx_bytes={} rx_dgrams={} rx_ios={} \
             tx_acks={} tx_stream={} tx_dgram_frames={} tx_ping={} tx_flow={} tx_crypto={} \
             tx_other={} rx_acks={} sent_packets={} lost_packets={}",
            self.conns,
            self.tx_datagrams,
            self.tx_ios,
            self.tx_bytes,
            self.rx_datagrams,
            self.rx_ios,
            self.tx_acks,
            self.tx_stream,
            self.tx_datagram_frames,
            self.tx_ping,
            self.tx_flow,
            self.tx_crypto,
            self.tx_other,
            self.rx_acks,
            self.sent_packets,
            self.lost_packets
        )
    }
}

/// Datagram counts: frames and datagrams sent, frames too large for one,
/// datagrams refused by QUIC, and frames and datagrams received.
pub static DG_SENT_FRAMES: AtomicU64 = AtomicU64::new(0);
pub static DG_SENT: AtomicU64 = AtomicU64::new(0);
pub static DG_TOO_LARGE: AtomicU64 = AtomicU64::new(0);
pub static DG_REFUSED: AtomicU64 = AtomicU64::new(0);
pub static DG_RECV_FRAMES: AtomicU64 = AtomicU64::new(0);
pub static DG_RECV: AtomicU64 = AtomicU64::new(0);

pub fn add(c: &AtomicU64, n: u64) {
    c.fetch_add(n, Ordering::Relaxed);
}

/// The counts, for a log line.
pub fn line() -> String {
    let g = |c: &AtomicU64| c.load(Ordering::Relaxed);
    format!(
        "x-dgram sent_frames={} sent={} too_large={} refused={} recv_frames={} recv={}",
        g(&DG_SENT_FRAMES),
        g(&DG_SENT),
        g(&DG_TOO_LARGE),
        g(&DG_REFUSED),
        g(&DG_RECV_FRAMES),
        g(&DG_RECV)
    )
}

/// Print [`line`] every ten seconds, once a process.
pub fn start_printer() {
    static STARTED: OnceLock<()> = OnceLock::new();
    if STARTED.set(()).is_ok() {
        tokio::spawn(async {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                eprintln!("{}", line());
            }
        });
    }
}

/// A client's long-lived request stream (CSTR), by endpoint and connection.
pub type LongStream =
    std::sync::Arc<tokio::sync::Mutex<Option<(quinn::SendStream, crate::frames::FrameStream)>>>;

pub fn long_stream(endpoint: usize, connection: u64) -> LongStream {
    use std::collections::HashMap;
    use std::sync::Mutex;
    static MAP: OnceLock<Mutex<HashMap<(usize, u64), LongStream>>> = OnceLock::new();
    MAP.get_or_init(Default::default)
        .lock()
        .unwrap()
        .entry((endpoint, connection))
        .or_default()
        .clone()
}

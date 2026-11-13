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

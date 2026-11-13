//! SCRATCH (lead 09:13 (2) and 10:43): the cheapest thing that moves the
//! bytes, for the transport floor. One current-thread tokio runtime, as a
//! voter runs, moves messages of the sizes the peer and API planes send
//! through a Unix datagram socket pair and through UDP on loopback: a send
//! and its receive, `batch` sends before their receives. No QUIC: no packet
//! protection, no connection or stream state, no acknowledgements. The
//! thread's CPU over the loop, a message, is the floor of tokio plus the
//! kernel for one message, against the QUIC path's measured figure.
//!
//! cargo test --release -p coord-transport --test xuds -- --ignored --nocapture

use std::time::{Duration, Instant};

use tokio::net::{UdpSocket, UnixDatagram};

/// This thread's CPU seconds (utime + stime, in clock ticks of 10 ms).
fn cpu_now() -> f64 {
    let s = std::fs::read_to_string("/proc/thread-self/stat").expect("stat");
    let after = &s[s.rfind(')').expect("comm") + 2..];
    let f: Vec<&str> = after.split(' ').collect();
    let utime: f64 = f[11].parse().expect("utime");
    let stime: f64 = f[12].parse().expect("stime");
    (utime + stime) / 100.0
}

const SIZES: [usize; 5] = [180, 450, 1029, 2200, 4300];
const BATCHES: [usize; 2] = [1, 8];
const SPAN: Duration = Duration::from_millis(2500);

async fn uds(size: usize, batch: usize) -> (f64, u64) {
    let (a, b) = UnixDatagram::pair().expect("pair");
    let buf = vec![7u8; size];
    let mut rx = vec![0u8; 65536];
    let (c0, t0) = (cpu_now(), Instant::now());
    let mut n = 0u64;
    while t0.elapsed() < SPAN {
        for _ in 0..batch {
            a.send(&buf).await.expect("send");
        }
        for _ in 0..batch {
            b.recv(&mut rx).await.expect("recv");
        }
        n += batch as u64;
    }
    (cpu_now() - c0, n)
}

async fn udp(size: usize, batch: usize) -> (f64, u64) {
    let a = UdpSocket::bind("127.0.0.1:0").await.expect("bind");
    let b = UdpSocket::bind("127.0.0.1:0").await.expect("bind");
    a.connect(b.local_addr().expect("addr")).await.expect("connect");
    let buf = vec![7u8; size];
    let mut rx = vec![0u8; 65536];
    let (c0, t0) = (cpu_now(), Instant::now());
    let mut n = 0u64;
    while t0.elapsed() < SPAN {
        for _ in 0..batch {
            a.send(&buf).await.expect("send");
        }
        for _ in 0..batch {
            b.recv_from(&mut rx).await.expect("recv");
        }
        n += batch as u64;
    }
    (cpu_now() - c0, n)
}

#[test]
#[ignore = "scratch measurement, run by hand"]
fn the_cheapest_transport_a_message() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async {
        for size in SIZES {
            for batch in BATCHES {
                let (cu, nu) = uds(size, batch).await;
                let (cp, np) = udp(size, batch).await;
                println!(
                    "XUDS size={size} batch={batch} uds_us_a_message={:.2} udp_loopback_us_a_message={:.2} (send and receive, {nu} and {np} messages)",
                    cu / nu as f64 * 1e6,
                    cp / np as f64 * 1e6,
                );
            }
        }
    });
}

//! `coord-jepsen`: one Jepsen client process.
//!
//! Binds a session on one voter's frontend, prints one ready line, then
//! answers one JSON line on stdout for each request line on stdin until
//! stdin closes. See `coord_jepsen::protocol` for the lines.
#![forbid(unsafe_code)]

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::Parser;
use coord_jepsen::{Codec, Session, Timing, protocol};
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[derive(Parser)]
#[command(
    name = "coord-jepsen",
    about = "A Jepsen client: one bound TupleSky session driven by JSON lines"
)]
struct Cli {
    /// The provisioned run directory (the one holding `harness.json`).
    #[arg(long)]
    dir: PathBuf,
    /// Which voter's frontend to bind on, one-based.
    #[arg(long)]
    voter: usize,
    /// Distinguishes this client's certificate and instance from the
    /// run's other clients (Jepsen's process number will do).
    #[arg(long, default_value_t = 0)]
    instance: u16,
    /// How long one attempt waits for its answer, in milliseconds.
    #[arg(long, default_value_t = 2_000)]
    attempt_ms: u64,
    /// How long one operation may take in all before it is reported
    /// `info`, in milliseconds.
    #[arg(long, default_value_t = 10_000)]
    budget_ms: u64,
    /// How long opening the session may take, in milliseconds.
    #[arg(long, default_value_t = 10_000)]
    connect_ms: u64,
    /// Prefix of every key the test touches.
    #[arg(long, default_value = "jepsen/")]
    prefix: String,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    if cli.voter == 0 {
        eprintln!("coord-jepsen: voters are numbered from 1");
        return ExitCode::FAILURE;
    }
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
    {
        Ok(r) => r,
        Err(e) => {
            eprintln!("coord-jepsen: runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    runtime.block_on(serve(cli))
}

async fn serve(cli: Cli) -> ExitCode {
    let timing = Timing {
        attempt: Duration::from_millis(cli.attempt_ms),
        budget: Duration::from_millis(cli.budget_ms),
        connect: Duration::from_millis(cli.connect_ms),
    };
    let mut stdout = tokio::io::stdout();
    let mut session = match Session::open(&cli.dir, cli.voter - 1, cli.instance, timing).await {
        Ok(session) => session,
        Err(e) => {
            let line = json!({"ready": false, "error": e.to_string()});
            let _ = stdout.write_all(format!("{line}\n").as_bytes()).await;
            let _ = stdout.flush().await;
            return ExitCode::FAILURE;
        }
    };
    let codec = Codec {
        namespace: session.namespace,
        prefix: cli.prefix,
    };
    let ready = json!({
        "ready": true,
        "voter": cli.voter,
        "session": format!("{:?}", session.session),
    });
    if stdout
        .write_all(format!("{ready}\n").as_bytes())
        .await
        .and(stdout.flush().await)
        .is_err()
    {
        return ExitCode::FAILURE;
    }
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        if line.trim().is_empty() {
            continue;
        }
        let answer = protocol::handle(&mut session, &codec, &line).await;
        if stdout
            .write_all(format!("{answer}\n").as_bytes())
            .await
            .and(stdout.flush().await)
            .is_err()
        {
            return ExitCode::FAILURE;
        }
    }
    ExitCode::SUCCESS
}

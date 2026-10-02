//! `coord-register`: drive a register workload through every frontend
//! of a provisioned domain, record the history and check that it is
//! linearizable (task-d50).
//!
//! Each caller loops on a few keys, writing a value no other write puts
//! or reading the key, and records when each operation was invoked and
//! when its outcome came back. Faults are applied from outside, by
//! `scripts/bench/register-faults.sh`; a caller whose frontend went away
//! connects again with a new session. At the end the history is written
//! out and checked ([`coord_wan_bench::register::check`]); the process
//! exits non-zero when a violation was found.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use coord_harness::domain::Provisioned;
use coord_harness::issuer::Minter;
use coord_types::ids::NamespaceId;
use coord_types::logical_v1::{CanonicalOperation, KeyRange, LogicalRequest, PutOp, RangeOp};
use coord_wan_bench::register::{Op, OpKind, check};
use coord_wan_bench::{Answer, Caller};

#[derive(Parser, Debug)]
#[command(
    name = "coord-register",
    about = "Drive a register workload through every frontend and check the history is linearizable"
)]
struct Cli {
    /// The directory `coord-harness provision` wrote.
    #[arg(long)]
    dir: PathBuf,
    /// Callers on each frontend.
    #[arg(long, default_value_t = 2)]
    callers_per_frontend: u32,
    /// Registers.
    #[arg(long, default_value_t = 4)]
    keys: u32,
    /// Percent of operations that write.
    #[arg(long, default_value_t = 30)]
    write_percent: u32,
    /// How long to run.
    #[arg(long, default_value_t = 30)]
    seconds: u64,
    /// Per-operation deadline.
    #[arg(long, default_value_t = 5000)]
    deadline_ms: u64,
    /// Frontends (1-based, comma-separated) whose callers only read.
    #[arg(long, default_value = "")]
    read_only_frontends: String,
    /// Windows, as `FROM-UNTIL` seconds into the run (comma-separated),
    /// in which no caller writes.
    #[arg(long, default_value = "")]
    quiet: String,
    /// Where the history goes, one operation per line.
    #[arg(long)]
    history: PathBuf,
    /// Where the summary goes.
    #[arg(long)]
    summary: PathBuf,
}

fn namespace_of(provisioned: &Provisioned) -> Result<NamespaceId, String> {
    let hex = &provisioned.namespace;
    let mut out = [0u8; 16];
    if hex.len() != 32 {
        return Err("the namespace is not 16 bytes".into());
    }
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
            .map_err(|_| "the namespace is not hexadecimal".to_string())?;
    }
    Ok(NamespaceId(out))
}

fn key_bytes(key: u32) -> Vec<u8> {
    format!("/registry/register/{key:04}").into_bytes()
}

fn request(namespace: NamespaceId, operation: CanonicalOperation) -> LogicalRequest {
    let mut request = LogicalRequest::new(namespace, operation);
    request.canonicalize();
    request
}

/// What a read came back with: the value and its modification revision,
/// or `None` when the result is not a one-key range.
fn read_result(result: &[u8]) -> Option<(Option<u64>, u64)> {
    let response: coord_state::Response = postcard::from_bytes(result).ok()?;
    let coord_state::Outcome::Range { items, .. } = response.outcome else {
        return None;
    };
    match items.as_slice() {
        [] => Some((None, 0)),
        [item] => {
            let value = u64::from_be_bytes(item.entry.value.as_slice().try_into().ok()?);
            Some((Some(value), item.entry.mod_revision.get()))
        }
        _ => None,
    }
}

/// A write's revision, from its result.
fn write_revision(result: &[u8]) -> Option<u64> {
    let response: coord_state::Response = postcard::from_bytes(result).ok()?;
    matches!(response.outcome, coord_state::Outcome::Put { .. }).then(|| response.revision.get())
}

struct Plan {
    namespace: NamespaceId,
    keys: u32,
    write_percent: u32,
    read_only: bool,
    quiet: Vec<(f64, f64)>,
    deadline: Duration,
    until: Instant,
    start: Instant,
}

impl Plan {
    fn writes_now(&self) -> bool {
        let at = self.start.elapsed().as_secs_f64();
        !self.read_only
            && !self
                .quiet
                .iter()
                .any(|(from, until)| at >= *from && at < *until)
    }
}

#[derive(Default)]
struct Counts {
    reads: u64,
    writes: u64,
    unknown_writes: u64,
    lost_reads: u64,
    reconnects: u64,
    /// Operations that did not complete, by kind and bounded reason.
    why: std::collections::BTreeMap<String, u64>,
}

/// A bounded name for how an operation failed to complete.
fn reason(outcome: &Result<coord_sdk::Outcome, Answer>) -> String {
    match outcome {
        Ok(coord_sdk::Outcome::Established { result, .. }) => {
            match postcard::from_bytes::<coord_state::Response>(result) {
                Ok(response) => format!("{:?}", response.outcome)
                    .chars()
                    .filter(|c| c.is_alphanumeric())
                    .take(48)
                    .collect::<String>()
                    .to_lowercase(),
                Err(_) => "undecodable-result".into(),
            }
        }
        Ok(other) => coord_wan_bench::caller::refusal(other),
        Err(answer) => answer.reason(),
    }
}

async fn caller(
    dir: PathBuf,
    provisioned: Arc<Provisioned>,
    minter: Arc<Minter>,
    index: u32,
    frontend: usize,
    plan: Arc<Plan>,
) -> (Vec<Op>, Counts) {
    let mut ops = Vec::new();
    let mut counts = Counts::default();
    let mut connection: Option<Caller> = None;
    let mut attempt: u32 = 0;
    let mut written: u64 = 0;
    let mut state = (index as u64) << 32 | 0x9e37;
    let mut unknown_in_row = 0;
    while Instant::now() < plan.until {
        let Some(caller) = connection.as_mut() else {
            // A new session each time: the instance identifies it.
            let instance = (index * 64 + attempt % 64) as u16;
            attempt += 1;
            match Caller::connect(&dir, &provisioned, &minter, frontend, instance).await {
                Ok(c) => {
                    if attempt > 1 {
                        counts.reconnects += 1;
                    }
                    connection = Some(c);
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(250)).await,
            }
            continue;
        };
        // xorshift: the mix need not be reproducible, only spread.
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let key = (state % plan.keys as u64) as u32;
        let write = plan.writes_now() && ((state >> 20) % 100) < plan.write_percent as u64;
        let invoked = plan.start.elapsed().as_nanos() as u64;
        if write {
            written += 1;
            let value = ((index as u64 + 1) << 32) | written;
            let outcome = caller
                .call(
                    &request(
                        plan.namespace,
                        CanonicalOperation::Put(PutOp {
                            key: key_bytes(key),
                            value: value.to_be_bytes().to_vec(),
                            lease: None,
                            prev_kv: false,
                        }),
                    ),
                    plan.deadline,
                )
                .await;
            let completed = plan.start.elapsed().as_nanos() as u64;
            let revision = match &outcome {
                Ok(coord_sdk::Outcome::Established { result, .. }) => write_revision(result),
                _ => None,
            };
            // Anything but an established write may still have happened.
            if revision.is_some() {
                counts.writes += 1;
            } else {
                counts.unknown_writes += 1;
                *counts
                    .why
                    .entry(format!("write:{}", reason(&outcome)))
                    .or_default() += 1;
            }
            ops.push(Op {
                caller: index,
                frontend: frontend as u32 + 1,
                key,
                invoked_ns: invoked,
                completed_ns: if revision.is_some() {
                    completed
                } else {
                    u64::MAX
                },
                kind: OpKind::Write { value, revision },
            });
            unknown_in_row = if matches!(outcome, Err(Answer::Unknown)) {
                unknown_in_row + 1
            } else {
                0
            };
        } else {
            let outcome = caller
                .call(
                    &request(
                        plan.namespace,
                        CanonicalOperation::Range(RangeOp {
                            range: KeyRange::exact(key_bytes(key)),
                            revision: None,
                            limit: 1,
                            keys_only: false,
                            count_only: false,
                        }),
                    ),
                    plan.deadline,
                )
                .await;
            let completed = plan.start.elapsed().as_nanos() as u64;
            match &outcome {
                Ok(coord_sdk::Outcome::Established { result, .. }) => {
                    match read_result(result) {
                        Some((value, revision)) => {
                            counts.reads += 1;
                            ops.push(Op {
                                caller: index,
                                frontend: frontend as u32 + 1,
                                key,
                                invoked_ns: invoked,
                                completed_ns: completed,
                                kind: OpKind::Read { value, revision },
                            });
                        }
                        None => {
                            counts.lost_reads += 1;
                            *counts
                                .why
                                .entry(format!("read:{}", reason(&outcome)))
                                .or_default() += 1;
                        }
                    }
                    unknown_in_row = 0;
                }
                _ => {
                    counts.lost_reads += 1;
                    *counts
                        .why
                        .entry(format!("read:{}", reason(&outcome)))
                        .or_default() += 1;
                    unknown_in_row = if matches!(outcome, Err(Answer::Unknown)) {
                        unknown_in_row + 1
                    } else {
                        0
                    };
                }
            }
        }
        // Two unanswered in a row: the frontend may be gone. Connect
        // again rather than wait out every deadline on a dead link.
        if unknown_in_row >= 2 {
            connection = None;
            unknown_in_row = 0;
        }
    }
    (ops, counts)
}

fn parse_list(text: &str) -> Vec<u32> {
    text.split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect()
}

fn parse_windows(text: &str) -> Vec<(f64, f64)> {
    text.split(',')
        .filter_map(|w| {
            let (from, until) = w.trim().split_once('-')?;
            Some((from.parse().ok()?, until.parse().ok()?))
        })
        .collect()
}

fn main() -> std::process::ExitCode {
    match run() {
        Ok(true) => std::process::ExitCode::SUCCESS,
        Ok(false) => std::process::ExitCode::from(1),
        Err(e) => {
            eprintln!("coord-register: {e}");
            std::process::ExitCode::from(2)
        }
    }
}

fn run() -> Result<bool, Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let provisioned = Arc::new(Provisioned::read(&cli.dir)?);
    let minter = Arc::new(Minter::of(&provisioned)?);
    let read_only = parse_list(&cli.read_only_frontends);
    let start = Instant::now();
    let base = Plan {
        namespace: namespace_of(&provisioned)?,
        keys: cli.keys.max(1),
        write_percent: cli.write_percent.min(100),
        read_only: false,
        quiet: parse_windows(&cli.quiet),
        deadline: Duration::from_millis(cli.deadline_ms),
        until: start + Duration::from_secs(cli.seconds),
        start,
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let frontends = provisioned.voters.len();
    let results = runtime.block_on(async {
        let mut tasks = Vec::new();
        let mut index = 0;
        for frontend in 0..frontends {
            let plan = Arc::new(Plan {
                read_only: read_only.contains(&(frontend as u32 + 1)),
                quiet: base.quiet.clone(),
                ..base
            });
            for _ in 0..cli.callers_per_frontend {
                tasks.push(tokio::spawn(caller(
                    cli.dir.clone(),
                    provisioned.clone(),
                    minter.clone(),
                    index,
                    frontend,
                    plan.clone(),
                )));
                index += 1;
            }
        }
        let mut out = Vec::new();
        for task in tasks {
            out.push(task.await);
        }
        out
    });

    let mut history = Vec::new();
    let mut counts = Counts::default();
    for result in results {
        let (ops, c) = result?;
        history.extend(ops);
        counts.reads += c.reads;
        counts.writes += c.writes;
        counts.unknown_writes += c.unknown_writes;
        counts.lost_reads += c.lost_reads;
        counts.reconnects += c.reconnects;
        for (why, n) in c.why {
            *counts.why.entry(why).or_default() += n;
        }
    }
    history.sort_by_key(|op| op.invoked_ns);
    let mut lines = String::new();
    for op in &history {
        lines.push_str(&serde_json::to_string(op)?);
        lines.push('\n');
    }
    std::fs::write(&cli.history, lines)?;

    let violations = check(&history);
    let mut by_frontend = std::collections::BTreeMap::<u32, u64>::new();
    for op in &history {
        if matches!(op.kind, OpKind::Read { .. }) {
            *by_frontend.entry(op.frontend).or_default() += 1;
        }
    }
    let summary = serde_json::json!({
        "seconds": cli.seconds,
        "callers": cli.callers_per_frontend as usize * frontends,
        "keys": cli.keys,
        "reads_completed": counts.reads,
        "reads_completed_by_frontend": by_frontend,
        "reads_not_completed": counts.lost_reads,
        "writes_established": counts.writes,
        "writes_unknown": counts.unknown_writes,
        "reconnects": counts.reconnects,
        "not_completed_by_reason": counts.why,
        "violations": violations.len(),
        "first_violations": violations.iter().take(10).collect::<Vec<_>>(),
    });
    let rendered = serde_json::to_string_pretty(&summary)?;
    std::fs::write(&cli.summary, &rendered)?;
    println!("{rendered}");
    Ok(violations.is_empty())
}

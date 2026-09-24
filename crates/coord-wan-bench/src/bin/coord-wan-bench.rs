//! `coord-wan-bench`: offer a scheduled workload to a provisioned domain
//! and report what it did (task-62).
//!
//! The run directory is one `coord-harness up` left behind. Impairment
//! is applied outside this process; `--topology` and `--impairment`
//! record what was applied, and they are declarations, not measurements.
#![forbid(unsafe_code)]

use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use coord_wan_bench::report::{Absent, Measured, Region, Topology};
use coord_wan_bench::workload::{Mix, Workload};
use coord_wan_bench::{RunSpec, run};

#[derive(Parser)]
#[command(
    name = "coord-wan-bench",
    about = "Offer a scheduled workload to a running domain and report what it did"
)]
struct Cli {
    /// The `coord-harness up` run directory.
    #[arg(long)]
    dir: PathBuf,
    /// A label for this run.
    #[arg(long)]
    label: String,
    /// The durability the domain is running under, in the operator's own
    /// words. There is no headline without one.
    #[arg(long)]
    durability: String,
    /// Scheduled inter-arrival time in nanoseconds. Zero is a closed
    /// loop, which is labelled as one.
    #[arg(long, default_value_t = 0)]
    arrival_ns: u64,
    /// Operations offered before measurement starts.
    #[arg(long, default_value_t = 200)]
    warmup_ops: u64,
    /// Operations measured.
    #[arg(long, default_value_t = 2000)]
    measured_ops: u64,
    /// Callers, each with its own session and connection.
    #[arg(long, default_value_t = 8)]
    callers: u32,
    /// Frontends the callers are spread over.
    #[arg(long, default_value_t = 1)]
    frontends: u32,
    /// Per-operation deadline in milliseconds.
    #[arg(long, default_value_t = 10_000)]
    deadline_ms: u64,
    /// Seed of the offered work.
    #[arg(long, default_value_t = 1)]
    seed: u64,
    /// Operation mix, `put=15,get=55,contended=20,txn=5,scan=5`.
    #[arg(long, default_value = "put=15,get=55,contended=20,txn=5,scan=5")]
    mix: String,
    /// Distinct keys the reads and unconditional writes touch.
    #[arg(long, default_value_t = 10_000)]
    keyspace: u32,
    /// Keys the conditional writes contend on. A small number is the
    /// hot-writer case.
    #[arg(long, default_value_t = 16)]
    hot_keys: u32,
    /// Value size in bytes.
    #[arg(long, default_value_t = 256)]
    value_bytes: usize,
    /// Keys per transaction.
    #[arg(long, default_value_t = 4)]
    transaction_keys: u32,
    /// Rows a scan asks for.
    #[arg(long, default_value_t = 32)]
    scan_limit: u32,
    /// A name for the shape the domain was deployed in.
    #[arg(long, default_value = "single-host")]
    topology: String,
    /// `name=voters` per region, repeatable.
    #[arg(long = "region")]
    regions: Vec<String>,
    /// The delay, loss and asymmetry that were applied, in the
    /// operator's own words. Omitted, the report says the impairment was
    /// not stated -- never that it was zero.
    #[arg(long)]
    impairment: Option<String>,
    /// Write the report here instead of standard output.
    #[arg(long)]
    out: Option<PathBuf>,
}

fn main() -> std::process::ExitCode {
    match drive() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("coord-wan-bench: {err}");
            std::process::ExitCode::FAILURE
        }
    }
}

fn drive() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let mix = Mix::parse(&cli.mix)?;
    let mut regions = Vec::new();
    for declaration in &cli.regions {
        let (name, voters) = declaration
            .split_once('=')
            .ok_or_else(|| format!("`{declaration}` is not name=voters"))?;
        regions.push(Region {
            name: name.to_owned(),
            voters: voters.parse()?,
        });
    }

    let provisioned = coord_harness::domain::Provisioned::read(&cli.dir)?;
    let topology = Topology {
        label: cli.topology.clone(),
        voters: provisioned.voters.len() as u32,
        frontend: provisioned.frontend().to_owned(),
        regions,
        impairment: match &cli.impairment {
            Some(text) => Measured::Observed(text.clone()),
            None => Measured::Absent(Absent::NotStated),
        },
    };

    let spec = RunSpec {
        label: cli.label,
        seed: cli.seed,
        durability: cli.durability,
        topology,
        arrival_ns: cli.arrival_ns,
        warmup_ops: cli.warmup_ops,
        measured_ops: cli.measured_ops,
        callers: cli.callers,
        deadline: Duration::from_millis(cli.deadline_ms),
        workload: Workload {
            // Replaced with the provisioned grant's namespace: a request
            // in any other namespace is refused, and a benchmark of
            // refusals is not a benchmark.
            namespace: coord_types::ids::NamespaceId([0; 16]),
            keyspace: cli.keyspace,
            hot_keys: cli.hot_keys,
            value_bytes: cli.value_bytes,
            transaction_keys: cli.transaction_keys,
            scan_limit: cli.scan_limit,
            mix,
        },
        frontends: cli.frontends,
    };

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let report = runtime.block_on(run(&cli.dir, spec))?;
    let rendered = serde_json::to_string_pretty(&report)?;
    match cli.out {
        Some(path) => std::fs::write(path, rendered)?,
        None => println!("{rendered}"),
    }
    Ok(())
}

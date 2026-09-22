//! `store-experiment`: the fresh local storage experiment entry point
//! (task-s04). `cargo xtask store-differential|store-bench|store-compare`
//! invokes it; it is never linked into a production artifact.
//!
//! Every subcommand allocates an absent run root under an experiment
//! directory, writes its manifests and raw measurements there, and leaves
//! failed runs in place.
#![forbid(unsafe_code)]

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use coord_store_bench::compare::{Semantics, compare, semantics};
use coord_store_bench::manifest::{EngineKind, TrialLabel};
use coord_store_bench::runroot::RunRoot;
use coord_store_bench::trial::{
    TrialSpec, committed_fixture, replay_fixture, run_trial, with_fixture_digest,
};
use coord_store_bench::workload::WorkloadSpec;

#[derive(Parser)]
#[command(
    name = "store-experiment",
    about = "Fresh local storage experiments: fixture replay, differential semantics and labelled cost comparison",
    disable_help_subcommand = true
)]
struct Cli {
    /// Directory the run root is allocated under. It must not be a store
    /// root and is never reset.
    #[arg(long, global = true, default_value = "target/experiments")]
    experiment_dir: PathBuf,
    /// Generator seed byte (expanded into the 32-byte seed).
    #[arg(long, global = true, default_value_t = 0x5a)]
    seed: u8,
    /// Measured operations per trial.
    #[arg(long, global = true)]
    measured_ops: Option<u32>,
    /// Scheduled inter-arrival time in nanoseconds; 0 is a closed loop.
    #[arg(long, global = true)]
    arrival_ns: Option<u64>,
    /// Engine read-cache bytes.
    #[arg(long, global = true, default_value_t = 8 * 1024 * 1024)]
    cache_bytes: usize,
    /// Trial label; tuned and sensitivity runs never replace the primary.
    #[arg(long, global = true, default_value = "primary")]
    label: String,
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Replay the committed fixture and the workload on every engine and
    /// compare the logical results. No cost is reported.
    Differential,
    /// Measure one engine.
    Bench {
        /// Engine: model, redb or fjall.
        #[arg(long, default_value = "redb")]
        engine: String,
    },
    /// Paired, order-alternated comparison of several engines.
    Compare {
        /// Comma-separated engines.
        #[arg(long, default_value = "redb,fjall")]
        engines: String,
        /// Repetitions per engine.
        #[arg(long, default_value_t = 3)]
        repetitions: u32,
    },
}

fn workload(cli: &Cli) -> WorkloadSpec {
    let mut spec = WorkloadSpec::smoke();
    spec.seed = [cli.seed; 32];
    if let Some(ops) = cli.measured_ops {
        spec.measured_ops = ops;
    }
    if let Some(ns) = cli.arrival_ns {
        spec.arrival_interval_ns = ns;
    }
    spec
}

fn label(name: &str) -> Result<TrialLabel, String> {
    match name {
        "primary" => Ok(TrialLabel::Primary),
        "tuned" => Ok(TrialLabel::Tuned),
        "sensitivity" => Ok(TrialLabel::Sensitivity),
        other => Err(format!("unknown label {other}")),
    }
}

fn engines(list: &str) -> Result<Vec<EngineKind>, String> {
    list.split(',')
        .map(|name| EngineKind::parse(name.trim()).ok_or(format!("unknown engine {name}")))
        .collect()
}

fn run(cli: &Cli) -> Result<String, String> {
    let label = label(&cli.label)?;
    let spec = workload(cli);
    let run_root = RunRoot::allocate(&cli.experiment_dir, "run").map_err(|e| e.to_string())?;
    let summary = match &cli.command {
        Cmd::Differential => {
            let fixture = committed_fixture();
            let mut reports = Vec::new();
            let mut digests = std::collections::BTreeMap::new();
            let mut fixture_differences = Vec::new();
            for engine in EngineKind::ALL {
                let (digest, matched, nanos) =
                    replay_fixture(engine, &fixture, &run_root, 0, cli.cache_bytes)
                        .map_err(|e| e.to_string())?;
                if !matched {
                    fixture_differences.push(format!(
                        "{}: the committed fixture replay differs from the oracle or the \
                         frozen digest",
                        engine.name()
                    ));
                }
                digests.insert(
                    engine.name().to_owned(),
                    format!(
                        "{} ({}; {nanos} ns)",
                        digest
                            .0
                            .iter()
                            .map(|b| format!("{b:02x}"))
                            .collect::<String>(),
                        if matched { "matches" } else { "DIFFERS" }
                    ),
                );
                let trial = TrialSpec {
                    engine,
                    workload: spec,
                    label,
                    repetition: 0,
                    cache_bytes: cli.cache_bytes,
                    maintenance: true,
                };
                // The report names the fixture that anchored its
                // correctness result, so a persisted raw report can be
                // traced back to it.
                let report = run_trial(&trial, &run_root).map_err(|e| e.to_string())?;
                reports.push(with_fixture_digest(report, digest));
            }
            let mut result: Semantics = semantics(&reports);
            result.fixture_digests = digests;
            // A fixture mismatch is a semantic difference like any other:
            // it fails the run rather than being rendered only into the
            // digest string beside a successful exit.
            if !fixture_differences.is_empty() {
                result.equal = false;
                result.differences.extend(fixture_differences);
            }
            run_root
                .write_json("differential.json", &result)
                .map_err(|e| e.to_string())?;
            for report in &reports {
                run_root
                    .write_json(
                        &format!("raw/{}-differential.json", report.manifest.engine.name),
                        report,
                    )
                    .map_err(|e| e.to_string())?;
            }
            if !result.equal {
                return Err(format!(
                    "engines disagree: {}\nrun root: {}",
                    result.differences.join("; "),
                    run_root.path().display()
                ));
            }
            format!(
                "differential: {} engines agree on the fixture digest and the workload state",
                reports.len()
            )
        }
        Cmd::Bench { engine } => {
            let engine = EngineKind::parse(engine).ok_or(format!("unknown engine {engine}"))?;
            let trial = TrialSpec {
                engine,
                workload: spec,
                label,
                repetition: 0,
                cache_bytes: cli.cache_bytes,
                maintenance: true,
            };
            let report = run_trial(&trial, &run_root).map_err(|e| e.to_string())?;
            run_root
                .write_json("experiment.json", &report.manifest)
                .map_err(|e| e.to_string())?;
            run_root
                .write_json(&format!("raw/{}-bench.json", engine.name()), &report)
                .map_err(|e| e.to_string())?;
            format!(
                "bench {}: {} measured operations, service p50 {:?} ns, p99 {:?} ns, backlog {}, \
                 maintenance steps {} ({} with work left)",
                engine.name(),
                report.service.count,
                report.service.p50_ns,
                report.service.p99_ns,
                report.counters.max_backlog,
                report.counters.maintenance_steps,
                report.counters.maintenance_debt_steps
            )
        }
        Cmd::Compare {
            engines: list,
            repetitions,
        } => {
            let engines = engines(list)?;
            let report = compare(
                &spec,
                &engines,
                *repetitions,
                label,
                cli.cache_bytes,
                &run_root,
            )
            .map_err(|e| e.to_string())?;
            run_root
                .write_json("comparison.json", &report)
                .map_err(|e| e.to_string())?;
            format!("compare: verdict {:?}", report.verdict)
        }
    };
    Ok(format!(
        "{summary}\nrun root: {}",
        run_root.path().display()
    ))
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(&cli) {
        Ok(summary) => {
            println!("{summary}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

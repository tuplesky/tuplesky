//! task-s04 acceptance, recording half: run roots are unique and never
//! overwrite anything, a service root is refused, every manifest field a
//! comparison depends on is recorded, unavailable metrics stay unavailable,
//! and a scheduled arrival process measures queueing rather than only what
//! the engine was ready to accept.

use std::path::Path;

use coord_store_bench::manifest::{EngineKind, SCHEMA, TrialLabel, lock_digest};
use coord_store_bench::measure::ProcessCounters;
use coord_store_bench::runroot::{MARKER, RunRoot, RunRootError};
use coord_store_bench::trial::{TrialSpec, run_trial};
use coord_store_bench::workload::WorkloadSpec;

#[test]
fn run_roots_are_unique_marked_and_never_overwrite() {
    let dir = tempfile::tempdir().unwrap();
    let a = RunRoot::allocate(dir.path(), "run").unwrap();
    let b = RunRoot::allocate(dir.path(), "run").unwrap();
    assert_ne!(a.path(), b.path(), "two runs never share a root");
    assert!(a.path().join(MARKER).is_file());
    let engine_dir = a.engine_dir("redb", 0).unwrap();
    assert!(engine_dir.is_dir());
    assert!(
        matches!(a.engine_dir("redb", 0), Err(RunRootError::AlreadyExists(_))),
        "an engine directory is allocated once"
    );
    a.write_json("raw/sample.json", &vec![1u64, 2, 3]).unwrap();
    assert!(b.path().join(MARKER).is_file());
    let path = b.path().to_path_buf();
    b.remove().unwrap();
    assert!(!path.exists(), "a marked run root is removable");
}

#[test]
fn a_store_root_is_never_used_or_removed() {
    let dir = tempfile::tempdir().unwrap();
    // A service root: the pointer alone is enough to refuse it.
    std::fs::write(dir.path().join("CURRENT"), b"gen-000001").unwrap();
    assert!(matches!(
        RunRoot::allocate(dir.path(), "run"),
        Err(RunRootError::LooksLikeStoreRoot(_))
    ));
    // A child of the service root, such as its `experiments` directory,
    // would put experiment generations inside production state: it is
    // refused too, and the error names the service root.
    std::fs::write(dir.path().join("lock"), b"").unwrap();
    let nested = dir.path().join("experiments").join("local");
    match RunRoot::allocate(&nested, "run") {
        Err(RunRootError::LooksLikeStoreRoot(root)) => assert_eq!(root, dir.path()),
        other => panic!("a directory beneath a store root was accepted: {other:?}"),
    }
    assert!(
        !nested.exists(),
        "nothing was created beneath the store root"
    );
    // An unmarked directory is never removed, even when it is empty.
    let other = tempfile::tempdir().unwrap();
    let run = RunRoot::allocate(other.path(), "run").unwrap();
    let path = run.path().to_path_buf();
    std::fs::remove_file(path.join(MARKER)).unwrap();
    assert!(matches!(run.remove(), Err(RunRootError::NotDisposable(_))));
    assert!(path.exists());
}

#[test]
fn a_trial_records_everything_a_comparison_depends_on() {
    let dir = tempfile::tempdir().unwrap();
    let run = RunRoot::allocate(dir.path(), "manifest").unwrap();
    let mut spec = TrialSpec::smoke(EngineKind::Fjall);
    spec.workload.measured_ops = 24;
    let report = run_trial(&spec, &run).unwrap();
    let m = &report.manifest;
    assert_eq!(m.schema, SCHEMA);
    assert_eq!(m.run_id, run.run_id());
    assert_eq!(m.label, TrialLabel::Primary.name());
    assert_eq!(m.provenance.lock_digest, lock_digest());
    assert!(!m.provenance.generator.is_empty());
    assert_eq!(m.engine.name, "fjall");
    assert_eq!(m.engine.version, EngineKind::Fjall.version());
    assert_eq!(m.engine.features, "lz4");
    assert_eq!(
        m.engine.durability_profile,
        EngineKind::Fjall.durability_profile()
    );
    assert_eq!(m.engine.layout, EngineKind::Fjall.layout());
    assert_eq!(m.engine.collections, 17);
    assert_eq!(m.engine.cache_bytes, spec.cache_bytes);
    assert_eq!(m.workload_digest, spec.workload.digest());
    assert_eq!(m.limits, spec.limits());
    assert!(m.maintenance_enabled);
    assert_eq!(m.environment.os, std::env::consts::OS);
    assert!(m.environment.cache_condition.contains("cold OS page cache"));
    // Resources: what the platform exposes is a number, what it does not is
    // absent and named.
    assert!(report.resources.logical_bytes_written > 0);
    assert!(report.resources.engine_bytes.is_some_and(|b| b > 0));
    assert!(
        report
            .resources
            .unavailable
            .iter()
            .any(|u| u.starts_with("free_disk_bytes")),
        "an unmeasured metric is named, not reported as zero"
    );
    // Raw measurements are preserved beside the summary.
    assert_eq!(report.raw_service_ns.len(), report.service.count as usize);
    let path = run.write_json("raw/fjall-0.json", &report).unwrap();
    assert!(path.is_file());
    let parsed: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(parsed["manifest"]["engine"]["name"], "fjall");
}

#[test]
fn engine_versions_match_the_workspace_pins() {
    // The manifest names the engine version; the workspace manifest is the
    // only place it may come from.
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let manifest: toml::Value =
        toml::from_str(&std::fs::read_to_string(root.join("Cargo.toml")).unwrap()).unwrap();
    let deps = manifest["workspace"]["dependencies"].as_table().unwrap();
    for (engine, crate_name) in [(EngineKind::Redb, "redb"), (EngineKind::Fjall, "fjall")] {
        let pin = deps[crate_name]["version"].as_str().unwrap();
        assert_eq!(
            pin.trim_start_matches('='),
            engine.version(),
            "{crate_name} pin drifted from the experiment manifest"
        );
        let features: Vec<&str> = deps[crate_name]["features"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f.as_str().unwrap())
            .collect();
        assert_eq!(features.join(","), engine.features());
    }
}

#[test]
fn a_scheduled_arrival_process_measures_queueing_not_only_service() {
    let dir = tempfile::tempdir().unwrap();
    let run = RunRoot::allocate(dir.path(), "offered").unwrap();
    let mut spec = TrialSpec::smoke(EngineKind::Redb);
    spec.workload = WorkloadSpec {
        measured_ops: 32,
        // One arrival per microsecond is far more than this workload can
        // durably commit, so the generator falls behind on purpose.
        arrival_interval_ns: 1_000,
        ..spec.workload
    };
    let report = run_trial(&spec, &run).unwrap();
    assert_eq!(report.service.count, 32);
    assert!(
        report.scheduled.p50_ns >= report.service.p50_ns,
        "latency from the scheduled arrival cannot be below the service time"
    );
    assert!(
        report.counters.max_backlog > 0,
        "an overloaded schedule reports its backlog instead of hiding it"
    );
    assert!(report.generator_lag.p99_ns.unwrap() > 0);
    assert_eq!(
        report.counters.offered,
        report.counters.admitted + report.counters.retained_retries
    );
    assert!(report.counters.published_revisions > 0);
    // A closed loop offers work only after the previous return, so it
    // reports no backlog at all.
    let mut closed = spec;
    closed.workload.arrival_interval_ns = 0;
    // A second trial in the same run root allocates its own directory; it
    // never reuses the first one.
    closed.repetition = 1;
    let closed_report = run_trial(&closed, &run).unwrap();
    assert_eq!(closed_report.counters.max_backlog, 0);
}

#[test]
fn process_counters_are_read_once_per_phase_boundary() {
    let before = ProcessCounters::read();
    let mut sum = 0u64;
    for i in 0..1_000_000u64 {
        sum = sum.wrapping_add(i);
    }
    assert!(sum > 0);
    let after = ProcessCounters::read();
    let resources = before.since(&after, 1);
    // On a platform that exposes them the counters move forward; on one
    // that does not they stay absent and are named.
    match resources.cpu_ns {
        Some(_) => assert!(
            !resources
                .unavailable
                .iter()
                .any(|u| u.starts_with("cpu_ns"))
        ),
        None => assert!(
            resources
                .unavailable
                .iter()
                .any(|u| u.starts_with("cpu_ns"))
        ),
    }
}

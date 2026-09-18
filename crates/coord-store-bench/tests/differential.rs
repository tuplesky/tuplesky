//! task-s04 acceptance, controlled failure-free half: the committed
//! `StoreScenarioV1` fixture and one protocol-shaped workload replay to the
//! same logical result on the model engine, the redb reference and the
//! experimental Fjall adapter, each in its own fresh run root, and each
//! survives a same-engine reopen.

use coord_store_bench::compare::{Verdict, compare, semantics};
use coord_store_bench::manifest::{EngineKind, TrialLabel};
use coord_store_bench::runroot::RunRoot;
use coord_store_bench::trial::{TrialSpec, committed_fixture, replay_fixture, run_trial};
use coord_store_bench::workload::WorkloadSpec;

fn spec() -> WorkloadSpec {
    WorkloadSpec::smoke()
}

#[test]
fn the_committed_fixture_replays_to_one_digest_on_every_engine() {
    let dir = tempfile::tempdir().unwrap();
    let run = RunRoot::allocate(dir.path(), "fixture").unwrap();
    let fixture = committed_fixture();
    assert!(fixture.expected_digest.is_some(), "the fixture is frozen");
    let mut digests = Vec::new();
    for engine in EngineKind::ALL {
        let (digest, matched, _ns) = replay_fixture(engine, &fixture, &run, 0, 4 << 20).unwrap();
        assert!(matched, "{}: fixture replay diverged", engine.name());
        digests.push(digest);
    }
    assert!(
        digests.windows(2).all(|w| w[0] == w[1]),
        "the physical layouts differ; the logical digest may not"
    );
    assert_eq!(digests[0], fixture.expected_digest.unwrap());
}

#[test]
fn the_protocol_shaped_workload_produces_one_logical_state_on_every_engine() {
    let dir = tempfile::tempdir().unwrap();
    let run = RunRoot::allocate(dir.path(), "differential").unwrap();
    let mut reports = Vec::new();
    for engine in EngineKind::ALL {
        let mut trial = TrialSpec::smoke(engine);
        trial.workload = spec();
        reports.push(run_trial(&trial, &run).unwrap());
    }
    let result = semantics(&reports);
    assert!(result.equal, "{:?}", result.differences);
    let first = &reports[0].observable;
    // The workload really exercises the shapes the comparison claims to
    // cover: several indexes, leases, retained retries and retention.
    assert!(first.kv_revision > 0);
    assert!(first.kv_rows > 0 && first.history_rows > 0 && first.event_rows > 0);
    assert!(first.lease_rows > 0 && first.lease_key_rows > 0);
    // Retained retry results are present; retry floors appear only once a
    // session window is exhausted, which this bounded workload does not do.
    assert!(first.retry_rows > 0);
    assert!(first.session_rows > 0 && first.policy_rows > 0);
    assert!(first.executed_rows > 0);
    assert!(first.retention_floor > 0, "a retention floor was published");
    for report in &reports {
        assert_eq!(
            report.observable, report.after_reopen,
            "{}: the same-engine reopen changed the logical state",
            report.manifest.engine.name
        );
        assert!(
            report.counters.retained_retries > 0,
            "retries were replayed"
        );
        assert!(report.counters.published_revisions > 0, "events published");
        assert!(report.counters.pinned_checks > 0, "pinned reads were held");
        assert!(report.counters.maintenance_steps > 0, "maintenance ran");
        assert_eq!(report.counters.errors, 0);
        assert_eq!(report.counters.rejected, 0);
        assert_eq!(report.service.count as u32, spec().measured_ops);
    }
}

#[test]
fn a_paired_comparison_reports_cost_only_between_durable_engines() {
    let dir = tempfile::tempdir().unwrap();
    let run = RunRoot::allocate(dir.path(), "compare").unwrap();
    let mut workload = spec();
    workload.measured_ops = 48;
    let report = compare(
        &workload,
        &[EngineKind::Redb, EngineKind::Fjall],
        2,
        TrialLabel::Primary,
        4 << 20,
        &run,
    )
    .unwrap();
    assert!(report.semantics.equal, "{:?}", report.semantics.differences);
    assert_eq!(report.order.len(), 2);
    assert_ne!(
        report.order[0], report.order[1],
        "the engine order alternates between repetitions"
    );
    match &report.verdict {
        Verdict::Comparable {
            baseline,
            service_p50_percent_of_baseline,
            ..
        } => {
            assert_eq!(baseline, "redb", "redb is the production reference");
            assert_eq!(service_p50_percent_of_baseline.len(), 2);
            assert_eq!(service_p50_percent_of_baseline["redb"], 100);
        }
        Verdict::Disqualified { reasons } => panic!("unexpectedly disqualified: {reasons:?}"),
    }
    assert!(!report.caveats.is_empty());
    for summary in &report.engines {
        assert_eq!(summary.repetitions, 2);
        assert_eq!(summary.service_p50_variation.repetitions, 2);
        assert!(summary.service_p50_variation.spread_percent.is_some());
    }
}

#[test]
fn the_model_engine_is_a_correctness_reference_not_a_speed_baseline() {
    let dir = tempfile::tempdir().unwrap();
    let run = RunRoot::allocate(dir.path(), "model-only").unwrap();
    let mut workload = spec();
    workload.measured_ops = 24;
    let report = compare(
        &workload,
        &[EngineKind::Model],
        1,
        TrialLabel::Primary,
        4 << 20,
        &run,
    )
    .unwrap();
    match &report.verdict {
        Verdict::Disqualified { reasons } => {
            assert!(
                reasons.iter().any(|r| r.contains("speed baseline")),
                "{reasons:?}"
            );
        }
        other => panic!("the model must never be a baseline: {other:?}"),
    }
}

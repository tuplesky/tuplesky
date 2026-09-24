//! Differential semantics and honestly labelled cost comparison (design
//! Sections 17.12 and 17.14).
//!
//! Semantics come first: the same controlled failure-free workload must
//! produce the same revisions, events, leases, authorization, retry state
//! and common digest on every engine. Only then may a cost be reported, and
//! only between engines that actually write durable bytes: the model is a
//! correctness reference, never a durable-engine baseline. Repetitions are
//! paired and the engine order is alternated. Any semantic difference,
//! error or budget difference disqualifies the comparison instead of
//! producing a speed claim with a caveat.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::driver::Observable;
use crate::manifest::{EngineKind, TrialLabel};
use crate::measure::Variation;
use crate::runroot::RunRoot;
use crate::trial::{TrialError, TrialReport, TrialSpec, run_trial};
use crate::workload::WorkloadSpec;

/// Caveats every comparison carries. They are part of the report, not
/// commentary added by a reader.
pub const CAVEATS: [&str; 6] = [
    "early local storage evaluation; not production engine qualification, \
     not a WAN or Kubernetes measurement and not a migration",
    "the experimental engine's SyncAll and the redb two-phase reference are \
     compared under ordinary crash assumptions only",
    "fjall's own flush and compaction internals are not fault injected here; \
     untested modes are reported, not assumed equivalent",
    "a fresh directory and a new process do not prove a cold OS page cache",
    "short measured phases are not steady state, and percentiles beyond the \
     sample count are not reported",
    "the model engine writes no durable bytes and is never a speed baseline",
];

/// What the differential check compared.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Semantics {
    /// Whether every engine produced the same logical state.
    pub equal: bool,
    /// Named differences, empty when equal.
    pub differences: Vec<String>,
    /// The logical state, per engine.
    pub per_engine: BTreeMap<String, Observable>,
    /// The committed fixture's replay digest per engine, when a fixture was
    /// replayed as part of the comparison.
    pub fixture_digests: BTreeMap<String, String>,
}

/// Whether a cost may be reported at all.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Verdict {
    /// Semantics match and the inputs are identical; the cost figures are
    /// relative to `baseline`.
    Comparable {
        /// Engine the ratios are relative to.
        baseline: String,
        /// Median service time ratio per engine, in percent of the
        /// baseline (100 means identical).
        service_p50_percent_of_baseline: BTreeMap<String, u64>,
        /// 99th percentile service time ratio, in percent of the baseline.
        service_p99_percent_of_baseline: BTreeMap<String, u64>,
    },
    /// No cost figure may be derived from this run.
    Disqualified {
        /// Why.
        reasons: Vec<String>,
    },
}

/// One engine's summary over the repetitions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EngineSummary {
    /// Engine name.
    pub engine: String,
    /// Whether the engine writes durable bytes.
    pub durable: bool,
    /// Repetitions.
    pub repetitions: u32,
    /// Spread of the median service time across repetitions.
    pub service_p50_variation: Variation,
    /// Spread of the 99th percentile across repetitions.
    pub service_p99_variation: Variation,
    /// Every trial of this engine.
    pub trials: Vec<TrialReport>,
}

/// A comparison of several engines under one workload.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComparisonReportV1 {
    /// Schema name.
    pub schema: String,
    /// Run identifier.
    pub run_id: String,
    /// Workload digest every trial shares.
    pub workload_digest: String,
    /// Trial label.
    pub label: String,
    /// Repetitions per engine.
    pub repetitions: u32,
    /// Engine order actually executed, per repetition.
    pub order: Vec<Vec<String>>,
    /// Per-engine summaries.
    pub engines: Vec<EngineSummary>,
    /// Semantic comparison.
    pub semantics: Semantics,
    /// Whether a cost may be reported.
    pub verdict: Verdict,
    /// Caveats.
    pub caveats: Vec<String>,
}

/// Schema name of the comparison report.
pub const COMPARISON_SCHEMA: &str = "store_comparison_v1";

fn hex(d: &coord_types::identity::Digest32) -> String {
    d.0.iter().map(|b| format!("{b:02x}")).collect()
}

/// Compare the logical state several engines produced. The state after
/// prefill is compared as well as the final state: a prefill that went
/// wrong on one engine can be overwritten or deleted by the later churn,
/// so equal final states alone do not show that every engine started the
/// measured phase from the same validated state.
pub fn semantics(reports: &[TrialReport]) -> Semantics {
    let mut per_engine: BTreeMap<String, Observable> = BTreeMap::new();
    let mut prefill_per_engine: BTreeMap<String, Observable> = BTreeMap::new();
    let mut differences = Vec::new();
    for report in reports {
        let engine = report.manifest.engine.name.clone();
        match per_engine.get(&engine) {
            Some(earlier) if *earlier != report.observable => differences.push(format!(
                "{engine}: repetitions of the same engine disagree (common digest {} vs {})",
                hex(&earlier.common_digest),
                hex(&report.observable.common_digest)
            )),
            Some(_) => {}
            None => {
                per_engine.insert(engine.clone(), report.observable.clone());
            }
        }
        match prefill_per_engine.get(&engine) {
            Some(earlier) if *earlier != report.after_prefill => differences.push(format!(
                "{engine}: repetitions of the same engine disagree after prefill \
                 (common digest {} vs {})",
                hex(&earlier.common_digest),
                hex(&report.after_prefill.common_digest)
            )),
            Some(_) => {}
            None => {
                prefill_per_engine.insert(engine, report.after_prefill.clone());
            }
        }
    }
    let mut prefill = prefill_per_engine.iter();
    if let Some((first_name, first)) = prefill.next() {
        for (name, other) in prefill {
            if other != first {
                differences.push(format!(
                    "{name} and {first_name} started the measured phase from different \
                     prefill states (common digest {} vs {})",
                    hex(&other.common_digest),
                    hex(&first.common_digest)
                ));
            }
        }
    }
    let mut iter = per_engine.iter();
    if let Some((first_name, first)) = iter.next() {
        for (name, other) in iter {
            if other.common_digest != first.common_digest {
                differences.push(format!(
                    "{name} and {first_name} produced different common state digests"
                ));
            }
            for (field, a, b) in [
                ("kv_revision", first.kv_revision, other.kv_revision),
                ("kv_rows", first.kv_rows, other.kv_rows),
                ("history_rows", first.history_rows, other.history_rows),
                ("event_rows", first.event_rows, other.event_rows),
                ("lease_rows", first.lease_rows, other.lease_rows),
                ("lease_key_rows", first.lease_key_rows, other.lease_key_rows),
                ("retry_rows", first.retry_rows, other.retry_rows),
                ("session_rows", first.session_rows, other.session_rows),
                ("policy_rows", first.policy_rows, other.policy_rows),
                ("grant_rows", first.grant_rows, other.grant_rows),
                ("executed_rows", first.executed_rows, other.executed_rows),
                (
                    "retention_floor",
                    first.retention_floor,
                    other.retention_floor,
                ),
            ] {
                if a != b {
                    differences.push(format!("{name}: {field} {b} differs from {first_name} {a}"));
                }
            }
        }
    }
    Semantics {
        equal: differences.is_empty(),
        differences,
        per_engine,
        fixture_digests: BTreeMap::new(),
    }
}

/// Why this set of trials may not produce a cost figure.
fn disqualifications(reports: &[TrialReport], semantics: &Semantics) -> Vec<String> {
    let mut reasons = Vec::new();
    if !semantics.equal {
        reasons.extend(semantics.differences.iter().cloned());
    }
    if let Some(first) = reports.first() {
        let key = first.manifest.comparable_key();
        for report in reports {
            if report.manifest.comparable_key() != key {
                reasons.push(format!(
                    "{}: workload, budgets, build or label differ from {}",
                    report.manifest.engine.name, first.manifest.engine.name
                ));
            }
        }
    }
    for report in reports {
        let engine = &report.manifest.engine.name;
        if report.counters.errors > 0 {
            reasons.push(format!(
                "{engine}: {} failed operations",
                report.counters.errors
            ));
        }
        if report.counters.rejected > 0 {
            reasons.push(format!(
                "{engine}: {} refused operations in a controlled failure-free workload",
                report.counters.rejected
            ));
        }
        if report.observable != report.after_reopen {
            reasons.push(format!(
                "{engine}: the same-engine reopen changed the state"
            ));
        }
        if report.service.count == 0 {
            reasons.push(format!("{engine}: no measured samples"));
        }
    }
    reasons
}

/// Run `repetitions` paired trials of every engine, alternating the order,
/// and produce the comparison report.
pub fn compare(
    workload: &WorkloadSpec,
    engines: &[EngineKind],
    repetitions: u32,
    label: TrialLabel,
    cache_bytes: usize,
    run_root: &RunRoot,
) -> Result<ComparisonReportV1, TrialError> {
    let mut reports: Vec<TrialReport> = Vec::new();
    let mut order = Vec::new();
    for repetition in 0..repetitions {
        // Alternate the order so a warming or cooling host cannot favour
        // whichever engine always runs first.
        let mut sequence: Vec<EngineKind> = engines.to_vec();
        sequence.rotate_left(repetition as usize % engines.len().max(1));
        order.push(sequence.iter().map(|e| e.name().to_owned()).collect());
        for engine in sequence {
            let spec = TrialSpec {
                engine,
                workload: *workload,
                label,
                repetition,
                cache_bytes,
                maintenance: true,
            };
            let report = run_trial(&spec, run_root)?;
            // Each completed trial's manifest and raw samples are written as
            // soon as it finishes, so a later engine or repetition that
            // fails leaves the evidence of the earlier ones in the run root
            // instead of only in memory.
            run_root.write_json(
                &format!("raw/{}-{repetition:03}.json", engine.name()),
                &report,
            )?;
            reports.push(report);
        }
    }
    let semantics = semantics(&reports);
    let reasons = disqualifications(&reports, &semantics);
    let mut summaries = Vec::new();
    for engine in engines {
        let trials: Vec<TrialReport> = reports
            .iter()
            .filter(|r| r.manifest.engine.name == engine.name())
            .cloned()
            .collect();
        let p50: Vec<u64> = trials.iter().filter_map(|t| t.service.p50_ns).collect();
        let p99: Vec<u64> = trials.iter().filter_map(|t| t.service.p99_ns).collect();
        summaries.push(EngineSummary {
            engine: engine.name().to_owned(),
            durable: engine.is_durable(),
            repetitions: trials.len() as u32,
            service_p50_variation: Variation::of(&p50),
            service_p99_variation: Variation::of(&p99),
            trials,
        });
    }
    let verdict = if reasons.is_empty() {
        verdict_from(&summaries)
    } else {
        Verdict::Disqualified { reasons }
    };
    Ok(ComparisonReportV1 {
        schema: COMPARISON_SCHEMA.to_owned(),
        run_id: run_root.run_id().to_owned(),
        workload_digest: hex(&workload.digest()),
        label: label.name().to_owned(),
        repetitions,
        order,
        engines: summaries,
        semantics,
        verdict,
        caveats: CAVEATS.iter().map(|c| (*c).to_owned()).collect(),
    })
}

/// Ratios between durable engines, relative to the redb reference.
fn verdict_from(summaries: &[EngineSummary]) -> Verdict {
    let durable: Vec<&EngineSummary> = summaries.iter().filter(|s| s.durable).collect();
    let Some(baseline) = durable
        .iter()
        .find(|s| s.engine == EngineKind::Redb.name())
        .or_else(|| durable.first())
    else {
        return Verdict::Disqualified {
            reasons: vec![
                "no durable engine ran; a model trial is not a speed baseline".to_owned(),
            ],
        };
    };
    let baseline_name = baseline.engine.clone();
    let (Some(base_p50), Some(base_p99)) = (
        baseline.service_p50_variation.median_ns,
        baseline.service_p99_variation.median_ns,
    ) else {
        return Verdict::Disqualified {
            reasons: vec!["the baseline produced no samples".to_owned()],
        };
    };
    if base_p50 == 0 || base_p99 == 0 {
        return Verdict::Disqualified {
            reasons: vec![
                "the baseline measured zero; the clock resolution is too coarse for \
                           this workload"
                    .to_owned(),
            ],
        };
    }
    let mut p50 = BTreeMap::new();
    let mut p99 = BTreeMap::new();
    for summary in &durable {
        if let Some(v) = summary.service_p50_variation.median_ns {
            p50.insert(summary.engine.clone(), v * 100 / base_p50);
        }
        if let Some(v) = summary.service_p99_variation.median_ns {
            p99.insert(summary.engine.clone(), v * 100 / base_p99);
        }
    }
    Verdict::Comparable {
        baseline: baseline_name,
        service_p50_percent_of_baseline: p50,
        service_p99_percent_of_baseline: p99,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::measure::{Counters, Percentiles, Resources};
    use crate::trial::PhaseTimings;
    use coord_types::identity::Digest32;

    fn observable(digest: u8, revision: u64) -> Observable {
        Observable {
            kv_revision: revision,
            retention_floor: 0,
            kv_rows: 1,
            history_rows: 1,
            event_rows: 1,
            lease_rows: 0,
            lease_key_rows: 0,
            retry_rows: 1,
            retry_floor_rows: 1,
            session_rows: 1,
            policy_rows: 1,
            grant_rows: 0,
            executed_rows: 1,
            common_digest: Digest32([digest; 32]),
        }
    }

    fn report(engine: EngineKind, p50: u64, state: Observable) -> TrialReport {
        let spec = TrialSpec::smoke(engine);
        TrialReport {
            manifest: crate::manifest::StoreExperimentV1 {
                schema: crate::manifest::SCHEMA.to_owned(),
                run_id: "run".to_owned(),
                repetition: 0,
                label: TrialLabel::Primary.name().to_owned(),
                provenance: crate::manifest::Provenance::current(),
                engine: crate::manifest::EngineDescription {
                    name: engine.name().to_owned(),
                    version: engine.version().to_owned(),
                    features: engine.features().to_owned(),
                    durability_profile: engine.durability_profile().to_owned(),
                    layout: engine.layout().to_owned(),
                    collections: 17,
                    cache_bytes: 1 << 20,
                },
                workload: spec.workload,
                workload_digest: spec.workload.digest(),
                limits: spec.limits(),
                environment: crate::manifest::Environment::describe(std::path::Path::new(".")),
                maintenance_enabled: true,
            },
            phases: PhaseTimings::default(),
            after_prefill: state.clone(),
            observable: state.clone(),
            after_reopen: state,
            service: Percentiles {
                count: 10,
                p50_ns: Some(p50),
                p99_ns: Some(p50 * 2),
                ..Percentiles::default()
            },
            scheduled: Percentiles::default(),
            generator_lag: Percentiles::default(),
            publication: Percentiles::default(),
            pinned_read: Percentiles::default(),
            maintenance_step: Percentiles::default(),
            counters: Counters::default(),
            resources: Resources::default(),
            raw_service_ns: vec![p50; 10],
        }
    }

    #[test]
    fn a_semantic_difference_disqualifies_every_speed_claim() {
        let reports = vec![
            report(EngineKind::Redb, 100, observable(1, 7)),
            report(EngineKind::Fjall, 50, observable(2, 7)),
        ];
        let s = semantics(&reports);
        assert!(!s.equal);
        let reasons = disqualifications(&reports, &s);
        assert!(!reasons.is_empty());
        let summaries = vec![
            EngineSummary {
                engine: "redb".to_owned(),
                durable: true,
                repetitions: 1,
                service_p50_variation: Variation::of(&[100]),
                service_p99_variation: Variation::of(&[200]),
                trials: Vec::new(),
            },
            EngineSummary {
                engine: "fjall".to_owned(),
                durable: true,
                repetitions: 1,
                service_p50_variation: Variation::of(&[50]),
                service_p99_variation: Variation::of(&[100]),
                trials: Vec::new(),
            },
        ];
        // The ratio itself is computable, but a run with differences never
        // reaches it.
        assert!(matches!(
            verdict_from(&summaries),
            Verdict::Comparable { .. }
        ));
        assert!(matches!(
            Verdict::Disqualified { reasons },
            Verdict::Disqualified { .. }
        ));
    }

    #[test]
    fn a_different_prefill_state_disqualifies_even_when_the_final_states_agree() {
        let mut reports = vec![
            report(EngineKind::Redb, 100, observable(1, 7)),
            report(EngineKind::Fjall, 50, observable(1, 7)),
        ];
        assert!(semantics(&reports).equal);
        // The measured churn erased the trace of a prefill that went
        // wrong; the final states agree, the starting states do not.
        reports[1].after_prefill = observable(9, 3);
        let s = semantics(&reports);
        assert!(!s.equal);
        assert!(
            s.differences.iter().any(|d| d.contains("prefill")),
            "{:?}",
            s.differences
        );
        assert!(!disqualifications(&reports, &s).is_empty());
        // Two repetitions of one engine must also prefill identically.
        let mut repeated = vec![
            report(EngineKind::Redb, 100, observable(1, 7)),
            report(EngineKind::Redb, 100, observable(1, 7)),
        ];
        repeated[1].after_prefill = observable(9, 3);
        assert!(!semantics(&repeated).equal);
    }

    #[test]
    fn a_refused_or_failed_operation_disqualifies_the_comparison() {
        let mut reports = vec![
            report(EngineKind::Redb, 100, observable(1, 7)),
            report(EngineKind::Fjall, 50, observable(1, 7)),
        ];
        assert!(disqualifications(&reports, &semantics(&reports)).is_empty());
        reports[1].counters.rejected = 1;
        assert!(!disqualifications(&reports, &semantics(&reports)).is_empty());
        reports[1].counters.rejected = 0;
        reports[1].counters.errors = 2;
        assert!(!disqualifications(&reports, &semantics(&reports)).is_empty());
    }

    #[test]
    fn a_silently_changed_budget_disqualifies_the_comparison() {
        let mut reports = vec![
            report(EngineKind::Redb, 100, observable(1, 7)),
            report(EngineKind::Fjall, 50, observable(1, 7)),
        ];
        reports[1].manifest.limits.group_max_records += 1;
        let reasons = disqualifications(&reports, &semantics(&reports));
        assert!(reasons.iter().any(|r| r.contains("budgets")), "{reasons:?}");
    }

    #[test]
    fn the_model_engine_is_never_the_speed_baseline() {
        let summaries = vec![EngineSummary {
            engine: "model".to_owned(),
            durable: false,
            repetitions: 1,
            service_p50_variation: Variation::of(&[10]),
            service_p99_variation: Variation::of(&[20]),
            trials: Vec::new(),
        }];
        assert!(matches!(
            verdict_from(&summaries),
            Verdict::Disqualified { .. }
        ));
    }
}

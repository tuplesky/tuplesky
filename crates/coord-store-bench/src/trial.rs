//! One trial: initialize, prefill, validate, warm, then measure with
//! maintenance active (design Section 17.13).
//!
//! A trial allocates a fresh directory, creates the generation explicitly,
//! and runs the identical logical workload through the common paths. The
//! measured phase drives a scheduled arrival process, so queueing before
//! admission is part of the reported latency rather than hidden by
//! observing only what the engine was ready to accept. Maintenance runs
//! with the workload and unfinished debt is reported. The trial ends with a
//! same-engine reopen that must reproduce the same logical state.

use std::time::Instant;

use coord_storage::GroupLimits;
use coord_storage::compaction::GcBudget;
use coord_storage_fjall::{FjallGeneration, FjallOpenOptions};
use coord_storage_redb::{
    Generation as RedbGeneration, OpenOptions as RedbOpenOptions, StoreIdentity,
};
use coord_store_api::engine::{EngineError, LocalEngine};
use coord_store_api::registry::Collection;
use coord_store_testkit::model::ModelEngine;
use coord_types::identity::Digest32;
use coord_types::ids::*;
use serde::{Deserialize, Serialize};

use crate::driver::{Applied, Domain, DriveError, Observable, SCAN_MAX_BYTES, SCAN_MAX_ROWS};
use crate::manifest::{
    EngineDescription, Environment, Limits, Provenance, SCHEMA, StoreExperimentV1, TrialLabel,
};
use crate::measure::{Counters, Percentiles, ProcessCounters, Resources, Samples, directory_bytes};
use crate::runroot::RunRoot;
use crate::workload::{Op, Workload, WorkloadSpec, generate};

pub use crate::manifest::EngineKind;

/// What a trial runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TrialSpec {
    /// Engine under test.
    pub engine: EngineKind,
    /// Workload.
    pub workload: WorkloadSpec,
    /// Label; only `Primary` is the reviewed configuration.
    pub label: TrialLabel,
    /// Repetition index within the run.
    pub repetition: u32,
    /// Engine read-cache budget.
    pub cache_bytes: usize,
    /// Whether maintenance runs with the measured workload.
    pub maintenance: bool,
}

impl TrialSpec {
    /// A primary trial of the small in-suite workload.
    pub fn smoke(engine: EngineKind) -> TrialSpec {
        TrialSpec {
            engine,
            workload: WorkloadSpec::smoke(),
            label: TrialLabel::Primary,
            repetition: 0,
            cache_bytes: 8 * 1024 * 1024,
            maintenance: true,
        }
    }

    /// The batching, scan and maintenance budgets of this trial. They are
    /// identical for every engine: a comparison may not vary them.
    pub fn limits(&self) -> Limits {
        let group = GroupLimits::default();
        Limits {
            group_max_records: group.max_records,
            group_max_bytes: group.max_bytes,
            group_max_single_batch_bytes: group.max_single_batch_bytes,
            group_max_queued_bytes: group.max_queued_bytes,
            scan_max_rows: SCAN_MAX_ROWS,
            scan_max_bytes: SCAN_MAX_BYTES,
            gc_budget_rows: self.workload.gc_budget_rows,
        }
    }
}

/// Why a trial failed. Failed runs keep their run root and raw output.
#[derive(Debug)]
pub enum TrialError {
    /// The engine generation could not be created or reopened.
    Lifecycle(String),
    /// The workload could not be driven.
    Drive(DriveError),
    /// The run root could not be used.
    RunRoot(crate::runroot::RunRootError),
    /// A same-engine reopen lost or changed durable state.
    ReopenDiverged {
        /// State before the reopen.
        before: Box<Observable>,
        /// State after the reopen.
        after: Box<Observable>,
    },
}

impl std::fmt::Display for TrialError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrialError::Lifecycle(e) => write!(f, "lifecycle: {e}"),
            TrialError::Drive(e) => write!(f, "drive: {e}"),
            TrialError::RunRoot(e) => write!(f, "run root: {e}"),
            TrialError::ReopenDiverged { .. } => {
                write!(f, "the same-engine reopen changed the logical state")
            }
        }
    }
}

impl std::error::Error for TrialError {}

impl From<DriveError> for TrialError {
    fn from(e: DriveError) -> Self {
        TrialError::Drive(e)
    }
}

impl From<crate::runroot::RunRootError> for TrialError {
    fn from(e: crate::runroot::RunRootError) -> Self {
        TrialError::RunRoot(e)
    }
}

/// Timings of the phases that are not part of the measured distribution.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhaseTimings {
    /// Creating the generation and bootstrapping session and policy.
    pub setup_ns: u64,
    /// Prefill.
    pub prefill_ns: u64,
    /// Warmup.
    pub warmup_ns: u64,
    /// The measured phase as a whole.
    pub measured_ns: u64,
    /// Same-engine reopen.
    pub reopen_ns: u64,
}

/// One trial's result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrialReport {
    /// Everything recorded about the run.
    pub manifest: StoreExperimentV1,
    /// Phase timings.
    pub phases: PhaseTimings,
    /// Logical state after prefill, before measurement.
    pub after_prefill: Observable,
    /// Logical state after the measured phase.
    pub observable: Observable,
    /// Logical state after the same-engine reopen.
    pub after_reopen: Observable,
    /// Commit entry to return, per measured operation.
    pub service: Percentiles,
    /// Scheduled arrival to return: queueing included.
    pub scheduled: Percentiles,
    /// How far behind its own schedule the generator ran.
    pub generator_lag: Percentiles,
    /// Return to publication of the revision's events.
    pub publication: Percentiles,
    /// Reads through a pinned snapshot held across churn.
    pub pinned_read: Percentiles,
    /// One bounded maintenance step.
    pub maintenance_step: Percentiles,
    /// Offered, admitted, refused, failed and published work.
    pub counters: Counters,
    /// Process and device counters.
    pub resources: Resources,
    /// Raw measured service samples in arrival order.
    pub raw_service_ns: Vec<u64>,
}

impl TrialReport {
    /// The comparable statistic: median service time of measured
    /// operations, in nanoseconds.
    pub fn service_p50(&self) -> Option<u64> {
        self.service.p50_ns
    }
}

fn identity() -> StoreIdentity {
    StoreIdentity {
        cluster_id: ClusterId([1; 16]),
        domain_id: DomainId([2; 16]),
        replica_id: ReplicaId([3; 16]),
        incarnation: ReplicaIncarnation::new(1).expect("non-zero"),
    }
}

struct Measured {
    phases: PhaseTimings,
    after_prefill: Observable,
    observable: Observable,
    after_reopen: Observable,
    service: Samples,
    scheduled: Samples,
    generator_lag: Samples,
    publication: Samples,
    pinned_read: Samples,
    maintenance_step: Samples,
    counters: Counters,
    logical_bytes: u64,
}

fn wait_until(start: Instant, deadline_ns: u64) -> u64 {
    let now = start.elapsed().as_nanos() as u64;
    if now >= deadline_ns {
        return now - deadline_ns;
    }
    let remaining = deadline_ns - now;
    if remaining > 50_000 {
        std::thread::sleep(std::time::Duration::from_nanos(remaining - 20_000));
    }
    while (start.elapsed().as_nanos() as u64) < deadline_ns {
        std::hint::spin_loop();
    }
    0
}

/// Drive the workload through one already-opened engine.
fn execute<E: LocalEngine>(
    engine: E,
    spec: &TrialSpec,
    workload: &Workload,
    reopen: &mut dyn FnMut(&mut E) -> Result<(), EngineError>,
) -> Result<Measured, TrialError> {
    let mut phases = PhaseTimings::default();
    let mut counters = Counters::default();
    let budget = GcBudget {
        max_examined: spec.workload.gc_budget_rows as usize * 4,
        max_deletes: spec.workload.gc_budget_rows as usize,
    };

    let t = Instant::now();
    let mut domain = Domain::bootstrap(engine, GroupLimits::default())?;
    phases.setup_ns = t.elapsed().as_nanos() as u64;

    let t = Instant::now();
    for op in &workload.prefill {
        record(&mut counters, domain.apply(op)?);
        let (revisions, events) = domain.drain_watch();
        counters.published_revisions += revisions;
        counters.published_events += events;
    }
    phases.prefill_ns = t.elapsed().as_nanos() as u64;
    let after_prefill = domain.observable()?;

    let t = Instant::now();
    for op in &workload.warmup {
        record(&mut counters, domain.apply(op)?);
        domain.drain_watch();
    }
    phases.warmup_ns = t.elapsed().as_nanos() as u64;

    let mut service = Samples::new();
    let mut scheduled = Samples::new();
    let mut generator_lag = Samples::new();
    let mut publication = Samples::new();
    let mut pinned_read = Samples::new();
    let mut maintenance_step = Samples::new();
    let interval = spec.workload.arrival_interval_ns;
    let pinned_every = spec.workload.pinned_read_every as usize;
    let hold = spec.workload.pinned_hold_ops as usize;
    let ops = &workload.measured;
    let start = Instant::now();
    let mut index = 0usize;
    while index < ops.len() {
        if pinned_every > 0
            && index > 0
            && index.is_multiple_of(pinned_every)
            && index + hold <= ops.len()
        {
            // The operations held under the pinned snapshot are measured
            // operations like any other, so maintenance follows each of
            // them exactly as it does outside the group; the held revision
            // is what keeps collection from vanishing under the snapshot.
            // The maintenance timings are collected inside the churn and
            // recorded afterwards, so the pinned read itself excludes them.
            let slice = &ops[index..index + hold];
            let mut inner = Vec::new();
            let mut maintained = Vec::new();
            let (_, pinned) = domain.pinned_read(|d| {
                for (offset, op) in slice.iter().enumerate() {
                    inner.push(one(d, op, start, interval, (index + offset) as u64)?);
                    if spec.maintenance {
                        maintained.push(maintain(d, budget)?);
                    }
                }
                Ok(())
            })?;
            for sample in inner {
                absorb(
                    sample,
                    &mut counters,
                    &mut service,
                    &mut scheduled,
                    &mut generator_lag,
                    &mut publication,
                );
            }
            for step in maintained {
                record_maintenance(step, &mut counters, &mut maintenance_step);
            }
            pinned_read.push(pinned.read_ns);
            counters.pinned_checks += 1;
            index += hold;
            continue;
        }
        let sample = one(&mut domain, &ops[index], start, interval, index as u64)?;
        absorb(
            sample,
            &mut counters,
            &mut service,
            &mut scheduled,
            &mut generator_lag,
            &mut publication,
        );
        if spec.maintenance {
            let step = maintain(&mut domain, budget)?;
            record_maintenance(step, &mut counters, &mut maintenance_step);
        }
        index += 1;
    }
    phases.measured_ns = start.elapsed().as_nanos() as u64;

    let observable = domain.observable()?;
    let t = Instant::now();
    domain.reopen(reopen)?;
    phases.reopen_ns = t.elapsed().as_nanos() as u64;
    let after_reopen = domain.observable()?;
    if after_reopen != observable {
        return Err(TrialError::ReopenDiverged {
            before: Box::new(observable),
            after: Box::new(after_reopen),
        });
    }
    Ok(Measured {
        phases,
        after_prefill,
        observable,
        after_reopen,
        service,
        scheduled,
        generator_lag,
        publication,
        pinned_read,
        maintenance_step,
        counters,
        logical_bytes: domain.logical_bytes(),
    })
}

/// One measured operation.
struct Sample {
    applied: Applied,
    service_ns: u64,
    scheduled_ns: u64,
    lag_ns: u64,
    publication_ns: u64,
    backlog: u64,
    revisions: u64,
    events: u64,
}

fn one<E: LocalEngine>(
    domain: &mut Domain<E>,
    op: &Op,
    start: Instant,
    interval: u64,
    index: u64,
) -> Result<Sample, DriveError> {
    // The scheduled arrival of an open-loop run is fixed in advance, so a
    // slow engine shows up as queueing rather than as fewer arrivals. A
    // closed loop offers the next operation on return, so its arrival is
    // now and its scheduled latency is its service latency.
    let (deadline_ns, lag_ns) = if interval == 0 {
        (start.elapsed().as_nanos() as u64, 0)
    } else {
        let deadline = interval.saturating_mul(index);
        (deadline, wait_until(start, deadline))
    };
    let backlog = if interval == 0 { 0 } else { lag_ns / interval };
    let entry = Instant::now();
    let applied = domain.apply(op)?;
    let service_ns = entry.elapsed().as_nanos() as u64;
    let published = Instant::now();
    let (revisions, events) = domain.drain_watch();
    let publication_ns = published.elapsed().as_nanos() as u64;
    let scheduled_ns = (start.elapsed().as_nanos() as u64).saturating_sub(deadline_ns);
    Ok(Sample {
        applied,
        service_ns,
        scheduled_ns,
        lag_ns,
        publication_ns,
        backlog,
        revisions,
        events,
    })
}

/// One bounded maintenance step: how long it took and whether collection
/// was complete afterwards.
struct MaintenanceStep {
    step_ns: u64,
    done: bool,
}

fn maintain<E: LocalEngine>(
    domain: &mut Domain<E>,
    budget: GcBudget,
) -> Result<MaintenanceStep, DriveError> {
    let t = Instant::now();
    let done = domain.maintenance_step(budget)?;
    Ok(MaintenanceStep {
        step_ns: t.elapsed().as_nanos() as u64,
        done,
    })
}

fn record_maintenance(step: MaintenanceStep, counters: &mut Counters, samples: &mut Samples) {
    samples.push(step.step_ns);
    counters.maintenance_steps += 1;
    if !step.done {
        counters.maintenance_debt_steps += 1;
    }
}

fn absorb(
    sample: Sample,
    counters: &mut Counters,
    service: &mut Samples,
    scheduled: &mut Samples,
    generator_lag: &mut Samples,
    publication: &mut Samples,
) {
    service.push(sample.service_ns);
    scheduled.push(sample.scheduled_ns);
    generator_lag.push(sample.lag_ns);
    publication.push(sample.publication_ns);
    counters.max_backlog = counters.max_backlog.max(sample.backlog);
    counters.published_revisions += sample.revisions;
    counters.published_events += sample.events;
    record(counters, sample.applied);
}

fn record(counters: &mut Counters, applied: Applied) {
    counters.offered += 1;
    match applied {
        Applied::Executed { .. } => counters.admitted += 1,
        Applied::Retained { .. } => counters.retained_retries += 1,
        Applied::Rejected(_) => counters.rejected += 1,
        Applied::Skipped => {}
    }
}

/// Run one trial in a fresh directory under `run_root`.
pub fn run_trial(spec: &TrialSpec, run_root: &RunRoot) -> Result<TrialReport, TrialError> {
    let directory = run_root.engine_dir(spec.engine.name(), spec.repetition)?;
    let workload = generate(&spec.workload);
    let before = ProcessCounters::read();
    let measured = match spec.engine {
        EngineKind::Model => execute(
            ModelEngine::new(),
            spec,
            &workload,
            &mut |engine: &mut ModelEngine| {
                engine.crash_and_reopen();
                Ok(())
            },
        )?,
        EngineKind::Redb => {
            let generation = RedbGeneration::create(
                &directory,
                identity(),
                RedbOpenOptions {
                    cache_bytes: spec.cache_bytes,
                },
            )
            .map_err(|e| TrialError::Lifecycle(format!("{e}")))?;
            let (engine, lock, _manifest) = generation.into_parts();
            let measured = execute(engine, spec, &workload, &mut |engine| engine.reopen())?;
            drop(lock);
            measured
        }
        EngineKind::Fjall => {
            let generation = FjallGeneration::create(
                &directory,
                identity(),
                FjallOpenOptions {
                    cache_bytes: spec.cache_bytes as u64,
                },
            )
            .map_err(|e| TrialError::Lifecycle(format!("{e}")))?;
            let (engine, lock, _manifest) = generation.into_parts();
            let measured = execute(engine, spec, &workload, &mut |engine| engine.reopen())?;
            drop(lock);
            measured
        }
    };
    let after = ProcessCounters::read();
    let mut resources: Resources = before.since(&after, measured.logical_bytes);
    resources.engine_bytes = directory_bytes(&directory);
    let manifest = StoreExperimentV1 {
        schema: SCHEMA.to_owned(),
        run_id: run_root.run_id().to_owned(),
        repetition: spec.repetition,
        label: spec.label.name().to_owned(),
        provenance: Provenance::current(),
        engine: EngineDescription {
            name: spec.engine.name().to_owned(),
            version: spec.engine.version().to_owned(),
            features: spec.engine.features().to_owned(),
            durability_profile: spec.engine.durability_profile().to_owned(),
            layout: spec.engine.layout().to_owned(),
            collections: Collection::ALL.len(),
            cache_bytes: spec.cache_bytes,
        },
        workload: spec.workload,
        workload_digest: spec.workload.digest(),
        limits: spec.limits(),
        environment: Environment::describe(run_root.path()),
        maintenance_enabled: spec.maintenance,
    };
    Ok(TrialReport {
        manifest,
        phases: measured.phases,
        after_prefill: measured.after_prefill,
        observable: measured.observable,
        after_reopen: measured.after_reopen,
        service: measured.service.summary(),
        scheduled: measured.scheduled.summary(),
        generator_lag: measured.generator_lag.summary(),
        publication: measured.publication.summary(),
        pinned_read: measured.pinned_read.summary(),
        maintenance_step: measured.maintenance_step.summary(),
        counters: measured.counters,
        resources,
        raw_service_ns: measured.service.raw().to_vec(),
    })
}

/// Replay an existing `StoreScenarioV1` fixture against one engine in a
/// fresh directory, returning the fixture's logical digest. This is the
/// committed fixture the model and both adapters already share; a trial
/// records its digest in the manifest so a comparison is anchored to it.
pub fn replay_fixture(
    engine: EngineKind,
    scenario: &coord_store_testkit::scenario::StoreScenarioV1,
    run_root: &RunRoot,
    repetition: u32,
    cache_bytes: usize,
) -> Result<(Digest32, bool, u64), TrialError> {
    use coord_store_testkit::scenario::replay;
    let directory = run_root.engine_dir(&format!("fixture-{}", engine.name()), repetition)?;
    let t = Instant::now();
    let outcome = match engine {
        EngineKind::Model => {
            let mut e = ModelEngine::new();
            replay(&mut e, scenario, |e| e.crash_and_reopen())
        }
        EngineKind::Redb => {
            let generation =
                RedbGeneration::create(&directory, identity(), RedbOpenOptions { cache_bytes })
                    .map_err(|e| TrialError::Lifecycle(format!("{e}")))?;
            let (mut engine, lock, _m) = generation.into_parts();
            let outcome = replay(&mut engine, scenario, |e| e.reopen().expect("reopen"));
            drop(engine);
            drop(lock);
            outcome
        }
        EngineKind::Fjall => {
            let generation = FjallGeneration::create(
                &directory,
                identity(),
                FjallOpenOptions {
                    cache_bytes: cache_bytes as u64,
                },
            )
            .map_err(|e| TrialError::Lifecycle(format!("{e}")))?;
            let (mut engine, lock, _m) = generation.into_parts();
            let outcome = replay(&mut engine, scenario, |e| e.reopen().expect("reopen"));
            drop(engine);
            drop(lock);
            outcome
        }
    }
    .map_err(TrialError::Lifecycle)?;
    Ok((
        outcome.digest,
        outcome.matches_oracle && outcome.matches_expected.unwrap_or(false),
        t.elapsed().as_nanos() as u64,
    ))
}

/// The committed `StoreScenarioV1` fixture of the shared conformance kit.
pub fn committed_fixture() -> coord_store_testkit::scenario::StoreScenarioV1 {
    serde_json::from_str(include_str!(
        "../../coord-store-testkit/fixtures/store_scenario_v1.json"
    ))
    .expect("the committed fixture parses")
}

/// Fold the committed fixture's digest into a trial manifest.
pub fn with_fixture_digest(mut report: TrialReport, fixture: Digest32) -> TrialReport {
    report.manifest.provenance.fixture_digest = Some(fixture);
    report
}

//! Versioned replay bundles and schedule minimization (design Section 21.1).
//!
//! A bundle records the format version, the build identity (crate version,
//! generator family, lockfile digest and optional source commit), the
//! complete scenario including the explicit fault schedule, and the expected
//! digests. Replaying under a different version fails clearly instead of
//! claiming reproducibility.

use std::collections::BTreeMap;
use std::path::Path;

use coord_types::identity::Digest32;
use serde::{Deserialize, Serialize};

use crate::network::NetworkConfig;
use crate::oracle::{DurableAckOracle, Violation};
use crate::rng::GENERATOR;
use crate::world::{NodeId, RunReport, StepOutcome, World};

/// Bundle format name.
pub const FORMAT: &str = "coord-sim-replay";
/// Bundle format version.
pub const FORMAT_VERSION: u32 = 1;

/// Which reference actor a scenario runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActorKind {
    /// Correct echo actor.
    DurableEcho,
    /// Faulty echo actor that omits its durable prerequisite.
    EagerEcho,
}

/// Storage model parameters.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Minimum completion delay in ticks.
    pub min_delay: u64,
    /// Maximum completion delay in ticks.
    pub max_delay: u64,
    /// Random batch failure probability in parts per million.
    pub fail_ppm: u32,
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig {
            min_delay: 2,
            max_delay: 8,
            fail_ppm: 0,
        }
    }
}

/// Workload parameters.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Workload {
    /// Number of client requests.
    pub requests: u32,
    /// Minimum gap between requests in ticks.
    pub min_gap: u64,
    /// Maximum gap between requests in ticks.
    pub max_gap: u64,
}

impl Default for Workload {
    fn default() -> Self {
        Workload {
            requests: 20,
            min_gap: 1,
            max_gap: 4,
        }
    }
}

/// An explicit scheduled fault.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Fault {
    /// Crash a node at a tick.
    Crash {
        /// Node.
        node: NodeId,
        /// Tick.
        tick: u64,
    },
    /// Restart a crashed node at a tick.
    Restart {
        /// Node.
        node: NodeId,
        /// Tick.
        tick: u64,
    },
    /// Cut a directed link.
    Cut {
        /// Sender.
        from: NodeId,
        /// Receiver.
        to: NodeId,
        /// Tick.
        tick: u64,
    },
    /// Heal a directed link.
    Heal {
        /// Sender.
        from: NodeId,
        /// Receiver.
        to: NodeId,
        /// Tick.
        tick: u64,
    },
    /// Fail the node's `nth` persist (1-based) with an indeterminate error.
    FailBatch {
        /// Node.
        node: NodeId,
        /// Which persist.
        nth: u64,
    },
}

impl Fault {
    /// Every node the fault names.
    pub fn nodes(&self) -> Vec<NodeId> {
        match self {
            Fault::Crash { node, .. } | Fault::Restart { node, .. } => vec![*node],
            Fault::Cut { from, to, .. } | Fault::Heal { from, to, .. } => vec![*from, *to],
            Fault::FailBatch { node, .. } => vec![*node],
        }
    }

    /// Tick at which the fault is scheduled (batch failures apply on submit).
    pub const fn tick(&self) -> u64 {
        match self {
            Fault::Crash { tick, .. }
            | Fault::Restart { tick, .. }
            | Fault::Cut { tick, .. }
            | Fault::Heal { tick, .. } => *tick,
            Fault::FailBatch { .. } => 0,
        }
    }
}

/// A complete scenario. Everything the world needs is here; nothing is
/// taken from the ambient environment.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Scenario {
    /// Master seed for the named substreams.
    pub seed: [u8; 32],
    /// Actor under test.
    pub actor: ActorKind,
    /// Number of nodes.
    pub nodes: NodeId,
    /// Network parameters.
    pub network: NetworkConfig,
    /// Storage parameters.
    pub storage: StorageConfig,
    /// Workload.
    pub workload: Workload,
    /// Explicit faults.
    pub faults: Vec<Fault>,
    /// Tick budget.
    pub max_ticks: u64,
}

impl Scenario {
    /// Reject configurations the world cannot run. Called by replay before
    /// any scheduling; [`World::new`] panics on an invalid scenario because
    /// programmatic construction is a caller bug, while a loaded bundle
    /// reports [`ReplayError::InvalidScenario`].
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.nodes == 0 {
            return Err("a scenario needs at least one node");
        }
        for fault in &self.faults {
            if fault.nodes().iter().any(|n| *n >= self.nodes) {
                return Err("a fault names a node outside the scenario");
            }
        }
        if self.workload.min_gap > self.workload.max_gap {
            return Err("workload min_gap exceeds max_gap");
        }
        if self.storage.min_delay > self.storage.max_delay {
            return Err("storage min_delay exceeds max_delay");
        }
        Ok(())
    }

    /// A default scenario for `actor` with `nodes` nodes.
    pub fn new(seed: [u8; 32], actor: ActorKind, nodes: NodeId) -> Self {
        Scenario {
            seed,
            actor,
            nodes,
            network: NetworkConfig::default(),
            storage: StorageConfig::default(),
            workload: Workload::default(),
            faults: Vec::new(),
            max_ticks: 10_000,
        }
    }
}

/// Identity of the build that produced a bundle.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildIdentity {
    /// `coord-sim` crate version.
    pub crate_version: String,
    /// RNG generator family and seeding rule.
    pub generator: String,
    /// BLAKE3 digest of `Cargo.lock`, when the workspace lockfile was found.
    pub lock_digest: Option<String>,
    /// Source commit, when provided through `COORD_SIM_GIT_COMMIT`.
    pub git_commit: Option<String>,
}

impl BuildIdentity {
    /// Identity of the running build.
    pub fn current() -> Self {
        let lock_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../Cargo.lock");
        let lock_digest = std::fs::read(lock_path)
            .ok()
            .map(|bytes| blake3::hash(&bytes).to_hex().to_string());
        BuildIdentity {
            crate_version: env!("CARGO_PKG_VERSION").to_owned(),
            generator: GENERATOR.to_owned(),
            lock_digest,
            git_commit: std::env::var("COORD_SIM_GIT_COMMIT")
                .ok()
                .filter(|s| !s.is_empty()),
        }
    }
}

/// Expected outcome recorded with a bundle.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Expected {
    /// Trace digest.
    pub trace_digest: Digest32,
    /// Visible-history digest.
    pub visible_digest: Digest32,
    /// Oracle violations (empty for a passing run).
    pub violations: Vec<(NodeId, Vec<u8>)>,
}

/// A replay bundle.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayBundleV1 {
    /// Format name.
    pub format: String,
    /// Format version.
    pub format_version: u32,
    /// Build identity.
    pub build: BuildIdentity,
    /// Scenario.
    pub scenario: Scenario,
    /// Expected outcome.
    pub expected: Expected,
    /// Free-form note (no secrets).
    pub note: String,
}

/// Why a bundle cannot be replayed here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplayError {
    /// A build/format field differs.
    Incompatible {
        /// Field name.
        field: &'static str,
        /// Value in the bundle.
        expected: String,
        /// Value of this build.
        found: String,
    },
    /// The bundle could not be read or parsed.
    Unreadable(String),
    /// The bundle's scenario is malformed (for example zero nodes with a
    /// workload); running it would abort rather than produce an outcome.
    InvalidScenario(String),
    /// The replay ran but produced a different outcome.
    Diverged {
        /// Report produced now.
        report: Box<RunReport>,
        /// Violations produced now.
        violations: Vec<(NodeId, Vec<u8>)>,
    },
}

impl std::fmt::Display for ReplayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplayError::Incompatible {
                field,
                expected,
                found,
            } => write!(
                f,
                "bundle {field} is {expected:?} but this build has {found:?}; not reproducible here"
            ),
            ReplayError::Unreadable(e) => write!(f, "bundle unreadable: {e}"),
            ReplayError::InvalidScenario(e) => write!(f, "bundle scenario invalid: {e}"),
            ReplayError::Diverged { .. } => {
                f.write_str("replay diverged from the recorded outcome")
            }
        }
    }
}

impl std::error::Error for ReplayError {}

/// Result of running a scenario with the echo oracle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Outcome {
    /// Run report.
    pub report: RunReport,
    /// Oracle violations.
    pub violations: Vec<Violation>,
}

/// Run a scenario with the durable-acknowledgement oracle checking every
/// crashed node and every node at the end.
pub fn run_scenario(scenario: &Scenario) -> Outcome {
    let mut world = World::new(scenario.clone());
    let mut oracle = DurableAckOracle::default();
    let report = world.run(|w, outcome| {
        if let StepOutcome::Crashed(node) = outcome {
            oracle.check(w, *node);
        }
    });
    for node in 0..scenario.nodes {
        oracle.check(&world, node);
    }
    Outcome {
        report,
        violations: oracle.violations().to_vec(),
    }
}

impl ReplayBundleV1 {
    /// Record a bundle for `scenario` from a fresh run.
    pub fn record(scenario: Scenario, note: &str) -> Self {
        let outcome = run_scenario(&scenario);
        ReplayBundleV1 {
            format: FORMAT.to_owned(),
            format_version: FORMAT_VERSION,
            build: BuildIdentity::current(),
            scenario,
            expected: Expected {
                trace_digest: outcome.report.trace_digest,
                visible_digest: outcome.report.visible_digest,
                violations: outcome
                    .violations
                    .iter()
                    .map(|v| (v.node, v.value.clone()))
                    .collect(),
            },
            note: note.to_owned(),
        }
    }

    /// Write as JSON.
    pub fn save(&self, path: &Path) -> Result<(), ReplayError> {
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| ReplayError::Unreadable(e.to_string()))?;
        std::fs::write(path, json).map_err(|e| ReplayError::Unreadable(e.to_string()))
    }

    /// Read from JSON without compatibility checks.
    pub fn load(path: &Path) -> Result<Self, ReplayError> {
        let text =
            std::fs::read_to_string(path).map_err(|e| ReplayError::Unreadable(e.to_string()))?;
        serde_json::from_str(&text).map_err(|e| ReplayError::Unreadable(e.to_string()))
    }

    /// Check that this build can claim to reproduce the bundle.
    pub fn check_compatible(&self, build: &BuildIdentity) -> Result<(), ReplayError> {
        let mismatch = |field, expected: &str, found: &str| ReplayError::Incompatible {
            field,
            expected: expected.to_owned(),
            found: found.to_owned(),
        };
        if self.format != FORMAT {
            return Err(mismatch("format", &self.format, FORMAT));
        }
        if self.format_version != FORMAT_VERSION {
            return Err(mismatch(
                "format_version",
                &self.format_version.to_string(),
                &FORMAT_VERSION.to_string(),
            ));
        }
        if self.build.crate_version != build.crate_version {
            return Err(mismatch(
                "crate_version",
                &self.build.crate_version,
                &build.crate_version,
            ));
        }
        if self.build.generator != build.generator {
            return Err(mismatch(
                "generator",
                &self.build.generator,
                &build.generator,
            ));
        }
        // A missing identity on either side is not a match: a build that
        // cannot prove it used the same lockfile or source cannot claim to
        // reproduce the bundle.
        let shown = |v: &Option<String>| v.clone().unwrap_or_else(|| "<unknown>".to_owned());
        if self.build.lock_digest != build.lock_digest {
            return Err(mismatch(
                "lock_digest",
                &shown(&self.build.lock_digest),
                &shown(&build.lock_digest),
            ));
        }
        if self.build.git_commit != build.git_commit {
            return Err(mismatch(
                "git_commit",
                &shown(&self.build.git_commit),
                &shown(&build.git_commit),
            ));
        }
        Ok(())
    }

    /// Replay under this build: compatibility first, then exact outcome.
    pub fn replay(&self) -> Result<Outcome, ReplayError> {
        self.check_compatible(&BuildIdentity::current())?;
        self.scenario
            .validate()
            .map_err(|e| ReplayError::InvalidScenario(e.to_owned()))?;
        let outcome = run_scenario(&self.scenario);
        let violations: Vec<(NodeId, Vec<u8>)> = outcome
            .violations
            .iter()
            .map(|v| (v.node, v.value.clone()))
            .collect();
        if outcome.report.trace_digest != self.expected.trace_digest
            || outcome.report.visible_digest != self.expected.visible_digest
            || violations != self.expected.violations
        {
            return Err(ReplayError::Diverged {
                report: Box::new(outcome.report),
                violations,
            });
        }
        Ok(outcome)
    }
}

/// Greedily minimize a failing scenario: drop faults one at a time and
/// shrink the workload while the predicate still holds. Deterministic, so
/// the minimized scenario is itself reproducible.
pub fn minimize(mut scenario: Scenario, fails: impl Fn(&Scenario) -> bool) -> Scenario {
    assert!(fails(&scenario), "scenario must fail before minimization");
    let mut progress = true;
    while progress {
        progress = false;
        let mut i = 0;
        while i < scenario.faults.len() {
            let mut candidate = scenario.clone();
            candidate.faults.remove(i);
            if fails(&candidate) {
                scenario = candidate;
                progress = true;
            } else {
                i += 1;
            }
        }
        while scenario.workload.requests > 1 {
            let mut candidate = scenario.clone();
            candidate.workload.requests = scenario.workload.requests.div_ceil(2);
            if fails(&candidate) {
                scenario = candidate;
                progress = true;
            } else {
                break;
            }
        }
        while scenario.workload.requests > 1 {
            let mut candidate = scenario.clone();
            candidate.workload.requests -= 1;
            if fails(&candidate) {
                scenario = candidate;
                progress = true;
            } else {
                break;
            }
        }
    }
    scenario
}

/// Draw counts summary helper for reports.
pub fn draw_summary(report: &RunReport) -> BTreeMap<String, u64> {
    report.draws.clone()
}

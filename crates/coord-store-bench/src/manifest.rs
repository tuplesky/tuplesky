//! `StoreExperimentV1`: what a trial must record before its numbers mean
//! anything (design Sections 17.13 and 17.14).
//!
//! Source and lock identity, fixture and workload digests, engine, version,
//! features, durability profile, collection layout, batching limits,
//! budgets, seeds, host, filesystem, cache condition and repetition are all
//! recorded with the run. Two trials may be compared only when their
//! comparable fields are identical.

use std::path::Path;

use coord_types::identity::Digest32;
use serde::{Deserialize, Serialize};

use crate::workload::{GENERATOR, WorkloadSpec};

/// Schema name of the experiment manifest.
pub const SCHEMA: &str = "store_experiment_v1";

/// Digest of the workspace lockfile this harness was built from; a run
/// records it so nobody compares numbers across resolutions.
pub fn lock_digest() -> Digest32 {
    let lock = include_str!("../../../Cargo.lock");
    Digest32(*blake3::hash(lock.as_bytes()).as_bytes())
}

/// Which state engine a trial ran on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum EngineKind {
    /// The deterministic in-memory model: a correctness reference, never a
    /// durable-engine speed baseline.
    Model,
    /// The production redb reference.
    Redb,
    /// The experimental single-writer Fjall adapter.
    Fjall,
}

impl EngineKind {
    /// Every engine a trial can run on.
    pub const ALL: [EngineKind; 3] = [EngineKind::Model, EngineKind::Redb, EngineKind::Fjall];

    /// Stable lowercase name.
    pub const fn name(self) -> &'static str {
        match self {
            EngineKind::Model => "model",
            EngineKind::Redb => "redb",
            EngineKind::Fjall => "fjall",
        }
    }

    /// Parse a name.
    pub fn parse(name: &str) -> Option<EngineKind> {
        EngineKind::ALL.into_iter().find(|e| e.name() == name)
    }

    /// Whether the engine writes durable bytes at all. The model does not,
    /// so its cost is never a durable-engine baseline.
    pub const fn is_durable(self) -> bool {
        !matches!(self, EngineKind::Model)
    }

    /// Pinned upstream version. `crates/coord-store-bench/tests/manifest.rs`
    /// checks these against the workspace pins so they cannot drift.
    pub const fn version(self) -> &'static str {
        match self {
            EngineKind::Model => "in-tree",
            EngineKind::Redb => "4.2.0",
            EngineKind::Fjall => "3.1.10",
        }
    }

    /// Resolved feature set of the engine crate.
    pub const fn features(self) -> &'static str {
        match self {
            EngineKind::Model => "",
            EngineKind::Redb => "std",
            EngineKind::Fjall => "lz4",
        }
    }

    /// Durability profile the adapter commits under.
    pub const fn durability_profile(self) -> &'static str {
        match self {
            EngineKind::Model => "model-immediate-v1",
            EngineKind::Redb => coord_storage_redb::manifest::PROFILE_NAME,
            EngineKind::Fjall => coord_storage_fjall::PROFILE_NAME,
        }
    }

    /// Physical collection layout.
    pub const fn layout(self) -> &'static str {
        match self {
            EngineKind::Model => "model-map-per-collection",
            EngineKind::Redb => "redb-table-per-collection",
            EngineKind::Fjall => coord_storage_fjall::LAYOUT_NAME,
        }
    }
}

/// How a trial is labelled. Only `Primary` runs carry the reviewed
/// configuration; tuned and sensitivity runs are labelled and never replace
/// it silently.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TrialLabel {
    /// The reviewed primary configuration.
    Primary,
    /// Private engine tunables varied within equivalent budgets.
    Tuned,
    /// A deliberately weaker or different configuration.
    Sensitivity,
}

impl TrialLabel {
    /// Stable lowercase name.
    pub const fn name(self) -> &'static str {
        match self {
            TrialLabel::Primary => "primary",
            TrialLabel::Tuned => "tuned",
            TrialLabel::Sensitivity => "sensitivity",
        }
    }
}

/// Source and build provenance.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Provenance {
    /// Harness crate version.
    pub crate_version: String,
    /// Digest of the workspace lockfile.
    pub lock_digest: Digest32,
    /// Source revision, from `TUPLESKY_SOURCE_REVISION` when the caller
    /// exports it; `None` rather than a guess.
    pub source_revision: Option<String>,
    /// Cargo profile of this build (`debug` numbers are not release
    /// numbers).
    pub build_profile: String,
    /// Pinned workload generator.
    pub generator: String,
    /// Digest of the fixture a replay trial used, when any.
    pub fixture_digest: Option<Digest32>,
}

impl Provenance {
    /// Read the provenance of this build.
    pub fn current() -> Provenance {
        Provenance {
            crate_version: env!("CARGO_PKG_VERSION").to_owned(),
            lock_digest: lock_digest(),
            source_revision: std::env::var("TUPLESKY_SOURCE_REVISION").ok(),
            build_profile: if cfg!(debug_assertions) {
                "debug".to_owned()
            } else {
                "release".to_owned()
            },
            generator: GENERATOR.to_owned(),
            fixture_digest: None,
        }
    }
}

/// The engine configuration of a trial.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EngineDescription {
    /// Engine name.
    pub name: String,
    /// Pinned version.
    pub version: String,
    /// Resolved features.
    pub features: String,
    /// Durability profile.
    pub durability_profile: String,
    /// Physical collection layout.
    pub layout: String,
    /// Registered logical collections.
    pub collections: usize,
    /// Read cache budget handed to the adapter.
    pub cache_bytes: usize,
}

/// Batching, scan and maintenance budgets. Identical for every compared
/// trial; a difference disqualifies the comparison.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Limits {
    /// Batches per durable transaction.
    pub group_max_records: usize,
    /// Target bytes per durable transaction.
    pub group_max_bytes: usize,
    /// Hard bound for one batch.
    pub group_max_single_batch_bytes: usize,
    /// Queued bytes before submissions are refused.
    pub group_max_queued_bytes: usize,
    /// Rows one scan page may return.
    pub scan_max_rows: u32,
    /// Bytes one scan page may return.
    pub scan_max_bytes: u32,
    /// History rows one maintenance step may remove.
    pub gc_budget_rows: u32,
}

/// Host, filesystem and cache conditions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Environment {
    /// Target operating system.
    pub os: String,
    /// Target architecture.
    pub arch: String,
    /// Kernel release, when readable.
    pub kernel: Option<String>,
    /// Available parallelism.
    pub cpus: Option<usize>,
    /// Total memory in bytes, when readable.
    pub total_ram_bytes: Option<u64>,
    /// Filesystem type of the run root, when readable.
    pub filesystem: Option<String>,
    /// Backing device of the run root, when readable.
    pub device: Option<String>,
    /// What the run assumes about caches.
    pub cache_condition: String,
}

impl Environment {
    /// Describe the host and the filesystem holding `run_root`.
    pub fn describe(run_root: &Path) -> Environment {
        let (filesystem, device) = mount_of(run_root);
        Environment {
            os: std::env::consts::OS.to_owned(),
            arch: std::env::consts::ARCH.to_owned(),
            kernel: std::fs::read_to_string("/proc/sys/kernel/osrelease")
                .ok()
                .map(|s| s.trim().to_owned()),
            cpus: std::thread::available_parallelism().ok().map(|n| n.get()),
            total_ram_bytes: total_ram_bytes(),
            filesystem,
            device,
            cache_condition: "warm after the measured trial's own warmup phase; a fresh \
                 directory and a new process do not prove a cold OS page cache"
                .to_owned(),
        }
    }
}

fn total_ram_bytes() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/meminfo").ok()?;
    let line = text.lines().find(|l| l.starts_with("MemTotal"))?;
    let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kb * 1024)
}

/// Longest-prefix mount entry covering `path`.
fn mount_of(path: &Path) -> (Option<String>, Option<String>) {
    let Ok(canonical) = path.canonicalize() else {
        return (None, None);
    };
    let Ok(mounts) = std::fs::read_to_string("/proc/self/mounts") else {
        return (None, None);
    };
    let mut best: Option<(usize, String, String)> = None;
    for line in mounts.lines() {
        let mut fields = line.split_whitespace();
        let (Some(device), Some(mount), Some(fstype)) =
            (fields.next(), fields.next(), fields.next())
        else {
            continue;
        };
        if canonical.starts_with(mount)
            && best.as_ref().is_none_or(|(len, _, _)| mount.len() > *len)
        {
            best = Some((mount.len(), fstype.to_owned(), device.to_owned()));
        }
    }
    match best {
        Some((_, fstype, device)) => (Some(fstype), Some(device)),
        None => (None, None),
    }
}

/// Everything recorded with one trial.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreExperimentV1 {
    /// Schema name.
    pub schema: String,
    /// Run identifier of the owning run root.
    pub run_id: String,
    /// Repetition index within the run.
    pub repetition: u32,
    /// Trial label.
    pub label: String,
    /// Source, lock and generator provenance.
    pub provenance: Provenance,
    /// Engine configuration.
    pub engine: EngineDescription,
    /// Workload specification.
    pub workload: WorkloadSpec,
    /// Digest of the workload specification.
    pub workload_digest: Digest32,
    /// Batching, scan and maintenance budgets.
    pub limits: Limits,
    /// Host and filesystem.
    pub environment: Environment,
    /// Whether engine maintenance ran with the measured workload.
    pub maintenance_enabled: bool,
}

impl StoreExperimentV1 {
    /// The fields two trials must share to be compared: workload, limits,
    /// durability profile, maintenance and provenance. Engine name,
    /// version, layout, cache and host are deliberately outside it.
    pub fn comparable_key(&self) -> Vec<String> {
        vec![
            format!("workload_digest={}", hex(&self.workload_digest)),
            format!("lock_digest={}", hex(&self.provenance.lock_digest)),
            format!("generator={}", self.provenance.generator),
            format!("build_profile={}", self.provenance.build_profile),
            format!("limits={:?}", self.limits),
            format!("maintenance_enabled={}", self.maintenance_enabled),
            format!("label={}", self.label),
        ]
    }
}

fn hex(d: &Digest32) -> String {
    d.0.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn comparable_key_ignores_engine_but_not_workload_or_budgets() {
        let base = StoreExperimentV1 {
            schema: SCHEMA.to_owned(),
            run_id: "r".to_owned(),
            repetition: 0,
            label: TrialLabel::Primary.name().to_owned(),
            provenance: Provenance::current(),
            engine: EngineDescription {
                name: EngineKind::Redb.name().to_owned(),
                version: EngineKind::Redb.version().to_owned(),
                features: EngineKind::Redb.features().to_owned(),
                durability_profile: EngineKind::Redb.durability_profile().to_owned(),
                layout: EngineKind::Redb.layout().to_owned(),
                collections: 17,
                cache_bytes: 1 << 20,
            },
            workload: WorkloadSpec::smoke(),
            workload_digest: WorkloadSpec::smoke().digest(),
            limits: Limits {
                group_max_records: 8,
                group_max_bytes: 1 << 20,
                group_max_single_batch_bytes: 1 << 20,
                group_max_queued_bytes: 1 << 22,
                scan_max_rows: 64,
                scan_max_bytes: 1 << 20,
                gc_budget_rows: 16,
            },
            environment: Environment::describe(Path::new(".")),
            maintenance_enabled: true,
        };
        let mut other_engine = base.clone();
        other_engine.engine.name = EngineKind::Fjall.name().to_owned();
        other_engine.engine.layout = EngineKind::Fjall.layout().to_owned();
        assert_eq!(base.comparable_key(), other_engine.comparable_key());
        let mut changed_budget = base.clone();
        changed_budget.limits.group_max_records = 16;
        assert_ne!(base.comparable_key(), changed_budget.comparable_key());
        let mut changed_workload = base.clone();
        changed_workload.workload.value_bytes += 1;
        changed_workload.workload_digest = changed_workload.workload.digest();
        assert_ne!(base.comparable_key(), changed_workload.comparable_key());
        let mut tuned = base.clone();
        tuned.label = TrialLabel::Tuned.name().to_owned();
        assert_ne!(base.comparable_key(), tuned.comparable_key());
    }
}

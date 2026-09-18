//! Run roots (design Section 17.13).
//!
//! Every trial allocates an absent directory under an experiment
//! directory, marks it as a disposable experiment root and never touches
//! anything else. Allocation refuses a directory that carries a store
//! lifecycle (a `CURRENT` pointer, a manifest or a root lock), so an
//! experiment can never be pointed at service state; removal refuses any
//! directory this harness did not mark. Raw outputs, including those of a
//! failed run, stay in the run root.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Name of the marker that identifies a disposable experiment run root.
pub const MARKER: &str = "EXPERIMENT_RUN_V1";

/// Files that identify a store root; an experiment never allocates inside
/// one and never removes one.
const STORE_MARKERS: [&str; 4] = ["CURRENT", "manifest.v1", "LOCK", "lock"];

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// Why a run root could not be allocated or removed.
#[derive(Debug)]
pub enum RunRootError {
    /// The path carries store lifecycle files.
    LooksLikeStoreRoot(PathBuf),
    /// The run root already exists; a run never overwrites one.
    AlreadyExists(PathBuf),
    /// The directory is not a marked experiment run root.
    NotDisposable(PathBuf),
    /// I/O failure.
    Io(std::io::Error),
}

impl std::fmt::Display for RunRootError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunRootError::LooksLikeStoreRoot(p) => {
                write!(f, "{} carries store lifecycle files", p.display())
            }
            RunRootError::AlreadyExists(p) => write!(f, "{} already exists", p.display()),
            RunRootError::NotDisposable(p) => {
                write!(f, "{} is not a marked experiment run root", p.display())
            }
            RunRootError::Io(e) => write!(f, "io: {e}"),
        }
    }
}

impl std::error::Error for RunRootError {}

impl From<std::io::Error> for RunRootError {
    fn from(e: std::io::Error) -> Self {
        RunRootError::Io(e)
    }
}

/// Contents of the disposable-run marker.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunMarker {
    /// Schema name.
    pub schema: String,
    /// Run identifier.
    pub run_id: String,
    /// Creation time, seconds since the Unix epoch.
    pub created_unix: u64,
}

/// Whether `path` carries store lifecycle files.
pub fn looks_like_store_root(path: &Path) -> bool {
    STORE_MARKERS.iter().any(|m| path.join(m).exists())
}

fn unique_id(label: &str) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or_default();
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{label}-{now:016x}-{:06}-{n:04}", std::process::id())
}

/// One allocated run root.
#[derive(Debug)]
pub struct RunRoot {
    path: PathBuf,
    run_id: String,
}

impl RunRoot {
    /// Allocate an absent run root under `experiment_dir`.
    pub fn allocate(experiment_dir: &Path, label: &str) -> Result<RunRoot, RunRootError> {
        if looks_like_store_root(experiment_dir) {
            return Err(RunRootError::LooksLikeStoreRoot(
                experiment_dir.to_path_buf(),
            ));
        }
        std::fs::create_dir_all(experiment_dir)?;
        let run_id = unique_id(label);
        let path = experiment_dir.join(&run_id);
        if path.exists() {
            return Err(RunRootError::AlreadyExists(path));
        }
        // `create_dir` fails if another process won the race, so two runs
        // can never share a root.
        std::fs::create_dir(&path)?;
        let marker = RunMarker {
            schema: MARKER.to_owned(),
            run_id: run_id.clone(),
            created_unix: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or_default(),
        };
        std::fs::write(
            path.join(MARKER),
            serde_json::to_vec_pretty(&marker).expect("marker encodes"),
        )?;
        Ok(RunRoot { path, run_id })
    }

    /// The run root directory.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The run identifier.
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// An absent directory for one engine's fresh state.
    pub fn engine_dir(&self, engine: &str, repetition: u32) -> Result<PathBuf, RunRootError> {
        let path = self.path.join(format!("{engine}-{repetition:03}"));
        if path.exists() {
            return Err(RunRootError::AlreadyExists(path));
        }
        std::fs::create_dir(&path)?;
        Ok(path)
    }

    /// Write a JSON artifact into the run root.
    pub fn write_json<T: Serialize>(&self, name: &str, value: &T) -> Result<PathBuf, RunRootError> {
        let path = self.path.join(name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(
            &path,
            serde_json::to_vec_pretty(value).map_err(std::io::Error::other)?,
        )?;
        Ok(path)
    }

    /// Remove the run root. Only a marked, non-store directory is removed,
    /// so a service root or an unmarked directory is never deleted.
    pub fn remove(self) -> Result<(), RunRootError> {
        if !self.path.join(MARKER).is_file() {
            return Err(RunRootError::NotDisposable(self.path));
        }
        if looks_like_store_root(&self.path) {
            return Err(RunRootError::LooksLikeStoreRoot(self.path));
        }
        std::fs::remove_dir_all(&self.path)?;
        Ok(())
    }
}

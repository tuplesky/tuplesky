//! Starting the provisioned daemons and waiting until they serve.
//!
//! A started process is not a running domain. `coordd` prints its
//! startup report and only then `phase=live`, so that is what this waits
//! for; a daemon that printed most of a report and stopped is a failure
//! with a reason, and the reason is in the log file this keeps beside
//! it.

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use crate::domain::Provisioned;

/// How long a daemon has to reach a serving state.
const READY: Duration = Duration::from_secs(60);

/// A daemon the harness started.
pub struct Daemon {
    /// The replica it is, hex.
    pub node: String,
    /// Its api-plane listener, as it reported it.
    pub api: String,
    /// Where its output is being written.
    pub log: PathBuf,
    child: Child,
}

impl Daemon {
    /// The process identifier, for a script that has to clean up.
    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    /// Whether it is still running.
    pub fn alive(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// What stopped a domain from coming up.
#[derive(Debug)]
pub enum RunError {
    /// A process could not be started or its output could not be kept.
    Io(std::io::Error),
    /// `coordd init` refused.
    Init {
        /// The replica that could not initialize.
        node: String,
        /// What it said, bounded to its own diagnostics.
        output: String,
    },
    /// A daemon never reached a serving state.
    NotServing {
        /// The replica that did not come up.
        node: String,
        /// Where its output was kept.
        log: PathBuf,
    },
}

impl std::fmt::Display for RunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunError::Io(e) => write!(f, "{e}"),
            RunError::Init { node, output } => {
                write!(f, "node {node} could not initialize its store: {output}")
            }
            RunError::NotServing { node, log } => write!(
                f,
                "node {node} did not reach a serving state; its output is {}",
                log.display()
            ),
        }
    }
}

impl std::error::Error for RunError {}

impl From<std::io::Error> for RunError {
    fn from(e: std::io::Error) -> Self {
        RunError::Io(e)
    }
}

/// Create each node's first store generation, once.
///
/// Deliberate here as it is in the daemon: a node whose state directory
/// already holds a generation is left exactly as it is, because the
/// command that would "repair" a missing store is the command that turns
/// an unmounted volume into a fresh, empty, valid voter.
pub fn initialize(coordd: &Path, provisioned: &Provisioned) -> Result<(), RunError> {
    for node in &provisioned.voters {
        if node.directory.join("state").is_dir() {
            continue;
        }
        let output = Command::new(coordd)
            .arg("--config")
            .arg(&node.config)
            .arg("init")
            .output()?;
        if !output.status.success() {
            return Err(RunError::Init {
                node: node.node.clone(),
                output: String::from_utf8_lossy(&output.stderr).into_owned(),
            });
        }
    }
    Ok(())
}

/// Start every committed voter and wait until each is serving.
///
/// Every one of them, because a committed voter that is not running is a
/// quorum this domain does not have: a three-voter genesis with two
/// processes would answer requests and would be measuring something
/// nobody deploys.
pub fn start_all(coordd: &Path, provisioned: &Provisioned) -> Result<Vec<Daemon>, RunError> {
    let mut running = Vec::new();
    for node in &provisioned.voters {
        running.push(start(coordd, &node.node, &node.config, &node.directory)?);
    }
    Ok(running)
}

fn start(coordd: &Path, node: &str, config: &Path, directory: &Path) -> Result<Daemon, RunError> {
    let log = directory.join("coordd.log");
    let mut sink = std::fs::File::create(&log)?;
    let mut child = Command::new(coordd)
        .arg("--config")
        .arg(config)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    let stdout = child.stdout.take().expect("piped");
    let stderr = child.stderr.take().expect("piped");
    let (tx, rx) = mpsc::channel();
    let (lines, collected) = mpsc::channel();
    let errors = lines.clone();
    std::thread::spawn(move || {
        let mut api = None;
        let mut live = false;
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if let Some(rest) = line.strip_prefix("listening api_quic=") {
                api = Some(rest.to_owned());
            }
            if !live && line.contains("phase=live") {
                live = true;
                let _ = tx.send(api.clone());
            }
            let _ = lines.send(line);
        }
        if !live {
            let _ = tx.send(None);
        }
    });
    std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            let _ = errors.send(line);
        }
    });
    // One writer for both streams, so the log reads in the order the
    // daemon said things.
    std::thread::spawn(move || {
        for line in collected {
            let _ = writeln!(sink, "{line}");
        }
    });

    match rx.recv_timeout(READY) {
        Ok(Some(api)) => Ok(Daemon {
            node: node.to_owned(),
            api,
            log,
            child,
        }),
        _ => {
            let _ = child.kill();
            let _ = child.wait();
            Err(RunError::NotServing {
                node: node.to_owned(),
                log,
            })
        }
    }
}

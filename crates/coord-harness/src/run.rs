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

    /// Wait for it to exit, and say how it did.
    pub fn wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
        self.child.wait()
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Voters that make a quorum of `voters` committed ones: a majority.
pub const fn quorum(voters: usize) -> usize {
    voters / 2 + 1
}

/// What a running domain is, given which of its voters are still up.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Standing {
    /// A quorum is still running, so the domain still serves; a voter
    /// that stopped is reported and the rest are left alone.
    Serving {
        /// Voters still running.
        alive: usize,
        /// Voters the domain was started with.
        of: usize,
    },
    /// Fewer than a quorum remain, so the domain cannot serve and a run
    /// that went on would be measuring an outage.
    Lost {
        /// Voters still running.
        alive: usize,
        /// Voters the domain was started with.
        of: usize,
    },
}

/// Where a domain of `of` voters stands with `alive` of them running.
///
/// Losing a minority is not the end of a run: a regional failover is
/// exactly that loss, and certifying that the survivors keep serving
/// needs the harness to keep them up rather than tear them down with the
/// one that stopped.
pub const fn standing(alive: usize, of: usize) -> Standing {
    if alive >= quorum(of) {
        Standing::Serving { alive, of }
    } else {
        Standing::Lost { alive, of }
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
        initialize_node(coordd, &node.node, &node.config, &node.directory)?;
    }
    Ok(())
}

/// Create one node's first store generation, once.
pub fn initialize_node(
    coordd: &Path,
    node: &str,
    config: &Path,
    directory: &Path,
) -> Result<(), RunError> {
    if directory.join("state").is_dir() {
        return Ok(());
    }
    let output = coordd_in(coordd, config, directory)?.arg("init").output()?;
    if !output.status.success() {
        return Err(RunError::Init {
            node: node.to_owned(),
            output: String::from_utf8_lossy(&output.stderr).into_owned(),
        });
    }
    Ok(())
}

/// `coordd --config <config>`, run where that configuration expects to
/// be run.
///
/// A bundle provisioned for another host names its files relative to
/// its own directory, and `coordd` opens a relative path against its
/// working directory -- not against the configuration file -- so a
/// bundle is run from inside itself, with the binary and the
/// configuration named absolutely so that moving there does not lose
/// them. A single-host configuration is run from wherever this process
/// is, as it always was: its paths are whatever the run directory was
/// given as, and a relative run directory is relative to here.
fn coordd_in(coordd: &Path, config: &Path, directory: &Path) -> std::io::Result<Command> {
    let bundle = std::fs::read_to_string(config)?.contains(crate::domain::BUNDLE_NOTE);
    let mut command;
    if bundle {
        // A bare name is looked up on the search path, which moving does
        // not change; anything with a separator is a path from here.
        let program = if coordd.components().count() > 1 {
            std::path::absolute(coordd)?
        } else {
            coordd.to_path_buf()
        };
        command = Command::new(program);
        command.current_dir(directory);
        command.arg("--config").arg(std::path::absolute(config)?);
    } else {
        command = Command::new(coordd);
        command.arg("--config").arg(config);
    }
    Ok(command)
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
        running.push(start_node(
            coordd,
            &node.node,
            &node.config,
            &node.directory,
        )?);
    }
    Ok(running)
}

/// Start one node from its own directory and wait until it serves.
///
/// Its output is appended to `coordd.log` in that directory rather than
/// replacing it: a voter restarted after it was killed is exactly the
/// run whose earlier output someone will want to read.
pub fn start_node(
    coordd: &Path,
    node: &str,
    config: &Path,
    directory: &Path,
) -> Result<Daemon, RunError> {
    let log = directory.join("coordd.log");
    let mut sink = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log)?;
    let mut child = coordd_in(coordd, config, directory)?
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

#[cfg(test)]
mod tests {
    use super::{Standing, quorum, standing};

    /// Losing one voter of three leaves a quorum, so the harness keeps
    /// the other two serving; losing a second does not, so it stops.
    #[test]
    fn a_minority_loss_keeps_serving_and_a_majority_loss_stops() {
        assert_eq!(quorum(3), 2);
        assert_eq!(standing(3, 3), Standing::Serving { alive: 3, of: 3 });
        assert_eq!(standing(2, 3), Standing::Serving { alive: 2, of: 3 });
        assert_eq!(standing(1, 3), Standing::Lost { alive: 1, of: 3 });
        // One voter is its own quorum, and losing it is losing the domain.
        assert_eq!(standing(1, 1), Standing::Serving { alive: 1, of: 1 });
        assert_eq!(standing(0, 1), Standing::Lost { alive: 0, of: 1 });
        // Five tolerate two.
        assert_eq!(standing(3, 5), Standing::Serving { alive: 3, of: 5 });
        assert_eq!(standing(2, 5), Standing::Lost { alive: 2, of: 5 });
    }
}

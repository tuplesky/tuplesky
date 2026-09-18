//! Recovery reports and Sync selection (design Sections 4.8-4.9; prototype
//! `swift/recovery.go`: `fillNewLeaderAckN`, `handleNewLeaderAckNs`,
//! `handleSync`).
//!
//! A report summarizes one replica's protocol state at its durable cut for
//! the new ballot: the ballot it last synchronized (`cballot`) and, per
//! command, phase and dependencies. The new leader selects, among a
//! majority of reports, those with the highest synchronized ballot and
//! adopts every ACCEPT/COMMIT command they carry (source rule). Selection
//! is a function of the set of reports, never of their arrival order, and
//! it never merges by highest phase across ballots. Eligible accepted
//! candidates must agree; an incompatible pair stops recovery with the
//! evidence. Commands only pre-accepted are re-proposed, never fabricated
//! as accepted no-ops.

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::vec::Vec;

use coord_types::CommandId;
use coord_types::ids::{Ballot, ReplicaId};
use serde::{Deserialize, Serialize};

use crate::phase::Phase;
use crate::quorum::BallotConfiguration;
use crate::vote::same_set;

/// One command in a recovery report.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ReportEntry {
    /// Command.
    pub command: CommandId,
    /// Phase at the cut.
    pub phase: Phase,
    /// Dependencies at the cut.
    pub deps: Vec<CommandId>,
    /// Whether the payload is durably known (a placeholder is not).
    pub payload_present: bool,
}

/// A replica's report for a new ballot (`MNewLeaderAckN`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryReport {
    /// Reporting replica.
    pub replica: ReplicaId,
    /// The new ballot being promised.
    pub ballot: Ballot,
    /// Ballot last synchronized by the replica (`cballot`).
    pub committed_ballot: Ballot,
    /// Commands at the cut.
    pub entries: Vec<ReportEntry>,
}

/// One command of a Sync decision.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SyncEntry {
    /// Command.
    pub command: CommandId,
    /// ACCEPT or COMMIT.
    pub phase: Phase,
    /// Dependencies.
    pub deps: Vec<CommandId>,
}

/// The selected recovery result (`MSync`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncDecision {
    /// New ballot.
    pub ballot: Ballot,
    /// Highest synchronized ballot among the reports (the source of state).
    pub source_ballot: Ballot,
    /// Adopted commands, keyed by identity.
    pub entries: BTreeMap<CommandId, SyncEntry>,
    /// Commands seen only below ACCEPT or only in lower-ballot reports:
    /// re-proposed under the new ballot.
    pub reproposed: BTreeSet<CommandId>,
}

/// Why no Sync was selected.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecoveryError {
    /// A report from a non-voter.
    NotAVoter {
        /// Reporter.
        replica: ReplicaId,
    },
    /// Two reports from one replica.
    DuplicateReport {
        /// Reporter.
        replica: ReplicaId,
    },
    /// A report for another ballot.
    WrongBallot {
        /// Reporter.
        replica: ReplicaId,
    },
    /// A synchronized ballot from another epoch.
    EpochMismatch {
        /// Reporter.
        replica: ReplicaId,
    },
    /// Fewer than a majority of reports.
    InsufficientReports {
        /// Reports.
        have: usize,
        /// Majority.
        need: usize,
    },
    /// An ACCEPT/COMMIT entry without a durable payload.
    HalfInitialized {
        /// Reporter.
        replica: ReplicaId,
        /// Command.
        command: CommandId,
    },
    /// Two eligible accepted (or committed) candidates disagree.
    IncompatibleAccepted {
        /// Command.
        command: CommandId,
        /// One candidate's dependencies.
        first: Vec<CommandId>,
        /// The other's.
        second: Vec<CommandId>,
    },
}

/// Select the Sync result from `reports` for the ballot of `config`.
pub fn select(
    config: &BallotConfiguration,
    reports: &[RecoveryReport],
) -> Result<SyncDecision, RecoveryError> {
    let mut seen = BTreeSet::new();
    for r in reports {
        if !config.is_voter(&r.replica) {
            return Err(RecoveryError::NotAVoter { replica: r.replica });
        }
        if !seen.insert(r.replica) {
            return Err(RecoveryError::DuplicateReport { replica: r.replica });
        }
        if r.ballot != config.ballot() {
            return Err(RecoveryError::WrongBallot { replica: r.replica });
        }
        if r.committed_ballot.epoch != config.epoch() {
            return Err(RecoveryError::EpochMismatch { replica: r.replica });
        }
        for e in &r.entries {
            if e.phase >= Phase::Accept && !e.payload_present {
                return Err(RecoveryError::HalfInitialized {
                    replica: r.replica,
                    command: e.command,
                });
            }
        }
    }
    if reports.len() < config.slow_size() {
        return Err(RecoveryError::InsufficientReports {
            have: reports.len(),
            need: config.slow_size(),
        });
    }
    // Source rule: only the reports at the highest synchronized ballot
    // supply state (prototype `handleNewLeaderAckNs`: `U`, `maxCbal`).
    let source_ballot = reports
        .iter()
        .map(|r| r.committed_ballot)
        .max_by(|a, b| a.compare_same_epoch(b).expect("same epoch"))
        .expect("at least one report");
    let mut entries: BTreeMap<CommandId, SyncEntry> = BTreeMap::new();
    let mut reproposed = BTreeSet::new();
    // Deterministic order: by reporter identity, then by command; the
    // result does not depend on it because merging is by phase class with
    // an agreement check, not by last writer.
    let mut sorted: Vec<&RecoveryReport> = reports.iter().collect();
    sorted.sort_by_key(|r| r.replica);
    for r in sorted {
        let at_source = r.committed_ballot == source_ballot;
        for e in &r.entries {
            if !at_source || e.phase < Phase::Accept {
                reproposed.insert(e.command);
                continue;
            }
            let phase = if e.phase >= Phase::Commit {
                Phase::Commit
            } else {
                Phase::Accept
            };
            match entries.get_mut(&e.command) {
                None => {
                    entries.insert(
                        e.command,
                        SyncEntry {
                            command: e.command,
                            phase,
                            deps: e.deps.clone(),
                        },
                    );
                }
                Some(existing) => {
                    if !same_set(&existing.deps, &e.deps) {
                        return Err(RecoveryError::IncompatibleAccepted {
                            command: e.command,
                            first: existing.deps.clone(),
                            second: e.deps.clone(),
                        });
                    }
                    if phase > existing.phase {
                        existing.phase = phase;
                    }
                }
            }
        }
    }
    for c in entries.keys() {
        reproposed.remove(c);
    }
    Ok(SyncDecision {
        ballot: config.ballot(),
        source_ballot,
        entries,
        reproposed,
    })
}

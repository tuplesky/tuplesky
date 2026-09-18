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
//!
//! Possible fast decisions (task-28; design Sections 4.2, 4.9): with the
//! fixed C2 fast set of the source ballot, a fast decision needs every
//! member's path evidence to equal the leader's. Any majority of reports
//! contains a member of that set, so when the source leader is not among
//! the reports, a command every reporting fast-set member pre-accepted
//! with the same path may have been learned fast; it is adopted with that
//! order (and its prefix must be consistent with it), never re-proposed
//! with a different one. A fast-set member that never saw the command, or
//! saw a different path, proves no fast decision happened. This is the
//! Fast Paxos recovery rule instantiated for C2 (intersection of one
//! member), not a generic phase priority.

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::vec::Vec;

use coord_types::CommandId;
use coord_types::identity::Digest32;
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
    /// Path evidence at the cut: the replica's own for a pre-accepted
    /// command, the leader's once adopted (task-28 possible-fast-decision
    /// recovery compares them).
    pub path: Digest32,
    /// Conflict keys of the payload.
    pub keys: Vec<Vec<u8>>,
    /// Whether the payload is durably known (a placeholder is not).
    pub payload_present: bool,
}

/// A replica's report for a new ballot (`MNewLeaderAckN`).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
    /// The leader's path evidence for the command (installed with it so
    /// a later recovery compares against the same evidence).
    pub path: Digest32,
}

/// The selected recovery result (`MSync`).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
        if r.ballot != config.ballot {
            return Err(RecoveryError::WrongBallot { replica: r.replica });
        }
        if r.committed_ballot.epoch != config.epoch {
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
                            path: e.path,
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
    possible_fast_decisions(config, source_ballot, reports, &mut entries);
    for c in entries.keys() {
        reproposed.remove(c);
    }
    Ok(SyncDecision {
        ballot: config.ballot,
        source_ballot,
        entries,
        reproposed,
    })
}

/// Adopt the commands that may have been learned fast under the source
/// ballot (module documentation). `entries` already holds the ACCEPT and
/// COMMIT state of the source reports, whose path evidence is the source
/// leader's.
///
/// A command `c` is a candidate when every reporting member of the source
/// fast set pre-accepted it with the same path. It is adopted, with that
/// order, when the members' order is consistent with everything the
/// leader is known to have ordered: every adopted command conflicting
/// with `c` precedes it in the member's dependency closure with the
/// leader's path (a learned command the leader ordered after `c` would
/// have carried `c` as an accepted dependency, so `c` would not be a
/// candidate), and every unadopted command in that closure is itself a
/// candidate. Anything else proves the member's path differs from the
/// leader's, so no fast decision was possible and the command is
/// re-proposed.
fn possible_fast_decisions(
    config: &BallotConfiguration,
    source_ballot: Ballot,
    reports: &[RecoveryReport],
    entries: &mut BTreeMap<CommandId, SyncEntry>,
) {
    let source_leader = source_ballot.leader;
    if reports
        .iter()
        .any(|r| r.replica == source_leader && r.committed_ballot == source_ballot)
    {
        // The source leader's own rows are authoritative: a command it
        // never proposed cannot have been learned fast.
        return;
    }
    // The fast set of the source ballot (the default rule until the
    // retained operator quorum table of task-m01).
    let Ok(source_config) =
        BallotConfiguration::c2_default(config.epoch, source_ballot, config.voters().clone())
    else {
        return;
    };
    let fast_reporters: Vec<&RecoveryReport> = reports
        .iter()
        .filter(|r| {
            r.committed_ballot == source_ballot
                && r.replica != source_leader
                && source_config.fast_set().contains(&r.replica)
        })
        .collect();
    let Some(first) = fast_reporters.first() else {
        return;
    };
    fn record<'a>(r: &'a RecoveryReport, c: &CommandId) -> Option<&'a ReportEntry> {
        r.entries.iter().find(|e| e.command == *c)
    }
    // Candidates: pre-accepted by every reporting fast-set member with the
    // same path evidence, and not already adopted.
    let mut candidates: BTreeMap<CommandId, ReportEntry> = BTreeMap::new();
    for e in &first.entries {
        if e.phase != Phase::PreAccept || !e.payload_present || entries.contains_key(&e.command) {
            continue;
        }
        let agreed = fast_reporters.iter().all(|r| {
            record(r, &e.command).is_some_and(|o| {
                o.phase == Phase::PreAccept && o.path == e.path && o.payload_present
            })
        });
        if agreed {
            candidates.insert(e.command, e.clone());
        }
    }
    // The member's dependency closure of a command (over its own records).
    let closure = |c: &CommandId| -> BTreeSet<CommandId> {
        let mut seen = BTreeSet::new();
        let mut stack: Vec<CommandId> =
            record(first, c).map(|e| e.deps.clone()).unwrap_or_default();
        while let Some(d) = stack.pop() {
            if seen.insert(d)
                && let Some(e) = record(first, &d)
            {
                stack.extend(e.deps.iter().copied());
            }
        }
        seen
    };
    let conflicts = |a: &[Vec<u8>], b: &[Vec<u8>]| a.iter().any(|k| b.contains(k));
    loop {
        let before = candidates.len();
        let keys: Vec<CommandId> = candidates.keys().copied().collect();
        for c in keys {
            let entry = candidates[&c].clone();
            let prefix = closure(&c);
            let ordered_after_adopted = entries.values().all(|adopted| {
                let a = record(first, &adopted.command);
                !conflicts(&entry.keys, a.map_or(&[][..], |a| &a.keys))
                    || (prefix.contains(&adopted.command)
                        && a.is_some_and(|a| a.path == adopted.path))
            });
            let prefix_decided = prefix
                .iter()
                .all(|d| entries.contains_key(d) || candidates.contains_key(d));
            if !ordered_after_adopted || !prefix_decided {
                candidates.remove(&c);
            }
        }
        if candidates.len() == before {
            break;
        }
    }
    for (c, e) in candidates {
        entries.insert(
            c,
            SyncEntry {
                command: c,
                phase: Phase::Accept,
                deps: e.deps,
                path: e.path,
            },
        );
    }
}

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
    /// Per-key path digests at the cut, the components the combined
    /// `path` is built from. A Sync installs these, not only the combined
    /// digest, so a replica adopting the selected order realigns its own
    /// per-key logs to it and later commands derive their dependencies
    /// from the selected tail.
    pub paths: Vec<(Vec<u8>, Digest32)>,
    /// Leader sequence number the per-key digests were synchronized at
    /// (zero when no leader order has been recorded for the command).
    pub seqnum: u64,
    /// Conflict keys of the payload.
    pub keys: Vec<Vec<u8>>,
    /// Whether the payload is durably known (a placeholder is not).
    pub payload_present: bool,
    /// The admission digest the reporter holds the command under: its
    /// record's, or, for a selected entry whose payload has not arrived,
    /// the one its Sync named (task-d14). `None` only for a Sync entry
    /// that named none.
    pub admission: Option<Digest32>,
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
    /// The per-key digests the combined `path` is built from: adopting
    /// the entry installs these into the record and synchronizes the
    /// per-key logs, so the replica's derived tails follow the selected
    /// order rather than its own pre-accept one.
    pub paths: Vec<(Vec<u8>, Digest32)>,
    /// Leader sequence number the per-key digests were synchronized at.
    pub seqnum: u64,
    /// The admission digest the command was accepted under (task-d14).
    ///
    /// One command is one set of attested facts: a vote under another
    /// digest than the ones counted is refused, so a decision has one.
    /// Each presentation of a request mints its own receipt, though, and
    /// a voter may hold the command under a presentation other than the
    /// one a quorum accepted. The selection names the accepted one, so
    /// the candidate re-proposes under it and every voter installs and
    /// executes under it. `None` when no reporter named any, which only
    /// a Sync entry without a digest can cause.
    pub admission: Option<Digest32>,
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
    /// re-proposed under the new ballot, as many as fit beside the
    /// entries within [`MAX_REPORT_ENTRIES`] (task-d24).
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
    /// Two eligible accepted (or committed) candidates name different
    /// admission facts for one command: a second decision, never merged
    /// (task-d14).
    IncompatibleAdmission {
        /// Command.
        command: CommandId,
        /// One candidate's admission digest.
        first: Digest32,
        /// The other's.
        second: Digest32,
    },
    /// A report carrying more entries than [`max_report_entries`] allows
    /// (task-d20). Another voter's is set aside while a majority remains
    /// without it; this is the campaign's failure when it is the
    /// candidate's own, or when every voter reported and too few fit.
    ReportTooLarge {
        /// Reporter.
        replica: ReplicaId,
        /// Entries it carried.
        entries: usize,
        /// The most a report may carry.
        limit: usize,
    },
    /// A report announcing more pages than a report of
    /// [`MAX_REPORT_ENTRIES`] takes, and one more (task-d24 review). It is
    /// past the bound however its pages come out, so it is set aside as
    /// one too large is, and it is the campaign's failure, by name, when
    /// every voter answered and too few fit. Before, each of its pages was
    /// refused and the campaign waited for the election's ceiling.
    ReportPastPages {
        /// Reporter.
        replica: ReplicaId,
        /// Pages it announced.
        pages: u32,
        /// The most a report may announce.
        limit: u32,
    },
    /// A selection whose Sync does not fit the row it is bound in or the
    /// frame it is published in (task-d20): the campaign is refused
    /// rather than the process ended mid-election.
    SyncTooLarge {
        /// Entries selected.
        entries: usize,
        /// Encoded size of the Sync.
        bytes: usize,
        /// The smaller of the row and frame limits.
        limit: usize,
    },
}

/// The most entries a recovery report may carry from a replica whose
/// command table holds `capacity` records (task-d20): its live records
/// and its retirement window, each at most the capacity.
pub const fn max_report_entries(capacity: usize) -> usize {
    capacity.saturating_mul(2)
}

/// The largest command table a voter may be configured with (task-d20):
/// five disjoint reports of [`max_report_entries`] at this capacity give a
/// Sync that fits its row (`coord_daemon`'s
/// `the_largest_table_gives_a_sync_that_fits_a_row_and_a_frame`).
pub const MAX_TABLE_CAPACITY: usize = 1000;

/// The most entries a campaign takes a report with (task-d20).
///
/// The bound is the domain's, not the reporter's own table: voters of one
/// domain may be configured with different capacities, and a follower
/// with a small table legitimately holds more than twice it when a leader
/// with a larger one orders commands past its limit (Codex review). A
/// voter that holds more -- far behind, with a large selection still to
/// install -- reports it all; its report is set aside rather than
/// selected over.
pub const MAX_REPORT_ENTRIES: usize = max_report_entries(MAX_TABLE_CAPACITY);

/// The order a new leader re-proposes a selection's entries in: each
/// command after every dependency that is itself an entry (task-d21).
///
/// A cycle among the entries is not reachable (see
/// `docs/tuplesky-impl-notes.md`, "A recovery cycle is an invariant
/// violation"): every entry is an acceptance or commit whose dependencies
/// were at least accepted where it was, a commit's only at commit, and
/// every acceptance at the source ballot carries its leader's order. So a
/// cycle means an invariant the protocol rests on was broken, and it is
/// returned as `Err` with every entry that could not be ordered, the
/// cycle and whatever depends on it, in identity order. Nothing is
/// guessed: no order of a cycle keeps every member's dependencies.
pub fn entry_order(decision: &SyncDecision) -> Result<Vec<CommandId>, Vec<CommandId>> {
    // The order is that of placing, pass after pass in identity order,
    // every entry whose entry dependencies are placed: an entry goes in
    // the pass after its latest dependency's, or in the same one when
    // that dependency comes first by identity. Computed here in one walk
    // of the dependency graph rather than one scan of every entry per
    // pass (review): a candidate asks this again at each step of its
    // payload fetches, and a selection of a few thousand entries in long
    // chains made that quadratic per step.
    let entries = &decision.entries;
    let mut unplaced: BTreeMap<CommandId, usize> = BTreeMap::new();
    let mut dependents: BTreeMap<CommandId, Vec<CommandId>> = BTreeMap::new();
    let mut ready: Vec<CommandId> = Vec::new();
    for (c, e) in entries {
        let deps: BTreeSet<CommandId> = e
            .deps
            .iter()
            .filter(|d| entries.contains_key(d))
            .copied()
            .collect();
        if deps.is_empty() {
            ready.push(*c);
        }
        unplaced.insert(*c, deps.len());
        for d in deps {
            dependents.entry(d).or_default().push(*c);
        }
    }
    let mut pass: BTreeMap<CommandId, usize> = BTreeMap::new();
    while let Some(d) = ready.pop() {
        let at = pass.get(&d).copied().unwrap_or(1);
        pass.insert(d, at);
        for c in dependents.remove(&d).unwrap_or_default() {
            let after = if d < c { at } else { at + 1 };
            let p = pass.entry(c).or_insert(1);
            *p = (*p).max(after);
            let left = unplaced.get_mut(&c).expect("an entry");
            *left -= 1;
            if *left == 0 {
                ready.push(c);
            }
        }
    }
    let stuck: Vec<CommandId> = unplaced
        .iter()
        .filter(|(_, left)| **left > 0)
        .map(|(c, _)| *c)
        .collect();
    if !stuck.is_empty() {
        return Err(stuck);
    }
    let mut order: Vec<(usize, CommandId)> = pass.into_iter().map(|(c, p)| (p, c)).collect();
    order.sort_unstable();
    Ok(order.into_iter().map(|(_, c)| c).collect())
}

/// Select the Sync result from `reports` for the ballot of `config`.
pub fn select(
    config: &BallotConfiguration,
    reports: &[RecoveryReport],
) -> Result<SyncDecision, RecoveryError> {
    select_with(config, reports, |_, _| false)
}

/// [`select`], where `supplied` names the commands the candidate can
/// supply itself beyond what its report says: it holds their payloads, or
/// executed them (task-d12). A report leaves out what its replica executed
/// long ago, the candidate's own included, so a behind voter's acceptance
/// of such a command otherwise reads as one nobody can supply. The
/// candidate that executed it knows it was decided; the selection carries
/// it, and the candidate marks it committed before binding
/// ([`crate::Campaign::commit_executed`]).
///
/// `supplied` is asked with the admission digest the reporters name for
/// the command, if any: a payload held under other facts supplies nothing
/// the selection can install (task-d14).
pub fn select_with(
    config: &BallotConfiguration,
    reports: &[RecoveryReport],
    supplied: impl Fn(&CommandId, Option<Digest32>) -> bool,
) -> Result<SyncDecision, RecoveryError> {
    select_from(config, reports, supplied, |_| None)
}

/// [`select_with`], where `source` gives the configuration the source
/// ballot ran under, if the candidate knows it (task-d31). Its fast set
/// is the one a fast decision of the source ballot was made by, so the
/// possible-fast rule is applied to it; a ballot whose configuration is
/// not known ran under the default fast set, as every production path
/// builds (`BallotConfiguration::c2_default`).
pub fn select_from(
    config: &BallotConfiguration,
    reports: &[RecoveryReport],
    supplied: impl Fn(&CommandId, Option<Digest32>) -> bool,
    source_configuration: impl Fn(&Ballot) -> Option<BallotConfiguration>,
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
    }
    // A command accepted at the cut whose payload no reporter holds is a
    // dead end: the selection would order state nobody can execute. The
    // check is per command across the whole set rather than per report,
    // because a replica that holds a durable selection while its payload
    // transfer is still in flight reports the selection honestly, and
    // another reporter usually has the payload. Failing the campaign on
    // that one report would stop recovery exactly when it is needed;
    // omitting the entry would lose the selected command.
    let mut accepted_somewhere: BTreeSet<CommandId> = BTreeSet::new();
    let mut with_payload: BTreeSet<CommandId> = BTreeSet::new();
    // The facts the selection will name for a command come from the
    // copies it selects from: an acceptance at the source ballot, or a
    // commit at any. An acceptance below the source is re-proposed and
    // names nothing (task-d14).
    let source = reports
        .iter()
        .map(|r| r.committed_ballot)
        .max_by(|a, b| a.compare_same_epoch(b).expect("same epoch"));
    let mut named: BTreeMap<CommandId, Digest32> = BTreeMap::new();
    let mut by_replica: Vec<&RecoveryReport> = reports.iter().collect();
    by_replica.sort_by_key(|r| r.replica);
    for r in &by_replica {
        for e in &r.entries {
            if e.phase >= Phase::Accept {
                accepted_somewhere.insert(e.command);
                let eligible = e.phase >= Phase::Commit || Some(r.committed_ballot) == source;
                if eligible && let Some(d) = e.admission {
                    // Two eligible copies under different facts are two
                    // decisions, whichever of them has a payload: stopped
                    // here, before the payload check could set one of the
                    // reports aside as half-initialized and leave the
                    // other to be bound.
                    match named.get(&e.command) {
                        Some(first) if *first != d => {
                            return Err(RecoveryError::IncompatibleAdmission {
                                command: e.command,
                                first: *first,
                                second: d,
                            });
                        }
                        Some(_) => {}
                        None => {
                            named.insert(e.command, d);
                        }
                    }
                }
            }
        }
    }
    // A payload supplies the command only under the facts the reporters
    // name for it: one under other facts is another presentation, and
    // fetching it would not let the candidate bind (task-d14).
    for r in &by_replica {
        for e in &r.entries {
            if e.phase >= Phase::Accept
                && e.payload_present
                && named
                    .get(&e.command)
                    .is_none_or(|d| e.admission == Some(*d))
            {
                with_payload.insert(e.command);
            }
        }
    }
    for c in &accepted_somewhere {
        if !with_payload.contains(c) && supplied(c, named.get(c).copied()) {
            with_payload.insert(*c);
        }
    }
    if let Some(command) = accepted_somewhere.difference(&with_payload).next() {
        let replica = reports
            .iter()
            .find(|r| r.entries.iter().any(|e| e.command == *command))
            .map_or(reports[0].replica, |r| r.replica);
        return Err(RecoveryError::HalfInitialized {
            replica,
            command: *command,
        });
    }
    if reports.len() < config.slow_size() {
        return Err(RecoveryError::InsufficientReports {
            have: reports.len(),
            need: config.slow_size(),
        });
    }
    // Source rule: only the reports at the highest synchronized ballot
    // supply state (prototype `handleNewLeaderAckNs`: `U`, `maxCbal`) --
    // except their commits. A commit is a quorum's acceptance of one
    // dependency set, final whatever ballot it was reached in; the source
    // rule chooses among acceptances and never discards a decision
    // (task-d12). A report below the source ballot supplies its entries at
    // COMMIT, executed-as-committed ones included; the rest of it is
    // re-proposed.
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
    // Entries whose path evidence so far is a below-source report's: an
    // at-source copy's replaces it, since sequence numbers and paths are a
    // ballot's own and only the source ballot's are the leader's order.
    let mut from_below: BTreeSet<CommandId> = BTreeSet::new();
    for r in sorted {
        let at_source = r.committed_ballot == source_ballot;
        for e in &r.entries {
            if e.phase < Phase::Accept || (!at_source && e.phase < Phase::Commit) {
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
                    if !at_source {
                        from_below.insert(e.command);
                    }
                    entries.insert(
                        e.command,
                        SyncEntry {
                            command: e.command,
                            phase,
                            deps: e.deps.clone(),
                            path: e.path,
                            paths: e.paths.clone(),
                            seqnum: e.seqnum,
                            admission: e.admission,
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
                    match (existing.admission, e.admission) {
                        (Some(first), Some(second)) if first != second => {
                            return Err(RecoveryError::IncompatibleAdmission {
                                command: e.command,
                                first,
                                second,
                            });
                        }
                        (None, Some(named)) => existing.admission = Some(named),
                        _ => {}
                    }
                    if phase > existing.phase {
                        existing.phase = phase;
                    }
                    // The reporters of one source ballot adopted one
                    // leader order, so their per-key digests agree; take
                    // the most recently synchronized copy, which is a
                    // maximum and therefore independent of report order.
                    // An at-source copy outranks a below-source one.
                    let existing_below = from_below.contains(&e.command);
                    let replaces = match (at_source, existing_below) {
                        (true, true) => true,
                        (false, false) => false,
                        _ => e.seqnum > existing.seqnum,
                    };
                    if at_source {
                        from_below.remove(&e.command);
                    }
                    if replaces {
                        existing.seqnum = e.seqnum;
                        existing.paths = e.paths.clone();
                    }
                }
            }
        }
    }
    let source_config = source_configuration(&source_ballot).filter(|c| {
        c.ballot() == source_ballot && c.epoch() == config.epoch() && c.voters() == config.voters()
    });
    possible_fast_decisions(
        config,
        source_ballot,
        source_config,
        reports,
        &mut entries,
        &from_below,
    );
    for c in entries.keys() {
        reproposed.remove(c);
    }
    let reproposed = within_bound(&entries, reproposed, MAX_REPORT_ENTRIES);
    Ok(SyncDecision {
        ballot: config.ballot(),
        source_ballot,
        entries,
        reproposed,
    })
}

/// The re-proposals a Sync carries (task-d24, from the review of
/// task-d20): every kept entry, then re-proposals until the Sync holds
/// `limit` commands, so a voter that installs it and takes its
/// re-proposals reports no more than a campaign accepts.
///
/// A re-proposal a kept entry depends on goes first, whatever its place:
/// the new leader re-proposes such an entry right after it, and left out
/// the entry would wait for a proposal nobody makes. The rest follow in
/// identity order (the order of the re-proposed set), which is
/// deterministic, not the order the leader proposes them in. What does
/// not fit was
/// decided nowhere, as every re-proposal was not: the installers release
/// it, as they release any record the Sync neither selects nor
/// re-proposes, and the collector offers it again.
///
/// Kept entries are never cut, since one may be decided, so a selection
/// whose kept entries alone pass `limit` carries them all; the notes
/// argue they stay within twice the table.
fn within_bound(
    entries: &BTreeMap<CommandId, SyncEntry>,
    reproposed: BTreeSet<CommandId>,
    limit: usize,
) -> BTreeSet<CommandId> {
    if entries.len().saturating_add(reproposed.len()) <= limit {
        return reproposed;
    }
    let followed: BTreeSet<CommandId> = entries
        .values()
        .flat_map(|e| e.deps.iter())
        .filter(|d| reproposed.contains(d))
        .copied()
        .collect();
    let room = limit.saturating_sub(entries.len().saturating_add(followed.len()));
    let rest: Vec<CommandId> = reproposed
        .iter()
        .filter(|c| !followed.contains(c))
        .take(room)
        .copied()
        .collect();
    followed.into_iter().chain(rest).collect()
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
    source_config: Option<BallotConfiguration>,
    reports: &[RecoveryReport],
    entries: &mut BTreeMap<CommandId, SyncEntry>,
    from_below: &BTreeSet<CommandId>,
) {
    // The fast set of the source ballot: its own configuration where the
    // candidate knows it (task-d31), else the default rule every
    // production path builds.
    let Some(source_config) = source_config.or_else(|| {
        BallotConfiguration::c2_default(config.epoch(), source_ballot, config.voters().clone()).ok()
    }) else {
        return;
    };
    let fast_reporters: Vec<&RecoveryReport> = reports
        .iter()
        .filter(|r| {
            r.committed_ballot == source_ballot && source_config.fast_set().contains(&r.replica)
        })
        .collect();
    if fast_reporters.is_empty() {
        return;
    }
    fn record<'a>(r: &'a RecoveryReport, c: &CommandId) -> Option<&'a ReportEntry> {
        r.entries.iter().find(|e| e.command == *c)
    }
    // How many reporting members must hold a pre-acceptance alike for a
    // fast quorum to have decided it. C2 has one fast set, and every
    // reporting member of it is in it: all of them. C1 decides with any
    // `fast_size` voters that include the leader (task-d31, Codex review
    // on #127): the voters that did not report may all have been in it,
    // so the reporting members that agree need only make up the rest,
    // and a reporting member that holds nothing may have been outside
    // it. Two different pre-acceptances cannot both reach that count,
    // since two fast quorums and a majority of reports intersect.
    let c1 = source_config.class() == crate::quorum::FastQuorumClass::C1;
    let needed = if c1 {
        let unreported = source_config.voters().len().saturating_sub(reports.len());
        source_config.fast_size().saturating_sub(unreported).max(1)
    } else {
        fast_reporters.len()
    };
    let source_leader = fast_reporters
        .iter()
        .position(|r| r.replica == source_ballot.leader);
    // Candidates: pre-accepted alike (the same path, dependencies and
    // admission) by enough reporting fast-set members, and not already
    // adopted; each with the first of those members, whose records the
    // order checks below read. Under C2 that member is the first
    // reporting member of the fast set, as before.
    let mut candidates: BTreeMap<CommandId, (ReportEntry, usize)> = BTreeMap::new();
    for (index, reporter) in fast_reporters.iter().enumerate() {
        for e in &reporter.entries {
            // A record a Sync demoted was pre-accepted in an earlier
            // ballot: no evidence of a fast decision in this one
            // (task-d11).
            if e.phase != Phase::PreAccept
                || !e.payload_present
                || e.path == crate::graph::demoted_path()
                || entries.contains_key(&e.command)
                || candidates.contains_key(&e.command)
            {
                continue;
            }
            // A fast decision counted one digest (task-d14): members
            // holding the command under different facts never decided it
            // together.
            let alike: Vec<usize> = fast_reporters
                .iter()
                .enumerate()
                .filter(|(_, r)| {
                    record(r, &e.command).is_some_and(|o| {
                        o.phase == Phase::PreAccept
                            && o.path == e.path
                            && same_set(&o.deps, &e.deps)
                            && o.payload_present
                            && o.admission == e.admission
                    })
                })
                .map(|(i, _)| i)
                .collect();
            // The source leader is in every fast quorum.
            let leader_agrees = !c1 || source_leader.is_none_or(|l| alike.contains(&l));
            if alike.len() >= needed && leader_agrees && alike.first() == Some(&index) {
                candidates.insert(e.command, (e.clone(), index));
            }
        }
    }
    // A member's dependency closure of a command (over its own records).
    let closure = |m: &RecoveryReport, c: &CommandId| -> BTreeSet<CommandId> {
        let mut seen = BTreeSet::new();
        let mut stack: Vec<CommandId> = record(m, c).map(|e| e.deps.clone()).unwrap_or_default();
        while let Some(d) = stack.pop() {
            if seen.insert(d)
                && let Some(e) = record(m, &d)
            {
                stack.extend(e.deps.iter().copied());
            }
        }
        seen
    };
    let conflicts = |a: &[Vec<u8>], b: &[Vec<u8>]| a.iter().any(|k| b.contains(k));
    // Whether the selection orders `x` after `c`: `c` is in `x`'s closure
    // over the selected entries' dependencies.
    let selected: BTreeMap<CommandId, Vec<CommandId>> =
        entries.iter().map(|(k, e)| (*k, e.deps.clone())).collect();
    let after = |x: &CommandId, c: &CommandId| -> bool {
        let mut seen = BTreeSet::new();
        let mut stack: Vec<CommandId> = selected.get(x).cloned().unwrap_or_default();
        while let Some(d) = stack.pop() {
            if d == *c {
                return true;
            }
            if seen.insert(d)
                && let Some(deps) = selected.get(&d)
            {
                stack.extend(deps.iter().copied());
            }
        }
        false
    };
    // Whether the member's order put `a` before a command whose closure
    // over the member's records is `prefix`: `a` is in it, or is decided
    // before a command in it that the member no longer holds. Such a
    // command was executed there, after everything the selection orders
    // before it, and retiring it cut the member's own closure short.
    let forgotten_before =
        |m: &RecoveryReport, prefix: &BTreeSet<CommandId>, a: &CommandId| -> bool {
            prefix.contains(a)
                || prefix
                    .iter()
                    .any(|d| record(m, d).is_none() && selected.contains_key(d) && after(d, a))
        };
    loop {
        let before = candidates.len();
        let keys: Vec<CommandId> = candidates.keys().copied().collect();
        for c in keys {
            let (entry, member) = candidates[&c].clone();
            let first = fast_reporters[member];
            let prefix = closure(first, &c);
            let ordered_after_adopted = entries.values().all(|adopted| {
                // Ordered after `c` by the selection itself: nothing it
                // says constrains the member's order before `c`.
                if after(&adopted.command, &c) {
                    return true;
                }
                match record(first, &adopted.command) {
                    Some(a) => {
                        !conflicts(&entry.keys, &a.keys)
                            || (prefix.contains(&adopted.command) && a.path == adopted.path)
                    }
                    // A decision of an earlier ballot precedes everything
                    // the source ballot ordered; the member may simply
                    // have executed it and forgotten it.
                    None if from_below.contains(&adopted.command) => true,
                    // Absent from the member's records, but its order put
                    // it before `c`: `c`'s own dependencies name it, or a
                    // command they name that the member no longer holds
                    // was decided after it. The member executed and
                    // retired it, and an ancestor it held is no evidence
                    // against the member's path (task-d34, #118 item 5).
                    None if forgotten_before(first, &prefix, &adopted.command) => true,
                    // Accepted at the source ballot and before `c` there,
                    // yet absent from the member's records and from what
                    // its order put before `c`: the member's order never
                    // placed it, so its path for `c` is not the leader's,
                    // whatever it says (task-d30). Only a command with no
                    // conflict keys at all is exempt.
                    None => {
                        let keys: Vec<Vec<u8>> =
                            adopted.paths.iter().map(|(k, _)| k.clone()).collect();
                        !keys.is_empty() && !conflicts(&entry.keys, &keys)
                    }
                }
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
    for (c, (e, _)) in candidates {
        entries.insert(
            c,
            SyncEntry {
                command: c,
                phase: Phase::Accept,
                deps: e.deps,
                path: e.path,
                paths: e.paths,
                seqnum: e.seqnum,
                admission: e.admission,
            },
        );
    }
}

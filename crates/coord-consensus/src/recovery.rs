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
use core::cell::Cell;

use coord_types::CommandId;
use coord_types::identity::Digest32;
use coord_types::ids::{Ballot, ReplicaId, ReplicaIncarnation, StoreInstanceId};
use serde::{Deserialize, Serialize};

use crate::commands::{CommandRecord, NOTHING_ADMISSION, is_nothing};
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

/// The store a counted voter's promise came from, under the incarnation
/// it promised as (task-d121).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CountedStore {
    /// The voter's incarnation.
    pub incarnation: ReplicaIncarnation,
    /// The store its promise came from.
    pub store: StoreInstanceId,
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
    /// The store of each voter whose promise and report the selection
    /// counted, the candidate's own among them (task-d121). A voter that
    /// installs the Sync records each one it has no record of, so it
    /// learns the stores of voters whose promises it never received.
    /// Empty in a Sync selected before the campaign counted it, and in
    /// one read from a version 3 row.
    pub counted: BTreeMap<ReplicaId, CountedStore>,
    /// The re-proposals its leader proposes as nothing, never under
    /// facts (task-d121): a subset of `reproposed`. A voter that installs
    /// the Sync keeps such a command's record demoted, with its payload
    /// row, until the proposal replaces it. Empty until a selection
    /// decides a nothing, and in a Sync read from a row of version 4 or 3.
    pub nothing: BTreeSet<CommandId>,
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
    /// The candidate executed and forgot commands the selection would
    /// otherwise re-propose, and has not read their durable dependency
    /// rows yet (task-d77): the campaign waits for them, as it waits for a
    /// payload, and the runtime reads them.
    ExecutedRowsUnread {
        /// The commands whose rows the selection waits on.
        commands: Vec<CommandId>,
    },
    /// The candidate executed a command the selection would otherwise
    /// re-propose, and keeps no dependency row of it (task-d77): it cannot
    /// say what the command was decided with, and a re-proposal would
    /// decide it again under whatever a voter behind holds, so the
    /// campaign is refused.
    ExecutedWithoutRow {
        /// The command.
        command: CommandId,
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
    select_decided(
        config,
        reports,
        supplied,
        source_configuration,
        &|_: &CommandId| false,
    )
}

/// What a candidate knows of its own decisions, beside the reports
/// (task-d71).
pub trait CandidateDecisions {
    /// Whether the candidate committed or executed `command`, retired and
    /// forgotten included.
    fn decided(&self, command: &CommandId) -> bool;

    /// Whether the candidate executed `first` before `then`. Execution is
    /// one order at every voter, so this is the order every voter executes
    /// the two in. False when the candidate does not know both positions.
    fn executed_before(&self, first: &CommandId, then: &CommandId) -> bool {
        let _ = (first, then);
        false
    }

    /// What the candidate executed `command` with, if it executed it
    /// (task-d77). The selection asks it of every command it would
    /// re-propose: one the candidate executed is never re-proposed, and is
    /// carried at COMMIT under these facts instead.
    fn executed(&self, command: &CommandId) -> ExecutedFacts {
        let _ = command;
        ExecutedFacts::No
    }

    /// Whether the candidate holds a payload of `command`, in memory or
    /// durable, under `facts`, or under any facts where `facts` is none
    /// (task-d121's condition 3). A record whose admission matches is not
    /// enough: a candidate holding `command`'s record without its payload
    /// supplies nothing, and reading it as a supplier would keep the rule
    /// off for ever. None, by default: the candidate's own report holds
    /// what its table holds.
    fn holds_payload(&self, command: &CommandId, facts: Option<Digest32>) -> bool {
        let _ = (command, facts);
        false
    }
}

/// What a candidate knows of a command it may have executed (task-d77).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutedFacts {
    /// Not executed here, as far as the candidate knows.
    No,
    /// Executed, with the record it was decided and executed with: its
    /// own while the candidate holds one, its durable dependency row once
    /// it has forgotten it.
    With(CommandRecord),
    /// Executed and forgotten, its dependency row not read yet.
    Unread,
    /// Executed, and no dependency row is kept for it.
    WithoutRow,
}

impl<F: Fn(&CommandId) -> bool> CandidateDecisions for F {
    fn decided(&self, command: &CommandId) -> bool {
        self(command)
    }
}

/// [`select_from`], where `decided` names the commands the candidate
/// itself committed or executed, retired and forgotten ones included
/// (task-d71). Reports leave out what their replica executed long ago,
/// so a voter behind can report its own pre-acceptance of a command the
/// candidate executed far back, and the selection re-proposes it. The
/// new leader does not chain such a re-proposal after the recovered
/// order (`Leader::from_recovered`): it was decided, with other
/// dependencies. So the possible-fast rule does not count a candidate
/// that follows one as ordered after the recovered order, as it counts
/// one that follows a command no ballot decided (protocol_sim row 12,
/// three voters, seed 387, without catch-up).
pub fn select_decided(
    config: &BallotConfiguration,
    reports: &[RecoveryReport],
    supplied: impl Fn(&CommandId, Option<Digest32>) -> bool,
    source_configuration: impl Fn(&Ballot) -> Option<BallotConfiguration>,
    decided: &impl CandidateDecisions,
) -> Result<SyncDecision, RecoveryError> {
    select_ruled(
        config,
        reports,
        supplied,
        source_configuration,
        decided,
        SelectionRules::default(),
    )
}

/// The rules a selection applies beyond task-29's, each resting on
/// something every voter's build must do (task-d90). The default applies
/// none of them, which is the selection a domain made before it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SelectionRules {
    /// Leave out a re-proposal no reporter holds a payload row for and no
    /// selected entry waits on ([`select_ruled`]). It rests on every vote
    /// resting on a payload row durable at the voter (point 4, #195): a
    /// build from before that can acknowledge a command whose payload row
    /// it never made durable, and come back holding the record alone, so a
    /// command decided over such a vote can reach a candidate with no
    /// reporter holding its payload. Left out, that decision would be
    /// lost; re-proposed, the campaign waits until a supplier is back.
    /// So it holds only in a domain whose voters all run point 4: one that
    /// activated `ReleaseAtExecution` (task-d99), whose activation every
    /// voter prepared durably on such a build and cannot roll back from.
    pub leave_out_unheld: bool,
    /// Decide as nothing a command whose payload no report holds, once
    /// every voter that held it is gone for good (task-d121, design
    /// Section 4.9). On only where `leave_out_unheld` is: the rule rests
    /// on the same premise, and on a domain that activated
    /// `DecidedNothing` as well.
    ///
    /// On, a selection also counts a payload row at any phase as
    /// supplying a command, and fails as `HalfInitialized` only on what it
    /// carries, after the possible-fast rule (task-d121's C5). Whether the
    /// rule may decide a nothing now is the campaign's: only once
    /// task-d120's wait has passed (`select_counted`'s `fire`).
    /// [`select_ruled`] decides at once.
    pub decide_nothing: bool,
}

/// [`select_decided`] under `rules`.
pub fn select_ruled(
    config: &BallotConfiguration,
    reports: &[RecoveryReport],
    supplied: impl Fn(&CommandId, Option<Digest32>) -> bool,
    source_configuration: impl Fn(&Ballot) -> Option<BallotConfiguration>,
    decided: &impl CandidateDecisions,
    rules: SelectionRules,
) -> Result<SyncDecision, RecoveryError> {
    select_counted(
        config,
        reports,
        supplied,
        source_configuration,
        decided,
        rules,
        rules.decide_nothing,
        &SelectionCounts::default(),
    )
}

/// What a selection counts besides what it selects.
#[derive(Debug, Default)]
pub(crate) struct SelectionCounts {
    /// A step for every record, dependency and order fact the selection
    /// reads (task-d108): what the tests bound its cost by.
    pub(crate) work: Cell<u64>,
    /// Possible-fast candidates the order check dropped whose
    /// dependencies reach, through the selection and the member's report,
    /// a command no report holds (task-d119). The walk doesn't follow a
    /// record a Sync demoted: what it names is no order the source
    /// ballot's leader gave.
    pub(crate) unreported: Cell<u64>,
    /// Of those, the ones where that command is one the candidate doesn't
    /// hold decided either. Where the candidate holds it decided, it
    /// executed it and forgot it, and the drop is right.
    pub(crate) unreported_undecided: Cell<u64>,
    /// Commands the selection decided as nothing (task-d121): entries
    /// whose admission it replaced with the marker, and re-proposals it
    /// put in [`SyncDecision::nothing`]. Not the nothings it carried.
    pub(crate) decided_nothing: Cell<u64>,
    /// Selections the rule would have decided a nothing in, kept off
    /// because the order check counted a drop in `unreported_undecided`
    /// (task-d121's condition 5).
    pub(crate) kept_by_drop: Cell<u64>,
}

/// [`select_ruled`], adding to `counts` (task-d108, task-d119), and
/// deciding a nothing only with `fire` (task-d121): the campaign sets it
/// once task-d120's wait has passed, and it counts only where `rules`
/// turns the rule on.
#[expect(
    clippy::too_many_arguments,
    reason = "the selection's inputs, when it may decide, and what it counts"
)]
pub(crate) fn select_counted(
    config: &BallotConfiguration,
    reports: &[RecoveryReport],
    supplied: impl Fn(&CommandId, Option<Digest32>) -> bool,
    source_configuration: impl Fn(&Ballot) -> Option<BallotConfiguration>,
    decided: &impl CandidateDecisions,
    rules: SelectionRules,
    fire: bool,
    counts: &SelectionCounts,
) -> Result<SyncDecision, RecoveryError> {
    // Behind the feature: the move and narrowing of the
    // `HalfInitialized` check, and a payload row at any phase supplying a
    // command. Deciding a nothing needs `fire` as well.
    let ruled = rules.decide_nothing && rules.leave_out_unheld;
    let fire = fire && ruled;
    let undecided_before = counts.unreported_undecided.get();
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
    // fetching it would not let the candidate bind (task-d14). A nothing
    // supplies itself: it has no payload to fetch (task-d121).
    //
    // Under task-d121's rule a payload row at any phase supplies the
    // command too: the campaign fetches it, and the selection carries the
    // command with it. Otherwise a payload held only at a pre-acceptance
    // failed the selection as `HalfInitialized` while the rule's first
    // condition kept it off (protocol_sim row 21, five voters, seed 518).
    // `reported_payload` is what the rule's first condition reads: a
    // reporter's payload row under the facts named, at any phase.
    let mut reported_payload: BTreeSet<CommandId> = BTreeSet::new();
    for r in &by_replica {
        for e in &r.entries {
            if named
                .get(&e.command)
                .is_some_and(|d| e.admission != Some(*d))
            {
                continue;
            }
            if e.payload_present {
                reported_payload.insert(e.command);
                if ruled || e.phase >= Phase::Accept {
                    with_payload.insert(e.command);
                }
            } else if e.phase >= Phase::Accept && e.admission.as_ref().is_some_and(is_nothing) {
                with_payload.insert(e.command);
            }
        }
    }
    for c in &accepted_somewhere {
        if !with_payload.contains(c) && supplied(c, named.get(c).copied()) {
            with_payload.insert(*c);
        }
    }
    // The reporter that holds the acceptance without its payload, not
    // merely the first that names the command: a campaign sets that
    // report aside and selects again. Named by a reporter that held the
    // command's payload at a pre-acceptance, the campaign set aside the
    // reports that could supply it, one by one, and never selected; a
    // leader that crashed between its Sync row and its payload rows held
    // the acceptance (protocol_sim row 21, five voters, seed 518).
    let half_initialized = |command: &CommandId| {
        let replica = reports
            .iter()
            .find(|r| {
                r.entries.iter().any(|e| {
                    e.command == *command && e.phase >= Phase::Accept && !e.payload_present
                })
            })
            .or_else(|| {
                reports
                    .iter()
                    .find(|r| r.entries.iter().any(|e| e.command == *command))
            })
            .map_or(reports[0].replica, |r| r.replica);
        RecoveryError::HalfInitialized {
            replica,
            command: *command,
        }
    };
    // Under task-d121's rule the check moves after the possible-fast rule
    // and the rule, and fails only on what the selection carries (below).
    if !ruled && let Some(command) = accepted_somewhere.difference(&with_payload).next() {
        return Err(half_initialized(command));
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
    // Never re-propose what the candidate executed (task-d77). A report
    // leaves out what its replica executed and forgot, so a voter behind
    // can be the only reporter of a command the candidate executed long
    // ago, at PRE-ACCEPT or at an ACCEPT below the source; re-proposed,
    // the command was decided again, under the dependencies that voter
    // holds or the new leader gives it, and executed there in another
    // order than the candidate executed it in (protocol_sim row 9, three
    // voters, seeds 362, 550, 633 and 863, with peer acknowledgements
    // lost). The candidate's execution is its commit, under the facts it
    // executed with, so the Sync carries it at COMMIT under them, and
    // every later selection keeps it by the commit exception above. It is
    // a decision of an earlier ballot, as a below-source report's is, to
    // the possible-fast rule: the rule skips what the candidate decided,
    // and execution is one order at every voter, so what the candidate
    // executed precedes every command that rule keeps, at a fast-set
    // member that executed it and forgot it too (#169 review).
    let mut unread = Vec::new();
    // A nothing the selection carries, against the candidate's own
    // executions (task-d121): one it executed under facts is a second
    // decision of one command, and the selection stops; one whose row it
    // has not read yet waits for the read, so no nothing is carried past
    // an execution the candidate hasn't read. Its execution of the
    // nothing itself passes, and so does one it keeps no row of, which
    // can't tell: the check is on the premises, not part of the argument.
    for (command, entry) in &entries {
        if !entry.admission.as_ref().is_some_and(is_nothing) {
            continue;
        }
        match decided.executed(command) {
            ExecutedFacts::No | ExecutedFacts::WithoutRow => {}
            ExecutedFacts::Unread => unread.push(*command),
            ExecutedFacts::With(record) => {
                if let Some(facts) = record.facts() {
                    return Err(RecoveryError::IncompatibleAdmission {
                        command: *command,
                        first: NOTHING_ADMISSION,
                        second: facts,
                    });
                }
            }
        }
    }
    for command in &reproposed {
        if entries.contains_key(command) {
            continue;
        }
        match decided.executed(command) {
            ExecutedFacts::No => {}
            ExecutedFacts::Unread => unread.push(*command),
            ExecutedFacts::WithoutRow => {
                return Err(RecoveryError::ExecutedWithoutRow { command: *command });
            }
            ExecutedFacts::With(record) => {
                from_below.insert(*command);
                entries.insert(
                    *command,
                    SyncEntry {
                        command: *command,
                        phase: Phase::Commit,
                        deps: record.deps,
                        path: record.path,
                        paths: record.paths,
                        seqnum: record.synced_seq.unwrap_or(0),
                        admission: record.payload,
                    },
                );
            }
        }
    }
    if !unread.is_empty() {
        return Err(RecoveryError::ExecutedRowsUnread { commands: unread });
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
        decided,
        counts,
    );
    for c in entries.keys() {
        reproposed.remove(c);
    }
    // Nor, under `rules.leave_out_unheld`, a command no reporter holds a
    // payload row for, the candidate's own report among them, and no
    // selected entry waits on (task-d90, point 4). Every vote rests on a payload row durable at the voter, so a
    // slow decision of it would be an acceptance in some report and a
    // fast one a pre-acceptance with its payload at a reporting member of
    // the fast quorum: it was never decided, and is left out as a command
    // no reporter held is. Re-proposed, the candidate waited for a
    // payload no voter it asks has, and never bound: a voter's
    // pre-acceptance whose payload batch failed, its record durable from
    // a later batch without the payload row (protocol_sim row 21, five
    // voters, seed 2). A candidate holding its record without the
    // payload is no supplier: re-proposed, its own vote rested on no
    // payload row (row 21, three voters, seed 4). One an entry was
    // decided after is still re-proposed, for the entry to execute after
    // it (task-d33).
    let waited_on: BTreeSet<CommandId> = entries
        .values()
        .flat_map(|e| e.deps.iter().copied())
        .collect();
    if rules.leave_out_unheld {
        let held: BTreeSet<CommandId> = reports
            .iter()
            .flat_map(|r| r.entries.iter())
            .filter(|e| e.payload_present)
            .map(|e| e.command)
            .collect();
        // Checked against the scan it replaces, for each re-proposal
        // (task-d108 review), where that scan is small enough to run.
        #[cfg(debug_assertions)]
        if reproposed
            .len()
            .saturating_mul(reports.iter().map(|r| r.entries.len()).sum())
            <= 1 << 20
        {
            for c in &reproposed {
                debug_assert_eq!(
                    held.contains(c),
                    reports.iter().any(|r| r
                        .entries
                        .iter()
                        .any(|e| e.command == *c && e.payload_present)),
                    "held with a payload: {c:?}"
                );
            }
        }
        reproposed.retain(|c| waited_on.contains(c) || held.contains(c));
    }
    let mut nothing: BTreeSet<CommandId> = BTreeSet::new();
    if fire {
        decide_nothing(
            &mut entries,
            &mut reproposed,
            &mut nothing,
            reports,
            &reported_payload,
            decided,
            counts.unreported_undecided.get() > undecided_before,
            counts,
        )?;
    }
    let reproposed = within_bound(&entries, reproposed, MAX_REPORT_ENTRIES);
    nothing.retain(|c| reproposed.contains(c));
    if ruled {
        // What the selection carries without a payload anywhere: an entry
        // under facts, or a re-proposal not decided as nothing. An
        // acceptance the selection re-proposes and leaves out is no
        // reason to wait.
        if let Some(command) = accepted_somewhere.difference(&with_payload).find(|c| {
            entries
                .get(c)
                .map_or(reproposed.contains(c) && !nothing.contains(c), |e| {
                    !e.admission.as_ref().is_some_and(is_nothing)
                })
        }) {
            return Err(half_initialized(command));
        }
    }
    Ok(SyncDecision {
        ballot: config.ballot(),
        source_ballot,
        entries,
        reproposed,
        counted: Default::default(),
        nothing,
    })
}

/// task-d121's rule (design Section 4.9): decide as nothing each command
/// `D` the selection must carry whose payload no report holds, where
/// `D` is
/// - an entry under facts below COMMIT whose payload no reporter holds
///   under those facts at any phase (the payload shape): its admission is
///   replaced with the marker, and its dependencies, paths and sequence
///   number are kept, so every ordering decision is the one the selection
///   makes with `D`'s payload supplied;
/// - a re-proposal an entry waits on, whose payload no reporter holds
///   under any facts (the re-proposal shape), put in `nothing`;
/// - a dependency of an entry that no report holds at all (the `Behind`
///   shape), added to the re-proposals and to `nothing`.
///
/// Conditions 1 and 2 are those: no reporter's payload row under the
/// facts named, and no copy at COMMIT or executed, since an entry with
/// one is selected at COMMIT. Condition 3 is the candidate's: it neither
/// committed `D`, nor executed it, nor holds its payload under those
/// facts ([`CandidateDecisions::holds_payload`]). An execution whose row
/// it hasn't read is waited for ([`RecoveryError::ExecutedRowsUnread`]).
/// Condition 4, task-d120's wait, is the campaign's: it is why this runs
/// at all. Condition 5: where the order check counted a drop in
/// `unreported_undecided`, `dropped`, no fast decision behind `D` can be
/// ruled out, and the selection is left as it is without the rule.
#[expect(
    clippy::too_many_arguments,
    reason = "the selection's state the rule rewrites, and what it reads"
)]
fn decide_nothing(
    entries: &mut BTreeMap<CommandId, SyncEntry>,
    reproposed: &mut BTreeSet<CommandId>,
    nothing: &mut BTreeSet<CommandId>,
    reports: &[RecoveryReport],
    reported_payload: &BTreeSet<CommandId>,
    decided: &impl CandidateDecisions,
    dropped: bool,
    counts: &SelectionCounts,
) -> Result<(), RecoveryError> {
    // Re-proposals and dependencies have no copy to name facts from, so a
    // payload row under any facts holds them.
    let held_anywhere: BTreeSet<CommandId> = reports
        .iter()
        .flat_map(|r| r.entries.iter())
        .filter(|e| e.payload_present)
        .map(|e| e.command)
        .collect();
    let mut unread = Vec::new();
    let mut free = |command: &CommandId, facts: Option<Digest32>| {
        if decided.decided(command) || decided.holds_payload(command, facts) {
            return false;
        }
        match decided.executed(command) {
            ExecutedFacts::No => true,
            ExecutedFacts::Unread => {
                unread.push(*command);
                false
            }
            ExecutedFacts::With(_) | ExecutedFacts::WithoutRow => false,
        }
    };
    let as_entries: Vec<CommandId> = entries
        .values()
        .filter(|e| {
            e.phase < Phase::Commit
                && !e.admission.as_ref().is_some_and(is_nothing)
                && !reported_payload.contains(&e.command)
        })
        .map(|e| (e.command, e.admission))
        .collect::<Vec<_>>()
        .into_iter()
        .filter(|(c, facts)| free(c, *facts))
        .map(|(c, _)| c)
        .collect();
    // A decision under facts with no payload at any reporter breaks the
    // premise the rule rests on (a deciding quorum meets the reports'
    // and holds the payload, point 4): the rule leaves it, and the
    // campaign keeps today's wait.
    #[cfg(debug_assertions)]
    for e in entries.values() {
        debug_assert!(
            e.phase < Phase::Commit
                || e.admission.as_ref().is_none_or(is_nothing)
                || reported_payload.contains(&e.command)
                || decided.decided(&e.command)
                || decided.holds_payload(&e.command, e.admission)
                || !matches!(decided.executed(&e.command), ExecutedFacts::No),
            "decided under facts with no payload at any reporter: {:?}",
            e.command
        );
    }
    let waited_on: BTreeSet<CommandId> = entries
        .values()
        .flat_map(|e| e.deps.iter().copied())
        .collect();
    let as_reproposals: Vec<CommandId> = reproposed
        .iter()
        .filter(|c| waited_on.contains(c) && !held_anywhere.contains(c))
        .copied()
        .collect::<Vec<_>>()
        .into_iter()
        .filter(|c| free(c, None))
        .collect();
    let gaps: Vec<CommandId> = waited_on
        .iter()
        .filter(|d| !entries.contains_key(d) && !reproposed.contains(d))
        .copied()
        .collect::<Vec<_>>()
        .into_iter()
        .filter(|d| free(d, None))
        .collect();
    let found = !as_entries.is_empty() || !as_reproposals.is_empty() || !gaps.is_empty();
    if found && dropped {
        counts.kept_by_drop.set(counts.kept_by_drop.get() + 1);
        return Ok(());
    }
    if !unread.is_empty() {
        return Err(RecoveryError::ExecutedRowsUnread { commands: unread });
    }
    let mut decided_nothing = 0u64;
    for c in as_entries {
        if let Some(e) = entries.get_mut(&c) {
            e.admission = Some(NOTHING_ADMISSION);
            decided_nothing += 1;
        }
    }
    for c in as_reproposals.into_iter().chain(gaps) {
        reproposed.insert(c);
        nothing.insert(c);
        decided_nothing += 1;
    }
    counts
        .decided_nothing
        .set(counts.decided_nothing.get() + decided_nothing);
    Ok(())
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
///
/// A command no ballot decided that `c`'s own dependencies reach is the
/// exception (task-d33): the selection re-proposes it, after the recovered
/// order, and `c` after it, so it may stand in `c`'s closure, and `c` is
/// ordered after every adopted command that waits on no re-proposal. So is
/// a command the member no longer holds, which it executed and retired.
#[expect(
    clippy::too_many_arguments,
    reason = "the selection's inputs, and the steps it counts"
)]
fn possible_fast_decisions(
    config: &BallotConfiguration,
    source_ballot: Ballot,
    source_config: Option<BallotConfiguration>,
    reports: &[RecoveryReport],
    entries: &mut BTreeMap<CommandId, SyncEntry>,
    from_below: &BTreeSet<CommandId>,
    decided: &impl CandidateDecisions,
    counts: &SelectionCounts,
) {
    let work = &counts.work;
    let tick = || work.set(work.get().saturating_add(1));
    // The fast set of the source ballot: its own configuration where the
    // candidate knows it (task-d31), else the default rule every
    // production path builds.
    let Some(source_config) = source_config.or_else(|| {
        BallotConfiguration::c2_default(config.epoch(), source_ballot, config.voters().clone()).ok()
    }) else {
        return;
    };
    let fast_reporters: Vec<usize> = reports
        .iter()
        .enumerate()
        .filter(|(_, r)| {
            r.committed_ballot == source_ballot && source_config.fast_set().contains(&r.replica)
        })
        .map(|(i, _)| i)
        .collect();
    if fast_reporters.is_empty() {
        return;
    }
    // Each report's records by command, the first a report lists of each
    // winning, as a scan for it would find (task-d108). Every check below
    // asks a member's record of a command; scanned for, one selection over
    // three reports of 2,000 commits took over a second.
    let by_report: Vec<BTreeMap<CommandId, &ReportEntry>> = reports
        .iter()
        .map(|r| {
            let mut by_command = BTreeMap::new();
            for e in &r.entries {
                tick();
                by_command.entry(e.command).or_insert(e);
            }
            by_command
        })
        .collect();
    // Checked against a scan of each report that does not go through the
    // index: every command once, at the first record the report lists of
    // it (task-d108 review).
    #[cfg(debug_assertions)]
    for (r, by_command) in reports.iter().zip(&by_report) {
        let mut seen = BTreeSet::new();
        for e in &r.entries {
            if seen.insert(e.command) {
                debug_assert!(
                    by_command
                        .get(&e.command)
                        .is_some_and(|indexed| core::ptr::eq(*indexed, e)),
                    "the index holds a record of {:?} other than the first listed",
                    e.command
                );
            }
        }
        debug_assert_eq!(
            seen.len(),
            by_command.len(),
            "the index holds no other command"
        );
    }
    let record = |m: usize, c: &CommandId| -> Option<&ReportEntry> {
        tick();
        by_report[m].get(c).copied()
    };
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
        .position(|&r| reports[r].replica == source_ballot.leader);
    // Candidates: pre-accepted alike (the same path, dependencies and
    // admission) by enough reporting fast-set members, and not already
    // adopted; each with the first of those members, whose records the
    // order checks below read. Under C2 that member is the first
    // reporting member of the fast set, as before.
    let mut candidates: BTreeMap<CommandId, (ReportEntry, usize)> = BTreeMap::new();
    for (index, &reporter) in fast_reporters.iter().enumerate() {
        for e in &reports[reporter].entries {
            tick();
            // A record a Sync demoted was pre-accepted in an earlier
            // ballot: no evidence of a fast decision in this one
            // (task-d11).
            //
            // Nor is a command the candidate decided itself: it was
            // decided with the dependencies the candidate committed it
            // with, and a member behind can still hold a pre-acceptance of
            // it under others (protocol_sim row 10, three voters, seed
            // 884, without catch-up). The candidate holds one it has not
            // retired in its own report, so this is one it executed and
            // forgot.
            if e.phase != Phase::PreAccept
                || !e.payload_present
                || e.path == crate::graph::demoted_path()
                || entries.contains_key(&e.command)
                || candidates.contains_key(&e.command)
                || decided.decided(&e.command)
            {
                continue;
            }
            // A fast decision counted one digest (task-d14): members
            // holding the command under different facts never decided it
            // together.
            let alike: Vec<usize> = fast_reporters
                .iter()
                .enumerate()
                .filter(|&(_, &r)| {
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
    if candidates.is_empty() {
        return;
    }
    // A member's dependency closure of a command (over its own records),
    // stopped at a command the selection holds decided (task-d78). The
    // member's record of such a command can be its own pre-acceptance,
    // which a leader synchronization since re-based its log past: the
    // record keeps the dependencies it was pre-accepted with, while the
    // decision ordered the command after others. What the record names
    // behind it was never in the leader's order before `c`, so it says
    // nothing of the member's path for `c`. Walked through, it reached a
    // command no ballot decided and dropped a fast decision of `c`, which
    // was re-proposed under other dependencies (protocol_sim row 4, three
    // voters, seed 720, with half the acknowledgements to followers lost
    // and catch-up on). `executed_closure` reads the same record the same
    // way for the order check (task-d71).
    let closure = |m: usize, c: &CommandId| -> BTreeSet<CommandId> {
        let mut seen = BTreeSet::new();
        let mut stack: Vec<CommandId> = record(m, c).map(|e| e.deps.clone()).unwrap_or_default();
        while let Some(d) = stack.pop() {
            tick();
            if seen.insert(d)
                && entries.get(&d).is_none_or(|e| e.phase != Phase::Commit)
                && let Some(e) = record(m, &d)
            {
                stack.extend(e.deps.iter().copied());
            }
        }
        seen
    };
    // The same closure, followed through what the selection holds by the
    // dependencies the selection gives it. It is what the member's order
    // says the selection executes before a command. The member's record
    // of a command the selection holds can be stale -- a pre-acceptance
    // the command was since decided past, under other dependencies -- and
    // the order it gives never executes (task-d33, protocol_sim row 10,
    // five voters, seed 67): read through it, an adopted command passed as
    // ordered before the candidate while the selection ordered both after
    // the same command. The member's own closure stays the evidence of its
    // path (`prefix_decided`).
    //
    // Past the selection's entries, a member's record is followed only
    // where it is an order the selection executes: one at ACCEPT or
    // above (a leader's), a candidate the selection keeps (alike at every
    // member), or a command the selection re-proposes (chained after the
    // recovered order). A pre-acceptance of a command the candidate
    // decided is none of these: the decision is not the record, and what
    // the walk reached through it alone the candidate need not follow, so
    // the candidate's executed order over it says nothing (task-d71, #158
    // review). The command itself stays in the closure.
    let executed_closure = |m: usize,
                            c: &CommandId,
                            candidates: &BTreeMap<CommandId, (ReportEntry, usize)>|
     -> BTreeSet<CommandId> {
        let mut seen = BTreeSet::new();
        let mut stack: Vec<CommandId> = record(m, c).map(|e| e.deps.clone()).unwrap_or_default();
        while let Some(d) = stack.pop() {
            tick();
            if !seen.insert(d) {
                continue;
            }
            if let Some(e) = entries.get(&d) {
                stack.extend(e.deps.iter().copied());
            } else if let Some(e) = record(m, &d)
                && (e.phase >= Phase::Accept || candidates.contains_key(&d) || !decided.decided(&d))
            {
                stack.extend(e.deps.iter().copied());
            }
        }
        seen
    };
    let conflicts = |a: &[Vec<u8>], b: &[Vec<u8>]| a.iter().any(|k| b.contains(k));
    // The commands each command is a dependency of, over the selected
    // entries and every candidate. A pass reads it through the entries and
    // the candidates it still keeps.
    let mut dependents: BTreeMap<CommandId, Vec<CommandId>> = BTreeMap::new();
    for (u, deps) in entries
        .iter()
        .map(|(u, e)| (u, &e.deps))
        .chain(candidates.iter().map(|(u, (e, _))| (u, &e.deps)))
    {
        for d in deps {
            tick();
            dependents.entry(*d).or_default().push(*u);
        }
    }
    // Whether a report holds `d` below COMMIT, the first record of it the
    // report lists read, as `record` reads it.
    let held_undecided: BTreeSet<CommandId> = by_report
        .iter()
        .flat_map(|by_command| by_command.values())
        .filter(|e| e.phase < Phase::Commit)
        .map(|e| e.command)
        .collect();
    #[cfg(debug_assertions)]
    {
        let mut scanned = BTreeSet::new();
        for r in reports {
            let mut seen = BTreeSet::new();
            for e in &r.entries {
                if seen.insert(e.command) && e.phase < Phase::Commit {
                    scanned.insert(e.command);
                }
            }
        }
        debug_assert_eq!(held_undecided, scanned, "held below COMMIT, scanned");
    }
    // Whether the selection orders `x` after `c`: `c` is in `x`'s closure
    // over the dependencies of the selected entries and of the candidates
    // still kept. A kept candidate is selected with the dependencies it
    // was reported with, and every pass judges each candidate against the
    // candidates that pass keeps, so the order read through one is the
    // order the selection ends with (#130 review). Read through the
    // entries alone, a command adopted after a candidate that follows
    // another was not seen to follow that other one.
    //
    // Walked from `x` for every adopted command and every candidate, it
    // cost a walk of the selection per pair. `ordered_after` is the same
    // set read from `c`'s side: every command whose closure reaches `c`,
    // found once per candidate by walking `dependents` back from it
    // (task-d108).
    let ordered_after = |candidates: &BTreeMap<CommandId, (ReportEntry, usize)>,
                         c: &CommandId|
     -> BTreeSet<CommandId> {
        let mut seen = BTreeSet::new();
        let mut stack = alloc::vec![*c];
        while let Some(x) = stack.pop() {
            tick();
            for u in dependents.get(&x).into_iter().flatten() {
                if (entries.contains_key(u) || candidates.contains_key(u)) && seen.insert(*u) {
                    stack.push(*u);
                }
            }
        }
        seen
    };
    // Every command the closures of `from` reach, over the same
    // dependencies: `after(x, a)` holds for some `x` in `from` exactly when
    // `a` is in it.
    let reached_from = |candidates: &BTreeMap<CommandId, (ReportEntry, usize)>,
                        from: &mut dyn Iterator<Item = CommandId>|
     -> BTreeSet<CommandId> {
        let deps_of = |d: &CommandId| -> Option<&Vec<CommandId>> {
            entries
                .get(d)
                .map(|e| &e.deps)
                .or_else(|| candidates.get(d).map(|(e, _)| &e.deps))
        };
        let mut seen = BTreeSet::new();
        let mut stack: Vec<CommandId> = Vec::new();
        for x in from {
            stack.extend(deps_of(&x).into_iter().flatten().copied());
        }
        while let Some(d) = stack.pop() {
            tick();
            if seen.insert(d)
                && let Some(deps) = deps_of(&d)
            {
                stack.extend(deps.iter().copied());
            }
        }
        seen
    };
    // The walk `ordered_after` and `reached_from` stand for, kept to check
    // them against in debug builds over a small selection.
    #[cfg(debug_assertions)]
    let after = |candidates: &BTreeMap<CommandId, (ReportEntry, usize)>,
                 x: &CommandId,
                 c: &CommandId|
     -> bool {
        let deps_of = |d: &CommandId| -> Option<&Vec<CommandId>> {
            entries
                .get(d)
                .map(|e| &e.deps)
                .or_else(|| candidates.get(d).map(|(e, _)| &e.deps))
        };
        let mut seen = BTreeSet::new();
        let mut stack: Vec<CommandId> = deps_of(x).cloned().unwrap_or_default();
        while let Some(d) = stack.pop() {
            if d == *c {
                return true;
            }
            if seen.insert(d)
                && let Some(deps) = deps_of(&d)
            {
                stack.extend(deps.iter().copied());
            }
        }
        false
    };
    // The commands `deps` reach, followed through the selection and the
    // candidates, that neither holds and a report holds undecided (or,
    // with `member`, that member holds undecided), and the candidate did
    // not decide itself: the ones the selection re-proposes and the new
    // leader chains after the recovered order (task-d71).
    //
    // With `member`, a command it holds no record of counts too when
    // another report holds it undecided (seed 147, row 10, three voters,
    // no catch-up, loss 750000). The member's acceptance that names it is
    // the leader's -- here the entry of a durable Sync whose installation
    // waits for it, after the member refused its re-proposal while its
    // promise was in flight -- so the member's never holding it says
    // nothing of an execution there. The selection re-proposes it, and
    // the candidate follows it as it follows any re-proposal. Left out,
    // the member's closure stopped at it, the order check dropped the
    // candidate a fast quorum had decided, and the candidate was
    // re-proposed under other dependencies.
    let undecided_reached = |deps: &[CommandId],
                             candidates: &BTreeMap<CommandId, (ReportEntry, usize)>,
                             member: Option<usize>|
     -> BTreeSet<CommandId> {
        let mut seen = BTreeSet::new();
        let mut found = BTreeSet::new();
        let mut stack: Vec<CommandId> = deps.to_vec();
        while let Some(d) = stack.pop() {
            tick();
            if !seen.insert(d) {
                continue;
            }
            if let Some(e) = entries.get(&d) {
                stack.extend(e.deps.iter().copied());
            } else if let Some((e, _)) = candidates.get(&d) {
                stack.extend(e.deps.iter().copied());
            } else if !decided.decided(&d)
                && match member {
                    Some(m) => record(m, &d)
                        .map_or_else(|| held_undecided.contains(&d), |r| r.phase < Phase::Commit),
                    None => held_undecided.contains(&d),
                }
            {
                found.insert(d);
                if let Some(m) = member
                    && let Some(r) = record(m, &d)
                {
                    stack.extend(r.deps.iter().copied());
                }
            }
        }
        found
    };
    // The last (in identity order) of the commands `undecided_reached`
    // finds from each selected entry's and kept candidate's dependencies,
    // with no member: all the order check reads of it. A walk per adopted
    // command and candidate cost a walk of the selection per pair; here
    // each command is labelled once a pass, from the last re-proposal
    // down, by walking `dependents` back from it. A command labelled
    // already reaches one at least as late, and so does every command
    // that reaches it, so the walk stops there (task-d108).
    let last_undecided =
        |candidates: &BTreeMap<CommandId, (ReportEntry, usize)>| -> BTreeMap<CommandId, CommandId> {
            let present = |u: &CommandId| entries.contains_key(u) || candidates.contains_key(u);
            let mut waiting: BTreeMap<CommandId, Vec<CommandId>> = BTreeMap::new();
            for (u, deps) in entries
                .iter()
                .map(|(u, e)| (u, &e.deps))
                .chain(candidates.iter().map(|(u, (e, _))| (u, &e.deps)))
            {
                for d in deps {
                    tick();
                    if !present(d) && held_undecided.contains(d) && !decided.decided(d) {
                        waiting.entry(*d).or_default().push(*u);
                    }
                }
            }
            let mut last = BTreeMap::new();
            for (undecided, waiters) in waiting.iter().rev() {
                let mut stack = waiters.clone();
                while let Some(u) = stack.pop() {
                    tick();
                    if last.contains_key(&u) {
                        continue;
                    }
                    last.insert(u, *undecided);
                    stack.extend(
                        dependents
                            .get(&u)
                            .into_iter()
                            .flatten()
                            .filter(|w| present(w) && !last.contains_key(*w)),
                    );
                }
            }
            last
        };
    // Each pass judges every candidate against the same set, and drops
    // first the failing ones that follow no other failing candidate. A
    // candidate that follows one judged against a set still holding it
    // is judged again once that one is gone: dropped, it is a command the
    // selection re-proposes, and a candidate after it may then pass
    // (task-d33). Dropped in the order the identities sort, the result
    // depended on which of the two came first.
    #[cfg(debug_assertions)]
    let checked = entries.len().saturating_add(candidates.len()) <= CHECKED_SELECTION;
    loop {
        let keys: Vec<CommandId> = candidates.keys().copied().collect();
        let mut failing: BTreeSet<CommandId> = BTreeSet::new();
        // Labelled on the first candidate that waits on a re-proposal.
        let mut lasts: Option<BTreeMap<CommandId, CommandId>> = None;
        for c in keys {
            let (entry, member) = candidates[&c].clone();
            let first = fast_reporters[member];
            let prefix = closure(first, &c);
            let ordered = executed_closure(first, &c, &candidates);
            let after_c = ordered_after(&candidates, &c);
            // What `forgotten_before` reads of the selection's order:
            // every command the closure of a command in `ordered` reaches
            // that the member no longer holds and the selection does.
            // Found once per candidate, on the first adopted command that
            // asks.
            let mut before_forgotten: Option<BTreeSet<CommandId>> = None;
            // Whether the member's order put `a` before `c`, whose closure
            // over the member's records is `ordered`: `a` is in it, or is
            // decided before a command in it that the member no longer
            // holds. Such a command was executed there, after everything
            // the selection orders before it, and retiring it cut the
            // member's own closure short.
            let mut forgotten_before = |a: &CommandId| -> bool {
                if ordered.contains(a) {
                    return true;
                }
                let reached = before_forgotten.get_or_insert_with(|| {
                    reached_from(
                        &candidates,
                        &mut ordered
                            .iter()
                            .filter(|d| record(first, d).is_none() && entries.contains_key(*d))
                            .copied(),
                    )
                });
                // Checked below against the walk it replaces, but only
                // ever on its false case (task-d108 review). The
                // simulator builds no C1 configuration, and no test
                // reaches a forgotten order that puts `a` before `c`
                // past `ordered`: `executed_closure` already follows the
                // selected entries through what the member retired, so
                // the walk adds a command only on a path through a
                // candidate the member does not hold.
                #[cfg(debug_assertions)]
                if checked {
                    // Not counted: the walks the counted steps replace.
                    let counted = work.get();
                    let walked = ordered.iter().any(|d| {
                        record(first, d).is_none()
                            && entries.contains_key(d)
                            && after(&candidates, d, a)
                    });
                    debug_assert_eq!(reached.contains(a), walked, "forgotten order of {a:?}");
                    work.set(counted);
                }
                reached.contains(a)
                    || ordered.iter().any(|d| {
                        tick();
                        decided.executed_before(a, d)
                    })
            };
            // The commands no ballot decided that `c` follows, which this
            // selection re-proposes: its own dependencies, followed
            // through what the selection keeps, reach them. The
            // re-proposals are chained after the recovered order and `c`
            // after them (task-d34). Only the selection's dependencies
            // count as a way in: an undecided command reached through the
            // member's records alone orders nothing.
            let awaited = undecided_reached(&entry.deps, &candidates, Some(first));
            let awaits_reproposal = !awaited.is_empty();
            if awaits_reproposal && lasts.is_none() {
                lasts = Some(last_undecided(&candidates));
            }
            let lasts = if awaits_reproposal {
                lasts.as_ref()
            } else {
                None
            };
            // What `c`'s own dependencies reach through what the
            // selection keeps.
            let selected_before = {
                let mut seen = BTreeSet::new();
                let mut stack: Vec<CommandId> = entry.deps.clone();
                while let Some(d) = stack.pop() {
                    tick();
                    if !seen.insert(d) {
                        continue;
                    }
                    if let Some(e) = entries.get(&d) {
                        stack.extend(e.deps.iter().copied());
                    } else if let Some((e, _)) = candidates.get(&d) {
                        stack.extend(e.deps.iter().copied());
                    }
                }
                seen
            };
            let ordered_after_adopted = entries.values().all(|adopted| {
                tick();
                // Ordered after `c` by the selection itself: nothing it
                // says constrains the member's order before `c`.
                #[cfg(debug_assertions)]
                if checked {
                    // Not counted: the walks the counted steps replace.
                    let counted = work.get();
                    debug_assert_eq!(
                        after_c.contains(&adopted.command),
                        after(&candidates, &adopted.command, &c),
                        "order of {:?} after {c:?}",
                        adopted.command,
                    );
                    work.set(counted);
                }
                if after_c.contains(&adopted.command) {
                    return true;
                }
                // Before the re-proposals `c` follows, whatever the
                // member's records of the undecided commands say: the
                // command follows no re-proposal, so the chain goes on
                // after it. Those records can be stale: the leader that
                // decided `c` fast may have accepted such a command after
                // this one, while the member kept its own pre-acceptance
                // of it, and the member's closure then misses this command
                // (task-d33, protocol_sim row 10, three voters, seed 31).
                // An adopted command that waits on a re-proposal too is
                // not ordered against `c` that way (row 2, seed 3).
                if let Some(lasts) = lasts {
                    let waits_last = lasts.get(&adopted.command);
                    let own_last = lasts.get(&c);
                    #[cfg(debug_assertions)]
                    if checked {
                        // Not counted: the walks the counted steps replace.
                        let counted = work.get();
                        debug_assert_eq!(
                            waits_last,
                            undecided_reached(&adopted.deps, &candidates, None).last(),
                            "last re-proposal {:?} waits on",
                            adopted.command,
                        );
                        debug_assert_eq!(
                            own_last,
                            undecided_reached(&entry.deps, &candidates, None).last(),
                            "last re-proposal {c:?} waits on",
                        );
                        work.set(counted);
                    }
                    if waits_last.is_none() || selected_before.contains(&adopted.command) {
                        return true;
                    }
                    // It waits on a re-proposal too. Neither command can
                    // have executed anywhere, since each follows a command
                    // no ballot decided, so the only order either will
                    // execute in is the one this selection's re-proposals
                    // give them, and the member's records of the undecided
                    // commands, which the re-proposals replace, are no
                    // evidence of it either way (task-d71). The new leader
                    // re-proposes the undecided commands one after another
                    // in identity order, and an entry right after the last
                    // one it waits on (`Leader::from_recovered`), so two
                    // entries whose last differs are chained by the
                    // re-proposals between them, in whichever order: the
                    // drop that order was read for re-proposed `c` under
                    // other dependencies than a fast quorum may have
                    // learned it with (protocol_sim row 5, three voters,
                    // seed 822). Two entries waiting last on the same one
                    // are re-proposed beside each other with their own
                    // dependencies and no edge between them (row 2, seed
                    // 3), so only a command with no conflict is exempt.
                    if waits_last != own_last {
                        return true;
                    }
                    let keys: Vec<Vec<u8>> = adopted.paths.iter().map(|(k, _)| k.clone()).collect();
                    return !keys.is_empty() && !conflicts(&entry.keys, &keys);
                }
                match record(first, &adopted.command) {
                    // Held by the member: before `c` in its order, or no
                    // conflict. Its own path for it is no evidence either
                    // way (task-d33): a leader synchronization re-bases
                    // the member's log after it, while its record keeps
                    // the path it was pre-accepted with, so a member
                    // whose path for `c` is the leader's -- and whose
                    // fast acknowledgement of `c` decided it -- can hold
                    // an ancestor under another. Nor is a closure the
                    // member cut short by retiring a command it executed:
                    // what the selection orders before that command was
                    // executed there before it, so before `c`.
                    Some(a) => {
                        !conflicts(&entry.keys, &a.keys) || forgotten_before(&adopted.command)
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
                    None if forgotten_before(&adopted.command) => true,
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
            // Nor is a dependency no ballot decided (task-d33). The
            // leader proposes `c` with the dependencies it knows, decided
            // or not, and a fast quorum decides `c` with them: a command
            // the source leader re-proposed and a Sync demoted, or one
            // only the leader accepted, can be named by a fast decision
            // while it is still undecided. This selection re-proposes it
            // and, when `c`'s dependencies reach it (`awaited`), orders
            // `c` after it (task-d34). Requiring `c`'s whole closure to be
            // decided dropped such a `c`, and re-proposed it under other
            // dependencies than the ones it was learned with.
            //
            // Nor is one the member no longer holds (task-d33, row 12,
            // three voters, seed 9). A member at the source ballot has
            // released nothing -- only a later Sync releases -- and a
            // pre-acceptance names only commands it held with a payload,
            // so the member executed it and retired it past the window
            // its report names. What it executed precedes `c`.
            let prefix_decided = prefix.iter().all(|d| {
                tick();
                entries.contains_key(d)
                    || candidates.contains_key(d)
                    || awaited.contains(d)
                    || record(first, d).is_none_or(|r| r.path == crate::graph::demoted_path())
            });
            // Nor one whose own dependencies, through what the selection
            // keeps, come back to it: a command the selection orders after
            // `c` is also before it, a cycle no decision holds, so `c` was
            // not decided with these dependencies (row 10, three voters,
            // seed 884, where a committed command depended on `c`).
            let closes_cycle = after_c.contains(&c);
            #[cfg(debug_assertions)]
            if checked {
                // Not counted: the walks the counted steps replace.
                let counted = work.get();
                debug_assert_eq!(
                    closes_cycle,
                    after(&candidates, &c, &c),
                    "cycle through {c:?}"
                );
                work.set(counted);
            }
            if !ordered_after_adopted || !prefix_decided || closes_cycle {
                failing.insert(c);
            }
        }
        if failing.is_empty() {
            break;
        }
        let follows_failing = |c: &CommandId| -> bool {
            let mut seen = BTreeSet::new();
            let mut stack: Vec<CommandId> = candidates[c].0.deps.clone();
            while let Some(d) = stack.pop() {
                tick();
                if d == *c || !seen.insert(d) {
                    continue;
                }
                if failing.contains(&d) {
                    return true;
                }
                if let Some(e) = entries.get(&d) {
                    stack.extend(e.deps.iter().copied());
                } else if let Some((e, _)) = candidates.get(&d) {
                    stack.extend(e.deps.iter().copied());
                }
            }
            false
        };
        let settled: Vec<CommandId> = failing
            .iter()
            .copied()
            .filter(|c| !follows_failing(c))
            .collect();
        // Failing candidates that all follow one another: a cycle, with
        // none settled first. They go together.
        let dropped: Vec<CommandId> = if settled.is_empty() {
            failing.into_iter().collect()
        } else {
            settled
        };
        for c in dropped {
            // What the dropped candidate's dependencies reach that no
            // report holds (task-d119). Read for the counts, not the
            // selection, and counted as its work all the same, so the
            // work bound covers everything a selection does.
            let (e, member) = &candidates[&c];
            let first = fast_reporters[*member];
            let (mut unreported, mut undecided) = (false, false);
            let mut seen = BTreeSet::new();
            let mut stack: Vec<CommandId> = e.deps.clone();
            while let Some(d) = stack.pop() {
                tick();
                if !seen.insert(d) {
                    continue;
                }
                let reported = || by_report.iter().any(|b| b.contains_key(&d));
                if let Some(x) = entries.get(&d) {
                    stack.extend(x.deps.iter().copied());
                } else if let Some((x, _)) = candidates.get(&d) {
                    stack.extend(x.deps.iter().copied());
                } else if decided.decided(&d) {
                    unreported |= !reported();
                } else if let Some(r) = by_report[first].get(&d) {
                    // Not through a record a Sync demoted: a pre-acceptance
                    // of an earlier ballot, and no order the source
                    // ballot's leader gave. A fast decision at the source
                    // counted the leader's record of `c`, and the leader
                    // ordered `d` by its own re-proposal of it, so what the
                    // demoted record names was never part of that decision.
                    // The prefix check above reads it as absent too.
                    if r.path != crate::graph::demoted_path() {
                        stack.extend(r.deps.iter().copied());
                    }
                } else if !reported() {
                    unreported = true;
                    undecided = true;
                }
            }
            let add = |cell: &Cell<u64>, yes: bool| cell.set(cell.get() + u64::from(yes));
            add(&counts.unreported, unreported);
            add(&counts.unreported_undecided, undecided);
            candidates.remove(&c);
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

/// The largest selection, entries and candidates together, whose
/// possible-fast checks are checked against the walks they replace in
/// debug builds (task-d108). Over a larger one the walks are the cost the
/// replacement removes.
#[cfg(debug_assertions)]
const CHECKED_SELECTION: usize = 512;

#[cfg(test)]
mod tests {
    use super::*;
    use coord_types::ids::ConfigurationEpoch;

    fn r(i: u8) -> ReplicaId {
        ReplicaId([i; 16])
    }

    fn ballot(number: u64, leader: u8) -> Ballot {
        Ballot {
            epoch: ConfigurationEpoch::new(1).unwrap(),
            number,
            leader: r(leader),
        }
    }

    fn id(n: u64) -> CommandId {
        let mut d = [0u8; 32];
        d[..8].copy_from_slice(&n.to_be_bytes());
        d[8] = 7;
        CommandId(Digest32(d))
    }

    fn entry(n: u64, phase: Phase, deps: Vec<CommandId>, path: u8) -> ReportEntry {
        let key = alloc::vec![(n % 5) as u8];
        ReportEntry {
            command: id(n),
            phase,
            deps,
            path: Digest32([path; 32]),
            paths: alloc::vec![(key.clone(), Digest32([path; 32]))],
            seqnum: n + 1,
            keys: alloc::vec![key],
            payload_present: true,
            admission: None,
        }
    }

    /// The takeover the review measured (#98): three voters reporting the
    /// same `commits` at COMMIT over five keys, each after the last on its
    /// key, and `pre_accepted` more pre-accepted alike at every voter,
    /// each after the last commit on its key. With `undecided`, the first
    /// voter alone pre-accepted one more command, which every
    /// pre-acceptance and every other commit follows as well: the
    /// selection re-proposes it, each candidate awaits it, and the order
    /// check reads what each adopted command waits on.
    fn reports(commits: u64, pre_accepted: u64, undecided: bool) -> Vec<RecoveryReport> {
        const REPROPOSED: u64 = 1 << 40;
        let reproposed = id(REPROPOSED);
        let mut entries = Vec::new();
        let mut last: [Option<CommandId>; 5] = [None; 5];
        for n in 0..commits + pre_accepted {
            let k = (n % 5) as usize;
            let mut deps: Vec<CommandId> = last[k].into_iter().collect();
            if undecided && (n >= commits || n % 2 == 1) {
                deps.push(reproposed);
            }
            if n < commits {
                entries.push(entry(n, Phase::Commit, deps, 1));
                last[k] = Some(id(n));
            } else {
                entries.push(entry(n, Phase::PreAccept, deps, 3));
            }
        }
        (0..3)
            .map(|v| {
                let mut entries = entries.clone();
                if undecided && v == 0 {
                    entries.push(entry(REPROPOSED, Phase::PreAccept, Vec::new(), 4));
                }
                RecoveryReport {
                    replica: r(v),
                    ballot: ballot(1, 1),
                    committed_ballot: ballot(0, 0),
                    entries,
                }
            })
            .collect()
    }

    /// The steps one selection takes over `reports`, and how many of the
    /// pre-acceptances it adopted.
    fn work(reports: &[RecoveryReport]) -> (u64, usize) {
        let config = BallotConfiguration::c2_default(
            ConfigurationEpoch::new(1).unwrap(),
            ballot(1, 1),
            (0..3).map(r).collect(),
        )
        .unwrap();
        let counts = SelectionCounts::default();
        let decision = select_counted(
            &config,
            reports,
            |_, _| false,
            |_| None,
            &|_: &CommandId| false,
            SelectionRules::default(),
            false,
            &counts,
        )
        .unwrap();
        let adopted = decision
            .entries
            .values()
            .filter(|e| e.phase == Phase::Accept)
            .count();
        (counts.work.get(), adopted)
    }

    /// Each step is a record, dependency or order fact read. Walked per
    /// adopted command and candidate, as before task-d108, the steps grew
    /// with the square of the commits: 1.4 s over 2,000 at the review's
    /// measure (#98), four times that over twice as many.
    #[test]
    fn a_takeover_selects_in_steps_linear_in_the_commits_reported() {
        const PRE_ACCEPTED: u64 = 30;
        for undecided in [false, true] {
            let mut before = None;
            for commits in [250, 500, 1_000, 2_000] {
                let (steps, adopted) = work(&reports(commits, PRE_ACCEPTED, undecided));
                assert_eq!(
                    adopted as u64, PRE_ACCEPTED,
                    "every pre-acceptance is adopted"
                );
                // A few walks of the selection for each candidate.
                let bound = 4 * (PRE_ACCEPTED + 1) * (commits + PRE_ACCEPTED);
                assert!(
                    steps <= bound,
                    "{steps} steps over {commits} commits (undecided {undecided}), bound {bound}"
                );
                if let Some(half) = before {
                    assert!(
                        steps * 10 <= half * 21,
                        "{steps} steps over {commits} commits, {half} over half as many"
                    );
                }
                before = Some(steps);
            }
        }
    }

    /// A report listing a command twice is read at its first record, as
    /// the scan before task-d108 read it: the selection is the one taken
    /// without the later record. No report builder here lists a command
    /// twice, so this is the one case where the index's check (which
    /// record of a command it holds) can find a difference.
    #[test]
    fn a_report_listing_a_command_twice_is_read_at_its_first_record() {
        let config = BallotConfiguration::c2_default(
            ConfigurationEpoch::new(1).unwrap(),
            ballot(1, 1),
            (0..3).map(r).collect(),
        )
        .unwrap();
        let select = |reports: &[RecoveryReport]| {
            select_counted(
                &config,
                reports,
                |_, _| false,
                |_| None,
                &|_: &CommandId| false,
                SelectionRules::default(),
                false,
                &SelectionCounts::default(),
            )
            .unwrap()
        };
        let once = reports(20, 6, true);
        let mut twice = once.clone();
        // A commit listed again below COMMIT, and a pre-acceptance listed
        // again under other dependencies and another path. (A later
        // record at COMMIT is a decision wherever it is listed, and the
        // selection takes it: not a case of which record is read.)
        twice[0]
            .entries
            .push(entry(2, Phase::PreAccept, alloc::vec![id(21)], 9));
        twice[0]
            .entries
            .push(entry(22, Phase::PreAccept, alloc::vec![id(2)], 9));
        assert_eq!(select(&twice), select(&once));
    }

    /// Seed 147's shape (protocol_sim row 10, three voters, no catch-up,
    /// three in four acknowledgements to a follower lost). The source
    /// leader r0 ordered A, D, X, C on one key, each after the one before.
    /// The fast-set member restarted and refused D's re-proposal while its
    /// promise was in flight, and was never sent it again. It took r0's
    /// Sync, which keeps X at ACCEPT naming D, and whose installation of X
    /// waits for D; and it pre-accepted C after X. So it reports A and X at
    /// ACCEPT and C pre-accepted, and no record of D. A fast quorum of r0
    /// and the member decided C with the dependency {X}. The other reporter
    /// holds A and X, and D at an old pre-acceptance (`other_holds_d`) or
    /// not at all.
    fn a_fast_candidate_behind_an_unheld_dependency(
        other_holds_d: bool,
        decided: &impl CandidateDecisions,
        counts: &SelectionCounts,
    ) -> SyncDecision {
        ruled_fast_candidate_behind_an_unheld_dependency(
            other_holds_d,
            decided,
            SelectionRules::default(),
            counts,
        )
    }

    /// [`a_fast_candidate_behind_an_unheld_dependency`] under `rules`, the
    /// rule deciding where they turn it on.
    fn ruled_fast_candidate_behind_an_unheld_dependency(
        other_holds_d: bool,
        decided: &impl CandidateDecisions,
        rules: SelectionRules,
        counts: &SelectionCounts,
    ) -> SyncDecision {
        let epoch = ConfigurationEpoch::new(1).unwrap();
        let source =
            BallotConfiguration::c2_default(epoch, ballot(0, 0), (0..3).map(r).collect()).unwrap();
        let member = (1u8..3)
            .find(|v| source.fast_set().contains(&r(*v)))
            .unwrap();
        let other = 3 - member;
        let config =
            BallotConfiguration::c2_default(epoch, ballot(1, other), (0..3).map(r).collect())
                .unwrap();
        // All on one key.
        let (a, d, x, c) = (0u64, 5, 10, 15);
        let held = alloc::vec![
            entry(a, Phase::Accept, Vec::new(), 1),
            entry(x, Phase::Accept, alloc::vec![id(d)], 1),
        ];
        let mut of_member = held.clone();
        of_member.push(entry(c, Phase::PreAccept, alloc::vec![id(x)], 1));
        let mut of_other = held;
        if other_holds_d {
            of_other.push(entry(d, Phase::PreAccept, Vec::new(), 4));
        }
        let report = |replica: u8, entries: Vec<ReportEntry>| RecoveryReport {
            replica: r(replica),
            ballot: ballot(1, other),
            committed_ballot: ballot(0, 0),
            entries,
        };
        select_counted(
            &config,
            &[report(member, of_member), report(other, of_other)],
            |_, _| false,
            |_| None,
            decided,
            rules,
            rules.decide_nothing,
            counts,
        )
        .unwrap()
    }

    /// Seed 147 (row 10, three voters, no catch-up, loss 750000): a fast
    /// candidate whose member's closure stops at a command the member
    /// never held, which another report holds undecided. The selection
    /// re-proposes that command and keeps the candidate with the
    /// dependencies it was learned with, after it. Before, the member's
    /// cut closure was read as an order that left A out, the candidate was
    /// re-proposed, and a voter executed it under other dependencies than
    /// the frontend learned.
    #[test]
    fn a_fast_candidate_behind_a_dependency_its_member_never_held_is_kept() {
        let counts = SelectionCounts::default();
        let decision =
            a_fast_candidate_behind_an_unheld_dependency(true, &|_: &CommandId| false, &counts);
        let kept = decision.entries.get(&id(15)).map(|e| e.deps.clone());
        assert_eq!(kept, Some(alloc::vec![id(10)]), "C kept with {{X}}");
        assert!(!decision.reproposed.contains(&id(15)), "C not re-proposed");
        assert!(decision.reproposed.contains(&id(5)), "D re-proposed");
        // Nothing dropped, so nothing counted (task-d119).
        assert_eq!(
            (counts.unreported.get(), counts.unreported_undecided.get()),
            (0, 0)
        );
    }

    /// KNOWN GAP in the selection, which the candidate closes. This pins
    /// today's selection, which the no-report note under task-d119 (the
    /// plan) keeps.
    ///
    /// The same shape with no report holding D at all. The member's
    /// closure stops at D, the order check reads that as an order that
    /// leaves A out, and C is re-proposed. A fast quorum decided C with
    /// {X}, so a voter given C's re-proposal could execute it under other
    /// dependencies than the frontend learned, seed 147's failure. None is
    /// given it: X is selected naming D, which the selection neither
    /// carries nor re-proposes, so a candidate that does not hold D
    /// committed refuses to bind it and gives its campaign up as `Behind`
    /// (`Follower::selection_gap`, task-d05; the activation test
    /// `a_selection_naming_a_dependency_no_report_holds_is_given_up_as_behind`).
    /// The cost is a stall, not a fork. A candidate that holds D decided
    /// does bind it, and there the drop is right: it executed everything D
    /// reaches, so an adopted A it has not executed runs after D at every
    /// voter, and one it executed before D is read by `executed_before`
    /// (the two tests after this one). task-d71's model test
    /// `a_candidate_after_a_command_executed_after_an_adopted_one_is_kept`
    /// re-proposes in the same shape ("nothing known, nothing kept") though
    /// its own story says c was decided fast. A rule that keeps C here must
    /// show it cannot leave C unordered against A when the member executed
    /// and forgot D before A (the test below).
    #[test]
    fn known_gap_a_fast_candidate_behind_a_dependency_no_report_holds_is_reproposed() {
        let counts = SelectionCounts::default();
        let decision =
            a_fast_candidate_behind_an_unheld_dependency(false, &|_: &CommandId| false, &counts);
        assert!(decision.reproposed.contains(&id(15)), "C re-proposed");
        assert!(!decision.entries.contains_key(&id(15)), "C not kept");
        assert!(!decision.reproposed.contains(&id(5)), "D held by no report");
        assert!(decision.entries[&id(10)].deps.contains(&id(5)), "X names D");
        // The drop the simulator counts (task-d119): C, behind D, which no
        // report holds and the candidate does not hold decided.
        assert_eq!(
            (counts.unreported.get(), counts.unreported_undecided.get()),
            (1, 1)
        );
    }

    /// The known gap under task-d121's rule: the order check dropped C
    /// behind D, which no report holds and the candidate doesn't hold
    /// decided, so condition 5 keeps the rule off. D is not decided as
    /// nothing, the selection is the one it is without the rule, and the
    /// candidate still gives up as `Behind` on D. Deciding D as nothing
    /// here would bind C's re-proposal beside the fast decision `{r0, m}`
    /// may have made with `{X}`.
    #[test]
    fn known_gap_under_the_rule_decides_nothing_and_counts_the_selection_kept_off() {
        let rules = SelectionRules {
            leave_out_unheld: true,
            decide_nothing: true,
        };
        let counts = SelectionCounts::default();
        let ruled = ruled_fast_candidate_behind_an_unheld_dependency(
            false,
            &|_: &CommandId| false,
            rules,
            &counts,
        );
        assert!(ruled.nothing.is_empty(), "{ruled:?}");
        assert!(!ruled.reproposed.contains(&id(5)), "D left a gap");
        assert_eq!(counts.unreported_undecided.get(), 1);
        assert_eq!(
            (counts.kept_by_drop.get(), counts.decided_nothing.get()),
            (1, 0)
        );
        let plain = a_fast_candidate_behind_an_unheld_dependency(
            false,
            &|_: &CommandId| false,
            &SelectionCounts::default(),
        );
        assert_eq!(ruled.entries, plain.entries);
        assert_eq!(ruled.reproposed, plain.reproposed);
        // Control: where the candidate holds D decided, the drop is
        // counted apart and the rule has no D to decide.
        let counts = SelectionCounts::default();
        let decided = ruled_fast_candidate_behind_an_unheld_dependency(
            false,
            &ExecutedInOrder(alloc::vec![id(5)]),
            rules,
            &counts,
        );
        assert!(decided.nothing.is_empty());
        assert_eq!(
            (counts.kept_by_drop.get(), counts.decided_nothing.get()),
            (0, 0)
        );
    }

    /// What the candidate executed, in order, for the known gap's bound
    /// half (#308 review).
    struct ExecutedInOrder(Vec<CommandId>);

    impl CandidateDecisions for ExecutedInOrder {
        fn decided(&self, command: &CommandId) -> bool {
            self.0.contains(command)
        }

        fn executed_before(&self, first: &CommandId, then: &CommandId) -> bool {
            let at = |c: &CommandId| self.0.iter().position(|e| e == c);
            matches!((at(first), at(then)), (Some(a), Some(b)) if a < b)
        }
    }

    /// The known gap's shape where the candidate executed and forgot D
    /// and holds nothing else decided: the selection is bound (D is
    /// committed at the candidate), and C is re-proposed. Rightly: the
    /// candidate executed everything D reaches, and not A, so A runs after
    /// D at every voter, and C with {X} does not reach A through D.
    #[test]
    fn a_fast_candidate_behind_a_dependency_only_the_candidate_decided_is_reproposed() {
        let counts = SelectionCounts::default();
        let decision = a_fast_candidate_behind_an_unheld_dependency(
            false,
            &ExecutedInOrder(alloc::vec![id(5)]),
            &counts,
        );
        assert!(decision.reproposed.contains(&id(15)), "C re-proposed");
        assert!(!decision.entries.contains_key(&id(15)), "C not kept");
        // No report holds D, and the candidate holds it decided.
        assert_eq!(
            (counts.unreported.get(), counts.unreported_undecided.get()),
            (1, 0)
        );
    }

    /// The same, where the candidate also executed A, before D: its own
    /// order says A ran before D, so C with {X} follows A through D, and
    /// C is kept with the dependencies it was learned with.
    #[test]
    fn a_fast_candidate_behind_a_dependency_the_candidate_executed_after_a_is_kept() {
        let counts = SelectionCounts::default();
        let decision = a_fast_candidate_behind_an_unheld_dependency(
            false,
            &ExecutedInOrder(alloc::vec![id(0), id(5)]),
            &counts,
        );
        let kept = decision.entries.get(&id(15)).map(|e| e.deps.clone());
        assert_eq!(kept, Some(alloc::vec![id(10)]), "C kept with {{X}}");
        assert_eq!(
            (counts.unreported.get(), counts.unreported_undecided.get()),
            (0, 0)
        );
    }

    /// What any rule that closes the gap above must still do. No report
    /// holds D, and A follows X as well: the source leader ordered D, X,
    /// A, C, and the member pre-accepted C after X before it took A. Every
    /// reporter may have executed and forgotten D, so D orders nothing
    /// between A and C, and kept with {X}, C would be unordered against A.
    /// So C is re-proposed after the recovered order, A included.
    #[test]
    fn a_candidate_beside_an_adopted_command_that_follows_the_same_unheld_dependency_is_reproposed()
    {
        let epoch = ConfigurationEpoch::new(1).unwrap();
        let source =
            BallotConfiguration::c2_default(epoch, ballot(0, 0), (0..3).map(r).collect()).unwrap();
        let member = (1u8..3)
            .find(|v| source.fast_set().contains(&r(*v)))
            .unwrap();
        let other = 3 - member;
        let config =
            BallotConfiguration::c2_default(epoch, ballot(1, other), (0..3).map(r).collect())
                .unwrap();
        // All on one key.
        let (a, d, x, c) = (0u64, 5, 10, 15);
        let held = alloc::vec![
            entry(x, Phase::Accept, alloc::vec![id(d)], 1),
            entry(a, Phase::Accept, alloc::vec![id(x)], 1),
        ];
        let mut of_member = held.clone();
        of_member.push(entry(c, Phase::PreAccept, alloc::vec![id(x)], 1));
        let report = |replica: u8, entries: Vec<ReportEntry>| RecoveryReport {
            replica: r(replica),
            ballot: ballot(1, other),
            committed_ballot: ballot(0, 0),
            entries,
        };
        let decision = select_counted(
            &config,
            &[report(member, of_member), report(other, held)],
            |_, _| false,
            |_| None,
            &|_: &CommandId| false,
            SelectionRules::default(),
            false,
            &SelectionCounts::default(),
        )
        .unwrap();
        assert!(!decision.entries.contains_key(&id(15)), "C not kept");
        assert!(decision.reproposed.contains(&id(15)), "C re-proposed");
        assert!(decision.entries.contains_key(&id(0)), "A adopted");
    }

    /// A takeover whose candidates are all dropped, the counting walk's
    /// worst shape (#309 review): `accepted` commands at ACCEPT chained on
    /// one key, X at ACCEPT after all but the last of them and naming a D
    /// no report holds, and `dropped` candidates the fast-set member alone
    /// pre-accepted after X. Each candidate's closure misses the last
    /// accepted command, so the order check drops it, and the counting
    /// walk reads the whole chain from it (task-d119). The steps the
    /// selection takes, the candidates it re-proposed, and the ones the
    /// walk counted behind D.
    fn dropped_work(accepted: u64, dropped: u64) -> (u64, usize, u64) {
        let epoch = ConfigurationEpoch::new(1).unwrap();
        let source =
            BallotConfiguration::c2_default(epoch, ballot(0, 0), (0..3).map(r).collect()).unwrap();
        let member = (1u8..3)
            .find(|v| source.fast_set().contains(&r(*v)))
            .unwrap();
        let other = 3 - member;
        let config =
            BallotConfiguration::c2_default(epoch, ballot(1, other), (0..3).map(r).collect())
                .unwrap();
        // Every command on key 0.
        let on_key = |i: u64| 5 * i;
        let (d, x) = (on_key(accepted), on_key(accepted + 1));
        let mut held: Vec<ReportEntry> = (0..accepted)
            .map(|i| {
                let deps = (i > 0).then(|| id(on_key(i - 1))).into_iter().collect();
                entry(on_key(i), Phase::Accept, deps, 1)
            })
            .collect();
        held.push(entry(
            x,
            Phase::Accept,
            alloc::vec![id(d), id(on_key(accepted - 2))],
            1,
        ));
        let mut of_member = held.clone();
        let candidates: Vec<u64> = (0..dropped).map(|j| on_key(accepted + 2 + j)).collect();
        for c in &candidates {
            of_member.push(entry(*c, Phase::PreAccept, alloc::vec![id(x)], 1));
        }
        let report = |replica: u8, entries: Vec<ReportEntry>| RecoveryReport {
            replica: r(replica),
            ballot: ballot(1, other),
            committed_ballot: ballot(0, 0),
            entries,
        };
        let counts = SelectionCounts::default();
        let decision = select_counted(
            &config,
            &[report(member, of_member), report(other, held)],
            |_, _| false,
            |_| None,
            &|_: &CommandId| false,
            SelectionRules::default(),
            false,
            &counts,
        )
        .unwrap();
        let reproposed = candidates
            .iter()
            .filter(|c| decision.reproposed.contains(&id(**c)))
            .count();
        (
            counts.work.get(),
            reproposed,
            counts.unreported_undecided.get(),
        )
    }

    /// The counting walk's steps are the selection's work (#309 review).
    /// In its worst shape the walk reads the whole chain once a dropped
    /// candidate, `accepted + 1` steps; the order check already reads it
    /// about eleven times a candidate there (84,074 steps at 250 accepted
    /// and 30 dropped without the walk, 91,604 with it). The steps grow
    /// linearly in the commands reported, under a bound of sixteen walks
    /// of the selection a candidate. (At 2,000 accepted the selection
    /// passes [`MAX_REPORT_ENTRIES`] and re-proposes none of them.)
    #[test]
    fn a_takeover_that_drops_its_candidates_counts_their_walks_as_work() {
        const DROPPED: u64 = 30;
        let mut before = None;
        for accepted in [125, 250, 500, 1_000] {
            let (steps, reproposed, behind_d) = dropped_work(accepted, DROPPED);
            assert_eq!(reproposed as u64, DROPPED, "every candidate dropped");
            assert_eq!(behind_d, DROPPED, "every walk reached D");
            let bound = 16 * DROPPED * (accepted + DROPPED);
            assert!(
                steps <= bound,
                "{steps} steps over {accepted} accepted, bound {bound}"
            );
            // The walk's own steps, pinned (#310 review): the order check
            // takes 84,074 here, and each dropped candidate's walk reads
            // the chain once, `accepted + 1` steps. Left uncounted, the
            // bounds above still hold.
            if accepted == 250 {
                assert_eq!(
                    steps,
                    84_074 + DROPPED * (accepted + 1),
                    "the counting walk's steps over {accepted} accepted"
                );
            }
            if let Some(half) = before {
                assert!(
                    steps * 10 <= half * 21,
                    "{steps} steps over {accepted} accepted, {half} over half as many"
                );
            }
            before = Some(steps);
        }
    }

    /// The cost a pass still has (task-d108 review): each pass judges every
    /// candidate against the selection, so the steps grow with candidates
    /// times entries. Pinned so: at a fixed 500 commits, doubling the
    /// pre-acceptances about doubles the steps, and never more than a few
    /// walks of the selection per candidate.
    #[test]
    fn a_takeover_selects_in_steps_linear_in_the_candidates_times_the_entries() {
        const COMMITS: u64 = 500;
        for undecided in [false, true] {
            let mut before = None;
            for pre_accepted in [30u64, 60, 120, 240] {
                let (steps, adopted) = work(&reports(COMMITS, pre_accepted, undecided));
                assert_eq!(
                    adopted as u64, pre_accepted,
                    "every pre-acceptance is adopted"
                );
                let bound = 4 * (pre_accepted + 1) * (COMMITS + pre_accepted);
                assert!(
                    steps <= bound,
                    "{steps} steps at {pre_accepted} pre-acceptances (undecided {undecided}), bound {bound}"
                );
                if let Some(half) = before {
                    assert!(
                        steps * 10 <= half * 21,
                        "{steps} steps at {pre_accepted} pre-acceptances, {half} at half as many"
                    );
                }
                before = Some(steps);
            }
        }
    }
}

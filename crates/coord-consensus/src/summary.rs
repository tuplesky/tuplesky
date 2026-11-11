//! Durable recovery summaries and bounded transfer (task-25; design
//! Sections 4.8-4.9, 5.1, 19.3; prototype `fillNewLeaderAckN`).
//!
//! A replica's report for a new ballot is built from its *durable ledger*:
//! the command records whose batches completed `JournalDurable`, never
//! from in-memory phases or from a projection that may lag behind the
//! journal. A vote whose batch has not completed is not a stable vote and
//! is absent from the report; a placeholder without payload is never a
//! report entry. The report travels in bounded pages that bind the
//! transfer (replica, ballot), the page number, the total and a digest;
//! an assembler counts a report only when every page arrived intact, and
//! refuses advertised totals beyond the bound.

use alloc::collections::{BTreeMap, BTreeSet, VecDeque};
use alloc::vec::Vec;

use coord_core::effect::{BarrierId, PeerId};
use coord_types::identity::{Digest32, HashDomain};
use coord_types::ids::{Ballot, LocalJournalSeq, ReplicaId};
use coord_types::{CommandId, RetryKey};
use serde::{Deserialize, Serialize};

use crate::commands::CommandRecord;
use crate::recovery::{RecoveryReport, ReportEntry};

/// Maximum pages one report may announce.
pub const MAX_REPORT_PAGES: u32 = 4096;
/// Maximum entries per page.
pub const MAX_PAGE_ENTRIES: usize = 256;
/// Most report pages one request asks for, and one answer carries
/// (task-d28): an answer is a burst on the lane that drops when full.
pub const MAX_PAGE_ASK: usize = 16;

/// The pages of the report this replica last sent a candidate (task-d28).
///
/// A page is published once on a lane that drops when full, so one lost
/// page used to cost the whole ballot: nothing asked for it, and a
/// regenerated report of the same ballot is another snapshot, which the
/// candidate's assembler refuses as inconsistent. The pages are kept, one
/// report at a time, and a page the candidate asks for again comes from
/// the same version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServedReport {
    /// The candidate, at the incarnation it asked from.
    pub to: PeerId,
    /// The pages, in order.
    pub pages: Vec<ReportPage>,
}

impl ServedReport {
    /// Keep the pages of `report`, sent to `to`.
    pub fn new(to: PeerId, report: &RecoveryReport) -> Self {
        ServedReport {
            to,
            pages: paginate(report, MAX_PAGE_ENTRIES),
        }
    }

    /// The ballot the report is for.
    pub fn ballot(&self) -> Option<Ballot> {
        self.pages.first().map(|p| p.ballot)
    }

    /// The pages `from` asks for under `ballot`: nothing unless it is the
    /// candidate this report went to, for that ballot; the first
    /// [`MAX_PAGE_ASK`] for an empty list; otherwise those named that
    /// exist, at most [`MAX_PAGE_ASK`].
    pub fn answer(&self, from: ReplicaId, ballot: Ballot, asked: &[u32]) -> Vec<ReportPage> {
        if from != self.to.replica || self.ballot() != Some(ballot) {
            return Vec::new();
        }
        if asked.is_empty() {
            return self.pages.iter().take(MAX_PAGE_ASK).cloned().collect();
        }
        let wanted: BTreeSet<u32> = asked.iter().copied().take(MAX_PAGE_ASK).collect();
        self.pages
            .iter()
            .filter(|p| wanted.contains(&p.page))
            .cloned()
            .collect()
    }
}

/// Command records as they are durable, tracked by the actor itself.
#[derive(Clone, Debug, Default)]
pub struct DurableLedger {
    records: BTreeMap<CommandId, CommandRecord>,
    /// Journal sequence each durable record was written at.
    sequences: BTreeMap<CommandId, LocalJournalSeq>,
    /// Records a batch in flight writes, by its barrier: usually one, and
    /// several for a Sync that demotes what it leaves out (task-d11).
    staged: BTreeMap<BarrierId, Vec<(CommandId, CommandRecord)>>,
    /// Records a batch in flight deletes, by its barrier (task-d24).
    removals: BTreeMap<BarrierId, Vec<CommandId>>,
    /// How many durable records the last history sweep left, lowered as
    /// records leave otherwise: what the next sweep is measured from
    /// (task-d110).
    swept_at: usize,
    /// History sweeps run since this ledger was restored (task-d110).
    sweeps: u64,
    /// Executions since the last history sweep (task-d110): each may have
    /// made a kept record forgettable.
    executed_since: usize,
    /// The commands executed here, in the order they executed, whose
    /// history the owner has not dropped yet (task-d111's cut (d)). The
    /// table forgets in that order: a command is forgotten once enough
    /// executions are durable past it and it has left the window of
    /// recent retirements. So the owner drops a forgotten command's
    /// entries from the front of this queue, one at a time, as it
    /// executes others, and a history sweep walks every map only as a
    /// backstop.
    forget_queue: VecDeque<CommandId>,
    /// Commands that left `forget_queue` unforgotten, to keep it
    /// bounded: only a history sweep drops their history, once the table
    /// forgets them.
    forgets_skipped: u64,
    /// The last command the queue let go unforgotten: once the table
    /// forgets it a history sweep is due, whatever the ledger's size
    /// (#221 review). The table forgets in the order commands executed,
    /// so by then it has forgotten every one let go before it too, and
    /// that one sweep drops all their history; without it, a ledger kept
    /// small by forgetting in order might never come due again.
    owed_sweep: Option<CommandId>,
    /// Retry keys bound to a command with no payload here to find them by
    /// (an exact duplicate presented after an earlier boot's rows, or a
    /// transferred payload whose binding was not kept): dropped with the
    /// command's history in order, as its payload's key would be (#221
    /// review). Only those, so it stays small.
    bare_bindings: BTreeMap<CommandId, RetryKey>,
    /// The commands the queue let go unforgotten, whose history is the
    /// next sweep's to drop (debug checks). One can stay in the maps past
    /// a sweep that runs before the table forgets it (#221 review); a
    /// sweep that drops it takes it out of this set.
    #[cfg(debug_assertions)]
    sweep_owes: BTreeSet<CommandId>,
    /// Executions noted since this ledger was made, for the owner's debug
    /// checks of its forgetting.
    executions: u64,
}

impl DurableLedger {
    /// Empty ledger.
    pub const fn new() -> Self {
        DurableLedger {
            records: BTreeMap::new(),
            sequences: BTreeMap::new(),
            staged: BTreeMap::new(),
            removals: BTreeMap::new(),
            swept_at: 0,
            sweeps: 0,
            executed_since: 0,
            forget_queue: VecDeque::new(),
            forgets_skipped: 0,
            owed_sweep: None,
            bare_bindings: BTreeMap::new(),
            #[cfg(debug_assertions)]
            sweep_owes: BTreeSet::new(),
            executions: 0,
        }
    }

    /// A ledger seeded from durable rows (after a crash).
    pub fn restore(rows: impl IntoIterator<Item = (CommandId, CommandRecord)>) -> Self {
        DurableLedger {
            records: rows.into_iter().collect(),
            sequences: BTreeMap::new(),
            staged: BTreeMap::new(),
            removals: BTreeMap::new(),
            swept_at: 0,
            sweeps: 0,
            executed_since: 0,
            forget_queue: VecDeque::new(),
            forgets_skipped: 0,
            owed_sweep: None,
            bare_bindings: BTreeMap::new(),
            #[cfg(debug_assertions)]
            sweep_owes: BTreeSet::new(),
            executions: 0,
        }
    }

    /// A batch writing `record` for `command` was submitted under `barrier`.
    /// A batch that writes several records stages each.
    pub fn stage(&mut self, barrier: BarrierId, command: CommandId, record: CommandRecord) {
        self.staged
            .entry(barrier)
            .or_default()
            .push((command, record));
    }

    /// The batch under `barrier` is durable at `journal_seq`: its record is
    /// now a fact. Completions may arrive in any order, so an older one
    /// never replaces what a newer batch already wrote; the journal
    /// sequence, not the callback order, says which record is newer.
    pub fn durable(
        &mut self,
        barrier: BarrierId,
        journal_seq: LocalJournalSeq,
    ) -> Option<CommandId> {
        for command in self.removals.remove(&barrier).unwrap_or_default() {
            match self.sequences.get(&command) {
                Some(seen) if *seen > journal_seq => {}
                _ => {
                    self.sequences.insert(command, journal_seq);
                    self.records.remove(&command);
                }
            }
        }
        let staged = self.staged.remove(&barrier)?;
        let first = staged.first().map(|(c, _)| *c);
        for (command, record) in staged {
            match self.sequences.get(&command) {
                // Strictly older completions are ignored; distinct batches
                // never share a journal sequence.
                Some(seen) if *seen > journal_seq => {}
                _ => {
                    self.sequences.insert(command, journal_seq);
                    self.records.insert(command, record);
                }
            }
        }
        first
    }

    /// The batch under `barrier` failed: nothing became durable.
    pub fn failed(&mut self, barrier: BarrierId) -> Option<CommandId> {
        self.removals.remove(&barrier);
        self.staged
            .remove(&barrier)
            .and_then(|s| s.first().map(|(c, _)| *c))
    }

    /// The commands the batch under `barrier` writes, while it is in
    /// flight.
    pub fn staged_commands(&self, barrier: &BarrierId) -> Vec<CommandId> {
        self.staged
            .get(barrier)
            .map(|s| s.iter().map(|(c, _)| *c).collect())
            .unwrap_or_default()
    }

    /// The newest record written of `command`: the one the newest batch
    /// in flight stages, else the durable one. Barriers are allocated in
    /// order within a boot.
    pub fn newest(&self, command: &CommandId) -> Option<&CommandRecord> {
        self.staged
            .values()
            .rev()
            .find_map(|batch| batch.iter().rev().find(|(c, _)| c == command))
            .map(|(_, r)| r)
            .or_else(|| self.records.get(command))
    }

    /// The barriers of the batches still in flight.
    pub fn in_flight(&self) -> impl Iterator<Item = BarrierId> + '_ {
        self.staged.keys().copied()
    }

    /// Durable record of a command.
    pub fn record(&self, command: &CommandId) -> Option<&CommandRecord> {
        self.records.get(command)
    }

    /// The record the newest batch in flight writes for each command it
    /// stages: what the ledger holds of it once those batches are durable
    /// (task-d90). Barriers are allocated in order within a boot, and a
    /// restart starts the ledger again from the durable rows.
    pub fn in_flight_records(&self) -> BTreeMap<CommandId, &CommandRecord> {
        let mut out = BTreeMap::new();
        for batch in self.staged.values() {
            for (command, record) in batch {
                out.insert(*command, record);
            }
        }
        out
    }

    /// Whether `command` has a durable record or one in a batch still in
    /// flight: a deletion has to follow either (task-d33).
    pub fn written(&self, command: &CommandId) -> bool {
        self.records.contains_key(command)
            || self
                .staged
                .values()
                .any(|batch| batch.iter().any(|(c, _)| c == command))
    }

    /// Every durable record.
    pub fn records(&self) -> impl Iterator<Item = (&CommandId, &CommandRecord)> {
        self.records.iter()
    }

    /// How many durable records the ledger holds.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether the ledger holds no durable record.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Keep only the durable records of the commands `keep` names. Staged
    /// batches are untouched: what is not durable yet is not the ledger's
    /// to forget.
    pub fn retain(&mut self, keep: impl Fn(&CommandId) -> bool) {
        self.records.retain(|c, _| keep(c));
        self.sequences.retain(|c, _| keep(c));
        if self.owed_sweep.as_ref().is_some_and(|c| !keep(c)) {
            self.owed_sweep = None;
        }
        self.bare_bindings.retain(|c, _| keep(c));
        #[cfg(debug_assertions)]
        self.sweep_owes.retain(|c| keep(c));
    }

    /// `command` was executed: what a sweep could forget may have grown
    /// though the ledger did not (task-d110), and its history is dropped
    /// once the table forgets it ([`DurableLedger::take_forgotten`]).
    pub fn executed(&mut self, command: CommandId) {
        self.executed_since = self.executed_since.saturating_add(1);
        self.executions = self.executions.wrapping_add(1);
        self.forget_queue.push_back(command);
    }

    /// Executions noted since this ledger was made.
    pub const fn executions(&self) -> u64 {
        self.executions
    }

    /// Queue `commands`, executed in this order before the owner was
    /// built, for their history to be dropped as the table forgets them:
    /// the executions a restart restores.
    pub fn executed_before(&mut self, commands: impl IntoIterator<Item = CommandId>) {
        self.forget_queue.extend(commands);
    }

    /// The commands at the front of the forget queue that `forgotten`
    /// says the table has forgotten, at most `limit` of them, in the order
    /// they executed; the owner drops their history ([`DurableLedger::forget`]
    /// and its own maps). Each is taken once.
    ///
    /// The queue holds at most `bound` commands: past it the oldest
    /// leaves whether or not it is forgotten yet, and then only a history
    /// sweep drops what it named ([`DurableLedger::forgets_skipped`]). A
    /// table that never forgets, an unbounded one, so costs nothing.
    pub fn take_forgotten(
        &mut self,
        forgotten: impl Fn(&CommandId) -> bool,
        limit: usize,
        bound: usize,
    ) -> Vec<CommandId> {
        let mut gone = Vec::new();
        while gone.len() < limit {
            match self.forget_queue.front() {
                Some(c) if forgotten(c) => {
                    gone.extend(self.forget_queue.pop_front());
                }
                _ => break,
            }
        }
        while self.forget_queue.len() > bound {
            let Some(c) = self.forget_queue.pop_front() else {
                break;
            };
            self.forgets_skipped += 1;
            self.owed_sweep = Some(c);
            #[cfg(debug_assertions)]
            self.sweep_owes.insert(c);
        }
        gone
    }

    /// How many commands left the forget queue unforgotten, their history
    /// left to a sweep.
    pub const fn forgets_skipped(&self) -> u64 {
        self.forgets_skipped
    }

    /// The last command the forget queue let go unforgotten, if no sweep
    /// has dropped it yet: a sweep is due once the table forgets it.
    pub const fn owed_sweep(&self) -> Option<&CommandId> {
        self.owed_sweep.as_ref()
    }

    /// Whether `command` left the forget queue unforgotten, so that an
    /// entry of it left in a map is the next sweep's to drop (debug
    /// checks).
    #[cfg(debug_assertions)]
    pub fn sweep_owes(&self, command: &CommandId) -> bool {
        self.sweep_owes.contains(command)
    }

    /// The commands waiting in the forget queue (debug checks).
    pub fn forget_queued(&self) -> BTreeSet<CommandId> {
        self.forget_queue.iter().copied().collect()
    }

    /// Drop the durable record of one forgotten command, as
    /// [`DurableLedger::retain`] would: and the retry key bound to it
    /// with no payload here, if any, for the owner to unbind.
    pub fn forget(&mut self, command: &CommandId) -> Option<RetryKey> {
        self.records.remove(command);
        self.sequences.remove(command);
        self.bare_bindings.remove(command)
    }

    /// `key` was bound to `command`, which has no payload here to find
    /// the binding by when its history is dropped.
    pub fn bound_bare(&mut self, command: CommandId, key: RetryKey) {
        self.bare_bindings.insert(command, key);
    }

    /// The retry key bound to `command` with no payload here, which no
    /// longer names it: the command was released, not executed.
    pub fn unbind_bare(&mut self, command: &CommandId) -> Option<RetryKey> {
        self.bare_bindings.remove(command)
    }

    /// The retry keys bound with no payload, for the owner's counts.
    pub fn bare_bindings(&self) -> usize {
        self.bare_bindings.len()
    }

    /// Whether a history sweep is due, at `threshold` records
    /// (task-d110): once the ledger holds more than the threshold, and the
    /// records added and the commands executed since the last sweep come
    /// to more than half of what it kept.
    ///
    /// A sweep forgets only what the command table forgets, executed
    /// history. Records the table keeps, such as committed commands a
    /// voter behind has not executed, stay through every sweep. Due at
    /// the threshold alone, the sweep then ran on every execution once
    /// those passed it, each a walk over all of them that removed
    /// nothing. Now:
    /// - A sweep that kept at most two thirds of the threshold is due at
    ///   the threshold, as before: the ledger is past it only by more
    ///   than half of what was kept.
    /// - One that kept more is due once half of what it kept has been
    ///   added or executed since, so its walk is paid for by those, and
    ///   the ledger holds at most half again what the last sweep kept.
    /// - Executions count, so a backlog that drains is reclaimed as it
    ///   becomes forgettable, without waiting for the ledger to grow.
    pub fn sweep_due(&mut self, threshold: usize) -> bool {
        self.swept_at = self.swept_at.min(self.records.len());
        let len = self.records.len();
        len > threshold
            && (len - self.swept_at).saturating_add(self.executed_since) > self.swept_at / 2
    }

    /// A history sweep has just run: the next is measured from what it
    /// left.
    pub fn swept(&mut self) {
        self.swept_at = self.records.len();
        self.executed_since = 0;
        self.sweeps += 1;
    }

    /// What the last history sweep left, lowered as records left
    /// otherwise (task-d110): what [`DurableLedger::sweep_due`] measures
    /// the growth and the executions since against.
    pub fn swept_at(&self) -> usize {
        self.swept_at.min(self.records.len())
    }

    /// History sweeps run since this ledger was restored (task-d110).
    pub const fn sweeps(&self) -> u64 {
        self.sweeps
    }

    /// Stage the deletion of `command`'s record under `barrier`: it leaves
    /// the ledger when that batch is durable, and stays if it fails
    /// (task-d24).
    pub fn stage_removal(&mut self, barrier: BarrierId, command: CommandId) {
        self.removals.entry(barrier).or_default().push(command);
    }

    /// Barriers still outstanding (the cut must wait for them).
    pub fn outstanding(&self) -> Vec<BarrierId> {
        let mut out: Vec<BarrierId> = self.staged.keys().copied().collect();
        for b in self.removals.keys() {
            if !out.contains(b) {
                out.push(*b);
            }
        }
        out
    }

    /// The report for `ballot` from durable state only.
    pub fn report(
        &self,
        replica: ReplicaId,
        ballot: Ballot,
        committed_ballot: Ballot,
    ) -> RecoveryReport {
        RecoveryReport {
            replica,
            ballot,
            committed_ballot,
            entries: self
                .records
                .iter()
                .filter(|(_, r)| r.payload.is_some())
                .map(|(c, r)| ReportEntry {
                    command: *c,
                    phase: r.phase,
                    deps: r.deps.clone(),
                    path: r.path,
                    paths: r.paths.clone(),
                    seqnum: r.synced_seq.unwrap_or(0),
                    keys: r.keys.clone(),
                    payload_present: true,
                    admission: r.payload,
                })
                .collect(),
        }
    }
}

/// One page of a report transfer.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ReportPage {
    /// Reporting replica.
    pub replica: ReplicaId,
    /// New ballot.
    pub ballot: Ballot,
    /// Synchronized ballot.
    pub committed_ballot: Ballot,
    /// Page number (0-based).
    pub page: u32,
    /// Total pages.
    pub total: u32,
    /// Entries of this page.
    pub entries: Vec<ReportEntry>,
    /// Digest of the whole report this page belongs to. Pages of one
    /// transfer all carry it, so a delayed page of an earlier report of
    /// the same ballot cannot be assembled together with pages of a later
    /// one into a report the replica never produced.
    pub snapshot: Digest32,
    /// Digest binding replica, ballots, page, total, snapshot and entries.
    pub digest: Digest32,
}

/// Digest of a complete report: what every one of its pages carries.
fn report_digest(report: &RecoveryReport) -> Digest32 {
    let body = postcard::to_allocvec(&(
        &report.replica,
        &report.ballot,
        &report.committed_ballot,
        &report.entries,
    ))
    .expect("bounded");
    HashDomain::JournalBatch.digest(&[b"recovery-report", &body])
}

fn page_digest(
    replica: &ReplicaId,
    ballot: &Ballot,
    committed: &Ballot,
    page: u32,
    total: u32,
    snapshot: &Digest32,
    entries: &[ReportEntry],
) -> Digest32 {
    let body = postcard::to_allocvec(&(replica, ballot, committed, page, total, snapshot, entries))
        .expect("bounded");
    HashDomain::JournalBatch.digest(&[b"recovery-page", &body])
}

/// Why a page was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PageError {
    /// The digest does not match the page contents.
    Corrupt,
    /// The total exceeds the bound, or the page number is beyond it.
    OutOfBounds,
    /// Pages of one transfer disagree on the total or the ballots.
    Inconsistent,
    /// Page for another ballot.
    WrongBallot,
    /// A verified page of a report announcing more pages than the bound
    /// (task-d24 review): its reporter is recorded as past the bound, and
    /// nothing of it is held.
    PastBound,
}

/// Split a report into verified pages.
pub fn paginate(report: &RecoveryReport, per_page: usize) -> Vec<ReportPage> {
    let per_page = per_page.clamp(1, MAX_PAGE_ENTRIES);
    let chunks: Vec<&[ReportEntry]> = if report.entries.is_empty() {
        alloc::vec![&report.entries[..]]
    } else {
        report.entries.chunks(per_page).collect()
    };
    let total = chunks.len() as u32;
    let snapshot = report_digest(report);
    chunks
        .into_iter()
        .enumerate()
        .map(|(i, entries)| ReportPage {
            replica: report.replica,
            ballot: report.ballot,
            committed_ballot: report.committed_ballot,
            page: i as u32,
            total,
            entries: entries.to_vec(),
            snapshot,
            digest: page_digest(
                &report.replica,
                &report.ballot,
                &report.committed_ballot,
                i as u32,
                total,
                &snapshot,
                entries,
            ),
        })
        .collect()
}

/// Per-replica page collector for one new ballot.
#[derive(Clone, Debug)]
pub struct ReportAssembler {
    ballot: Ballot,
    /// The most pages a report may announce (task-d28).
    max_pages: u32,
    pages: BTreeMap<ReplicaId, BTreeMap<u32, ReportPage>>,
    /// Per replica: page total, synchronized ballot and report digest.
    totals: BTreeMap<ReplicaId, (u32, Ballot, Digest32)>,
    /// Replicas whose report announced more than `max_pages` pages, with
    /// the total announced (task-d24 review).
    past_bound: BTreeMap<ReplicaId, u32>,
}

impl ReportAssembler {
    /// Collect pages for `ballot`.
    pub const fn new(ballot: Ballot) -> Self {
        ReportAssembler {
            ballot,
            max_pages: MAX_REPORT_PAGES,
            pages: BTreeMap::new(),
            totals: BTreeMap::new(),
            past_bound: BTreeMap::new(),
        }
    }

    /// Refuse a report announcing more than the pages `entries` entries
    /// take, and one page more (task-d28). The page more is slack, not
    /// what lets a report just past the bound assemble: at 2,000 entries
    /// and 256 a page, 8 pages already hold 2,048. What the bound admits
    /// is up to one full page past the pages `entries` take, 2,304
    /// entries at the default, and the campaign then refuses what is past
    /// `entries` by name (task-d20); nothing larger is held.
    pub fn bound_entries(&mut self, entries: usize) {
        let pages = entries.div_ceil(MAX_PAGE_ENTRIES).saturating_add(1);
        self.max_pages = u32::try_from(pages).map_or(MAX_REPORT_PAGES, |p| p.min(MAX_REPORT_PAGES));
    }

    /// The pages of `replica`'s report still missing: `None` when none has
    /// arrived, so its total is unknown; empty when it is complete.
    pub fn missing(&self, replica: &ReplicaId) -> Option<Vec<u32>> {
        let (total, _, _) = self.totals.get(replica)?;
        let held = self.pages.get(replica);
        Some(
            (0..*total)
                .filter(|p| held.is_none_or(|h| !h.contains_key(p)))
                .collect(),
        )
    }

    /// The replicas whose report announced more pages than the bound, and
    /// the total each announced.
    pub const fn past_bound(&self) -> &BTreeMap<ReplicaId, u32> {
        &self.past_bound
    }

    /// The most pages a report may announce.
    pub const fn max_pages(&self) -> u32 {
        self.max_pages
    }

    /// Pages held, across every replica.
    pub fn pages_held(&self) -> usize {
        self.pages.values().map(BTreeMap::len).sum()
    }

    /// Accept a page after verification. Duplicates are idempotent.
    pub fn accept(&mut self, page: ReportPage) -> Result<(), PageError> {
        if page.ballot != self.ballot {
            return Err(PageError::WrongBallot);
        }
        if page.total == 0 || page.page >= page.total || page.entries.len() > MAX_PAGE_ENTRIES {
            return Err(PageError::OutOfBounds);
        }
        let expected = page_digest(
            &page.replica,
            &page.ballot,
            &page.committed_ballot,
            page.page,
            page.total,
            &page.snapshot,
            &page.entries,
        );
        if page.total > self.max_pages {
            // A report past the bound however its pages come out: named,
            // so the campaign sets it aside or fails on it rather than
            // waiting for pages it will never hold. A page that does not
            // verify says nothing about its reporter.
            if expected != page.digest {
                return Err(PageError::OutOfBounds);
            }
            self.past_bound.insert(page.replica, page.total);
            return Err(PageError::PastBound);
        }
        if expected != page.digest {
            return Err(PageError::Corrupt);
        }
        // Every page of one replica's transfer must name the same report.
        match self.totals.get(&page.replica) {
            Some((total, committed, snapshot))
                if *total != page.total
                    || *committed != page.committed_ballot
                    || *snapshot != page.snapshot =>
            {
                return Err(PageError::Inconsistent);
            }
            Some(_) => {}
            None => {
                self.totals.insert(
                    page.replica,
                    (page.total, page.committed_ballot, page.snapshot),
                );
            }
        }
        // A duplicate is idempotent only if it is the same page; two
        // different pages with the same number are a conflict, never an
        // arrival-order decision.
        match self.pages.entry(page.replica).or_default().entry(page.page) {
            alloc::collections::btree_map::Entry::Occupied(held) => {
                if *held.get() != page {
                    return Err(PageError::Inconsistent);
                }
            }
            alloc::collections::btree_map::Entry::Vacant(slot) => {
                slot.insert(page);
            }
        }
        Ok(())
    }

    /// The replicas every page of whose report is held, not yet verified
    /// against the report digest: what [`ReportAssembler::complete`] can
    /// at most return, found without assembling anything (task-d26).
    pub fn all_pages_held(&self) -> impl Iterator<Item = &ReplicaId> {
        self.pages.iter().filter_map(|(replica, pages)| {
            self.totals
                .get(replica)
                .is_some_and(|(total, _, _)| pages.len() == *total as usize)
                .then_some(replica)
        })
    }

    /// The complete, verified reports; a replica with any page missing is
    /// absent and never counts. Each call assembles every complete report
    /// afresh, a copy of every entry held: a caller asks only once enough
    /// of them could be complete to act on (task-d26).
    pub fn complete(&self) -> Vec<RecoveryReport> {
        let mut out = Vec::new();
        for (replica, pages) in &self.pages {
            let Some((total, committed, snapshot)) = self.totals.get(replica) else {
                continue;
            };
            if pages.len() != *total as usize {
                continue;
            }
            let entries = (0..*total)
                .flat_map(|i| pages[&i].entries.iter().cloned())
                .collect();
            let report = RecoveryReport {
                replica: *replica,
                ballot: self.ballot,
                committed_ballot: *committed,
                entries,
            };
            // The assembled report is the one the pages claimed.
            if report_digest(&report) != *snapshot {
                continue;
            }
            out.push(report);
        }
        out
    }

    /// Replicas with at least one page but not all.
    pub fn incomplete(&self) -> Vec<ReplicaId> {
        self.pages
            .iter()
            .filter(|(r, pages)| {
                self.totals
                    .get(r)
                    .is_some_and(|(total, _, _)| pages.len() != *total as usize)
            })
            .map(|(r, _)| *r)
            .collect()
    }
}

/// How many entries each map keyed by command holds that forgetting
/// drops (task-d111's cut (d), #221 review): what a voter keeps of its
/// history beside the ledger's records. A leader holds no adoptions or
/// held waiters, a follower no proposals.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HistoryMaps {
    /// Payloads.
    pub payloads: usize,
    /// Payloads this replica serves.
    pub served_payloads: usize,
    /// Vote sets.
    pub votes: usize,
    /// Adoption order.
    pub adopted: usize,
    /// Retry-key bindings.
    pub bindings: usize,
    /// Bindings with no payload here (the ledger's own).
    pub bare_bindings: usize,
    /// Commands to answer on release.
    pub answer_on_release: usize,
    /// Commands whose proposals carry their payload (task-d102).
    pub relayed: usize,
    /// Held proposals' waits, by the command each waits on.
    pub held_waiters: usize,
    /// The leader's proposals.
    pub proposals: usize,
}

impl HistoryMaps {
    /// Every entry counted.
    pub const fn total(&self) -> usize {
        self.payloads
            + self.served_payloads
            + self.votes
            + self.adopted
            + self.bindings
            + self.bare_bindings
            + self.answer_on_release
            + self.relayed
            + self.held_waiters
            + self.proposals
    }

    /// The largest of the maps.
    pub fn largest(&self) -> usize {
        [
            self.payloads,
            self.served_payloads,
            self.votes,
            self.adopted,
            self.bindings,
            self.bare_bindings,
            self.answer_on_release,
            self.relayed,
            self.held_waiters,
            self.proposals,
        ]
        .into_iter()
        .max()
        .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Phase;

    fn rows(from: u64, to: u64) -> Vec<(CommandId, CommandRecord)> {
        (from..to)
            .map(|n| {
                let mut d = [0u8; 32];
                d[..8].copy_from_slice(&n.to_be_bytes());
                (
                    CommandId(Digest32(d)),
                    CommandRecord {
                        phase: Phase::Commit,
                        deps: Vec::new(),
                        keys: Vec::new(),
                        payload: None,
                        paths: Vec::new(),
                        synced_seq: None,
                        path: Digest32([3; 32]),
                    },
                )
            })
            .collect()
    }

    /// A command the bounded forget queue lets go before the table forgets
    /// it stays the sweep's to drop, past any sweep that runs before it is
    /// forgotten (#221 review): a sweep that keeps it doesn't clear it,
    /// and one that drops it does. The owed set is kept in debug builds
    /// only, for the voters' checks.
    #[cfg(debug_assertions)]
    #[test]
    fn a_command_the_queue_let_go_unforgotten_stays_skipped_until_a_sweep_drops_it() {
        let mut ledger = DurableLedger::restore(rows(0, 3));
        let ids: Vec<CommandId> = rows(0, 3).into_iter().map(|(c, _)| c).collect();
        for c in &ids {
            ledger.executed(*c);
        }
        // Nothing forgotten, a queue of two: the oldest leaves.
        assert!(ledger.take_forgotten(|_| false, 4, 2).is_empty());
        assert_eq!(ledger.forgets_skipped(), 1);
        assert!(ledger.sweep_owes(&ids[0]));
        assert_eq!(ledger.owed_sweep(), Some(&ids[0]));
        // A sweep before the table forgets it keeps its record.
        ledger.retain(|_| true);
        ledger.swept();
        assert!(ledger.sweep_owes(&ids[0]), "a sweep that kept it");
        assert_eq!(ledger.owed_sweep(), Some(&ids[0]), "and is still owed");
        // Forgotten now: in-order forgetting takes the queued ones only,
        // and the next sweep drops it.
        let gone = ledger.take_forgotten(|_| true, 4, 2);
        assert_eq!(gone, ids[1..].to_vec());
        for c in &gone {
            ledger.forget(c);
            assert!(!ledger.sweep_owes(c), "dropped in order");
        }
        ledger.retain(|c| *c != ids[0]);
        ledger.swept();
        assert!(!ledger.sweep_owes(&ids[0]), "the sweep that dropped it");
        assert_eq!(ledger.owed_sweep(), None, "owes nothing more");
        assert_eq!(ledger.forgets_skipped(), 1);
    }

    /// task-d110: a sweep is due past the threshold once the records added
    /// and the commands executed since the last come to more than half of
    /// what it kept: at the threshold after one that kept little, and
    /// after half again otherwise, growth or executions alike. Records
    /// leaving without a sweep lower the mark.
    #[test]
    fn a_sweep_is_due_once_growth_and_executions_pass_half_what_the_last_kept() {
        let mut ledger = DurableLedger::restore(rows(0, 40));
        assert!(!ledger.sweep_due(40), "40 is not past 40");
        ledger.records.extend(rows(40, 41));
        assert!(ledger.sweep_due(40));
        // A sweep that kept 20 of 41: due past the threshold, as before.
        ledger.retain(|c| c.0.0[7] >= 21);
        ledger.swept();
        assert!(!ledger.sweep_due(40));
        ledger.records.extend(rows(41, 61));
        assert!(!ledger.sweep_due(40), "40 records");
        ledger.records.extend(rows(61, 62));
        assert!(ledger.sweep_due(40));
        // A sweep that forgot nothing of 100 (a voter behind): not due
        // again until half of 100 is added or executed.
        ledger.records.extend(rows(62, 121));
        assert_eq!(ledger.len(), 100);
        ledger.swept();
        assert!(!ledger.sweep_due(40));
        ledger.records.extend(rows(121, 151));
        for _ in 0..20 {
            ledger.executed(rows(0, 1)[0].0);
        }
        assert!(!ledger.sweep_due(40), "30 added and 20 executed");
        ledger.executed(rows(0, 1)[0].0);
        assert!(ledger.sweep_due(40), "30 added and 21 executed");
        // A backlog that drains: nothing added, executions alone.
        ledger.swept();
        for _ in 0..65 {
            ledger.executed(rows(0, 1)[0].0);
        }
        assert!(!ledger.sweep_due(40), "65 executed of 130 kept");
        ledger.executed(rows(0, 1)[0].0);
        assert!(ledger.sweep_due(40), "66 executed of 130 kept");
        // Records leaving without a sweep lower the mark: 39 left of the
        // 130 kept, under the threshold.
        ledger.swept();
        ledger.retain(|c| c.0.0[7] >= 112);
        assert_eq!(ledger.len(), 39);
        assert!(!ledger.sweep_due(40));
        ledger.records.extend(rows(151, 153));
        assert!(!ledger.sweep_due(40), "2 added since 39");
        ledger.records.extend(rows(153, 171));
        assert!(ledger.sweep_due(40), "20 added since 39");
    }

    fn number(c: &CommandId) -> u64 {
        u64::from_be_bytes(c.0.0[..8].try_into().unwrap())
    }

    /// task-d110 (#217 review, point 1): a ledger that forgets nothing,
    /// grown tenfold one record at a time. It is swept a few times, each
    /// sweep paid for by the growth since the last (at most three records
    /// looked at per record added), and it never holds more than half
    /// again what the last sweep kept, or the threshold, past the record
    /// that makes a sweep due.
    #[test]
    fn a_ledger_that_grows_is_swept_at_a_bounded_cost_per_record() {
        const THRESHOLD: usize = 32;
        for start in [500u64, 2_000] {
            let mut ledger = DurableLedger::restore(rows(0, start));
            ledger.swept();
            let (mut sweeps, mut looked) = (0u64, 0u64);
            for n in start..10 * start {
                ledger.records.extend(rows(n, n + 1));
                let bound = THRESHOLD.max(ledger.swept_at() + ledger.swept_at() / 2) + 1;
                assert!(
                    ledger.len() <= bound,
                    "{} held, bound {bound}",
                    ledger.len()
                );
                if ledger.sweep_due(THRESHOLD) {
                    sweeps += 1;
                    looked += ledger.len() as u64;
                    ledger.swept();
                }
            }
            let added = 9 * start;
            assert!((4..=6).contains(&sweeps), "{sweeps} sweeps from {start}");
            assert!(looked <= 3 * added, "{looked} looked at for {added} added");
        }
    }

    /// task-d110 (#217 review, point 2): a voter 2,000 behind at capacity
    /// 8 executes its backlog, with nothing new arriving. Each command
    /// becomes forgettable once eight later executions are past it, and a
    /// sweep takes what is forgettable. The executions alone bring the
    /// sweeps, each paid for by the executions since the last, and once
    /// the backlog has drained the ledger is back under the threshold.
    /// Counting growth alone, the ledger held all 2,000 for good.
    #[test]
    fn a_drained_backlog_is_reclaimed_by_its_executions() {
        const CAPACITY: u64 = 8;
        const THRESHOLD: usize = 4 * CAPACITY as usize;
        const BEHIND: u64 = 2_000;
        let mut ledger = DurableLedger::restore(rows(0, BEHIND));
        let (mut sweeps, mut looked) = (0u64, 0u64);
        for executed in 1..=BEHIND {
            ledger.executed(rows(0, 1)[0].0);
            if ledger.sweep_due(THRESHOLD) {
                sweeps += 1;
                looked += ledger.len() as u64;
                let forgotten = executed.saturating_sub(CAPACITY);
                ledger.retain(|c| number(c) >= forgotten);
                ledger.swept();
            }
        }
        assert!(
            ledger.len() <= THRESHOLD,
            "{} held once drained",
            ledger.len()
        );
        assert!((5..=8).contains(&sweeps), "{sweeps} sweeps");
        assert!(looked <= 3 * BEHIND, "{looked} looked at for {BEHIND}");
    }
}

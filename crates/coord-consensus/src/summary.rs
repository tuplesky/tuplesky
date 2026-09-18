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

use alloc::collections::BTreeMap;
use alloc::vec::Vec;

use coord_core::effect::BarrierId;
use coord_types::CommandId;
use coord_types::identity::{Digest32, HashDomain};
use coord_types::ids::{Ballot, LocalJournalSeq, ReplicaId};
use serde::{Deserialize, Serialize};

use crate::commands::CommandRecord;
use crate::recovery::{RecoveryReport, ReportEntry};

/// Maximum pages one report may announce.
pub const MAX_REPORT_PAGES: u32 = 4096;
/// Maximum entries per page.
pub const MAX_PAGE_ENTRIES: usize = 256;

/// Command records as they are durable, tracked by the actor itself.
#[derive(Clone, Debug, Default)]
pub struct DurableLedger {
    records: BTreeMap<CommandId, CommandRecord>,
    /// Journal sequence each durable record was written at.
    sequences: BTreeMap<CommandId, LocalJournalSeq>,
    staged: BTreeMap<BarrierId, (CommandId, CommandRecord)>,
}

impl DurableLedger {
    /// Empty ledger.
    pub const fn new() -> Self {
        DurableLedger {
            records: BTreeMap::new(),
            sequences: BTreeMap::new(),
            staged: BTreeMap::new(),
        }
    }

    /// A ledger seeded from durable rows (after a crash).
    pub fn restore(rows: impl IntoIterator<Item = (CommandId, CommandRecord)>) -> Self {
        DurableLedger {
            records: rows.into_iter().collect(),
            sequences: BTreeMap::new(),
            staged: BTreeMap::new(),
        }
    }

    /// A batch writing `record` for `command` was submitted under `barrier`.
    pub fn stage(&mut self, barrier: BarrierId, command: CommandId, record: CommandRecord) {
        self.staged.insert(barrier, (command, record));
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
        let (command, record) = self.staged.remove(&barrier)?;
        match self.sequences.get(&command) {
            // Strictly older completions are ignored; distinct batches
            // never share a journal sequence.
            Some(seen) if *seen > journal_seq => {}
            _ => {
                self.sequences.insert(command, journal_seq);
                self.records.insert(command, record);
            }
        }
        Some(command)
    }

    /// The batch under `barrier` failed: nothing became durable.
    pub fn failed(&mut self, barrier: BarrierId) -> Option<CommandId> {
        self.staged.remove(&barrier).map(|(c, _)| c)
    }

    /// Durable record of a command.
    pub fn record(&self, command: &CommandId) -> Option<&CommandRecord> {
        self.records.get(command)
    }

    /// Every durable record.
    pub fn records(&self) -> impl Iterator<Item = (&CommandId, &CommandRecord)> {
        self.records.iter()
    }

    /// Barriers still outstanding (the cut must wait for them).
    pub fn outstanding(&self) -> Vec<BarrierId> {
        self.staged.keys().copied().collect()
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
                    payload_present: true,
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
    pages: BTreeMap<ReplicaId, BTreeMap<u32, ReportPage>>,
    /// Per replica: page total, synchronized ballot and report digest.
    totals: BTreeMap<ReplicaId, (u32, Ballot, Digest32)>,
}

impl ReportAssembler {
    /// Collect pages for `ballot`.
    pub const fn new(ballot: Ballot) -> Self {
        ReportAssembler {
            ballot,
            pages: BTreeMap::new(),
            totals: BTreeMap::new(),
        }
    }

    /// Accept a page after verification. Duplicates are idempotent.
    pub fn accept(&mut self, page: ReportPage) -> Result<(), PageError> {
        if page.ballot != self.ballot {
            return Err(PageError::WrongBallot);
        }
        if page.total == 0 || page.total > MAX_REPORT_PAGES || page.page >= page.total {
            return Err(PageError::OutOfBounds);
        }
        if page.entries.len() > MAX_PAGE_ENTRIES {
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

    /// The complete, verified reports; a replica with any page missing is
    /// absent and never counts.
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

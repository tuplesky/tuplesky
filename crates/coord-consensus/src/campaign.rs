//! A candidate's recovery campaign (task-26; design Sections 4.8-4.9;
//! prototype `handleNewLeaderAckNs`, `MSync`).
//!
//! The candidate promises itself first, asks the other voters for
//! promises, and collects their reports page by page. Once a majority of
//! promising voters have complete reports, the source selection runs on
//! that set; the result is bound durably to the ballot before any Sync
//! message is produced, so a crash after binding republishes the same
//! result and never selects again under the same identity.

use alloc::collections::BTreeSet;
use alloc::vec::Vec;

use coord_core::effect::BarrierId;
use coord_types::CommandId;
use coord_types::identity::Digest32;
use coord_types::ids::{Ballot, ReplicaId};

use crate::phase::Phase;
use crate::quorum::BallotConfiguration;
use crate::recovery::{
    CandidateDecisions, RecoveryError, RecoveryReport, SyncDecision, select_ruled,
};
use crate::summary::{PageError, ReportAssembler, ReportPage};

/// The campaign for one ballot.
#[derive(Clone, Debug)]
pub struct Campaign {
    config: BallotConfiguration,
    assembler: ReportAssembler,
    promised: BTreeSet<ReplicaId>,
    own: Option<RecoveryReport>,
    decision: Option<SyncDecision>,
    bound: Option<BarrierId>,
    durable: bool,
    published: bool,
    /// What the selection waits for before it is bound: the commands
    /// [`crate::follower::Follower`]'s selection read named missing when
    /// the decision was checked, less those supplied since. `None` until
    /// the decision is checked, which is once per decision: the
    /// whole-selection checks and this read ran again on every payload
    /// frame, and a candidate behind by 836 commands spent 12 s of its
    /// domain thread in them before it led (#98, the takeover gap).
    awaited: Option<BTreeSet<CommandId>>,
    /// How many payload asks this campaign made to one voter: the next
    /// one goes to the next promised voter in turn.
    payload_asks: usize,
    /// The voter the last such ask went to.
    payload_source: Option<ReplicaId>,
    /// Voters that left an ask unanswered until it was asked again:
    /// skipped in turn while another promised voter is left.
    payload_sources_failed: BTreeSet<ReplicaId>,
    /// How many times the complete reports were assembled (task-d26).
    assemblies: u64,
    /// The complete reports last assembled from, by count, and whether a
    /// payload arrived since: a selection that waited is tried again only
    /// when one of the two moved (task-d33).
    assembled_from: Option<usize>,
    supply_moved: bool,
    /// How many times a payload or an execution arrived while the
    /// campaign had not selected (diagnostic, task-d33).
    supply_moves: u64,
    /// Configurations of earlier ballots the candidate ran under, the
    /// source ballot's among them if it is known (task-d31).
    sources: Vec<BallotConfiguration>,
    /// Commands the candidate executed and forgot whose durable
    /// dependency rows the selection last waited on (task-d77).
    rows_wanted: Vec<CommandId>,
    /// The rules its selection applies beyond task-29's (task-d90).
    rules: crate::recovery::SelectionRules,
}

impl Campaign {
    /// A campaign for the ballot of `config`.
    pub fn new(config: BallotConfiguration) -> Self {
        let ballot = config.ballot();
        Campaign {
            config,
            assembler: ReportAssembler::new(ballot),
            promised: BTreeSet::new(),
            own: None,
            decision: None,
            bound: None,
            durable: false,
            published: false,
            awaited: None,
            payload_asks: 0,
            payload_source: None,
            payload_sources_failed: BTreeSet::new(),
            assemblies: 0,
            assembled_from: None,
            supply_moved: false,
            supply_moves: 0,
            sources: Vec::new(),
            rows_wanted: Vec::new(),
            rules: crate::recovery::SelectionRules::default(),
        }
    }

    /// Select under `rules` ([`crate::recovery::select_ruled`]).
    #[must_use]
    pub const fn ruled(mut self, rules: crate::recovery::SelectionRules) -> Self {
        self.rules = rules;
        self
    }

    /// Select under `rules` from here on.
    pub const fn set_rules(&mut self, rules: crate::recovery::SelectionRules) {
        self.rules = rules;
    }

    /// Voters whose promise for this ballot arrived (the candidate
    /// included once it promised itself).
    pub const fn promised(&self) -> &BTreeSet<ReplicaId> {
        &self.promised
    }

    /// Whether this campaign asked for payloads yet.
    pub const fn asked_payloads(&self) -> bool {
        self.payload_asks > 0
    }

    /// The promised voter, `me` excluded, to ask for the next batch of
    /// payloads: one voter a batch, in turn (the takeover gap on #98).
    /// Asked of every promised voter, each payload came back once from
    /// each, and two of every three frames were duplicates. A voter that
    /// left an ask unanswered is passed over while another is left.
    pub fn next_payload_source(&mut self, me: ReplicaId) -> Option<ReplicaId> {
        let others = |skip: &BTreeSet<ReplicaId>| -> Vec<ReplicaId> {
            self.promised
                .iter()
                .filter(|v| **v != me && !skip.contains(v))
                .copied()
                .collect()
        };
        let mut sources = others(&self.payload_sources_failed);
        if sources.is_empty() {
            sources = others(&BTreeSet::new());
        }
        let source = *sources.get(self.payload_asks % sources.len().max(1))?;
        self.payload_asks = self.payload_asks.wrapping_add(1);
        self.payload_source = Some(source);
        Some(source)
    }

    /// The ask out was not answered in full and is asked again: the voter
    /// it went to is passed over from here on, while another promised
    /// voter is left.
    pub fn payload_source_failed(&mut self) {
        if let Some(v) = self.payload_source.take() {
            self.payload_sources_failed.insert(v);
        }
    }

    /// What the selection waits for, once its decision is checked.
    pub const fn awaited(&self) -> Option<&BTreeSet<CommandId>> {
        self.awaited.as_ref()
    }

    /// The decision is checked, and it waits for `missing`.
    pub fn await_payloads(&mut self, missing: BTreeSet<CommandId>) {
        self.awaited = Some(missing);
    }

    /// Drop from what the selection waits for `command`, if `still`
    /// says it is no longer missing, and then every awaited command from
    /// the front that is not: each supplied command leaves once, so the
    /// waits a candidate makes cost in all what it lacked, not that
    /// times the selection.
    pub fn settle_awaited(
        &mut self,
        command: Option<&CommandId>,
        still: impl Fn(&SyncDecision, &CommandId) -> bool,
    ) {
        let (Some(decision), Some(awaited)) = (self.decision.as_ref(), self.awaited.as_mut())
        else {
            return;
        };
        if let Some(c) = command
            && !still(decision, c)
        {
            awaited.remove(c);
        }
        while let Some(first) = awaited.first() {
            if still(decision, first) {
                break;
            }
            awaited.pop_first();
        }
    }

    /// A campaign resumed from a durably bound selection (after a crash):
    /// the decision is fixed and only needs publishing.
    pub fn resumed(config: BallotConfiguration, decision: SyncDecision) -> Self {
        let mut c = Campaign::new(config);
        c.decision = Some(decision);
        c.durable = true;
        c
    }

    /// A campaign that has selected `decision` and not bound it yet, for
    /// the tests that read what such a candidate lacks (audit item 5 on
    /// #98).
    #[cfg(test)]
    pub(crate) fn selected(config: BallotConfiguration, decision: SyncDecision) -> Self {
        let mut c = Campaign::new(config);
        c.decision = Some(decision);
        c
    }

    /// Ballot.
    pub const fn ballot(&self) -> Ballot {
        self.config.ballot()
    }

    /// The new ballot's configuration.
    pub const fn config(&self) -> &BallotConfiguration {
        &self.config
    }

    /// A voter promised.
    pub fn promise(&mut self, replica: ReplicaId) {
        if self.config.is_voter(&replica) {
            self.promised.insert(replica);
        }
    }

    /// The candidate's own report (its promise to itself).
    pub fn own_report(&mut self, report: RecoveryReport) {
        self.promised.insert(report.replica);
        self.own = Some(report);
    }

    /// The candidate's own report, once taken.
    pub const fn own(&self) -> Option<&RecoveryReport> {
        self.own.as_ref()
    }

    /// A report page from a voter.
    pub fn page(&mut self, page: ReportPage) -> Result<(), PageError> {
        self.assembler.accept(page)
    }

    /// The configuration an earlier ballot ran under (task-d31): if it is
    /// the source ballot of the selection, the possible-fast rule is
    /// applied to its fast set rather than the default one.
    #[must_use]
    pub fn knowing(mut self, source: BallotConfiguration) -> Self {
        if source.ballot() != self.config.ballot() {
            self.sources.push(source);
        }
        self
    }

    /// Hold no report larger than `report_limit` entries, and a page more
    /// (task-d28; [`ReportAssembler::bound_entries`]).
    #[must_use]
    pub fn bounded(mut self, report_limit: usize) -> Self {
        self.assembler.bound_entries(report_limit);
        self
    }

    /// The report pages to ask each promising voter for again (task-d28):
    /// those still missing from a report some page of which arrived, or
    /// an empty list -- the first pages -- for one of which none did. The
    /// candidate's own report is never asked for, and nothing is once
    /// the selection is made.
    pub fn missing_pages(&self) -> Vec<(ReplicaId, Vec<u32>)> {
        if self.decision.is_some() {
            return Vec::new();
        }
        let me = self.config.leader();
        self.promised
            .iter()
            .filter(|r| **r != me && !self.assembler.past_bound().contains_key(r))
            .filter_map(|r| match self.assembler.missing(r) {
                None => Some((*r, Vec::new())),
                Some(missing) if missing.is_empty() => None,
                Some(missing) => Some((
                    *r,
                    missing
                        .into_iter()
                        .take(crate::summary::MAX_PAGE_ASK)
                        .collect(),
                )),
            })
            .collect()
    }

    /// The voters to ask for their promise again (task-d82): every voter
    /// of the ballot whose promise has not arrived, the candidate
    /// excluded, once its own promise is durable (its report is taken
    /// then, as the first `NewLeader` waited for) and until the selection
    /// is made.
    ///
    /// A `NewLeader` is published once, and one sent on a connection its
    /// peer was closing is lost: a voter restarted under its incarnation
    /// is linked for a moment by each dial its peers then close, and a
    /// campaign it starts in that moment reached nobody. Nothing asked
    /// again, so the campaign held the voter until the election replaced
    /// it at its ceiling, 16 s later.
    pub fn unpromised(&self) -> Vec<ReplicaId> {
        if self.decision.is_some() || self.own.is_none() {
            return Vec::new();
        }
        let me = self.config.leader();
        self.config
            .voters()
            .iter()
            .filter(|r| **r != me && !self.promised.contains(r))
            .copied()
            .collect()
    }

    /// The commands whose durable dependency rows the selection last
    /// waited on (task-d77): ones the candidate executed and forgot that a
    /// voter behind still reports below COMMIT.
    pub fn rows_wanted(&self) -> &[CommandId] {
        &self.rows_wanted
    }

    /// A payload arrived that the selection may have been waiting on: the
    /// next [`Campaign::try_select`] assembles again (task-d33).
    pub fn supply_moved(&mut self) {
        self.supply_moved = true;
        if self.decision.is_none() {
            self.supply_moves += 1;
        }
    }

    /// How many times something the selection may wait on arrived before
    /// it was made: each may cost one more assembly (task-d33).
    pub const fn supply_moves(&self) -> u64 {
        self.supply_moves
    }

    /// How many times the complete reports were assembled for a selection
    /// (task-d26): once per arrival from a majority on, never per page
    /// before it.
    pub const fn assemblies(&self) -> u64 {
        self.assemblies
    }

    /// Report pages held (task-d28).
    pub fn pages_held(&self) -> usize {
        self.assembler.pages_held()
    }

    /// Complete reports from promising voters, own included.
    pub fn reports(&self) -> Vec<RecoveryReport> {
        let mut out: Vec<RecoveryReport> = self
            .assembler
            .complete()
            .into_iter()
            .filter(|r| self.promised.contains(&r.replica))
            .collect();
        if let Some(own) = &self.own {
            out.push(own.clone());
        }
        out
    }

    /// Run the selection once a majority of complete reports is present.
    /// `Ok(None)` means not yet; an error stops the campaign with the
    /// evidence. `supplied` names the commands this candidate holds a
    /// payload for, under the admission digest the reporters name when
    /// they name one, or executed ([`crate::recovery::select_with`]).
    ///
    /// A far-behind voter can report an acceptance, from a Sync it
    /// installed, of a command whose payload never reached it and which
    /// the others executed long ago. They leave such a command out of
    /// their reports (task-d05), and one that retired it may no longer
    /// hold its payload, so every selection that included the behind
    /// report failed as [`RecoveryError::HalfInitialized`] (task-d12).
    /// Two things let the campaign through:
    ///
    /// - A command the candidate itself holds or executed is supplied: it
    ///   is selected, and committed before binding if executed.
    /// - Otherwise the report is set aside while a majority remains
    ///   without it. Any majority of promises is a sound basis for the
    ///   selection -- it is the set the candidate would have had if that
    ///   report had arrived later -- so leaving one out needs no
    ///   judgement about the reporter. The candidate's own report is
    ///   never set aside: its own state is what it goes on to lead from.
    ///
    /// Both are safe because no acceptance is ever voted without its
    /// payload: a Sync entry whose payload is missing waits in
    /// `sync_pending` until it arrives, a proposal is adopted only with
    /// its payload, and no message acknowledges a Sync. An acceptance that
    /// no reporter has a payload for was therefore either never decided,
    /// or decided by voters that have since executed it and leave it out
    /// of their reports; by quorum intersection, leaving it out of the
    /// selection loses nothing.
    ///
    /// When neither applies, the campaign waits for the voters that have
    /// not reported, and fails only once every voter has.
    ///
    /// A report carrying more than `report_limit` entries is set aside the
    /// same way (task-d20), so the Sync is selected from reports of a
    /// bounded size; the candidate's own is never set aside, and the
    /// campaign fails on it as [`RecoveryError::ReportTooLarge`]. A report
    /// that announced more pages than the bound counts as one too large
    /// ([`RecoveryError::ReportPastPages`]).
    pub fn try_select(
        &mut self,
        report_limit: usize,
        supplied: impl Fn(&CommandId, Option<Digest32>) -> bool,
    ) -> Result<Option<&SyncDecision>, RecoveryError> {
        self.try_select_decided(report_limit, supplied, |_: &CommandId| false)
    }

    /// [`Campaign::try_select`], where `decided` names the commands this
    /// candidate committed or executed ([`crate::recovery::select_decided`],
    /// task-d71).
    pub fn try_select_decided(
        &mut self,
        report_limit: usize,
        supplied: impl Fn(&CommandId, Option<Digest32>) -> bool,
        decided: impl CandidateDecisions,
    ) -> Result<Option<&SyncDecision>, RecoveryError> {
        if self.decision.is_some() {
            return Ok(self.decision.as_ref());
        }
        // Assembling copies every entry of every complete report, and a
        // campaign is asked on every page and promise that arrives: until
        // a majority could be complete nothing is assembled (task-d26). A
        // voter whose report is past the page bound answered too, and
        // counts: with too few in bounds the campaign fails on it by name
        // (task-d24) rather than waiting for pages it will never hold.
        let could_be_complete = self
            .assembler
            .all_pages_held()
            .chain(self.assembler.past_bound().keys())
            .filter(|r| self.promised.contains(r))
            .count()
            + usize::from(self.own.is_some());
        if could_be_complete < self.config.slow_size() {
            return Ok(None);
        }
        // A campaign is asked again on every page, promise and payload
        // that arrives. Nothing it selects from moved unless another
        // report completed or a payload arrived: assembled again, the
        // same reports gave the same wait (task-d33).
        if self.assembled_from == Some(could_be_complete) && !self.supply_moved {
            return Ok(None);
        }
        self.assembled_from = Some(could_be_complete);
        self.supply_moved = false;
        self.assemblies += 1;
        let mut reports = self.reports();
        // A voter whose report announced more pages than the bound
        // answered too: its report is past the bound, as one too large is.
        let past_pages: Vec<RecoveryError> = self
            .assembler
            .past_bound()
            .iter()
            .filter(|(r, _)| self.promised.contains(r) && !reports.iter().any(|x| x.replica == **r))
            .map(|(r, pages)| RecoveryError::ReportPastPages {
                replica: *r,
                pages: *pages,
                limit: self.assembler.max_pages(),
            })
            .collect();
        let everyone = reports.len() + past_pages.len() >= self.config.voters().len();
        let oversize = |r: &RecoveryReport| RecoveryError::ReportTooLarge {
            replica: r.replica,
            entries: r.entries.len(),
            limit: report_limit,
        };
        if let Some(own) = reports
            .iter()
            .find(|r| r.replica == self.config.leader() && r.entries.len() > report_limit)
        {
            return Err(oversize(own));
        }
        let first_oversize = reports
            .iter()
            .find(|r| r.entries.len() > report_limit)
            .map(oversize)
            .or_else(|| past_pages.into_iter().next());
        reports.retain(|r| r.entries.len() <= report_limit);
        if reports.len() < self.config.slow_size() {
            return match first_oversize {
                Some(e) if everyone => Err(e),
                _ => Ok(None),
            };
        }
        let decision = loop {
            let source = |b: &Ballot| self.sources.iter().find(|c| c.ballot() == *b).cloned();
            match select_ruled(
                &self.config,
                &reports,
                &supplied,
                source,
                &decided,
                self.rules,
            ) {
                Ok(decision) => break decision,
                // Rows the runtime reads for the candidate: waited for, as
                // a payload is (task-d77).
                Err(RecoveryError::ExecutedRowsUnread { commands }) => {
                    self.rows_wanted = commands;
                    return Ok(None);
                }
                Err(RecoveryError::HalfInitialized { replica, command }) => {
                    if replica != self.config.leader() && reports.len() > self.config.slow_size() {
                        reports.retain(|r| r.replica != replica);
                        continue;
                    }
                    if everyone {
                        return Err(RecoveryError::HalfInitialized { replica, command });
                    }
                    return Ok(None);
                }
                Err(e) => return Err(e),
            }
        };
        self.rows_wanted.clear();
        self.decision = Some(decision);
        Ok(self.decision.as_ref())
    }

    /// Mark committed every selected entry this candidate executed.
    ///
    /// Executing a command means it was committed, and a commit is not
    /// written as a row: every report can say ACCEPT for a command the
    /// candidate has long executed. Re-proposing it is how the other
    /// voters would have learned the commit, but a candidate that retired
    /// what it executed has no record left to re-propose from, and a
    /// voter holding the command at ACCEPT would then wait for a vote
    /// that never comes, with everything after it waiting too. Selected
    /// as committed, it is committed wherever the selection is installed.
    ///
    /// `executed` answers with the dependencies the candidate executed
    /// the command under, or `Some(None)` for one retired since, whose
    /// dependencies it no longer holds. An entry whose dependencies
    /// disagree with the executed ones is left as selected.
    pub fn commit_executed(
        &mut self,
        executed: impl Fn(&CommandId) -> Option<Option<Vec<CommandId>>>,
    ) {
        let Some(decision) = self.decision.as_mut() else {
            return;
        };
        for (command, entry) in &mut decision.entries {
            if entry.phase >= Phase::Commit {
                continue;
            }
            match executed(command) {
                Some(None) => entry.phase = Phase::Commit,
                Some(Some(deps)) if deps == entry.deps => entry.phase = Phase::Commit,
                _ => {}
            }
        }
    }

    /// The selection, once made.
    pub const fn decision(&self) -> Option<&SyncDecision> {
        self.decision.as_ref()
    }

    /// The selection was submitted for durable binding under `barrier`.
    pub fn bound(&mut self, barrier: BarrierId) {
        self.bound = Some(barrier);
    }

    /// Barrier of the binding, if submitted.
    pub const fn binding(&self) -> Option<BarrierId> {
        self.bound
    }

    /// The binding became durable.
    pub fn on_durable(&mut self, barrier: BarrierId) -> bool {
        if self.bound == Some(barrier) {
            self.durable = true;
        }
        self.durable
    }

    /// Whether the selection is durably bound.
    pub const fn is_durable(&self) -> bool {
        self.durable
    }

    /// Whether the Sync was published.
    pub const fn is_published(&self) -> bool {
        self.published
    }

    /// Mark the Sync published.
    pub fn mark_published(&mut self) {
        self.published = true;
    }
}

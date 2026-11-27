//! A candidate's recovery campaign (task-26; design Sections 4.8-4.9;
//! prototype `handleNewLeaderAckNs`, `MSync`).
//!
//! The candidate promises itself first, asks the other voters for
//! promises, and collects their reports page by page. Once a majority of
//! promising voters have complete reports, the source selection runs on
//! that set; the result is bound durably to the ballot before any Sync
//! message is produced, so a crash after binding republishes the same
//! result and never selects again under the same identity.

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::vec::Vec;

use coord_core::effect::{BarrierId, PeerId};
use coord_types::CommandId;
use coord_types::identity::Digest32;
use coord_types::ids::{Ballot, ReplicaId, ReplicaIncarnation, StoreInstanceId};

use crate::phase::Phase;
use crate::quorum::BallotConfiguration;
use crate::recovery::{
    CandidateDecisions, CountedStore, RecoveryError, RecoveryReport, SelectionCounts, SyncDecision,
    select_counted,
};
use crate::summary::{PageError, ReportAssembler, ReportPage};

/// The campaign for one ballot.
#[derive(Clone, Debug)]
pub struct Campaign {
    config: BallotConfiguration,
    assembler: ReportAssembler,
    promised: BTreeSet<ReplicaId>,
    /// The store each promise came from (task-d121): a voter's report
    /// counts only from that store.
    promise_stores: BTreeMap<ReplicaId, StoreInstanceId>,
    /// The incarnation each of those promises came from, where the
    /// transport named it: what the Sync's counted stores are keyed by
    /// at the voters that install it (task-d121).
    promise_incarnations: BTreeMap<ReplicaId, ReplicaIncarnation>,
    /// The candidate's own store, counted with its own report.
    own_store: Option<CountedStore>,
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
    /// The voters that answered as the last assembly counted them: a
    /// report read, the candidate's own included, or one past the page
    /// bound (#335 review). [`Campaign::all_reported`] reads it, so that it
    /// counts what the selection does.
    answered: usize,
    /// The promised voters that could have answered when `answered` was
    /// last counted, by count: counted again only when one more could
    /// have (#335 review).
    answered_from: Option<usize>,
    /// The reports the count read, and how many could be complete when
    /// it read them: the next selection from as many takes them rather
    /// than read them again (#335 review).
    answered_reports: Option<(usize, Vec<RecoveryReport>)>,
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
    /// Possible-fast candidates its selections dropped behind a command
    /// no report holds and the candidate doesn't hold decided, every
    /// selection made counted, since last taken (task-d119).
    unreported_undecided: u64,
    /// Whether the selection it holds dropped a possible-fast candidate
    /// whose dependencies reach a command no report holds (task-d119).
    selected_past_unreported: bool,
    /// Of those, whether that command is one the candidate doesn't hold
    /// decided either.
    selected_past_undecided: bool,
    /// The dependency a selection was refused as `Behind` on while a
    /// voter's report was still unread (task-d120): the campaign waits
    /// for the reports it hasn't read and selects again as each one
    /// completes.
    report_wait: Option<CommandId>,
    /// Whether that wait has ended: the runtime's bound passed, and the
    /// next refusal gives the campaign up.
    report_wait_over: bool,
    /// Whether this campaign has waited so at all.
    waited: bool,
    /// Commands the selection it holds decided as nothing (task-d121).
    decided_nothing: u64,
    /// Selections condition 5 kept the rule off in, since last taken
    /// (task-d121).
    kept_by_drop: u64,
}

impl Campaign {
    /// A campaign for the ballot of `config`.
    pub fn new(config: BallotConfiguration) -> Self {
        let ballot = config.ballot();
        Campaign {
            config,
            assembler: ReportAssembler::new(ballot),
            promised: BTreeSet::new(),
            promise_stores: BTreeMap::new(),
            promise_incarnations: BTreeMap::new(),
            own_store: None,
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
            answered: 0,
            answered_from: None,
            answered_reports: None,
            supply_moved: false,
            supply_moves: 0,
            sources: Vec::new(),
            rows_wanted: Vec::new(),
            rules: crate::recovery::SelectionRules::default(),
            unreported_undecided: 0,
            selected_past_unreported: false,
            selected_past_undecided: false,
            report_wait: None,
            report_wait_over: false,
            waited: false,
            decided_nothing: 0,
            kept_by_drop: 0,
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

    /// A voter promised from the store `store`. A second promise of the
    /// same voter from another store changes nothing: the first one's
    /// store is the one its report must come from (task-d121).
    pub fn promise(&mut self, replica: ReplicaId, store: StoreInstanceId) {
        if self.config.is_voter(&replica) {
            if self
                .promise_stores
                .get(&replica)
                .is_some_and(|s| *s != store)
            {
                return;
            }
            self.promised.insert(replica);
            self.promise_stores.insert(replica, store);
        }
    }

    /// A voter promised as `peer` from the store `store`:
    /// [`Campaign::promise`], keeping the incarnation for the Sync's
    /// counted stores (task-d121).
    pub fn promise_from(&mut self, peer: PeerId, store: StoreInstanceId) {
        self.promise(peer.replica, store);
        if self.promise_stores.get(&peer.replica) == Some(&store) {
            self.promise_incarnations
                .entry(peer.replica)
                .or_insert(peer.incarnation);
        }
    }

    /// The candidate's own store and incarnation, counted with its own
    /// report in the Sync (task-d121).
    pub fn own_store(&mut self, incarnation: ReplicaIncarnation, store: StoreInstanceId) {
        self.own_store = Some(CountedStore { incarnation, store });
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
        // A page from another store than the voter's promise names
        // another report (task-d121).
        if self
            .promise_stores
            .get(&page.replica)
            .is_some_and(|s| *s != page.store)
        {
            return Err(PageError::Inconsistent);
        }
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

    /// Whether the selection it holds ends the asks for promises and
    /// report pages: one is made, and it is not one kept only while it
    /// waits task-d120's bound for a payload's holders (task-d121). That
    /// selection is made again when the wait ends, from every report read
    /// by then, and a voter's promise is what its payload asks go by: a
    /// lost promise or first page of the only voter left that holds the
    /// payload was asked for again only after the selection was dropped,
    /// too late to count (#335 review).
    const fn selection_settled(&self) -> bool {
        self.decision.is_some()
            && (self.report_wait.is_none() || self.bound.is_some() || self.durable)
    }

    /// The report pages to ask each promising voter for again (task-d28):
    /// those still missing from a report some page of which arrived, or
    /// an empty list -- the first pages -- for one of which none did. The
    /// candidate's own report is never asked for, and nothing is once
    /// the selection is made, but for one kept through a payload wait
    /// ([`Campaign::wait_for_payload_holders`]).
    pub fn missing_pages(&self) -> Vec<(ReplicaId, Vec<u32>)> {
        if self.selection_settled() {
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
    /// is made, but for one kept through a payload wait
    /// ([`Campaign::wait_for_payload_holders`]).
    ///
    /// A `NewLeader` is published once, and one sent on a connection its
    /// peer was closing is lost: a voter restarted under its incarnation
    /// is linked for a moment by each dial its peers then close, and a
    /// campaign it starts in that moment reached nobody. Nothing asked
    /// again, so the campaign held the voter until the election replaced
    /// it at its ceiling, 16 s later.
    pub fn unpromised(&self) -> Vec<ReplicaId> {
        if self.selection_settled() || self.own.is_none() {
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
    /// it was made, or the wait for the reports not read ended (task-d120,
    /// task-d121): each may cost one more assembly (task-d33).
    pub const fn supply_moves(&self) -> u64 {
        self.supply_moves
    }

    /// How many times the complete reports were assembled, for a selection
    /// or to count the voters that answered (task-d26): once per arrival
    /// from a majority on, never per page before it. A selection made
    /// after the count from the same reports takes the count's.
    pub const fn assemblies(&self) -> u64 {
        self.assemblies
    }

    /// Possible-fast candidates this campaign's selections dropped whose
    /// dependencies reach a command no report holds and the candidate
    /// doesn't hold decided, since the last call: every selection made
    /// counted, kept or not (task-d119).
    pub fn take_unreported_undecided(&mut self) -> u64 {
        core::mem::take(&mut self.unreported_undecided)
    }

    /// Whether the selection this campaign holds dropped a possible-fast
    /// candidate whose dependencies reach a command no report holds
    /// (task-d119).
    pub const fn selected_past_unreported(&self) -> bool {
        self.decision.is_some() && self.selected_past_unreported
    }

    /// Whether every voter of the ballot has answered: its report read,
    /// from the store its promise named and matching its digest, or past
    /// the page bound, the candidate's own included. No other report can
    /// arrive. Every page held is not enough (#335 review): a report from
    /// another store than its promise, or one whose digest fails, is never
    /// read, and the wait it would end is the one that may still bring the
    /// voter's report in. The selection counts the same voters.
    pub fn all_reported(&mut self) -> bool {
        self.count_answers();
        self.answered >= self.config.voters().len()
    }

    /// Wait for the reports not read yet, rather than give the campaign
    /// up as `Behind` on `missing` (task-d120). The selection is dropped:
    /// it was never bound, so selecting again costs only the payload asks
    /// it made. While it is dropped the campaign asks again for the
    /// promises and report pages that haven't arrived
    /// ([`Campaign::unpromised`], [`Campaign::missing_pages`]), and the
    /// next report that completes is selected from with every other.
    ///
    /// `false`, and nothing changes, once every voter has answered or the
    /// wait has ended: the campaign is then given up as before.
    pub fn wait_for_reports(&mut self, missing: CommandId) -> bool {
        if self.report_wait_over || self.bound.is_some() || self.durable || self.all_reported() {
            return false;
        }
        self.report_wait = Some(missing);
        self.waited = true;
        self.decision = None;
        self.awaited = None;
        true
    }

    /// Whether this campaign has waited for the other reports at all
    /// (task-d120).
    pub const fn waited(&self) -> bool {
        self.waited
    }

    /// The dependency the campaign waits on the other reports for, while
    /// it does (task-d120).
    pub const fn report_wait(&self) -> Option<CommandId> {
        self.report_wait
    }

    /// Whether the wait for the other reports has ended: the runtime's
    /// bound passed (task-d120). Where task-d121's rule is on, a selection
    /// decides a command as nothing only once it has (C5).
    pub const fn report_wait_passed(&self) -> bool {
        self.report_wait_over
    }

    /// End the wait: the runtime's bound passed. The selection is made
    /// again from the reports in hand, and a refusal now gives the
    /// campaign up. Whether it was waiting.
    ///
    /// A selection kept while it waited for payloads (task-d121) is
    /// dropped, unless it was bound: made again, under the rule, it may
    /// decide as nothing what nobody could send.
    ///
    /// With no wait running nothing changes (#335 review): a stray call
    /// would otherwise let the next selection decide a nothing with no
    /// wait behind it.
    pub fn end_report_wait(&mut self) -> bool {
        if self.report_wait.take().is_none() {
            return false;
        }
        let over = core::mem::replace(&mut self.report_wait_over, true);
        // The wait ends once, and the selection made after it may cost
        // one more assembly.
        if !over && self.bound.is_none() && !self.durable {
            self.supply_moves += 1;
        }
        self.supply_moved = true;
        if self.bound.is_none() && !self.durable {
            self.decision = None;
            self.awaited = None;
        }
        true
    }

    /// Wait task-d120's bound before a selection under task-d121's rule
    /// decides as nothing `missing`, a payload the selection waits for
    /// that may be one nobody can send, keeping the selection and its
    /// asks meanwhile: for the payloads, and for the promises and report
    /// pages not in ([`Campaign::unpromised`], [`Campaign::missing_pages`]),
    /// so that a voter whose promise or first page was lost is asked
    /// again, its payload asked of it once it promises, and its report
    /// read by the selection made after the wait. `false`, and nothing changes, where the rule is off,
    /// the wait has passed or already runs, or the selection is bound.
    pub fn wait_for_payload_holders(&mut self, missing: CommandId) -> bool {
        if !self.rules.ruled()
            || self.report_wait_over
            || self.report_wait.is_some()
            || self.bound.is_some()
            || self.durable
        {
            return false;
        }
        self.report_wait = Some(missing);
        self.waited = true;
        true
    }

    /// Where task-d121's rule is on, end task-d120's wait at once, with
    /// nothing left to read: every voter has answered, or the runtime
    /// doesn't wait. The selection is dropped and made again under the
    /// rule. `false`, and nothing changes, where the rule is off, the wait
    /// has passed already, or the selection is bound.
    pub fn pass_report_wait(&mut self) -> bool {
        if !self.rules.ruled() || self.report_wait_over || self.bound.is_some() || self.durable {
            return false;
        }
        self.report_wait_over = true;
        self.report_wait = None;
        self.decision = None;
        self.awaited = None;
        self.supply_moved = true;
        // The wait ends once, and the selection made again costs one
        // more assembly.
        self.supply_moves += 1;
        true
    }

    /// The rules its selection applies.
    pub const fn rules(&self) -> crate::recovery::SelectionRules {
        self.rules
    }

    /// Commands the selection it holds decided as nothing (task-d121):
    /// zero without a selection.
    pub const fn decided_nothing(&self) -> u64 {
        if self.decision.is_some() {
            self.decided_nothing
        } else {
            0
        }
    }

    /// Selections that condition 5 kept task-d121's rule off in, counted
    /// since last taken.
    pub fn take_kept_by_drop(&mut self) -> u64 {
        core::mem::take(&mut self.kept_by_drop)
    }

    /// Whether the selection this campaign holds dropped a possible-fast
    /// candidate whose dependencies reach a command no report holds and
    /// the candidate doesn't hold decided (task-d119).
    pub const fn selected_past_undecided(&self) -> bool {
        self.decision.is_some() && self.selected_past_undecided
    }

    /// Report pages held (task-d28).
    pub fn pages_held(&self) -> usize {
        self.assembler.pages_held()
    }

    /// Count again the voters that answered, once one more could have:
    /// the reports are read again only then, as the assembly reads them.
    /// A selection held is not made again, and a report that completes
    /// meanwhile is counted here (#335 review).
    fn count_answers(&mut self) {
        let could_be_complete = self.could_be_complete();
        if self.answered_from != Some(could_be_complete) {
            self.answered_from = Some(could_be_complete);
            let reports = self.read_reports();
            self.answered = self.count_answered(&reports);
            self.answered_reports = Some((could_be_complete, reports));
        }
    }

    /// The complete reports ([`Campaign::reports`]), counted as one
    /// assembly: each copies and hashes every entry of every report.
    fn read_reports(&mut self) -> Vec<RecoveryReport> {
        self.assemblies += 1;
        self.reports()
    }

    /// The promised voters whose report is complete or past the page
    /// bound, the candidate's own included: those that could have
    /// answered, before the reports are read.
    fn could_be_complete(&self) -> usize {
        self.assembler
            .all_pages_held()
            .chain(self.assembler.past_bound().keys())
            .filter(|r| self.promised.contains(r))
            .count()
            + usize::from(self.own.is_some())
    }

    /// The voters that answered, given the reports read: those, and
    /// each promised voter whose report is past the page bound.
    fn count_answered(&self, reports: &[RecoveryReport]) -> usize {
        reports.len()
            + self
                .assembler
                .past_bound()
                .keys()
                .filter(|r| self.promised.contains(r) && !reports.iter().any(|x| x.replica == **r))
                .count()
    }

    /// Complete reports from promising voters, own included.
    pub fn reports(&self) -> Vec<RecoveryReport> {
        let mut out: Vec<RecoveryReport> = self
            .assembler
            .complete()
            .into_iter()
            .filter(|r| {
                // Counted only from the store the promise came from; the
                // page check above refuses a page that differs once the
                // promise is in, and this one a report whose pages came
                // first (task-d121).
                self.promised.contains(&r.replica)
                    && self.promise_stores.get(&r.replica)
                        == self.assembler.store_of(&r.replica).as_ref()
            })
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
        // Assembling copies every entry of every complete report, and a
        // campaign is asked on every page and promise that arrives: until
        // a majority could be complete nothing is assembled (task-d26). A
        // voter whose report is past the page bound answered too, and
        // counts: with too few in bounds the campaign fails on it by name
        // (task-d24) rather than waiting for pages it will never hold.
        let could_be_complete = self.could_be_complete();
        if self.decision.is_some() {
            return Ok(self.decision.as_ref());
        }
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
        // The count read these reports already where it read them from as
        // many: the last page of a payload wait is counted, then selected
        // from at once (#335 review).
        let mut reports = match self.answered_reports.take() {
            Some((from, reports)) if from == could_be_complete => reports,
            _ => self.read_reports(),
        };
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
        self.answered = reports.len() + past_pages.len();
        self.answered_from = Some(could_be_complete);
        debug_assert_eq!(self.answered, self.count_answered(&reports));
        let everyone = self.answered >= self.config.voters().len();
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
        // task-d121's rule decides a nothing only once task-d120's wait
        // has passed: its bound, or every voter answered. Until then the
        // campaign selects without it, sets a half-initialized report
        // aside where that alone gives a selection, and otherwise waits
        // the bound. After it, the rule applies to a selection over every
        // report read, none set aside; where that selection still fails,
        // the set-aside, without the rule, and task-d79's give-up are the
        // fallback.
        let ruled = self.rules.ruled();
        let mut fire = ruled && self.report_wait_over;
        // The reports before any was set aside, kept only where the rule
        // may yet apply to them.
        let mut every_report: Option<Vec<RecoveryReport>> = None;
        let mut set_aside = false;
        let mut decision = loop {
            let source = |b: &Ballot| self.sources.iter().find(|c| c.ballot() == *b).cloned();
            let counts = SelectionCounts::default();
            let selected = select_counted(
                &self.config,
                &reports,
                &supplied,
                source,
                &decided,
                self.rules,
                fire && !set_aside,
                &counts,
            );
            self.unreported_undecided += counts.unreported_undecided.get();
            self.kept_by_drop += counts.kept_by_drop.get();
            match selected {
                Ok(decision) => {
                    self.selected_past_unreported = counts.unreported.get() > 0;
                    self.selected_past_undecided = counts.unreported_undecided.get() > 0;
                    self.decided_nothing = counts.decided_nothing.get();
                    break decision;
                }
                // Rows the runtime reads for the candidate: waited for, as
                // a payload is (task-d77).
                Err(RecoveryError::ExecutedRowsUnread { commands }) => {
                    self.rows_wanted = commands;
                    return Ok(None);
                }
                Err(RecoveryError::HalfInitialized { replica, command }) => {
                    if replica != self.config.leader() && reports.len() > self.config.slow_size() {
                        if ruled && !fire && every_report.is_none() {
                            every_report = Some(reports.clone());
                        }
                        reports.retain(|r| r.replica != replica);
                        set_aside = true;
                        continue;
                    }
                    if ruled && !fire {
                        // Every voter answered: nothing is left to wait
                        // for, and the wait has passed.
                        if everyone {
                            self.report_wait_over = true;
                            fire = true;
                            set_aside = false;
                            if let Some(every) = every_report.take() {
                                reports = every;
                            }
                            continue;
                        }
                        self.report_wait = Some(command);
                        self.waited = true;
                        return Ok(None);
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
        // The stores whose reports it was selected from (task-d121).
        decision.counted = reports
            .iter()
            .filter_map(|r| {
                let counted = if r.replica == self.config.leader() {
                    self.own_store?
                } else {
                    CountedStore {
                        incarnation: *self.promise_incarnations.get(&r.replica)?,
                        store: *self.promise_stores.get(&r.replica)?,
                    }
                };
                Some((r.replica, counted))
            })
            .collect();
        self.decision = Some(decision);
        // A new selection: refused as `Behind` again, it waits again.
        self.report_wait = None;
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
        // Bound, it waits for no report or payload holder (task-d121).
        self.report_wait = None;
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

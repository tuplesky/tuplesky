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
use coord_types::ids::{Ballot, ReplicaId};

use crate::quorum::BallotConfiguration;
use crate::recovery::{RecoveryError, RecoveryReport, SyncDecision, select};
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
    payloads_requested: bool,
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
            payloads_requested: false,
        }
    }

    /// Voters whose promise for this ballot arrived (the candidate
    /// included once it promised itself).
    pub const fn promised(&self) -> &BTreeSet<ReplicaId> {
        &self.promised
    }

    /// Whether the payloads the selection needs were already requested.
    pub const fn payloads_requested(&self) -> bool {
        self.payloads_requested
    }

    /// Record that the missing payloads were requested from the reporting
    /// voters (asked once; the selection waits for them).
    pub const fn mark_payloads_requested(&mut self) {
        self.payloads_requested = true;
    }

    /// A campaign resumed from a durably bound selection (after a crash):
    /// the decision is fixed and only needs publishing.
    pub fn resumed(config: BallotConfiguration, decision: SyncDecision) -> Self {
        let mut c = Campaign::new(config);
        c.decision = Some(decision);
        c.durable = true;
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

    /// A report page from a voter.
    pub fn page(&mut self, page: ReportPage) -> Result<(), PageError> {
        self.assembler.accept(page)
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
    /// evidence.
    pub fn try_select(&mut self) -> Result<Option<&SyncDecision>, RecoveryError> {
        if self.decision.is_some() {
            return Ok(self.decision.as_ref());
        }
        let reports = self.reports();
        if reports.len() < self.config.slow_size() {
            return Ok(None);
        }
        let decision = select(&self.config, &reports)?;
        self.decision = Some(decision);
        Ok(self.decision.as_ref())
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

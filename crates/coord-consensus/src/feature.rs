//! Replicated feature activation (task-60; design Sections 13, 17.7).
//!
//! A format is a local decision: a binary either reads some bytes or it
//! does not, and [`coord_types::formats::Format`] says which. A feature
//! is not. A feature changes replicated behaviour or writes durable
//! state every voter must be able to interpret, so activating one while
//! a voter cannot take part would produce a cluster where some replicas
//! understand the state and others silently do not -- which is the one
//! failure mode no amount of later care recovers from.
//!
//! So the rule here is **unanimity of the configured voters**, not a
//! majority. A majority is the right rule for deciding something (a
//! floor, a handoff) because the minority can be caught up afterwards
//! from what the majority holds. It is the wrong rule for activating a
//! capability, because the minority is not behind: it *cannot* do the
//! thing, and no amount of catching up changes that. The same asymmetry
//! is why [`crate::floor`] certifies with a majority and why
//! `coord_checkpoint::trim` requires every voter: a decision about
//! state and a decision about capability are not the same shape.
//!
//! Activation is one-way. There is no deactivation and no downgrade:
//! once a cluster has written state under a feature, a binary that
//! cannot read that state refuses to serve rather than operating on
//! what it half understands. Rolling back before activation is running
//! the old binary; rolling back after it is a restore (task-59).

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::vec::Vec;

use coord_types::formats::Feature;
use coord_types::ids::{ConfigurationEpoch, ReplicaId};

use crate::quorum::EpochVoters;

/// One voter's report of what its binary can take part in.
///
/// A report is a fact about a build, not a vote: a voter that reports
/// less than it supports delays an activation, which is harmless, and
/// one that reports more activates something it cannot do, which is
/// why the report is derived from the registry rather than configured.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Support {
    /// The reporting voter.
    pub voter: ReplicaId,
    /// The configuration it reported under.
    pub epoch: ConfigurationEpoch,
    /// The features its binary supports.
    pub features: BTreeSet<Feature>,
}

/// Why a report was not recorded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SupportError {
    /// The report is for another configuration.
    EpochMismatch,
    /// The reporter is not a voter of this configuration.
    NotAVoter,
    /// A voter withdrew support for a feature it had already reported.
    ///
    /// Refused because the only honest way to stop supporting a feature
    /// is to stop being a voter: a cluster that let a report shrink
    /// could activate a feature and then find a voter claiming it never
    /// had it.
    Withdrawn {
        /// The feature it had reported.
        feature: Feature,
    },
}

/// What the voters of one configuration have reported.
#[derive(Clone, Debug)]
pub struct SupportLedger {
    epoch: ConfigurationEpoch,
    voters: EpochVoters,
    reported: BTreeMap<ReplicaId, BTreeSet<Feature>>,
}

impl SupportLedger {
    /// An empty ledger for `voters`.
    pub fn new(voters: EpochVoters) -> Self {
        SupportLedger {
            epoch: voters.epoch(),
            voters,
            reported: BTreeMap::new(),
        }
    }

    /// A ledger rebuilt from durable reports.
    pub fn recovered(voters: EpochVoters, reports: &[Support]) -> Self {
        let mut ledger = SupportLedger::new(voters);
        for report in reports {
            let _ = ledger.record(report);
        }
        ledger
    }

    /// The configuration this ledger is for.
    pub const fn epoch(&self) -> ConfigurationEpoch {
        self.epoch
    }

    /// What `voter` reported, if anything.
    pub fn reported(&self, voter: &ReplicaId) -> Option<&BTreeSet<Feature>> {
        self.reported.get(voter)
    }

    /// Record one report.
    pub fn record(&mut self, report: &Support) -> Result<(), SupportError> {
        if report.epoch != self.epoch {
            return Err(SupportError::EpochMismatch);
        }
        if !self.voters.is_voter(&report.voter) {
            return Err(SupportError::NotAVoter);
        }
        if let Some(held) = self.reported.get(&report.voter)
            && let Some(dropped) = held.difference(&report.features).next()
        {
            return Err(SupportError::Withdrawn { feature: *dropped });
        }
        self.reported.insert(report.voter, report.features.clone());
        Ok(())
    }

    /// The features every configured voter has reported.
    ///
    /// A voter that has not reported at all supports nothing as far as
    /// this is concerned: silence is never assent, because the silent
    /// voter is exactly the one that might be an old binary.
    pub fn unanimous(&self) -> BTreeSet<Feature> {
        let mut out: BTreeSet<Feature> = Feature::ALL.iter().copied().collect();
        for voter in self.voters.voters() {
            match self.reported.get(voter) {
                Some(features) => out.retain(|f| features.contains(f)),
                None => return BTreeSet::new(),
            }
        }
        out
    }

    /// Which voters have not reported `feature`, in identifier order.
    ///
    /// The operator-facing half: "why is this not active yet" has to
    /// name the nodes, because the answer is always that somebody has
    /// not been upgraded.
    pub fn missing(&self, feature: Feature) -> Vec<ReplicaId> {
        self.voters
            .voters()
            .iter()
            .copied()
            .filter(|v| {
                self.reported
                    .get(v)
                    .is_none_or(|features| !features.contains(&feature))
            })
            .collect()
    }
}

/// Why an activation was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ActivationError {
    /// The request is for another configuration.
    EpochMismatch,
    /// At least one configured voter has not reported support.
    NotUnanimous {
        /// The feature.
        feature: Feature,
        /// Voters that have not reported it.
        missing: Vec<ReplicaId>,
    },
    /// The feature is already active; activation is idempotent but the
    /// caller is told, because a second activation is usually a second
    /// operator.
    AlreadyActive,
}

/// The features a cluster has activated, and under which configuration.
///
/// Monotone: activating adds, and nothing removes. A configuration
/// handoff carries the set forward, because the state written under a
/// feature does not stop existing when the voters change.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct ActiveFeatures {
    active: BTreeSet<Feature>,
}

impl ActiveFeatures {
    /// Nothing active: a cluster at genesis.
    pub fn new() -> Self {
        ActiveFeatures::default()
    }

    /// Rebuilt from a durable record.
    pub fn recovered(active: BTreeSet<Feature>) -> Self {
        ActiveFeatures { active }
    }

    /// Whether `feature` is active.
    pub fn is_active(&self, feature: Feature) -> bool {
        self.active.contains(&feature)
    }

    /// The active set, in identifier order.
    pub const fn active(&self) -> &BTreeSet<Feature> {
        &self.active
    }

    /// Activate `feature`, given what the voters have reported.
    ///
    /// The ledger is the evidence and the only evidence: there is no
    /// argument by which a caller asserts that the voters are ready.
    pub fn activate(
        &mut self,
        feature: Feature,
        ledger: &SupportLedger,
        epoch: ConfigurationEpoch,
    ) -> Result<(), ActivationError> {
        if ledger.epoch() != epoch {
            return Err(ActivationError::EpochMismatch);
        }
        if self.active.contains(&feature) {
            return Err(ActivationError::AlreadyActive);
        }
        if !ledger.unanimous().contains(&feature) {
            return Err(ActivationError::NotUnanimous {
                feature,
                missing: ledger.missing(feature),
            });
        }
        self.active.insert(feature);
        Ok(())
    }

    /// Whether a binary supporting `supported` may serve a cluster with
    /// this active set.
    ///
    /// Every active feature must be one the binary supports. This is
    /// the rollback guard: an old binary started against a cluster that
    /// has moved on refuses rather than operating on state it cannot
    /// fully interpret, and it names what it is missing so the answer
    /// is "this node is too old for this cluster" rather than a
    /// puzzling failure later.
    pub fn admits(&self, supported: &BTreeSet<Feature>) -> Result<(), Vec<Feature>> {
        let missing: Vec<Feature> = self
            .active
            .iter()
            .copied()
            .filter(|f| !supported.contains(f))
            .collect();
        if missing.is_empty() {
            Ok(())
        } else {
            Err(missing)
        }
    }
}

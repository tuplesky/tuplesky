//! The durable side of replicated feature activation (task-60; design
//! Sections 13, 17.7).
//!
//! [`coord_consensus::feature`] holds the rule -- unanimity of the
//! configured voters, and no deactivation -- and this holds the rows.
//! Two of them, in this order:
//!
//! 1. [`FeatureSupportV1`], one per voter: what that node's *binary*
//!    can take part in. It is written from the frozen registry rather
//!    than from configuration, because a node that could be told it
//!    supported something could be told it supported something it does
//!    not.
//! 2. [`ActiveFeaturesV1`], the activation: the features every
//!    configured voter reported, made durable before any of them is
//!    used. It names its reporters for the same reason a floor names
//!    its signers -- the authority is the reports behind it, and an
//!    operator reading one has to see whose they were.
//!
//! The asymmetry with a floor is deliberate and worth stating at the
//! row level too: a floor is certified by a *majority* because the
//! minority can be caught up from what the majority holds, and a
//! feature needs *every* voter because the minority is not behind, it
//! is incapable. [`admit`] is the other end of the same rule: a binary
//! started against a store whose activation names something it does not
//! support refuses, and says which feature and which build.

use std::collections::BTreeSet;
use std::ops::Bound;

use coord_consensus::feature::{ActiveFeatures, Support, SupportLedger};
use coord_consensus::quorum::EpochVoters;
use coord_core::effect::StoreUpdate;
use coord_store_api::engine::{Direction, EngineError, OrderedRead, ScanRequest};
use coord_store_api::registry::Collection;
use coord_types::formats::{Feature, Supported};
use coord_types::ids::{ClusterId, ConfigurationEpoch, DomainId, ReplicaId};
use serde::{Deserialize, Serialize};

use crate::trim::{TrimError, TrimLimits, corrupt, decode_record, encode_record};

/// Record kind of a voter's durable support report in `checkpoint_v1`.
pub const SUPPORT_RECORD_KIND: u16 = 0x000a;
/// Record kind of the durable activation.
pub const ACTIVE_RECORD_KIND: u16 = 0x000b;
/// Key prefix of the support rows; the voter identity follows.
pub const SUPPORT_KEY_PREFIX: &[u8] = b"feature_support_v1/";
/// Key of the durable activation.
pub const ACTIVE_KEY: &[u8] = b"active_features_v1";

/// One voter's durable report of what its binary supports.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeatureSupportV1 {
    /// The reporting voter.
    pub voter: ReplicaId,
    /// Cluster.
    pub cluster: ClusterId,
    /// Domain.
    pub domain: DomainId,
    /// The configuration it reported under.
    pub configuration: ConfigurationEpoch,
    /// Frozen feature identifiers, in order and without duplicates.
    pub features: Vec<u16>,
}

impl FeatureSupportV1 {
    /// This build's own report.
    ///
    /// Derived from the registry, never from configuration: the whole
    /// value of a report is that it is a fact about the binary.
    pub fn of_this_build(
        voter: ReplicaId,
        cluster: ClusterId,
        domain: DomainId,
        configuration: ConfigurationEpoch,
    ) -> Self {
        FeatureSupportV1 {
            voter,
            cluster,
            domain,
            configuration,
            features: Supported::features().iter().map(|f| f.id()).collect(),
        }
    }

    /// The features, refusing an unknown identifier rather than
    /// ignoring it: a report this build cannot fully read is a report
    /// from a newer binary, and treating it as a smaller one would
    /// silently under-count support.
    pub fn features(&self) -> Result<BTreeSet<Feature>, EngineError> {
        let mut out = BTreeSet::new();
        for id in &self.features {
            let feature = Feature::from_id(*id).ok_or_else(|| corrupt("unknown feature"))?;
            if !out.insert(feature) {
                return Err(corrupt("duplicate feature in a support report"));
            }
        }
        Ok(out)
    }

    /// The consensus-level report.
    pub fn report(&self) -> Result<Support, EngineError> {
        Ok(Support {
            voter: self.voter,
            epoch: self.configuration,
            features: self.features()?,
        })
    }

    /// Encode as a `checkpoint_v1` row value.
    pub fn encode(&self) -> Result<Vec<u8>, EngineError> {
        encode_record(SUPPORT_RECORD_KIND, self, "feature support encode")
    }

    /// Decode a `checkpoint_v1` row value.
    pub fn decode(bytes: &[u8]) -> Result<Self, EngineError> {
        decode_record(SUPPORT_RECORD_KIND, bytes, "feature support record")
    }
}

/// The key of `voter`'s support row.
pub fn support_key(voter: &ReplicaId) -> Vec<u8> {
    let mut key = SUPPORT_KEY_PREFIX.to_vec();
    key.extend_from_slice(voter.as_bytes());
    key
}

/// The durable activation: what this cluster has turned on, and who
/// reported it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveFeaturesV1 {
    /// Cluster.
    pub cluster: ClusterId,
    /// Domain.
    pub domain: DomainId,
    /// The configuration under which it was activated.
    pub configuration: ConfigurationEpoch,
    /// Frozen feature identifiers, in order.
    pub features: Vec<u16>,
    /// The voters whose reports made it unanimous, in identifier order.
    pub reporters: Vec<ReplicaId>,
}

impl ActiveFeaturesV1 {
    /// The active features, refusing an unknown identifier.
    ///
    /// This is the rollback guard's input, so an unreadable identifier
    /// must never decode to a smaller set: a binary that quietly
    /// dropped the feature it did not recognize would conclude it may
    /// serve precisely when it may not.
    pub fn features(&self) -> Result<BTreeSet<Feature>, EngineError> {
        let mut out = BTreeSet::new();
        for id in &self.features {
            let feature = Feature::from_id(*id).ok_or_else(|| corrupt("unknown active feature"))?;
            if !out.insert(feature) {
                return Err(corrupt("duplicate active feature"));
            }
        }
        Ok(out)
    }

    /// The consensus-level active set.
    pub fn active(&self) -> Result<ActiveFeatures, EngineError> {
        Ok(ActiveFeatures::recovered(self.features()?))
    }

    /// Encode as a `checkpoint_v1` row value.
    pub fn encode(&self) -> Result<Vec<u8>, EngineError> {
        encode_record(ACTIVE_RECORD_KIND, self, "active features encode")
    }

    /// Decode a `checkpoint_v1` row value.
    pub fn decode(bytes: &[u8]) -> Result<Self, EngineError> {
        decode_record(ACTIVE_RECORD_KIND, bytes, "active features record")
    }
}

/// This node's own support row, if it has written one.
pub fn own_support<V: OrderedRead>(
    view: &V,
    voter: &ReplicaId,
) -> Result<Option<FeatureSupportV1>, EngineError> {
    match view.get(Collection::CheckpointV1.id(), &support_key(voter))? {
        None => Ok(None),
        Some(bytes) => {
            let record = FeatureSupportV1::decode(&bytes)?;
            if record.voter != *voter {
                return Err(corrupt("support row disagrees with its key"));
            }
            Ok(Some(record))
        }
    }
}

/// The update that records `offered`, refusing a withdrawal.
///
/// A report may grow -- that is an upgrade -- and may never shrink. The
/// only honest way to stop supporting a feature is to stop being a
/// voter: a cluster that let a report shrink could activate something
/// and then find a voter claiming it never had it.
pub fn record_support<V: OrderedRead>(
    view: &V,
    voters: &EpochVoters,
    offered: &FeatureSupportV1,
) -> Result<StoreUpdate, TrimError> {
    let mut ledger = SupportLedger::new(voters.clone());
    if let Some(held) = own_support(view, &offered.voter)? {
        // A row from an earlier configuration is what a voter retained
        // across a membership change left behind, and the report it is
        // making now replaces it: refusing the row as another
        // configuration's would leave the voter unable to report ever
        // again, and the activation waiting on it forever. What the row
        // reported still binds, though, so it is read into this
        // configuration's ledger and a withdrawal across the change is
        // refused exactly as one within it. A row from a *later*
        // configuration is one this node cannot explain, and stays an
        // error.
        if held.configuration > voters.epoch() {
            return Err(TrimError::Engine(corrupt(
                "a durable support row is from a later configuration",
            )));
        }
        let prior = Support {
            epoch: voters.epoch(),
            ..held.report()?
        };
        ledger
            .record(&prior)
            .map_err(|_| corrupt("a durable support row is not this configuration's"))?;
    }
    ledger
        .record(&offered.report()?)
        .map_err(|e| TrimError::Engine(corrupt(support_refusal(e))))?;
    Ok(StoreUpdate {
        collection: Collection::CheckpointV1.id(),
        key: support_key(&offered.voter),
        value: Some(offered.encode()?),
    })
}

const fn support_refusal(e: coord_consensus::feature::SupportError) -> &'static str {
    use coord_consensus::feature::SupportError as E;
    match e {
        E::EpochMismatch => "a support report for another configuration",
        E::NotAVoter => "a support report from a replica that is not a voter",
        E::Withdrawn { .. } => "a support report that withdrew a feature already reported",
    }
}

/// The support rows this node durably holds, in voter order.
pub fn read_support<V: OrderedRead>(
    view: &V,
    limits: &TrimLimits,
) -> Result<Vec<FeatureSupportV1>, TrimError> {
    limits.validate()?;
    let mut out: Vec<FeatureSupportV1> = Vec::new();
    let mut resume: Option<Vec<u8>> = None;
    loop {
        let page = view.scan_page(
            Collection::CheckpointV1.id(),
            &ScanRequest {
                lower: Bound::Included(SUPPORT_KEY_PREFIX.to_vec()),
                upper: Bound::Unbounded,
                direction: Direction::Forward,
                resume_after: resume.clone(),
                max_rows: limits.page_rows(),
                max_bytes: limits.page_bytes(),
            },
        )?;
        let mut past_prefix = false;
        for row in &page.rows {
            let Some(suffix) = row.key.strip_prefix(SUPPORT_KEY_PREFIX) else {
                past_prefix = true;
                break;
            };
            if out.len() as u32 >= limits.max_acknowledgements {
                return Err(TrimError::AcknowledgementBudget {
                    limit: limits.max_acknowledgements,
                });
            }
            let voter = ReplicaId::from_slice(suffix).map_err(|_| corrupt("support key"))?;
            let record = FeatureSupportV1::decode(&row.value)?;
            if record.voter != voter {
                return Err(TrimError::Engine(corrupt(
                    "support row disagrees with its key",
                )));
            }
            out.push(record);
        }
        match page.rows.last() {
            Some(last) if !past_prefix && !page.exhausted => resume = Some(last.key.clone()),
            _ => break,
        }
    }
    Ok(out)
}

/// The durable activation, or `None` where nothing has been activated.
/// A corrupt record is an error, never an absence.
pub fn published_activation<V: OrderedRead>(
    view: &V,
) -> Result<Option<ActiveFeaturesV1>, EngineError> {
    match view.get(Collection::CheckpointV1.id(), ACTIVE_KEY)? {
        None => Ok(None),
        Some(bytes) => Ok(Some(ActiveFeaturesV1::decode(&bytes)?)),
    }
}

/// Why an activation could not be written.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ActivateError {
    /// At least one configured voter has not reported the feature.
    NotUnanimous {
        /// The feature.
        feature: Feature,
        /// Voters that have not reported it.
        missing: Vec<ReplicaId>,
    },
    /// Already active. A second activation is a no-op, and the caller
    /// is told because a second one is usually a second operator.
    AlreadyActive {
        /// The feature.
        feature: Feature,
    },
    /// The store failed.
    Engine(EngineError),
}

impl core::fmt::Display for ActivateError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ActivateError::NotUnanimous { feature, missing } => write!(
                f,
                "{} is not supported by every voter yet: {} have not reported it",
                feature.name(),
                missing.len()
            ),
            ActivateError::AlreadyActive { feature } => {
                write!(f, "{} is already active", feature.name())
            }
            ActivateError::Engine(e) => write!(f, "{e}"),
        }
    }
}

impl core::error::Error for ActivateError {}

impl From<EngineError> for ActivateError {
    fn from(e: EngineError) -> Self {
        ActivateError::Engine(e)
    }
}

/// The update that activates `feature`, given the durable reports.
///
/// The reports are the evidence and the only evidence: there is no
/// argument by which a caller asserts the cluster is ready.
pub fn activate_feature<V: OrderedRead>(
    view: &V,
    voters: &EpochVoters,
    cluster: ClusterId,
    domain: DomainId,
    feature: Feature,
    limits: &TrimLimits,
) -> Result<StoreUpdate, ActivateError> {
    let rows = read_support(view, limits).map_err(|e| match e {
        TrimError::Engine(e) => ActivateError::Engine(e),
        other => ActivateError::Engine(corrupt(match other {
            TrimError::AcknowledgementBudget { .. } => "too many support rows",
            _ => "support rows could not be read",
        })),
    })?;
    let mut reports = Vec::with_capacity(rows.len());
    for row in &rows {
        // A row is evidence only for the cluster and domain it names.
        // The consensus-level report carries neither, so this is the
        // one place a row copied or replayed from another store can be
        // caught before it counts toward unanimity here -- and such a
        // row is corruption of this store, not a stale report to skip.
        if row.cluster != cluster || row.domain != domain {
            return Err(ActivateError::Engine(corrupt(
                "a support row of another cluster or domain",
            )));
        }
        reports.push(row.report()?);
    }
    let ledger = SupportLedger::recovered(voters.clone(), &reports);
    let mut active = match published_activation(view)? {
        Some(record) => record.active()?,
        None => ActiveFeatures::new(),
    };
    active
        .activate(feature, &ledger, voters.epoch())
        .map_err(|e| match e {
            coord_consensus::feature::ActivationError::NotUnanimous { feature, missing } => {
                ActivateError::NotUnanimous { feature, missing }
            }
            coord_consensus::feature::ActivationError::AlreadyActive => {
                ActivateError::AlreadyActive { feature }
            }
            coord_consensus::feature::ActivationError::EpochMismatch => {
                ActivateError::Engine(corrupt("a support ledger of another configuration"))
            }
        })?;
    let record = ActiveFeaturesV1 {
        cluster,
        domain,
        configuration: voters.epoch(),
        features: active.active().iter().map(|f| f.id()).collect(),
        reporters: voters.voters().iter().copied().collect(),
    };
    Ok(StoreUpdate {
        collection: Collection::CheckpointV1.id(),
        key: ACTIVE_KEY.to_vec(),
        value: Some(record.encode()?),
    })
}

/// Why this binary may not serve this store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TooOld {
    /// Active features this build does not support.
    pub missing: Vec<Feature>,
}

impl core::fmt::Display for TooOld {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "this build is too old for this cluster: ")?;
        for (i, feature) in self.missing.iter().enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            f.write_str(feature.name())?;
        }
        f.write_str(" is active and this build does not support it")
    }
}

impl core::error::Error for TooOld {}

/// Whether this build may serve the store behind `view`.
///
/// The rollback guard. A binary started against a cluster that has
/// activated something it cannot do refuses here, before admission,
/// and names the feature: the answer an operator needs is "this node is
/// too old for this cluster", not a puzzling failure three steps later.
/// A store with no activation admits every build, which is what lets
/// compatible binaries coexist before activation.
pub fn admit<V: OrderedRead>(view: &V) -> Result<BTreeSet<Feature>, AdmitError> {
    let Some(record) = published_activation(view)? else {
        return Ok(BTreeSet::new());
    };
    let active = record.features()?;
    let supported: BTreeSet<Feature> = Supported::features().into_iter().collect();
    ActiveFeatures::recovered(active.clone())
        .admits(&supported)
        .map_err(|missing| AdmitError::TooOld(TooOld { missing }))?;
    Ok(active)
}

/// Why a store was not admitted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AdmitError {
    /// This build does not support something the cluster has activated.
    TooOld(TooOld),
    /// The store failed, or its activation record is not readable.
    Engine(EngineError),
}

impl core::fmt::Display for AdmitError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            AdmitError::TooOld(e) => write!(f, "{e}"),
            AdmitError::Engine(e) => write!(f, "{e}"),
        }
    }
}

impl core::error::Error for AdmitError {}

impl From<EngineError> for AdmitError {
    fn from(e: EngineError) -> Self {
        AdmitError::Engine(e)
    }
}

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
            features: Supported::reported().map(|f| f.id()).collect(),
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

    /// The update that writes this row.
    pub fn update(&self) -> Result<StoreUpdate, EngineError> {
        Ok(StoreUpdate {
            collection: Collection::CheckpointV1.id(),
            key: support_key(&self.voter),
            value: Some(self.encode()?),
        })
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

    /// The update that writes this record.
    pub fn update(&self) -> Result<StoreUpdate, EngineError> {
        Ok(StoreUpdate {
            collection: Collection::CheckpointV1.id(),
            key: ACTIVE_KEY.to_vec(),
            value: Some(self.encode()?),
        })
    }

    /// The features it turns on: its ids without the prepare markers.
    pub fn turned_on(&self) -> Result<BTreeSet<Feature>, EngineError> {
        Ok(self
            .features()?
            .into_iter()
            .filter(|f| !f.is_marker())
            .collect())
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
    let held = own_support(view, &offered.voter)?;
    check_support(held.as_ref(), voters, offered)?;
    Ok(offered.update()?)
}

/// [`record_support`]'s rule over the row this store holds for the same
/// voter, `held`, as the caller knows it: a runtime whose last batch is
/// still in the journal knows a row its view does not show yet.
pub fn check_support(
    held: Option<&FeatureSupportV1>,
    voters: &EpochVoters,
    offered: &FeatureSupportV1,
) -> Result<(), TrimError> {
    let mut ledger = SupportLedger::new(voters.clone());
    if let Some(held) = held {
        if held.voter != offered.voter {
            return Err(TrimError::Engine(corrupt(
                "a support row checked against another voter's",
            )));
        }
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
    Ok(())
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
    /// A prepare marker, which is written by a prepare and never
    /// activated (task-d99).
    NotActivatable {
        /// The marker.
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
            ActivateError::NotActivatable { feature } => {
                write!(f, "{} is a prepare marker, not a feature", feature.name())
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
    let held = published_activation(view)?;
    let record = activation_over(&rows, held.as_ref(), voters, cluster, domain, feature)?;
    Ok(record.update()?)
}

/// [`activate_feature`]'s rule over the support rows and the activation
/// record the caller knows this store holds: the record that activates
/// `feature`, keeping every id `held` names.
pub fn activation_over(
    rows: &[FeatureSupportV1],
    held: Option<&ActiveFeaturesV1>,
    voters: &EpochVoters,
    cluster: ClusterId,
    domain: DomainId,
    feature: Feature,
) -> Result<ActiveFeaturesV1, ActivateError> {
    if feature.is_marker() {
        return Err(ActivateError::NotActivatable { feature });
    }
    let mut reports = Vec::with_capacity(rows.len());
    for row in rows {
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
    let mut active = match held {
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
    Ok(ActiveFeaturesV1 {
        cluster,
        domain,
        configuration: voters.epoch(),
        features: active.active().iter().map(|f| f.id()).collect(),
        reporters: voters.voters().iter().copied().collect(),
    })
}

/// Why this binary may not serve this store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TooOld {
    /// Active features this build does not support.
    pub missing: Vec<Feature>,
    /// Active feature ids this build does not know at all: a build older
    /// than the feature, which is what a real downgrade meets. Refused as
    /// too old, not as a corrupt record (#227 review).
    pub unknown: Vec<u16>,
}

impl core::fmt::Display for TooOld {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "this build is too old for this cluster: ")?;
        if !self.unknown.is_empty() {
            f.write_str("it does not know the active feature id ")?;
            for (i, id) in self.unknown.iter().enumerate() {
                if i > 0 {
                    f.write_str(", ")?;
                }
                write!(f, "{id:#06x}")?;
            }
            if self.missing.is_empty() {
                return Ok(());
            }
            f.write_str("; and ")?;
        }
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
    admit_record(&record)
}

/// [`admit`] over an encoded activation record, as an update to
/// [`ACTIVE_KEY`] carries it: what a start checks in the journal's records
/// the projection has not applied yet, before replaying any of them.
pub fn admit_encoded(bytes: &[u8]) -> Result<BTreeSet<Feature>, AdmitError> {
    admit_record(&ActiveFeaturesV1::decode(bytes)?)
}

/// Whether this build can do every feature `record` makes active, and
/// what it turns on.
pub fn admit_record(record: &ActiveFeaturesV1) -> Result<BTreeSet<Feature>, AdmitError> {
    let supported: BTreeSet<Feature> = Supported::features().into_iter().collect();
    if let Some(too_old) = unknown_ids(record, &supported) {
        return Err(AdmitError::TooOld(too_old));
    }
    let active = record.features()?;
    ActiveFeatures::recovered(active.clone())
        .admits(&supported)
        .map_err(|missing| {
            AdmitError::TooOld(TooOld {
                missing,
                unknown: Vec::new(),
            })
        })?;
    // A prepare marker is a requirement on this build, read above, and
    // turns nothing on: only a feature's own id does (task-d99).
    Ok(active.into_iter().filter(|f| !f.is_marker()).collect())
}

/// A record naming an id this build doesn't know: refused as too old,
/// with the ids it doesn't know and the known ones it doesn't support.
/// `None` when every id is known (a duplicate is still read as corrupt
/// by [`ActiveFeaturesV1::features`]).
fn unknown_ids(record: &ActiveFeaturesV1, supported: &BTreeSet<Feature>) -> Option<TooOld> {
    let unknown: Vec<u16> = record
        .features
        .iter()
        .copied()
        .filter(|id| Feature::from_id(*id).is_none())
        .collect();
    if unknown.is_empty() {
        return None;
    }
    let missing = record
        .features
        .iter()
        .filter_map(|id| Feature::from_id(*id))
        .filter(|f| !supported.contains(f))
        .collect();
    Some(TooOld { missing, unknown })
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

/// Why a prepare could not be written (task-d99).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PrepareError {
    /// The feature has no prepare marker, so nothing would bind this
    /// voter's report once durable, and it is not activated through the
    /// running path.
    NotActivatable {
        /// The feature.
        feature: Feature,
    },
    /// This build does not report the feature.
    Unsupported {
        /// The feature.
        feature: Feature,
    },
    /// The support row was refused (another configuration, not a voter,
    /// or a withdrawal), or the store failed.
    Refused(String),
}

impl core::fmt::Display for PrepareError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            PrepareError::NotActivatable { feature } => write!(
                f,
                "{} has no prepare marker and cannot be activated in a running domain",
                feature.name()
            ),
            PrepareError::Unsupported { feature } => {
                write!(f, "this build does not support {}", feature.name())
            }
            PrepareError::Refused(why) => f.write_str(why),
        }
    }
}

impl core::error::Error for PrepareError {}

/// What a voter's prepare of `feature` writes, in one batch (task-d99,
/// design Section 13): its durable support row, from `record_support`,
/// and `feature`'s prepare marker in its own activation record.
///
/// The marker is the rollback guard. Every build since `admit` reads the
/// activation record at start and refuses one naming an id it does not
/// know, so once this batch is durable a build from before the marker
/// cannot start on this store, and the report it holds cannot outlive the
/// binary that made it. A record with no activation yet is created with
/// the marker alone and no reporters: it activates nothing.
pub fn prepare_feature<V: OrderedRead>(
    view: &V,
    voters: &EpochVoters,
    offered: &FeatureSupportV1,
    feature: Feature,
) -> Result<Vec<StoreUpdate>, PrepareError> {
    let refused = |e: EngineError| PrepareError::Refused(format!("{e:?}"));
    let support = own_support(view, &offered.voter).map_err(refused)?;
    let held = published_activation(view).map_err(refused)?;
    let record = prepare_rows(support.as_ref(), held.as_ref(), voters, offered, feature)?;
    let mut updates = vec![offered.update().map_err(refused)?];
    if let Some(record) = record {
        updates.push(record.update().map_err(refused)?);
    }
    Ok(updates)
}

/// [`prepare_feature`]'s rule over the support row and the activation
/// record the caller knows this voter's store holds: `offered` is the row
/// to write, and the record is returned where the marker is not in it yet.
pub fn prepare_rows(
    support: Option<&FeatureSupportV1>,
    held: Option<&ActiveFeaturesV1>,
    voters: &EpochVoters,
    offered: &FeatureSupportV1,
    feature: Feature,
) -> Result<Option<ActiveFeaturesV1>, PrepareError> {
    let Some(marker) = feature.prepare_marker() else {
        return Err(PrepareError::NotActivatable { feature });
    };
    if !offered.features.contains(&feature.id()) {
        return Err(PrepareError::Unsupported { feature });
    }
    check_support(support, voters, offered).map_err(|e| PrepareError::Refused(format!("{e:?}")))?;
    let mut record = held.cloned().unwrap_or(ActiveFeaturesV1 {
        cluster: offered.cluster,
        domain: offered.domain,
        configuration: voters.epoch(),
        features: Vec::new(),
        reporters: Vec::new(),
    });
    if record.cluster != offered.cluster || record.domain != offered.domain {
        return Err(PrepareError::Refused(
            "the activation record is another cluster's or domain's".into(),
        ));
    }
    if record.features.contains(&marker.id()) {
        return Ok(None);
    }
    record.features.push(marker.id());
    record.features.sort_unstable();
    Ok(Some(record))
}

/// Whether this voter's store holds a durable prepare of `feature`: its
/// own support row names it, and its activation record names the marker.
pub fn prepared<V: OrderedRead>(
    view: &V,
    me: &ReplicaId,
    feature: Feature,
) -> Result<bool, EngineError> {
    let Some(marker) = feature.prepare_marker() else {
        return Ok(false);
    };
    let supported = own_support(view, me)?.is_some_and(|row| row.features.contains(&feature.id()));
    let marked = published_activation(view)?.is_some_and(|r| r.features.contains(&marker.id()));
    Ok(supported && marked)
}

/// Why an activation record was not installed (task-d99).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InstallActivationError {
    /// The record names another cluster or domain.
    OtherDomain,
    /// This voter's own durable support row does not name these features:
    /// an activation never lands in a store whose build did not promise it.
    NotPrepared {
        /// The features its row does not name.
        missing: Vec<Feature>,
    },
    /// This build does not support a feature the record names.
    TooOld(TooOld),
    /// The record would drop a feature this store already holds active:
    /// two records with different reporters, which no install merges.
    Diverged,
    /// The record is unreadable, or the store failed.
    Engine(EngineError),
}

impl core::fmt::Display for InstallActivationError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            InstallActivationError::OtherDomain => {
                f.write_str("the activation is another cluster's or domain's")
            }
            InstallActivationError::NotPrepared { missing } => {
                f.write_str("this voter has not prepared ")?;
                for (i, feature) in missing.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    f.write_str(feature.name())?;
                }
                Ok(())
            }
            InstallActivationError::TooOld(e) => write!(f, "{e}"),
            InstallActivationError::Diverged => {
                f.write_str("the activation would drop a feature this store holds active")
            }
            InstallActivationError::Engine(e) => write!(f, "{e}"),
        }
    }
}

impl core::error::Error for InstallActivationError {}

impl From<EngineError> for InstallActivationError {
    fn from(e: EngineError) -> Self {
        InstallActivationError::Engine(e)
    }
}

/// The update that installs `offered`, an activation another voter holds,
/// into this voter's store, or `None` where the store already holds it
/// (task-d99, design Section 13).
///
/// It is checked against this voter's own durable support row and nothing
/// else: the prepared reports `activate_feature` counted are in the store
/// of the leader that wrote it. The record is installed unchanged -- its
/// configuration and reporters are the domain's one record, whichever
/// voter holds it -- except that this store's own prepare markers are
/// kept, since dropping one would undo its rollback guard.
pub fn install_activation<V: OrderedRead>(
    view: &V,
    me: &ReplicaId,
    offered: &ActiveFeaturesV1,
) -> Result<Option<StoreUpdate>, InstallActivationError> {
    let held = published_activation(view)?;
    let own = own_support(view, me)?;
    match installed_record(held.as_ref(), own.as_ref(), offered)? {
        Some(record) => Ok(Some(record.update()?)),
        None => Ok(None),
    }
}

/// [`install_activation`]'s rule over the record and the support row the
/// caller knows this voter's store holds: the record to write, or `None`.
pub fn installed_record(
    held: Option<&ActiveFeaturesV1>,
    own: Option<&FeatureSupportV1>,
    offered: &ActiveFeaturesV1,
) -> Result<Option<ActiveFeaturesV1>, InstallActivationError> {
    if let Some(held) = held
        && (held.cluster != offered.cluster || held.domain != offered.domain)
    {
        return Err(InstallActivationError::OtherDomain);
    }
    let supported: BTreeSet<Feature> = Supported::features().into_iter().collect();
    if let Some(too_old) = unknown_ids(offered, &supported) {
        return Err(InstallActivationError::TooOld(too_old));
    }
    let features = offered.features()?;
    let unsupported: Vec<Feature> = features
        .iter()
        .copied()
        .filter(|f| !Supported::supports(*f))
        .collect();
    if !unsupported.is_empty() {
        return Err(InstallActivationError::TooOld(TooOld {
            missing: unsupported,
            unknown: Vec::new(),
        }));
    }
    let offered_active: BTreeSet<Feature> = features
        .iter()
        .copied()
        .filter(|f| !f.is_marker())
        .collect();
    if let Some(own) = own
        && (own.cluster != offered.cluster || own.domain != offered.domain)
    {
        return Err(InstallActivationError::OtherDomain);
    }
    let promised: BTreeSet<u16> = own
        .map(|row| row.features.iter().copied().collect())
        .unwrap_or_default();
    let missing: Vec<Feature> = offered_active
        .iter()
        .copied()
        .filter(|f| !promised.contains(&f.id()))
        .collect();
    if !missing.is_empty() {
        return Err(InstallActivationError::NotPrepared { missing });
    }
    let held_active = match held {
        Some(record) => record.turned_on()?,
        None => BTreeSet::new(),
    };
    if !held_active.is_subset(&offered_active) {
        return Err(InstallActivationError::Diverged);
    }
    if held_active == offered_active {
        return Ok(None);
    }
    let mut ids: BTreeSet<u16> = offered.features.iter().copied().collect();
    if let Some(held) = held {
        ids.extend(held.features.iter().copied());
    }
    Ok(Some(ActiveFeaturesV1 {
        features: ids.into_iter().collect(),
        ..offered.clone()
    }))
}

/// The features a store's activation record turns on: its ids without the
/// prepare markers. Empty where nothing is activated.
pub fn active_features<V: OrderedRead>(view: &V) -> Result<BTreeSet<Feature>, EngineError> {
    Ok(match published_activation(view)? {
        Some(record) => record.turned_on()?,
        None => BTreeSet::new(),
    })
}

/// A voter's own feature rows, which a generation replacement carries
/// (task-d99, design Section 13): its support row, and its activation
/// record with its prepare markers.
///
/// They live in `checkpoint_v1`, node-private and outside the common
/// hash, so no checkpoint image carries them and an install writes none.
/// A voter whose new generation lacked them would start with no
/// activation and no marker, and its next `admit` would admit any build.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FeatureRows {
    /// This voter's support row.
    pub support: Option<FeatureSupportV1>,
    /// Its activation record, markers included.
    pub activation: Option<ActiveFeaturesV1>,
}

impl FeatureRows {
    /// `me`'s rows in the store behind `view`.
    pub fn read<V: OrderedRead>(view: &V, me: &ReplicaId) -> Result<Self, EngineError> {
        Ok(FeatureRows {
            support: own_support(view, me)?,
            activation: published_activation(view)?,
        })
    }

    /// Whether there is nothing to carry.
    pub const fn is_empty(&self) -> bool {
        self.support.is_none() && self.activation.is_none()
    }

    /// The updates that write these rows into another generation.
    pub fn updates(&self) -> Result<Vec<StoreUpdate>, EngineError> {
        let mut out = Vec::new();
        if let Some(support) = &self.support {
            out.push(StoreUpdate {
                collection: Collection::CheckpointV1.id(),
                key: support_key(&support.voter),
                value: Some(support.encode()?),
            });
        }
        if let Some(activation) = &self.activation {
            out.push(StoreUpdate {
                collection: Collection::CheckpointV1.id(),
                key: ACTIVE_KEY.to_vec(),
                value: Some(activation.encode()?),
            });
        }
        Ok(out)
    }

    /// Whether the store behind `view` holds at least these rows: the same
    /// voter's support row naming every feature this one does, and an
    /// activation record naming every id this one does. Less is a store a
    /// build could be admitted to that this voter's rows refuse.
    pub fn held_by<V: OrderedRead>(&self, view: &V) -> Result<bool, EngineError> {
        if let Some(support) = &self.support {
            let Some(theirs) = own_support(view, &support.voter)? else {
                return Ok(false);
            };
            if !support.features.iter().all(|f| theirs.features.contains(f)) {
                return Ok(false);
            }
        }
        if let Some(activation) = &self.activation {
            let Some(theirs) = published_activation(view)? else {
                return Ok(false);
            };
            if !activation
                .features
                .iter()
                .all(|f| theirs.features.contains(f))
            {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

/// Whether a learner may be promoted to voter, given the features the
/// domain holds active and what the learner currently advertises (task-d99,
/// design Section 13): every active feature, or the ones it lacks.
///
/// Images carry no `checkpoint_v1`, so a joining voter holds no feature
/// rows and its `admit` accepts any build. The activation's claim that
/// every configured voter supports the feature would stop holding at the
/// join, so promotion waits for a current advertisement of each, and the
/// leader prepares and activates for the joiner alone before it completes.
pub fn promotion_missing(
    active: &BTreeSet<Feature>,
    advertised: &BTreeSet<Feature>,
) -> Vec<Feature> {
    active
        .iter()
        .copied()
        .filter(|f| !f.is_marker() && !advertised.contains(f))
        .collect()
}

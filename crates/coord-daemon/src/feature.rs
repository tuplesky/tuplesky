//! Activating a replicated feature in a running domain (task-d99; design
//! Section 13).
//!
//! The libraries hold the rule and the rows ([`coord_consensus::feature`],
//! [`coord_checkpoint::feature`]); this is the runtime's part: what each
//! voter tells the others, when its rows are written, and what the leader
//! does on an operator's request.
//!
//! **Advertised, not committed.** Each voter sends
//! [`ProtocolMessage::FeatureSupport`] at start and on every new peer
//! link. A receiver keeps the latest from each peer in memory, forgets it
//! on that peer's next link, and it binds nothing. No voter may skip a
//! committed command it cannot decode, and an older build drops a peer
//! message it cannot, so nothing here reaches the order.
//!
//! **Two phases at the leader.** [`Features::activate`] refuses unless
//! every configured voter advertised the feature under this configuration.
//! It then asks each to prepare: write its support row and the feature's
//! prepare marker in one batch, and answer once that is durable. The
//! leader records each answer in its own store. Once every voter's
//! prepared report is durable there, it writes the activation over them,
//! and once that is durable it sends the record. A voter installs it only
//! over its own durable support row naming every feature in it.
//!
//! **The window.** A voter whose activation row is durable says
//! [`ProtocolMessage::FeatureHeld`] to its leader, with the record, again
//! at start, at a new ballot and in answer to an activation it already
//! holds. A leader keeps who said it per link: an entry goes when a new
//! link from that voter comes up, since a restart draws only new links,
//! and every entry goes at a new ballot. A gated behaviour acts toward a
//! voter only while it is in that set ([`Features::holders`]).
//!
//! **One switch per node.** [`Features::active`] is what this node's own
//! durable record turns on, and it changes the moment that record is
//! durable: a node does not wait for a restart to use what it holds.
//!
//! **A leader that inherits an activation.** A new leader that never heard
//! the old one's record installs it from a follower's `FeatureHeld`,
//! checked against its own durable support row, and then re-sends it as
//! the old leader would have: for each feature its own record turns on,
//! it asks every voter not yet holding it to prepare and sends it the
//! record, paced by [`Features::resend`]. The same re-send prepares and
//! activates a voter that joined after the activation.
//!
//! **Rows the view does not show yet.** A batch is durable in the journal
//! before its rows are in the store's view, so the rows this voter wrote
//! and the reports it recorded are kept here as they are written, as the
//! floor keeps its promises: every rule is applied to what the store will
//! hold, never to a view that may be behind it.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use coord_checkpoint::feature::{
    ActivateError, ActiveFeaturesV1, FeatureSupportV1, activation_over, check_support,
    installed_record, own_support, prepare_rows, published_activation, read_support,
};
use coord_checkpoint::trim::TrimLimits;
use coord_consensus::quorum::EpochVoters;
use coord_consensus::{FeatureRecord, ProtocolMessage};
use coord_core::effect::{BarrierId, BootId, StoreUpdate};
use coord_core::outbox::BarrierAllocator;
use coord_store_api::engine::OrderedRead;
use coord_types::formats::{Feature, Supported};
use coord_types::ids::{
    Ballot, ClusterId, ConfigurationEpoch, DomainId, ReplicaId, ReplicaIncarnation,
};

/// What a voter's feature driver did since boot (diagnostic).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FeatureCounts {
    /// Advertisements heard from peers.
    pub advertised: u64,
    /// Prepares this voter made durable.
    pub prepared: u64,
    /// Peers' prepared reports this leader recorded.
    pub recorded: u64,
    /// Activations this leader wrote.
    pub activated: u64,
    /// Activation records this voter installed from a peer.
    pub installed: u64,
    /// Peers that said they hold an activation.
    pub held: u64,
    /// Messages refused: another configuration, a build that cannot, or a
    /// record that does not fit this store.
    pub refused: u64,
    /// Feature batches that failed.
    pub failed: u64,
}

/// Who this node is in the ballot it is at.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Standing {
    /// The ballot it is at.
    pub ballot: Ballot,
    /// Whether it leads that ballot.
    pub leading: bool,
}

/// What a step asks the runtime to do: batches to journal, each under its
/// own barrier, and messages to send now, each to one peer.
#[derive(Debug, Default)]
pub struct FeatureStep {
    /// Batches to journal, in order.
    pub batches: Vec<(BarrierId, Vec<StoreUpdate>)>,
    /// Messages to send.
    pub sends: Vec<(ReplicaId, ProtocolMessage)>,
}

impl FeatureStep {
    /// Append what `other` asks for after this.
    pub fn absorb(&mut self, other: FeatureStep) {
        self.batches.extend(other.batches);
        self.sends.extend(other.sends);
    }

    /// Whether there is nothing to do.
    pub fn is_empty(&self) -> bool {
        self.batches.is_empty() && self.sends.is_empty()
    }
}

/// Why an activation was not started.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ActivateRefusal {
    /// This node does not lead; the request goes to the one that does.
    NotLeader {
        /// The leader of the ballot this node is at.
        leader: ReplicaId,
    },
    /// Not a feature a running domain activates: a prepare marker, or a
    /// feature without one.
    NotActivatable(Feature),
    /// This node's own build does not support it.
    Unsupported(Feature),
    /// Voters without a current advertisement of the feature.
    Missing {
        /// The feature.
        feature: Feature,
        /// The voters.
        voters: Vec<ReplicaId>,
    },
    /// Another activation is in progress.
    Busy(Feature),
}

impl core::fmt::Display for ActivateRefusal {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ActivateRefusal::NotLeader { leader } => {
                write!(f, "this node does not lead; the leader is {}", hex(leader))
            }
            ActivateRefusal::NotActivatable(feature) => {
                write!(f, "{} is not activated in a running domain", feature.name())
            }
            ActivateRefusal::Unsupported(feature) => {
                write!(f, "this node's build does not support {}", feature.name())
            }
            ActivateRefusal::Missing { feature, voters } => {
                write!(
                    f,
                    "{} has no current advertisement from voter(s)",
                    feature.name()
                )?;
                for voter in voters {
                    write!(f, " {}", hex(voter))?;
                }
                Ok(())
            }
            ActivateRefusal::Busy(feature) => {
                write!(f, "{} is being activated", feature.name())
            }
        }
    }
}

/// What an accepted request did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Activation {
    /// Prepares were sent; the leader activates once every voter's
    /// prepared report is durable in its store.
    Started,
    /// This leader already holds it active; the voters that do not hold it
    /// yet are sent it again.
    AlreadyActive,
}

/// One voter, as the leader sees it ([`Features::status`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VoterFeatures {
    /// The voter.
    pub voter: ReplicaId,
    /// What it advertised under this configuration, if it did since its
    /// last link.
    pub advertised: Option<BTreeSet<Feature>>,
    /// The features its durable report in this node's store names, under
    /// this configuration.
    pub prepared: BTreeSet<Feature>,
    /// The features it said it holds active since its last link, or, for
    /// this node, what its own durable record turns on.
    pub held: Option<BTreeSet<Feature>>,
}

/// The view `coordctl feature status` prints.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeatureStatus {
    /// The configuration.
    pub epoch: ConfigurationEpoch,
    /// Whether this node leads: only a leader's view is the domain's.
    pub leading: bool,
    /// The leader of the ballot this node is at.
    pub leader: Option<ReplicaId>,
    /// What this node's durable record turns on.
    pub active: BTreeSet<Feature>,
    /// The activation in progress, if any.
    pub activating: Option<Feature>,
    /// Every configured voter.
    pub voters: Vec<VoterFeatures>,
}

/// A support row as this node's store will hold it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Row {
    row: FeatureSupportV1,
    durable: bool,
}

/// What a journaled batch wrote, and what follows once it is durable.
#[derive(Debug)]
struct Batch {
    barrier: BarrierId,
    row: Option<FeatureSupportV1>,
    record: Option<ActiveFeaturesV1>,
    /// Send the record to every voter not holding it: this leader's own
    /// activation of a feature.
    announce: Option<Feature>,
}

/// A voter's part in activating features.
#[derive(Debug)]
pub struct Features {
    cluster: ClusterId,
    domain: DomainId,
    me: ReplicaId,
    voters: EpochVoters,
    /// Every support row this store holds or has journaled, this voter's
    /// own among them.
    rows: BTreeMap<ReplicaId, Row>,
    /// Each voter's support row as this store's durable batches hold it:
    /// what `rows` falls back to when a batch fails, since the view can
    /// lag a batch already durable in the journal.
    durable_rows: BTreeMap<ReplicaId, FeatureSupportV1>,
    /// This voter's activation record as its latest batch writes it.
    record: Option<ActiveFeaturesV1>,
    /// As its durable rows hold it: the switch, and what `FeatureHeld`
    /// carries.
    durable_record: Option<ActiveFeaturesV1>,
    /// Peers' advertisements since their last link.
    advertised: BTreeMap<ReplicaId, (ConfigurationEpoch, BTreeSet<Feature>)>,
    /// Who said it holds an activation, and what, since its last link and
    /// in this ballot. Only a leader keeps it.
    holders: BTreeMap<ReplicaId, BTreeSet<Feature>>,
    /// Leaders owed a `FeaturePrepared`, per feature, once this voter's
    /// prepare is durable.
    owed_prepared: BTreeSet<(ReplicaId, Feature)>,
    /// The leader is owed a `FeatureHeld` once the record is durable.
    owed_held: bool,
    /// The feature this leader is activating.
    activating: Option<Feature>,
    standing: Option<Standing>,
    unsettled: VecDeque<Batch>,
    /// The switch or the window may have moved since the node last read
    /// them ([`Features::take_changed`]).
    changed: bool,
    barriers: BarrierAllocator,
    /// Since boot.
    pub counts: FeatureCounts,
}

impl Features {
    /// A voter's driver, from what its store already holds.
    pub fn open<V: OrderedRead>(
        cluster: ClusterId,
        domain: DomainId,
        me: ReplicaId,
        voters: EpochVoters,
        view: &V,
        incarnation: ReplicaIncarnation,
        boot: BootId,
    ) -> Result<Self, String> {
        let mut features = Features {
            cluster,
            domain,
            me,
            voters,
            rows: BTreeMap::new(),
            durable_rows: BTreeMap::new(),
            record: None,
            durable_record: None,
            advertised: BTreeMap::new(),
            holders: BTreeMap::new(),
            owed_prepared: BTreeSet::new(),
            owed_held: false,
            activating: None,
            standing: None,
            unsettled: VecDeque::new(),
            changed: true,
            barriers: BarrierAllocator::new(incarnation, boot).for_features(),
            counts: FeatureCounts::default(),
        };
        features.reload(view)?;
        Ok(features)
    }

    /// Read the rows from the store, at open. Never again: a batch is
    /// durable in the journal before its rows are in the view, so after
    /// open this driver's own record of its durable batches is the truth
    /// and the view may be behind it.
    fn reload<V: OrderedRead>(&mut self, view: &V) -> Result<(), String> {
        let rows = read_support(view, &TrimLimits::default())
            .map_err(|e| format!("the support rows: {e:?}"))?;
        self.durable_rows = rows.iter().map(|row| (row.voter, row.clone())).collect();
        self.rows = rows
            .into_iter()
            .map(|row| (row.voter, Row { row, durable: true }))
            .collect();
        let record = published_activation(view).map_err(|e| format!("the activation: {e:?}"))?;
        if let Some(record) = &record
            && (record.cluster != self.cluster || record.domain != self.domain)
        {
            return Err("the activation record is another cluster's or domain's".into());
        }
        self.record = record.clone();
        self.durable_record = record;
        self.changed = true;
        Ok(())
    }

    /// After a failed batch: what the store will hold is what its durable
    /// batches hold, with the batches still in flight on top, in order.
    /// Kept here, not read back, since the view can lag a durable batch.
    fn roll_back(&mut self) {
        self.rows = self
            .durable_rows
            .iter()
            .map(|(voter, row)| {
                (
                    *voter,
                    Row {
                        row: row.clone(),
                        durable: true,
                    },
                )
            })
            .collect();
        self.record = self.durable_record.clone();
        for batch in &self.unsettled {
            if let Some(row) = &batch.row {
                self.rows.insert(
                    row.voter,
                    Row {
                        row: row.clone(),
                        durable: false,
                    },
                );
            }
            if let Some(record) = &batch.record {
                self.record = Some(record.clone());
            }
        }
    }

    /// Whether [`Features::active`] or [`Features::holders`] may have
    /// changed since this was last asked: the node re-reads them only
    /// then, since it asks on every pass of its loop.
    pub fn take_changed(&mut self) -> bool {
        core::mem::take(&mut self.changed)
    }

    /// The configured voters.
    pub const fn voters(&self) -> &EpochVoters {
        &self.voters
    }

    /// What this node's own durable record turns on: its switch.
    pub fn active(&self) -> BTreeSet<Feature> {
        self.durable_record
            .as_ref()
            .and_then(|r| r.turned_on().ok())
            .unwrap_or_default()
    }

    /// The voters that said, since their last link and in this ballot,
    /// that they hold `feature` active. A gated behaviour acts toward a
    /// voter only while it is here. Empty unless this node leads.
    pub fn holders(&self, feature: Feature) -> BTreeSet<ReplicaId> {
        if !self.standing.is_some_and(|s| s.leading) {
            return BTreeSet::new();
        }
        self.holders
            .iter()
            .filter(|(_, held)| held.contains(&feature))
            .map(|(voter, _)| *voter)
            .collect()
    }

    /// This build's advertisement.
    pub fn advertisement(&self) -> ProtocolMessage {
        ProtocolMessage::FeatureSupport {
            epoch: self.voters.epoch(),
            features: Supported::reported().map(|f| f.id()).collect(),
        }
    }

    fn peers(&self) -> impl Iterator<Item = ReplicaId> + '_ {
        self.voters
            .voters()
            .iter()
            .copied()
            .filter(move |v| *v != self.me)
    }

    fn held_message(&self) -> Option<ProtocolMessage> {
        let record = self.durable_record.as_ref()?;
        if record.turned_on().ok()?.is_empty() {
            return None;
        }
        Some(ProtocolMessage::FeatureHeld {
            record: wire(record),
        })
    }

    /// What this voter says at start: its advertisement to every peer, and
    /// what it holds to the leader of the ballot it is at.
    pub fn start(&mut self, standing: Standing) -> FeatureStep {
        let mut step = FeatureStep::default();
        for peer in self.peers().collect::<Vec<_>>() {
            step.sends.push((peer, self.advertisement()));
        }
        step.absorb(self.observe(standing));
        step
    }

    /// A new link from or to `peer` came up: what it said before is
    /// forgotten -- a restart draws only new links -- and it is told this
    /// voter's advertisement, and, if it leads, what this voter holds.
    pub fn linked(&mut self, peer: ReplicaId) -> FeatureStep {
        let mut step = FeatureStep::default();
        if peer == self.me || !self.voters.is_voter(&peer) {
            return step;
        }
        self.advertised.remove(&peer);
        if self.holders.remove(&peer).is_some() {
            self.changed = true;
        }
        step.sends.push((peer, self.advertisement()));
        if let Some(standing) = self.standing
            && !standing.leading
            && standing.ballot.leader == peer
            && let Some(held) = self.held_message()
        {
            step.sends.push((peer, held));
        }
        step
    }

    /// The ballot this node is at, after anything that may have moved it.
    /// A new ballot forgets every holder, and a follower tells its new
    /// leader what it holds.
    pub fn observe(&mut self, standing: Standing) -> FeatureStep {
        let mut step = FeatureStep::default();
        let moved = self.standing.is_none_or(|s| s.ballot != standing.ballot);
        let promoted = self
            .standing
            .is_some_and(|s| s.ballot == standing.ballot && !s.leading && standing.leading);
        if self.standing != Some(standing) {
            self.changed = true;
        }
        self.standing = Some(standing);
        if moved {
            self.holders.clear();
            self.activating = None;
        }
        if promoted {
            // The candidate of this ballot became its leader: what voters
            // said to it while it campaigned was said to this ballot's
            // leader, and stays.
            return step;
        }
        if moved
            && !standing.leading
            && standing.ballot.leader != self.me
            && let Some(held) = self.held_message()
        {
            step.sends.push((standing.ballot.leader, held));
        }
        step
    }

    /// A peer's message, over its own authenticated link.
    pub fn hear(&mut self, from: ReplicaId, message: &ProtocolMessage) -> FeatureStep {
        if from == self.me || !self.voters.is_voter(&from) {
            return FeatureStep::default();
        }
        match message {
            ProtocolMessage::FeatureSupport { epoch, features } => {
                let mut set = BTreeSet::new();
                for id in features {
                    // An id this build does not know is a feature it cannot
                    // take part in, so it counts for nothing here.
                    if let Some(f) = Feature::from_id(*id)
                        && !f.is_marker()
                    {
                        set.insert(f);
                    }
                }
                self.counts.advertised += 1;
                self.advertised.insert(from, (*epoch, set));
                // The leader's advertisement is also its ask: a voter
                // answers it with its own, which the leader may have
                // forgotten at a new link after it arrived. Only the
                // leader's is answered, so two voters never echo each
                // other.
                let mut step = FeatureStep::default();
                if let Some(standing) = self.standing
                    && !standing.leading
                    && standing.ballot.leader == from
                {
                    step.sends.push((from, self.advertisement()));
                }
                step
            }
            ProtocolMessage::FeaturePrepare { feature, epoch } => {
                self.on_prepare(from, *feature, *epoch)
            }
            ProtocolMessage::FeaturePrepared {
                feature,
                epoch,
                features,
            } => self.on_prepared(from, *feature, *epoch, features),
            ProtocolMessage::FeatureActivated { record, .. } => self.on_activated(from, record),
            ProtocolMessage::FeatureHeld { record } => self.on_held(from, record),
            _ => FeatureStep::default(),
        }
    }

    /// Whether this voter's prepare of `feature` under this configuration
    /// is durable.
    fn durably_prepared(&self, feature: Feature) -> bool {
        let Some(marker) = feature.prepare_marker() else {
            return false;
        };
        let row = self.rows.get(&self.me).is_some_and(|r| {
            r.durable
                && r.row.configuration == self.voters.epoch()
                && r.row.features.contains(&feature.id())
        });
        let marked = self
            .durable_record
            .as_ref()
            .is_some_and(|r| r.features.contains(&marker.id()));
        row && marked
    }

    fn prepared_message(&self, feature: Feature) -> Option<ProtocolMessage> {
        let row = &self.rows.get(&self.me)?.row;
        Some(ProtocolMessage::FeaturePrepared {
            feature: feature.id(),
            epoch: row.configuration,
            features: row.features.clone(),
        })
    }

    fn journal(
        &mut self,
        row: Option<FeatureSupportV1>,
        record: Option<ActiveFeaturesV1>,
        announce: Option<Feature>,
    ) -> Option<(BarrierId, Vec<StoreUpdate>)> {
        let mut updates = Vec::new();
        if let Some(row) = &row {
            updates.push(row.update().ok()?);
        }
        if let Some(record) = &record {
            updates.push(record.update().ok()?);
        }
        if updates.is_empty() {
            return None;
        }
        let barrier = self.barriers.allocate();
        if let Some(row) = &row {
            self.rows.insert(
                row.voter,
                Row {
                    row: row.clone(),
                    durable: false,
                },
            );
        }
        if let Some(record) = &record {
            self.record = Some(record.clone());
        }
        self.unsettled.push_back(Batch {
            barrier,
            row,
            record,
            announce,
        });
        Some((barrier, updates))
    }

    /// This voter's own prepare of `feature`, asked for by `leader`.
    fn on_prepare(&mut self, leader: ReplicaId, id: u16, epoch: ConfigurationEpoch) -> FeatureStep {
        let mut step = FeatureStep::default();
        let Some(feature) = Feature::from_id(id) else {
            self.counts.refused += 1;
            return step;
        };
        if epoch != self.voters.epoch()
            || feature.prepare_marker().is_none()
            || !Supported::reported().any(|f| f == feature)
        {
            self.counts.refused += 1;
            return step;
        }
        if self.durably_prepared(feature) {
            if leader != self.me
                && let Some(message) = self.prepared_message(feature)
            {
                step.sends.push((leader, message));
            }
            return step;
        }
        self.owed_prepared.insert((leader, feature));
        let offered = FeatureSupportV1::of_this_build(self.me, self.cluster, self.domain, epoch);
        let held_row = self.rows.get(&self.me).map(|r| r.row.clone());
        let record = match prepare_rows(
            held_row.as_ref(),
            self.record.as_ref(),
            &self.voters,
            &offered,
            feature,
        ) {
            Ok(record) => record,
            Err(_) => {
                self.counts.refused += 1;
                self.owed_prepared.remove(&(leader, feature));
                return step;
            }
        };
        let row = (held_row.as_ref() != Some(&offered)).then_some(offered);
        if let Some(batch) = self.journal(row, record, None) {
            step.batches.push(batch);
        }
        step
    }

    /// A voter's prepared report, recorded in this leader's store.
    fn on_prepared(
        &mut self,
        voter: ReplicaId,
        id: u16,
        epoch: ConfigurationEpoch,
        features: &[u16],
    ) -> FeatureStep {
        let mut step = FeatureStep::default();
        let leading = self.standing.is_some_and(|s| s.leading);
        let Some(feature) = Feature::from_id(id) else {
            self.counts.refused += 1;
            return step;
        };
        if !leading || epoch != self.voters.epoch() || !features.contains(&id) {
            self.counts.refused += 1;
            return step;
        }
        let row = FeatureSupportV1 {
            voter,
            cluster: self.cluster,
            domain: self.domain,
            configuration: epoch,
            features: features.to_vec(),
        };
        let held = self.rows.get(&voter).map(|r| r.row.clone());
        if held.as_ref() == Some(&row) {
            return step;
        }
        if check_support(held.as_ref(), &self.voters, &row).is_err() || row.features().is_err() {
            self.counts.refused += 1;
            return step;
        }
        let _ = feature;
        self.counts.recorded += 1;
        if let Some(batch) = self.journal(Some(row), None, None) {
            step.batches.push(batch);
        }
        step
    }

    /// The record another voter holds, installed here if this voter's own
    /// durable support row names every feature in it.
    fn install(&mut self, record: &FeatureRecord) -> Result<FeatureStep, ()> {
        let mut step = FeatureStep::default();
        let offered = ActiveFeaturesV1 {
            cluster: self.cluster,
            domain: self.domain,
            configuration: record.epoch,
            features: record.features.clone(),
            reporters: record.reporters.clone(),
        };
        // Only a durable row: an activation never lands in a store whose
        // own promise could still be lost.
        let own = self
            .rows
            .get(&self.me)
            .filter(|r| r.durable)
            .map(|r| r.row.clone());
        match installed_record(self.record.as_ref(), own.as_ref(), &offered) {
            Ok(Some(record)) => {
                self.counts.installed += 1;
                if let Some(batch) = self.journal(None, Some(record), None) {
                    step.batches.push(batch);
                }
                Ok(step)
            }
            Ok(None) => Ok(step),
            Err(_) => Err(()),
        }
    }

    /// The leader's activation record: installed, and the leader told once
    /// it is durable.
    fn on_activated(&mut self, from: ReplicaId, record: &FeatureRecord) -> FeatureStep {
        // A voter whose durable record already turns these on says so,
        // whatever the store's history: its record may predate its support
        // row, and the answer is what repairs the leader's held set.
        let covered = |held: &Option<ActiveFeaturesV1>| {
            held.as_ref().is_some_and(|held| {
                record.features.iter().all(|f| {
                    held.features.contains(f)
                        || Feature::from_id(*f).is_some_and(Feature::is_marker)
                })
            })
        };
        if covered(&self.durable_record) {
            let mut step = FeatureStep::default();
            if let Some(held) = self.held_message() {
                step.sends.push((from, held));
            }
            return step;
        }
        match self.install(record) {
            Ok(step) => {
                if !covered(&self.durable_record) {
                    self.owed_held = true;
                }
                step
            }
            Err(()) => {
                self.counts.refused += 1;
                FeatureStep::default()
            }
        }
    }

    /// A follower holds `record`: it may be sent gated behaviour, and a
    /// leader that did not hold the record yet installs it unchanged.
    fn on_held(&mut self, from: ReplicaId, record: &FeatureRecord) -> FeatureStep {
        if !self
            .standing
            .is_some_and(|s| s.leading || s.ballot.leader == self.me)
        {
            return FeatureStep::default();
        }
        let held: BTreeSet<Feature> = record
            .features
            .iter()
            .filter_map(|id| Feature::from_id(*id))
            .filter(|f| !f.is_marker())
            .collect();
        self.counts.held += 1;
        if self.holders.get(&from) != Some(&held) {
            self.changed = true;
        }
        self.holders.insert(from, held);
        // A leader that never heard the activation finishes it with the
        // record its followers hold, not one of its own: the reports it
        // rests on are in the old leader's store.
        let missing = !self
            .durable_record
            .as_ref()
            .is_some_and(|own| record.features.iter().all(|f| own.features.contains(f)))
            && !self
                .record
                .as_ref()
                .is_some_and(|own| record.features.iter().all(|f| own.features.contains(f)));
        if missing {
            return self.install(record).unwrap_or_else(|()| {
                self.counts.refused += 1;
                FeatureStep::default()
            });
        }
        FeatureStep::default()
    }

    /// The operator's request, at the leader.
    pub fn activate(
        &mut self,
        feature: Feature,
    ) -> Result<(Activation, FeatureStep), ActivateRefusal> {
        let Some(standing) = self.standing else {
            return Err(ActivateRefusal::NotLeader { leader: self.me });
        };
        if !standing.leading {
            return Err(ActivateRefusal::NotLeader {
                leader: standing.ballot.leader,
            });
        }
        if feature.prepare_marker().is_none() {
            return Err(ActivateRefusal::NotActivatable(feature));
        }
        if !Supported::reported().any(|f| f == feature) {
            return Err(ActivateRefusal::Unsupported(feature));
        }
        if self.active().contains(&feature) {
            return Ok((Activation::AlreadyActive, self.resend()));
        }
        if let Some(busy) = self.activating
            && busy != feature
        {
            return Err(ActivateRefusal::Busy(busy));
        }
        let epoch = self.voters.epoch();
        let missing: Vec<ReplicaId> = self
            .peers()
            .filter(|v| {
                !self
                    .advertised
                    .get(v)
                    .is_some_and(|(e, set)| *e == epoch && set.contains(&feature))
            })
            .collect();
        if !missing.is_empty() {
            return Err(ActivateRefusal::Missing {
                feature,
                voters: missing,
            });
        }
        self.activating = Some(feature);
        let mut step = self.on_prepare(self.me, feature.id(), epoch);
        for peer in self.peers().collect::<Vec<_>>() {
            step.sends.push((
                peer,
                ProtocolMessage::FeaturePrepare {
                    feature: feature.id(),
                    epoch,
                },
            ));
        }
        step.absorb(self.try_activate());
        Ok((Activation::Started, step))
    }

    /// Whether `voter`'s report naming `feature` is durable in this store
    /// under this configuration.
    fn recorded(&self, voter: &ReplicaId, feature: Feature) -> bool {
        self.rows.get(voter).is_some_and(|r| {
            r.durable
                && r.row.configuration == self.voters.epoch()
                && r.row.features.contains(&feature.id())
        })
    }

    /// Activate once every voter's prepared report is durable here.
    fn try_activate(&mut self) -> FeatureStep {
        let mut step = FeatureStep::default();
        let Some(feature) = self.activating else {
            return step;
        };
        if !self.standing.is_some_and(|s| s.leading) {
            return step;
        }
        if self.unsettled.iter().any(|b| b.announce == Some(feature)) {
            return step;
        }
        if !self.durably_prepared(feature)
            || !self
                .voters
                .voters()
                .iter()
                .all(|v| self.recorded(v, feature))
        {
            return step;
        }
        let rows: Vec<FeatureSupportV1> = self.rows.values().map(|r| r.row.clone()).collect();
        match activation_over(
            &rows,
            self.record.as_ref(),
            &self.voters,
            self.cluster,
            self.domain,
            feature,
        ) {
            Ok(record) => {
                self.counts.activated += 1;
                if let Some(batch) = self.journal(None, Some(record), Some(feature)) {
                    step.batches.push(batch);
                }
            }
            Err(ActivateError::AlreadyActive { .. }) => self.activating = None,
            Err(_) => self.counts.refused += 1,
        }
        step
    }

    /// Hand the driver what became of its batches, in order, and do what
    /// each durable one leads to.
    pub fn settle(
        &mut self,
        is_durable: impl Fn(&BarrierId) -> bool,
        is_failed: impl Fn(&BarrierId) -> bool,
    ) -> FeatureStep {
        let mut step = FeatureStep::default();
        while let Some(batch) = self.unsettled.front() {
            if is_failed(&batch.barrier) {
                // What it would have written never lands. The rest stands:
                // a prepare or an install it carried is asked for again by
                // the leader's re-send, and an activation it carried is
                // written again by `try_activate` below.
                self.unsettled.pop_front();
                self.counts.failed += 1;
                self.roll_back();
                continue;
            }
            if !is_durable(&batch.barrier) {
                break;
            }
            let batch = self.unsettled.pop_front().expect("checked above");
            if let Some(row) = &batch.row {
                self.durable_rows.insert(row.voter, row.clone());
                if let Some(held) = self.rows.get_mut(&row.voter)
                    && held.row == *row
                {
                    held.durable = true;
                }
            }
            if let Some(record) = batch.record {
                self.durable_record = Some(record);
                self.changed = true;
            }
            if let Some(feature) = batch.announce {
                self.activating = None;
                step.absorb(self.announce(feature));
            }
        }
        // Answers owed once this voter's prepare is durable.
        let owed: Vec<(ReplicaId, Feature)> = self.owed_prepared.iter().copied().collect();
        for (leader, feature) in owed {
            if self.durably_prepared(feature) {
                self.owed_prepared.remove(&(leader, feature));
                if leader == self.me {
                    self.counts.prepared += 1;
                } else if let Some(message) = self.prepared_message(feature) {
                    self.counts.prepared += 1;
                    step.sends.push((leader, message));
                }
            }
        }
        if self.owed_held
            && self.unsettled.iter().all(|b| b.record.is_none())
            && let Some(standing) = self.standing
        {
            self.owed_held = false;
            if standing.ballot.leader != self.me
                && let Some(held) = self.held_message()
            {
                step.sends.push((standing.ballot.leader, held));
            }
        }
        step.absorb(self.try_activate());
        step
    }

    /// This leader's activation of `feature` is durable: every voter not
    /// holding it is sent the record.
    fn announce(&mut self, feature: Feature) -> FeatureStep {
        let mut step = FeatureStep::default();
        let Some(record) = self.durable_record.as_ref().map(wire) else {
            return step;
        };
        for peer in self.peers().collect::<Vec<_>>() {
            if !self
                .holders
                .get(&peer)
                .is_some_and(|h| h.contains(&feature))
            {
                step.sends.push((
                    peer,
                    ProtocolMessage::FeatureActivated {
                        feature: feature.id(),
                        record: record.clone(),
                    },
                ));
            }
        }
        step
    }

    /// The leader's re-send, paced by the runtime. A voter whose
    /// advertisement it has not heard since its last link is asked for it.
    /// For each feature its own
    /// durable record turns on, every voter that has not said it holds it
    /// is asked to prepare and sent the record -- after a lost message, a
    /// leader change, or a voter that joined after the activation -- and,
    /// while an activation waits for prepared reports, the voters whose
    /// reports are not durable here are asked again.
    pub fn resend(&mut self) -> FeatureStep {
        let mut step = FeatureStep::default();
        if !self.standing.is_some_and(|s| s.leading) {
            return step;
        }
        let epoch = self.voters.epoch();
        let peers: Vec<ReplicaId> = self.peers().collect();
        // A voter whose advertisement this leader has not heard since its
        // last link is asked for it.
        for peer in &peers {
            if self.advertised.get(peer).is_none_or(|(e, _)| *e != epoch) {
                step.sends.push((*peer, self.advertisement()));
            }
        }
        if let Some(feature) = self.activating {
            for peer in &peers {
                if !self.recorded(peer, feature) {
                    step.sends.push((
                        *peer,
                        ProtocolMessage::FeaturePrepare {
                            feature: feature.id(),
                            epoch,
                        },
                    ));
                }
            }
        }
        let record = self.durable_record.as_ref().map(wire);
        for feature in self.active() {
            let Some(record) = &record else { break };
            for peer in &peers {
                if self.holders.get(peer).is_some_and(|h| h.contains(&feature)) {
                    continue;
                }
                if !self.recorded(peer, feature) {
                    step.sends.push((
                        *peer,
                        ProtocolMessage::FeaturePrepare {
                            feature: feature.id(),
                            epoch,
                        },
                    ));
                }
                step.sends.push((
                    *peer,
                    ProtocolMessage::FeatureActivated {
                        feature: feature.id(),
                        record: record.clone(),
                    },
                ));
            }
        }
        step
    }

    /// The leader's view of every configured voter.
    pub fn status(&self) -> FeatureStatus {
        let epoch = self.voters.epoch();
        let voters = self
            .voters
            .voters()
            .iter()
            .map(|voter| {
                let prepared = Feature::ALL
                    .iter()
                    .copied()
                    .filter(|f| !f.is_marker() && self.recorded(voter, *f))
                    .collect();
                if *voter == self.me {
                    VoterFeatures {
                        voter: *voter,
                        advertised: Some(Supported::reported().collect()),
                        prepared,
                        held: Some(self.active()),
                    }
                } else {
                    VoterFeatures {
                        voter: *voter,
                        advertised: self
                            .advertised
                            .get(voter)
                            .filter(|(e, _)| *e == epoch)
                            .map(|(_, set)| set.clone()),
                        prepared,
                        held: self.holders.get(voter).cloned(),
                    }
                }
            })
            .collect();
        FeatureStatus {
            epoch,
            leading: self.standing.is_some_and(|s| s.leading),
            leader: self.standing.map(|s| s.ballot.leader),
            active: self.active(),
            activating: self.activating,
            voters,
        }
    }
}

fn wire(record: &ActiveFeaturesV1) -> FeatureRecord {
    FeatureRecord {
        epoch: record.configuration,
        features: record.features.clone(),
        reporters: record.reporters.clone(),
    }
}

/// A replica's identity, for operator output.
pub fn hex(replica: &ReplicaId) -> String {
    replica.0.iter().map(|b| format!("{b:02x}")).collect()
}

/// This voter's own support row as its store holds it, for a caller that
/// has only a view (start, before the driver is open).
pub fn own_row<V: OrderedRead>(
    view: &V,
    me: &ReplicaId,
) -> Result<Option<FeatureSupportV1>, String> {
    own_support(view, me).map_err(|e| format!("{e:?}"))
}

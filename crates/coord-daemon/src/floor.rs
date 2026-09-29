//! Agreeing a forgetting floor (task-d27; design Section 5.3).
//!
//! A voter forgets history only below a floor a majority of the voters
//! has promised, each about the same shared checkpoint. The libraries
//! hold the rules ([`coord_checkpoint::floor`]); this is the runtime's
//! part: when a voter exports, where it keeps what it promised about,
//! and how the promises travel.
//!
//! **A common boundary.** Every [`FLOOR_INTERVAL`] executed positions,
//! right after the command at that position is applied, the voter
//! exports the shared checkpoint. The applier returns only once the
//! command's own batch is readable, and nothing but a command moves the
//! execution frontier, so the export is at exactly that position. Voters
//! that executed the same commands export the same state, so they name
//! the same boundary and the same root, and promises about one
//! checkpoint can be counted together. Exported at each voter's own
//! frontier instead, no two would ever agree.
//!
//! **A promise that is durable before it is told.** The image is kept
//! (synced, renamed into place) before the readiness row is written, and
//! the row is journaled before the readiness goes to the peers: the
//! send waits on the row's barrier like any vote.
//!
//! **Peers' promises are recorded, not trusted.** A readiness is taken
//! only from the voter it names, over that voter's authenticated link,
//! and only through [`record_readiness`], which refuses a promise that
//! goes back or competes with one held.
//!
//! **An image outlives every durable row that names it.** A new
//! promise or activation supersedes the images the older rows named,
//! but those rows stay what recovery reads until the new batch is
//! durable. So superseded images are reclaimed only once the batch
//! that superseded them is ([`Floor::settle`]), and a batch that fails
//! reclaims nothing. On reopening, the image this voter's own latest
//! durable promise names is read back and verified: a voter that lost
//! it does not serve on advertising a promise it cannot keep.
//!
//! **Activation.** When a majority's promises name one checkpoint, the
//! certificate is journaled. Nothing is deleted below it here: that is
//! trimming's, which comes after the agreement it rests on.

use std::collections::BTreeMap;
use std::path::Path;

use coord_checkpoint::floor::published_activation;
use coord_checkpoint::{
    ActivatedFloorV1, CheckpointOrigin, CheckpointReadinessV1, ExportLimits, SharedImageStore,
    TrimLimits, activate_floor, export_shared, publish_activation, read_readiness,
    record_readiness,
};
use coord_consensus::quorum::EpochVoters;
use coord_core::effect::{BarrierId, BootId, StoreUpdate};
use coord_core::outbox::BarrierAllocator;
use coord_store_api::engine::OrderedRead;
use coord_types::identity::Digest32;
use coord_types::ids::{ExecutionPosition, ReplicaId, ReplicaIncarnation};

/// Executed positions between floor boundaries.
///
/// The schema's, not a setting: voters agree on a floor only by naming
/// the same boundary, and a boundary each voter derived from its own
/// setting would agree only by coincidence. It moves into replicated
/// policy when intervals do.
pub const FLOOR_INTERVAL: u64 = 4096;

/// What a voter's floor did since boot (diagnostic).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FloorCounts {
    /// Boundaries this voter exported, kept and promised.
    pub promised: u64,
    /// Boundaries it refused, and said why ([`Floor::last_refusal`]).
    pub refused: u64,
    /// Peers' promises it recorded.
    pub heard: u64,
    /// Peers' promises it refused: malformed, not the sender's own, or
    /// going back on one it holds.
    pub rejected: u64,
    /// Floors it activated.
    pub activated: u64,
    /// Times superseded images could not be removed.
    pub unreclaimed: u64,
}

/// Why a boundary was not promised.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FloorRefusal {
    /// The images' filesystem could not be asked for its free space.
    Disk(String),
    /// Less free space than the headroom and the image would take.
    Headroom {
        /// Bytes free.
        free: u64,
        /// Bytes needed.
        need: u64,
    },
    /// The export failed.
    Export(String),
    /// The export was not at the boundary: the frontier had moved.
    Moved {
        /// The boundary.
        boundary: ExecutionPosition,
        /// Where the export was.
        exported: ExecutionPosition,
    },
    /// The image could not be kept.
    Image(String),
    /// The promise rules refused this voter's own readiness.
    Promise(String),
}

impl core::fmt::Display for FloorRefusal {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            FloorRefusal::Disk(e) => write!(f, "the images' free space is unknown: {e}"),
            FloorRefusal::Headroom { free, need } => {
                write!(f, "{free} bytes free, {need} needed")
            }
            FloorRefusal::Export(e) => write!(f, "the export failed: {e}"),
            FloorRefusal::Moved { boundary, exported } => write!(
                f,
                "exported at {} for the boundary {}",
                exported.get(),
                boundary.get()
            ),
            FloorRefusal::Image(e) => write!(f, "the image was not kept: {e}"),
            FloorRefusal::Promise(e) => write!(f, "the promise was refused: {e}"),
        }
    }
}

/// A boundary this voter promised: the rows to journal, and the
/// readiness to send once they are durable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Promised {
    /// The readiness row, and the activation it completed, if any.
    pub updates: Vec<StoreUpdate>,
    /// The encoded readiness.
    pub readiness: Vec<u8>,
}

/// Where a voter keeps its floor, and whom it agrees it with.
#[derive(Clone, Debug)]
pub struct FloorSettings {
    /// Executed positions between boundaries ([`FLOOR_INTERVAL`]).
    pub interval: u64,
    /// Directory the promised images are kept in.
    pub images: std::path::PathBuf,
    /// Free bytes to keep beyond the image an export adds.
    pub headroom_bytes: u64,
    /// Cluster and domain.
    pub origin: CheckpointOrigin,
    /// The configuration's voters.
    pub voters: std::collections::BTreeSet<ReplicaId>,
    /// This voter.
    pub me: ReplicaId,
}

/// One voter's side of the floor agreement.
#[derive(Debug)]
pub struct Floor {
    interval: u64,
    images: SharedImageStore,
    headroom: u64,
    origin: CheckpointOrigin,
    voters: EpochVoters,
    me: ReplicaId,
    /// The latest promise of each voter this one holds, its own among
    /// them. Kept here as well as in the rows because a row still in the
    /// journal is not in the view yet.
    heard: BTreeMap<ReplicaId, CheckpointReadinessV1>,
    activated: Option<ActivatedFloorV1>,
    /// The images the durable rows name: this voter's latest durable
    /// promise and the latest durable activation.
    standing: Vec<Digest32>,
    /// Batches of this floor's rows not yet durable, oldest first, each
    /// with the images its rows name.
    unsettled: Vec<(BarrierId, Vec<Digest32>)>,
    /// A batch of this boot failed: what the durable rows name is no
    /// longer what any later batch held, so nothing is reclaimed until a
    /// restart reads the rows back.
    diverged: bool,
    last_image_bytes: u64,
    barriers: BarrierAllocator,
    incarnation: ReplicaIncarnation,
    boot: BootId,
    last_refusal: Option<FloorRefusal>,
    /// Since boot.
    pub counts: FloorCounts,
}

impl Floor {
    /// A voter's floor, from what its store already holds: the promises
    /// recorded and the floor activated before this boot.
    pub fn open<V: OrderedRead>(
        settings: FloorSettings,
        view: &V,
        incarnation: ReplicaIncarnation,
        boot: BootId,
    ) -> Result<Self, String> {
        if settings.interval == 0 {
            return Err("a floor interval of zero names no boundary".into());
        }
        let images = SharedImageStore::open(&settings.images)
            .map_err(|e| format!("the floor images' directory: {e}"))?;
        // The epoch a floor is agreed in is the one its checkpoints name:
        // the configuration the store's execution frontier was reached
        // under, which is what an export binds into the manifest. Until a
        // reconfiguration executes that is the initial one, whatever the
        // membership's epoch, as it is for every record the journal
        // stamps (`Persistence::follow_ballot`).
        let epoch = coord_storage::lowering::DurableMeta::read(view)
            .map_err(|e| format!("the store's frontier: {e:?}"))?
            .frontier
            .configuration;
        let voters = EpochVoters::new(epoch, settings.voters)
            .ok_or("a floor is agreed by voters, and there are none")?;
        let heard: BTreeMap<ReplicaId, CheckpointReadinessV1> =
            read_readiness(view, &TrimLimits::default())
                .map_err(|e| format!("the recorded floor promises: {e:?}"))?
                .into_iter()
                .filter(|r| r.configuration == epoch && voters.is_voter(&r.voter))
                .map(|r| (r.voter, r))
                .collect();
        let activated =
            published_activation(view).map_err(|e| format!("the activated floor: {e:?}"))?;
        // Rows are durable only after the image they name was kept, so an
        // image missing now was lost after the promise was made.
        if let Some(own) = heard.get(&settings.me) {
            images.verify(&own.root).map_err(|e| {
                format!(
                    "the image this voter promised at {} cannot be read back: {e}",
                    own.boundary.execution_position.get()
                )
            })?;
        }
        let standing = roots(heard.get(&settings.me), activated.as_ref());
        Ok(Floor {
            interval: settings.interval,
            images,
            headroom: settings.headroom_bytes,
            origin: settings.origin,
            voters,
            me: settings.me,
            heard,
            activated,
            standing,
            unsettled: Vec::new(),
            diverged: false,
            last_image_bytes: 0,
            barriers: BarrierAllocator::new(incarnation, boot).for_runtime(),
            incarnation,
            boot,
            last_refusal: None,
            counts: FloorCounts::default(),
        })
    }

    /// Whether `position` is a boundary this voter has still to promise.
    pub fn due(&self, position: ExecutionPosition) -> bool {
        position.get() > 0
            && position.get().is_multiple_of(self.interval)
            && self
                .heard
                .get(&self.me)
                .is_none_or(|own| own.boundary.execution_position < position)
    }

    /// Export, keep and promise the checkpoint of `view`, which is at
    /// the boundary `position`. `None` when refused, and why is kept
    /// ([`Floor::last_refusal`]): a refused boundary promises nothing,
    /// and the next one is tried afresh.
    pub fn boundary<V: OrderedRead>(
        &mut self,
        view: &V,
        position: ExecutionPosition,
    ) -> Option<Promised> {
        match self.promise(view, position) {
            Ok(promised) => {
                self.counts.promised += 1;
                Some(promised)
            }
            Err(refusal) => {
                self.counts.refused += 1;
                self.last_refusal = Some(refusal);
                None
            }
        }
    }

    fn promise<V: OrderedRead>(
        &mut self,
        view: &V,
        position: ExecutionPosition,
    ) -> Result<Promised, FloorRefusal> {
        let free = available(self.images.root()).map_err(|e| FloorRefusal::Disk(e.to_string()))?;
        let need = self.headroom.saturating_add(self.last_image_bytes);
        if free < need {
            return Err(FloorRefusal::Headroom { free, need });
        }
        let checkpoint = export_shared(view, self.origin, &ExportLimits::default())
            .map_err(|e| FloorRefusal::Export(e.to_string()))?;
        let exported = checkpoint.manifest.boundary.execution_position;
        if exported != position {
            return Err(FloorRefusal::Moved {
                boundary: position,
                exported,
            });
        }
        // Kept before it is promised about: a readiness says this voter
        // holds these bytes.
        self.last_image_bytes = self
            .images
            .keep(&checkpoint)
            .map_err(|e| FloorRefusal::Image(e.to_string()))?;
        let readiness = CheckpointReadinessV1::for_manifest(self.me, &checkpoint.manifest);
        let row = record_readiness(view, &self.voters, &readiness)
            .map_err(|e| FloorRefusal::Promise(format!("{e:?}")))?;
        let encoded = readiness
            .encode()
            .map_err(|e| FloorRefusal::Promise(e.to_string()))?;
        self.heard.insert(self.me, readiness);
        let mut updates = vec![row];
        updates.extend(self.activation());
        Ok(Promised {
            updates,
            readiness: encoded,
        })
    }

    /// Record a peer's promise, arrived over `from`'s own link. The rows
    /// to journal, if it was news.
    pub fn hear<V: OrderedRead>(
        &mut self,
        view: &V,
        from: ReplicaId,
        encoded: &[u8],
    ) -> Option<Vec<StoreUpdate>> {
        let Ok(record) = CheckpointReadinessV1::decode(encoded) else {
            self.counts.rejected += 1;
            return None;
        };
        // A voter promises for itself: a readiness relayed for another
        // would let one voter sign for a majority.
        if record.voter != from
            || from == self.me
            || record.cluster != self.origin.cluster
            || record.domain != self.origin.domain
        {
            self.counts.rejected += 1;
            return None;
        }
        match self.heard.get(&from) {
            Some(held) if *held == record => return None,
            // Going back, or another checkpoint at the same boundary:
            // checked here too, because the row it would be checked
            // against may still be in the journal and not in the view.
            Some(held)
                if held.boundary.execution_position >= record.boundary.execution_position =>
            {
                self.counts.rejected += 1;
                return None;
            }
            _ => {}
        }
        let Ok(row) = record_readiness(view, &self.voters, &record) else {
            self.counts.rejected += 1;
            return None;
        };
        self.heard.insert(from, record);
        self.counts.heard += 1;
        let mut updates = vec![row];
        updates.extend(self.activation());
        Some(updates)
    }

    /// Take what became of this floor's batches, and reclaim the images
    /// no durable row names any more once one of them is durable.
    ///
    /// Kept: the images the latest durable batch names (this voter's own
    /// promise and the activated floor), those of every batch still
    /// unsettled, and those this voter holds now. A batch that failed
    /// wrote nothing, so a later one's images are no longer what the
    /// rows name: from then on nothing is reclaimed until a restart reads
    /// the rows back. A failure to remove costs space, not a promise, so
    /// it is counted rather than refused.
    pub fn settle(
        &mut self,
        durable: impl Fn(&BarrierId) -> bool,
        failed: impl Fn(&BarrierId) -> bool,
    ) {
        if self.unsettled.is_empty() {
            return;
        }
        if self.unsettled.iter().any(|(b, _)| failed(b)) {
            self.diverged = true;
        }
        // The journal makes batches durable in order, so the last durable
        // one names what recovery would read, and any before it are
        // superseded whatever became of them.
        let mut moved = false;
        if let Some(last) = self.unsettled.iter().rposition(|(b, _)| durable(b)) {
            let (_, roots) = self
                .unsettled
                .drain(..=last)
                .next_back()
                .expect("one at least");
            self.standing = roots;
            moved = true;
        }
        self.unsettled.retain(|(b, _)| !failed(b));
        if !moved || self.diverged {
            return;
        }
        let keep: Vec<Digest32> = self
            .standing
            .iter()
            .chain(self.unsettled.iter().flat_map(|(_, roots)| roots))
            .copied()
            .chain(roots(self.heard.get(&self.me), self.activated.as_ref()))
            .collect();
        if self.images.reclaim(&keep).is_err() {
            self.counts.unreclaimed += 1;
        }
    }

    /// The certificate update, when the promises held now activate a
    /// floor above the one activated.
    fn activation(&mut self) -> Option<StoreUpdate> {
        let rows: Vec<CheckpointReadinessV1> = self.heard.values().copied().collect();
        let certified =
            activate_floor(&rows, &self.voters, self.origin.cluster, self.origin.domain).ok()?;
        if self
            .activated
            .as_ref()
            .is_some_and(|a| a.boundary.execution_position >= certified.boundary.execution_position)
        {
            return None;
        }
        let update = publish_activation(&certified, self.activated.as_ref()).ok()?;
        self.activated = Some(certified);
        self.counts.activated += 1;
        Some(update)
    }

    /// A barrier for a batch of this floor's rows, which name what this
    /// voter holds now: those images are not reclaimed until the batch
    /// has settled ([`Floor::settle`]).
    pub fn barrier(&mut self) -> BarrierId {
        let barrier = self.barriers.allocate();
        let named = roots(self.heard.get(&self.me), self.activated.as_ref());
        self.unsettled.push((barrier, named));
        barrier
    }

    /// The voters a promise goes to: every one but this.
    pub fn peers(&self) -> impl Iterator<Item = ReplicaId> + '_ {
        self.voters
            .voters()
            .iter()
            .copied()
            .filter(|v| *v != self.me)
    }

    /// The configuration the floor is agreed in.
    pub fn voters(&self) -> &EpochVoters {
        &self.voters
    }

    /// The incarnation and boot a send is stamped with.
    pub const fn stamp(&self) -> (ReplicaIncarnation, BootId) {
        (self.incarnation, self.boot)
    }

    /// The domain.
    pub const fn origin(&self) -> CheckpointOrigin {
        self.origin
    }

    /// The highest floor activated, if any.
    pub const fn activated(&self) -> Option<&ActivatedFloorV1> {
        self.activated.as_ref()
    }

    /// The boundary this voter last promised, if any.
    pub fn promised(&self) -> Option<ExecutionPosition> {
        self.heard
            .get(&self.me)
            .map(|own| own.boundary.execution_position)
    }

    /// A boundary refused before it reached the export: no view to
    /// export from.
    pub fn refuse(&mut self, refusal: FloorRefusal) {
        self.counts.refused += 1;
        self.last_refusal = Some(refusal);
    }

    /// Why the last refused boundary was refused.
    pub const fn last_refusal(&self) -> Option<&FloorRefusal> {
        self.last_refusal.as_ref()
    }
}

/// The images a voter's own promise and an activated floor name.
fn roots(
    own: Option<&CheckpointReadinessV1>,
    activated: Option<&ActivatedFloorV1>,
) -> Vec<Digest32> {
    own.map(|own| own.root)
        .into_iter()
        .chain(activated.map(|a| a.root))
        .collect()
}

/// Bytes free to an unprivileged writer on the filesystem holding `path`.
#[cfg(unix)]
fn available(path: &Path) -> std::io::Result<u64> {
    let stat = rustix::fs::statvfs(path)?;
    Ok(stat.f_bavail.saturating_mul(stat.f_frsize))
}

/// Unknown off unix: no headroom is enforced there.
#[cfg(not(unix))]
fn available(_: &Path) -> std::io::Result<u64> {
    Ok(u64::MAX)
}

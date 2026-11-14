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
    TrimLimits, activate_floor, common_state_bytes, export_shared, publish_activation,
    read_readiness, record_readiness,
};
use coord_consensus::quorum::EpochVoters;
use coord_core::effect::{BarrierId, BootId, StoreUpdate};
use coord_core::outbox::BarrierAllocator;
use coord_store_api::engine::OrderedRead;
use coord_types::identity::Digest32;
use coord_types::ids::{ExecutionPosition, ReplicaId, ReplicaIncarnation};

/// Most common state, in key and value bytes, a floor exports.
///
/// The export and the image's write run inline, on the domain's serving
/// loop, so their time is time the domain neither serves nor answers a
/// peer: they cost O(common state) in reads, memory and written bytes,
/// and an fsync per 1 MiB chunk. Measured with redb at coordd's default
/// cache, release build, one 4-core container: 16 MiB 0.16 s, 32 MiB
/// 0.29 s, 48 MiB 0.49 s, 64 MiB 0.69 s, 128 MiB 1.6 s, 384 MiB 11 s
/// (`measure_the_inline_boundary` in coord-checkpoint's export tests).
/// 32 MiB keeps one boundary under a third of the election's 1 s
/// patience on that machine. The cap goes when the export moves off the
/// serving loop onto a pinned snapshot (task-d27's second part).
pub const INLINE_EXPORT_CAP_BYTES: u64 = 32 * 1024 * 1024;

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
    /// The last image was over [`INLINE_EXPORT_CAP_BYTES`]: exporting
    /// again would hold the serving loop longer than the cap allows.
    OverCap {
        /// Bytes of the last image.
        image: u64,
    },
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
            FloorRefusal::OverCap { image } => write!(
                f,
                "the last image was {image} bytes, over the inline export's cap of \
                 {INLINE_EXPORT_CAP_BYTES}"
            ),
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

/// A batch of the floor's rows not yet durable: the images its rows name,
/// and this voter's promise and the activated floor once it is.
#[derive(Debug)]
struct Unsettled {
    barrier: BarrierId,
    roots: Vec<Digest32>,
    own: Option<CheckpointReadinessV1>,
    activated: Option<ActivatedFloorV1>,
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
    /// This voter's latest promise and the activated floor as the durable
    /// rows hold them: what a failed batch rolls `heard` and `activated`
    /// back to.
    durable_own: Option<CheckpointReadinessV1>,
    durable_activated: Option<ActivatedFloorV1>,
    /// This voter's own promise is not durable yet: made, and waiting for
    /// its batch's barrier (`None`) or for that batch to settle. Until it
    /// is durable, no activation a later batch journals counts it: its
    /// row may never exist.
    own_pending: Option<Option<BarrierId>>,
    /// The images the durable rows name: this voter's latest durable
    /// promise and the latest durable activation.
    standing: Vec<Digest32>,
    /// Batches of this floor's rows not yet durable, oldest first.
    unsettled: Vec<Unsettled>,
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
        // Counted before anything else is read, and only as far as the
        // cap: past it the answer is the same however large the state.
        let common = common_state_bytes(view, INLINE_EXPORT_CAP_BYTES, &ExportLimits::default())
            .map_err(|e| format!("the common state's size: {e}"))?;
        if common > INLINE_EXPORT_CAP_BYTES {
            return Err(format!(
                "the floor exports inline on the serving loop, and this store's common state \
                 is over its cap of {INLINE_EXPORT_CAP_BYTES} bytes; set [floor] enabled = false"
            ));
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
                let root: String = own.root.0.iter().map(|b| format!("{b:02x}")).collect();
                format!(
                    "the image {root} this voter promised at {} cannot be read back: {e}",
                    own.boundary.execution_position.get()
                )
            })?;
        }
        let standing = roots(heard.get(&settings.me), activated.as_ref());
        let durable_own = heard.get(&settings.me).copied();
        let durable_activated = activated.clone();
        Ok(Floor {
            interval: settings.interval,
            images,
            headroom: settings.headroom_bytes,
            origin: settings.origin,
            voters,
            me: settings.me,
            heard,
            activated,
            durable_own,
            durable_activated,
            own_pending: None,
            standing,
            unsettled: Vec::new(),
            diverged: false,
            // No image written yet: the count stands in for its size, and
            // is at least what the first export will carry.
            last_image_bytes: common,
            barriers: BarrierAllocator::new(incarnation, boot).for_runtime(),
            incarnation,
            boot,
            last_refusal: None,
            counts: FloorCounts::default(),
        })
    }

    /// Whether `position` is a boundary this voter has still to promise,
    /// with the execution frontier at `frontier`. A retry answered from
    /// the record reports the original command's position, which the
    /// frontier is past: its export would be refused as moved.
    pub fn due(&self, position: ExecutionPosition, frontier: ExecutionPosition) -> bool {
        position == frontier
            && position.get() > 0
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
        if self.last_image_bytes > INLINE_EXPORT_CAP_BYTES {
            return Err(FloorRefusal::OverCap {
                image: self.last_image_bytes,
            });
        }
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
        self.own_pending = Some(None);
        let mut updates = vec![row];
        // In the promise's own batch its row and the activation it
        // completes are durable together or not at all.
        updates.extend(self.activation(true));
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
        updates.extend(self.activation(false));
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
    ///
    /// Once this voter's own promise is durable it counts: the rows to
    /// journal when the promises heard while it was in flight activate a
    /// floor with it.
    pub fn settle(
        &mut self,
        durable: impl Fn(&BarrierId) -> bool,
        failed: impl Fn(&BarrierId) -> bool,
    ) -> Option<Vec<StoreUpdate>> {
        if self.unsettled.is_empty() {
            return None;
        }
        let own_failed = self.own_pending.flatten().is_some_and(|b| failed(&b));
        let own_durable = self.own_pending.flatten().is_some_and(|b| durable(&b));
        let any_failed = self.unsettled.iter().any(|u| failed(&u.barrier));
        if any_failed {
            self.diverged = true;
        }
        // The journal makes batches durable in order, so the last durable
        // one names what recovery would read, and any before it are
        // superseded whatever became of them.
        let mut moved = false;
        if let Some(last) = self.unsettled.iter().rposition(|u| durable(&u.barrier)) {
            let batch = self
                .unsettled
                .drain(..=last)
                .next_back()
                .expect("one at least");
            self.standing = batch.roots;
            if !own_failed {
                self.durable_own = batch.own;
            }
            if !any_failed {
                self.durable_activated = batch.activated;
            }
            moved = true;
        }
        self.unsettled.retain(|u| !failed(&u.barrier));
        if own_durable {
            self.own_pending = None;
        }
        // What a failed batch wrote is not there: this voter's promise
        // and the activation go back to what the durable rows hold, and
        // the next boundary is promised afresh.
        if own_failed {
            self.own_pending = None;
            match self.durable_own {
                Some(own) => self.heard.insert(self.me, own),
                None => self.heard.remove(&self.me),
            };
        }
        if any_failed {
            self.activated = self.durable_activated.clone();
        }
        if moved && !self.diverged {
            self.reclaim();
        }
        if own_durable && !own_failed {
            return self.activation(false).map(|update| vec![update]);
        }
        None
    }

    /// Remove the images no durable row, unsettled batch or current
    /// promise names.
    fn reclaim(&mut self) {
        let keep: Vec<Digest32> = self
            .standing
            .iter()
            .chain(self.unsettled.iter().flat_map(|u| &u.roots))
            .copied()
            .chain(roots(self.heard.get(&self.me), self.activated.as_ref()))
            .collect();
        if self.images.reclaim(&keep).is_err() {
            self.counts.unreclaimed += 1;
        }
    }

    /// The certificate update, when the promises held now activate a
    /// floor above the one activated.
    ///
    /// This voter's own promise counts only when it is durable, or when
    /// it is journaled in the same batch (`with_own`).
    fn activation(&mut self, with_own: bool) -> Option<StoreUpdate> {
        let own_counts = with_own || self.own_pending.is_none();
        let rows: Vec<CheckpointReadinessV1> = self
            .heard
            .iter()
            .filter(|(voter, _)| own_counts || **voter != self.me)
            .map(|(_, row)| *row)
            .collect();
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
        if self.own_pending == Some(None) {
            self.own_pending = Some(Some(barrier));
        }
        self.unsettled.push(Unsettled {
            barrier,
            roots: named,
            own: self.heard.get(&self.me).copied(),
            activated: self.activated.clone(),
        });
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

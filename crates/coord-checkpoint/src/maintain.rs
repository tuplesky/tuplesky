//! Keeping a node's own recovery baseline current (design Section
//! 17.16.3, task-j04).
//!
//! [`local`] can write an image and [`JournaledStore`] can publish a
//! pointer and retire a prefix, but neither of them can decide to. The
//! five steps of the publication order live in three crates -- the
//! snapshot and the image in storage and the filesystem, the pointer
//! and the retirement in the journal -- and this is where a running
//! node drives them as one step:
//!
//! 1. pin a snapshot of the projection;
//! 2. write a complete inactive image of it;
//! 3. append the pointer and make it durable;
//! 4. retire the journal prefix it represents;
//! 5. reclaim the images it supersedes.
//!
//! Steps 1 and 2 are split so that they need not run on the thread that
//! owns the store (task-d51). [`LocalBaseline::pin_local`] pins the
//! snapshot there, [`Pinned::write`] produces and writes the image
//! anywhere, [`LocalBaseline::finish_local`] runs steps 3 and 4 back on
//! the owning thread, and [`reclaim_local`] runs step 5 anywhere again.
//! An image is a copy of the whole projection, so its export grows with
//! it, and so does removing the one it supersedes; the pin and the
//! pointer do not.
//!
//! *When* is not decided here either. The caller asks how far the
//! journal has run past the last baseline and spends the I/O when it
//! judges the prefix worth reclaiming, because that is a local
//! operational question: no replicated result depends on the answer,
//! and a node that published on a rule of its own would still be
//! correct.
//!
//! Nothing in the cycle is a precondition for serving. A failed export,
//! a failed write and a failed retirement all leave the previous
//! baseline standing and the journal holding more history than it
//! needs, which is the safe direction: the cost of not publishing is
//! disk, and the cost of publishing something unloadable would be the
//! prefix that proved it.
//!
//! [`local`]: crate::local
//! [`JournaledStore`]: coord_storage::journaled::JournaledStore

use core::cell::Cell;
use core::fmt;
use std::time::{Duration, Instant};

use coord_journal_api::JournalEngine;
use coord_journal_api::RecordOrigin;
use coord_journal_api::frontier::CheckpointPointerV1;
use coord_storage::JournaledDomain;
use coord_storage::journaled::{JournaledError, PublishPhases};
use coord_storage::view::{GatedView, ViewError};
use coord_store_api::engine::{
    CollectionId, EngineError, ErrorClass, LocalEngine, OrderedRead, RowPage, ScanRequest,
    SnapshotSource,
};
use coord_types::ids::LocalJournalSeq;

use crate::local::{LocalError, LocalLimits, export_local};
use crate::store::{LocalCheckpointStore, StoreError};

/// What one completed publication did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Publication {
    /// The pointer that now selects this node's recovery baseline.
    pub pointer: CheckpointPointerV1,
    /// The sequence it represents (`C`).
    pub represented: LocalJournalSeq,
    /// Whether the journal prefix through it was retired as well.
    ///
    /// `false` is not a failure. The pointer is durable and the
    /// baseline authoritative; the prefix is simply still on disk and
    /// a later publication retires it.
    pub retired: bool,
    /// Images the publication superseded and removed.
    pub reclaimed: usize,
    /// How long each step took.
    pub phases: Phases,
}

/// How long each step of a publication took, in the order they run.
///
/// The image is written from a snapshot and the pointer appended to the
/// journal, and the two grow for different reasons: the export with the
/// projection it copies, the journal's part with what was lent out and
/// with the profile. Reported apart so that a run whose publications
/// grow says which.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Phases {
    /// Step 1: pinning the snapshot, on the thread that owns the store.
    pub pin: Duration,
    /// Step 2's read: producing the image from the snapshot.
    pub export: Duration,
    /// Step 2's write: the image file, synced, and its directory.
    pub write: Duration,
    /// Steps 3 and 4, the journal's.
    pub journal: PublishPhases,
    /// Step 5.
    pub reclaim: Duration,
}

/// Why a publication did not complete.
#[derive(Debug)]
pub enum BaselineError {
    /// The domain is not attached to this store, so there is no
    /// projection to image and no stream to publish into.
    Unattached,
    /// No snapshot could be pinned.
    View(ViewError),
    /// The image could not be produced from the snapshot.
    Export(LocalError),
    /// The image could not be written, loaded or reclaimed.
    Image(StoreError),
    /// The pointer could not be made durable.
    Journal(JournaledError),
    /// The export ran past its deadline and was given up. The snapshot
    /// is released and nothing was written.
    Abandoned,
    /// The domain was reattached under another origin while the image
    /// was written, so the image describes storage this node no longer
    /// has. Nothing was published.
    Superseded,
}

impl fmt::Display for BaselineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BaselineError::Unattached => write!(f, "the domain is not attached"),
            BaselineError::View(e) => write!(f, "no snapshot to image: {e:?}"),
            BaselineError::Export(e) => write!(f, "the image could not be produced: {e}"),
            BaselineError::Image(e) => write!(f, "{e}"),
            BaselineError::Journal(e) => write!(f, "the pointer was refused: {e}"),
            BaselineError::Abandoned => write!(f, "the export ran past its deadline"),
            BaselineError::Superseded => {
                write!(f, "the domain was reattached while its image was written")
            }
        }
    }
}

impl std::error::Error for BaselineError {}

impl From<ViewError> for BaselineError {
    fn from(e: ViewError) -> Self {
        BaselineError::View(e)
    }
}

impl From<LocalError> for BaselineError {
    fn from(e: LocalError) -> Self {
        BaselineError::Export(e)
    }
}

impl From<StoreError> for BaselineError {
    fn from(e: StoreError) -> Self {
        BaselineError::Image(e)
    }
}

impl From<JournaledError> for BaselineError {
    fn from(e: JournaledError) -> Self {
        BaselineError::Journal(e)
    }
}

/// A publication whose snapshot is pinned and whose image is still to
/// be produced (task-d51).
///
/// Holding it holds the snapshot, and the engine cannot reuse pages
/// freed while a snapshot is open: the file grows by about what is
/// written meanwhile. [`Pinned::write`] releases it as soon as the image
/// is produced, and a deadline bounds how long that may take.
pub struct Pinned<V> {
    view: GatedView<V>,
    origin: RecordOrigin,
    represented: LocalJournalSeq,
    pin: Duration,
}

impl<V: OrderedRead> Pinned<V> {
    /// The sequence the image will represent (`C` once published).
    pub fn represented(&self) -> LocalJournalSeq {
        self.represented
    }

    /// Step 2: produce the image from the snapshot and write it as a
    /// complete inactive image.
    ///
    /// Touches neither the store's writer nor the journal, so it runs on
    /// any thread while the owner goes on committing. A read after
    /// `deadline` fails the export as [`BaselineError::Abandoned`]; the
    /// write, once the image is produced, is not interrupted.
    pub fn write(
        self,
        images: &LocalCheckpointStore,
        limits: &LocalLimits,
        deadline: Option<Instant>,
    ) -> Result<Written, BaselineError> {
        let started = Instant::now();
        let bounded = Bounded {
            view: self.view.view(),
            deadline,
            exceeded: Cell::new(false),
        };
        let checkpoint = match export_local(&bounded, self.origin, self.represented, limits) {
            Ok(checkpoint) => checkpoint,
            Err(_) if bounded.exceeded.get() => return Err(BaselineError::Abandoned),
            Err(e) => return Err(e.into()),
        };
        drop(self.view);
        let export = started.elapsed();
        let started = Instant::now();
        let pointer = images.write(&checkpoint)?;
        Ok(Written {
            pointer,
            pin: self.pin,
            export,
            write: started.elapsed(),
        })
    }
}

/// An image written and not yet selected: what [`Pinned::write`] hands
/// back to the thread that owns the store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Written {
    /// The pointer that would select it.
    pub pointer: CheckpointPointerV1,
    /// How long the pin took.
    pub pin: Duration,
    /// How long producing the image took.
    pub export: Duration,
    /// How long writing it took.
    pub write: Duration,
}

/// A snapshot whose reads fail once a deadline has passed.
///
/// `export_local` reads a page at a time, so a read that refuses ends
/// the export within a page of the deadline.
struct Bounded<'a, V> {
    view: &'a V,
    deadline: Option<Instant>,
    exceeded: Cell<bool>,
}

impl<V> Bounded<'_, V> {
    fn check(&self) -> Result<(), EngineError> {
        match self.deadline {
            Some(deadline) if Instant::now() > deadline => {
                self.exceeded.set(true);
                Err(EngineError::new(
                    ErrorClass::Limit,
                    "the checkpoint export ran past its deadline",
                ))
            }
            _ => Ok(()),
        }
    }
}

impl<V: OrderedRead> OrderedRead for Bounded<'_, V> {
    fn get(&self, collection: CollectionId, key: &[u8]) -> Result<Option<Vec<u8>>, EngineError> {
        self.check()?;
        self.view.get(collection, key)
    }

    fn scan_page(
        &self,
        collection: CollectionId,
        request: &ScanRequest,
    ) -> Result<RowPage, EngineError> {
        self.check()?;
        self.view.scan_page(collection, request)
    }
}

/// Storage that keeps a local recovery baseline of its own.
///
/// A narrow seam rather than a method on the coordinator: publishing
/// needs the filesystem and the image format, and the coordinator is
/// deliberately ignorant of both. A caller holds the store the images
/// live in and asks for the cycle to be run against it.
pub trait LocalBaseline {
    /// The snapshot an image is produced from. It is sent to the thread
    /// that writes the image.
    type View: OrderedRead + Send + 'static;

    /// Journal records this node holds that the current baseline does
    /// not represent: `J - C`.
    ///
    /// Zero for a domain with no baseline and nothing durable. This is
    /// what a caller watches to decide when a publication is worth its
    /// I/O.
    fn unreclaimed(&self) -> u64;

    /// This domain's three durable frontiers `(J, M, C)`, or `None`
    /// where the projection is not attached (task-61).
    ///
    /// Reported as three numbers rather than one position because they
    /// are three different facts: `J - M` says whether this node is
    /// keeping up with its own durable log, and `M - C` says what a
    /// reclamation would still have to replay. A single "storage
    /// position" would hide both.
    fn frontiers(&self) -> Option<(u64, u64, u64)>;

    /// Step 1: pin a snapshot covering everything the projection has
    /// materialized.
    ///
    /// `Ok(None)` when there is nothing new to represent: the baseline
    /// already covers everything the projection has materialized, and
    /// an image of it would select the same state and retire nothing.
    fn pin_local(&mut self) -> Result<Option<Pinned<Self::View>>, BaselineError>;

    /// Steps 3 and 4 for an image [`Pinned::write`] wrote: append the
    /// pointer and retire the prefix. Step 5 is [`reclaim_local`]'s, and
    /// until it runs the publication says it reclaimed nothing.
    fn finish_local(&mut self, written: Written) -> Result<Publication, BaselineError>;

    /// Run the publication cycle once, every step on this thread.
    ///
    /// `Ok(None)` when there is nothing new to represent, as for
    /// [`LocalBaseline::pin_local`].
    fn publish_local(
        &mut self,
        images: &LocalCheckpointStore,
        limits: &LocalLimits,
    ) -> Result<Option<Publication>, BaselineError> {
        let Some(pinned) = self.pin_local()? else {
            return Ok(None);
        };
        let written = pinned.write(images, limits, None)?;
        let mut publication = self.finish_local(written)?;
        reclaim_local(images, &mut publication)?;
        Ok(Some(publication))
    }
}

/// Step 5: remove the images `publication` supersedes, and record what
/// it removed and how long that took.
///
/// Last, and on any thread: an image is removed only once a newer one
/// is durably selected, so a crash anywhere before it leaves a baseline
/// that still loads, and nothing but this removes an image. A failure
/// leaves the publication standing and the images on disk for the next
/// one to remove.
pub fn reclaim_local(
    images: &LocalCheckpointStore,
    publication: &mut Publication,
) -> Result<(), BaselineError> {
    let started = Instant::now();
    publication.reclaimed = images.reclaim(&publication.pointer)?;
    publication.phases.reclaim = started.elapsed();
    Ok(())
}

impl<J: JournalEngine, E: LocalEngine> LocalBaseline for JournaledDomain<J, E>
where
    <E::Reader as SnapshotSource>::View: Send + 'static,
{
    type View = <E::Reader as SnapshotSource>::View;

    fn unreclaimed(&self) -> u64 {
        self.store().frontiers(self.domain()).map_or(0, |f| {
            f.durable().get().saturating_sub(f.checkpoint().get())
        })
    }

    fn frontiers(&self) -> Option<(u64, u64, u64)> {
        self.store().frontiers(self.domain()).map(|f| {
            (
                f.durable().get(),
                f.materialized().get(),
                f.checkpoint().get(),
            )
        })
    }

    fn pin_local(&mut self) -> Result<Option<Pinned<Self::View>>, BaselineError> {
        let domain = self.domain();
        let store = self.store();
        let frontiers = store.frontiers(domain).ok_or(BaselineError::Unattached)?;
        // `M`, not `J`. What the image can represent is what the
        // projection has applied; a durable record the projection still
        // owes is an obligation this node has taken on and does not yet
        // hold, and an image claiming it would be a baseline missing
        // what its own pointer promised.
        let represented = frontiers.materialized();
        if represented <= frontiers.checkpoint() {
            return Ok(None);
        }
        let origin = store.origin(domain).ok_or(BaselineError::Unattached)?;
        // The snapshot is pinned after the frontier is read, and one
        // `&mut` owns both, so it covers at least `represented`. Being
        // ahead of it is harmless -- the pointer retires through what
        // it claims, never through what the image happens to contain.
        // What is committed after the pin is not in the snapshot, so the
        // image may be produced on another thread while it is.
        let started = Instant::now();
        let view = store
            .reader(domain)
            .ok_or(BaselineError::Unattached)?
            .snapshot()?;
        Ok(Some(Pinned {
            view,
            origin,
            represented,
            pin: started.elapsed(),
        }))
    }

    fn finish_local(&mut self, written: Written) -> Result<Publication, BaselineError> {
        let domain = self.domain();
        // The image was produced from the storage the pin saw. A domain
        // reattached since then, from a peer's baseline, has another
        // origin, and the image describes storage it no longer has.
        // It is left unselected, and the next publication's reclaim
        // removes it.
        if self.store().origin(domain) != Some(written.pointer.origin) {
            return Err(BaselineError::Superseded);
        }
        let pointer = written.pointer;
        // Step 3 and step 4, in that order and inside the journal.
        let published = self.store_mut().publish_checkpoint(domain, &pointer)?;
        Ok(Publication {
            pointer,
            represented: published.published,
            retired: published.retired,
            reclaimed: 0,
            phases: Phases {
                pin: written.pin,
                export: written.export,
                write: written.write,
                journal: published.phases,
                reclaim: Duration::ZERO,
            },
        })
    }
}

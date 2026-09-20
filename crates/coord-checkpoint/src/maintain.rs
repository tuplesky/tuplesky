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

use core::fmt;

use coord_journal_api::JournalEngine;
use coord_journal_api::frontier::CheckpointPointerV1;
use coord_storage::JournaledDomain;
use coord_storage::journaled::JournaledError;
use coord_storage::view::ViewError;
use coord_store_api::engine::LocalEngine;
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
}

impl fmt::Display for BaselineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BaselineError::Unattached => write!(f, "the domain is not attached"),
            BaselineError::View(e) => write!(f, "no snapshot to image: {e:?}"),
            BaselineError::Export(e) => write!(f, "the image could not be produced: {e}"),
            BaselineError::Image(e) => write!(f, "{e}"),
            BaselineError::Journal(e) => write!(f, "the pointer was refused: {e}"),
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

/// Storage that keeps a local recovery baseline of its own.
///
/// A narrow seam rather than a method on the coordinator: publishing
/// needs the filesystem and the image format, and the coordinator is
/// deliberately ignorant of both. A caller holds the store the images
/// live in and asks for the cycle to be run against it.
pub trait LocalBaseline {
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

    /// Run the publication cycle once.
    ///
    /// `Ok(None)` when there is nothing new to represent: the baseline
    /// already covers everything the projection has materialized, and
    /// an image of it would select the same state and retire nothing.
    fn publish_local(
        &mut self,
        images: &LocalCheckpointStore,
        limits: &LocalLimits,
    ) -> Result<Option<Publication>, BaselineError>;
}

impl<J: JournalEngine, E: LocalEngine> LocalBaseline for JournaledDomain<J, E> {
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

    fn publish_local(
        &mut self,
        images: &LocalCheckpointStore,
        limits: &LocalLimits,
    ) -> Result<Option<Publication>, BaselineError> {
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
        let gated = store
            .reader(domain)
            .ok_or(BaselineError::Unattached)?
            .snapshot()?;
        let checkpoint = export_local(gated.view(), origin, represented, limits)?;
        let pointer = images.write(&checkpoint)?;
        // Step 3 and step 4, in that order and inside the journal.
        let published = self.store_mut().publish_checkpoint(domain, &pointer)?;
        // Step 5, last: an image is removed only once a newer one is
        // durably selected, so a crash anywhere above leaves a baseline
        // that still loads.
        let reclaimed = images.reclaim(&pointer)?;
        Ok(Some(Publication {
            pointer,
            represented: published.published,
            retired: published.retired,
            reclaimed,
        }))
    }
}

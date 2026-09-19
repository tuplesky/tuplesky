//! The startup sequence (design Section 22.1).
//!
//! `Boot -> StorageValidated -> IdentityValidated -> MembershipChecked ->
//! ProtocolRecovered -> Serving`. The order is the point: each step is
//! evidence the next one rests on, so a process that cannot establish one
//! of them never reaches the next, and never reports readiness it has not
//! earned. A step that fails does not retry in place -- it quarantines,
//! because every failure here means durable state, identity or membership
//! is not what this node was configured to be (Sections 10.5, 17.1).
//!
//! This module also holds the production [`GenesisStore`]: the durable
//! initialization contract of `coord-membership` had test implementations
//! only, so nothing pinned a genesis digest where a real node would read
//! it back. [`StoreGenesis`] pins it in the store's own `meta_v1`
//! collection, which is where the collection registry already says
//! cluster, incarnation and genesis identity live.

use coord_membership::genesis::GenesisManifest;
use coord_membership::init::{GenesisStore, InitError, Initialized, StoreFailure, initialize};
use coord_store_api::engine::{LocalEngine, OrderedRead, SnapshotSource, WriteTxn};
use coord_store_api::registry::{Collection, meta_fields};
use coord_types::identity::Digest32;

use crate::lifecycle::QuarantineReason;

/// How far startup has progressed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum StartupPhase {
    /// Nothing established yet.
    #[default]
    Boot,
    /// Durable storage opened and its identity matches the configuration.
    StorageValidated,
    /// This node's own credentials and trust anchors are usable.
    IdentityValidated,
    /// The genesis manifest is pinned (or matched) and the committed
    /// membership is known.
    MembershipChecked,
    /// Protocol state was recovered from durable evidence.
    ProtocolRecovered,
}

impl StartupPhase {
    /// The step that must complete before this one may be attempted.
    const fn predecessor(self) -> Option<StartupPhase> {
        match self {
            StartupPhase::Boot => None,
            StartupPhase::StorageValidated => Some(StartupPhase::Boot),
            StartupPhase::IdentityValidated => Some(StartupPhase::StorageValidated),
            StartupPhase::MembershipChecked => Some(StartupPhase::IdentityValidated),
            StartupPhase::ProtocolRecovered => Some(StartupPhase::MembershipChecked),
        }
    }

    /// A stable name for diagnostics.
    pub const fn name(self) -> &'static str {
        match self {
            StartupPhase::Boot => "Boot",
            StartupPhase::StorageValidated => "StorageValidated",
            StartupPhase::IdentityValidated => "IdentityValidated",
            StartupPhase::MembershipChecked => "MembershipChecked",
            StartupPhase::ProtocolRecovered => "ProtocolRecovered",
        }
    }
}

/// Why startup stopped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StartupError {
    /// A step was attempted before the one it rests on completed. This is
    /// a defect in the composition, not an environment failure, and it is
    /// reported rather than tolerated so a reordered startup cannot
    /// quietly skip a check.
    OutOfOrder {
        /// The step attempted.
        attempted: StartupPhase,
        /// How far startup had actually got.
        reached: StartupPhase,
    },
    /// Durable storage is missing, unreadable or not this node's.
    Storage(String),
    /// Credentials or trust anchors are missing or unusable.
    Identity(String),
    /// Genesis or membership did not check out.
    Genesis(InitError),
    /// Durable protocol state could not be recovered.
    Protocol(String),
}

impl StartupError {
    /// Storage did not open, or is not this node's.
    pub fn storage(reason: impl core::fmt::Display) -> Self {
        StartupError::Storage(reason.to_string())
    }
    /// Credentials or trust anchors are unusable.
    pub fn identity(reason: impl core::fmt::Display) -> Self {
        StartupError::Identity(reason.to_string())
    }
    /// Durable protocol state could not be recovered.
    pub fn protocol(reason: impl core::fmt::Display) -> Self {
        StartupError::Protocol(reason.to_string())
    }

    /// How the process quarantines for this failure. Storage and protocol
    /// failures are a disk quarantine: durable state is not usable and a
    /// new generation is required. Genesis and identity failures are a
    /// genesis quarantine: the node is not who the manifest says it is,
    /// and running it again cannot change that.
    pub const fn quarantine_reason(&self) -> QuarantineReason {
        match self {
            StartupError::Storage(_) | StartupError::Protocol(_) => QuarantineReason::Disk,
            StartupError::Identity(_) | StartupError::Genesis(_) => QuarantineReason::Genesis,
            // A composition that runs its steps out of order has
            // established nothing about the disk or the manifest, so it
            // is reported as the process's own fault rather than as
            // evidence against the durable state.
            StartupError::OutOfOrder { .. } => QuarantineReason::Worker,
        }
    }
}

impl core::fmt::Display for StartupError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            StartupError::OutOfOrder { attempted, reached } => write!(
                f,
                "{} attempted after only {}",
                attempted.name(),
                reached.name()
            ),
            StartupError::Storage(e) => write!(f, "storage: {e}"),
            StartupError::Identity(e) => write!(f, "identity: {e}"),
            StartupError::Genesis(e) => write!(f, "genesis: {e:?}"),
            StartupError::Protocol(e) => write!(f, "protocol: {e}"),
        }
    }
}

impl core::error::Error for StartupError {}

/// This node's own durable journal, as durable initialization sees it.
///
/// `coord-membership` needs to know whether the node's journal exists,
/// and to create it on a first boot, but it must not depend on a journal
/// implementation to ask. The daemon supplies the real one; a test
/// supplies a stand-in.
pub trait NodeJournal {
    /// Whether the node's own journal exists and is intact.
    fn intact(&self) -> Result<bool, StoreFailure>;
    /// Create it (first boot only).
    fn establish(&mut self) -> Result<(), StoreFailure>;
}

/// The production [`GenesisStore`]: the pinned manifest digest lives in
/// the store's `meta_v1` collection, and journal presence is the node's
/// own journal.
///
/// The two are deliberately separate facts. A node that pinned a digest
/// and then lost its journal is not a fresh node -- it is a returning
/// voter with no history, the state that must quarantine rather than
/// rejoin, so a pin alone is never read as "initialized".
pub struct StoreGenesis<'a, E: LocalEngine, J: NodeJournal> {
    engine: &'a mut E,
    journal: &'a mut J,
}

impl<'a, E: LocalEngine, J: NodeJournal> StoreGenesis<'a, E, J> {
    /// A genesis store over `engine` and this node's `journal`.
    pub fn new(engine: &'a mut E, journal: &'a mut J) -> Self {
        StoreGenesis { engine, journal }
    }
}

impl<E: LocalEngine, J: NodeJournal> GenesisStore for StoreGenesis<'_, E, J> {
    fn pinned_digest(&self) -> Result<Option<Digest32>, StoreFailure> {
        let view = self.engine.reader().snapshot().map_err(|_| StoreFailure)?;
        let Some(bytes) = view
            .get(Collection::MetaV1.id(), meta_fields::GENESIS_DIGEST)
            .map_err(|_| StoreFailure)?
        else {
            return Ok(None);
        };
        // A short or long record is not "no digest": it is a damaged one,
        // and reading it as absent would re-pin whatever manifest the
        // node was handed next.
        let exact: [u8; 32] = bytes.as_slice().try_into().map_err(|_| StoreFailure)?;
        Ok(Some(Digest32(exact)))
    }

    fn pin(&mut self, digest: Digest32) -> Result<(), StoreFailure> {
        let mut txn = self.engine.begin_write().map_err(|_| StoreFailure)?;
        txn.put(
            Collection::MetaV1.id(),
            meta_fields::GENESIS_DIGEST,
            &digest.0,
        )
        .map_err(|_| StoreFailure)?;
        // The pin is the node's identity, so it is synced here rather
        // than left to a later batch: a crash just after it must not come
        // back unpinned and free to adopt a different manifest.
        txn.commit_durable().map_err(|_| StoreFailure)
    }

    fn journal_intact(&self) -> Result<bool, StoreFailure> {
        self.journal.intact()
    }

    fn establish_journal(&mut self) -> Result<(), StoreFailure> {
        self.journal.establish()
    }
}

/// The startup sequence of one process.
#[derive(Clone, Copy, Debug, Default)]
pub struct Startup {
    reached: StartupPhase,
}

impl Startup {
    /// A process at `Boot`.
    pub const fn new() -> Self {
        Startup {
            reached: StartupPhase::Boot,
        }
    }

    /// How far startup has got.
    pub const fn reached(&self) -> StartupPhase {
        self.reached
    }

    /// Whether every step completed, so the process may serve.
    pub const fn recovered(&self) -> bool {
        matches!(self.reached, StartupPhase::ProtocolRecovered)
    }

    fn enter(&mut self, step: StartupPhase) -> Result<(), StartupError> {
        if step.predecessor() != Some(self.reached) {
            return Err(StartupError::OutOfOrder {
                attempted: step,
                reached: self.reached,
            });
        }
        self.reached = step;
        Ok(())
    }

    /// Durable storage opened and carries this node's identity. The
    /// engine adapter has already refused a mismatched, uninitialized or
    /// rolled-back root, so the caller's `?` is the verdict and this
    /// records it.
    pub fn storage_validated(&mut self) -> Result<(), StartupError> {
        self.enter(StartupPhase::StorageValidated)
    }

    /// This node's credentials and trust anchors loaded.
    ///
    /// They are established against the storage this node has proved is
    /// its own, so a node that could not open its store never gets as far
    /// as presenting an identity.
    pub fn identity_validated(&mut self) -> Result<(), StartupError> {
        self.enter(StartupPhase::IdentityValidated)
    }

    /// Pin or match the genesis manifest and produce the committed
    /// membership.
    pub fn check_membership(
        &mut self,
        manifest: &GenesisManifest,
        store: &mut dyn GenesisStore,
    ) -> Result<Initialized, StartupError> {
        // The ordering check comes first: initializing would write a pin,
        // and a pin must never be the side effect of a step taken out of
        // order.
        if StartupPhase::MembershipChecked.predecessor() != Some(self.reached) {
            return Err(StartupError::OutOfOrder {
                attempted: StartupPhase::MembershipChecked,
                reached: self.reached,
            });
        }
        let initialized = initialize(manifest, store).map_err(StartupError::Genesis)?;
        self.enter(StartupPhase::MembershipChecked)?;
        Ok(initialized)
    }

    /// Durable protocol state was recovered.
    pub fn protocol_recovered(&mut self) -> Result<(), StartupError> {
        self.enter(StartupPhase::ProtocolRecovered)
    }
}

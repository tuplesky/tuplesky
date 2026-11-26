//! Durable initialization (design Sections 10.2, 17.1): a first boot
//! pins the manifest digest and the committed configuration; a later
//! boot requires the stored digest to match. Missing, empty or
//! rolled-back durable state is quarantined and requires a new
//! generation through the handoff lifecycle, never a silent
//! create-or-open of an empty voter.

use coord_types::identity::Digest32;
use coord_types::ids::StoreInstanceId;

use crate::genesis::GenesisManifest;
use crate::membership::{Membership, MembershipError};

/// The durable state the initializer reads and writes.
pub trait GenesisStore {
    /// The pinned genesis digest, if this node was ever initialized.
    fn pinned_digest(&self) -> Result<Option<Digest32>, StoreFailure>;
    /// This store's identity, written with the pin (task-d121).
    fn store_instance(&self) -> Result<Option<StoreInstanceId>, StoreFailure>;
    /// Pin the digest and write the store's identity, in one durable
    /// transaction (first boot only).
    fn pin(&mut self, digest: Digest32, store: StoreInstanceId) -> Result<(), StoreFailure>;
    /// Whether the node's own durable journal exists and is intact.
    fn journal_intact(&self) -> Result<bool, StoreFailure>;
    /// Create the node's durable journal (first boot). It must be
    /// established before the manifest is pinned: a node that pinned
    /// first and crashed came back with a pinned digest and no journal,
    /// and its own returning-node check then quarantined it although it
    /// had never run.
    fn establish_journal(&mut self) -> Result<(), StoreFailure>;
}

/// The store could not be read or written.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreFailure;

/// Why initialization did not produce a running membership.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InitError {
    /// The manifest does not parse into a membership.
    Membership(MembershipError),
    /// The durable state is unreadable.
    Store,
    /// A node initialized under one manifest was handed another: the
    /// digest does not match. Quarantine; do not reinitialize.
    DigestMismatch {
        /// The digest the node was initialized under.
        pinned: Digest32,
        /// The digest offered now.
        offered: Digest32,
    },
    /// The node was initialized but its journal is missing or corrupt:
    /// quarantine and rejoin through learner admission and handoff, never
    /// resurrect an empty voter here.
    JournalLost,
}

/// The outcome of a successful initialization.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Initialized {
    /// The committed membership.
    pub membership: Membership,
    /// Whether this boot performed the first-time pin.
    pub first_boot: bool,
    /// This store's identity: the one pinned now on a first boot, the one
    /// read back otherwise (task-d121).
    pub store_instance: StoreInstanceId,
}

/// Initialize (or re-open) this node against `manifest` and `store`.
///
/// `fresh` draws the store identity a first boot pins, from a secure
/// random source the caller holds (core crates draw no randomness). Only
/// a first boot calls it: a returning boot, and a backup of a pinned
/// store, read the pinned one back and draw nothing.
pub fn initialize(
    manifest: &GenesisManifest,
    store: &mut dyn GenesisStore,
    fresh: impl FnOnce() -> Result<StoreInstanceId, InitError>,
) -> Result<Initialized, InitError> {
    let membership = Membership::from_genesis(manifest).map_err(InitError::Membership)?;
    let digest = manifest.digest();
    match store.pinned_digest().map_err(|_| InitError::Store)? {
        None => {
            // First boot: establish the journal, then pin the manifest.
            // No trust-on-first-use of a peer; the manifest itself came
            // through deployment trust. The order matters: pinning
            // first and crashing left a pinned digest with no journal,
            // which the returning-node path reads as a lost journal.
            let store_instance = fresh()?;
            store.establish_journal().map_err(|_| InitError::Store)?;
            store
                .pin(digest, store_instance)
                .map_err(|_| InitError::Store)?;
            Ok(Initialized {
                membership,
                first_boot: true,
                store_instance,
            })
        }
        Some(pinned) if pinned == digest => {
            // A returning node: its own journal must be intact. A missing
            // or rolled-back journal is quarantined.
            if !store.journal_intact().map_err(|_| InitError::Store)? {
                return Err(InitError::JournalLost);
            }
            // The identity is written with the pin, so a pinned store
            // without one is damaged, not one to give a new identity:
            // a voter that took a new identity here would be refused by
            // every peer that saw the old one, and one that kept none
            // could not be told from a store initialized again.
            let store_instance = store
                .store_instance()
                .map_err(|_| InitError::Store)?
                .ok_or(InitError::Store)?;
            Ok(Initialized {
                membership,
                first_boot: false,
                store_instance,
            })
        }
        Some(pinned) => Err(InitError::DigestMismatch {
            pinned,
            offered: digest,
        }),
    }
}

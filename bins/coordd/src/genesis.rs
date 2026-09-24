//! The genesis manifest this node was initialized under, pinned in its
//! own store (design Sections 10.2, 17.1, 22.1).
//!
//! The manifest is re-read from its file on every start, and a file can
//! be edited. Without a pin, a node initialized under one set of voters
//! and one policy would start under whatever the file said next, as long
//! as the cluster and domain were unchanged -- and the committed
//! configuration it votes by would be the operator's latest edit rather
//! than the one every replica agreed on. So `coordd init` pins the
//! manifest's digest in the store it creates, and every later start runs
//! the startup sequence through `check_membership` against that pin: the
//! same manifest is this node's, any other is a genesis quarantine.
//!
//! A start never pins. A store with no pin is one whose initialization
//! did not finish -- `coordd init` created the generation and stopped
//! before the pin was durable -- and serving it would pin whatever
//! manifest it was handed then, which is trust on first use by another
//! name. It is refused, and `coordd init` finishes it: a generation that
//! was never pinned has never been served, so it holds no history to
//! lose.

use coord_daemon::{NodeJournal, Startup, StartupError, StoreGenesis};
use coord_membership::genesis::GenesisManifest;
use coord_membership::init::{GenesisStore, InitError, StoreFailure};
use coord_storage_redb::lifecycle::Generation;

/// What the pin is being checked for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Intent {
    /// `coordd init`: the store was just created (or its creation is
    /// being finished), so the manifest is pinned.
    Initialize,
    /// A start: the pin must already be there, and must match.
    Serve,
}

/// Why the node is not running under the manifest it was handed.
#[derive(Debug)]
pub enum GenesisError {
    /// The store was created but its initialization never pinned a
    /// manifest.
    NeverPinned,
    /// A step of the startup sequence refused.
    Startup(StartupError),
}

impl GenesisError {
    /// How the process quarantines for this.
    pub const fn quarantine_reason(&self) -> coord_daemon::QuarantineReason {
        match self {
            GenesisError::NeverPinned => coord_daemon::QuarantineReason::Genesis,
            GenesisError::Startup(e) => e.quarantine_reason(),
        }
    }
}

impl core::fmt::Display for GenesisError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            GenesisError::NeverPinned => write!(
                f,
                "genesis quarantine: this store was never pinned to a genesis \
                 manifest, so its initialization did not finish. Run \
                 `coordd init` to finish it"
            ),
            GenesisError::Startup(StartupError::Genesis(InitError::DigestMismatch {
                pinned,
                offered,
            })) => write!(
                f,
                "genesis quarantine: this node was initialized under genesis {} \
                 and was handed genesis {}. A node's genesis is fixed when it \
                 is initialized; restore the manifest it was initialized under",
                short(&pinned.0),
                short(&offered.0)
            ),
            GenesisError::Startup(e) => write!(f, "{:?} quarantine: {e}", e.quarantine_reason()),
        }
    }
}

impl core::error::Error for GenesisError {}

/// The node's durable history, as durable initialization asks after it.
///
/// On this build a node's history is its store generation and nothing
/// else: there is no separate journal yet. By the time the pin is
/// checked, `store::open` has already found the generation and matched
/// it to this node's identity (and created it, for `coordd init`), so
/// the history exists and establishing it is already done.
struct GenerationHistory;

impl NodeJournal for GenerationHistory {
    fn intact(&self) -> Result<bool, StoreFailure> {
        Ok(true)
    }
    fn establish(&mut self) -> Result<(), StoreFailure> {
        Ok(())
    }
}

/// Whether `generation` was created by a `coordd init` that stopped
/// before it pinned a manifest. `None` when the store cannot say.
pub fn unfinished(generation: &mut Generation) -> Option<bool> {
    let mut history = GenerationHistory;
    let store = StoreGenesis::new(generation.engine(), &mut history);
    store.pinned_digest().ok().map(|pin| pin.is_none())
}

/// Run the startup sequence over `generation` through the membership
/// check: pin `manifest` for [`Intent::Initialize`], require and match
/// the pin for [`Intent::Serve`].
pub fn check(
    generation: &mut Generation,
    manifest: &GenesisManifest,
    intent: Intent,
) -> Result<Startup, GenesisError> {
    let mut history = GenerationHistory;
    let mut store = StoreGenesis::new(generation.engine(), &mut history);
    if intent == Intent::Serve {
        let pin = store
            .pinned_digest()
            .map_err(|_| GenesisError::Startup(StartupError::Genesis(InitError::Store)))?;
        if pin.is_none() {
            return Err(GenesisError::NeverPinned);
        }
    }
    let mut startup = Startup::new();
    // `store::open` has matched the generation to this node's identity,
    // and the caller has verified the credentials that identity came
    // from, before this is reached.
    startup.storage_validated().map_err(GenesisError::Startup)?;
    startup
        .identity_validated()
        .map_err(GenesisError::Startup)?;
    startup
        .check_membership(manifest, &mut store)
        .map_err(GenesisError::Startup)?;
    Ok(startup)
}

fn short(bytes: &[u8]) -> String {
    bytes[..4].iter().map(|b| format!("{b:02x}")).collect()
}

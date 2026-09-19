//! Opening this node's durable projection (design Sections 19.3, 22.1).
//!
//! Two things happen to a store and they are deliberately not the same
//! command. Initialization creates generation 1 under an empty root and
//! is a decision an operator takes once; serving opens the generation
//! the root already selects and never creates anything.
//!
//! Keeping them apart is the point. A daemon that created a store when
//! it did not find one would turn a lost disk, an unmounted volume or a
//! mistyped path into a fresh, empty, *valid* node -- which would then
//! vote, and vote for whatever it was asked, having forgotten everything
//! it had ever promised. Every other guard in the system is downstream
//! of that one, so nothing here infers intent: `coordd init` creates,
//! `coordd` serves, and each refuses the other's situation.

use std::path::Path;

use coord_daemon::Config;
use coord_storage_redb::lifecycle::{Generation, OpenError, OpenOptions, StoreIdentity};
use coord_types::ids::{ClusterId, DomainId, ReplicaId, ReplicaIncarnation};

/// What this node's projection is being opened for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Intent {
    /// Create generation 1. Refused if the root already holds one.
    Initialize,
    /// Open what the root selects. Refused if there is nothing to open.
    Serve,
}

/// Why the projection could not be opened.
#[derive(Debug)]
pub enum StoreError {
    /// There is no store to serve. Named separately from every other
    /// failure because it is the one an operator is most tempted to fix
    /// by initializing, and the one where that is most often wrong.
    NotInitialized {
        /// Where this node looked.
        root: String,
    },
    /// There is already a store here, so initializing would replace a
    /// node's identity and history with an empty one.
    AlreadyInitialized {
        /// Where it is.
        root: String,
    },
    /// The store is real and could not be opened.
    Refused {
        /// Where it is.
        root: String,
        /// Why, from the lifecycle.
        reason: String,
    },
}

impl core::fmt::Display for StoreError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            StoreError::NotInitialized { root } => write!(
                f,
                "no store at {root}: a node serves the store it already has. \
                 If this node is genuinely new, initialize it deliberately \
                 with `coordd init`; if it is not, the store it had is what \
                 needs finding"
            ),
            StoreError::AlreadyInitialized { root } => write!(
                f,
                "a store already exists at {root}: initializing would give \
                 this node an empty history under its own identity"
            ),
            StoreError::Refused { root, reason } => {
                write!(f, "cannot open the store at {root}: {reason}")
            }
        }
    }
}

impl core::error::Error for StoreError {}

/// Open the projection `config` names, for `intent`.
pub fn open(
    config: &Config,
    intent: Intent,
    cluster: ClusterId,
    domain: DomainId,
    replica: ReplicaId,
    incarnation: ReplicaIncarnation,
) -> Result<Generation, StoreError> {
    let root = config.state.root_path(&config.state_directory);
    let identity = StoreIdentity {
        cluster_id: cluster,
        domain_id: domain,
        replica_id: replica,
        incarnation,
    };
    let options = OpenOptions {
        cache_bytes: config.state.cache_bytes,
    };
    match intent {
        Intent::Initialize => {
            // The parent has to exist; the root itself is the lifecycle's
            // to create, because creating it here would make an
            // interrupted initialization indistinguishable from a
            // complete one.
            if let Some(parent) = root.parent() {
                std::fs::create_dir_all(parent).map_err(|e| StoreError::Refused {
                    root: show(&root),
                    reason: e.to_string(),
                })?;
            }
            Generation::create(&root, identity, options).map_err(|e| match e {
                OpenError::AlreadyInitialized => {
                    StoreError::AlreadyInitialized { root: show(&root) }
                }
                other => StoreError::Refused {
                    root: show(&root),
                    reason: format!("{other:?}"),
                },
            })
        }
        Intent::Serve => Generation::open_existing(&root, identity, options).map_err(|e| match e {
            OpenError::NotInitialized | OpenError::MissingGeneration(_) => {
                StoreError::NotInitialized { root: show(&root) }
            }
            other => StoreError::Refused {
                root: show(&root),
                reason: format!("{other:?}"),
            },
        }),
    }
}

fn show(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

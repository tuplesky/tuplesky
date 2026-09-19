//! Opening this node's durable storage (design Sections 17.3, 19.3,
//! 22.1).
//!
//! The serving profile is `journaled-strict-v1`: the shared journal
//! records the authoritative transition and the projection materializes
//! it afterwards, in journal order. Both are opened here, and the
//! journal is opened first -- it is the authority, and a projection
//! without the journal that owns it is a set of rows nothing vouches
//! for.
//!
//! Two things happen to durable storage and they are deliberately not
//! the same command. Initialization creates generation 1 under an empty root and
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

use std::path::{Path, PathBuf};

use coord_core::effect::BootId;
use coord_daemon::Config;
use coord_journal_api::stream::ShardId;
use coord_journal_raft_engine::journal::{
    JournalIdentity, JournalOptions, OpenError as JournalOpenError, RaftEngineJournal,
};
use coord_storage::JournaledDomain;
use coord_storage::journaled::{JournalLimits, JournaledStore};
use coord_storage_redb::RedbEngine;
use coord_storage_redb::lifecycle::{Generation, OpenError, OpenOptions, StoreIdentity};
use coord_types::ids::{Ballot, ClusterId, DomainId, ReplicaId, ReplicaIncarnation};

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

/// This node's durable storage, open and ready to attach an applier to.
pub struct Storage {
    /// The journal-first coordinator: one shared journal, this node's
    /// domain attached to it.
    pub domain: JournaledDomain<RaftEngineJournal, RedbEngine>,
    /// Where the projection's generation lives, for diagnostics.
    pub generation: PathBuf,
}

/// Open the journal and the projection, and attach the domain.
///
/// The order is the durability order. The journal is opened first
/// because it is the authority; the projection is attached to it, which
/// is where the two frontiers are checked against each other and the
/// journal's suffix is replayed into the projection. Nothing is served
/// until that has happened: a projection that has not caught up with the
/// journal has forgotten transitions this node already acknowledged.
pub fn open_storage(
    config: &Config,
    intent: Intent,
    cluster: ClusterId,
    domain_id: DomainId,
    replica: ReplicaId,
    incarnation: ReplicaIncarnation,
) -> Result<Storage, StoreError> {
    let journal_root = root_path(&config.state_directory, &config.journal.root);
    let identity = JournalIdentity { cluster, replica };
    let options = JournalOptions::default();

    let journal = match intent {
        Intent::Initialize => {
            if let Some(parent) = journal_root.parent() {
                std::fs::create_dir_all(parent).map_err(|e| StoreError::Refused {
                    root: show(&journal_root),
                    reason: e.to_string(),
                })?;
            }
            RaftEngineJournal::create(&journal_root, identity, &options)
        }
        Intent::Serve => RaftEngineJournal::open_existing(&journal_root, identity, &options),
    }
    .map_err(|e| match e {
        JournalOpenError::AlreadyInitialized => StoreError::AlreadyInitialized {
            root: show(&journal_root),
        },
        JournalOpenError::NotInitialized => StoreError::NotInitialized {
            root: show(&journal_root),
        },
        other => StoreError::Refused {
            root: show(&journal_root),
            reason: format!("{other:?}"),
        },
    })?;

    let generation = open(config, intent, cluster, domain_id, replica, incarnation)?;
    let directory = generation.directory().to_path_buf();
    let (engine, _lock, _manifest) = generation.into_parts();

    let mut store = JournaledStore::open(
        journal,
        cluster,
        replica,
        incarnation,
        BootId(boot_of(replica, incarnation)),
        JournalLimits::default(),
    )
    .map_err(|e| StoreError::Refused {
        root: show(&journal_root),
        reason: format!("{e:?}"),
    })?;
    // Shard 0 for the single-domain preview; task-j07 is where a node
    // spreads domains over a shard set.
    let shard = ShardId::new(0).expect("shard zero");
    store
        .attach(domain_id, shard, engine)
        .map_err(|e| StoreError::Refused {
            root: show(&directory),
            reason: format!("the projection could not be attached to the journal: {e:?}"),
        })?;

    // A transition's ballot is in the epoch of the configuration its base
    // names; the replica's own ballot replaces this as soon as its
    // machine has one.
    let base = store.application_base(domain_id).expect("just attached");
    let ballot = Ballot {
        epoch: base.configuration,
        number: 0,
        leader: replica,
    };
    let domain = JournaledDomain::new(store, domain_id, ballot).expect("just attached");
    Ok(Storage {
        domain,
        generation: directory,
    })
}

/// A boot identity for this run.
///
/// It must differ between runs of the same node -- the boot fence rests
/// on that -- and be the same for every component within one run.
fn boot_of(replica: ReplicaId, incarnation: ReplicaIncarnation) -> [u8; 16] {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64);
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&nanos.to_be_bytes());
    out[8..12].copy_from_slice(&replica.0[..4]);
    out[12..].copy_from_slice(&(incarnation.get() as u32).to_be_bytes());
    out
}

fn root_path(state_directory: &str, root: &str) -> PathBuf {
    let path = Path::new(root);
    if path.is_absolute() {
        return path.to_path_buf();
    }
    Path::new(state_directory).join(path)
}

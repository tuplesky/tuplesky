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
use coord_storage_redb::lifecycle::{
    Generation, InactiveGeneration, OpenError, OpenOptions, StoreIdentity,
};
use coord_types::ids::{
    Ballot, ClusterId, DomainId, LocalJournalSeq, ReplicaId, ReplicaIncarnation,
};

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
) -> Result<(Generation, Option<ReplicaIncarnation>), StoreError> {
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
        Intent::Initialize => initialize(&root, identity, options, |_| Ok(())).map(|g| (g, None)),
        Intent::Serve => {
            // An authorized replacement keeps this node's durable state
            // (task-58). `place` has already established that this
            // certificate's generation is the *committed* voter's, so a
            // root stamped with an earlier one belongs to this same
            // replica before its key was replaced -- its journal,
            // checkpoints and epoch metadata are exactly what the new
            // generation comes back on. A root stamped *later* is the
            // fencing case and is refused, so a clone restored from
            // before a replacement still cannot serve.
            //
            // Nothing is adopted here. The generation is opened as it is
            // stamped, and the adoption is only reported as pending: the
            // genesis pin is checked on this generation before anything
            // durable moves (`Opened::attach` carries the stream and then
            // advances the manifest), so a manifest the node was not
            // initialized under is refused while the store is still
            // exactly as it was.
            let not_there = |e| match e {
                OpenError::NotInitialized | OpenError::MissingGeneration(_) => {
                    StoreError::NotInitialized { root: show(&root) }
                }
                other => StoreError::Refused {
                    root: show(&root),
                    reason: format!("{other:?}"),
                },
            };
            let pending = Generation::adoption_pending(&root, identity).map_err(not_there)?;
            let stamped = StoreIdentity {
                incarnation: pending.unwrap_or(incarnation),
                ..identity
            };
            let generation =
                Generation::open_existing(&root, stamped, options).map_err(not_there)?;
            Ok((generation, pending))
        }
    }
}

/// Create this node's first generation, filled by `fill`, and select it
/// only once `fill` has returned.
///
/// The generation is staged, not created selected: `CURRENT` is written
/// by the activation that follows a successful fill and by nothing
/// earlier. A fill that fails, or a process that dies while filling,
/// therefore leaves a root with no selection -- which every later start
/// reads as `NotInitialized` -- rather than a selected generation holding
/// part of what was being written. For `init` the fill is empty and this
/// is the same empty generation it always was; for a restore (task-59) the
/// fill is the whole backup, and the receipt it commits last is in the
/// generation before anything can select it.
fn initialize(
    root: &Path,
    identity: StoreIdentity,
    options: OpenOptions,
    fill: impl FnOnce(&mut RedbEngine) -> Result<(), StoreError>,
) -> Result<Generation, StoreError> {
    let refused = |e: &dyn core::fmt::Debug| StoreError::Refused {
        root: show(root),
        reason: format!("{e:?}"),
    };
    // The parent has to exist; the root itself is the lifecycle's to
    // create, because creating it here would make an interrupted
    // initialization indistinguishable from a complete one.
    if let Some(parent) = root.parent() {
        std::fs::create_dir_all(parent).map_err(|e| StoreError::Refused {
            root: show(root),
            reason: e.to_string(),
        })?;
    }
    // A selected generation is a node's history, whatever it holds.
    // Staging would validate it as a predecessor, which is the install
    // path's question; initialization's is only whether one is there.
    if root.join("CURRENT").exists() {
        return Err(StoreError::AlreadyInitialized { root: show(root) });
    }
    let mut staged = InactiveGeneration::stage(root, identity, options).map_err(|e| match e {
        OpenError::AlreadyInitialized => StoreError::AlreadyInitialized { root: show(root) },
        other => refused(&other),
    })?;
    if staged.previous().is_some() {
        // Selected between the check above and the lock: the same answer.
        staged.abandon().map_err(|e| refused(&e))?;
        return Err(StoreError::AlreadyInitialized { root: show(root) });
    }
    if let Err(e) = fill(staged.engine()) {
        // Nothing was selected, so the root is exactly as uninitialized
        // as before. Removing the staging is housekeeping, not safety: an
        // unselected directory is never opened, and the next staging
        // takes the next number, so the fill's own error is the one
        // reported even if the removal fails.
        let _ = staged.abandon();
        return Err(e);
    }
    let generation = staged.activate().map_err(|e| refused(&e))?;
    // A staging a crashed attempt left behind is unreferenced now that
    // this one is selected. Failing to reclaim it costs disk, not
    // correctness, and must not undo a selection that is already
    // durable, so it is reported and not returned.
    if let Err(e) = generation.prune_unselected() {
        eprintln!(
            "an abandoned staging under {} could not be removed: {e}",
            show(root)
        );
    }
    Ok(generation)
}

fn show(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

/// This node's persistence: the journal is the record, `redb` the
/// projection of it that answers reads.
pub type Persistence = JournaledDomain<RaftEngineJournal, RedbEngine>;

/// This node's durable storage, open and ready to attach an applier to.
pub struct Storage {
    /// The journal-first coordinator: one shared journal, this node's
    /// domain attached to it.
    pub domain: Persistence,
    /// Where this node's local recovery checkpoints are published.
    pub checkpoints: coord_checkpoint::LocalCheckpointStore,
    /// The baseline the journal selects, if one was ever published.
    ///
    /// Read and validated at startup rather than when it is needed: an
    /// image this node published and can no longer load is the loss of
    /// a durable prefix, and finding that out at the moment it is
    /// needed is finding it out too late.
    pub baseline: Option<coord_journal_api::CheckpointPointerV1>,
    /// Where the projection's generation lives, for diagnostics.
    pub generation: PathBuf,
    /// The boot this storage was opened under.
    ///
    /// Everything of this run that allocates a barrier has to use it:
    /// the boot fence refuses a batch of any other, so a component that
    /// invented its own would have every write refused.
    pub boot: BootId,
}

/// The journal and the projection, open and not yet attached.
///
/// The two steps are apart so that what has to be settled about the
/// projection before anything is replayed into it -- the genesis it is
/// pinned to (see `genesis`) -- is settled on the generation itself,
/// while it is still only this node's store and not yet the journal's
/// materialization.
pub struct Opened {
    /// The journal-first coordinator, open over the journal with nothing
    /// attached to it yet (and, for an authorized replacement, this
    /// node's stream already carried forward).
    store: JournaledStore<RaftEngineJournal, RedbEngine>,
    journal_root: PathBuf,
    /// The projection's generation, matched to this node's identity.
    pub generation: Generation,
    domain_id: DomainId,
    replica: ReplicaId,
    boot: BootId,
    /// Where this node's local recovery checkpoints are published.
    checkpoint_root: PathBuf,
    /// An authorized replacement's adoption, still to be made durable.
    adoption: Option<Adoption>,
}

/// An authorized replacement this start comes back on (task-58), found
/// by [`open_storage`] and carried out by [`Opened::attach`] -- after the
/// genesis pin has been checked on the generation as it is stamped.
struct Adoption {
    /// The generation the root is stamped with.
    previous: ReplicaIncarnation,
    /// The projection root.
    root: PathBuf,
    /// This node's identity at the committed generation.
    identity: StoreIdentity,
    options: OpenOptions,
}

/// Open the journal and the projection, for `intent`.
///
/// The order is the durability order. To serve, the journal is opened
/// first because it is the authority, and the projection is then opened
/// under this node's identity; to initialize, the projection is created
/// and selected first and the journal created after it (see
/// [`open_storage_with`]). Either way both are handed back unattached.
/// Attaching them is [`Opened::attach`].
pub fn open_storage(
    config: &Config,
    intent: Intent,
    cluster: ClusterId,
    domain_id: DomainId,
    replica: ReplicaId,
    incarnation: ReplicaIncarnation,
) -> Result<Opened, StoreError> {
    open_storage_with(
        config,
        intent,
        cluster,
        domain_id,
        replica,
        incarnation,
        |_| Ok(()),
    )
}

/// The same, running `before_attach` against the generation's engine
/// before it is attached to the journal.
///
/// That window is the install lifecycle's (task-50) and the restore's
/// (task-59): a whole store's worth of rows is written directly, which
/// is admissible precisely because nothing has attached the projection
/// to the journal yet and therefore no frontier exists to violate.
/// Every other write to a projection goes through the journal.
///
/// Under [`Intent::Initialize`] the engine is a staged generation that
/// nothing selects: it is selected only after `before_attach` returns
/// `Ok`, and the journal is created only after that. An error from
/// `before_attach`, or a crash while it runs, leaves no selected
/// generation and no journal, so the node refuses to serve and the same
/// initialization can simply be run again.
pub fn open_storage_with(
    config: &Config,
    intent: Intent,
    cluster: ClusterId,
    domain_id: DomainId,
    replica: ReplicaId,
    incarnation: ReplicaIncarnation,
    before_attach: impl FnOnce(&mut RedbEngine) -> Result<(), StoreError>,
) -> Result<Opened, StoreError> {
    let journal_root = root_path(&config.state_directory, &config.journal.root);
    let identity = JournalIdentity { cluster, replica };
    let options = JournalOptions::default();

    let journal_error = |e: JournalOpenError| match e {
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
    };

    let (generation, pending, journal) = match intent {
        Intent::Initialize => {
            // Refused before anything is written if the journal is already
            // there, so a restore is not carried out in full only to be
            // refused by its journal -- unless it is the journal an
            // initialization left before it created the projection, which
            // is reused (see `left_by_an_interrupted_initialization`).
            let occupied = std::fs::read_dir(&journal_root)
                .map(|mut entries| entries.next().is_some())
                .unwrap_or(false);
            let identity_of_store = StoreIdentity {
                cluster_id: cluster,
                domain_id,
                replica_id: replica,
                incarnation,
            };
            let reused = if occupied {
                Some(
                    left_by_an_interrupted_initialization(
                        config,
                        identity_of_store,
                        &journal_root,
                        identity,
                        &options,
                    )
                    .map_err(journal_error)?,
                )
            } else {
                None
            };
            let options_of_store = OpenOptions {
                cache_bytes: config.state.cache_bytes,
            };
            let generation = initialize(
                &config.state.root_path(&config.state_directory),
                identity_of_store,
                options_of_store,
                before_attach,
            )?;
            // The journal is created only once the generation is selected.
            // A journal is what makes a root servable and what a second
            // attempt is refused by, so creating it first would leave a
            // failed restore both unservable and unretryable; created
            // last, a failure anywhere before it leaves nothing at all.
            let journal = match reused {
                Some(journal) => journal,
                None => {
                    if let Some(parent) = journal_root.parent() {
                        std::fs::create_dir_all(parent).map_err(|e| StoreError::Refused {
                            root: show(&journal_root),
                            reason: e.to_string(),
                        })?;
                    }
                    RaftEngineJournal::create(&journal_root, identity, &options)
                        .map_err(journal_error)?
                }
            };
            (generation, None, journal)
        }
        Intent::Serve => {
            let journal = RaftEngineJournal::open_existing(&journal_root, identity, &options)
                .map_err(journal_error)?;
            let (mut generation, pending) =
                open(config, intent, cluster, domain_id, replica, incarnation)?;
            before_attach(generation.engine())?;
            (generation, pending, journal)
        }
    };

    let boot = BootId(boot_of(replica, incarnation));
    let store = JournaledStore::open(
        journal,
        cluster,
        replica,
        incarnation,
        boot,
        JournalLimits::default(),
    )
    .map_err(|e| StoreError::Refused {
        root: show(&journal_root),
        reason: format!("{e:?}"),
    })?;

    let checkpoint_root = root_path(&config.state_directory, &config.state.checkpoints);
    let adoption = pending.map(|previous| Adoption {
        previous,
        root: config.state.root_path(&config.state_directory),
        identity: StoreIdentity {
            cluster_id: cluster,
            domain_id,
            replica_id: replica,
            incarnation,
        },
        options: OpenOptions {
            cache_bytes: config.state.cache_bytes,
        },
    });
    Ok(Opened {
        store,
        journal_root,
        generation,
        domain_id,
        replica,
        boot,
        checkpoint_root,
        adoption,
    })
}

impl Opened {
    /// Attach the projection to the journal.
    ///
    /// This is where the two frontiers are checked against each other
    /// and the journal's suffix is replayed into the projection. Nothing
    /// is served until that has happened: a projection that has not
    /// caught up with the journal has forgotten transitions this node
    /// already acknowledged.
    pub fn attach(self) -> Result<Storage, StoreError> {
        self.attach_between(|| Ok(()))
    }

    /// [`Opened::attach`], running `between` after an adoption's first
    /// durable write and before its second -- the one point where a stop
    /// decides whether the next start can finish it.
    fn attach_between(
        self,
        between: impl FnOnce() -> Result<(), StoreError>,
    ) -> Result<Storage, StoreError> {
        let Opened {
            mut store,
            journal_root,
            generation,
            domain_id,
            replica,
            boot,
            checkpoint_root,
            adoption,
        } = self;
        let mut generation = generation;
        // The rollback guard (task-60). A build started against a cluster
        // that has activated something it cannot do refuses here, before
        // anything is adopted or the domain is attached, and long before
        // it could vote, and names the feature: the answer an operator
        // needs is "this node is too old for this cluster", not a
        // puzzling failure three steps later. A store with no activation
        // admits every build, which is what lets compatible binaries
        // coexist before activation.
        {
            use coord_store_api::engine::{LocalEngine, SnapshotSource};
            let directory = generation.directory().to_path_buf();
            let view =
                generation
                    .engine()
                    .reader()
                    .snapshot()
                    .map_err(|e| StoreError::Refused {
                        root: show(&directory),
                        reason: format!("the activation record could not be read: {e}"),
                    })?;
            coord_checkpoint::feature::admit(&view).map_err(|e| StoreError::Refused {
                root: show(&directory),
                reason: format!("{e}"),
            })?;
        }
        let generation = match adoption {
            None => generation,
            Some(adoption) => adopt(
                &mut store,
                &journal_root,
                domain_id,
                generation,
                adoption,
                between,
            )?,
        };
        let directory = generation.directory().to_path_buf();
        let (engine, _lock, _manifest) = generation.into_parts();

        // The local checkpoint directory, and what the journal says the
        // baseline is. Both are read before the domain is attached: the
        // baseline is a fact of the stream, not of the projection, and an
        // image that no longer loads has to stop the node here rather than
        // when a recovery needs it.
        let checkpoints =
            coord_checkpoint::LocalCheckpointStore::open(&checkpoint_root).map_err(|e| {
                StoreError::Refused {
                    root: show(&checkpoint_root),
                    reason: format!("the local checkpoint directory could not be opened: {e}"),
                }
            })?;
        let baseline = store
            .recovery_baseline(domain_id)
            .map_err(|e| StoreError::Refused {
                root: show(&journal_root),
                reason: format!("the local recovery baseline could not be read: {e}"),
            })?;
        if let Some(pointer) = &baseline {
            checkpoints.load(pointer).map_err(|e| StoreError::Refused {
                root: show(&checkpoint_root),
                reason: format!("{e}"),
            })?;
        }

        // Shard 0 for the single-domain preview; task-j07 is where a node
        // spreads domains over a shard set.
        let shard = ShardId::new(0).expect("shard zero");
        // The baseline goes in with the projection. `C` recovered at zero
        // would count the whole retained stream as unreclaimed, and the
        // housekeeping that watches that number would publish a full image
        // on every restart of a node that had once crossed its threshold.
        let represented = baseline.map_or(LocalJournalSeq::ZERO, |pointer| pointer.represented);
        store
            .attach_with_baseline(domain_id, shard, engine, represented)
            .map_err(|e| StoreError::Refused {
                root: show(&directory),
                reason: format!("the projection could not be attached to the journal: {e:?}"),
            })?;

        // A transition's ballot is in the epoch of the configuration its base
        // names. The number and leader here are only what a process that
        // never votes keeps: a voter hands the store its machine's ballot
        // when it is built and on every ballot it moves to (`Voter::new`,
        // `Voter::set_ballot`, through `Persistence::follow_ballot`), so
        // nothing a voter records is stamped with this one.
        let base = store.application_base(domain_id).expect("just attached");
        let ballot = Ballot {
            epoch: base.configuration,
            number: 0,
            leader: replica,
        };
        let domain = JournaledDomain::new(store, domain_id, ballot).expect("just attached");
        Ok(Storage {
            domain,
            checkpoints,
            baseline,
            generation: directory,
            boot,
        })
    }
}

/// Carry out an authorized replacement's adoption: the journal's stream
/// first, then the projection's manifest, then the generation reopened
/// under the committed identity.
///
/// The stream is carried *before* the projection's manifest is advanced,
/// and that order is the whole of crash safety here. The generation to
/// carry from is recorded only in the manifest, and advancing the
/// manifest overwrites it: stream-then-manifest leaves, after a crash
/// between them, a manifest still behind, so the next start reads the
/// same generation and finishes (carrying the stream again is a no-op
/// once it is this incarnation's). Manifest-then-stream left a manifest
/// that had moved and a stream that had not, which the next start could
/// not tell from a fresh node, and quarantined.
///
/// The stream is keyed by the incarnation that allocated it, so a
/// replaced node would otherwise attach to a fresh stream whose durable
/// head is zero while its projection is materialized far past that --
/// the shape of a lost prefix, which `attach` refuses. Its records each
/// carry the incarnation that wrote them, so nothing about provenance is
/// blurred by keeping the stream.
fn adopt(
    store: &mut JournaledStore<RaftEngineJournal, RedbEngine>,
    journal_root: &Path,
    domain_id: DomainId,
    generation: Generation,
    adoption: Adoption,
    between: impl FnOnce() -> Result<(), StoreError>,
) -> Result<Generation, StoreError> {
    let Adoption {
        previous,
        root,
        identity,
        options,
    } = adoption;
    // The root lock is the generation's; the manifest cannot be advanced
    // while it is held.
    drop(generation);
    store
        .adopt_stream(domain_id, previous)
        .map_err(|e| StoreError::Refused {
            root: show(journal_root),
            reason: format!("this node's journal stream could not be carried forward: {e:?}"),
        })?;
    between()?;
    let refused = |e| match e {
        OpenError::NotInitialized | OpenError::MissingGeneration(_) => {
            StoreError::NotInitialized { root: show(&root) }
        }
        other => StoreError::Refused {
            root: show(&root),
            reason: format!("{other:?}"),
        },
    };
    if let Some(previous) = Generation::adopt(&root, identity).map_err(refused)? {
        // On stdout, with the rest of the startup report: an adoption is
        // a durable, one-way step and the operator who replaced the node
        // is the one who has to see it.
        println!(
            "adopted this node's durable state from incarnation {} under {}",
            previous.get(),
            identity.incarnation.get()
        );
    }
    Generation::open_existing(&root, identity, options).map_err(refused)
}

/// The journal an initialization created before it stopped, or
/// `AlreadyInitialized`.
///
/// Initialization creates the journal and then the projection, and the
/// two are separate directories, so no single write makes both exist.
/// One that failed or stopped in between -- the projection's parent
/// unwritable, the disk full, the process killed -- left a journal and no
/// projection. Refusing that as "a store already exists" while a start
/// refused it as "no store" would leave the node with no command that
/// works, and only deleting the journal by hand as a way out.
///
/// So the journal is reused, and only when both of these hold: there is
/// no projection generation at all, and the journal is this node's and
/// has never allocated a stream -- nothing was ever attached to it, so
/// nothing was ever recorded in it. A journal with any history is a real
/// store and stays refused.
fn left_by_an_interrupted_initialization(
    config: &Config,
    projection: StoreIdentity,
    journal_root: &Path,
    identity: JournalIdentity,
    options: &JournalOptions,
) -> Result<RaftEngineJournal, JournalOpenError> {
    use coord_journal_api::JournalEngine;

    // "No projection" is the lifecycle's own verdict -- no generation is
    // selected -- rather than a guess from the directory's contents. What
    // a partly created generation directory means is the lifecycle's to
    // decide when initialization creates it again.
    let root = config.state.root_path(&config.state_directory);
    if !matches!(
        Generation::open_existing(&root, projection, OpenOptions::default()),
        Err(OpenError::NotInitialized)
    ) {
        return Err(JournalOpenError::AlreadyInitialized);
    }
    let journal = RaftEngineJournal::open_existing(journal_root, identity, options)
        .map_err(|_| JournalOpenError::AlreadyInitialized)?;
    match journal.mappings() {
        Ok((high_water, mappings))
            if high_water == coord_journal_api::stream::StreamHighWater::NONE
                && mappings.is_empty() =>
        {
            Ok(journal)
        }
        _ => Err(JournalOpenError::AlreadyInitialized),
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use coord_store_api::engine::{LocalEngine, WriteTxn};
    use coord_store_api::registry::Collection;

    /// A configuration whose store lives under `dir`, with a genesis
    /// policy to record (so the journal holds something a fresh stream
    /// would be missing).
    fn config(dir: &Path) -> Config {
        Config::parse(&format!(
            r#"config_version = 2
role = "voter-frontend-observer"
cluster_manifest = "{root}/genesis.json"
genesis_admin_key = "{root}/genesis-admin.pem"
cluster_endpoints = "{root}/endpoints.bin"
domain = "control-plane-test"
state_directory = "{root}"

[listen]
api_quic = "127.0.0.1:0"
peer_quic = "127.0.0.1:0"

[capability]
writer_queue_bytes = 16777216
buffer_bytes_per_subscription = 8388608
max_live_subscriptions = 4096

[state]
root = "state"

[journal]
root = "journal"
shards = 1

[identity]
trust_bundle = "{root}/roots.pem"
node_certificate = "{root}/node.pem"
node_key = "{root}/node.key"
collector_certificate = "{root}/collector.pem"
collector_key = "{root}/collector.key"

[sts]
issuer = "https://sts.test"
resource = "control-plane-test"
jwks = "{root}/sts-jwks.json"
trust_rule = "7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c"

[[grant]]
principal = "0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a"
namespace = "5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e"
"#,
            root = dir.display()
        ))
        .expect("config")
    }

    fn workspace(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("coordd-store-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("workspace");
        dir
    }

    const CLUSTER: ClusterId = ClusterId([0xc1; 16]);
    const DOMAIN: DomainId = DomainId([0xd0; 16]);
    const REPLICA: ReplicaId = ReplicaId([1; 16]);

    fn incarnation(n: u64) -> ReplicaIncarnation {
        ReplicaIncarnation::new(n).expect("incarnation")
    }

    fn serve(config: &Config, at: u64) -> Result<Opened, StoreError> {
        open_storage(
            config,
            Intent::Serve,
            CLUSTER,
            DOMAIN,
            REPLICA,
            incarnation(at),
        )
    }

    /// A replacement that stops after the first of its two durable
    /// writes finishes on the next start, on the same journal.
    ///
    /// The stop is made between the two writes themselves, whichever
    /// order they are in: were the manifest advanced first, this stop
    /// would leave the root at the new generation with the stream still
    /// keyed to the old one, the next start would find nothing pending,
    /// allocate a fresh stream and refuse the projection as materialized
    /// past it. A replacement is not reachable through `coordd` while the
    /// genesis pin admits no change (see `genesis`), so this is where the
    /// order is held.
    #[test]
    fn an_adoption_stopped_between_its_two_writes_finishes_on_the_next_start() {
        let dir = workspace("adopt-between");
        let config = config(&dir);

        let recorded = {
            let opened = open_storage(
                &config,
                Intent::Initialize,
                CLUSTER,
                DOMAIN,
                REPLICA,
                incarnation(1),
            )
            .expect("init");
            let mut storage = opened.attach().expect("attach");
            crate::write_genesis_policy(&config, &mut storage, incarnation(1)).expect("policy");
            storage
                .domain
                .store()
                .frontiers(DOMAIN)
                .expect("frontiers")
                .durable()
        };
        assert!(recorded.get() > 0, "the journal recorded nothing");

        let opened = serve(&config, 2).expect("open at the next generation");
        assert!(opened.adoption.is_some(), "no adoption was pending");
        let stopped = opened.attach_between(|| {
            Err(StoreError::Refused {
                root: "test".into(),
                reason: "stopped between the two writes".into(),
            })
        });
        assert!(
            matches!(&stopped, Err(StoreError::Refused { reason, .. }) if reason == "stopped between the two writes"),
            "the start did not stop where this test stops it: {:?}",
            stopped.err()
        );

        let storage = serve(&config, 2)
            .expect("reopen at the next generation")
            .attach()
            .expect("the next start did not finish the adoption");
        let durable = storage
            .domain
            .store()
            .frontiers(DOMAIN)
            .expect("frontiers")
            .durable();
        assert!(
            durable >= recorded,
            "the adoption came back on a stream without what was recorded: {durable:?} < {recorded:?}"
        );
        drop(storage);

        // And it is finished: the root has moved to the new generation, so
        // the credential it replaced is fenced.
        assert!(
            serve(&config, 1).is_err(),
            "the replaced generation could still open the store"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A fill that fails after it has durably committed rows -- a restore
    /// that dies between two of its bounded transactions -- selects
    /// nothing and creates no journal: the node refuses to serve the
    /// partial generation, and the same initialization then succeeds.
    #[test]
    fn a_fill_that_fails_after_committing_rows_selects_nothing() {
        let dir = workspace("partial-fill");
        let config = config(&dir);
        let failed = open_storage_with(
            &config,
            Intent::Initialize,
            CLUSTER,
            DOMAIN,
            REPLICA,
            incarnation(1),
            |engine| {
                let mut txn = engine.begin_write().expect("write");
                txn.put(Collection::KvCurrentV1.id(), b"partial", b"row")
                    .expect("put");
                txn.commit_durable().expect("commit");
                Err(StoreError::Refused {
                    root: "fill".into(),
                    reason: "the restore stopped half way".into(),
                })
            },
        );
        match failed {
            Err(StoreError::Refused { reason, .. }) => {
                assert_eq!(reason, "the restore stopped half way");
            }
            Err(other) => panic!("the fill's own error was not reported: {other}"),
            Ok(_) => panic!("a failed fill initialized a node"),
        }
        assert!(
            !dir.join("state").join("CURRENT").exists(),
            "a partially filled generation was selected"
        );
        assert!(
            std::fs::read_dir(dir.join("journal")).map_or(true, |mut e| e.next().is_none()),
            "a journal was created for a generation that was never selected"
        );
        match open_storage(
            &config,
            Intent::Serve,
            CLUSTER,
            DOMAIN,
            REPLICA,
            incarnation(1),
        ) {
            Err(StoreError::NotInitialized { .. }) => {}
            Err(other) => panic!("the partial generation was not read as absent: {other}"),
            Ok(_) => panic!("a node served a partially filled generation"),
        }

        let retried = open_storage(
            &config,
            Intent::Initialize,
            CLUSTER,
            DOMAIN,
            REPLICA,
            incarnation(1),
        );
        assert!(
            retried.is_ok(),
            "the same initialization was refused after a failed fill: {:?}",
            retried.err().map(|e| e.to_string())
        );
        drop(retried);
        match open_storage(
            &config,
            Intent::Serve,
            CLUSTER,
            DOMAIN,
            REPLICA,
            incarnation(1),
        ) {
            Ok(_) => {}
            Err(e) => panic!("the initialized node did not open: {e}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}

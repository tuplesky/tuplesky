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

use std::num::NonZeroU32;
use std::path::{Path, PathBuf};

use coord_core::effect::BootId;
use coord_daemon::Config;
use coord_journal_api::stream::ShardId;
use coord_journal_raft_engine::journal::{
    JournalIdentity, JournalOptions, OpenError as JournalOpenError, RaftEngineJournal,
};
use coord_storage::JournaledDomain;
use coord_storage::journaled::{
    DurableCadence, JournalLimits, JournaledError, JournaledStore, ProjectionProfile,
};
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

/// What a serving start found at the projection root.
enum Found {
    /// The selected generation, and the incarnation an adoption is
    /// pending from, if one is.
    Opened(Generation, Option<ReplicaIncarnation>),
    /// A selected generation whose database cannot be opened
    /// ([`OpenError::is_unreadable_database`]): under the replay-backed
    /// profile it is reinstalled from the local baseline
    /// ([`reinstall_unreadable`]), and otherwise refused with `error`.
    Unreadable {
        /// The identity the root is stamped with.
        stamped: StoreIdentity,
        /// As for [`Found::Opened`].
        pending: Option<ReplicaIncarnation>,
        /// Why the database did not open.
        error: OpenError,
    },
}

/// Open the generation `root` selects to serve as `identity`.
fn find_serving(
    root: &Path,
    identity: StoreIdentity,
    options: OpenOptions,
) -> Result<Found, StoreError> {
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
            StoreError::NotInitialized { root: show(root) }
        }
        other => StoreError::Refused {
            root: show(root),
            reason: format!("{other:?}"),
        },
    };
    let pending = Generation::adoption_pending(root, identity).map_err(not_there)?;
    let stamped = StoreIdentity {
        incarnation: pending.unwrap_or(identity.incarnation),
        ..identity
    };
    match Generation::open_existing(root, stamped, options) {
        Ok(generation) => Ok(Found::Opened(generation, pending)),
        Err(error) if error.is_unreadable_database() => Ok(Found::Unreadable {
            stamped,
            pending,
            error,
        }),
        Err(e) => Err(not_there(e)),
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
    /// The features this store's domain has activated, as `admit` read
    /// them at start (task-60): each one this build supports, or the
    /// store was refused.
    pub active: std::collections::BTreeSet<coord_types::formats::Feature>,
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
    /// How the projection's generation is opened, for a reinstall
    /// (task-j06).
    options: OpenOptions,
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

    let mut before_attach = Some(before_attach);
    let (found, journal) = match intent {
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
                before_attach.take().expect("not yet run"),
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
            (Found::Opened(generation, None), journal)
        }
        Intent::Serve => {
            let journal = RaftEngineJournal::open_existing(&journal_root, identity, &options)
                .map_err(journal_error)?;
            let found = find_serving(
                &config.state.root_path(&config.state_directory),
                StoreIdentity {
                    cluster_id: cluster,
                    domain_id,
                    replica_id: replica,
                    incarnation,
                },
                OpenOptions {
                    cache_bytes: config.state.cache_bytes,
                },
            )?;
            (found, journal)
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
    let mut store = store;
    // A publication's steps run on the domain thread, and its own CPU
    // and run-queue time over each say what the wall time was (task-d66).
    store.time_thread(crate::cpu::this_thread_time);
    // The replay-backed profile is set before the domain is attached, so
    // the attach's own replay already commits under it (task-j06).
    if config.journal.replays_projection() {
        let cadence = DurableCadence {
            commits: NonZeroU32::new(config.journal.projection_durable_commits)
                .expect("validated at parse"),
            records: NonZeroU32::new(config.journal.projection_durable_records)
                .expect("validated at parse"),
        };
        store
            .replay_projection(cadence)
            .map_err(|e| StoreError::Refused {
                root: show(&journal_root),
                reason: format!("{e}"),
            })?;
    }

    let checkpoint_root = root_path(&config.state_directory, &config.state.checkpoints);
    let (mut generation, pending) = match found {
        Found::Opened(generation, pending) => {
            // A staging a stopped reinstall left behind is unreferenced
            // once a start finds the selection opens. Reclaiming it costs
            // nothing a start reads, and failing to costs only disk.
            match generation.prune_abandoned_stagings() {
                Ok(removed) if !removed.is_empty() => {
                    eprintln!("removed abandoned stagings {removed:?}");
                }
                Ok(_) => {}
                Err(e) => eprintln!("abandoned stagings could not be removed: {e}"),
            }
            (generation, pending)
        }
        Found::Unreadable {
            stamped,
            pending,
            error,
        } => {
            let root = config.state.root_path(&config.state_directory);
            let mut staged = reinstall_unreadable(
                &store,
                &root,
                &checkpoint_root,
                domain_id,
                stamped,
                error,
                OpenOptions {
                    cache_bytes: config.state.cache_bytes,
                },
            )?;
            // Everything that can refuse the start runs on the staged
            // generation, and only then is it selected (#212 review): a
            // refusal leaves `CURRENT` where it was, naming the unreadable
            // generation, with the staging unreferenced.
            if let Some(before_attach) = before_attach.take() {
                before_attach(staged.engine())?;
            }
            let directory = staged.directory().to_path_buf();
            rollback_guard(staged.engine(), &directory)?;
            let discarded = staged.previous().map(|m| m.generation);
            let generation = staged.activate().map_err(|e| StoreError::Refused {
                root: show(&root),
                reason: format!("the reinstalled generation: {e}"),
            })?;
            prune_after_reinstall(&generation, discarded);
            (generation, pending)
        }
    };
    if let Some(before_attach) = before_attach.take() {
        before_attach(generation.engine())?;
    }
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
        options: OpenOptions {
            cache_bytes: config.state.cache_bytes,
        },
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
            options,
        } = self;
        let mut generation = generation;
        // The rollback guard (task-60). A build started against a cluster
        // that has activated something it cannot do refuses here, before
        // anything is adopted or the domain is attached, and long before
        // it could vote, and names the feature: the answer an operator
        // needs is "this node is too old for this cluster", not a
        // puzzling failure three steps later. A store with no activation
        // admits every build, which is what lets compatible binaries
        // coexist before activation. It runs again after the attach's
        // replay, below.
        {
            let directory = generation.directory().to_path_buf();
            rollback_guard(generation.engine(), &directory)?;
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
        let image = match &baseline {
            Some(pointer) => Some(checkpoints.load(pointer).map_err(|e| StoreError::Refused {
                root: show(&checkpoint_root),
                reason: format!("{e}"),
            })?),
            None => None,
        };
        let represented = baseline.map_or(LocalJournalSeq::ZERO, |pointer| pointer.represented);
        let mut generation = if store.projection_profile() == ProjectionProfile::Strict {
            refuse_if_invalid(&store, domain_id, generation, represented)?
        } else {
            reinstall_if_invalid(
                &store,
                domain_id,
                generation,
                baseline.as_ref().zip(image.as_ref()),
                represented,
                options,
            )?
        };
        let directory = generation.directory().to_path_buf();
        // The rollback guard over what the attach is about to replay
        // (#212 review). An activation is a journaled transition like any
        // other, and the replay applies its rows without asking whether
        // this build can do the feature: one journaled after the
        // projection's stamp -- a replay-backed projection behind the
        // journal, a strict one a crash cut before its commit, or a
        // reinstalled baseline's image -- is not in the projection the
        // first guard read. Refused here, before the replay writes a row
        // or the boot record is appended, at the record that activates it.
        {
            let refusal = store
                .scan_unapplied(domain_id, generation.engine(), |update| {
                    (update.collection == coord_store_api::registry::Collection::CheckpointV1.id()
                        && update.key == coord_checkpoint::feature::ACTIVE_KEY)
                        .then(|| match &update.value {
                            Some(bytes) => coord_checkpoint::feature::admit_encoded(bytes)
                                .err()
                                .map(|e| e.to_string()),
                            None => Some("the activation record is deleted".to_owned()),
                        })
                        .flatten()
                })
                .map_err(|e| StoreError::Refused {
                    root: show(&journal_root),
                    reason: format!("the journal's unapplied records could not be read: {e}"),
                })?;
            if let Some((seq, why)) = refusal {
                return Err(StoreError::Refused {
                    root: show(&directory),
                    reason: format!(
                        "{why}: activated by the journal's record {} that the projection has \
                         not applied; nothing was replayed",
                        seq.get()
                    ),
                });
            }
        }
        let (engine, _lock, _manifest) = generation.into_parts();

        // Shard 0 for the single-domain preview; task-j07 is where a node
        // spreads domains over a shard set.
        let shard = ShardId::new(0).expect("shard zero");
        // The baseline goes in with the projection. `C` recovered at zero
        // would count the whole retained stream as unreclaimed, and the
        // housekeeping that watches that number would publish a full image
        // on every restart of a node that had once crossed its threshold.
        store
            .attach_with_baseline(domain_id, shard, engine, represented)
            .map_err(|e| StoreError::Refused {
                root: show(&directory),
                reason: format!("the projection could not be attached to the journal: {e:?}"),
            })?;
        // The guard again, over what the attach's replay applied: the
        // features the store now has active, which the scan above has
        // already admitted. Nothing is served before this.
        let active = {
            let reader = store.reader(domain_id).expect("just attached");
            let view = reader.snapshot().map_err(|e| StoreError::Refused {
                root: show(&directory),
                reason: format!("the activation record could not be read after the replay: {e:?}"),
            })?;
            coord_checkpoint::feature::admit(view.view()).map_err(|e| StoreError::Refused {
                root: show(&directory),
                reason: format!("{e}"),
            })?
        };

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
            active,
        })
    }
}

/// The rollback guard (task-60) over an engine's latest snapshot: the
/// features its activation record names, or a refusal naming the ones this
/// build cannot do.
fn rollback_guard(
    engine: &mut RedbEngine,
    directory: &Path,
) -> Result<std::collections::BTreeSet<coord_types::formats::Feature>, StoreError> {
    use coord_store_api::engine::{LocalEngine, SnapshotSource};
    let view = engine
        .reader()
        .snapshot()
        .map_err(|e| StoreError::Refused {
            root: show(directory),
            reason: format!("the activation record could not be read: {e}"),
        })?;
    coord_checkpoint::feature::admit(&view).map_err(|e| StoreError::Refused {
        root: show(directory),
        reason: format!("{e}"),
    })
}

/// Under the strict profile, refuse a projection that does not continue
/// the journal before anything is attached (#211). Nothing rolls a strict
/// projection back, so one behind the published baseline, or whose stamp
/// names a record the journal does not hold where it says, is damage: to
/// the projection, or to a journal that opened shorter than the
/// projection's commit point. It is not reinstalled.
fn refuse_if_invalid(
    store: &JournaledStore<RaftEngineJournal, RedbEngine>,
    domain_id: DomainId,
    mut generation: Generation,
    represented: LocalJournalSeq,
) -> Result<Generation, StoreError> {
    let directory = generation.directory().to_path_buf();
    let refused = |reason: String| StoreError::Refused {
        root: show(&directory),
        reason,
    };
    match store.validate_projection(domain_id, generation.engine(), represented) {
        Ok(()) => Ok(generation),
        Err(JournaledError::ProjectionInvalid(why)) => Err(refused(format!(
            "the projection does not continue the journal ({why}); the strict profile \
             does not reinstall it"
        ))),
        Err(e) => Err(refused(format!(
            "the projection could not be checked against the journal: {e}"
        ))),
    }
}

/// Under the replay-backed profile (task-j06, Section 17.16.4), keep the
/// projection a boot found if it continues the journal, and otherwise
/// discard it and install the selected baseline's image in its place.
///
/// A crash rolls such a projection back to its last durable commit, and
/// every forced durable commit -- before a checkpoint pointer retires a
/// prefix -- is what keeps that commit at the baseline or past it, so the
/// ordinary case is a projection behind the journal that the attach
/// replays forward. One that is behind the baseline, or whose stamp is
/// not the record the journal holds at it, cannot be: the image is this
/// node's own state at the baseline, and the attach then replays
/// `(C, J]` onto it. Without a baseline there is nothing to install, and
/// the node refuses to serve rather than guess.
fn reinstall_if_invalid(
    store: &JournaledStore<RaftEngineJournal, RedbEngine>,
    domain_id: DomainId,
    mut generation: Generation,
    baseline: Option<(
        &coord_journal_api::CheckpointPointerV1,
        &coord_checkpoint::LocalCheckpointV1,
    )>,
    represented: LocalJournalSeq,
    options: OpenOptions,
) -> Result<Generation, StoreError> {
    let directory = generation.directory().to_path_buf();
    let refused = |reason: String| StoreError::Refused {
        root: show(&directory),
        reason,
    };
    let why = match store.validate_projection(domain_id, generation.engine(), represented) {
        Ok(()) => return Ok(generation),
        Err(JournaledError::ProjectionInvalid(why)) => why,
        Err(e) => {
            return Err(refused(format!(
                "the projection could not be checked against the journal: {e}"
            )));
        }
    };
    let Some((pointer, image)) = baseline else {
        return Err(refused(format!(
            "the projection does not continue the journal ({why}) and no local \
             checkpoint was published to reinstall it from"
        )));
    };
    eprintln!(
        "the projection does not continue the journal ({why}); reinstalling the local \
         checkpoint at {}",
        pointer.represented.get()
    );
    let manifest = generation.manifest();
    let identity = StoreIdentity {
        cluster_id: manifest.cluster_id,
        domain_id: manifest.domain_id,
        replica_id: manifest.replica_id,
        incarnation: manifest.incarnation,
    };
    let staged = generation
        .stage_reinstall(identity, options)
        .map_err(|e| refused(format!("a generation to reinstall into: {e}")))?;
    let mut staged = install_baseline(staged, pointer, image, refused)?;
    // The guard on what would be selected, before it is.
    let staged_directory = staged.directory().to_path_buf();
    rollback_guard(staged.engine(), &staged_directory)?;
    let discarded = staged.previous().map(|m| m.generation);
    let generation = staged
        .activate()
        .map_err(|e| refused(format!("the reinstalled generation: {e}")))?;
    prune_after_reinstall(&generation, discarded);
    Ok(generation)
}

/// Once a reinstall is selected, keep the generation it discarded, as
/// evidence of what was lost and why, and remove every earlier one, so
/// repeated reinstalls do not pile up (#212 review). A failure to prune
/// loses nothing and stops nothing; it is reported.
fn prune_after_reinstall(generation: &Generation, discarded: Option<u64>) {
    match generation.prune_unselected_except(discarded) {
        Ok(removed) if !removed.is_empty() => {
            eprintln!("removed earlier discarded generations {removed:?}");
        }
        Ok(_) => {}
        Err(e) => eprintln!("earlier discarded generations could not be removed: {e}"),
    }
}

/// Under the replay-backed profile (task-j05 step 3), a projection whose
/// database cannot be opened at all -- absent, empty, refused by redb or
/// failing its integrity check -- is the projection's loss, not the
/// node's: the journal and the image of the local baseline rebuild it, as
/// they rebuild one that does not continue the journal
/// ([`reinstall_if_invalid`]). A generation holding the baseline's image is
/// staged and filled, and returned unselected: the caller runs the genesis
/// pin and the rollback guard on it and activates it last, and the attach
/// then replays `(C, J]` onto it.
///
/// Under the strict profile the projection is the authority for what it
/// committed, so the file is refused as it always was; and without a
/// baseline there is nothing to install, so the node refuses to serve
/// rather than guess. A database whose identity record names another
/// node never reaches here: it is not unreadable, it is someone else's.
fn reinstall_unreadable(
    store: &JournaledStore<RaftEngineJournal, RedbEngine>,
    root: &Path,
    checkpoint_root: &Path,
    domain_id: DomainId,
    stamped: StoreIdentity,
    error: OpenError,
    options: OpenOptions,
) -> Result<InactiveGeneration, StoreError> {
    let refused = |reason: String| StoreError::Refused {
        root: show(root),
        reason,
    };
    if store.projection_profile() == ProjectionProfile::Strict {
        return Err(refused(format!("{error:?}")));
    }
    // The stream the root is stamped with: before an authorized
    // replacement's adoption, which `Opened::attach` carries out after
    // this, the node's pointers are still in its predecessor's stream.
    let baseline = store
        .recovery_baseline_of(domain_id, stamped.incarnation)
        .map_err(|e| {
            refused(format!(
                "the local recovery baseline could not be read: {e}"
            ))
        })?;
    let Some(pointer) = baseline else {
        return Err(refused(format!(
            "the projection could not be opened ({error}) and no local checkpoint was \
             published to reinstall it from"
        )));
    };
    let checkpoints =
        coord_checkpoint::LocalCheckpointStore::open(checkpoint_root).map_err(|e| {
            StoreError::Refused {
                root: show(checkpoint_root),
                reason: format!("the local checkpoint directory could not be opened: {e}"),
            }
        })?;
    let image = checkpoints
        .load(&pointer)
        .map_err(|e| StoreError::Refused {
            root: show(checkpoint_root),
            reason: format!("{e}"),
        })?;
    eprintln!(
        "the projection could not be opened ({error}); reinstalling the local checkpoint at {}",
        pointer.represented.get()
    );
    let staged = Generation::stage_reinstall_unreadable(root, stamped, options)
        .map_err(|e| refused(format!("a generation to reinstall into: {e}")))?;
    install_baseline(staged, &pointer, &image, refused)
}

/// Fill `staged` with the baseline's image and select it.
fn install_baseline(
    mut staged: InactiveGeneration,
    pointer: &coord_journal_api::CheckpointPointerV1,
    image: &coord_checkpoint::LocalCheckpointV1,
    refused: impl Fn(String) -> StoreError,
) -> Result<InactiveGeneration, StoreError> {
    coord_checkpoint::install_local(
        staged.engine(),
        image,
        &pointer.origin,
        &coord_checkpoint::local::InstallLocalLimits::default(),
    )
    .map_err(|e| {
        refused(format!(
            "the local checkpoint could not be installed: {e:?}"
        ))
    })?;
    Ok(staged)
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

pub(crate) fn root_path(state_directory: &str, root: &str) -> PathBuf {
    let path = Path::new(root);
    if path.is_absolute() {
        return path.to_path_buf();
    }
    Path::new(state_directory).join(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use coord_checkpoint::LocalBaseline;
    use coord_store_api::engine::{LocalEngine, SnapshotSource, WriteTxn};
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

    fn copy_tree(from: &Path, to: &Path) {
        std::fs::create_dir_all(to).expect("copy target");
        for entry in std::fs::read_dir(from).expect("copy source") {
            let entry = entry.expect("entry");
            let target = to.join(entry.file_name());
            if entry.file_type().expect("type").is_dir() {
                copy_tree(&entry.path(), &target);
            } else {
                std::fs::copy(entry.path(), target).expect("copy");
            }
        }
    }

    /// End two boots with a projection behind the baseline the second
    /// published: the first boot's projection is put back after the
    /// second boot's checkpoint retired the prefix it would replay.
    /// Returns the journal head the second boot reached and the baseline.
    /// A node initialized at incarnation 1 and served at `at` -- an
    /// adoption when `at` is later -- whose projection is put back behind
    /// the baseline a later boot published. Returns the journal's durable
    /// head and the baseline's represented sequence.
    fn a_projection_behind_its_baseline(config: &Config, dir: &Path, at: u64) -> (u64, u64) {
        let opened = open_storage(
            config,
            Intent::Initialize,
            CLUSTER,
            DOMAIN,
            REPLICA,
            incarnation(1),
        )
        .expect("init");
        let mut storage = opened.attach().expect("attach");
        crate::write_genesis_policy(config, &mut storage, incarnation(1)).expect("policy");
        drop(storage);
        if at > 1 {
            let opened = serve(config, at).expect("serve");
            assert!(opened.adoption.is_some(), "no adoption was pending");
            drop(opened.attach().expect("the adoption"));
        }
        copy_tree(&dir.join("state"), &dir.join("state-before"));

        // The next boot appends its lifecycle record, materializes it
        // and publishes a baseline that represents it.
        let mut storage = serve(config, at).expect("serve").attach().expect("attach");
        let published = storage
            .domain
            .publish_local(
                &storage.checkpoints,
                &coord_checkpoint::LocalLimits::default(),
            )
            .expect("publish")
            .expect("something to represent");
        let durable = storage
            .domain
            .store()
            .frontiers(DOMAIN)
            .expect("frontiers")
            .durable()
            .get();
        drop(storage);
        std::fs::remove_dir_all(dir.join("state")).expect("remove");
        std::fs::rename(dir.join("state-before"), dir.join("state")).expect("put back");
        (durable, published.represented.get())
    }

    /// task-j06: under the strict profile a projection behind the
    /// published baseline stops the node -- nothing would have rolled it
    /// back, so it is damage -- and under the replay-backed profile the
    /// same projection is discarded and the baseline's image installed in
    /// its place, after which the attach replays `(C, J]` and the node
    /// serves at the journal's head.
    #[test]
    fn a_projection_behind_its_baseline_is_reinstalled_only_under_the_replay_profile() {
        let dir = workspace("reinstall");
        let mut config = config(&dir);
        let (durable, represented) = a_projection_behind_its_baseline(&config, &dir, 1);
        assert!(represented > 0);

        let refused = serve(&config, 1).expect("serve").attach();
        assert!(
            matches!(&refused, Err(StoreError::Refused { reason, .. })
                if reason.contains("does not continue the journal")
                    && reason.contains("the strict profile does not reinstall it")),
            "the strict profile attached a projection behind its baseline: {:?}",
            refused.err().map(|e| e.to_string())
        );

        config.journal.profile = coord_daemon::config::JOURNAL_REPLAY_PROFILE.to_owned();
        let storage = serve(&config, 1)
            .expect("serve")
            .attach()
            .expect("the replay profile reinstalls the baseline");
        let frontiers = storage.domain.store().frontiers(DOMAIN).expect("frontiers");
        assert_eq!(frontiers.materialized(), frontiers.durable());
        assert_eq!(frontiers.checkpoint().get(), represented);
        // This boot's lifecycle record is past the second boot's head.
        assert!(frontiers.durable().get() > durable);
        assert_eq!(
            storage.baseline.map(|p| p.represented.get()),
            Some(represented)
        );
        drop(storage);
        // The reinstalled generation is the one selected now.
        serve(&config, 1)
            .expect("serve")
            .attach()
            .expect("the reinstalled generation serves again");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// task-j06, on a node that has been through an adoption (task-58):
    /// the manifest moved to the new incarnation and the projection, and
    /// every image published from it, kept the one it was created under.
    /// The reinstall stages a generation under the manifest's and installs
    /// the image's record into it, which a selected generation may hold.
    #[test]
    fn a_projection_behind_its_baseline_is_reinstalled_after_an_adoption() {
        let dir = workspace("reinstall-adopted");
        let mut config = config(&dir);
        config.journal.profile = coord_daemon::config::JOURNAL_REPLAY_PROFILE.to_owned();
        let (durable, represented) = a_projection_behind_its_baseline(&config, &dir, 2);
        assert!(represented > 0);

        let storage = serve(&config, 2)
            .expect("serve")
            .attach()
            .expect("an adopted node reinstalls its baseline");
        let frontiers = storage.domain.store().frontiers(DOMAIN).expect("frontiers");
        assert_eq!(frontiers.materialized(), frontiers.durable());
        assert_eq!(frontiers.checkpoint().get(), represented);
        assert!(frontiers.durable().get() > durable);
        drop(storage);
        assert_eq!(
            coord_storage_redb::lifecycle::selected_manifest(
                &config.state.root_path(&config.state_directory)
            )
            .expect("manifest")
            .incarnation,
            incarnation(2)
        );
        serve(&config, 2)
            .expect("serve")
            .attach()
            .expect("the reinstalled generation serves again");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// task-j06: a reinstall stopped after its generation is activated
    /// and before the attach has replayed anything onto it. The next
    /// start finds the image's projection at the baseline, which
    /// continues the journal, so it keeps it -- no second reinstall --
    /// and replays `(C, J]` onto it as any boot does.
    #[test]
    fn a_reinstall_stopped_before_its_replay_is_finished_by_the_next_start() {
        let dir = workspace("reinstall-stopped");
        let mut config = config(&dir);
        config.journal.profile = coord_daemon::config::JOURNAL_REPLAY_PROFILE.to_owned();
        let (durable, represented) = a_projection_behind_its_baseline(&config, &dir, 1);

        let reinstalled = {
            let opened = serve(&config, 1).expect("serve");
            let checkpoints = coord_checkpoint::LocalCheckpointStore::open(&opened.checkpoint_root)
                .expect("checkpoints");
            let baseline = opened
                .store
                .recovery_baseline(opened.domain_id)
                .expect("baseline")
                .expect("a published baseline");
            let image = checkpoints.load(&baseline).expect("image");
            let generation = reinstall_if_invalid(
                &opened.store,
                opened.domain_id,
                opened.generation,
                Some((&baseline, &image)),
                baseline.represented,
                opened.options,
            )
            .expect("reinstalled");
            // The process stops here: the reinstalled generation is
            // selected, and nothing has been replayed onto it.
            generation.directory().to_path_buf()
        };

        let storage = serve(&config, 1)
            .expect("serve")
            .attach()
            .expect("the next start replays onto the reinstalled generation");
        assert_eq!(storage.generation, reinstalled, "reinstalled a second time");
        let frontiers = storage.domain.store().frontiers(DOMAIN).expect("frontiers");
        assert_eq!(frontiers.materialized(), frontiers.durable());
        assert_eq!(frontiers.checkpoint().get(), represented);
        assert!(frontiers.durable().get() > durable);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// How a lost projection is lost (task-j05 step 3, `replay`'s case).
    #[derive(Clone, Copy, Debug)]
    enum Loss {
        /// A fresh database: the projection holds nothing.
        Empty,
        /// The projection of an earlier boot, behind the baseline.
        Stale,
        /// A stamp naming a record the journal does not hold.
        Foreign,
        /// A file redb cannot open: garbage, empty, or gone.
        Garbage,
        ZeroLength,
        Missing,
    }

    /// The selected generation's database file.
    fn selected_database(config: &Config) -> PathBuf {
        let root = config.state.root_path(&config.state_directory);
        let current = std::fs::read_to_string(root.join("CURRENT")).expect("CURRENT");
        root.join(current.trim()).join("domain.redb")
    }

    /// Lose the selected projection of a node that published a baseline,
    /// in the way `loss` names.
    fn lose(config: &Config, dir: &Path, loss: Loss) {
        let database = selected_database(config);
        match loss {
            Loss::Empty => {
                let scratch = dir.join("scratch");
                let fresh = Generation::create(
                    &scratch,
                    StoreIdentity {
                        cluster_id: CLUSTER,
                        domain_id: DOMAIN,
                        replica_id: REPLICA,
                        incarnation: incarnation(1),
                    },
                    OpenOptions::default(),
                )
                .expect("a fresh generation");
                let path = fresh.directory().join("domain.redb");
                drop(fresh);
                std::fs::copy(path, &database).expect("replace");
                std::fs::remove_dir_all(scratch).expect("scratch");
            }
            Loss::Stale => {
                std::fs::remove_dir_all(dir.join("state")).expect("remove");
                copy_tree(&dir.join("state-before"), &dir.join("state"));
            }
            Loss::Foreign => {
                let identity = StoreIdentity {
                    cluster_id: CLUSTER,
                    domain_id: DOMAIN,
                    replica_id: REPLICA,
                    incarnation: incarnation(1),
                };
                let root = config.state.root_path(&config.state_directory);
                let mut generation =
                    Generation::open_existing(&root, identity, OpenOptions::default())
                        .expect("open");
                let engine = generation.engine();
                let meta = coord_storage::lowering::DurableMeta::read(
                    &engine.reader().snapshot().expect("snapshot"),
                )
                .expect("meta");
                let foreign = coord_storage::lowering::DurableMeta {
                    stamp: coord_store_api::envelope::AppliedStamp::new(
                        meta.stamp.store_seq(),
                        coord_types::identity::Digest32([0xee; 32]),
                    ),
                    ..meta
                };
                let mut txn = engine.begin_write().expect("write");
                foreign.write(&mut txn).expect("stamp");
                txn.commit_durable().expect("commit");
            }
            Loss::Garbage => std::fs::write(&database, vec![0x5a; 64 << 10]).expect("garbage"),
            Loss::ZeroLength => std::fs::write(&database, []).expect("truncate"),
            Loss::Missing => std::fs::remove_file(&database).expect("remove"),
        }
    }

    /// A node initialized, given its genesis policy and booted once more
    /// to publish a baseline, with the state of the first boot kept in
    /// `state-before`. Returns the baseline's represented sequence.
    fn a_node_with_a_baseline(config: &Config, dir: &Path) -> u64 {
        let opened = open_storage(
            config,
            Intent::Initialize,
            CLUSTER,
            DOMAIN,
            REPLICA,
            incarnation(1),
        )
        .expect("init");
        let mut storage = opened.attach().expect("attach");
        crate::write_genesis_policy(config, &mut storage, incarnation(1)).expect("policy");
        drop(storage);
        copy_tree(&dir.join("state"), &dir.join("state-before"));
        let mut storage = serve(config, 1).expect("serve").attach().expect("attach");
        let published = storage
            .domain
            .publish_local(
                &storage.checkpoints,
                &coord_checkpoint::LocalLimits::default(),
            )
            .expect("publish")
            .expect("something to represent");
        published.represented.get()
    }

    /// task-j05 step 3: a projection lost in any of the ways `replay`'s
    /// case names -- empty, behind the baseline, stamped with a record the
    /// journal does not hold, or a file redb cannot open -- stops a strict
    /// node, and a replay-backed node discards it, installs its baseline
    /// and replays the journal onto it, serving at the journal's head.
    #[test]
    fn a_lost_projection_is_reinstalled_under_replay_and_refused_under_strict() {
        for loss in [
            Loss::Empty,
            Loss::Stale,
            Loss::Foreign,
            Loss::Garbage,
            Loss::ZeroLength,
            Loss::Missing,
        ] {
            let dir = workspace(&format!("lost-{loss:?}"));
            let mut config = config(&dir);
            let represented = a_node_with_a_baseline(&config, &dir);
            assert!(represented > 0);
            lose(&config, &dir, loss);

            let refused = serve(&config, 1).and_then(Opened::attach);
            assert!(
                matches!(refused, Err(StoreError::Refused { .. })),
                "{loss:?}: the strict profile served a lost projection: {:?}",
                refused.map(|_| ()).map_err(|e| e.to_string())
            );

            config.journal.profile = coord_daemon::config::JOURNAL_REPLAY_PROFILE.to_owned();
            let storage = serve(&config, 1)
                .and_then(Opened::attach)
                .unwrap_or_else(|e| panic!("{loss:?}: not reinstalled: {e}"));
            let frontiers = storage.domain.store().frontiers(DOMAIN).expect("frontiers");
            assert_eq!(frontiers.materialized(), frontiers.durable(), "{loss:?}");
            assert_eq!(frontiers.checkpoint().get(), represented, "{loss:?}");
            drop(storage);
            serve(&config, 1)
                .and_then(Opened::attach)
                .unwrap_or_else(|e| panic!("{loss:?}: the reinstalled generation: {e}"));
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    /// An authorized replacement that starts with an unreadable
    /// projection: the baseline is in the predecessor's stream, which the
    /// adoption carries forward only after the reinstall. The reinstall
    /// reads it from the stream the root is stamped with, and the
    /// adoption then proceeds on the reinstalled generation (Codex review
    /// on #212).
    #[test]
    fn an_unreadable_projection_is_reinstalled_before_a_pending_adoption() {
        let dir = workspace("unreadable-adoption");
        let mut config = config(&dir);
        config.journal.profile = coord_daemon::config::JOURNAL_REPLAY_PROFILE.to_owned();
        a_node_with_a_baseline(&config, &dir);
        lose(&config, &dir, Loss::Garbage);

        let opened = serve(&config, 2).expect("an unreadable projection is reinstalled");
        assert!(opened.adoption.is_some(), "no adoption was pending");
        let storage = opened
            .attach()
            .expect("the adoption on the reinstalled generation");
        let frontiers = storage.domain.store().frontiers(DOMAIN).expect("frontiers");
        assert_eq!(frontiers.materialized(), frontiers.durable());
        drop(storage);
        assert_eq!(
            coord_storage_redb::lifecycle::selected_manifest(
                &config.state.root_path(&config.state_directory)
            )
            .expect("manifest")
            .incarnation,
            incarnation(2)
        );
        serve(&config, 2)
            .and_then(Opened::attach)
            .expect("the adopted generation serves again");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// An adoption stopped between its two writes (the stream re-keyed to
    /// the new incarnation, the manifest not moved yet) and then an
    /// unreadable projection: the baseline is still found, under the
    /// stream's new key with the stamped origin, and reinstalled; the
    /// adoption then finishes (#212 review).
    #[test]
    fn an_unreadable_projection_after_an_interrupted_adoption_is_reinstalled() {
        let dir = workspace("unreadable-interrupted-adoption");
        let mut config = config(&dir);
        config.journal.profile = coord_daemon::config::JOURNAL_REPLAY_PROFILE.to_owned();
        a_node_with_a_baseline(&config, &dir);

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
            "{:?}",
            stopped.err()
        );
        lose(&config, &dir, Loss::Garbage);

        let opened = serve(&config, 2).expect("an unreadable projection is reinstalled");
        assert!(opened.adoption.is_some(), "the adoption is still pending");
        let storage = opened.attach().expect("the adoption finishes");
        let frontiers = storage.domain.store().frontiers(DOMAIN).expect("frontiers");
        assert_eq!(frontiers.materialized(), frontiers.durable());
        drop(storage);
        serve(&config, 2)
            .and_then(Opened::attach)
            .expect("the adopted generation serves again");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The checks that can refuse a start run on the staged reinstall,
    /// and it is selected only after them (#212 review): a pin that
    /// refuses leaves `CURRENT` naming the unreadable generation, whose
    /// file is untouched, and the next start reinstalls again.
    #[test]
    fn a_reinstall_the_pin_refuses_is_never_selected() {
        let dir = workspace("reinstall-pin-refuses");
        let mut config = config(&dir);
        config.journal.profile = coord_daemon::config::JOURNAL_REPLAY_PROFILE.to_owned();
        a_node_with_a_baseline(&config, &dir);
        lose(&config, &dir, Loss::Garbage);
        let database = selected_database(&config);

        let refused = open_storage_with(
            &config,
            Intent::Serve,
            CLUSTER,
            DOMAIN,
            REPLICA,
            incarnation(1),
            |_| {
                Err(StoreError::Refused {
                    root: "test".into(),
                    reason: "the pin refuses".into(),
                })
            },
        );
        assert!(
            matches!(&refused, Err(StoreError::Refused { reason, .. }) if reason == "the pin refuses"),
            "{:?}",
            refused.err()
        );
        assert_eq!(
            selected_database(&config),
            database,
            "the staging was selected"
        );
        assert_eq!(
            std::fs::read(&database).expect("kept"),
            vec![0x5a; 64 << 10]
        );
        serve(&config, 1)
            .and_then(Opened::attach)
            .expect("the next start reinstalls");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A reinstall keeps the generation it discarded, as evidence, and
    /// removes every earlier one: repeated reinstalls leave the selected
    /// generation and the last one discarded (#212 review).
    #[test]
    fn repeated_reinstalls_keep_only_the_last_discarded_generation() {
        let dir = workspace("reinstall-prune");
        let mut config = config(&dir);
        config.journal.profile = coord_daemon::config::JOURNAL_REPLAY_PROFILE.to_owned();
        a_node_with_a_baseline(&config, &dir);
        let root = config.state.root_path(&config.state_directory);
        let generations = || {
            let mut names: Vec<String> = std::fs::read_dir(&root)
                .expect("root")
                .map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
                .filter(|n| n.starts_with("gen-"))
                .collect();
            names.sort();
            names
        };
        let mut discarded = Vec::new();
        for _ in 0..3 {
            let lost = selected_database(&config);
            discarded.push(
                lost.parent()
                    .and_then(|d| d.file_name())
                    .map(|n| n.to_string_lossy().into_owned())
                    .expect("generation name"),
            );
            lose(&config, &dir, Loss::Garbage);
            drop(
                serve(&config, 1)
                    .and_then(Opened::attach)
                    .expect("reinstalled"),
            );
        }
        let selected = selected_database(&config)
            .parent()
            .and_then(|d| d.file_name())
            .map(|n| n.to_string_lossy().into_owned())
            .expect("generation name");
        let mut expected = vec![discarded.last().expect("three").clone(), selected];
        expected.sort();
        assert_eq!(generations(), expected);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_ordinary_start_removes_an_abandoned_staging_and_keeps_the_discarded_generation() {
        let dir = workspace("prune-staging");
        let mut config = config(&dir);
        config.journal.profile = coord_daemon::config::JOURNAL_REPLAY_PROFILE.to_owned();
        a_node_with_a_baseline(&config, &dir);
        let root = config.state.root_path(&config.state_directory);
        let name_of = |path: &Path| {
            path.file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .expect("generation name")
        };
        let generations = || {
            let mut names: Vec<String> = std::fs::read_dir(&root)
                .expect("root")
                .map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
                .filter(|n| n.starts_with("gen-"))
                .collect();
            names.sort();
            names
        };
        // A reinstall leaves its discarded generation below the selection.
        let discarded = name_of(selected_database(&config).parent().expect("generation"));
        lose(&config, &dir, Loss::Garbage);
        drop(
            serve(&config, 1)
                .and_then(Opened::attach)
                .expect("reinstalled"),
        );
        let selected_dir = selected_database(&config)
            .parent()
            .expect("generation")
            .to_path_buf();
        let selected = name_of(&selected_dir);
        // A stopped attempt left a filled staging above it, never selected.
        let number: u64 = selected["gen-".len()..].parse().expect("number");
        let staging = root.join(format!("gen-{:06}", number + 1));
        std::fs::create_dir(&staging).expect("staging");
        for entry in std::fs::read_dir(&selected_dir).expect("selected") {
            let path = entry.expect("entry").path();
            std::fs::copy(&path, staging.join(path.file_name().expect("name"))).expect("copy");
        }
        let mut before = vec![discarded.clone(), selected.clone(), name_of(&staging)];
        before.sort();
        assert_eq!(generations(), before);

        drop(serve(&config, 1).and_then(Opened::attach).expect("served"));
        let mut after = vec![discarded, selected];
        after.sort();
        assert_eq!(generations(), after, "the staging goes, the evidence stays");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Every regular file under `root`, by path relative to it, with its
    /// bytes.
    fn tree_bytes(root: &Path) -> Vec<(PathBuf, Vec<u8>)> {
        let mut out = Vec::new();
        let mut stack = vec![root.to_path_buf()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).expect("read dir") {
                let path = entry.expect("entry").path();
                if path.is_dir() {
                    stack.push(path);
                } else {
                    let rel = path.strip_prefix(root).expect("under root").to_path_buf();
                    out.push((rel, std::fs::read(&path).expect("read")));
                }
            }
        }
        out.sort();
        out
    }

    /// Every row of every table of the generation's database, by table:
    /// the projection's logical content. Its bytes are no measure, since
    /// redb rewrites its header and trims the file on any open and close.
    fn rows_of(generation: &Path) -> Vec<(String, Vec<(Vec<u8>, Vec<u8>)>)> {
        use redb::{ReadableDatabase, ReadableTable, TableHandle};
        let db = redb::Database::open(generation.join("domain.redb")).expect("open");
        let read = db.begin_read().expect("read");
        let mut out = Vec::new();
        for handle in read.list_tables().expect("tables") {
            let name = handle.name().to_owned();
            let def: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new(&name);
            let table = read.open_table(def).expect("table");
            let rows = table
                .iter()
                .expect("iter")
                .map(|row| {
                    let (k, v) = row.expect("row");
                    (k.value().to_vec(), v.value().to_vec())
                })
                .collect();
            out.push((name, rows));
        }
        out.sort();
        out
    }

    /// An activation is a journaled transition, and the attach's replay
    /// applies it without asking whether this build can do the feature:
    /// one journaled after the projection's stamp is not in the projection
    /// the first rollback guard reads. The start reads what the replay
    /// would apply first, and refuses at the activation record, under
    /// either profile, before a row is replayed or a boot record appended:
    /// the generation and the journal are byte for byte as they were
    /// (#212 reviews).
    #[test]
    fn an_activation_only_the_journal_holds_is_refused_before_the_replay() {
        use coord_storage::Persistence;
        for replay in [false, true] {
            let dir = workspace(&format!("activation-in-journal-{replay}"));
            let mut config = config(&dir);
            if replay {
                config.journal.profile = coord_daemon::config::JOURNAL_REPLAY_PROFILE.to_owned();
            }
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
            drop(storage);
            let generation = selected_database(&config)
                .parent()
                .expect("generation")
                .to_path_buf();
            let before = dir.join("projection-before");
            copy_tree(&generation, &before);

            // The cluster activates a feature this build has never heard
            // of, through the journal.
            let mut storage = serve(&config, 1).expect("serve").attach().expect("attach");
            let record = coord_checkpoint::feature::ActiveFeaturesV1 {
                cluster: CLUSTER,
                domain: DOMAIN,
                configuration: coord_types::ids::ConfigurationEpoch::new(1).expect("epoch"),
                features: vec![0x7fff],
                reporters: vec![REPLICA],
            };
            let mut barriers =
                coord_core::outbox::BarrierAllocator::new(incarnation(1), storage.boot);
            storage
                .domain
                .submit(
                    coord_core::effect::PersistBatch {
                        barrier: barriers.allocate(),
                        base: None,
                        updates: vec![record.update().expect("update")],
                    },
                    coord_storage::journaled::TransitionKind::Protocol,
                )
                .expect("submit");
            for _ in 0..8 {
                if storage.domain.queued() == 0 && storage.domain.unmaterialized() == 0 {
                    break;
                }
                storage.domain.lower().expect("lower");
            }
            // And a record after it, which the replay would apply too.
            storage
                .domain
                .submit(
                    coord_core::effect::PersistBatch {
                        barrier: barriers.allocate(),
                        base: None,
                        updates: vec![coord_core::effect::StoreUpdate {
                            collection: coord_store_api::registry::Collection::ProtocolV1.id(),
                            key: b"after-the-activation".to_vec(),
                            value: Some(vec![1]),
                        }],
                    },
                    coord_storage::journaled::TransitionKind::Protocol,
                )
                .expect("submit");
            for _ in 0..8 {
                if storage.domain.queued() == 0 && storage.domain.unmaterialized() == 0 {
                    break;
                }
                storage.domain.lower().expect("lower");
            }
            assert_eq!(storage.domain.unmaterialized(), 0, "replay={replay}");
            drop(storage);

            // The projection from before both: it continues the journal,
            // so it is kept, and the attach would replay them onto it.
            std::fs::remove_dir_all(&generation).expect("remove");
            copy_tree(&before, &generation);
            let journal = root_path(&config.state_directory, &config.journal.root);
            let (projection_was, journal_was) = (rows_of(&generation), tree_bytes(&journal));
            for start in 0..2 {
                match serve(&config, 1).and_then(Opened::attach) {
                    Err(StoreError::Refused { reason, .. }) => {
                        assert!(
                            reason.contains("unknown active feature")
                                && reason.contains("nothing was replayed"),
                            "replay={replay}, start {start}: {reason}"
                        );
                    }
                    Err(other) => panic!("replay={replay}: refused for another reason: {other}"),
                    Ok(_) => panic!("replay={replay}: served a store it cannot fully read"),
                }
                assert!(
                    rows_of(&generation) == projection_was,
                    "replay={replay}, start {start}: the projection's rows changed"
                );
                assert!(
                    tree_bytes(&journal) == journal_was,
                    "replay={replay}, start {start}: the journal changed"
                );
            }
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    /// Without a published baseline there is nothing to install: a
    /// replay-backed node whose projection file cannot be opened refuses
    /// to serve and says why, and the file is left where it was.
    #[test]
    fn an_unreadable_projection_without_a_baseline_is_refused() {
        let dir = workspace("unreadable-no-baseline");
        let mut config = config(&dir);
        config.journal.profile = coord_daemon::config::JOURNAL_REPLAY_PROFILE.to_owned();
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
        drop(storage);
        let database = selected_database(&config);
        std::fs::write(&database, vec![0x5a; 64 << 10]).expect("garbage");

        match serve(&config, 1) {
            Err(StoreError::Refused { reason, .. }) => assert!(
                reason.contains("no local checkpoint was published"),
                "{reason}"
            ),
            Err(other) => panic!("refused for another reason: {other}"),
            Ok(_) => panic!("an unreadable projection served with no baseline"),
        }
        assert_eq!(selected_database(&config), database, "nothing was staged");
        assert_eq!(
            std::fs::read(&database).expect("kept"),
            vec![0x5a; 64 << 10]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A whole database redb won't open for a reason that says nothing
    /// about its bytes -- here another opener holds its lock -- is not
    /// unreadable: a replay-backed node with a baseline refuses it and
    /// stages nothing, rather than discarding a projection that may be
    /// healthy (#212 review, point 1).
    #[test]
    fn a_database_another_opener_holds_is_refused_not_reinstalled() {
        let dir = workspace("database-held");
        let mut config = config(&dir);
        config.journal.profile = coord_daemon::config::JOURNAL_REPLAY_PROFILE.to_owned();
        a_node_with_a_baseline(&config, &dir);
        let database = selected_database(&config);
        let held = redb::Database::open(&database).expect("hold the database");
        match serve(&config, 1) {
            Err(StoreError::Refused { reason, .. }) => {
                assert!(reason.contains("Database("), "{reason}");
            }
            Err(other) => panic!("refused for another reason: {other}"),
            Ok(_) => panic!("a held database was served or reinstalled over"),
        }
        assert_eq!(selected_database(&config), database, "nothing was staged");
        drop(held);
        serve(&config, 1)
            .and_then(Opened::attach)
            .expect("the same generation serves once released");
        assert_eq!(selected_database(&config), database);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A database whose identity record names another node is not
    /// unreadable: it is someone else's, and even a replay-backed node
    /// with a baseline refuses it rather than reinstalling over it.
    #[test]
    fn a_foreign_database_is_refused_even_with_a_baseline() {
        let dir = workspace("foreign-database");
        let mut config = config(&dir);
        config.journal.profile = coord_daemon::config::JOURNAL_REPLAY_PROFILE.to_owned();
        a_node_with_a_baseline(&config, &dir);
        let scratch = dir.join("scratch");
        let other = Generation::create(
            &scratch,
            StoreIdentity {
                cluster_id: CLUSTER,
                domain_id: DOMAIN,
                replica_id: ReplicaId([2; 16]),
                incarnation: incarnation(1),
            },
            OpenOptions::default(),
        )
        .expect("another node's generation");
        let path = other.directory().join("domain.redb");
        drop(other);
        std::fs::copy(path, selected_database(&config)).expect("swap");
        match serve(&config, 1) {
            Err(StoreError::Refused { reason, .. }) => {
                assert!(reason.contains("IdentityRecordMismatch"), "{reason}");
            }
            Err(other) => panic!("refused for another reason: {other}"),
            Ok(_) => panic!("another node's database was served or reinstalled over"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}

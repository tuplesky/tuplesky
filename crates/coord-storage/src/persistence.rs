//! Where a replica's durable batches go (design Sections 17.3, 19.3).
//!
//! Two coordinators make a batch durable and they are not
//! interchangeable in shape. [`StoreWorker`] writes it straight into the
//! domain's projection, which is then itself the durable record.
//! [`JournaledStore`] records it in the shared journal first and applies
//! it to the projection afterwards, in journal order, so the
//! authoritative transition survives even if the projection does not.
//!
//! What they are interchangeable in is everything the *application* path
//! needs, and that is what this trait says. Execution plans a command,
//! turns the plan into a batch and needs that batch to become durable;
//! it has no business knowing which of the two did it, and if it did
//! know there would be two copies of the planning and admission logic
//! with one of them quietly falling behind.
//!
//! The trait is deliberately narrow. It does not expose the journal, the
//! projection engine or a domain's attachment, because those are the
//! things that genuinely differ and that a caller reaching for them
//! would be reaching past the abstraction rather than through it.
//!
//! [`JournaledStore`]: crate::journaled::JournaledStore

use coord_core::effect::{ApplyBase, BootId, PersistBatch};
use coord_core::event::StorageEvent;
use coord_store_api::engine::EngineError;

use crate::journaled::TransitionKind;
use crate::view::GatedReader;

/// Why a batch was not accepted for durability.
///
/// The two coordinators refuse for overlapping but differently named
/// reasons; what the application path has to distinguish is only what it
/// would *do* next, which is this.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Refused {
    /// The outcome of earlier work is unknown. Reconcile before anything
    /// else: neither "it happened" nor "it did not" may be assumed.
    NotReady(String),
    /// The base this batch carries no longer extends the frontier, so the
    /// plan was computed against state that has moved. Rebuild the view
    /// and plan again; it is not an error and not a retry of this batch.
    StaleBase,
    /// The queue is full. Flushing lowers it; the batch was not taken.
    QueueFull,
    /// Refused for good: the batch may not become durable here at all.
    Rejected(String),
}

impl core::fmt::Display for Refused {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Refused::NotReady(why) => write!(f, "not ready: {why}"),
            Refused::StaleBase => f.write_str("the base no longer extends the frontier"),
            Refused::QueueFull => f.write_str("the queue is full"),
            Refused::Rejected(why) => write!(f, "refused: {why}"),
        }
    }
}

impl core::error::Error for Refused {}

/// What one lowering produced.
///
/// A flush of either coordinator reports the same three things the
/// application path acts on: the facts to feed the machines, whether the
/// outcome is in doubt, and nothing else.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Lowered {
    /// Storage facts, in the order they became true.
    pub events: Vec<StorageEvent>,
    /// Whether some outcome is unknown and reconciliation is required.
    ///
    /// It is not "failed": an indeterminate write may or may not have
    /// happened, and assuming either answer is how a durable prefix gets
    /// lost or a command gets applied twice.
    pub indeterminate: bool,
}

/// Somewhere a replica's batches become durable.
pub trait Persistence {
    /// The projection reader this coordinator hands out.
    type Reader: coord_store_api::engine::SnapshotSource;

    /// The boot this coordinator serves. A batch of another boot is not
    /// this process's to make durable.
    fn boot(&self) -> BootId;

    /// The base the next application batch must carry.
    fn application_base(&self) -> ApplyBase;

    /// A gated reader: it hands out a snapshot only together with the
    /// stamp that snapshot proves.
    fn reader(&self) -> GatedReader<Self::Reader>;

    /// Batches accepted and not yet lowered.
    fn queued(&self) -> usize;

    /// Records that are durable and that the projection still owes.
    ///
    /// Zero on a coordinator with no journal, where the projection *is*
    /// the record and there is no moment between the two. On the
    /// journal-first path this is what tells "the materialization has
    /// not happened yet" apart from "nothing is coming": the coordinator
    /// deliberately reports no event for a deferred materialization,
    /// because a durable journal record is never a failed batch.
    fn unmaterialized(&self) -> usize;

    /// Take `batch` for durability, as a transition of `kind`.
    ///
    /// `kind` is what the journal records the transition as; a
    /// coordinator that keeps no journal has no use for it and ignores
    /// it. It is passed rather than inferred because only the caller
    /// knows whether a batch is an execution's redo or a protocol step,
    /// and inferring it from the batch's contents would make the
    /// journal's meaning depend on the shape of a row.
    fn submit(&mut self, batch: PersistBatch, kind: TransitionKind) -> Result<(), Refused>;

    /// Lower as much accepted work as one group allows.
    fn lower(&mut self) -> Result<Lowered, EngineError>;

    /// Resolve an indeterminate outcome against what is actually durable.
    fn reconcile(&mut self) -> Result<Lowered, EngineError>;

    /// Everything a restarted replica wires its consensus machine from.
    ///
    /// This is on the seam, and it is the only way through it, because
    /// the difference between the two coordinators here is exactly the
    /// difference that a composition could get wrong without noticing.
    /// Where the projection *is* the record, its snapshot is the whole
    /// story. Where a journal is the record, the projection may lag it,
    /// and a promise or a vote that is durable in the journal but not
    /// materialized yet is still an obligation this replica owes: a
    /// summary built from the projection alone would omit it, and this
    /// replica would come back contradicting a vote it had already sent.
    ///
    /// So the journal-first implementation reads the authoritative cut
    /// (design Section 4.8) and never the projection snapshot, and a
    /// caller cannot ask for the other one because there is nothing here
    /// to ask.
    fn recovered(
        &self,
        epoch: coord_types::ids::ConfigurationEpoch,
        budget: crate::views::ViewBudget,
    ) -> Result<crate::protocol::RecoveredProtocol, EngineError>;

    /// The replica's consensus machine is now at `ballot`, so what this
    /// coordinator records from here on is stamped with the ballot the
    /// transition was actually made under.
    ///
    /// A coordinator with no journal records no ballot and ignores it.
    /// The journal-first one stamps every transition, and a stamp that
    /// lagged the machine recorded a follower's promises and votes under
    /// a ballot it never entered -- its own replica as leader -- which a
    /// fence at the promised ballot, tie-broken on the leader, would then
    /// refuse as obsolete. It takes the machine's number and leader. The
    /// epoch stays the configuration epoch of the application base, which
    /// is what the record format checks every application transition
    /// against; that is the configuration the record extends, and it is
    /// not the machine's to move.
    fn follow_ballot(&mut self, ballot: coord_types::ids::Ballot) {
        let _ = ballot;
    }
}

/// The reference path: the projection is itself the durable record.
///
/// There is no journal, so the transition kind has nowhere to be
/// recorded and is dropped. That is not a loss of meaning here: without
/// a journal there is no separate authoritative record for the kind to
/// describe, and the batch's own ordering guard already carries what the
/// projection needs.
impl<E: coord_store_api::engine::LocalEngine> Persistence for crate::worker::StoreWorker<E> {
    type Reader = E::Reader;

    fn boot(&self) -> BootId {
        crate::worker::StoreWorker::boot(self)
    }

    fn application_base(&self) -> ApplyBase {
        crate::worker::StoreWorker::application_base(self)
    }

    fn reader(&self) -> GatedReader<Self::Reader> {
        crate::worker::StoreWorker::reader(self)
    }

    fn queued(&self) -> usize {
        crate::worker::StoreWorker::queued(self)
    }

    fn unmaterialized(&self) -> usize {
        // The projection is the record: there is nothing between them to
        // be owed.
        0
    }

    fn submit(&mut self, batch: PersistBatch, _kind: TransitionKind) -> Result<(), Refused> {
        crate::worker::StoreWorker::submit(self, batch).map_err(|e| match e {
            crate::worker::SubmitError::NotReady(state) => Refused::NotReady(format!("{state:?}")),
            crate::worker::SubmitError::QueueFull => Refused::QueueFull,
            other => Refused::Rejected(format!("{other:?}")),
        })
    }

    fn lower(&mut self) -> Result<Lowered, EngineError> {
        crate::worker::StoreWorker::flush(self).map(lowered_from_worker)
    }

    fn reconcile(&mut self) -> Result<Lowered, EngineError> {
        crate::worker::StoreWorker::reconcile(self).map(lowered_from_worker)
    }

    /// The projection is the record, so its snapshot is the whole story:
    /// there is no moment at which something is durable and not visible
    /// here, and therefore nothing a cut could add.
    fn recovered(
        &self,
        epoch: coord_types::ids::ConfigurationEpoch,
        budget: crate::views::ViewBudget,
    ) -> Result<crate::protocol::RecoveredProtocol, EngineError> {
        let gated = Persistence::reader(self).snapshot().map_err(|e| match e {
            crate::view::ViewError::Engine(e) => e,
            other => EngineError::new(
                coord_store_api::engine::ErrorClass::Busy,
                format!("no snapshot to recover from: {other:?}"),
            ),
        })?;
        crate::protocol::read_protocol(gated.view(), epoch, budget)
    }
}

fn lowered_from_worker(outcome: crate::worker::FlushOutcome) -> Lowered {
    Lowered {
        events: outcome.events,
        indeterminate: outcome.indeterminate,
    }
}

/// One domain's view of the journal-first coordinator.
///
/// [`JournaledStore`] serves several domains from one shared journal,
/// which is what lets a node hold many groups without a journal each.
/// The application path works on exactly one domain at a time, so it
/// sees this: the store, the domain it is applying to, and the ballot
/// the transitions it produces are bound to.
///
/// The ballot is held here rather than passed with each batch because it
/// is a property of the replica's current term, not of a batch: it
/// changes when the machine's ballot changes and not otherwise. Holding
/// a stale one is not silent -- the store fences on the promise it has
/// and refuses an obsolete ballot -- so the failure is a refusal rather
/// than a transition recorded under a term that has passed.
///
/// [`JournaledStore`]: crate::journaled::JournaledStore
pub struct JournaledDomain<
    J: coord_journal_api::JournalEngine,
    E: coord_store_api::engine::LocalEngine,
> {
    store: crate::journaled::JournaledStore<J, E>,
    domain: coord_types::ids::DomainId,
    ballot: coord_types::ids::Ballot,
}

impl<J: coord_journal_api::JournalEngine, E: coord_store_api::engine::LocalEngine>
    JournaledDomain<J, E>
{
    /// A view of `domain` in `store`, producing transitions at `ballot`.
    ///
    /// The domain must already be attached: attaching is where the
    /// projection's frontiers are checked against the journal's and where
    /// the suffix is replayed, and doing it implicitly here would let a
    /// caller serve a domain whose recovery had never been validated.
    pub fn new(
        store: crate::journaled::JournaledStore<J, E>,
        domain: coord_types::ids::DomainId,
        ballot: coord_types::ids::Ballot,
    ) -> Option<Self> {
        store.status(domain)?;
        Some(JournaledDomain {
            store,
            domain,
            ballot,
        })
    }

    /// The domain this view serves.
    pub const fn domain(&self) -> coord_types::ids::DomainId {
        self.domain
    }

    /// The ballot transitions are recorded under.
    pub const fn ballot(&self) -> coord_types::ids::Ballot {
        self.ballot
    }

    /// Record transitions under `ballot` from now on.
    pub const fn set_ballot(&mut self, ballot: coord_types::ids::Ballot) {
        self.ballot = ballot;
    }

    /// The shared store (diagnostics, maintenance, other domains).
    pub const fn store(&self) -> &crate::journaled::JournaledStore<J, E> {
        &self.store
    }

    /// The shared store, mutably.
    pub const fn store_mut(&mut self) -> &mut crate::journaled::JournaledStore<J, E> {
        &mut self.store
    }

    /// Give the store back.
    pub fn into_store(self) -> crate::journaled::JournaledStore<J, E> {
        self.store
    }
}

impl<J: coord_journal_api::JournalEngine, E: coord_store_api::engine::LocalEngine> Persistence
    for JournaledDomain<J, E>
{
    type Reader = E::Reader;

    fn boot(&self) -> BootId {
        self.store.boot()
    }

    fn application_base(&self) -> ApplyBase {
        self.store
            .application_base(self.domain)
            .expect("the domain is attached: checked at construction")
    }

    fn reader(&self) -> GatedReader<Self::Reader> {
        self.store
            .reader(self.domain)
            .expect("the domain is attached: checked at construction")
    }

    fn queued(&self) -> usize {
        self.store.queued(self.domain)
    }

    fn unmaterialized(&self) -> usize {
        self.store.unmaterialized(self.domain)
    }

    fn follow_ballot(&mut self, ballot: coord_types::ids::Ballot) {
        self.ballot = coord_types::ids::Ballot {
            epoch: self.application_base().configuration,
            ..ballot
        };
    }

    fn submit(&mut self, batch: PersistBatch, kind: TransitionKind) -> Result<(), Refused> {
        self.store
            .submit(crate::journaled::Submission {
                domain: self.domain,
                ballot: self.ballot,
                kind,
                batch,
            })
            .map_err(|e| match e {
                // The plan was computed against a frontier that has moved.
                // Replanning is the answer, not retrying this batch: the
                // command would otherwise be applied at a position that
                // is no longer its own.
                crate::journaled::SubmitRefused::StaleBase { .. } => Refused::StaleBase,
                crate::journaled::SubmitRefused::QueueFull => Refused::QueueFull,
                crate::journaled::SubmitRefused::NotReady(status) => {
                    Refused::NotReady(format!("{status:?}"))
                }
                other => Refused::Rejected(format!("{other:?}")),
            })
    }

    fn lower(&mut self) -> Result<Lowered, EngineError> {
        self.store.flush().map(lowered_from_journal).map_err(engine)
    }

    fn reconcile(&mut self) -> Result<Lowered, EngineError> {
        self.store
            .reconcile(self.domain)
            .map(lowered_from_journal)
            .map_err(engine)
    }

    /// The authoritative cut, never the projection snapshot: the
    /// materialized state plus every obligation the journal already
    /// holds and the projection has not caught up with.
    fn recovered(
        &self,
        epoch: coord_types::ids::ConfigurationEpoch,
        budget: crate::views::ViewBudget,
    ) -> Result<crate::protocol::RecoveredProtocol, EngineError> {
        let cut = self.store.recovery_cut(self.domain).map_err(|e| {
            EngineError::new(
                coord_store_api::engine::ErrorClass::Corrupt,
                format!("no authoritative recovery cut: {e:?}"),
            )
        })?;
        crate::protocol::read_protocol(&cut, epoch, budget)
    }
}

fn lowered_from_journal(report: crate::journaled::FlushReport) -> Lowered {
    Lowered {
        events: report.events,
        indeterminate: report.indeterminate,
    }
}

fn engine(error: crate::journaled::JournaledError) -> EngineError {
    EngineError::new(
        coord_store_api::engine::ErrorClass::Io,
        format!("journal: {error:?}"),
    )
}

//! Ordered application of learned commands through the common
//! materializer (task-24; design Sections 4.5, 17.4).
//!
//! The consensus machines decide *which* command executes next; the
//! [`Applier`] executes it deterministically: it rehashes the durable
//! payload against the command identity, admits the invocation through the
//! retry layer (a retained result is returned unchanged, never
//! re-executed), builds the authorized view at the established
//! predecessor, plans, applies the plan atomically with its retry binding,
//! and only after the durable commit publishes the revision's complete
//! event set to the watch hub. The outcome it reports is what the learner
//! seals into an `EstablishedResult`: nothing is established before the
//! application is irrevocable, and no watch event precedes it.

use coord_consensus::{AppliedOutcome, PayloadRecordV1};
use coord_core::capability::{AdmissionFacts, AdmissionPurpose};
use coord_core::outbox::BarrierAllocator;
use coord_state::{
    AdmissionReceiptV1, InternalCommand, PlanError, PlanLimits, RejectionReason,
    SESSION_RETRY_WINDOW, authorize_retained, plan, plan_internal, rejection_plan,
    rejection_plan_at,
};
use coord_store_api::engine::EngineError;
use coord_types::ids::{KvRevision, NamespaceId};
use coord_types::logical_v1::{CanonicalOperation, LogicalRequest};
use coord_types::{CommandId, RetryKey};

use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;

use coord_core::effect::BarrierId;
use coord_core::event::{StorageError, StorageEvent};
use coord_state::KvEvent;
use coord_store_api::engine::SnapshotSource;

use crate::cut::{CutOverlay, RecoveryCut};
use crate::lowering::ExecutionFrontier;
use crate::materialize::{
    ApplyOutcome, Pending, Submitted, apply_plan_sharing, apply_refused_plan_sharing, prepare,
};
use crate::persistence::Persistence;
use crate::retry::{self, Admission, RetryBinding};
use crate::view::{GatedView, ViewError};
use crate::views::{
    ViewBudget, ViewBuildError, build_authorized_view, build_internal_view, load_authorization,
};
use crate::watch::{PublishError, WatchHub};

/// Whether `session` may still have the result `record` retained for
/// `request`, under the policy `view` holds now.
///
/// The one rule for a retained result, wherever it is handed out: at
/// execution, when an already-executed command is presented again, and at
/// a frontend that answers a retry from the durable record before
/// submitting anything. Losing a permission protects what it produced,
/// exactly as it would a fresh execution's, and a result that cannot be
/// decoded or authorized is not handed out.
pub fn retained_is_authorized<V: coord_store_api::engine::OrderedRead>(
    view: &V,
    session: &coord_types::ids::SessionId,
    request: &LogicalRequest,
    record: &crate::codecs::RetryRecordV1,
) -> bool {
    let namespace = request.namespace;
    let Ok(auth) = load_authorization(view, namespace, session, ViewBudget::default()) else {
        return false;
    };
    let Ok(stored) = postcard::from_bytes::<coord_state::Response>(&record.response) else {
        return false;
    };
    authorize_retained(&auth, &namespace, request, &stored.outcome).is_ok()
}

/// Why a command could not be applied.
#[derive(Debug)]
pub enum ApplyError {
    /// The payload does not decode to a canonical request.
    MalformedPayload,
    /// The payload rehashes to another identity.
    IdentityMismatch {
        /// Expected identity.
        expected: CommandId,
        /// Identity of the payload.
        actual: CommandId,
    },
    /// The invocation was not admitted (unknown/retired session, retired
    /// sequence, window, or a conflicting payload under the retry key).
    NotAdmitted(Admission),
    /// The view could not be built.
    View(ViewBuildError),
    /// The plan could not be produced.
    Plan(PlanError),
    /// Engine failure.
    Engine(EngineError),
    /// No durable view is available (ahead of completion or quarantined).
    Unavailable(ViewError),
    /// Replanning did not converge.
    Diverged,
    /// A durable revision could not be published to the watch hub.
    Publish(PublishError),
    /// A group's batches were definitely not journaled after the commands
    /// in it were reported applied (task-d47). Nothing of the group was
    /// released; the replica restarts from its journal.
    GroupLost,
    /// Within a group, the store has no room for the command's batch
    /// beside what is already queued (task-d47). Nothing was submitted or
    /// changed: the caller lowers the group and the queue, and applies the
    /// same command again.
    GroupFull,
}

impl From<EngineError> for ApplyError {
    fn from(e: EngineError) -> Self {
        ApplyError::Engine(e)
    }
}

impl From<ViewError> for ApplyError {
    fn from(e: ViewError) -> Self {
        ApplyError::Unavailable(e)
    }
}

/// The parts of an applied outcome a pending application already knows.
///
/// A command that has been planned and recorded knows what it did before
/// anyone can read it: the position it took, the revision it produced
/// and the digest of its exact result. Only the fact that it has
/// happened is still outstanding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppliedOutcomeParts {
    /// Execution position.
    pub position: coord_types::ids::ExecutionPosition,
    /// KV revision produced, if any.
    pub revision: Option<coord_types::ids::KvRevision>,
    /// Digest of the exact result bytes.
    pub result_digest: coord_types::identity::Digest32,
}

/// The application side of one replica.
///
/// `P` is where its batches become durable: [`StoreWorker`] on the
/// reference path, where the projection is itself the record, or
/// [`JournaledDomain`] on the journal-first one, where the record
/// reaches the shared journal before the projection. Planning,
/// admission, retry resolution and the watch hub are the same either
/// way, which is the point of the parameter: a second copy of them for
/// the journal would be a second copy to keep correct.
///
/// [`StoreWorker`]: crate::worker::StoreWorker
/// [`JournaledDomain`]: crate::persistence::JournaledDomain
pub struct Applier<P: Persistence> {
    store: P,
    alloc: BarrierAllocator,
    hub: WatchHub,
    /// Storage facts about batches that were not this applier's, met
    /// while it lowered or reconciled its own ([`Applier::take_foreign`]).
    /// Kept only once [`Applier::share_foreign`] says someone takes them.
    foreign: Vec<coord_core::event::StorageEvent>,
    sharing: bool,
    /// The commands applied since [`Applier::begin_group`] whose batches
    /// have not materialized yet (task-d47).
    group: Option<Group>,
    /// Groups handed to a pipelined store and not yet known to have
    /// materialized (task-d52), oldest first.
    inflight: VecDeque<InFlight>,
}

/// A group handed off to a pipelined store's materializer (task-d52).
struct InFlight {
    /// The position of the group's last command: the group has
    /// materialized once the projection has committed through it.
    position: coord_types::ids::ExecutionPosition,
    /// The rows the group's own batches write.
    own: Arc<CutOverlay>,
    /// Watch publications owed once it has materialized, in order.
    publications: Vec<Publication>,
}

/// Commands applied as one group: planned one after another, each over
/// what the ones before it wrote, and lowered together (task-d47).
#[derive(Default)]
struct Group {
    /// Every row the group's batches write, in order, a later write of a
    /// key winning: what a command planned after them reads through. On
    /// a pipelined store it starts from the rows of the groups still in
    /// flight (task-d52).
    overlay: Arc<CutOverlay>,
    /// The rows the group's own batches write, without those of the
    /// groups in flight before it (task-d52).
    own: Arc<CutOverlay>,
    /// Barriers of the group's batches.
    barriers: BTreeSet<BarrierId>,
    /// The application the group's last batch completes. The journal
    /// keeps the batches in order and the projection takes them in that
    /// order, so this one materialized means all of them did.
    last: Option<Pending>,
    /// Watch publications owed once the group has materialized, in order.
    publications: Vec<Publication>,
}

/// A watch publication held until its revision has materialized.
enum Publication {
    /// A revision this group produced, with its complete event set.
    Events {
        namespace: NamespaceId,
        revision: KvRevision,
        events: Vec<KvEvent>,
    },
    /// Every durable revision through this one, read back from storage.
    Through(KvRevision),
}

/// What a command is planned against: the projection's snapshot, with a
/// group's own unmaterialized writes over it.
type PlanningView<P> =
    GatedView<RecoveryCut<<<P as Persistence>::Reader as SnapshotSource>::View, Arc<CutOverlay>>>;

/// Whether `event` says one of `barriers` will never be journaled.
fn lost(event: &StorageEvent, barriers: &BTreeSet<BarrierId>) -> bool {
    matches!(
        event,
        StorageEvent::Failed {
            barrier_id,
            error: StorageError::DefinitelyNotCommitted,
        } if barriers.contains(barrier_id)
    )
}

/// How many lowerings finishing a group may wait through (as
/// `materialize::complete` waits for one application).
const GROUP_LOWERINGS: usize = 64;

impl<P: Persistence> Applier<P> {
    /// An applier over an opened store; the watch hub starts at the
    /// durable KV revision.
    ///
    /// `alloc` is moved to the application's half of the barrier
    /// sequences ([`BarrierAllocator::for_application`]), so no barrier
    /// of this applier's is also one of the protocol machine's that
    /// shares its store and its boot.
    pub fn new(store: P, alloc: BarrierAllocator) -> Result<Self, EngineError> {
        let (published, floor) = {
            let gated = store.reader().snapshot().map_err(|_| {
                EngineError::new(coord_store_api::engine::ErrorClass::Busy, "no durable view")
            })?;
            (
                crate::codecs::read_kv_revision(gated.view())?,
                crate::codecs::read_retention_floor(gated.view())?,
            )
        };
        Ok(Applier {
            store,
            alloc: alloc.for_application(),
            hub: WatchHub::new(published, floor),
            foreign: Vec::new(),
            sharing: false,
            group: None,
            inflight: VecDeque::new(),
        })
    }

    /// Keep the storage facts about other batches for
    /// [`Applier::take_foreign`]. A node whose protocol machine shares
    /// this store calls it; an applier that stands alone (a tool, a
    /// test) does not, and keeps nothing it would never hand back.
    pub fn share_foreign(&mut self) {
        self.sharing = true;
    }

    /// The storage facts about other batches -- the protocol's votes,
    /// promises and adoptions -- that the store produced while this
    /// applier lowered or reconciled its own, in the order they came.
    ///
    /// Every one of them is owed to whoever submitted that batch. A
    /// lowering takes whatever is queued, and a reconcile settles
    /// whatever was uncertain, so the protocol's barriers become durable
    /// here as often as anywhere, and a send waiting on one that nobody
    /// was told of waits until a new ballot or a restart.
    pub fn take_foreign(&mut self) -> Vec<coord_core::event::StorageEvent> {
        let mut foreign = core::mem::take(&mut self.foreign);
        foreign.retain(|event| {
            !event
                .barrier()
                .is_some_and(|barrier| coord_core::outbox::is_application(&barrier))
        });
        foreign
    }

    /// Reconcile the store, keeping what it settles for other batches.
    ///
    /// Within a group, a batch of the group found definitely not journaled
    /// ends it: the commands it holds were already reported applied.
    fn reconcile(&mut self) -> Result<(), ApplyError> {
        let settled = self.store.reconcile()?;
        let group_lost = self
            .group
            .as_ref()
            .is_some_and(|group| settled.events.iter().any(|e| lost(e, &group.barriers)));
        if self.sharing {
            self.foreign.extend(settled.events);
        }
        if group_lost {
            return Err(ApplyError::GroupLost);
        }
        Ok(())
    }

    /// Apply the commands that follow as one group (task-d47), when the
    /// store can chain them ([`Persistence::chains_applications`]).
    ///
    /// Each is planned over the snapshot with the group's own writes laid
    /// over it, and submitted; [`Applier::apply`] returns its outcome
    /// without lowering anything. [`Applier::finish_group`] then lowers
    /// the group, one journal write and one projection transaction for
    /// all of it within the journal's group bounds, and only then
    /// publishes its revisions to watches. Until then nothing it applied
    /// is readable, and its caller must disclose none of it.
    ///
    /// Returns whether commands are now being grouped.
    pub fn begin_group(&mut self) -> bool {
        if self.group.is_none() && self.store.chains_applications() {
            let mut group = Group::default();
            // The projection may not hold the groups still in flight yet,
            // so a command planned now reads through their rows as well,
            // oldest first. A snapshot that already holds some of them
            // reads the same: those rows are the ones it holds, and every
            // later write is laid over them again.
            if !self.inflight.is_empty() {
                let mut overlay = CutOverlay::new();
                for flight in &self.inflight {
                    overlay.absorb(&flight.own);
                }
                group.overlay = Arc::new(overlay);
            }
            self.group = Some(group);
        }
        self.group.is_some()
    }

    /// Whether a group is open ([`Applier::begin_group`]).
    pub const fn in_group(&self) -> bool {
        self.group.is_some()
    }

    /// Batches applied in the open group and not lowered yet.
    pub fn grouped(&self) -> usize {
        self.group.as_ref().map_or(0, |group| group.barriers.len())
    }

    /// Lower the open group until every batch in it has materialized, then
    /// publish its revisions to watches in order.
    ///
    /// A batch of the group definitely not journaled is
    /// [`ApplyError::GroupLost`]: its command was reported applied, so it
    /// cannot be planned again here. A batch journaled whose projection
    /// is still owed is not a failure; lowering goes on.
    pub fn finish_group(&mut self) -> Result<(), ApplyError> {
        let Some(group) = self.group.take() else {
            return Ok(());
        };
        if let Some(last) = group.last {
            let mut materialized = false;
            for _ in 0..GROUP_LOWERINGS {
                if self.materialized_position()? >= last.position {
                    materialized = true;
                    break;
                }
                let lowered = self.store.lower()?;
                let produced = lowered.events.len();
                let group_lost = lowered.events.iter().any(|e| lost(e, &group.barriers));
                if self.sharing {
                    self.foreign.extend(lowered.events);
                }
                if group_lost {
                    return Err(ApplyError::GroupLost);
                }
                if lowered.indeterminate {
                    let settled = self.store.reconcile()?;
                    let group_lost = settled.events.iter().any(|e| lost(e, &group.barriers));
                    if self.sharing {
                        self.foreign.extend(settled.events);
                    }
                    if group_lost {
                        return Err(ApplyError::GroupLost);
                    }
                } else if produced == 0
                    && self.store.queued() == 0
                    && self.store.unmaterialized() == 0
                    && self.materialized_position()? < last.position
                {
                    return Err(ApplyError::Engine(EngineError::new(
                        coord_store_api::engine::ErrorClass::Corrupt,
                        "a group's last batch was neither materialized nor rejected",
                    )));
                }
            }
            if !materialized && self.materialized_position()? < last.position {
                return Err(ApplyError::Engine(EngineError::new(
                    coord_store_api::engine::ErrorClass::Busy,
                    "a group did not materialize within its bound",
                )));
            }
        }
        // Groups handed off before this one have materialized with it
        // (a pipelined store's lowering takes its commits back first), and
        // their revisions go out before this group's (task-d52).
        if !self.inflight.is_empty() {
            let drained = self.store.drain()?;
            self.take_lowered(drained, &group.barriers)?;
            self.publish_materialized()?;
        }
        for publication in group.publications {
            match publication {
                Publication::Events {
                    namespace,
                    revision,
                    events,
                } => self.publish(namespace, revision, &events)?,
                Publication::Through(revision) => self.publish_through(revision)?,
            }
        }
        Ok(())
    }

    /// Whether the store's projection commits leave this thread
    /// (task-d52, [`Persistence::pipelined`]).
    pub fn pipelined(&self) -> bool {
        self.store.pipelined()
    }

    /// Groups handed off and not yet known to have materialized.
    pub fn in_flight(&self) -> usize {
        self.inflight.len()
    }

    /// Journal the open group and hand its projection commit to the
    /// pipelined store's materializer, without waiting for it (task-d52).
    ///
    /// Returns the execution position the projection must commit before
    /// what the group's commands led to may be disclosed: the group's
    /// last command, or, for a group that submitted nothing, the last of
    /// the groups still in flight. `None` when nothing is owed. Its watch
    /// publications are held to the same position, and go out from
    /// [`Applier::settle`].
    ///
    /// A store that is not pipelined materializes the group here, as
    /// [`Applier::finish_group`] does, and returns `None`.
    pub fn hand_off_group(
        &mut self,
    ) -> Result<Option<coord_types::ids::ExecutionPosition>, ApplyError> {
        if !self.store.pipelined() {
            self.finish_group()?;
            return Ok(None);
        }
        let Some(group) = self.group.take() else {
            return Ok(None);
        };
        let Some(last) = group.last else {
            // Nothing of its own to materialize; what it publishes may
            // still name a revision a group in flight wrote.
            if let Some(flight) = self.inflight.back_mut() {
                flight.publications.extend(group.publications);
                return Ok(Some(flight.position));
            }
            self.publish_now(group.publications)?;
            return Ok(None);
        };
        // A flush journals a staged group with the protocol's batches;
        // a group closed between flushes is journaled here. One append
        // takes what fits a journal group, so this goes on until the
        // queue is empty or an append moves nothing.
        let mut attempts = self.store.queued() + 1;
        while self.store.queued() > 0 && attempts > 0 {
            let before = self.store.queued();
            let journaled = self.store.journal()?;
            self.take_lowered(journaled, &group.barriers)?;
            if self.store.queued() >= before {
                break;
            }
            attempts -= 1;
        }
        self.inflight.push_back(InFlight {
            position: last.position,
            own: group.own,
            publications: group.publications,
        });
        let handed = self.store.hand_off()?;
        self.take_lowered(handed, &group.barriers)?;
        self.publish_materialized()?;
        Ok(Some(last.position))
    }

    /// Take back the projection commits a pipelined store's materializer
    /// has finished, without waiting, hand it the next, and publish the
    /// revisions of every group that has now materialized (task-d52).
    ///
    /// Returns the execution position the projection has committed:
    /// whatever was held to a position at or below it may go out.
    pub fn settle(&mut self) -> Result<coord_types::ids::ExecutionPosition, ApplyError> {
        if self.store.pipelined() {
            let handed = self.store.hand_off()?;
            self.take_lowered(handed, &BTreeSet::new())?;
        }
        self.publish_materialized()
    }

    /// Wait until every group handed off has materialized (task-d52):
    /// the projection commit out is waited for, and what was journaled
    /// meanwhile is committed after it.
    pub fn drain(&mut self) -> Result<coord_types::ids::ExecutionPosition, ApplyError> {
        if self.store.pipelined() {
            for _ in 0..GROUP_LOWERINGS {
                let drained = self.store.drain()?;
                self.take_lowered(drained, &BTreeSet::new())?;
                if self.store.unmaterialized() == 0 {
                    break;
                }
                let handed = self.store.hand_off()?;
                self.take_lowered(handed, &BTreeSet::new())?;
            }
        }
        let through = self.publish_materialized()?;
        if let Some(flight) = self.inflight.front() {
            return Err(ApplyError::Engine(EngineError::new(
                coord_store_api::engine::ErrorClass::Busy,
                format!(
                    "a group handed off at position {:?} did not materialize while draining",
                    flight.position
                ),
            )));
        }
        Ok(through)
    }

    /// Publish the revisions of the groups in flight that the projection
    /// has now committed, oldest first, and forget those groups. Returns
    /// the position it has committed through.
    fn publish_materialized(&mut self) -> Result<coord_types::ids::ExecutionPosition, ApplyError> {
        let through = self.store.materialized_through()?;
        while self
            .inflight
            .front()
            .is_some_and(|flight| flight.position <= through)
        {
            let flight = self.inflight.pop_front().expect("checked above");
            self.publish_now(flight.publications)?;
        }
        Ok(through)
    }

    /// Hand the facts a lowering produced for other batches on, settle an
    /// outcome in doubt, and refuse one that lost a batch of `barriers`.
    fn take_lowered(
        &mut self,
        lowered: crate::persistence::Lowered,
        barriers: &BTreeSet<BarrierId>,
    ) -> Result<(), ApplyError> {
        let group_lost = lowered.events.iter().any(|e| lost(e, barriers));
        if self.sharing {
            self.foreign.extend(lowered.events);
        }
        if group_lost {
            return Err(ApplyError::GroupLost);
        }
        if lowered.indeterminate {
            let settled = self.store.reconcile()?;
            let group_lost = settled.events.iter().any(|e| lost(e, barriers));
            if self.sharing {
                self.foreign.extend(settled.events);
            }
            if group_lost {
                return Err(ApplyError::GroupLost);
            }
        }
        Ok(())
    }

    /// Publish revisions that have materialized, in order.
    fn publish_now(&mut self, publications: Vec<Publication>) -> Result<(), ApplyError> {
        for publication in publications {
            match publication {
                Publication::Events {
                    namespace,
                    revision,
                    events,
                } => self.publish_to_hub(namespace, revision, &events)?,
                Publication::Through(revision) => self.publish_stored_through(revision)?,
            }
        }
        Ok(())
    }

    /// The execution position the projection has materialized through.
    fn materialized_position(&self) -> Result<coord_types::ids::ExecutionPosition, ApplyError> {
        Ok(self
            .store
            .reader()
            .snapshot()?
            .meta()
            .frontier
            .execution_position)
    }

    /// The view a command is planned against. Outside a group, the
    /// projection's snapshot. Within one, the same snapshot with the
    /// group's writes over it, at the frontier the group has reached, so
    /// the plan's base is the queued frontier the store will check.
    fn planning_view(&self) -> Result<PlanningView<P>, ApplyError> {
        let gated = self.store.reader().snapshot()?;
        let mut meta = *gated.meta();
        let overlay = match &self.group {
            Some(group) => {
                let base = self.store.application_base();
                meta.frontier = ExecutionFrontier {
                    configuration: base.configuration,
                    execution_position: base.execution_position,
                };
                group.overlay.clone()
            }
            None => Arc::new(CutOverlay::new()),
        };
        let durable = gated.meta().stamp.journal_seq();
        Ok(GatedView::new(
            RecoveryCut::new(gated, overlay, durable),
            meta,
        ))
    }

    /// Make `plan` durable as its own batch, or, within a group, submit it
    /// and add it to the group. `refused` is the command a refusal records
    /// as executed ([`apply_refused_plan_sharing`]).
    fn land(
        &mut self,
        barrier: BarrierId,
        namespace: NamespaceId,
        plan: &coord_state::ApplyPlan,
        binding: Option<&RetryBinding>,
        refused: Option<&CommandId>,
    ) -> Result<ApplyOutcome, ApplyError> {
        if self.group.is_none() {
            let others = self.sharing.then_some(&mut self.foreign);
            return Ok(match refused {
                Some(command) => apply_refused_plan_sharing(
                    &mut self.store,
                    barrier,
                    namespace,
                    plan,
                    command,
                    others,
                ),
                None => {
                    apply_plan_sharing(&mut self.store, barrier, namespace, plan, binding, others)
                }
            }?);
        }
        let (mut batch, kind, pending) = prepare(barrier, namespace, plan, binding)?;
        if let Some(command) = refused {
            batch.updates.push(crate::retry::executed_update(
                command,
                plan.position,
                plan.revision,
                pending.result_digest,
            )?);
        }
        // A group's batches stay queued until it is lowered, so the queue
        // can fill -- in bytes as well as in batches -- before the group's
        // count does. Refused as full, the batch would fail the voter; the
        // caller lowers instead and comes back. With nothing queued and
        // nothing grouped, lowering would free nothing, and the submission
        // below reports what is wrong.
        let lowerable =
            self.group.as_ref().is_some_and(|g| g.last.is_some()) || self.store.queued() > 0;
        if lowerable && !self.store.has_room(&batch) {
            return Err(ApplyError::GroupFull);
        }
        let updates = batch.updates.clone();
        match crate::materialize::submit(&mut self.store, batch, kind)? {
            Submitted::Accepted => {
                let group = self.group.as_mut().expect("checked above");
                Arc::make_mut(&mut group.overlay).extend(&updates);
                Arc::make_mut(&mut group.own).extend(&updates);
                group.barriers.insert(barrier);
                group.last = Some(pending);
                Ok(ApplyOutcome::Applied(Vec::new()))
            }
            Submitted::Replan => Ok(ApplyOutcome::Replan),
            Submitted::Indeterminate => Ok(ApplyOutcome::Indeterminate),
        }
    }

    /// Announce `revision`'s complete event set to watches, once it is
    /// durable: now, or within a group once the group has materialized.
    fn publish(
        &mut self,
        namespace: NamespaceId,
        revision: KvRevision,
        events: &[KvEvent],
    ) -> Result<(), ApplyError> {
        let publication = Publication::Events {
            namespace,
            revision,
            events: events.to_vec(),
        };
        if let Some(group) = self.group.as_mut() {
            group.publications.push(publication);
            return Ok(());
        }
        // Behind a group in flight, whose revisions come first (task-d52).
        if let Some(flight) = self.inflight.back_mut() {
            flight.publications.push(publication);
            return Ok(());
        }
        self.publish_to_hub(namespace, revision, events)
    }

    /// Announce a materialized revision to watches now.
    fn publish_to_hub(
        &mut self,
        namespace: NamespaceId,
        revision: KvRevision,
        events: &[KvEvent],
    ) -> Result<(), ApplyError> {
        match self.hub.publish(namespace, revision, events) {
            Ok(()) => Ok(()),
            // The hub is behind, because an earlier revision was
            // established by reconciliation rather than by this path.
            // Replay the durable events of everything it missed; a gap is
            // never left behind, since every later publication would be
            // refused.
            Err(PublishError::Gap { .. }) => self.publish_stored_through(revision),
            Err(e) => Err(ApplyError::Publish(e)),
        }
    }

    /// The watch hub fed by this applier.
    pub const fn hub(&self) -> &WatchHub {
        &self.hub
    }

    /// Where its batches become durable.
    pub const fn store(&self) -> &P {
        &self.store
    }

    /// The same, mutably (administration batches).
    pub const fn store_mut(&mut self) -> &mut P {
        &mut self.store
    }

    /// Give the store back; this boot's application side ends.
    ///
    /// The watch hub goes with it. A hub belongs to one boot's view of
    /// the applied revision, and handing it to the next boot would let a
    /// subscriber carry a position across a recovery that may not have
    /// reached the same place.
    pub fn into_store(self) -> P {
        self.store
    }

    /// The barrier allocator.
    pub const fn alloc(&mut self) -> &mut BarrierAllocator {
        &mut self.alloc
    }

    /// Apply `command` from its durable payload.
    pub fn apply(
        &mut self,
        command: CommandId,
        payload: &PayloadRecordV1,
    ) -> Result<AppliedOutcome, ApplyError> {
        // Outside a group a command is planned over the projection alone,
        // so whatever a pipelined store still has in flight lands first.
        if self.group.is_none() && !self.inflight.is_empty() {
            self.drain()?;
        }
        let request: LogicalRequest =
            postcard::from_bytes(&payload.logical).map_err(|_| ApplyError::MalformedPayload)?;
        let actual = CommandId::derive(&payload.retry_key, &request)
            .map_err(|_| ApplyError::MalformedPayload)?;
        if actual != command {
            return Err(ApplyError::IdentityMismatch {
                expected: command,
                actual,
            });
        }
        let binding = RetryBinding {
            retry_key: payload.retry_key,
            command_id: command,
            // Resolved per attempt against the view the attempt plans
            // against, in `apply_bound`. Nothing is retired for a
            // command the frontend originated.
            retires: None,
        };
        // Which planner runs is decided by the admission the command was
        // *accepted* under, never by the operation alone.
        //
        // The operation is the caller's; the admission is the
        // verifier's, bound into the accepted command and recovered here
        // from its own durable record. Reading the operation first and
        // trusting it to say "this creates a session" would let a
        // payload select the path that reads a principal. Reading the
        // admission first means an establishing receipt authorizes
        // exactly one action, and that action exists for nothing else.
        match (&request.operation, payload.admission) {
            (CanonicalOperation::ConsumeAdmission, Some(facts))
                if facts.purpose() == AdmissionPurpose::Establish =>
            {
                self.apply_establishment(&request, &binding, &facts)
            }
            // An establishing admission with any other operation, and
            // the establishing operation under anything else: neither
            // authorizes the other.
            (CanonicalOperation::ConsumeAdmission, _) => self.apply_refusal(
                command,
                request.namespace,
                RejectionReason::AdmissionMismatch,
            ),
            // A service operation, accepted with no admission because
            // no verifier admitted one: this is a voter's own proposal,
            // and it is the only kind of command that arrives without a
            // receipt. Every collector submission carries one, so a
            // caller naming one of these falls through to the arm below
            // and is refused whatever its session may do.
            (operation, None) if Self::is_service_operation(operation) => {
                let internal = coord_state::service_command(&request).expect("a service operation");
                self.apply_internal(request.namespace, &internal, &binding)
            }
            (operation, Some(_)) if Self::is_service_operation(operation) => self.apply_refusal(
                command,
                request.namespace,
                RejectionReason::AdmissionMismatch,
            ),
            (_, Some(facts)) if facts.purpose() == AdmissionPurpose::Establish => self
                .apply_refusal(
                    command,
                    request.namespace,
                    RejectionReason::AdmissionMismatch,
                ),
            // An ordinary submission. The receipt attests that the caller
            // is bound to the session the retry key names; a receipt
            // naming another session admits nothing under this one.
            (_, Some(facts)) if facts.attested.session != payload.retry_key.session_id => self
                .apply_refusal(
                    command,
                    request.namespace,
                    RejectionReason::AdmissionMismatch,
                ),
            _ => self.apply_bound(&request, &binding, payload.ack_through),
        }
    }

    /// Whether `operation` is one of the service's own.
    ///
    /// Named here rather than derived from `service_command`, because
    /// the dispatch has to answer it for an operation alone, before it
    /// decides which planner runs.
    fn is_service_operation(operation: &CanonicalOperation) -> bool {
        matches!(
            operation,
            CanonicalOperation::EstablishLeaseAuthority { .. }
                | CanonicalOperation::ExpireLease { .. }
        )
    }

    /// Create the session an accepted establishment receipt attests.
    ///
    /// This is the whole of session establishment: consuming the
    /// receipt, writing the session row and binding the outcome to the
    /// retry key happen in one batch, so a crash leaves either all of it
    /// or none of it, and the caller's credential is released only after
    /// that batch is durable.
    ///
    /// Nothing here consults a clock. The credential deadline the
    /// receipt carries was checked once, at admission; a replica
    /// applying the command -- now, or on a replay years later --
    /// decides only what current replicated policy says, which is what
    /// [`plan_internal`] rechecks.
    fn apply_establishment(
        &mut self,
        request: &LogicalRequest,
        binding: &RetryBinding,
        facts: &AdmissionFacts,
    ) -> Result<AppliedOutcome, ApplyError> {
        let namespace = request.namespace;
        // The receipt names the session it creates and the retry key
        // names the session its record belongs to. They must be the same
        // session, or the command would establish one identity while
        // recording its outcome under another.
        let Some(receipt) = AdmissionReceiptV1::of(facts) else {
            return self.apply_refusal(
                binding.command_id,
                namespace,
                RejectionReason::AdmissionMismatch,
            );
        };
        if receipt.session != binding.retry_key.session_id {
            return self.apply_refusal(
                binding.command_id,
                namespace,
                RejectionReason::AdmissionMismatch,
            );
        }
        let internal = InternalCommand::ConsumeAdmission {
            namespace,
            receipt,
            // Login grants are consumed by the exchange that issues the
            // credential, not by the binding that presents it.
            code: None,
            refresh_family: None,
            window: SESSION_RETRY_WINDOW,
        };
        self.apply_internal(namespace, &internal, binding)
    }

    /// Execute one internal command: the service's own transition,
    /// planned against replicated state and bound to its invocation.
    ///
    /// The ordinary admission path cannot answer for one of these. It
    /// asks a session whether the command may run, and an internal
    /// command has no session -- it is the service's, admitted by
    /// nobody, and what decides whether it changes anything is the
    /// planner rechecking every condition it carries against current
    /// replicated state. So a redelivery is resolved from the retry
    /// record directly, and everything else is the same batch an
    /// ordinary command gets: outcome, retry binding and state in one.
    fn apply_internal(
        &mut self,
        namespace: NamespaceId,
        internal: &InternalCommand,
        binding: &RetryBinding,
    ) -> Result<AppliedOutcome, ApplyError> {
        for _ in 0..8 {
            let gated = self.planning_view()?;
            match retry::lookup(gated.view(), &binding.retry_key)? {
                Some(record) if record.command_id == binding.command_id => {
                    if let Some(revision) = record.revision {
                        self.publish_through(revision)?;
                    }
                    return Ok(AppliedOutcome {
                        position: record.position,
                        revision: record.revision,
                        result_digest: record.result_digest,
                        response: record.response.clone(),
                    });
                }
                Some(_) => {
                    drop(gated);
                    return self.apply_refusal(
                        binding.command_id,
                        namespace,
                        RejectionReason::RetryConflict,
                    );
                }
                None => {}
            }
            let planned = match build_internal_view(&gated, internal, ViewBudget::SCHEMA) {
                Ok(view) => match plan_internal(internal, &view, &PlanLimits::default()) {
                    Ok(planned) => planned,
                    Err(e) => match e.terminal() {
                        Some(reason) => rejection_plan(&view, reason).map_err(ApplyError::Plan)?,
                        None => return Err(ApplyError::Plan(e)),
                    },
                },
                Err(ViewBuildError::BudgetExceeded) => rejection_plan_at(
                    self.store.application_base(),
                    crate::codecs::read_kv_revision(gated.view())?,
                    RejectionReason::ViewTooLarge,
                )
                .map_err(ApplyError::Plan)?,
                Err(e) => return Err(ApplyError::View(e)),
            };
            drop(gated);
            let barrier = self.alloc.allocate();
            // The retry binding rides in the same batch as whatever the
            // command changed -- a session row and a consumed receipt,
            // an authority epoch, a deleted key -- so the outcome is
            // recoverable exactly when the change is.
            match self.land(barrier, namespace, &planned, Some(binding), None)? {
                ApplyOutcome::Applied(_) => {
                    let response = postcard::to_allocvec(&planned.response)
                        .map_err(|_| ApplyError::MalformedPayload)?;
                    return Ok(AppliedOutcome {
                        position: planned.position,
                        revision: planned.revision,
                        result_digest: retry::result_digest(&response),
                        response,
                    });
                }
                ApplyOutcome::Replan => continue,
                ApplyOutcome::Indeterminate => {
                    self.reconcile()?;
                }
            }
        }
        Err(ApplyError::Diverged)
    }

    /// Execute `reason` as this command's whole result: it takes the
    /// position that is already its own, records that `command` executed,
    /// records nothing under the retry key, and changes nothing else.
    fn apply_refusal(
        &mut self,
        command: CommandId,
        namespace: NamespaceId,
        reason: RejectionReason,
    ) -> Result<AppliedOutcome, ApplyError> {
        for _ in 0..8 {
            let gated = self.planning_view()?;
            let planned = rejection_plan_at(
                self.store.application_base(),
                crate::codecs::read_kv_revision(gated.view())?,
                reason,
            )
            .map_err(ApplyError::Plan)?;
            drop(gated);
            let barrier = self.alloc.allocate();
            match self.land(barrier, namespace, &planned, None, Some(&command))? {
                ApplyOutcome::Applied(_) => {
                    let response = postcard::to_allocvec(&planned.response)
                        .map_err(|_| ApplyError::MalformedPayload)?;
                    return Ok(AppliedOutcome {
                        position: planned.position,
                        revision: None,
                        result_digest: retry::result_digest(&response),
                        response,
                    });
                }
                ApplyOutcome::Replan => continue,
                ApplyOutcome::Indeterminate => {
                    self.reconcile()?;
                }
            }
        }
        Err(ApplyError::Diverged)
    }

    /// The terminal outcome of a semantic admission refusal.
    fn admission_rejection_of(admission: &Admission) -> RejectionReason {
        match admission {
            Admission::Conflict { .. } => RejectionReason::RetryConflict,
            Admission::TooOld { .. } => RejectionReason::RetryTooOld,
            Admission::OutOfWindow { .. } => RejectionReason::RetryOutOfWindow,
            Admission::UnknownSession | Admission::SessionRetired => {
                RejectionReason::SessionInvalid
            }
            Admission::Unauthorized => RejectionReason::RetryUnauthorized,
            // Handled above; never a refusal.
            Admission::New | Admission::Retry(_) => RejectionReason::Invalid,
        }
    }

    fn apply_bound(
        &mut self,
        request: &LogicalRequest,
        binding: &RetryBinding,
        payload_ack: u64,
    ) -> Result<AppliedOutcome, ApplyError> {
        let namespace: NamespaceId = request.namespace;
        let session = binding.retry_key.session_id;
        for _ in 0..8 {
            let gated = self.planning_view()?;
            // A retained result is handed back only if the request would
            // still be authorized now: losing a permission protects what
            // it produced, exactly as it would a fresh execution.
            let authorized = |record: &crate::codecs::RetryRecordV1| {
                retained_is_authorized(gated.view(), &session, request, record)
            };
            // What this command's acknowledgement retires, resolved
            // against the same view the plan is built on and applied in
            // the same batch. A client that is never told its results
            // were received cannot be told to stop asking for them:
            // nothing else advances the floor, so a client instance
            // would serve exactly one window of requests and then be
            // refused for ever. It is resolved before admission because
            // admission measures the window from the floor this very
            // acknowledgement establishes: the frame that first crosses
            // a filled window is the one that acknowledges everything
            // in it, and refusing it would leave the window shut for
            // good.
            let retires = retry::retirement(gated.view(), &binding.retry_key, payload_ack)?;
            let binding = &RetryBinding {
                retires,
                ..binding.clone()
            };
            match retry::admit(gated.view(), binding, authorized)? {
                Admission::New => {}
                Admission::Retry(record) => {
                    // The command already executed: either earlier, or by
                    // the commit that reconciliation established after an
                    // indeterminate acknowledgement. Its revision is
                    // durable but may never have reached the hub, so it is
                    // published before the outcome is reported.
                    if let Some(revision) = record.revision {
                        self.publish_through(revision)?;
                    }
                    return Ok(AppliedOutcome {
                        position: record.position,
                        revision: record.revision,
                        result_digest: record.result_digest,
                        response: record.response.clone(),
                    });
                }
                // Every chosen command finishes. A semantic admission
                // refusal is an outcome, not a reason to leave the
                // command unexecuted: the position is already its own,
                // every successor conflicts with it, and retrying can
                // only refuse it again. The refusal takes the position
                // and records that the command executed, and records
                // nothing under the retry key, so the original binding
                // stands and a retired request is not resurrected.
                other => {
                    let reason = Self::admission_rejection_of(&other);
                    let planned = rejection_plan_at(
                        self.store.application_base(),
                        crate::codecs::read_kv_revision(gated.view())?,
                        reason,
                    )
                    .map_err(ApplyError::Plan)?;
                    drop(gated);
                    let barrier = self.alloc.allocate();
                    match self.land(
                        barrier,
                        namespace,
                        &planned,
                        None,
                        Some(&binding.command_id),
                    )? {
                        ApplyOutcome::Applied(_) => {
                            let response = postcard::to_allocvec(&planned.response)
                                .map_err(|_| ApplyError::MalformedPayload)?;
                            return Ok(AppliedOutcome {
                                position: planned.position,
                                revision: None,
                                result_digest: retry::result_digest(&response),
                                response,
                            });
                        }
                        ApplyOutcome::Replan => continue,
                        ApplyOutcome::Indeterminate => {
                            self.reconcile()?;
                            continue;
                        }
                    }
                }
            }
            // The budget here is the schema's, identical on every replica,
            // never a local setting: a command that overruns it overruns
            // it everywhere, so the rejection below is a replicated result
            // and not this node's resource limit leaking into the history.
            let built =
                build_authorized_view(&gated, namespace, &session, request, ViewBudget::SCHEMA);
            let planned = match built {
                Ok(view) => match plan(request, &view, &PlanLimits::default()) {
                    Ok(planned) => planned,
                    Err(e) => match e.terminal() {
                        // A command already chosen to execute cannot be
                        // left unexecuted because the request is impossible
                        // against this state: retrying can only reject it
                        // again, and every successor conflicts with it. The
                        // rejection is its result, taking its execution
                        // position, durable and retry-resolvable, changing
                        // nothing.
                        Some(reason) => rejection_plan(&view, reason).map_err(ApplyError::Plan)?,
                        None => return Err(ApplyError::Plan(e)),
                    },
                },
                // The same argument covers a command whose view cannot be
                // built at all: the state it would have to read is beyond
                // the schema's budget, retrying reads the same state, and
                // returning an error here would leave the command forever
                // unexecuted with every successor waiting behind it.
                Err(ViewBuildError::BudgetExceeded) => rejection_plan_at(
                    self.store.application_base(),
                    crate::codecs::read_kv_revision(gated.view())?,
                    RejectionReason::ViewTooLarge,
                )
                .map_err(ApplyError::Plan)?,
                // An engine failure is this node's, not the request's.
                Err(e) => return Err(ApplyError::View(e)),
            };
            drop(gated);
            let barrier = self.alloc.allocate();
            match self.land(barrier, namespace, &planned, Some(binding), None)? {
                ApplyOutcome::Applied(_) => {
                    if let Some(revision) = planned.revision {
                        // Irrevocable now: the revision's complete event set
                        // becomes visible to watches only at this point (a
                        // group's, once the group has materialized).
                        self.publish(namespace, revision, &planned.events)?;
                    }
                    let response = postcard::to_allocvec(&planned.response)
                        .map_err(|_| ApplyError::MalformedPayload)?;
                    return Ok(AppliedOutcome {
                        position: planned.position,
                        revision: planned.revision,
                        result_digest: retry::result_digest(&response),
                        response,
                    });
                }
                ApplyOutcome::Replan => continue,
                ApplyOutcome::Indeterminate => {
                    self.reconcile()?;
                }
            }
        }
        Err(ApplyError::Diverged)
    }

    /// Publish every durable revision the hub has not announced yet, up to
    /// and including `through`, reading each revision's complete event set
    /// from storage. The hub requires contiguous revisions, so a revision
    /// established outside the applying path has to reach it this way
    /// before anything later can be published.
    fn publish_through(&mut self, through: KvRevision) -> Result<(), ApplyError> {
        // Within a group the revision may be one the group wrote, which
        // storage does not hold yet: it is published once the group has
        // materialized.
        if let Some(group) = self.group.as_mut() {
            group.publications.push(Publication::Through(through));
            return Ok(());
        }
        if let Some(flight) = self.inflight.back_mut() {
            flight.publications.push(Publication::Through(through));
            return Ok(());
        }
        self.publish_stored_through(through)
    }

    /// [`Applier::publish_through`] now, from what storage holds.
    fn publish_stored_through(&mut self, through: KvRevision) -> Result<(), ApplyError> {
        loop {
            let published = self.hub.published();
            if published >= through {
                return Ok(());
            }
            let next = published
                .checked_next()
                .map_err(|_| ApplyError::Plan(PlanError::CounterOverflow))?;
            let stored = {
                let gated = self.store.reader().snapshot()?;
                crate::views::stored_events_at(gated.view(), next)?
            };
            let events = stored.unwrap_or_default();
            // Every event of one revision belongs to the command that
            // produced it, so they share a namespace.
            let namespace = events.first().map_or(NamespaceId([0; 16]), |e| e.namespace);
            let batch: Vec<_> = events.into_iter().map(|e| e.event).collect();
            self.hub
                .publish(namespace, next, &batch)
                .map_err(ApplyError::Publish)?;
        }
    }

    /// Durable KV revision.
    pub fn kv_revision(&self) -> Result<KvRevision, EngineError> {
        let gated = self.store.reader().snapshot().map_err(|_| {
            EngineError::new(coord_store_api::engine::ErrorClass::Busy, "no durable view")
        })?;
        crate::codecs::read_kv_revision(gated.view())
    }

    /// The retry key of a payload (convenience for drivers).
    pub fn retry_key_of(payload: &PayloadRecordV1) -> RetryKey {
        payload.retry_key
    }
}

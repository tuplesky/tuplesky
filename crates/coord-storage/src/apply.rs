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
use coord_core::outbox::BarrierAllocator;
use coord_state::{
    PlanError, PlanLimits, RejectionReason, authorize_retained, plan, rejection_plan,
    rejection_plan_at,
};
use coord_store_api::engine::EngineError;
use coord_types::ids::{KvRevision, NamespaceId};
use coord_types::logical_v1::LogicalRequest;
use coord_types::{CommandId, RetryKey};

use crate::materialize::{ApplyOutcome, apply_plan};
use crate::persistence::Persistence;
use crate::retry::{self, Admission, RetryBinding};
use crate::view::ViewError;
use crate::views::{ViewBudget, ViewBuildError, build_authorized_view, load_authorization};
use crate::watch::{PublishError, WatchHub};

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
}

impl<P: Persistence> Applier<P> {
    /// An applier over an opened store; the watch hub starts at the
    /// durable KV revision.
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
            alloc,
            hub: WatchHub::new(published, floor),
        })
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
        };
        self.apply_bound(&request, &binding)
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
    ) -> Result<AppliedOutcome, ApplyError> {
        let namespace: NamespaceId = request.namespace;
        let session = binding.retry_key.session_id;
        for _ in 0..8 {
            let gated = self.store.reader().snapshot()?;
            // A retained result is handed back only if the request would
            // still be authorized now: losing a permission protects what
            // it produced, exactly as it would a fresh execution.
            let authorized = |record: &crate::codecs::RetryRecordV1| {
                let Ok(auth) =
                    load_authorization(gated.view(), namespace, &session, ViewBudget::default())
                else {
                    return false;
                };
                let Ok(stored) = postcard::from_bytes::<coord_state::Response>(&record.response)
                else {
                    return false;
                };
                authorize_retained(&auth, &namespace, request, &stored.outcome).is_ok()
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
                // and records nothing under the retry key, so the
                // original binding stands and a retired request is not
                // resurrected.
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
                    match apply_plan(&mut self.store, barrier, namespace, &planned, None)? {
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
                            self.store.reconcile()?;
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
            match apply_plan(&mut self.store, barrier, namespace, &planned, Some(binding))? {
                ApplyOutcome::Applied(_) => {
                    if let Some(revision) = planned.revision {
                        // Irrevocable now: the revision's complete event set
                        // becomes visible to watches only at this point.
                        match self.hub.publish(namespace, revision, &planned.events) {
                            Ok(()) => {}
                            // The hub is behind, because an earlier
                            // revision was established by reconciliation
                            // rather than by this path. Replay the durable
                            // events of everything it missed; a gap is
                            // never left behind, since every later
                            // publication would be refused.
                            Err(PublishError::Gap { .. }) => self.publish_through(revision)?,
                            Err(e) => return Err(ApplyError::Publish(e)),
                        }
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
                    self.store.reconcile()?;
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

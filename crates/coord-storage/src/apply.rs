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
use coord_state::{PlanError, PlanLimits, plan};
use coord_store_api::engine::{EngineError, LocalEngine};
use coord_types::ids::{KvRevision, NamespaceId};
use coord_types::logical_v1::LogicalRequest;
use coord_types::{CommandId, RetryKey};

use crate::materialize::{ApplyOutcome, apply_plan};
use crate::retry::{self, Admission, RetryBinding};
use crate::view::ViewError;
use crate::views::{ViewBudget, ViewBuildError, build_authorized_view};
use crate::watch::WatchHub;
use crate::worker::StoreWorker;

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

/// The application side of one replica.
pub struct Applier<E: LocalEngine> {
    worker: StoreWorker<E>,
    alloc: BarrierAllocator,
    hub: WatchHub,
}

impl<E: LocalEngine> Applier<E> {
    /// An applier over an opened worker; the watch hub starts at the
    /// durable KV revision.
    pub fn new(worker: StoreWorker<E>, alloc: BarrierAllocator) -> Result<Self, EngineError> {
        let (published, floor) = {
            let gated = worker.reader().snapshot().map_err(|_| {
                EngineError::new(coord_store_api::engine::ErrorClass::Busy, "no durable view")
            })?;
            (
                crate::codecs::read_kv_revision(gated.view())?,
                crate::codecs::read_retention_floor(gated.view())?,
            )
        };
        Ok(Applier {
            worker,
            alloc,
            hub: WatchHub::new(published, floor),
        })
    }

    /// The watch hub fed by this applier.
    pub const fn hub(&self) -> &WatchHub {
        &self.hub
    }

    /// The worker.
    pub const fn worker(&self) -> &StoreWorker<E> {
        &self.worker
    }

    /// The worker, mutably (administration batches).
    pub const fn worker_mut(&mut self) -> &mut StoreWorker<E> {
        &mut self.worker
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

    fn apply_bound(
        &mut self,
        request: &LogicalRequest,
        binding: &RetryBinding,
    ) -> Result<AppliedOutcome, ApplyError> {
        let namespace: NamespaceId = request.namespace;
        let session = binding.retry_key.session_id;
        for _ in 0..8 {
            let gated = self.worker.reader().snapshot()?;
            match retry::admit(gated.view(), binding)? {
                Admission::New => {}
                Admission::Retry(record) => {
                    return Ok(AppliedOutcome {
                        position: record.position,
                        revision: record.revision,
                        result_digest: record.result_digest,
                    });
                }
                other => return Err(ApplyError::NotAdmitted(other)),
            }
            let view =
                build_authorized_view(&gated, namespace, &session, request, ViewBudget::default())
                    .map_err(ApplyError::View)?;
            let planned = plan(request, &view, &PlanLimits::default()).map_err(ApplyError::Plan)?;
            drop(gated);
            let barrier = self.alloc.allocate();
            match apply_plan(
                &mut self.worker,
                barrier,
                namespace,
                &planned,
                Some(binding),
            )? {
                ApplyOutcome::Applied(_) => {
                    if let Some(revision) = planned.revision {
                        // Irrevocable now: the revision's complete event set
                        // becomes visible to watches only at this point.
                        let _ = self.hub.publish(namespace, revision, &planned.events);
                    }
                    let response = postcard::to_allocvec(&planned.response)
                        .map_err(|_| ApplyError::MalformedPayload)?;
                    return Ok(AppliedOutcome {
                        position: planned.position,
                        revision: planned.revision,
                        result_digest: retry::result_digest(&response),
                    });
                }
                ApplyOutcome::Replan => continue,
                ApplyOutcome::Indeterminate => {
                    self.worker.reconcile()?;
                }
            }
        }
        Err(ApplyError::Diverged)
    }

    /// Durable KV revision.
    pub fn kv_revision(&self) -> Result<KvRevision, EngineError> {
        let gated = self.worker.reader().snapshot().map_err(|_| {
            EngineError::new(coord_store_api::engine::ErrorClass::Busy, "no durable view")
        })?;
        crate::codecs::read_kv_revision(gated.view())
    }

    /// The retry key of a payload (convenience for drivers).
    pub fn retry_key_of(payload: &PayloadRecordV1) -> RetryKey {
        payload.retry_key
    }
}

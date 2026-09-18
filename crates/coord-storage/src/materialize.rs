//! Lowering an `ApplyPlan` into one atomic batch and applying it through
//! the worker (design Section 17.4).

use coord_core::effect::{BarrierId, PersistBatch, StoreUpdate};
use coord_core::event::{StorageError, StorageEvent};
use coord_state::{ApplyPlan, Mutation};
use coord_store_api::engine::{EngineError, LocalEngine};
use coord_store_api::registry::{Collection, meta_fields};
use coord_types::ids::NamespaceId;

use crate::retry::RetryBinding;

use crate::codecs;
use crate::worker::{StoreWorker, SubmitError};

/// Lower a plan into the immutable batch materialization will apply:
/// current rows, history versions, complete events of the revision, lease
/// index rows, the KV revision frontier, the retention floor and, when the
/// command has a stable invocation identity, its retry record and executed
/// identity. The batch carries the plan's base so the worker rechecks it in
/// the transaction.
pub fn plan_to_batch(
    barrier: BarrierId,
    namespace: NamespaceId,
    plan: &ApplyPlan,
    binding: Option<&RetryBinding>,
) -> Result<PersistBatch, EngineError> {
    let mut updates = Vec::new();
    if let Some(binding) = binding {
        let response = postcard::to_allocvec(&plan.response).map_err(|_| {
            EngineError::new(
                coord_store_api::engine::ErrorClass::Limit,
                "response encode",
            )
        })?;
        updates.extend(crate::retry::binding_updates(
            binding,
            plan.position,
            plan.revision,
            response,
        )?);
    }
    let revision = plan.revision;
    for m in &plan.mutations {
        match m {
            Mutation::Write { key, entry } => {
                let rev = revision.expect("write implies a revision");
                updates.push(StoreUpdate {
                    collection: Collection::KvCurrentV1.id(),
                    key: codecs::current_key(&namespace, key),
                    value: Some(codecs::encode_current(entry)?),
                });
                updates.push(StoreUpdate {
                    collection: Collection::KvHistoryV1.id(),
                    key: codecs::history_key(&namespace, key, rev),
                    value: Some(codecs::encode_history(&codecs::HistoryRecordV1 {
                        entry: Some(entry.clone()),
                    })?),
                });
            }
            Mutation::Delete { key, .. } => {
                let rev = revision.expect("delete implies a revision");
                updates.push(StoreUpdate {
                    collection: Collection::KvCurrentV1.id(),
                    key: codecs::current_key(&namespace, key),
                    value: None,
                });
                updates.push(StoreUpdate {
                    collection: Collection::KvHistoryV1.id(),
                    key: codecs::history_key(&namespace, key, rev),
                    value: Some(codecs::encode_history(&codecs::HistoryRecordV1 {
                        entry: None,
                    })?),
                });
            }
            Mutation::LeaseAttach { lease, key } => {
                updates.push(StoreUpdate {
                    collection: Collection::LeaseKeysV1.id(),
                    key: codecs::lease_key(lease, &namespace, key),
                    value: Some(codecs::encode_lease_key()?),
                });
            }
            Mutation::LeaseDetach { lease, key } => {
                updates.push(StoreUpdate {
                    collection: Collection::LeaseKeysV1.id(),
                    key: codecs::lease_key(lease, &namespace, key),
                    value: None,
                });
            }
            Mutation::CompactTo { revision } => {
                updates.push(StoreUpdate {
                    collection: Collection::MetaV1.id(),
                    key: meta_fields::RETENTION_FLOOR.to_vec(),
                    value: Some(codecs::encode_counter(revision.get())?),
                });
            }
        }
    }
    if let Some(rev) = revision {
        for (ordinal, event) in plan.events.iter().enumerate() {
            let ordinal = u32::try_from(ordinal).map_err(|_| {
                EngineError::new(
                    coord_store_api::engine::ErrorClass::Limit,
                    "too many events",
                )
            })?;
            updates.push(StoreUpdate {
                collection: Collection::EventsV1.id(),
                key: codecs::event_key(rev, ordinal),
                value: Some(codecs::encode_event(&codecs::event_record(
                    namespace, event,
                ))?),
            });
        }
        updates.push(StoreUpdate {
            collection: Collection::MetaV1.id(),
            key: meta_fields::KV_REVISION.to_vec(),
            value: Some(codecs::encode_counter(rev.get())?),
        });
    }
    Ok(PersistBatch {
        barrier,
        base: Some(plan.base),
        updates,
    })
}

/// Outcome of applying one plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// Durably applied; the storage facts to feed the machine.
    Applied(Vec<StorageEvent>),
    /// The plan's base no longer matches the durable frontier: rebuild the
    /// view and plan again.
    Replan,
    /// The commit outcome is unknown; reconcile before anything else.
    Indeterminate,
}

/// Apply one plan through the worker as its own bounded group.
pub fn apply_plan<E: LocalEngine>(
    worker: &mut StoreWorker<E>,
    barrier: BarrierId,
    namespace: NamespaceId,
    plan: &ApplyPlan,
    binding: Option<&RetryBinding>,
) -> Result<ApplyOutcome, EngineError> {
    let batch = plan_to_batch(barrier, namespace, plan, binding)?;
    match worker.submit(batch) {
        Ok(()) => {}
        Err(SubmitError::NotReady(_)) => return Ok(ApplyOutcome::Indeterminate),
        Err(e) => {
            return Err(EngineError::new(
                coord_store_api::engine::ErrorClass::Limit,
                format!("submit refused: {e:?}"),
            ));
        }
    }
    // Flush until this plan's own barrier is resolved: a flush lowers one
    // bounded group, so older queued work may go first and the plan stays
    // queued (not durable) after the first flush. An absent event is never
    // success.
    let mut events = Vec::new();
    loop {
        let outcome = worker.flush()?;
        if outcome.indeterminate {
            return Ok(ApplyOutcome::Indeterminate);
        }
        let mine = outcome.events.iter().any(|e| e.barrier() == Some(barrier));
        let rejected = outcome.events.iter().any(|e| matches!(e, StorageEvent::Failed { barrier_id, error: StorageError::DefinitelyNotCommitted } if *barrier_id == barrier));
        events.extend(outcome.events);
        if rejected {
            return Ok(ApplyOutcome::Replan);
        }
        if mine {
            return Ok(ApplyOutcome::Applied(events));
        }
        if worker.queued() == 0 {
            return Err(EngineError::new(
                coord_store_api::engine::ErrorClass::Corrupt,
                "plan barrier was neither committed nor rejected",
            ));
        }
    }
}

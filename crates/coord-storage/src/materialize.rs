//! Lowering an `ApplyPlan` into one atomic batch and applying it through
//! the worker (design Section 17.4).

use coord_core::effect::{BarrierId, PersistBatch, StoreUpdate};
use coord_core::event::{StorageError, StorageEvent};
use coord_state::{ApplyPlan, Mutation};
use coord_store_api::engine::EngineError;
use coord_store_api::registry::{Collection, meta_fields};
use coord_types::identity::Digest32;
use coord_types::ids::{ExecutionPosition, KvRevision, NamespaceId};

use crate::retry::RetryBinding;

use crate::codecs;
use crate::journaled::TransitionKind;
use crate::persistence::{Persistence, Refused};

/// Lower a plan into the immutable batch materialization will apply:
/// current rows, history versions, complete events of the revision, lease
/// records and reverse-index rows, the KV revision frontier, the retention floor and, when the
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
            Mutation::LeaseAttach {
                lease,
                key,
                generation,
                mod_revision,
            } => {
                updates.push(StoreUpdate {
                    collection: Collection::LeaseKeysV1.id(),
                    key: codecs::lease_key(lease, &namespace, key),
                    value: Some(codecs::encode_lease_key(&codecs::LeaseKeyRecordV1 {
                        generation: *generation,
                        mod_revision: *mod_revision,
                    })?),
                });
            }
            Mutation::LeaseWrite { lease, record } => {
                updates.push(StoreUpdate {
                    collection: Collection::LeaseV1.id(),
                    key: codecs::lease_row_key(lease),
                    value: Some(codecs::encode_lease(record)?),
                });
            }
            Mutation::LeaseDetach { lease, key } => {
                updates.push(StoreUpdate {
                    collection: Collection::LeaseKeysV1.id(),
                    key: codecs::lease_key(lease, &namespace, key),
                    value: None,
                });
            }
            Mutation::SessionWrite { session, record } => {
                updates.push(StoreUpdate {
                    collection: Collection::SessionV1.id(),
                    key: codecs::session_key(session),
                    value: record.as_ref().map(codecs::encode_session).transpose()?,
                });
            }
            Mutation::GrantWrite { commitment, record } => {
                updates.push(StoreUpdate {
                    collection: Collection::AuthGrantV1.id(),
                    key: codecs::grant_key(commitment),
                    value: Some(codecs::encode_grant(record)?),
                });
            }
            Mutation::PolicyRuleWrite {
                principal,
                rule,
                record,
            } => {
                updates.push(StoreUpdate {
                    collection: Collection::PolicyV1.id(),
                    key: codecs::policy_rule_key(principal, rule),
                    value: record
                        .as_ref()
                        .map(codecs::encode_policy_rule)
                        .transpose()?,
                });
            }
            Mutation::TrustRuleWrite { rule, record } => {
                updates.push(StoreUpdate {
                    collection: Collection::PolicyV1.id(),
                    key: codecs::trust_rule_key(rule),
                    value: Some(codecs::encode_trust_rule(record)?),
                });
            }
            Mutation::LeaseAuthority { epoch } => {
                updates.push(StoreUpdate {
                    collection: Collection::MetaV1.id(),
                    key: meta_fields::LEASE_AUTHORITY.to_vec(),
                    value: Some(codecs::encode_counter(epoch.get())?),
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

/// An application that has been planned and submitted, and is waiting
/// for its own record to be materialized.
///
/// It exists because submitting is not applying. On the journal-first
/// path the record reaches the journal first and the projection
/// afterwards, so between those two moments the command is decided and
/// durable but has not happened to the state anyone can read. What the
/// caller is owed -- the outcome, and the events to publish -- is held
/// here until it has.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pending {
    /// The barrier whose materialization completes this application, and
    /// the only one that does.
    pub barrier: BarrierId,
    /// Execution position the command took.
    pub position: ExecutionPosition,
    /// KV revision it produced, if it mutated KV.
    pub revision: Option<KvRevision>,
    /// Digest of the exact result bytes it returned.
    pub result_digest: Digest32,
}

impl Pending {
    /// The outcome this application reports once it has materialized.
    pub const fn outcome(&self) -> crate::apply::AppliedOutcomeParts {
        crate::apply::AppliedOutcomeParts {
            position: self.position,
            revision: self.revision,
            result_digest: self.result_digest,
        }
    }
}

/// The transition a plan records.
///
/// It is derived from the plan and not from the batch: the journal's
/// record of *what happened* is the execution's own account of itself --
/// the position it took, the revision it produced and the digest of the
/// exact result it returned -- and reading that back out of the rows the
/// batch happens to contain would make the journal's meaning depend on
/// the shape of a row.
fn transition_of(plan: &ApplyPlan) -> Result<(TransitionKind, Digest32), EngineError> {
    let response = postcard::to_allocvec(&plan.response).map_err(|e| {
        EngineError::new(
            coord_store_api::engine::ErrorClass::Limit,
            format!("response not encodable: {e:?}"),
        )
    })?;
    let result_digest = crate::retry::result_digest(&response);
    Ok((
        TransitionKind::Application {
            position: plan.position,
            revision: plan.revision,
            result_digest,
        },
        result_digest,
    ))
}

/// Turn `plan` into the exact immutable batch that will be recorded, and
/// the transition it is recorded as.
///
/// This is the boundary both drivers share. Planning, admission and
/// lowering happen once, here; what differs between the reference and
/// journal-first paths is only where the batch then goes, which is why
/// neither of them re-plans.
pub fn prepare(
    barrier: BarrierId,
    namespace: NamespaceId,
    plan: &ApplyPlan,
    binding: Option<&RetryBinding>,
) -> Result<(PersistBatch, TransitionKind, Pending), EngineError> {
    let batch = plan_to_batch(barrier, namespace, plan, binding)?;
    let (kind, result_digest) = transition_of(plan)?;
    Ok((
        batch,
        kind,
        Pending {
            barrier,
            position: plan.position,
            revision: plan.revision,
            result_digest,
        },
    ))
}

/// Offer a prepared batch for durability.
pub fn submit<P: Persistence>(
    store: &mut P,
    batch: PersistBatch,
    kind: TransitionKind,
) -> Result<Submitted, EngineError> {
    match store.submit(batch, kind) {
        Ok(()) => Ok(Submitted::Accepted),
        Err(Refused::NotReady(_)) => Ok(Submitted::Indeterminate),
        // The frontier moved between planning and submitting. The
        // command would take a position that is no longer its own, so
        // the plan is rebuilt rather than the batch retried.
        Err(Refused::StaleBase) => Ok(Submitted::Replan),
        Err(e) => Err(EngineError::new(
            coord_store_api::engine::ErrorClass::Limit,
            format!("submit refused: {e}"),
        )),
    }
}

/// What offering a batch produced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Submitted {
    /// Taken; it now awaits its own materialization.
    Accepted,
    /// The base has moved: plan again.
    Replan,
    /// The outcome of earlier work is unknown; reconcile first.
    Indeterminate,
}

/// How many lowerings one application may wait through.
///
/// A projection that keeps refusing is a real condition, not a bug to
/// spin on: the record is durable either way, so the caller is told to
/// reconcile rather than held here indefinitely. The bound is generous
/// because a shared pipeline legitimately lowers other groups first.
const LOWERINGS: usize = 64;

/// Lower until `pending` has materialized, or is known not to have.
///
/// The completion rule is the matching barrier's own `Materialized`
/// event and nothing else. Three weaker rules are each wrong here and
/// each look right: a successful submission says only that the batch was
/// taken; a flush that did not fail says only that *something* was
/// lowered; and any event naming the barrier includes `JournalDurable`,
/// which on the journal-first path means the record is safe but the
/// state nobody can read yet. Reporting an outcome on any of those
/// publishes a revision the next reader will not find.
pub fn complete<P: Persistence>(
    store: &mut P,
    pending: &Pending,
) -> Result<ApplyOutcome, EngineError> {
    let mut events = Vec::new();
    for _ in 0..LOWERINGS {
        let lowered = store.lower()?;
        let produced = lowered.events.len();
        let mut materialized = false;
        let mut definitely_not = false;
        let mut uncertain = false;
        for event in &lowered.events {
            if event.barrier() != Some(pending.barrier) {
                continue;
            }
            match event {
                StorageEvent::Materialized { .. } => materialized = true,
                StorageEvent::Failed {
                    error: StorageError::DefinitelyNotCommitted,
                    ..
                } => definitely_not = true,
                // A record that is durable in the journal and whose
                // projection attempt failed has not failed: the
                // materialization is still owed, and reconciliation is
                // what establishes which. Treating it as a failure here
                // would replace a command that already happened.
                StorageEvent::Failed { .. } => uncertain = true,
                _ => {}
            }
        }
        events.extend(lowered.events);
        if materialized {
            return Ok(ApplyOutcome::Applied(events));
        }
        if definitely_not {
            return Ok(ApplyOutcome::Replan);
        }
        if uncertain || lowered.indeterminate {
            return Ok(ApplyOutcome::Indeterminate);
        }
        // Nothing of ours, and nothing moved. A record the projection
        // still owes will come; one that is owed by nobody will not.
        if produced == 0 && store.queued() == 0 && store.unmaterialized() == 0 {
            return Err(EngineError::new(
                coord_store_api::engine::ErrorClass::Corrupt,
                "the application's barrier was neither materialized nor rejected",
            ));
        }
    }
    // The record is durable and the projection has not taken it within
    // the bound. That is not a failure and not a command to plan again:
    // reconciliation is what settles which, and it settles it in favour
    // of the record that already exists.
    Ok(ApplyOutcome::Indeterminate)
}

/// Apply one plan through `store` as its own bounded group.
///
/// Prepare, submit, complete -- the composed form, for a caller that has
/// nothing else to do while the plan lands.
pub fn apply_plan<P: Persistence>(
    store: &mut P,
    barrier: BarrierId,
    namespace: NamespaceId,
    plan: &ApplyPlan,
    binding: Option<&RetryBinding>,
) -> Result<ApplyOutcome, EngineError> {
    let (batch, kind, pending) = prepare(barrier, namespace, plan, binding)?;
    match submit(store, batch, kind)? {
        Submitted::Accepted => complete(store, &pending),
        Submitted::Replan => Ok(ApplyOutcome::Replan),
        Submitted::Indeterminate => Ok(ApplyOutcome::Indeterminate),
    }
}

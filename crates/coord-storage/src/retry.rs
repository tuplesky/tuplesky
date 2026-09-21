//! Deduplication, result resolution and retry floors (task-12; design
//! Sections 6.5, 17.1, 10.5.3).
//!
//! The retry record, the executed identity and the application rows are
//! written in **one** batch, so a crash between materialization and the
//! notification of the caller cannot duplicate work: the next presentation
//! of the same retry key finds the record. Records are keyed by retry key
//! only; membership epochs, endpoints and tokens never enter them, so the
//! state transfers across epochs and replays exactly.
//!
//! Retention is bounded by the per-client floor: the client acknowledges
//! results through a sequence, records at or below it are deleted, and a
//! request at or below the floor is `TooOld`, never new work. Nothing here
//! promises exactly-once across a lost upstream invocation identity.
//!
//! Admission is checked against a snapshot, and the rows it depends on
//! (session state, retry records, floors, executed identities) change only
//! through application batches, in execution order. A bound command's own
//! batch carries the base its admission was checked at, so the worker's
//! frontier guard rejects it atomically when a retirement, a floor update or
//! a session change was ordered in between; the caller replans from a fresh
//! view and admission runs again. A stale retirement is rejected the same
//! way, so the floor never moves backward.

use coord_core::effect::StoreUpdate;
use coord_store_api::engine::{EngineError, OrderedRead};
use coord_store_api::registry::Collection;
use coord_types::identity::{CommandId, Digest32, HashDomain, RetryKey};
use coord_types::ids::{ClientInstanceId, RequestSequence, SessionId};
use serde::{Deserialize, Serialize};

use coord_state::policy::SessionRecord;

use crate::codecs::{self, ExecutedRecordV1, RetryFloorV1, RetryRecordV1};

/// Default outstanding window when a session does not specify one.
pub const DEFAULT_WINDOW: u32 = 1024;

/// The largest number of invocation identities one command's
/// acknowledgement retires.
///
/// A client acknowledges on every request, so the ordinary advance is
/// one sequence and this bound never binds. It exists for the client
/// that was stalled and then acknowledged a long prefix at once: without
/// it, that single command's batch would carry up to a whole window of
/// deletes. The floor still reaches the acknowledged value, a step per
/// command, because every request after it repeats the acknowledgement.
pub const MAX_RETIRE_PER_COMMAND: u64 = 64;

/// The prefix of a client's sequences that executing one command
/// retires: the floor it moves from and the floor it moves to. Resolved
/// against the view at execution, so it is a function of the durable
/// state and the accepted payload and therefore the same on every
/// replica.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Retirement {
    /// Floor the client's instance stands at.
    pub from: RequestSequence,
    /// Floor it moves to; strictly above `from`.
    pub through: RequestSequence,
    /// The window width to keep on the floor row. Carried rather than
    /// re-read, because the row this writes replaces the one the width
    /// came from.
    pub width: u32,
}

/// Binding of a plan to its invocation identity, persisted with it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryBinding {
    /// Stable invocation identity.
    pub retry_key: RetryKey,
    /// Command identity (retry key plus canonical payload).
    pub command_id: CommandId,
    /// What this command's acknowledgement retires, if anything.
    ///
    /// Resolved by [`retirement`] from the accepted payload's
    /// `ack_through` and the floor in the view at execution, and applied
    /// in the same atomic batch as the command: the floor moves exactly
    /// when the command that acknowledged it becomes durable, never
    /// before and never as a separate write that a crash could lose or
    /// duplicate.
    #[serde(default)]
    pub retires: Option<Retirement>,
}

/// Admission decision for a presented invocation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Admission {
    /// First presentation inside the window: execute as new work.
    New,
    /// Already executed: return the retained result, never re-execute.
    Retry(RetryRecordV1),
    /// Same retry key with a different payload.
    Conflict {
        /// Identity bound first.
        bound: CommandId,
    },
    /// At or below the retired floor.
    TooOld {
        /// Current floor.
        floor: RequestSequence,
    },
    /// Beyond the bounded outstanding window.
    OutOfWindow {
        /// Current floor.
        floor: RequestSequence,
        /// Window width.
        width: u32,
    },
    /// No such session; fail closed.
    UnknownSession,
    /// The session was retired; nothing executes under it.
    SessionRetired,
    /// Already executed, but current authorization no longer permits
    /// handing out the retained result (the session lost a permission the
    /// request needed); nothing re-executes either.
    Unauthorized,
}

/// Resolution of a previously submitted invocation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Resolution {
    /// The retained result.
    Result(RetryRecordV1),
    /// Inside the window but not executed (yet, or ever).
    Pending,
    /// Retired below the floor: the outcome is no longer retrievable here.
    Retired {
        /// Current floor.
        floor: RequestSequence,
    },
    /// Same retry key bound to another payload.
    Conflict {
        /// Identity bound.
        bound: CommandId,
    },
    /// Denied by current authorization (the operation still happened).
    Unauthorized,
    /// Session unknown or retired.
    NoSession,
}

fn session_state<V: OrderedRead>(
    view: &V,
    session: &SessionId,
) -> Result<Option<SessionRecord>, EngineError> {
    match view.get(Collection::SessionV1.id(), &codecs::session_key(session))? {
        Some(bytes) => Ok(Some(codecs::decode_session(&bytes)?)),
        None => Ok(None),
    }
}

/// Whether a session record may execute now: active, and its trust rule
/// enabled at the generation it was admitted under (Section 9.3).
pub fn record_executable<V: OrderedRead>(
    view: &V,
    record: &SessionRecord,
) -> Result<bool, EngineError> {
    if !record.active {
        return Ok(false);
    }
    match view.get(
        Collection::PolicyV1.id(),
        &codecs::trust_rule_key(&record.trust_rule),
    )? {
        Some(bytes) => {
            let rule = codecs::decode_trust_rule(&bytes)?;
            Ok(rule.enabled && rule.generation == record.rule_generation)
        }
        None => Ok(false),
    }
}

/// Whether `session` may execute now (see [`record_executable`]); an
/// unknown session cannot. This is the authorization a retained result
/// requires: a retired session, or one whose rule was disabled or
/// regenerated, cannot read cached outcomes.
pub fn session_executable<V: OrderedRead>(
    view: &V,
    session: &SessionId,
) -> Result<bool, EngineError> {
    match session_state(view, session)? {
        Some(record) => record_executable(view, &record),
        None => Ok(false),
    }
}

/// Floor of a client instance (default when none was persisted).
pub fn floor<V: OrderedRead>(
    view: &V,
    session: &SessionId,
    client: &ClientInstanceId,
    default_width: u32,
) -> Result<RetryFloorV1, EngineError> {
    match view.get(
        Collection::RetryFloorV1.id(),
        &codecs::retry_floor_key(session, client),
    )? {
        Some(bytes) => codecs::decode_retry_floor(&bytes),
        None => Ok(RetryFloorV1 {
            floor: RequestSequence::ZERO,
            width: default_width,
        }),
    }
}

/// Retained record for a retry key.
pub fn lookup<V: OrderedRead>(
    view: &V,
    key: &RetryKey,
) -> Result<Option<RetryRecordV1>, EngineError> {
    match view.get(Collection::RetryV1.id(), &codecs::retry_key(key))? {
        Some(bytes) => Ok(Some(codecs::decode_retry(&bytes)?)),
        None => Ok(None),
    }
}

/// Decide how to treat a presented invocation. Checked at execution time
/// against the durable state, so retirement and session changes ordered
/// earlier are honored. A retained result is handed out only when
/// `authorize` accepts it under the current policy (see
/// [`coord_state::authorize_retained`]); otherwise the admission is
/// [`Admission::Unauthorized`].
pub fn admit<V: OrderedRead>(
    view: &V,
    binding: &RetryBinding,
    authorize: impl FnOnce(&RetryRecordV1) -> bool,
) -> Result<Admission, EngineError> {
    let key = &binding.retry_key;
    let session = match session_state(view, &key.session_id)? {
        None => return Ok(Admission::UnknownSession),
        Some(s) if !record_executable(view, &s)? => return Ok(Admission::SessionRetired),
        Some(s) => s,
    };
    let floor = floor(
        view,
        &key.session_id,
        &key.client_instance_id,
        session.window,
    )?;
    if key.request_sequence <= floor.floor {
        return Ok(Admission::TooOld { floor: floor.floor });
    }
    if key.request_sequence.get() - floor.floor.get() > u64::from(floor.width) {
        return Ok(Admission::OutOfWindow {
            floor: floor.floor,
            width: floor.width,
        });
    }
    match lookup(view, key)? {
        Some(record) if record.command_id == binding.command_id => {
            if authorize(&record) {
                Ok(Admission::Retry(record))
            } else {
                Ok(Admission::Unauthorized)
            }
        }
        Some(record) => Ok(Admission::Conflict {
            bound: record.command_id,
        }),
        None => Ok(Admission::New),
    }
}

/// Resolve an invocation's outcome, subject to current authorization for
/// reading the retained result.
pub fn resolve<V: OrderedRead>(
    view: &V,
    binding: &RetryBinding,
    authorize: impl FnOnce(&RetryRecordV1) -> bool,
) -> Result<Resolution, EngineError> {
    let key = &binding.retry_key;
    let session = match session_state(view, &key.session_id)? {
        Some(s) if record_executable(view, &s)? => s,
        _ => return Ok(Resolution::NoSession),
    };
    let floor = floor(
        view,
        &key.session_id,
        &key.client_instance_id,
        session.window,
    )?;
    if key.request_sequence <= floor.floor {
        return Ok(Resolution::Retired { floor: floor.floor });
    }
    match lookup(view, key)? {
        Some(record) if record.command_id != binding.command_id => Ok(Resolution::Conflict {
            bound: record.command_id,
        }),
        Some(record) => {
            if authorize(&record) {
                Ok(Resolution::Result(record))
            } else {
                Ok(Resolution::Unauthorized)
            }
        }
        None => Ok(Resolution::Pending),
    }
}

/// Digest of an encoded response.
pub fn result_digest(response: &[u8]) -> Digest32 {
    HashDomain::CommandResult.digest(&[response])
}

/// Rows binding a plan to its invocation: the retry record and the
/// executed identity. Added to the plan's batch by materialization.
pub fn binding_updates(
    binding: &RetryBinding,
    position: coord_types::ids::ExecutionPosition,
    revision: Option<coord_types::ids::KvRevision>,
    response: Vec<u8>,
) -> Result<Vec<StoreUpdate>, EngineError> {
    let digest = result_digest(&response);
    let record = RetryRecordV1 {
        command_id: binding.command_id,
        position,
        revision,
        response,
        result_digest: digest,
    };
    let executed = ExecutedRecordV1 {
        position,
        revision,
        result_digest: digest,
    };
    let mut updates = vec![
        StoreUpdate {
            collection: Collection::RetryV1.id(),
            key: codecs::retry_key(&binding.retry_key),
            value: Some(codecs::encode_retry(&record)?),
        },
        StoreUpdate {
            collection: Collection::ExecutedV1.id(),
            key: codecs::executed_key(&binding.command_id),
            value: Some(codecs::encode_executed(&executed)?),
        },
    ];
    if let Some(retires) = binding.retires {
        updates.extend(retirement_updates(&binding.retry_key, retires)?);
    }
    Ok(updates)
}

/// Rows applying a [`Retirement`]: the retained result of every sequence
/// it covers is removed and the client's floor is written.
///
/// The removals are unconditional rather than checked against the view.
/// A sequence in the range either has a retained result, in which case
/// this is the removal, or it has none -- a sequence allocated and never
/// executed, or one whose result an earlier retirement already took --
/// in which case removing it is a no-op in every engine. Reading first
/// to find out would cost a lookup per sequence to decide something the
/// write settles.
fn retirement_updates(
    key: &RetryKey,
    retires: Retirement,
) -> Result<Vec<StoreUpdate>, EngineError> {
    let mut updates = Vec::new();
    let mut seq = retires.from;
    while seq < retires.through {
        seq = seq.checked_next().map_err(|_| {
            EngineError::new(
                coord_store_api::engine::ErrorClass::Limit,
                "sequence exhausted",
            )
        })?;
        let retired = RetryKey {
            request_sequence: seq,
            ..*key
        };
        updates.push(StoreUpdate {
            collection: Collection::RetryV1.id(),
            key: codecs::retry_key(&retired),
            value: None,
        });
    }
    updates.push(StoreUpdate {
        collection: Collection::RetryFloorV1.id(),
        key: codecs::retry_floor_key(&key.session_id, &key.client_instance_id),
        value: Some(codecs::encode_retry_floor(&RetryFloorV1 {
            floor: retires.through,
            width: retires.width,
        })?),
    });
    Ok(updates)
}

/// What a command acknowledging `ack_through` retires, against the
/// floor in `view`: `None` when it retires nothing.
///
/// Every bound is applied here rather than trusted from the payload,
/// because the payload is the caller's:
///
/// * A client cannot acknowledge its own invocation or anything after
///   it -- it has not received those results, by construction -- so the
///   acknowledgement is capped at the sequence below this command's.
/// * An acknowledgement at or below the floor retires nothing.
/// * One command retires at most [`MAX_RETIRE_PER_COMMAND`] sequences;
///   the rest follow on the next command, which repeats the same
///   acknowledgement.
///
/// The result is a function of the durable view and the accepted
/// payload, so every replica computes the same retirement for the same
/// command at the same position.
pub fn retirement<V: OrderedRead>(
    view: &V,
    key: &RetryKey,
    ack_through: u64,
) -> Result<Option<Retirement>, EngineError> {
    if ack_through == 0 {
        return Ok(None);
    }
    let Some(state) = session_state(view, &key.session_id)? else {
        return Ok(None);
    };
    let current = floor(view, &key.session_id, &key.client_instance_id, state.window)?;
    // Nothing a client may acknowledge reaches its own sequence.
    let capped = ack_through.min(key.request_sequence.get().saturating_sub(1));
    let capped = capped.min(current.floor.get().saturating_add(MAX_RETIRE_PER_COMMAND));
    if capped <= current.floor.get() {
        return Ok(None);
    }
    let Ok(through) = RequestSequence::new(capped) else {
        return Ok(None);
    };
    Ok(Some(Retirement {
        from: current.floor,
        through,
        width: current.width,
    }))
}

/// Rows retiring every retained result of the client at or below
/// `through` and advancing its floor. The caller only issues this after the
/// client acknowledged receipt of every result through that sequence, and
/// applies the rows as an application batch based at the frontier of
/// `view`, so a retirement computed from an older snapshot than one already
/// applied is rejected rather than lowering the floor. The window width
/// comes from the session record when no floor row exists yet.
pub fn retire_updates<V: OrderedRead>(
    view: &V,
    session: &SessionId,
    client: &ClientInstanceId,
    through: RequestSequence,
) -> Result<Vec<StoreUpdate>, EngineError> {
    let state = session_state(view, session)?.ok_or_else(|| {
        EngineError::new(
            coord_store_api::engine::ErrorClass::Unsupported,
            "unknown session",
        )
    })?;
    let current = floor(view, session, client, state.window)?;
    let mut updates = Vec::new();
    if through <= current.floor {
        return Ok(updates);
    }
    // Sequences beyond the active window were never admitted, so an
    // acknowledgement past it is malformed: reject it instead of scanning
    // every skipped sequence and advancing the floor past unissued work.
    if through.get() - current.floor.get() > u64::from(current.width) {
        return Err(EngineError::new(
            coord_store_api::engine::ErrorClass::Unsupported,
            "acknowledgement beyond the active window",
        ));
    }
    let mut seq = current.floor;
    while seq < through {
        seq = seq.checked_next().map_err(|_| {
            EngineError::new(
                coord_store_api::engine::ErrorClass::Limit,
                "sequence exhausted",
            )
        })?;
        let key = RetryKey {
            cluster_id: coord_types::ids::ClusterId([0; 16]),
            domain_id: coord_types::ids::DomainId([0; 16]),
            session_id: *session,
            client_instance_id: *client,
            request_sequence: seq,
        };
        if view
            .get(Collection::RetryV1.id(), &codecs::retry_key(&key))?
            .is_some()
        {
            updates.push(StoreUpdate {
                collection: Collection::RetryV1.id(),
                key: codecs::retry_key(&key),
                value: None,
            });
        }
    }
    updates.push(StoreUpdate {
        collection: Collection::RetryFloorV1.id(),
        key: codecs::retry_floor_key(session, client),
        value: Some(codecs::encode_retry_floor(&RetryFloorV1 {
            floor: through,
            width: current.width,
        })?),
    });
    Ok(updates)
}

/// Row activating (or retiring) a session with the given window. task-18
/// replaces this with the full session record; the key and collection stay.
pub fn session_update(
    session: &SessionId,
    record: &SessionRecord,
) -> Result<StoreUpdate, EngineError> {
    Ok(StoreUpdate {
        collection: Collection::SessionV1.id(),
        key: codecs::session_key(session),
        value: Some(codecs::encode_session(record)?),
    })
}

/// The semantic response limit must fit the envelope that stores the
/// retained result, with room for the rest of the record.
///
/// `coord-state` sets the limit and cannot see the envelope; this is
/// where both are visible, so this is where they are held together. A
/// response the planner accepts and storage cannot persist leaves a
/// chosen command unresolved, which is the failure this prevents.
const _: () = {
    // Command identity, execution position, optional revision, result
    // digest and postcard framing, generously.
    const RECORD_OVERHEAD: usize = 12 * 1024;
    assert!(
        coord_state::limits::MAX_RETAINED_RESPONSE_BYTES + RECORD_OVERHEAD
            <= coord_store_api::envelope::MAX_ENVELOPE_PAYLOAD,
        "a planner-valid response must fit its retry envelope"
    );
};

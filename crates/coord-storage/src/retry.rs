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

use coord_core::effect::StoreUpdate;
use coord_store_api::engine::{EngineError, OrderedRead};
use coord_store_api::registry::Collection;
use coord_types::identity::{CommandId, Digest32, HashDomain, RetryKey};
use coord_types::ids::{ClientInstanceId, RequestSequence, SessionId};
use serde::{Deserialize, Serialize};

use crate::codecs::{self, ExecutedRecordV1, RetryFloorV1, RetryRecordV1, SessionStateV1};

/// Default outstanding window when a session does not specify one.
pub const DEFAULT_WINDOW: u32 = 1024;

/// Binding of a plan to its invocation identity, persisted with it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryBinding {
    /// Stable invocation identity.
    pub retry_key: RetryKey,
    /// Command identity (retry key plus canonical payload).
    pub command_id: CommandId,
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
) -> Result<Option<SessionStateV1>, EngineError> {
    match view.get(Collection::SessionV1.id(), &codecs::session_key(session))? {
        Some(bytes) => Ok(Some(codecs::decode_session(&bytes)?)),
        None => Ok(None),
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
/// earlier are honored.
pub fn admit<V: OrderedRead>(view: &V, binding: &RetryBinding) -> Result<Admission, EngineError> {
    let key = &binding.retry_key;
    let session = match session_state(view, &key.session_id)? {
        None => return Ok(Admission::UnknownSession),
        Some(s) if !s.active => return Ok(Admission::SessionRetired),
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
        Some(record) if record.command_id == binding.command_id => Ok(Admission::Retry(record)),
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
        Some(s) if s.active => s,
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
    Ok(vec![
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
    ])
}

/// Rows retiring every retained result of the client at or below
/// `through` and advancing its floor. The caller only issues this after the
/// client acknowledged receipt of every result through that sequence. The
/// window width comes from the session record when no floor row exists yet.
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
    active: bool,
    window: u32,
) -> Result<StoreUpdate, EngineError> {
    Ok(StoreUpdate {
        collection: Collection::SessionV1.id(),
        key: codecs::session_key(session),
        value: Some(codecs::encode_session(&SessionStateV1 { active, window })?),
    })
}

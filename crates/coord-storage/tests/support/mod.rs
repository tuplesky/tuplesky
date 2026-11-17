//! What the storage tests share: a command's payload, put before the
//! command executes (task-d113, step 2).
//!
//! An execution moves its command's pending payload to `payload_v1` under
//! the position, and an execution with no pending payload is refused (rule
//! (b)). A node always holds the payload first -- a vote, the proposal
//! batch, a Sync's installation, a fetch or a catch-up install puts it --
//! but a test that drives the applier or the planner alone has no protocol
//! in front of it, so it puts the payload here, as the protocol would.
#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};

use coord_consensus::PayloadRecordV1;
use coord_consensus::rows::payload_update;
use coord_core::effect::{BarrierId, BootId, PersistBatch};
use coord_core::outbox::RUNTIME_BARRIERS;
use coord_storage::{Applier, ApplyError, Persistence, TransitionKind};
use coord_types::ids::ReplicaIncarnation;
use coord_types::logical_v1::LogicalRequest;
use coord_types::{CommandId, RetryKey};

/// The next barrier from the runtime's sequences, which no allocator the
/// tests make for the protocol or the application reaches. Every test
/// store is incarnation 1.
fn barrier(boot: BootId) -> BarrierId {
    static NEXT: AtomicU64 = AtomicU64::new(RUNTIME_BARRIERS);
    BarrierId {
        node_generation: ReplicaIncarnation::new(1).unwrap(),
        boot_id: boot,
        sequence: NEXT.fetch_add(1, Ordering::Relaxed),
    }
}

/// Put `payload` as `command`'s payload, in a protocol batch of its own,
/// lowered with the next batch the store lowers.
pub fn stage<P: Persistence>(store: &mut P, command: &CommandId, payload: &PayloadRecordV1) {
    let boot = store.boot();
    store
        .submit(
            PersistBatch {
                barrier: barrier(boot),
                base: None,
                updates: vec![payload_update(command, payload).unwrap()],
            },
            TransitionKind::Protocol,
        )
        .unwrap();
}

/// The payload of an invocation of `request` under `key`.
pub fn payload_of(key: RetryKey, request: &LogicalRequest) -> PayloadRecordV1 {
    PayloadRecordV1 {
        ack_through: 0,
        retry_key: key,
        logical: postcard::to_allocvec(request).unwrap(),
        admission: None,
    }
}

/// [`stage`] the payload of the invocation `command` is.
pub fn stage_request<P: Persistence>(
    store: &mut P,
    command: &CommandId,
    key: RetryKey,
    request: &LogicalRequest,
) {
    stage(store, command, &payload_of(key, request));
}

/// An applier that puts a command's payload before it applies it.
pub trait Staged {
    /// [`Applier::apply`], after [`stage`].
    fn apply_staged(
        &mut self,
        command: CommandId,
        payload: &PayloadRecordV1,
    ) -> Result<coord_consensus::AppliedOutcome, ApplyError>;
}

impl<P: Persistence> Staged for Applier<P> {
    fn apply_staged(
        &mut self,
        command: CommandId,
        payload: &PayloadRecordV1,
    ) -> Result<coord_consensus::AppliedOutcome, ApplyError> {
        stage(self.store_mut(), &command, payload);
        self.apply(command, payload)
    }
}

/// [`stage`] the payload of `binding`'s invocation of `request`, if the
/// plan is bound to one: only a bound plan writes an executed row.
pub fn stage_bound<P: Persistence>(
    store: &mut P,
    binding: Option<&coord_storage::RetryBinding>,
    request: &LogicalRequest,
) {
    if let Some(b) = binding {
        stage_request(store, &b.command_id, b.retry_key, request);
    }
}

/// [`stage`] each of `items`, and lower them: payloads in the store before
/// a group begins, so the group holds only its applications.
pub fn stage_all<P: Persistence>(store: &mut P, items: &[(CommandId, PayloadRecordV1)]) {
    for (command, payload) in items {
        stage(store, command, payload);
    }
    store.lower().unwrap();
}

//! Stable invocation identity (design Sections 4.4, 6.5): a client
//! instance identity allocated once, a monotonic request sequence, and a
//! retry key plus canonical payload fixed at first submission. Retries
//! reuse both verbatim; a different payload under the same sequence is a
//! conflict, and a restored instance continues its sequence rather than
//! reusing one.

use std::collections::BTreeMap;

use coord_types::ids::{ClientInstanceId, ClusterId, DomainId, RequestSequence, SessionId};
use coord_types::logical_v1::LogicalRequest;
use coord_types::wire_v1::{MessageV1, RequestV1};
use coord_types::{CommandId, RetryKey};
use serde::{Deserialize, Serialize};

/// One invocation: its identity and the exact bytes every retry sends.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Invocation {
    /// Retry key.
    pub retry_key: RetryKey,
    /// Command identity.
    pub command_id: CommandId,
    /// The `Request` frame, encoded once.
    pub frame: Vec<u8>,
}

/// The persistable state of a client instance.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceState {
    /// Cluster.
    pub cluster: ClusterId,
    /// Domain.
    pub domain: DomainId,
    /// Session.
    pub session: SessionId,
    /// Instance identity, allocated once.
    pub instance: ClientInstanceId,
    /// Next sequence to allocate (never reused).
    pub next_sequence: u64,
    /// Sequences whose payload is fixed, with their command identity.
    pub bound: BTreeMap<u64, Binding>,
    /// The floor this instance has acknowledged: every sequence at or
    /// below it reached a result here, so the domain may retire their
    /// invocation identities and let the outstanding window move on.
    /// Only the lifecycle's contiguous finished prefix advances it; a
    /// sequence still outstanding holds it where it is, which is what
    /// keeps the identity of a request that may yet be retried.
    #[serde(default)]
    pub acked: u64,
}

/// Why an invocation could not be built.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InvocationError {
    /// The logical request did not validate or canonicalize.
    Invalid,
    /// The sequence space is exhausted.
    Exhausted,
    /// A retry named a sequence bound to another payload.
    PayloadConflict {
        /// Sequence.
        sequence: u64,
    },
    /// A retry named a sequence never allocated here.
    UnknownSequence {
        /// Sequence.
        sequence: u64,
    },
    /// A retry named a sequence allocated with another deadline. The
    /// deadline is part of the frame but not of the command identity, so
    /// accepting it would send different bytes for the same invocation.
    DeadlineConflict {
        /// Sequence.
        sequence: u64,
        /// Deadline bound at allocation.
        bound: u32,
    },
}

/// What an allocated sequence is bound to: everything the original
/// request frame is built from besides the request itself, so a retry
/// after a restart reproduces that frame exactly rather than a frame
/// that merely shares its identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Binding {
    /// Identity derived at allocation.
    pub command_id: CommandId,
    /// Deadline the original frame carried.
    pub deadline_ms: u32,
    /// The acknowledged floor the original frame carried. Fixed at
    /// allocation and repeated on every retry: the floor a command
    /// retires is part of what the command durably is, so a retry that
    /// acknowledged more would be the same command asking the domain to
    /// retire a different prefix.
    #[serde(default)]
    pub ack_through: u64,
}

/// A client instance: the allocator of invocation identities.
#[derive(Clone, Debug)]
pub struct ClientInstance {
    state: InstanceState,
}

impl ClientInstance {
    /// A fresh instance whose identity the caller allocated (from the
    /// world's entropy, once per process).
    pub fn new(
        cluster: ClusterId,
        domain: DomainId,
        session: SessionId,
        instance: ClientInstanceId,
    ) -> Self {
        ClientInstance {
            state: InstanceState {
                cluster,
                domain,
                session,
                instance,
                next_sequence: 1,
                bound: BTreeMap::new(),
                acked: 0,
            },
        }
    }

    /// Restore a persisted instance: it continues its sequence.
    pub const fn restore(state: InstanceState) -> Self {
        ClientInstance { state }
    }

    /// The persistable state.
    pub const fn state(&self) -> &InstanceState {
        &self.state
    }

    /// The instance identity.
    pub const fn instance(&self) -> ClientInstanceId {
        self.state.instance
    }

    /// The session.
    pub const fn session(&self) -> SessionId {
        self.state.session
    }

    fn key(&self, sequence: u64) -> Option<RetryKey> {
        Some(RetryKey {
            cluster_id: self.state.cluster,
            domain_id: self.state.domain,
            session_id: self.state.session,
            client_instance_id: self.state.instance,
            request_sequence: RequestSequence::new(sequence).ok()?,
        })
    }

    /// Allocate a new invocation for `request` with `deadline_ms`.
    pub fn allocate(
        &mut self,
        request: &LogicalRequest,
        deadline_ms: u32,
    ) -> Result<Invocation, InvocationError> {
        let sequence = self.state.next_sequence;
        let key = self.key(sequence).ok_or(InvocationError::Exhausted)?;
        let ack_through = self.state.acked;
        let invocation = build(key, request, deadline_ms, ack_through)?;
        self.state.next_sequence = sequence.checked_add(1).ok_or(InvocationError::Exhausted)?;
        self.state.bound.insert(
            sequence,
            Binding {
                command_id: invocation.command_id,
                deadline_ms,
                ack_through,
            },
        );
        Ok(invocation)
    }

    /// Rebuild the invocation of an allocated `sequence` for a retry: the
    /// payload and the deadline must be the ones bound at allocation, so
    /// the frame a retry sends is byte for byte the one it repeats. The
    /// deadline is not part of the command identity, so a binding that
    /// recorded only the identity would pass the conflict check and still
    /// change the wire bytes and the collector's deadline under the same
    /// invocation.
    pub fn retry(
        &self,
        sequence: u64,
        request: &LogicalRequest,
        deadline_ms: u32,
    ) -> Result<Invocation, InvocationError> {
        let bound = self
            .state
            .bound
            .get(&sequence)
            .copied()
            .ok_or(InvocationError::UnknownSequence { sequence })?;
        if deadline_ms != bound.deadline_ms {
            return Err(InvocationError::DeadlineConflict {
                sequence,
                bound: bound.deadline_ms,
            });
        }
        let key = self.key(sequence).ok_or(InvocationError::Exhausted)?;
        let invocation = build(key, request, deadline_ms, bound.ack_through)?;
        if invocation.command_id != bound.command_id {
            return Err(InvocationError::PayloadConflict { sequence });
        }
        Ok(invocation)
    }

    /// Forget sequences at or below `floor` (acknowledged and retired),
    /// and record the floor so that the invocations allocated after this
    /// point tell the domain about it. The domain cannot retire what it
    /// was never told reached the caller, so a client that pruned its own
    /// bindings and said nothing would run out of window and stay there.
    pub fn retire(&mut self, floor: u64) {
        self.state.bound.retain(|s, _| *s > floor);
        self.state.acked = self.state.acked.max(floor);
    }

    /// The floor this instance has acknowledged.
    pub const fn acked(&self) -> u64 {
        self.state.acked
    }
}

fn build(
    key: RetryKey,
    request: &LogicalRequest,
    deadline_ms: u32,
    ack_through: u64,
) -> Result<Invocation, InvocationError> {
    let mut canonical = request.clone();
    canonical.canonicalize();
    let command_id = CommandId::derive(&key, &canonical).map_err(|_| InvocationError::Invalid)?;
    let wire = RequestV1::new(key, &canonical, deadline_ms, ack_through)
        .map_err(|_| InvocationError::Invalid)?;
    let frame = MessageV1::Request(wire)
        .encode()
        .map_err(|_| InvocationError::Invalid)?;
    Ok(Invocation {
        retry_key: key,
        command_id,
        frame,
    })
}

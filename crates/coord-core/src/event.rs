//! Owned input events with provenance.

use alloc::vec::Vec;

use coord_types::identity::Digest32;
use coord_types::ids::{LocalJournalSeq, ReplicaId, ReplicaIncarnation};
use serde::{Deserialize, Serialize};

use crate::capability::AdmissionReceipt;
use crate::effect::{BarrierId, BootId, TimerId};
use crate::machine::ClockSnapshot;

/// Storage failure classes reported to the machine.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StorageError {
    /// Definitely not committed (guard rejection before append).
    DefinitelyNotCommitted,
    /// Outcome unknown; reconcile from semantic records, never blind-retry.
    Indeterminate,
    /// Corruption or uncertain I/O; quarantine affected scope.
    Quarantine,
    /// Out of space.
    NoSpace,
}

/// Storage facts. None of them is protocol establishment.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StorageEvent {
    /// The batch behind `barrier` is durable in the journal.
    JournalDurable {
        /// Barrier.
        barrier_id: BarrierId,
        /// Local sequence assigned.
        journal_seq: LocalJournalSeq,
    },
    /// The batch has been materialized into the state projection.
    Materialized {
        /// Barrier.
        barrier_id: BarrierId,
        /// Sequence materialized through.
        journal_seq: LocalJournalSeq,
    },
    /// A local recovery checkpoint covering `journal_seq` is published.
    LocalCheckpointPublished {
        /// Checkpoint digest/identity.
        checkpoint_id: Digest32,
        /// Sequence represented.
        journal_seq: LocalJournalSeq,
    },
    /// The batch failed.
    Failed {
        /// Barrier.
        barrier_id: BarrierId,
        /// Failure class.
        error: StorageError,
    },
}

impl StorageEvent {
    /// Barrier this event completes, if any.
    pub const fn barrier(&self) -> Option<BarrierId> {
        match self {
            StorageEvent::JournalDurable { barrier_id, .. }
            | StorageEvent::Materialized { barrier_id, .. }
            | StorageEvent::Failed { barrier_id, .. } => Some(*barrier_id),
            StorageEvent::LocalCheckpointPublished { .. } => None,
        }
    }
}

/// Transport-issued proof that a frame arrived on an authenticated,
/// role-bound connection. Only the transport constructs it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PeerProvenance {
    from: ReplicaId,
    incarnation: ReplicaIncarnation,
    connection: u64,
}

impl PeerProvenance {
    /// Construct at the transport boundary after TLS, role and membership
    /// binding succeeded. Only transport crates may contain this call:
    /// `cargo xtask check-deps` scans every non-test crate for it and rejects
    /// any outside the reviewed allow list, so decoded bytes cannot be cast
    /// into an authenticated peer event elsewhere.
    pub const fn from_transport(
        from: ReplicaId,
        incarnation: ReplicaIncarnation,
        connection: u64,
    ) -> Self {
        PeerProvenance {
            from,
            incarnation,
            connection,
        }
    }
    /// Construct for evidence a voter running in this very process
    /// produced.
    ///
    /// It is the same proof for the same reason: the identity is not
    /// asserted by a frame, it is the committed identity of the voter
    /// instance the runtime is holding. What differs is only that the
    /// bytes did not cross a connection, so there is no connection
    /// identity to record.
    ///
    /// It is a *separate* constructor rather than `from_transport` with
    /// a made-up connection so that the two sources stay distinguishable
    /// in the source and each keeps its own reviewed allow list;
    /// `cargo xtask check-deps` names both.
    pub const fn from_local_voter(from: ReplicaId, incarnation: ReplicaIncarnation) -> Self {
        PeerProvenance {
            from,
            incarnation,
            // No connection carried it. Diagnostic only: every rule that
            // matters -- deduplication, quorum counting -- is by voter
            // identity, which is why local evidence can enter the same
            // path without a special case.
            connection: 0,
        }
    }
    /// Sender.
    pub const fn from(&self) -> ReplicaId {
        self.from
    }
    /// Sender incarnation.
    pub const fn incarnation(&self) -> ReplicaIncarnation {
        self.incarnation
    }
    /// Connection identity (diagnostic).
    pub const fn connection(&self) -> u64 {
        self.connection
    }
}

/// An authenticated peer message: decoded frame bytes plus provenance. There
/// is no way to build one from bytes alone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthenticatedPeerMessage {
    provenance: PeerProvenance,
    frame: Vec<u8>,
}

impl AuthenticatedPeerMessage {
    /// Bind a received frame to its provenance.
    pub fn new(provenance: PeerProvenance, frame: Vec<u8>) -> Self {
        AuthenticatedPeerMessage { provenance, frame }
    }
    /// Provenance.
    pub const fn provenance(&self) -> &PeerProvenance {
        &self.provenance
    }
    /// Frame bytes.
    pub fn frame(&self) -> &[u8] {
        &self.frame
    }
}

/// An admitted client request: receipt plus the opaque request frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmittedRequest {
    /// Admission receipt.
    pub receipt: AdmissionReceipt,
    /// Encoded `wire_v1` request frame.
    pub frame: Vec<u8>,
}

/// Owned events a machine consumes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// This boot started; the first event of every incarnation.
    Boot {
        /// Boot identity.
        boot_id: BootId,
        /// Incarnation.
        incarnation: ReplicaIncarnation,
    },
    /// Storage fact.
    Storage(StorageEvent),
    /// Timer fired (generation checked by the machine).
    Timer(TimerId),
    /// Injected clock reading.
    Clock(ClockSnapshot),
    /// Authenticated peer message.
    Peer(AuthenticatedPeerMessage),
    /// Admitted client request.
    Admitted(AdmittedRequest),
    /// An owned read view is ready.
    ViewReady {
        /// Correlation identity.
        request: u64,
        /// Serialized view rows (collection, key, value).
        rows: Vec<(u16, Vec<u8>, Vec<u8>)>,
    },
    /// Entropy delivered for an earlier request.
    Entropy {
        /// Correlation identity.
        request: u64,
        /// Bytes.
        bytes: [u8; 32],
    },
    /// A connection closed.
    ConnectionClosed {
        /// Connection identity.
        connection: u64,
    },
}

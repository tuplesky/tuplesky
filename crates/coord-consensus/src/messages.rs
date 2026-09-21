//! Protocol messages of this increment (prototype `MNewLeader`,
//! `MNewLeaderAckN` without its report; design Section 4.8). Encoded with
//! postcard for the logical outbox; the transport wraps them in wire
//! frames (task-30). Never a durable format.

use alloc::vec::Vec;

use coord_types::CommandId;
use coord_types::error::DecodeError;
use coord_types::identity::Digest32;
use coord_types::ids::{Ballot, ReplicaId};
use serde::{Deserialize, Serialize};

use crate::recovery::SyncDecision;
use crate::rows::PayloadRecordV1;
use crate::summary::ReportPage;
use crate::vote::{FastAck, SlowAck};

/// Per-key path digests through one command, in key order.
pub type PathAnchors = Vec<(Vec<u8>, Digest32)>;

/// How many payloads one [`ProtocolMessage::PayloadRequest`] asks for,
/// and how many one peer answers with.
///
/// The ask repeats until the content arrives, and every peer frame of a
/// domain shares one bounded lane. An unbounded ask therefore answers
/// itself: a replica missing a tableful of payloads asks for all of
/// them several times a second, the peer answers with that many payload
/// frames each time, the lane fills, and what it drops includes the
/// proposals and acknowledgements that would have let the replica catch
/// up -- so it falls further behind and asks for more. The bound is on
/// both sides, because neither side may be able to make the other
/// flood: a request for more than this is answered with this many.
pub const MAX_PAYLOAD_TRANSFER: usize = 8;

/// Whether an encoded protocol message is payload transfer.
///
/// Payload transfer is bulk. A replica catching up moves whole command
/// payloads, and it must not move them on the lane that carries the
/// proposals and acknowledgements the rest of the domain is waiting
/// for: that is the difference between a replica that catches up and
/// one that starves the domain while it tries. Sharing the lane is not
/// a theoretical risk -- it is what one voter of three did under a
/// benchmark, its catch-up traffic filling the control queue until the
/// frames it needed to catch up with were the ones being dropped.
///
/// Read from the encoded discriminant rather than by decoding, because
/// the caller is about to hand the frame to the transport and decoding
/// a payload to decide where to send it would cost more than the send.
/// `payload_transfer_is_recognized_from_the_encoded_discriminant` pins
/// these two bytes against the encoder, so a variant added above them
/// fails a test rather than quietly mis-routing.
pub fn is_payload_transfer(frame: &[u8]) -> bool {
    matches!(
        frame.first(),
        Some(&PAYLOAD_REQUEST_TAG | &PAYLOAD_RESPONSE_TAG)
    )
}

/// The encoded discriminant of [`ProtocolMessage::PayloadRequest`].
const PAYLOAD_REQUEST_TAG: u8 = 7;

/// The encoded discriminant of [`ProtocolMessage::PayloadResponse`].
const PAYLOAD_RESPONSE_TAG: u8 = 8;

/// A peer message of the ballot/promise increment.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProtocolMessage {
    /// A candidate asks for promises under `ballot` (`MNewLeader`).
    NewLeader {
        /// Ballot; its leader is the sender.
        ballot: Ballot,
    },
    /// A voter's promise (`MNewLeaderAckN` header): it will vote in no
    /// ballot below `ballot`, and its state is that of `synced`. The
    /// recovery report itself arrives with task-25.
    Promise {
        /// Promised ballot.
        ballot: Ballot,
        /// Highest synchronized ballot.
        synced: Ballot,
        /// Promising replica.
        replica: ReplicaId,
    },
    /// The leader's proposal for a command (`MFastAck` from the leader,
    /// carrying its sequence number and the per-key path anchors a
    /// follower feeds to `CommandTable::record_leader_path`).
    Proposal(FastAck),
    /// A follower's fast acknowledgement (`MFastAck`).
    FastAck(FastAck),
    /// A follower's adoption acknowledgement (`MLightSlowAck`).
    SlowAck(SlowAck),
    /// The leader's reply to the frontend used for learning (`MReply`):
    /// the proposal's evidence; never a result.
    LeaderReply {
        /// Ballot.
        ballot: Ballot,
        /// Command.
        command: CommandId,
        /// Leader sequence number.
        seqnum: u64,
        /// Dependencies the leader ordered.
        deps: Vec<CommandId>,
        /// Dependency-path evidence.
        path: Digest32,
    },
    /// One page of a recovery report (`MNewLeaderAckN`, bounded).
    ReportPage(ReportPage),
    /// A request for the durable payloads of commands a replica lacks.
    PayloadRequest {
        /// Commands.
        commands: Vec<CommandId>,
    },
    /// A durable payload; the receiver rehashes it against the identity.
    PayloadResponse {
        /// Command.
        command: CommandId,
        /// Payload.
        payload: PayloadRecordV1,
    },
    /// The new leader's selected recovery result (`MSync`), durably bound
    /// before it is sent.
    Sync(SyncDecision),
    /// A coordinator asks the old voters to seal for a transition
    /// (task-55; a TupleSky extension -- the paper is
    /// fixed-membership).
    SealRequest {
        /// The transition.
        transition: crate::handoff::Transition,
    },
    /// A voter's seal: ordinary voting is over here for every ballot of
    /// the old configuration. Published only once the seal row and
    /// every batch submitted before the cut are durable, so what it
    /// reports is complete.
    Sealed {
        /// The transition it sealed for.
        transition: crate::handoff::Transition,
        /// The ballot it had promised when it sealed (evidence, never
        /// an authorization).
        at: Ballot,
        /// Sealing replica.
        replica: ReplicaId,
    },
}

impl ProtocolMessage {
    /// Postcard encoding.
    pub fn encode(&self) -> Vec<u8> {
        postcard::to_allocvec(self).expect("bounded message")
    }

    /// The command this message is evidence about, where it is evidence.
    ///
    /// A runtime that has to say which caller's submission a frame
    /// belongs to needs this: in a deployment with more than one
    /// collector, a voter's evidence goes back to the collector that
    /// submitted, and the command is what says which one that was. It is
    /// `None` for everything that is about a ballot or a replica rather
    /// than about one command.
    pub const fn command(&self) -> Option<CommandId> {
        match self {
            ProtocolMessage::Proposal(a) | ProtocolMessage::FastAck(a) => Some(a.command),
            ProtocolMessage::SlowAck(a) => Some(a.command),
            ProtocolMessage::LeaderReply { command, .. }
            | ProtocolMessage::PayloadResponse { command, .. } => Some(*command),
            ProtocolMessage::NewLeader { .. }
            | ProtocolMessage::Promise { .. }
            | ProtocolMessage::ReportPage(_)
            | ProtocolMessage::PayloadRequest { .. }
            | ProtocolMessage::SealRequest { .. }
            | ProtocolMessage::Sealed { .. }
            | ProtocolMessage::Sync(_) => None,
        }
    }

    /// Decode exactly one message.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let (m, rest): (ProtocolMessage, &[u8]) =
            postcard::take_from_bytes(bytes).map_err(|_| DecodeError::Truncated)?;
        if !rest.is_empty() {
            return Err(DecodeError::TrailingBytes);
        }
        Ok(m)
    }
}

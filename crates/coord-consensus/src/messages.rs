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

use crate::vote::{FastAck, SlowAck};

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
    /// carrying its sequence number).
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
}

impl ProtocolMessage {
    /// Postcard encoding.
    pub fn encode(&self) -> Vec<u8> {
        postcard::to_allocvec(self).expect("bounded message")
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

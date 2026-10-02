//! Protocol messages of this increment (prototype `MNewLeader`,
//! `MNewLeaderAckN` without its report; design Section 4.8). Encoded with
//! postcard for the logical outbox; the transport wraps them in wire
//! frames (task-30). Never a durable format.

use alloc::vec::Vec;

use coord_types::CommandId;
use coord_types::error::DecodeError;
use coord_types::identity::Digest32;
use coord_types::ids::{Ballot, ExecutionPosition, KvRevision, ReplicaId};
use serde::{Deserialize, Serialize};

use crate::commands::CommandRecord;
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

/// How many commands one [`ProtocolMessage::ProposalRequest`] names,
/// and how many proposals the leader answers one with (task-d09).
///
/// Bounded for the reason [`MAX_PAYLOAD_TRANSFER`] is: the ask repeats
/// with every commit frontier, and the answers share the lane with the
/// proposals and acknowledgements the domain is waiting for.
pub const MAX_PROPOSAL_ASK: usize = 16;

/// How many commands one [`ProtocolMessage::CatchUpPage`] carries at
/// most (task-d08).
///
/// A page is executed before the next is asked for, so this bounds what a
/// voter catching up holds beside its table, and what one ask can make a
/// donor read and send on its bulk lane.
pub const MAX_CATCH_UP_COMMANDS: usize = 64;

/// How many payload bytes one [`ProtocolMessage::CatchUpPage`] carries
/// at most, beyond its first command (task-d08).
///
/// A page always carries at least one command when the donor has one to
/// give, whatever its size: a command larger than this would otherwise
/// never be served. One command is bounded by the request frame, well
/// inside a protocol frame.
pub const MAX_CATCH_UP_BYTES: usize = 1024 * 1024;

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
///
/// Catch-up is bulk for the same reason (task-d08): a page is up to
/// [`MAX_CATCH_UP_COMMANDS`] payloads, and a voter far behind asks for
/// one after another.
pub fn is_payload_transfer(frame: &[u8]) -> bool {
    matches!(
        frame.first(),
        Some(
            &PAYLOAD_REQUEST_TAG
                | &PAYLOAD_RESPONSE_TAG
                | &CATCH_UP_REQUEST_TAG
                | &CATCH_UP_PAGE_TAG
        )
    )
}

/// The encoded discriminant of [`ProtocolMessage::PayloadRequest`].
const PAYLOAD_REQUEST_TAG: u8 = 7;

/// The encoded discriminant of [`ProtocolMessage::PayloadResponse`].
const PAYLOAD_RESPONSE_TAG: u8 = 8;

/// The encoded discriminant of [`ProtocolMessage::CatchUpRequest`].
const CATCH_UP_REQUEST_TAG: u8 = 15;

/// The encoded discriminant of [`ProtocolMessage::CatchUpPage`].
const CATCH_UP_PAGE_TAG: u8 = 16;

/// One command of a catch-up page (task-d08): a command the donor
/// executed, with what it was decided and executed as.
///
/// Everything here is read from the donor's durable rows: the payload
/// from `payload_v1`, the decided record from its dependency row in
/// `protocol_v1`, the position, revision and result digest from its
/// `executed_v1` row. The admission digest is the payload's own.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CatchUpEntry {
    /// Command.
    pub command: CommandId,
    /// Its payload, which the receiver rehashes against the identity.
    pub payload: PayloadRecordV1,
    /// The donor's dependency row of the command: the dependencies it was
    /// decided with. `None` when the donor keeps no row for it.
    pub decided: Option<CommandRecord>,
    /// The position the donor executed it at.
    pub position: ExecutionPosition,
    /// The KV revision its execution produced there, if any.
    pub revision: Option<KvRevision>,
    /// The digest of its result there.
    pub result_digest: Digest32,
}

impl CatchUpEntry {
    /// The length of its encoding: what it costs a page against
    /// [`MAX_CATCH_UP_BYTES`].
    pub fn encoded_len(&self) -> usize {
        postcard::to_allocvec(self).map_or(usize::MAX, |bytes| bytes.len())
    }
}

/// A peer message of the ballot/promise increment.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProtocolMessage {
    /// A candidate asks for promises under `ballot` (`MNewLeader`).
    NewLeader {
        /// Ballot; its leader is the sender.
        ballot: Ballot,
        /// What the candidate executed through (task-d10): a voter more
        /// than a table ahead of it refuses to promise.
        executed: ExecutionPosition,
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
    /// The leader's commit frontier (task-d09): every proposal of
    /// `ballot` with a sequence number up to `through` is committed at
    /// the leader, and its batch is durable there.
    ///
    /// A follower otherwise learns a command only from acknowledgements
    /// it receives itself, each published once on a lane that drops
    /// frames. One missed quorum left it at ACCEPT on that command, and
    /// with the chain total on everything after it, for good. With this
    /// it commits what it adopted from the leader of `ballot` up to the
    /// frontier. The leader publishes it again on its re-send timer, so a
    /// lost frame is repaired by the next one.
    Committed {
        /// The ballot the sequence numbers are in; its leader is the
        /// sender.
        ballot: Ballot,
        /// Highest sequence number of the committed prefix.
        through: u64,
    },
    /// A follower asks the leader of `ballot` for its proposals of
    /// commands the follower adopted without knowing their sequence
    /// number in that ballot (task-d09).
    ///
    /// An adoption restored from the rows after a restart carries no
    /// sequence number (a ballot numbers from zero, and the row names no
    /// ballot), so the commit frontier cannot commit it. The leader
    /// counted its acknowledgement before the restart and never re-sends
    /// it. Answered with the leader's durable proposals of the ballot,
    /// which the follower adopts again with their sequence numbers; a
    /// command the leader did not propose in the ballot is not answered.
    ProposalRequest {
        /// The ballot whose proposals are asked for.
        ballot: Ballot,
        /// Commands; at most [`MAX_PROPOSAL_ASK`] are answered.
        commands: Vec<CommandId>,
    },
    /// A voter's refusal to promise `ballot` to a candidate more than a
    /// table behind it (task-d10). The candidate abandons the campaign
    /// and does not campaign again until it has executed past
    /// `executed`; the refuser never promised, so for it the ballot is
    /// leaderless.
    PromiseRefused {
        /// The refused ballot.
        ballot: Ballot,
        /// The refusing replica.
        replica: ReplicaId,
        /// What the refusing replica executed through.
        executed: ExecutionPosition,
    },
    /// A voter behind its peers asks one of them for the commands it
    /// executed after `after`, in execution order (task-d08).
    ///
    /// Answered only by a voter synchronized at `ballot`, from its durable
    /// rows, with at most one [`ProtocolMessage::CatchUpPage`].
    CatchUpRequest {
        /// The ballot the asking voter is synchronized at.
        ballot: Ballot,
        /// What the asking voter executed through.
        after: ExecutionPosition,
    },
    /// The answer to a [`ProtocolMessage::CatchUpRequest`] (task-d08):
    /// the commands the donor executed at `after + 1` onward, contiguous
    /// and in position order, at most [`MAX_CATCH_UP_COMMANDS`] of them.
    CatchUpPage {
        /// The ballot the donor is synchronized at; a page of any other
        /// ballot is dropped.
        ballot: Ballot,
        /// The position the page follows.
        after: ExecutionPosition,
        /// What the donor executed through when it answered.
        through: ExecutionPosition,
        /// The commands.
        entries: Vec<CatchUpEntry>,
    },
    /// A candidate asks a voter that promised `ballot` for pages of its
    /// report that never arrived (task-d28). The voter answers from the
    /// report it sent, never a regenerated one: an empty list asks for
    /// the first [`crate::summary::MAX_PAGE_ASK`] pages, and at most that
    /// many are answered.
    ReportPageRequest {
        /// The ballot promised.
        ballot: Ballot,
        /// The pages wanted, by number.
        pages: Vec<u32>,
    },
    /// A voter's answer to a submission it refused and can say nothing
    /// else about (task-d22). Sent to the frontend that submitted, so the
    /// collector's entry for `command` ends rather than waits for
    /// evidence that is never coming from this voter.
    Refused {
        /// The ballot the voter was at.
        ballot: Ballot,
        /// The command the submission derives to.
        command: CommandId,
        /// Why.
        refusal: SubmissionRefusal,
    },
    /// A voter's durable promise about the shared checkpoint it holds at a
    /// forgetting-floor boundary (task-d27), sent for its peers to record
    /// so a majority's promises can activate the floor.
    ///
    /// The bytes are an encoded `CheckpointReadinessV1`, which the
    /// machines never read: the runtime that exports checkpoints and
    /// holds the readiness rows does. A voter sends it only once its own
    /// row is durable, since a promise that is not durable is not one.
    FloorReadiness {
        /// The encoded readiness.
        readiness: Vec<u8>,
    },
    /// A leader asks whether its ballot is still every voter's promise
    /// (task-d50; design Section 6.3). One round answers every read that
    /// reached the leader before the round was started, so a leader
    /// starts one after a read arrives and never answers a read from a
    /// round started earlier.
    ReadConfirm {
        /// The ballot the leader leads.
        ballot: Ballot,
        /// The leader's round number, echoed in the answer.
        round: u64,
    },
    /// A voter's answer to [`ProtocolMessage::ReadConfirm`]: its promise
    /// is `ballot`, with no higher one in flight. A voter that has
    /// promised anything else does not answer.
    ReadConfirmed {
        /// The ballot confirmed.
        ballot: Ballot,
        /// The round answered.
        round: u64,
        /// The voter answering.
        replica: ReplicaId,
    },
}

/// Why a voter refused a submission (task-d22).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SubmissionRefusal {
    /// The command is bound here under other admission facts, and this
    /// voter never acknowledges it under these.
    OtherFacts {
        /// The admission digest it is bound under.
        accepted: Digest32,
    },
    /// The retry key is bound here to another command.
    OtherCommand {
        /// That command.
        bound: CommandId,
    },
    /// A duplicate this voter keeps no payload of any more: it went to
    /// history, and what it did is in the durable record.
    Forgotten,
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
            | ProtocolMessage::PayloadResponse { command, .. }
            | ProtocolMessage::Refused { command, .. } => Some(*command),
            ProtocolMessage::NewLeader { .. }
            | ProtocolMessage::Promise { .. }
            | ProtocolMessage::ReportPage(_)
            | ProtocolMessage::ReportPageRequest { .. }
            | ProtocolMessage::PayloadRequest { .. }
            | ProtocolMessage::SealRequest { .. }
            | ProtocolMessage::Sealed { .. }
            | ProtocolMessage::Committed { .. }
            | ProtocolMessage::ProposalRequest { .. }
            | ProtocolMessage::PromiseRefused { .. }
            | ProtocolMessage::CatchUpRequest { .. }
            | ProtocolMessage::CatchUpPage { .. }
            | ProtocolMessage::FloorReadiness { .. }
            | ProtocolMessage::ReadConfirm { .. }
            | ProtocolMessage::ReadConfirmed { .. }
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

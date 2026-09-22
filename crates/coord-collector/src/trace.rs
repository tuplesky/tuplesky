//! The collector's golden event trace (design Section 3.2): every
//! transition of the collector contract as a language-neutral record, so
//! the authorized Go collector (task-m02) can be differential-tested
//! against the same scenarios. Identities are lowercase hex.

use coord_types::CommandId;
use coord_types::ids::ReplicaId;
use serde::{Deserialize, Serialize};

/// Lowercase hex of bytes.
pub fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Hex of a command identity.
pub fn command_hex(command: &CommandId) -> String {
    hex(&command.0.0)
}

/// Hex of a replica identity.
pub fn replica_hex(replica: &ReplicaId) -> String {
    hex(&replica.0)
}

/// One collector transition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event")]
pub enum CollectorEvent {
    /// A request was submitted to every voter in parallel.
    Submitted {
        /// Command.
        command: String,
        /// Request sequence of the retry key.
        sequence: u64,
        /// Voters the command was fanned out to.
        targets: Vec<String>,
    },
    /// A retry attached to a pending command (no second fan-out).
    Attached {
        /// Command.
        command: String,
        /// Request sequence.
        sequence: u64,
    },
    /// A retry of a resolved command returned the retained outcome.
    Retained {
        /// Command.
        command: String,
        /// Request sequence.
        sequence: u64,
    },
    /// A submission was refused.
    Refused {
        /// Request sequence.
        sequence: u64,
        /// Reason.
        reason: String,
    },
    /// Evidence arrived from a voter identity.
    Evidence {
        /// Command it names.
        command: String,
        /// Sender identity (never the connection).
        from: String,
        /// Kind: `leader-reply`, `fast-ack`, `slow-ack` or `release`.
        kind: String,
        /// Whether it was counted.
        accepted: bool,
        /// Why not, when not.
        reason: Option<String>,
    },
    /// The command is not releasable yet.
    Held {
        /// Command.
        command: String,
        /// What is missing.
        reason: String,
    },
    /// The command's result was released.
    Released {
        /// Command.
        command: String,
        /// Whether the release preceded materialization.
        speculative: bool,
        /// Whether the collector learned it on the fast path.
        fast: bool,
        /// Voter identities counted.
        voters: Vec<String>,
        /// KV revision produced.
        revision: Option<u64>,
        /// Whether a caller was still attached to receive it.
        delivered: bool,
    },
    /// The durable record of the command's execution on this node
    /// completed what the collector half held (task-c02). Followed by
    /// the `Released` it produced.
    SettledFromRecord {
        /// Command.
        command: String,
        /// What the collector held of its own: `release` or `votes`.
        corroborated: String,
    },
    /// The caller went away; identity and outcome resolution stay.
    Cancelled {
        /// Command.
        command: String,
    },
    /// A resolution query was answered.
    Resolved {
        /// Request sequence.
        sequence: u64,
        /// `outcome`, `pending`, `conflict` or `unknown`.
        result: String,
    },
    /// The client deadline passed before establishment.
    TimedOut {
        /// Command.
        command: String,
    },
    /// The ballot changed: evidence of the old ballot is void.
    Reconfigured {
        /// New ballot number.
        ballot: u64,
        /// Pending commands whose evidence was dropped.
        reset: usize,
    },
}

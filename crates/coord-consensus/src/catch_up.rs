//! Bringing a voter that fell behind up from a peer's executed history
//! (task-d08).
//!
//! A voter cut off while the others went on comes back missing commands
//! that no peer offers it again: the leader re-sends what is in its own
//! table, and a voter behind by more than a table's worth lacks commands
//! the leader executed and retired long ago. Every peer still holds them
//! on disk -- the payload in `payload_v1`, the decided record in
//! `protocol_v1`, the execution in `executed_v1` -- and nothing trims
//! those rows while a voter has not acknowledged a floor past them.
//!
//! So the voter asks a peer synchronized at its own ballot for the
//! commands executed after its own `executed_through`, one bounded page
//! at a time ([`crate::ProtocolMessage::CatchUpRequest`]), and installs
//! each as a decided commit under the rules a Sync's COMMIT entry is
//! installed under (task-d14). The ordinary executor runs it, and its
//! position, revision and result digest are compared with the donor's; a
//! difference stops the voter ([`CatchUpDivergence`]).
//!
//! What this trusts is one peer's executed order at one ballot: the same
//! trust the voter already gives that ballot's Sync. A donor that itself
//! executed out of order is not caught here; its own divergence stops are
//! what catch that.

use alloc::collections::VecDeque;

use coord_core::effect::BarrierId;
use coord_types::CommandId;
use coord_types::identity::Digest32;
use coord_types::ids::{Ballot, ExecutionPosition, KvRevision, ReplicaId};

use crate::messages::CatchUpEntry;

/// What a donor said a pulled command executed as (task-d08).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DonorExecution {
    /// The donor.
    pub donor: ReplicaId,
    /// The ballot the donor answered at.
    pub ballot: Ballot,
    /// The position it executed the command at.
    pub position: ExecutionPosition,
    /// The KV revision it produced there.
    pub revision: Option<KvRevision>,
    /// The digest of its result there.
    pub result_digest: Digest32,
}

/// What this voter's own execution of a pulled command produced, where
/// it produced anything.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OwnExecution {
    /// The position this voter executed the command at.
    pub position: ExecutionPosition,
    /// The KV revision it produced.
    pub revision: Option<KvRevision>,
    /// The digest of its result.
    pub result_digest: Digest32,
    /// The length of the encoded result.
    pub response_len: usize,
}

/// A command pulled from a donor whose execution here disagrees with the
/// donor's (task-d08). The voter executes nothing more, and the process
/// running it stops, as on any other divergence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatchUpDivergence {
    /// The command.
    pub command: CommandId,
    /// What the donor executed it as.
    pub donor: DonorExecution,
    /// What this voter executed it as: `None` when it had executed the
    /// command already, before the position the donor names.
    pub own: Option<OwnExecution>,
}

/// The pulled command installed and waiting for the executor.
#[derive(Clone, Debug)]
pub(crate) struct Running {
    /// The command.
    pub(crate) command: CommandId,
    /// What the donor executed it as.
    pub(crate) donor: DonorExecution,
    /// The batch that made its installation durable.
    pub(crate) barrier: BarrierId,
    /// Whether that batch is durable: a pulled command executes only once
    /// its decision is on disk here, as any other commit's is.
    pub(crate) durable: bool,
    /// The page entry it came from, put back at the head of the page if
    /// that batch fails, so it is installed again rather than left
    /// waiting on a batch that will never be durable.
    pub(crate) entry: CatchUpEntry,
}

/// The catch-up this follower has in hand: at most one page, and the one
/// command of it that is installed and not yet executed.
#[derive(Clone, Debug, Default)]
pub(crate) struct CatchUp {
    /// The page's commands not yet installed, in position order, with
    /// the donor and ballot they came from.
    pub(crate) queue: VecDeque<(ReplicaId, Ballot, CatchUpEntry)>,
    /// The pulled command installed and waiting for the executor.
    pub(crate) running: Option<Running>,
    /// The donor the last page came from, and whether that page was full:
    /// a full page is followed by the next ask as soon as it is executed,
    /// rather than on the caller's timer.
    pub(crate) continue_from: Option<ReplicaId>,
    /// Pages taken, for the caller's counters.
    pub(crate) pages: u64,
    /// Commands executed from pages, likewise.
    pub(crate) executed: u64,
}

impl CatchUp {
    /// Whether a page is in hand.
    pub(crate) fn busy(&self) -> bool {
        self.running.is_some() || !self.queue.is_empty()
    }
}

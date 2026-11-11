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
//!
//! A page is installed a window at a time (task-d25): as many of its
//! commands as follow one another in one batch, rather than one batch and
//! one execution per command, which left a voter pulling about a hundred
//! commands a second and never closing its gap under a domain admitting
//! more. The window executes in the donor's positions, and after a
//! restart the first ask starts at the frontier read at boot, so what the
//! voter executed since then -- a window installed before the crash
//! included -- is compared with the donor's too ([`SinceBoot`]).

use alloc::collections::{BTreeMap, BTreeSet, VecDeque};

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

/// A pulled command installed and waiting for the executor.
#[derive(Clone, Debug)]
pub(crate) struct Running {
    /// The command.
    pub(crate) command: CommandId,
    /// What the donor executed it as.
    pub(crate) donor: DonorExecution,
    /// The batch that made its window durable: one for the window.
    pub(crate) barrier: BarrierId,
    /// Whether that batch is durable: a pulled command executes only once
    /// its decision is on disk here, as any other commit's is.
    pub(crate) durable: bool,
    /// The page entry it came from, put back at the head of the page with
    /// the rest of its window if that batch fails, so it is installed
    /// again rather than left waiting on a batch that will never be
    /// durable.
    pub(crate) entry: CatchUpEntry,
}

/// The catch-up this follower has in hand: at most two pages' worth, what
/// it executes and the page it asked ahead for (task-d25), and the window
/// of them that is installed and not yet executed.
#[derive(Clone, Debug, Default)]
pub(crate) struct CatchUp {
    /// The pages' commands not yet installed, in position order, with
    /// the donor and ballot they came from. Outside the table: only a
    /// window of them is installed at a time.
    pub(crate) queue: VecDeque<(ReplicaId, Ballot, CatchUpEntry)>,
    /// The window: pulled commands installed in one batch and not yet
    /// executed, in position order (task-d25). Only its first executes,
    /// so the window runs in the donor's positions whatever else the
    /// table holds.
    pub(crate) running: VecDeque<Running>,
    /// What this replica executed since it booted and no donor has
    /// confirmed yet (task-d25).
    pub(crate) since_boot: SinceBoot,
    /// The donor the last page came from, and whether that page was full:
    /// a full page is followed by the next ask as soon as it is executed,
    /// rather than on the caller's timer.
    pub(crate) continue_from: Option<ReplicaId>,
    /// Where the last ask published asked from: the page that answers it
    /// carries the same `after`. A page that starts below this replica's
    /// frontier is taken only for what it carries past it, and goes on
    /// asking only if it answers this ask, so a late answer to an earlier
    /// one never starts a second chain beside the running one (task-d25
    /// at the bar's load).
    pub(crate) asked_after: Option<ExecutionPosition>,
    /// Pages taken, for the caller's counters.
    pub(crate) pages: u64,
    /// Commands executed from pages, likewise.
    pub(crate) executed: u64,
    /// What ended each ask and page that went nowhere, by path
    /// (task-d106 step 0).
    pub(crate) paths: CatchUpPaths,
}

/// How this follower's catch-up asks and pages went, by path, since it
/// started (task-d106 step 0).
///
/// Every path counted here ends an ask or a page without a rejection or
/// a log line of its own, so a voter that stops catching up says which
/// one stopped it only through these. Diagnostic only: nothing reads
/// them back.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CatchUpPaths {
    /// Asks published to a donor.
    pub asked: u64,
    /// Asks declined before booting.
    pub declined_unbooted: u64,
    /// Asks declined while a page was in hand.
    pub declined_busy: u64,
    /// Asks declined by a replica that stopped: halted or diverged.
    pub declined_stopped: u64,
    /// Asks declined by a replica not synchronized at its ballot.
    pub declined_unsynced: u64,
    /// Asks declined for the donor: this replica, or not a voter.
    pub declined_donor: u64,
    /// Pages dropped as answered at another ballot than this replica's.
    pub dropped_ballot: u64,
    /// Pages dropped by a replica not synchronized at its ballot.
    pub dropped_unsynced: u64,
    /// Pages dropped for their sender: this replica, or not a voter.
    pub dropped_sender: u64,
    /// Pages ignored while another was in hand: neither the page asked
    /// ahead nor one this replica could take after what it holds.
    pub ignored_busy: u64,
    /// Asks published ahead: from the end of the page in hand, before it
    /// has executed (task-d25).
    pub asked_ahead: u64,
    /// Pages taken after the page in hand, answering the ask made ahead.
    pub taken_ahead: u64,
    /// Pages ignored by a replica that stopped.
    pub ignored_stopped: u64,
    /// Pages ignored while a campaign of this replica's ran.
    pub ignored_campaign: u64,
    /// Pages ignored for not following where this replica asked from:
    /// past its frontier, or, after a boot, not at the boot frontier.
    pub ignored_after: u64,
    /// Pages taken that start below this replica's frontier: it executed
    /// past where it asked from while the page was on its way. Only what
    /// they carry past the frontier is installed.
    pub taken_stale: u64,
    /// Of those, the ones answering an earlier ask than the last: taken,
    /// and not followed by an ask, which the last ask's page makes.
    pub stale_unasked: u64,
    /// Pages of which nothing was taken.
    pub empty: u64,
    /// Pages taken in part: an entry out of place or not whole ended
    /// them.
    pub cut_short: u64,
    /// Windows stopped for a page answered at a ballot this replica has
    /// left.
    pub stopped_ballot: u64,
    /// Windows stopped for a stale page: this replica executed past where
    /// it starts.
    pub stopped_stale: u64,
}

impl CatchUpPaths {
    /// Add `other`'s counts to these: a follower's, when it gives up the
    /// role, to the node's since it started.
    pub fn add(&mut self, other: &Self) {
        self.asked += other.asked;
        self.declined_unbooted += other.declined_unbooted;
        self.declined_busy += other.declined_busy;
        self.declined_stopped += other.declined_stopped;
        self.declined_unsynced += other.declined_unsynced;
        self.declined_donor += other.declined_donor;
        self.dropped_ballot += other.dropped_ballot;
        self.dropped_unsynced += other.dropped_unsynced;
        self.dropped_sender += other.dropped_sender;
        self.ignored_busy += other.ignored_busy;
        self.asked_ahead += other.asked_ahead;
        self.taken_ahead += other.taken_ahead;
        self.ignored_stopped += other.ignored_stopped;
        self.ignored_campaign += other.ignored_campaign;
        self.ignored_after += other.ignored_after;
        self.taken_stale += other.taken_stale;
        self.stale_unasked += other.stale_unasked;
        self.empty += other.empty;
        self.cut_short += other.cut_short;
        self.stopped_ballot += other.stopped_ballot;
        self.stopped_stale += other.stopped_stale;
    }
}

impl CatchUp {
    /// Whether a page is in hand.
    pub(crate) fn busy(&self) -> bool {
        !self.running.is_empty() || !self.queue.is_empty()
    }

    /// The pulled commands in hand: installed and not executed, and not
    /// installed yet.
    pub(crate) fn held(&self) -> usize {
        self.running.len() + self.queue.len()
    }

    /// The last position in hand, if anything is: where the next page
    /// follows on.
    pub(crate) fn tail(&self) -> Option<ExecutionPosition> {
        self.queue
            .back()
            .map(|(_, _, e)| e.position)
            .or_else(|| self.running.back().map(|r| r.donor.position))
    }
}

/// What a replica executed after the frontier it booted at that a donor
/// has not confirmed yet (task-d25).
///
/// A window's batch makes every command of it a durable commit, and a
/// replica that crashes in the middle of the window executes the rest
/// after its restart through the ordinary learner, with the page and the
/// donor's word gone. So the commits restored at boot and not executed
/// are expected: each one's execution is kept, by position, and the
/// first ask after a boot starts at the frontier read at boot rather than
/// at the current one. Each command of a page at a kept position is
/// compared with what this replica executed there, and asks go on from
/// where the last page stopped until nothing expected or kept is left.
/// Bounded by the restored commits, which the table bounds: an execution
/// of anything else was never pulled, and the protocol decided it.
#[derive(Clone, Debug, Default)]
pub(crate) struct SinceBoot {
    /// Where the next ask starts while something waits for a donor: the
    /// boot frontier, then the last position a page covered. `None` once
    /// nothing does.
    pub(crate) from: Option<ExecutionPosition>,
    /// Commits restored at boot and not executed yet.
    pub(crate) expected: BTreeSet<CommandId>,
    /// Executions of those, by position, not yet compared.
    pub(crate) outcomes: BTreeMap<ExecutionPosition, (CommandId, OwnExecution)>,
}

impl SinceBoot {
    /// Compare the executions of `expected` after `frontier`, the one
    /// read at boot.
    pub(crate) fn at(frontier: ExecutionPosition, expected: BTreeSet<CommandId>) -> Self {
        SinceBoot {
            from: (!expected.is_empty()).then_some(frontier),
            expected,
            outcomes: BTreeMap::new(),
        }
    }

    /// Keep this execution if it is one of the restored commits.
    pub(crate) fn note(&mut self, command: CommandId, own: OwnExecution) {
        if self.from.is_some() && self.expected.remove(&command) {
            self.outcomes.insert(own.position, (command, own));
        }
    }

    /// Whether anything still waits for a donor.
    pub(crate) fn waiting(&self) -> bool {
        !self.expected.is_empty() || !self.outcomes.is_empty()
    }

    /// Nothing more to compare.
    pub(crate) fn done(&mut self) {
        self.from = None;
        self.expected = BTreeSet::new();
        self.outcomes = BTreeMap::new();
    }
}

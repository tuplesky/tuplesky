//! State carried across a role change (task-26): everything durable or
//! learned survives; ballot-scoped votes do not.

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::vec::Vec;

use coord_core::effect::{BarrierId, BootId, PeerId};
use coord_core::outbox::{BarrierAllocator, Outbox};
use coord_types::ids::{Ballot, ExecutionPosition, ReplicaId};
use coord_types::{CommandId, RetryKey};

use crate::ballot::{BallotState, ConfigurationIdentity};
use crate::commands::CommandTable;
use crate::learner::Learner;
use crate::rows::PayloadRecordV1;
use crate::summary::{DurableLedger, ServedReport};

/// A recovery report owed to a candidate; the replica's state
/// independent of its role carries it across a role change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingReport {
    /// Ballot the report is for.
    pub ballot: Ballot,
    /// Candidate to send it to, at its authenticated incarnation.
    pub to: PeerId,
    /// Barriers the report's pages wait for.
    pub requires: Vec<BarrierId>,
    /// What the candidate said it executed through, in its `NewLeader`:
    /// none for the replica's own campaign's report.
    pub candidate: Option<ExecutionPosition>,
}

impl PendingReport {
    /// The refusal owed in place of this report if the replica cutting
    /// it, which executed through `own`, is now more than `window`
    /// commands past the candidate (task-d119).
    ///
    /// The rule is task-d10's ([`BallotState::refuses_behind`]), read
    /// again at the cut: the replica keeps executing between its promise
    /// and the cut, which waits for the promise's batch, and a report cut
    /// past the window can leave out a command the candidate never
    /// executed while holding one that names it. Its candidate then finds
    /// a dependency no report holds. Refused instead, it catches up first.
    pub fn behind(&self, own: ExecutionPosition, window: u64) -> Option<crate::PromiseRejection> {
        let candidate = self.candidate?;
        (own.get() > candidate.get().saturating_add(window))
            .then_some(crate::PromiseRejection::CandidateBehind { candidate, own })
    }
}

/// A refusal a replica sent a candidate at its report's cut, in place of
/// the report (task-d119).
///
/// It is kept as the pages of a report are (task-d28): the candidate has
/// this replica's promise, so it waits for the report, and asks for its
/// pages again when none arrive. A refusal that was lost is answered to
/// that ask, or the campaign waits for its election timeout.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ServedRefusal {
    /// Ballot refused.
    pub ballot: Ballot,
    /// Candidate refused, at its authenticated incarnation.
    pub to: PeerId,
    /// What the replica executed through at the cut.
    pub executed: ExecutionPosition,
}

impl ServedRefusal {
    /// Whether `from`, asking for the pages of its report for `ballot`,
    /// is answered with this refusal.
    pub fn answers(&self, from: ReplicaId, ballot: Ballot) -> bool {
        self.to.replica == from && self.ballot == ballot
    }
}

/// State carried across a role change.
#[derive(Debug)]
pub struct RecoveredState {
    /// Configuration identity.
    pub identity: ConfigurationIdentity,
    /// Promise state.
    pub ballots: BallotState,
    /// Command table (phases, dependencies, paths).
    pub table: CommandTable,
    /// Durable ledger.
    pub ledger: DurableLedger,
    /// Durable payloads by command.
    pub payloads: BTreeMap<CommandId, PayloadRecordV1>,
    /// Retry-key bindings.
    pub bindings: BTreeMap<RetryKey, CommandId>,
    /// Payloads whose batches are durable (servable).
    pub served_payloads: BTreeSet<CommandId>,
    /// Execution frontier.
    pub learner: Learner,
    /// Boot, if booted.
    pub boot: Option<BootId>,
    /// Barrier allocator of this boot.
    pub alloc: Option<BarrierAllocator>,
    /// Logical outbox of this boot (pending sends carry over; the ballot
    /// fence decides their fate).
    pub outbox: Option<Outbox>,
    /// A recovery report this replica owes a candidate, if any: the
    /// obligation survives a role change, since the candidate may need
    /// this replica for its majority.
    pub report_due: Option<PendingReport>,
    /// The pages of the last report this replica sent a candidate, which
    /// it answers a lost page from (task-d28).
    pub served_report: Option<ServedReport>,
    /// The refusal this replica last sent a candidate at a report's cut,
    /// which it answers a lost one from (task-d119).
    pub served_refusal: Option<ServedRefusal>,
    /// The ballot this replica last reported for and the commands its
    /// report named: the only records a Sync of that ballot may release
    /// (task-d24).
    pub report_cut: Option<(Ballot, BTreeSet<CommandId>)>,
    /// Frontend.
    pub frontend: PeerId,
    /// Command table capacity.
    pub capacity: usize,
    /// The selection of the synchronized ballot, when this replica holds
    /// it: a deposed leader's own Sync, whose entries it may not have
    /// installed (task-d34). Its follower resumes it as a restart resumes
    /// the durable Sync row, so the entries install at the selected facts
    /// and every report names them.
    pub synced_selection: Option<crate::recovery::SyncDecision>,
    /// Commands this replica took in after its campaign cut its own
    /// report, still only pre-accepted here: no selection of that
    /// campaign can name them, so the leader it becomes proposes them
    /// (task-d33).
    pub arrived: BTreeSet<CommandId>,
}

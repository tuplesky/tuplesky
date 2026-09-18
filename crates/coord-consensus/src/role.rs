//! State carried across a role change (task-26): everything durable or
//! learned survives; ballot-scoped votes do not.

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::vec::Vec;

use coord_core::effect::{BarrierId, BootId, PeerId};
use coord_core::outbox::{BarrierAllocator, Outbox};
use coord_types::ids::Ballot;
use coord_types::{CommandId, RetryKey};

use crate::ballot::{BallotState, ConfigurationIdentity};
use crate::commands::CommandTable;
use crate::learner::Learner;
use crate::rows::PayloadRecordV1;
use crate::summary::DurableLedger;

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
    /// Frontend.
    pub frontend: PeerId,
    /// Command table capacity.
    pub capacity: usize,
}

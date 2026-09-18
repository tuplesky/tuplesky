//! State carried across a role change (task-26): everything durable or
//! learned survives; ballot-scoped votes do not.

use alloc::collections::{BTreeMap, BTreeSet};

use coord_core::effect::{BootId, PeerId};
use coord_core::outbox::{BarrierAllocator, Outbox};
use coord_types::{CommandId, RetryKey};

use crate::ballot::{BallotState, ConfigurationIdentity};
use crate::commands::CommandTable;
use crate::learner::Learner;
use crate::rows::PayloadRecordV1;
use crate::summary::DurableLedger;

/// The replica's state independent of its role.
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
    /// Frontend.
    pub frontend: PeerId,
    /// Command table capacity.
    pub capacity: usize,
}

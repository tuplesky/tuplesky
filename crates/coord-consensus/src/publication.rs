//! Durable publication obligations (design Section 5.1).

use serde::{Deserialize, Serialize};

/// Something a replica publishes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Publication {
    /// A new promise or a recovery response.
    PromiseOrRecoveryResponse,
    /// A fast acknowledgement.
    FastAck,
    /// A leader reply used for learning.
    LeaderReply,
    /// A slow (adoption) acknowledgement.
    SlowAck,
    /// A finalized application result.
    FinalizedResult,
    /// Checkpoint readiness.
    CheckpointReady,
}

/// A durable record a publication depends on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DurableRecord {
    /// The promised ballot.
    PromisedBallot,
    /// Complete source-required recovery state at the cut.
    RecoveryStateAtCut,
    /// The command payload.
    Payload,
    /// The vote itself.
    Vote,
    /// Dependency path/order evidence.
    PathEvidence,
    /// Stable prerequisite dependencies.
    PrerequisiteDependencies,
    /// Recoverable proposal/payload state.
    ProposalState,
    /// The adopted leader order.
    AdoptedLeaderOrder,
    /// Prerequisite acceptance state.
    PrerequisiteAcceptance,
    /// The recoverable established command.
    EstablishedCommand,
    /// The atomic application/deduplication outcome.
    ApplicationOutcome,
    /// A complete validated checkpoint.
    Checkpoint,
    /// Checkpoint identity.
    CheckpointIdentity,
    /// Recovery-floor metadata.
    RecoveryFloor,
}

impl Publication {
    /// The records that must be durable before this is published. Issuing
    /// a write or producing a message establishes nothing; completion does.
    pub const fn requires(self) -> &'static [DurableRecord] {
        match self {
            Publication::PromiseOrRecoveryResponse => &[
                DurableRecord::PromisedBallot,
                DurableRecord::RecoveryStateAtCut,
            ],
            Publication::FastAck => &[
                DurableRecord::Payload,
                DurableRecord::Vote,
                DurableRecord::PathEvidence,
                DurableRecord::PrerequisiteDependencies,
            ],
            Publication::LeaderReply => &[DurableRecord::ProposalState],
            Publication::SlowAck => &[
                DurableRecord::AdoptedLeaderOrder,
                DurableRecord::PrerequisiteAcceptance,
            ],
            Publication::FinalizedResult => &[
                DurableRecord::EstablishedCommand,
                DurableRecord::ApplicationOutcome,
            ],
            Publication::CheckpointReady => &[
                DurableRecord::Checkpoint,
                DurableRecord::CheckpointIdentity,
                DurableRecord::RecoveryFloor,
            ],
        }
    }
}

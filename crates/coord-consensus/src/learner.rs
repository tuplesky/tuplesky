//! Conservative slow learner and ordered execution (task-24; design
//! Sections 4.3-4.5, 17.4, 18.3; prototype `getFastAndSlowAcksHandler`
//! with the slow set, `deliver`).
//!
//! A command is learned when its slow predicate holds (the leader's order
//! adopted by a majority including the leader) and every dependency is
//! committed; it executes when every dependency has executed, in the
//! leader's sequence order, at the next execution position. Application
//! itself happens through the common materializer outside this crate; the
//! learner turns the materializer's outcome into the sealed
//! [`EstablishedResult`] and only then does the machine emit
//! `Effect::Established`. A leader reply, or any single response, can
//! never establish anything here.

use alloc::collections::BTreeMap;
use alloc::vec::Vec;

use coord_core::capability::{EstablishError, EstablishedResult, EstablishmentEvidence};
use coord_types::CommandId;
use coord_types::identity::Digest32;
use coord_types::ids::{Ballot, ConfigurationEpoch, ExecutionPosition, KvRevision};
use serde::{Deserialize, Serialize};

use crate::commands::CommandTable;
use crate::graph::ClosureProgress;
use crate::phase::{GuardViolation, Phase, guard_execute};
use crate::vote::VoteSet;

/// What the common materializer reports after applying a command.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedOutcome {
    /// Execution position the application took.
    pub position: ExecutionPosition,
    /// KV revision produced, if any.
    pub revision: Option<KvRevision>,
    /// Digest of the exact result.
    pub result_digest: Digest32,
}

/// Why an application outcome could not be established.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LearnError {
    /// The command is not committed with every dependency executed.
    NotExecutable(CommandId),
    /// The materializer's position disagrees with the learner's order.
    PositionMismatch {
        /// Expected.
        expected: ExecutionPosition,
        /// Reported.
        got: ExecutionPosition,
    },
    /// A guard failed.
    Guard(GuardViolation),
    /// The evidence did not seal.
    Establish(EstablishError),
    /// The execution position cannot advance.
    Overflow,
}

/// The learner of one replica: the execution frontier.
#[derive(Clone, Debug)]
pub struct Learner {
    executed_through: ExecutionPosition,
}

impl Learner {
    /// A learner whose durable execution frontier is `executed_through`.
    pub const fn new(executed_through: ExecutionPosition) -> Self {
        Learner { executed_through }
    }

    /// Highest executed position.
    pub const fn executed_through(&self) -> ExecutionPosition {
        self.executed_through
    }

    /// Commit every accepted command whose slow predicate holds and whose
    /// dependencies are committed; repeat while progress is made. Returns
    /// the commands committed.
    pub fn commit_learned(
        table: &mut CommandTable,
        votes: &BTreeMap<CommandId, VoteSet>,
    ) -> Vec<CommandId> {
        let mut committed = Vec::new();
        loop {
            let candidates: Vec<CommandId> = votes
                .iter()
                .filter(|(c, v)| {
                    table.phase_of(c) == Some(Phase::Accept) && v.learned_slow().is_some()
                })
                .map(|(c, _)| *c)
                .collect();
            let mut progressed = false;
            for c in candidates {
                if table.commit(c).is_ok() {
                    committed.push(c);
                    progressed = true;
                }
            }
            if !progressed {
                return committed;
            }
        }
    }

    /// The next command to execute: committed, every dependency executed,
    /// lowest leader sequence number (then identity).
    pub fn next_executable(
        &self,
        table: &CommandTable,
        seqnum_of: impl Fn(&CommandId) -> Option<u64>,
    ) -> Option<CommandId> {
        table
            .records()
            .filter(|(_, r)| r.phase == Phase::Commit)
            .filter(|(_, r)| guard_execute(&r.deps, |d| table.phase_of(d)).is_ok())
            .map(|(c, _)| (seqnum_of(c).unwrap_or(u64::MAX), *c))
            .min()
            .map(|(_, c)| c)
    }

    /// Seal the materializer's outcome for `command` into an established
    /// result, marking the command executed at the next position.
    pub fn established(
        &mut self,
        table: &mut CommandTable,
        command: CommandId,
        epoch: ConfigurationEpoch,
        ballot: Ballot,
        outcome: &AppliedOutcome,
    ) -> Result<EstablishedResult, LearnError> {
        let record = table
            .record(&command)
            .ok_or(LearnError::NotExecutable(command))?;
        if record.phase != Phase::Commit {
            return Err(LearnError::NotExecutable(command));
        }
        guard_execute(&record.deps.clone(), |d| table.phase_of(d)).map_err(LearnError::Guard)?;
        let expected = self
            .executed_through
            .checked_next()
            .map_err(|_| LearnError::Overflow)?;
        if outcome.position != expected {
            return Err(LearnError::PositionMismatch {
                expected,
                got: outcome.position,
            });
        }
        let cursor = table.closure_start(command).map_err(LearnError::Guard)?;
        let closed = match table
            .closure_step(cursor, usize::MAX)
            .map_err(LearnError::Guard)?
        {
            ClosureProgress::Complete(c) => c.members.into_iter().collect(),
            ClosureProgress::Continue(_) => Vec::new(),
        };
        table.execute(command).map_err(LearnError::Guard)?;
        self.executed_through = expected;
        EstablishedResult::establish(EstablishmentEvidence {
            command,
            epoch,
            ballot,
            position: expected,
            closed_predecessors: closed,
            result_digest: outcome.result_digest,
            revision: outcome.revision,
            fast_path: false,
        })
        .map_err(LearnError::Establish)
    }
}

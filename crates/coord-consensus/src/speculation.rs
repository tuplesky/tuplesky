//! Bounded speculative execution and result-release gating (task-29;
//! design Sections 4.3, 4.5, 17.4, 18.1).
//!
//! The leader may compute a *tentative* result for a proposal over a
//! disposable overlay of the durable state plus the tentative effects of
//! every earlier proposal in its order (the materializer's speculation
//! companion in common storage builds it; this module only tracks the
//! outcomes). A tentative result is exactly the final one whenever the
//! leader's order of the whole prefix is learned unchanged, which is the
//! only case the [`ReleaseGate`] releases it: the command and every
//! earlier proposal are committed by the learning predicate (fast or
//! slow), so command, closed predecessor order, authorization (the view
//! the plan was authorized on, unchanged by the prefix) and the exact
//! result are determined by durable evidence. Overlays are bounded and
//! discarded whenever the role changes; nothing tentative survives a
//! deposition, and no event or credential is ever released here.

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::vec::Vec;

use coord_core::capability::{
    EstablishError, EstablishedResult, EstablishmentEvidence, ReleasedResult,
};
use coord_types::CommandId;
use coord_types::identity::Digest32;
use coord_types::ids::{Ballot, ConfigurationEpoch, ExecutionPosition, KvRevision};
use serde::{Deserialize, Serialize};

/// Default number of unexecuted proposals the leader speculates ahead.
pub const DEFAULT_SPECULATION_BOUND: usize = 64;

/// What the leader asks the speculation companion to compute: the command
/// at the position it would occupy if every earlier unexecuted proposal
/// (the prefix, in the leader's order) executes first.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpeculationRequest {
    /// Command to plan.
    pub command: CommandId,
    /// Earlier unexecuted proposals, in order; each already has a
    /// tentative outcome in the overlay.
    pub prefix: Vec<CommandId>,
    /// Execution position the command takes after the prefix.
    pub position: ExecutionPosition,
}

/// A tentative outcome computed over the overlay.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TentativeOutcome {
    /// Command.
    pub command: CommandId,
    /// Position it was planned at.
    pub position: ExecutionPosition,
    /// KV revision it would produce, if any.
    pub revision: Option<KvRevision>,
    /// Digest of the exact response.
    pub result_digest: Digest32,
    /// The exact encoded response.
    pub response: Vec<u8>,
    /// The prefix it was planned after.
    pub prefix: Vec<CommandId>,
}

/// Why a released speculative result and the materialized outcome
/// disagree: a fail-closed condition (the replica halts rather than
/// serving two answers for one command).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpeculationMismatch {
    /// Command.
    pub command: CommandId,
    /// Tentative position and digest.
    pub tentative: (ExecutionPosition, Digest32),
    /// Materialized position and digest.
    pub materialized: (ExecutionPosition, Digest32),
}

/// Tentative outcomes of one leader ballot.
#[derive(Clone, Debug, Default)]
pub struct Speculation {
    outcomes: BTreeMap<CommandId, TentativeOutcome>,
    /// Commands the companion refused (not speculable, or over budget):
    /// the chain stops there until they execute.
    declined: BTreeSet<CommandId>,
    released: BTreeSet<CommandId>,
    bound: usize,
}

impl Speculation {
    /// Empty state with the default bound.
    pub fn new() -> Self {
        Speculation {
            bound: DEFAULT_SPECULATION_BOUND,
            ..Speculation::default()
        }
    }

    /// Set the bound (zero disables speculation).
    pub const fn set_bound(&mut self, bound: usize) {
        self.bound = bound;
    }

    /// The bound.
    pub const fn bound(&self) -> usize {
        self.bound
    }

    /// Outcomes held (bound accounting).
    pub fn outstanding(&self) -> usize {
        self.outcomes.len()
    }

    /// The tentative outcome of a command, if computed.
    pub fn outcome(&self, command: &CommandId) -> Option<&TentativeOutcome> {
        self.outcomes.get(command)
    }

    /// Whether the companion declined the command.
    pub fn declined(&self, command: &CommandId) -> bool {
        self.declined.contains(command)
    }

    /// Whether the command's result was released speculatively.
    pub fn released(&self, command: &CommandId) -> bool {
        self.released.contains(command)
    }

    /// The next request along the leader's order (`proposals`: unexecuted
    /// proposals in sequence order): the first proposal without an
    /// outcome, provided every earlier one has one and the bound allows.
    pub fn next_request(
        &self,
        proposals: &[CommandId],
        executed_through: ExecutionPosition,
    ) -> Option<SpeculationRequest> {
        let mut prefix = Vec::new();
        for c in proposals {
            if self.outcomes.contains_key(c) {
                prefix.push(*c);
                continue;
            }
            if self.declined.contains(c) || prefix.len() >= self.bound {
                return None;
            }
            let position = executed_through
                .get()
                .checked_add(prefix.len() as u64 + 1)
                .and_then(|p| ExecutionPosition::new(p).ok())?;
            return Some(SpeculationRequest {
                command: *c,
                prefix,
                position,
            });
        }
        None
    }

    /// Record a computed outcome.
    pub fn record(&mut self, outcome: TentativeOutcome) {
        self.declined.remove(&outcome.command);
        self.outcomes.insert(outcome.command, outcome);
    }

    /// Record a refusal.
    pub fn decline(&mut self, command: CommandId) {
        self.declined.insert(command);
    }

    /// Forget a command (executed, or its proposal gone).
    pub fn forget(&mut self, command: &CommandId) -> Option<TentativeOutcome> {
        self.declined.remove(command);
        self.released.remove(command);
        self.outcomes.remove(command)
    }

    /// Discard everything (role change: nothing tentative survives).
    pub fn clear(&mut self) {
        *self = Speculation {
            bound: self.bound,
            ..Speculation::default()
        };
    }

    /// Check a materialized outcome against the tentative one, forgetting
    /// it either way.
    pub fn reconcile(
        &mut self,
        command: CommandId,
        position: ExecutionPosition,
        result_digest: Digest32,
    ) -> Result<(), SpeculationMismatch> {
        let released = self.released.contains(&command);
        let Some(t) = self.forget(&command) else {
            return Ok(());
        };
        if released && (t.position != position || t.result_digest != result_digest) {
            return Err(SpeculationMismatch {
                command,
                tentative: (t.position, t.result_digest),
                materialized: (position, result_digest),
            });
        }
        Ok(())
    }
}

/// The release gate: what the learner must show before a tentative
/// result leaves the replica.
pub struct ReleaseGate<'a> {
    /// Epoch.
    pub epoch: ConfigurationEpoch,
    /// Ballot the evidence belongs to.
    pub ballot: Ballot,
    /// Whether a command is committed by the learning predicate.
    pub committed: &'a dyn Fn(&CommandId) -> bool,
    /// Whether a command was learned by the fast predicate (measurement).
    pub fast: &'a dyn Fn(&CommandId) -> bool,
}

impl ReleaseGate<'_> {
    /// Release every tentative result whose command and whole prefix are
    /// committed, along the leader's order (`proposals`: unexecuted
    /// proposals in sequence order). Stops at the first proposal that is
    /// not committed or has no outcome: a later command's tentative result
    /// depends on the whole prefix.
    pub fn release(
        &self,
        speculation: &mut Speculation,
        proposals: &[CommandId],
    ) -> Result<Vec<ReleasedResult>, EstablishError> {
        let mut out = Vec::new();
        for c in proposals {
            if !(self.committed)(c) {
                break;
            }
            let Some(t) = speculation.outcomes.get(c) else {
                break;
            };
            if speculation.released.contains(c) {
                continue;
            }
            let established = EstablishedResult::establish(EstablishmentEvidence {
                command: *c,
                epoch: self.epoch,
                ballot: self.ballot,
                position: t.position,
                closed_predecessors: t.prefix.clone(),
                result_digest: t.result_digest,
                revision: t.revision,
                fast_path: (self.fast)(c),
            })?;
            out.push(ReleasedResult::from_gate(
                established,
                t.response.clone(),
                true,
            ));
            speculation.released.insert(*c);
        }
        Ok(out)
    }
}

//! Command phases and the source normal-operation guards (design Section
//! 4.7; prototype phase constants and the unenforced `TODO` in
//! `swift/swift.go` `fastAckFromLeader`).

use coord_types::CommandId;
use serde::{Deserialize, Serialize};

/// Phase of a command at one replica.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Phase {
    /// Known by identity only (a descriptor exists, no payload yet).
    Start,
    /// Payload and local dependencies recorded; fast acknowledgement sent.
    PreAccept,
    /// The leader's order adopted.
    Accept,
    /// Learned (fast or slow quorum).
    Commit,
    /// Executed at its position.
    Executed,
}

/// A normal-operation guard did not hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum GuardViolation {
    /// A direct dependency is not known at all (a half-initialized or
    /// pending-ingress placeholder cannot satisfy a guard).
    DependencyUnknown {
        /// Dependency.
        dep: CommandId,
    },
    /// A direct dependency is below ACCEPT.
    DependencyNotAccepted {
        /// Dependency.
        dep: CommandId,
    },
    /// A dependency is below COMMIT.
    DependencyNotCommitted {
        /// Dependency.
        dep: CommandId,
    },
    /// A dependency has not executed.
    DependencyNotExecuted {
        /// Dependency.
        dep: CommandId,
    },
}

fn check(
    deps: &[CommandId],
    phase_of: impl Fn(&CommandId) -> Option<Phase>,
    minimum: Phase,
    violation: fn(CommandId) -> GuardViolation,
) -> Result<(), GuardViolation> {
    for dep in deps {
        match phase_of(dep) {
            None => return Err(GuardViolation::DependencyUnknown { dep: *dep }),
            Some(p) if p < minimum => return Err(violation(*dep)),
            Some(_) => {}
        }
    }
    Ok(())
}

/// Entering ACCEPT requires every direct dependency in ACCEPT or COMMIT.
pub fn guard_accept(
    deps: &[CommandId],
    phase_of: impl Fn(&CommandId) -> Option<Phase>,
) -> Result<(), GuardViolation> {
    check(deps, phase_of, Phase::Accept, |dep| {
        GuardViolation::DependencyNotAccepted { dep }
    })
}

/// Entering COMMIT requires every dependency committed.
pub fn guard_commit(
    deps: &[CommandId],
    phase_of: impl Fn(&CommandId) -> Option<Phase>,
) -> Result<(), GuardViolation> {
    check(deps, phase_of, Phase::Commit, |dep| {
        GuardViolation::DependencyNotCommitted { dep }
    })
}

/// Finalized execution requires every dependency executed.
pub fn guard_execute(
    deps: &[CommandId],
    phase_of: impl Fn(&CommandId) -> Option<Phase>,
) -> Result<(), GuardViolation> {
    check(deps, phase_of, Phase::Executed, |dep| {
        GuardViolation::DependencyNotExecuted { dep }
    })
}

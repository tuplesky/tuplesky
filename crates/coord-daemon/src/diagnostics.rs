//! Secret-safe diagnostics (design Sections 19.4, 22.2): a redacted
//! wrapper and a snapshot the admin endpoint may serve. Tokens, keys,
//! auth handles and user data never appear.

use std::fmt;

use crate::lifecycle::{Lifecycle, Phase};
use crate::role::RoleSet;

/// A value whose Debug and Display are always `<redacted>`.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct Redacted<T>(pub T);

impl<T> Redacted<T> {
    /// The wrapped value (only code that must use it calls this).
    pub const fn expose(&self) -> &T {
        &self.0
    }
}

impl<T> fmt::Debug for Redacted<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

impl<T> fmt::Display for Redacted<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

/// A diagnostics snapshot for the loopback admin endpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Diagnostics {
    /// Roles the process runs.
    pub roles: Vec<crate::role::Role>,
    /// The lifecycle phase.
    pub phase: &'static str,
    /// Whether the process serves.
    pub serving: bool,
    /// Whether consensus has fresh quorum.
    pub fresh_quorum: bool,
    /// Restarts recorded across supervised workers.
    pub worker_restarts: usize,
}

impl Diagnostics {
    /// Build a snapshot from the lifecycle and role set.
    pub fn snapshot(roles: &RoleSet, lifecycle: &Lifecycle, worker_restarts: usize) -> Self {
        Diagnostics {
            roles: roles.roles().to_vec(),
            phase: match lifecycle.phase() {
                Phase::Starting => "starting",
                Phase::Live => "live",
                Phase::Ready => "ready",
                Phase::Draining => "draining",
                Phase::Stopped => "stopped",
                Phase::Quarantined(_) => "quarantined",
            },
            serving: lifecycle.serving(),
            fresh_quorum: lifecycle.readiness().fresh_quorum,
            worker_restarts,
        }
    }
}

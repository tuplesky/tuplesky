//! The process lifecycle (design Sections 22.1, 10.4): startup, readiness
//! and shutdown. Readiness separates transport liveness from
//! fresh-quorum consensus readiness: a voter that only has cached
//! leadership is not ready to serve. Disk quarantine drains and stops
//! rather than serving corrupt state.

use crate::role::RoleSet;

/// Where the process is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    /// Opening storage, binding listeners, loading identities.
    Starting,
    /// Listeners up, but not yet serving (consensus not fresh).
    Live,
    /// Serving.
    Ready,
    /// Draining: no new work, finishing in-flight, closing at deadline.
    Draining,
    /// Stopped.
    Stopped,
    /// Quarantined: not serving; a new generation is required.
    Quarantined(QuarantineReason),
}

/// Why the process quarantined.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuarantineReason {
    /// Durable state is missing, corrupt or rolled back.
    Disk,
    /// A supervised worker exhausted its restart budget.
    Worker,
    /// Genesis did not match (digest mismatch or lost journal).
    Genesis,
}

/// What makes a role's readiness.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Readiness {
    /// Listeners are bound and negotiating.
    pub listeners_up: bool,
    /// Storage opened and its projection is caught up.
    pub storage_ready: bool,
    /// Identities loaded and the genesis membership installed.
    pub identity_ready: bool,
    /// Consensus has *fresh* quorum contact (a current-ballot leader with
    /// a live quorum), not merely a cached leadership belief.
    pub fresh_quorum: bool,
    /// The auth broker's issuer keys are usable.
    pub auth_ready: bool,
}

/// The readiness gate for a role set.
#[derive(Clone, Debug)]
pub struct ReadyGate {
    roles: RoleSet,
}

impl ReadyGate {
    /// A gate for `roles`.
    pub const fn new(roles: RoleSet) -> Self {
        ReadyGate { roles }
    }

    /// Whether the process is ready to serve under `readiness`. A voter
    /// requires fresh quorum, never cached leadership; a frontend or
    /// observer requires storage and identity but not its own vote.
    pub fn ready(&self, r: &Readiness) -> bool {
        if !r.listeners_up || !r.identity_ready {
            return false;
        }
        for role in self.roles.roles() {
            let ok = match role {
                crate::role::Role::Voter => r.storage_ready && r.fresh_quorum,
                crate::role::Role::Frontend => r.storage_ready,
                crate::role::Role::Observer => r.storage_ready,
                crate::role::Role::AuthBroker => r.auth_ready,
                crate::role::Role::NodeIssuer => true,
            };
            if !ok {
                return false;
            }
        }
        true
    }
}

/// The lifecycle state machine.
#[derive(Debug)]
pub struct Lifecycle {
    phase: Phase,
    gate: ReadyGate,
    readiness: Readiness,
}

impl Lifecycle {
    /// A lifecycle for `roles`, starting.
    pub fn new(roles: RoleSet) -> Self {
        Lifecycle {
            phase: Phase::Starting,
            gate: ReadyGate::new(roles),
            readiness: Readiness::default(),
        }
    }

    /// The current phase.
    pub const fn phase(&self) -> Phase {
        self.phase
    }

    /// The current readiness.
    pub const fn readiness(&self) -> &Readiness {
        &self.readiness
    }

    /// Whether the process serves now.
    pub fn serving(&self) -> bool {
        self.phase == Phase::Ready
    }

    /// Update readiness; advances Starting/Live to Ready or back. A
    /// quarantined or stopped process never returns to serving.
    pub fn observe(&mut self, readiness: Readiness) {
        if matches!(
            self.phase,
            Phase::Quarantined(_) | Phase::Stopped | Phase::Draining
        ) {
            return;
        }
        self.readiness = readiness;
        self.phase = if self.gate.ready(&readiness) {
            Phase::Ready
        } else {
            Phase::Live
        };
    }

    /// Begin a graceful drain (a shutdown signal).
    pub fn drain(&mut self) {
        if !matches!(self.phase, Phase::Quarantined(_) | Phase::Stopped) {
            self.phase = Phase::Draining;
        }
    }

    /// The drain deadline passed (or drain completed).
    pub fn stopped(&mut self) {
        if !matches!(self.phase, Phase::Quarantined(_)) {
            self.phase = Phase::Stopped;
        }
    }

    /// Quarantine the process; it stops serving and cannot recover in
    /// place. From here only a new generation through the handoff
    /// lifecycle brings the node back.
    pub fn quarantine(&mut self, reason: QuarantineReason) {
        self.phase = Phase::Quarantined(reason);
        self.readiness = Readiness::default();
    }
}

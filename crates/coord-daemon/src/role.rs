//! The role set a process runs (design Sections 3, 22.1). A colocated
//! process may run several roles with separate credentials; a
//! frontend-only or observer-only process mints no voting identity.

use serde::{Deserialize, Serialize};

/// One role a process may run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Role {
    /// A voting replica.
    Voter,
    /// A trusted native frontend collector.
    Frontend,
    /// A non-voting observer / relay.
    Observer,
    /// The auth broker and STS.
    AuthBroker,
    /// The independent node issuer.
    NodeIssuer,
}

impl Role {
    /// Whether the role votes.
    pub const fn votes(self) -> bool {
        matches!(self, Role::Voter)
    }
    /// Whether the role terminates the peer plane (needs a peer listener).
    pub const fn needs_peer_listener(self) -> bool {
        matches!(self, Role::Voter | Role::Observer)
    }
    /// Whether the role serves the native API (needs an API listener).
    pub const fn needs_api_listener(self) -> bool {
        matches!(self, Role::Voter | Role::Frontend | Role::Observer)
    }
    /// Whether the role serves HTTPS credential establishment.
    pub const fn needs_https(self) -> bool {
        matches!(self, Role::AuthBroker | Role::NodeIssuer)
    }
}

/// The set of roles one process runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoleSet {
    roles: Vec<Role>,
}

impl RoleSet {
    /// A role set from a `role` string like `voter-frontend-observer`.
    pub fn parse(spec: &str) -> Result<Self, String> {
        let mut roles = Vec::new();
        for token in spec.split('-') {
            let role = match token {
                "voter" => Role::Voter,
                "frontend" => Role::Frontend,
                "observer" => Role::Observer,
                "auth" => Role::AuthBroker,
                "issuer" => Role::NodeIssuer,
                other => return Err(format!("unknown role token {other:?}")),
            };
            if roles.contains(&role) {
                return Err(format!("role {token:?} named twice"));
            }
            roles.push(role);
        }
        if roles.is_empty() {
            return Err("no roles".into());
        }
        Ok(RoleSet { roles })
    }

    /// The roles.
    pub fn roles(&self) -> &[Role] {
        &self.roles
    }
    /// Whether any role votes.
    pub fn votes(&self) -> bool {
        self.roles.iter().any(|r| r.votes())
    }
    /// Whether any role needs a peer listener.
    pub fn needs_peer_listener(&self) -> bool {
        self.roles.iter().any(|r| r.needs_peer_listener())
    }
    /// Whether any role needs an API listener.
    pub fn needs_api_listener(&self) -> bool {
        self.roles.iter().any(|r| r.needs_api_listener())
    }
    /// Whether any role needs an HTTPS listener.
    pub fn needs_https(&self) -> bool {
        self.roles.iter().any(|r| r.needs_https())
    }
}

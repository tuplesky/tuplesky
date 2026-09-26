//! Role policies (design Sections 10.1, 10.2, 20.4): a matched workload
//! identity maps to a node identity and the certificate fields it may
//! hold. Fields are built from policy, never copied from the CSR.

use std::collections::BTreeMap;

use coord_authn::VerifiedIdentity;
use coord_types::ids::{ClusterId, ReplicaId, ReplicaIncarnation};
use coord_types::wire_v1::PeerRole;

use crate::identity::NodeIdentity;

/// Why no node identity could be built.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PolicyError {
    /// No rule matches the workload identity: deny by default.
    NoRule,
    /// The requested node is not one this rule authorizes.
    NodeNotAuthorized,
    /// The requested incarnation is below the rule's floor.
    IncarnationTooOld,
    /// The lifetime exceeds the rule's maximum.
    LifetimeTooLong,
}

/// One node role policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RolePolicy {
    /// Issuer configuration name the workload authenticated through.
    pub issuer: String,
    /// Workload attributes that must match exactly (issuer, subject,
    /// namespace, service account, repository identifiers, and so on).
    pub required: BTreeMap<String, String>,
    /// Cluster the node belongs to.
    pub cluster: ClusterId,
    /// The exact node identities this policy may enroll.
    pub nodes: Vec<ReplicaId>,
    /// Role the certificate grants.
    pub role: PeerRole,
    /// Lowest incarnation the policy will issue for (fencing rollback).
    pub min_incarnation: u64,
    /// Longest certificate lifetime, in seconds.
    pub max_lifetime_secs: u64,
    /// Endpoint DNS names the certificate may carry.
    pub dns_names: Vec<String>,
    /// Endpoint IP addresses the certificate may carry.
    ///
    /// An endpoint catalog may list a voter by IP literal, and a peer
    /// dialling one checks the address against the certificate's IP
    /// SANs, not its DNS names. A policy that could only grant names
    /// would issue a node reached by address a certificate its peers
    /// refuse -- and a renewal (task-d02) would take away the address
    /// the node's first certificate carried, which ends the node's
    /// reachability at the moment it was meant to extend it.
    pub ip_addresses: Vec<std::net::IpAddr>,
}

/// The node identity and certificate shape a policy authorizes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodePolicy {
    /// The bound node identity.
    pub identity: NodeIdentity,
    /// Endpoint DNS names.
    pub dns_names: Vec<String>,
    /// Endpoint IP addresses.
    pub ip_addresses: Vec<std::net::IpAddr>,
    /// Certificate lifetime in seconds.
    pub lifetime_secs: u64,
}

fn matches(
    rule: &RolePolicy,
    identity: &VerifiedIdentity,
    attrs: &BTreeMap<String, String>,
) -> bool {
    rule.issuer == identity.name && rule.required.iter().all(|(k, v)| attrs.get(k) == Some(v))
}

/// Build the node policy for `identity` requesting `node`/`incarnation`
/// with `lifetime_secs`, under the first matching rule.
pub fn authorize(
    rules: &[RolePolicy],
    identity: &VerifiedIdentity,
    node: ReplicaId,
    incarnation: ReplicaIncarnation,
    lifetime_secs: u64,
) -> Result<NodePolicy, PolicyError> {
    let attrs = coord_authn::receipt::attributes(identity);
    let rule = rules
        .iter()
        .find(|r| matches(r, identity, &attrs))
        .ok_or(PolicyError::NoRule)?;
    if !rule.nodes.contains(&node) {
        return Err(PolicyError::NodeNotAuthorized);
    }
    if incarnation.get() < rule.min_incarnation {
        return Err(PolicyError::IncarnationTooOld);
    }
    if lifetime_secs == 0 || lifetime_secs > rule.max_lifetime_secs {
        return Err(PolicyError::LifetimeTooLong);
    }
    Ok(NodePolicy {
        identity: NodeIdentity {
            cluster: rule.cluster,
            node,
            incarnation,
            role: rule.role,
        },
        dns_names: rule.dns_names.clone(),
        ip_addresses: rule.ip_addresses.clone(),
        lifetime_secs,
    })
}

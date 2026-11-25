//! The node identity a certificate binds, encoded as a `tuplesky:` URI
//! SAN the transport binder reads (design Sections 10.1, 10.5.1, 11.1,
//! 20.4).

use coord_types::ids::{ClusterId, DomainId, ReplicaId, ReplicaIncarnation};
use coord_types::wire_v1::PeerRole;

/// A node's committed identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeIdentity {
    /// Cluster.
    pub cluster: ClusterId,
    /// The one domain the certificate may reach.
    ///
    /// A cluster hosts many domains under one issuer, so the cluster
    /// alone doesn't say which domain's listeners a certificate is for.
    /// The binder takes the domain from here, never from the caller's
    /// `Hello` (design Section 11.1).
    pub domain: DomainId,
    /// Node (replica) identity.
    pub node: ReplicaId,
    /// Key generation / incarnation.
    pub incarnation: ReplicaIncarnation,
    /// Peer role.
    pub role: PeerRole,
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex<const N: usize>(s: &str) -> Option<[u8; N]> {
    if s.len() != 2 * N {
        return None;
    }
    let mut out = [0u8; N];
    for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
        out[i] = u8::from_str_radix(std::str::from_utf8(chunk).ok()?, 16).ok()?;
    }
    Some(out)
}

/// The name a role goes by in a node URI and an enrollment request.
pub const fn role_str(role: PeerRole) -> &'static str {
    match role {
        PeerRole::Voter => "voter",
        PeerRole::Observer => "observer",
        PeerRole::Learner => "learner",
        PeerRole::Frontend => "frontend",
        PeerRole::KineCollector => "kine-collector",
        PeerRole::Client => "client",
    }
}

/// The role `s` names, as [`role_str`] writes it.
pub fn role_of(s: &str) -> Option<PeerRole> {
    Some(match s {
        "voter" => PeerRole::Voter,
        "observer" => PeerRole::Observer,
        "learner" => PeerRole::Learner,
        "frontend" => PeerRole::Frontend,
        "kine-collector" => PeerRole::KineCollector,
        "client" => PeerRole::Client,
        _ => return None,
    })
}

/// The URI SAN encoding of a node identity.
pub fn node_uri(identity: &NodeIdentity) -> String {
    format!(
        "tuplesky://cluster/{}/domain/{}/node/{}/incarnation/{}/role/{}",
        hex(&identity.cluster.0),
        hex(&identity.domain.0),
        hex(&identity.node.0),
        identity.incarnation.get(),
        role_str(identity.role),
    )
}

/// Parse a node identity URI SAN.
///
/// A URI without a domain is not an identity: such a certificate names
/// no domain it may reach, so it reaches none.
pub fn parse_node_uri(uri: &str) -> Option<NodeIdentity> {
    let rest = uri.strip_prefix("tuplesky://")?;
    let parts: Vec<&str> = rest.split('/').collect();
    if parts.len() != 10
        || parts[0] != "cluster"
        || parts[2] != "domain"
        || parts[4] != "node"
        || parts[6] != "incarnation"
        || parts[8] != "role"
    {
        return None;
    }
    Some(NodeIdentity {
        cluster: ClusterId(unhex(parts[1])?),
        domain: DomainId(unhex(parts[3])?),
        node: ReplicaId(unhex(parts[5])?),
        incarnation: ReplicaIncarnation::new(parts[7].parse().ok()?).ok()?,
        role: role_of(parts[9])?,
    })
}

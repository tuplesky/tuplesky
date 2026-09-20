//! Where this domain's other voters are (design Sections 3.3, 10.5,
//! 22.1).
//!
//! The committed configuration says who the voters are and what key each
//! of them proves with. It deliberately does not say where any of them
//! is, because an address is not something a cluster agrees on: it
//! changes when a node moves, and a node that moved is the same voter.
//!
//! So addresses come from a signed endpoint catalog, verified against
//! the committed membership this process already holds. What the
//! signature buys is not authority over identity -- the catalog cannot
//! introduce a voter, re-incarnate one, or belong to another cluster or
//! epoch, and the peer binder proves who answered from the certificate
//! against the same committed set. What it buys is that a file this
//! process was pointed at was written by a voter of this domain, so a
//! misconfiguration is refused here rather than turning into a cluster
//! that quietly cannot form a quorum.
//!
//! An address is therefore a hint. A wrong one costs a failed handshake
//! and nothing else.

use std::net::{SocketAddr, ToSocketAddrs};

use coord_membership::membership::Membership;
use coord_types::config_v1::EndpointCatalogV1;
use coord_types::ids::{ReplicaId, ReplicaIncarnation};

/// One voter this process may dial.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Peer {
    /// The voter.
    pub replica: ReplicaId,
    /// The incarnation the committed configuration names for it.
    pub incarnation: ReplicaIncarnation,
    /// Where to try.
    pub address: SocketAddr,
    /// The name its certificate must be valid for.
    pub server_name: String,
}

/// Why this process cannot say where its peers are.
#[derive(Debug)]
pub enum PeerError {
    /// A voting process shares its domain with other committed voters
    /// and was given no catalog to find them in.
    NoCatalog {
        /// How many other voters it would have to reach.
        others: usize,
    },
    /// The catalog could not be read.
    Unreadable {
        /// Where this node looked.
        path: String,
        /// Why.
        reason: String,
    },
    /// The catalog is not one this domain's committed voters attested.
    Rejected(String),
    /// An address in the catalog is not one this process can dial.
    Unresolvable {
        /// The voter it was for.
        replica: ReplicaId,
        /// What the catalog said.
        address: String,
        /// Why.
        reason: String,
    },
    /// The catalog verified but names no address for a committed voter.
    Missing {
        /// The voter with nowhere to be reached.
        replica: ReplicaId,
    },
}

impl core::fmt::Display for PeerError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            PeerError::NoCatalog { others } => write!(
                f,
                "this node votes alongside {others} other committed voter(s) \
                 and names no cluster_endpoints to find them at"
            ),
            PeerError::Unreadable { path, reason } => {
                write!(f, "cannot read the endpoint catalog at {path}: {reason}")
            }
            PeerError::Rejected(why) => write!(
                f,
                "the endpoint catalog is not this domain's committed voters': {why}"
            ),
            PeerError::Unresolvable {
                replica,
                address,
                reason,
            } => write!(
                f,
                "voter {} is listed at {address}, which is not dialable: {reason}",
                short(replica)
            ),
            PeerError::Missing { replica } => write!(
                f,
                "the endpoint catalog names no address for committed voter {}",
                short(replica)
            ),
        }
    }
}

impl core::error::Error for PeerError {}

fn short(replica: &ReplicaId) -> String {
    replica.0[..4].iter().map(|b| format!("{b:02x}")).collect()
}

/// Every committed voter of this domain except `me`, and where to try
/// each of them.
///
/// `None` for `path` is only allowed when there is nobody else to reach:
/// a single-voter domain, or a process that does not vote. Anything else
/// is refused here, before a listener exists, rather than at the first
/// submission that finds no quorum.
pub fn resolve(
    membership: &Membership,
    me: ReplicaId,
    path: Option<&str>,
) -> Result<Vec<Peer>, PeerError> {
    let others: Vec<ReplicaId> = membership
        .voters()
        .map(|v| v.node)
        .filter(|node| *node != me)
        .collect();
    // Nobody to look up. A single-voter domain, or a process that does
    // not vote, has no peer to dial, and a catalog is an address book
    // for peers: there is nothing here for it to answer.
    if others.is_empty() {
        return Ok(Vec::new());
    }
    let Some(path) = path else {
        return Err(PeerError::NoCatalog {
            others: others.len(),
        });
    };

    let bytes = std::fs::read(path).map_err(|e| PeerError::Unreadable {
        path: path.to_owned(),
        reason: e.to_string(),
    })?;
    let catalog: EndpointCatalogV1 =
        postcard::from_bytes(&bytes).map_err(|e| PeerError::Unreadable {
            path: path.to_owned(),
            reason: format!("not an endpoint catalog: {e}"),
        })?;
    membership
        .verify_endpoints(&catalog)
        .map_err(|e| PeerError::Rejected(format!("{e:?}")))?;

    let mut peers = Vec::with_capacity(others.len());
    for replica in others {
        let entry = catalog
            .endpoints
            .iter()
            .find(|e| e.node == replica)
            .ok_or(PeerError::Missing { replica })?;
        // The first address that resolves. A catalog may list several --
        // a node with more than one interface, or a rename in flight --
        // and any of them reaches the same voter, because which voter
        // answered is decided by its certificate and not by the address
        // that found it.
        let listed = entry
            .addresses
            .first()
            .ok_or(PeerError::Missing { replica })?;
        let address = listed
            .to_socket_addrs()
            .map_err(|e| PeerError::Unresolvable {
                replica,
                address: listed.clone(),
                reason: e.to_string(),
            })?
            .next()
            .ok_or_else(|| PeerError::Unresolvable {
                replica,
                address: listed.clone(),
                reason: "resolved to no address".into(),
            })?;
        peers.push(Peer {
            replica,
            incarnation: entry.incarnation,
            address,
            server_name: server_name(listed),
        });
    }
    Ok(peers)
}

/// The name a listed address expects a certificate to be valid for.
///
/// The host part of `host:port`. A numeric address is not a name, so a
/// catalog that lists one is saying "reach this voter here" and leaving
/// the name to the deployment's own convention; the binder still refuses
/// a certificate that is not this domain's voter, so a wrong name costs
/// a handshake.
fn server_name(listed: &str) -> String {
    match listed.rsplit_once(':') {
        Some((host, _)) => host.trim_matches(['[', ']']).to_owned(),
        None => listed.to_owned(),
    }
}

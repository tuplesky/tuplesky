//! Provisioning one real domain into a directory (design Sections 3.1,
//! 6.8.4, 8.2, 23 G4).
//!
//! What this writes is what a deployment has: a trust bundle, per-node
//! credentials committed by a genesis manifest, one signed endpoint
//! catalog, the issuer's published keys, and a strict `coordd.toml` per
//! node. Nothing here is a stub of a protocol -- the daemons that read
//! it run the real startup checks, refuse the same things they refuse in
//! production, and reject this material the moment it stops agreeing
//! with itself.
//!
//! The one thing it is not is an identity provider. The harness signs
//! service tokens with the domain's configured issuer key rather than
//! federating a workload assertion, because what task-48 certifies is
//! the Kubernetes storage edge and not the token exchange; task-35 and
//! task-36 own that, and `crates/coord-sts` runs it for real. The
//! [`crate::issuer`] endpoint says so in its own documentation, and the
//! provisioned configuration never claims an external issuer it does not
//! have.

use std::net::{SocketAddr, TcpListener, UdpSocket};
use std::path::{Path, PathBuf};

use coord_types::ids::{
    ClusterId, ConfigurationEpoch, DomainId, EndpointGeneration, ReplicaId, ReplicaIncarnation,
};
use coord_types::wire_v1::PeerRole;
use serde::{Deserialize, Serialize};

use crate::pki::{Ca, Issued, restrict};

/// The name every node certificate in a provisioned domain carries, and
/// the name a peer or a collector asks for when it dials.
pub const SERVER_NAME: &str = "node.tuplesky.harness";

/// The name the Kubernetes storage edge's server certificate carries.
pub const EDGE_SERVER_NAME: &str = "kine.tuplesky.harness";

/// The client identity the edge authorizes: what a real deployment puts
/// on the API server's `--etcd-certfile` leaf.
pub const EDGE_CLIENT_NAME: &str = "kube-apiserver.tuplesky.harness";

/// A client identity of the same authority that the edge does *not*
/// authorize. Being issued by the client CA is not authorization.
pub const EDGE_INTRUDER_NAME: &str = "not-the-apiserver.tuplesky.harness";

/// The `iss` claim the domain's frontends require.
pub const ISSUER: &str = "https://sts.tuplesky.harness";

/// The name every credential endpoint certificate carries, whatever else
/// it names. Because every one carries it, it says nothing about where an
/// endpoint was provisioned to be reached, so it is never such a place.
pub const ISSUER_NAME: &str = "sts.tuplesky.harness";

/// The `aud`/`resource` the domain's frontends require, and the domain
/// name in the configuration.
pub const RESOURCE: &str = "tuplesky-harness";

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Where one provisioned node lives.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Node {
    /// The replica identifier, hex.
    pub node: String,
    /// Its strict configuration.
    pub config: PathBuf,
    /// Its state directory.
    pub directory: PathBuf,
    /// The api-plane listener callers and collectors dial.
    pub api: String,
    /// The peer-plane listener other voters dial.
    pub peer: String,
}

/// The Kubernetes storage edge's material: two authorities, because the
/// API server's client CA is not the domain's peer CA.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Edge {
    /// Where `kine-coord` listens, `host:port`.
    pub listen: String,
    /// The etcd endpoint an API server is configured with.
    pub endpoint: String,
    /// The name the edge's server certificate carries.
    pub server_name: String,
    /// The edge's server certificate and key.
    pub server_certificate: PathBuf,
    /// The edge's server key.
    pub server_key: PathBuf,
    /// The authority that issues API-server client identities.
    pub client_ca: PathBuf,
    /// The authority an API server verifies the edge against.
    pub server_ca: PathBuf,
    /// The exact identity the edge authorizes.
    pub allowed_client: String,
    /// The authorized client's certificate.
    pub client_certificate: PathBuf,
    /// The authorized client's key.
    pub client_key: PathBuf,
    /// A certificate of the same authority that is not authorized.
    pub unauthorized_certificate: PathBuf,
    /// The unauthorized client's key.
    pub unauthorized_key: PathBuf,
    /// A certificate of a completely different authority.
    pub foreign_certificate: PathBuf,
    /// The foreign client's key.
    pub foreign_key: PathBuf,
    /// The foreign authority's root, so a test can show that trusting it
    /// is not enough.
    pub foreign_ca: PathBuf,
}

/// The harness issuer endpoint's address and trust material.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Issuer {
    /// `host:port` it binds.
    pub listen: String,
    /// The `https://` base URL a client exchanges against.
    pub url: String,
    /// The authority that signs it.
    pub ca: PathBuf,
    /// Its server certificate.
    pub certificate: PathBuf,
    /// Its server key.
    pub key: PathBuf,
    /// A workload assertion file, so a client has something to present.
    pub assertion: PathBuf,
    /// The signing keys, PKCS#8 DER, base64url, one per line. Never
    /// printed and never leaves the run directory.
    pub signing_key: PathBuf,
}

/// Everything a provisioned domain is, written beside it as
/// `harness.json` so every other process reads one description instead
/// of reconstructing it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Provisioned {
    /// Cluster identifier, hex.
    pub cluster: String,
    /// Domain identifier, hex.
    pub domain: String,
    /// The namespace the genesis grant covers and every request names.
    pub namespace: String,
    /// The principal the genesis grant names.
    pub principal: String,
    /// The trust rule replicated policy holds enabled.
    pub trust_rule: String,
    /// The name node certificates carry.
    pub server_name: String,
    /// The `iss` claim frontends require.
    pub issuer_claim: String,
    /// The `aud`/resource frontends require.
    pub resource: String,
    /// The domain's trust bundle.
    pub trust_bundle: PathBuf,
    /// The genesis manifest every node reads.
    pub manifest: PathBuf,
    /// The signed endpoint catalog every node reads.
    pub catalog: PathBuf,
    /// The provisioned voters, in committed order.
    pub voters: Vec<Node>,
    /// A native caller's credential, issued by the domain's authority in
    /// the client role. A benchmark or an operator tool dials with this;
    /// it names no replica and carries no voting authority.
    pub caller_certificate: PathBuf,
    /// The native caller's key.
    pub caller_key: PathBuf,
    /// The domain authority's signing key, so a driver can issue the
    /// caller credentials it needs. A fixture authority of a throwaway
    /// run directory, never a deployment's.
    pub authority_key: PathBuf,
    /// The harness issuer.
    pub issuer: Issuer,
    /// The Kubernetes storage edge.
    pub edge: Edge,
}

impl Provisioned {
    /// Read a provisioned domain's description.
    pub fn read(dir: &Path) -> std::io::Result<Self> {
        let raw = std::fs::read(dir.join("harness.json"))?;
        serde_json::from_slice(&raw).map_err(std::io::Error::other)
    }

    /// The frontend a caller reaches by default: the first voter.
    pub fn frontend(&self) -> &str {
        &self.voters[0].api
    }
}

/// How a domain is provisioned.
#[derive(Clone, Debug)]
pub struct Plan {
    /// Where to write it.
    pub directory: PathBuf,
    /// How many voters the genesis commits. Every one of them is
    /// started: a committed voter that never runs is a quorum this
    /// domain does not have.
    pub voters: u8,
    /// The port the storage edge binds, or 0 for an ephemeral one.
    pub edge_port: u16,
    /// Where each voter runs, when that is not this host's loopback
    /// (task-d04). Empty is the single-host domain this harness has
    /// always written, byte for byte; otherwise one entry per voter.
    pub hosts: Vec<Host>,
    /// Bind every listener a host list places on the unspecified address
    /// rather than on the named host, for hosts whose listed address is
    /// not on any of their interfaces (a cloud instance's public address
    /// behind one-to-one NAT). A DNS name is always bound this way, and a
    /// loopback address never is.
    pub listen_any: bool,
    /// The host the storage edge is reached at, when that is not
    /// loopback. Its server certificate carries it.
    pub edge_host: Option<String>,
    /// Where the credential endpoint is reached, `host:port`, when that
    /// is not loopback. Its server certificate carries the host.
    pub issuer_listen: Option<Address>,
}

impl Plan {
    /// A single-host domain on loopback: what `coord-harness provision`
    /// writes without `--hosts`.
    pub fn loopback(directory: PathBuf, voters: u8, edge_port: u16) -> Self {
        Plan {
            directory,
            voters,
            edge_port,
            hosts: Vec::new(),
            listen_any: false,
            edge_host: None,
            issuer_listen: None,
        }
    }
}

/// A host and a port, where the host is an IP literal or a DNS name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Address {
    /// An IP literal or a DNS name, without brackets.
    pub host: String,
    /// The port.
    pub port: u16,
}

impl Address {
    /// Parse `host:port`, with an IPv6 literal in brackets.
    pub fn parse(text: &str) -> Result<Self, String> {
        let (host, port) = text
            .rsplit_once(':')
            .ok_or_else(|| format!("`{text}` is not host:port"))?;
        let port = port
            .parse()
            .map_err(|_| format!("`{port}` in `{text}` is not a port"))?;
        let host = bracketed(host)
            .ok_or_else(|| format!("`{text}` names an IPv6 address without brackets"))?;
        if crate::pki::reach(host).is_none() {
            return Err(format!(
                "`{host}` in `{text}` is neither an IP address nor a DNS name"
            ));
        }
        Ok(Address {
            host: host.to_owned(),
            port,
        })
    }
}

/// Where one voter of a multi-host domain runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Host {
    /// Which voter, one-based: `n1` is replica `01..01`.
    pub node: u8,
    /// An IP literal or a DNS name, without brackets. The catalog lists
    /// it, peers dial it, and the node's certificates carry it.
    pub host: String,
    /// The api-plane port.
    pub api: u16,
    /// The peer-plane port.
    pub peer: u16,
}

/// Parse `n1=host:api_port:peer_port,n2=...`.
///
/// Every voter from `n1` up is named exactly once and nothing else is:
/// a genesis commits voters one to N, and a host list with a gap would
/// provision a committed voter that no host runs. Two voters on one
/// host must not share a port, and a voter's two planes are two ports,
/// because each is its own listener.
pub fn parse_hosts(spec: &str) -> Result<Vec<Host>, String> {
    let mut hosts = Vec::new();
    for entry in spec.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        let (name, place) = entry
            .split_once('=')
            .ok_or_else(|| format!("`{entry}` is not nN=host:api_port:peer_port"))?;
        let node: u8 = name
            .strip_prefix('n')
            .and_then(|n| n.parse().ok())
            .filter(|n| *n > 0)
            .ok_or_else(|| format!("`{name}` is not a voter name like n1"))?;
        let (rest, peer) = place
            .rsplit_once(':')
            .ok_or_else(|| format!("`{place}` is not host:api_port:peer_port"))?;
        let (host, api) = rest
            .rsplit_once(':')
            .ok_or_else(|| format!("`{place}` is not host:api_port:peer_port"))?;
        let port = |text: &str| -> Result<u16, String> {
            text.parse()
                .ok()
                .filter(|p| *p > 0)
                .ok_or_else(|| format!("`{text}` in `{entry}` is not a fixed port"))
        };
        let host = bracketed(host)
            .ok_or_else(|| format!("`{entry}` names an IPv6 address without brackets"))?;
        if crate::pki::reach(host).is_none() {
            return Err(format!(
                "`{host}` in `{entry}` is neither an IP address nor a DNS name"
            ));
        }
        hosts.push(Host {
            node,
            host: host.to_owned(),
            api: port(api)?,
            peer: port(peer)?,
        });
    }
    if hosts.is_empty() {
        return Err("the host list names no voter".into());
    }
    hosts.sort_by_key(|h| h.node);
    for (index, host) in hosts.iter().enumerate() {
        if usize::from(host.node) != index + 1 {
            return Err(format!(
                "voters are n1 to n{} with each named once; n{} is {}",
                hosts.len(),
                index + 1,
                if usize::from(host.node) > index + 1 {
                    "missing"
                } else {
                    "named twice"
                }
            ));
        }
    }
    let mut taken = std::collections::BTreeSet::new();
    for host in &hosts {
        for port in [host.api, host.peer] {
            if !taken.insert((host.host.clone(), port)) {
                return Err(format!(
                    "port {port} on {} is given to two listeners",
                    host.host
                ));
            }
        }
    }
    Ok(hosts)
}

/// The host part of an authority: an IPv6 literal without its brackets,
/// or anything else as it is. `None` for an IPv6 literal written without
/// them, whose last group would otherwise be read as the port.
fn bracketed(host: &str) -> Option<&str> {
    match host.strip_prefix('[').and_then(|h| h.strip_suffix(']')) {
        Some(inside) => Some(inside),
        None if host.contains([':', '[', ']']) => None,
        None => Some(host),
    }
}

/// `host:port`, with an IPv6 literal in brackets, as a catalog lists it
/// and a client dials it.
fn authority(host: &str, port: u16) -> String {
    match host.parse::<std::net::IpAddr>() {
        Ok(ip) => SocketAddr::new(ip, port).to_string(),
        Err(_) => format!("{host}:{port}"),
    }
}

/// The socket a listener reached at `host` binds.
///
/// The named address itself where that is possible, so that several
/// "hosts" on one machine's loopback range really are several listeners
/// and a node never answers on an interface it was not placed on. The
/// unspecified address of the same family where it is not: a DNS name,
/// which a strict configuration cannot hold, or `listen_any`, for an
/// address that is not on any of the host's interfaces. A loopback
/// address always is on one, so it is always bound as itself. The QUIC
/// stack answers each datagram from the address it arrived at, so a
/// wildcard bind is still reached at the listed address.
fn bind_address(host: &str, port: u16, listen_any: bool) -> SocketAddr {
    match host.parse::<std::net::IpAddr>() {
        Ok(ip) if !listen_any || ip.is_loopback() => SocketAddr::new(ip, port),
        Ok(std::net::IpAddr::V6(_)) => {
            SocketAddr::new(std::net::IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED), port)
        }
        _ => SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), port),
    }
}

/// Ask the operating system for a port nothing is using, then let go of
/// it. A harness that hardcoded ports would fail on a busy runner for a
/// reason that has nothing to do with what it measures.
fn free_udp() -> std::io::Result<u16> {
    Ok(UdpSocket::bind("127.0.0.1:0")?.local_addr()?.port())
}

fn free_tcp() -> std::io::Result<u16> {
    Ok(TcpListener::bind("127.0.0.1:0")?.local_addr()?.port())
}

fn write_json(path: &Path, value: &serde_json::Value) -> std::io::Result<()> {
    std::fs::write(
        path,
        serde_json::to_vec_pretty(value).map_err(std::io::Error::other)?,
    )
}

/// Provision a domain into `plan.directory`.
pub fn provision(plan: &Plan) -> std::io::Result<Provisioned> {
    let dir = &plan.directory;
    let invalid = |why: String| std::io::Error::new(std::io::ErrorKind::InvalidInput, why);
    if !plan.hosts.is_empty() && plan.hosts.len() != usize::from(plan.voters) {
        return Err(invalid(format!(
            "the host list places {} voters and the plan commits {}",
            plan.hosts.len(),
            plan.voters
        )));
    }
    // What a certificate is reached at, settled before anything is
    // written: a host a certificate cannot name is refused here rather
    // than as a handshake failure on another machine.
    let reach = |host: &str| {
        crate::pki::reach(host)
            .ok_or_else(|| invalid(format!("`{host}` is neither an IP address nor a DNS name")))
    };
    let mut voter_reach = Vec::new();
    for host in &plan.hosts {
        voter_reach.push(reach(&host.host)?);
    }
    let edge_reach = plan.edge_host.as_deref().map(reach).transpose()?;
    if let Some(at) = &plan.issuer_listen
        && at.host.eq_ignore_ascii_case(ISSUER_NAME)
    {
        return Err(invalid(format!(
            "`{ISSUER_NAME}` is the name every issuer certificate carries, not a host; \
             --issuer-listen names the host the endpoint is reached at"
        )));
    }
    let issuer_reach = plan
        .issuer_listen
        .as_ref()
        .map(|a| reach(&a.host))
        .transpose()?;
    std::fs::create_dir_all(dir)?;

    let cluster = ClusterId([0x11; 16]);
    let domain = DomainId([0x22; 16]);
    let namespace = [0x5e; 16];
    let principal = [0x0a; 16];
    let trust_rule = [0x7c; 16];

    let ca = Ca::new();
    let trust_bundle = dir.join("roots.pem");
    std::fs::write(&trust_bundle, ca.root_pem())?;

    // One key ring, published as the JWKS every frontend verifies
    // against. The private half stays in the run directory: the issuer
    // endpoint reads it back, and nothing else does.
    let issuer_key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)
        .map_err(std::io::Error::other)?;
    let ring = coord_sts::KeyRing::new(
        coord_sts::SigningKey::from_pkcs8_der("tuplesky-harness-1", &issuer_key.serialize_der())
            .map_err(|e| std::io::Error::other(format!("{e:?}")))?,
    );
    let jwks = dir.join("sts-jwks.json");
    write_json(&jwks, &ring.jwks())?;
    let signing_key = dir.join("sts-signing.key");
    std::fs::write(
        &signing_key,
        coord_sts::keys::b64url(&issuer_key.serialize_der()),
    )?;
    restrict(&signing_key)?;

    // Voters: credentials first, because the genesis commits to the keys
    // and not to the names.
    let mut nodes = Vec::new();
    let mut voter_keys: Vec<Vec<u8>> = Vec::new();
    let mut attester: Option<Vec<u8>> = None;
    let incarnation = ReplicaIncarnation::new(1).expect("one is positive");
    for n in 1u8..=plan.voters {
        let directory = dir.join(format!("n{n}"));
        std::fs::create_dir_all(&directory)?;
        let node = ReplicaId([n; 16]);
        // Both of a node's certificates carry the host it is placed on:
        // its node certificate because peers dial it there, its
        // collector certificate because the collector presents it when
        // it dials the others. One key per certificate, issued once, so
        // the key the genesis below commits is the key this leaf holds.
        let issue = |role| match voter_reach.get(usize::from(n) - 1) {
            None => ca.issue_node(SERVER_NAME, cluster, node, incarnation, role),
            Some(at) => ca.issue_node_at(SERVER_NAME, cluster, node, incarnation, role, at.clone()),
        };
        let issued = issue(PeerRole::Voter);
        voter_keys.push(issued.spki());
        if n == 1 {
            attester = Some(issued.key.serialize_der());
        }
        issued.write(&directory.join("node.pem"), &directory.join("node.key"))?;
        // A different certificate, because it is a different principal:
        // the role that may submit on a caller's behalf is the
        // collector's, not the voter's.
        let collector = issue(PeerRole::Frontend);
        collector.write(
            &directory.join("collector.pem"),
            &directory.join("collector.key"),
        )?;
        std::fs::copy(&trust_bundle, directory.join("roots.pem"))?;
        std::fs::copy(&jwks, directory.join("sts-jwks.json"))?;
        nodes.push((n, node, directory));
    }

    let manifest_path = dir.join("genesis.json");
    let voters: Vec<serde_json::Value> = (0..usize::from(plan.voters))
        .map(|i| {
            serde_json::json!({
                "node": hex(&[i as u8 + 1; 16]),
                "incarnation": 1,
                "public_key": coord_sts::keys::b64url(&voter_keys[i]),
            })
        })
        .collect();
    write_json(
        &manifest_path,
        &serde_json::json!({
            "cluster": hex(&cluster.0),
            "domain": hex(&domain.0),
            "epoch": 1,
            "voters": voters,
            "issuer_roots": [coord_sts::keys::b64url(&[0xca; 8])],
            "wif_rules": [{ "issuer": "harness" }],
            "admin": hex(&principal),
            "protocol_version": 1,
        }),
    )?;

    // Listeners. Both of a node's addresses go in one catalog entry:
    // which of them serves which plane is settled by dialling, because a
    // peer and a collector offer different ALPNs.
    //
    // Without a host list every voter is on this host's loopback at a
    // port nothing is using. With one, each voter is where the list put
    // it, at the ports it named: the catalog is what every other node
    // dials, so it has to say where the node really is.
    let mut api = Vec::new();
    let mut peer = Vec::new();
    if plan.hosts.is_empty() {
        for _ in 0..plan.voters {
            let (a, p) = (free_udp()?, free_udp()?);
            // Listed where it listens.
            api.push((format!("127.0.0.1:{a}"), format!("127.0.0.1:{a}")));
            peer.push((format!("127.0.0.1:{p}"), format!("127.0.0.1:{p}")));
        }
    } else {
        for host in &plan.hosts {
            let listen = |port| bind_address(&host.host, port, plan.listen_any).to_string();
            api.push((authority(&host.host, host.api), listen(host.api)));
            peer.push((authority(&host.host, host.peer), listen(host.peer)));
        }
    }

    let catalog_path = dir.join("endpoints.bin");
    write_catalog(
        &catalog_path,
        cluster,
        domain,
        attester.as_deref().expect("at least one voter"),
        &api.iter().map(|(at, _)| at.clone()).collect::<Vec<_>>(),
        &peer.iter().map(|(at, _)| at.clone()).collect::<Vec<_>>(),
    )?;

    let mut voters_out = Vec::new();
    for (index, (_, node, directory)) in nodes.iter().enumerate() {
        let config = directory.join("coordd.toml");
        // A host list means the node runs somewhere else, from a copy of
        // its own directory. Its configuration then names everything
        // relative to that directory, and the directory holds its own
        // copy of the genesis and the catalog, so the bundle works
        // wherever it is unpacked. Without one the configuration is the
        // absolute one this harness has always written.
        let layout = if plan.hosts.is_empty() {
            Layout::Absolute {
                root: dir,
                node: directory,
            }
        } else {
            std::fs::copy(&manifest_path, directory.join("genesis.json"))?;
            std::fs::copy(&catalog_path, directory.join("endpoints.bin"))?;
            Layout::Bundle
        };
        std::fs::write(
            &config,
            node_config(
                &layout,
                &api[index].1,
                &peer[index].1,
                &hex(&principal),
                &hex(&namespace),
                &hex(&trust_rule),
            ),
        )?;
        voters_out.push(Node {
            node: hex(&node.0),
            config,
            directory: directory.clone(),
            api: api[index].0.clone(),
            peer: peer[index].0.clone(),
        });
    }

    // The caller's credential, and the authority that can issue more.
    //
    // More is the operative word. A client credential carries the
    // node-identity URI this domain's issuer binds, and an api link is
    // keyed by the identity on it -- so two concurrent callers holding
    // one credential would contend for a single slot and a run with two
    // of them would be measuring a queue the harness invented. The
    // benchmark therefore issues one per caller, which it can only do
    // if the authority outlives provisioning.
    let caller_certificate = dir.join("caller.pem");
    let caller_key = dir.join("caller.key");
    issue_caller(&ca, cluster, 0).write(&caller_certificate, &caller_key)?;
    let authority_key = dir.join("domain-ca.key");
    std::fs::write(&authority_key, coord_sts::keys::b64url(&ca.signing_key()))?;
    restrict(&authority_key)?;

    let issuer = provision_issuer(
        dir,
        plan.issuer_listen.as_ref().zip(issuer_reach),
        plan.listen_any,
    )?;
    let edge = provision_edge(
        dir,
        plan.edge_port,
        plan.edge_host.as_deref().zip(edge_reach),
        plan.listen_any,
    )?;

    let provisioned = Provisioned {
        cluster: hex(&cluster.0),
        domain: hex(&domain.0),
        namespace: hex(&namespace),
        principal: hex(&principal),
        trust_rule: hex(&trust_rule),
        server_name: SERVER_NAME.to_owned(),
        issuer_claim: ISSUER.to_owned(),
        resource: RESOURCE.to_owned(),
        trust_bundle,
        manifest: manifest_path,
        catalog: catalog_path,
        voters: voters_out,
        caller_certificate,
        caller_key,
        authority_key,
        issuer,
        edge,
    };
    std::fs::write(
        dir.join("harness.json"),
        serde_json::to_vec_pretty(&provisioned).map_err(std::io::Error::other)?,
    )?;
    Ok(provisioned)
}

/// One caller credential. `ordinal` distinguishes concurrent callers:
/// each is a different client identity, as separate processes would be.
pub fn issue_caller(ca: &Ca, cluster: ClusterId, ordinal: u16) -> Issued {
    let mut node = [0x0c; 16];
    node[14..].copy_from_slice(&ordinal.to_be_bytes());
    ca.issue_node(
        "caller.tuplesky.harness",
        cluster,
        ReplicaId(node),
        ReplicaIncarnation::new(1).expect("one is positive"),
        PeerRole::Client,
    )
}

/// The signed address book. It is this domain's because one of its
/// committed voters attested it; an address list nobody signed is
/// nobody's.
fn write_catalog(
    path: &Path,
    cluster: ClusterId,
    domain: DomainId,
    attester_key: &[u8],
    api: &[String],
    peer: &[String],
) -> std::io::Result<()> {
    use coord_types::config_v1::{EndpointCatalogV1, EndpointV1, VoterSignatureV1};

    let incarnation = ReplicaIncarnation::new(1).expect("one is positive");
    let endpoints: Vec<EndpointV1> = (0..api.len())
        .map(|i| EndpointV1 {
            node: ReplicaId([i as u8 + 1; 16]),
            incarnation,
            addresses: vec![peer[i].clone(), api[i].clone()],
            certificate_fingerprint: None,
        })
        .collect();
    let mut catalog = EndpointCatalogV1 {
        cluster,
        domain,
        epoch: ConfigurationEpoch::new(1).expect("one is positive"),
        generation: EndpointGeneration::new(1).expect("one is positive"),
        endpoints,
        attestation: VoterSignatureV1 {
            node: ReplicaId([1; 16]),
            incarnation,
            signature: vec![0; 64],
        },
    };
    catalog.attestation.signature = coord_membership::configuration::sign_message(
        &jsonwebtoken::EncodingKey::from_ec_der(attester_key),
        &catalog.catalog_message(),
    )
    .map_err(|e| std::io::Error::other(format!("{e:?}")))?;
    std::fs::write(
        path,
        postcard::to_allocvec(&catalog).map_err(std::io::Error::other)?,
    )
}

/// What a bundle's configuration says about itself, and how the harness
/// recognizes one: its paths are relative, so it has to be run from its
/// own directory.
pub const BUNDLE_NOTE: &str = "# Every path below is relative to this directory, and coordd opens a
# relative path against its working directory: run it from here.
";

/// Where a node's configuration says its files are.
enum Layout<'a> {
    /// Absolute paths into the run directory, which is where the node
    /// runs: the single-host domain.
    Absolute {
        /// The run directory, which holds the shared genesis and catalog.
        root: &'a Path,
        /// The node's own directory.
        node: &'a Path,
    },
    /// Paths relative to the node's own directory, which holds
    /// everything the node reads. `coordd` opens a relative path against
    /// its working directory -- it does not resolve paths against the
    /// configuration file, and this harness does not change it to -- so
    /// a bundle is run from inside itself.
    Bundle,
}

impl Layout<'_> {
    fn shared(&self, name: &str) -> String {
        match self {
            Layout::Absolute { root, .. } => format!("{}/{name}", root.display()),
            Layout::Bundle => name.to_owned(),
        }
    }

    fn own(&self, name: &str) -> String {
        match self {
            Layout::Absolute { node, .. } => format!("{}/{name}", node.display()),
            Layout::Bundle => name.to_owned(),
        }
    }

    fn state_directory(&self) -> String {
        match self {
            Layout::Absolute { node, .. } => node.display().to_string(),
            Layout::Bundle => ".".to_owned(),
        }
    }

    fn preamble(&self) -> &'static str {
        match self {
            Layout::Absolute { .. } => "",
            Layout::Bundle => BUNDLE_NOTE,
        }
    }
}

fn node_config(
    layout: &Layout<'_>,
    api: &str,
    peer: &str,
    principal: &str,
    namespace: &str,
    trust_rule: &str,
) -> String {
    format!(
        r#"# Written by `coord-harness provision`. A strict configuration: the
# daemon that reads it runs the same startup checks it runs anywhere.
{preamble}config_version = 2
role = "voter-frontend-observer"
cluster_manifest = "{manifest}"
cluster_endpoints = "{endpoints}"
domain = "{resource}"
state_directory = "{state}"

[listen]
api_quic = "{api}"
peer_quic = "{peer}"

[capability]
writer_queue_bytes = 16777216
buffer_bytes_per_subscription = 8388608
max_live_subscriptions = 4096

[state]
root = "state"

[journal]
root = "journal"
shards = 1

[identity]
trust_bundle = "{roots}"
node_certificate = "{node_certificate}"
node_key = "{node_key}"
collector_certificate = "{collector_certificate}"
collector_key = "{collector_key}"

[sts]
issuer = "{issuer}"
resource = "{resource}"
jwks = "{jwks}"
trust_rule = "{trust_rule}"

[[grant]]
principal = "{principal}"
namespace = "{namespace}"
"#,
        preamble = layout.preamble(),
        manifest = layout.shared("genesis.json"),
        endpoints = layout.shared("endpoints.bin"),
        state = layout.state_directory(),
        roots = layout.own("roots.pem"),
        node_certificate = layout.own("node.pem"),
        node_key = layout.own("node.key"),
        collector_certificate = layout.own("collector.pem"),
        collector_key = layout.own("collector.key"),
        jwks = layout.own("sts-jwks.json"),
        issuer = ISSUER,
        resource = RESOURCE,
    )
}

/// The credential endpoint's material. On loopback unless `at` names
/// where it is reached, in which case its certificate carries that host
/// and its URL names it -- the certificate is what a client on another
/// host verifies, so the two are written together or not at all.
fn provision_issuer(
    dir: &Path,
    at: Option<(&Address, rcgen::SanType)>,
    listen_any: bool,
) -> std::io::Result<Issuer> {
    let ca = Ca::new();
    let root = dir.join("issuer-ca.pem");
    std::fs::write(&root, ca.root_pem())?;
    let issued = match &at {
        None => ca.issue_server(ISSUER_NAME),
        Some((_, reach)) => ca.issue_server_at(ISSUER_NAME, reach.clone()),
    };
    let certificate = dir.join("issuer.pem");
    let key = dir.join("issuer.key");
    issued.write(&certificate, &key)?;
    let (listen, url) = match at {
        None => {
            let port = free_tcp()?;
            (
                format!("127.0.0.1:{port}"),
                format!("https://127.0.0.1:{port}"),
            )
        }
        Some((address, _)) => {
            let port = if address.port == 0 {
                free_tcp()?
            } else {
                address.port
            };
            (
                bind_address(&address.host, port, listen_any).to_string(),
                format!("https://{}", authority(&address.host, port)),
            )
        }
    };
    let assertion = dir.join("workload-assertion");
    std::fs::write(&assertion, "harness-workload-assertion\n")?;
    restrict(&assertion)?;
    Ok(Issuer {
        listen,
        url,
        ca: root,
        certificate,
        key,
        assertion,
        signing_key: dir.join("sts-signing.key"),
    })
}

/// The storage edge's material. `at` is the host an API server reaches
/// it at, when that is not loopback: an API server verifies the edge
/// against the host of the endpoint it is configured with unless told
/// otherwise, so the server certificate carries it.
fn provision_edge(
    dir: &Path,
    port: u16,
    at: Option<(&str, rcgen::SanType)>,
    listen_any: bool,
) -> std::io::Result<Edge> {
    let server_ca = Ca::new();
    let client_ca = Ca::new();
    let foreign_ca = Ca::new();

    let server_root = dir.join("edge-server-ca.pem");
    std::fs::write(&server_root, server_ca.root_pem())?;
    let client_root = dir.join("edge-client-ca.pem");
    std::fs::write(&client_root, client_ca.root_pem())?;
    let foreign_root = dir.join("edge-foreign-ca.pem");
    std::fs::write(&foreign_root, foreign_ca.root_pem())?;

    let server = match &at {
        None => server_ca.issue_server(EDGE_SERVER_NAME),
        Some((_, reach)) => server_ca.issue_server_at(EDGE_SERVER_NAME, reach.clone()),
    };
    let server_certificate = dir.join("edge-server.pem");
    let server_key = dir.join("edge-server.key");
    server.write(&server_certificate, &server_key)?;

    let write_client = |ca: &Ca, name: &str, stem: &str| -> std::io::Result<(PathBuf, PathBuf)> {
        let issued: Issued = ca.issue_leaf(&[name.to_owned()], Vec::new());
        let certificate = dir.join(format!("{stem}.pem"));
        let key = dir.join(format!("{stem}.key"));
        issued.write(&certificate, &key)?;
        Ok((certificate, key))
    };
    let (client_certificate, client_key) =
        write_client(&client_ca, EDGE_CLIENT_NAME, "edge-client")?;
    let (unauthorized_certificate, unauthorized_key) =
        write_client(&client_ca, EDGE_INTRUDER_NAME, "edge-unauthorized")?;
    let (foreign_certificate, foreign_key) =
        write_client(&foreign_ca, EDGE_CLIENT_NAME, "edge-foreign")?;

    let port = if port == 0 { free_tcp()? } else { port };
    let (listen, endpoint) = match at {
        None => {
            let listen: SocketAddr = format!("127.0.0.1:{port}").parse().expect("loopback");
            (listen.to_string(), format!("https://{listen}"))
        }
        Some((host, _)) => (
            bind_address(host, port, listen_any).to_string(),
            format!("https://{}", authority(host, port)),
        ),
    };
    Ok(Edge {
        listen,
        endpoint,
        server_name: EDGE_SERVER_NAME.to_owned(),
        server_certificate,
        server_key,
        client_ca: client_root,
        server_ca: server_root,
        allowed_client: EDGE_CLIENT_NAME.to_owned(),
        client_certificate,
        client_key,
        unauthorized_certificate,
        unauthorized_key,
        foreign_certificate,
        foreign_key,
        foreign_ca: foreign_root,
    })
}

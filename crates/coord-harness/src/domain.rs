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
        let issued = ca.issue_node(SERVER_NAME, cluster, node, incarnation, PeerRole::Voter);
        voter_keys.push(issued.spki());
        if n == 1 {
            attester = Some(issued.key.serialize_der());
        }
        issued.write(&directory.join("node.pem"), &directory.join("node.key"))?;
        // A different certificate, because it is a different principal:
        // the role that may submit on a caller's behalf is the
        // collector's, not the voter's.
        let collector = ca.issue_node(SERVER_NAME, cluster, node, incarnation, PeerRole::Frontend);
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
    let mut api = Vec::new();
    let mut peer = Vec::new();
    for _ in 0..plan.voters {
        api.push(free_udp()?);
        peer.push(free_udp()?);
    }

    let catalog_path = dir.join("endpoints.bin");
    write_catalog(
        &catalog_path,
        cluster,
        domain,
        attester.as_deref().expect("at least one voter"),
        &api,
        &peer,
    )?;

    let mut voters_out = Vec::new();
    for (index, (n, node, directory)) in nodes.iter().enumerate() {
        let config = directory.join("coordd.toml");
        std::fs::write(
            &config,
            node_config(
                dir,
                directory,
                api[index],
                peer[index],
                &hex(&principal),
                &hex(&namespace),
                &hex(&trust_rule),
            ),
        )?;
        let _ = n;
        voters_out.push(Node {
            node: hex(&node.0),
            config,
            directory: directory.clone(),
            api: format!("127.0.0.1:{}", api[index]),
            peer: format!("127.0.0.1:{}", peer[index]),
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

    let issuer = provision_issuer(dir)?;
    let edge = provision_edge(dir, plan.edge_port)?;

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
    api: &[u16],
    peer: &[u16],
) -> std::io::Result<()> {
    use coord_types::config_v1::{EndpointCatalogV1, EndpointV1, VoterSignatureV1};

    let incarnation = ReplicaIncarnation::new(1).expect("one is positive");
    let endpoints: Vec<EndpointV1> = (0..api.len())
        .map(|i| EndpointV1 {
            node: ReplicaId([i as u8 + 1; 16]),
            incarnation,
            addresses: vec![
                format!("127.0.0.1:{}", peer[i]),
                format!("127.0.0.1:{}", api[i]),
            ],
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

#[allow(clippy::too_many_arguments)]
fn node_config(
    root: &Path,
    node: &Path,
    api: u16,
    peer: u16,
    principal: &str,
    namespace: &str,
    trust_rule: &str,
) -> String {
    format!(
        r#"# Written by `coord-harness provision`. A strict configuration: the
# daemon that reads it runs the same startup checks it runs anywhere.
config_version = 2
role = "voter-frontend-observer"
cluster_manifest = "{root}/genesis.json"
cluster_endpoints = "{root}/endpoints.bin"
domain = "{resource}"
state_directory = "{node}"

[listen]
api_quic = "127.0.0.1:{api}"
peer_quic = "127.0.0.1:{peer}"

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
trust_bundle = "{node}/roots.pem"
node_certificate = "{node}/node.pem"
node_key = "{node}/node.key"
collector_certificate = "{node}/collector.pem"
collector_key = "{node}/collector.key"

[sts]
issuer = "{issuer}"
resource = "{resource}"
jwks = "{node}/sts-jwks.json"
trust_rule = "{trust_rule}"

[[grant]]
principal = "{principal}"
namespace = "{namespace}"
"#,
        root = root.display(),
        node = node.display(),
        issuer = ISSUER,
        resource = RESOURCE,
    )
}

fn provision_issuer(dir: &Path) -> std::io::Result<Issuer> {
    let ca = Ca::new();
    let root = dir.join("issuer-ca.pem");
    std::fs::write(&root, ca.root_pem())?;
    let issued = ca.issue_server("sts.tuplesky.harness");
    let certificate = dir.join("issuer.pem");
    let key = dir.join("issuer.key");
    issued.write(&certificate, &key)?;
    let port = free_tcp()?;
    let assertion = dir.join("workload-assertion");
    std::fs::write(&assertion, "harness-workload-assertion\n")?;
    restrict(&assertion)?;
    Ok(Issuer {
        listen: format!("127.0.0.1:{port}"),
        url: format!("https://127.0.0.1:{port}"),
        ca: root,
        certificate,
        key,
        assertion,
        signing_key: dir.join("sts-signing.key"),
    })
}

fn provision_edge(dir: &Path, port: u16) -> std::io::Result<Edge> {
    let server_ca = Ca::new();
    let client_ca = Ca::new();
    let foreign_ca = Ca::new();

    let server_root = dir.join("edge-server-ca.pem");
    std::fs::write(&server_root, server_ca.root_pem())?;
    let client_root = dir.join("edge-client-ca.pem");
    std::fs::write(&client_root, client_ca.root_pem())?;
    let foreign_root = dir.join("edge-foreign-ca.pem");
    std::fs::write(&foreign_root, foreign_ca.root_pem())?;

    let server = server_ca.issue_server(EDGE_SERVER_NAME);
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
    let listen: SocketAddr = format!("127.0.0.1:{port}").parse().expect("loopback");
    Ok(Edge {
        listen: listen.to_string(),
        endpoint: format!("https://{listen}"),
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

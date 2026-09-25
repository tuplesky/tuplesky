//! What a provisioned domain has to be true of before anything is
//! started against it.
//!
//! These are cheap checks of the fixture, and they are here because a
//! harness bug looks exactly like a result. A domain whose genesis does
//! not commit the key a node will present, or whose catalog nobody
//! signed, would make every daemon refuse to start for a reason that has
//! nothing to do with what the run is measuring.

use std::path::Path;

use coord_harness::domain::{Address, Plan, Provisioned, parse_hosts, provision};

fn provisioned(voters: u8) -> (tempfile::TempDir, Provisioned) {
    let dir = tempfile::tempdir().expect("a run directory");
    let out = provision(&Plan::loopback(dir.path().to_path_buf(), voters, 0)).expect("provisioned");
    (dir, out)
}
/// Every file the description names is on disk, and the description is
/// what a second process reads rather than reconstructs.
#[test]
fn the_description_names_material_that_exists() {
    let (dir, out) = provisioned(3);
    for path in [
        &out.trust_bundle,
        &out.manifest,
        &out.catalog,
        &out.caller_certificate,
        &out.caller_key,
        &out.authority_key,
        &out.issuer.ca,
        &out.issuer.certificate,
        &out.issuer.key,
        &out.issuer.assertion,
        &out.issuer.signing_key,
        &out.edge.server_certificate,
        &out.edge.client_ca,
        &out.edge.client_certificate,
        &out.edge.unauthorized_certificate,
        &out.edge.foreign_certificate,
    ] {
        assert!(path.is_file(), "{} is missing", path.display());
    }
    for node in &out.voters {
        assert!(node.config.is_file(), "{}", node.config.display());
        assert!(node.directory.join("node.pem").is_file());
        assert!(node.directory.join("collector.pem").is_file());
    }
    let read = Provisioned::read(dir.path()).expect("the description reads back");
    assert_eq!(read.cluster, out.cluster);
    assert_eq!(read.voters.len(), 3);
}

/// The genesis commits the key each node will actually present.
///
/// Without this a node starts perfectly well -- nothing it does alone
/// checks its own key -- and is then refused by every peer, which looks
/// like a network problem and is not.
#[test]
fn genesis_commits_the_key_every_node_presents() {
    use x509_parser::prelude::FromDer;

    let (_dir, out) = provisioned(3);
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&out.manifest).expect("manifest")).expect("json");
    let voters = manifest["voters"].as_array().expect("voters");
    assert_eq!(voters.len(), 3);
    for (index, node) in out.voters.iter().enumerate() {
        let pem = std::fs::read_to_string(node.directory.join("node.pem")).expect("node.pem");
        let der = decode_pem(&pem);
        let (_, certificate) =
            x509_parser::certificate::X509Certificate::from_der(&der).expect("a certificate");
        let committed = voters[index]["public_key"].as_str().expect("a key");
        assert_eq!(
            committed,
            coord_sts::keys::b64url(certificate.public_key().raw),
            "voter {} is committed under a key it does not hold",
            index + 1
        );
    }
}

/// The two authorities of the storage edge are different authorities.
///
/// The API server's client CA is not the domain's peer CA, and a harness
/// that used one for both would pass the "an unauthorized client is
/// refused" case for the wrong reason.
#[test]
fn the_storage_edge_has_two_separate_authorities() {
    let (_dir, out) = provisioned(1);
    let client = std::fs::read_to_string(&out.edge.client_ca).expect("client ca");
    let server = std::fs::read_to_string(&out.edge.server_ca).expect("server ca");
    let foreign = std::fs::read_to_string(&out.edge.foreign_ca).expect("foreign ca");
    let domain = std::fs::read_to_string(&out.trust_bundle).expect("trust bundle");
    assert_ne!(client, server);
    assert_ne!(client, foreign);
    assert_ne!(client, domain);
    assert_ne!(server, domain);
}

/// A caller credential is issued per caller, and two of them are two
/// identities.
#[test]
fn every_caller_gets_its_own_identity() {
    let (_dir, out) = provisioned(1);
    let encoded = std::fs::read_to_string(&out.authority_key).expect("authority key");
    let authority =
        coord_harness::pki::Ca::reopen(&b64url_decode(&encoded).expect("base64url")).expect("ca");
    let cluster =
        coord_types::ids::ClusterId(parse_id(&out.cluster).expect("a cluster identifier"));
    let first = coord_harness::domain::issue_caller(&authority, cluster, 0);
    let second = coord_harness::domain::issue_caller(&authority, cluster, 1);
    assert_ne!(
        first.certificate.der().as_ref(),
        second.certificate.der().as_ref()
    );
}

/// The minted credential is a real service token of the keys the domain
/// publishes, and two exchanges name two sessions.
#[test]
fn the_minted_credential_verifies_against_the_published_keys() {
    let (dir, out) = provisioned(1);
    let minter = coord_harness::issuer::Minter::load(dir.path()).expect("minter");
    let (first, one) = minter.mint_token().expect("a token");
    let (_second, two) = minter.mint_token().expect("a second token");
    assert_ne!(one, two, "two exchanges named one session");

    let jwks: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("sts-jwks.json")).expect("jwks"))
            .expect("json");
    assert!(coord_sts::usable_verification_keys(&jwks) > 0);
    let verified = coord_sts::verify_service_token(
        &first,
        &jwks,
        &out.issuer_claim,
        &out.resource,
        &coord_authn::ClockHealth::healthy(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("after the epoch")
                .as_secs(),
            5,
        ),
    )
    .expect("the domain's own verifier accepts it");
    assert_eq!(verified.sid, hex(&one));
}

/// The issuer binds loopback and refuses anything else: it signs
/// credentials on presentation of any assertion, so it must not be
/// reachable from off the host.
#[test]
fn the_issuer_is_loopback_only() {
    let (dir, _out) = provisioned(1);
    let description = dir.path().join("harness.json");
    let mut value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&description).expect("read")).expect("json");
    value["issuer"]["listen"] = serde_json::json!("0.0.0.0:0");
    std::fs::write(&description, serde_json::to_vec(&value).expect("json")).expect("write");
    let err = coord_harness::issuer::Endpoint::bind(dir.path())
        .err()
        .expect("a non-loopback issuer is refused");
    assert!(
        matches!(err, coord_harness::issuer::IssuerError::NotLoopback(_)),
        "{err}"
    );
}

/// The configuration `coord-harness provision` wrote before a host list
/// existed, verbatim. `{root}`, `{node}`, `{api}` and `{peer}` are the
/// run directory, the node's directory and its two loopback ports.
const LOOPBACK_CONFIG: &str = r#"# Written by `coord-harness provision`. A strict configuration: the
# daemon that reads it runs the same startup checks it runs anywhere.
config_version = 2
role = "voter-frontend-observer"
cluster_manifest = "{root}/genesis.json"
cluster_endpoints = "{root}/endpoints.bin"
domain = "tuplesky-harness"
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
issuer = "https://sts.tuplesky.harness"
resource = "tuplesky-harness"
jwks = "{node}/sts-jwks.json"
trust_rule = "7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c"

[[grant]]
principal = "0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a"
namespace = "5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e"
"#;

/// Every file a three-voter loopback run directory held before a host
/// list existed, and nothing else.
const LOOPBACK_FILES: &[&str] = &[
    "caller.key",
    "caller.pem",
    "domain-ca.key",
    "edge-client-ca.pem",
    "edge-client.key",
    "edge-client.pem",
    "edge-foreign-ca.pem",
    "edge-foreign.key",
    "edge-foreign.pem",
    "edge-server-ca.pem",
    "edge-server.key",
    "edge-server.pem",
    "edge-unauthorized.key",
    "edge-unauthorized.pem",
    "endpoints.bin",
    "genesis.json",
    "harness.json",
    "issuer-ca.pem",
    "issuer.key",
    "issuer.pem",
    "n1/collector.key",
    "n1/collector.pem",
    "n1/coordd.toml",
    "n1/node.key",
    "n1/node.pem",
    "n1/roots.pem",
    "n1/sts-jwks.json",
    "n2/collector.key",
    "n2/collector.pem",
    "n2/coordd.toml",
    "n2/node.key",
    "n2/node.pem",
    "n2/roots.pem",
    "n2/sts-jwks.json",
    "n3/collector.key",
    "n3/collector.pem",
    "n3/coordd.toml",
    "n3/node.key",
    "n3/node.pem",
    "n3/roots.pem",
    "n3/sts-jwks.json",
    "roots.pem",
    "sts-jwks.json",
    "sts-signing.key",
    "workload-assertion",
];

/// Provisioning without a host list writes what it always wrote
/// (task-d04).
///
/// Keys and free ports are fresh on every run, so "byte for byte" is
/// byte for byte around them: the same files and no others, each node's
/// configuration exactly the text above, every address loopback, and
/// every certificate reachable at loopback and nowhere else. The
/// certification workflow, the benchmarks and every script that reads a
/// run directory depend on this staying put while the host list is
/// added beside it.
#[test]
fn provisioning_without_hosts_writes_what_it_always_wrote() {
    let (dir, out) = provisioned(3);
    let root = dir.path();

    let mut files = Vec::new();
    walk(root, root, &mut files);
    files.sort();
    assert_eq!(files, LOOPBACK_FILES);

    let catalog = catalog(&out.catalog);
    for (index, node) in out.voters.iter().enumerate() {
        let api = loopback_port(&node.api);
        let peer = loopback_port(&node.peer);
        let expected = LOOPBACK_CONFIG
            .replace("{root}", &root.display().to_string())
            .replace("{node}", &node.directory.display().to_string())
            .replace("{api}", &api.to_string())
            .replace("{peer}", &peer.to_string());
        assert_eq!(
            std::fs::read_to_string(&node.config).expect("config"),
            expected
        );
        assert_eq!(node.directory, root.join(format!("n{}", index + 1)));
        assert_eq!(
            catalog.endpoints[index].addresses,
            vec![node.peer.clone(), node.api.clone()]
        );
        for certificate in ["node.pem", "collector.pem"] {
            let names = sans(&node.directory.join(certificate));
            assert_eq!(names.len(), 3, "{names:?}");
            assert_eq!(names[..2], ["DNS:node.tuplesky.harness", "IP:127.0.0.1"]);
            assert!(names[2].starts_with("URI:tuplesky://cluster/"), "{names:?}");
        }
    }

    let issuer_port = loopback_port(&out.issuer.listen);
    assert_eq!(out.issuer.url, format!("https://127.0.0.1:{issuer_port}"));
    assert_eq!(
        sans(&out.issuer.certificate),
        ["DNS:sts.tuplesky.harness", "IP:127.0.0.1"]
    );
    let edge_port = loopback_port(&out.edge.listen);
    assert_eq!(out.edge.endpoint, format!("https://127.0.0.1:{edge_port}"));
    assert_eq!(
        sans(&out.edge.server_certificate),
        ["DNS:kine.tuplesky.harness", "IP:127.0.0.1"]
    );

    // The description has the fields it had, and no new ones.
    let description: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("harness.json")).expect("read"))
            .expect("json");
    let keys = |value: &serde_json::Value| -> Vec<String> {
        let mut keys: Vec<String> = value.as_object().expect("object").keys().cloned().collect();
        keys.sort();
        keys
    };
    assert_eq!(
        keys(&description),
        [
            "authority_key",
            "caller_certificate",
            "caller_key",
            "catalog",
            "cluster",
            "domain",
            "edge",
            "issuer",
            "issuer_claim",
            "manifest",
            "namespace",
            "principal",
            "resource",
            "server_name",
            "trust_bundle",
            "trust_rule",
            "voters"
        ]
    );
    assert_eq!(
        keys(&description["voters"][0]),
        ["api", "config", "directory", "node", "peer"]
    );
    assert_eq!(
        keys(&description["issuer"]),
        [
            "assertion",
            "ca",
            "certificate",
            "key",
            "listen",
            "signing_key",
            "url"
        ]
    );
    assert_eq!(keys(&description["edge"]).len(), 15);
}

/// A host list puts every voter where it says: the catalog lists the
/// given addresses, each configuration listens on the given fixed ports,
/// and each node's certificates are valid for its host rather than for
/// loopback (task-d04).
///
/// A peer verifies the certificate it is shown against the host part of
/// the catalog address it dialled, so a node placed at `127.0.0.3` with
/// a certificate for `127.0.0.1` is refused by every peer -- and that
/// looks like a network fault. A DNS name is carried as a name and
/// listened for on the unspecified address, because a strict
/// configuration's listener is an address and not a name.
#[test]
fn a_host_list_places_every_voter_where_it_says() {
    let dir = tempfile::tempdir().expect("a run directory");
    let root = dir.path();
    let hosts = parse_hosts(
        "n1=127.0.0.2:7101:7102, n2=127.0.0.3:7101:7102, n3=n3.tuplesky.test:7101:7102",
    )
    .expect("a host list");
    let out = provision(&Plan {
        hosts,
        edge_host: Some("edge.tuplesky.test".into()),
        issuer_listen: Some(Address::parse("192.0.2.10:8443").expect("an address")),
        ..Plan::loopback(root.to_path_buf(), 3, 7379)
    })
    .expect("provisioned");

    let placed = ["127.0.0.2", "127.0.0.3", "n3.tuplesky.test"];
    let listens = ["127.0.0.2", "127.0.0.3", "0.0.0.0"];
    let reached = ["IP:127.0.0.2", "IP:127.0.0.3", "DNS:n3.tuplesky.test"];
    let catalog = catalog(&out.catalog);
    for (index, node) in out.voters.iter().enumerate() {
        assert_eq!(node.api, format!("{}:7101", placed[index]));
        assert_eq!(node.peer, format!("{}:7102", placed[index]));
        assert_eq!(
            catalog.endpoints[index].addresses,
            vec![node.peer.clone(), node.api.clone()]
        );
        let config = std::fs::read_to_string(&node.config).expect("config");
        assert!(config.contains(&format!("api_quic = \"{}:7101\"\n", listens[index])));
        assert!(config.contains(&format!("peer_quic = \"{}:7102\"\n", listens[index])));
        for certificate in ["node.pem", "collector.pem"] {
            let names = sans(&node.directory.join(certificate));
            assert_eq!(names.len(), 3, "{names:?}");
            assert_eq!(names[..2], ["DNS:node.tuplesky.harness", reached[index]]);
            assert!(names[2].starts_with("URI:tuplesky://cluster/"), "{names:?}");
        }
    }

    assert_eq!(out.issuer.listen, "192.0.2.10:8443");
    assert_eq!(out.issuer.url, "https://192.0.2.10:8443");
    assert_eq!(
        sans(&out.issuer.certificate),
        ["DNS:sts.tuplesky.harness", "IP:192.0.2.10"]
    );
    assert_eq!(out.edge.listen, "0.0.0.0:7379");
    assert_eq!(out.edge.endpoint, "https://edge.tuplesky.test:7379");
    assert_eq!(
        sans(&out.edge.server_certificate),
        ["DNS:kine.tuplesky.harness", "DNS:edge.tuplesky.test"]
    );
}

/// Each `nN/` of a placed domain is a bundle: its configuration names
/// only files inside it, relative to it, and it runs wherever it is
/// copied (task-d04).
///
/// The node reads its own copy of the genesis and the catalog, so the
/// copy has to be the same bytes the run directory's are and the genesis
/// has to commit the key the bundle's certificate holds. `coordd` opens a
/// relative path against its working directory, which is why the harness
/// starts a bundle from inside it and the runbook says to.
#[test]
fn every_placed_node_is_a_bundle_that_runs_wherever_it_is_copied() {
    use x509_parser::prelude::FromDer;

    let dir = tempfile::tempdir().expect("a run directory");
    let run = dir.path().join("run");
    let out = provision(&Plan {
        hosts: parse_hosts("n1=10.0.0.1:7101:7102,n2=10.0.0.2:7101:7102,n3=10.0.0.3:7101:7102")
            .expect("a host list"),
        ..Plan::loopback(run.clone(), 3, 0)
    })
    .expect("provisioned");

    let manifest = std::fs::read(&out.manifest).expect("genesis");
    let committed: serde_json::Value = serde_json::from_slice(&manifest).expect("json");
    for (index, node) in out.voters.iter().enumerate() {
        // Somewhere else entirely, as another host would have it.
        let bundle = dir.path().join(format!("elsewhere-{index}"));
        std::fs::create_dir_all(&bundle).expect("bundle");
        for entry in std::fs::read_dir(&node.directory).expect("read") {
            let entry = entry.expect("entry");
            std::fs::copy(entry.path(), bundle.join(entry.file_name())).expect("copy");
        }
        let config = std::fs::read_to_string(bundle.join("coordd.toml")).expect("config");
        assert!(config.contains(coord_harness::domain::BUNDLE_NOTE));
        assert!(config.contains("state_directory = \".\"\n"));
        for line in config.lines() {
            let Some((name, value)) = line.split_once(" = \"") else {
                continue;
            };
            let value = value.trim_end_matches('"');
            if !value.contains('.') || value.contains("://") || value.contains(':') {
                continue;
            }
            assert!(
                !value.starts_with('/'),
                "{name} names an absolute path: {value}"
            );
            if value != "." {
                assert!(
                    bundle.join(value).is_file(),
                    "{name} = {value} is not in the bundle"
                );
            }
        }
        assert_eq!(
            std::fs::read(bundle.join("genesis.json")).expect("genesis"),
            manifest
        );
        assert_eq!(
            std::fs::read(bundle.join("endpoints.bin")).expect("catalog"),
            std::fs::read(&out.catalog).expect("catalog")
        );
        let der = decode_pem(&std::fs::read_to_string(bundle.join("node.pem")).expect("pem"));
        let (_, certificate) =
            x509_parser::certificate::X509Certificate::from_der(&der).expect("a certificate");
        assert_eq!(
            committed["voters"][index]["public_key"].as_str(),
            Some(coord_sts::keys::b64url(certificate.public_key().raw).as_str()),
            "voter {} is committed under a key its bundle does not hold",
            index + 1
        );
    }
}

/// A host list names voters one to N once each, on ports no two
/// listeners share, at hosts a certificate can name -- or it is refused
/// before anything is written.
#[test]
fn a_host_list_that_does_not_place_every_voter_once_is_refused() {
    for (spec, why) in [
        ("n1=10.0.0.1:1:2,n3=10.0.0.3:1:2", "a gap"),
        ("n1=10.0.0.1:1:2,n1=10.0.0.2:1:2", "a voter twice"),
        ("n2=10.0.0.2:1:2", "no n1"),
        ("n1=10.0.0.1:1:1", "one port for both planes"),
        ("n1=10.0.0.1:1:2,n2=10.0.0.1:2:3", "two voters on one port"),
        ("n1=10.0.0.1:0:2", "an ephemeral port"),
        ("n1=10.0.0.1:1", "a missing port"),
        ("n1=bad_host!:1:2", "a host no certificate can name"),
        ("x1=10.0.0.1:1:2", "a voter name"),
        ("n1=fd00::1:7101:7102", "an IPv6 host without brackets"),
        ("", "nothing"),
    ] {
        assert!(parse_hosts(spec).is_err(), "{why} was accepted: {spec}");
    }
    assert!(Address::parse("fd00::1:7443").is_err(), "unbracketed IPv6");
    assert_eq!(
        Address::parse("[fd00::1]:7443").expect("bracketed"),
        Address {
            host: "fd00::1".into(),
            port: 7443
        }
    );

    // Two voters may share a port on different hosts, which is what a
    // deployment does, and an IPv6 host is written in brackets.
    let hosts = parse_hosts("n2=[fd00::2]:7101:7102,n1=[fd00::1]:7101:7102").expect("accepted");
    assert_eq!(hosts[0].node, 1);
    assert_eq!(hosts[0].host, "fd00::1");

    let dir = tempfile::tempdir().expect("a run directory");
    let out = provision(&Plan {
        hosts: hosts.clone(),
        ..Plan::loopback(dir.path().to_path_buf(), 2, 0)
    })
    .expect("provisioned");
    assert_eq!(out.voters[0].api, "[fd00::1]:7101");
    let config = std::fs::read_to_string(&out.voters[0].config).expect("config");
    assert!(config.contains("api_quic = \"[fd00::1]:7101\"\n"));

    // A plan whose host list does not place every committed voter.
    let dir = tempfile::tempdir().expect("a run directory");
    assert!(
        provision(&Plan {
            hosts,
            ..Plan::loopback(dir.path().to_path_buf(), 3, 0)
        })
        .is_err(),
        "a committed voter no host runs was provisioned"
    );
}

/// The credential endpoint leaves loopback only for the host its
/// certificate was provisioned for, on its port (task-d04).
///
/// It signs on presentation of any assertion, so where it listens is the
/// whole of its containment. `--issuer-listen` at provisioning is the
/// opt-in, because that is when the certificate is issued; a bind asked
/// for afterwards is checked against the certificate, so editing the
/// description does not buy a network listener.
#[test]
fn the_issuer_leaves_loopback_only_for_the_host_it_was_provisioned_for() {
    use coord_harness::issuer::{Endpoint, IssuerError};

    let free = || {
        std::net::TcpListener::bind("0.0.0.0:0")
            .and_then(|l| l.local_addr())
            .expect("a free port")
            .port()
    };
    let refused = |dir: &Path, listen: &str| match Endpoint::bind_on(
        dir,
        Some(listen.parse().expect("an address")),
    ) {
        Err(IssuerError::NotLoopback(_)) => true,
        Err(other) => panic!("{listen} was refused for another reason: {other}"),
        Ok(_) => false,
    };

    // Provisioned for a host this machine does not have, which is what
    // one-to-one NAT looks like from the inside.
    let port = free();
    let dir = tempfile::tempdir().expect("a run directory");
    provision(&Plan {
        issuer_listen: Some(Address {
            host: "192.0.2.10".into(),
            port,
        }),
        ..Plan::loopback(dir.path().to_path_buf(), 1, 0)
    })
    .expect("provisioned");
    let endpoint = Endpoint::bind_on(dir.path(), Some(format!("0.0.0.0:{port}").parse().unwrap()))
        .expect("the unspecified address on the provisioned port is permitted");
    drop(endpoint);
    assert!(refused(
        dir.path(),
        &format!("0.0.0.0:{}", port.wrapping_add(1).max(1))
    ));
    assert!(refused(dir.path(), &format!("192.0.2.11:{port}")));
    // Loopback is always permitted.
    assert!(!refused(dir.path(), "127.0.0.1:0"));

    // A loopback domain does not leave loopback, whatever is asked...
    let (dir, out) = provisioned(1);
    let port = loopback_port(&out.issuer.listen);
    assert!(refused(dir.path(), &format!("0.0.0.0:{port}")));
    // ...and pointing its description somewhere else does not change
    // what its certificate names.
    let description = dir.path().join("harness.json");
    let mut value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&description).expect("read")).expect("json");
    value["issuer"]["url"] = serde_json::json!(format!("https://192.0.2.10:{port}"));
    std::fs::write(&description, serde_json::to_vec(&value).expect("json")).expect("write");
    assert!(refused(dir.path(), &format!("0.0.0.0:{port}")));
    // Nor does pointing it at the issuer's own name, which the loopback
    // certificate does carry, as every issuer certificate does.
    value["issuer"]["url"] = serde_json::json!(format!(
        "https://{}:{port}",
        coord_harness::domain::ISSUER_NAME.to_ascii_uppercase()
    ));
    std::fs::write(&description, serde_json::to_vec(&value).expect("json")).expect("write");
    assert!(refused(dir.path(), &format!("0.0.0.0:{port}")));

    // And that name is refused as a place to provision the endpoint at.
    let dir = tempfile::tempdir().expect("a run directory");
    let error = provision(&Plan {
        issuer_listen: Some(Address {
            host: coord_harness::domain::ISSUER_NAME.into(),
            port: free(),
        }),
        ..Plan::loopback(dir.path().to_path_buf(), 1, 0)
    })
    .expect_err("the issuer's own name is not a host");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput, "{error}");
    // Nor is a name that only ever means the machine it is resolved on,
    // which would otherwise be a DNS name bound at the wildcard.
    for host in ["localhost", "Issuer.LOCALHOST."] {
        let dir = tempfile::tempdir().expect("a run directory");
        let error = provision(&Plan {
            issuer_listen: Some(Address {
                host: host.into(),
                port: free(),
            }),
            ..Plan::loopback(dir.path().to_path_buf(), 1, 0)
        })
        .expect_err("a loopback name is not a host other machines reach");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput, "{error}");
    }

    // Provisioned for a DNS name, the wildcard on its port is permitted,
    // and a description naming any other host is not: the certificate
    // names exactly one, and the URL has to be it.
    let port = free();
    let dir = tempfile::tempdir().expect("a run directory");
    provision(&Plan {
        issuer_listen: Some(Address {
            host: "issuer.example".into(),
            port,
        }),
        ..Plan::loopback(dir.path().to_path_buf(), 1, 0)
    })
    .expect("provisioned");
    drop(
        Endpoint::bind_on(dir.path(), Some(format!("0.0.0.0:{port}").parse().unwrap()))
            .expect("the unspecified address on the provisioned port is permitted"),
    );
    let description = dir.path().join("harness.json");
    let mut value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&description).expect("read")).expect("json");
    for host in ["localhost", "other.example", "192.0.2.10"] {
        value["issuer"]["url"] = serde_json::json!(format!("https://{host}:{port}"));
        std::fs::write(&description, serde_json::to_vec(&value).expect("json")).expect("write");
        assert!(refused(dir.path(), &format!("0.0.0.0:{port}")), "{host}");
    }
}

fn walk(root: &Path, dir: &Path, out: &mut Vec<String>) {
    for entry in std::fs::read_dir(dir).expect("read") {
        let path = entry.expect("entry").path();
        if path.is_dir() {
            walk(root, &path, out);
        } else {
            out.push(
                path.strip_prefix(root)
                    .expect("inside")
                    .display()
                    .to_string(),
            );
        }
    }
}

/// The port of a `127.0.0.1:port` address, which it has to be.
fn loopback_port(address: &str) -> u16 {
    address
        .strip_prefix("127.0.0.1:")
        .and_then(|port| port.parse().ok())
        .unwrap_or_else(|| panic!("{address} is not a loopback address"))
}

fn catalog(path: &Path) -> coord_types::config_v1::EndpointCatalogV1 {
    postcard::from_bytes(&std::fs::read(path).expect("catalog")).expect("a catalog")
}

/// A certificate's subject alternative names, in order, as `DNS:`,
/// `IP:` or `URI:` strings.
fn sans(path: &Path) -> Vec<String> {
    use x509_parser::extensions::GeneralName;
    use x509_parser::prelude::FromDer;

    let der = decode_pem(&std::fs::read_to_string(path).expect("pem"));
    let (_, certificate) =
        x509_parser::certificate::X509Certificate::from_der(&der).expect("a certificate");
    let names = certificate
        .subject_alternative_name()
        .expect("well formed")
        .expect("present");
    names
        .value
        .general_names
        .iter()
        .map(|name| match name {
            GeneralName::DNSName(dns) => format!("DNS:{dns}"),
            GeneralName::URI(uri) => format!("URI:{uri}"),
            GeneralName::IPAddress(bytes) => match bytes.len() {
                4 => format!(
                    "IP:{}",
                    std::net::Ipv4Addr::from(<[u8; 4]>::try_from(*bytes).expect("four"))
                ),
                16 => format!(
                    "IP:{}",
                    std::net::Ipv6Addr::from(<[u8; 16]>::try_from(*bytes).expect("sixteen"))
                ),
                _ => "IP:?".into(),
            },
            other => format!("{other:?}"),
        })
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn parse_id(text: &str) -> Option<[u8; 16]> {
    let mut out = [0u8; 16];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(text.get(i * 2..i * 2 + 2)?, 16).ok()?;
    }
    Some(out)
}

fn b64url_decode(text: &str) -> Option<Vec<u8>> {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut bits = 0u32;
    let mut have = 0u32;
    let mut out = Vec::new();
    for byte in text.trim().bytes() {
        let value = A.iter().position(|c| *c == byte)? as u32;
        bits = (bits << 6) | value;
        have += 6;
        if have >= 8 {
            have -= 8;
            out.push((bits >> have) as u8);
        }
    }
    Some(out)
}

fn decode_pem(pem: &str) -> Vec<u8> {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let body: String = pem
        .lines()
        .filter(|line| !line.starts_with("-----"))
        .collect();
    let mut bits = 0u32;
    let mut have = 0u32;
    let mut out = Vec::new();
    for byte in body.bytes() {
        if byte == b'=' {
            break;
        }
        let Some(value) = A.iter().position(|c| *c == byte) else {
            continue;
        };
        bits = (bits << 6) | value as u32;
        have += 6;
        if have >= 8 {
            have -= 8;
            out.push((bits >> have) as u8);
        }
    }
    out
}

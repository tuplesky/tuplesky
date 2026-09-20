//! What a provisioned domain has to be true of before anything is
//! started against it.
//!
//! These are cheap checks of the fixture, and they are here because a
//! harness bug looks exactly like a result. A domain whose genesis does
//! not commit the key a node will present, or whose catalog nobody
//! signed, would make every daemon refuse to start for a reason that has
//! nothing to do with what the run is measuring.

use coord_harness::domain::{Plan, Provisioned, provision};

fn provisioned(voters: u8) -> (tempfile::TempDir, Provisioned) {
    let dir = tempfile::tempdir().expect("a run directory");
    let out = provision(&Plan {
        directory: dir.path().to_path_buf(),
        voters,
        edge_port: 0,
    })
    .expect("provisioned");
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

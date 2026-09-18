//! Frozen postcard vectors of the Kine-facing `Response` subset (task-46).
//!
//! The Go adapter decodes the exact bytes the leader releases; these vectors
//! are the Rust encoding it is verified against. Set
//! `COORD_STATE_WRITE_FIXTURES=1` to regenerate (reviewed schema change
//! only). Without it the committed fixture is authoritative.

use std::path::PathBuf;

use coord_state::{KineKv, KvEntry, Outcome, RangeItem, Response};
use coord_types::ids::{KvRevision, LeaseGeneration, LeaseId};
use serde::{Deserialize, Serialize};

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join(name)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn write_or_compare<T: Serialize + for<'de> Deserialize<'de> + PartialEq + std::fmt::Debug>(
    name: &str,
    value: &T,
) {
    let path = fixture_path(name);
    if std::env::var_os("COORD_STATE_WRITE_FIXTURES").is_some() {
        let mut json = serde_json::to_string_pretty(value).unwrap();
        json.push('\n');
        std::fs::write(&path, json).unwrap();
        return;
    }
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("missing fixture {}: {e}", path.display()));
    let stored: T = serde_json::from_str(&text).unwrap();
    assert_eq!(
        &stored, value,
        "fixture {name} drifted; this is a frozen encoding"
    );
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct ResponseVector {
    name: String,
    response: Response,
    payload_hex: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct ResponseFixture {
    schema: String,
    vectors: Vec<ResponseVector>,
}

fn rev(n: u64) -> KvRevision {
    KvRevision::new(n).unwrap()
}

fn entry(value: &[u8], create: u64, modr: u64, version: u64, lease: Option<LeaseId>) -> KvEntry {
    KvEntry {
        value: value.to_vec(),
        create_revision: rev(create),
        mod_revision: rev(modr),
        version,
        lease,
        lease_generation: lease.map(|_| LeaseGeneration::new(1).unwrap()),
    }
}

fn vector(name: &str, revision: u64, outcome: Outcome) -> ResponseVector {
    let response = Response {
        revision: rev(revision),
        outcome,
    };
    let payload = postcard::to_allocvec(&response).unwrap();
    // The encoding round-trips exactly.
    let (decoded, rest): (Response, &[u8]) = postcard::take_from_bytes(&payload).unwrap();
    assert_eq!(decoded, response);
    assert!(rest.is_empty());
    ResponseVector {
        name: name.to_owned(),
        response,
        payload_hex: hex(&payload),
    }
}

/// The Kine-facing outcomes and their exact postcard bytes are frozen.
#[test]
fn kine_response_vectors_are_frozen() {
    const B1: LeaseId = LeaseId(*b"kine-bind-000001");
    let a = RangeItem {
        key: b"/registry/a".to_vec(),
        entry: entry(b"va", 3, 7, 2, None),
    };
    let b = RangeItem {
        key: b"/registry/b".to_vec(),
        entry: entry(b"vb", 11, 11, 1, Some(B1)),
    };
    let bound = KineKv {
        key: b"/registry/b".to_vec(),
        entry: entry(b"vb", 11, 11, 1, Some(B1)),
        ttl_seconds: 30,
    };
    let unbound = KineKv {
        key: b"/registry/a".to_vec(),
        entry: entry(b"va", 3, 7, 2, None),
        ttl_seconds: 0,
    };
    let vectors = vec![
        vector(
            "range-two-items-more",
            12,
            Outcome::Range {
                items: vec![a.clone(), b.clone()],
                count: 5,
                more: true,
            },
        ),
        vector(
            "range-count-only",
            9,
            Outcome::Range {
                items: vec![],
                count: 3,
                more: false,
            },
        ),
        vector(
            "range-empty",
            9,
            Outcome::Range {
                items: vec![],
                count: 0,
                more: false,
            },
        ),
        vector(
            "range-keys-only",
            9,
            Outcome::Range {
                items: vec![RangeItem {
                    key: b"/registry/a".to_vec(),
                    entry: entry(b"", 3, 7, 2, None),
                }],
                count: 1,
                more: false,
            },
        ),
        vector("kine-created", 5, Outcome::KineCreated),
        vector("err-key-exists", 5, Outcome::ErrKeyExists),
        vector(
            "kine-updated-applied",
            11,
            Outcome::KineUpdated {
                updated: true,
                current: Some(bound.clone()),
            },
        ),
        vector(
            "kine-updated-mismatch",
            7,
            Outcome::KineUpdated {
                updated: false,
                current: Some(unbound.clone()),
            },
        ),
        vector(
            "kine-updated-absent",
            7,
            Outcome::KineUpdated {
                updated: false,
                current: None,
            },
        ),
        vector(
            "kine-deleted-absent",
            7,
            Outcome::KineDeleted {
                deleted: true,
                prev: None,
            },
        ),
        vector(
            "kine-deleted-mismatch",
            7,
            Outcome::KineDeleted {
                deleted: false,
                prev: Some(bound.clone()),
            },
        ),
        vector(
            "kine-deleted-applied",
            8,
            Outcome::KineDeleted {
                deleted: true,
                prev: Some(unbound.clone()),
            },
        ),
        vector("err-compacted", 20, Outcome::ErrCompacted),
        vector("err-future-revision", 20, Outcome::ErrFutureRevision),
        vector("err-permission-denied", 20, Outcome::ErrPermissionDenied),
        vector("err-session-invalid", 20, Outcome::ErrSessionInvalid),
        vector("err-lease-exists", 20, Outcome::ErrLeaseExists),
    ];
    write_or_compare(
        "kine_responses_v1.json",
        &ResponseFixture {
            schema: "kine_responses_v1".to_owned(),
            vectors,
        },
    );
}

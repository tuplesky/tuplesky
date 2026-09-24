//! Frozen vectors for ordered keys and command identities.
//!
//! Set `COORD_TYPES_WRITE_FIXTURES=1` to regenerate (reviewed schema change
//! only). Without it the committed fixtures are authoritative.

use std::path::PathBuf;

use coord_types::ids::*;
use coord_types::logical_v1::*;
use coord_types::ordered_key::{encode_current, encode_history};
use coord_types::{CommandId, RetryKey};
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
    if std::env::var_os("COORD_TYPES_WRITE_FIXTURES").is_some() {
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
struct OrderedKeyVector {
    namespace: String,
    key: String,
    revision: Option<u64>,
    encoded: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct OrderedKeyFixture {
    schema: String,
    vectors: Vec<OrderedKeyVector>,
    /// Indices of `vectors` in ascending encoded order.
    ascending: Vec<usize>,
}

#[test]
fn ordered_key_vectors_are_frozen() {
    let ns_a = NamespaceId(*b"namespace-aaaaaa");
    let ns_b = NamespaceId(*b"namespace-bbbbbb");
    let inputs: Vec<(NamespaceId, Vec<u8>, Option<u64>)> = vec![
        (ns_a, vec![], None),
        (ns_a, vec![0], None),
        (ns_a, vec![0, 0], None),
        (ns_a, vec![0, 0xff], None),
        (ns_a, vec![0xff], None),
        (ns_a, b"a".to_vec(), None),
        (ns_a, b"a\0".to_vec(), None),
        (ns_a, b"ab".to_vec(), None),
        (ns_a, b"a".to_vec(), Some(0)),
        (ns_a, b"a".to_vec(), Some(1)),
        (ns_a, b"a".to_vec(), Some(256)),
        (ns_a, b"a".to_vec(), Some(i64::MAX as u64)),
        (ns_a, b"/registry/pods/default/x".to_vec(), Some(42)),
        (ns_b, vec![], None),
        (ns_b, b"a".to_vec(), Some(7)),
    ];
    let vectors: Vec<OrderedKeyVector> = inputs
        .iter()
        .map(|(ns, key, rev)| {
            let encoded = match rev {
                None => encode_current(ns, key),
                Some(r) => encode_history(ns, key, KvRevision::new(*r).unwrap()),
            };
            OrderedKeyVector {
                namespace: hex(ns.as_bytes()),
                key: hex(key),
                revision: *rev,
                encoded: hex(&encoded),
            }
        })
        .collect();
    let mut ascending: Vec<usize> = (0..vectors.len()).collect();
    ascending.sort_by(|&i, &j| vectors[i].encoded.cmp(&vectors[j].encoded));
    write_or_compare(
        "ordered_keys_v1.json",
        &OrderedKeyFixture {
            schema: "ordered_keys_v1".to_owned(),
            vectors,
            ascending,
        },
    );
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct CommandIdVector {
    name: String,
    cluster_id: String,
    domain_id: String,
    session_id: String,
    client_instance_id: String,
    request_sequence: u64,
    request: LogicalRequest,
    payload_hex: String,
    command_id: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct CommandIdFixture {
    schema: String,
    hash_context: String,
    vectors: Vec<CommandIdVector>,
}

fn vector(name: &str, key: RetryKey, request: LogicalRequest) -> CommandIdVector {
    let payload = request.canonical_bytes().unwrap();
    let id = CommandId::derive(&key, &request).unwrap();
    CommandIdVector {
        name: name.to_owned(),
        cluster_id: hex(key.cluster_id.as_bytes()),
        domain_id: hex(key.domain_id.as_bytes()),
        session_id: hex(key.session_id.as_bytes()),
        client_instance_id: hex(key.client_instance_id.as_bytes()),
        request_sequence: key.request_sequence.get(),
        request,
        payload_hex: hex(&payload),
        command_id: hex(id.as_bytes()),
    }
}

#[test]
fn command_id_vectors_are_frozen() {
    let key = RetryKey {
        cluster_id: ClusterId(*b"cluster-00000001"),
        domain_id: DomainId(*b"domain-000000001"),
        session_id: SessionId(*b"session-00000001"),
        client_instance_id: ClientInstanceId(*b"client-000000001"),
        request_sequence: RequestSequence::new(1).unwrap(),
    };
    let ns = NamespaceId(*b"tenant-000000001");
    let lease = LeaseId(*b"lease-0000000001");
    let put = LogicalRequest::new(
        ns,
        CanonicalOperation::Put(PutOp {
            key: b"/k".to_vec(),
            value: b"v".to_vec(),
            lease: None,
            prev_kv: false,
        }),
    );
    let put_other_value = LogicalRequest::new(
        ns,
        CanonicalOperation::Put(PutOp {
            key: b"/k".to_vec(),
            value: b"w".to_vec(),
            lease: None,
            prev_kv: false,
        }),
    );
    let range = LogicalRequest::new(
        ns,
        CanonicalOperation::Range(RangeOp {
            range: KeyRange::interval(b"/a".to_vec(), b"/b".to_vec()),
            revision: Some(KvRevision::new(10).unwrap()),
            limit: 50,
            keys_only: true,
            count_only: false,
        }),
    );
    let delete = LogicalRequest::new(
        ns,
        CanonicalOperation::DeleteRange(DeleteRangeOp {
            range: KeyRange::exact(b"/k".to_vec()),
            prev_kv: true,
        }),
    );
    let mut txn = LogicalRequest::new(
        ns,
        CanonicalOperation::Txn(TxnOp {
            compares: vec![
                Compare {
                    key: b"/k".to_vec(),
                    target: CompareTarget::ModRevision,
                    result: CompareResult::Equal,
                    operand: CompareOperand::Counter(3),
                },
                Compare {
                    key: b"/j".to_vec(),
                    target: CompareTarget::Value,
                    result: CompareResult::NotEqual,
                    operand: CompareOperand::Bytes(vec![0, 1]),
                },
                Compare {
                    key: b"/j".to_vec(),
                    target: CompareTarget::Lease,
                    result: CompareResult::Equal,
                    operand: CompareOperand::Lease(Some(lease)),
                },
            ],
            success: vec![BranchOp::Put(PutOp {
                key: b"/k".to_vec(),
                value: b"new".to_vec(),
                lease: Some(lease),
                prev_kv: true,
            })],
            failure: vec![BranchOp::Range(RangeOp {
                range: KeyRange::exact(b"/k".to_vec()),
                revision: None,
                limit: 0,
                keys_only: false,
                count_only: false,
            })],
        }),
    );
    txn.canonicalize();
    let grant = LogicalRequest::new(
        ns,
        CanonicalOperation::LeaseGrant {
            lease_id: lease,
            ttl_seconds: 30,
        },
    );
    let keepalive = LogicalRequest::new(ns, CanonicalOperation::LeaseKeepAlive { lease_id: lease });
    let revoke = LogicalRequest::new(ns, CanonicalOperation::LeaseRevoke { lease_id: lease });
    let ttl = LogicalRequest::new(
        ns,
        CanonicalOperation::LeaseTimeToLive {
            lease_id: lease,
            keys: true,
        },
    );
    let compact = LogicalRequest::new(
        ns,
        CanonicalOperation::Compact {
            revision: KvRevision::new(1000).unwrap(),
        },
    );
    let binding = LeaseId(*b"kine-bind-000001");
    let kine_create = LogicalRequest::new(
        ns,
        CanonicalOperation::KineCreate(KineCreateOp {
            key: b"/registry/pods/p".to_vec(),
            value: b"pod".to_vec(),
            ttl_seconds: 60,
            binding: Some(binding),
        }),
    );
    let kine_update = LogicalRequest::new(
        ns,
        CanonicalOperation::KineUpdate(KineUpdateOp {
            key: b"/registry/pods/p".to_vec(),
            value: b"pod2".to_vec(),
            expected_mod_revision: KvRevision::new(7).unwrap(),
            ttl_seconds: 0,
            binding: None,
        }),
    );
    let kine_delete = LogicalRequest::new(
        ns,
        CanonicalOperation::KineDelete(KineDeleteOp {
            key: b"/registry/pods/p".to_vec(),
            expected_mod_revision: Some(KvRevision::new(8).unwrap()),
        }),
    );
    let mut key2 = key;
    key2.request_sequence = RequestSequence::new(2).unwrap();

    let vectors = vec![
        vector("put", key, put.clone()),
        vector("put-retry-same-identity", key, put.clone()),
        vector("put-different-payload-same-retry-key", key, put_other_value),
        vector("put-next-sequence", key2, put),
        vector("range-historical-keys-only", key, range),
        vector("delete-prev-kv", key, delete),
        vector("txn-canonical", key, txn),
        vector("lease-grant", key, grant),
        vector("kine-create-ttl", key, kine_create),
        vector("kine-update-cas-no-ttl", key, kine_update),
        vector("kine-delete-conditional", key, kine_delete),
        vector("lease-keepalive", key, keepalive),
        vector("lease-revoke", key, revoke),
        vector("lease-ttl", key, ttl),
        vector("compact", key, compact),
    ];
    assert_eq!(vectors[0].command_id, vectors[1].command_id);
    assert_ne!(vectors[0].command_id, vectors[2].command_id);
    assert_ne!(vectors[0].command_id, vectors[3].command_id);
    write_or_compare(
        "command_ids_v1.json",
        &CommandIdFixture {
            schema: "command_ids_v1".to_owned(),
            hash_context: coord_types::HashDomain::CommandId.context().to_owned(),
            vectors,
        },
    );
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct KineBindingVector {
    name: String,
    cluster_id: String,
    domain_id: String,
    session_id: String,
    client_instance_id: String,
    request_sequence: u64,
    binding_id: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct KineBindingFixture {
    schema: String,
    hash_context: String,
    vectors: Vec<KineBindingVector>,
}

/// The hidden Kine binding identity is a function of the retry key alone
/// (task-46): a retry reproduces it, the next sequence names a fresh one.
#[test]
fn kine_binding_vectors_are_frozen() {
    let key = RetryKey {
        cluster_id: ClusterId(*b"cluster-00000001"),
        domain_id: DomainId(*b"domain-000000001"),
        session_id: SessionId(*b"session-00000001"),
        client_instance_id: ClientInstanceId(*b"client-000000001"),
        request_sequence: RequestSequence::new(1).unwrap(),
    };
    let next = RetryKey {
        request_sequence: RequestSequence::new(2).unwrap(),
        ..key
    };
    let other_instance = RetryKey {
        client_instance_id: ClientInstanceId(*b"client-000000002"),
        ..key
    };
    let vector = |name: &str, key: RetryKey| KineBindingVector {
        name: name.to_owned(),
        cluster_id: hex(key.cluster_id.as_bytes()),
        domain_id: hex(key.domain_id.as_bytes()),
        session_id: hex(key.session_id.as_bytes()),
        client_instance_id: hex(key.client_instance_id.as_bytes()),
        request_sequence: key.request_sequence.get(),
        binding_id: hex(coord_types::kine_binding_id(&key).as_bytes()),
    };
    let vectors = vec![
        vector("sequence-1", key),
        vector("sequence-1-retry", key),
        vector("sequence-2", next),
        vector("other-instance", other_instance),
    ];
    assert_eq!(vectors[0].binding_id, vectors[1].binding_id);
    assert_ne!(vectors[0].binding_id, vectors[2].binding_id);
    assert_ne!(vectors[0].binding_id, vectors[3].binding_id);
    write_or_compare(
        "kine_bindings_v1.json",
        &KineBindingFixture {
            schema: "kine_bindings_v1".to_owned(),
            hash_context: coord_types::HashDomain::KineBinding.context().to_owned(),
            vectors,
        },
    );
}

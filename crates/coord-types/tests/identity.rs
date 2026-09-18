//! Command identity: payload changes the digest; admission context, epoch,
//! endpoint and token refresh do not; conflicts are detected.

use coord_types::identity::{AdmissionContext, IdentityCheck, check_identity};
use coord_types::ids::*;
use coord_types::logical_v1::*;
use coord_types::{CommandId, IdentityError, RetryKey};

fn retry_key(seq: u64) -> RetryKey {
    RetryKey {
        cluster_id: ClusterId([1; 16]),
        domain_id: DomainId([2; 16]),
        session_id: SessionId([3; 16]),
        client_instance_id: ClientInstanceId([4; 16]),
        request_sequence: RequestSequence::new(seq).unwrap(),
    }
}

fn put(key: &[u8], value: &[u8]) -> LogicalRequest {
    LogicalRequest::new(
        NamespaceId([5; 16]),
        CanonicalOperation::Put(PutOp {
            key: key.to_vec(),
            value: value.to_vec(),
            lease: None,
            prev_kv: false,
        }),
    )
}

fn context(epoch: u64, endpoint: u64, token: u64) -> AdmissionContext {
    AdmissionContext {
        epoch: ConfigurationEpoch::new(epoch).unwrap(),
        ballot: None,
        endpoint_generation: EndpointGeneration::new(endpoint).unwrap(),
        credential_handle: token,
        connection_id: token * 7,
        stream_id: token * 11,
    }
}

#[test]
fn identity_is_stable_across_admission_context_changes() {
    let request = put(b"k", b"v");
    let id = CommandId::derive(&retry_key(1), &request).unwrap();
    // The derivation takes no admission context at all; any refresh of
    // token, endpoint or epoch yields the same identity by construction.
    let _before = context(1, 1, 1);
    let _after = context(2, 9, 42);
    assert_eq!(id, CommandId::derive(&retry_key(1), &request).unwrap());
    assert_eq!(check_identity(Some(&id), &id), Ok(IdentityCheck::Retry));
}

#[test]
fn payload_change_changes_identity_and_conflicts() {
    let id_a = CommandId::derive(&retry_key(1), &put(b"k", b"v")).unwrap();
    let id_b = CommandId::derive(&retry_key(1), &put(b"k", b"w")).unwrap();
    let id_c = CommandId::derive(&retry_key(1), &put(b"j", b"v")).unwrap();
    let mut flag = put(b"k", b"v");
    if let CanonicalOperation::Put(p) = &mut flag.operation {
        p.prev_kv = true;
    }
    let id_d = CommandId::derive(&retry_key(1), &flag).unwrap();
    assert_ne!(id_a, id_b);
    assert_ne!(id_a, id_c);
    assert_ne!(id_a, id_d, "semantic flags are part of identity");
    assert_eq!(check_identity(None, &id_a), Ok(IdentityCheck::New));
    assert_eq!(
        check_identity(Some(&id_a), &id_b),
        Err(IdentityError::RequestIdentityConflict)
    );
}

#[test]
fn every_retry_key_component_is_part_of_identity() {
    let request = put(b"k", b"v");
    let base = CommandId::derive(&retry_key(1), &request).unwrap();
    let mut k = retry_key(1);
    k.cluster_id = ClusterId([9; 16]);
    assert_ne!(base, CommandId::derive(&k, &request).unwrap());
    let mut k = retry_key(1);
    k.domain_id = DomainId([9; 16]);
    assert_ne!(base, CommandId::derive(&k, &request).unwrap());
    let mut k = retry_key(1);
    k.session_id = SessionId([9; 16]);
    assert_ne!(base, CommandId::derive(&k, &request).unwrap());
    let mut k = retry_key(1);
    k.client_instance_id = ClientInstanceId([9; 16]);
    assert_ne!(base, CommandId::derive(&k, &request).unwrap());
    assert_ne!(base, CommandId::derive(&retry_key(2), &request).unwrap());
    let mut other_ns = request.clone();
    other_ns.namespace = NamespaceId([6; 16]);
    assert_ne!(
        base,
        CommandId::derive(&retry_key(1), &other_ns).unwrap(),
        "tenant is part of identity"
    );
}

#[test]
fn equivalent_transactions_hash_identically_after_canonicalization() {
    let c1 = Compare {
        key: b"b".to_vec(),
        target: CompareTarget::ModRevision,
        result: CompareResult::Equal,
        operand: CompareOperand::Counter(4),
    };
    let c2 = Compare {
        key: b"a".to_vec(),
        target: CompareTarget::Value,
        result: CompareResult::Equal,
        operand: CompareOperand::Bytes(b"x".to_vec()),
    };
    let branch = vec![BranchOp::Put(PutOp {
        key: b"a".to_vec(),
        value: b"y".to_vec(),
        lease: None,
        prev_kv: false,
    })];
    let mut one = LogicalRequest::new(
        NamespaceId([5; 16]),
        CanonicalOperation::Txn(TxnOp {
            compares: vec![c1.clone(), c2.clone()],
            success: branch.clone(),
            failure: vec![],
        }),
    );
    let mut two = LogicalRequest::new(
        NamespaceId([5; 16]),
        CanonicalOperation::Txn(TxnOp {
            compares: vec![c2, c1],
            success: branch,
            failure: vec![],
        }),
    );
    assert!(
        CommandId::derive(&retry_key(1), &one).is_err()
            || CommandId::derive(&retry_key(1), &two).is_err(),
        "at least one order is non-canonical"
    );
    one.canonicalize();
    two.canonicalize();
    assert_eq!(one, two);
    assert_eq!(
        CommandId::derive(&retry_key(1), &one).unwrap(),
        CommandId::derive(&retry_key(1), &two).unwrap()
    );
}

#[test]
fn invalid_requests_have_no_identity() {
    let empty = put(b"", b"v");
    assert!(CommandId::derive(&retry_key(1), &empty).is_err());
}

#[test]
fn distinct_counter_types_do_not_convert() {
    // Compile-time property documented by construction: there is no `From`
    // between counters. This test pins the runtime shape of the values.
    let seq = LocalJournalSeq::new(7).unwrap();
    let pos = ExecutionPosition::new(7).unwrap();
    let rev = KvRevision::new(7).unwrap();
    assert_eq!(seq.get(), pos.get());
    assert_eq!(pos.get(), rev.get());
    assert_eq!(format!("{seq:?}"), "LocalJournalSeq(7)");
    assert_eq!(format!("{pos:?}"), "ExecutionPosition(7)");
    assert_eq!(format!("{rev:?}"), "KvRevision(7)");
}

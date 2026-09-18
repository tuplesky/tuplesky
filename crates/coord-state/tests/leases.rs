//! task-15 acceptance at the planner: grant/attach/detach/revoke plans,
//! stable non-reused identities, ownership, quotas rechecked on value
//! growth, and revocation deleting exactly the current attachments.

use std::collections::BTreeMap;

use coord_core::effect::ApplyBase;
use coord_state::planner::{apply_leases, apply_to_map};
use coord_state::{
    ApplyPlan, KvEntry, KvEventKind, LeasePurpose, LeaseRecord, LeaseStatus, Mutation, Outcome,
    PlanError, PlanLimits, ReadView, attachment_cost, plan,
};
use coord_types::ids::*;
use coord_types::logical_v1::*;

const NS: NamespaceId = NamespaceId([7; 16]);
const OTHER_NS: NamespaceId = NamespaceId([8; 16]);
const ALICE: PrincipalId = PrincipalId([0xa; 16]);
const BOB: PrincipalId = PrincipalId([0xb; 16]);
const L1: LeaseId = LeaseId([1; 16]);
const L2: LeaseId = LeaseId([2; 16]);

fn rev(n: u64) -> KvRevision {
    KvRevision::new(n).unwrap()
}

fn req(op: CanonicalOperation) -> LogicalRequest {
    let mut r = LogicalRequest::new(NS, op);
    r.canonicalize();
    r
}

fn put(key: &[u8], value: &[u8], lease: Option<LeaseId>) -> LogicalRequest {
    req(CanonicalOperation::Put(PutOp {
        key: key.to_vec(),
        value: value.to_vec(),
        lease,
        prev_kv: false,
    }))
}

fn grant(id: LeaseId, ttl: u32) -> LogicalRequest {
    req(CanonicalOperation::LeaseGrant {
        lease_id: id,
        ttl_seconds: ttl,
    })
}

fn revoke(id: LeaseId) -> LogicalRequest {
    req(CanonicalOperation::LeaseRevoke { lease_id: id })
}

fn ttl(id: LeaseId, keys: bool) -> LogicalRequest {
    req(CanonicalOperation::LeaseTimeToLive { lease_id: id, keys })
}

/// In-memory domain: current entries, lease records and the reverse index
/// derived from the entries (what common storage builds for the planner).
struct Fixture {
    current: BTreeMap<Vec<u8>, KvEntry>,
    leases: BTreeMap<LeaseId, LeaseRecord>,
    revision: u64,
    position: u64,
    limits: PlanLimits,
}

impl Fixture {
    fn new() -> Self {
        Fixture {
            current: BTreeMap::new(),
            leases: BTreeMap::new(),
            revision: 0,
            position: 0,
            limits: PlanLimits::default(),
        }
    }

    fn view(&self, principal: PrincipalId) -> ReadView {
        let base = ApplyBase {
            configuration: ConfigurationEpoch::ZERO,
            execution_position: ExecutionPosition::new(self.position).unwrap(),
        };
        let mut v = ReadView::empty(base, NS, principal, rev(self.revision));
        v.current = self.current.clone();
        v.leases = self.leases.clone();
        for (k, e) in &self.current {
            if let Some(l) = e.lease {
                v.lease_keys.entry(l).or_default().insert(k.clone());
            }
        }
        for l in self.leases.keys() {
            v.lease_keys.entry(*l).or_default();
        }
        v
    }

    fn run_as(&mut self, principal: PrincipalId, r: &LogicalRequest) -> ApplyPlan {
        let v = self.view(principal);
        let p = plan(r, &v, &self.limits).unwrap();
        assert_eq!(p.position.get(), self.position + 1);
        apply_to_map(&mut self.current, &p);
        apply_leases(&mut self.leases, &p);
        self.position += 1;
        if let Some(r) = p.revision {
            assert_eq!(r.get(), self.revision + 1);
            self.revision = r.get();
        }
        p
    }

    fn run(&mut self, r: &LogicalRequest) -> ApplyPlan {
        self.run_as(ALICE, r)
    }

    fn record(&self, id: LeaseId) -> &LeaseRecord {
        &self.leases[&id]
    }
}

#[test]
fn grant_is_stable_never_reused_and_needs_no_revision() {
    let mut f = Fixture::new();
    let p = f.run(&grant(L1, 30));
    assert_eq!(
        p.response.outcome,
        Outcome::LeaseGranted {
            lease_id: L1,
            generation: LeaseGeneration::new(1).unwrap(),
            ttl_seconds: 30
        }
    );
    assert_eq!(
        p.revision, None,
        "lease administration takes no KV revision"
    );
    assert_eq!(p.response.revision, rev(0));
    assert!(p.events.is_empty());
    assert_eq!(
        p.mutations,
        vec![Mutation::LeaseWrite {
            lease: L1,
            record: LeaseRecord {
                namespace: NS,
                generation: LeaseGeneration::new(1).unwrap(),
                owner: ALICE,
                ttl_seconds: 30,
                renewal_sequence: 0,
                purpose: LeasePurpose::Native,
                status: LeaseStatus::Active,
                attached_keys: 0,
                attached_bytes: 0,
            }
        }]
    );
    // A second grant of the same identity (a different request colliding
    // on the derived id, or a lost retry not caught by dedup) never grants
    // twice; it is a recorded failure without mutations.
    let again = f.run(&grant(L1, 60));
    assert_eq!(again.response.outcome, Outcome::ErrLeaseExists);
    assert!(again.mutations.is_empty());
    assert_eq!(f.record(L1).ttl_seconds, 30);
    // After a revoke the tombstone still refuses reuse of the identity.
    assert_eq!(
        f.run(&revoke(L1)).response.outcome,
        Outcome::LeaseRevoked { deleted: 0 }
    );
    assert_eq!(f.record(L1).status, LeaseStatus::Revoked);
    assert_eq!(
        f.run(&grant(L1, 5)).response.outcome,
        Outcome::ErrLeaseExists
    );
    assert_eq!(
        f.run(&ttl(L1, false)).response.outcome,
        Outcome::ErrLeaseNotFound
    );
    assert_eq!(
        f.run(&revoke(L1)).response.outcome,
        Outcome::ErrLeaseNotFound
    );
}

#[test]
fn revoke_deletes_exactly_the_current_attachments_in_one_revision() {
    let mut f = Fixture::new();
    f.run(&grant(L1, 30));
    f.run(&grant(L2, 30));
    f.run(&put(b"a", b"1", Some(L1))); // rev 1
    f.run(&put(b"b", b"2", Some(L1))); // rev 2
    f.run(&put(b"c", b"3", Some(L2))); // rev 3
    f.run(&put(b"d", b"4", None)); // rev 4
    // Moving b to L2 detaches it from L1; overwriting a without a lease detaches too.
    f.run(&put(b"b", b"22", Some(L2))); // rev 5
    f.run(&put(b"e", b"5", Some(L1))); // rev 6
    assert_eq!(f.record(L1).attached_keys, 2);
    assert_eq!(f.record(L2).attached_keys, 2);
    assert_eq!(
        f.record(L1).attached_bytes,
        attachment_cost(b"a", b"1") + attachment_cost(b"e", b"5")
    );
    let t = f.run(&ttl(L1, true));
    assert_eq!(
        t.response.outcome,
        Outcome::LeaseTimeToLive {
            lease_id: L1,
            generation: LeaseGeneration::new(1).unwrap(),
            granted_ttl_seconds: 30,
            renewal_sequence: 0,
            keys: Some(vec![b"a".to_vec(), b"e".to_vec()]),
        }
    );
    assert_eq!(t.revision, None);

    let p = f.run(&revoke(L1));
    assert_eq!(p.response.outcome, Outcome::LeaseRevoked { deleted: 2 });
    assert_eq!(p.revision, Some(rev(7)), "one revision for the whole set");
    assert_eq!(p.events.len(), 2);
    assert!(p.events.iter().all(|e| e.kind == KvEventKind::Delete));
    assert_eq!(
        p.events.iter().map(|e| e.key.clone()).collect::<Vec<_>>(),
        vec![b"a".to_vec(), b"e".to_vec()]
    );
    assert!(p.events.iter().all(|e| e.prev.is_some()));
    assert!(!f.current.contains_key(b"a".as_slice()));
    assert!(!f.current.contains_key(b"e".as_slice()));
    assert!(f.current.contains_key(b"b".as_slice()), "b moved to L2");
    assert!(f.current.contains_key(b"c".as_slice()));
    assert!(f.current.contains_key(b"d".as_slice()));
    assert_eq!(f.record(L1).status, LeaseStatus::Revoked);
    assert_eq!(f.record(L1).attached_keys, 0);
    assert_eq!(f.record(L1).attached_bytes, 0);
    assert_eq!(f.record(L2).attached_keys, 2);
    // A revoke of L2 after b's overwrite deletes b's *current* value.
    let p = f.run(&revoke(L2));
    assert_eq!(p.response.outcome, Outcome::LeaseRevoked { deleted: 2 });
    assert_eq!(p.revision, Some(rev(8)));
    assert_eq!(
        p.events[0].prev.as_ref().unwrap().value,
        b"22".to_vec(),
        "the current attachment, not the value attached first"
    );
    assert_eq!(f.current.len(), 1);
}

#[test]
fn value_growth_cannot_evade_the_deletion_budget() {
    let mut f = Fixture::new();
    f.limits.max_lease_bytes = 2 * attachment_cost(b"k", b"1234") + 1;
    f.limits.max_lease_attachments = 2;
    f.run(&grant(L1, 30));
    f.run(&put(b"k", b"1", Some(L1)));
    f.run(&put(b"m", b"1", Some(L1)));
    // A third attachment exceeds the count quota.
    let p = f.run(&put(b"n", b"1", Some(L1)));
    assert_eq!(p.response.outcome, Outcome::ErrLeaseQuota);
    assert_eq!(p.revision, None);
    assert!(p.mutations.is_empty());
    // Growing an attached value is rechecked against the byte quota.
    let p = f.run(&put(b"k", b"1234", Some(L1)));
    assert_eq!(p.response.outcome, Outcome::Put { prev: None });
    let p = f.run(&put(b"m", b"123456", Some(L1)));
    assert_eq!(
        p.response.outcome,
        Outcome::ErrLeaseQuota,
        "value growth of an already attached key is checked"
    );
    assert_eq!(f.current[b"m".as_slice()].value, b"1".to_vec());
    assert_eq!(
        f.record(L1).attached_bytes,
        attachment_cost(b"k", b"1234") + attachment_cost(b"m", b"1")
    );
    // Shrinking frees budget; a transaction attaching two keys is checked
    // cumulatively and fails as a whole.
    f.run(&put(b"k", b"1", Some(L1)));
    let txn = req(CanonicalOperation::Txn(TxnOp {
        compares: vec![],
        success: vec![
            BranchOp::DeleteRange(DeleteRangeOp {
                range: KeyRange::exact(b"m".to_vec()),
                prev_kv: false,
            }),
            BranchOp::Put(PutOp {
                key: b"x".to_vec(),
                value: b"1".to_vec(),
                lease: Some(L1),
                prev_kv: false,
            }),
            BranchOp::Put(PutOp {
                key: b"y".to_vec(),
                value: b"1".to_vec(),
                lease: Some(L1),
                prev_kv: false,
            }),
        ],
        failure: vec![],
    }));
    let p = f.run(&txn);
    assert_eq!(p.response.outcome, Outcome::ErrLeaseQuota);
    assert!(
        f.current.contains_key(b"m".as_slice()),
        "nothing of the txn applied"
    );
    // The revoke of a full lease fits the event budget by construction.
    f.limits.max_events_per_revision = 2;
    let p = f.run(&revoke(L1));
    assert_eq!(p.response.outcome, Outcome::LeaseRevoked { deleted: 2 });
}

#[test]
fn ownership_is_by_principal_and_attachment_cannot_imply_protected_deletion() {
    let mut f = Fixture::new();
    f.run_as(ALICE, &grant(L1, 30));
    // Bob may write his own key but cannot bind it to Alice's lease: that
    // would let Alice's revoke delete a key she has no permission on, and
    // let Bob evict Alice's key set from under her.
    let p = f.run_as(BOB, &put(b"bob", b"v", Some(L1)));
    assert_eq!(p.response.outcome, Outcome::ErrLeasePermission);
    assert!(p.mutations.is_empty());
    assert_eq!(f.record(L1).attached_keys, 0);
    // Bob cannot revoke or inspect Alice's lease either.
    assert_eq!(
        f.run_as(BOB, &revoke(L1)).response.outcome,
        Outcome::ErrLeasePermission
    );
    assert_eq!(
        f.run_as(BOB, &ttl(L1, true)).response.outcome,
        Outcome::ErrLeasePermission
    );
    // Ownership is the principal, not a session: Alice's key attached
    // through any session stays hers, and Alice's revoke deletes only it.
    f.run_as(ALICE, &put(b"alice", b"v", Some(L1)));
    f.run_as(BOB, &put(b"bob", b"v", None));
    let p = f.run_as(ALICE, &revoke(L1));
    assert_eq!(p.response.outcome, Outcome::LeaseRevoked { deleted: 1 });
    assert_eq!(p.events[0].key, b"alice".to_vec());
    assert!(f.current.contains_key(b"bob".as_slice()));
}

#[test]
fn kine_private_bindings_and_other_namespaces_are_invisible() {
    let mut f = Fixture::new();
    f.run(&grant(L1, 30));
    let mut hidden = f.record(L1).clone();
    hidden.purpose = LeasePurpose::KinePrivate;
    f.leases.insert(L2, hidden);
    for r in [put(b"k", b"v", Some(L2)), revoke(L2), ttl(L2, false)] {
        assert_eq!(f.run(&r).response.outcome, Outcome::ErrLeaseNotFound);
    }
    // The hidden identity is still not grantable natively.
    assert_eq!(
        f.run(&grant(L2, 1)).response.outcome,
        Outcome::ErrLeaseExists
    );
    // A lease of another namespace is not visible here.
    let mut foreign = f.record(L1).clone();
    foreign.namespace = OTHER_NS;
    f.leases.insert(LeaseId([3; 16]), foreign);
    assert_eq!(
        f.run(&put(b"k", b"v", Some(LeaseId([3; 16]))))
            .response
            .outcome,
        Outcome::ErrLeaseNotFound
    );
    // Renewal is task-16.
    let v = f.view(ALICE);
    assert_eq!(
        plan(
            &req(CanonicalOperation::LeaseKeepAlive { lease_id: L1 }),
            &v,
            &f.limits
        )
        .unwrap_err(),
        PlanError::Unsupported
    );
}

#[test]
fn inconsistent_or_incomplete_views_are_rejected_not_planned() {
    let mut f = Fixture::new();
    f.run(&grant(L1, 30));
    f.run(&put(b"a", b"1", Some(L1)));
    // Reverse index missing for a lease with attachments: incomplete.
    let mut v = f.view(ALICE);
    v.lease_keys.remove(&L1);
    assert_eq!(
        plan(&revoke(L1), &v, &f.limits).unwrap_err(),
        PlanError::ViewIncomplete
    );
    // Index disagreeing with the record: inconsistent.
    let mut v = f.view(ALICE);
    v.lease_keys.get_mut(&L1).unwrap().insert(b"zz".to_vec());
    assert_eq!(
        plan(&revoke(L1), &v, &f.limits).unwrap_err(),
        PlanError::ViewInconsistent
    );
    // An entry referencing a lease the view does not carry.
    let mut v = f.view(ALICE);
    v.leases.clear();
    assert_eq!(
        plan(
            &req(CanonicalOperation::DeleteRange(DeleteRangeOp {
                range: KeyRange::exact(b"a".to_vec()),
                prev_kv: false
            })),
            &v,
            &f.limits
        )
        .unwrap_err(),
        PlanError::ViewInconsistent
    );
}

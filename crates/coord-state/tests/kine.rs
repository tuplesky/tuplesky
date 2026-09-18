//! task-17 acceptance at the planner: Kine create/CAS-update/conditional
//! delete return every revision and conflict fact from one execution
//! point; a Kine TTL is a private per-key binding created atomically with
//! the write, replaced or removed by the next write of the key; a failed
//! compare changes neither data nor expiry; a replaced binding's
//! expiration is stale; bindings are invisible to native lease operations.

mod common;

use common::*;
use coord_state::{KineKv, LeasePurpose, LeaseStatus, Mutation, Outcome, PlanError, plan};
use coord_types::error::ValidationError;
use coord_types::ids::*;

const B1: LeaseId = LeaseId([0x11; 16]);
const B2: LeaseId = LeaseId([0x22; 16]);
const B3: LeaseId = LeaseId([0x33; 16]);

fn kv(f: &Fixture, key: &[u8], ttl: u32) -> KineKv {
    KineKv {
        key: key.to_vec(),
        entry: f.current[key].clone(),
        ttl_seconds: ttl,
    }
}

#[test]
fn create_is_atomic_with_its_binding_and_duplicates_are_exact() {
    let mut f = Fixture::new();
    let p = f.run(&kine_create(b"/k", b"v1", 60, Some(B1)));
    assert_eq!(p.response.outcome, Outcome::KineCreated);
    assert_eq!(
        p.revision,
        Some(rev(1)),
        "the creation revision is the header"
    );
    assert_eq!(p.response.revision, rev(1));
    assert_eq!(p.events.len(), 1);
    // The binding is created in the same plan: attach + record write.
    assert!(p.mutations.iter().any(|m| matches!(
        m,
        Mutation::LeaseAttach { lease, key, mod_revision, .. }
            if *lease == B1 && key == b"/k" && *mod_revision == rev(1)
    )));
    assert!(p.mutations.iter().any(|m| matches!(
        m,
        Mutation::LeaseWrite { lease, record }
            if *lease == B1 && record.purpose == LeasePurpose::KinePrivate
    )));
    let entry = &f.current[b"/k".as_slice()];
    assert_eq!(entry.lease, Some(B1));
    assert_eq!(
        entry.lease_generation,
        Some(LeaseGeneration::new(1).unwrap())
    );
    let b1 = f.record(B1);
    assert_eq!(b1.ttl_seconds, 60);
    assert_eq!(b1.owner, ALICE);
    assert_eq!(b1.attached_keys, 1);
    assert_eq!(b1.status, LeaseStatus::Active);
    // Exact duplicate-key result: nothing changes, no binding for the
    // losing request, no revision.
    let p = f.run(&kine_create(b"/k", b"other", 30, Some(B2)));
    assert_eq!(p.response.outcome, Outcome::ErrKeyExists);
    assert_eq!(p.response.revision, rev(1));
    assert!(p.mutations.is_empty() && p.revision.is_none());
    assert!(!f.leases.contains_key(&B2));
    assert_eq!(f.current[b"/k".as_slice()].value, b"v1".to_vec());
    // No TTL: no binding at all.
    let p = f.run(&kine_create(b"/j", b"x", 0, None));
    assert_eq!(p.response.outcome, Outcome::KineCreated);
    assert_eq!(f.current[b"/j".as_slice()].lease, None);
    assert_eq!(f.leases.len(), 1);
}

#[test]
fn failed_cas_changes_neither_data_nor_expiry() {
    let mut f = Fixture::new();
    f.run(&kine_create(b"/k", b"v1", 60, Some(B1)));
    // Wrong expected revision: the current entry and its Kine-facing TTL
    // come back from the same execution point; nothing is written.
    let p = f.run(&kine_update(b"/k", b"v2", 7, 30, Some(B2)));
    assert_eq!(
        p.response.outcome,
        Outcome::KineUpdated {
            updated: false,
            current: Some(kv(&f, b"/k", 60)),
        }
    );
    assert_eq!(p.response.revision, rev(1));
    assert!(p.mutations.is_empty() && p.revision.is_none() && p.events.is_empty());
    assert!(
        !f.leases.contains_key(&B2),
        "no binding for the failed update"
    );
    assert_eq!(f.record(B1).status, LeaseStatus::Active);
    assert_eq!(f.current[b"/k".as_slice()].lease, Some(B1));
    // Absent key: not updated, nothing current.
    let p = f.run(&kine_update(b"/none", b"v", 1, 0, None));
    assert_eq!(
        p.response.outcome,
        Outcome::KineUpdated {
            updated: false,
            current: None
        }
    );
    assert!(p.mutations.is_empty());
    // Conditional delete mismatch changes nothing either.
    let p = f.run(&kine_delete(b"/k", Some(9)));
    assert_eq!(
        p.response.outcome,
        Outcome::KineDeleted {
            deleted: false,
            prev: Some(kv(&f, b"/k", 60)),
        }
    );
    assert!(p.mutations.is_empty() && p.revision.is_none());
    assert_eq!(f.record(B1).attached_keys, 1);
}

#[test]
fn replaced_ttl_fences_the_prior_binding_and_zero_detaches() {
    let mut f = Fixture::new();
    f.run_internal(&establish(1));
    f.run(&kine_create(b"/k", b"v1", 60, Some(B1)));
    // Successful CAS with a new TTL: the old binding ends in the same plan.
    let p = f.run(&kine_update(b"/k", b"v2", 1, 30, Some(B2)));
    let Outcome::KineUpdated {
        updated: true,
        current: Some(current),
    } = &p.response.outcome
    else {
        panic!("{:?}", p.response.outcome)
    };
    assert_eq!(current.ttl_seconds, 30);
    assert_eq!(current.entry.mod_revision, rev(2));
    assert_eq!(current.entry.version, 2);
    assert_eq!(current.entry.lease, Some(B2));
    assert_eq!(p.revision, Some(rev(2)));
    assert_eq!(f.record(B1).status, LeaseStatus::Replaced);
    assert_eq!(f.record(B1).attached_keys, 0);
    assert_eq!(f.record(B2).status, LeaseStatus::Active);
    // The prior binding's timer is fenced: its expiration is stale and
    // deletes nothing.
    let p = f.run_internal(&expire(B1, 1, 0, 1));
    assert_eq!(p.response.outcome, Outcome::ExpireStale);
    assert!(f.current.contains_key(b"/k".as_slice()));
    // The live binding expires the key conditionally.
    let p = f.run_internal(&expire(B2, 1, 0, 1));
    assert_eq!(p.response.outcome, Outcome::LeaseExpired { deleted: 1 });
    assert!(!f.current.contains_key(b"/k".as_slice()));
    assert_eq!(f.record(B2).status, LeaseStatus::Expired);
    // TTL zero removes the compatibility binding.
    f.run(&kine_create(b"/m", b"1", 10, Some(B3)));
    let p = f.run(&kine_update(b"/m", b"2", 4, 0, None));
    assert!(matches!(
        p.response.outcome,
        Outcome::KineUpdated {
            updated: true,
            current: Some(KineKv { ttl_seconds: 0, .. })
        }
    ));
    assert_eq!(f.current[b"/m".as_slice()].lease, None);
    assert_eq!(f.record(B3).status, LeaseStatus::Replaced);
    assert_eq!(
        f.run_internal(&expire(B3, 1, 0, 1)).response.outcome,
        Outcome::ExpireStale
    );
    assert!(f.current.contains_key(b"/m".as_slice()));
    // Identities are never reused: a later request colliding on a spent
    // binding identity is refused without touching the key.
    let p = f.run(&kine_update(b"/m", b"3", 5, 10, Some(B3)));
    assert_eq!(p.response.outcome, Outcome::ErrLeaseExists);
    assert_eq!(f.current[b"/m".as_slice()].value, b"2".to_vec());
}

#[test]
fn delete_retains_zero_revision_absent_and_mismatch_distinctions() {
    let mut f = Fixture::new();
    // Absent key: reported gone, nothing seen, no revision.
    let p = f.run(&kine_delete(b"/k", Some(3)));
    assert_eq!(
        p.response.outcome,
        Outcome::KineDeleted {
            deleted: true,
            prev: None
        }
    );
    assert!(p.revision.is_none());
    f.run(&kine_create(b"/k", b"v1", 60, Some(B1)));
    let seen = kv(&f, b"/k", 60);
    // Mismatch: not deleted, the entry seen is returned.
    let p = f.run(&kine_delete(b"/k", Some(2)));
    assert_eq!(
        p.response.outcome,
        Outcome::KineDeleted {
            deleted: false,
            prev: Some(seen.clone())
        }
    );
    assert!(p.revision.is_none());
    // Zero revision (unconditional): deleted, binding ended.
    let p = f.run(&kine_delete(b"/k", None));
    assert_eq!(
        p.response.outcome,
        Outcome::KineDeleted {
            deleted: true,
            prev: Some(seen)
        }
    );
    assert_eq!(p.revision, Some(rev(2)));
    assert_eq!(p.events.len(), 1);
    assert!(!f.current.contains_key(b"/k".as_slice()));
    assert_eq!(f.record(B1).status, LeaseStatus::Replaced);
    // Conditional match.
    f.run(&kine_create(b"/k", b"v2", 0, None));
    let p = f.run(&kine_delete(b"/k", Some(3)));
    assert!(matches!(
        p.response.outcome,
        Outcome::KineDeleted {
            deleted: true,
            prev: Some(KineKv { ttl_seconds: 0, .. })
        }
    ));
    assert_eq!(p.revision, Some(rev(4)));
}

#[test]
fn private_bindings_are_invisible_to_native_operations_and_native_writes_end_them() {
    let mut f = Fixture::new();
    f.run(&kine_create(b"/k", b"v1", 60, Some(B1)));
    // Native lease operations cannot see, extend, revoke or attach to it,
    // and a native grant cannot take its identity.
    for r in [
        put(b"x", b"1", Some(B1)),
        keep_alive(B1),
        revoke(B1),
        ttl(B1, true),
    ] {
        let p = f.run(&r);
        assert_eq!(p.response.outcome, Outcome::ErrLeaseNotFound, "{r:?}");
        assert!(p.mutations.is_empty());
    }
    assert_eq!(
        f.run(&grant(B1, 5)).response.outcome,
        Outcome::ErrLeaseExists
    );
    // A native overwrite of a Kine-managed key ends the binding.
    f.run(&put(b"/k", b"native", None));
    assert_eq!(f.current[b"/k".as_slice()].lease, None);
    assert_eq!(f.record(B1).status, LeaseStatus::Replaced);
    // A Kine update of a natively leased key detaches the native lease,
    // which stays active for its other keys.
    f.run(&grant(L1, 30));
    f.run(&put(b"/n", b"1", Some(L1)));
    f.run(&put(b"/o", b"1", Some(L1)));
    assert_eq!(f.record(L1).attached_keys, 2);
    let p = f.run(&kine_update(b"/n", b"2", 3, 10, Some(B2)));
    assert!(matches!(
        p.response.outcome,
        Outcome::KineUpdated { updated: true, .. }
    ));
    assert_eq!(f.record(L1).attached_keys, 1);
    assert_eq!(f.record(L1).status, LeaseStatus::Active);
    assert_eq!(f.current[b"/n".as_slice()].lease, Some(B2));
    // Revoking the native lease deletes only its remaining attachment.
    let p = f.run(&revoke(L1));
    assert_eq!(p.response.outcome, Outcome::LeaseRevoked { deleted: 1 });
    assert!(f.current.contains_key(b"/n".as_slice()));
    assert!(!f.current.contains_key(b"/o".as_slice()));
    // Schema rules: a binding must accompany a positive TTL and vice versa.
    let v = f.view(ALICE);
    let bad = kine_create(b"/z", b"1", 5, None);
    assert_eq!(
        plan(&bad, &v, &f.limits).unwrap_err(),
        PlanError::Invalid(ValidationError::BindingMismatch)
    );
    let bad = kine_create(b"/z", b"1", 0, Some(B3));
    assert_eq!(
        plan(&bad, &v, &f.limits).unwrap_err(),
        PlanError::Invalid(ValidationError::BindingMismatch)
    );
    let bad = kine_update(b"/z", b"1", 0, 0, None);
    assert_eq!(
        plan(&bad, &v, &f.limits).unwrap_err(),
        PlanError::Invalid(ValidationError::ZeroRevision)
    );
}

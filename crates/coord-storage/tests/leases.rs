//! task-15 at the storage layer: lease records and reverse-index rows are
//! identical on the model engine and redb, a revocation deletes exactly the
//! current attachments in one atomic revision (all or nothing across a
//! crash), and a retried grant neither grants twice nor takes a revision.

use std::collections::BTreeMap;

use coord_core::effect::{BootId, PersistBatch};
use coord_core::outbox::BarrierAllocator;
use coord_redb_faultkit::{FaultBackend, FaultPlan, Tail};
use coord_state::{InternalCommand, LeaseStatus, Outcome, PlanLimits, plan, plan_internal};
use coord_storage::codecs;
use coord_storage::retry::{self, Admission, RetryBinding};
use coord_storage::{
    ApplyOutcome, GroupLimits, StoreWorker, ViewBudget, active_leases, apply_plan,
    build_internal_view, build_read_view, events_at,
};
use coord_storage_redb::RedbEngine;
use coord_store_api::engine::{LocalEngine, OrderedRead, ScanRequest, SnapshotSource};
use coord_store_api::registry::Collection;
use coord_store_testkit::model::ModelEngine;
use coord_types::ids::*;
use coord_types::logical_v1::*;
use coord_types::{CommandId, RetryKey};

const NS: NamespaceId = NamespaceId([0x11; 16]);
const ALICE: PrincipalId = PrincipalId([0xa; 16]);
const BOB: PrincipalId = PrincipalId([0xb; 16]);
const L1: LeaseId = LeaseId([1; 16]);
const L2: LeaseId = LeaseId([2; 16]);
const SESSION: SessionId = SessionId([0x51; 16]);
const CLIENT: ClientInstanceId = ClientInstanceId([0xc1; 16]);

fn inc() -> ReplicaIncarnation {
    ReplicaIncarnation::new(1).unwrap()
}

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

fn keep_alive(id: LeaseId) -> LogicalRequest {
    req(CanonicalOperation::LeaseKeepAlive { lease_id: id })
}

fn establish(epoch: u64) -> InternalCommand {
    InternalCommand::EstablishLeaseAuthority {
        namespace: NS,
        epoch: LeaseAuthorityEpoch::new(epoch).unwrap(),
    }
}

fn expire(id: LeaseId, seq: u64, epoch: u64) -> InternalCommand {
    InternalCommand::ExpireLease {
        namespace: NS,
        lease_id: id,
        generation: LeaseGeneration::new(1).unwrap(),
        expected_renewal_sequence: seq,
        authority_epoch: LeaseAuthorityEpoch::new(epoch).unwrap(),
    }
}

fn retry_key(seq: u64) -> RetryKey {
    RetryKey {
        cluster_id: ClusterId([1; 16]),
        domain_id: DomainId([2; 16]),
        session_id: SESSION,
        client_instance_id: CLIENT,
        request_sequence: RequestSequence::new(seq).unwrap(),
    }
}

fn binding(seq: u64, request: &LogicalRequest) -> RetryBinding {
    RetryBinding {
        retry_key: retry_key(seq),
        command_id: CommandId::derive(&retry_key(seq), request).unwrap(),
    }
}

struct Domain<E: LocalEngine> {
    worker: StoreWorker<E>,
    alloc: BarrierAllocator,
}

/// Every row of the lease-relevant collections, tagged by collection id.
type Rows = BTreeMap<(u16, Vec<u8>), Vec<u8>>;

impl<E: LocalEngine> Domain<E> {
    fn new(engine: E) -> Self {
        let boot = BootId([1; 16]);
        Domain {
            worker: StoreWorker::open(engine, boot, inc(), GroupLimits::default()).unwrap(),
            alloc: BarrierAllocator::new(inc(), boot),
        }
    }

    fn run_as(&mut self, principal: PrincipalId, request: &LogicalRequest) -> Outcome {
        self.run_bound(principal, request, None).1
    }

    fn run_bound(
        &mut self,
        principal: PrincipalId,
        request: &LogicalRequest,
        seq: Option<u64>,
    ) -> (Admission, Outcome) {
        loop {
            let gated = self.worker.reader().snapshot().unwrap();
            let bound = seq.map(|s| binding(s, request));
            let admission = match &bound {
                Some(b) => retry::admit(gated.view(), b, |_| true).unwrap(),
                None => Admission::New,
            };
            if let Admission::Retry(record) = &admission {
                let stored: coord_state::Response = postcard::from_bytes(&record.response).unwrap();
                return (admission.clone(), stored.outcome);
            }
            assert_eq!(admission, Admission::New);
            let view =
                build_read_view(&gated, NS, principal, request, ViewBudget::default()).unwrap();
            let planned = plan(request, &view, &PlanLimits::default()).unwrap();
            drop(gated);
            match apply_plan(
                &mut self.worker,
                self.alloc.allocate(),
                NS,
                &planned,
                bound.as_ref(),
            )
            .unwrap()
            {
                ApplyOutcome::Applied(_) => return (admission, planned.response.outcome),
                ApplyOutcome::Replan => continue,
                ApplyOutcome::Indeterminate => {
                    self.worker.reconcile().unwrap();
                }
            }
        }
    }

    fn run_internal(&mut self, command: &InternalCommand) -> Outcome {
        loop {
            let gated = self.worker.reader().snapshot().unwrap();
            let view = build_internal_view(&gated, command, ViewBudget::default()).unwrap();
            let planned = plan_internal(command, &view, &PlanLimits::default()).unwrap();
            drop(gated);
            match apply_plan(&mut self.worker, self.alloc.allocate(), NS, &planned, None).unwrap() {
                ApplyOutcome::Applied(_) => return planned.response.outcome,
                ApplyOutcome::Replan => continue,
                ApplyOutcome::Indeterminate => {
                    self.worker.reconcile().unwrap();
                }
            }
        }
    }

    fn activate_session(&mut self) {
        let update =
            coord_storage::policy::bootstrap_session(&SESSION, PrincipalId([0xaa; 16]), 16, true)
                .unwrap();
        self.worker
            .submit(PersistBatch {
                barrier: self.alloc.allocate(),
                base: Some(self.worker.application_base()),
                updates: update,
            })
            .unwrap();
        assert_eq!(self.worker.flush().unwrap().committed, 1);
    }

    fn rows(&self) -> Rows {
        let gated = self.worker.reader().snapshot().unwrap();
        rows_of(gated.view())
    }

    fn lease(&self, id: LeaseId) -> Option<coord_state::LeaseRecord> {
        let gated = self.worker.reader().snapshot().unwrap();
        gated
            .view()
            .get(Collection::LeaseV1.id(), &codecs::lease_row_key(&id))
            .unwrap()
            .map(|v| codecs::decode_lease(&v).unwrap())
    }

    fn bindings(&self, id: LeaseId) -> Vec<(Vec<u8>, codecs::LeaseKeyRecordV1)> {
        self.rows()
            .into_iter()
            .filter(|((c, _), _)| *c == Collection::LeaseKeysV1.id().0)
            .filter_map(|((_, k), v)| {
                let (lease, ns, key) = codecs::decode_lease_key_row(&k).unwrap();
                (lease == id && ns == NS).then(|| (key, codecs::decode_lease_key(&v).unwrap()))
            })
            .collect()
    }

    fn kv_revision(&self) -> u64 {
        let gated = self.worker.reader().snapshot().unwrap();
        codecs::read_kv_revision(gated.view()).unwrap().get()
    }
}

fn rows_of<V: OrderedRead>(view: &V) -> Rows {
    let mut out = BTreeMap::new();
    for c in [
        Collection::KvCurrentV1,
        Collection::KvHistoryV1,
        Collection::EventsV1,
        Collection::LeaseV1,
        Collection::LeaseKeysV1,
    ] {
        let mut request = ScanRequest::all(1000, 1 << 24);
        loop {
            let page = view.scan_page(c.id(), &request).unwrap();
            for r in &page.rows {
                out.insert((c.id().0, r.key.clone()), r.value.clone());
            }
            if page.exhausted {
                break;
            }
            request.resume_after = Some(page.rows.last().unwrap().key.clone());
        }
    }
    out
}

fn workload<E: LocalEngine>(d: &mut Domain<E>) {
    assert!(matches!(
        d.run_as(ALICE, &grant(L1, 30)),
        Outcome::LeaseGranted { lease_id: L1, .. }
    ));
    d.run_as(ALICE, &grant(L2, 30));
    d.run_as(ALICE, &put(b"a", b"1", Some(L1))); // rev 1
    d.run_as(ALICE, &put(b"b", b"2", Some(L1))); // rev 2
    d.run_as(ALICE, &put(b"c", b"3", Some(L2))); // rev 3
    d.run_as(BOB, &put(b"d", b"4", None)); // rev 4
    d.run_as(ALICE, &put(b"a", b"11", Some(L1))); // rev 5: refreshed binding
    d.run_as(ALICE, &put(b"b", b"22", Some(L2))); // rev 6: moved to L2
    assert_eq!(
        d.run_as(BOB, &put(b"bob", b"x", Some(L1))),
        Outcome::ErrLeasePermission
    );
}

#[test]
fn lease_rows_match_across_engines_and_revoke_is_one_atomic_revision() {
    let (backend, _) = FaultBackend::new(Vec::new(), FaultPlan::default());
    let mut redb = Domain::new(RedbEngine::create_on_backend(backend, 4 << 20).unwrap());
    let mut model = Domain::new(ModelEngine::new());
    for d in [&mut model as &mut dyn Driver, &mut redb] {
        d.workload();
    }
    assert_eq!(model.rows(), redb.rows(), "identical rows before revoke");
    // Reverse index: a and its refreshed mod revision under L1; b moved.
    let l1 = redb.bindings(L1);
    assert_eq!(l1.len(), 1);
    assert_eq!(l1[0].0, b"a".to_vec());
    assert_eq!(l1[0].1.mod_revision, rev(5));
    assert_eq!(l1[0].1.generation, LeaseGeneration::new(1).unwrap());
    let l2 = redb.bindings(L2);
    assert_eq!(
        l2.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
        vec![b"b".to_vec(), b"c".to_vec()]
    );
    let record = redb.lease(L1).unwrap();
    assert_eq!(record.owner, ALICE);
    assert_eq!(record.attached_keys, 1);
    assert_eq!(
        record.attached_bytes,
        coord_state::attachment_cost(b"a", b"11")
    );
    assert_eq!(
        redb.run_as(ALICE, &ttl(L2, true)),
        Outcome::LeaseTimeToLive {
            lease_id: L2,
            generation: LeaseGeneration::new(1).unwrap(),
            granted_ttl_seconds: 30,
            renewal_sequence: 0,
            keys: Some(vec![b"b".to_vec(), b"c".to_vec()]),
        }
    );
    assert_eq!(redb.kv_revision(), 6, "inspection takes no revision");

    for d in [&mut model as &mut dyn Driver, &mut redb] {
        assert_eq!(d.revoke_l2(), Outcome::LeaseRevoked { deleted: 2 });
    }
    assert_eq!(model.rows(), redb.rows(), "identical rows after revoke");
    assert_eq!(redb.kv_revision(), 7);
    let gated = redb.worker.reader().snapshot().unwrap();
    let events = events_at(gated.view(), rev(7)).unwrap().unwrap();
    assert_eq!(events.len(), 2, "one revision for the whole set");
    assert_eq!(
        events.iter().map(|e| e.key.clone()).collect::<Vec<_>>(),
        vec![b"b".to_vec(), b"c".to_vec()]
    );
    assert_eq!(events[0].prev.as_ref().unwrap().value, b"22".to_vec());
    let current = |k: &[u8]| {
        gated
            .view()
            .get(Collection::KvCurrentV1.id(), &codecs::current_key(&NS, k))
            .unwrap()
    };
    assert!(current(b"b").is_none() && current(b"c").is_none());
    assert!(current(b"a").is_some() && current(b"d").is_some());
    drop(gated);
    assert!(redb.bindings(L2).is_empty());
    assert_eq!(redb.lease(L2).unwrap().status, LeaseStatus::Revoked);
    assert_eq!(redb.lease(L2).unwrap().attached_keys, 0);
    assert_eq!(redb.bindings(L1).len(), 1, "other leases untouched");
    // The tombstone refuses a new grant of the identity and further use.
    assert_eq!(redb.run_as(ALICE, &grant(L2, 1)), Outcome::ErrLeaseExists);
    assert_eq!(redb.run_as(ALICE, &revoke(L2)), Outcome::ErrLeaseNotFound);
    assert_eq!(
        redb.run_as(ALICE, &put(b"z", b"1", Some(L2))),
        Outcome::ErrLeaseNotFound
    );
    assert_eq!(redb.kv_revision(), 7, "failures take no revision");
}

/// Object-safe driver so one loop runs both engines.
trait Driver {
    fn workload(&mut self);
    fn revoke_l2(&mut self) -> Outcome;
}

impl<E: LocalEngine> Driver for Domain<E> {
    fn workload(&mut self) {
        workload(self)
    }
    fn revoke_l2(&mut self) -> Outcome {
        self.run_as(ALICE, &revoke(L2))
    }
}

#[test]
fn a_retried_grant_never_grants_twice_or_allocates_a_revision() {
    let mut d = Domain::new(ModelEngine::new());
    d.activate_session();
    let g = grant(L1, 30);
    let (admission, outcome) = d.run_bound(ALICE, &g, Some(1));
    assert_eq!(admission, Admission::New);
    let granted = Outcome::LeaseGranted {
        lease_id: L1,
        generation: LeaseGeneration::new(1).unwrap(),
        ttl_seconds: 30,
    };
    assert_eq!(outcome, granted);
    let position = d.worker.application_base().execution_position;
    // Session activation is an ordered step, so the grant is position 2.
    assert_eq!(position.get(), 2);
    // The response was lost; the same identity is presented again.
    let (admission, outcome) = d.run_bound(ALICE, &g, Some(1));
    assert!(matches!(admission, Admission::Retry(_)));
    assert_eq!(outcome, granted, "the retained result, not a new grant");
    assert_eq!(d.worker.application_base().execution_position, position);
    assert_eq!(d.kv_revision(), 0, "no revision was ever allocated");
    assert_eq!(
        d.lease(L1).unwrap().generation,
        LeaseGeneration::new(1).unwrap()
    );
    // A different request colliding on the identity is a recorded failure.
    let (admission, outcome) = d.run_bound(ALICE, &grant(L1, 60), Some(2));
    assert_eq!(admission, Admission::New);
    assert_eq!(outcome, Outcome::ErrLeaseExists);
    assert_eq!(d.lease(L1).unwrap().ttl_seconds, 30);
    assert_eq!(d.kv_revision(), 0);
    // The retained failure is returned on retry as well.
    let (admission, outcome) = d.run_bound(ALICE, &grant(L1, 60), Some(2));
    assert!(matches!(admission, Admission::Retry(_)));
    assert_eq!(outcome, Outcome::ErrLeaseExists);
    // A retried revoke deletes once: the attachment's revision is allocated
    // exactly one time.
    d.run_as(ALICE, &put(b"a", b"1", Some(L1)));
    let r = revoke(L1);
    let (_, outcome) = d.run_bound(ALICE, &r, Some(3));
    assert_eq!(outcome, Outcome::LeaseRevoked { deleted: 1 });
    assert_eq!(d.kv_revision(), 2);
    let (admission, outcome) = d.run_bound(ALICE, &r, Some(3));
    assert!(matches!(admission, Admission::Retry(_)));
    assert_eq!(outcome, Outcome::LeaseRevoked { deleted: 1 });
    assert_eq!(d.kv_revision(), 2);
}

#[test]
fn a_crash_during_revoke_leaves_all_attachments_or_none() {
    let setup_requests = [
        grant(L1, 30),
        put(b"a", b"1", Some(L1)),
        put(b"b", &[2; 300], Some(L1)),
        put(b"c", &[3; 300], Some(L1)),
        put(b"d", b"4", None),
    ];
    let (backend, shared) = FaultBackend::new(Vec::new(), FaultPlan::default());
    let mut d = Domain::new(RedbEngine::create_on_backend(backend, 4 << 20).unwrap());
    for r in &setup_requests {
        d.run_as(ALICE, r);
    }
    let before = shared.ops();
    d.run_as(ALICE, &revoke(L1));
    let total = shared.ops() - before;
    let image_before = {
        // Rebuild a clean image with the setup only (the revoke above is
        // just for counting operations).
        let (backend, shared) = FaultBackend::new(Vec::new(), FaultPlan::default());
        let mut d = Domain::new(RedbEngine::create_on_backend(backend, 4 << 20).unwrap());
        for r in &setup_requests {
            d.run_as(ALICE, r);
        }
        drop(d);
        shared.crash_image(Tail::All)
    };
    drop(d);
    let mut saw_none = false;
    let mut saw_all = false;
    for k in 1..=total {
        let (backend, shared) = FaultBackend::new(image_before.clone(), FaultPlan::default());
        let mut d = Domain::new(RedbEngine::from_backend(backend, 4 << 20).unwrap());
        let start = shared.ops();
        shared.set_plan(FaultPlan {
            crash_after: Some(start + k),
            tail: Tail::Seeded(k),
            ..FaultPlan::default()
        });
        let gated = d.worker.reader().snapshot().unwrap();
        let r = revoke(L1);
        let view = build_read_view(&gated, NS, ALICE, &r, ViewBudget::default()).unwrap();
        let planned = plan(&r, &view, &PlanLimits::default()).unwrap();
        drop(gated);
        let _ = apply_plan(&mut d.worker, d.alloc.allocate(), NS, &planned, None);
        let image = shared.crash_image(Tail::Seeded(k));
        drop(d);
        let (backend, _) = FaultBackend::new(image, FaultPlan::default());
        let engine = RedbEngine::from_backend(backend, 4 << 20).unwrap();
        let view = engine.reader().snapshot().unwrap();
        let rows = rows_of(&view);
        let current: Vec<Vec<u8>> = rows
            .keys()
            .filter(|(c, _)| *c == Collection::KvCurrentV1.id().0)
            .map(|(_, k)| coord_types::ordered_key::decode_current(k).unwrap().key)
            .collect();
        let bindings = rows
            .keys()
            .filter(|(c, _)| *c == Collection::LeaseKeysV1.id().0)
            .count();
        let lease = codecs::decode_lease(
            &view
                .get(Collection::LeaseV1.id(), &codecs::lease_row_key(&L1))
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        let kv_revision = codecs::read_kv_revision(&view).unwrap().get();
        if kv_revision == 4 {
            assert_eq!(
                current,
                vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]
            );
            assert_eq!(bindings, 3, "k={k}");
            assert_eq!(lease.status, LeaseStatus::Active, "k={k}");
            assert_eq!(lease.attached_keys, 3);
            saw_none = true;
        } else {
            assert_eq!(kv_revision, 5, "k={k}");
            assert_eq!(current, vec![b"d".to_vec()], "k={k}");
            assert_eq!(bindings, 0, "k={k}");
            assert_eq!(lease.status, LeaseStatus::Revoked, "k={k}");
            assert_eq!(lease.attached_keys, 0);
            assert_eq!(events_at(&view, rev(5)).unwrap().unwrap().len(), 3, "k={k}");
            saw_all = true;
        }
    }
    assert!(
        saw_none && saw_all,
        "the crash matrix covered both outcomes"
    );
}

#[test]
fn a_lease_at_its_byte_quota_is_still_revocable_in_one_batch() {
    // Worst case for the revoke batch: keys of the maximum length made of
    // zero bytes (the ordered-key escaping doubles them in three row keys)
    // holding values of the maximum length. The quota must admit exactly
    // what one revocation can write below the worker's single-batch limit.
    use coord_storage::lowering::batch_bytes;
    use coord_types::logical_v1::limits::{MAX_KEY_BYTES, MAX_VALUE_BYTES};
    let mut d = Domain::new(ModelEngine::new());
    d.activate_session();
    d.run_as(ALICE, &grant(L1, 30));
    let limits = PlanLimits::default();
    let value = vec![0x5a; MAX_VALUE_BYTES];
    let per_key = coord_state::attachment_cost(&vec![0; MAX_KEY_BYTES], &value);
    let fits = limits.max_lease_bytes / per_key;
    assert!(fits >= 2, "the default quota admits several maximal keys");
    for i in 0..fits {
        let mut key = vec![0u8; MAX_KEY_BYTES];
        key[MAX_KEY_BYTES - 1] = i as u8;
        assert_eq!(
            d.run_as(ALICE, &put(&key, &value, Some(L1))),
            Outcome::Put { prev: None }
        );
    }
    let mut over = vec![0u8; MAX_KEY_BYTES];
    over[MAX_KEY_BYTES - 1] = 0xff;
    assert_eq!(
        d.run_as(ALICE, &put(&over, &value, Some(L1))),
        Outcome::ErrLeaseQuota
    );
    let record = d.lease(L1).unwrap();
    assert_eq!(record.attached_bytes, fits * per_key);
    assert!(record.attached_bytes <= limits.max_lease_bytes);
    // The materialized revoke batch, retry binding included, stays under
    // the worker's single-batch limit and applies.
    let r = revoke(L1);
    let bound = binding(7, &r);
    let gated = d.worker.reader().snapshot().unwrap();
    let view = build_read_view(&gated, NS, ALICE, &r, ViewBudget::default()).unwrap();
    let planned = plan(&r, &view, &limits).unwrap();
    drop(gated);
    let batch =
        coord_storage::plan_to_batch(d.alloc.allocate(), NS, &planned, Some(&bound)).unwrap();
    let bytes = batch_bytes(&batch);
    assert!(
        bytes <= GroupLimits::default().max_single_batch_bytes,
        "revoke batch of {bytes} bytes exceeds the single-batch limit"
    );
    assert!(
        bytes as u64 <= record.attached_bytes + 64 * 1024,
        "the quota bounds the batch: {bytes} bytes for {} charged",
        record.attached_bytes
    );
    assert_eq!(
        d.run_bound(ALICE, &r, Some(7)).1,
        Outcome::LeaseRevoked { deleted: fits }
    );
    assert_eq!(d.lease(L1).unwrap().status, LeaseStatus::Revoked);
    assert!(d.bindings(L1).is_empty());
}

#[test]
fn a_retried_keepalive_extends_once_and_renewals_are_replicated() {
    let mut d = Domain::new(ModelEngine::new());
    d.activate_session();
    d.run_as(ALICE, &grant(L1, 30));
    let k = keep_alive(L1);
    let renewed = |seq: u64| Outcome::LeaseKeptAlive {
        lease_id: L1,
        generation: LeaseGeneration::new(1).unwrap(),
        renewal_sequence: seq,
        ttl_seconds: 30,
    };
    let (admission, outcome) = d.run_bound(ALICE, &k, Some(1));
    assert_eq!(admission, Admission::New);
    assert_eq!(outcome, renewed(1));
    assert_eq!(d.lease(L1).unwrap().renewal_sequence, 1);
    // The same invocation presented again returns the retained renewal and
    // does not extend the lease a second time.
    let (admission, outcome) = d.run_bound(ALICE, &k, Some(1));
    assert!(matches!(admission, Admission::Retry(_)));
    assert_eq!(outcome, renewed(1));
    assert_eq!(d.lease(L1).unwrap().renewal_sequence, 1);
    // A new invocation is a new replicated renewal.
    let (_, outcome) = d.run_bound(ALICE, &k, Some(2));
    assert_eq!(outcome, renewed(2));
    assert_eq!(d.lease(L1).unwrap().renewal_sequence, 2);
    assert_eq!(d.kv_revision(), 0, "renewals take no KV revision");
    assert_eq!(d.run_as(BOB, &keep_alive(L1)), Outcome::ErrLeasePermission);
}

#[test]
fn expiration_through_storage_is_conditional_and_recovery_lists_active_leases() {
    let (backend, _) = FaultBackend::new(Vec::new(), FaultPlan::default());
    let mut d = Domain::new(RedbEngine::create_on_backend(backend, 4 << 20).unwrap());
    d.run_as(ALICE, &grant(L1, 5));
    d.run_as(ALICE, &grant(L2, 5));
    d.run_as(ALICE, &put(b"a", b"1", Some(L1)));
    d.run_as(ALICE, &put(b"b", b"2", Some(L1)));
    d.run_as(ALICE, &put(b"c", b"3", Some(L2)));
    // No authority yet: nothing expires, not even a command carrying the
    // zero sentinel the unestablished frontier holds.
    assert_eq!(
        d.run_internal(&expire(L1, 0, 1)),
        Outcome::ErrStaleAuthority
    );
    assert_eq!(
        d.run_internal(&expire(L1, 0, 0)),
        Outcome::ErrStaleAuthority,
        "the zero epoch never authorizes an expiration"
    );
    assert_eq!(d.lease(L1).unwrap().status, LeaseStatus::Active);
    assert_eq!(
        d.run_internal(&establish(1)),
        Outcome::LeaseAuthorityEstablished {
            epoch: LeaseAuthorityEpoch::new(1).unwrap()
        }
    );
    assert_eq!(d.run_internal(&establish(1)), Outcome::ErrStaleAuthority);
    // A renewal ordered first makes the old expiration a no-op.
    d.run_as(ALICE, &keep_alive(L1));
    assert_eq!(d.run_internal(&expire(L1, 0, 1)), Outcome::ExpireStale);
    assert_eq!(d.kv_revision(), 3);
    // Matching: attachments deleted in one revision, record marked, index cleared.
    assert_eq!(
        d.run_internal(&expire(L1, 1, 1)),
        Outcome::LeaseExpired { deleted: 2 }
    );
    assert_eq!(d.kv_revision(), 4);
    let gated = d.worker.reader().snapshot().unwrap();
    assert_eq!(events_at(gated.view(), rev(4)).unwrap().unwrap().len(), 2);
    let recovered = active_leases(gated.view(), ViewBudget::default()).unwrap();
    drop(gated);
    assert_eq!(
        recovered.len(),
        1,
        "only the surviving lease is armed on recovery"
    );
    assert_eq!(recovered[0].0, L2);
    assert_eq!(recovered[0].1.renewal_sequence, 0);
    assert_eq!(d.lease(L1).unwrap().status, LeaseStatus::Expired);
    assert!(d.bindings(L1).is_empty());
    assert_eq!(d.run_as(ALICE, &keep_alive(L1)), Outcome::ErrLeaseNotFound);
    // A successor authority fences the former epoch's expirations.
    d.run_internal(&establish(2));
    assert_eq!(
        d.run_internal(&expire(L2, 0, 1)),
        Outcome::ErrStaleAuthority
    );
    assert!(d.lease(L2).unwrap().status == LeaseStatus::Active);
    assert_eq!(
        d.run_internal(&expire(L2, 0, 2)),
        Outcome::LeaseExpired { deleted: 1 }
    );
    assert_eq!(d.kv_revision(), 5);
    let gated = d.worker.reader().snapshot().unwrap();
    assert!(
        active_leases(gated.view(), ViewBudget::default())
            .unwrap()
            .is_empty()
    );
}

#[test]
fn active_lease_recovery_pages_past_tombstones_within_a_small_budget() {
    // Ended leases stay as tombstones and are examined by the recovery
    // scan; with far more of them than one budget admits, recovery must
    // still reach every active lease by paging.
    use coord_storage::active_leases_page;
    let mut d = Domain::new(ModelEngine::new());
    let mut active = Vec::new();
    for i in 0..40u8 {
        let id = LeaseId([i + 0x10; 16]);
        d.run_as(ALICE, &grant(id, 30));
        if i % 8 == 0 {
            active.push(id);
        } else {
            assert_eq!(
                d.run_as(ALICE, &revoke(id)),
                Outcome::LeaseRevoked { deleted: 0 }
            );
        }
    }
    let budget = ViewBudget {
        max_rows: 7,
        max_bytes: 1 << 20,
    };
    let gated = d.worker.reader().snapshot().unwrap();
    let mut after = None;
    let mut pages = 0;
    let mut found = Vec::new();
    loop {
        let page = active_leases_page(gated.view(), after, budget).unwrap();
        pages += 1;
        assert!(page.leases.len() <= 7);
        found.extend(page.leases.iter().map(|(id, _)| *id));
        match page.next {
            Some(next) => {
                assert!(after.is_none_or(|a: LeaseId| next > a), "pages advance");
                after = Some(next);
            }
            None => break,
        }
    }
    assert!(
        pages >= 6,
        "40 rows under a 7-row budget take several pages"
    );
    assert_eq!(found, active);
    assert_eq!(
        active_leases(gated.view(), budget)
            .unwrap()
            .into_iter()
            .map(|(id, _)| id)
            .collect::<Vec<_>>(),
        active,
        "the drained listing pages internally"
    );
    // A budget too small for even one row is an error, not an empty page
    // that would never advance.
    assert!(
        active_leases_page(
            gated.view(),
            None,
            ViewBudget {
                max_rows: 1,
                max_bytes: 8,
            }
        )
        .is_err()
    );
}

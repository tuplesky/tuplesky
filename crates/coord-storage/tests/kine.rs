//! task-17 at the storage layer: a Kine write is one logical operation
//! (one execution position, the revision in its own result, the binding in
//! the same atomic batch, no pre-read, CurrentRevision or lease-grant round
//! trip), retries preserve the binding, a recovering scheduler sees private
//! bindings, and a replaced binding's expiration is stale on redb.

use coord_core::effect::{BootId, PersistBatch};
use coord_core::outbox::BarrierAllocator;
use coord_redb_faultkit::{FaultBackend, FaultPlan};
use coord_state::{
    InternalCommand, LeasePurpose, LeaseStatus, Outcome, PlanLimits, plan, plan_internal,
};
use coord_storage::codecs;
use coord_storage::retry::{self, Admission, RetryBinding};
use coord_storage::{
    ApplyOutcome, GroupLimits, StoreWorker, ViewBudget, active_leases, apply_plan,
    build_internal_view, build_read_view, events_at,
};
use coord_storage_redb::RedbEngine;
use coord_store_api::engine::{LocalEngine, OrderedRead};
use coord_store_api::registry::Collection;
use coord_types::ids::*;
use coord_types::logical_v1::*;
use coord_types::{CommandId, RetryKey};

const NS: NamespaceId = NamespaceId([0x11; 16]);
const ALICE: PrincipalId = PrincipalId([0xa; 16]);
const B1: LeaseId = LeaseId([0x11; 16]);
const B2: LeaseId = LeaseId([0x22; 16]);
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

fn kine_create(key: &[u8], value: &[u8], ttl: u32, binding: Option<LeaseId>) -> LogicalRequest {
    req(CanonicalOperation::KineCreate(KineCreateOp {
        key: key.to_vec(),
        value: value.to_vec(),
        ttl_seconds: ttl,
        binding,
    }))
}

fn kine_update(
    key: &[u8],
    value: &[u8],
    expected: u64,
    ttl: u32,
    binding: Option<LeaseId>,
) -> LogicalRequest {
    req(CanonicalOperation::KineUpdate(KineUpdateOp {
        key: key.to_vec(),
        value: value.to_vec(),
        expected_mod_revision: rev(expected),
        ttl_seconds: ttl,
        binding,
    }))
}

fn kine_delete(key: &[u8], expected: Option<u64>) -> LogicalRequest {
    req(CanonicalOperation::KineDelete(KineDeleteOp {
        key: key.to_vec(),
        expected_mod_revision: expected.map(rev),
    }))
}

fn establish(epoch: u64) -> InternalCommand {
    InternalCommand::EstablishLeaseAuthority {
        namespace: NS,
        epoch: LeaseAuthorityEpoch::new(epoch).unwrap(),
    }
}

fn expire(id: LeaseId, epoch: u64) -> InternalCommand {
    InternalCommand::ExpireLease {
        namespace: NS,
        lease_id: id,
        generation: LeaseGeneration::new(1).unwrap(),
        expected_renewal_sequence: 0,
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
    /// Logical operations submitted (the trace).
    trace: Vec<String>,
}

impl<E: LocalEngine> Domain<E> {
    fn new(engine: E) -> Self {
        let boot = BootId([1; 16]);
        Domain {
            worker: StoreWorker::open(engine, boot, inc(), GroupLimits::default()).unwrap(),
            alloc: BarrierAllocator::new(inc(), boot),
            trace: Vec::new(),
        }
    }

    fn activate_session(&mut self) {
        let update = retry::session_update(&SESSION, true, 16).unwrap();
        self.worker
            .submit(PersistBatch {
                barrier: self.alloc.allocate(),
                base: Some(self.worker.application_base()),
                updates: vec![update],
            })
            .unwrap();
        assert_eq!(self.worker.flush().unwrap().committed, 1);
    }

    /// One logical operation: admit, build the view, plan, apply.
    fn run(&mut self, request: &LogicalRequest, seq: u64) -> (Admission, coord_state::Response) {
        let b = binding(seq, request);
        let gated = self.worker.reader().snapshot().unwrap();
        let admission = retry::admit(gated.view(), &b).unwrap();
        if let Admission::Retry(record) = &admission {
            let stored: coord_state::Response = postcard::from_bytes(&record.response).unwrap();
            return (admission.clone(), stored);
        }
        assert_eq!(admission, Admission::New);
        let view = build_read_view(&gated, NS, ALICE, request, ViewBudget::default()).unwrap();
        let planned = plan(request, &view, &PlanLimits::default()).unwrap();
        drop(gated);
        self.trace.push(
            format!("{:?}", request.operation)
                .split('(')
                .next()
                .unwrap()
                .to_owned(),
        );
        match apply_plan(
            &mut self.worker,
            self.alloc.allocate(),
            NS,
            &planned,
            Some(&b),
        )
        .unwrap()
        {
            ApplyOutcome::Applied(_) => (admission, planned.response),
            other => panic!("{other:?}"),
        }
    }

    fn run_internal(&mut self, command: &InternalCommand) -> Outcome {
        let gated = self.worker.reader().snapshot().unwrap();
        let view = build_internal_view(&gated, command, ViewBudget::default()).unwrap();
        let planned = plan_internal(command, &view, &PlanLimits::default()).unwrap();
        drop(gated);
        match apply_plan(&mut self.worker, self.alloc.allocate(), NS, &planned, None).unwrap() {
            ApplyOutcome::Applied(_) => planned.response.outcome,
            other => panic!("{other:?}"),
        }
    }

    fn lease(&self, id: LeaseId) -> Option<coord_state::LeaseRecord> {
        let gated = self.worker.reader().snapshot().unwrap();
        gated
            .view()
            .get(Collection::LeaseV1.id(), &codecs::lease_row_key(&id))
            .unwrap()
            .map(|v| codecs::decode_lease(&v).unwrap())
    }

    fn current(&self, key: &[u8]) -> Option<coord_state::KvEntry> {
        let gated = self.worker.reader().snapshot().unwrap();
        gated
            .view()
            .get(Collection::KvCurrentV1.id(), &codecs::current_key(&NS, key))
            .unwrap()
            .map(|v| codecs::decode_current(&v).unwrap())
    }

    fn position(&self) -> u64 {
        self.worker.application_base().execution_position.get()
    }

    fn active(&self) -> Vec<(LeaseId, coord_state::LeaseRecord)> {
        let gated = self.worker.reader().snapshot().unwrap();
        active_leases(gated.view(), ViewBudget::default()).unwrap()
    }
}

#[test]
fn a_kine_write_is_one_logical_operation_and_retries_preserve_the_binding() {
    let (backend, _) = FaultBackend::new(Vec::new(), FaultPlan::default());
    let mut d = Domain::new(RedbEngine::create_on_backend(backend, 4 << 20).unwrap());
    d.activate_session();
    let create = kine_create(b"/registry/pods/p", b"pod", 60, Some(B1));
    let before = d.position();
    let (admission, response) = d.run(&create, 1);
    assert_eq!(admission, Admission::New);
    assert_eq!(response.outcome, Outcome::KineCreated);
    assert_eq!(
        response.revision,
        rev(1),
        "the revision comes back in the result"
    );
    assert_eq!(d.position(), before + 1, "exactly one execution position");
    assert_eq!(
        d.trace,
        vec!["KineCreate"],
        "no pre-read, CurrentRevision or lease grant"
    );
    let b1 = d.lease(B1).unwrap();
    assert_eq!(b1.purpose, LeasePurpose::KinePrivate);
    assert_eq!(b1.ttl_seconds, 60);
    assert_eq!(d.current(b"/registry/pods/p").unwrap().lease, Some(B1));
    // The response was lost; the retry returns the retained result with the
    // same binding, without a second execution or a second binding.
    let (admission, again) = d.run(&create, 1);
    assert!(matches!(admission, Admission::Retry(_)));
    assert_eq!(again, response);
    assert_eq!(d.position(), before + 1);
    assert_eq!(d.lease(B1).unwrap(), b1);
    assert_eq!(d.active().len(), 1);
    // A recovering scheduler arms the private binding like any lease.
    assert_eq!(d.active()[0].0, B1);

    // CAS update with TTL replacement: one operation, old binding replaced.
    let update = kine_update(b"/registry/pods/p", b"pod2", 1, 30, Some(B2));
    let (_, response) = d.run(&update, 2);
    assert!(matches!(
        response.outcome,
        Outcome::KineUpdated { updated: true, current: Some(ref kv) } if kv.ttl_seconds == 30
    ));
    assert_eq!(response.revision, rev(2));
    assert_eq!(d.lease(B1).unwrap().status, LeaseStatus::Replaced);
    assert_eq!(d.lease(B2).unwrap().status, LeaseStatus::Active);
    let active = d.active();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].0, B2);
    // A failed CAS is still one operation and changes nothing durable.
    let rows_before = d.position();
    let (_, response) = d.run(
        &kine_update(b"/registry/pods/p", b"x", 1, 5, Some(LeaseId([9; 16]))),
        3,
    );
    assert!(matches!(
        response.outcome,
        Outcome::KineUpdated { updated: false, current: Some(ref kv) } if kv.ttl_seconds == 30
    ));
    assert_eq!(response.revision, rev(2));
    assert_eq!(d.position(), rows_before + 1);
    assert!(d.lease(LeaseId([9; 16])).is_none());
    assert_eq!(
        d.current(b"/registry/pods/p").unwrap().value,
        b"pod2".to_vec()
    );

    // The replaced binding's expiration is stale; the live one expires the key.
    d.run_internal(&establish(1));
    assert_eq!(d.run_internal(&expire(B1, 1)), Outcome::ExpireStale);
    assert!(d.current(b"/registry/pods/p").is_some());
    assert_eq!(
        d.run_internal(&expire(B2, 1)),
        Outcome::LeaseExpired { deleted: 1 }
    );
    assert!(d.current(b"/registry/pods/p").is_none());
    let gated = d.worker.reader().snapshot().unwrap();
    assert_eq!(events_at(gated.view(), rev(3)).unwrap().unwrap().len(), 1);
    drop(gated);
    assert!(d.active().is_empty());
    // Conditional delete distinctions after expiry: absent key.
    let (_, response) = d.run(&kine_delete(b"/registry/pods/p", Some(2)), 4);
    assert_eq!(
        response.outcome,
        Outcome::KineDeleted {
            deleted: true,
            prev: None
        }
    );
    assert_eq!(response.revision, rev(3));
    assert_eq!(
        d.trace,
        vec!["KineCreate", "KineUpdate", "KineUpdate", "KineDelete"]
    );
}

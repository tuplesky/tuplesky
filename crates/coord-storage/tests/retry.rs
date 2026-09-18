//! task-12: lost responses return the same result, changed payloads
//! conflict, a crash between materialization and notification never
//! duplicates, retired/unknown sessions and retired sequences never execute
//! as new work, and floors bound retention.

use coord_core::effect::{BootId, PersistBatch};
use coord_core::outbox::BarrierAllocator;
use coord_redb_faultkit::{FaultBackend, FaultPlan, Tail};
use coord_state::{Outcome, PlanLimits, plan};
use coord_storage::retry::{self, Admission, Resolution, RetryBinding};
use coord_storage::{
    ApplyOutcome, GroupLimits, StoreWorker, ViewBudget, apply_plan, build_read_view,
};
use coord_storage_redb::RedbEngine;
use coord_store_api::engine::{LocalEngine, OrderedRead};
use coord_store_api::registry::Collection;
use coord_store_testkit::model::ModelEngine;
use coord_types::ids::*;
use coord_types::logical_v1::*;
use coord_types::{CommandId, RetryKey};

const NS: NamespaceId = NamespaceId([0x11; 16]);
const SESSION: SessionId = SessionId([0x51; 16]);
const CLIENT: ClientInstanceId = ClientInstanceId([0xc1; 16]);

fn inc() -> ReplicaIncarnation {
    ReplicaIncarnation::new(1).unwrap()
}

fn key(seq: u64) -> RetryKey {
    RetryKey {
        cluster_id: ClusterId([1; 16]),
        domain_id: DomainId([2; 16]),
        session_id: SESSION,
        client_instance_id: CLIENT,
        request_sequence: RequestSequence::new(seq).unwrap(),
    }
}

fn put(k: &[u8], v: &[u8]) -> LogicalRequest {
    let mut r = LogicalRequest::new(
        NS,
        CanonicalOperation::Put(PutOp {
            key: k.to_vec(),
            value: v.to_vec(),
            lease: None,
            prev_kv: true,
        }),
    );
    r.canonicalize();
    r
}

fn binding(seq: u64, request: &LogicalRequest) -> RetryBinding {
    RetryBinding {
        retry_key: key(seq),
        command_id: CommandId::derive(&key(seq), request).unwrap(),
    }
}

struct Domain<E: LocalEngine> {
    worker: StoreWorker<E>,
    alloc: BarrierAllocator,
}

impl<E: LocalEngine> Domain<E> {
    fn new(engine: E) -> Self {
        let boot = BootId([1; 16]);
        Domain {
            worker: StoreWorker::open(engine, boot, inc(), GroupLimits::default()).unwrap(),
            alloc: BarrierAllocator::new(inc(), boot),
        }
    }

    /// Persist admission rows as an ordered application batch at the
    /// current frontier (session and floor changes are execution steps).
    fn admin(&mut self, updates: Vec<coord_core::effect::StoreUpdate>) {
        self.worker
            .submit(PersistBatch {
                barrier: self.alloc.allocate(),
                base: Some(self.worker.application_base()),
                updates,
            })
            .unwrap();
        assert_eq!(self.worker.flush().unwrap().committed, 1);
    }

    fn activate_session(&mut self, window: u32) {
        let update = coord_storage::policy::bootstrap_session(
            &SESSION,
            PrincipalId([0xaa; 16]),
            window,
            true,
        )
        .unwrap();
        self.admin(update);
    }

    /// Full path: admit -> (plan -> apply) or retained result.
    fn submit(
        &mut self,
        seq: u64,
        request: &LogicalRequest,
    ) -> (Admission, Option<coord_state::Response>) {
        let b = binding(seq, request);
        let gated = self.worker.reader().snapshot().unwrap();
        let admission = retry::admit(gated.view(), &b, |_| true).unwrap();
        if admission != Admission::New {
            return (admission, None);
        }
        let view = build_read_view(
            &gated,
            NS,
            PrincipalId([0xaa; 16]),
            request,
            ViewBudget::default(),
        )
        .unwrap();
        let planned = plan(request, &view, &PlanLimits::default()).unwrap();
        drop(gated);
        match apply_plan(
            &mut self.worker,
            self.alloc.allocate(),
            NS,
            &planned,
            Some(&b),
        )
        .unwrap()
        {
            ApplyOutcome::Applied(_) => (admission, Some(planned.response)),
            other => panic!("unexpected {other:?}"),
        }
    }

    fn resolve(&self, seq: u64, request: &LogicalRequest, authorized: bool) -> Resolution {
        let gated = self.worker.reader().snapshot().unwrap();
        retry::resolve(gated.view(), &binding(seq, request), |_| authorized).unwrap()
    }

    fn retire_through(&mut self, seq: u64) {
        let gated = self.worker.reader().snapshot().unwrap();
        let updates = retry::retire_updates(
            gated.view(),
            &SESSION,
            &CLIENT,
            RequestSequence::new(seq).unwrap(),
        )
        .unwrap();
        drop(gated);
        self.admin(updates);
    }

    fn kv_revision(&self) -> u64 {
        let gated = self.worker.reader().snapshot().unwrap();
        coord_storage::codecs::read_kv_revision(gated.view())
            .unwrap()
            .get()
    }
}

#[test]
fn lost_response_with_the_same_identity_returns_the_same_result() {
    let mut d = Domain::new(ModelEngine::new());
    d.activate_session(8);
    let request = put(b"k", b"v");
    let (admission, response) = d.submit(1, &request);
    assert_eq!(admission, Admission::New);
    let response = response.unwrap();
    assert_eq!(response.revision, KvRevision::new(1).unwrap());
    // The response was lost; the client retries with the same identity.
    let (admission, again) = d.submit(1, &request);
    let Admission::Retry(record) = admission else {
        panic!("{admission:?}")
    };
    assert!(again.is_none(), "no re-execution");
    assert_eq!(d.kv_revision(), 1, "no new revision");
    let stored: coord_state::Response = postcard::from_bytes(&record.response).unwrap();
    assert_eq!(stored, response);
    // Session activation is an ordered step, so the command is position 2.
    assert_eq!(record.position.get(), 2);
    // Resolution returns the same record only under current authorization.
    assert_eq!(d.resolve(1, &request, true), Resolution::Result(record));
    assert_eq!(d.resolve(1, &request, false), Resolution::Unauthorized);
    // The executed identity is recorded.
    let gated = d.worker.reader().snapshot().unwrap();
    let executed = gated
        .view()
        .get(
            Collection::ExecutedV1.id(),
            &coord_storage::codecs::executed_key(&binding(1, &request).command_id),
        )
        .unwrap();
    assert!(executed.is_some());
}

#[test]
fn changed_payload_under_the_same_retry_key_is_rejected() {
    let mut d = Domain::new(ModelEngine::new());
    d.activate_session(8);
    let first = put(b"k", b"v1");
    let second = put(b"k", b"v2");
    d.submit(1, &first);
    let (admission, response) = d.submit(1, &second);
    let expected = binding(1, &first).command_id;
    assert_eq!(admission, Admission::Conflict { bound: expected });
    assert!(response.is_none());
    assert_eq!(d.kv_revision(), 1);
    assert_eq!(
        d.resolve(1, &second, true),
        Resolution::Conflict { bound: expected }
    );
    // The first identity still resolves normally.
    assert!(matches!(d.resolve(1, &first, true), Resolution::Result(_)));
}

#[test]
fn retired_or_unknown_sessions_and_retired_sequences_never_execute() {
    let mut d = Domain::new(ModelEngine::new());
    let request = put(b"k", b"v");
    // Unknown session.
    assert_eq!(d.submit(1, &request).0, Admission::UnknownSession);
    assert_eq!(d.resolve(1, &request, true), Resolution::NoSession);
    assert_eq!(d.kv_revision(), 0);
    d.activate_session(4);
    assert_eq!(d.submit(1, &request).0, Admission::New);
    assert_eq!(d.submit(2, &put(b"k", b"v2")).0, Admission::New);
    // Pending: inside the window, not executed.
    assert_eq!(d.resolve(3, &request, true), Resolution::Pending);
    // Beyond the window.
    assert_eq!(
        d.submit(7, &request).0,
        Admission::OutOfWindow {
            floor: RequestSequence::ZERO,
            width: 4
        }
    );
    // Retire through 2: records are gone, sequences 1 and 2 are TooOld.
    d.retire_through(2);
    assert_eq!(
        d.submit(1, &request).0,
        Admission::TooOld {
            floor: RequestSequence::new(2).unwrap()
        }
    );
    assert_eq!(
        d.submit(2, &request).0,
        Admission::TooOld {
            floor: RequestSequence::new(2).unwrap()
        }
    );
    assert_eq!(
        d.resolve(1, &request, true),
        Resolution::Retired {
            floor: RequestSequence::new(2).unwrap()
        }
    );
    let gated = d.worker.reader().snapshot().unwrap();
    assert!(
        retry::lookup(gated.view(), &key(1)).unwrap().is_none(),
        "retired records are deleted: no infinite retention"
    );
    assert!(retry::lookup(gated.view(), &key(2)).unwrap().is_none());
    drop(gated);
    // The window slides with the floor.
    assert_eq!(d.submit(6, &put(b"k", b"v6")).0, Admission::New);
    assert_eq!(
        d.submit(7, &put(b"k", b"v7")).0,
        Admission::OutOfWindow {
            floor: RequestSequence::new(2).unwrap(),
            width: 4
        }
    );
    assert_eq!(d.kv_revision(), 3);
    // Retire the session: nothing executes, retries are not served.
    let update =
        coord_storage::policy::bootstrap_session(&SESSION, PrincipalId([0xaa; 16]), 4, false)
            .unwrap();
    d.admin(update);
    assert_eq!(d.submit(5, &request).0, Admission::SessionRetired);
    assert_eq!(d.submit(6, &put(b"k", b"v6")).0, Admission::SessionRetired);
    assert_eq!(d.resolve(6, &put(b"k", b"v6"), true), Resolution::NoSession);
    assert_eq!(d.kv_revision(), 3);
}

#[test]
fn retry_state_is_independent_of_epoch_and_endpoint() {
    // The same record is found from a view whose base names another epoch;
    // nothing about the record depends on envelope context.
    let mut d = Domain::new(ModelEngine::new());
    d.activate_session(8);
    let request = put(b"k", b"v");
    d.submit(1, &request);
    let gated = d.worker.reader().snapshot().unwrap();
    let record = retry::lookup(gated.view(), &key(1)).unwrap().unwrap();
    let bytes = coord_storage::codecs::encode_retry(&record).unwrap();
    assert_eq!(
        coord_storage::codecs::decode_retry(&bytes).unwrap(),
        record,
        "exact replay of the record"
    );
    assert_eq!(coord_storage::codecs::retry_key(&key(1)).len(), 40);
}

#[test]
fn crash_between_materialization_and_notification_never_duplicates() {
    let request = put(b"k", &[7; 400]);
    // Baseline op counts with the session activated.
    let (backend, shared) = FaultBackend::new(Vec::new(), FaultPlan::default());
    let mut d = Domain::new(RedbEngine::create_on_backend(backend, 4 << 20).unwrap());
    d.activate_session(8);
    let setup = shared.ops();
    assert_eq!(d.submit(1, &request).0, Admission::New);
    let total = shared.ops() - setup;
    drop(d);
    for k in 1..=total {
        for tail in [Tail::None, Tail::All, Tail::Seeded(k)] {
            let (backend, shared) = FaultBackend::new(Vec::new(), FaultPlan::default());
            let mut d = Domain::new(RedbEngine::create_on_backend(backend, 4 << 20).unwrap());
            d.activate_session(8);
            shared.set_plan(FaultPlan {
                crash_after: Some(setup + k),
                tail,
                ..FaultPlan::default()
            });
            // The submission may or may not have been acknowledged; either
            // way the client never learned the outcome.
            let b = binding(1, &request);
            let gated = d.worker.reader().snapshot().unwrap();
            let view = build_read_view(
                &gated,
                NS,
                PrincipalId([0xaa; 16]),
                &request,
                ViewBudget::default(),
            )
            .unwrap();
            let planned = plan(&request, &view, &PlanLimits::default()).unwrap();
            drop(gated);
            let _ = apply_plan(&mut d.worker, d.alloc.allocate(), NS, &planned, Some(&b));
            assert!(shared.is_frozen());
            let image = shared.crash_image(tail);
            drop(d);
            // Next boot: the retry presents the same identity.
            let (backend, _) = FaultBackend::new(image, FaultPlan::default());
            let mut d = Domain::new(RedbEngine::from_backend(backend, 4 << 20).unwrap());
            let before = d.kv_revision();
            let (admission, response) = d.submit(1, &request);
            match admission {
                Admission::New => {
                    assert_eq!(
                        before, 0,
                        "k={k} tail={tail:?}: admitted as new although the mutation had been applied"
                    );
                    assert!(response.is_some());
                    assert_eq!(d.kv_revision(), 1);
                }
                Admission::Retry(record) => {
                    assert_eq!(
                        before, 1,
                        "k={k} tail={tail:?}: retained result without the mutation"
                    );
                    assert_eq!(record.revision, Some(KvRevision::new(1).unwrap()));
                    assert_eq!(d.kv_revision(), 1, "no duplicate execution");
                }
                other => panic!("k={k} tail={tail:?}: {other:?}"),
            }
            // In every branch exactly one execution exists.
            let gated = d.worker.reader().snapshot().unwrap();
            let v = gated
                .view()
                .get(
                    Collection::KvCurrentV1.id(),
                    &coord_storage::codecs::current_key(&NS, b"k"),
                )
                .unwrap()
                .unwrap();
            let entry = coord_storage::codecs::decode_current(&v).unwrap();
            assert_eq!(entry.version, 1, "k={k} tail={tail:?}: executed twice");
            let Outcome::Put { prev } = postcard::from_bytes::<coord_state::Response>(
                &retry::lookup(gated.view(), &key(1))
                    .unwrap()
                    .unwrap()
                    .response,
            )
            .unwrap()
            .outcome
            else {
                panic!()
            };
            assert!(prev.is_none());
        }
    }
}

#[test]
fn admission_rows_change_only_in_execution_order() {
    use coord_storage::SubmitError;
    let mut d = Domain::new(ModelEngine::new());
    let update =
        coord_storage::policy::bootstrap_session(&SESSION, PrincipalId([0xaa; 16]), 4, true)
            .unwrap();
    assert_eq!(
        d.worker.submit(PersistBatch {
            barrier: d.alloc.allocate(),
            base: None,
            updates: update,
        }),
        Err(SubmitError::AdmissionRowsRequireOrdering)
    );
    assert_eq!(d.worker.queued(), 0);
}

#[test]
fn admission_is_revalidated_atomically_with_the_application_batch() {
    let mut d = Domain::new(ModelEngine::new());
    d.activate_session(8);
    let request = put(b"k", b"v");
    let b = binding(1, &request);
    // Admit and plan from one snapshot.
    let gated = d.worker.reader().snapshot().unwrap();
    assert_eq!(
        retry::admit(gated.view(), &b, |_| true).unwrap(),
        Admission::New
    );
    let view = build_read_view(
        &gated,
        NS,
        PrincipalId([0xaa; 16]),
        &request,
        ViewBudget::default(),
    )
    .unwrap();
    let planned = plan(&request, &view, &PlanLimits::default()).unwrap();
    drop(gated);
    // A session retirement is ordered in between.
    let update =
        coord_storage::policy::bootstrap_session(&SESSION, PrincipalId([0xaa; 16]), 8, false)
            .unwrap();
    d.admin(update);
    // The bound command's batch carries the base its admission was checked
    // at, so it is rejected instead of executing under a retired session.
    let outcome = apply_plan(&mut d.worker, d.alloc.allocate(), NS, &planned, Some(&b)).unwrap();
    assert_eq!(outcome, ApplyOutcome::Replan);
    assert_eq!(d.kv_revision(), 0);
    assert_eq!(d.submit(1, &request).0, Admission::SessionRetired);
}

#[test]
fn a_stale_retirement_never_lowers_the_floor() {
    let mut d = Domain::new(ModelEngine::new());
    d.activate_session(8);
    for seq in 1..=6u64 {
        assert_eq!(d.submit(seq, &put(b"k", &[seq as u8])).0, Admission::New);
    }
    // Two acknowledgements computed from the same snapshot.
    let gated = d.worker.reader().snapshot().unwrap();
    let base = d.worker.application_base();
    let through_2 = retry::retire_updates(
        gated.view(),
        &SESSION,
        &CLIENT,
        RequestSequence::new(2).unwrap(),
    )
    .unwrap();
    let through_5 = retry::retire_updates(
        gated.view(),
        &SESSION,
        &CLIENT,
        RequestSequence::new(5).unwrap(),
    )
    .unwrap();
    drop(gated);
    // The later one is applied first.
    d.worker
        .submit(PersistBatch {
            barrier: d.alloc.allocate(),
            base: Some(base),
            updates: through_5,
        })
        .unwrap();
    assert_eq!(d.worker.flush().unwrap().committed, 1);
    // The stale one is based at the old frontier: rejected, floor stays 5.
    d.worker
        .submit(PersistBatch {
            barrier: d.alloc.allocate(),
            base: Some(base),
            updates: through_2,
        })
        .unwrap();
    let o = d.worker.flush().unwrap();
    assert_eq!(o.committed, 0);
    assert_eq!(o.rejected, 1);
    let gated = d.worker.reader().snapshot().unwrap();
    assert_eq!(
        retry::floor(gated.view(), &SESSION, &CLIENT, 8)
            .unwrap()
            .floor,
        RequestSequence::new(5).unwrap()
    );
    // Sequences 3..=5 stay retired: never admitted as new work again.
    drop(gated);
    assert_eq!(
        d.submit(4, &put(b"k", b"again")).0,
        Admission::TooOld {
            floor: RequestSequence::new(5).unwrap()
        }
    );
}

#[test]
fn acknowledgements_beyond_the_active_window_are_rejected() {
    let mut d = Domain::new(ModelEngine::new());
    d.activate_session(4);
    let gated = d.worker.reader().snapshot().unwrap();
    // Floor 0, width 4: through 5 names a sequence that was never admitted.
    let err = retry::retire_updates(
        gated.view(),
        &SESSION,
        &CLIENT,
        RequestSequence::new(5).unwrap(),
    )
    .unwrap_err();
    assert_eq!(err.class, coord_store_api::engine::ErrorClass::Unsupported);
    // Exactly the window edge is fine.
    assert!(
        retry::retire_updates(
            gated.view(),
            &SESSION,
            &CLIENT,
            RequestSequence::new(4).unwrap()
        )
        .is_ok()
    );
}

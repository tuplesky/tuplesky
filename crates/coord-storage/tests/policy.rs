//! task-18 at the storage layer: authorized views load session, trust rule
//! and rules at the same snapshot as the entries on redb; denial and
//! revocation are ordered at execution; a retired session, or one whose
//! trust rule was disabled, cannot read protected cached retry outcomes;
//! sessions and policy advance execution without a KV revision.

use coord_core::effect::BootId;
use coord_core::outbox::BarrierAllocator;
use coord_redb_faultkit::{FaultBackend, FaultPlan};
use coord_state::policy::{Action, AdmissionReceiptV1, KeyInterval, PolicyRule, TrustRule};
use coord_state::{InternalCommand, Outcome, PlanLimits, authorize_retained, plan, plan_internal};
use coord_storage::retry::{self, Admission, Resolution, RetryBinding};
use coord_storage::{
    ApplyOutcome, GroupLimits, StoreWorker, ViewBudget, apply_plan, build_authorized_view,
    build_internal_view, load_authorization,
};
use coord_storage_redb::RedbEngine;
use coord_store_api::engine::LocalEngine;
use coord_types::identity::Digest32;
use coord_types::ids::*;
use coord_types::logical_v1::*;
use coord_types::{CommandId, RetryKey};

const NS: NamespaceId = NamespaceId([0x11; 16]);
const ALICE: PrincipalId = PrincipalId([0xa; 16]);
const S1: SessionId = SessionId([0x51; 16]);
const S2: SessionId = SessionId([0x52; 16]);
const CLIENT: ClientInstanceId = ClientInstanceId([0xc1; 16]);
const RULE: TrustRuleId = TrustRuleId([0x71; 16]);

fn inc() -> ReplicaIncarnation {
    ReplicaIncarnation::new(1).unwrap()
}

fn req(op: CanonicalOperation) -> LogicalRequest {
    let mut r = LogicalRequest::new(NS, op);
    r.canonicalize();
    r
}

fn put(k: &[u8], v: &[u8]) -> LogicalRequest {
    req(CanonicalOperation::Put(PutOp {
        key: k.to_vec(),
        value: v.to_vec(),
        lease: None,
        prev_kv: false,
    }))
}

fn retry_key(session: SessionId, seq: u64) -> RetryKey {
    RetryKey {
        cluster_id: ClusterId([1; 16]),
        domain_id: DomainId([2; 16]),
        session_id: session,
        client_instance_id: CLIENT,
        request_sequence: RequestSequence::new(seq).unwrap(),
    }
}

fn binding(session: SessionId, seq: u64, request: &LogicalRequest) -> RetryBinding {
    RetryBinding {
        retires: None,
        retry_key: retry_key(session, seq),
        command_id: CommandId::derive(&retry_key(session, seq), request).unwrap(),
    }
}

fn receipt(id: u8, session: SessionId) -> AdmissionReceiptV1 {
    AdmissionReceiptV1 {
        receipt_id: Digest32([id; 32]),
        session,
        principal: ALICE,
        scope_ceiling: Action::FULL_CEILING,
        trust_rule: RULE,
        rule_generation: 1,
        expires_at: u64::MAX,
    }
}

fn admit(id: u8, session: SessionId) -> InternalCommand {
    InternalCommand::ConsumeAdmission {
        namespace: NS,
        receipt: receipt(id, session),
        code: None,
        refresh_family: None,
        window: 8,
    }
}

fn trust(enabled: bool) -> InternalCommand {
    InternalCommand::PutTrustRule {
        namespace: NS,
        rule: RULE,
        record: TrustRule {
            enabled,
            generation: 1,
        },
    }
}

fn allow_write(id: u8, lo: &[u8], hi: &[u8]) -> InternalCommand {
    InternalCommand::PutPolicyRule {
        namespace: NS,
        principal: ALICE,
        rule: PolicyRuleId([id; 16]),
        record: Some(PolicyRule {
            principal: ALICE,
            action: Action::Write,
            namespace: NS,
            interval: KeyInterval {
                lower: lo.to_vec(),
                upper: Some(hi.to_vec()),
            },
        }),
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

    fn run_internal(&mut self, command: &InternalCommand) -> coord_state::Response {
        let gated = self.worker.reader().snapshot().unwrap();
        let view = build_internal_view(&gated, command, ViewBudget::default()).unwrap();
        let planned = plan_internal(command, &view, &PlanLimits::default()).unwrap();
        drop(gated);
        match apply_plan(&mut self.worker, self.alloc.allocate(), NS, &planned, None).unwrap() {
            ApplyOutcome::Applied(_) => planned.response,
            other => panic!("{other:?}"),
        }
    }

    /// A client request under `session` with retry identity `seq`.
    fn run(
        &mut self,
        session: SessionId,
        seq: u64,
        request: &LogicalRequest,
    ) -> (Admission, Option<coord_state::Response>) {
        let b = binding(session, seq, request);
        let gated = self.worker.reader().snapshot().unwrap();
        // A retained result is handed out only when the request would be
        // authorized under the policy in force now.
        let admission = retry::admit(gated.view(), &b, |record| {
            let auth =
                load_authorization(gated.view(), NS, &session, ViewBudget::default()).unwrap();
            let stored: coord_state::Response = postcard::from_bytes(&record.response).unwrap();
            authorize_retained(&auth, &NS, request, &stored.outcome).is_ok()
        })
        .unwrap();
        if admission != Admission::New {
            return (admission, None);
        }
        let view =
            build_authorized_view(&gated, NS, &session, request, ViewBudget::default()).unwrap();
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
            other => panic!("{other:?}"),
        }
    }

    fn resolve(&self, session: SessionId, seq: u64, request: &LogicalRequest) -> Resolution {
        let gated = self.worker.reader().snapshot().unwrap();
        let view = gated.view();
        retry::resolve(view, &binding(session, seq, request), |_| {
            retry::session_executable(view, &session).unwrap()
        })
        .unwrap()
    }

    fn position(&self) -> u64 {
        self.worker.application_base().execution_position.get()
    }

    fn kv_revision(&self) -> u64 {
        let gated = self.worker.reader().snapshot().unwrap();
        coord_storage::codecs::read_kv_revision(gated.view())
            .unwrap()
            .get()
    }
}

#[test]
fn ordered_policy_and_revocation_on_redb_and_revoked_sessions_cannot_read_results() {
    let (backend, _) = FaultBackend::new(Vec::new(), FaultPlan::default());
    let mut d = Domain::new(RedbEngine::create_on_backend(backend, 4 << 20).unwrap());
    // Unknown session: denied before anything else, still one ordered
    // execution position (the denial is a recorded outcome).
    let request = put(b"a", b"1");
    assert_eq!(d.run(S1, 1, &request).0, Admission::UnknownSession);
    // Trust rule, session and a write rule on [a, c): none takes a revision.
    d.run_internal(&trust(true));
    let r = d.run_internal(&admit(1, S1));
    assert_eq!(r.outcome, Outcome::SessionCreated { session: S1 });
    d.run_internal(&allow_write(1, b"a", b"c"));
    assert_eq!(d.kv_revision(), 0);
    assert_eq!(d.position(), 3);
    // Permitted write executes; a key outside the rule is denied at its
    // execution position with the denial retained for retries.
    let (a, r) = d.run(S1, 1, &request);
    assert_eq!(a, Admission::New);
    assert_eq!(r.unwrap().outcome, Outcome::Put { prev: None });
    let outside = put(b"z", b"1");
    let (_, r) = d.run(S1, 2, &outside);
    assert_eq!(r.unwrap().outcome, Outcome::ErrPermissionDenied);
    assert_eq!(d.kv_revision(), 1);
    let (a, _) = d.run(S1, 2, &outside);
    let Admission::Retry(record) = a else {
        panic!("{a:?}")
    };
    let stored: coord_state::Response = postcard::from_bytes(&record.response).unwrap();
    assert_eq!(stored.outcome, Outcome::ErrPermissionDenied);
    // The retained result of sequence 1 is readable while the session is
    // executable...
    assert!(matches!(d.resolve(S1, 1, &request), Resolution::Result(_)));
    // ...and protected once the session is retired: neither admission nor
    // resolution hands it out.
    assert_eq!(
        d.run_internal(&InternalCommand::RetireSession {
            namespace: NS,
            session: S1
        })
        .outcome,
        Outcome::SessionRetired
    );
    assert_eq!(d.resolve(S1, 1, &request), Resolution::NoSession);
    assert_eq!(d.run(S1, 1, &request).0, Admission::SessionRetired);
    assert_eq!(d.run(S1, 3, &put(b"b", b"1")).0, Admission::SessionRetired);
    // A second session under the same rule works until the rule is
    // disabled; disabling it is ordered revocation for every session it
    // admitted, including cached results.
    d.run_internal(&admit(2, S2));
    let (_, r) = d.run(S2, 1, &put(b"b", b"2"));
    assert_eq!(r.unwrap().outcome, Outcome::Put { prev: None });
    assert!(matches!(
        d.resolve(S2, 1, &put(b"b", b"2")),
        Resolution::Result(_)
    ));
    d.run_internal(&trust(false));
    assert_eq!(d.resolve(S2, 1, &put(b"b", b"2")), Resolution::NoSession);
    assert_eq!(d.run(S2, 2, &put(b"b", b"3")).0, Admission::SessionRetired);
    assert_eq!(d.kv_revision(), 2);
    // Re-enabling restores S2 (never S1, which was retired).
    d.run_internal(&trust(true));
    assert!(matches!(
        d.resolve(S2, 1, &put(b"b", b"2")),
        Resolution::Result(_)
    ));
    assert_eq!(d.run(S1, 4, &put(b"b", b"3")).0, Admission::SessionRetired);
    // Removing the rule denies the next execution of an already admitted
    // request shape; nothing about policy changed KV.
    d.run_internal(&InternalCommand::PutPolicyRule {
        namespace: NS,
        principal: ALICE,
        rule: PolicyRuleId([1; 16]),
        record: None,
    });
    let (_, r) = d.run(S2, 2, &put(b"b", b"3"));
    assert_eq!(r.unwrap().outcome, Outcome::ErrPermissionDenied);
    assert_eq!(d.kv_revision(), 2);
}

#[test]
fn a_lost_permission_protects_the_retained_result_of_an_executed_request() {
    let (backend, _) = FaultBackend::new(Vec::new(), FaultPlan::default());
    let mut d = Domain::new(RedbEngine::create_on_backend(backend, 4 << 20).unwrap());
    d.run_internal(&trust(true));
    d.run_internal(&admit(1, S1));
    d.run_internal(&allow_write(1, b"a", b"c"));
    d.run_internal(&InternalCommand::PutPolicyRule {
        namespace: NS,
        principal: ALICE,
        rule: PolicyRuleId([2; 16]),
        record: Some(PolicyRule {
            principal: ALICE,
            action: Action::Read,
            namespace: NS,
            interval: KeyInterval {
                lower: b"a".to_vec(),
                upper: Some(b"c".to_vec()),
            },
        }),
    });
    d.run(S1, 1, &put(b"a", b"secret"));
    let read = req(CanonicalOperation::Range(RangeOp {
        range: KeyRange::exact(b"a".to_vec()),
        revision: None,
        limit: 0,
        keys_only: false,
        count_only: false,
    }));
    let (a, r) = d.run(S1, 2, &read);
    assert_eq!(a, Admission::New);
    assert!(matches!(r.unwrap().outcome, Outcome::Range { .. }));
    // Retrying while the read rule holds returns the retained result...
    assert!(matches!(d.run(S1, 2, &read).0, Admission::Retry(_)));
    // ...and no longer once the rule is removed, although the session is
    // still executable: the identity neither re-executes nor replays.
    d.run_internal(&InternalCommand::PutPolicyRule {
        namespace: NS,
        principal: ALICE,
        rule: PolicyRuleId([2; 16]),
        record: None,
    });
    assert_eq!(d.run(S1, 2, &read).0, Admission::Unauthorized);
    assert_eq!(d.position(), 7, "nothing executed for the refused retry");
    // The retained write result (no value returned) is still replayed.
    assert!(matches!(
        d.run(S1, 1, &put(b"a", b"secret")).0,
        Admission::Retry(_)
    ));
}

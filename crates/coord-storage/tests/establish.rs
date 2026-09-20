//! Acceptance for session establishment as a replicated command
//! (task-j09; design Sections 9.3, 12, 22.1).
//!
//! The claim under test is narrow and the whole point of the task: the
//! trusted boundary attests *who was authenticated*, and the replicated
//! state machine decides *what session exists*. So these drive the real
//! applying path -- `Applier::apply` over a durable payload record
//! carrying the admission -- and check that
//!
//! * the payload cannot select the principal, the rule, the ceiling or
//!   the deadline, and an establishing admission authorizes exactly one
//!   action;
//! * a second delivery of the same command recovers what the first
//!   established rather than creating a second session, and no other
//!   command can reuse a consumed receipt;
//! * current replicated policy decides: a trust rule revoked at an
//!   earlier position prevents creation, a retired session is never
//!   resurrected, and an accepted command executes the same way long
//!   after the credential that admitted it expired, with no clock read
//!   anywhere in application.

use coord_consensus::PayloadRecordV1;
use coord_core::capability::{
    AdmissionFacts, AttestedAdmission, AttestedEstablishment, CredentialDeadline,
};
use coord_core::effect::PersistBatch;
use coord_core::outbox::BarrierAllocator;
use coord_state::policy::{Action, TrustRule};
use coord_state::{InternalCommand, Outcome, PlanLimits, RejectionReason, Response, plan_internal};
use coord_storage::policy::bootstrap_trust_rule;
use coord_storage::{
    Applier, ApplyOutcome, GroupLimits, StoreWorker, ViewBudget, apply_plan, build_internal_view,
};
use coord_store_testkit::model::ModelEngine;
use coord_types::identity::Digest32;
use coord_types::ids::*;
use coord_types::logical_v1::*;
use coord_types::{CommandId, RetryKey};

const NS: NamespaceId = NamespaceId([0x5e; 16]);
const CLUSTER: ClusterId = ClusterId([1; 16]);
const DOMAIN: DomainId = DomainId([2; 16]);
const ALICE: PrincipalId = PrincipalId([0xa; 16]);
const MALLORY: PrincipalId = PrincipalId([0xbd; 16]);
const RULE: TrustRuleId = TrustRuleId([0x7c; 16]);
const SESSION: SessionId = SessionId([0x44; 16]);

fn inc() -> ReplicaIncarnation {
    ReplicaIncarnation::new(1).unwrap()
}

/// The invocation identity of an establishment, as the binding boundary
/// derives it: the session it creates, the credential's own identifier
/// as the client instance, sequence one.
fn establishment_key(session: SessionId, receipt: Digest32) -> RetryKey {
    let mut instance = [0u8; 16];
    instance.copy_from_slice(&receipt.0[..16]);
    RetryKey {
        cluster_id: CLUSTER,
        domain_id: DOMAIN,
        session_id: session,
        client_instance_id: ClientInstanceId(instance),
        request_sequence: RequestSequence::new(1).unwrap(),
    }
}

fn facts(session: SessionId, receipt: u8, principal: PrincipalId, until: u64) -> AdmissionFacts {
    AdmissionFacts {
        attested: AttestedAdmission {
            cluster: CLUSTER,
            domain: DOMAIN,
            session,
            rule_generation: 1,
            scope_ceiling: Action::FULL_CEILING,
            receipt_id: Digest32([receipt; 32]),
            admitted_at_ticks: 17,
        },
        establishing: Some(AttestedEstablishment {
            principal,
            trust_rule: RULE,
            credential_valid_until: CredentialDeadline(until),
        }),
    }
}

fn submitting(session: SessionId, receipt: u8) -> AdmissionFacts {
    AdmissionFacts {
        attested: AttestedAdmission {
            cluster: CLUSTER,
            domain: DOMAIN,
            session,
            rule_generation: 1,
            scope_ceiling: Action::FULL_CEILING,
            receipt_id: Digest32([receipt; 32]),
            admitted_at_ticks: 17,
        },
        establishing: None,
    }
}

fn request(op: CanonicalOperation) -> LogicalRequest {
    let mut r = LogicalRequest::new(NS, op);
    r.canonicalize();
    r
}

/// The durable record of a command: its invocation, its canonical
/// request and the admission every voter accepted it under.
fn accepted(
    key: RetryKey,
    request: &LogicalRequest,
    admission: Option<AdmissionFacts>,
) -> (CommandId, PayloadRecordV1) {
    (
        CommandId::derive(&key, request).unwrap(),
        PayloadRecordV1 {
            retry_key: key,
            logical: postcard::to_allocvec(request).unwrap(),
            admission,
        },
    )
}

/// An applier over a domain whose genesis trusts `RULE` and holds no
/// session at all: every session these tests see is one a command wrote.
fn domain() -> Applier<StoreWorker<ModelEngine>> {
    let boot = coord_core::effect::BootId([1; 16]);
    let mut worker =
        StoreWorker::open(ModelEngine::new(), boot, inc(), GroupLimits::default()).unwrap();
    let mut alloc = BarrierAllocator::new(inc(), boot);
    worker
        .submit(PersistBatch {
            barrier: alloc.allocate(),
            base: Some(worker.application_base()),
            updates: vec![bootstrap_trust_rule(&RULE).unwrap()],
        })
        .unwrap();
    worker.flush().unwrap();
    Applier::new(worker, alloc).unwrap()
}

/// Apply an internal command directly, for the *ordered administration*
/// these tests need before an establishment (revoking a trust rule,
/// retiring a session). It stands in for the administrative path, which
/// is its own task; what is under test here is what the establishment
/// does against the state it finds.
fn administer(applier: &mut Applier<StoreWorker<ModelEngine>>, command: &InternalCommand) {
    let planned = {
        let gated = applier.store().reader().snapshot().unwrap();
        let view = build_internal_view(&gated, command, ViewBudget::SCHEMA).unwrap();
        plan_internal(command, &view, &PlanLimits::default()).unwrap()
    };
    let barrier = applier.alloc().allocate();
    match apply_plan(applier.store_mut(), barrier, NS, &planned, None).unwrap() {
        ApplyOutcome::Applied(_) => {}
        other => panic!("{other:?}"),
    }
}

fn outcome(response: &[u8]) -> Outcome {
    postcard::from_bytes::<Response>(response).unwrap().outcome
}

/// The one action an establishing admission authorizes, and what it
/// creates: the verifier's facts, not the payload's.
#[test]
fn an_establishing_admission_creates_the_session_its_verifier_attested() {
    let mut applier = domain();
    let key = establishment_key(SESSION, Digest32([9; 32]));
    let (command, payload) = accepted(
        key,
        &request(CanonicalOperation::ConsumeAdmission),
        Some(facts(SESSION, 9, ALICE, 1_000)),
    );
    let applied = applier.apply(command, &payload).unwrap();
    assert_eq!(
        outcome(&applied.response),
        Outcome::SessionCreated { session: SESSION }
    );
    assert!(
        applied.revision.is_none(),
        "creating a session is not a KV revision"
    );
}

/// The payload cannot select another principal, rule, ceiling or
/// deadline -- because it cannot carry one.
///
/// `ConsumeAdmission` is an empty variant: there is no field for a
/// caller to put a principal in. What the session gets comes from the
/// admission record, so two commands whose payloads are byte-identical
/// create sessions of whichever principals their *verifiers* attested.
#[test]
fn the_session_is_the_admissions_and_never_the_payloads() {
    let other = SessionId([0x45; 16]);
    let mut applier = domain();
    let alice = establishment_key(SESSION, Digest32([9; 32]));
    let mallory = establishment_key(other, Digest32([8; 32]));
    let operation = request(CanonicalOperation::ConsumeAdmission);

    let (c1, p1) = accepted(alice, &operation, Some(facts(SESSION, 9, ALICE, 1_000)));
    let (c2, p2) = accepted(mallory, &operation, Some(facts(other, 8, MALLORY, 1_000)));
    assert_eq!(p1.logical, p2.logical, "the same payload, byte for byte");
    assert_ne!(c1, c2, "different invocations are different commands");

    applier.apply(c1, &p1).unwrap();
    applier.apply(c2, &p2).unwrap();
    let gated = applier.store().reader().snapshot().unwrap();
    let auth = coord_storage::load_authorization(gated.view(), NS, &SESSION, ViewBudget::default())
        .unwrap();
    assert_eq!(auth.session.unwrap().principal, ALICE);
    let auth =
        coord_storage::load_authorization(gated.view(), NS, &other, ViewBudget::default()).unwrap();
    assert_eq!(auth.session.unwrap().principal, MALLORY);
}

/// An establishing admission authorizes exactly one action, and that
/// action exists for nothing else.
#[test]
fn an_establishing_admission_authorizes_one_action_and_nothing_else_authorizes_it() {
    let mut applier = domain();
    let key = establishment_key(SESSION, Digest32([9; 32]));

    // An establishing receipt presented with an ordinary operation.
    let (command, payload) = accepted(
        key,
        &request(CanonicalOperation::Put(PutOp {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            lease: None,
            prev_kv: false,
        })),
        Some(facts(SESSION, 9, ALICE, 1_000)),
    );
    assert_eq!(
        outcome(&applier.apply(command, &payload).unwrap().response),
        Outcome::ErrRejected {
            reason: RejectionReason::AdmissionMismatch
        },
        "an establishing admission is not authority for ordinary work"
    );

    // The establishing operation under a submission receipt: the
    // strictly narrower authority does not reach the wider action.
    let (command, payload) = accepted(
        establishment_key(SESSION, Digest32([7; 32])),
        &request(CanonicalOperation::ConsumeAdmission),
        Some(submitting(SESSION, 7)),
    );
    assert_eq!(
        outcome(&applier.apply(command, &payload).unwrap().response),
        Outcome::ErrRejected {
            reason: RejectionReason::AdmissionMismatch
        },
        "being admitted under a session is not authority to originate one"
    );

    // And with no admission at all.
    let (command, payload) = accepted(
        establishment_key(SESSION, Digest32([6; 32])),
        &request(CanonicalOperation::ConsumeAdmission),
        None,
    );
    assert_eq!(
        outcome(&applier.apply(command, &payload).unwrap().response),
        Outcome::ErrRejected {
            reason: RejectionReason::AdmissionMismatch
        }
    );

    // Nothing was created by any of them.
    let gated = applier.store().reader().snapshot().unwrap();
    let auth = coord_storage::load_authorization(gated.view(), NS, &SESSION, ViewBudget::default())
        .unwrap();
    assert!(
        auth.session.is_none(),
        "a refused admission created nothing"
    );
}

/// The receipt and the retry key must name one session.
///
/// Otherwise a command would establish one identity while recording its
/// outcome under another: a retry of it would find no record, and a
/// second establishment of the same session would follow.
#[test]
fn a_receipt_cannot_establish_a_session_the_invocation_does_not_name() {
    let mut applier = domain();
    let key = establishment_key(SessionId([0x45; 16]), Digest32([9; 32]));
    let (command, payload) = accepted(
        key,
        &request(CanonicalOperation::ConsumeAdmission),
        Some(facts(SESSION, 9, ALICE, 1_000)),
    );
    assert_eq!(
        outcome(&applier.apply(command, &payload).unwrap().response),
        Outcome::ErrRejected {
            reason: RejectionReason::AdmissionMismatch
        }
    );
}

/// A second delivery of the same command recovers what the first
/// established; it does not create a second session.
#[test]
fn a_duplicate_delivery_recovers_the_established_outcome() {
    let mut applier = domain();
    let key = establishment_key(SESSION, Digest32([9; 32]));
    let (command, payload) = accepted(
        key,
        &request(CanonicalOperation::ConsumeAdmission),
        Some(facts(SESSION, 9, ALICE, 1_000)),
    );
    let first = applier.apply(command, &payload).unwrap();
    let again = applier.apply(command, &payload).unwrap();
    assert_eq!(first.position, again.position, "one position, one command");
    assert_eq!(first.result_digest, again.result_digest);
    assert_eq!(
        outcome(&again.response),
        Outcome::SessionCreated { session: SESSION },
        "the retry recovers the outcome rather than re-deciding it"
    );
}

/// Another command cannot reuse a consumed receipt, and no second
/// session of the same identity is ever created.
#[test]
fn a_consumed_receipt_establishes_nothing_a_second_time() {
    let mut applier = domain();
    let operation = request(CanonicalOperation::ConsumeAdmission);
    let (command, payload) = accepted(
        establishment_key(SESSION, Digest32([9; 32])),
        &operation,
        Some(facts(SESSION, 9, ALICE, 1_000)),
    );
    applier.apply(command, &payload).unwrap();

    // The same receipt under another invocation: a different command,
    // so nothing recovers it from a retry record.
    let other = RetryKey {
        request_sequence: RequestSequence::new(2).unwrap(),
        ..establishment_key(SESSION, Digest32([9; 32]))
    };
    let (command, payload) = accepted(other, &operation, Some(facts(SESSION, 9, ALICE, 1_000)));
    assert_eq!(
        outcome(&applier.apply(command, &payload).unwrap().response),
        Outcome::ErrReceiptConsumed
    );

    // A fresh receipt for a session that now exists is refused too: the
    // row is never replaced, whatever the new credential says.
    let (command, payload) = accepted(
        establishment_key(SESSION, Digest32([5; 32])),
        &operation,
        Some(facts(SESSION, 5, MALLORY, 9_000)),
    );
    assert_eq!(
        outcome(&applier.apply(command, &payload).unwrap().response),
        Outcome::ErrReceiptConsumed
    );
    let gated = applier.store().reader().snapshot().unwrap();
    let auth = coord_storage::load_authorization(gated.view(), NS, &SESSION, ViewBudget::default())
        .unwrap();
    assert_eq!(
        auth.session.unwrap().principal,
        ALICE,
        "a disagreeing receipt did not overwrite the session"
    );
}

/// A trust rule revoked at an earlier position prevents creation.
///
/// The verifier attested the rule and its generation; what decides is
/// the rule replicated policy holds *at this command's position*.
#[test]
fn a_rule_revoked_before_the_admission_is_consumed_creates_nothing() {
    let mut applier = domain();
    administer(
        &mut applier,
        &InternalCommand::PutTrustRule {
            namespace: NS,
            rule: RULE,
            record: TrustRule {
                enabled: false,
                generation: 1,
            },
        },
    );
    let (command, payload) = accepted(
        establishment_key(SESSION, Digest32([9; 32])),
        &request(CanonicalOperation::ConsumeAdmission),
        Some(facts(SESSION, 9, ALICE, 1_000)),
    );
    assert_eq!(
        outcome(&applier.apply(command, &payload).unwrap().response),
        Outcome::ErrTrustRuleInvalid
    );

    // The same holds for a rule regenerated past the attested
    // generation: the admission is about a rule that no longer exists in
    // that form.
    administer(
        &mut applier,
        &InternalCommand::PutTrustRule {
            namespace: NS,
            rule: RULE,
            record: TrustRule {
                enabled: true,
                generation: 2,
            },
        },
    );
    let (command, payload) = accepted(
        establishment_key(SESSION, Digest32([8; 32])),
        &request(CanonicalOperation::ConsumeAdmission),
        Some(facts(SESSION, 8, ALICE, 1_000)),
    );
    assert_eq!(
        outcome(&applier.apply(command, &payload).unwrap().response),
        Outcome::ErrTrustRuleInvalid
    );
}

/// A retired session is not resurrected by a later admission.
#[test]
fn a_retired_session_is_never_established_again() {
    let mut applier = domain();
    let (command, payload) = accepted(
        establishment_key(SESSION, Digest32([9; 32])),
        &request(CanonicalOperation::ConsumeAdmission),
        Some(facts(SESSION, 9, ALICE, 1_000)),
    );
    applier.apply(command, &payload).unwrap();
    administer(
        &mut applier,
        &InternalCommand::RetireSession {
            namespace: NS,
            session: SESSION,
        },
    );
    let (command, payload) = accepted(
        establishment_key(SESSION, Digest32([4; 32])),
        &request(CanonicalOperation::ConsumeAdmission),
        Some(facts(SESSION, 4, ALICE, 9_000)),
    );
    assert_eq!(
        outcome(&applier.apply(command, &payload).unwrap().response),
        Outcome::ErrReceiptConsumed,
        "retirement is permanent: the identity is spent"
    );
    let gated = applier.store().reader().snapshot().unwrap();
    let auth = coord_storage::load_authorization(gated.view(), NS, &SESSION, ViewBudget::default())
        .unwrap();
    assert!(
        !auth.session.unwrap().active,
        "the session is still retired"
    );
}

/// An accepted command executes the same way long after the credential
/// that admitted it expired.
///
/// Expiry is an admission constraint checked once, at the
/// authentication boundary, under that boundary's clock assumptions. No
/// replica consults a clock while applying, and recovery does not
/// revalidate an already accepted command against today's: a command
/// admitted before the deadline may finish after it, and what denies
/// execution later is an ordered retirement or revocation at its own
/// position, not the passage of time.
///
/// So the same accepted command, whose credential deadline is in the
/// distant past, still creates its session -- and the deadline is
/// carried into the row it writes rather than compared with anything.
#[test]
fn an_accepted_command_needs_no_issuer_and_no_clock() {
    let mut applier = domain();
    let expired = 1;
    let (command, payload) = accepted(
        establishment_key(SESSION, Digest32([9; 32])),
        &request(CanonicalOperation::ConsumeAdmission),
        Some(facts(SESSION, 9, ALICE, expired)),
    );
    assert_eq!(
        outcome(&applier.apply(command, &payload).unwrap().response),
        Outcome::SessionCreated { session: SESSION }
    );
    let gated = applier.store().reader().snapshot().unwrap();
    let record =
        coord_storage::load_authorization(gated.view(), NS, &SESSION, ViewBudget::default())
            .unwrap()
            .session
            .unwrap();
    assert_eq!(
        record.expires_at, expired,
        "the credential's own deadline is recorded, not reinterpreted"
    );
    assert!(record.active, "no replica denied it from a clock");
}

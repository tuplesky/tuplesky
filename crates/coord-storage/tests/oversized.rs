//! A read whose view overruns the schema's byte budget is refused as a
//! replicated result (`ViewTooLarge`), at the position it was chosen for,
//! and the commands behind it execute.
//!
//! Each page of the view's scan used to ask the engine for no more than
//! the bytes the budget had left. A row larger than that came back as the
//! engine's refusal of an oversized first row (`ErrorClass::Limit`), which
//! `apply_bound` returned as `ApplyError::View(ViewBuildError::Engine(..))`:
//! the voter stopped on it, every voter stopped on the same command, and
//! replay reached it again. A value of a MiB is a row a little over a MiB,
//! so fifteen fit the 16 MiB budget and one read of sixteen stopped the
//! domain. The tests read seventeen through an interval and as exact keys
//! in one transaction, on one projection and on a journal with its
//! projection.

mod support;

use coord_consensus::PayloadRecordV1;
use coord_core::effect::{BootId, PersistBatch};
use coord_core::outbox::BarrierAllocator;
use coord_journal_api::stream::ShardId;
use coord_state::policy::{Action, KeyInterval, PolicyRule};
use coord_state::{Outcome, RejectionReason, Response};
use coord_storage::journaled::{JournalLimits, JournaledStore, TransitionKind};
use coord_storage::policy::{bootstrap_session, rule_update};
use coord_storage::{Applier, GroupLimits, JournaledDomain, Persistence, StoreWorker};
use coord_store_testkit::journal::ModelJournal;
use coord_store_testkit::model::ModelEngine;
use coord_types::identity::Digest32;
use coord_types::ids::*;
use coord_types::logical_v1::limits::MAX_VALUE_BYTES;
use coord_types::logical_v1::*;
use coord_types::{CommandId, RetryKey};
use support::Staged;

const NS: NamespaceId = NamespaceId([5; 16]);
const SESSION: SessionId = SessionId([3; 16]);
const CLIENT: ClientInstanceId = ClientInstanceId([4; 16]);
const ALICE: PrincipalId = PrincipalId([0xa; 16]);
const CLUSTER: ClusterId = ClusterId([1; 16]);
const DOMAIN: DomainId = DomainId([2; 16]);
const REPLICA: ReplicaId = ReplicaId([7; 16]);
const BOOT: BootId = BootId([1; 16]);

/// One more key of a MiB than the view's 16 MiB budget holds.
const KEYS: u8 = 17;

fn inc() -> ReplicaIncarnation {
    ReplicaIncarnation::new(1).unwrap()
}

fn command(seq: u64, op: CanonicalOperation) -> (CommandId, PayloadRecordV1) {
    let key = RetryKey {
        cluster_id: CLUSTER,
        domain_id: DOMAIN,
        session_id: SESSION,
        client_instance_id: CLIENT,
        request_sequence: RequestSequence::new(seq).unwrap(),
    };
    let mut request = LogicalRequest::new(NS, op);
    request.canonicalize();
    (
        CommandId::derive(&key, &request).unwrap(),
        support::payload_of(key, &request),
    )
}

fn big_key(i: u8) -> Vec<u8> {
    format!("big/{i:02}").into_bytes()
}

fn read(range: KeyRange) -> RangeOp {
    RangeOp {
        range,
        revision: None,
        limit: 1,
        count_only: false,
        keys_only: false,
    }
}

fn bootstrap() -> Vec<coord_core::effect::StoreUpdate> {
    let mut updates = bootstrap_session(&SESSION, ALICE, 1 << 20, true).unwrap();
    for (i, action) in Action::ALL.iter().enumerate() {
        updates.push(
            rule_update(
                &PolicyRuleId([i as u8 + 1; 16]),
                &PolicyRule {
                    principal: ALICE,
                    action: *action,
                    namespace: NS,
                    interval: KeyInterval::all(),
                },
            )
            .unwrap(),
        );
    }
    updates
}

/// One projection; each command its own batch.
fn worker() -> Applier<StoreWorker<ModelEngine>> {
    let mut worker =
        StoreWorker::open(ModelEngine::new(), BOOT, inc(), GroupLimits::default()).unwrap();
    let mut alloc = BarrierAllocator::new(inc(), BOOT);
    worker
        .submit(PersistBatch {
            barrier: alloc.allocate(),
            base: Some(worker.application_base()),
            updates: bootstrap(),
        })
        .unwrap();
    worker.flush().unwrap();
    Applier::new(worker, alloc).unwrap()
}

/// One journal and its projection, as a node runs them.
fn journaled() -> Applier<JournaledDomain<ModelJournal, ModelEngine>> {
    let mut store = JournaledStore::open(
        ModelJournal::new(),
        CLUSTER,
        REPLICA,
        inc(),
        BOOT,
        JournalLimits::default(),
    )
    .unwrap();
    store
        .attach(DOMAIN, ShardId::new(0).unwrap(), ModelEngine::new())
        .unwrap();
    let base = store.application_base(DOMAIN).unwrap();
    let mut domain = JournaledDomain::new(
        store,
        DOMAIN,
        Ballot {
            epoch: base.configuration,
            number: 0,
            leader: REPLICA,
        },
    )
    .expect("attached");
    let mut alloc = BarrierAllocator::new(inc(), BOOT);
    domain
        .submit(
            PersistBatch {
                barrier: alloc.allocate(),
                base: Some(base),
                updates: bootstrap(),
            },
            TransitionKind::Application {
                position: base.execution_position.checked_next().unwrap(),
                revision: None,
                result_digest: Digest32([0; 32]),
            },
        )
        .unwrap();
    domain.lower().unwrap();
    Applier::new(domain, alloc).unwrap()
}

/// Write [`KEYS`] keys of a MiB each, one command apiece, from sequence 1.
fn seed<P: Persistence>(applier: &mut Applier<P>) -> u64 {
    for i in 0..KEYS {
        let (id, payload) = command(
            u64::from(i) + 1,
            CanonicalOperation::Put(PutOp {
                key: big_key(i),
                value: vec![i; MAX_VALUE_BYTES],
                lease: None,
                prev_kv: false,
            }),
        );
        let written = applier.apply_staged(id, &payload).unwrap();
        assert!(written.revision.is_some(), "key {i} written");
    }
    u64::from(KEYS) + 1
}

/// `op` executes and is refused as `ViewTooLarge`: a result, retained
/// under its retry key, at the position after `before`, writing nothing.
fn refused_as_too_large<P: Persistence>(
    applier: &mut Applier<P>,
    seq: u64,
    op: CanonicalOperation,
) -> ExecutionPosition {
    let revision = applier.kv_revision().unwrap();
    let (id, payload) = command(seq, op);
    let outcome = applier
        .apply_staged(id, &payload)
        .expect("a replicated refusal, not this node's engine error");
    let response: Response = postcard::from_bytes(&outcome.response).unwrap();
    assert_eq!(
        response.outcome,
        Outcome::ErrRejected {
            reason: RejectionReason::ViewTooLarge
        }
    );
    assert_eq!(outcome.revision, None, "a refusal writes nothing");
    assert_eq!(applier.kv_revision().unwrap(), revision);
    // Asked again, it answers the same from its retained result.
    let again = applier.apply(id, &payload).unwrap();
    assert_eq!(again.position, outcome.position);
    assert_eq!(again.result_digest, outcome.result_digest);
    outcome.position
}

fn reads_over_the_byte_budget_are_refused<P: Persistence>(mut applier: Applier<P>) {
    let mut seq = seed(&mut applier);
    // An interval: one row asked for, every row in it loaded.
    let interval = refused_as_too_large(
        &mut applier,
        seq,
        CanonicalOperation::Range(read(KeyRange::interval(b"big/".to_vec(), b"big0".to_vec()))),
    );
    seq += 1;
    // The same rows as exact keys, each its own touch of the view.
    let exact = refused_as_too_large(
        &mut applier,
        seq,
        CanonicalOperation::Txn(TxnOp {
            compares: Vec::new(),
            success: (0..KEYS)
                .map(|i| BranchOp::Range(read(KeyRange::exact(big_key(i)))))
                .collect(),
            failure: Vec::new(),
        }),
    );
    seq += 1;
    assert_eq!(exact.get(), interval.get() + 1);
    // Fifteen of them fit, so the same transaction over fifteen keys is
    // served. It asks for their keys alone: the view loads every row
    // whole either way, and fifteen values would be a response larger
    // than a retry record retains, refused for that instead.
    let (id, payload) = command(
        seq,
        CanonicalOperation::Txn(TxnOp {
            compares: Vec::new(),
            success: (0..KEYS - 2)
                .map(|i| {
                    BranchOp::Range(RangeOp {
                        keys_only: true,
                        ..read(KeyRange::exact(big_key(i)))
                    })
                })
                .collect(),
            failure: Vec::new(),
        }),
    );
    seq += 1;
    let served = applier.apply_staged(id, &payload).unwrap();
    let response: Response = postcard::from_bytes(&served.response).unwrap();
    assert!(
        matches!(
            response.outcome,
            Outcome::Txn {
                succeeded: true,
                ..
            }
        ),
        "{:?}",
        response.outcome
    );
    // The write behind them executes, at the next position.
    let (id, payload) = command(
        seq,
        CanonicalOperation::Put(PutOp {
            key: b"after".to_vec(),
            value: b"1".to_vec(),
            lease: None,
            prev_kv: false,
        }),
    );
    let after = applier.apply_staged(id, &payload).unwrap();
    assert!(after.revision.is_some());
    assert_eq!(after.position.get(), served.position.get() + 1);
}

#[test]
fn reads_over_the_byte_budget_are_refused_on_a_projection() {
    reads_over_the_byte_budget_are_refused(worker());
}

#[test]
fn reads_over_the_byte_budget_are_refused_on_a_journal() {
    reads_over_the_byte_budget_are_refused(journaled());
}

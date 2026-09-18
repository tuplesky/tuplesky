//! Acceptance for the applying path (task-24): a command chosen to execute
//! always reaches a terminal, retry-resolvable result, and every durable
//! revision reaches the watch hub exactly once and in order, including one
//! established by reconciliation after an indeterminate acknowledgement.

use coord_consensus::PayloadRecordV1;
use coord_core::effect::PersistBatch;
use coord_core::outbox::BarrierAllocator;
use coord_state::policy::{Action, KeyInterval, PolicyRule};
use coord_state::{Outcome, RejectionReason, Response};
use coord_storage::policy::{bootstrap_session, rule_update};
use coord_storage::{Applier, GroupLimits, StoreWorker, WatchItem, WatchSpec};
use coord_store_testkit::model::{CommitScript, ModelEngine};
use coord_types::ids::*;
use coord_types::logical_v1::*;
use coord_types::{CommandId, RetryKey};

const NS: NamespaceId = NamespaceId([5; 16]);
const SESSION: SessionId = SessionId([3; 16]);
const CLIENT: ClientInstanceId = ClientInstanceId([4; 16]);
const ALICE: PrincipalId = PrincipalId([0xa; 16]);

fn retry_key(seq: u64) -> RetryKey {
    RetryKey {
        cluster_id: ClusterId([1; 16]),
        domain_id: DomainId([2; 16]),
        session_id: SESSION,
        client_instance_id: CLIENT,
        request_sequence: RequestSequence::new(seq).unwrap(),
    }
}

/// A payload record for `request` under sequence `seq`, as the consensus
/// layer would have made durable.
fn payload(seq: u64, request: &LogicalRequest) -> (CommandId, PayloadRecordV1) {
    let command = CommandId::derive(&retry_key(seq), request).unwrap();
    (
        command,
        PayloadRecordV1 {
            retry_key: retry_key(seq),
            logical: postcard::to_allocvec(request).unwrap(),
        },
    )
}

fn req(op: CanonicalOperation) -> LogicalRequest {
    let mut r = LogicalRequest::new(NS, op);
    r.canonicalize();
    r
}

fn put(key: &[u8], value: &[u8]) -> LogicalRequest {
    req(CanonicalOperation::Put(PutOp {
        key: key.to_vec(),
        value: value.to_vec(),
        lease: None,
        prev_kv: false,
    }))
}

/// An applier over a model engine with Alice's session and full policy.
fn applier(engine: ModelEngine) -> Applier<ModelEngine> {
    let boot = coord_core::effect::BootId([1; 16]);
    let inc = ReplicaIncarnation::new(1).unwrap();
    let mut worker = StoreWorker::open(engine, boot, inc, GroupLimits::default()).unwrap();
    let mut alloc = BarrierAllocator::new(inc, boot);
    // A wide retry window: the seeding below spends thousands of
    // sequence numbers before the interesting command.
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
    worker
        .submit(PersistBatch {
            barrier: alloc.allocate(),
            base: Some(worker.application_base()),
            updates,
        })
        .unwrap();
    worker.flush().unwrap();
    Applier::new(worker, alloc).unwrap()
}

#[test]
fn a_terminal_semantic_rejection_is_a_result_and_never_blocks_successors() {
    let mut applier = applier(ModelEngine::new());
    // Seed more keys than one range delete may remove. The view budget
    // admits them; the planner's delete limit does not.
    let limit = PlanLimitsProbe::max_delete_keys();
    seed_keys(&mut applier, limit + 1);
    let before = applier.kv_revision().unwrap();
    // The oversized delete was already chosen to execute: it cannot be
    // left unexecuted, so it takes its position and records a rejection.
    let doomed = req(CanonicalOperation::DeleteRange(DeleteRangeOp {
        range: KeyRange::interval(vec![0], vec![0xff, 0xff, 0xff, 0xff]),
        prev_kv: false,
    }));
    let (command, record) = payload(1, &doomed);
    let outcome = applier.apply(command, &record).unwrap();
    assert_eq!(outcome.revision, None, "nothing was written");
    assert_eq!(applier.kv_revision().unwrap(), before);
    // It is retry-resolvable: the same identity returns the same result at
    // the same position, and does not execute again.
    let again = applier.apply(command, &record).unwrap();
    assert_eq!(again.position, outcome.position);
    assert_eq!(again.result_digest, outcome.result_digest);
    // The recorded result names the rejection.
    let gated = applier.worker().reader().snapshot().unwrap();
    let stored = coord_storage::retry::lookup(gated.view(), &retry_key(1))
        .unwrap()
        .expect("retained");
    drop(gated);
    let response: Response = postcard::from_bytes(&stored.response).unwrap();
    assert_eq!(
        response.outcome,
        Outcome::ErrRejected {
            reason: RejectionReason::TooManyDeletes
        }
    );
    // A valid write after it proceeds: nothing is wedged.
    let (command, record) = payload(2, &put(b"after", b"1"));
    let outcome = applier.apply(command, &record).unwrap();
    assert!(outcome.revision.is_some());
    assert_eq!(
        outcome.position.get(),
        again.position.get() + 1,
        "the rejection occupied exactly one position"
    );
}

/// Write `count` current entries directly, in one administrative batch:
/// the planner counts them, and seeding them one command at a time would
/// spend thousands of executions on setup.
fn seed_keys(applier: &mut Applier<ModelEngine>, count: u32) {
    let entry = coord_state::KvEntry {
        value: b"v".to_vec(),
        create_revision: KvRevision::new(1).unwrap(),
        mod_revision: KvRevision::new(1).unwrap(),
        version: 1,
        lease: None,
        lease_generation: None,
    };
    let updates: Vec<_> = (0..count)
        .map(|i| coord_core::effect::StoreUpdate {
            collection: coord_store_api::registry::Collection::KvCurrentV1.id(),
            key: coord_storage::codecs::current_key(&NS, &i.to_be_bytes()),
            value: Some(coord_storage::codecs::encode_current(&entry).unwrap()),
        })
        .collect();
    let base = applier.worker().application_base();
    let barrier = applier.alloc().allocate();
    applier
        .worker_mut()
        .submit(PersistBatch {
            barrier,
            base: Some(base),
            updates,
        })
        .unwrap();
    applier.worker_mut().flush().unwrap();
}

/// The planner's delete limit, read from its defaults.
struct PlanLimitsProbe;
impl PlanLimitsProbe {
    fn max_delete_keys() -> u32 {
        u32::try_from(coord_state::PlanLimits::default().max_delete_keys).unwrap()
    }
}

#[test]
fn a_commit_established_by_reconciliation_still_reaches_open_watches() {
    let engine = ModelEngine::new();
    let mut applier = applier(engine);
    let registration = applier
        .hub()
        .register(WatchSpec {
            namespace: NS,
            key: vec![],
            range_end: Some(vec![0xff]),
            start_revision: None,
            prev_kv: false,
            progress_notify: false,
            queue_capacity: 64,
        })
        .unwrap();
    applier.hub().replay_complete(registration.id).unwrap();
    // A write whose commit takes effect but is acknowledged as unknown.
    applier
        .worker_mut()
        .engine_mut()
        .script_commit(CommitScript::Indeterminate { applied: true });
    let (command, record) = payload(1, &put(b"a", b"1"));
    let first = applier.apply(command, &record).unwrap();
    let r1 = first.revision.expect("the commit took effect");
    // A following ordinary write.
    let (command, record) = payload(2, &put(b"b", b"2"));
    let second = applier.apply(command, &record).unwrap();
    let r2 = second.revision.expect("applied");
    assert_eq!(r2.get(), r1.get() + 1);
    // The watch saw both revisions, in order and once each: the hub never
    // fell behind, so nothing later was refused as a gap.
    let mut seen = Vec::new();
    while let Some(item) = applier.hub().next(registration.id, |_| true) {
        match item {
            WatchItem::Batch(b) => seen.push(b.revision),
            WatchItem::Progress(_) => {}
            WatchItem::Closed { reason, .. } => panic!("{reason:?}"),
        }
    }
    assert_eq!(seen, vec![r1, r2], "each revision once, in order");
    assert_eq!(applier.hub().published(), r2);
}

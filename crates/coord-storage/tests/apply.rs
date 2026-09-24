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
use coord_storage::{
    Applier, GroupLimits, Overlay, SpeculationLimits, SpeculationRefused, StoreWorker, ViewBudget,
    WatchItem, WatchSpec, speculate,
};
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
fn applier(engine: ModelEngine) -> Applier<StoreWorker<ModelEngine>> {
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
    let gated = applier.store().reader().snapshot().unwrap();
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
fn seed_keys(applier: &mut Applier<StoreWorker<ModelEngine>>, count: u32) {
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
    let base = applier.store().application_base();
    let barrier = applier.alloc().allocate();
    applier
        .store_mut()
        .submit(PersistBatch {
            barrier,
            base: Some(base),
            updates,
        })
        .unwrap();
    applier.store_mut().flush().unwrap();
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
        .store_mut()
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

#[test]
fn a_view_too_large_to_build_is_a_rejection_and_never_blocks_successors() {
    // The view is built before the planner runs, so a request whose
    // touched interval exceeds the schema's view budget never reached the
    // terminal-rejection path: it returned an error with no executed
    // identity and no position advance, and every successor waited behind
    // a command that would fail again on every retry.
    let mut applier = applier(ModelEngine::new());
    let rows = ViewBudget::SCHEMA.max_rows + 1;
    seed_keys(&mut applier, rows);
    let before = applier.kv_revision().unwrap();
    // A bounded read: the limit is one row, but the interval it is taken
    // from is loaded first.
    let doomed = req(CanonicalOperation::Range(RangeOp {
        range: KeyRange::interval(vec![0], vec![0xff, 0xff, 0xff, 0xff]),
        revision: None,
        limit: 1,
        count_only: false,
        keys_only: false,
    }));
    let (command, record) = payload(1, &doomed);
    let outcome = applier
        .apply(command, &record)
        .expect("a result, not an error");
    assert_eq!(outcome.revision, None, "a read writes nothing");
    assert_eq!(applier.kv_revision().unwrap(), before);
    // Retry-resolvable at the same position, with the same result.
    let again = applier.apply(command, &record).unwrap();
    assert_eq!(again.position, outcome.position);
    assert_eq!(again.result_digest, outcome.result_digest);
    let gated = applier.store().reader().snapshot().unwrap();
    let stored = coord_storage::retry::lookup(gated.view(), &retry_key(1))
        .unwrap()
        .expect("retained");
    drop(gated);
    let response: Response = postcard::from_bytes(&stored.response).unwrap();
    assert_eq!(
        response.outcome,
        Outcome::ErrRejected {
            reason: RejectionReason::ViewTooLarge
        }
    );
    // The write behind it proceeds.
    let (command, record) = payload(2, &put(b"after", b"1"));
    let outcome = applier.apply(command, &record).unwrap();
    assert!(outcome.revision.is_some());
    assert_eq!(
        outcome.position.get(),
        again.position.get() + 1,
        "the rejection occupied exactly one position"
    );
}

/// A range read over `count` seeded keys: it mutates nothing and
/// publishes no event, so only its response has any size.
fn range_all() -> LogicalRequest {
    req(CanonicalOperation::Range(RangeOp {
        range: KeyRange::interval(vec![0u8], vec![0xffu8]),
        revision: None,
        limit: 0,
        count_only: false,
        keys_only: false,
    }))
}

#[test]
fn the_overlay_bound_is_checked_against_the_plan_it_would_hold() {
    // The byte bound is on what the overlay holds *after* admitting a
    // plan. Checking only what it held before let a single plan of any
    // size in, and a range read — no mutations, no events — was charged
    // nothing at all while its response carried every value it returned.
    let mut applier = applier(ModelEngine::new());
    seed_keys(&mut applier, 64);
    let request = range_all();
    let (command, record) = payload(1, &request);
    let ask = coord_consensus::SpeculationRequest {
        command,
        prefix: vec![],
        position: ExecutionPosition::new(
            applier.store().application_base().execution_position.get() + 1,
        )
        .unwrap(),
    };
    // Generous bound: the read is speculated, and the overlay is charged
    // the response it holds, not zero.
    let mut overlay = Overlay::new();
    let outcome = speculate(
        applier.store(),
        &mut overlay,
        &SpeculationLimits {
            max_commands: 8,
            max_bytes: 1 << 20,
        },
        &ask,
        &record,
    )
    .expect("speculated");
    assert!(
        outcome.response.len() > 64,
        "the read returned the seeded rows: {} bytes",
        outcome.response.len()
    );
    let charged = overlay.bytes();
    assert!(
        charged >= outcome.response.len(),
        "the response is charged: {charged} < {}",
        outcome.response.len()
    );
    // The same read under a bound smaller than its own response is
    // refused, even though the overlay was empty.
    let mut tight = Overlay::new();
    let refused = speculate(
        applier.store(),
        &mut tight,
        &SpeculationLimits {
            max_commands: 8,
            max_bytes: charged - 1,
        },
        &ask,
        &record,
    );
    assert!(
        matches!(refused, Err(SpeculationRefused::OverBudget)),
        "{refused:?}"
    );
    assert_eq!(tight.bytes(), 0, "a refusal leaves the overlay unchanged");
    assert!(tight.is_empty());
    // Retiring the plan returns exactly what admitting it took.
    overlay.retire(&command);
    assert_eq!(overlay.bytes(), 0);
}

/// Put the domain's revision counter at `revision`.
///
/// Fixture construction: it lets the obsolete versions below be written
/// at revisions genuinely below the current one, as a replica that has
/// been running for a while would hold them.
fn set_kv_revision(applier: &mut Applier<StoreWorker<ModelEngine>>, revision: u64) {
    let base = applier.store().application_base();
    let barrier = applier.alloc().allocate();
    applier
        .store_mut()
        .submit(PersistBatch {
            barrier,
            base: Some(base),
            updates: vec![coord_core::effect::StoreUpdate {
                collection: coord_store_api::registry::Collection::MetaV1.id(),
                key: coord_store_api::registry::meta_fields::KV_REVISION.to_vec(),
                value: Some(coord_storage::codecs::encode_counter(revision).unwrap()),
            }],
        })
        .unwrap();
    applier.store_mut().flush().unwrap();
}

/// Seed `versions` obsolete stored versions of `key`, every one strictly
/// below `current`.
///
/// This is what a replica that has not collected them holds on disk; a
/// replica that has collected them holds only the current entry. Nothing
/// logical distinguishes the two.
fn seed_obsolete_versions(
    applier: &mut Applier<StoreWorker<ModelEngine>>,
    key: &[u8],
    versions: u32,
    current: u64,
) {
    assert!(
        current > u64::from(versions),
        "the versions must be below it"
    );
    let updates: Vec<_> = (1..=versions)
        .map(|v| {
            let revision =
                KvRevision::new(current - u64::from(versions) + u64::from(v) - 1).unwrap();
            let entry = coord_state::KvEntry {
                value: format!("obsolete-{v}").into_bytes(),
                create_revision: KvRevision::new(1).unwrap(),
                mod_revision: revision,
                version: u64::from(v),
                lease: None,
                lease_generation: None,
            };
            coord_core::effect::StoreUpdate {
                collection: coord_store_api::registry::Collection::KvHistoryV1.id(),
                key: coord_storage::codecs::history_key(&NS, key, revision),
                value: Some(
                    coord_storage::codecs::encode_history(
                        &coord_storage::codecs::HistoryRecordV1 { entry: Some(entry) },
                    )
                    .unwrap(),
                ),
            }
        })
        .collect();
    let base = applier.store().application_base();
    let barrier = applier.alloc().allocate();
    applier
        .store_mut()
        .submit(PersistBatch {
            barrier,
            base: Some(base),
            updates,
        })
        .unwrap();
    applier.store_mut().flush().unwrap();
}

#[test]
fn local_garbage_collection_progress_never_changes_a_replicated_outcome() {
    // The view budget was charged against physical rows scanned, and a
    // historical read scans every stored version of the keys it covers.
    // Garbage collection runs at its own pace on each replica, so two
    // replicas holding the same logical state at the same compaction
    // floor could disagree about the same chosen command: the collected
    // one answered the read, the uncollected one exceeded the budget and
    // durably recorded a rejection. That is a mutation on one replica
    // and a rejection on another, not merely different timing.
    let versions = ViewBudget::SCHEMA.max_rows + 16;
    let start = u64::from(versions) + 1;
    let mut churned = applier(ModelEngine::new());
    let mut collected = applier(ModelEngine::new());
    // Both reach the same logical state at the same revision.
    for a in [&mut churned, &mut collected] {
        set_kv_revision(a, start);
        let (command, record) = payload(1, &put(b"hot", b"final"));
        a.apply(command, &record).unwrap();
    }
    let revision = churned.kv_revision().unwrap();
    assert_eq!(revision, collected.kv_revision().unwrap());
    // One replica still holds every obsolete version below the current
    // entry; the other has collected them. Nothing logical distinguishes
    // the two, and their compaction floors are the same.
    seed_obsolete_versions(&mut churned, b"hot", versions, revision.get());

    let historical = req(CanonicalOperation::Range(RangeOp {
        range: KeyRange::interval(b"h".to_vec(), b"i".to_vec()),
        revision: Some(revision),
        limit: 0,
        count_only: false,
        keys_only: false,
    }));
    let (command, record) = payload(2, &historical);
    let churned_outcome = churned
        .apply(command, &record)
        .expect("a result, not a local failure");
    let collected_outcome = collected.apply(command, &record).expect("a result");

    let stored_response = |a: &mut Applier<StoreWorker<ModelEngine>>| {
        let gated = a.store().reader().snapshot().unwrap();
        let record = coord_storage::retry::lookup(gated.view(), &retry_key(2))
            .unwrap()
            .expect("retained");
        drop(gated);
        postcard::from_bytes::<Response>(&record.response).unwrap()
    };
    let churned_response = stored_response(&mut churned);
    let collected_response = stored_response(&mut collected);
    assert!(
        !matches!(
            churned_response.outcome,
            Outcome::ErrRejected {
                reason: RejectionReason::ViewTooLarge
            }
        ),
        "uncollected history is not a reason to reject: {:?}",
        churned_response.outcome
    );
    // The same command against the same logical state gives the same
    // answer, whatever each replica happens to have collected.
    assert_eq!(
        churned_response.outcome, collected_response.outcome,
        "the outcome is the logical state's, not local cleanup progress"
    );
    assert_eq!(
        churned_outcome.result_digest,
        collected_outcome.result_digest
    );
    assert!(churned_outcome.revision.is_none() && collected_outcome.revision.is_none());
}

#[test]
fn an_admission_refusal_finishes_the_command_it_refuses() {
    // Retry admission could refuse a chosen command with no terminal
    // outcome and no position advance: it failed again on every retry
    // and every successor waited behind it for ever.
    let mut applier = applier(ModelEngine::new());
    // A request whose sequence is far beyond the session's window.
    let beyond = 1u64 << 40;
    let (command, record) = payload(beyond, &put(b"k", b"v"));
    let outcome = applier
        .apply(command, &record)
        .expect("a result, not an error");
    assert_eq!(outcome.revision, None, "a refusal writes nothing");
    let gated = applier.store().reader().snapshot().unwrap();
    // The refusal writes no retry record: the key it names is not bound
    // to it, so no other command's result can be overwritten.
    assert!(
        coord_storage::retry::lookup(gated.view(), &retry_key(beyond))
            .unwrap()
            .is_none(),
        "an out-of-window sequence binds nothing"
    );
    drop(gated);
    // The command behind it proceeds at the next position.
    let (next, next_record) = payload(1, &put(b"after", b"1"));
    let after = applier.apply(next, &next_record).unwrap();
    assert!(after.revision.is_some());
    assert_eq!(
        after.position.get(),
        outcome.position.get() + 1,
        "the refusal occupied exactly one position"
    );
}

#[test]
fn a_planner_valid_response_always_fits_its_retry_record() {
    // The planner allowed responses four times larger than the envelope
    // that stores the retained result, so a response could plan and then
    // fail to persist, leaving the chosen command unresolved. The limits
    // are aligned now; this holds the behaviour at the boundary.
    let mut applier = applier(ModelEngine::new());
    // Three large values: comfortably inside the view budget, and over
    // the retry envelope had the planner still allowed 8 MiB.
    let value = vec![b'x'; 768 * 1024];
    for (i, key) in [b"big-1", b"big-2", b"big-3"].iter().enumerate() {
        let (command, record) = payload(i as u64 + 1, &put(key.as_slice(), &value));
        applier.apply(command, &record).unwrap();
    }
    let (command, record) = payload(10, &range_all());
    let outcome = applier
        .apply(command, &record)
        .expect("a result, not a storage error");
    let gated = applier.store().reader().snapshot().unwrap();
    let stored = coord_storage::retry::lookup(gated.view(), &retry_key(10))
        .unwrap()
        .expect("the result is retained, whatever it is");
    drop(gated);
    let response: Response = postcard::from_bytes(&stored.response).unwrap();
    // Either the values came back or the size was refused in an ordered
    // way; what must not happen is an unresolved command.
    match response.outcome {
        Outcome::Range { .. } => {}
        Outcome::ErrRejected {
            reason: RejectionReason::ResponseTooLarge,
        } => {}
        other => panic!("{other:?}"),
    }
    // And the command behind it proceeds.
    let (next, next_record) = payload(11, &put(b"after", b"1"));
    let after = applier.apply(next, &next_record).unwrap();
    assert_eq!(after.position.get(), outcome.position.get() + 1);
}

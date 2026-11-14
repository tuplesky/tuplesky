//! A command whose rows don't fit one journal record is refused as a
//! replicated result (`TooManyEvents`), at the position it was chosen for,
//! and the commands behind it execute.
//!
//! A node journals each command's batch as one record, which holds 4 MiB
//! and 4,096 updates. A write's rows carry its previous values, so a
//! request well within the planner's limits could still be larger than
//! that: the journal refused it on submit, `apply_bound` returned the
//! refusal as `ApplyError::Engine`, the voter stopped on it, every voter
//! stopped on the same command, and replay reached it again. A projection
//! alone allows 8 MiB and so accepted it, which no voter of a node is.
//! A record also bounded each row key at 16 KiB, and a row key is the
//! request's key escaped, which writes each zero byte as two: a key of
//! 8 KiB zeros was stopping the domain the same way. A record now holds the
//! longest row key a command writes.
//!
//! The applier now measures the batch against the record's bound before
//! submitting it, whatever its store, so a journal and a projection alone
//! reach the same refusal with the same bytes. The tests run each case on
//! both.

mod support;

use coord_consensus::{AppliedOutcome, PayloadRecordV1};
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
use coord_types::logical_v1::limits::{MAX_KEY_BYTES, MAX_VALUE_BYTES};
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

fn put(seq: u64, key: &[u8], len: usize, prev_kv: bool) -> (CommandId, PayloadRecordV1) {
    command(
        seq,
        CanonicalOperation::Put(PutOp {
            key: key.to_vec(),
            value: vec![7; len],
            lease: None,
            prev_kv,
        }),
    )
}

fn delete_range(seq: u64, from: &[u8], to: &[u8]) -> (CommandId, PayloadRecordV1) {
    command(
        seq,
        CanonicalOperation::DeleteRange(DeleteRangeOp {
            range: KeyRange::interval(from.to_vec(), to.to_vec()),
            prev_kv: false,
        }),
    )
}

fn count(seq: u64, from: &[u8], to: &[u8]) -> (CommandId, PayloadRecordV1) {
    command(
        seq,
        CanonicalOperation::Range(RangeOp {
            range: KeyRange::interval(from.to_vec(), to.to_vec()),
            revision: None,
            limit: 0,
            count_only: true,
            keys_only: false,
        }),
    )
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

/// One projection; each command its own batch, up to the worker's 8 MiB.
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

fn apply<P: Persistence>(
    applier: &mut Applier<P>,
    c: &(CommandId, PayloadRecordV1),
) -> (AppliedOutcome, Outcome) {
    let applied = applier
        .apply_staged(c.0, &c.1)
        .expect("a replicated result, not this voter's error");
    let response: Response = postcard::from_bytes(&applied.response).unwrap();
    (applied, response.outcome)
}

const REFUSED: Outcome = Outcome::ErrRejected {
    reason: RejectionReason::TooManyEvents,
};

/// A put of a MiB with `prev_kv` over a MiB: its current row, its event
/// row and its retained response (which holds the previous value) come to
/// more than a record's 4 MiB. Each run returns what it answered, for the
/// two stores to be compared.
fn oversized_put<P: Persistence>(mut a: Applier<P>) -> Vec<(AppliedOutcome, Outcome)> {
    let mut out = Vec::new();
    let (first, outcome) = apply(&mut a, &put(1, b"k", MAX_VALUE_BYTES, false));
    assert!(
        matches!(outcome, Outcome::Put { prev: None }),
        "{outcome:?}"
    );
    let before = a.kv_revision().unwrap();

    // Control: the same put without the previous value fits, as does one
    // with it whose value is smaller.
    let (_, outcome) = apply(&mut a, &put(2, b"k", MAX_VALUE_BYTES / 4, true));
    assert!(
        matches!(outcome, Outcome::Put { prev: Some(_) }),
        "{outcome:?}"
    );
    let (_, outcome) = apply(&mut a, &put(3, b"k", MAX_VALUE_BYTES, false));
    assert!(
        matches!(outcome, Outcome::Put { prev: None }),
        "{outcome:?}"
    );
    let kept = a.kv_revision().unwrap();
    assert_eq!(kept.get(), before.get() + 2);

    let oversized = put(4, b"k", MAX_VALUE_BYTES, true);
    let (refused, outcome) = apply(&mut a, &oversized);
    assert_eq!(outcome, REFUSED);
    assert_eq!(refused.revision, None, "a refusal writes no revision");
    assert_eq!(a.kv_revision().unwrap(), kept, "nothing changed");
    assert!(refused.position > first.position);
    // Asked again, it answers from its retained result.
    let (again, outcome) = apply(&mut a, &oversized);
    assert_eq!(outcome, REFUSED);
    assert_eq!(again.position, refused.position);
    assert_eq!(again.result_digest, refused.result_digest);
    out.push((refused.clone(), REFUSED));
    // The next command executes at the next position.
    let (next, outcome) = apply(&mut a, &put(5, b"k", 16, false));
    assert!(
        matches!(outcome, Outcome::Put { prev: None }),
        "{outcome:?}"
    );
    assert_eq!(next.position.get(), refused.position.get() + 1);
    assert_eq!(a.kv_revision().unwrap().get(), kept.get() + 1);
    out.push((next, outcome));
    out
}

/// A range delete of five keys of a MiB: its event rows carry each
/// deleted value, so its batch is more than a record's 4 MiB, though the
/// planner allows 4,096 deleted keys.
fn oversized_delete<P: Persistence>(mut a: Applier<P>) -> Vec<(AppliedOutcome, Outcome)> {
    let mut out = Vec::new();
    for i in 0..5u8 {
        let (_, outcome) = apply(
            &mut a,
            &put(
                u64::from(i) + 1,
                &[b'd', b'/', b'0' + i],
                MAX_VALUE_BYTES,
                false,
            ),
        );
        assert!(
            matches!(outcome, Outcome::Put { prev: None }),
            "{outcome:?}"
        );
    }
    let kept = a.kv_revision().unwrap();
    let (refused, outcome) = apply(&mut a, &delete_range(6, b"d/", b"d0"));
    assert_eq!(outcome, REFUSED);
    assert_eq!(refused.revision, None);
    assert_eq!(a.kv_revision().unwrap(), kept);
    let (_, outcome) = apply(&mut a, &count(7, b"d/", b"d0"));
    assert!(
        matches!(outcome, Outcome::Range { count: 5, .. }),
        "every key is still there: {outcome:?}"
    );
    out.push((refused, REFUSED));
    // Control: two of them fit one record and are deleted.
    let (deleted, outcome) = apply(&mut a, &delete_range(8, b"d/0", b"d/2"));
    assert!(
        matches!(outcome, Outcome::Delete { deleted: 2, .. }),
        "{outcome:?}"
    );
    out.push((deleted, outcome));
    out
}

fn small_put(key: String) -> BranchOp {
    BranchOp::Put(PutOp {
        key: key.into_bytes(),
        value: b"v".to_vec(),
        lease: None,
        prev_kv: false,
    })
}

/// A range delete of 2,160 small keys: each is a current-row deletion and
/// an event row, and the command adds its own three, so its batch is 4,323
/// rows, past a record's 4,096 updates though it is a few hundred KiB and
/// the planner allows 4,096 deleted keys. A deleted key is two rows (three
/// if attached to a lease), so a range delete of more than 2,046 keys is
/// refused `TooManyEvents`. This is the test that reaches the record's
/// update count.
fn too_many_rows<P: Persistence>(mut a: Applier<P>) -> Vec<(AppliedOutcome, Outcome)> {
    let mut out = Vec::new();
    let mut seq = 1;
    for batch in 0..18u32 {
        let success = (0..120u32)
            .map(|i| small_put(format!("m/{:05}", batch * 120 + i)))
            .collect();
        let (_, outcome) = apply(
            &mut a,
            &command(
                seq,
                CanonicalOperation::Txn(TxnOp {
                    compares: Vec::new(),
                    success,
                    failure: Vec::new(),
                }),
            ),
        );
        assert!(
            matches!(
                outcome,
                Outcome::Txn {
                    succeeded: true,
                    ..
                }
            ),
            "{outcome:?}"
        );
        seq += 1;
    }
    let kept = a.kv_revision().unwrap();
    let (refused, outcome) = apply(&mut a, &delete_range(seq, b"m/", b"m0"));
    seq += 1;
    assert_eq!(outcome, REFUSED);
    assert_eq!(refused.revision, None);
    assert_eq!(a.kv_revision().unwrap(), kept);
    out.push((refused, REFUSED));
    // Control: 2,000 of them are 4,003 rows, which fit.
    let (deleted, outcome) = apply(&mut a, &delete_range(seq, b"m/", b"m/02000"));
    assert!(
        matches!(outcome, Outcome::Delete { deleted: 2000, .. }),
        "{outcome:?}"
    );
    out.push((deleted, outcome));
    out
}

/// A key a request may carry at its longest, every byte zero. The
/// ordered-key escaping writes each zero byte as two, so its rows' keys are
/// twice its length and more; attached to a lease, its reverse-index row
/// has the longest row key a command writes
/// (`codecs::LONGEST_ROW_KEY_BYTES`). A record held row keys of 16 KiB,
/// which that one passes: the put stopped every voter, and #294's first
/// version refused it. A record now holds it, so the key is put, read and
/// revoked with its lease like any other.
fn escaped_key<P: Persistence>(mut a: Applier<P>) -> Vec<(AppliedOutcome, Outcome)> {
    const LEASE: LeaseId = LeaseId([8; 16]);
    let zeros = vec![0; MAX_KEY_BYTES];
    let longest = coord_storage::codecs::lease_key(&LEASE, &NS, &zeros).len();
    assert_eq!(longest, coord_storage::codecs::LONGEST_ROW_KEY_BYTES);
    assert!(longest > 16 * 1024, "past the record's old bound");
    let mut out = Vec::new();
    let (granted, outcome) = apply(
        &mut a,
        &command(
            1,
            CanonicalOperation::LeaseGrant {
                lease_id: LEASE,
                ttl_seconds: 60,
            },
        ),
    );
    assert!(
        matches!(outcome, Outcome::LeaseGranted { .. }),
        "{outcome:?}"
    );
    out.push((granted, outcome));
    let (written, outcome) = apply(
        &mut a,
        &command(
            2,
            CanonicalOperation::Put(PutOp {
                key: zeros.clone(),
                value: vec![7; 16],
                lease: Some(LEASE),
                prev_kv: false,
            }),
        ),
    );
    assert!(
        matches!(outcome, Outcome::Put { prev: None }),
        "{outcome:?}"
    );
    assert!(written.revision.is_some(), "a put writes a revision");
    out.push((written, outcome));
    let (read, outcome) = apply(
        &mut a,
        &command(
            3,
            CanonicalOperation::Range(RangeOp {
                range: KeyRange::exact(zeros.clone()),
                revision: None,
                limit: 0,
                count_only: false,
                keys_only: false,
            }),
        ),
    );
    let Outcome::Range {
        items, count: 1, ..
    } = &outcome
    else {
        panic!("{outcome:?}");
    };
    assert_eq!(items.len(), 1);
    out.push((read, outcome));
    let before = a.kv_revision().unwrap();
    let (revoked, outcome) = apply(
        &mut a,
        &command(4, CanonicalOperation::LeaseRevoke { lease_id: LEASE }),
    );
    assert_eq!(outcome, Outcome::LeaseRevoked { deleted: 1 });
    assert_eq!(a.kv_revision().unwrap().get(), before.get() + 1);
    out.push((revoked, outcome));
    out
}

/// A lease at its byte quota (`MAX_LEASE_BYTES`) expires.
///
/// As many attached keys of the longest length with the largest value as
/// the quota takes, then one whose value brings the lease's attachment cost
/// to the quota exactly (at 3 MiB, two and a third). Its expiry deletes them
/// all in one revision: the batch, which carries each key's previous value
/// in its event row, is one record, and the view the expiry plans against,
/// which loads each attached key's current entry, is inside the view's
/// budget. An expiry is the domain's own command, so a lease the quota
/// admits that couldn't expire would never end.
fn lease_at_quota<P: Persistence>(mut a: Applier<P>) -> Vec<(AppliedOutcome, Outcome)> {
    const LEASE: LeaseId = LeaseId([9; 16]);
    let mut out = Vec::new();
    let (_, granted) = apply(
        &mut a,
        &command(
            1,
            CanonicalOperation::LeaseGrant {
                lease_id: LEASE,
                ttl_seconds: 60,
            },
        ),
    );
    let Outcome::LeaseGranted { generation, .. } = granted else {
        panic!("{granted:?}");
    };
    let key = |i: usize| {
        let mut key = vec![b'k'; MAX_KEY_BYTES];
        key[MAX_KEY_BYTES - 1] = b'0' + i as u8;
        key
    };
    let full = coord_state::attachment_cost(&key(0), &vec![0; MAX_VALUE_BYTES]);
    let bare = coord_state::attachment_cost(&key(0), &[]);
    let mut attached = Vec::new();
    let mut rest = coord_state::MAX_LEASE_BYTES;
    while rest >= full {
        attached.push((key(attached.len()), MAX_VALUE_BYTES));
        rest -= full;
    }
    if rest > 0 {
        assert!(rest >= bare, "the quota's remainder holds a key");
        attached.push((key(attached.len()), (rest - bare) as usize));
    }
    let cost: u64 = attached
        .iter()
        .map(|(k, len)| coord_state::attachment_cost(k, &vec![0; *len]))
        .sum();
    assert_eq!(cost, coord_state::MAX_LEASE_BYTES, "at the quota exactly");
    let mut seq = 2;
    for (k, len) in &attached {
        let (_, outcome) = apply(
            &mut a,
            &command(
                seq,
                CanonicalOperation::Put(PutOp {
                    key: k.clone(),
                    value: vec![seq as u8; *len],
                    lease: Some(LEASE),
                    prev_kv: false,
                }),
            ),
        );
        assert!(
            matches!(outcome, Outcome::Put { prev: None }),
            "{outcome:?}"
        );
        seq += 1;
    }
    // At the quota: any further attachment is refused.
    let (_, outcome) = apply(
        &mut a,
        &command(
            seq,
            CanonicalOperation::Put(PutOp {
                key: b"over".to_vec(),
                value: vec![1],
                lease: Some(LEASE),
                prev_kv: false,
            }),
        ),
    );
    assert_eq!(outcome, Outcome::ErrLeaseQuota);
    seq += 1;
    let epoch = LeaseAuthorityEpoch::new(1).unwrap();
    let (_, outcome) = apply(
        &mut a,
        &command(seq, CanonicalOperation::EstablishLeaseAuthority { epoch }),
    );
    assert_eq!(outcome, Outcome::LeaseAuthorityEstablished { epoch });
    seq += 1;
    let before = a.kv_revision().unwrap();
    let (expired, outcome) = apply(
        &mut a,
        &command(
            seq,
            CanonicalOperation::ExpireLease {
                lease_id: LEASE,
                generation,
                expected_renewal_sequence: 0,
                authority_epoch: epoch,
            },
        ),
    );
    assert_eq!(
        outcome,
        Outcome::LeaseExpired {
            deleted: attached.len() as u64
        }
    );
    assert_eq!(a.kv_revision().unwrap().get(), before.get() + 1);
    out.push((expired, outcome));
    out
}

/// The two stores answer the same, byte for byte, at the same positions.
fn same(journal: &[(AppliedOutcome, Outcome)], worker: &[(AppliedOutcome, Outcome)]) {
    assert_eq!(journal.len(), worker.len());
    for ((j, jo), (w, wo)) in journal.iter().zip(worker) {
        assert_eq!(jo, wo);
        assert_eq!(j.position, w.position);
        assert_eq!(j.revision, w.revision);
        assert_eq!(j.response, w.response);
        assert_eq!(j.result_digest, w.result_digest);
    }
}

#[test]
fn a_write_too_large_for_one_record_is_refused_alike_on_a_journal_and_a_projection() {
    same(&oversized_put(journaled()), &oversized_put(worker()));
}

#[test]
fn a_delete_too_large_for_one_record_is_refused_alike_on_a_journal_and_a_projection() {
    same(&oversized_delete(journaled()), &oversized_delete(worker()));
}

#[test]
fn a_delete_of_more_rows_than_a_record_holds_is_refused_alike_on_a_journal_and_a_projection() {
    same(&too_many_rows(journaled()), &too_many_rows(worker()));
}

#[test]
fn a_key_of_zeros_at_its_limit_is_put_read_and_revoked_alike_on_a_journal_and_a_projection() {
    same(&escaped_key(journaled()), &escaped_key(worker()));
}

#[test]
fn a_lease_at_its_byte_quota_expires_alike_on_a_journal_and_a_projection() {
    same(&lease_at_quota(journaled()), &lease_at_quota(worker()));
}
